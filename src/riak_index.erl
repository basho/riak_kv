%% -------------------------------------------------------------------
%%
%% riak_index: central module for indexing.
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

%% @doc central module for indexing.

-module(riak_index).
-export([
         mapred_index/2,
         mapred_index/3,
         parse_object_hook/1,
         parse_object/1,
         parse_fields/1,
         format_failure_reason/1,
         normalize_index_field/1,
         timestamp/0,
         to_index_query/2,
         to_index_query/3
        ]).
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include("riak_kv_wm_raw.hrl").
-define(TIMEOUT, 30000).
-define(BUCKETFIELD, <<"$bucket">>).
-define(KEYFIELD, <<"$key">>).

%% @type data_type_defs()  :: [data_type_def()].
%% @type data_type_def()   :: {MatchFunction::function(), ParseFunction::function()}.
%% @type failure_reason()  :: {unknown_field_type, Field :: binary()}
%%                          | {field_parsing_failed, {Field :: binary(), Value :: binary()}}.

%% @type bucketname()      :: binary().
%% @type index_field()     :: binary().
%% @type index_value()     :: binary() | integer().
%% @type query_element()   :: {eq,    index_field(), [index_value()]},
%%                         :: {range, index_field(), [index_value(), index_value()}


mapred_index(Dest, Args) ->
    mapred_index(Dest, Args, ?TIMEOUT).
mapred_index(_Pipe, [Bucket, Query], Timeout) ->
    {ok, C} = riak:local_client(),
    {ok, ReqId} = C:stream_get_index(Bucket, Query, Timeout),
    {ok, Bucket, ReqId}.

%% @spec parse_object_hook(riak_object:riak_object()) ->
%%         riak_object:riak_object() | {fail, [failure_reason()]}.
%%
%% @doc Parse the index fields stored in object metadata. Conforms to
%%      the pre-commit hook interface. Return the new object with
%%      parsed fields stuffed into the matadata if validation was
%%      successful, or {fail, [Reasons]} if validation failed. Reason
%%      is either `{unknown_field_type, Field}` or
%%      `{field_parsing_failed, {Field, Value}}.`
parse_object_hook(RObj) ->
    %% Ensure that the object only has a single metadata, or fail
    %% loudly.
    case riak_object:value_count(RObj) == 1 of
        true ->
            %% Parse the object and update the metadata...
            case parse_object(RObj) of
                {ok, ParsedFields} ->
                    MD1 = riak_object:get_metadata(RObj),
                    MD2 = dict:store(?MD_INDEX, ParsedFields, MD1),
                    riak_object:update_metadata(RObj, MD2);
                {error, Reasons} ->
                    {fail, Reasons}
            end;
        false ->
            %% The object has siblings. This can only happen when a
            %% put is initated through the riak_client:put/N function.
            %% Any operation that occurs through the HTTP or PB
            %% interface is forced to resolve siblings and send back a
            %% single update.
            lager:error("Siblings not allowed: ~p", [RObj]),
            {fail, {siblings_not_allowed, RObj}}
    end.

%% @spec parse_object(riak_object:riak_object()) -> {ok, [{Field::binary(), Val :: term()}]}
%%                                                | {error, [failure_reason()]}.
%%
%% @doc Pull out index fields stored in the metadata of the provided
%%      Riak Object. Parse the fields, and return {ok, [{Field,
%%      Value}]} if successful, or {error, [Reasons]} on error. Reason
%%      is either `{unknown_field_type, Field}` or
%%      `{field_parsing_failed, {Field, Value}}.`
parse_object(RObj) ->
    %% For each object metadata, pull out any IndexFields. This could
    %% be called on a write with siblings, so we need to examine *all*
    %% metadatas.
    F = fun(X, Acc) ->
                case dict:find(?MD_INDEX, X) of
                    {ok, IFs} ->
                        IFs ++ Acc;
                    error ->
                        Acc
                end
        end,
    IndexFields = lists:foldl(F, [], riak_object:get_metadatas(RObj)),

    %% Now parse the fields, returning the result.
    parse_fields(IndexFields).

%% @spec parse_fields([Field :: {Key:binary(), Value :: binary()}]) ->
%%       {ok, [{Field :: binary(), Value :: term()}]} | {error, [failure_reason()]}.
%%
%% @doc Parse the provided index fields. Returns {ok, Fields} if the
%%      parsing was successful, or {error, Reasons} if parsing
%%      failed. Reason is either `{unknown_field_type, Field}` or
%%      `{field_parsing_failed, {Field, Value}}.`
parse_fields(IndexFields) ->
    %% Call parse_field on each field, and accumulate in ResultAcc or
    %% ErrorAcc, depending on whether the operation was successful.
    Types = field_types(),
    F = fun({Field, Value}, {ResultAcc, ErrorAcc}) ->
                Field1 = normalize_index_field(Field),
                case parse_field(Field1, Value, Types) of
                    {ok, ParsedValue} ->
                        NewResultAcc = [{Field1, ParsedValue} | ResultAcc],
                        {NewResultAcc, ErrorAcc};
                    {error, Reason} ->
                        NewErrorAcc = [Reason | ErrorAcc],
                        {ResultAcc, NewErrorAcc}
                end
        end,
    {Results, FailureReasons} = lists:foldl(F, {[],[]}, IndexFields),

    %% Return the object, or a list of Reasons.
    case FailureReasons == [] of
        true  -> {ok, lists:usort(Results)};
        false -> {error, lists:usort(FailureReasons)}
    end.


%% @spec parse_field(Key::binary(), Value::binary(), Types::data_type_defs()) ->
%%         {ok, Value} | {error, Reason}.
%%
%% @doc Parse an index field. Return {ok, Value} on success, or
%%      {error, Reason} if there is a problem. Reason is either
%%      `{unknown_field_type, Field}` or `{field_parsing_failed,
%%      {Field, Value}}.`
parse_field(Key, Value, [Type|Types]) ->
    %% Run the regex to check if the key suffix matches this data
    %% type.
    {Suffix, ParseFunction} = Type,

    %% If this is a match, then parse the field...
    case is_field_match(Key, Suffix) of
        true ->
            %% We have a match. Parse the value.
            case ParseFunction(Value) of
                {ok, ParsedValue} ->
                    {ok, ParsedValue};
                _ ->
                    {error, {field_parsing_failed, {Key, Value}}}
            end;
        false ->
            %% Try the next data type.
            parse_field(Key, Value, Types)
    end;
parse_field(Key, _Value, []) ->
    %% No matching data types, return an error.
    {error, {unknown_field_type, Key}}.


%% @private
%% @spec is_field_match(Key :: binary(), Suffix :: binary()) -> boolean().
%% 
%% @doc Return true if the Key matches the suffix. Special case for $bucket
%% and $key.
is_field_match(Key, ?BUCKETFIELD) ->
    %% When Suffix equals $bucket, then Key must equal $bucket too.
    Key == ?BUCKETFIELD;
is_field_match(Key, ?KEYFIELD) ->
    %% When Suffix equals $key, then Key must equal $key too.
    Key == ?KEYFIELD;
is_field_match(Key, Suffix) when size(Suffix) < size(Key) ->
    %% Perform a pattern match to make sure the Key ends with the
    %% suffix.
    Offset = size(Key) - size(Suffix),
    case Key of
        <<_:Offset/binary, Suffix/binary>> -> 
            true;
        _ -> 
            false
    end;
is_field_match(_, _) ->
    false.

%% @spec format_failure_reason(FailureReason :: {atom(), term()}) -> string().
%%
%% @doc Given a failure reason, turn it into a human-readable string.
format_failure_reason(FailureReason) ->
    case FailureReason of
        {unknown_field_type, Field} ->
            io_lib:format("Unknown field type for field: '~s'.~n", [Field]);
        {field_parsing_failed, {Field, Value}} ->
            io_lib:format("Could not parse field '~s', value '~s'.~n", [Field, Value])
    end.

%% @spec timestamp() -> integer().
%%
%% @doc Get a timestamp, the number of milliseconds returned by
%%      erlang:now().
timestamp() ->
    {MegaSeconds,Seconds,MilliSeconds}=os:timestamp(),
    (MegaSeconds * 1000000000000) + (Seconds * 1000000) + MilliSeconds.


to_index_query(IndexField, Args) ->
    to_index_query(IndexField, Args, false).

%% @spec to_index_query(binary(), [binary()]) ->
%%         {ok, {atom(), binary(), list(binary())}} | {error, Reasons}.
%% @doc Given an IndexOp, IndexName, and Args, construct and return a
%%      valid query, or a list of errors if the query is malformed.
to_index_query(IndexField, Args, ReturnTerms) ->
    %% Normalize the index field...
    IndexField1 = riak_index:normalize_index_field(IndexField),

    %% Normalize the arguments...
    case riak_index:parse_fields([{IndexField1, X} || X <- Args]) of
        {ok, []} ->
            {error, {too_few_arguments, Args}};

        {ok, [{_, Value}]} ->
            %% One argument == exact match query
            {ok, {eq, IndexField1, Value}};

        {ok, [{Field, Start}, {Field, End}]} ->
            %% Two arguments == range query
            case End > Start of
                true ->
                    case {Field, ReturnTerms} of
                        {?KEYFIELD, _} ->
                            %% Not a range query where we can return the index term, since
                            %% it is the Key itself
                            {ok, {range, IndexField1, Start, End}};
                        {_, true} ->
                            {ok, {range, IndexField1, Start, End, true}};
                        {_, false} ->
                            {ok,{range, IndexField1, Start, End}}
                    end;
                false ->
                    {error, {invalid_range, Args}}
            end;

        {ok, _} ->
            {error, {too_many_arguments, Args}};

        {error, FailureReasons} ->
            {error, FailureReasons}
    end.


%% @spec field_types() -> data_type_defs().
%%
%% @doc Return a list of {MatchFunction, ParseFunction} tuples that
%%      map a field name to a field type.
field_types() ->
    [
     {?BUCKETFIELD, fun parse_binary/1},
     {?KEYFIELD,    fun parse_binary/1},
     {<<"_bin">>,    fun parse_binary/1},
     {<<"_int">>,    fun parse_integer/1}
    ].


%% @private
%% @spec parse_binary(string()) -> {ok, binary()}
%%
%% @doc Parse a primary key field. Transforms value to a binary.
parse_binary(Value) when is_binary(Value) ->
    {ok, Value};
parse_binary(Value) when is_list(Value) ->
    {ok, list_to_binary(Value)}.

%% @private
%% @spec parse_integer(string()) -> {ok, integer()} | {error, Reason}
%%
%% @doc Parse a string into an integer value.
parse_integer(Value) when is_integer(Value) ->
    {ok, Value};
parse_integer(Value) when is_binary(Value) ->
    parse_integer(binary_to_list(Value));
parse_integer(Value) when is_list(Value) ->
    try
        {ok, list_to_integer(Value)}
    catch
        _Type : Reason ->
            {error, Reason}
    end.

normalize_index_field(V) when is_binary(V) ->
    normalize_index_field(binary_to_list(V));
normalize_index_field(V) when is_list(V) ->
    list_to_binary(string:to_lower(V)).

%% ====================
%% TESTS
%% ====================

-ifdef(TEST).

parse_binary_test() ->
    ?assertMatch({ok, <<"">>}, parse_binary(<<"">>)),
    ?assertMatch({ok, <<"A">>}, parse_binary(<<"A">>)),
    ?assertMatch({ok, <<"123">>}, parse_binary(<<"123">>)),
    ?assertMatch({ok, <<"4.56">>}, parse_binary(<<"4.56">>)),
    ?assertMatch({ok, <<".789">>}, parse_binary(<<".789">>)).

parse_integer_test() ->
    ?assertMatch({error, _}, parse_integer(<<"">>)),
    ?assertMatch({error, _}, parse_integer(<<"A">>)),
    ?assertMatch({ok, 123}, parse_integer(<<"123">>)),
    ?assertMatch({error, _}, parse_integer(<<"4.56">>)),
    ?assertMatch({error, _}, parse_integer(<<".789">>)).

parse_field_bin_test() ->
    %% Test parsing of "*_bin" fields...
    Types = field_types(),
    F = fun(Key, Value) -> parse_field(Key, Value, Types) end,

    ?assertMatch(
       {ok, <<"">>},
       F(<<"field_bin">>, <<"">>)),

    ?assertMatch(
       {ok, <<"A">>},
       F(<<"field_bin">>, <<"A">>)),

    ?assertMatch(
       {ok, <<"123">>},
       F(<<"field_bin">>, <<"123">>)).

parse_field_integer_test() ->
    %% Test parsing of "*_int" fields...
    Types = field_types(),
    F = fun(Key, Value) -> parse_field(Key, Value, Types) end,

    ?assertMatch(
       {error, {field_parsing_failed, {<<"field_int">>, <<"">>}}},
       F(<<"field_int">>, <<"">>)),

    ?assertMatch(
       {error, {field_parsing_failed, {<<"field_int">>, <<"A">>}}},
       F(<<"field_int">>, <<"A">>)),

    ?assertMatch(
       {ok, 123},
       F(<<"field_int">>, <<"123">>)),

    ?assertMatch(
       {error, {field_parsing_failed, {<<"field_int">>, <<"4.56">>}}},
       F(<<"field_int">>, <<"4.56">>)),

    ?assertMatch(
       {error, {field_parsing_failed, {<<"field_int">>, <<".789">>}}},
       F(<<"field_int">>, <<".789">>)).

validate_unknown_field_type_test() ->
    %% Test error on unknown field types.
    Types = field_types(),
    F = fun(Key, Value) -> parse_field(Key, Value, Types) end,

    ?assertMatch(
       {error, {unknown_field_type, <<"unknowntype">>}},
       F(<<"unknowntype">>, <<"A">>)),

    ?assertMatch(
       {error, {unknown_field_type, <<"test_$bucket">>}},
       F(<<"test_$bucket">>, <<"A">>)),

    ?assertMatch(
       {error, {unknown_field_type, <<"test_$key">>}},
       F(<<"test_$key">>, <<"A">>)),

    ?assertMatch(
       {error, {unknown_field_type, <<"_int">>}},
       F(<<"_int">>, <<"A">>)),

    ?assertMatch(
       {error, {unknown_field_type, <<"_bin">>}},
       F(<<"_bin">>, <<"A">>)).


parse_object_hook_test() ->
    %% Helper function to create an object using a proplist of
    %% supplied data, and call parse_object_hook on it.
    F = fun(MetaDataList) ->
                Obj = riak_object:new(<<"B">>, <<"K">>, <<"VAL">>, dict:from_list([{?MD_INDEX, MetaDataList}])),
                parse_object_hook(Obj)
        end,

    ?assertMatch(
       {r_object, _, _, _, _, _, _},
       F([
          {<<"field_bin">>, <<"A">>},
          {<<"field_int">>, <<"1">>}
         ])),

    ?assertMatch(
       {fail, [{field_parsing_failed, {<<"field_int">>, <<"A">>}}]},
       F([
          {<<"field_bin">>, <<"A">>},
          {<<"field_int">>, <<"A">>}
         ])),

    ?assertMatch(
       {fail, [
               {field_parsing_failed, {<<"field_int">>, <<"A">>}}
              ]},
       F([
          {<<"field_bin">>, <<"A">>},
          {<<"field_int">>, <<"1">>},
          {<<"field_int">>, <<"A">>}
         ])),

    ?assertMatch(
       {fail, [
               {field_parsing_failed, {<<"field_int">>, <<"A">>}},
               {unknown_field_type, <<"field_foo">>}
              ]},
       F([
          {<<"field_bin">>, <<"A">>},
          {<<"field_int">>, <<"A">>},
          {<<"field_foo">>, <<"fail">>}
         ])).


-ifdef(SLF_BROKEN_TEST).
parse_object_test() ->
    %% Helper function to create an object using a proplist of
    %% supplied data, and call validate_object on it.
    F = fun(MetaDataList) ->
                Obj = riak_object:new(<<"B">>, <<"K">>, <<"VAL">>, dict:from_list([{?MD_INDEX, MetaDataList}])),
                parse_object(Obj)
        end,

    ?assertMatch(
       {ok, [
             {<<"field_bin">>, <<"A">>},
             {<<"field_int">>, 1}
       ]},
       F([
          {<<"field_bin">>, <<"A">>},
          {<<"field_int">>, <<"1">>}
         ])),

    ?assertMatch(
       {ok, [
             {?BUCKETFIELD, <<"B">>},
             {?KEYFIELD, <<"K">>}
       ]},
       F([
          {<<"$bucket">>, <<"ignored">>},
          {<<"$key">>, <<"ignored">>}
         ])).
-endif. % SLF_BROKEN_TEST

-endif.
