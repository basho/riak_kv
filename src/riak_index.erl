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
         to_index_query/1,
         to_index_query/2,
         make_continuation/1,
         return_terms/2,
         return_body/1,
         upgrade_query/1,
         object_key_in_range/3,
         index_key_in_range/3,
         add_timeout_opt/2
        ]).
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include("riak_kv_wm_raw.hrl").
-include("riak_kv_index.hrl").
-define(TIMEOUT, 30000).
-define(BUCKETFIELD, <<"$bucket">>).
-define(KEYFIELD, <<"$key">>).
-define(CURRENT_2I_VERSION, v3).

%% See GH610, this default is for backwards compat, so 2i behaves as
%% it did before the FSM timeout bug was "fixed"
-define(DEFAULT_TIMEOUT, infinity).

%% @type data_type_defs()  :: [data_type_def()].
%% @type data_type_def()   :: {MatchFunction::function(), ParseFunction::function()}.
%% @type failure_reason()  :: {unknown_field_type, Field :: binary()}
%%                          | {field_parsing_failed, {Field :: binary(), Value :: binary()}}.

%% @type bucketname()      :: binary().
%% @type index_field()     :: binary().
%% @type index_value()     :: binary() | integer().
%% @type query_element()   :: {eq,    index_field(), [index_value()]},
%%                         :: {range, index_field(), [index_value(), index_value()}

-type query_def() :: {ok, term()} | {error, term()} | {term(), {error, term()}}.
-export_type([query_def/0]).

-type last_result() :: {value(), key()} | key().
-type value() :: binary() | integer().
-type key() :: binary().
-type continuation() :: binary() | undefined. %% encoded last_result().

-type query_version() :: v1 | v2 | v3.
mapred_index(Dest, Args) ->
    mapred_index(Dest, Args, ?TIMEOUT).
mapred_index(_Pipe, [Bucket, Query], Timeout) ->
    {ok, C} = riak:local_client(),
    {ok, ReqId, _} = C:stream_get_index(Bucket, Query, [{timeout, Timeout}]),
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

parse_field_if_defined(_, undefined) ->
    {ok, undefined};
parse_field_if_defined(Field, Val) ->
    parse_field(Field, Val, field_types()).

to_index_query(?CURRENT_2I_VERSION, Args) ->
    Defaults = ?KV_INDEX_Q{},
    [Field1, Start, StartInc, End, EndInc,
     TermRegex, ReturnBody, MaxResults, Cont
    ] =
        [proplists:get_value(F, Args, Default) ||
            {F, Default} <-
            [{field, Defaults?KV_INDEX_Q.filter_field},
             {start_term, Defaults?KV_INDEX_Q.start_term},
             {start_inclusive, Defaults?KV_INDEX_Q.start_inclusive},
             {end_term, Defaults?KV_INDEX_Q.end_term},
             {end_inclusive, Defaults?KV_INDEX_Q.end_inclusive},
             {term_regex, Defaults?KV_INDEX_Q.term_regex},
             {return_body, Defaults?KV_INDEX_Q.return_body},
             {max_results, Defaults?KV_INDEX_Q.max_results},
             {continuation, undefined}]],
    Field = normalize_index_field(Field1),
    SKeyDefault = case Field of
        ?KEYFIELD -> Start;
        _ -> Defaults?KV_INDEX_Q.start_key
    end,
    StartKey = proplists:get_value(start_key, Args, SKeyDefault),
    %% Internal return terms, not to be confused with user facing
    %% return terms in the external clients.
    ReturnTerms = proplists:get_value(return_terms, Args, true) andalso
    Field /= ?KEYFIELD,

    case {parse_field_if_defined(Field, Start),
          parse_field_if_defined(Field, End)} of
        {{ok, NormStart}, {ok, NormEnd}} ->
            Req1 = ?KV_INDEX_Q{
                    start_key=StartKey,
                    filter_field=Field,
                    start_term=NormStart,
                    end_term=NormEnd,
                    return_terms=ReturnTerms,
                    start_inclusive=StartInc,
                    end_inclusive=EndInc,
                    return_body=ReturnBody,
                    term_regex=TermRegex,
                    max_results=MaxResults
                     },
            case Cont of
                undefined ->
                    {ok, Req1};
                _ ->
                    apply_continuation(Req1, decode_continuation(Cont))
            end;
        {{error, _} = Err, _} ->
            Err;
        {_, {error, _} = Err} ->
            Err
    end;
to_index_query(OldVersion, Args) ->
    case to_index_query(?CURRENT_2I_VERSION, Args) of
        {ok, Req} ->
            downgrade_query(OldVersion, Req);
        Err ->
            Err
    end.

to_index_query(Args) ->
    Version = riak_core_capability:get({riak_kv, secondary_index_version}, v1),
    to_index_query(Version, Args).

%% @doc upgrade a V1 Query to a v2 Query
make_query({eq, ?BUCKETFIELD, _Bucket}, Q) ->
    {ok, Q?KV_INDEX_Q{filter_field=?BUCKETFIELD, return_terms=false}};
make_query({eq, ?KEYFIELD, Value}, Q) ->
    {ok, Q?KV_INDEX_Q{filter_field=?KEYFIELD, start_key=Value, start_term=Value,
                 end_term=Value, return_terms=false}};
make_query({eq, Field, Value}, Q) ->
    {ok, Q?KV_INDEX_Q{filter_field=Field, start_term=Value, end_term=Value, return_terms=false}};
make_query({range, ?KEYFIELD, Start, End}, Q) ->
    {ok, Q?KV_INDEX_Q{filter_field=?KEYFIELD, start_term=Start, start_key=Start,
                end_term=End, return_terms=false}};
make_query({range, Field, Start, End}, Q) ->
    {ok, Q?KV_INDEX_Q{filter_field=Field, start_term=Start,
                 end_term=End, return_terms=false}};
make_query(V1Q, _) ->
    {error, {invalid_v1_query, V1Q}}.

%% @doc if a continuation is specified, use it to update the query
apply_continuation(Q, undefined) ->
    {ok, Q};
apply_continuation(Q=?KV_INDEX_Q{}, {Value, Key}) when is_binary(Key),
                                                                   is_integer(Value) orelse is_binary(Value) ->
    {ok, Q?KV_INDEX_Q{start_key=Key, start_inclusive=false, start_term=Value}};
apply_continuation(Q=?KV_INDEX_Q{filter_field=FF}, Key) when is_binary(Key), FF /= ?KEYFIELD, FF /= ?BUCKETFIELD ->
    %% an EQ index, never change start_term, only start key
    {ok, Q?KV_INDEX_Q{start_key=Key, start_inclusive=false}};
apply_continuation(Q=?KV_INDEX_Q{}, Key) when is_binary(Key) ->
    %% a Keys / bucket index
    {ok, Q?KV_INDEX_Q{start_key=Key, start_term=Key, start_inclusive=false}};
apply_continuation(Q, C) ->
    {error, {invalid_continuation, C, Q}}.

%% @doc upgrade a query to the current latest version
upgrade_query(Q=?KV_INDEX_Q{}) ->
    Q;
upgrade_query(#riak_kv_index_v2{
                start_key=StartKey,
                filter_field=Field,
                start_term=StartTerm,
                end_term=EndTerm,
                return_terms=ReturnTerms,
                start_inclusive=StartInclusive,
                end_inclusive=EndInclusive,
                return_body=ReturnBody}) ->
    ?KV_INDEX_Q{
        start_key=StartKey,
        filter_field=Field,
        start_term=StartTerm,
        end_term=EndTerm,
        return_terms=ReturnTerms,
        start_inclusive=StartInclusive,
        end_inclusive=EndInclusive,
        return_body=ReturnBody};
upgrade_query(Q) when is_tuple(Q) ->
    {ok, Q2} = make_query(Q, ?KV_INDEX_Q{}),
    Q2.

%% @doc Downgrade a lastest version query record to a previous version.
-spec downgrade_query(V :: query_version(), ?KV_INDEX_Q{}) ->
    {ok, tuple() | #riak_kv_index_v2{}} | {error, any()}.
downgrade_query(v1, ?KV_INDEX_Q{
                       filter_field=Field, start_term=Start, end_term=Start,
                       return_terms=false}) ->
    {ok, {eq, Field, Start}};
downgrade_query(v1, ?KV_INDEX_Q{
                filter_field=Field, start_term=Start, end_term=End}) ->
    {ok, {range, Field, Start, End}};
downgrade_query(v2, ?KV_INDEX_Q{
                filter_field=Field, start_key=StartKey,
                start_term=StartTerm, start_inclusive=StartInc,
                end_term=EndTerm, end_inclusive=EndInc,
                return_terms=ReturnTerms, return_body=ReturnBody}) ->
    {ok, #riak_kv_index_v2{
            filter_field = Field, start_key=StartKey,
            start_term=StartTerm, start_inclusive=StartInc,
            end_term=EndTerm, end_inclusive=EndInc,
            return_terms=ReturnTerms, return_body=ReturnBody
            }};
downgrade_query(V, Q) ->
    {error, {downgrade_not_supported, V, Q}}.

%% @doc Should index terms be returned in a result
%% to the client. Requires both that they are wanted (arg1)
%% and available (arg2)
return_terms(false, _) ->
    false;
return_terms(true, ?KV_INDEX_Q{return_terms=ReturnTerms}) ->
    ReturnTerms;
return_terms(true, OldQ) ->
    return_terms(true, upgrade_query(OldQ));
return_terms(_, _) ->
    false.

%% @doc Should the object body of an indexed key
%% be returned with the result?
return_body(?KV_INDEX_Q{return_body=true, filter_field=FF})
  when FF =:= ?KEYFIELD;
       FF =:= ?BUCKETFIELD ->
    true;
return_body(_) ->
    false.

%% @doc is an index key in range for a 2i query?
index_key_in_range({Bucket, Key, Field, Term}=IK, Bucket,
                   ?KV_INDEX_Q{filter_field=Field,
                               start_key=StartKey,
                               start_inclusive=StartInc,
                               start_term=StartTerm,
                               end_term=EndTerm})
  when Term >= StartTerm,
       Term =< EndTerm ->
    in_range(gt(StartInc, {Term, Key}, {StartTerm, StartKey}), true, IK);
index_key_in_range(ignore, _, _) ->
    {skip, ignore};
index_key_in_range(_, _, _) ->
    false.

%% @doc Is a {bucket, key} pair in range for an index query
object_key_in_range({Bucket, Key} = OK, Bucket,
                    ?KV_INDEX_Q{end_term=undefined,
                                start_key=Start,
                                start_inclusive=StartInc}) ->
    in_range(gt(StartInc, Key, Start), true, OK);
object_key_in_range({Bucket, Key}=OK, Bucket,
                    ?KV_INDEX_Q{filter_field=?KEYFIELD,
                                start_key=Start,
                                term_regex=TermRe,
                                start_inclusive=StartInc,
                                end_term=End,
                                end_inclusive=EndInc})
        when TermRe =/= undefined ->
    case in_range(gt(StartInc, Key, Start), gt(EndInc, End, Key), OK) of
        {true, OK} ->
            case re:run(Key, TermRe) of
                nomatch -> {skip, OK};
                _ -> {true, OK}
            end;
        Other ->
            Other
    end;
object_key_in_range({Bucket, Key}=OK, Bucket, Q) ->
    ?KV_INDEX_Q{start_key=Start, start_inclusive=StartInc,
                end_term=End, end_inclusive=EndInc} = Q,
    in_range(gt(StartInc, Key, Start), gt(EndInc, End, Key), OK);
object_key_in_range(ignore, _, _) ->
    {skip, ignore};
object_key_in_range(_, _, _) ->
    false.

in_range(true, true, OK) ->
    {true, OK};
in_range(skip, true, OK) ->
    {skip, OK};
in_range(_, _, _) ->
    false.

gt(true, A, B) when A >= B ->
    true;
gt(false, A, B) when A > B ->
    true;
gt(false, A, B) when A == B ->
    skip;
gt(_, _, _) ->
    false.

%% @doc To enable pagination.
%% returns an opaque value that
%% must be passed to
%% `to_index_query/3' to
%% get a query that will "continue"
%% from the given last result
-spec make_continuation(list()) -> continuation().
make_continuation([]) ->
    undefined;
make_continuation(L) ->
    Last = lists:last(L),
    base64:encode(term_to_binary(Last)).

%% @doc decode a continuation received from the outside world.
-spec decode_continuation(continuation() | undefined) -> last_result() | undefined.
decode_continuation(undefined) ->
    undefined;
decode_continuation(Bin) ->
    binary_to_term(base64:decode(Bin)).

%% @doc add the `timeout' option tuple to the
%% `Opts' proplist. if `Timeout' is undefined
%% then the `app.config' property
%% `{riak_kv, seconady_index timeout}' is used
%% If that config property is defined, then
%% a default of `infinity'  is used.
%% We use `infinity' as the default to
%% match the behavior pre 1.4
add_timeout_opt(undefined, Opts) ->
    Timeout = app_helper:get_env(riak_kv, secondary_index_timeout, ?DEFAULT_TIMEOUT),
    [{timeout, Timeout} | Opts];
add_timeout_opt(0, Opts) ->
    [{timeout, infinity} | Opts];
add_timeout_opt(Timeout, Opts) ->
    [{timeout, Timeout} | Opts].

%% @spec field_types() -> data_type_defs().
%%
%% @doc Return a list of {MatchFunction, ParseFunction} tuples that
%%      map a field name to a field type.
field_types() ->
    [
     {?BUCKETFIELD, fun parse_bucket/1},
     {?KEYFIELD,    fun parse_binary/1},
     {<<"_bin">>,    fun parse_binary/1},
     {<<"_int">>,    fun parse_integer/1}
    ].


parse_bucket({Type, Bucket}) when is_binary(Type), is_binary(Bucket) ->
    {ok, {Type, Bucket}};
parse_bucket(Bucket) ->
    parse_binary(Bucket).

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

index_in_range_test() ->
    %% In that case that same Key has multiple values for an index
    %% make sure that we don't skip in range values
    %% when start_inclusive is false
    FF = <<"f1">>,
    K = <<"k">>,
    B = <<"b">>,
    IK = {B, K, FF, 1},
    IK2 = {B, K, FF, 2},
    %% Expect IK to be out of range but IK2 to be in
    Q = ?KV_INDEX_Q{filter_field=FF, start_key=K, start_term=1, start_inclusive=false, end_term=3},
    ?assertEqual({skip, IK}, index_key_in_range(IK, B, Q)),
    ?assertEqual({true, IK2}, index_key_in_range(IK2, B, Q)).

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
