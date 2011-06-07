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
         validate_object_hook/1,
         parse_object/1,
         parse_fields/1,
         format_failure_reason/1,
         timestamp/0
        ]).
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include("riak_kv_wm_raw.hrl").

%% @type data_type_defs()  :: [data_type_def()].
%% @type data_type_def()   :: {CompiledRegex::re:mp(), Module::module()}.
%% @type failure_reason()  :: {unknown_field_type, Field :: string()}
%%                          | {field_parsing_failed, {Field :: string(), Value :: string()}}.

%% @spec validate_object_hook(riak_object:riak_object()) -> 
%%         riak_object:riak_object() | {fail, [failure_reason()]}.
%%
%% @doc Validate the index fields stored in object metadata. Conforms
%%      to the pre-commit hook interface. Return the unmodified object
%%      if validation was successful, or {fail, [Reasons]} if validation
%%      failed. Reason is either `{unknown_field_type, Field}` or
%%      `{field_parsing_failed, {Field, Value}}.`
validate_object_hook(RObj) ->
    case parse_object(RObj) of
        {ok, _} -> 
            RObj;
        {error, Reasons} ->
            {fail, Reasons}
    end.

%% @spec parse_object(riak_object:riak_object()) -> {ok, [{Field::string(), Val :: term()}]}
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


%% @spec parse_fields([Field :: {Key:string(), Value :: string()}]) -> 
%%       {ok, [{Field :: string(), Value :: term()}]} | {error, [failure_reason()]}.
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
                case parse_field(Field, Value, Types) of
                    {ok, ParsedValue} -> 
                        NewResultAcc = [{Field, ParsedValue} | ResultAcc],
                        {NewResultAcc, ErrorAcc};
                    {error, Reason} -> 
                        NewErrorAcc = [Reason | ErrorAcc],
                        {ResultAcc, NewErrorAcc}
                end
        end,
    {Results, FailureReasons} = lists:foldl(F, {[],[]}, IndexFields),
                
    %% Return the object, or a list of Reasons.
    case FailureReasons == [] of
        true  -> {ok, lists:reverse(Results)};
        false -> {error, lists:reverse(FailureReasons)}
    end.


%% @spec parse_field(Key::string(), Value::string(), Types::data_type_defs()) -> 
%%         {ok, Value} | {error, Reason}.
%%
%% @doc Parse an index field. Return {ok, Value} on success, or
%%      {error, Reason} if there is a problem. Reason is either
%%      `{unknown_field_type, Field}` or `{field_parsing_failed,
%%      {Field, Value}}.`
parse_field(Key, Value, [Type|Types]) ->
    %% Run the regex to check if the key suffix matches this data
    %% type.
    {RE, Function} = Type,
    case re:run(Key, RE) of
        {match, _} ->
            %% We have a match. Parse the value.
            case Function(Value) of
                {ok, ParsedValue} -> 
                    {ok, ParsedValue};
                _ -> 
                    {error, {field_parsing_failed, {Key, Value}}}
            end;
        nomatch ->
            %% Try the next data type.
            parse_field(Key, Value, Types)
    end;
parse_field(Key, _Value, []) ->
    %% No matching data types, return an error.
    {error, {unknown_field_type, Key}}.

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
    {MegaSeconds,Seconds,MilliSeconds}=erlang:now(),
    (MegaSeconds * 1000000000000) + (Seconds * 1000000) + MilliSeconds.

%% @spec field_types() -> data_type_defs().
%%
%% @doc Return a list of {Regex, Function} records that map a
%%      field name to a field type. 
field_types() -> 
    F = fun(S) -> 
        {ok, RE} = re:compile(S),
        RE
    end,
    [
     {F(".*_id"),    fun parse_id/1},
     {F(".*_int"),   fun parse_integer/1},
     {F(".*_float"), fun parse_float/1}
    ].

%% @private
%% @spec parse_id(string()) -> {ok, string()}
%%
%% @doc Parse an 'id' field. Just return the field.
parse_id(Value) -> 
    {ok, Value}.

%% @private
%% @spec parse_integer(string()) -> {ok, integer()} | {error, Reason}
%%
%% @doc Parse a string into an integer value.
parse_integer(Value) ->
    try 
        {ok, list_to_integer(Value)}
    catch 
        _Type : Reason ->
            {error, Reason}
    end.

%% @private
%% @spec parse_float(string()) -> {ok, float()} | {error, Reason}
%%
%% @doc Parse a string into a float value.
parse_float(Value) when Value /= ""->
    %% Erlang chokes on floats that start with a decimal point or
    %% don't contain a decimal point.  Normalize the incoming value
    %% for these conditions. We accept both decimal notation (3.14, 3,
    %% .14) and scientific notation (3.14e5, 3.14E5).
    Value1 = "0" ++ Value,
    Value2 = case string:str(Value1, ".") of
                 0 ->
                     %% If no decimal, then add one with a trailing
                     %% zero.
                     Value1 ++ ".0";
                 _ -> 
                     %% Otherwise nothing to do.
                     Value1
             end,
    
    try
        {ok, list_to_float(Value2)}
    catch
        _Type : Reason ->
            {error, Reason}
    end;
parse_float("") ->
    %% Explicitly checking the empty string case because otherwise our
    %% normalization would fix it into 0.0. This stays consistent with
    %% parse_integer/1 above.
    {error, "Missing value."}.


%% ====================
%% TESTS
%% ====================

-ifdef(TEST).

parse_id_test() ->
    ?assertMatch({ok, ""}, parse_id("")),
    ?assertMatch({ok, "A"}, parse_id("A")),
    ?assertMatch({ok, "123"}, parse_id("123")),
    ?assertMatch({ok, "4.56"}, parse_id("4.56")),
    ?assertMatch({ok, ".789"}, parse_id(".789")).

parse_integer_test() ->
    ?assertMatch({error, _}, parse_integer("")),
    ?assertMatch({error, _}, parse_integer("A")),
    ?assertMatch({ok, 123}, parse_integer("123")),
    ?assertMatch({error, _}, parse_integer("4.56")),
    ?assertMatch({error, _}, parse_integer(".789")).

parse_float_test() ->
    ?assertMatch({error, _}, parse_float("")),
    ?assertMatch({error, _}, parse_float("A")),
    ?assertMatch({ok, 123.0}, parse_float("123")),
    ?assertMatch({ok, 4.56}, parse_float("4.56")),
    ?assertMatch({ok, 0.789}, parse_float(".789")).

parse_field_id_test() ->
    %% Test parsing of "*_id" fields...
    Types = field_types(),
    F = fun(Key, Value) -> parse_field(Key, Value, Types) end,

    ?assertMatch(
       {ok, ""}, 
       F("field_id", "")),

    ?assertMatch(
       {ok, "A"}, 
       F("field_id", "A")),

    ?assertMatch(
       {ok, "123"}, 
       F("field_id", "123")).

parse_field_integer_test() ->
    %% Test parsing of "*_int" fields...
    Types = field_types(),
    F = fun(Key, Value) -> parse_field(Key, Value, Types) end,

    ?assertMatch(
       {error, {field_parsing_failed, {"field_int", ""}}}, 
       F("field_int", "")),

    ?assertMatch(
       {error, {field_parsing_failed, {"field_int", "A"}}}, 
       F("field_int", "A")),

    ?assertMatch(
       {ok, 123},
       F("field_int", "123")),

    ?assertMatch(
       {error, {field_parsing_failed, {"field_int", "4.56"}}}, 
       F("field_int", "4.56")),

    ?assertMatch(
       {error, {field_parsing_failed, {"field_int", ".789"}}}, 
       F("field_int", ".789")).


validate_field_float_test() ->
    %% Test parsing of "*_float" fields...
    Types = field_types(),
    F = fun(Key, Value) -> parse_field(Key, Value, Types) end,

    ?assertMatch(
       {error, {field_parsing_failed, {"field_float", ""}}}, 
       F("field_float", "")),

    ?assertMatch(
       {error, {field_parsing_failed, {"field_float", "A"}}}, 
       F("field_float", "A")),

    ?assertMatch(
       {ok, 123.0},
       F("field_float", "123")),

    ?assertMatch(
       {ok, 4.56},
       F("field_float", "4.56")),

    ?assertMatch(
       {ok, 0.789},
       F("field_float", ".789")),

    ?assertMatch(
       {ok, 1.0e5},
       F("field_float", "1.0e5")).


validate_unknown_field_type_test() ->
    %% Test error on unknown field types.
    Types = field_types(),
    F = fun(Key, Value) -> parse_field(Key, Value, Types) end,

    ?assertMatch(
       {error, {unknown_field_type, "unknowntype"}}, 
       F("unknowntype", "A")).

validate_object_test() ->
    %% Helper function to create an object using a proplist of
    %% supplied data, and call validate_object on it.
    F = fun(MetaDataList) ->
                Obj = riak_object:new(<<"B">>, <<"K">>, <<"VAL">>, dict:from_list([{?MD_INDEX, MetaDataList}])),
                validate_object_hook(Obj)
        end,

    ?assertMatch(
       {r_object, _, _, _, _, _, _},
       F([
          {"field_id", "A"},
          {"field_int", "1"},
          {"field_float", "0.5"}
         ])),

    ?assertMatch(
       {fail, [{field_parsing_failed, {"field_int", "A"}}]},
       F([
          {"field_id", "A"},
          {"field_int", "A"},
          {"field_float", "0.5"}
         ])),

    ?assertMatch(
       {fail, [
               {field_parsing_failed, {"field_int", "A"}},
               {field_parsing_failed, {"field_float", "B"}}
              ]},
       F([
          {"field_id", "A"},
          {"field_int", "1"},
          {"field_int", "A"},
          {"field_float", "0.5"},
          {"field_float", "B"}
         ])),

    ?assertMatch(
       {fail, [
               {field_parsing_failed, {"field_int", "A"}},
               {unknown_field_type, "field_foo"}
              ]},
       F([
          {"field_id", "A"},
          {"field_int", "A"},
          {"field_foo", "fail"},
          {"field_float", "0.5"}
         ])).

-endif.
