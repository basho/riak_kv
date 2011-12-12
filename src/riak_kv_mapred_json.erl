%% -------------------------------------------------------------------
%%
%% riak_mapred_json: JSON parsing for mapreduce
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

%% @doc JSON parsing for mapreduce

-module(riak_kv_mapred_json).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([parse_request/1, parse_inputs/1, parse_query/1]).
-export([jsonify_not_found/1, dejsonify_not_found/1]).
-export([jsonify_bkeys/2]).
-export([jsonify_pipe_error/2]).

-define(QUERY_TOKEN, <<"query">>).
-define(INPUTS_TOKEN, <<"inputs">>).
-define(TIMEOUT_TOKEN, <<"timeout">>).
-define(DEFAULT_TIMEOUT, 60000).

parse_request(Req) ->
    case catch mochijson2:decode(Req) of
        {struct, MapReduceDesc} ->
            Timeout = case proplists:get_value(?TIMEOUT_TOKEN, MapReduceDesc,
                                               ?DEFAULT_TIMEOUT) of
                          X when is_number(X) andalso X > 0 ->
                              X;
                          _ ->
                              ?DEFAULT_TIMEOUT
                      end,
            Inputs = proplists:get_value(?INPUTS_TOKEN, MapReduceDesc),
            Query = proplists:get_value(?QUERY_TOKEN, MapReduceDesc),
            case not(Inputs =:= undefined) andalso not(Query =:= undefined) of
                true ->
                    case riak_kv_mapred_json:parse_inputs(Inputs) of
                        {ok, ParsedInputs} ->
                            case riak_kv_mapred_json:parse_query(Query) of
                                {ok, ParsedQuery} ->
                                    {ok, ParsedInputs, ParsedQuery, Timeout};
                                {error, Reason} ->
                                    {error, {'query', Reason}}
                            end;
                        {error, Reason} ->
                            {error, {inputs, Reason}}
                    end;
                false ->
                    {error, missing_field}
            end;
        {'EXIT', Message} ->
            {error, {invalid_json, Message}};
        _ ->
            {error, not_json}
    end.

parse_inputs(Bucket) when is_binary(Bucket) ->
    {ok, Bucket};
parse_inputs(Targets) when is_list(Targets) ->
    parse_inputs(Targets, []);
parse_inputs({struct, Inputs}) ->
    %% Determine the input type...
    IsModFun = is_modfun_input(Inputs),
    IsSearch = is_search_input(Inputs),
    IsIndex  = is_index_input(Inputs),
    IsKeyFilter = is_keyfilter_input(Inputs),

    %% Parse the input based on type...
    if
        IsModFun -> parse_modfun_input(Inputs);
        IsSearch -> parse_search_input(Inputs);
        IsIndex  -> parse_index_input(Inputs);
        IsKeyFilter -> parse_keyfilter_input(Inputs);
        true ->
            invalid_input_error({struct, Inputs})
    end;
parse_inputs(Invalid) ->
    invalid_input_error(Invalid).

invalid_input_error(Invalid) ->
    {error,
     ["Unrecognized format of \"inputs\" field:",
      "   ", mochijson2:encode(Invalid), "\n\n",
      "Valid formats are:\n",
      "\n"
      "Key List:\n",
      "  - Map/Reduce over the provided set of keys.\n",
      "  - {\"inputs\":[[\"mybucket\",\"mykey1\"], [\"mybucket\",\"mykey2\"], ...]}\n",
      "\n",
      "Key List with Key Data:\n",
      "  - Map/Reduce over the provided set of keys.\n",
      "  - {\"inputs\":[[\"mybucket\",\"mykey1\", \"mykeydata1\"], [\"mybucket\",\"mykey2\", \"mykeydata1\"], ...]}\n",
      "\n",
      "Index Query - Exact Match:\n",
      "  - Indexing backend must be enabled.\n",
      "  - {\"inputs\":{\"bucket\":\"mybucket\", \"index\":\"myindex_bin\", \"key\":\"mykey\"}}\n",
      "\n",
      "Index Query - Range Search:\n",
      "  - Indexing backend must be enabled.\n",
      "  - Use secondary indexes to generate a starting set of keys.\n",
      "  - {\"inputs\":{\"bucket\":\"mybucket\", \"index\":\"myindex_bin\", \"start\":\"key1\", \"end\":\"key2\"}}\n",
      "\n",
      "Search Query:\n",
      "  - Riak Search must be enabled and bucket hook installed.\n",
      "  - {\"inputs\":{\"bucket\":\"mybucket\", \"query\":\"foo OR bar\"}}\n",
      "\n",
      "Search Query With Filter:\n",
      "  - Riak Search must be enabled and bucket hook installed.\n",
      "  - {\"inputs\":{\"bucket\":\"mybucket\", \"query\":\"field1:(foo OR bar)\", \"filter\":\"field2:baz\"}}\n",
      "\n",
      "Bucket:\n",
      "  - Map/Reduce over all keys in the provided bucket.\n",
      "  - WARNING: THIS CAN BE A SLOW OPERATION!\n",
      "  - {\"inputs\":\"mybucket\"}\n"
      "\n",
      "Bucket With Key Filter:\n",
      "  - Filter keys in a bucket, then Map/Reduce.\n",
      "  - WARNING: THIS CAN BE A SLOW OPERATION!\n",
      "  - {\"inputs\":{\"bucket\":\"mybucket\", \"key_filters\":[[\"matches\", \"foo.*\"]]}}\n\n"
     ]}.

parse_inputs([], Accum) ->
    if
        length(Accum) > 0 ->
            {ok, lists:reverse(Accum)};
        true ->
            {error, "No inputs were given.\n"}
    end;
parse_inputs([[Bucket, Key]|T], Accum) when is_binary(Bucket),
                                             is_binary(Key) ->
    parse_inputs(T, [{Bucket, Key}|Accum]);
parse_inputs([[Bucket, Key, KeyData]|T], Accum) when is_binary(Bucket),
                                                      is_binary(Key) ->
    parse_inputs(T, [{{Bucket, Key}, KeyData}|Accum]);
parse_inputs([Input|_], _Accum) ->
    invalid_input_error(Input).

is_modfun_input(Inputs) when is_list(Inputs) ->
    HasModule = proplists:get_value(<<"module">>, Inputs) /= undefined,
    HasFunction = proplists:get_value(<<"function">>, Inputs) /= undefined,
    HasArg = proplists:get_value(<<"arg">>, Inputs) /= undefined,
    HasModule andalso HasFunction andalso HasArg;
is_modfun_input(_) -> false.

parse_modfun_input(Inputs) ->
    Module = proplists:get_value(<<"module">>, Inputs),
    Function = proplists:get_value(<<"function">>, Inputs),
    Arg = proplists:get_value(<<"arg">>, Inputs),

    {ok, {modfun,
          binary_to_atom(Module, utf8),
          binary_to_atom(Function, utf8),
          Arg}}.

is_search_input(Inputs) when is_list(Inputs) ->
    HasBucket = proplists:get_value(<<"bucket">>, Inputs) /= undefined,
    HasQuery = proplists:get_value(<<"query">>, Inputs) /= undefined,
    HasBucket andalso HasQuery;
is_search_input(_) -> false.

parse_search_input(Inputs) ->
    Bucket = proplists:get_value(<<"bucket">>, Inputs),
    Query = proplists:get_value(<<"query">>, Inputs),
    Filter = proplists:get_value(<<"filter">>, Inputs, []),
    {ok, {search, Bucket, Query, Filter}}.

%% Allowed forms:
%% {"input":{"bucket":BucketName, "index":IndexName, "key":SecondaryKey},
%%  "map":{...},
%%  "query":{...}
%% }
%%
%% /* Inclusive secondary key range */
%% {"input":{"bucket":BucketName, "index":IndexName, "start":LowSecondaryKey, "end":HighSecondaryKey},
%%  "map":{...},
%%  "query":{...}
%% }
is_index_input(Inputs) when is_list(Inputs)->
    HasBucket   = proplists:get_value(<<"bucket">>, Inputs) /= undefined,
    HasIndex    = proplists:get_value(<<"index">>, Inputs) /= undefined,
    HasKey      = proplists:get_value(<<"key">>, Inputs) /= undefined,
    HasStartKey = proplists:get_value(<<"start">>, Inputs) /= undefined,
    HasEndKey   = proplists:get_value(<<"end">>, Inputs) /= undefined,

    HasBucket andalso
      ((HasIndex andalso HasKey) orelse
       (HasIndex andalso (HasStartKey andalso HasEndKey)));
is_index_input(_) -> false.

parse_index_input(Inputs) ->
    Bucket   = proplists:get_value(<<"bucket">>, Inputs),
    Index    = proplists:get_value(<<"index">>, Inputs),
    Key      = proplists:get_value(<<"key">>, Inputs),
    StartKey = proplists:get_value(<<"start">>, Inputs),
    EndKey   = proplists:get_value(<<"end">>, Inputs),

    if
        Key /= undefined ->
            case riak_index:parse_fields([{Index, Key}]) of
                {ok, [{Index1, Key1}]} ->
                    {ok, {index, Bucket, Index1, Key1}};
                {error, Reasons} ->
                    {error, Reasons}
            end;
        StartKey /= undefined andalso EndKey /= undefined ->
            case riak_index:parse_fields([{Index, StartKey}, {Index, EndKey}]) of
                {ok, [{Index1, Key1}]} ->
                    {ok, {index, Bucket, Index1, Key1}};
                {ok, [{Index1, StartKey1}, {Index1, EndKey1}]} ->
                    {ok, {index, Bucket, Index1, StartKey1, EndKey1}};
                {error, Reasons} ->
                    {error, Reasons}
            end;
        true ->
            invalid_input_error(Inputs)
    end.

is_keyfilter_input(Inputs) when is_list(Inputs) ->
    HasBucket = proplists:get_value(<<"bucket">>, Inputs) /= undefined,
    HasKeyFilters = proplists:get_value(<<"key_filters">>, Inputs) /= undefined,
    HasBucket andalso HasKeyFilters;
is_keyfilter_input(_) -> false.

parse_keyfilter_input(Inputs) ->
    Bucket = proplists:get_value(<<"bucket">>, Inputs),
    KeyFilters = proplists:get_value(<<"key_filters">>, Inputs),

    case length(KeyFilters) >= 1 of
        true ->
            {ok, {Bucket, KeyFilters}};
        false ->
            invalid_input_error(Inputs)
    end.

parse_query(Query) ->
    parse_query(Query, []).

parse_query([], Accum) ->
    {ok, lists:reverse(Accum)};
parse_query([{struct, [{Type, {struct, StepDef}}]}|T], Accum)
  when Type =:= <<"map">>; Type =:= <<"reduce">>; Type =:= <<"link">> ->
    StepType = case Type of
                   <<"map">> -> map;
                   <<"reduce">> -> reduce;
                   <<"link">> -> link
               end,
    Keep = proplists:get_value(<<"keep">>, StepDef, T==[]),
    Step = case not(Keep =:= true orelse Keep =:= false) of
               true ->
                   {error, ["The \"keep\" field was not a boolean value in:\n"
                            "   ",mochijson2:encode(
                                    {struct,[{Type,{struct,StepDef}}]}),
                            "\n"]};
               false ->
                   if StepType == link ->
                          case parse_link_step(StepDef) of
                              {ok, {Bucket, Tag}} ->
                                  {ok, {link, Bucket, Tag, Keep}};
                              LError ->
                                  LError
                          end;
                      true -> % map or reduce
                           Lang = proplists:get_value(<<"language">>, StepDef),
                           case parse_step(Lang, StepDef) of
                               {ok, ParsedStep} ->
                                   Arg = proplists:get_value(<<"arg">>, StepDef, none),
                                   {ok, {StepType, ParsedStep, Arg, Keep}};
                               QError ->
                                   QError
                           end
                   end
           end,
    case Step of
        {ok, S} -> parse_query(T, [S|Accum]);
        SError  -> SError
    end;
parse_query([Phase|_], _Accum) ->
    {error, ["Unrecognized format of query phase:\n"
             "   ",mochijson2:encode(Phase),
             "\n\nValid formats are:\n"
             "   {\"map\":{...spec...}}\n"
             "   {\"reduce\":{...spec...}}\n"
             "   {\"link\:{...spec}}\n"]};
parse_query(Invalid, _Accum) ->
    {error, ["The value of the \"query\" field was not a list:\n"
             "   ",mochijson2:encode(Invalid),"\n"]}.

dejsonify_not_found({struct, [{<<"not_found">>,
                     {struct, [{<<"bucket">>, Bucket},
                               {<<"key">>, Key},
                               {<<"keydata">>, KeyData}]}}]}) ->
    {not_found, {Bucket, Key}, KeyData};
dejsonify_not_found(Data) ->
    Data.

jsonify_not_found({not_found, {Bucket, Key}, KeyData}) ->
    {struct, [{not_found, {struct, [{<<"bucket">>, Bucket},
                                    {<<"key">>, Key},
                                    {<<"keydata">>, KeyData}]}}]};
jsonify_not_found(Data) ->
    Data.

jsonify_bkeys(Results, HasMRQuery) when HasMRQuery == true ->
    Results;
jsonify_bkeys(Results, HasMRQuery) when HasMRQuery == false ->
    jsonify_bkeys_1(Results, []).
jsonify_bkeys_1([{{B, K},D}|Rest], Acc) ->
    jsonify_bkeys_1(Rest, [[B,K,D]|Acc]);
jsonify_bkeys_1([{B, K}|Rest], Acc) ->
    jsonify_bkeys_1(Rest, [[B,K]|Acc]);
jsonify_bkeys_1([], Acc) ->
    lists:reverse(Acc).



parse_link_step(StepDef) ->
    Bucket = proplists:get_value(<<"bucket">>, StepDef, <<"_">>),
    Tag = proplists:get_value(<<"tag">>, StepDef, <<"_">>),
    case not(is_binary(Bucket) andalso is_binary(Tag)) of
        true ->
            {error, ["Invalid link step specification:\n"
                     "   ",mochijson2:encode({struct,StepDef}),
                     "\n\n \"bucket\" and \"tag\" fields"
                     " must have string values.\n"]};
        false ->
            {ok, {if Bucket == <<"_">> -> '_';
                     true              -> Bucket
                  end,
                  if Tag == <<"_">> -> '_';
                     true           -> Tag
                  end}}
    end.

parse_step(<<"javascript">>, StepDef) ->
    Source = proplists:get_value(<<"source">>, StepDef),
    Name = proplists:get_value(<<"name">>, StepDef),
    Bucket = proplists:get_value(<<"bucket">>, StepDef),
    Key = proplists:get_value(<<"key">>, StepDef),
    case Source of
        undefined ->
            case Name of
                undefined ->
                    case Bucket of
                        undefined ->
                            {error, ["No function specified in Javascript phase:\n"
                                     "   ",mochijson2:encode({struct,StepDef}),
                                     "\n\nFunctions may be specified by:\n"
                                     "   - a \"source\" field, with source for"
                                     " a Javascript function\n"
                                     "   - a \"name\" field, naming a predefined"
                                     " Javascript function\n"
                                     "   - \"bucket\" and \"key\" fields,"
                                     " specifying a Riak object containing"
                                     " Javascript function source\n"]};
                        _ ->
                            case Key of
                                undefined ->
                                    {error, ["Javascript phase was missing a"
                                             " \"key\" field to match the \"bucket\""
                                             " field, pointing to the function"
                                             " to evaluate in:"
                                             "   ",mochijson2:encode(
                                                    {struct,StepDef}),
                                             "\n"]};
                                _ ->
                                    {ok, {jsanon, {Bucket, Key}}}
                            end
                    end;
                _ ->
                    {ok, {jsfun, Name}}
            end;
        _ ->
            {ok, {jsanon, Source}}
    end;
parse_step(<<"erlang">>, StepDef) ->
    case extract_fun_type(StepDef) of
        {source, Source} ->
            {ok, {strfun, Source}};
        {bucket_key, {Bucket, Key}} ->
            {ok, {strfun, {Bucket, Key}}};
        {modfun, M, F} ->
            {ok, {modfun, M, F}};
        Else -> Else
    end;
parse_step(undefined, StepDef) ->
    {error, ["No \"language\" was specified for the phase:\n",
             "   ",mochijson2:encode({struct,StepDef}),"\n"]};
parse_step(Language,StepDef) ->
    {error, ["Unknown language ",mochijson2:encode(Language)," in phase:\n",
             "   ",mochijson2:encode({struct,StepDef}),"\n"]}.

bin_to_atom(Binary) when is_binary(Binary) ->
    L = binary_to_list(Binary),
    try
        {ok, list_to_atom(L)}
    catch
        error:badarg -> error
    end.

extract_fun_type(StepDef) ->
    Source = proplists:get_value(<<"source">>, StepDef),
    Bucket = proplists:get_value(<<"bucket">>, StepDef),
    Key = proplists:get_value(<<"key">>, StepDef),
    Module = proplists:get_value(<<"module">>, StepDef),
    Function = proplists:get_value(<<"function">>, StepDef),
    match_fun_type(StepDef, Source, {Bucket, Key}, {Module, Function}).

match_fun_type(_StepDef, Source, {undefined, undefined}, {undefined, undefined})
  when is_binary(Source) orelse is_list(Source) ->
    {source, Source};
match_fun_type(_StepDef, undefined, {Bucket, Key}, {undefined, undefined})
  when is_binary(Bucket) andalso is_binary(Key) ->
    {bucket_key, {Bucket, Key}};
match_fun_type(StepDef, undefined, {undefined, undefined}, {Module, Function})
  when is_binary(Module) andalso is_binary(Function) ->
    case {bin_to_atom(Module), bin_to_atom(Function)} of
        {{ok, M}, {ok, F}} ->
            {modfun, M, F};
        {error, _} ->
            modfun_error("module", StepDef);
        {_, error} ->
            modfun_error("function", StepDef)
    end;
match_fun_type(StepDef, _, _, _) ->
    erl_phase_error(StepDef).

modfun_error(What, StepDef) ->
    {error, ["Could not convert \"" ++ What ++ "\" field value"
             " to an atom in:"
             "   ",mochijson2:encode({struct, StepDef}),
             "\n"]}.

erl_phase_error(StepDef) ->
    {error, ["No function specified in Erlang phase:\n"
             "   ", mochijson2:encode({struct, StepDef}),
             "\n\nFunctions may be specified by:\n"
             "   - a \"source\" field, with source for"
             " an Erlang function\n"
             "   - \"module\" and \"function\" fields,"
             " specifying an Erlang module and function\n"
             "   - \"bucket\" and \"key\" fields,"
             " specifying a Riak object containing"
             " Erlang function source\n"]}.

%% @doc Produce a list of mochjson2 props from a pipe error log
jsonify_pipe_error(From, {Error, Input}) ->
    %% map function returned error tuple
    [{phase, pipe_phase_index(From)},
     {error, trunc_print(Error)},
     {input, trunc_print(Input)}];
jsonify_pipe_error(_From, Elist) when is_list(Elist) ->
    %% generic pipe fitting error

    %% phase pulled from Elist should be the same as From,
    %% but just to dig into that info more, we use Elist here
    [{phase, pipe_error_phase(Elist)},
     {error, pipe_error_error(Elist)},
     {input, pipe_error_input(Elist)},
     {type, pipe_error_type(Elist)},
     {stack, pipe_error_stack(Elist)}];
jsonify_pipe_error(From, Other) ->
    %% some other error
    [{phase, pipe_phase_index(From)},
     {error, trunc_print(Other)}].

%% @doc Turn the pipe fitting name into a MapReduce phase index.
-spec pipe_phase_index(integer()|{term(),integer()}) -> integer().
pipe_phase_index({_Type, I}) -> I;
pipe_phase_index(I)          -> I.

%% @doc convenience for formatting ~500chars of a term as a
%% human-readable string
-spec trunc_print(term()) -> binary().
trunc_print(Term) ->
    {Msg, _Len} = lager_trunc_io:print(Term, 500),
    iolist_to_binary(Msg).

%% @doc Pull a field out of a proplist, and possibly transform it.
pipe_error_field(Field, Proplist) ->
    pipe_error_field(Field, Proplist, fun(X) -> X end).
pipe_error_field(Field, Proplist, TrueFun) ->
    pipe_error_field(Field, Proplist, TrueFun, null).
pipe_error_field(Field, Proplist, TrueFun, FalseVal) ->
    case lists:keyfind(Field, 1, Proplist) of
        {Field, Value} -> TrueFun(Value);
        false          -> FalseVal
    end.

%% @doc Pull a field out of a proplist, and format it as a reasonably
%% short binary if available.
pipe_error_trunc_print(Field, Elist) ->
    pipe_error_field(Field, Elist, fun trunc_print/1).

%% @doc Determine the phase that this error came from.
pipe_error_phase(Elist) ->
    Details = pipe_error_field(details, Elist, fun(X) -> X end, []),
    pipe_error_field(name, Details, fun pipe_phase_index/1).

pipe_error_type(Elist) ->
    %% is this really useful?
    pipe_error_field(type, Elist).

pipe_error_error(Elist) ->
    pipe_error_field(error, Elist, fun trunc_print/1,
                     pipe_error_trunc_print(reason, Elist)).

pipe_error_input(Elist) ->
    %% translate common inputs?
    %% e.g. strip 'ok' tuple from map input
    pipe_error_trunc_print(input, Elist).

pipe_error_stack(Elist) ->
    %% truncate stacks to "important" part?
    pipe_error_trunc_print(stack, Elist).

-ifdef(TEST).

bucket_input_test() ->
    %% Test parsing a bucket input.
    JSON = <<"\"mybucket\"">>,
    Expected = {ok, <<"mybucket">>},
    ?assertEqual(Expected, parse_inputs(mochijson2:decode(JSON))).

key_input_test() ->
    %% Test parsing bkey inputs without keydata.
    JSON1 = <<"[[\"b1\", \"k1\"], [\"b2\", \"k2\"], [\"b3\", \"k3\"]]">>,
    Expected1 = {ok, [
                      {<<"b1">>, <<"k1">>},
                      {<<"b2">>, <<"k2">>},
                      {<<"b3">>, <<"k3">>}
                     ]},
    ?assertEqual(Expected1, parse_inputs(mochijson2:decode(JSON1))),

    %% Test parsing bkey inputs with keydata.
    JSON2 = <<"[[\"b1\", \"k1\", \"v1\"], [\"b2\", \"k2\", \"v2\"], [\"b3\", \"k3\", \"v3\"]]">>,
    Expected2 = {ok, [
                      {{<<"b1">>, <<"k1">>}, <<"v1">>},
                      {{<<"b2">>, <<"k2">>}, <<"v2">>},
                      {{<<"b3">>, <<"k3">>}, <<"v3">>}
                     ]},
    ?assertEqual(Expected2, parse_inputs(mochijson2:decode(JSON2))).

modfun_input_test() ->
    JSON = <<"{\"module\":\"mymod\",\"function\":\"myfun\",\"arg\":[1,2,3]}">>,
    Expected = {ok, {modfun, mymod, myfun, [1,2,3]}},
    ?assertEqual(Expected, parse_inputs(mochijson2:decode(JSON))).

search_input_test() ->
    JSON1 = <<"{\"bucket\":\"mybucket\",\"query\":\"foo bar\"}">>,
    Expected1 = {ok, {search, <<"mybucket">>, <<"foo bar">>, []}},
    ?assertEqual(Expected1, parse_inputs(mochijson2:decode(JSON1))),

    JSON2 = <<"{\"bucket\":\"mybucket\",\"query\":\"foo bar\", \"filter\":\"baz\"}">>,
    Expected2 = {ok, {search, <<"mybucket">>, <<"foo bar">>, <<"baz">>}},
    ?assertEqual(Expected2, parse_inputs(mochijson2:decode(JSON2))).

index_input_test() ->
    JSON1 = <<"{\"bucket\":\"mybucket\",\"index\":\"myindex_bin\", \"key\":\"mykey\"}">>,
    Expected1 = {ok, {index, <<"mybucket">>, <<"myindex_bin">>, <<"mykey">>}},
    ?assertEqual(Expected1, parse_inputs(mochijson2:decode(JSON1))),

    JSON2 = <<"{\"bucket\":\"mybucket\",\"index\":\"myindex_bin\", \"start\":\"vala\", \"end\":\"valb\"}">>,
    Expected2 = {ok, {index, <<"mybucket">>, <<"myindex_bin">>, <<"vala">>, <<"valb">>}},
    ?assertEqual(Expected2, parse_inputs(mochijson2:decode(JSON2))).

keyfilter_input_test() ->
    JSON = <<"{\"bucket\":\"mybucket\",\"key_filters\":[[\"match\", \"key.*\"]]}">>,
    Expected = {ok, {<<"mybucket">>, [[<<"match">>, <<"key.*">>]]}},
    ?assertEqual(Expected, parse_inputs(mochijson2:decode(JSON))).

-define(DISCRETE_ERLANG_JOB, <<"{\"inputs\": [[\"foo\", \"bar\"]], \"query\":[{\"map\":{\"language\":\"erlang\","
                              "\"module\":\"foo\", \"function\":\"bar\", \"keep\": false}}]}">>).
-define(DISCRETE_JS_JOB, <<"{\"inputs\": [[\"foo\", \"bar\"]], \"query\":[{\"map\":{\"language\":\"javascript\","
                          "\"name\":\"Riak.foo\", \"keep\": false}}]}">>).

-define(BUCKET_ERLANG_JOB, <<"{\"inputs\": \"foo\", \"query\":[{\"map\":{\"language\":\"erlang\","
                              "\"module\":\"foo\", \"function\":\"bar\", \"keep\": false}}]}">>).
-define(BUCKET_JS_JOB, <<"{\"inputs\": \"foo\", \"query\":[{\"map\":{\"language\":\"javascript\","
                              "\"source\":\"function(obj) { return obj; }\", \"keep\": false}}]}">>).

-define(KEY_SELECTION_JOB, <<"{\"inputs\": {\"bucket\":\"users\","
                            "\"key_filters\": [[\"matches\", \"ksmith.*\"]]},"
                            "\"query\":[{\"map\":{\"language\":\"javascript\","
                            "\"source\":\"function(obj) { return obj; }\", \"keep\": false}}]}">>).

-define(ERLANG_STRFUN_JOB, <<"{\"inputs\": [[\"foo\", \"bar\"]], \"query\":[{\"map\":{\"language\":\"erlang\","
                               "\"source\":\"fun(_, _, _) -> [] end.\"}}]}">>).

discrete_test() ->
    {ok, _Inputs, _Query, _Timeout} = riak_kv_mapred_json:parse_request(?DISCRETE_ERLANG_JOB),
    {ok, _Inputs1, _Query1, _Timeout1} = riak_kv_mapred_json:parse_request(?DISCRETE_JS_JOB).

bucket_test() ->
    {ok, _Inputs, _Query, _Timeout} = riak_kv_mapred_json:parse_request(?BUCKET_ERLANG_JOB),
    {ok, _Inputs1, _Query1, _Timeout1} = riak_kv_mapred_json:parse_request(?BUCKET_JS_JOB).

key_select_test() ->
    {ok, _Inputs, _Query, _Timeout} = riak_kv_mapred_json:parse_request(?KEY_SELECTION_JOB).

strfun_test() ->
    {ok, _Inputs, _Query, _Timeout} = riak_kv_mapred_json:parse_request(?ERLANG_STRFUN_JOB).

-endif.
