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
                                {error, Message} ->
                                    {error, {'query', Message}}
                            end;
                        {error, Message} ->
                            {error, {inputs, Message}}
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
    case is_modfun(Inputs) of
        true ->
            parse_modfun(Inputs);
        false ->
            parse_key_filters(Inputs)
    end;
parse_inputs(Invalid) ->
    {error, ["Unrecognized format of \"inputs\" field:",
             "   ",mochijson2:encode(Invalid),
             "\n\nValid formats are:\n"
             "   - a bucket name, as a string\n"
             "   - a list of bucket/key pairs\n",
             "   - an object naming a module and function to run, ex:\n",
             "     {\"module\":\"my_module\",\n",
             "      \"function\":\"my_function\",\n",
             "      \"arg\":[\"my\",\"arguments\"]}\n"]}.

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
    {error, ["Unrecognized format of input element:\n"
             "   ",mochijson2:encode(Input),
             "\n\nValid formats are:\n"
             "   [Bucket, Key]\n"
             "   [Bucket, Key, KeyData]\n"
             "where Bucket and Key are strings\n"]}.

parse_modfun(ModFun) ->
    case {proplists:lookup(<<"module">>, ModFun),
          proplists:lookup(<<"function">>, ModFun),
          proplists:lookup(<<"arg">>, ModFun)} of
        {{_, Module}, {_, Function}, {_, Options}} ->
            {ok, {modfun,
                  binary_to_atom(Module, utf8),
                  binary_to_atom(Function, utf8),
                  Options}};
        _ ->
            {error, ["Missing fields in modfun input specification.\n"
                     "Required fields are:\n"
                     "   - module : string name of a module\n",
                     "   - function : string name of a function in module\n",
                     "   - arg : argument to pass function\n"]}
    end.

parse_key_filters(Filter) when is_list(Filter) ->
    case proplists:get_value(<<"bucket">>, Filter) of
        undefined ->
            {error, ["Key filter expression missing target bucket.\n"]};
        Bucket ->
            case proplists:get_value(<<"key_filters">>, Filter) of
                undefined ->
                    {error, ["Key filter expression missing filter list.\n"]};
                Pred ->
                    case length(Pred) >= 1 of
                        true ->
                            {ok, {Bucket, Pred}};
                        false ->
                            {error, ["Illegal key filter statement.\n"]}
                    end
            end
    end;
parse_key_filters(_Filter) ->
    {error, ["Illegal key filter statement.\n"]}.

parse_query(Query) ->
    parse_query(Query, []).

parse_query([], Accum) ->
    if
        length(Accum) > 0 ->
            {ok, lists:reverse(Accum)};
        true ->
            {error, "No query phases were given\n"}
    end;
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

bin_to_atom(undefined) ->
    error;
bin_to_atom(Binary) when is_binary(Binary) ->
    L = binary_to_list(Binary),
    try
        {ok, list_to_atom(L)}
    catch
        error:badarg -> error
    end.

is_modfun(Inputs) ->
    case proplists:get_value(<<"module">>, Inputs) of
        undefined ->
            false;
        _ ->
            true
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

-ifdef(TEST).
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


discrete_test() ->
    {ok, _Inputs, _Query, _Timeout} = riak_kv_mapred_json:parse_request(?DISCRETE_ERLANG_JOB),
    {ok, _Inputs1, _Query1, _Timeout1} = riak_kv_mapred_json:parse_request(?DISCRETE_JS_JOB).

bucket_test() ->
    {ok, _Inputs, _Query, _Timeout} = riak_kv_mapred_json:parse_request(?BUCKET_ERLANG_JOB),
    {ok, _Inputs1, _Query1, _Timeout1} = riak_kv_mapred_json:parse_request(?BUCKET_JS_JOB).

key_select_test() ->
    {ok, _Inputs, _Query, _Timeout} = riak_kv_mapred_json:parse_request(?KEY_SELECTION_JOB).

-endif.
