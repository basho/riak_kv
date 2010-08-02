%% -------------------------------------------------------------------
%%
%% riak_mapred_query: driver for mapreduce query
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

%% @doc riak_kv_mapred_query is the driver of a mapreduce query.
%%
%%      Map phases are expected to have inputs of the form
%%      [{Bucket,Key}] or [{{Bucket,Key},KeyData}] (the first form is
%%      equivalent to [{{Bucket,Key},undefined}]) and will execute
%%      with locality to each key and must return a list that is valid
%%      input to the next phase
%%
%%      Reduce phases take any list, but the function must be
%%      commutative and associative, and the next phase will block
%%      until the reduce phase is entirely done, and the reduce fun
%%      must return a list that is valid input to the next phase
%%
%%      Valid terms for Query:
%%<ul>
%%<li>  {link, Bucket, Tag, Acc}</li>
%%<li>  {map, FunTerm, Arg, Acc}</li>
%%<li>  {reduce, FunTerm, Arg, Acc}</li>
%%</ul>
%%      where FunTerm is one of:
%% <ul>
%%<li>  {modfun, Mod, Fun} : Mod and Fun both atoms ->
%%         Mod:Fun(Object,KeyData,Arg)</li>
%%<li>  {qfun, Fun} : Fun is an actual fun ->
%%         Fun(Object,KeyData,Arg)</li>
%%</ul>
%% @type mapred_queryterm() =
%%         {map, mapred_funterm(), Arg :: term(),
%%          Accumulate :: boolean()} |
%%         {reduce, mapred_funterm(), Arg :: term(),
%%          Accumulate :: boolean()} |
%%         {link, Bucket :: riak_object:bucket(), Tag :: term(),
%%          Accumulate :: boolean()}
%% @type mapred_funterm() =
%%         {modfun, Module :: atom(), Function :: atom()}|
%%         {qfun, function()}
%% @type mapred_result() = [term()]

-module(riak_kv_mapred_query).

-export([start/6]).

start(Node, Client, ReqId, Query0, ResultTransformer, Timeout) ->
    case check_query_syntax(Query0) of
        {ok, Query} ->
            luke:new_flow(Node, Client, ReqId, Query, ResultTransformer, Timeout);
        {bad_qterm, QTerm} ->
            {stop, {bad_qterm, QTerm}}
    end.

check_query_syntax(Query) ->
    check_query_syntax(lists:reverse(Query), []).

check_query_syntax([], Accum) ->
    {ok, Accum};
check_query_syntax([QTerm|Rest], Accum) ->
    io:format("QTerm: ~p~n", [QTerm]),
    {QTermType, QueryFun, Misc, Rereduce, Acc} = parse_qterm(QTerm),
    PhaseDef = case QTermType of
                   link ->
                       {phase_mod(link), phase_behavior(link, QueryFun, Rereduce, Acc), [{erlang, QTerm}]};
                   T when T =:= map orelse T=:= reduce ->
                       case QueryFun of
                           {modfun, Mod, Fun} when is_atom(Mod),
                                                   is_atom(Fun) ->
                               {phase_mod(T), phase_behavior(T, QueryFun, Rereduce, Acc), [{erlang, QTerm}]};
                           {qfun, Fun} when is_function(Fun) ->
                               {phase_mod(T), phase_behavior(T, QueryFun, Rereduce, Acc), [{erlang, QTerm}]};
                           {jsanon, JS} when is_binary(JS) ->
                               {phase_mod(T), phase_behavior(T, QueryFun, Rereduce, Acc), [{javascript, QTerm}]};
                           {jsanon, {Bucket, Key}} when is_binary(Bucket),
                                                        is_binary(Key) ->
                               case fetch_js(Bucket, Key) of
                                   {ok, JS} ->
                                       {phase_mod(T), phase_behavior(T, QueryFun, Rereduce, Acc),
                                        [{javascript, {T, {jsanon, JS}, Misc, Rereduce, Acc}}]};
                                   _ ->
                                       {bad_qterm, QTerm}
                               end;
                           {jsfun, JS} when is_binary(JS) ->
                               {phase_mod(T), phase_behavior(T, QueryFun, Rereduce, Acc), [{javascript, QTerm}]};
                           _ ->
                               {bad_qterm, QTerm}
                       end
               end,
    case PhaseDef of
        {bad_qterm, _} ->
            PhaseDef;
        _ ->
            check_query_syntax(Rest, [PhaseDef|Accum])
    end.

parse_qterm({Type, QueryFun, Misc, Rereduce, Accum}) ->
    {Type, QueryFun, Misc, Rereduce, Accum};
parse_qterm({Type, QueryFun, Misc, Accum}) ->
    {Type, QueryFun, Misc, false, Accum}.

phase_mod(link) ->
    riak_kv_map_phase;
phase_mod(map) ->
    riak_kv_map_phase;
phase_mod(reduce) ->
    riak_kv_reduce_phase.

phase_behavior(link, _QueryFun, _, true) ->
    [accumulate];
phase_behavior(link, _QueryFun, _, false) ->
    [];
phase_behavior(map, _QueryFun, _, true) ->
    [accumulate];
phase_behavior(map, _QueryFun, _, false) ->
    [];
phase_behavior(reduce, _QueryFun, Rereduce, Accumulate) ->
    Props = build_props(Rereduce, Accumulate),
    [{converge, 2}|Props].

build_props(Rereduce, Accumulate) ->
    Props1 = [{rereduce, Rereduce}],
    case Accumulate of
        true ->
            [accumulate|Props1];
        false ->
            Props1
    end.

fetch_js(Bucket, Key) ->
    {ok, Client} = riak:local_client(),
    case Client:get(Bucket, Key, 1) of
        {ok, Obj} ->
            {ok, riak_object:get_value(Obj)};
        _ ->
            {error, bad_fetch}
    end.
