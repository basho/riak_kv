%% -------------------------------------------------------------------
%%
%% riak_kv_mapred_planner: Plans batched mapreduce processing
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

-module(riak_kv_mapred_planner).
-author('John Muellerleile <johnm@basho.com>').
-author('Kevin Smith <kevin@basho.com>').

-include("riak_kv_map_phase.hrl").

-export([plan_map/1]).

plan_map(Inputs) ->
    build_claim_list(add_map_inputs(Inputs)).

%% Internal functions
build_claim_list(InputData) ->
    {keys, Keys} = lists:keyfind(keys, 1, InputData),
    InputData1 = lists:keydelete(keys, 1, InputData),
    F = fun({_, KeysA} , {_, KeysB}) -> length(KeysA) =< length(KeysB) end,
    PartList0 = lists:sort(F, InputData1),
    claim_keys(PartList0, [], Keys).

claim_keys([], [], _) ->
    exit(exhausted_preflist);
claim_keys(_, ClaimList, []) ->
    ClaimList;
claim_keys([H|T], ClaimList, Keys) ->
    {P, PKeys} = H,
    PKeys1 = lists:filter(fun(PK) ->
        lists:member(PK, Keys)
    end, PKeys),
    case PKeys1 == [] of
        true ->
            claim_keys(T, ClaimList, Keys);
        false ->
            NewKeys = lists:subtract(Keys, PKeys1),
            claim_keys(T, ClaimList ++ [{P, PKeys1}], NewKeys)
    end.

add_map_inputs(Inputs) ->
    add_map_inputs(Inputs, [{keys, []}]).
add_map_inputs([], InputData) ->
    InputData;
add_map_inputs([#riak_kv_map_input{preflist=PList}=H|T], InputData) ->
    {keys, Keys} = lists:keyfind(keys, 1, InputData),
    InputData1 = lists:foldl(fun(P, Acc) ->
        case lists:keyfind(P, 1, Acc) of
            false ->
                lists:keystore(P, 1, Acc, {P, [H]});
            {P, PKeys} ->
                lists:keystore(P, 1, Acc, {P, PKeys ++ [H]})
        end
    end, InputData, PList),
    add_map_inputs(T, lists:keystore(keys, 1, InputData1, {keys, Keys ++ [H]})).
