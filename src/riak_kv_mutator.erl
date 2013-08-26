%% -------------------------------------------------------------------
%%
%% riak_kv_mutators - Storage and retrieval for get/put mutation
%% functions
%%
%% Copyright (c) 2013 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc There are circumstances where the object stored on disk is not
%% the object to return; and there are times the object written to the
%% data storage backend is not meant to be the object given. An
%% example would be storing only meta data for an object on a remote
%% cluster. This module is an interface to register mutators that will
%% can be run.
%%
%% This doubles as a behavior defining module for the mutators.

-module(riak_kv_mutator).

-export([register/1, register/2, unregister/1]).
-export([get/0]).
-export([mutate_put/2, mutate_get/1]).

-define(DEFAULT_PRIORITY, 0).

register(Module) ->
    ?MODULE:register(Module, ?DEFAULT_PRIORITY).

register(Module, Priority) ->
    Modifier = fun
        (undefined) ->
            [{Module, Priority}];
        (Values) ->
            Values2 = merge_values(Values),
            orddict:store(Module, Priority, Values2)
    end,
    riak_core_metadata:put({riak_kv, mutators}, list, Modifier).

unregister(Module) ->
    Modifier = fun
        (undefined) ->
            [];
        (Values) ->
            Values2 = merge_values(Values),
            orddict:erase(Module, Values2)
    end,
    riak_core_metadata:put({riak_kv, mutators}, list, Modifier, []).

get() ->
    Resolver = fun(Values) ->
        Values2 = lists:filter(fun erlang:is_list/1, Values),
        merge_values(Values2)
    end,
    ModulesAndPriors = riak_core_metadata:get({riak_kv, mutators}, list, [{default, []}, {resolver, Resolver}]),
    Flipped = [{P, M} || {M, P} <- ModulesAndPriors],
    Sorted = lists:sort(Flipped),
    Modules = [M || {_P, M} <- Sorted],
    {ok, Modules}.

mutate_get(Object) ->
    Meta = riak_object:get_metadata(Object),
    {AppliedMutators, Meta2} = case dict:find(mutators_applied, Meta) of
        error ->
            {[], Meta};
        {ok, Applied} ->
            {Applied, dict:erase(mutators_applied, Meta)}
    end,
    Object2 = riak_object:update_metadata(Object, Meta2),
    Object3 = riak_object:apply_updates(Object2),
    FoldFun = fun(Module, Obj) ->
        Module:mutate_get(Obj)
    end,
    lists:foldl(FoldFun, Object3, lists:reverse(AppliedMutators)).

mutate_put(Object, BucketProps) ->
    FoldFun = fun(Module, Obj) ->
        Module:mutate_put(Obj, BucketProps)
    end,
    {ok, Modules} = ?MODULE:get(),
    Object2 = lists:foldl(FoldFun, Object, Modules),
    Meta = riak_object:get_metadata(Object2),
    Meta2 = dict:store(mutators_applied, Modules, Meta),
    Object3 = riak_object:update_metadata(Object2, Meta2),
    riak_object:apply_updates(Object3).

merge_values([]) ->
    [];

merge_values(Values) ->
    case lists:filter(fun erlang:is_list/1, Values) of
        [] ->
            [];
        [Head | Tail] ->
            merge_values(Tail, Head)
    end.

merge_values([], Acc) ->
    Acc;

merge_values([Head | Tail], Acc) ->
    Acc2 = orddict:merge(fun merge_fun/3, Acc, Head),
    merge_values(Tail, Acc2).

merge_fun(_Key, P1, P2) when P1 < P2 ->
    P1;
merge_fun(_Key, _P1, P2) ->
    P2.
