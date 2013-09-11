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
%%
%% == Callbacks ==
%%
%% A mutator callback must implement 2 function: mutate_put/5 and mutate_get/1.
%%
%% <code><b>mutate_put(MetaData, Value, ExposedMeta,
%% FullObject, BucketProperties) -> Result</b></code>
%%
%% Types:
%% ```
%%     MetaData = dict()
%%     Value = term()
%%     ExposedMeta = dict()
%%     FullObject = riak_object:riak_object()
%%     BucketProperties = orddict:orddict()
%%     Result = {NewMeta, NewValue, NewExposedMeta}
%%         NewMeta = dict()
%%         NewValue = term()
%%         NewExposedMeta = dict()'''
%%
%% The mutate_put callback is called for each metadata/value pair a riak_object
%% has. The return value of NewMeta and NewValue are used by the storage backend
%% while the NewExposedMeta is used for the client return where NewMeta would
%% normally. The NewExposedMeta is merged with the NewMeta to generate the
%% exposed metadata; if the same key is found, the NewExposedMeta value is used.
%%
%% The mutations are run in the same process as the vnode.
%%
%% <code><b>mutate_get(Object) -> Result</b></code>
%%
%% Types:
%% ```
%%     Object = riak_object:riak_object()
%%     Result = riak_object:riak_object() | 'notfound'
%% '''
%% Take the object from storage and reverse whatever mutation was applied. Note
%% the bucket properties are not part of this callback, so if some data is
%% important to reverse a mutation, it must be put in the metadata by the
%% `mutate_put' function. Also note how the entire object is given as opposed to
%% simply a metadata/value pair. Care must be taken not to corrupt the object.
%%
%% A return of ``'notfound''' stops the mutator chain and returns immediately. This
%% provides an escape hatch of sorts; if the mutator cannot reverse the mutation
%% effectively, return ``'notfound'''.

-module(riak_kv_mutator).

-include_lib("eunit/include/eunit.hrl").

-export([register/1, register/2, unregister/1]).
-export([get/0]).
-export([mutate_put/2, mutate_get/1]).

-callback mutate_put(Meta :: dict(), Value :: any(), ExposedMeta :: dict(), FullObject :: riak_object:riak_object(), BucketProps :: orddict:orddict()) -> {dict(), any(), dict()}.
-callback mutate_get(FullObject :: riak_object:riak_object()) -> riak_object:riak_object() | 'notfound'.

-define(DEFAULT_PRIORITY, 0).

%% @doc Register the given module as a mutator with the default priority of 0.
%% @see register/2
-spec register(Module :: atom()) -> 'ok'.
register(Module) ->
    ?MODULE:register(Module, ?DEFAULT_PRIORITY).

%% @doc Register a module as a mutator with the given priority. Modules with
%% equal priority are done in sort sort (alphabetical) order. A module
%% can only be registered once.
-spec register(Module :: atom(), Priority :: term()) -> 'ok'.
register(Module, Priority) ->
    Modifier = fun
        (undefined) ->
            [{Module, Priority}];
        (Values) ->
            Values2 = merge_values(Values),
            orddict:store(Module, Priority, Values2)
    end,
    riak_core_metadata:put({riak_kv, mutators}, list, Modifier).

%% @doc Remove a module from the mutator list.
-spec unregister(Module :: atom()) -> 'ok'.
unregister(Module) ->
    Modifier = fun
        (undefined) ->
            [];
        (Values) ->
            Values2 = merge_values(Values),
            orddict:erase(Module, Values2)
    end,
    riak_core_metadata:put({riak_kv, mutators}, list, Modifier, []).

%% @doc Retrieve the list of mutators in the order to apply them when doing a
%% a put mutation. To get the order when doing a get mutation, reverse the list.
-spec get() -> [atom()].
get() ->
    Resolver = fun
        ('$deleted', '$delete') ->
            [];
        ('$deleted', Values) ->
            Values;
        (Values, '$deleted') ->
            Values;
        (Values1, Values2) ->
            merge_values([Values1, Values2])
    end,
    ModulesAndPriors = riak_core_metadata:get({riak_kv, mutators}, list, [{default, []}, {resolver, Resolver}]),
    Flipped = [{P, M} || {M, P} <- ModulesAndPriors],
    Sorted = lists:sort(Flipped),
    Modules = [M || {_P, M} <- Sorted],
    {ok, Modules}.

%% @doc Unmutate an object after retrieval from storage. When an object is
%% mutated, the mutators applied are put into the object's metadata.
-spec mutate_get(Object :: riak_object:riak_object()) -> riak_object:riak_object().
mutate_get(Object) ->
    [Meta | _] = riak_object:get_metadatas(Object),
    case dict:find(mutators_applied, Meta) of
        error ->
            Object;
        {ok, Applied} ->
            DeMutateOrder = lists:reverse(Applied),
            mutate_get(Object, DeMutateOrder)
    end.

mutate_get(Object, []) ->
    Object;
mutate_get(Object, [Mutator | Tail]) ->
    case Mutator:mutate_get(Object) of
        notfound ->
            notfound;
        Object2 ->
            mutate_get(Object2, Tail)
    end.

%% @doc Mutate an object in preparation to storage, returning a tuple of the
%% object to store and the object to return to the client.
-spec mutate_put(Object :: riak_object:riak_object(), BucketProps :: orddict:orddict()) -> {riak_object:riak_object(), riak_object:riak_object()}.
mutate_put(Object, BucketProps) ->
    Contents = riak_object:get_contents(Object),
    {ok, Modules} = ?MODULE:get(),
    MetasValuesRevealeds = lists:map(fun({InMeta, InValue}) ->
        {InMeta1, InValue1, InRevealed} = lists:foldl(fun(Module, {InInMeta, InInValue, InInRevealed}) ->
            Module:mutate_put(InInMeta, InInValue, InInRevealed, Object, BucketProps)
        end, {InMeta, InValue, dict:new()}, Modules),
        InMeta2 = dict:store(mutators_applied, Modules, InMeta1),
        {InMeta2, InValue1, InRevealed}
    end, Contents),
    Contents2 = [{M,V} || {M,V,_R} <- MetasValuesRevealeds],
    Mutated = riak_object:set_contents(Object, Contents2),
    FakedContents = lists:map(fun({InMeta, InContent, InRevealed}) ->
        FixedMeta = dict:merge(fun(_Key, _NotMutated, MutatedVal) ->
            MutatedVal
        end, InMeta, InRevealed),
        {FixedMeta, InContent}
    end, MetasValuesRevealeds),
    Faked = riak_object:set_contents(Object, FakedContents),
    {Mutated, Faked}.

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
