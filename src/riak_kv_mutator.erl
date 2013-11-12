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
%% be run.
%%
%% This doubles as a behavior defining module for the mutators.
%%
%% == Words of Warning ==
%%
%% Once an object has been stored with a given mutator list, attempting to
%% retreive that object will require those mutators. If the mutators done't
%% exists or have changed in a non-backwards compatible way, you can expect
%% to have at worst a crash, or at best a corrupted object.
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

-export([register/1, register/2, unregister/1]).
-export([get/0]).
-export([mutate_put/3, mutate_put/2, mutate_get/1]).

-callback mutate_put(Meta :: dict(), Value :: any(), ExposedMeta :: dict(), FullObject :: riak_object:riak_object(), BucketProps :: orddict:orddict()) -> {dict(), any(), dict()}.
-callback mutate_get(FullObject :: riak_object:riak_object()) -> riak_object:riak_object() | 'notfound'.

-define(DEFAULT_PRIORITY, 0).

%% @doc Register the given module as a mutator with the default priority of 0.
%% @see register/2
-spec register(Module :: atom()) -> 'ok'.
register(Module) ->
    ?MODULE:register(Module, ?DEFAULT_PRIORITY).

%% @doc Register a module as a mutator with the given priority. Modules with
%% equal priority are done in default (alphabetical) order. A module
%% can only be registered once. When there is a conflict (two different
%% lists), those lists are merged. Note that if an object is stored with
%% a mutator, that mutator is used on retrieval. If that mutator code is
%% removed or changed in a backwards incompatible manner, at best the
%% object will be corrupted; at worst it will cause a crash.
-spec register(Module :: atom(), Priority :: term()) -> 'ok'.
register(Module, Priority) ->
    Modifier = fun
        (undefined) ->
            [{Priority, Module}];
        (Values) ->
            Values2 = merge_values(Values),
            insert_mutator(Module, Priority, Values2)
    end,
    riak_core_metadata:put({riak_kv, mutators}, list, Modifier).

%% @doc Remove a module from the mutator list. Removing a mutator from the
%% list does not remove the mutator from use for retreiving objects. Any
%% object that was stored while the mutator was registered will use that
%% mutator on get. Thus, if the code for a mutator is not available or
%% was changed in a non-backwards compatible way, at best one can expect
%% corrupt objects, at worst a crash.
-spec unregister(Module :: atom()) -> 'ok'.
unregister(Module) ->
    Modifier = fun
        (undefined) ->
            [];
        (Values) ->
            Values2 = merge_values(Values),
            lists:keydelete(Module, 2, Values2)
    end,
    riak_core_metadata:put({riak_kv, mutators}, list, Modifier, []).

%% @doc Retrieve the list of mutators in the order to apply them when doing a
%% a put mutation. To get the order when doing a get mutation, reverse the list.
-spec get() -> {ok, [atom()]}.
get() ->
    Resolver = fun
        ('$deleted', '$deleted') ->
            [];
        ('$deleted', Values) ->
            Values;
        (Values, '$deleted') ->
            Values;
        (Values, Values) ->
            Values;
        (Values1, Values2) ->
            merge_values([Values1, Values2])
    end,
    ModulesAndPriors = riak_core_metadata:get({riak_kv, mutators}, list, [{default, []}, {resolver, Resolver}]),
    Modules = [M || {_P, M} <- ModulesAndPriors],
    {ok, Modules}.

%% @doc Unmutate an object after retrieval from storage. When an object is
%% mutated, the mutators applied are put into the object's metadata.
%% If the mutator does not exist anymore or has changed in a backwards
%% incompatible manner, at best there will be corrupt objects, at worst
%% a crash.
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
    % so event though the mutate_put callback has to return
    % {Meta, Value, Exposed} values, the get callback gets away with just
    % giving the object? This is to avoid complicated interaction with
    % notfound return.
    case Mutator:mutate_get(Object) of
        notfound ->
            notfound;
        Object2 ->
            mutate_get(Object2, Tail)
    end.

-spec mutate_put(Object :: riak_object:riak_object(), BucketProps :: orddict:orddict()) -> {riak_object:riak_object(), riak_object:riak_object()}.
mutate_put(Object, BucketProps) ->
    {ok, Modules} = ?MODULE:get(),
    mutate_put(Object, BucketProps, Modules).

%% @doc Mutate an object in preparation to storage, returning a tuple of the
%% object to store and the object to return to the client. For each sibling
%% the object has {Meta, Value} pair, each mutator is called with a copy
%% that iteration's Meta used as the exposed meta." Later mutators are
%% given the results of previous mutators. Once all mutations are complete,
%% two {@link riak_object:riak_object()}s are returned. The first is what
%% is to be stored, while the second has the exposed meta set with the
%% orginal value(s).
-spec mutate_put(Object :: riak_object:riak_object(), BucketProps :: orddict:orddict(), Modules :: [atom()]) -> {riak_object:riak_object(), riak_object:riak_object()}.
mutate_put(Object, BucketProps, Modules) ->
    Contents = riak_object:get_contents(Object),
    MetasValuesRevealeds = lists:map(fun({InMeta, InValue}) ->
        {InMeta1, InValue1, InRevealed} = lists:foldl(fun(Module, {InInMeta, InInValue, InInRevealed}) ->
            % why not just give the riak_object? because of a warning
            % in riak_object stating that set_contents is for internal
            % use only. Hopefully this qualifies.
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

merge_values([{Priority, Module} = Mutator | Tail], Acc) ->
    Acc2 = case lists:keyfind(Module, 2, Acc) of
        false ->
            ordsets:add_element(Mutator, Acc);
        {P1, _} when P1 < Priority ->
            Acc;
        Else ->
            ordsets:add_element(Mutator, ordsets:del_element(Else, Acc))
    end,
    merge_values(Tail, Acc2);

merge_values([MutatorList | Tail], Acc) ->
    Acc2 = merge_values(MutatorList, Acc),
    merge_values(Tail, Acc2).

insert_mutator(Module, Priority, Mutators) ->
    Mutators2 = lists:keydelete(Module, 2, Mutators),
    ordsets:add_element({Priority, Module}, Mutators2).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

merge_test() ->
    ?assertEqual([], merge_values([])),
    ?assertEqual([], merge_values([not_a_list_so_trashed])),
    ?assertEqual([{1, module1}], merge_values([[{1, module1}]])),
    ?assertEqual([{1, module1}], merge_values([[{1, module1}],[]])),
    ?assertEqual([{1, module1}], merge_values([[], [{1, module1}],[]])),
    ?assertEqual([{1, module1}], merge_values([{1, module1}], [{9, module1}])),
    ?assertEqual([{1, module1}], merge_values([{9, module1}], [{1, module1}])),
    ?assertEqual([{1, module1}], merge_values([[{1, module1}], [{9, module1}]])),
    ?assertEqual([{1, module1}], merge_values([[{9, module1}], [{3, module1}], [{1, module1}]])),
    ?assertEqual([{1, module1},{2, module2}],
                 merge_values([[{9, module1}, {4, module2}],
                               [{3, module1}, {2, module2}],
                               [{1, module1}, {10, module2}]])),
    ok.

-endif.
