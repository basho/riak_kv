%% -------------------------------------------------------------------
%%
%% riak_kv_coverage_filter: Construct coverage filter functions.
%%
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc This module is used to construct a property list of VNode
%%      indexes and functions to filter results from a coverage
%%      operation. This may include filtering based on the particular
%%      VNode or filtering on each item in the result list from any
%%      VNode.

-module(riak_kv_coverage_filter).
-author('Kelly McLaughlin <kelly@basho.com>').

%% API
-export([build_filter/3]).

-type bucket() :: binary().
-type filter() :: none | fun().
-type index() :: non_neg_integer().

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Build the list of filter functions for any required VNode indexes.
-spec build_filter(bucket(), filter(), [index()]) -> filter().
build_filter(Bucket, ItemFilterInput, FilterVNode) ->
    ItemFilter = build_item_filter(ItemFilterInput),

    if
        (ItemFilter == none) andalso
        (FilterVNode == undefined) -> % no filtering
            none;
        (FilterVNode == undefined) -> % only key filtering
            %% Compose a key filtering function for the VNode
            compose_filter(ItemFilter);
        (ItemFilter == none) -> % only vnode filtering required
            {ok, Ring} = riak_core_ring_manager:get_my_ring(),
            PrefListFun = build_preflist_fun(Bucket, Ring),
            %% Create a VNode filter
            compose_filter(FilterVNode, PrefListFun);
        true -> % key and vnode filtering
            {ok, Ring} = riak_core_ring_manager:get_my_ring(),
            PrefListFun = build_preflist_fun(Bucket, Ring),
            %% Create a filter for the VNode
            compose_filter(FilterVNode, PrefListFun, ItemFilter) 
    end.

%% ====================================================================
%% Internal functions
%% ====================================================================

%% @private
compose_filter(ItemFilter) ->
    fun(Item, Acc) ->
            case ItemFilter(Item) of
                true ->
                    [Item | Acc];
                false ->
                    Acc
            end
    end.

compose_filter(KeySpaceIndexes, PrefListFun) ->
    VNodeFilter = build_vnode_filter(KeySpaceIndexes, PrefListFun),
    fun(Key, Acc) ->
            case VNodeFilter(Key) of
                true ->
                    [Key|Acc];
                false ->
                    Acc
            end

    end.

compose_filter(undefined, _, ItemFilter) ->
    compose_filter(ItemFilter);
compose_filter(KeySpaceIndexes, PrefListFun, ItemFilter) ->
    VNodeFilter = build_vnode_filter(KeySpaceIndexes, PrefListFun),
    fun(Item, Acc) ->
            case ItemFilter(Item) andalso VNodeFilter(Item) of
                true ->
                    [Item | Acc];
                false ->
                    Acc
            end

    end.

%% @private
build_vnode_filter(KeySpaceIndexes, PrefListFun) ->
    fun(X) ->
            PrefListIndex = PrefListFun(X),
            lists:member(PrefListIndex, KeySpaceIndexes)
    end.

%% @private
build_item_filter(none) ->
    none;
build_item_filter(FilterInput) when is_function(FilterInput) ->
    FilterInput;
build_item_filter(FilterInput) ->
    %% FilterInput is a list of MFA tuples
    compose(FilterInput).


%% @private
build_preflist_fun(Bucket, Ring) ->
    fun(Key) ->
            ChashKey = riak_core_util:chash_key({Bucket, Key}),
            riak_core_ring:responsible_index(ChashKey, Ring)
    end.

compose([]) ->
    none;
compose(Filters) ->
    compose(Filters, fun(V) -> V end).

compose([], F0) -> F0;
compose([Filter1|Filters], F0) ->
    {FilterMod, FilterFun, Args} = Filter1,
    Fun1 = FilterMod:FilterFun(Args),
    F1 = fun(CArgs) -> Fun1(F0(CArgs)) end,
    compose(Filters, F1).

