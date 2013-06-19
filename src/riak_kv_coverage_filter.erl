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

-export_type([filter/0]).

-type bucket() :: binary().
-type filter() :: none | fun((any()) -> boolean()) | [{atom(), atom(), [any()]}].
-type index() :: non_neg_integer().

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Build the list of filter functions for any required VNode indexes.
%%
%% The ItemFilterInput parameter can be the atom `none' to indicate
%% no filtering based on the request items, a function that returns
%% a boolean indicating whether or not the item should be included
%% in the final results, or a list of tuples of the form 
%% {Module, Function, Args}. The latter is the form used by 
%% MapReduce filters such as those in the {@link riak_kv_mapred_filters}
%% module. The list of tuples is composed into a function that is
%% used to determine if an item should be included in the final 
%% result set.
-spec build_filter(bucket(), filter(), [index()]) -> filter().
build_filter(Bucket, ItemFilterInput, FilterVNode) ->
    ItemFilter = build_item_filter(ItemFilterInput),

    if
        (ItemFilter == none) andalso
        (FilterVNode == undefined) -> % no filtering
            none;
        (FilterVNode == undefined) -> % only key filtering
            %% Compose a key filtering function for the VNode
            ItemFilter;
        (ItemFilter == none) -> % only vnode filtering required
            {ok, CHBin} = riak_core_ring_manager:get_chash_bin(),
            PrefListFun = build_preflist_fun(Bucket, CHBin),
            %% Create a VNode filter
            compose_filter(FilterVNode, PrefListFun);
        true -> % key and vnode filtering
            {ok, CHBin} = riak_core_ring_manager:get_chash_bin(),
            PrefListFun = build_preflist_fun(Bucket, CHBin),
            %% Create a filter for the VNode
            compose_filter(FilterVNode, PrefListFun, ItemFilter) 
    end.

%% ====================================================================
%% Internal functions
%% ====================================================================

%% @private
compose_filter(KeySpaceIndexes, PrefListFun) ->
    VNodeFilter = build_vnode_filter(KeySpaceIndexes, PrefListFun),
    VNodeFilter.

compose_filter(undefined, _, ItemFilter) ->
    ItemFilter;
compose_filter(KeySpaceIndexes, PrefListFun, ItemFilter) ->
    VNodeFilter = build_vnode_filter(KeySpaceIndexes, PrefListFun),
    fun(Item) ->
            ItemFilter(Item) andalso VNodeFilter(Item)
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
    %% FilterInput is a list of {Module, Fun, Args} tuples
    compose(FilterInput).


%% @private
build_preflist_fun(Bucket, CHBin) ->
    fun({o, Key, _Value}) -> %% $ index return_body
            ChashKey = riak_core_util:chash_key({Bucket, Key}),
            chashbin:responsible_index(ChashKey, CHBin);
       ({_Value, Key}) ->
            ChashKey = riak_core_util:chash_key({Bucket, Key}),
            chashbin:responsible_index(ChashKey, CHBin);
       (Key) ->
            ChashKey = riak_core_util:chash_key({Bucket, Key}),
            chashbin:responsible_index(ChashKey, CHBin)
    end.



compose([]) ->
    none;
compose(Filters) ->
    compose(Filters, []).

compose([], RevFilterFuns) ->
    FilterFuns = lists:reverse(RevFilterFuns),
    fun(Val) ->
            true =:= lists:foldl(fun(Fun, Acc) -> Fun(Acc) end,
                                 Val,
                                 FilterFuns)
    end;
compose([Filter | RestFilters], FilterFuns) ->
    {FilterMod, FilterFun, Args} = Filter,
    Fun = FilterMod:FilterFun(Args),
    compose(RestFilters, [Fun | FilterFuns]).

