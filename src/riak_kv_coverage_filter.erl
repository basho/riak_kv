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

%% API
-export([build_filter/1, build_filter/3, build_filter/4]).

-export_type([filter/0]).

-type bucket() :: binary()|{binary(), binary()}.
-type filter() :: none | fun((any()) -> boolean()) | [{atom(), atom(), [any()]}].
-type index() :: non_neg_integer().

%% An integer, and the number of bits to shift it left
-type subpartition() :: { non_neg_integer(), pos_integer() }.

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
-spec build_filter(filter()) -> filter().
build_filter(Filter) ->
    build_item_filter(Filter).

-spec build_filter(bucket(), filter(), [index()]|subpartition()) -> filter().
build_filter(Bucket, ItemFilterInput, FilterVNode) ->
    KeyConvFn = fun(StorageKey) -> StorageKey end,
    build_filter(Bucket, ItemFilterInput, FilterVNode, KeyConvFn).

-spec build_filter(bucket(), filter(), [index()]|subpartition(), function()) -> filter().
build_filter(Bucket, ItemFilterInput, FilterVNode, undefined) ->
    %% Generate an identity function for `KeyConvFn' and come back
    build_filter(Bucket, ItemFilterInput, FilterVNode);
build_filter(Bucket, ItemFilterInput, {_Hash, _Mask}=SubP, KeyConvFn) ->
    ItemFilter = build_item_filter(ItemFilterInput),
    if
        (ItemFilter == none) -> % only subpartition filtering required
            SubpartitionFun = build_subpartition_fun(Bucket, KeyConvFn),
            compose_sub_filter(SubP, SubpartitionFun);
        true -> % key and vnode filtering
            SubpartitionFun = build_subpartition_fun(Bucket, KeyConvFn),
            compose_sub_filter(SubP, SubpartitionFun, ItemFilter)
    end;
build_filter(Bucket, ItemFilterInput, FilterVNode, KeyConvFn) ->
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
            PrefListFun = build_preflist_fun(Bucket, CHBin, KeyConvFn),
            %% Create a VNode filter
            compose_filter(FilterVNode, PrefListFun);
        true -> % key and vnode filtering
            {ok, CHBin} = riak_core_ring_manager:get_chash_bin(),
            PrefListFun = build_preflist_fun(Bucket, CHBin, KeyConvFn),
            %% Create a filter for the VNode
            compose_filter(FilterVNode, PrefListFun, ItemFilter)
    end.

%% ====================================================================
%% Internal functions
%% ====================================================================

%% @private
build_subpartition_filter({Mask, BSR}, Fun) ->
    fun(X) ->
            <<Idx:160/integer>> = Fun(X),
            %% lager:error("{~B, ~B} Result, Mask, Index: ~B, ~B, ~B",
            %%             [Mask, BSR, FullMask band Idx,
            %%              FullMask, Idx]),
            Idx bsr BSR =:= Mask
    end.

%% @private
compose_sub_filter(Subpartition, SubpFun) ->
    build_subpartition_filter(Subpartition, SubpFun).


compose_sub_filter(Subpartition, SubpFun, ItemFilter) ->
    SubpFilter = build_subpartition_filter(Subpartition, SubpFun),
    fun(Item) ->
            ItemFilter(Item) andalso SubpFilter(Item)
    end.

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

%% The functions build_preflist_fun and build_subpartition_fun are
%% used to compute the hash and/or vnode index corresponding to a key
%% read from the backend.
%%
%% For example, the function returned in build_preflist_fun is the
%% PrefListFun called in the filter returned by build_vnode_filter
%% above.  That filter is the Filter arg used in
%% riak_kv_vnode:fold_fun variants to determine if a key should be
%% added to a results buffer during a fold.
%%
%% These functions have been modified to accept a key conversion
%% function, KeyConvFn, which allows for an arbitrary transformation
%% to be performed on the storage key prior to computing a hash.  
%%
%% This is required for timseries keys for example, because the local
%% key read from the backend must be transformed to a partition key to
%% determine what hash partition it belongs to.
%%
%% The key conversion function is constructed once when the fold is
%% initialized and passed into these functions to avoid incurring
%% per-key overheads (e.g., extracting the DDL from a TS bucket every
%% time a key is read from the backend, when instead the DDL can be
%% retrieved once and built into the key conversion function)
%%
%% For ordinary keys, KeyConvFn will usually be a no-op, i.e.,
%% KeyConvFn = fun(Key) -> Key end.  We leave it here as a generic
%% hook to allow transformations on any type of key.

%% @private
build_preflist_fun(Bucket, CHBin, KeyConvFn) ->
    fun({o, Key, _Value}) -> %% $ index return_body
            ChashKey = riak_core_util:chash_key({Bucket, KeyConvFn(Key)}),
            chashbin:responsible_index(ChashKey, CHBin);
       ({_Value, Key}) ->
            ChashKey = riak_core_util:chash_key({Bucket, KeyConvFn(Key)}),
            chashbin:responsible_index(ChashKey, CHBin);
       (Key) ->
            ChashKey = riak_core_util:chash_key({Bucket, KeyConvFn(Key)}),
            chashbin:responsible_index(ChashKey, CHBin)
    end.

%% @private
build_subpartition_fun(Bucket, KeyConvFn) ->
    fun({o, Key, _Value}) -> %% $ index return_body
            riak_core_util:chash_key({Bucket, KeyConvFn(Key)});
       ({_Value, Key}) ->
            riak_core_util:chash_key({Bucket, KeyConvFn(Key)});
       (Key) ->
            riak_core_util:chash_key({Bucket, KeyConvFn(Key)})
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
