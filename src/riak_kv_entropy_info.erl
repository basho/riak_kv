%% -------------------------------------------------------------------
%%
%% Copyright (c) 2012 Basho Technologies, Inc.  All Rights Reserved.
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
-module(riak_kv_entropy_info).

-export([tree_built/2,
         exchange_complete/4,
         create_table/0,
         dump/0,
         compute_exchange_info/0,
         compute_tree_info/0]).

-define(ETS, ets_riak_kv_entropy).

-type index() :: non_neg_integer().
-type index_n() :: {index(), pos_integer()}.
-type exchange_id() :: {index(), index_n()}.
-type orddict(K,V) :: [{K,V}].
-type riak_core_ring() :: riak_core_ring:riak_core_ring().
-type t_now() :: calendar:t_now().

-record(simple_stat, {last, min, max, count, sum}).

-type repair_stats() :: {Last :: pos_integer(),
                         Min  :: pos_integer(),
                         Max  :: pos_integer(),
                         Mean :: pos_integer()}.

-record(exchange_info, {time :: t_now(),
                        repaired :: pos_integer()}).

-type exchange_info() :: #exchange_info{}.

-record(index_info, {build_time    = undefined     :: t_now(),
                     repaired      = undefined     :: pos_integer(),
                     exchanges     = orddict:new() :: orddict(exchange_id(), exchange_info()),
                     last_exchange = undefined     :: exchange_id()}).

-type index_info() :: #index_info{}.

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Store AAE tree build time
-spec tree_built(index(), t_now()) -> ok.
tree_built(Index, Time) ->
    update_index_info(Index, {tree_built, Time}).

%% @doc Store information about a just-completed AAE exchange
-spec exchange_complete(index(), index(), index_n(), pos_integer()) -> ok.
exchange_complete(Index, RemoteIdx, IndexN, Repaired) ->
    update_index_info(Index, {exchange_complete, RemoteIdx, IndexN, Repaired}).

%% @doc Called by {@link riak_kv_sup} to create public ETS table used for
%%      holding AAE information for reporting. Table will be owned by
%%      the `riak_kv_sup' to ensure longevity.
create_table() ->
    (ets:info(?ETS) /= undefined) orelse
        ets:new(?ETS, [named_table, public, set, {write_concurrency, true}]).

%% @doc Return state of ets_riak_kv_entropy table as a list
dump() ->
    ets:tab2list(?ETS).

%% @doc
%% Return a list containing information about exchanges for all locally owned
%% indices. For each index, return a tuple containing time of most recent
%% exchange; time since the index completed exchanges with all sibling indices;
%% as well as statistics about repairs triggered by different exchanges.
-spec compute_exchange_info()  -> [{index(), Last :: t_now(), All :: t_now(),
                                    repair_stats()}].
compute_exchange_info() ->
    filter_index_info(),
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Indices = riak_core_ring:my_indices(Ring),
    Defaults = [{Index, undefined, undefined, undefined} || Index <- Indices],
    KnownInfo = [compute_exchange_info(Ring, Index, Info) || {Index, Info} <- all_index_info()],
    merge_to_first(KnownInfo, Defaults).

%% @doc Return a list of AAE build times for each locally owned index.
-spec compute_tree_info() -> [{index(), t_now()}].
compute_tree_info() ->
    filter_index_info(),
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Indices = riak_core_ring:my_indices(Ring),
    Defaults = [{Index, undefined} || Index <- Indices],
    KnownInfo = [{Index, Info#index_info.build_time} || {Index, Info} <- all_index_info()],
    merge_to_first(KnownInfo, Defaults).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% Utility function to load stored information for a given index,
%% invoke `handle_index_info' to update the information, and then
%% store the new info back into the ETS table.
-spec update_index_info(index(), term()) -> ok.
update_index_info(Index, Cmd) ->
    Info = case ets:lookup(?ETS, {index, Index}) of
               [] ->
                   #index_info{};
               [{_, I}] ->
                   I
           end,
    Info2 = handle_index_info(Cmd, Index, Info),
    ets:insert(?ETS, {{index, Index}, Info2}),
    ok.

%% Return a list of all stored index information.
-spec all_index_info() -> [{index(), index_info()}].
all_index_info() ->
    ets:select(?ETS, [{{{index, '$1'}, '$2'}, [], [{{'$1','$2'}}]}]).

%% Remove information for indices that this node no longer owns.
filter_index_info() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Primaries = riak_core_ring:my_indices(Ring),
    Indices = ets:select(?ETS, [{{{index, '$1'}, '$2'}, [], ['$1']}]),
    Others = ordsets:subtract(ordsets:from_list(Indices),
                              ordsets:from_list(Primaries)),
    [ets:delete(?ETS, {index, Idx}) || Idx <- Others],
    ok.

%% Update provided index info based on request.
-spec handle_index_info(term(), index(), index_info()) -> index_info().
handle_index_info({tree_built, Time}, _, Info) ->
    Info#index_info{build_time=Time};

handle_index_info({exchange_complete, RemoteIdx, IndexN, Repaired}, _, Info) ->
    ExInfo = #exchange_info{time=os:timestamp(),
                            repaired=Repaired},
    ExId = {RemoteIdx, IndexN},
    Exchanges = orddict:store(ExId, ExInfo, Info#index_info.exchanges),
    RepairStat = update_simple_stat(Repaired, Info#index_info.repaired),
    Info#index_info{exchanges=Exchanges,
                    repaired=RepairStat,
                    last_exchange=ExId}.

%% Return a list of all exchanges necessary to guarantee that `Index' is
%% fully up-to-date.
-spec all_exchanges(riak_core_ring(), index())
                   -> {index(), [{index(), index_n()}]}.
all_exchanges(Ring, Index) ->
    L1 = riak_kv_entropy_manager:all_pairwise_exchanges(Index, Ring),
    L2 = [{RemoteIdx, IndexN} || {_, RemoteIdx, IndexN} <- L1],
    {Index, L2}.

compute_exchange_info(Ring, Index, #index_info{exchanges=Exchanges,
                                               repaired=Repaired}) ->
    {_, AllExchanges} = all_exchanges(Ring, Index),
    Defaults = [{Exchange, undefined} || Exchange <- AllExchanges],
    KnownTime = [{Exchange, EI#exchange_info.time} || {Exchange, EI} <- Exchanges],
    AllTime = merge_to_first(KnownTime, Defaults),
    %% Rely upon fact that undefined < tuple
    AllTime2 = lists:keysort(2, AllTime),
    {_, LastAll} = hd(AllTime2),
    {_, Recent} = hd(lists:reverse(AllTime2)),
    {Index, Recent, LastAll, stat_tuple(Repaired)}.

%% Merge two lists together based on the key at position 1. When both lists
%% contain the same key, the value associated with `L1' is kept.
merge_to_first(L1, L2) ->
    lists:ukeysort(1, L1 ++ L2).

update_simple_stat(Value, undefined) ->
    #simple_stat{last=Value, min=Value, max=Value, sum=Value, count=1};
update_simple_stat(Value, Stat=#simple_stat{max=Max, min=Min, sum=Sum, count=Cnt}) ->
    Stat#simple_stat{last=Value,
                     max=erlang:max(Value, Max),
                     min=erlang:min(Value, Min),
                     sum=Sum+Value,
                     count=Cnt+1}.

stat_tuple(undefined) ->
    undefined;
stat_tuple(#simple_stat{last=Last, max=Max, min=Min, sum=Sum, count=Cnt}) ->
    Mean = Sum div Cnt,
    {Last, Min, Max, Mean}.
