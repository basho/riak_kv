%% -------------------------------------------------------------------
%%
%% riak_kv_tictacaae_repairs: functions for tictac aae prompted repairs
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


%% @doc Various functions that are useful for repairing entropy via tictac aae
-module(riak_kv_tictacaae_repairs).

-export([prompt_tictac_exchange/7, log_tictac_result/4]).

-include_lib("kernel/include/logger.hrl").

-define(EXCHANGE_PAUSE_MS, 1000).
-define(AAE_MAX_RESULTS, 128).
-define(AAE_RANGE_BOOST, 2).
-define(MEGA, 1000000).
-define(SCAN_TIMEOUT_MS, 120000).
-define(MIN_REPAIRTIME_MS, 10000).
-define(MIN_REPAIRPAUSE_MS, 10).

-type repair_list() ::
    list({riak_object:bucket(),
            pos_integer(),
            list(riak_object:key()),
            list(erlang:timestamp())}).

-type keyclock_list() ::
    list({{riak_object:bucket(), riak_object:key()},
            {vclock:vclock(), vclock:vclock()}}).


%% ===================================================================
%% Public API
%% ===================================================================


-spec prompt_tictac_exchange({riak_core_ring:partition_id(), node()},
                        {riak_core_ring:partition_id(), node()},
                        {non_neg_integer(), pos_integer()},
                        pos_integer(), pos_integer(),
                        fun((term()) -> ok),
                        aae_exchange:filters()) -> ok.
prompt_tictac_exchange(LocalVnode, RemoteVnode, IndexN,
                    ScanTimeout, LoopCount,
                    ReplyFun, Filter) ->
    ExchangePause =
        app_helper:get_env(riak_kv,
                            tictacaae_exchangepause,
                            ?EXCHANGE_PAUSE_MS),
    RangeBoost =
        case Filter of
            none ->
                1;
            _ ->
                app_helper:get_env(riak_kv,
                                    tictacaae_rangeboost,
                                    ?AAE_RANGE_BOOST)
        end,
    MaxResults = 
        case app_helper:get_env(riak_kv, tictacaae_maxresults) of
            MR when is_integer(MR) ->
                MR * RangeBoost;
            _ ->
                ?AAE_MAX_RESULTS * RangeBoost
        end,
    ExchangeOptions =
        [{scan_timeout, ScanTimeout},
            {transition_pause_ms, ExchangePause},
            {purpose, kv_aae},
            {max_results, MaxResults}],
    
    BlueList = 
        [{riak_kv_vnode:aae_send(LocalVnode), [IndexN]}],
    PinkList = 
        [{riak_kv_vnode:aae_send(RemoteVnode), [IndexN]}],
    PromptRehash = Filter == none,
    RepairFun = 
        prompt_readrepair([LocalVnode, RemoteVnode],
                            IndexN,
                            MaxResults,
                            LoopCount,
                            PromptRehash,
                            os:timestamp()),
    {ok, _AAEPid, AAExid} =
        aae_exchange:start(full,
                        BlueList, 
                        PinkList, 
                        RepairFun, 
                        ReplyFun,
                        Filter,
                        ExchangeOptions),
    _ = 
        ?LOG_DEBUG("Exchange prompted with exchange_id=~s between ~w and ~w",
                [AAExid, LocalVnode, RemoteVnode]),
    ok.


-spec log_tictac_result(
    {root_compare|branch_compare|clock_compare|
        error|timeout|not_supported, non_neg_integer()},
    bucket|modtime|exchange,
    non_neg_integer()|initial,
    riak_core_ring:partition_id()) -> ok.
log_tictac_result(ExchangeResult, FilterType, LoopCount, Index) ->
    PotentialRepairs =
        case element(2, ExchangeResult) of
            N when is_integer(N), N > 0 ->
                riak_kv_stat:update({tictac_aae, FilterType, N}),
                N;
            _ ->
                0
        end,
    ExchangeState =
        element(1, ExchangeResult),
    case expected_aae_state(ExchangeState) of
        true ->
            riak_kv_stat:update({tictac_aae, ExchangeState});
        _ ->
            ok
    end,
    case ExchangeState of
        PositiveState
            when PositiveState == root_compare;
                    PositiveState == branch_compare ->
            ok;
        ExchangeState ->
            ?LOG_INFO("Tictac AAE exchange for partition=~w " ++
                        "pending_state=~w filter_type=~w loop_count=~w " ++
                        "potential_repairs=~w",
                        [Index,
                            ExchangeState, FilterType, LoopCount,
                            PotentialRepairs])
    end,
    ok.


%% ===================================================================
%% Utility functions
%% ===================================================================

expected_aae_state(ExchangeState) ->
    lists:member(ExchangeState, 
        [root_compare, branch_compare, clock_compare,
            error, timeout, not_supported]).


-spec prompt_readrepair(
        [{riak_core_ring:partition_id(), node()}],
            {non_neg_integer(), pos_integer()},
            non_neg_integer(),
            non_neg_integer(),
            boolean(),
            erlang:timestamp()) ->
    fun((keyclock_list()) -> ok).
prompt_readrepair(VnodeList, IndexN, MaxResults,
                    LoopCount, Rehash, StartTime) ->
    prompt_readrepair(VnodeList,
                        IndexN,
                        MaxResults,
                        LoopCount,
                        StartTime,
                        Rehash,
                        app_helper:get_env(riak_kv, log_readrepair, false)).

prompt_readrepair(VnodeList, IndexN, MaxResults, 
                    LoopCount, StartTime, Rehash, LogRepair) ->
    {ok, C} = riak:local_client(),
    FetchFun = 
        fun({{B, K}, {_BlueClock, _PinkClock}}) ->
            case riak_kv_util:consistent_object(B) of
                true ->
                    riak_kv_exchange_fsm:repair_consistent({B, K});
                false ->
                    riak_client:get(B, K, C)
            end
        end,
    LogFun = 
        fun({{B, K}, {BlueClock, PinkClock}}) ->
            ?LOG_INFO(
                "Prompted read repair Bucket=~p Key=~p Clocks ~w ~w",
                    [B, K, BlueClock, PinkClock])
        end,
    fun(RepairList) ->
        SW = os:timestamp(),
        RepairCount = length(RepairList),
        ?LOG_INFO("Repairing key_count=~w between ~w",
                    [RepairCount, VnodeList]),
        Pause =
            max(?MIN_REPAIRPAUSE_MS,
                ?MIN_REPAIRTIME_MS div max(1, RepairCount)),
        RehashFun = 
            fun({{B, K}, {_BlueClock, _PinkClock}}) ->
                timer:sleep(Pause),
                riak_kv_vnode:rehash(VnodeList, B, K)
            end,
        lists:foreach(FetchFun, RepairList),
        case Rehash of
            true ->
                lists:foreach(RehashFun, RepairList);
            _ ->
                ok
            end,
        case LogRepair of
            true ->
                lists:foreach(LogFun, RepairList);
            false ->
                ok
        end,
        EndTime = os:timestamp(),
        ?LOG_INFO("Repaired key_count=~w " ++ 
                        "in repair_time=~w ms with pause_time=~w ms " ++
                        "total process_time=~w ms",
                    [RepairCount,
                        timer:now_diff(EndTime, SW) div 1000,
                        RepairCount * Pause,
                        timer:now_diff(EndTime, StartTime) div 1000]),
        case LoopCount of
            LoopCount when LoopCount > 0 ->
                case analyse_repairs(RepairList, MaxResults) of
                    {false, none} ->
                        ?LOG_INFO("Repair cycle type=false at LoopCount=~w",
                                    [LoopCount]);
                    {FilterType, Filter} ->
                        ?LOG_INFO("Repair cycle type=~p at LoopCount=~w",
                                    [FilterType, LoopCount]),
                        [LocalVnode, RemoteVnode] = VnodeList,
                        ReplyFun =
                            fun(ExchangeResult) ->
                                log_tictac_result(
                                    ExchangeResult,
                                    FilterType,
                                    LoopCount,
                                    element(1, IndexN))
                            end,
                        prompt_tictac_exchange(
                            LocalVnode, RemoteVnode, IndexN,
                            ?SCAN_TIMEOUT_MS, LoopCount - 1,
                            ReplyFun, Filter)
                end;
            LoopCount ->
                ?LOG_INFO("Repair cycle type=complete at LoopCount=~w",
                                [LoopCount])
        end              
    end.

%% @doc
%% Is there a majority of repairs which belong to a particular bucket.
%% In which case we should re-run the aae_exchange, but this time filter
%% the fetch clocks to look only in the relevant Bucket and KeyRange and
%% Last Modified range discovered for this bucket.
%% If there is no majority bucket, but there are more than half MaxResults
%% returned, then re-run with only a last modified range as a filter.
%% With the filters applied, one can reasonable expect the resulting
%% fetch_clock queries to be substantially faster (unless there happens to
%% be no majority bucket, and a low last_modified time)
-spec analyse_repairs(keyclock_list(), non_neg_integer()) ->
        {false|bucket|modtime, aae_exchange:filters()}.
analyse_repairs(KeyClockList, MaxRepairs) ->
    RepairList = lists:foldl(fun analyse_repair/2, [], KeyClockList),
    EnableKeyRange =
        app_helper:get_env(riak_kv, tictacaae_enablekeyrange, false),
    ClusterCapable =
        riak_core_capability:get({riak_kv, tictacaae_prompted_repairs}, false),
    analyse_repairs(RepairList, MaxRepairs, EnableKeyRange, ClusterCapable).

-spec analyse_repairs(repair_list(), non_neg_integer(), boolean(), boolean())
                            -> {false|bucket|modtime, aae_exchange:filters()}.
analyse_repairs(_RepairList, _MaxRepairs, _EnableKeyRange, false) ->
    {false, none};
analyse_repairs(RepairList, MaxRepairs, EnableKeyRange, _) ->
    case lists:reverse(lists:keysort(2, RepairList)) of
        [Candidate|_Rest] ->
            Threshold = MaxRepairs div 3,
            case Candidate of
                {B, CandCount, KL, MTL} when CandCount > (2 * Threshold) ->
                    [FirstKey|RestKeys] = lists:sort(KL),
                    LastKey = lists:last(RestKeys),
                    KeyRange =
                        case EnableKeyRange of 
                            true -> {FirstKey, LastKey};
                            false -> all
                        end,
                    {bucket,
                        {filter,
                            B, KeyRange, large, all,
                            get_modified_range(MTL),
                            pre_hash}};
                _ ->
                    FoldFun = fun({_B, _C, _KL, MTL}, Acc) -> Acc ++ MTL end,
                    MTL = lists:sort(lists:foldl(FoldFun, [], RepairList)),
                    case length(MTL) of
                        CandCount when CandCount > Threshold ->
                            {modtime, 
                                {filter,
                                    all, all, large, all,
                                    get_modified_range(MTL),
                                    pre_hash}};
                        _ ->
                            {false, none}
                    end
            end;
        [] ->
            {false, none}
    end.
            

-spec get_modified_range(list(calendar:datetime()))
                            -> {pos_integer(), pos_integer()}.
get_modified_range(ModifiedDateTimeList) ->
    [FirstDate|RestDates] = lists:sort(ModifiedDateTimeList),
    HighDate = lists:last(RestDates),
    EpochTime =
        calendar:datetime_to_gregorian_seconds({{1970,1,1},{0,0,0}}),
    LowTS = 
        calendar:datetime_to_gregorian_seconds(FirstDate) - EpochTime,
    HighTS =
        calendar:datetime_to_gregorian_seconds(HighDate) - EpochTime,
    {LowTS - 1, HighTS + 1}.

%% @doc Look at the last modified time on each clock
%% Further repairs will only on those prompted in the clean case
%% when the last modified time is different between the clocks - i.e.
%% one clock has an identifiably higher last modified time which can be
%% considered a likely timestamp of an incident
-spec analyse_repair({{riak_object:bucket(), riak_object:key()},
                        {vclock:vclock(), vclock:vclock()}},
                        repair_list()) -> repair_list().
analyse_repair({{B, K}, {BlueClock, PinkClock}}, ByBucketAcc) ->
    BlueTime = last_modified(BlueClock),
    PinkTime = last_modified(PinkClock),
    ModTime = 
        if 
            BlueTime > PinkTime ->
                BlueTime;
            PinkTime > BlueTime ->
                PinkTime;
            true ->
                false
        end,
    case ModTime of
        false ->
            ByBucketAcc;
        ModTime ->
            case lists:keytake(B, 1, ByBucketAcc) of
                false ->
                    [{B, 1, [K], [ModTime]}|ByBucketAcc];
                {value, {B, C, KL, TL}, RemBucketAcc} ->
                    [{B, C + 1, [K|KL], [ModTime|TL]}|RemBucketAcc]
            end
    end.

last_modified(none) ->
    none;
last_modified(VC) ->
    vclock:last_modified(VC).

