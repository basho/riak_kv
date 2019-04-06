%% -------------------------------------------------------------------
%%
%% riak_kv_ttaefs_manager: coordination of full-sync replication
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

%% @doc coordination of full-sync replication

-module(riak_kv_ttaaefs_manager).
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-behaviour(gen_server).

-export([init/1,
        handle_call/3,
        handle_cast/2,
        handle_info/2,
        terminate/2,
        code_change/3]).

-export([start_link/0,
            pause/1,
            resume/1,
            change_sink/4]).

-export([take_next_workitem/5,
            reply_complete/2]).

-define(SLICE_COUNT, 100).
-define(SECONDS_IN_DAY, 86400).
-define(INITIAL_TIMEOUT, 60000).
    % Wait a minute before the first allocation is considered,  Lot may be
    % going on at a node immeidately at startup
-define(LOOP_TIMEOUT, 15000).
    % Always wait at least 15s after completing an action before
    % prompting another
-define(CRASH_TIMEOUT, 3600 * 1000).
    % Assume that an exchange has crashed if not response received in this
    % interval, to allow exchanges to be re-scheduled.
-define(REPAIR_RATE, 100).

-record(state, {slice_allocations = [] :: list(allocation()),
                slice_set_start = os:timestamp() :: erlang:timestamp(),
                schedule :: schedule_wants(),
                backup_schedule :: schedule_wants(),
                peer_ip,
                peer_port,
                peer_protocol :: http|pb,
                scope :: bucket_list|all,
                bucket_list :: list()|undefined,
                nval_list :: list(pos_integer())|undefined,
                slot_info_fun :: fun(),
                repair_rate = ?REPAIR_RATE :: pos_integer(),
                slice_count = ?SLICE_COUNT :: pos_integer()
                }).

% -type fullsync_state() :: #state{}.

-type remote_client_protocol() :: http.
-type remote_ip() :: string().
-type remote_port() :: pos_integer().
-type nval_or_range() :: pos_integer()|range. % Range queries do not have an n-val
-type work_item() :: no_sync|all_sync|day_sync|hour_sync.
-type schedule_want() :: {work_item(), non_neg_integer()}.
-type slice() :: pos_integer().
-type allocation() :: {slice(), work_item()}.
-type schedule_wants() :: [schedule_want()].
-type node_info() :: {pos_integer(), pos_integer()}. % Node and node count

%%%============================================================================
%%% API
%%%============================================================================

start_link() ->
    gen_server:start_link(?MODULE, [], []).

reply_complete(Pid, ExchangeID) ->
    gen_server:cast(Pid, {reply_complete, ExchangeID}).

pause(Pid) ->
    gen_server:call(Pid, pause).

resume(Pid) ->
    gen_server:call(Pid, resume).

change_sink(Pid, Protocol, IP, Port) ->
    gen_server:call(Pid, {change_sink, Protocol, IP, Port}).



%%%============================================================================
%%% gen_server callbacks
%%%============================================================================

init([]) ->
    State0 = #state{},
    % Get basic coniguration of scope (either all keys for each n_val, or a
    % specific list of buckets)
    Scope = app_helper:get_env(riak_kv, ttaaefs_scope),
    State1 = 
        case Scope of
            all ->
                NValList = app_helper:get_env(riak_kv, ttaaefs_nvallist),
                State0#state{scope=all, nval_list=NValList};
            bucket_list ->
                BucketList = app_helper:get_env(riak_kv, ttaaefs_bucketlist),
                State0#state{scope=bucket_list, bucket_list=BucketList}
        end,
    
    % Fetch connectivity information for remote cluster
    PeerIP = app_helper:get_env(riak_kv, ttaaefs_peerip),
    PeerPort = app_helper:get_env(riak_kv, ttaaefs_port),
    PeerProtocol = app_helper:get_env(riak_kv, ttaaefs_protocol),
    State2 = 
        State1#state{peer_ip = PeerIP,
                        peer_port = PeerPort,
                        peer_protocol = PeerProtocol},
    
    NoCheck = app_helper:get_env(riak_kv, ttaaes_nocheck),
    AllCheck = app_helper:get_env(riak_kv, ttaaes_allcheck),
    HourCheck = app_helper:get_env(riak_kv, ttaaes_hourcheck),
    DayCheck = app_helper:get_env(riak_kv, ttaaes_daycheck),

    Schedule = 
        case Scope of
            all ->
                SliceCount = NoCheck + AllCheck,
                [{no_sync, NoCheck}, {all_sync, AllCheck}];
            bucket_list ->
                SliceCount = NoCheck + AllCheck + HourCheck + DayCheck,
                [{no_sync, NoCheck}, {all_sync, AllCheck},
                    {hour_sync, HourCheck}, {day_sync, DayCheck}]
        end,
    State3 = State2#state{schedule = Schedule,
                            slice_count = SliceCount,
                            slot_info_fun = fun get_slotinfo/0},
    lager:info("Initiated Tictac AAE Full-Sync Mgr with scope=~w", [Scope]),
    {ok, State3, ?INITIAL_TIMEOUT}.

handle_call(pause, _From, State) ->
    PausedSchedule = [{no_sync, State#state.slice_count}],
    BackupSchedule = State#state.schedule,
    {reply, ok, State#state{schedule = PausedSchedule,
                            backup_schedule = BackupSchedule}};
handle_call(resume, _From, State) ->
    Schedule = State#state.backup_schedule,
    {reply, ok, State#state{schedule = Schedule}, ?INITIAL_TIMEOUT};
handle_call({change_sink, Protocol, PeerIP, PeerPort}, _From, State) ->
    State0 = 
        State#state{peer_ip = PeerIP,
                        peer_port = PeerPort,
                        peer_protocol = Protocol},
    {reply, ok, State0, ?INITIAL_TIMEOUT}.

handle_cast({reply_complete, _ExchangeID}, State) ->
    {noreply, State, ?LOOP_TIMEOUT};
handle_cast(no_sync, State) ->
    {noreply, State, ?LOOP_TIMEOUT};
handle_cast(all_sync, State) ->
    {ThisNVal, ThisFilter, NextNValList, NextBucketList, Ref} =
        case State#state.scope of
            all ->
                [H|T] = State#state.nval_list,
                {H, none, T ++ [H], undefined, full};
            bucket_list ->
                [H|T] = State#state.bucket_list,
                {range, 
                    {filter, H, all, large, all, all, pre_hash},
                    undefined,
                    T ++ [H],
                    partial}
        end,

    ExchangeRef = pid_to_list(self()) ++ atom_to_list(Ref),
    LocalSendFun = local_sendfun(ThisNVal),
    RemoteClient =
        remote_client(State#state.peer_protocol,
                        State#state.peer_ip,
                        State#state.peer_port),
    case RemoteClient of
        no_client ->
            {noreply,
                State#state{nval_list = NextNValList,
                            bucket_list = NextBucketList},
                ?LOOP_TIMEOUT};
        _ ->
            RemoteSendFun = remote_sendfun(RemoteClient, ThisNVal),
            RepairFun =
                generate_repairfun(RemoteClient,
                                    ExchangeRef,
                                    State#state.repair_rate),
            ReplyFun = generate_replyfun(ExchangeRef),

            {ok, ExPid, ExID} =
                aae_exchange:start(Ref,
                                    [{LocalSendFun, all}],
                                    [{RemoteSendFun, all}],
                                    RepairFun,
                                    ReplyFun,
                                    ThisFilter, []),
            
            lager:info("Starting full-sync ExchangeRef=~s id=~w pid=~w",
                            [ExchangeRef, ExID, ExPid]),
            
            {noreply,
                State#state{nval_list = NextNValList,
                            bucket_list = NextBucketList},
                ?CRASH_TIMEOUT}
    end;
handle_cast(hour_sync, State) ->
    {noreply, State, ?CRASH_TIMEOUT};
handle_cast(day_sync, State) ->
    {noreply, State, ?CRASH_TIMEOUT}.


handle_info(timeout, State) ->
    SlotInfoFun = State#state.slot_info_fun,
    {WorkItem, Wait, RemainingSlices, ScheduleStartTime} = 
        take_next_workitem(State#state.slice_allocations,
                            State#state.schedule,
                            State#state.slice_set_start,
                            SlotInfoFun(),
                            State#state.slice_count),
    erlang:send_after(Wait, self(), WorkItem),
    {noreply, State#state{slice_allocations = RemainingSlices,
                            slice_set_start = ScheduleStartTime}}.

terminate(normal, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%============================================================================
%%% Internal functions
%%%============================================================================


%% @doc
%% Check the slot info - how many nodes are there active in the cluster, and
%% which is the slot for this node in the cluster.
%% An alternative slot_info function may be passed in when initialising the
%% server (e.g. for test)
-spec get_slotinfo() -> node_info().
get_slotinfo() ->
    UpNodes = lists:sort(riak_core_node_watcher:nodes(riak_kv)),
    NotMe = lists:takewhile(fun(N) -> not N == node() end, UpNodes),
    {length(NotMe) + 1, length(UpNodes)}.

%% @doc
%% Return a function which will send aae_exchange messages to a remote
%% cluster, and return the response.  The function shouls make an async call
%% to try and make the remote and local cluster sends happen as close to
%% parallel as possible. 
-spec remote_sendfun(rhc:rhc(), nval_or_range()) -> fun().
remote_sendfun(RHC, NVal) ->
    fun(Msg, all, Colour) ->
        AAE_Exchange = self(),
        ReturnFun = 
            fun(R) -> 
                aae_exchange:reply(AAE_Exchange, R, Colour)
            end,
        SendFun = remote_sender(Msg, RHC, ReturnFun, NVal),
        _SpawnedPid = spawn(SendFun),
        ok
    end.

%% @doc
%% Make a remote client for connecting to the remote cluster
-spec remote_client(remote_client_protocol(), remote_ip(), remote_port())
                                                        -> rhc:rhc()|no_client.
remote_client(http, IP, Port) ->
    RHC = rhc:create(IP, Port, "riak", []),
    case rhc:ping(RHC) of
        ok ->
            RHC;
        {error, Error} ->
            lager:warning("Cannot reach remote cluster ~p ~p with error ~p",
                            [IP, Port, Error]),
            no_client
    end.

%% @doc
%% Translate aae_Exchange messages into riak erlang http client requests
-spec remote_sender(any(), rhc:rhc(), fun(), nval_or_range()) -> fun().
remote_sender(fetch_root, RHC, ReturnFun, NVal) ->
    fun() ->
        {ok, {root, Root}}
            = rhc:aae_merge_root(RHC, NVal),
        ReturnFun(Root)
    end;
remote_sender({fetch_branches, BranchIDs}, RHC, ReturnFun, NVal) ->
    fun() ->
        {ok, {branches, ListOfBranchResults}}
            = rhc:aae_merge_branches(RHC, NVal, BranchIDs),
        ReturnFun(ListOfBranchResults)
    end;
remote_sender({fetch_clocks, SegmentIDs}, RHC, ReturnFun, NVal) ->
    fun() ->
        {ok, {keysclocks, KeysClocks}}
            = rhc:aae_fetch_clocks(RHC, NVal, SegmentIDs),
        ReturnFun(KeysClocks)
    end;
remote_sender({merge_tree_range, B, KR, TS, SF, MR, HM},
                    RHC, ReturnFun, range) ->
    fun() ->
        {ok, {tree, Tree}}
            = rhc:aae_range_tree(RHC, B, KR, TS, SF, MR, HM),
        ReturnFun(Tree)
    end;
remote_sender({fetch_clocks_range, B, KR, SF, MR}, RHC, ReturnFun, range) ->
    fun() ->
        {ok, {keysclocks, KeysClocks}}
            = rhc:aae_range_clocks(RHC, B, KR, SF, MR),
        ReturnFun(KeysClocks)
    end.


%% @doc
%% Generate the send fun to be used to contact the local (source) cluster
-spec local_sendfun(nval_or_range()) -> fun().
local_sendfun(NVal) ->
    RC = riak_client:new(node(), undefined),
    % We assume as this is the local node, the ping used for the remote client
    % is unnecessary
    fun(Msg, all, Colour) ->
        AAE_Exchange = self(),
        ReturnFun = 
            fun(R) -> 
                aae_exchange:reply(AAE_Exchange, R, Colour)
            end,
        Query = local_query(Msg, NVal),
        SendFun = 
            fun() ->
                {ok, R} = riak_client:aae_fold(Query, RC),
                ReturnFun(R)
            end,
        _SpawnedPid = spawn(SendFun),
        ok
    end.

%% @doc
%% Convert the query message from aae_exchange to the format required by the
%% riak_kv_clusteraae_fsm
-spec local_query(fetch_root|tuple(), nval_or_range()) -> tuple().
local_query(fetch_root, NVal) ->
    {merge_root_nval, NVal};
local_query({fetch_branches, BranchIDs}, NVal) ->
    {merge_branch_nval, NVal, BranchIDs};
local_query({fetch_clocks, SegmentIDs}, NVal) ->
    {fetch_clocks_nval, NVal, SegmentIDs};
local_query({merge_tree_range, B, KR, TS, SF, MR, HM}, range) ->
    {merge_tree_range, B, KR, TS, SF, MR, HM};
local_query({fetch_clocks_range, B, KR, SF, MR}, range) ->
    {fetch_clocks_range, B, KR, SF, MR}.


%% @doc
%% Generate a reply fun (as there is nothing to reply to this will simply
%% update the stats for Tictac AAE full-syncs
generate_replyfun(ExchangeID) ->
    Pid = self(),
    fun({ExchangeState, KeyDeltas}) ->
        lager:info("ExchangeState=~w Deltas=~w for Exchange with ID=~w",
                    [ExchangeState, KeyDeltas, ExchangeID]),
        reply_complete(Pid, ExchangeID)
    end.

%% @doc
%% Generate a repair fun which will compare clocks between source and sink
%% cluster, and prompt the re-replication of objects that are more up-to-date
%% in the local (source) cluster
%%
%% The RepairFun will receieve a list of tuples of:
%% - Bucket
%% - Key
%% - Source-Side VC
%% - Sink-Side VC
%%
%% If the sink side dominates the repair should log and not repair, otherwise
%% the object should be repaired by requeueing.  Requeueing will cause the
%% object to be re-replicated to all destination clusters (not just a specific
%% sink cluster)
-spec generate_repairfun(rhc:rhc(), string(), pos_integer()) -> fun().
generate_repairfun(RemoteClient, ExchangeID, RepairsPerSecond) ->
    fun(RepairList) ->
        FoldFun =
            fun({B, K, SrcVC, SinkVC}, {SourceL, SinkC}) ->
                % how are the vector clocks encoded at this point?
                % The erlify_aae_keyclock will have base64 decoded the clock
                case vclock:dominates(SinkVC, SrcVC) of
                    true ->
                        {SourceL, SinkC + 1};
                    false ->
                        [{B, K}|SourceL]
                end
            end,
        {ToRepair, SinkDCount} = lists:foldl(FoldFun, {[], 0}, RepairList),
        lager:info("AAE exchange ~s shows sink ahead for ~w keys", 
                    [ExchangeID, SinkDCount]),
        lager:info("AAE exchange ~s outputs ~w keys to be repaired",
                    [ExchangeID, length(ToRepair)]),
        C = riak_client:new(node(), undefined),
        requeue_keys(C, RemoteClient, ToRepair, RepairsPerSecond),
        lager:info("AAE exchange ~s has requeue complete for ~w keys",
                    [ExchangeID, length(ToRepair)])
    end.

%% @doc
%% requeue the keys, in batches, with a pause at the ned of each batch up to
%% The 1s point since the start of the batch 
-spec requeue_keys(riak_client:riak_client(), rhc:rhc(),
                    list(tuple()), pos_integer()) -> ok.
requeue_keys(_Client, _RHC, [], _RepairsPerSecond) ->
    ok;
requeue_keys(Client, RHC, ToRepair, RepairsPerSecond) ->
    {SubList, StillToRepair} = 
        case length(ToRepair) > RepairsPerSecond of
            true ->
                lists:split(RepairsPerSecond, ToRepair);
            false ->
                {ToRepair, []}
        end,
    SW = os:timestamp(),
    RequeueFun = 
        fun({B, K}) ->
            % TODO: should use rt_enqueue here
            % however, having issues with dialyzer as riak_repl not a dep
            % the having issues with mismatched deps on riak_kv when it is
            % For now - be dumb and do a touch
            case riak_client:get(B, K, Client) of
                {ok, Obj} ->
                    % Happy day only for now
                    rhc:put(RHC, Obj, [asis])
            end
        end,
    ok = lists:foreach(RequeueFun, lists:usort(SubList)),
    TimeMS = timer:now_diff(os:timestamp(), SW) div 1000,
    case TimeMS < 1000 of
        true ->
            timer:sleep(1000 - TimeMS);
        false ->
            ok
    end,
    requeue_keys(Client, RHC, StillToRepair, RepairsPerSecond).



%% @doc
%% Take the next work item from the list of allocations, assuming that the
%% starting time for that work item has not alreasy passed.  If there are no
%% more items queue, start a new queue based on the wants for the schedule.
-spec take_next_workitem(list(allocation()), 
                            schedule_wants(),
                            erlang:timestamp(),
                            node_info(),
                            pos_integer()) ->
                                {work_item(), pos_integer(),
                                    list(allocation()), erlang:timestamp()}.
take_next_workitem([], Wants, ScheduleStartTime, SlotInfo, SliceCount) ->
    NewAllocations = choose_schedule(Wants),
    % Should be 24 hours after ScheduleStartTime - so add 24 hours to
    % ScheduleStartTime
    {Mega, Sec, _Micro} = ScheduleStartTime,
    Seconds = Mega * 1000 + Sec + 86400,
    RevisedStartTime = {Seconds div 1000, Seconds rem 1000, 0},
    take_next_workitem(NewAllocations, Wants,
                        RevisedStartTime, SlotInfo, SliceCount);
take_next_workitem([NextAlloc|T], Wants,
                        ScheduleStartTime, SlotInfo, SliceCount) ->
    {NodeNumber, NodeCount} = SlotInfo,
    SliceSeconds = ?SECONDS_IN_DAY div SliceCount,
    SlotSeconds =
        NodeNumber * (SliceSeconds div NodeCount) 
        + random:uniform(SliceSeconds),
    {SliceNumber, NextAction} = NextAlloc,
    {Mega, Sec, _Micro} = ScheduleStartTime,
    ScheduleSeconds = 
        Mega * 1000 + Sec + SlotSeconds + SliceNumber * SliceSeconds,
    {MegaNow, SecNow, _MicroNow} = os:timestamp(),
    NowSeconds = MegaNow * 1000 + SecNow,
    case ScheduleSeconds > NowSeconds of
        true ->
            {NextAction, ScheduleSeconds - NowSeconds, T, ScheduleStartTime};
        false ->
            lager:info("Tictac AAE skipping action ~w as manager running"
                        ++ "~w seconds late",
                        [NextAction, NowSeconds - ScheduleSeconds]),
            take_next_workitem(T, Wants,
                                ScheduleStartTime, SlotInfo, SliceCount)
    end.


%% @doc
%% Calculate an allocation of activity for the next 24 hours based on the
%% configured schedule-needs.
-spec choose_schedule(schedule_wants()) -> list(allocation()).
choose_schedule(ScheduleWants) ->
    [{no_sync, NoSync}, {all_sync, AllSync},
        {day_sync, DaySync}, {hour_sync, HourSync}] = ScheduleWants,
    SliceCount = NoSync + AllSync + DaySync + HourSync,
    Slices = lists:seq(1, SliceCount),
    Allocations = [],
    lists:sort(choose_schedule(Slices,
                                Allocations,
                                {NoSync, AllSync, DaySync, HourSync})).

choose_schedule([], Allocations, {0, 0, 0, 0}) ->
    lists:ukeysort(1, Allocations);
choose_schedule(Slices, Allocations, {NoSync, 0, 0, 0}) ->
    {HL, [Allocation|TL]} =
        lists:split(random:uniform(length(Slices)) - 1, Slices),
    choose_schedule(HL ++ TL,
                    [{Allocation, no_sync}|Allocations],
                    {NoSync - 1, 0, 0, 0});
choose_schedule(Slices, Allocations, {NoSync, AllSync, 0, 0}) ->
    {HL, [Allocation|TL]} =
        lists:split(random:uniform(length(Slices)) - 1, Slices),
    choose_schedule(HL ++ TL,
                    [{Allocation, all_sync}|Allocations],
                    {NoSync, AllSync - 1, 0, 0});
choose_schedule(Slices, Allocations, {NoSync, AllSync, DaySync, 0}) ->
    {HL, [Allocation|TL]} =
        lists:split(random:uniform(length(Slices)) - 1, Slices),
    choose_schedule(HL ++ TL,
                    [{Allocation, day_sync}|Allocations],
                    {NoSync, AllSync, DaySync - 1, 0});
choose_schedule(Slices, Allocations, {NoSync, AllSync, DaySync, HourSync}) ->
    {HL, [Allocation|TL]} =
        lists:split(random:uniform(length(Slices)) - 1, Slices),
    choose_schedule(HL ++ TL,
                    [{Allocation, hour_sync}|Allocations],
                    {NoSync, AllSync, DaySync, HourSync - 1}).


%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).

choose_schedule_test() ->
    NoSyncAllSchedule =
        [{no_sync, 100}, {all_sync, 0}, {day_sync, 0}, {hour_sync, 0}],
    NoSyncAll = choose_schedule(NoSyncAllSchedule),
    ExpNoSyncAll = lists:map(fun(I) -> {I, no_sync} end, lists:seq(1, 100)),
    ?assertMatch(NoSyncAll, ExpNoSyncAll),

    AllSyncAllSchedule =
        [{no_sync, 0}, {all_sync, 100}, {day_sync, 0}, {hour_sync, 0}],
    AllSyncAll = choose_schedule(AllSyncAllSchedule),
    ExpAllSyncAll = lists:map(fun(I) -> {I, all_sync} end, lists:seq(1, 100)),
    ?assertMatch(AllSyncAll, ExpAllSyncAll),
    
    MixedSyncSchedule = 
        [{no_sync, 0}, {all_sync, 1}, {day_sync, 4}, {hour_sync, 95}],
    MixedSync = choose_schedule(MixedSyncSchedule),
    ?assertMatch(100, length(MixedSync)),
    IsSyncFun = fun({_I, Type}) -> Type == hour_sync end,
    SliceForHourFun = fun({I, hour_sync}) -> I end,
    HourWorkload =
        lists:map(SliceForHourFun, lists:filter(IsSyncFun, MixedSync)),
    ?assertMatch(95, length(lists:usort(HourWorkload))),
    FoldFun = 
        fun(I, Acc) ->
            true = I > Acc,
            I
        end,
    BiggestI = lists:foldl(FoldFun, 0, HourWorkload),
    ?assertMatch(true, BiggestI >= 95).

take_first_workitem_test() ->
    Wants = [{no_sync, 100}, {all_sync, 0}, {day_sync, 0}, {hour_sync, 0}],
    {Mega, Sec, Micro} = os:timestamp(),
    TwentyFourHoursAgo = Mega * 1000 + Sec - (60 * 60 * 24),
    {NextAction, PromptSeconds, _T, ScheduleStartTime} = 
        take_next_workitem([], Wants,
                            {TwentyFourHoursAgo div 1000,
                                TwentyFourHoursAgo rem 1000, Micro},
                            {1, 8},
                            100),
    ?assertMatch(no_sync, NextAction),
    ?assertMatch(true, ScheduleStartTime > {Mega, Sec, Micro}),
    ?assertMatch(true, PromptSeconds > 0),
    {NextAction, PromptMoreSeconds, _T, ScheduleStartTime} = 
        take_next_workitem([], Wants,
                            {TwentyFourHoursAgo div 1000,
                                TwentyFourHoursAgo rem 1000, Micro},
                            {2, 8},
                            100),
    ?assertMatch(true, PromptMoreSeconds > PromptSeconds),
    {NextAction, PromptEvenMoreSeconds, T, ScheduleStartTime} = 
        take_next_workitem([], Wants,
                            {TwentyFourHoursAgo div 1000,
                                TwentyFourHoursAgo rem 1000, Micro},
                            {7, 8},
                            100),
    ?assertMatch(true, PromptEvenMoreSeconds > PromptMoreSeconds),
    {NextAction, PromptYetMoreSeconds, _T0, ScheduleStartTime} = 
        take_next_workitem(T, Wants, ScheduleStartTime, {1, 8}, 100),
    ?assertMatch(true, PromptYetMoreSeconds > PromptEvenMoreSeconds).



-endif.
