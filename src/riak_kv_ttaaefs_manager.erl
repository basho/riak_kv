%% -------------------------------------------------------------------
%%
%% riak_kv_ttaaefs_manager: coordination of full-sync replication
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
            pause/0,
            resume/0,
            set_sink/3,
            set_queuename/1,
            set_allsync/2,
            set_bucketsync/1,
            enable_ssl/2,
            process_workitem/3]).

-export([set_range/4,
            clear_range/0,
            get_range/0,
            autocheck_suppress/0,
            autocheck_suppress/1]).

-define(SECONDS_IN_DAY, 86400).
-define(INITIAL_TIMEOUT, 60000).
    % Wait a minute before the first allocation is considered,  Lot may be
    % going on at a node immeidately at startup
-define(LOOP_TIMEOUT, 15000).
    % Always wait at least 15s after completing an action before
    % prompting another
-define(CRASH_TIMEOUT, 7200 * 1000).
    % Assume that an exchange has crashed if no response received in this
    % interval, to allow exchanges to be re-scheduled.
-define(EXCHANGE_PAUSE_MS, 1000).
    % Pause between stages of the AAE exchange
-define(MAX_RESULTS, 64).
    % Max number of segments in the AAE tree to be repaired each loop
-define(RANGE_BOOST, 8).
    % Factor to boost range_check max_results values
-define(MEGA, 1000000).
-define(AUTOCHECK_SUPPRESS, 2).
    % How many autocheck checks to suppress following a failure to sync,
    % combines with a failure to find repair work
-define(RANGE_DIVISOR, 2).
    % Reset the range if the number of results for repair are less than
    % MaxResults div ?RANGE_DIVISOR.  Either all the results have been
    % returned, or the range has narrowed and is no longer effective
-define(SCHEDULE_JITTER, 60).
    % Jitter each schedule start time by up to a minute, to avoid overlaps

-record(state, {slice_allocations = [] :: list(allocation()),
                slice_set_start :: erlang:timestamp()|undefined,
                schedule :: schedule_wants()|undefined,
                backup_schedule :: schedule_wants()|undefined,
                peer_ip :: string() | undefined,
                peer_port :: integer() | undefined,
                peer_protocol = pb :: http|pb,
                ssl_credentials :: ssl_credentials() | undefined,
                scope :: bucket|all|disabled,
                bucket_list :: list()|undefined,
                local_nval :: pos_integer()|undefined,
                remote_nval :: pos_integer()|undefined,
                queue_name ::
                    riak_kv_replrtq_src:queue_name() | undefined,
                peer_queue_name ::
                    riak_kv_replrtq_src:queue_name() | disabled,
                slot_info_fun :: slot_info_fun(),
                slice_count :: pos_integer(),
                is_paused = false :: boolean(),
                last_exchange_start = os:timestamp() :: erlang:timestamp(),
                previous_success = os:timestamp() :: false|erlang:timestamp(),
                check_window = always :: check_window(),
                node_info :: node_info()|undefined
                }).

-type req_id() :: no_reply|integer().
-type client_protocol() :: http.
-type client_ip() :: string().
-type client_port() :: pos_integer().
-type nval() :: pos_integer()|range. % Range queries do not have an n-val
-type work_item() ::
    no_check|all_check|day_check|hour_check|range_check|auto_check.
-type work_scope() :: partial|full.
-type schedule_want() :: {work_item(), non_neg_integer()}.
-type slice() :: pos_integer().
-type allocation() :: {slice(), work_item()}.
-type schedule_wants() :: [schedule_want()].
-type node_info() ::
    {pos_integer(), pos_integer(), 1..4}.
    % Node, node count, cluster slice (of 4)
-type ttaaefs_state() :: #state{}.
-type ssl_credentials() :: {string(), string(), string(), string()}.
    %% {cacert_filename, cert_filename, key_filename, username}
-type repair_reference() ::
    {riak_object:bucket(), riak_object:key(), vclock:vclock(), any()}.
-type check_window() :: always|never|{0..23,0..23}.
-type do_repair_fun() ::
    fun((list(repair_reference())) -> list(repair_reference())).
-type repair_summary() ::
    {riak_object:bucket(),
        pos_integer(),
        calendar:datetime(),
        calendar:datetime()}.
-type slot_info_fun() ::
    fun(() -> node_info()).

-export_type([work_item/0]).

%%%============================================================================
%%% API
%%%============================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).
    
%% @doc
%% Override shcedule and process an individual work_item.  If called from
%% riak_client an integer ReqID is passed to allow for a response to be
%% returned.  If using directly from riak attach use no_reply as the request
%% ID.  The fourth element of the input should be an erlang timestamp
%% representing now - but may be altered to something in the past or future in
%% tests.
-spec process_workitem(work_item(), req_id(), erlang:timestamp()) -> ok.
process_workitem(WorkItem, ReqID, Now) ->
    process_workitem(WorkItem, ReqID, self(), Now).

-spec process_workitem(work_item(), req_id(), pid(), erlang:timestamp()) -> ok.
process_workitem(WorkItem, ReqID, From, Now) ->
    gen_server:cast(?MODULE, {WorkItem, ReqID, From, Now}).

%% @doc
%% Pause the management of full-sync from this node 
-spec pause() -> ok|{error, already_paused}.
pause() ->
    gen_server:call(?MODULE, pause).

%% @doc
%% Resume the management of full-sync from this node
-spec resume() -> ok|{error, not_paused}.
resume() ->
    gen_server:call(?MODULE, resume).

%% @doc
%% Define the sink port and address to be used for full-sync
-spec set_sink(http, string(), integer()) -> ok.
set_sink(Protocol, IP, Port) ->
    gen_server:call(?MODULE, {set_sink, Protocol, IP, Port}).

%% @doc
%% Set the queue name to be used for full-sync jobs on this node
-spec set_queuename(riak_kv_replrtq_src:queue_name()) -> ok.
set_queuename(QueueName) ->
    gen_server:call(?MODULE, {set_queuename, QueueName}).

%% @doc
%% Set the manager to do full sync (e.g. using cached trees).  This will leave
%% automated sync disabled.  To re-enable the sync use resume/1
-spec set_allsync(pos_integer(), pos_integer()) -> ok.
set_allsync(LocalNVal, RemoteNVal) ->
    gen_server:call(?MODULE, {set_allsync, LocalNVal, RemoteNVal}).

-spec enable_ssl(boolean(), ssl_credentials() | undefined) -> ok.
enable_ssl(Enable, Credentials) ->
    gen_server:call(?MODULE, {enable_ssl, Enable, Credentials}).

%% @doc
%% Set the manager to sync or a list of buckets.  This will leave
%% automated sync disabled.  To re-enable the sync use resume/1
-spec set_bucketsync(list(riak_object:bucket())) -> ok.
set_bucketsync(BucketList) ->
    gen_server:call(?MODULE, {set_bucketsync, BucketList}).


%%%============================================================================
%%% gen_server callbacks
%%%============================================================================

init([]) ->
    % Get basic coniguration of scope (either all keys for each n_val, or a
    % specific list of buckets)
    Scope = app_helper:get_env(riak_kv, ttaaefs_scope, disabled),
    NoCheck = app_helper:get_env(riak_kv, ttaaefs_nocheck),
    AllCheck = app_helper:get_env(riak_kv, ttaaefs_allcheck),
    HourCheck = app_helper:get_env(riak_kv, ttaaefs_hourcheck),
    DayCheck = app_helper:get_env(riak_kv, ttaaefs_daycheck),
    RangeCheck = app_helper:get_env(riak_kv, ttaaefs_rangecheck),
    AutoCheck = app_helper:get_env(riak_kv, ttaaefs_autocheck),

    {SliceCount, Schedule} = 
        case Scope of
            disabled ->
                {24,
                    [{no_check, 24},
                        {all_check, 0}, 
                        {day_check, 0},
                        {hour_check, 0},
                        {range_check, 0},
                        {auto_check, 0}]};
                    % No sync once an hour if disabled
            _ ->
                {NoCheck + AllCheck + DayCheck
                    + HourCheck + RangeCheck + AutoCheck,
                    [{no_check, NoCheck},
                        {all_check, AllCheck},
                        {day_check, DayCheck},
                        {hour_check, HourCheck},
                        {range_check, RangeCheck},
                        {auto_check, AutoCheck}]}
        end,
    
    CheckWindow =
        app_helper:get_env(riak_kv, ttaaefs_allcheck_window),

    State1 = 
        case Scope of
            all ->
                LocalNVal = app_helper:get_env(riak_kv, ttaaefs_localnval),
                RemoteNVal = app_helper:get_env(riak_kv, ttaaefs_remotenval),
                #state{scope=all,
                        local_nval = LocalNVal,
                        remote_nval = RemoteNVal,
                        schedule = Schedule,
                        slice_count = SliceCount,
                        slot_info_fun = fun get_slotinfo/0};
            bucket ->
                B = app_helper:get_env(riak_kv, ttaaefs_bucketfilter_name),
                T = app_helper:get_env(riak_kv, ttaaefs_bucketfilter_type),
                B0 =
                    case is_binary(B) of 
                        true ->
                            B;
                        false ->
                            list_to_binary(B)
                    end,
                BucketT =
                    case T of
                        "default" ->
                            B0;
                        T when is_binary(T) ->
                            {T, B0};
                        _ ->
                            {list_to_binary(T), B0}
                    end,
                #state{scope=bucket,
                        bucket_list=[BucketT],
                        schedule = Schedule,
                        slice_count = SliceCount,
                        slot_info_fun = fun get_slotinfo/0};
            disabled ->
                #state{scope = disabled,
                        schedule = Schedule,
                        slice_count = SliceCount,
                        slot_info_fun = fun get_slotinfo/0}
        end,
    
    
    % Fetch connectivity information for remote cluster
    PeerIP = app_helper:get_env(riak_kv, ttaaefs_peerip),
    PeerPort = app_helper:get_env(riak_kv, ttaaefs_peerport),
    PeerProtocol = app_helper:get_env(riak_kv, ttaaefs_peerprotocol),
    CaCertificateFilename =
        app_helper:get_env(riak_kv, repl_cacert_filename),
    CertificateFilename =
        app_helper:get_env(riak_kv, repl_cert_filename),
    KeyFilename =
        app_helper:get_env(riak_kv, repl_key_filename),
    SecuritySitename = 
        app_helper:get_env(riak_kv, repl_username),
    SSLEnabled = 
        (CaCertificateFilename =/= undefined) and
        (CertificateFilename =/= undefined) and
        (KeyFilename =/= undefined) and
        (SecuritySitename =/= undefined),
    SSLCredentials =
        case SSLEnabled of
            true ->
                {CaCertificateFilename,
                    CertificateFilename,
                    KeyFilename,
                    SecuritySitename};
            false ->
                undefined
        end,
    
    % Queue name to be used for AAE exchanges on this cluster
    SrcQueueName = app_helper:get_env(riak_kv, ttaaefs_queuename),
    PeerQueueName =
        application:get_env(riak_kv, ttaaefs_queuename_peer, disabled),

    State2 = 
        State1#state{peer_ip = PeerIP,
                        peer_port = PeerPort,
                        peer_protocol = PeerProtocol,
                        ssl_credentials = SSLCredentials,
                        queue_name = SrcQueueName,
                        peer_queue_name = PeerQueueName,
                        check_window = CheckWindow},
    
    lager:info("Initiated Tictac AAE Full-Sync Mgr with scope=~w", [Scope]),
    {ok, State2, ?INITIAL_TIMEOUT}.

handle_call(pause, _From, State) ->
    case State#state.is_paused of
        true ->
            {reply, {error, already_paused}, State};
        false -> 
            PausedSchedule =
                [{no_check, State#state.slice_count},
                    {all_check, 0},
                    {day_check, 0},
                    {hour_check, 0},
                    {range_check, 0},
                    {auto_check, 0}],
            BackupSchedule = State#state.schedule,
            {reply,
                ok,
                State#state{
                    schedule = PausedSchedule,
                    backup_schedule = BackupSchedule,
                    slice_allocations = [],
                    slice_set_start = undefined,
                    is_paused = true},
                ?INITIAL_TIMEOUT}
    end;
handle_call(resume, _From, State) ->
    case State#state.is_paused of
        true ->
            Schedule = State#state.backup_schedule,
            {reply,
                ok,
                State#state{
                    schedule = Schedule,
                    is_paused = false,
                    slice_allocations = [],
                    slice_set_start = undefined},
                ?INITIAL_TIMEOUT};
        false ->
            {reply, {error, not_paused}, State, ?INITIAL_TIMEOUT}
    end;
handle_call({set_sink, Protocol, PeerIP, PeerPort}, _From, State) ->
    State0 = 
        State#state{peer_ip = PeerIP,
                        peer_port = PeerPort,
                        peer_protocol = Protocol},
    {reply, ok, State0, ?INITIAL_TIMEOUT};
handle_call({set_queuename, QueueName}, _From, State) ->
    {reply, ok, State#state{queue_name = QueueName}};
handle_call({set_allsync, LocalNVal, RemoteNVal}, _From, State) ->
    {reply,
        ok,
        State#state{scope = all,
                    local_nval = LocalNVal,
                    remote_nval = RemoteNVal}};
handle_call({enable_ssl, Enable, Credentials}, _From, State) ->
    case Enable of
        true ->
            {reply, ok, State#state{ssl_credentials = Credentials}};
        false ->
            {reply, ok, State#state{ssl_credentials = undefined}}
    end;
handle_call({set_bucketsync, BucketList}, _From, State) ->
    {reply,
        ok,
        State#state{scope = bucket,
                    bucket_list = BucketList}}.

handle_cast({reply_complete, ReqID, Result}, State) ->
    LastExchangeStart = State#state.last_exchange_start,
    Duration = timer:now_diff(os:timestamp(), LastExchangeStart),
    {Pause, State0} = 
        case Result of
            {waiting_all_results, _Deltas} ->
                % If the exchange ends with waiting all results, then consider
                % this to be equivalent to a crash, and so requiring a full
                % pause to backoff
                lager:info("exchange=~w failed to complete in duration=~w s" ++
                                    " sync_state=unknown",
                                [ReqID, Duration div 1000000]),
                riak_kv_stat:update({ttaaefs, sync_fail, Duration}),
                {?CRASH_TIMEOUT, State#state{previous_success = false}};
            {SyncState, 0} when SyncState == root_compare;
                                SyncState == branch_compare ->
                riak_kv_stat:update({ttaaefs, sync_sync, Duration}),
                lager:info("exchange=~w complete result=~w in duration=~w s" ++
                                    " sync_state=true",
                                [ReqID, Result, Duration div 1000000]),
                disable_tree_repairs(),
                {?LOOP_TIMEOUT,
                    State#state{previous_success = LastExchangeStart}};
            _ ->
                lager:info("exchange=~w complete result=~w in duration=~w s" ++
                                    " sync_state=false",
                                [ReqID, Result, Duration div 1000000]),
                riak_kv_stat:update({ttaaefs, sync_nosync, Duration}),
                % If exchanges start slowing, then start increasing the pauses
                % Gradually degrade in response to an increased workload
                {max(?LOOP_TIMEOUT, (Duration div 1000) div 2),
                    State#state{previous_success = false}}
        end,
    {noreply, State0, Pause};
handle_cast({no_check, ReqID, From, _}, State) ->
    case ReqID of
        no_reply ->
            ok;
        _ ->
            From ! {ReqID, {no_check, 0}}
    end,
    {noreply, State, ?LOOP_TIMEOUT};
handle_cast({all_check, ReqID, From, _Now}, State) ->
    {LNVal, RNVal, Filter, NextBucketList, Ref} =
        case State#state.scope of
            all ->
                {State#state.local_nval, State#state.remote_nval,
                    none, undefined, full};
            bucket ->
                [H|T] = State#state.bucket_list,
                {range, range, 
                    {filter, H, all, large, all, all, pre_hash},
                    T ++ [H],
                    partial}
        end,
    {State0, Timeout} =
        sync_clusters(From, ReqID, LNVal, RNVal, Filter,
                        NextBucketList, Ref, State,
                        all_check),
    {noreply, State0, Timeout};
handle_cast({day_check, ReqID, From, Now}, State) ->
    {MegaSecs, Secs, _MicroSecs} = Now,
    UpperTime = MegaSecs * ?MEGA  + Secs,
    LowerTime = UpperTime - 60 * 60 * 24,
    case State#state.scope of
        all ->
            Filter =
                {filter, all, all, large, all,
                {LowerTime, UpperTime}, pre_hash},
            {State0, Timeout} =
                sync_clusters(From,
                                ReqID,
                                State#state.local_nval,
                                State#state.remote_nval,
                                Filter,
                                undefined, 
                                full,
                                State,
                                day_check),
            {noreply, State0, Timeout};
        bucket ->
            [H|T] = State#state.bucket_list,
            % Note that the tree size is amended as well as the time range.
            % The bigger the time range, the bigger the tree.  Bigger trees
            % are less efficient when there is little change, but can more
            % accurately reflect bigger changes (with less false positives).
            Filter =
                {filter, H, all, medium, all,
                {LowerTime, UpperTime}, pre_hash},
            NextBucketList = T ++ [H],
            {State0, Timeout} =
                sync_clusters(From, ReqID, range, range, Filter,
                                NextBucketList, partial, State,
                                day_check),
            {noreply, State0, Timeout}
    end;
handle_cast({hour_check, ReqID, From, Now}, State) ->
    {MegaSecs, Secs, _MicroSecs} = Now,
    UpperTime = MegaSecs * ?MEGA  + Secs,
    LowerTime = UpperTime - 60 * 60,
    case State#state.scope of
        all ->
            Filter =
                {filter, all, all, large, all,
                {LowerTime, UpperTime}, pre_hash},
            {State0, Timeout} =
                sync_clusters(From,
                                ReqID,
                                State#state.local_nval,
                                State#state.remote_nval,
                                Filter,
                                undefined, 
                                full,
                                State,
                                hour_check),
            {noreply, State0, Timeout};
        bucket ->
            [H|T] = State#state.bucket_list,
            
            % Note that the tree size is amended as well as the time range.
            % The bigger the time range, the bigger the tree.  Bigger trees
            % are less efficient when there is little change, but can more
            % accurately reflect bigger changes (with less false positives).
            Filter =
                {filter, H, all, small, all,
                {LowerTime, UpperTime}, pre_hash},
            NextBucketList = T ++ [H],
            {State0, Timeout} =
                sync_clusters(From, ReqID, range, range, Filter,
                                NextBucketList, partial, State,
                                hour_check),
            {noreply, State0, Timeout}
    end;
handle_cast({range_check, ReqID, From, _Now}, State) ->
    Range =
        case get_range() of
            none ->
                case State#state.previous_success of
                    false ->
                        lager:info("No range sync as no range set"),
                        none;
                    {PrevMega, PrevSecs, _PrevMS} ->
                        % Add to the high time, and arbitrary 5s, as the last
                        % success time will be a timestamp set within
                        % sync_clusters/9 function, and it is preferable to
                        % ensure this query went beyond that time so that there
                        % should not be a gap between this queries high time,
                        % and the low time of the next
                        {MegaSecs, Secs, _MicroSecs} = os:timestamp(),
                        NowSecs = MegaSecs * ?MEGA  + Secs + 5,
                        {all,
                            all,
                            PrevMega * ?MEGA + PrevSecs,
                            NowSecs}
                end;
            SetRange ->
                SetRange
        end,
    case Range of
        none ->
            case ReqID of
                no_reply ->
                    ok;
                _ ->
                    From ! {ReqID, {range_check, 0}}
            end,
            {noreply, State, ?LOOP_TIMEOUT};
        {Bucket, KeyRange, LowerTime, UpperTime} ->
            clear_range(),
            case State#state.scope of
                all ->
                    Filter =
                        {filter, Bucket, all, large, all,
                        {LowerTime, UpperTime}, pre_hash},
                    {State0, Timeout} =
                        sync_clusters(From,
                                        ReqID,
                                        State#state.local_nval,
                                        State#state.remote_nval,
                                        Filter,
                                        undefined, 
                                        full,
                                        State,
                                        range_check),
                    {noreply, State0, Timeout};
                bucket ->
                    {B, NextBucketList} =
                        case Bucket of
                            all ->
                                [H|T] = State#state.bucket_list,
                                {H, T ++ [H]};
                            Bucket ->
                                {Bucket, State#state.bucket_list}
                        end,
                    Filter =
                        {filter, B, KeyRange, small, all,
                        {LowerTime, UpperTime}, pre_hash},
                    {State0, Timeout} =
                        sync_clusters(From, ReqID, range, range, Filter,
                                        NextBucketList, partial, State,
                                        range_check),
                    {noreply, State0, Timeout}
            end
    end;
handle_cast({auto_check, ReqID, From, Now}, State) ->
    case {get_range(),
            State#state.previous_success,
            in_window(Now, State#state.check_window),
            drop_next_autocheck(),
            State#state.peer_queue_name} of
        {none, false, _, true, disabled} ->
            % As there is no peer queue defined, this manager cannot repair
            % discovered differences when the remote cluster is in advance
            % of this cluster.  This skips checks as previous discovery work
            % has gone to waste
            lager:info(
                "Auto check prompts no_check reqid=~w as sink ahead",
                [ReqID]),
            process_workitem(no_check, ReqID, From, Now);
        {none, false, true, _DN, _QN} ->
            lager:info("Auto check prompts all_check reqid=~w", [ReqID]),
            process_workitem(all_check, ReqID, From, Now);
        {none, false, false, _DN, _QN} ->
            lager:info("Auto check prompts day_check reqid=~w", [ReqID]),
            process_workitem(day_check, ReqID, From, Now);
        Clause ->
            % Whenever there is a range defined of the last check was
            % successful, a range check is the optimal way of proceeding
            lager:info(
                "Auto check prompts range_check reqid=~w due to clause ~p",
                [ReqID, Clause]),
            process_workitem(range_check, ReqID, From, Now)
    end,
    {noreply, State, timeout}.


handle_info(timeout, State) ->
    SlotInfoFun = State#state.slot_info_fun,
    SlotInfo = SlotInfoFun(),
    {Allocations, StartTime} =
        case State#state.node_info of
            undefined ->
                lager:info(
                    "Initiating schedule at startup SlotInfo ~p Schedule ~p",
                    [SlotInfo, State#state.schedule]),
                {[], undefined};
            SlotInfo ->
                {State#state.slice_allocations, State#state.slice_set_start};
            OldInfo ->
                lager:info(
                    "SlotInfo changed from ~p to ~p so resetting schedule",
                    [OldInfo, SlotInfo]),
                {[], undefined}
        end,
    {WorkItem, Wait, RemainingSlices, ScheduleStartTime} = 
        take_next_workitem(
            Allocations,
            State#state.schedule,
            StartTime,
            SlotInfo,
            State#state.slice_count),
    case ?SECONDS_IN_DAY div State#state.slice_count of
        SlotTimeLength when SlotTimeLength < Wait ->
            % It is possible for multiple work items to be looping, so
            % don't trigger unless it appears to be within the next slice.
            {noreply, State, SlotTimeLength};
        _ ->
            lager:info(
                "Sending work_item=~w msg in ~w seconds ~w more items in loop",
                [WorkItem, Wait, length(RemainingSlices)]),
            erlang:send_after(Wait * 1000, self(), {work_item, WorkItem}),
            {noreply,
                State#state{
                    slice_allocations = RemainingSlices,
                    slice_set_start = ScheduleStartTime,
                    node_info = SlotInfo}}
    end;
handle_info({work_item, WorkItem}, State) ->
    case State#state.is_paused of
        true when WorkItem =/= no_check ->
            process_workitem(no_check, no_reply, os:timestamp());
        _ ->
            process_workitem(WorkItem, no_reply, os:timestamp())
    end,
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%============================================================================
%%% Environment state management functions
%%%============================================================================

%% @doc
%% Set a specific range to be the target for subsequent range_check queries
-spec set_range(riak_object:bucket()|all,
                        {riak_object:key(), riak_object:key()}|all,
                        calendar:datetime(),
                        calendar:datetime())
                    -> ok.
set_range(Bucket, KeyRange, LowDate, HighDate) ->
    EpochTime =
        calendar:datetime_to_gregorian_seconds({{1970,1,1},{0,0,0}}),
    LowTS = 
        calendar:datetime_to_gregorian_seconds(LowDate) - EpochTime,
    HighTS =
        calendar:datetime_to_gregorian_seconds(HighDate) - EpochTime,
    true = HighTS >= LowTS,
    true = LowTS > 0,
    application:set_env(riak_kv, ttaaefs_check_range,
                        {Bucket, KeyRange, LowTS - 1, HighTS + 1}).

-spec clear_range() -> ok.
clear_range() ->
    application:set_env(riak_kv, ttaaefs_check_range, none).

-spec get_range() ->
        none|{riak_object:bucket()|all, 
                {riak_object:key(), riak_object:key()}|all,
                pos_integer(), pos_integer()}.
get_range() ->
    application:get_env(riak_kv, ttaaefs_check_range, none).


-spec autocheck_suppress() -> ok.
autocheck_suppress() ->
    autocheck_suppress(
        application:get_env(
            riak_kv,
            ttaaefs_autocheck_suppress_count,
            ?AUTOCHECK_SUPPRESS)).

-spec autocheck_suppress(non_neg_integer()) -> ok.
autocheck_suppress(SuppressCount) ->
    application:set_env(riak_kv, ttaaefs_autocheck_dropnext, SuppressCount).

-spec drop_next_autocheck() -> boolean().
drop_next_autocheck() ->
    case application:get_env(riak_kv, ttaaefs_autocheck_dropnext, 0) of
        SC when SC > 0 ->
            autocheck_suppress(SC - 1),
            true;
        _ ->
            false
    end.

-spec trigger_tree_repairs() -> ok.
trigger_tree_repairs() ->
    lager:info(
        "Setting node to repair trees as unsync'd all_check had no repairs"),
    application:set_env(riak_kv, aae_fetchclocks_repair, true).

-spec disable_tree_repairs() -> ok.
disable_tree_repairs() ->
    case application:get_env(riak_kv, aae_fetchclocks_repair_force, false) of
        true ->
            ok;
        false ->
            application:set_env(riak_kv, aae_fetchclocks_repair, false)
    end.

%%%============================================================================
%%% Internal functions
%%%============================================================================

%% @doc
%% Sync two clusters - return an updated loop state and a timeout
-spec sync_clusters(pid(), integer()|no_reply,
                nval(), nval(), tuple(), list()|undefined, work_scope(),
                ttaaefs_state(),
                work_item()) ->
                    {ttaaefs_state(), pos_integer()}.
sync_clusters(From, ReqID, LNVal, RNVal, Filter, NextBucketList,
                Ref, State, WorkType) ->
    {RemoteClient, RemoteMod} =
        init_client(State#state.peer_protocol,
                    State#state.peer_ip,
                    State#state.peer_port,
                    State#state.ssl_credentials),

    case RemoteClient of
        no_client ->
            {State#state{bucket_list = NextBucketList},
                ?LOOP_TIMEOUT};
        _ ->
            StopFun = fun() -> stop_client(RemoteClient, RemoteMod) end,
            RemoteSendFun = generate_sendfun({RemoteClient, RemoteMod}, RNVal),
            LocalSendFun = generate_sendfun(local, LNVal),
            ReqID0 = 
                case ReqID of
                    no_reply ->
                        erlang:phash2({self(), os:timestamp()});
                    _ ->
                        ReqID
                end,
            ReplyFun =
                generate_replyfun(ReqID == no_reply, ReqID0, From, StopFun),
            
            MaxResults =
                case WorkType of
                    range_check ->
                        RB = app_helper:get_env(riak_kv,
                                                ttaaefs_rangeboost,
                                                ?RANGE_BOOST),
                        MR = app_helper:get_env(riak_kv,
                                                ttaaefs_maxresults,
                                                ?MAX_RESULTS),
                        RB * MR;
                    _ ->
                        app_helper:get_env(riak_kv,
                                            ttaaefs_maxresults,
                                            ?MAX_RESULTS)
                end,
            
            LocalRepairFun =
                fun(RepairList) ->
                    riak_kv_replrtq_src:replrtq_ttaaefs(
                        State#state.queue_name,
                        RepairList),
                    RepairList
                end,
            RemoteRepairFun =
                case State#state.peer_queue_name of
                    disabled ->
                        fun(_RepairList) -> [] end;
                    PeerQueueName ->
                        {PeerClient, PeerMod} =
                            init_client(State#state.peer_protocol,
                                        State#state.peer_ip,
                                        State#state.peer_port,
                                        State#state.ssl_credentials),
                        EncodeClockFun =
                            encode_clock_fun(State#state.peer_protocol),
                        EncodeKeyClockFun =
                            fun({B, K, C, to_fetch}) ->
                                {B, K, EncodeClockFun(C)}
                            end,
                        fun(RepairList) ->
                            PeerMod:push(
                                PeerClient,
                                atom_to_binary(PeerQueueName, utf8),
                                lists:map(
                                    EncodeKeyClockFun,
                                    RepairList)),
                            stop_client(PeerClient, PeerMod),
                            RepairList
                        end
                end,

            RepairFun =
                generate_repairfun(
                    LocalRepairFun,
                    RemoteRepairFun,
                    MaxResults,
                    {ReqID0, Ref, WorkType}),

            ExchangePause =
                app_helper:get_env(riak_kv,
                                    tictacaae_exchangepause,
                                    ?EXCHANGE_PAUSE_MS),
            {ok, ExPid, ExID} =
                aae_exchange:start(Ref,
                                    [{LocalSendFun, all}],
                                    [{RemoteSendFun, all}],
                                    RepairFun,
                                    ReplyFun,
                                    Filter, 
                                    [{transition_pause_ms, ExchangePause},
                                        {max_results, MaxResults},
                                        {scan_timeout, ?CRASH_TIMEOUT div 2},
                                        {purpose, WorkType}]),
            
            lager:info("Starting ~w full-sync work_item=~w " ++ 
                                "reqid=~w exchange id=~s pid=~w",
                            [Ref, WorkType, ReqID0, ExID, ExPid]),
            riak_kv_stat:update({ttaaefs, WorkType}),
            
            {State#state{bucket_list = NextBucketList,
                            last_exchange_start = os:timestamp()},
                ?CRASH_TIMEOUT}
    end.


encode_clock_fun(http) ->
    fun(C) ->
        base64:encode_to_string(riak_object:encode_vclock(C))
    end;
encode_clock_fun(pb) ->
    fun(C) ->
        riak_object:encode_vclock(C)
    end.

%% @doc
%% Check the slot info - how many nodes are there active in the cluster, and
%% which is the slot for this node in the cluster.
%% An alternative slot_info function may be passed in when initialising the
%% server (e.g. for test)
-spec get_slotinfo() -> node_info().
get_slotinfo() ->
    UpNodes = lists:sort(riak_core_node_watcher:nodes(riak_kv)),
    UpNodes0 =
        case lists:member(node(), UpNodes) of
            true ->
                UpNodes;
            false ->
                %% Assume we will eventually come up!
                lists:sort([node()|UpNodes])
        end,
    NotMe = lists:takewhile(fun(N) -> N /= node() end, UpNodes0),
    ClusterSlice = 
        max(min(app_helper:get_env(riak_kv, ttaaefs_cluster_slice, 1), 4), 1),
    {length(NotMe) + 1, length(UpNodes0), ClusterSlice}.

%% @doc
%% Return a function which will send aae_exchange messages to a remote
%% cluster, and return the response.  The function should make an async call
%% to try and make the remote and local cluster sends happen as close to
%% parallel as possible. 
-spec generate_sendfun({rhc:rhc(), rhc}|{pid(), riakc_pb_socket}|local,
                        nval()) -> aae_exchange:send_fun().
generate_sendfun(SendClient, NVal) ->
    fun(Msg, all, Colour) ->
        AAE_Exchange = self(),
        ReturnFun = 
            fun(R) -> 
                aae_exchange:reply(AAE_Exchange, R, Colour)
            end,
        SendFun = 
            case SendClient of
                local ->
                    C = riak_client:new(node(), undefined),
                    local_sender(Msg, C, ReturnFun, NVal);
                {Client, Mod} ->
                    remote_sender(Msg, Client, Mod, ReturnFun, NVal)
            end,
        _SpawnedPid = spawn(SendFun),
        ok
    end.

%% @doc
%% Make a remote client for connecting to the remote cluster
-spec init_client(client_protocol(), client_ip(), client_port(),
                    ssl_credentials()|undefined)
                    -> {rhc:rhc()|no_client, rhc}|
                        {pid()|no_client, riakc_pb_socket}.
init_client(http, IP, Port, _Cert) ->
    RHC = rhc:create(IP, Port, "riak", []),
    case rhc:ping(RHC) of
        ok ->
            {RHC, rhc};
        {error, Error} ->
            lager:warning("Cannot reach remote cluster ~p ~p with error ~p",
                            [IP, Port, Error]),
            {no_client, rhc}
    end;
init_client(pb, IP, Port, undefined) ->
    Options = [{auto_reconnect, true}],
    init_pbclient(IP, Port, Options);
init_client(pb, IP, Port, Credentials) ->
    SecurityOpts = 
        [{cacertfile, element(1, Credentials)},
            {certfile, element(2, Credentials)},
            {keyfile, element(3, Credentials)},
            {credentials, element(4, Credentials), ""}],
    Options = [{auto_reconnect, true}|SecurityOpts],
    init_pbclient(IP, Port, Options).

%% @doc
%% Stop the client (if PBC), nothing started for RHC.
-spec stop_client(rhc:rhc()|pid(), rhc|riak_c_pb_socket) -> ok.
stop_client(_RemoteClient, rhc) ->
    ok;
stop_client(RemoteClient, Mod) ->
    Mod:stop(RemoteClient).

init_pbclient(IP, Port, Options) ->
    {ok, Pid} = riakc_pb_socket:start_link(IP, Port, Options),
    try riakc_pb_socket:ping(Pid) of
        pong ->
            {Pid, riakc_pb_socket};
        {error, Reason} ->
            lager:info("Cannot reach remote cluster ~p ~p as ~p",
                            [IP, Port, Reason]),
            {no_client, riakc_pb_socket}
    catch 
        _Exception:Reason ->
            lager:warning("Cannot reach remote cluster ~p ~p exception ~p",
                            [IP, Port, Reason]),
            {no_client, riakc_pb_socket}
    end.

-spec local_sender(any(), riak_client:riak_client(), fun((any()) -> ok), nval())
                                                             -> fun(() -> ok).
local_sender(fetch_root, C, ReturnFun, NVal) ->
    run_localfold({merge_root_nval, NVal}, C, ReturnFun);
local_sender({fetch_branches, BranchIDs}, C, ReturnFun, NVal) ->
    run_localfold({merge_branch_nval, NVal, BranchIDs}, C, ReturnFun);
local_sender({fetch_clocks, SegmentIDs}, C, ReturnFun, NVal) ->
    run_localfold({fetch_clocks_nval, NVal, SegmentIDs}, C, ReturnFun);
local_sender({fetch_clocks, SegmentIDs, MR}, C, ReturnFun, NVal) ->
    %% riak_client expects modified range of form
    %% {date, non_neg_integer(), non_neg_integer()}
    %% where as the riak erlang clients just expect 
    %% {non_neg_integer(), non_neg_integer()}
    %% They keyword all must also be supported
    LMR = localise_modrange(MR),
    run_localfold({fetch_clocks_nval, NVal, SegmentIDs, LMR}, C, ReturnFun);
local_sender({merge_tree_range, B, KR, TS, SF, MR, HM}, C, ReturnFun, range) ->
    LMR = localise_modrange(MR),
    run_localfold({merge_tree_range, B, KR, TS, SF, LMR, HM}, C, ReturnFun);
local_sender({fetch_clocks_range, B0, KR, SF, MR}, C, ReturnFun, _NVal) ->
    LMR = localise_modrange(MR),
    run_localfold({fetch_clocks_range, B0, KR, SF, LMR}, C, ReturnFun).


-spec run_localfold(riak_kv_clusteraae_fsm:query_definition(),
                        riak_client:riak_client(),
                        fun((any()) -> ok)) -> 
                            fun(() -> ok).
run_localfold(Query, Client, ReturnFun) ->
    fun() ->
        case riak_client:aae_fold(Query, Client) of
            {ok, R} -> 
                ReturnFun(R);
            {error, Error} ->
                ReturnFun({error, Error})
        end
    end.

localise_modrange(all) ->
    all;
localise_modrange({LowTime, HighTime}) ->
    {date, LowTime, HighTime}.


%% @doc
%% Translate aae_Exchange messages into riak erlang http client requests
-spec remote_sender(any(), rhc:rhc()|pid(), module(), fun((any()) -> ok), nval())
                                                            -> fun(() -> ok).
remote_sender(fetch_root, Client, Mod, ReturnFun, NVal) ->
    fun() ->
        case Mod:aae_merge_root(Client, NVal) of
            {ok, {root, Root}} ->
                ReturnFun(Root);
            {error, Error} ->
                lager:warning("Error of ~w in root request", [Error]),
                ReturnFun({error, Error})
        end
    end;
remote_sender({fetch_branches, BranchIDs}, Client, Mod, ReturnFun, NVal) ->
    fun() ->
        case Mod:aae_merge_branches(Client, NVal, BranchIDs) of
            {ok, {branches, ListOfBranchResults}} ->
                ReturnFun(ListOfBranchResults);
            {error, Error} ->
                lager:warning("Error of ~w in branches request", [Error]),
                ReturnFun({error, Error})
        end
    end;
remote_sender({fetch_clocks, SegmentIDs}, Client, Mod, ReturnFun, NVal) ->
    fun() ->
        case Mod:aae_fetch_clocks(Client, NVal, SegmentIDs) of
            {ok, {keysclocks, KeysClocks}} ->
                ReturnFun(lists:map(fun remote_decode/1, KeysClocks));
            {error, Error} ->
                lager:warning("Error of ~w in clocks request", [Error]),
                ReturnFun({error, Error})
        end
    end;
remote_sender({fetch_clocks, SegmentIDs, MR}, Client, Mod, ReturnFun, NVal) ->
    fun() ->
        case Mod:aae_fetch_clocks(Client, NVal, SegmentIDs, MR) of
            {ok, {keysclocks, KeysClocks}} ->
                ReturnFun(lists:map(fun remote_decode/1, KeysClocks));
            {error, Error} ->
                lager:warning("Error of ~w in clocks request", [Error]),
                ReturnFun({error, Error})
        end
    end;
remote_sender({merge_tree_range, B, KR, TS, SF, MR, HM},
                    Client, Mod, ReturnFun, range) ->
    SF0 = format_segment_filter(SF),
    fun() ->
        case Mod:aae_range_tree(Client, B, KR, TS, SF0, MR, HM) of
            {ok, {tree, Tree}} ->
                ReturnFun(leveled_tictac:import_tree(Tree));
            {error, Error} ->
                lager:warning("Error of ~w in tree request", [Error]),
                ReturnFun({error, Error})
        end
    end;
remote_sender({fetch_clocks_range, B0, KR, SF, MR},
                    Client, Mod, ReturnFun, _NVal) ->
    SF0 = format_segment_filter(SF),
    fun() ->
        case Mod:aae_range_clocks(Client, B0, KR, SF0, MR) of
            {ok, {keysclocks, KeysClocks}} ->
                ReturnFun(lists:map(fun remote_decode/1, KeysClocks));
            {error, Error} ->
                lager:warning("Error of ~w in segment request", [Error]),
                ReturnFun({error, Error})
        end
    end.

-spec remote_decode({{riak_object:bucket(), riak_object:key()}, binary()}) ->
                    {riak_object:bucket(), riak_object:key(), vclock:vclock()}.
remote_decode({{B, K}, VC}) ->
    {B, K, decode_clock(VC)}.


%% @doc
%% The segment filter as produced by aae_exchange has a different format to
%% thann the one expected by the riak http erlang client - so that conflict
%% is resolved here
format_segment_filter(all) ->
    all;
format_segment_filter({segments, SegList, TreeSize}) ->
    {SegList, TreeSize}.

%% @doc
%% Generate a reply fun (as there is nothing to reply to this will simply
%% update the stats for Tictac AAE full-syncs
-spec generate_replyfun(boolean(), integer(), pid(), fun(() -> ok))
                                                -> aae_exchange:reply_fun().
generate_replyfun(Clientless, ReqID, From, StopClientFun) ->
    fun(Result) ->
        case Clientless of
            true ->
                ok;
            _ ->
                % Reply to riak_client
                From ! {ReqID, Result}
        end,
        gen_server:cast(?MODULE, {reply_complete, ReqID, Result}),    
        StopClientFun()
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
-spec generate_repairfun(do_repair_fun(),
                            do_repair_fun(),
                            non_neg_integer(),
                            {non_neg_integer(), work_scope(), work_item()})
                        -> aae_exchange:repair_fun().
generate_repairfun(LocalRepairFun, RemoteRepairFun, MaxResults, LogInfo) ->
    LogRepairs = app_helper:get_env(riak_kv, ttaaefs_logrepairs, false),
    {ExchangeID, WorkScope, WorkItem} = LogInfo,
    fun(RepairList) ->
        FoldFun =
            fun({{B, K}, {SrcVC, SnkVC}}, {SourceL, SinkL}) ->
                case vclock_equal(SrcVC, SnkVC) of
                    true ->
                        {SourceL, SinkL};
                    false ->
                        maybe_log_repair(LogRepairs, {B, K, SrcVC, SnkVC}),
                        case vclock_dominates(SnkVC, SrcVC) of
                            true ->
                                {SourceL, [{B, K, SnkVC, to_fetch}|SinkL]};
                            false ->
                                {[{B, K, SrcVC, to_fetch}|SourceL], SinkL}
                        end
                end
            end,
        {SrcRepair, SnkRepair} = lists:foldl(FoldFun, {[], []}, RepairList),
        lager:info(
            "AAE reqid=~w work_item=~w scope=~w shows sink ahead " ++
                "for key_count=~w keys limited by max_results=~w", 
            [ExchangeID, WorkItem, WorkScope, length(SnkRepair), MaxResults]),
        lager:info(
            "AAE reqid=~w work_item=~w scope=~w shows source ahead " ++
                "for key_count=~w keys limited by max_results=~w", 
            [ExchangeID, WorkItem, WorkScope, length(SrcRepair), MaxResults]),
        riak_kv_stat:update({ttaaefs, snk_ahead, length(SnkRepair)}),
        riak_kv_stat:update({ttaaefs, src_ahead, length(SrcRepair)}),
        AllRepairs = LocalRepairFun(SrcRepair) ++ RemoteRepairFun(SnkRepair),
        PBDL = summarise_repairs(ExchangeID, AllRepairs, WorkScope, WorkItem),
        determine_next_action(
            length(AllRepairs),
            MaxResults div ?RANGE_DIVISOR,
            WorkScope,
            WorkItem,
            PBDL)
    end.


%% @doc Examine the number of repairs, and the repair summary and determine
%% what to do next e.g. set a range for the next range_check 
-spec determine_next_action(
    non_neg_integer(), 
    pos_integer(),
    work_scope(), work_item(),
    list(repair_summary())) -> ok.
determine_next_action(0, _Target, full, WorkItem, _RepairRanges) ->
    % Only do this if the scope is full, otherwise may suppress checks for
    % other buckets where those checks could have been successful.
    % This action is in support of environments where bi-directional repair
    % is not enabled (i.e. the peer queue_name is set to disabled).
    lager:info(
        "Suppressing auto_checks as no repairs for work_item=~w",
        [WorkItem]),
    case WorkItem of
        all_check -> trigger_tree_repairs();
        _ -> ok
    end,
    autocheck_suppress();
determine_next_action(
    RepairCount, Target, _Scope, WorkItem, [{B, KC, LowDT, HighDT}])
        when RepairCount > Target ->
    lager:info(
        "Setting range to bucket=~p ~w ~w last_count=~w set by work_item=~w",
        [B, LowDT, HighDT, KC, WorkItem]),
    set_range(B, all, LowDT, HighDT);
determine_next_action(
    RepairCount, Target, _Scope, WorkItem, RepairRanges)
        when RepairCount > Target ->
    MapFun = fun({_B, _KC, LD, HD}) -> {LD, HD} end,
    {LDL, HDL} =
        lists:unzip(lists:map(MapFun, RepairRanges)),
    LoDT = lists:min(LDL),
    HiDT = lists:max(HDL),
    lager:info(
        "Setting range to bucket=all ~w ~w last_count=~w set by work_item=~w",
        [LoDT, HiDT, RepairCount, WorkItem]),
    set_range(all, all, LoDT, HiDT);
determine_next_action(RepairCount, _Target, _Scope, WorkItem, _RepairRanges) ->
    lager:info(
        "No range set for last_count=~w repairs by work_item=~w",
        [RepairCount, WorkItem]),
    ok.

maybe_log_repair(false, _) ->
    ok;
maybe_log_repair(true, {B, K, SrcVC, SnkVC}) ->
    lager:info(
        "Repair B=~p K=~p SrcVC=~w SnkVC=~w",
        [B, K, SrcVC, SnkVC]).

vclock_dominates(none, _SrcVC)  ->
    false;
vclock_dominates(_SinkVC, none) ->
    true;
vclock_dominates(SinkVC, SrcVC) ->
    vclock:dominates(SinkVC, SrcVC).

vclock_equal(none, _VC1) ->
    false;
vclock_equal(_VC0, none) ->
    false;
vclock_equal(VC0, VC1) ->
    vclock:equal(VC0, VC1).

decode_clock(none) ->
    none;
decode_clock(EncodedClock) ->
    riak_object:decode_vclock(EncodedClock).


%% @doc Summarise the repairs by bucket and modified rnane
-spec summarise_repairs(integer(),
                        list(repair_reference()),
                        work_scope(),
                        work_item()) -> 
                            list(repair_summary()).
summarise_repairs(ExchangeID, RepairList, WorkScope, WorkItem) ->
    FoldFun =
        fun({B, _K, SrcVC, _Action}, Acc) ->
            LMD = vclock:last_modified(SrcVC),
            case lists:keyfind(B, 1, Acc) of
                {B, C, LowMD, HighMD} ->
                    UpdTuple = {B, C + 1, min(LMD, LowMD), max(LMD, HighMD)},
                    lists:keyreplace(B, 1, Acc, UpdTuple);
                _ ->
                    [{B, 1, LMD, LMD}|Acc]
            end
        end,
    PerBucketData = lists:foldl(FoldFun, [], RepairList),
    LogFun =
        fun({B, C, MinDT, MaxDT}) ->
            lager:info(
                "AAE exchange=~w work_item=~w type=~w repaired " ++ 
                    "key_count=~w for bucket=~p with low date ~p high date ~p",
                [ExchangeID, WorkScope, WorkItem, C, B, MinDT, MaxDT])
        end,
    lists:foreach(LogFun, PerBucketData),
    PerBucketData.

%% @doc
%% Take the next work item from the list of allocations, assuming that the
%% starting time for that work item has not alreasy passed.  If there are no
%% more items queue, start a new queue based on the wants for the schedule.
-spec take_next_workitem(list(allocation()), 
                            schedule_wants(),
                            erlang:timestamp()|undefined,
                            node_info(),
                            pos_integer()) ->
                                {work_item(), pos_integer(),
                                    list(allocation()), erlang:timestamp()}.
take_next_workitem([], Wants, ScheduleStartTime, SlotInfo, SliceCount) ->
    NewAllocations = choose_schedule(Wants),
    % Should be 24 hours after ScheduleStartTime - so add 24 hours to
    % ScheduleStartTime
    RevisedStartTime = 
        case ScheduleStartTime of
            undefined ->
                beginning_of_next_hour(os:timestamp());
            {Mega, Sec, _Micro} ->
                Seconds = Mega * ?MEGA + Sec + 86400,
                {Seconds div ?MEGA, Seconds rem ?MEGA, 0}
        end,
    take_next_workitem(NewAllocations, Wants,
                        RevisedStartTime, SlotInfo, SliceCount);
take_next_workitem([NextAlloc|T], Wants,
                        ScheduleStartTime, SlotInfo, SliceCount) ->
    {NodeNumber, NodeCount, ClusterSlice} = SlotInfo,
    {SliceNumber, NextAction} = NextAlloc,
    SecsFromStartTime =
        schedule_seconds(
            NodeNumber,
            SliceNumber,
            ClusterSlice,
            NodeCount,
            SliceCount),
    {Mega, Sec, _Micro} = ScheduleStartTime,
    ScheduleSeconds =
        Mega * ?MEGA
     + Sec + SecsFromStartTime
     + rand:uniform(?SCHEDULE_JITTER),
    {MegaNow, SecNow, _MicroNow} = os:timestamp(),
    NowSeconds = MegaNow * ?MEGA + SecNow,
    case ScheduleSeconds > NowSeconds of
        true ->
            {NextAction, ScheduleSeconds - NowSeconds, T, ScheduleStartTime};
        false ->
            lager:info("Tictac AAE skipping action ~w as manager running "
                        ++ "~w seconds late",
                        [NextAction, NowSeconds - ScheduleSeconds]),
            lager:info("Clearing any range due to skip to reduce load"),
            clear_range(),
            take_next_workitem(T, Wants,
                                ScheduleStartTime, SlotInfo, SliceCount)
    end.

-spec schedule_seconds(
        pos_integer(), pos_integer(), 1..4, pos_integer(), pos_integer())
            -> non_neg_integer().
schedule_seconds(
        NodeNumber, SliceNumber, ClusterSliceNumber, NodeCount, SliceCount) ->
    SliceSeconds = ?SECONDS_IN_DAY div max(SliceCount, 1),
    NodeSliceSeconds = SliceSeconds div max(NodeCount, 1),
    ClusterSliceSeconds = NodeSliceSeconds div 4,
    SlotSeconds =
        (NodeNumber - 1) * NodeSliceSeconds
        + (ClusterSliceNumber - 1) * ClusterSliceSeconds,
    SlotSeconds + (SliceNumber - 1) * SliceSeconds.


-spec beginning_of_next_hour(erlang:timestamp()) -> erlang:timestamp().
beginning_of_next_hour({Mega, Sec, _Micro}) ->
    {{Y, Mo, D}, {H, Min, S}} = calendar:now_to_datetime({Mega, Sec, 0}),
    NowGS =
        calendar:datetime_to_gregorian_seconds({{Y, Mo, D}, {H, Min, S}}),
    TopOfHourGS =
        calendar:datetime_to_gregorian_seconds({{Y, Mo, D}, {H, 0, 0}}),
    NextHourGS = TopOfHourGS + 60 * 60,
    EpochSeconds = Mega * ?MEGA + Sec + NextHourGS - NowGS,
    {EpochSeconds div ?MEGA, EpochSeconds rem ?MEGA, 0}.


%% @doc
%% Calculate an allocation of activity for the next 24 hours based on the
%% configured schedule-needs.
-spec choose_schedule(schedule_wants()) -> list(allocation()).
choose_schedule(ScheduleWants) ->
    [{all_check, AllCheck},
        {auto_check, AutoCheck},
        {day_check, DayCheck},
        {hour_check, HourCheck},
        {no_check, NoCheck},
        {range_check, RangeCheck}] = lists:sort(ScheduleWants),
    SliceCount =
        NoCheck + AllCheck + DayCheck + HourCheck + RangeCheck + AutoCheck,
    Slices = lists:seq(1, SliceCount),
    Allocations = [],
    lists:sort(
        choose_schedule(Slices,
            Allocations,
            {NoCheck, AllCheck, DayCheck, HourCheck, RangeCheck, AutoCheck})).

-spec choose_schedule(list(pos_integer()),
                        list({pos_integer(), work_item()}),
                        {non_neg_integer(),
                            non_neg_integer(),
                            non_neg_integer(),
                            non_neg_integer(),
                            non_neg_integer(),
                            non_neg_integer()}) ->
                                list({pos_integer(), work_item()}).
choose_schedule([],Allocations, {0, 0, 0, 0, 0, 0}) ->
    lists:ukeysort(1, Allocations);
choose_schedule(Slices, Allocations, {NoCheck, 0, 0, 0, 0, 0}) ->
    {HL, [Allocation|TL]} =
        lists:split(rand:uniform(length(Slices)) - 1, Slices),
    choose_schedule(HL ++ TL,
        [{Allocation, no_check}|Allocations],
        {NoCheck - 1, 0, 0, 0, 0, 0});
choose_schedule(Slices, Allocations, {NoCheck, AllCheck, 0, 0, 0, 0}) ->
    {HL, [Allocation|TL]} =
        lists:split(rand:uniform(length(Slices)) - 1, Slices),
    choose_schedule(HL ++ TL,
        [{Allocation, all_check}|Allocations],
        {NoCheck, AllCheck - 1, 0, 0, 0, 0});
choose_schedule(Slices, Allocations, {NoCheck, AllCheck, DayCheck, 0, 0, 0}) ->
    {HL, [Allocation|TL]} =
        lists:split(rand:uniform(length(Slices)) - 1, Slices),
    choose_schedule(HL ++ TL,
        [{Allocation, day_check}|Allocations],
        {NoCheck, AllCheck, DayCheck - 1, 0, 0, 0});
choose_schedule(Slices, Allocations,
                {NoCheck, AllCheck, DayCheck, HourCheck, 0, 0}) ->
    {HL, [Allocation|TL]} =
        lists:split(rand:uniform(length(Slices)) - 1, Slices),
    choose_schedule(HL ++ TL,
        [{Allocation, hour_check}|Allocations],
        {NoCheck, AllCheck, DayCheck, HourCheck - 1, 0, 0});
choose_schedule(Slices,
        Allocations,
        {NoCheck, AllCheck, DayCheck, HourCheck, RangeCheck, 0}) ->
    {HL, [Allocation|TL]} =
        lists:split(rand:uniform(length(Slices)) - 1, Slices),
    choose_schedule(HL ++ TL,
        [{Allocation, range_check}|Allocations],
        {NoCheck, AllCheck, DayCheck, HourCheck, RangeCheck - 1, 0});
choose_schedule(Slices,
        Allocations,
        {NoCheck, AllCheck, DayCheck, HourCheck, RangeCheck, AutoCheck}) ->
    {HL, [Allocation|TL]} =
        lists:split(rand:uniform(length(Slices)) - 1, Slices),
    choose_schedule(HL ++ TL,
        [{Allocation, auto_check}|Allocations],
        {NoCheck, AllCheck, DayCheck, HourCheck, RangeCheck, AutoCheck - 1}).

-spec in_window(erlang:timestamp(), check_window()) -> boolean().
in_window(_Now, always) ->
    true;
in_window(_Now, never) ->
    false;
in_window(Now, {Start, End}) when Start > End ->
    case calendar:now_to_datetime(Now) of
        {_Date, {HH, _MM, _SS}} when HH >= Start; HH < End ->
            true;
        _ ->
            false
    end;
in_window(Now, {SingleHour, SingleHour}) ->
    case calendar:now_to_datetime(Now) of
        {_Date, {SingleHour, _MM, _SS}} ->
            true;
        _ ->
            false
    end;
in_window(Now, {Start, End}) ->
    case calendar:now_to_datetime(Now) of
        {_Date, {HH, _MM, _SS}} when HH >= Start, HH < End ->
            true;
        _ ->
            false
    end.

%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).

choose_schedule_test() ->
    NoSyncAllSchedule =
        [{no_check, 100},
            {all_check, 0},
            {auto_check, 0},
            {day_check, 0},
            {hour_check, 0},
            {range_check, 0}],
    NoSyncAll = choose_schedule(NoSyncAllSchedule),
    ExpNoSyncAll = lists:map(fun(I) -> {I, no_check} end, lists:seq(1, 100)),
    ?assertMatch(NoSyncAll, ExpNoSyncAll),

    AllSyncAllSchedule =
        [{no_check, 0},
            {all_check, 100},
            {auto_check, 0},
            {day_check, 0},
            {hour_check, 0},
            {range_check, 0}],
    AllSyncAll = choose_schedule(AllSyncAllSchedule),
    ExpAllSyncAll = lists:map(fun(I) -> {I, all_check} end, lists:seq(1, 100)),
    ?assertMatch(AllSyncAll, ExpAllSyncAll),
    
    MixedSyncSchedule = 
        [{no_check, 6},
            {all_check, 1},
            {auto_check, 3},
            {day_check, 4},
            {hour_check, 84},
            {range_check, 2}],
    MixedSync = choose_schedule(MixedSyncSchedule),
    ?assertMatch(100, length(MixedSync)),
    IsSyncFun = fun({_I, Type}) -> Type == hour_check end,
    SliceForHourFun = fun({I, hour_check}) -> I end,
    HourWorkload =
        lists:map(SliceForHourFun, lists:filter(IsSyncFun, MixedSync)),
    ?assertMatch(84, length(lists:usort(HourWorkload))),
    FoldFun = 
        fun(I, Acc) ->
            true = I > Acc,
            I
        end,
    BiggestI = lists:foldl(FoldFun, 0, HourWorkload),
    ?assertMatch(true, BiggestI >= 84),
    
    CountFun =
        fun({_I, Type}, Acc) ->
            {Type, CD} = lists:keyfind(Type, 1, Acc),
            lists:ukeysort(1, [{Type, CD - 1}|Acc])
        end,
    ?assertMatch(
        [{all_check, 0}, {auto_check, 0}, {day_check, 0},
            {hour_check, 0}, {no_check, 0}, {range_check, 0}],
            lists:foldl(CountFun, MixedSyncSchedule, MixedSync)).

take_first_workitem_test() ->
    SC = 48,
    Wants =
        [{no_check, SC},
        {all_check, 0}, 
        {auto_check, 0},
        {day_check, 0},
        {hour_check, 0},
        {range_check, 0}],
    {Mega, Sec, Micro} = os:timestamp(),
    TwentyFourHoursAgo = Mega * ?MEGA + Sec - (60 * 60 * 24),
    OrigStartTime =
        beginning_of_next_hour(
            {TwentyFourHoursAgo div ?MEGA,
                TwentyFourHoursAgo rem ?MEGA,
                Micro}),
    SchedRem =
        lists:map(fun(I) -> {I, no_check} end, lists:seq(2, SC)),
    ScheduleStartTime =
        beginning_of_next_hour({Mega, Sec, Micro}),
    % 24 hours on, the new scheudle start time should be the same it would be
    % if we started now
    {no_check, PromptSeconds, SchedRem, ScheduleStartTime} = 
        take_next_workitem([], Wants, OrigStartTime, {1, 8, 1}, SC),
    ?assertMatch(true, ScheduleStartTime > {Mega, Sec, Micro}),
    ?assertMatch(true, PromptSeconds > 0),
    {no_check, PromptMoreSeconds, SchedRem, ScheduleStartTime} = 
        take_next_workitem([], Wants, OrigStartTime, {2, 8, 1}, SC),
    ?assertMatch(true, PromptMoreSeconds > PromptSeconds),
    {no_check, PromptEvenMoreSeconds, SchedRem, ScheduleStartTime} = 
        take_next_workitem([], Wants, OrigStartTime, {7, 8, 1}, SC),
    ?assertMatch(true, PromptEvenMoreSeconds > PromptMoreSeconds),
    {no_check, PromptYetMoreSeconds, _T0, ScheduleStartTime} = 
        take_next_workitem(SchedRem, Wants, ScheduleStartTime, {1, 8, 1}, SC),
    ?assertMatch(true, PromptYetMoreSeconds > PromptEvenMoreSeconds),
    {no_check, PromptS2YetMoreSeconds, _, ScheduleStartTime} = 
        take_next_workitem(SchedRem, Wants, ScheduleStartTime, {1, 8, 2}, SC),
    {no_check, PromptS3YetMoreSeconds, _, ScheduleStartTime} = 
        take_next_workitem(SchedRem, Wants, ScheduleStartTime, {1, 8, 3}, SC),
    {no_check, PromptS4YetMoreSeconds, _, ScheduleStartTime} = 
        take_next_workitem(SchedRem, Wants, ScheduleStartTime, {1, 8, 4}, SC),
    {no_check, PromptN2YetMoreSeconds, _, ScheduleStartTime} = 
        take_next_workitem(SchedRem, Wants, ScheduleStartTime, {2, 8, 1}, SC),
    ?assertMatch(true, PromptS4YetMoreSeconds > PromptS3YetMoreSeconds),
    ?assertMatch(true, PromptS3YetMoreSeconds > PromptS2YetMoreSeconds),
    ?assertMatch(true, PromptN2YetMoreSeconds > PromptS4YetMoreSeconds).

window_test() ->
    NowSecs0 =
        calendar:datetime_to_gregorian_seconds(
            {{2000, 1, 1}, {0, 59, 59}}),
    Now0 = {NowSecs0 div ?MEGA, NowSecs0 rem ?MEGA, 0},
    ?assert(in_window(Now0, {0, 1})),
    ?assertNot(in_window(Now0, {1, 2})),
    ?assert(in_window(Now0, {23, 1})),
    ?assertNot(in_window(Now0, {23, 0})),
    ?assert(in_window(Now0, {0, 0})),
    ?assertNot(in_window(Now0, {1, 1})),
    ?assertNot(in_window(Now0, {23, 23})),
    
    NowSecs1 =
        calendar:datetime_to_gregorian_seconds(
            {{2000, 1, 1}, {23, 59, 59}}),
    Now1 = {NowSecs1 div ?MEGA, NowSecs1 rem ?MEGA, 0},
    ?assert(in_window(Now1, {23, 0})),
    ?assertNot(in_window(Now1, {22, 23})),
    ?assert(in_window(Now1, {22, 1})),
    ?assertNot(in_window(Now1, {0, 1})),
    ?assert(in_window(Now1, {23, 23})),
    ?assertNot(in_window(Now1, {1, 1})),

    ?assert(in_window(Now1, always)),
    ?assertNot(in_window(Now1, never)).

-define(DAYS_FROM_GREGORIAN_BASE_TO_EPOCH, (1970 * 365 + 478)).
-define(SECONDS_FROM_GREGORIAN_BASE_TO_EPOCH,
    ?DAYS_FROM_GREGORIAN_BASE_TO_EPOCH * ?SECONDS_IN_DAY).

datetime_to_timestamp(DT) ->
    TS =
        calendar:datetime_to_gregorian_seconds(DT)
        - ?SECONDS_FROM_GREGORIAN_BASE_TO_EPOCH,
    {TS div ?MEGA, TS rem ?MEGA, 0}.

beginning_of_next_hour_test() ->
    Now = {1649,320592,173737}, % 08:36 7 April 2022
    ?assertEqual({{2022, 4, 7}, {8, 36, 32}}, calendar:now_to_datetime(Now)),
    NH = beginning_of_next_hour(Now),
    ?assertMatch({{2022, 4, 7}, {9, 0, 0}}, calendar:now_to_datetime(NH)),
    DT23 = {{2022, 4, 7}, {23, 59, 59}},
    NH23 = beginning_of_next_hour(datetime_to_timestamp(DT23)),
    ?assertMatch({{2022, 4, 8}, {0, 0, 0}}, calendar:now_to_datetime(NH23)).

schedule_seconds_test() ->
    NodeNumber = 1,
    SliceNumber = 1,
    ClusterSliceNumber = 1,
    NodeCount = 3,
    SliceCount = 24,
    SecsFromStartTime0 =
        schedule_seconds(
        NodeNumber, SliceNumber, ClusterSliceNumber, NodeCount, SliceCount),
    % 24 slice count - so 60 minute window
    % First node should be in first 20 minutes
    % As first cluster slice should be at start
    % => 0
    ?assertEqual(0, SecsFromStartTime0),
    % If this was cluster slice 3 - should be 10 minutes
    SecsFromStartTime1 =
        schedule_seconds(NodeNumber, SliceNumber, 3, NodeCount, SliceCount),
    ?assertEqual(600, SecsFromStartTime1),
    % If this was Node 2 and cluster slice 2 then - 25 minutes
    SecsFromStartTime2 =
        schedule_seconds(2, SliceNumber, 2, NodeCount, SliceCount),
    ?assertEqual(1500, SecsFromStartTime2).


-endif.
