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
            get_range/0]).

-define(SLICE_COUNT, 100).
-define(SECONDS_IN_DAY, 86400).
-define(INITIAL_TIMEOUT, 60000).
    % Wait a minute before the first allocation is considered,  Lot may be
    % going on at a node immeidately at startup
-define(LOOP_TIMEOUT, 15000).
    % Always wait at least 15s after completing an action before
    % prompting another
-define(CRASH_TIMEOUT, 7200 * 1000).
    % Assume that an exchange has crashed if not response received in this
    % interval, to allow exchanges to be re-scheduled.
-define(EXCHANGE_PAUSE, 1000).
    % Pause between stages of the AAE exchange
-define(MAX_RESULTS, 64).
    % Max nu,ber of segments in the AAE tree to be repaired each loop
-define(RANGE_BOOST, 8).
    % Factor to boost range_check max_results values
-define(MEGA, 1000000).


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
                queue_name :: riak_kv_replrtq_src:queue_name() | undefined,
                slot_info_fun :: fun(() -> {pos_integer(), pos_integer()}),
                slice_count = ?SLICE_COUNT :: pos_integer(),
                is_paused = false :: boolean(),
                last_exchange_start = os:timestamp() :: erlang:timestamp()
                }).

-type req_id() :: no_reply|integer().
-type client_protocol() :: http.
-type client_ip() :: string().
-type client_port() :: pos_integer().
-type nval() :: pos_integer()|range. % Range queries do not have an n-val
-type work_item() :: no_sync|all_sync|day_sync|hour_sync|range_sync.
-type schedule_want() :: {work_item(), non_neg_integer()}.
-type slice() :: pos_integer().
-type allocation() :: {slice(), work_item()}.
-type schedule_wants() :: [schedule_want()].
-type node_info() :: {pos_integer(), pos_integer()}. % Node and node count
-type ttaaefs_state() :: #state{}.
-type ssl_credentials() :: {string(), string(), string(), string()}.
    %% {cacert_filename, cert_filename, key_filename, username}
-type repair_reference() ::
    {riak_object:bucket(), riak_object:key(), vclock:vclock(), any()}.


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
    gen_server:cast(?MODULE, {WorkItem, ReqID, self(), Now}).

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

    {SliceCount, Schedule} = 
        case Scope of
            disabled ->
                {24, [{no_sync, 24}, {all_sync, 0}, 
                        {day_sync, 0}, {hour_sync, 0},
                        {range_sync, 0}]};
                    % No sync once an hour if disabled
            _ ->
                {NoCheck + AllCheck + HourCheck + DayCheck,
                    [{no_sync, NoCheck}, {all_sync, AllCheck},
                        {day_sync, DayCheck}, {hour_sync, HourCheck},
                        {range_sync, RangeCheck}]}
        end,

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

    State2 = 
        State1#state{peer_ip = PeerIP,
                        peer_port = PeerPort,
                        peer_protocol = PeerProtocol,
                        ssl_credentials = SSLCredentials,
                        queue_name = SrcQueueName},
    
    lager:info("Initiated Tictac AAE Full-Sync Mgr with scope=~w", [Scope]),
    {ok, State2, ?INITIAL_TIMEOUT}.

handle_call(pause, _From, State) ->
    case State#state.is_paused of
        true ->
            {reply, {error, already_paused}, State};
        false -> 
            PausedSchedule =
                [{no_sync, State#state.slice_count}, {all_sync, 0},
                    {day_sync, 0}, {hour_sync, 0}, {range_sync, 0}],
            BackupSchedule = State#state.schedule,
            {reply, ok, State#state{schedule = PausedSchedule,
                                    backup_schedule = BackupSchedule,
                                    is_paused = true}}
    end;
handle_call(resume, _From, State) ->
    case State#state.is_paused of
        true ->
            Schedule = State#state.backup_schedule,
            {reply,
                ok,
                State#state{schedule = Schedule, is_paused = false},
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
    Duration = timer:now_diff(os:timestamp(), State#state.last_exchange_start),
    Pause = 
        case Result of
            {waiting_all_results, _Deltas} ->
                % If the exchange ends with waiting all results, then consider
                % this to be equivalent to a crash, and so requiring a full
                % pause to backoff
                lager:info("exchange=~w failed to complete in duration=~w s",
                                [ReqID, Duration div 1000000]),
                ?CRASH_TIMEOUT;
            _ ->
                % If exchanges start slowing, then start increasing the pauses
                % Gradually degrade in response to an increased workload
                max(?LOOP_TIMEOUT, (Duration div 1000) div 2)
        end,
    {noreply, State, Pause};
handle_cast({no_sync, ReqID, From, _}, State) ->
    case ReqID of
        no_reply ->
            ok;
        _ ->
            From ! {ReqID, {no_sync, 0}}
    end,
    {noreply, State, ?LOOP_TIMEOUT};
handle_cast({all_sync, ReqID, From, _Now}, State) ->
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
                        all_sync),
    {noreply, State0, Timeout};
handle_cast({day_sync, ReqID, From, Now}, State) ->
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
                                day_sync),
            {noreply, State0, Timeout};
        bucket ->
            [H|T] = State#state.bucket_list,
            % Note that the tree size is amended as well as the time range.
            % The bigger the time range, the bigger the tree.  Bigger trees
            % are less efficient when there is little change, but can more
            % accurately reflect bigger changes (with less fasle positives).
            Filter =
                {filter, H, all, medium, all,
                {LowerTime, UpperTime}, pre_hash},
            NextBucketList = T ++ [H],
            {State0, Timeout} =
                sync_clusters(From, ReqID, range, range, Filter,
                                NextBucketList, partial, State,
                                day_sync),
            {noreply, State0, Timeout}
    end;
handle_cast({hour_sync, ReqID, From, Now}, State) ->
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
                                hour_sync),
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
                                hour_sync),
            {noreply, State0, Timeout}
    end;
handle_cast({range_sync, ReqID, From, _Now}, State) ->
    case get_range() of
        none ->
            case ReqID of
                no_reply ->
                    ok;
                _ ->
                    From ! {ReqID, {range_sync, 0}}
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
                                        range_sync),
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
                                        hour_sync),
                    {noreply, State0, Timeout}
            end
    end.

handle_info(timeout, State) ->
    SlotInfoFun = State#state.slot_info_fun,
    {WorkItem, Wait, RemainingSlices, ScheduleStartTime} = 
        take_next_workitem(State#state.slice_allocations,
                            State#state.schedule,
                            State#state.slice_set_start,
                            SlotInfoFun(),
                            State#state.slice_count),
    erlang:send_after(Wait * 1000, self(), {work_item, WorkItem}),
    {noreply, State#state{slice_allocations = RemainingSlices,
                            slice_set_start = ScheduleStartTime}};
handle_info({work_item, WorkItem}, State) ->
    process_workitem(WorkItem, no_reply, os:timestamp()),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%============================================================================
%%% Internal functions
%%%============================================================================


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
                        {Bucket, KeyRange, LowTS, HighTS}).

clear_range() ->
    application:set_env(riak_kv, ttaaefs_check_range, none).

-spec get_range() ->
        none|{riak_object:bucket()|all, 
                {riak_object:key(), riak_object:key()}|all,
                pos_integer(), pos_integer()}.
get_range() ->
    application:get_env(riak_kv, ttaaefs_check_range, none).

%% @doc
%% Sync two clusters - return an updated loop state and a timeout
-spec sync_clusters(pid(), integer()|no_reply,
                nval(), nval(), tuple(), list()|undefined, full|partial,
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
            ReplyFun = generate_replyfun(ReqID, From, StopFun),
            ReqID0 = 
                case ReqID of
                    no_reply ->
                        erlang:phash2({self(), os:timestamp()});
                    _ ->
                        ReqID
                end,
            
            MaxResults =
                case WorkType of
                    range_sync ->
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
            
            RepairFun =
                generate_repairfun(ReqID0,
                                    State#state.queue_name,
                                    MaxResults,
                                    Ref,
                                    WorkType),

            {ok, ExPid, ExID} =
                aae_exchange:start(Ref,
                                    [{LocalSendFun, all}],
                                    [{RemoteSendFun, all}],
                                    RepairFun,
                                    ReplyFun,
                                    Filter, 
                                    [{transition_pause_ms, ?EXCHANGE_PAUSE},
                                        {max_results, MaxResults},
                                        {scan_timeout, ?CRASH_TIMEOUT div 2},
                                        {purpose, WorkType}]),
            
            lager:info("Starting ~w full-sync work_item=~w " ++ 
                                "ReqID=~w exchange id=~s pid=~w",
                            [Ref, WorkType, ReqID0, ExID, ExPid]),
            
            {State#state{bucket_list = NextBucketList,
                            last_exchange_start = os:timestamp()},
                ?CRASH_TIMEOUT}
    end.


%% @doc
%% Check the slot info - how many nodes are there active in the cluster, and
%% which is the slot for this node in the cluster.
%% An alternative slot_info function may be passed in when initialising the
%% server (e.g. for test)
-spec get_slotinfo() -> node_info().
get_slotinfo() ->
    UpNodes = lists:sort(riak_core_node_watcher:nodes(riak_kv)),
    NotMe = lists:takewhile(fun(N) -> N /= node() end, UpNodes),
    {length(NotMe) + 1, length(UpNodes)}.

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
    fun() ->
        {ok, R} =
            riak_client:aae_fold({merge_root_nval, NVal}, C),
        ReturnFun(R)
    end;
local_sender({fetch_branches, BranchIDs}, C, ReturnFun, NVal) ->
    fun() ->
        {ok, R} =
            riak_client:aae_fold({merge_branch_nval, NVal, BranchIDs}, C),
        ReturnFun(R)
    end;
local_sender({fetch_clocks, SegmentIDs}, C, ReturnFun, NVal) ->
    fun() ->
        {ok, R} =
            riak_client:aae_fold({fetch_clocks_nval, NVal, SegmentIDs}, C),
        ReturnFun(R)
    end;
local_sender({fetch_clocks, SegmentIDs, MR}, C, ReturnFun, NVal) ->
    LMR = localise_modrange(MR),
    fun() ->
        {ok, R} =
            riak_client:aae_fold({fetch_clocks_nval, NVal, SegmentIDs, LMR},
                                    C),
        ReturnFun(R)
    end;
local_sender({merge_tree_range, B, KR, TS, SF, MR, HM}, C, ReturnFun, range) ->
    fun() ->
        LMR = localise_modrange(MR),
        %% riak_client expects modified range of form
        %% {date, non_neg_integer(), non_neg_integer()}
        %% where as the riak erlang clients just expect 
        %% {non_neg_integer(), non_neg_integer()}
        %% They keyword all must also be supported
        {ok, R} =
            riak_client:aae_fold({merge_tree_range, B, KR, TS, SF, LMR, HM},
                                    C),
        ReturnFun(R)
    end;
local_sender({fetch_clocks_range, B0, KR, SF, MR}, C, ReturnFun, _NVal) ->
    fun() ->
        LMR = localise_modrange(MR),
        {ok, R} =
            riak_client:aae_fold({fetch_clocks_range, B0, KR, SF, LMR}, C),
        ReturnFun(R)
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
                MapFun =
                    fun({{B, K}, VC}) ->
                        {B, K, decode_clock(VC)}
                    end,
                ReturnFun(lists:map(MapFun, KeysClocks));
            {error, Error} ->
                lager:warning("Error of ~w in clocks request", [Error]),
                ReturnFun({error, Error})
        end
    end;
remote_sender({fetch_clocks, SegmentIDs, MR}, Client, Mod, ReturnFun, NVal) ->
    fun() ->
        case Mod:aae_fetch_clocks(Client, NVal, SegmentIDs, MR) of
            {ok, {keysclocks, KeysClocks}} ->
                MapFun =
                    fun({{B, K}, VC}) ->
                        {B, K, decode_clock(VC)}
                    end,
                ReturnFun(lists:map(MapFun, KeysClocks));
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
                MapFun =
                    fun({{B, K}, VC}) ->
                        {B, K, decode_clock(VC)}
                    end,
                ReturnFun(lists:map(MapFun, KeysClocks));
            {error, Error} ->
                lager:warning("Error of ~w in segment request", [Error]),
                ReturnFun({error, Error})
        end
    end.

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
-spec generate_replyfun(integer()|no_reply, pid(), fun(() -> ok))
                                                -> aae_exchange:reply_fun().
generate_replyfun(ReqID, From, StopClientFun) ->
    fun(Result) ->
        case ReqID of
            no_reply ->
                ok;
            _ ->
                % Reply to riak_client
                From ! {ReqID, Result}
        end,
        lager:info("Completed full-sync with result=~w", [Result]),
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
-spec generate_repairfun(integer(),
                            riak_kv_replrtq_src:queue_name(),
                            non_neg_integer(),
                            full|partial,
                            work_item()) -> aae_exchange:repair_fun().
generate_repairfun(ExchangeID, QueueName, MaxResults, Ref, WorkType) ->
    LogRepairs = app_helper:get_env(riak_kv, ttaaefs_logrepairs, false),
    fun(RepairList) ->
        FoldFun =
            fun({{B, K}, {SrcVC, SinkVC}}, {SourceL, SinkC}) ->
                % how are the vector clocks encoded at this point?
                % The erlify_aae_keyclock will have base64 decoded the clock
                case vclock_dominates(SinkVC, SrcVC) of
                    true ->
                        {SourceL, SinkC + 1};
                    false ->
                        % If the vector clock in the source is not dominated
                        % by the sink, then we should replicate if it differs
                        case {vclock_equal(SrcVC, SinkVC), LogRepairs} of
                            {false, true} ->
                                lager:info(
                                    "Repair B=~p K=~p SrcVC=~w SnkVC=~w",
                                        [B, K, SrcVC, SinkVC]),
                                {[{B, K, SrcVC, to_fetch}|SourceL], SinkC};
                            {false, false} ->
                                {[{B, K, SrcVC, to_fetch}|SourceL], SinkC};
                            {true, _} ->
                                {SourceL, SinkC}
                        end
                end
            end,
        {ToRepair, SinkDCount} = lists:foldl(FoldFun, {[], 0}, RepairList),
        lager:info("AAE exchange=~w work_item=~w type=~w shows sink ahead " ++
                        "for key_count=~w keys limited by max_results=~w", 
                    [ExchangeID, WorkType, Ref, SinkDCount, MaxResults]),
        riak_kv_replrtq_src:replrtq_ttaaefs(QueueName, ToRepair),
        PBDL = report_repairs(ExchangeID, ToRepair, Ref, WorkType),
        SetRange =
            case {WorkType, length(ToRepair)} of 
                {range_sync, RepairCount} when RepairCount > 0 ->
                    true;
                {_AltSync, RepairCount} when RepairCount > (MaxResults div 2) ->
                    true;
                _ ->
                    false
            end,
        case {SetRange, PBDL} of
            {true, [{B, KC, LowDT, HighDT}]} ->
                lager:info("Setting range to bucket=~p ~w ~w last_count=~w" ++
                                " set by work_item=~w",
                            [B, LowDT, HighDT, KC, WorkType]),
                set_range(B, all, LowDT, HighDT),
                ok;
            {true, PBDL} ->
                MapFun = fun({_B, _KC, LD, HD}) -> {LD, HD} end,
                {LDL, HDL} =
                    lists:unzip(lists:map(MapFun, PBDL)),
                LowDT = lists:min(LDL),
                HighDT = lists:max(HDL),
                lager:info("Setting range to bucket=all ~w ~w last_count=~w" ++
                                " set by work_item=~w",
                            [LowDT, HighDT, length(ToRepair), WorkType]),
                set_range(all, all, LowDT, HighDT),
                ok;
            _ ->
                ok
        end
    end.


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


-spec report_repairs(integer(),
                        list(repair_reference()),
                        full|partial,
                        work_item()) -> 
                            list({riak_object:bucket(),
                                    pos_integer(),
                                    calendar:datetime(),
                                    calendar:datetime()}).
report_repairs(ExchangeID, RepairList, Ref, WorkType) ->
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
            lager:info("AAE exchange=~w work_item=~w type=~w repaired " ++ 
                            "key_count=~w for " ++
                            "bucket=~p with low date ~p high date ~p",
                        [ExchangeID, WorkType, Ref, C, B, MinDT, MaxDT])
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
                os:timestamp();
            {Mega, Sec, _Micro} ->
                Seconds = Mega * ?MEGA + Sec + 86400,
                {Seconds div ?MEGA, Seconds rem ?MEGA, 0}
        end,
    take_next_workitem(NewAllocations, Wants,
                        RevisedStartTime, SlotInfo, SliceCount);
take_next_workitem([NextAlloc|T], Wants,
                        ScheduleStartTime, SlotInfo, SliceCount) ->
    {NodeNumber, NodeCount} = SlotInfo,
    SliceSeconds = ?SECONDS_IN_DAY div max(SliceCount, 1),
    SlotSeconds = (NodeNumber - 1) * (SliceSeconds div max(NodeCount, 1)),
    {SliceNumber, NextAction} = NextAlloc,
    {Mega, Sec, _Micro} = ScheduleStartTime,
    ScheduleSeconds = 
        Mega * ?MEGA + Sec + SlotSeconds + SliceNumber * SliceSeconds,
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


%% @doc
%% Calculate an allocation of activity for the next 24 hours based on the
%% configured schedule-needs.
-spec choose_schedule(schedule_wants()) -> list(allocation()).
choose_schedule(ScheduleWants) ->
    [{no_sync, NoSync}, {all_sync, AllSync},
        {day_sync, DaySync}, {hour_sync, HourSync},
        {range_sync, RangeSync}] = ScheduleWants,
    SliceCount = NoSync + AllSync + DaySync + HourSync + RangeSync,
    Slices = lists:seq(1, SliceCount),
    Allocations = [],
    lists:sort(
        choose_schedule(Slices,
                        Allocations,
                        {NoSync, AllSync, DaySync, HourSync, RangeSync})).

-spec choose_schedule(list(pos_integer()),
                        list({pos_integer(), work_item()}),
                        {non_neg_integer(),
                            non_neg_integer(),
                            non_neg_integer(),
                            non_neg_integer(),
                            non_neg_integer()}) ->
                                list({pos_integer(), work_item()}).
choose_schedule([], Allocations, {0, 0, 0, 0, 0}) ->
    lists:ukeysort(1, Allocations);
choose_schedule(Slices, Allocations, {NoSync, 0, 0, 0, 0}) ->
    {HL, [Allocation|TL]} =
        lists:split(rand:uniform(length(Slices)) - 1, Slices),
    choose_schedule(HL ++ TL,
                    [{Allocation, no_sync}|Allocations],
                    {NoSync - 1, 0, 0, 0, 0});
choose_schedule(Slices, Allocations, {NoSync, AllSync, 0, 0, 0}) ->
    {HL, [Allocation|TL]} =
        lists:split(rand:uniform(length(Slices)) - 1, Slices),
    choose_schedule(HL ++ TL,
                    [{Allocation, all_sync}|Allocations],
                    {NoSync, AllSync - 1, 0, 0, 0});
choose_schedule(Slices, Allocations, {NoSync, AllSync, DaySync, 0, 0}) ->
    {HL, [Allocation|TL]} =
        lists:split(rand:uniform(length(Slices)) - 1, Slices),
    choose_schedule(HL ++ TL,
                    [{Allocation, day_sync}|Allocations],
                    {NoSync, AllSync, DaySync - 1, 0, 0});
choose_schedule(Slices, Allocations, {NoSync, AllSync, DaySync, HourSync, 0}) ->
    {HL, [Allocation|TL]} =
        lists:split(rand:uniform(length(Slices)) - 1, Slices),
    choose_schedule(HL ++ TL,
                    [{Allocation, hour_sync}|Allocations],
                    {NoSync, AllSync, DaySync, HourSync - 1, 0});
choose_schedule(Slices, Allocations, {NoSync, AllSync, DaySync, HourSync, RangeSync}) ->
    {HL, [Allocation|TL]} =
        lists:split(rand:uniform(length(Slices)) - 1, Slices),
    choose_schedule(HL ++ TL,
                    [{Allocation, range_sync}|Allocations],
                    {NoSync, AllSync, DaySync, HourSync, RangeSync - 1}).


%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).

choose_schedule_test() ->
    NoSyncAllSchedule =
        [{no_sync, 100}, {all_sync, 0}, {day_sync, 0}, {hour_sync, 0}, {range_sync, 0}],
    NoSyncAll = choose_schedule(NoSyncAllSchedule),
    ExpNoSyncAll = lists:map(fun(I) -> {I, no_sync} end, lists:seq(1, 100)),
    ?assertMatch(NoSyncAll, ExpNoSyncAll),

    AllSyncAllSchedule =
        [{no_sync, 0}, {all_sync, 100}, {day_sync, 0}, {hour_sync, 0}, {range_sync, 0}],
    AllSyncAll = choose_schedule(AllSyncAllSchedule),
    ExpAllSyncAll = lists:map(fun(I) -> {I, all_sync} end, lists:seq(1, 100)),
    ?assertMatch(AllSyncAll, ExpAllSyncAll),
    
    MixedSyncSchedule = 
        [{no_sync, 0}, {all_sync, 1}, {day_sync, 4}, {hour_sync, 95}, {range_sync, 0}],
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
    Wants = [{no_sync, 100}, {all_sync, 0}, {day_sync, 0}, {hour_sync, 0}, {range_sync, 0}],
    {Mega, Sec, Micro} = os:timestamp(),
    TwentyFourHoursAgo = Mega * ?MEGA + Sec - (60 * 60 * 24),
    {NextAction, PromptSeconds, _T, ScheduleStartTime} = 
        take_next_workitem([], Wants,
                            {TwentyFourHoursAgo div ?MEGA,
                                TwentyFourHoursAgo rem ?MEGA, Micro},
                            {1, 8},
                            100),
    ?assertMatch(no_sync, NextAction),
    ?assertMatch(true, ScheduleStartTime < {Mega, Sec, Micro}),
    ?assertMatch(true, PromptSeconds > 0),
    {NextAction, PromptMoreSeconds, _T, ScheduleStartTime} = 
        take_next_workitem([], Wants,
                            {TwentyFourHoursAgo div ?MEGA,
                                TwentyFourHoursAgo rem ?MEGA, Micro},
                            {2, 8},
                            100),
    ?assertMatch(true, PromptMoreSeconds > PromptSeconds),
    {NextAction, PromptEvenMoreSeconds, T, ScheduleStartTime} = 
        take_next_workitem([], Wants,
                            {TwentyFourHoursAgo div ?MEGA,
                                TwentyFourHoursAgo rem ?MEGA, Micro},
                            {7, 8},
                            100),
    ?assertMatch(true, PromptEvenMoreSeconds > PromptMoreSeconds),
    {NextAction, PromptYetMoreSeconds, _T0, ScheduleStartTime} = 
        take_next_workitem(T, Wants, ScheduleStartTime, {1, 8}, 100),
    ?assertMatch(true, PromptYetMoreSeconds > PromptEvenMoreSeconds).



-endif.
