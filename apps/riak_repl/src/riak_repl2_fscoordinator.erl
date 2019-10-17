%% @doc Coordinates full sync replication parallelism.  Uses 3
%% `riak_repl' application environment values: `fullsync_on_connect',
%% `max_fssource_cluster', and `max_fssource_node',
%% `max_fssource_retries' and `fssource_retry_wait'
%%
%% <dl>
%%  <dt>`{fullsync_on_connect, boolean()}'</dt>
%%  <dd>
%% If `true', as soon as a connection to the remote cluster is established,
%% fullsync starts.  If `false', then an explicit start must be sent.
%% Defaults to true.
%%  </dd>
%%
%%  <dt>`{max_fssource_cluster, pos_integer()}'</dt>
%%  <dd>
%% How many sources can be started across all nodes in the local cluster.
%% Defaults to 5.
%%  </dd>
%%
%%  <dt>`{max_fssource_node, pos_integer()}'</dt>
%%  <dd>
%% How many sources can be started on a single node, provided starting one
%% wouldn't exceed the `max_fssource_cluster' setting. Defaults to 1.
%%  </dd>
%%
%%  <dt>`{`max_fssource_retries, pos_integer()}'</dt>
%%  <dd>
%% How many times a "soft_exiting" partition (eg. where the sink is
%% locked, or rebuilding for will be retried before it is
%% discarded. Defaults to 100.
%%  </dd>
%%
%%  <dt>`{`fssource_retry_wait, pos_integer()}'</dt>
%%  <dd>
%% How many seconds must elapse between retrying a fullsynce on a
%% "soft_exiting" partition. to 60.
%%  </dd>
%% </dl>

-module(riak_repl2_fscoordinator).
-include("riak_repl.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-behaviour(gen_server).
-define(SERVER, ?MODULE).

% how long to wait for a reply from remote cluster before moving on to
% next partition.
-define(WAITING_TIMEOUT, 5000).
% How often stats should be cached in milliseconds.
-ifndef(DEFAULT_STAT_REFRESH_INTERVAL).
-ifdef(TEST).
-define(DEFAULT_STAT_REFRESH_INTERVAL, 1000).
-else.
-define(DEFAULT_STAT_REFRESH_INTERVAL, 60000).
-endif.
-endif.

-record(stat_cache, {
    worker :: {pid(), reference()} | undefined,
    refresh_timer :: reference() | undefined,
    refresh_interval = app_helper:get_env(riak_repl, fullsync_stat_refresh_interval, ?DEFAULT_STAT_REFRESH_INTERVAL) :: pos_integer(),
    last_refresh = riak_core_util:moment() :: 'undefined' | pos_integer(),
    stats = [] :: [tuple()]
}).

-record(state, {
    leader_node :: 'undefined' | node(),
    leader_pid :: 'undefined' | node(),
    other_cluster,
    socket,
    transport,
    largest_n,
    connection_ref,
    partition_queue = queue:new(),
    retries = dict:new(),
    reserve_retries = dict:new(),
    soft_retries = dict:new(),
    whereis_waiting = [],
    busy_nodes = sets:new(),
    running_sources = [],
    purgatory = queue:new(),
    dropped = [],
    successful_exits = 0,
    error_exits = 0,
    retry_exits = 0,
    soft_retry_exits = 0,
    pending_fullsync = false,
    dirty_nodes = ordsets:new(),          % these nodes should run fullsync
    dirty_nodes_during_fs = ordsets:new(), % these nodes reported realtime errors
                                          % during an already running fullsync
    fullsyncs_completed = 0,
    fullsync_start_time = undefined,
    last_fullsync_duration = undefined,
    last_fullsync_completed = undefined,
    stat_cache = #stat_cache{}
}).

-record(partition_info, {
    index,
    node,
    running_source,
    whereis_tref
}).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/1, start_fullsync/1, stop_fullsync/1,
    status/0, status/1, status/2, is_running/1,
        node_dirty/1, node_dirty/2, node_clean/1, node_clean/2]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% ------------------------------------------------------------------
%% connection manager Function Exports
%% ------------------------------------------------------------------

-export([connected/6,connect_failed/3]).

%% ------------------------------------------------------------------
%% stat caching mechanism export
%% ------------------------------------------------------------------

-export([refresh_stats_worker/2]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

%% @doc Start a fullsync coordinator for managing a sync to the remote Cluster.
-spec start_link(Cluster :: string()) -> {'ok', pid()}.
start_link(Cluster) ->
    gen_server:start_link(?MODULE, Cluster, []).

%% @doc Begin syncing.  If called while a fullsync is in progress, nothing
%% happens.
-spec start_fullsync(Pid :: pid()) -> 'ok'.
start_fullsync(Pid) ->
    gen_server:cast(Pid, start_fullsync).

%% @doc Stop syncing.  A start will begin the fullsync completely over.
-spec stop_fullsync(Pid :: pid()) -> 'ok'.
stop_fullsync(Pid) ->
    gen_server:cast(Pid, stop_fullsync).

%% @doc Get a status report as a proplist for each fullsync enabled. Usually
%% for use with a console.
-spec status() -> [tuple()].
status() ->
    LeaderNode = riak_repl2_leader:leader_node(),
    Timeout = case node() of
        LeaderNode ->
            infinity;
        _ ->
            ?LONG_TIMEOUT
    end,
    case LeaderNode of
        undefined ->
            [];
        _ -> [{Remote, status(Pid, Timeout)} || {Remote, Pid} <-
                riak_repl2_fscoordinator_sup:started(LeaderNode)]
    end.

%% @doc Get the status proplist for the given fullsync process. Same as
%% `status(Pid, infinity)'.
%% @see status/2
-spec status(Pid :: pid()) -> [tuple()].
status(Pid) ->
    status(Pid, infinity).

%% @doc Get the stats proplist for the given fullsync process, or give up after
%% the timeout.  The atom `infinity' means never timeout.
-spec status(Pid :: pid(), Timeout :: timeout()) -> [tuple()].
status(Pid, Timeout) ->
    try
        gen_server:call(Pid, status, Timeout)
    catch
        _:{timeout, _} ->
        []
    end.

%% @doc Return true if the given fullsync coordinator is in the middle of
%% syncing, otherwise false.
-spec is_running(Pid :: pid()) -> boolean().
is_running(Pid) when is_pid(Pid) ->
    gen_server:call(Pid, is_running, infinity);
is_running(_Other) ->
    false.

node_dirty(Node) ->
    %% if fullsync running
    %% keep 2 lists:
    %%  1) non-running - clear these out when fullsync finishes, and the node
    %%  doesn't appear in list 2
    %%  2) running - don't clear these out when fullsync finishes
    case riak_core_cluster_mgr:get_leader() of
        undefined ->
            lager:debug("rt_dirty status updated locally, but not registered with leader");
        Leader ->
            Fullsyncs = riak_repl2_fscoordinator_sup:started(Leader),
            [riak_repl2_fscoordinator:node_dirty(Pid, Node) ||
                {_, Pid} <- Fullsyncs]
    end.

node_dirty(Pid, Node) ->
    gen_server:call(Pid, {node_dirty, Node}, infinity),
    lager:debug("Node ~p marked dirty and needs a fullsync",[Node]).

node_clean(Node) ->
    Leader = riak_core_cluster_mgr:get_leader(),
    Fullsyncs = riak_repl2_fscoordinator_sup:started(Leader),
     [riak_repl2_fscoordinator:node_clean(Pid, Node) ||
        {_, Pid} <- Fullsyncs].

node_clean(Pid, Node) ->
    gen_server:call(Pid, {node_clean, Node}, infinity),
    lager:debug("Node ~p marked clean",[Node]).


%% ------------------------------------------------------------------
%% connection manager callbacks
%% ------------------------------------------------------------------

%% @hidden
connected(Socket, Transport, Endpoint, Proto, Pid, _Props) ->
    Transport:controlling_process(Socket, Pid),
    gen_server:cast(Pid, {connected, Socket, Transport, Endpoint, Proto}).

%% @hidden
connect_failed(_ClientProto, Reason, SourcePid) ->
    gen_server:cast(SourcePid, {connect_failed, self(), Reason}).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

%% @hidden
init(Cluster) ->
    process_flag(trap_exit, true),
    TcpOptions = [
        {keepalive, true},
        {nodelay, true},
        {packet, 4},
        {active, false}
    ],
    ClientSpec = {{fs_coordinate, [{1,0}]}, {TcpOptions, ?MODULE, self()}},
    case riak_core_connection_mgr:connect({rt_repl, Cluster}, ClientSpec) of
        {ok, Ref} ->
            _ = riak_repl_util:schedule_cluster_fullsync(Cluster),
            {ok, refresh_stats(#state{other_cluster = Cluster, connection_ref = Ref})};
        {error, Error} ->
            lager:error("Error connecting to remote with message: ~p", [Error]),
            {stop, Error}
    end.

%% @hidden
handle_call(status, _From, State = #state{socket=Socket}) ->
    SourceStats = State#state.stat_cache#stat_cache.stats,
    StatFreshness = calendar:gregorian_seconds_to_datetime(State#state.stat_cache#stat_cache.last_refresh),
    SocketStats = riak_core_tcp_mon:format_socket_stats(
        riak_core_tcp_mon:socket_status(Socket), []),
    StartTime =
        case State#state.fullsync_start_time of
            undefined -> undefined;
            _N -> calendar:gregorian_seconds_to_datetime(State#state.fullsync_start_time)
        end,
    FinishTime =
        case State#state.last_fullsync_completed of
            undefined -> undefined;
            LastFSCompleted -> calendar:gregorian_seconds_to_datetime(LastFSCompleted)
        end,
    SelfStats = [
        {cluster, State#state.other_cluster},
        {queued, queue:len(State#state.partition_queue)},
        {in_progress, length(State#state.running_sources)},
        {waiting_for_retry, queue:len(State#state.purgatory)},
        {starting, length(State#state.whereis_waiting)},
        {successful_exits, State#state.successful_exits},
        {error_exits, State#state.error_exits},
        {retry_exits, State#state.retry_exits},
        {soft_retry_exits, State#state.soft_retry_exits},
        {busy_nodes, sets:size(State#state.busy_nodes)},
        {last_running_refresh, StatFreshness},
        {running_stats, SourceStats},
        {socket, SocketStats},
        {fullsyncs_completed, State#state.fullsyncs_completed},
        {last_fullsync_started, StartTime},
        {last_fullsync_duration, State#state.last_fullsync_duration},
        {last_fullsync_completed, FinishTime},
        {fullsync_suggested,
            nodeset_to_string_list(State#state.dirty_nodes)},
        {fullsync_suggested_during_fs,
            nodeset_to_string_list(State#state.dirty_nodes_during_fs)}
    ],
    {reply, SelfStats, State};

handle_call(is_running, _From, State) ->
    IsRunning = is_fullsync_in_progress(State),
    {reply, IsRunning, State};

handle_call({node_dirty, Node}, _From,
            State = #state{
                dirty_nodes=DirtyNodes,
                dirty_nodes_during_fs=DirtyDuringFS}) ->
    NewState =
        case is_fullsync_in_progress(State) of
          true -> lager:debug("Node dirty during fullsync from ~p ", [Node]),
                  NewDirty = ordsets:add_element(Node, DirtyDuringFS),
                  State#state{dirty_nodes_during_fs = NewDirty};
          false -> lager:debug("Node dirty from ~p ", [Node]),
                   NewDirty = ordsets:add_element(Node, DirtyNodes),
                   State#state{dirty_nodes = NewDirty}
        end,
    {reply, ok, NewState};

handle_call({node_clean, Node}, _From, State = #state{dirty_nodes=DirtyNodes}) ->
    lager:debug("Marking ~p clean after fullsync", [Node]),
    NewDirtyNodes = ordsets:del_element(Node, DirtyNodes),
    NewState = State#state{dirty_nodes=NewDirtyNodes},
    {reply, ok, NewState};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

%% @hidden
handle_cast({connected, Socket, Transport, _Endpoint, _Proto}, State) ->
    lager:info("Fullsync coordinator connected to ~p", [State#state.other_cluster]),
    SocketTag = riak_repl_util:generate_socket_tag("fs_coord", Transport, Socket),
    lager:debug("Keeping stats for " ++ SocketTag),
    riak_core_tcp_mon:monitor(Socket, {?TCP_MON_FULLSYNC_APP, coord,
                                       SocketTag}, Transport),

    Transport:setopts(Socket, [{active, once}]),
    State2 = State#state{ socket = Socket, transport = Transport},
    case app_helper:get_env(riak_repl, fullsync_on_connect, true) orelse
        State#state.pending_fullsync of
        true ->
            start_fullsync(self());
        false ->
            ok
    end,
    {noreply, State2};

handle_cast({connect_failed, _From, Why}, State) ->
    lager:warning("Fullsync remote connection to ~p failed due to ~p, retrying",
                  [State#state.other_cluster, Why]),
    {stop, connect_failed, State};

handle_cast(start_fullsync, #state{socket=undefined} = State) ->
    %% not connected yet...
    {noreply, State#state{pending_fullsync = true}};
handle_cast(start_fullsync,  State) ->
    case is_fullsync_in_progress(State) of
        true ->
            lager:notice("Fullsync already in progress; ignoring start"),
            {noreply, State};
        false ->
            MaxSource = app_helper:get_env(riak_repl, max_fssource_node, ?DEFAULT_SOURCE_PER_NODE),
            MaxCluster = app_helper:get_env(riak_repl, max_fssource_cluster, ?DEFAULT_SOURCE_PER_CLUSTER),
            lager:info("Starting fullsync (source) with max_fssource_node=~p and max_fssource_cluster=~p",
                       [MaxSource, MaxCluster]),
            {ok, Ring} = riak_core_ring_manager:get_my_ring(),
            N = largest_n(Ring),
            Partitions = sort_partitions(Ring),
            State2 = State#state{
                largest_n = N,
                partition_queue = queue:from_list(Partitions),
                retries = dict:new(),
                reserve_retries = dict:new(),
                purgatory = queue:new(),
                soft_retries = dict:new(),
                dropped = [],
                successful_exits = 0,
                error_exits = 0,
                retry_exits = 0,
                fullsync_start_time = riak_core_util:moment()
            },
            State3 = start_up_reqs(State2),
            {noreply, State3}
    end;

handle_cast(stop_fullsync, State) ->
    % exit all running, cancel all timers, and reset the state.
    _ = [erlang:cancel_timer(Tref) || #partition_info{whereis_tref = Tref}
                                      <- State#state.whereis_waiting],
    _ = [begin
             unlink(Pid),
             riak_repl2_fssource:stop_fullsync(Pid),
             riak_repl2_fssource_sup:disable(node(Pid), Part)
         end || #partition_info{index = Part, running_source = Pid}
                <- State#state.running_sources],
    State2 = State#state{
        largest_n = undefined,
        partition_queue = queue:new(),
        retries = dict:new(),
        reserve_retries = dict:new(),
        purgatory = queue:new(),
        dropped = [],
        whereis_waiting = [],
        running_sources = []
    },
    {noreply, State2};

handle_cast(_Msg, State) ->
    {noreply, State}.


%% @hidden
handle_info({'EXIT', Pid, Cause},
            #state{socket=Socket, transport=Transport}=State) when Cause =:= normal; Cause =:= shutdown ->
    lager:debug("fssource ~p exited normally", [Pid]),
    PartitionEntry = lists:keytake(Pid, #partition_info.running_source, State#state.running_sources),
    case PartitionEntry of
        false ->
            % late exit or otherwise non-existant
            {noreply, State};
        {value, Partition, Running} ->
            #partition_info{index = Index, node = Node} = Partition,
            % likely a slot on the remote node opened up, so re-enable that
            % remote node for whereis requests.
            NewBusies = sets:del_element(Node, State#state.busy_nodes),

            % ensure we unreserve the partition on the remote node
            % instead of waiting for a timeout.
            Transport:send(Socket, term_to_binary({unreserve, Index})),

            % stats
            Sucesses = State#state.successful_exits + 1,
            State2 = State#state{successful_exits = Sucesses,
                                 busy_nodes = NewBusies},

            % are we done?
            maybe_complete_fullsync(Running, State2)
    end;

handle_info({soft_exit, Pid, Cause}, State) ->
    lager:info("fssource ~p soft exit with reason ~p", [Pid, Cause]),
    handle_abnormal_exit(soft_exit, Pid, Cause, State);

handle_info({'EXIT', Pid, Cause}, State) ->
    lager:info("fssource ~p exited abnormally: ~p", [Pid, Cause]),
    handle_abnormal_exit('EXIT', Pid, Cause, State);

handle_info({Partition, whereis_timeout}, State) ->
    #state{whereis_waiting = Waiting} = State,
    case lists:keytake(Partition, #partition_info.index, Waiting) of
        false ->
            % late timeout.
            {noreply, State};
        {value, PartitionInfo, Waiting2} ->
            Q = queue:in(PartitionInfo#partition_info{whereis_tref = undefined}, State#state.partition_queue),
            State2 = State#state{whereis_waiting = Waiting2, partition_queue = Q},
            State3 = start_up_reqs(State2),
            {noreply, State3}
    end;

handle_info({Erred, Socket, _Reason}, #state{socket = Socket} = State) when
    Erred =:= tcp_error; Erred =:= ssl_error ->
    lager:error("Connection closed unexpectedly with message: ~p", [Erred]),
    % Yes I do want to die horribly; my supervisor should restart me.
    {stop, {connection_error, Erred}, State};

handle_info({_Proto, Socket, Data}, #state{socket = Socket} = State) when is_binary(Data) ->
    #state{transport = Transport} = State,
    Transport:setopts(Socket, [{active, once}]),
    Data1 = binary_to_term(Data),
    State2 = handle_socket_msg(Data1, State),
    {noreply, State2};

handle_info({Closed, Socket}, #state{socket = Socket} = State) when
    Closed =:= tcp_closed; Closed =:= ssl_closed ->
    lager:info("Fullsync coordinator connection closed with cluster: ~p", [State#state.other_cluster]),
    % Yes I do want to die horribly; my supervisor should restart me.
    {stop, connection_closed, State};

handle_info(send_next_whereis_req, State) ->
    State2 = case is_fullsync_in_progress(State) of
        true ->
            % this is in response to a potential desync or stale cache of
            % remote nodes, so we'll ditch what we have and try again.
            NewBusies = sets:new(),
            start_up_reqs(State#state{busy_nodes = NewBusies});
        false ->
            State
    end,
    {noreply, State2};

handle_info({stat_update, Worker, Time, Stats}, #state{stat_cache = #stat_cache{worker = {Worker, Mon}} = StatCache} = State) ->
    erlang:demonitor(Mon, [flush]),
    StatCache1 = StatCache#stat_cache{worker = undefined, last_refresh = Time, stats = Stats},
    StatCache2 = schedule_stat_refresh(StatCache1),
    {noreply, State#state{stat_cache = StatCache2}};

handle_info(refresh_stats, State) ->
    State2 = refresh_stats(State),
    {noreply, State2};

handle_info({'DOWN', Mon, process, Pid, Why}, #state{stat_cache = #stat_cache{worker = {Pid, Mon}} = StatCache} = State) ->
    lager:notice("Stat gathering worker process ~p unexpected exit: ~p", [Pid, Why]),
    StatCache1 = StatCache#stat_cache{worker = undefined},
    StatCache2 = refresh_stats(StatCache1, State#state.running_sources),
    {noreply, State#state{stat_cache = StatCache2}};

handle_info(_Info, State) ->
    {noreply, State}.

handle_abnormal_exit(ExitType, Pid, Cause, State) ->
    PartitionEntry = lists:keytake(Pid, #partition_info.running_source, State#state.running_sources),
    handle_abnormal_exit(ExitType, Pid, Cause, PartitionEntry, State).

handle_abnormal_exit(_ExtiType, _Pid, _Cause, false, State) ->
    % late exit
    {noreply, State};

handle_abnormal_exit(ExitType, Pid, _Cause, {value, PartitionWithSource, Running}, State) ->

    Partition = PartitionWithSource#partition_info{running_source = undefined},

    #partition_info{index = Index, node = Node} = Partition,
    #state{socket = Socket, transport = Transport} = State,
    % even a bad exit opens a slot on the remote node
    NewBusies = sets:del_element(Node, State#state.busy_nodes),

    % ensure we unreserve the partition on the remote node
    % instead of waiting for a timeout.
    Transport:send(Socket, term_to_binary({unreserve, Index})),

    % stats
    #state{partition_queue = PQueue} = State,

    State2 = State#state{busy_nodes = NewBusies, running_sources = Running},
    {ErrorCount, State3} = increment_error_dict(Partition, ExitType, State2),

    case ExitType of
        soft_exit ->
            lager:debug("putting partition ~p in purgatory due to soft exit of ~p", [Index, Pid]),
            _ = flush_exit_message(Pid),
            State4 = start_up_reqs(State3),
            SoftRetryLimit = app_helper:get_env(riak_repl, max_fssource_soft_retries, ?DEFAULT_SOURCE_SOFT_RETRIES),
            SoftRetryCount = State4#state.soft_retry_exits + 1,
            if
                SoftRetryLimit =:= infinity ->
                    Now = riak_core_util:moment(),
                    Purgatory = queue:in({Partition, Now}, State4#state.purgatory),
                    {noreply, State4#state{purgatory = Purgatory, soft_retry_exits = SoftRetryCount}};

                SoftRetryLimit < ErrorCount ->
                    lager:info("Discarding partition ~p since it has reached the soft exit retry limit of ~p",
                               [Partition#partition_info.index, SoftRetryLimit]),

                    ErrorExits1 = State4#state.error_exits + 1,
                    Dropped = [Partition#partition_info.index | State4#state.dropped],
                    Purgatory = queue:filter(fun({P, _}) -> P =/= Partition end,
                                             State4#state.purgatory),
                    maybe_complete_fullsync(Running,
                                            State4#state{error_exits = ErrorExits1,
                                                            purgatory = Purgatory,
                                                            dropped = Dropped});
                true ->
                    Now = riak_core_util:moment(),
                    Purgatory = queue:in({Partition, Now}, State4#state.purgatory),
                    {noreply, State4#state{purgatory = Purgatory, soft_retry_exits = SoftRetryCount}}
            end;

        'EXIT' ->
            lager:debug("Incrementing retries for partition ~p due to error exit of ~p", [Index, Pid]),
            RetryLimit = app_helper:get_env(riak_repl, max_fssource_retries,
                                            ?DEFAULT_SOURCE_RETRIES),

            if
                ErrorCount > RetryLimit ->
                    lager:warning("fssource dropping partition: ~p, ~p failed"
                                "retries", [Partition, RetryLimit]),
                    ErrorExits = State#state.error_exits + 1,
                    State4 = State3#state{ error_exits = ErrorExits},
                    Dropped = [Partition#partition_info.index | State4#state.dropped],
                    maybe_complete_fullsync(Running, State4#state{dropped = Dropped});
                true -> %% have not run out of retries yet
                    % reset for retry later
                    lager:info("fssource rescheduling partition: ~p",
                                [Partition]),
                    PQueue2 = queue:in(Partition, PQueue),
                    RetryExits = State3#state.retry_exits + 1,
                    State4 = State3#state{partition_queue = PQueue2,
                                         retry_exits = RetryExits},
                    State5 = start_up_reqs(State4),
                    {noreply, State5}
            end
    end.

%% @hidden
terminate(_Reason, _State) ->
    ok.


%% @hidden
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

% handle the replies from the fscoordinator_serv, which lives on the sink side.
% we stash on our side what nodes gave a busy reply so we don't send too many
% pointless whereis requests.
handle_socket_msg({location, Partition, {Node, Ip, Port}}, #state{whereis_waiting = Waiting} = State) ->
    case lists:keytake(Partition, #partition_info.index, Waiting) of
        false ->
            State;
        {value, PartitionInfo, Waiting2} ->
            Tref = PartitionInfo#partition_info.whereis_tref,
            _ = erlang:cancel_timer(Tref),
            % we don't know for sure it's no longer busy until we get a busy reply
            NewBusies = sets:del_element(Node, State#state.busy_nodes),
            State2 = State#state{whereis_waiting = Waiting2, busy_nodes = NewBusies},
            Partition2 = PartitionInfo#partition_info{node = Node, whereis_tref = undefined},
            State3 = start_fssource(Partition2, Ip, Port, State2),
            start_up_reqs(State3)
    end;
handle_socket_msg({location_busy, Partition}, #state{whereis_waiting = Waiting} = State) ->
    lager:debug("anya location_busy, partition = ~p", [Partition]),
    case lists:keytake(Partition, #partition_info.index, Waiting) of
        false ->
            State;
        {value, PartitionInfo, Waiting2} ->
            lager:info("anya Partition ~p is too busy on cluster ~p at node ~p",
                       [Partition, State#state.other_cluster, PartitionInfo#partition_info.node]),
            Tref = PartitionInfo#partition_info.whereis_tref,
            _ = erlang:cancel_timer(Tref),
            State2 = State#state{whereis_waiting = Waiting2},
            Partition2 = PartitionInfo#partition_info{whereis_tref = undefined},
            PQueue = State2#state.partition_queue,
            PQueue2 = queue:in(Partition2, PQueue),
            NewBusies = sets:add_element(Partition2#partition_info.node, State#state.busy_nodes),
            State3 = State2#state{partition_queue = PQueue2, busy_nodes = NewBusies},
            start_up_reqs(State3)
    end;
handle_socket_msg({location_busy, Partition, Node}, #state{whereis_waiting = Waiting} = State) ->
    case lists:keytake(Partition, #partition_info.index, Waiting) of
        false ->
            State;
        {value, PartitionInfo, Waiting2} ->
            lager:info("Partition ~p is too busy on cluster ~p at node ~p", [Partition, State#state.other_cluster, Node]),
            Tref = PartitionInfo#partition_info.whereis_tref,
            _ = erlang:cancel_timer(Tref),

            State2 = State#state{whereis_waiting = Waiting2},

            Partition2 = PartitionInfo#partition_info{node = Node},
            PQueue = State2#state.partition_queue,
            PQueue2 = queue:in(Partition2, PQueue),
            NewBusies = sets:add_element(Node, State#state.busy_nodes),
            State3 = State2#state{partition_queue = PQueue2, busy_nodes = NewBusies},
            start_up_reqs(State3)
    end;

handle_socket_msg({location_down, Partition}, #state{whereis_waiting=Waiting} = State) ->
    lager:warning("anya location_down, partition = ~p", [Partition]),
    case lists:keytake(Partition, #partition_info.index, Waiting) of
        false ->
            State;
        {value, PartitionInfo, Waiting2} ->
            lager:info("Partition ~p is unavailable on cluster ~p",
                [Partition, State#state.other_cluster]),
            Tref = PartitionInfo#partition_info.whereis_tref,
            _ = erlang:cancel_timer(Tref),
            Dropped = [Partition | State#state.dropped],
            #state{retry_exits = RetryExits, error_exits = ErrorExits} = State,
            State2 = State#state{whereis_waiting = Waiting2, dropped = Dropped,
                                retry_exits = RetryExits + 1,
                                error_exits = ErrorExits + 1},
            start_up_reqs(State2)
    end;
handle_socket_msg({location_down, Partition, _Node}, #state{whereis_waiting=Waiting} = State) ->
    case lists:keytake(Partition, #partition_info.index, Waiting) of
        false ->
            State;
        {value, PartitionInfo, Waiting2} ->
            Tref = PartitionInfo#partition_info.whereis_tref,
            _ = erlang:cancel_timer(Tref),
            RetryLimit = app_helper:get_env(riak_repl, max_reserve_retries, ?DEFAULT_RESERVE_RETRIES),
            lager:info("Partition ~p is unavailable on cluster ~p", [Partition, State#state.other_cluster]),
            State2 = State#state{whereis_waiting = Waiting2},
            {RetriedCount, State3} = increment_error_dict(PartitionInfo, #state.reserve_retries, State2),
            State4 = case RetriedCount of
                N when N > RetryLimit, is_integer(N) ->
                    lager:warning("Fullsync dropping partition ~p, ~p location_down failed retries", [PartitionInfo#partition_info.index, RetryLimit]),
                    Dropped = [Partition | State#state.dropped],
                    #state{retry_exits = RetryExits,
                           error_exits = ErrorExits} = State,
                    State3#state{dropped = Dropped,
                                 error_exits = ErrorExits + 1,
                                 retry_exits = RetryExits + 1};
                _ ->
                    PQueue = queue:in(PartitionInfo, State3#state.partition_queue),
                    #state{retry_exits = RetryExits} = State,
                    State3#state{partition_queue = PQueue,
                                retry_exits = RetryExits + 1}
            end,
            start_up_reqs(State4)
    end.

% try our best to reach maximum capacity by sending as many whereis requests
% as we can under the condition that we don't overload our local nodes or
% remote nodes.
start_up_reqs(State) ->
    Max = app_helper:get_env(riak_repl, max_fssource_cluster, ?DEFAULT_SOURCE_PER_CLUSTER),
    Running = length(State#state.running_sources),
    Waiting = length(State#state.whereis_waiting),
    StartupCount = Max - Running - Waiting,
    State2 = maybe_pop_from_purgatory(State),
    start_up_reqs(State2, StartupCount).

maybe_pop_from_purgatory(State) ->
    case queue:out(State#state.purgatory) of
        {empty, _} ->
            State;
        {{value, {Partition, Moment}}, NewPurgatory} ->
            pop_if_wait_time_elapsed(Partition, Moment, NewPurgatory, State)
    end.

%% @private enforce the riak_repl.fssource_retry_wait setting.
pop_if_wait_time_elapsed(Partition, Moment, NewPurgatory, State) ->
    Now = riak_core_util:moment(),
    RetryWaitTimeSecs = app_helper:get_env(riak_repl, fssource_retry_wait,
                                           ?DEFAULT_SOURCE_RETRY_WAIT_SECS),
    Elapsed = Now - Moment,
    case Elapsed >= RetryWaitTimeSecs of
        true ->
            PartitionQ2 = queue:in(Partition, State#state.partition_queue),
            State#state{purgatory = NewPurgatory, partition_queue = PartitionQ2};
        false ->
            State
    end.

start_up_reqs(State, N) when N < 1 ->
    State;
start_up_reqs(State, N) ->
    case send_next_whereis_req(State) of
        {ok, State2} ->
            start_up_reqs(State2, N - 1);
        {defer, State2} ->
            State2
    end.

% If a whereis was send, {ok, #state{}} is returned, else {defer, #state{}}
% this allows the start_up_reqs to stop early.
-spec send_next_whereis_req(State :: #state{}) -> {'defer', #state{}} | {'ok', #state{}}.
send_next_whereis_req(State) ->
    case below_max_sources(State) of
        false ->
            {defer, State};
        true ->
            {Partition, Queue} = determine_best_partition(State),
            case Partition of

                undefined when State#state.whereis_waiting == [], State#state.running_sources == [] ->
                    % something has gone wrong, usually a race condition where we
                    % handled a source exit but the source's supervisor process has
                    % not.  Another possiblity is another fullsync is in resource
                    % contention with use.  In either case, we just need to try
                    % again later.
                    lager:debug("No partition available to start, no events outstanding, trying again later"),
                    erlang:send_after(?RETRY_WHEREIS_INTERVAL, self(), send_next_whereis_req),
                    {defer, State#state{partition_queue = Queue}};

                undefined ->
                    % something may have gone wrong, but we have outstanding
                    % whereis requests or running sources, so we can wait for
                    % one of those to finish and try again
                    {defer, State#state{partition_queue = Queue}};

                P when is_record(P, partition_info) ->
                    #partition_info{index = Pval} = P,
                    #state{transport = Transport, socket = Socket, whereis_waiting = Waiting} = State,
                    Tref = erlang:send_after(?WAITING_TIMEOUT, self(), {Pval, whereis_timeout}),
                    PartitionInfo2 = P#partition_info{whereis_tref = Tref},
                    Waiting2 = [PartitionInfo2 | Waiting],
                    {ok, {PeerIP, PeerPort}} = Transport:peername(Socket),
                    lager:debug("Sending whereis request for partition ~p", [P]),
                    Transport:send(Socket,
                        term_to_binary({whereis, Pval, PeerIP, PeerPort})),
                    {ok, State#state{partition_queue = Queue, whereis_waiting =
                        Waiting2}}
            end
    end.

% two specs:  is the local node available, and does our cache of remote nodes
% say the remote node is available.
determine_best_partition(State) ->
    #state{partition_queue = Queue, busy_nodes = Busies, whereis_waiting = Waiting} = State,
    SeedPart = queue:out(Queue),
    lager:debug("Starting partition search"),
    determine_best_partition(SeedPart, Busies, Waiting, queue:new()).

determine_best_partition({empty, _Q}, _Business, _Waiting, AccQ) ->
    lager:debug("No partition in the queue that will not exceed a limit; will try again later."),
    % there is no best partition, try again later
    {undefined, AccQ};

determine_best_partition({{value, Part}, Queue}, Busies, Waiting, AccQ) ->
    case node_available(Part, Waiting) of
        false ->
            determine_best_partition(queue:out(Queue), Busies, Waiting, queue:in(Part, AccQ));
        skip ->
            determine_best_partition(queue:out(Queue), Busies, Waiting, AccQ);
        true ->
            case remote_node_available(Part, Busies) of
                false ->
                    determine_best_partition(queue:out(Queue), Busies, Waiting, queue:in(Part, AccQ));
                true ->
                    {Part, queue:join(Queue, AccQ)}
            end
    end.

% Items in the whereis_waiting list are counted toward max_sources to avoid
% sending a whereis again just because we didn't bother planning ahead.
below_max_sources(State) ->
    Max = app_helper:get_env(riak_repl, max_fssource_cluster, ?DEFAULT_SOURCE_PER_CLUSTER),
    ( length(State#state.running_sources) + length(State#state.whereis_waiting) ) < Max.

node_available(PartitionInfo, Waiting) ->
    Partition = PartitionInfo#partition_info.index,
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Owners = riak_core_ring:all_owners(Ring),
    LocalNode = proplists:get_value(Partition, Owners),
    Max = app_helper:get_env(riak_repl, max_fssource_node, ?DEFAULT_SOURCE_PER_NODE),
    try riak_repl2_fssource_sup:enabled(LocalNode) of
        RunningList ->
            PartsSameNode = [Part || {Part, PNode} <- Owners, PNode =:= LocalNode],
            PartsWaiting = [Part || #partition_info{index = Part} <- Waiting, lists:member(Part, PartsSameNode)],
            if
                ( length(PartsWaiting) + length(RunningList) ) < Max ->
                    case proplists:get_value(Partition, RunningList) of
                        undefined ->
                            true;
                        _ ->
                            false
                    end;
                true ->
                    false
            end
    catch
        exit:{noproc, _} ->
            skip;
        exit:{{nodedown, _}, _} ->
            skip
    end.

remote_node_available(Partition, _Busies) when Partition#partition_info.node =:= undefined ->
    true;
remote_node_available(Partition, Busies) ->
    not sets:is_element(Partition#partition_info.node, Busies).

start_fssource(PartitionVal, Ip, Port, State) ->
    Partition = PartitionVal#partition_info.index,
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    LocalNode = riak_core_ring:index_owner(Ring, Partition),
    case riak_repl2_fssource_sup:enable(LocalNode, Partition, {Ip, Port}) of
        {ok, Pid} ->
            link(Pid),
            _ = riak_repl2_fssource:soft_link(Pid),
            Running = lists:keystore(Pid, #partition_info.running_source, State#state.running_sources, PartitionVal#partition_info{running_source = Pid}),
            State#state{running_sources = Running};
        {error, Reason} ->
            case Reason of
                {already_started, OtherPid} ->
                    lager:info("A fullsync for partition ~p is already in"
                        " progress for ~p", [Partition,
                            riak_repl2_fssource:cluster_name(OtherPid)]);
                {{max_concurrency, Lock},_ChildSpec} ->
                    lager:debug("Fullsync for partition ~p postponed"
                                 " because ~p is at max_concurrency",
                                 [Partition, Lock]);
                _ ->
                    lager:error("Failed to start fullsync for partition ~p :"
                        " ~p", [Partition, Reason])
            end,
            #state{transport = Transport, socket = Socket} = State,
            Transport:send(Socket, term_to_binary({unreserve, Partition})),
            PQueue = queue:in(PartitionVal, State#state.partition_queue),
            State#state{partition_queue=PQueue}
    end.

largest_n(Ring) ->
    Defaults = app_helper:get_env(riak_core, default_bucket_props, []),
    Buckets = riak_core_bucket:get_buckets(Ring),
    lists:foldl(fun(Bucket, Acc) ->
                max(riak_core_bucket:n_val(Bucket), Acc)
        end, riak_core_bucket:n_val(Defaults), Buckets).

sort_partitions(Ring) ->
    BigN = largest_n(Ring),
    RawPartitions = [P || {P, _Node} <- riak_core_ring:all_owners(Ring)],
    %% tag partitions with their index, for convienience in detecting preflist
    %% collisions later
    Partitions = lists:zip(RawPartitions,lists:seq(1,length(RawPartitions))),
    %% pick a random partition in the ring
    R = rand:uniform(length(Partitions)),
    %% pretend that the ring starts at offset R
    {A, B} = lists:split(R, Partitions),
    OffsetPartitions = B ++ A,
    %% now grab every Nth partition out of the ring until there are no more
    sort_partitions(OffsetPartitions, BigN, []).

sort_partitions([], _, Acc) ->
    [#partition_info{index = P} || {P,_N} <- lists:reverse(Acc)];
sort_partitions(In, N, Acc) ->
    Split = min(length(In), N) - 1,
    {A, [P|B]} = lists:split(Split, In),
    sort_partitions(B++A, N, [P|Acc]).

refresh_stats(#state{stat_cache = Stats} = State) ->
    StatState = refresh_stats(Stats, State#state.running_sources),
    State#state{stat_cache = StatState}.

refresh_stats(StatCache, Sources) ->
    StatCache1 = maybe_exit_stat_worker(StatCache),
    StatCache2 = maybe_cancel_timer(StatCache1),
    {Pid, Mon} = erlang:spawn_monitor(?MODULE, refresh_stats_worker, [self(), Sources]),
    StatCache2#stat_cache{worker = {Pid, Mon}}.

maybe_exit_stat_worker(#stat_cache{worker = undefined} = StatCache) ->
    StatCache;

maybe_exit_stat_worker(#stat_cache{worker = {Pid, Mon}} = StatCache) ->
    % we don't want to have to wait for a message to be processed, so we're
    % just going to kill it. Since this isn't an otp process or anything
    % special, there should be no warnings about its death.
    exit(Pid, cancel),
    erlang:demonitor(Mon, [flush]),
    StatCache#stat_cache{worker = undefined}.

maybe_cancel_timer(#stat_cache{refresh_timer = undefined} = StatCache) ->
    StatCache;

maybe_cancel_timer(#stat_cache{refresh_timer = Timer} = StatCache) ->
    _ = erlang:cancel_timer(Timer),
    receive
        refresh_stats -> ok
    after 0 ->
        ok
    end,
    StatCache#stat_cache{refresh_timer = undefined}.

schedule_stat_refresh(StatCache) ->
    StatCache1 = maybe_cancel_timer(StatCache),
    Timer = erlang:send_after(StatCache#stat_cache.refresh_interval, self(), refresh_stats),
    StatCache1#stat_cache{refresh_timer = Timer}.

%% @private Exported just to be able to spawn with arguments more nicely.
refresh_stats_worker(ReportTo, Sources) ->
    lager:debug("Gathering source data for ~p", [Sources]),
    SourceStats = gather_source_stats(Sources),
    Time = riak_core_util:moment(),
    Self = self(),
    ReportTo ! {stat_update, Self, Time, SourceStats}.

gather_source_stats(PDict) ->
    gather_source_stats(PDict, []).

gather_source_stats([], Acc) ->
    lists:reverse(Acc);

gather_source_stats([PartitionInfo | Tail], Acc) ->
    Pid = PartitionInfo#partition_info.running_source,
    try riak_repl2_fssource:legacy_status(Pid, infinity) of
        Stats ->
            gather_source_stats(Tail, [{riak_repl_util:safe_pid_to_list(Pid), Stats} | Acc])
    catch
        exit:Y ->
            lager:notice("getting source info failed due to exit:~p", [Y]),
            gather_source_stats(Tail, [{riak_repl_util:safe_pid_to_list(Pid), []} | Acc])
    end.

is_fullsync_in_progress(State) ->
    QEmpty = queue:is_empty(State#state.partition_queue),
    PurgatoryEmpty = queue:is_empty(State#state.purgatory),
    Waiting = State#state.whereis_waiting,
    Running = State#state.running_sources,
    case {QEmpty, PurgatoryEmpty, Waiting, Running} of
        {true, true, [], []} ->
            false;
        _ ->
            true
    end.

maybe_complete_fullsync(Running, State) ->
    EmptyRunning =  Running == [],
    QEmpty = queue:is_empty(State#state.partition_queue),
    PurgatoryEmpty = queue:is_empty(State#state.purgatory),
    Waiting = State#state.whereis_waiting,
    case {EmptyRunning, QEmpty, PurgatoryEmpty, Waiting} of
        {true, true, true, []} ->
            MyClusterName = riak_core_connection:symbolic_clustername(),
            lager:info("Fullsync complete from ~s to ~s",
                       [MyClusterName, State#state.other_cluster]),
            % clear the "rt dirty" stat if it's set,
            % otherwise, don't do anything
            State2 = notify_rt_dirty_nodes(State),
            %% update legacy stats too! some riak_tests depend on them.
            riak_repl_stats:server_fullsyncs(),
            TotalFullsyncs = State#state.fullsyncs_completed + 1,
            Finish = riak_core_util:moment(),
            ElapsedSeconds = Finish - State#state.fullsync_start_time,
            riak_repl_util:schedule_cluster_fullsync(State#state.other_cluster),
            {noreply, State2#state{running_sources = Running,
                                   fullsyncs_completed = TotalFullsyncs,
                                   fullsync_start_time = undefined,
                                   last_fullsync_duration=ElapsedSeconds,
                                   last_fullsync_completed = Finish
                                  }};
        _ ->
            % there's something waiting for a response.
            State2 = start_up_reqs(State#state{running_sources = Running}),
            {noreply, State2}
    end.

% dirty_nodes is the set of nodes that are marked "dirty"
% due to a realtime repl issue while fullsync isn't running.
% dirty_nodes_during_fs is the set of nodes that are marked "dirty"
% due to a realtime repl issue while fullsync IS running.
% After a fullsync, notify all dirty_nodes that fullsync is complete
% and they are now clear/clean. Also, move any nodes that were marked
% as dirty_nodes_during_fs to dirty_nodes, as they should be cleared out
% during the next fullsync. If any state is lost in the coordinator,
% a dirty node won't lose it's dirty state as a persistent file is kept
% on that node.
notify_rt_dirty_nodes(State = #state{dirty_nodes = DirtyNodes,
                                     dirty_nodes_during_fs =
                                     DirtyNodesDuringFS}) ->
    State1 = case ordsets:size(DirtyNodes) > 0 of
        true ->
            lager:debug("Notifying dirty nodes after fullsync"),
            % notify all nodes in case some weren't registered with the coord
            AllNodesList = riak_core_node_watcher:nodes(riak_repl),
            NodesToNotify = lists:subtract(AllNodesList,
                                           ordsets:to_list(DirtyNodesDuringFS)),
            lager:debug("Notifying nodes ~p", [ NodesToNotify]),
            _ = rpc:multicall(NodesToNotify, riak_repl_stats, clear_rt_dirty, []),
            State#state{dirty_nodes=ordsets:new()};
        false ->
            lager:debug("No dirty nodes before fullsync started"),
            State
    end,
    case ordsets:size(DirtyNodesDuringFS) > 0 of
        true ->
            lager:debug("Nodes marked dirty during fullsync"),
            % move dirty_nodes_during_fs to dirty_nodes so they will be
            % cleaned out during the next fullsync
            State#state{dirty_nodes = DirtyNodesDuringFS,
                          dirty_nodes_during_fs = ordsets:new()};
        false ->
            lager:debug("No nodes marked dirty during fullsync"),
            State1
        end.

nodeset_to_string_list(Set) ->
    string:join([erlang:atom_to_list(V) || V <- ordsets:to_list(Set)],",").

increment_error_dict(PartitionInfo, ExitType, State) when is_record(PartitionInfo, partition_info) ->
    increment_error_dict(PartitionInfo#partition_info.index, ExitType, State);

increment_error_dict(PartitionIndex, soft_exit, State) ->
    increment_error_dict(PartitionIndex, #state.soft_retries, State);

increment_error_dict(PartitionIndex, 'EXIT', State) ->
    increment_error_dict(PartitionIndex, #state.retries, State);

increment_error_dict(PartitionIndex, ElementN, State) when is_integer(ElementN) ->
    Dict = element(ElementN, State),
    Dict2 = dict:update_counter(PartitionIndex, 1, Dict),
    State2 = setelement(ElementN, State, Dict2),
    {dict:fetch(PartitionIndex, Dict2), State2}.

% If we are linked to a remote pid, it is possible for the disterl to
% reconnect at a bad time and lose the exit message, thus we cannot
% rely solely on the exit message to flush it. What we can do is monitor
% the process. 'DOWN' messages always come in after the exit message, so
% if we get the 'DOWN' message first, we know the exit message is never
% going to arrive, and is effectively flushed. If we get the exit first,
% we can flush the down message next, since that will arrive as a noproc
% in any case.
flush_exit_message(Pid) ->
    Mon = erlang:monitor(process, Pid),
    receive
        {'EXIT', Pid, _} ->
            receive
                {'DOWN', Mon, process, Pid, _} ->
                    ok
            end;
        {'DOWN', Mon, process, Pid, _} ->
            ok
    end.

-ifdef(TEST).

%% tests that the retry count and wait settings are respected.  Uses
% mock for controlling time.
handle_abnormal_exit_test_() ->
    {setup,
     fun setup/0,
     fun teardown/1,
     [{"test purgatory wait time", fun test_purgatory_wait_time/0},
      {"test retry count", fun test_retry_count/0}]
    }.

%% @doc
%% Test for changes for riak_repl#772. Checks that a soft-exiting
%% partition sync attempt is not popped from purgatory until the
%% configured retry wait time is passed.
%% @see setup/0, teardown/1
test_purgatory_wait_time() ->
    Index = 0,
    FakeNode = 'not@anode.net',
    FakePid = fake_pid(),
    PartInfo = #partition_info{running_source= FakePid, index=Index, node=FakeNode},
    State =
        #state{running_sources=[PartInfo],
                transport=mock_transport,
                fullsync_start_time = riak_core_util:moment()},
    meck:expect(riak_core_util, moment, [],
                meck:seq([1, 2, 6])),

    meck:expect(mock_transport, send, ['_', '_'], meck:val(ok)),

    %% fail to sync, soft-exit, not_built
    {noreply, State2=#state{purgatory=Purgatory, partition_queue=PQ}} = handle_abnormal_exit(soft_exit,
                                                                                             FakePid,
                                                                                             not_built,
                                                                                             {value, PartInfo, []}, State),

    %% the failing/soft-exiting partition should've been added to
    %% purgatory, with a time of 1
    ?assertEqual(1, queue:len(Purgatory)),
    ?assertEqual(0, queue:len(PQ)),
    QElem = queue:get(Purgatory),
    Expected = PartInfo#partition_info{running_source=undefined},
    ?assertEqual({Expected, 1}, QElem),

    %% the purgatory member should _not_ be popped (yet) as time is
    %% only 2
    State3 = #state{purgatory=Purgatory2, partition_queue=PQ2} = maybe_pop_from_purgatory(State2),
    ?assertEqual(1, queue:len(Purgatory2)),
    ?assertEqual(0, queue:len(PQ2)),
    QElem2 = queue:get(Purgatory2),
    Expected2 = PartInfo#partition_info{running_source=undefined},
    ?assertEqual({Expected2, 1}, QElem2),

    %% next call to moment returns 6, 6 - 1 >= fssource_retry_wait, therefore should be
    %% copied from purgatory to partition queue
    #state{purgatory=Purgatory3, partition_queue=PQ3} = maybe_pop_from_purgatory(State3),
    ?assertEqual(0, queue:len(Purgatory3)),
    ?assertEqual(1, queue:len(PQ3)),
    PQElem = queue:get(PQ3),
    ?assertEqual(Expected2, PQElem),

    meck:validate(mock_transport),
    ok.

%% @doc
%% Test for changes for riak_repl#772. Checks that a soft-exiting
%% partition sync attempt fails when retry count (when count <
%% infinity) is reached
%% @see setup/0, teardown/1
test_retry_count() ->
    meck:expect(riak_core_util, moment, [],
                meck:seq(lists:seq(1, 100, 6))),

    meck:expect(mock_transport, send, ['_', '_'], meck:val(ok)),

    {ok, MaxRetries} = application:get_env(riak_repl, max_fssource_soft_retries),
    MaxRetriesExpected = MaxRetries+1, %% NOTE: in abnormal_exits the test is ErrorCount > Limit

    #state{purgatory=Purgatory,
           dropped=Dropped,
           error_exits=ErrorExits} =
        retry(0,
                MaxRetriesExpected,
                #state{transport=mock_transport,
                        fullsync_start_time = riak_core_util:moment()}),

    ?assertEqual(1, ErrorExits),
    ?assertEqual(1, length(Dropped)),
    ?assertEqual(0, hd(Dropped)),
    ?assertEqual(0, queue:len(Purgatory)),

    meck:validate(mock_transport),
    ok.

retry(MaxRetries, MaxRetries, State) ->
    State;
retry(Attempt, MaxRetries, State) ->
    Index = 0,
    FakeNode = 'not@anode.net',
    FakePid = fake_pid(),
    PartInfo = #partition_info{running_source= FakePid, index=Index, node=FakeNode},
    %% fake removing from purgatory and setting up as running, too
    %% complex to mock
    State2 = State#state{running_sources=[PartInfo], purgatory=queue:new()},
    {noreply, State3} = handle_abnormal_exit(soft_exit, FakePid,  not_built,
                                             {value, PartInfo, []}, State2),
    retry(Attempt+1, MaxRetries, State3).

%% need a real pid for the calls to monitor in the code under test.
fake_pid() ->
    spawn(fun() ->
                  whatever
          end).

setup() ->
    %% play nice with others
    OrigRetries = application:get_env(riak_repl, max_fssource_soft_retries, ?DEFAULT_SOURCE_SOFT_RETRIES),
    OrigWait = application:get_env(riak_repl, fssource_retry_wait, ?DEFAULT_SOURCE_RETRY_WAIT_SECS),
    %% set the app_env vars
    application:set_env(riak_repl, max_fssource_soft_retries, 3),
    application:set_env(riak_repl, fssource_retry_wait, 5),
    %% control time with mocking, only want to mock one fun (moment)
    meck:new(riak_core, [passthrough]),
    %% make a fake transport
    meck:new(mock_transport, [non_strict]),
    meck:new(riak_core_connection),
    meck:expect(riak_core_connection,
                symbolic_clustername,
                fun() -> "test_cluster" end),
    {OrigRetries, OrigWait}.

teardown({OrigRetries, OrigWait}) ->
    meck:unload(riak_core),
    meck:unload(mock_transport),
    application:set_env(riak_repl, max_fssource_soft_retries, OrigRetries),
    application:set_env(riak_repl, fssource_retry_wait, OrigWait).

-endif.
