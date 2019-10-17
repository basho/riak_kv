-module(riak_repl2_fssource).
-include("riak_repl.hrl").

-behaviour(gen_server).
%% API
-export([start_link/2, start_link/3, connected/6, connect_failed/3,
    start_fullsync/1, stop_fullsync/1, fullsync_complete/1,
    cluster_name/1, legacy_status/2, soft_link/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {
        transport,
        socket,
        ip,
        partition,
        cluster,
        connection_ref,
        fullsync_worker,
        work_dir,
        strategy,
        owner
    }).

start_link(Partition, IP) ->
    start_link(Partition, IP, undefined).

start_link(Partition, IP, Owner) ->
    gen_server:start_link(?MODULE, [Partition, IP, Owner], []).

%% connection manager callbacks
connected(Socket, Transport, Endpoint, Proto, Pid, Props) ->
    Transport:controlling_process(Socket, Pid),
    gen_server:call(Pid,
        {connected, Socket, Transport, Endpoint, Proto, Props}, ?LONG_TIMEOUT).

connect_failed(_ClientProto, Reason, RtSourcePid) ->
    gen_server:cast(RtSourcePid, {connect_failed, self(), Reason}).

start_fullsync(Pid) ->
    gen_server:call(Pid, start_fullsync, ?LONG_TIMEOUT).

stop_fullsync(Pid) ->
    gen_server:call(Pid, stop_fullsync, ?LONG_TIMEOUT).

fullsync_complete(Pid) ->
    %% cast to avoid deadlock in terminate
    gen_server:cast(Pid, fullsync_complete).

%% get the cluster name
cluster_name(Pid) ->
    gen_server:call(Pid, cluster_name, ?LONG_TIMEOUT).

legacy_status(Pid, Timeout) ->
    gen_server:call(Pid, legacy_status, Timeout).

%% @doc Create a 'soft link' between the calling process and the fssource.
%% A soft-link allows for a soft_exit message to be sent before a normal
%% exit to any process that has created a soft link. Only one link is
%% held at a time, and alink is in only one direction (the fssource
%% reports to calling process).
soft_link(Pid) ->
    % not using default long timeout because this is primarily used by the
    % fscoordinator, and we don't want to potentially block that for up to
    % 2 minutes. 15 seconds is bad enough in a worst case scenario.
    try gen_server:call(Pid, {soft_link, self()}, timer:seconds(15)) of
        ok -> % older versions returned 'ok' for the catchall
            false;
        true ->
            true
    catch
        _What:Reason ->
            lager:debug("Could not create soft link to ~p from ~p due to ~p", [Pid, self(), Reason]),
            {error, Reason}
    end.

%% gen server

init([Partition, IP]) ->
    init([Partition, IP, undefined]);

init([Partition, IP, Owner]) ->

    RequestedStrategy = app_helper:get_env(riak_repl,
                                           fullsync_strategy,
                                           ?DEFAULT_FULLSYNC_STRATEGY),

    %% Determine what kind of fullsync worker strategy we want to start with,
    %% which could change if we talk to the sink and it can't speak AAE. If
    %% AAE is not enabled in KV, then we can't use aae strategy.
    OurCaps = decide_our_caps(RequestedStrategy),
    SupportedStrategy = proplists:get_value(strategy, OurCaps),

    %% Possibly try to obtain the per-vnode lock before connecting.
    %% If we return error, we expect the coordinator to start us again later.
    case riak_repl_util:maybe_get_vnode_lock(Partition) of
        ok ->
            %% got the lock, or ignored it.
            case connect(IP, SupportedStrategy, Partition) of
                {error, Reason} ->
                    {stop, Reason};
                {ok, State}->
                    {ok, State#state{owner = Owner}}
            end;
        {error, Reason} ->
            %% the vnode is probably busy. Try again later.
            {stop, Reason}
    end.

handle_call({connected, Socket, Transport, _Endpoint, Proto, Props},
            _From, State=#state{ip=IP, partition=Partition, strategy=RequestedStrategy}) ->
    Cluster = proplists:get_value(clustername, Props),
    lager:info("Fullsync connection to ~p for ~p",[IP, Partition]),

    SocketTag = riak_repl_util:generate_socket_tag("fs_source", Transport, Socket),
    lager:debug("Keeping stats for " ++ SocketTag),
    riak_core_tcp_mon:monitor(Socket, {?TCP_MON_FULLSYNC_APP, source,
                                       SocketTag}, Transport),

    %% Strategy still depends on what the sink is capable of.
    {_Proto,{CommonMajor,_CMinor},{CommonMajor,_HMinor}} = Proto,

    OurCaps = decide_our_caps(RequestedStrategy),
    TheirCaps = maybe_exchange_caps(CommonMajor, OurCaps, Socket, Transport),
    Strategy = decide_common_strategy(OurCaps, TheirCaps),
    {_, ClientVer, _} = Proto,

    case Strategy of
        keylist ->
            %% Keylist server strategy
            {ok, WorkDir} = riak_repl_fsm_common:work_dir(Transport, Socket, Cluster),
            {ok, Client} = riak:local_client(),
            %% We maintain ownership of the socket. We will consume TCP messages in handle_info/2
            Transport:setopts(Socket, [{active, once}]),
            {ok, FullsyncWorker} = riak_repl_keylist_server:start_link(Cluster,
                                                                       Transport, Socket,
                                                                       WorkDir, Client, ClientVer),
            _ = riak_repl_keylist_server:start_fullsync(FullsyncWorker, [Partition]),
            {reply, ok, State#state{transport=Transport, socket=Socket, cluster=Cluster,
                                    fullsync_worker=FullsyncWorker, work_dir=WorkDir,
                                    strategy=keylist}};
        aae ->
            %% AAE strategy
            {ok, Client} = riak:local_client(),
            {ok, FullsyncWorker} = riak_repl_aae_source:start_link(Cluster,
                                                                   Client, Transport,
                                                                   Socket, Partition,
                                                                   self(), Proto),
            %% Give control of socket to AAE worker. It will consume all TCP messages.
            ok = Transport:controlling_process(Socket, FullsyncWorker),
            riak_repl_aae_source:start_exchange(FullsyncWorker),
            {reply, ok,
             State#state{transport=Transport, socket=Socket, cluster=Cluster,
                         fullsync_worker=FullsyncWorker, work_dir=undefined,
                         strategy=aae}}
    end;

handle_call(start_fullsync, _From, State=#state{fullsync_worker=FSW,
                                                strategy=Strategy}) ->
    case Strategy of
        keylist ->
            riak_repl_keylist_server:start_fullsync(FSW);
        aae ->
            ok
    end,
    {reply, ok, State};
handle_call(stop_fullsync, _From, State=#state{fullsync_worker=FSW,
                                               strategy=Strategy}) ->
    case Strategy of
        keylist ->
            riak_repl_keylist_server:cancel_fullsync(FSW);
        aae ->
            riak_repl_aae_source:cancel_fullsync(FSW)
    end,
    {reply, ok, State};
handle_call(legacy_status, _From, State=#state{fullsync_worker=FSW,
                                               socket=Socket,
                                               strategy=Strategy}) ->
    lager:debug("Sending status to ~p", [FSW]),
    Res = case is_pid(FSW) of
        true ->
            % try/catch because there may be a message in the pid's
            % mailbox that will cause it to exit before it gets to our
            % status request message.
            try gen_fsm_compat:sync_send_all_state_event(FSW, status, infinity) of
                SyncSendRes ->
                    SyncSendRes
            catch
                What:Why ->
                    lager:notice("Error getting fullsync worker ~p status: ~p:~p", [FSW, What, Why]),
                    []
            end;
        false ->
            []
    end,
    SocketStats = riak_core_tcp_mon:format_socket_stats(
        riak_core_tcp_mon:socket_status(Socket), []),
    Desc =
        [
            {node, node()},
            {site, State#state.cluster},
            {strategy, Strategy},
            {fullsync_worker, riak_repl_util:safe_pid_to_list(FSW)},
            {socket, SocketStats}
        ],
    {reply, Desc ++ Res, State};
handle_call(cluster_name, _From, State) ->
    Name = case State#state.cluster of
        undefined ->
            {connecting, State#state.ip};
        ClusterName ->
            ClusterName
    end,
    {reply, Name, State};
handle_call({soft_link, NewOwner}, _From, State) ->
    lager:debug("Changing soft_link from ~p to ~p", [State#state.owner, NewOwner]),
    State2 = State#state{owner = NewOwner},
    {reply, true, State2};
handle_call(_Msg, _From, State) ->
    {reply, ok, State}.

handle_cast(not_responsible, State=#state{partition=Partition}) ->
    lager:info("Fullsync of partition ~p stopped because AAE trees can't be compared.", [Partition]),
    lager:info("Probable cause is one or more differing bucket n_val properties between source and sink clusters."),
    lager:info("Restarting fullsync connection for partition ~p with keylist strategy.", [Partition]),
    Strategy = keylist,
    case connect(State#state.ip, Strategy, Partition) of
        {ok, State2} ->
            {noreply, State2};
        {error, Reason} ->
            {stop, Reason, State}
    end;
handle_cast(fullsync_complete, State=#state{partition=Partition}) ->
    %% sent from AAE fullsync worker
    lager:info("Fullsync for partition ~p complete.", [Partition]),
    {stop, normal, State};
handle_cast({connect_failed, _Pid, Reason},
     State = #state{cluster = Cluster}) ->
     lager:warning("Fullsync replication connection to cluster ~p failed ~p",
        [Cluster, Reason]),
    {stop, normal, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({Closed, Socket}, State=#state{socket=Socket})
        when Closed == tcp_closed; Closed == ssl_closed ->
    lager:info("Connection for site ~p closed", [State#state.cluster]),
    {stop, normal, State};
handle_info({Error, _Socket, Reason}, State)
        when Error == tcp_error; Error == ssl_error ->
    lager:error("Connection for site ~p closed unexpectedly: ~p",
        [State#state.cluster, Reason]),
    {stop, normal, State};
handle_info({Proto, Socket, Data},
        State=#state{socket=Socket,transport=Transport}) when Proto==tcp; Proto==ssl ->
    Transport:setopts(Socket, [{active, once}]),
    Msg = binary_to_term(Data),
    case Msg == fullsync_complete of
        true ->
            %% sent from the keylist_client when it's done.
            %% stop on fullsync completion, which will call
            %% our terminate function and stop the keylist_server.
            {stop, normal, State};
        _ ->
            gen_fsm_compat:send_event(State#state.fullsync_worker, Msg),
            {noreply, State}
    end;
handle_info({soft_exit, Pid, Reason}, State = #state{fullsync_worker = Pid}) ->
    lager:debug("Fullsync worker exited normally, but really wanted it to be ~p", [Reason]),
    maybe_soft_exit(Reason, State);
handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, #state{fullsync_worker=FSW, work_dir=WorkDir}) ->
    %% try to exit the fullsync worker; if we're dying because it did,
    %% don't worry about the error (cause it's already dead).
    case is_pid(FSW) of
        false ->
            ok;
        true ->
            catch gen_fsm_compat:sync_send_all_state_event(FSW, stop) 
    end,
    case WorkDir of
        undefined -> ok;
        _ ->
            %% clean up work dir
            Cmd = lists:flatten(io_lib:format("rm -rf ~s", [WorkDir])),
            os:cmd(Cmd)
    end.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%% Based on the agreed common protocol level and the supported
%% mode of AAE, decide what strategy we are capable of offering.
decide_our_caps(RequestedStrategy) ->
    SupportedStrategy =
        case {riak_kv_entropy_manager:enabled(), RequestedStrategy} of
            {false, _} -> keylist;
            {true, aae} -> aae;
            {true, keylist} -> keylist;
            {true, _UnSupportedStrategy} -> RequestedStrategy
        end,
    [{strategy, SupportedStrategy}].

%% decide what strategy to use, given our own capabilties and those
%% of the remote source.
decide_common_strategy(_OurCaps, []) -> keylist;
decide_common_strategy(OurCaps, TheirCaps) ->
    OurStrategy = proplists:get_value(strategy, OurCaps, keylist),
    TheirStrategy = proplists:get_value(strategy, TheirCaps, keylist),
    case {OurStrategy,TheirStrategy} of
        {aae,aae} -> aae;
        {_,_}     -> keylist
    end.

%% Depending on the protocol version number, send our capabilities
%% as a list of properties, in binary.
maybe_exchange_caps(1, _Caps, _Socket, _Transport) ->
    [];
maybe_exchange_caps(_, Caps, Socket, Transport) ->
    TheirCaps =
        case Transport:recv(Socket, 0, ?PEERINFO_TIMEOUT) of
            {ok, Data} ->
                binary_to_term(Data);
            {Error, Socket} ->
                throw(Error);
            {Error, Socket, Reason} ->
                throw({Error, Reason})
        end,
    Transport:send(Socket, term_to_binary(Caps)),
    TheirCaps.

%% Start a connection to the remote sink node at IP, using the given fullsync strategy,
%% for the given partition. The protocol version will be determined from the strategy.
connect(IP, Strategy, Partition) ->
    lager:debug("Connecting to remote ~p for partition ~p", [IP, Partition]),
    TcpOptions = [{keepalive, true},
                  {nodelay, true},
                  {packet, 4},
                  {active, false}],

    %% 1,1 support for binary object
    %% 2,0 support for AAE fullsync + binary objects
    %% 3,0 support for typed buckets
    ClientSpec = {{fullsync,[{3,0}, {2,0}, {1,1}]}, {TcpOptions, ?MODULE, self()}},

    case riak_core_connection_mgr:connect({identity, IP}, ClientSpec) of
        {ok, Ref} ->
            {ok, #state{strategy = Strategy, ip = IP,
                        connection_ref = Ref, partition=Partition}};
        {error, Reason}->
            lager:warning("Error connecting to remote ~p for partition ~p", [IP, Partition]),
            {error, Reason}
    end.

maybe_soft_exit(Reason, State) ->
    case State#state.owner of
        undefined ->
            {stop, Reason, State};
        Owner ->
            Owner ! {soft_exit, self(), Reason},
            {stop, normal, State}
    end.

