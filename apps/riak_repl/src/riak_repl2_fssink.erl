-module(riak_repl2_fssink).
-include("riak_repl.hrl").

%% @doc fssink
%%
%% This module is responsible, at a high level, for accomplishing the full sync
%% of a single partition. The partition to synchronize is transmitted from the
%% connected fssource on the source cluster. The strategy for fullsync is determined
%% dynamically as well:
%%   keylist - used if the protocol version is &lt; 2.0
%%   aae     - used if the protocol version is >= 2.0 and AAE is enabled and repl_strategy=aae
%%
%% Protocol Support Summary:
%% -------------------------
%% 1,0 and up supports keylist strategy
%% 1,1 and up supports binary object
%% 2,0 and up supports AAE strategy
%%
%% For keylist, the "old" tcp_server is used. That module is capable of handling multiple
%% sequential partitions, but we only use it to sync a single partition and then we stop it.
%% For aae, the repl_aae_sink is desinged to process a single partition and be stopped.
%%
%% If the protocol version >= 2.0, and the cluster has aae enabled and the replication
%% configuration stanza enables the aae strategy (the default is keylist), then aae strategy
%% will be selected. It is negotiated as follows: if the version is >= 2.0, then upon
%% accepting a connection from the soruce, we send our capabilities to the the source. The
%% source then sends it's caps to us. This is all done by the fssource and fssink modules
%% in order to decide which strategy will be used. Once caps are exchanged, both sides can
%% mutually agree on the strategy (aae if both sides announced it in their caps). If the
%% node does not have aae enabled in riak_kv or aae strategy is not enabled in riak_repl,
%% then the caps will not include the "aae" strategy.
%%
%% Once the strategy is settled, the sockets are handled to the appropriate spawned
%% fullssync worker processes.

-behaviour(gen_server).
%% API
-export([start_link/4, sync_register_service/0, start_service/5, legacy_status/2, fullsync_complete/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {
        transport,
        socket,
        cluster,
        fullsync_worker,
        work_dir = undefined,
        strategy = keylist :: keylist | aae,
        proto,
        ver              % highest common wire protocol in common with fs source
    }).

start_link(Socket, Transport, Proto, Props) ->
    gen_server:start_link(?MODULE, [Socket, Transport, Proto, Props], []).

fullsync_complete(Pid) ->
    %% cast to avoid deadlock in terminate
    gen_server:cast(Pid, fullsync_complete).

%% @doc Register with service manager
sync_register_service() ->
    %% 1,0 and up supports keylist strategy
    %% 1,1 and up supports binary object
    %% 2,0 and up supports AAE strategy
    %% 3,0 and up supports Typed Buckets
    ProtoPrefs = {fullsync,[{1,1}, {2,0}, {3,0}]},
    TcpOptions = [{keepalive, true}, % find out if connection is dead, this end doesn't send
                  {packet, 4},
                  {active, false},
                  {nodelay, true}],
    HostSpec     = {ProtoPrefs, {TcpOptions, ?MODULE, start_service, undefined}},
    riak_core_service_mgr:sync_register_service(HostSpec, {round_robin, undefined}).

%% Callback from service manager
start_service(Socket, Transport, Proto, _Args, Props) ->
    {ok, Pid} = riak_repl2_fssink_sup:start_child(Socket, Transport,
        Proto, Props),
    ok = Transport:controlling_process(Socket, Pid),
    Pid ! init_ack,
    {ok, Pid}.

legacy_status(Pid, Timeout) ->
    gen_server:call(Pid, legacy_status, Timeout).

%% gen server

init([Socket, Transport, OKProto, Props]) ->
    %% TODO: remove annoying 'ok' from service mgr proto
    {ok, Proto} = OKProto,
    Ver = riak_repl_util:deduce_wire_version_from_proto(Proto),
    SocketTag = riak_repl_util:generate_socket_tag("fs_sink", Transport, Socket),
    lager:debug("Negotiated ~p with ver ~p", [Proto, Ver]),
    lager:debug("Keeping stats for " ++ SocketTag),
    riak_core_tcp_mon:monitor(Socket, {?TCP_MON_FULLSYNC_APP, sink,
                                       SocketTag}, Transport),

    Cluster = proplists:get_value(clustername, Props),
    lager:debug("fullsync connection (ver ~p) from cluster ~p", [Ver, Cluster]),
    {ok, #state{proto=Proto, socket=Socket, transport=Transport, cluster=Cluster, ver=Ver}}.

handle_call(legacy_status, _From, State=#state{fullsync_worker=FSW,
                                               socket=Socket, strategy=Strategy}) ->
    Res = case is_pid(FSW) andalso is_process_alive(FSW) of
              true ->
                  case Strategy of
                      keylist ->
                          gen_fsm_compat:sync_send_all_state_event(FSW, status);
                      aae ->
                          gen_server:call(FSW, status, ?LONG_TIMEOUT)
                  end;
              false -> []
          end,
    SocketStats = riak_core_tcp_mon:socket_status(Socket),
    Desc =
        [
            {node, node()},
            {site, State#state.cluster},
            {strategy, Strategy},
            {fullsync_worker, riak_repl_util:safe_pid_to_list(State#state.fullsync_worker)},
            {socket, riak_core_tcp_mon:format_socket_stats(SocketStats, [])}
        ],
    {reply, Desc ++ Res, State};

handle_call(_Msg, _From, State) ->
    {reply, ok, State}.

handle_cast(fullsync_complete, State) ->
    %% sent from AAE fullsync worker
    %% TODO: The sink state should include the partition ID
    %% or some other useful information
    lager:info("Fullsync of partition complete."),
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
    %% aae strategy will not receive messages here
    Transport:setopts(Socket, [{active, once}]),
    case binary_to_term(Data) of
        {fs_diff_obj, BinObj} ->
            RObj = riak_repl_util:decode_bin_obj(BinObj),
            riak_repl_util:do_repl_put(RObj);
        Other ->
            gen_fsm_compat:send_event(State#state.fullsync_worker, Other)
    end,
    {noreply, State};
handle_info(init_ack, State=#state{socket=Socket,
                                   transport=Transport,
                                   proto=Proto,
                                   cluster=Cluster}) ->
    {_Proto,{CommonMajor,_CMinor},{CommonMajor,_HMinor}} = Proto,

    %% possibly exchange fullsync capabilities with the remote
    OurCaps = decide_our_caps(CommonMajor),
    TheirCaps = maybe_exchange_caps(CommonMajor, OurCaps, Socket, Transport),
    Strategy = decide_common_strategy(OurCaps, TheirCaps),

    case Strategy of
        keylist ->
            %% Keylist server strategy
            Transport:setopts(Socket, [{active, once}]),
            {ok, WorkDir} = riak_repl_fsm_common:work_dir(Transport, Socket, Cluster),
            {ok, FullsyncWorker} = riak_repl_keylist_client:start_link(Cluster, Transport,
                                                                       Socket, WorkDir),
            {noreply, State#state{cluster=Cluster, fullsync_worker=FullsyncWorker, work_dir=WorkDir,
                                  strategy=keylist}};
        aae ->
            %% AAE strategy
            {ok, FullsyncWorker} = riak_repl_aae_sink:start_link(Cluster, Transport, Socket, self()),
            ok = Transport:controlling_process(Socket, FullsyncWorker),
            riak_repl_aae_sink:init_sync(FullsyncWorker),
            {noreply, State#state{cluster=Cluster, fullsync_worker=FullsyncWorker, strategy=aae}}
    end;
handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, #state{fullsync_worker=FSW, work_dir=WorkDir, strategy=Strategy}) ->
    %% TODO: define a fullsync worker behavior and call it's stop function
    case is_pid(FSW) andalso is_process_alive(FSW) of
        true ->
            case Strategy of
                keylist ->
                    gen_fsm_compat:sync_send_all_state_event(FSW, stop);
                aae ->
                    gen_server:call(FSW, stop, ?LONG_TIMEOUT)
            end;
        _ ->
            ok
    end,
    %% clean up work dir
    case WorkDir of
        undefined ->
            ok;
        _Other ->
            Cmd = lists:flatten(io_lib:format("rm -rf ~s", [WorkDir])),
            os:cmd(Cmd)
    end.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

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

%% Based on the agreed common protocol level and the supported
%% mode of AAE, decide what strategy we are capable of offering.
decide_our_caps(CommonMajor) ->
    SupportedStrategy =
        case {riak_kv_entropy_manager:enabled(), CommonMajor} of
            {_,1} -> keylist;
            {false,_} -> keylist;
            {true,_} -> aae
        end,
    [{strategy, SupportedStrategy}].

%% Depending on the protocol version number, send our capabilities
%% as a list of properties, in binary.
maybe_exchange_caps(1, _Caps, _Socket, _Transport) ->
    [];
maybe_exchange_caps(_, Caps, Socket, Transport) ->
    Transport:send(Socket, term_to_binary(Caps)),
    case Transport:recv(Socket, 0, ?PEERINFO_TIMEOUT) of
        {ok, Data} ->
            binary_to_term(Data);
        {Error, Socket} ->
            throw(Error);
        {Error, Socket, Reason} ->
            throw({Error, Reason})
    end.


