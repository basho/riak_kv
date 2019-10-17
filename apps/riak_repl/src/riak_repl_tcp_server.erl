%% Riak EnterpriseDS
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.

%% @doc This module is responsible for the server-side TCP communication
%% during replication. A seperate instance of this module is started for every
%% replication connection that is established. A handshake with the client is
%% then exchanged and, using that information, certain protocol extensions are
%% enabled and the fullsync strategy is negotiated.
%%
%% This module handles the realtime part of the replication itself, but all
%% the details of the fullsync replication are delegated to the negotiated
%% fullsync worker, which implements its own protocol. Any unrecognized
%% messages received over the TCP connection are sent to the fullsync process.
%%
%% Realtime replication is quite simple. Using a postcommit hook, writes to
%% the cluster are sent to the replication leader, which will then forward
%% the update out to any connected replication sites. An optional protocol
%% extension is to use a bounded queue to throttle the stream of updates.
-module(riak_repl_tcp_server).

-include("riak_repl.hrl").

-behaviour(gen_server).

%% API
-export([start_link/4, set_socket/2, send/3, status/1, status/2]).
-export([start_fullsync/1, cancel_fullsync/1, pause_fullsync/1,
        resume_fullsync/1, handle_peerinfo/3, make_state/6]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {
        sitename :: repl_sitename() | undefined,
        socket :: repl_socket(),
        transport :: 'ranch_tcp' | 'ranch_ssl',
        listener :: pid() | undefined,
        client :: tuple() | undefined,
        q :: undefined | pid(),
        work_dir :: string() | undefined,   %% working directory for this repl session
        my_pi :: #peer_info{} | undefined,
        their_pi :: #peer_info{} |undefined,
        fullsync_worker :: pid() | undefined,
        fullsync_strategy :: atom() |undefined,
        election_timeout :: undefined | reference(), % reference for the election timeout
        keepalive_time :: undefined | integer(),
        ver = w0
    }).

make_state(Sitename, Transport, Socket, MyPI, WorkDir, Client) ->
    #state{sitename=Sitename, socket=Socket, transport=Transport, my_pi=MyPI,
           work_dir=WorkDir, client=Client}.

start_link(Listener, Socket, Transport, _Opts) ->
    gen_server:start_link(?MODULE, [Listener, Socket, Transport], []).

set_socket(Pid, Socket) ->
    gen_server:call(Pid, {set_socket, Socket}, ?LONG_TIMEOUT).

start_fullsync(Pid) ->
    gen_server:call(Pid, start_fullsync, ?LONG_TIMEOUT).

cancel_fullsync(Pid) ->
    gen_server:call(Pid, cancel_fullsync, ?LONG_TIMEOUT).

pause_fullsync(Pid) ->
    gen_server:call(Pid, pause_fullsync, ?LONG_TIMEOUT).

resume_fullsync(Pid) ->
    gen_server:call(Pid, resume_fullsync, ?LONG_TIMEOUT).

status(Pid) ->
    status(Pid, infinity).

status(Pid, Timeout) ->
    gen_server:call(Pid, status, Timeout).

init([Listener, Socket, Transport]) ->
    self() ! init_ack,
    {ok, #state{socket=Socket, transport=Transport, listener=Listener}}.

handle_call(start_fullsync, _From, #state{fullsync_worker=FSW,
        fullsync_strategy=Mod} = State) ->
    Mod:start_fullsync(FSW),
    {reply, ok, State};
handle_call(cancel_fullsync, _From, #state{fullsync_worker=FSW,
        fullsync_strategy=Mod} = State) ->
    Mod:cancel_fullsync(FSW),
    {reply, ok, State};
handle_call(pause_fullsync, _From, #state{fullsync_worker=FSW,
        fullsync_strategy=Mod} = State) ->
    Mod:pause_fullsync(FSW),
    {reply, ok, State};
handle_call(resume_fullsync, _From, #state{fullsync_worker=FSW,
        fullsync_strategy=Mod} = State) ->
    Mod:resume_fullsync(FSW),
    {reply, ok, State};
handle_call(status, _From, #state{fullsync_worker=FSW, q=Q} = State) ->
    Res = case is_pid(FSW) of
        true -> gen_fsm_compat:sync_send_all_state_event(FSW, status, infinity);
        false -> []
    end,
    Desc =
        [
            {node, node()},
            {site, State#state.sitename},
            {strategy, State#state.fullsync_strategy},
            {fullsync_worker, State#state.fullsync_worker}
        ] ++
        case Q of
            undefined ->
                [{bounded_queue, disabled}];
            _ ->
                riak_repl_bq:status(Q)
        end,
    {reply, {status, Desc ++ Res}, State}.

handle_cast(_Event, State) ->
    {noreply, State}.

handle_info({repl, RObj}, State=#state{transport=Transport, socket=Socket}) when State#state.q == undefined ->
    V = State#state.ver,
    case riak_repl_util:repl_helper_send_realtime(RObj, State#state.client) of
        Objects when is_list(Objects) ->
            _ = [send(Transport, Socket, riak_repl_util:encode_obj_msg(V, {diff_obj, O}))
             || O <- Objects],
            _ = send(Transport, Socket, riak_repl_util:encode_obj_msg(V, {diff_obj, RObj})),
            {noreply, State};
        cancel ->
            {noreply, State}
    end;
handle_info({tcp_closed, Socket}, State=#state{socket=Socket}) ->
    lager:info("Connection for site ~p closed", [State#state.sitename]),
    {stop, normal, State};
handle_info({tcp_error, _Socket, Reason}, State) ->
    lager:error("Connection for site ~p closed unexpectedly: ~p",
        [State#state.sitename, Reason]),
    {stop, normal, State};
handle_info({ssl_closed, Socket}, State=#state{socket=Socket}) ->
    lager:info("Connection for site ~p closed", [State#state.sitename]),
    {stop, normal, State};
handle_info({ssl_error, _Socket, Reason}, State) ->
    lager:error("Connection for site ~p closed unexpectedly: ~p",
        [State#state.sitename, Reason]),
    {stop, normal, State};
handle_info({Proto, Socket, Data},
        State=#state{socket=Socket,transport=Transport}) when Proto==tcp; Proto==ssl ->
    Transport:setopts(Socket, [{active, once}]),
    Msg = binary_to_term(Data),
    Reply = handle_msg(Msg, State),
    riak_repl_stats:server_bytes_recv(size(Data)),
    case Reply of
        {noreply, NewState} ->
            case NewState#state.keepalive_time of
                Time when is_integer(Time) ->
                    %% set the keepalive timeout
                    {noreply, NewState, Time};
                _ ->
                    Reply
            end;
        _ ->
            Reply
    end;
handle_info(init_ack, State=#state{transport=Transport, socket=Socket}) ->
    %% acknowledge the change of socket ownership
    ok = ranch:accept_ack(State#state.listener),
    ok = Transport:setopts(Socket, [
            binary,
            {keepalive, true},
            {nodelay, true},
            {packet, 4},
            {reuseaddr, true},
            {active, false}]),
    case Transport:recv(Socket, 0, ?PEERINFO_TIMEOUT) of
        {ok, SiteNameBin} ->
            SiteName = binary_to_list(SiteNameBin),
            ok = riak_repl_util:configure_socket(Transport, Socket),
            self() ! send_peerinfo,
            Timeout = erlang:send_after(?ELECTION_TIMEOUT, self(), election_timeout),
            {noreply, State#state{sitename=SiteName, election_timeout=Timeout}};
        {error, Reason} ->
            riak_repl_stats:server_connect_errors(),
            %% debug to avoid DOS logging
            lager:debug("Failed to receive site name banner from replication"
                "client: ~p", [Reason]),
            {stop, normal, State}
    end;
handle_info(send_peerinfo, State) ->
    send_peerinfo(State);
handle_info(election_timeout, #state{election_timeout=Timer} = State) when is_reference(Timer) ->
    lager:error("Timed out waiting for a leader to be elected"),
    {stop, normal, State};
handle_info(election_wait, State) ->
    send_peerinfo(State);
handle_info(timeout, State) ->
    case State#state.keepalive_time of
        Time when is_integer(Time) ->
            %% keepalive timeout fired
            _ = send(State#state.transport, State#state.socket, keepalive),
            {noreply, State, Time};
        _ ->
            {noreply, State}
    end;
handle_info(_Event, State) ->
    {noreply, State}.

terminate(_Reason, #state{fullsync_worker=FSW, work_dir=WorkDir,q=Q}) ->
    case is_pid(FSW) of
        true ->
            gen_fsm_compat:sync_send_all_state_event(FSW, stop);
        _ ->
            ok
    end,
    catch(riak_repl_bq:stop(Q)),
    %% clean up work dir
    Cmd = lists:flatten(io_lib:format("rm -rf ~s", [WorkDir])),
    os:cmd(Cmd).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% internal functions

handle_msg({peerinfo, PI}, State) ->
    Capability = riak_repl_util:capability_from_vsn(PI),
    handle_msg({peerinfo, PI, Capability}, State);
handle_msg({peerinfo, TheirPI, Capability}, State) ->
    handle_peerinfo(State, TheirPI, Capability);
handle_msg({q_ack, N}, #state{q=BQ} = State) ->
    riak_repl_bq:q_ack(BQ, N),
    {noreply, State};
handle_msg(keepalive, State) ->
    _ = send(State#state.transport, State#state.socket, keepalive_ack),
    {noreply, State};
handle_msg(keepalive_ack, State) ->
    %% noop
    {noreply, State};
handle_msg({proxy_get, Ref, Bucket, Key, Options}, State) ->
    lager:debug("Got proxy_get for ~p:~p", [Bucket, Key]),
    %% do this GET in a worker?
    C = State#state.client,
    Res = C:get(Bucket, Key, Options),
    _ = send(State#state.transport, State#state.socket, {proxy_get_resp, Ref, Res}),
    {noreply, State};
handle_msg(Msg, #state{fullsync_worker = FSW} = State) ->
    gen_fsm_compat:send_event(FSW, Msg),
    {noreply, State}.

handle_peerinfo(#state{sitename=SiteName, transport=Transport, socket=Socket, my_pi=MyPI} = State, TheirPI, Capability) ->
    case riak_repl_util:validate_peer_info(TheirPI, MyPI) of
        true ->
            case app_helper:get_env(riak_repl, inverse_connection) == true
                andalso get(inverted) /= true of
                true ->
                    lager:info("Inverting connection"),
                    riak_repl_leader:rm_receiver_pid(self()),
                    self() ! {tcp, Socket, term_to_binary({peerinfo,
                                TheirPI, Capability})},
                    put(inverted, true),
                    NewState = riak_repl_tcp_client:make_state(SiteName,
                        Transport, Socket, State#state.my_pi,
                        State#state.work_dir,
                        State#state.client),
                    gen_server:enter_loop(riak_repl_tcp_client,
                        [], NewState),

                    {stop, normal, State};
                _ ->

                    ClientStrats = proplists:get_value(fullsync_strategies, Capability,
                        [?LEGACY_STRATEGY]),
                    ServerStrats = app_helper:get_env(riak_repl, fullsync_strategies,
                        [?LEGACY_STRATEGY]),
                    Strategy = riak_repl_util:choose_strategy(ServerStrats, ClientStrats),
                    StratMod = riak_repl_util:strategy_module(Strategy, server),
                    lager:info("Using fullsync strategy ~p.", [StratMod]),
                    {ok, WorkDir} = riak_repl_fsm_common:work_dir(Transport, Socket, SiteName),
                    {ok, FullsyncWorker} = StratMod:start_link(SiteName,
                        Transport, {self(), Socket}, WorkDir, State#state.client, {1,0}),
                    %% Set up bounded queue if remote supports it
                    State1 = case proplists:get_bool(bounded_queue, Capability) of
                        true ->
                            {ok, BQPid} = riak_repl_bq:start_link(Transport,
                                {self(), Socket}),
                            ok = riak_repl_leader:add_receiver_pid(BQPid),

                            State#state{q = BQPid,
                                fullsync_worker = FullsyncWorker,
                                fullsync_strategy = StratMod,
                                work_dir = WorkDir};
                        false ->
                            ok = riak_repl_leader:add_receiver_pid(self()),
                            State#state{fullsync_worker = FullsyncWorker,
                                fullsync_strategy = StratMod}
                    end,

                    KeepaliveTime = case proplists:get_bool(keepalive, Capability) of
                        true ->
                            ?KEEPALIVE_TIME;
                        _ ->
                            undefined
                    end,

                    case app_helper:get_env(riak_repl, fullsync_on_connect, true) of
                        true ->
                            FullsyncWorker ! start_fullsync,
                            lager:info("Full-sync on connect"),
                            {noreply, State1#state{keepalive_time=KeepaliveTime}};
                        false ->
                            {noreply, State1#state{keepalive_time=KeepaliveTime}}
                    end
            end;
        false ->
            lager:error("Invalid peer info, ring sizes do not match."),
            {stop, normal, State}
    end.

send_peerinfo(#state{transport=Transport, socket=Socket, sitename=SiteName} = State) ->
    OurNode = node(),
    case riak_repl_leader:leader_node()  of
        undefined -> % leader not elected yet
            %% check again in 5 seconds
            erlang:send_after(5000, self(), election_wait),
            {noreply, State};
        OurNode ->
            _ = erlang:cancel_timer(State#state.election_timeout),
            %% are we configured to upgrade to ssl?
            case {riak_repl_util:maybe_use_ssl(), Transport:name()} of
                {B, T} when B == false; T == ssl ->
                    %% if there's no valid ssl config or we've already
                    %% upgraded
                    Props = riak_repl_fsm_common:common_init(Transport, Socket),

                    %% annoying we have to do this again, but the ring
                    %% in peerinfo is the 0.14 'downgraded' ring,
                    %% which lacks the cluster_name stuff
                    {ok, Ring} = riak_core_ring_manager:get_my_ring(),

                    PI = proplists:get_value(my_pi, Props),
                    _ = send(Transport, Socket, {peerinfo, PI,
                                             [{cluster_name, riak_core_ring:cluster_name(Ring)},
                                             bounded_queue, keepalive, {fullsync_strategies,
                                                                         app_helper:get_env(riak_repl, fullsync_strategies,
                                                                                            [?LEGACY_STRATEGY])}]}),
                    Transport:setopts(Socket, [{active, once}]),
                    {noreply, State#state{
                            client=proplists:get_value(client, Props),
                            election_timeout=undefined,
                            my_pi=PI}};
                {Config, tcp} ->
                    _ = send(Transport, Socket, {peerinfo,
                                             riak_repl_util:make_fake_peer_info(),
                                             [ssl_required]}),
                    %% verify the other side has sent us a fake ring
                    %% with the ssl_required capability
                    {ok, Data} = Transport:recv(Socket, 0, infinity),
                    case binary_to_term(Data) of
                        {peerinfo, _, [ssl_required|_]} ->
                            case ssl:ssl_accept(Socket, Config) of
                                {ok, SSLSocket} ->
                                    send_peerinfo(State#state{socket=SSLSocket,
                                                              transport=ranch_ssl});
                                {error, Reason} ->
                                    lager:error("Unable to negotiate SSL for "
                                                "replication connection to ~p: ~p",
                                                [SiteName, Reason]),
                                    {stop, normal, State}
                            end;
                        _ ->
                            lager:error("Client does not support SSL; "
                                        "closing connection to ~p",
                                        [SiteName]),
                            %% sleep for a really long time so the
                            %% client doesn't DOS us with reconnect
                            %% attempts
                            timer:sleep(300000),
                            {stop, normal, State}
                    end
            end;
        OtherNode ->
            %% receive stuff off of wire
            case Transport:recv(Socket, 0, infinity) of
                {ok, Data} ->
                    case binary_to_term(Data) of
                        {peerinfo, _, Capabilities} ->
                            ConnectedIP = proplists:get_value(connected_ip,Capabilities),
                            {ok, Ring} = riak_core_ring_manager:get_my_ring(),
                            {Ip, Port} = ip_and_port_for_node(OtherNode, Ring, ConnectedIP),
                            send(Transport, Socket, {redirect, Ip, Port});
                        {peerinfo, _PeerInfo} ->
                            %% 1.0 and earlier nodes don't send capability
                            {ok, Ring} = riak_core_ring_manager:get_my_ring(),
                            {Ip, Port} = ip_and_port_for_node(OtherNode, Ring, undefined),
                            send(Transport, Socket, {redirect, Ip, Port});
                        Other ->
                            Transport:close(Socket),
                            lager:error("Received unknown peer data: ~p",[Other])
                    end;
                {error, closed} ->
                    Transport:close(Socket),
                    lager:error("Peer info tcp error")
            end,
            {stop, normal, State}
    end.

send(Transport, {Owner, Socket}, Data) when is_binary(Data) ->
    case Transport:send(Socket, Data) of
        ok ->
            riak_repl_stats:server_bytes_sent(size(Data)),
            ok;
        {error, _} = Error ->
            Owner ! {list_to_atom(atom_to_list(Transport:name()) ++ "_error"), Socket, Error},
            Error
    end;
send(Transport, Socket, Data) when is_binary(Data)->
    send(Transport, {self(), Socket}, Data);
send(Transport, Socket, Data) ->
    send(Transport, Socket, term_to_binary(Data)).

ip_and_port_for_node(Node, Ring, ConnectedIp) ->
    ReplConfig = riak_repl_ring:get_repl_config(Ring),
    Listeners = dict:fetch(listeners, ReplConfig),
    NodeListeners = [L || L <- Listeners,
                          L#repl_listener.nodename == Node],
    NatListeners =
        case dict:find(natlisteners, ReplConfig) of
            {ok, Value} -> Value;
            error -> []
        end,
    NatNodeListeners = [N || N <- NatListeners,
                             N#nat_listener.nodename == Node],
    NatListenAddrs = [R#nat_listener.nat_addr || R <- NatListeners],
    UseNats = lists:keymember(ConnectedIp, 1, NatListenAddrs),
    case UseNats of
        false ->
            L = hd(NodeListeners),
            L#repl_listener.listen_addr;
        true ->
            NL = hd(NatNodeListeners),
            NL#nat_listener.nat_addr
    end.


%% unit tests

-ifdef(TEST).

nat_redirect_test() ->
    Ring0 = riak_repl_ring:ensure_config_test(),
    NodeName   = "test@test",
    ListenAddr = "127.0.0.1",
    ListenPort = 9010,
    NatAddr    = "10.11.12.13",
    NatPort    = 9011,
    NatListener = #nat_listener{nodename=NodeName,
                                listen_addr={ListenAddr, ListenPort},
                                nat_addr={NatAddr, NatPort}
                               },
    Ring1 = riak_repl_ring:add_nat_listener(Ring0, NatListener),
    {Ip, Port} = ip_and_port_for_node(NodeName, Ring1, NatAddr),
    ?assertEqual("10.11.12.13", Ip),
    ?assertEqual(9011, Port).

non_nat_redirect_test() ->
    Ring0 = riak_repl_ring:ensure_config_test(),
    NodeName   = "test@test",
    ListenAddr = "127.0.0.1",
    ListenPort = 9010,
    Listener = #repl_listener{nodename=NodeName,
                              listen_addr={ListenAddr, ListenPort}},
    Ring1 = riak_repl_ring:add_listener(Ring0, Listener),
    {Ip, Port} = ip_and_port_for_node(NodeName, Ring1, ListenAddr),
    ?assertEqual("127.0.0.1", Ip),
    ?assertEqual(9010, Port).

skip_nat_test() ->
    Ring0 = riak_repl_ring:ensure_config_test(),
    NodeName   = "test@test",
    ListenAddr = "127.0.0.1",
    ListenPort = 9010,
    NatAddr    = "10.11.12.13",
    NatPort    = 9011,
    Listener = #repl_listener{nodename=NodeName,
                              listen_addr={ListenAddr, ListenPort}},

    NatListener = #nat_listener{nodename=NodeName,
                                listen_addr={ListenAddr, ListenPort},
                                nat_addr={NatAddr, NatPort}
                               },
    Ring1 = riak_repl_ring:add_nat_listener(Ring0, NatListener),
    Ring2 = riak_repl_ring:add_listener(Ring1, Listener),
    {Ip, Port} = ip_and_port_for_node(NodeName, Ring2, ListenAddr),
    ?assertEqual("127.0.0.1", Ip),
    ?assertEqual(9010, Port).
-endif.
