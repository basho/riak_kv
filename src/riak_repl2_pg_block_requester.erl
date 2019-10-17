%% Riak EnterpriseDS
%% Copyright 2007-2013 Basho Technologies, Inc. All Rights Reserved.
%%
%% block_requester sends a proxy_get request from a *SINK* cluster to
%% a *SOURCE* cluster, which runs a block_provider.
%%

-module(riak_repl2_pg_block_requester).
-include("riak_repl.hrl").

-behaviour(gen_server).
%% API
-export([start_link/4, sync_register_service/0, start_service/5, status/1, status/2,
         proxy_get/4]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3, provider_cluster_info/1]).

-record(state, {
          transport,
          socket,
          cluster,
          proxy_gets = [],
          remote_cluster_id=undefined,
          remote_cluster_name=undefined,
          leader_mref=undefined,
          proxy_gets_requested = 0
         }).

start_link(Socket, Transport, Proto, Props) ->
    gen_server:start_link(?MODULE, [Socket, Transport, Proto, Props], []).

%% @doc Register with service manager
sync_register_service() ->
    lager:debug("Registering proxy_get requester service"),
    ProtoPrefs = {proxy_get,[{1,0}]},
    TcpOptions = [{keepalive, true}, % find out if connection is dead, this end doesn't send
                  {packet, 4},
                  {active, false},
                  {nodelay, true}],
    HostSpec = {ProtoPrefs, {TcpOptions, ?MODULE, start_service, undefined}},
    riak_core_service_mgr:sync_register_service(HostSpec, {round_robin, undefined}).

%% Callback from service manager
start_service(Socket, Transport, Proto, _Args, Props) ->
    lager:debug("Proxy get service enabled"),
    {ok, Pid} = riak_repl2_pg_block_requester_sup:start_child(Socket, Transport,
        Proto, Props),
    ok = Transport:controlling_process(Socket, Pid),
    Pid ! init_ack,
    {ok, Pid}.


proxy_get(Pid, Bucket, Key, Options) ->
    gen_server:call(Pid, {proxy_get, Bucket, Key, Options}, ?LONG_TIMEOUT).

status(Pid) ->
    gen_server:call(Pid, status, infinity).

status(Pid, Timeout) ->
    gen_server:call(Pid, status, Timeout).

provider_cluster_info(Pid) ->
    gen_server:call(Pid, provider_cluster_info, ?LONG_TIMEOUT).

%% gen server

init([Socket, Transport, _Proto, Props]) ->
    lager:info("Starting Proxy Get Block Requester"),

    SocketTag = riak_repl_util:generate_socket_tag("pg_requester", Transport, Socket),
    lager:debug("Keeping stats for " ++ SocketTag),
    riak_core_tcp_mon:monitor(Socket, {?TCP_MON_PROXYGET_APP, sink,
                                        SocketTag}, Transport),

    Cluster = proplists:get_value(clustername, Props),
    State = #state{cluster=Cluster, transport=Transport, socket=Socket},
    {ok, State}.

handle_call({proxy_get, Bucket, Key, Options}, From,
            State=#state{socket=Socket,transport=Transport,
                         proxy_gets_requested=PGCount}) ->
    lager:debug("Proxy getting ~p ~p ~p",[Bucket, Key, Options]),
    Ref = make_ref(),
    Data = term_to_binary({proxy_get, Ref, Bucket, Key, Options}),
    Transport:send(Socket, Data),
    NewState = State#state{proxy_gets=[{Ref, From}|State#state.proxy_gets],
                           proxy_gets_requested = PGCount+1},
    {noreply, NewState};

handle_call(provider_cluster_info, _From,
            State=#state{remote_cluster_id=ClusterID, remote_cluster_name=ClusterName}) ->
    ClusterInfo = {ClusterID, ClusterName},
    lager:debug("Returning cluster info ~p", [ClusterInfo]),
    {reply, ClusterInfo, State};
handle_call(status, _From, State=#state{socket=Socket,
                                        proxy_gets_requested=PGCount}) ->
    SocketStats = riak_core_tcp_mon:socket_status(Socket),
    FormattedSS =  {socket,
                    riak_core_tcp_mon:format_socket_stats(SocketStats,[])},

    Status = [ {requester_count, PGCount},
               FormattedSS],
    {reply, Status, State};

handle_call(_Msg, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.


handle_info({tcp_closed, Socket}, State=#state{socket=Socket}) ->
    lager:info("Connection for proxy_get ~p closed", [State#state.cluster]),
    {stop, normal, State};
handle_info({tcp_error, _Socket, Reason}, State) ->
    lager:error("Connection for proxy_get ~p closed unexpectedly: ~p",
    [State#state.cluster, Reason]),
    {stop, normal, State};
handle_info({ssl_closed, Socket}, State=#state{socket=Socket}) ->
    lager:info("Connection for proxy_get ~p closed", [State#state.cluster]),
    {stop, normal, State};
handle_info({ssl_error, _Socket, Reason}, State) ->
    lager:error("Connection for proxy_get ~p closed unexpectedly: ~p",
        [State#state.cluster, Reason]),
    {stop, normal, State};

handle_info({'DOWN', _MRef, _Type, _Pid, _Reason}, State0) ->
    lager:debug("Re-registering pg_proxy service 2"),
    State = register_with_leader(State0),
    {noreply, State};
handle_info({Proto, Socket, Data},
  State=#state{socket=Socket,transport=Transport}) when Proto==tcp; Proto==ssl ->
    Transport:setopts(Socket, [{active, once}]),
    Msg = binary_to_term(Data),
    riak_repl_stats:client_bytes_recv(size(Data)),
    Reply =
        case Msg of
            stay_awake ->
                {noreply, State};
            {proxy_get_resp, Ref, Resp} ->
                case lists:keytake(Ref, 1, State#state.proxy_gets) of
                    false ->
                        lager:info("got unexpected proxy_get_resp message"),
                        {noreply, State};
                    {value, {Ref, From}, ProxyGets} ->
                        lager:debug("PG response = ~p", [Resp]),
                        %% send the response to the patiently waiting client
                        gen_server:reply(From, Resp),
                        {noreply, State#state{proxy_gets=ProxyGets}}
                end;
            {get_cluster_info_resp, ClusterID, RemoteClusterName} ->
                RemoteClusterID = list_to_binary(io_lib:format("~p",[ClusterID])),
                lager:debug("Remote cluster id = ~p", [RemoteClusterID]),
                lager:debug("Remote cluster name = ~p", [RemoteClusterName]),
                State2 = register_with_leader(State#state{remote_cluster_id=RemoteClusterID,
                                                 remote_cluster_name=RemoteClusterName}),
                {noreply, State2};
            _ ->
                {noreply, State}
        end,
    Reply;
handle_info(init_ack, State=#state{socket=Socket, transport=Transport}) ->
    lager:debug("init_ack"),
    Transport:setopts(Socket, [{active, once}]),
    Data = term_to_binary(get_cluster_info),
    Transport:send(Socket, Data),
    {noreply, State};
handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, #state{}) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

make_pg_proxy(Remote) ->
    Name = riak_repl_util:make_pg_proxy_name(Remote),
    {Name, {riak_repl2_pg_proxy, start_link, [Name]},
        transient, 5000, worker, [riak_repl2_pg_proxy, pg_proxy]}.

register_with_leader(#state{leader_mref=MRef, cluster=Cluster}=State) ->
    lager:debug("register with leader"),
    case MRef of
        undefined -> ok;
        M ->
            erlang:demonitor(M)
    end,
    Leader = riak_core_cluster_mgr:get_leader(),
    ProxyForCluster = riak_repl_util:make_pg_proxy_name(Cluster),
    %% this can fail if the leader node is shutting down
    try supervisor:which_children({riak_repl2_pg_proxy_sup, Leader}) of
        Children ->
            Child = [{Remote, Pid} || {Remote, Pid, _, _} <- Children,
                is_pid(Pid),
                Remote == ProxyForCluster],
            case Child of
                [{_Remote, _Pid}] ->
                    lager:debug("Not starting a new proxy process, one already exists"),
                    ok;
                _ ->
                    lager:debug("Starting a new proxy process"),
                    %% this can fail if the leader node is shutting down
                    catch(supervisor:start_child({riak_repl2_pg_proxy_sup, Leader},
                            make_pg_proxy(Cluster)))
            end,
            %% this can fail if the leader node is shutting down
            catch(gen_server:call({ProxyForCluster, Leader},
                                  {register, Cluster, node(), self()},
                                  ?LONG_TIMEOUT))
    catch
        _:_ ->
            %% the monitor below will take care of us...
            ok
    end,
    %% if the leader node is shutting down or already stopped, we'll get a
    %% DOWN message real soon
    Monitor = erlang:monitor(process, {ProxyForCluster, Leader}),
    State#state{leader_mref=Monitor}.
