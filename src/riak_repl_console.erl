%% Riak EnterpriseDS
%% Copyright 2007-2009 Basho Technologies, Inc. All Rights Reserved.
-module(riak_repl_console).
-author('Andy Gross <andy@basho.com>').
-include("riak_repl.hrl").
-export([add_listener/1, del_listener/1, add_nat_listener/1]).
-export([add_site/1, del_site/1]).
-export([status/1, start_fullsync/1, cancel_fullsync/1,
         pause_fullsync/1, resume_fullsync/1]).
-export([client_stats_rpc/0, server_stats_rpc/0]).
-export([extract_rt_fs_send_recv_kbps/1]).

-export([clustername/1, clusters/1,clusterstats/1,
         connect/1, disconnect/1, connections/1,
         realtime/1, fullsync/1, proxy_get/1
        ]).
-export([rt_remotes_status/0,
         fs_remotes_status/0]).

-export([get_config/0,
         cluster_mgr_stats/0,
         leader_stats/0,
         client_stats/0,
         server_stats/0,
         coordinator_stats/0,
         coordinator_srv_stats/0]).
-export([modes/1, set_modes/1, get_modes/0,
         max_fssource_node/1,
         max_fssource_cluster/1,
         max_fssink_node/1,
         realtime_cascades/1,
         cascades/1,
         show_nat_map/1,
         add_nat_map/1,
         del_nat_map/1,
         add_block_provider_redirect/1,
         show_block_provider_redirect/1,
         show_local_cluster_id/1,
         delete_block_provider_redirect/1
     ]).

add_listener(Params) ->
    lager:warning(?V2REPLDEP, []),
    Ring = get_ring(),
    case add_listener_internal(Ring,Params) of
        {ok, NewRing} ->
            ok = maybe_set_ring(Ring, NewRing);
        error -> error
    end.

add_nat_listener(Params) ->
    lager:warning(?V2REPLDEP, []),
    Ring = get_ring(),
    case add_nat_listener_internal(Ring, Params) of
        {ok, NewRing} ->
            ok = maybe_set_ring(Ring, NewRing);
        error -> error
    end.

add_listener_internal(Ring, [NodeName, IP, Port]) ->
    Listener = make_listener(NodeName, IP, Port),
    case lists:member(Listener#repl_listener.nodename, riak_core_ring:all_members(Ring)) of
        true ->
            case catch rpc:call(Listener#repl_listener.nodename,
                                riak_repl_util, valid_host_ip, [IP]) of
                true ->
                    NewRing = riak_repl_ring:add_listener(Ring, Listener),
                    {ok,NewRing};
                false ->
                    io:format("~p is not a valid IP address for ~p\n",
                              [IP, Listener#repl_listener.nodename]),
                    error;
                Error ->
                    io:format("Node ~p must be available to add listener: ~p\n",
                              [Listener#repl_listener.nodename, Error]),
                    error
            end;
        false ->
            io:format("~p is not a member of the cluster\n", [Listener#repl_listener.nodename]),
            error
    end.

add_nat_listener_internal(Ring, [NodeName, IP, Port, PublicIP, PublicPort]) ->
    case add_listener_internal(Ring, [NodeName, IP, Port]) of
        {ok,NewRing} ->
            case inet_parse:address(PublicIP) of
                {ok, _} ->
                    NatListener = make_nat_listener(NodeName, IP, Port, PublicIP, PublicPort),
                    NewRing2 = riak_repl_ring:add_nat_listener(NewRing, NatListener),
                    {ok, NewRing2};
                {error, IPParseError} ->
                    io:format("Invalid NAT IP address: ~p~n", [IPParseError]),
                    error
            end;
        error ->
            io:format("Error adding nat address. ~n"),
            error
    end.

del_listener([NodeName, IP, Port]) ->
    lager:warning(?V2REPLDEP, []),
    Ring = get_ring(),
    Listener = make_listener(NodeName, IP, Port),
    NewRing0 = riak_repl_ring:del_listener(Ring, Listener),
    NewRing = riak_repl_ring:del_nat_listener(NewRing0, Listener),
    ok = maybe_set_ring(Ring, NewRing).

add_site([IP, Port, SiteName]) ->
    lager:warning(?V2REPLDEP, []),
    Ring = get_ring(),
    Site = make_site(SiteName, IP, Port),
    NewRing = riak_repl_ring:add_site(Ring, Site),
    ok = maybe_set_ring(Ring, NewRing).

del_site([SiteName]) ->
    lager:warning(?V2REPLDEP, []),
    Ring = get_ring(),
    NewRing = riak_repl_ring:del_site(Ring, SiteName),
    ok = maybe_set_ring(Ring, NewRing).

set_modes(Modes) ->
    Ring = get_ring(),
    NewRing = riak_repl_ring:set_modes(Ring, Modes),
    ok = maybe_set_ring(Ring, NewRing).

get_modes() ->
    Ring = get_ring(),
    riak_repl_ring:get_modes(Ring).


status([]) ->
    status2(true);
status(quiet) ->
    status2(false).

status2(Verbose) ->
    Config = get_config(),
    Stats1 = riak_repl_stats:get_stats(),
    RTRemotesStatus = rt_remotes_status(),
    FSRemotesStatus = fs_remotes_status(),
    PGRemotesStatus = pg_remotes_status(),
    LeaderStats = leader_stats(),
    ClientStats = client_stats(),
    ServerStats = server_stats(),
    CoordStats = coordinator_stats(),
    CoordSrvStats = coordinator_srv_stats(),
    CMgrStats = cluster_mgr_stats(),
    RTQStats = rtq_stats(),
    PGStats = riak_repl2_pg:status(),

    Most =
        RTRemotesStatus++FSRemotesStatus++PGRemotesStatus++Config++
        Stats1++LeaderStats++ClientStats++ServerStats++
        CoordStats++CoordSrvStats++CMgrStats++RTQStats++PGStats,
    SendRecvKbps = extract_rt_fs_send_recv_kbps(Most),
    All = Most ++ SendRecvKbps,
    if Verbose ->
            format_counter_stats(All);
       true ->
            All
    end.

pg_remotes_status() ->
    Ring = get_ring(),
    Enabled = string:join(riak_repl_ring:pg_enabled(Ring),", "),
    [{proxy_get_enabled, Enabled}].

rt_remotes_status() ->
    Ring = get_ring(),
    Enabled = string:join(riak_repl_ring:rt_enabled(Ring),", "),
    Started = string:join(riak_repl_ring:rt_started(Ring),", "),
    [{realtime_enabled, Enabled},
     {realtime_started, Started}].

fs_remotes_status() ->
    Ring = get_ring(),
    Sinks = riak_repl_ring:fs_enabled(Ring),
    RunningSinks = [Sink || Sink <- Sinks, cluster_fs_running(Sink)],
    [{fullsync_enabled, string:join(Sinks, ", ")},
     {fullsync_running, string:join(RunningSinks, ", ")}].

cluster_fs_running(Sink) ->
    ClusterCoord = riak_repl2_fscoordinator_sup:coord_for_cluster(Sink),
    riak_repl2_fscoordinator:is_running(ClusterCoord).

start_fullsync([]) ->
    lager:warning(?V2REPLDEP, []),
    _ = [riak_repl_tcp_server:start_fullsync(Pid) ||
        Pid <- riak_repl_listener_sup:server_pids()],
    ok.

cancel_fullsync([]) ->
    lager:warning(?V2REPLDEP, []),
    _ = [riak_repl_tcp_server:cancel_fullsync(Pid) ||
        Pid <- riak_repl_listener_sup:server_pids()],
    ok.

pause_fullsync([]) ->
    lager:warning(?V2REPLDEP, []),
    _ = [riak_repl_tcp_server:pause_fullsync(Pid) ||
        Pid <- riak_repl_listener_sup:server_pids()],
    ok.

resume_fullsync([]) ->
    lager:warning(?V2REPLDEP, []),
    _ = [riak_repl_tcp_server:resume_fullsync(Pid) ||
        Pid <- riak_repl_listener_sup:server_pids()],
    ok.


%%
%% Repl2 commands
%%
rtq_stats() ->
    case erlang:whereis(riak_repl2_rtq) of
        Pid when is_pid(Pid) ->
            [{realtime_queue_stats, riak_repl2_rtq:status()}];
        _ -> []
    end.

cluster_mgr_stats() ->
    case erlang:whereis(riak_repl_leader_gs) of
        Pid when is_pid(Pid) ->
            ConnectedClusters = case riak_core_cluster_mgr:get_known_clusters() of
                {ok, Clusters} ->
                    [erlang:list_to_binary(Cluster) || Cluster <-
                                                       Clusters];
                Error -> Error
            end,
            [{cluster_name,
              erlang:list_to_binary(riak_core_connection:symbolic_clustername())},
             {cluster_leader, riak_core_cluster_mgr:get_leader()},
             {connected_clusters, ConnectedClusters}];
        _ -> []
    end.

%% Show cluster stats for this node
clusterstats([]) ->
    %% connection manager stats
    CMStats = cluster_mgr_stats(),
    CConnStats = riak_core_connection_mgr_stats:get_consolidated_stats(),
    Stats = CMStats ++ CConnStats,
    io:format("~p~n", [Stats]);
%% slice cluster stats by remote "IP:Port" or "protocol-id".
%% Example protocol-id is rt_repl
clusterstats([Arg]) ->
    NWords = string:words(Arg, $:),
    case NWords of
        1 ->
            %% assume protocol-id
            ProtocolId = list_to_atom(Arg),
            CConnStats = riak_core_connection_mgr_stats:get_stats_by_protocol(ProtocolId),
            CMStats = cluster_mgr_stats(),
            Stats = CMStats ++ CConnStats,
            io:format("~p~n", [Stats]);
        2 ->
            Address = Arg,
            IP = string:sub_word(Address, 1, $:),
            PortStr = string:sub_word(Address, 2, $:),
            {Port,_Rest} = string:to_integer(PortStr),
            CConnStats = riak_core_connection_mgr_stats:get_stats_by_ip({IP,Port}),
            CMStats = cluster_mgr_stats(),
            Stats = CMStats ++ CConnStats,
            io:format("~p~n", [Stats]);
        _ ->
            {error, {badarg, Arg}}
    end.

%% TODO: cluster naming belongs in riak_core_ring, not in riak_core_connection, but
%% not until we move all of the connection stuff to core.
clustername([]) ->
    MyName = riak_core_connection:symbolic_clustername(),
    io:format("~s~n", [MyName]),
    ok;
clustername([ClusterName]) ->
    ?LOG_USER_CMD("Set clustername to ~p", [ClusterName]),
    riak_core_ring_manager:ring_trans(fun riak_core_connection:set_symbolic_clustername/2,
                                      ClusterName),
    ok.

clusters([]) ->
    {ok, Clusters} = riak_core_cluster_mgr:get_known_clusters(),
    lists:foreach(
      fun(ClusterName) ->
              {ok,Members} = riak_core_cluster_mgr:get_ipaddrs_of_cluster(ClusterName),
              IPs = [string_of_ipaddr(Addr) || Addr <- Members],
              io:format("~s: ~p~n", [ClusterName, IPs]),
              ok
      end,
      Clusters),
    ok.

string_of_ipaddr({IP, Port}) ->
    lists:flatten(io_lib:format("~s:~p", [IP, Port])).

choose_best_addr({cluster_by_addr, {IP,Port}}, _ClientAddr) ->
    string_of_ipaddr({IP,Port});
choose_best_addr({cluster_by_name, _}, ClientAddr) ->
    string_of_ipaddr(ClientAddr).

string_of_remote({cluster_by_addr, {IP,Port}}) ->
    string_of_ipaddr({IP,Port});
string_of_remote({cluster_by_name, ClusterName}) ->
    ClusterName.

%% Print info about this sink
%% Remote :: {ip,port} | ClusterName
showClusterConn({Remote,Pid}) ->
    ConnName = string_of_remote(Remote),
    PidStr = io_lib:format("~p", [Pid]),
    %% try to get status from Pid of cluster control channel.
    %% if we haven't connected successfully yet, it will time out, which we will fail
    %% fast for since it's a local process, not a remote one.
    try riak_core_cluster_conn:status(Pid, 2) of
        {Pid, status, {ClientAddr, _Transport, Name, Members}} ->
            IPs = [string_of_ipaddr(Addr) || Addr <- Members],
            CAddr = choose_best_addr(Remote, ClientAddr),
            io:format("~-20s ~-20s ~-15s ~p (via ~s)~n",
                      [ConnName, Name, PidStr,IPs, CAddr]);
        {_StateName, SRemote} ->
            io:format("~-20s ~-20s ~-15s (connecting to ~p)~n",
                      [ConnName, "", PidStr, string_of_remote(SRemote)])
    catch
        'EXIT':{timeout, _} ->
            io:format("~-20s ~-20s ~-15s (status timed out)~n",
                      [ConnName, "", PidStr])
    end.

connections([]) ->
    %% get cluster manager's outbound connections to other "remote" clusters,
    %% which for now, are all the "sinks".
    {ok, Conns} = riak_core_cluster_mgr:get_connections(),
    io:format("~-20s ~-20s ~-15s [Members]~n", ["Connection", "Cluster Name", "<Ctrl-Pid>"]),
    io:format("~-20s ~-20s ~-15s ---------~n", ["----------", "------------", "----------"]),
    _ = [showClusterConn(Conn) || Conn <- Conns],
    ok.

connect([Address]) ->
    ?LOG_USER_CMD("Connect to cluster at ~p", [Address]),
    NWords = string:words(Address, $:),
    case NWords of
        2 ->
            IP = string:sub_word(Address, 1, $:),
            PortStr = string:sub_word(Address, 2, $:),
            connect([IP, PortStr]);
        _ ->
            io:format("Error: remote connection is missing port. Expected 'connect <host:port>'~n"),
            {error, {badarg, Address}}
    end;
connect([IP, PortStr]) ->
    ?LOG_USER_CMD("Connect to cluster at ~p:~p", [IP, PortStr]),
    {Port,_Rest} = string:to_integer(PortStr),
    case riak_core_connection:symbolic_clustername() of
        "undefined" ->
            io:format("Error: Unable to establish connections until local cluster is named.~n"),
            io:format("First use 'riak-repl clustername <clustername>'~n"),
            {error, undefined_cluster_name};
        _Name ->
            riak_core_cluster_mgr:add_remote_cluster({IP, Port}),
            ok
    end.

%% remove a remote connection by clustername or by IP/Port address:
%% clustername
%% | ip:port
%% | ip port
disconnect([Address]) ->
    ?LOG_USER_CMD("Disconnect from cluster at ~p", [Address]),
    NWords = string:words(Address, $:),
    case NWords of
        1 ->
            Remote = Address,
            %% TODO: need to wrap a single ring transition around all of these.
            %% fullsync(["stop",    Remote]),
            %% fullsync(["disable", Remote]),
            %% realtime(["stop",    Remote]),
            %% realtime(["disable", Remote]),
            %% tear down cluster manager connection
            riak_core_cluster_mgr:remove_remote_cluster(Remote),
            ok;
        2 ->
            IP = string:sub_word(Address, 1, $:),
            PortStr = string:sub_word(Address, 2, $:),
            _ = disconnect([IP, PortStr]),
            ok;
        _ ->
            {error, {badarg, Address}}
    end;
disconnect([IP, PortStr]) ->
    ?LOG_USER_CMD("Disconnect from cluster at ~p:~p", [IP, PortStr]),
    {Port,_Rest} = string:to_integer(PortStr),
    riak_core_cluster_mgr:remove_remote_cluster({IP, Port}),
    ok.

realtime([Cmd, Remote]) ->
    case Cmd of
        "enable" ->
            ?LOG_USER_CMD("Enable Realtime Replication to cluster ~p", [Remote]),
            riak_repl2_rt:enable(Remote);
        "disable" ->
            ?LOG_USER_CMD("Disable Realtime Replication to cluster ~p", [Remote]),
            riak_repl2_rt:disable(Remote);
        "start" ->
            ?LOG_USER_CMD("Start Realtime Replication to cluster ~p", [Remote]),
            riak_repl2_rt:start(Remote);
        "stop" ->
            ?LOG_USER_CMD("Stop Realtime Replication to cluster ~p", [Remote]),
            riak_repl2_rt:stop(Remote)
    end,
    ok;
realtime([Cmd]) ->
    Remotes = riak_repl2_rt:enabled(),
    _ = case Cmd of
        "start" ->
            ?LOG_USER_CMD("Start Realtime Replication to all connected clusters",
                          []),
            _ = [riak_repl2_rt:start(Remote) || Remote <- Remotes];
        "stop" ->
            ?LOG_USER_CMD("Stop Realtime Replication to all connected clusters",
                      []),
            _ = [riak_repl2_rt:stop(Remote) || Remote <- Remotes]
    end,
    ok.

fullsync([Cmd, Remote]) ->
    Leader = riak_core_cluster_mgr:get_leader(),
    case Cmd of
        "enable" ->
            ?LOG_USER_CMD("Enable Fullsync Replication to cluster ~p", [Remote]),
            riak_core_ring_manager:ring_trans(fun
                    riak_repl_ring:fs_enable_trans/2, Remote),
            _ = riak_repl2_fscoordinator_sup:start_coord(Leader, Remote),
            ok;
        "disable" ->
            ?LOG_USER_CMD("Disable Fullsync Replication to cluster ~p", [Remote]),
            riak_core_ring_manager:ring_trans(fun
                    riak_repl_ring:fs_disable_trans/2, Remote),
            _ = riak_repl2_fscoordinator_sup:stop_coord(Leader, Remote),
            ok;
        "start" ->
            ?LOG_USER_CMD("Start Fullsync Replication to cluster ~p", [Remote]),
            Fullsyncs = riak_repl2_fscoordinator_sup:started(Leader),
            case proplists:get_value(Remote, Fullsyncs) of
                undefined ->
                    io:format("Fullsync not enabled for cluster ~p~n", [Remote]),
                    io:format("Use 'fullsync enable ~p' before start~n", [Remote]),
                    {error, not_enabled};
                Pid ->
                    riak_repl2_fscoordinator:start_fullsync(Pid),
                    ok
            end;
        "stop" ->
            ?LOG_USER_CMD("Stop Fullsync Replication to cluster ~p", [Remote]),
            Fullsyncs = riak_repl2_fscoordinator_sup:started(Leader),
            case proplists:get_value(Remote, Fullsyncs) of
                undefined ->
                    %% Fullsync is not enabled, but carry on quietly.
                    ok;
                Pid ->
                    riak_repl2_fscoordinator:stop_fullsync(Pid),
                    ok
            end
    end;
fullsync([Cmd]) ->
    Leader = riak_core_cluster_mgr:get_leader(),
    Fullsyncs = riak_repl2_fscoordinator_sup:started(Leader),
    case Cmd of
        "start" ->
            ?LOG_USER_CMD("Start Fullsync Replication to all connected clusters",[]),
            _ = [riak_repl2_fscoordinator:start_fullsync(Pid) || {_, Pid} <-
                Fullsyncs],
            ok;
        "stop" ->
            ?LOG_USER_CMD("Stop Fullsync Replication to all connected clusters",[]),
            _ = [riak_repl2_fscoordinator:stop_fullsync(Pid) || {_, Pid} <-
                Fullsyncs],
            ok
    end,
    ok.

proxy_get([Cmd, Remote]) ->
    case Cmd of
        "enable" ->
            ?LOG_USER_CMD("Enable Riak CS Proxy GET block provider for ~p",[Remote]),
            riak_core_ring_manager:ring_trans(fun
                    riak_repl_ring:pg_enable_trans/2, Remote),
            ok;
        "disable" ->
            ?LOG_USER_CMD("Disable Riak CS Proxy GET block provider for ~p",[Remote]),
            riak_core_ring_manager:ring_trans(fun
                    riak_repl_ring:pg_disable_trans/2, Remote),
            ok
    end.

modes([]) ->
    CurrentModes = get_modes(),
    io:format("Current replication modes: ~p~n",[CurrentModes]),
    ok;
modes(NewModes) ->
    ?LOG_USER_CMD("Set replication mode(s) to ~p",[NewModes]),
    Modes = [ list_to_atom(Mode) || Mode <- NewModes],
    set_modes(Modes),
    modes([]).

realtime_cascades(["always"]) ->
    ?LOG_USER_CMD("Enable Realtime Replication cascading", []),
    riak_core_ring_manager:ring_trans(fun
        riak_repl_ring:rt_cascades_trans/2, always);
realtime_cascades(["never"]) ->
    ?LOG_USER_CMD("Disable Realtime Replication cascading", []),
    riak_core_ring_manager:ring_trans(fun
        riak_repl_ring:rt_cascades_trans/2, never);
realtime_cascades([]) ->
    Cascades = app_helper:get_env(riak_repl, realtime_cascades, always),
    io:format("realtime_cascades: ~p~n", [Cascades]);
realtime_cascades(_Wut) ->
    io:format("realtime_cascades either \"always\" or \"never\"~n").

cascades(Val) ->
    realtime_cascades(Val).

%% For each of these "max" parameter changes, we need to make an rpc multi-call to every node
%% so that all nodes have the new value in their application environment. That way, whoever
%% becomes the fullsync coordinator will have the correct values. TODO: what happens when a
%% machine bounces and becomes leader? It won't know the new value. Seems like we need a central
%% place to hold these configuration values.
max_fssource_node([]) ->
    %% show the default so as not to confuse the user
    io:format("max_fssource_node value = ~p~n",
              [app_helper:get_env(riak_repl, max_fssource_node,
                                  ?DEFAULT_SOURCE_PER_NODE)]);
max_fssource_node([FSSourceNode]) ->
    NewVal = erlang:list_to_integer(FSSourceNode),
    riak_core_util:rpc_every_member(?MODULE, max_fssource_node, [NewVal], ?CONSOLE_RPC_TIMEOUT),
    ?LOG_USER_CMD("Set max number of Fullsync workers per Source node to ~p",[NewVal]),
    max_fssource_node([]),
    ok;
max_fssource_node(NewVal) ->
    ?LOG_USER_CMD("Locally set max number of Fullsync workers to ~p",[NewVal]),
    application:set_env(riak_repl, max_fssource_node, NewVal).

max_fssource_cluster([]) ->
    %% show the default so as not to confuse the user
    io:format("max_fssource_cluster value = ~p~n",
              [app_helper:get_env(riak_repl, max_fssource_cluster,
                                  ?DEFAULT_SOURCE_PER_CLUSTER)]);
max_fssource_cluster([FSSourceCluster]) ->
    NewVal = erlang:list_to_integer(FSSourceCluster),
    riak_core_util:rpc_every_member(?MODULE, max_fssource_cluster, [NewVal], ?CONSOLE_RPC_TIMEOUT),
    ?LOG_USER_CMD("Set max number of Fullsync workers for Source cluster to ~p",[NewVal]),
    max_fssource_cluster([]),
    ok;
max_fssource_cluster(NewVal) ->
    ?LOG_USER_CMD("Locally set max number of Fullsync workersfor Source cluster to ~p",[NewVal]),
    application:set_env(riak_repl, max_fssource_cluster, NewVal).

max_fssink_node([]) ->
    io:format("max_fssink_node value = ~p~n",
        [app_helper:get_env(riak_repl, max_fssink_node, ?DEFAULT_MAX_SINKS_NODE)]);
max_fssink_node([FSSinkNode]) ->
    NewVal = erlang:list_to_integer(FSSinkNode),
    riak_core_util:rpc_every_member(?MODULE, max_fssink_node, [NewVal], ?CONSOLE_RPC_TIMEOUT),
    ?LOG_USER_CMD("Set max number of Fullsync works per Sink node to ~p",[NewVal]),
    max_fssink_node([]),
    ok;
max_fssink_node(NewVal) ->
    ?LOG_USER_CMD("Locally set max number of Fullsync workers per Sink node to ~p",[NewVal]),
    application:set_env(riak_repl, max_fssink_node, NewVal).

show_nat_map([]) ->
    Ring = get_ring(),
    io:format("Nat map: ~n"),
    [io:format("        ~-21.. s -> ~s~n",
            [print_ip_and_maybe_port(Int), print_ip_and_maybe_port(Ext)])
        || {Int, Ext} <- riak_repl_ring:get_nat_map(Ring)].

add_nat_map([External, Internal]) ->
    case {parse_ip_and_maybe_port(External, false),
            parse_ip_and_maybe_port(Internal, true)} of
        {{error, Reason}, _} ->
            io:format("Bad external IP ~p", [Reason]),
            error;
        {_, {error, Reason}} ->
            io:format("Bad internal IP ~p", [Reason]),
            error;
        {ExternalIP, InternalIP} ->
            ?LOG_USER_CMD("Add a NAT map from External IP ~p to Internal IP ~p", [ExternalIP, InternalIP]),
            riak_core_ring_manager:ring_trans(
                fun riak_repl_ring:add_nat_map/2,
                {ExternalIP, InternalIP}),
            ok
    end.

del_nat_map([External, Internal]) ->
    case {parse_ip_and_maybe_port(External, false),
            parse_ip_and_maybe_port(Internal, true)} of
        {{error, Reason}, _} ->
            io:format("Bad external IP ~p", [Reason]),
            error;
        {_, {error, Reason}} ->
            io:format("Bad internal IP ~p", [Reason]),
            error;
        {ExternalIP, InternalIP} ->
            ?LOG_USER_CMD("Delete a NAT map from External IP ~p to Internal IP ~p", [ExternalIP, InternalIP]),
            riak_core_ring_manager:ring_trans(
                fun riak_repl_ring:del_nat_map/2,
                {ExternalIP, InternalIP}),
            ok
    end.

% NB: the following commands are around the "Dead Cluster" redirect feature,
%     306. They all operate using cluster_id (tuple), not clustername, for now, as 
%     of this writing we had no reliable way to map a clustername to an id 
%     over disterlang. When this API becomes available, this feature may use
%     it. 
add_block_provider_redirect([FromClusterId, ToClusterId]) ->
    lager:info("Redirecting cluster id: ~p to ~p", [FromClusterId, ToClusterId]),
    riak_core_metadata:put({<<"replication">>, <<"cluster-mapping">>}, 
                           FromClusterId, ToClusterId).

show_block_provider_redirect([FromClusterId]) ->
    case riak_core_metadata:get({<<"replication">>, <<"cluster-mapping">>}, FromClusterId) of
        undefined ->
            io:format("No mapping for ~p~n", [FromClusterId]);
        ToClusterId ->
            io:format("Cluster id ~p redirecting to cluster id ~p~n", [FromClusterId, ToClusterId])
    end.

delete_block_provider_redirect([FromClusterId]) ->
    lager:info("Deleting redirect to ~p", [FromClusterId]),
    riak_core_metadata:delete({<<"replication">>, <<"cluster-mapping">>}, FromClusterId).

show_local_cluster_id([]) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    ClusterId = lists:flatten(
        io_lib:format("~p", [riak_core_ring:cluster_name(Ring)])),
    io:format("local cluster id: ~p~n", [ClusterId]).

%% helper functions

parse_ip_and_maybe_port(String, Hostname) ->
    case string:tokens(String, ":") of
        [IPStr, PortStr] ->
            case inet_parse:ipv4strict_address(IPStr) of
                {ok, IP} ->
                    try list_to_integer(PortStr) of
                        Port ->
                            {IP, Port}
                    catch
                        _:_ ->
                            {error, {bad_port, PortStr}}
                    end;
                _ when Hostname ->
                    case inet_gethost_native:gethostbyname(IPStr) of
                        {ok, _} ->
                            try list_to_integer(PortStr) of
                                Port ->
                                    {IPStr, Port}
                            catch
                                _:_ ->
                                    {error, {bad_port, PortStr}}
                            end;
                        _ ->
                            {error, {bad_ip, IPStr}}
                    end;
                _ ->
                    {error, {bad_ip, IPStr}}
            end;
        [IPStr] ->
            case inet_parse:ipv4strict_address(IPStr) of
                {ok, IP} ->
                    IP;
                _ when Hostname ->
                    case inet_gethost_native:gethostbyname(IPStr) of
                        {ok, _} ->
                            IPStr;
                        _ ->
                            {error, {bad_ip, IPStr}}
                    end;
                _ ->
                    {error, {bad_ip, IPStr}}
            end
    end.

%% helper functions

extract_rt_fs_send_recv_kbps(Most) ->
    RTSendKbps = sum_rt_send_kbps(Most),
    RTRecvKbps = sum_rt_recv_kbps(Most),
    FSSendKbps = sum_fs_send_kbps(Most),
    FSRecvKbps = sum_fs_recv_kbps(Most),
    [{realtime_send_kbps, RTSendKbps}, {realtime_recv_kbps, RTRecvKbps},
        {fullsync_send_kbps, FSSendKbps}, {fullsync_recv_kbps, FSRecvKbps}].

print_ip_and_maybe_port({IP, Port}) when is_tuple(IP) ->
    [inet_parse:ntoa(IP), $:, integer_to_list(Port)];
print_ip_and_maybe_port({Host, Port}) when is_list(Host) ->
    [Host, $:, integer_to_list(Port)];
print_ip_and_maybe_port(IP) when is_tuple(IP) ->
    inet_parse:ntoa(IP);
print_ip_and_maybe_port(Host) when is_list(Host) ->
    Host.

format_counter_stats([]) -> ok;
format_counter_stats([{K,V}|T]) when is_list(K) ->
    io:format("~s: ~p~n", [K,V]),
    format_counter_stats(T);
%format_counter_stats([{K,V}|T]) when K == fullsync_coordinator ->
%    io:format("V = ~p",[V]),
%    case V of
%        [] -> io:format("~s: {}~n",[K]);
%        Val -> io:format("~s: ~s",[K,Val])
%    end,
%    format_counter_stats(T);
format_counter_stats([{K,V}|T]) when K == client_rx_kbps;
                                     K == client_tx_kbps;
                                     K == server_rx_kbps;
                                     K == server_tx_kbps ->
    io:format("~s: ~w~n", [K,V]),
    format_counter_stats(T);
format_counter_stats([{K,V}|T]) ->
    io:format("~p: ~p~n", [K,V]),
    format_counter_stats(T);
format_counter_stats([{_K,_IPAddr,_V}|T]) ->
    %% Don't include per-IP stats in this output
    %% io:format("~p(~p): ~p~n", [K,IPAddr,V]),
    format_counter_stats(T).

make_listener(NodeName, IP, Port) ->
    #repl_listener{nodename=list_to_atom(NodeName),
                   listen_addr={IP, list_to_integer(Port)}}.

make_nat_listener(NodeName, IP, Port, PublicIP, PublicPort) ->
    #nat_listener{nodename=list_to_atom(NodeName),
                listen_addr={IP, list_to_integer(Port)},
                nat_addr={PublicIP, list_to_integer(PublicPort)}}.


make_site(SiteName, IP, Port) ->
    #repl_site{name=SiteName, addrs=[{IP, list_to_integer(Port)}]}.

maybe_set_ring(_R, _R) -> ok;
maybe_set_ring(_R1, R2) ->
    RC = riak_repl_ring:get_repl_config(R2),
    F = fun(InRing, ReplConfig) ->
                {new_ring, riak_repl_ring:set_repl_config(InRing, ReplConfig)}
        end,
    RC = riak_repl_ring:get_repl_config(R2),
    {ok, _NewRing} = riak_core_ring_manager:ring_trans(F, RC),
    ok.

get_ring() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    riak_repl_ring:ensure_config(Ring).

get_config() ->
    {ok, R} = riak_core_ring_manager:get_my_ring(),
    case riak_repl_ring:get_repl_config(R) of
        undefined ->
            [];
        Repl ->
            case dict:find(sites, Repl) of
                error ->
                    [];
                {ok, Sites} ->
                    lists:flatten([format_site(S) || S <- Sites])
            end ++
            case dict:find(listeners, Repl) of
                error ->
                    [];
                {ok, Listeners} ->
                    lists:flatten([format_listener(L) || L <- Listeners])
            end ++
            case dict:find(natlisteners, Repl) of
                error ->
                    [];
                {ok, NatListeners} ->
                    lists:flatten([format_nat_listener(L) || L <- NatListeners])
            end
    end.

format_site(S) ->
    [{S#repl_site.name ++ "_ips", format_ips(S#repl_site.addrs)}].

format_ips(IPs) ->
    string:join([format_ip(IP) || IP <- IPs], ", ").

format_ip({Addr,Port}) ->
    lists:flatten(io_lib:format("~s:~p", [Addr, Port])).

format_listener(L) ->
    [{"listener_" ++ atom_to_list(L#repl_listener.nodename),
      format_ip(L#repl_listener.listen_addr)}].

format_nat_listener(L) ->
    [{"natlistener_" ++ atom_to_list(L#nat_listener.nodename),
      format_ip(L#nat_listener.listen_addr) ++ "->" ++
      format_ip(L#nat_listener.nat_addr)}].

leader_stats() ->
    case erlang:whereis(riak_repl_leader_gs) of
        Pid when is_pid(Pid) ->
            LeaderNode = riak_repl_leader:leader_node(),
            LocalStats =
                         try
                LocalProcInfo = erlang:process_info(whereis(riak_repl_leader_gs),
                                                    [message_queue_len, heap_size]),
                [{"local_leader_" ++  atom_to_list(K), V} || {K,V} <- LocalProcInfo]
            catch _:_ ->
                    []
            end,
            RemoteStats =
                          try
                LeaderPid = rpc:call(LeaderNode, erlang, whereis,
                                     [riak_repl_leader_gs]),
                LeaderStats = rpc:call(LeaderNode, erlang, process_info,
                                       [LeaderPid, [message_queue_len,
                                                    total_heap_size,
                                                    heap_size,
                                                    stack_size,
                                                    reductions,
                                                    garbage_collection]]),
                [{"leader_" ++  atom_to_list(K), V} || {K,V} <- LeaderStats]
            catch
                _:_ ->
                    []
            end,
            [{leader, LeaderNode}] ++ RemoteStats ++ LocalStats;
        _ -> []
    end.

client_stats() ->
    case erlang:whereis(riak_repl_leader_gs) of
        Pid when is_pid(Pid) ->
            %% NOTE: rpc:multicall to all clients removed
            riak_repl_console:client_stats_rpc();
        _ -> []
    end.

client_stats_rpc() ->
    RT2 = [rt2_sink_stats(P) || P <- riak_repl2_rt:get_sink_pids()] ++
          [fs2_sink_stats(P) || P <- riak_repl2_fssink_sup:started()],
    Pids = [P || {_,P,_,_} <- supervisor:which_children(riak_repl_client_sup), P /= undefined],
    [{client_stats, [client_stats(P) || P <- Pids]}, {sinks, RT2}].

server_stats() ->
    case erlang:whereis(riak_repl_leader_gs) of
        Pid when is_pid(Pid) ->
            RT2 = [rt2_source_stats(P) || {_R,P} <-
                                          riak_repl2_rtsource_conn_sup:enabled()],
            LeaderNode = riak_repl_leader:leader_node(),
            case LeaderNode of
                undefined ->
                    [{sources, RT2}];
                _ ->
                    [{server_stats, rpc:call(LeaderNode, ?MODULE, server_stats_rpc,
                                             [])},
                     {sources, RT2}]
            end;
        _ -> []
    end.

server_stats_rpc() ->
    [server_stats(P) ||
        P <- riak_repl_listener_sup:server_pids()].

%%socket_stats(Pid) ->
%%    Timeout = app_helper:get_env(riak_repl, status_timeout, 5000),
%%    State = try
%%                riak_repl_tcp_mon:status(Pid, Timeout)
%%            catch
%%                _:_ ->
%%                    too_busy
%%            end,
%%    {Pid, erlang:process_info(Pid, message_queue_len), State}.

client_stats(Pid) ->
    Timeout = app_helper:get_env(riak_repl, status_timeout, 5000),
    State = try
                riak_repl_tcp_client:status(Pid, Timeout)
            catch
                _:_ ->
                    too_busy
            end,
    {Pid, erlang:process_info(Pid, message_queue_len), State}.

server_stats(Pid) ->
    Timeout = app_helper:get_env(riak_repl, status_timeout, 5000),
    State = try
                riak_repl_tcp_server:status(Pid, Timeout)
            catch
                _:_ ->
                    too_busy
            end,
    {Pid, erlang:process_info(Pid, message_queue_len), State}.

coordinator_stats() ->
    case erlang:whereis(riak_repl_leader_gs) of
        Pid when is_pid(Pid) ->
            [{fullsync_coordinator, riak_repl2_fscoordinator:status()}];
        _ -> []
    end.

coordinator_srv_stats() ->
    case erlang:whereis(riak_repl_leader_gs) of
        Pid when is_pid(Pid) ->
            [{fullsync_coordinator_srv, riak_repl2_fscoordinator_serv:status()}];
        _ -> []
    end.

rt2_source_stats(Pid) ->
    Timeout = app_helper:get_env(riak_repl, status_timeout, 5000),
    State = try
                riak_repl2_rtsource_conn:status(Pid, Timeout)
            catch
                _:_ ->
                    too_busy
            end,
    FormattedPid = riak_repl_util:safe_pid_to_list(Pid),
    {source_stats, [{pid,FormattedPid}, erlang:process_info(Pid, message_queue_len),
     {rt_source_connected_to, State}]}.

rt2_sink_stats(Pid) ->
    Timeout = app_helper:get_env(riak_repl, status_timeout, 5000),
    State = try
                riak_repl2_rtsink_conn:status(Pid, Timeout)
            catch
                _:_ ->
                    too_busy
            end,
    %%{Pid, erlang:process_info(Pid, message_queue_len), State}.
    FormattedPid = riak_repl_util:safe_pid_to_list(Pid),
    {sink_stats, [{pid,FormattedPid}, erlang:process_info(Pid, message_queue_len),
     {rt_sink_connected_to, State}]}.

fs2_sink_stats(Pid) ->
    Timeout = app_helper:get_env(riak_repl, status_timeout, 5000),
    State = try
        %% even though it's named legacy_status, it's BNW code
        riak_repl2_fssink:legacy_status(Pid, Timeout)
    catch
        _:_ ->
            too_busy
    end,
   %% {Pid, erlang:process_info(Pid, message_queue_len), State}.
    {sink_stats, [{pid,riak_repl_util:safe_pid_to_list(Pid)},
                  erlang:process_info(Pid, message_queue_len),
                  {fs_connected_to, State}]}.

sum_rt_send_kbps(Stats) ->
    sum_rt_kbps(Stats, send_kbps).

sum_rt_recv_kbps(Stats) ->
    sum_rt_kbps(Stats, recv_kbps).

sum_rt_kbps(Stats, KbpsDirection) ->
    Sinks = proplists:get_value(sinks, Stats, []),
    Sources = proplists:get_value(sources, Stats, []),
    Kbpss = lists:foldl(fun({StatKind, SinkProps}, Acc) ->
        Path1 = case StatKind of
            sink_stats -> rt_sink_connected_to;
            source_stats -> rt_source_connected_to;
            _Else -> not_found
        end,
        KbpsStr = proplists_get([Path1, socket, KbpsDirection], SinkProps, "[]"),
        get_first_kbsp(KbpsStr) + Acc
    end, 0, Sinks ++ Sources),
    Kbpss.

sum_fs_send_kbps(Stats) ->
    sum_fs_kbps(Stats, send_kbps).

sum_fs_recv_kbps(Stats) ->
    sum_fs_kbps(Stats, recv_kbps).

sum_fs_kbps(Stats, Direction) ->
    Coordinators = proplists:get_value(fullsync_coordinator, Stats),
    CoordFoldFun = fun({_SinkName, FSCoordStats}, Acc) ->
        CoordKbpsStr = proplists_get([socket, Direction], FSCoordStats, "[]"),
        CoordKbps = get_first_kbsp(CoordKbpsStr),
        CoordSourceKpbs = sum_fs_source_kbps(FSCoordStats, Direction),
        SinkKbps = sum_fs_sink_kbps(Stats, Direction),
        Acc + CoordKbps + CoordSourceKpbs + SinkKbps
    end,
    CoordSrvs = proplists:get_value(fullsync_coordinator_srv, Stats),
    CoordSrvsFoldFun = fun({_IPPort, SrvStats}, Acc) ->
        KbpsStr = proplists_get([socket, Direction], SrvStats, "[]"),
        Kbps = get_first_kbsp(KbpsStr),
        Kbps + Acc
    end,
    lists:foldl(CoordFoldFun, 0, Coordinators) + lists:foldl(CoordSrvsFoldFun, 0, CoordSrvs).

sum_fs_source_kbps(CoordStats, Direction) ->
    Running = proplists:get_value(running_stats, CoordStats, []),
    FoldFun = fun({_Pid, Stats}, Acc) ->
        KbpsStr = proplists_get([socket, Direction], Stats, "[]"),
        Kbps = get_first_kbsp(KbpsStr),
        Acc + Kbps
    end,
    lists:foldl(FoldFun, 0, Running).

sum_fs_sink_kbps(Stats, Direction) ->
    Sinks = proplists:get_value(sinks, Stats, []),
    FoldFun = fun({sink_stats, SinkStats}, Acc) ->
        case proplists_get([fs_connected_to, socket, Direction], SinkStats) of
            undefined ->
                Acc;
            KbpsStr ->
                Kbps = get_first_kbsp(KbpsStr),
                Acc + Kbps
        end
    end,
    lists:foldl(FoldFun, 0, Sinks).

proplists_get(Path, Props) ->
    proplists_get(Path, Props, undefined).

proplists_get([], undefined, Default) ->
    Default;
proplists_get([], Value, _Default) ->
    Value;
proplists_get([Key], Props, Default) when is_list(Props) ->
    Value = proplists:get_value(Key, Props),
    proplists_get([], Value, Default);
proplists_get([Key | Path], Props, Default) when is_list(Props) ->
    case proplists:get_value(Key, Props) of
        undefined ->
            Default;
        too_busy ->
            lager:debug("Something was too busy to give stats"),
            Default;
        AList when is_list(AList) ->
            proplists_get(Path, AList, Default);
        Wut ->
            lager:warning("~p Not a list when getting stepwise key ~p: ~p", [Wut, [Key | Path], Props]),
            Default
    end.

get_first_kbsp(Str) ->
    case simple_parse(Str ++ ".") of
        [] -> 0;
        [V | _] -> V
    end.

simple_parse(Str) ->
    {ok, Tokens, _EoL} = erl_scan:string(Str),
    {ok, AbsForm} = erl_parse:parse_exprs(Tokens),
    {value, Value, _Bs} = erl_eval:exprs(AbsForm, erl_eval:new_bindings()),
    Value.
