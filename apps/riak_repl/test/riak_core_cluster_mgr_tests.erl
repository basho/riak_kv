%% Eunit test cases for the Connection Manager
%% Copyright (c) 2012 Basho Technologies, Inc.  All Rights Reserved.

-module(riak_core_cluster_mgr_tests).
-compile([export_all, nowarn_export_all]).

-ifdef(TEST).
-include("riak_core_cluster.hrl").
-include("riak_core_connection.hrl").
-include_lib("eunit/include/eunit.hrl").

%% internal functions
%-export([ctrlService/5, ctrlServiceProcess/5]).

%% For testing, both clusters have to look like they are on the same machine
%% and both have the same port number for offered sub-protocols. "my cluster"
%% is using the standard "cluster_mgr" protocol id, while "remote cluster" is
%% using a special test protocol "test_cluster_mgr" to allow us to fake out
%% the service manager and let two cluster managers run on the same IP and Port.

%% My cluster
-define(MY_CLUSTER_NAME, "bob").
-define(MY_CLUSTER_ADDR, {"127.0.0.1", 4097}).

%% Remote cluster
-define(REMOTE_CLUSTER_NAME, "betty").
-define(REMOTE_CLUSTER_ADDR, {"127.0.0.1", 4097}).
-define(REMOTE_MEMBERS, [{"127.0.0.1",5001}, {"127.0.0.1",5002}, {"127.0.0.1",5003}]).
-define(MULTINODE_REMOTE_ADDR, {"127.0.0.1", 6097}).

single_node_test_() ->
    {setup,
    fun start_link_setup/0,
    fun cleanup/1,
    fun(_) -> [

        {"is leader", ?_assert(riak_core_cluster_mgr:get_is_leader() == false)},

        {"become leader", fun() ->
            become_leader(),
            ?assert(node() == riak_core_cluster_mgr:get_leader()),
            ?assert(riak_core_cluster_mgr:get_is_leader() == true)
        end},

        {"no leader, become proxy", fun() ->
            riak_core_cluster_mgr:set_leader(undefined, self()),
            ?assert(riak_core_cluster_mgr:get_is_leader() == false)
        end},

        {"register member fun", fun() ->
            MemberFun = fun(_Addr) -> ?REMOTE_MEMBERS end,
            riak_core_cluster_mgr:register_member_fun(MemberFun),
            Members = gen_server:call(?CLUSTER_MANAGER_SERVER, {get_my_members, ?MY_CLUSTER_ADDR}),
            ?assert(Members == ?REMOTE_MEMBERS)
        end},

        {"register save cluster members", fun() ->
            Fun = fun(_C,_M) -> ok end,
            riak_core_cluster_mgr:register_save_cluster_members_fun(Fun)
        end},

        {"regsiter restore cluster members fun", fun() ->
            Fun = fun() -> [{test_name_locator,?REMOTE_CLUSTER_ADDR}] end,
            riak_core_cluster_mgr:register_restore_cluster_targets_fun(Fun),
            ok
        end},

        {"get known clusters when empty", fun() ->
            Clusters = riak_core_cluster_mgr:get_known_clusters(),
            lager:debug("get_known_clusters_when_empty_test(): ~p", [Clusters]),
            ?assert({ok,[]} == Clusters)
        end},

        {"get ipaddrs of cluster with unknown name", ?_assert({ok,[]} == riak_core_cluster_mgr:get_ipaddrs_of_cluster("unknown"))},

        {"add remote cluster multiple times but can still resolve", fun() ->
            riak_core_cluster_mgr:add_remote_cluster(?REMOTE_CLUSTER_ADDR),
            ?assert({ok,[]} == riak_core_cluster_mgr:get_known_clusters()),
            riak_core_cluster_mgr:add_remote_cluster(?REMOTE_CLUSTER_ADDR),
            ?assert({ok,[]} == riak_core_cluster_mgr:get_known_clusters())
        end},

        {"add remote while leader", fun() ->
            ?assert(riak_core_cluster_mgr:get_is_leader() == false),
            become_leader(),
            riak_core_cluster_mgr:add_remote_cluster(?REMOTE_CLUSTER_ADDR),
            ?assert({ok,[]} == riak_core_cluster_mgr:get_known_clusters()),
            riak_core_cluster_mgr:add_remote_cluster(?REMOTE_CLUSTER_ADDR),
            ?assert({ok,[]} == riak_core_cluster_mgr:get_known_clusters())
        end},

        {"connect to remote cluster", fun() ->
            become_leader(),
            lager:info("Is leader:~p~n", [riak_core_cluster_mgr:get_is_leader()]),
            timer:sleep(2000),
            DoneFun = fun() ->
                Out = riak_core_cluster_mgr:get_known_clusters(),
                case Out of
                    {ok, [?REMOTE_CLUSTER_NAME]} = Out ->
                        {done, Out};
                    _ ->
                        busy
                end
            end,
            Knowners = wait_for(DoneFun),
            ?assertEqual({ok, [?REMOTE_CLUSTER_NAME]}, Knowners)
        end},

        %% We should get "127.0.0.1",5002 as first in the list every time
        %% since local is always nonode@nohost
        {"get ipaddres of cluster", fun() ->
            {ok, Original} = riak_core_cluster_mgr:get_ipaddrs_of_cluster(?REMOTE_CLUSTER_NAME),
            ?assertEqual({"127.0.0.1",5002}, hd(Original)),
            ?assertEqual({ok,Original},riak_core_cluster_mgr:get_ipaddrs_of_cluster(?REMOTE_CLUSTER_NAME)),
            ?assertEqual({ok,Original},riak_core_cluster_mgr:get_ipaddrs_of_cluster(?REMOTE_CLUSTER_NAME)),
            ?assertEqual({ok,Original},riak_core_cluster_mgr:get_ipaddrs_of_cluster(?REMOTE_CLUSTER_NAME))
        end}

    ] end }.

wait_for(Fun) ->
    wait_for(Fun, 2000, 10).

wait_for(Fun, Remaining, _Interval) when Remaining =< 0 ->
    case Fun() of
        {done, Out} ->
            Out;
        busy ->
            {error, timeout}
    end;

wait_for(Fun, Remaining, Interval) ->
    case Fun() of
        {done, Out} ->
            Out;
        busy ->
            timer:sleep(Interval),
            wait_for(Fun, Remaining - Interval, Interval)
    end.

%% XXX this test is disabled because it doesn't run when certain gostname
%% configurations are used. Disabling it until we can rewrite it as a
%% riak_test.
multinode_test__() ->
    {setup, fun() ->
        % superman, batman, and wonder woman are all part of the JLA
        % (Justice League of America). Superman is the defacto leader, with
        % batman and wonder woman as 2 and 3.
        NodeConfigs = [
            {rt_superman, [{port, 6001}]},
            {rt_batman, [{port, 6002}]},
            {rt_wonderwoman, [{port, 6003}]}
        ],
        TearDownHints = maybe_start_master(),
        Localhost = list_to_atom(net_adm:localhost()),
        CodePath = code:get_path(),
        ResAndNode = [begin
            {ok, Node} = start_setup_node(Name, Localhost),
            sync_paths(CodePath, Node),
            Port = proplists:get_value(port, Props),
            R = rpc:call(Node, ?MODULE, start_link_setup, [{"127.0.0.1", Port}]),
            {R, Node}
        end || {Name, Props} <- NodeConfigs],
        {Res, Nodes} = lists:unzip(ResAndNode),
        ?debugFmt("Setup result:  ~p", [Res]),
        [watchit(Pid) || Pid <- lists:flatten(Res)],
        {TearDownHints, Nodes}
    end,
    fun({TDH, Nodes}) ->
        rpc:multicall(Nodes, ?MODULE, cleanup, []),
        teardown_nodes(TDH, Nodes)
    end,
    fun({_TDH, [Superman, Batman, Wonder] = Nodes}) -> [

        {"leader election responses", fun() ->
            rpc:multicall(Nodes, riak_core_cluster_mgr, set_leader, [Superman, self()]),
            {Leaders, []} = rpc:multicall(Nodes, riak_core_cluster_mgr, get_leader, []),
            [?assertEqual(Superman, L) || L <- Leaders],
            {IsLeaders, []} = rpc:multicall(Nodes, riak_core_cluster_mgr, get_is_leader, []),
            ?assertEqual([true, false, false], IsLeaders)
        end},

        {"swap leaders", fun() ->
            rpc:multicall(Nodes, riak_core_cluster_mgr, set_leader, [
                Batman, self()]),
            {Leaders, []} = rpc:multicall(Nodes, riak_core_cluster_mgr, get_leader, []),
            [?assertEqual(Batman, L) || L <- Leaders],
            {IsLeaders, []} = rpc:multicall(Nodes, riak_core_cluster_mgr,
                get_is_leader, []),
            ?assertEqual([false, true, false], IsLeaders),

            % reset the leader to superman for the rest of the tests.
            rpc:multicall(Nodes, riak_core_cluster_mgr, set_leader, [Superman, self()])
        end},

        {"register member fun", fun() ->
            MemberFun = fun ?MODULE:register_member_fun_cb/1,
            rpc:multicall(Nodes, riak_core_cluster_mgr, register_member_fun, [MemberFun]),
            {Res, []} = rpc:multicall(Nodes, gen_server, call, [?CLUSTER_MANAGER_SERVER, {get_my_members, {"127.0.0.1", 6001}}]),
            Expected = repeat(?REMOTE_MEMBERS, 3),
            ?assertEqual(Expected, Res)
        end},

        {"get known clusters when empty", fun() ->
            % need to register member fun and save cluster members first
            MemberFun = fun ?MODULE:return_ok/2,
            rpc:multicall(Nodes, riak_core_cluter_mgr, register_save_cluster_members_fun, [MemberFun]),

            % and the restore fun
            %RestoreFun = fun ?MODULE:restore_fun/0,
            RestoreFun = fun ?MODULE:multinode_restore_fun/0,
            rpc:multicall(Nodes, riak_core_cluster_mgr, register_restore_cluster_targets_fun, [RestoreFun]),

            % and now the test proper.
            {Res, []} = rpc:multicall(Nodes, riak_core_cluster_mgr, get_known_clusters, []),
            Expected = repeat({ok, []}, 3),
            ?assertEqual(Expected, Res)
        end},

        {"get ipaddrs of cluster with unknown name", fun() ->
            {Res, []} = rpc:multicall(Nodes, riak_core_cluster_mgr, get_ipaddrs_of_cluster, ["unknown"]),
            Expected = repeat({ok, []}, 3),
            ?assertEqual(Expected, Res)
        end},

        {"get list of remote cluster names", fun() ->
            {Res, []} = rpc:multicall(Nodes, riak_core_cluster_mgr, get_known_clusters, []),
            Expected = repeat({ok, ["unknown"]}, 3),
            ?assertEqual(Expected, Res)
        end},

        {"remove cluster by name from non-leader node", fun() ->
            rpc:call(Batman, riak_core_cluster_mgr, remove_remote_cluster, ["unknown"]),
            % removals are done by casts, and since we're asking for a
            % removal from a different node, our asking could arrive before
            % the removal request to the leader occurs, so do it twice
            % (preferred over timeout because it will scale better to the
            % speed of the machine running the test).
            rpc:multicall(Nodes, riak_core_cluster_mgr, get_known_clusters, []),
            {Res, []} = rpc:multicall(Nodes, riak_core_cluster_mgr, get_known_clusters, []),
            Expected = repeat({ok, []}, 3),
            ?assertEqual(Expected, Res)
        end},

        {"add remote cluster from two different non-leader nodes, but still resolve", fun() ->
            rpc:call(Batman, riak_core_cluster_mgr, add_remote_cluster, [?REMOTE_CLUSTER_ADDR]),
            {Res, []} = rpc:multicall(Nodes, riak_core_cluster_mgr, get_known_clusters, []),
            Expected = repeat({ok, []}, 3),
            ?assertEqual(Expected, Res),
            rpc:call(Wonder, riak_core_cluster_mgr, add_remote_cluster, [?REMOTE_CLUSTER_ADDR]),
            ?assertEqual({Res, []}, rpc:multicall(Nodes, riak_core_cluster_mgr, get_known_clusters, []))
        end},

        {"connect to remote cluster", fun() ->
            rpc:multicall(Nodes, riak_core_cluster_mgr, set_leader, [Superman, undefined]),
            start_fake_remote_cluster_listener(),
            rpc:call(Superman, riak_core_cluster_mgr, add_remote_cluster, [?MULTINODE_REMOTE_ADDR]),
            ?debugFmt("Connections:  ~p", [rpc:call(Superman, riak_core_cluster_conn_sup, connections, [])]),
            {Res, []} = rpc:multicall(Nodes, riak_core_cluster_mgr, get_known_clusters, []),
            Expected = repeat({ok, [?REMOTE_CLUSTER_NAME]}, 3),
            ?assertEqual(Expected, Res)
        end},

        {"get ipaddres of cluster", fun() ->
            Original = [{"127.0.0.1",5001}, {"127.0.0.1",5002}, {"127.0.0.1",5003}],
            Rotated1 = [{"eunit:test([riak_core_cluster_mgr_tests]).127.0.0.1",5002}, {"127.0.0.1",5003}, {"127.0.0.1",5001}],
            Rotated2 = [{"127.0.0.1",5003}, {"127.0.0.1",5001}, {"127.0.0.1",5002}],
            %{[R1, R2, R3], []} = rpc:multicall(Nodes, riak_core_cluster_mgr, get_ipaddrs_of_cluster, [?REMOTE_CLUSTER_NAME]),
            R1 = rpc:call(Superman, riak_core_cluster_mgr,
                          get_ipaddrs_of_cluster, [?REMOTE_CLUSTER_NAME]),
            R2 = rpc:call(Batman, riak_core_cluster_mgr,
                          get_ipaddrs_of_cluster, [?REMOTE_CLUSTER_NAME]),
            R3 = rpc:call(Wonder, riak_core_cluster_mgr,
                          get_ipaddrs_of_cluster, [?REMOTE_CLUSTER_NAME]),
            ?assertEqual({ok, Original}, R1),
            ?assertEqual({ok, Rotated1}, R2),
            ?assertEqual({ok, Rotated2}, R3)
        end}

    ] end }.

%% this test runs first and leaves the server running for other tests
start_link_setup() ->
    start_link_setup(?MY_CLUSTER_ADDR).

start_link_setup(ClusterAddr) ->
    Started = riak_repl_test_util:maybe_start_lager(),
    % core features that are needed
    {ok, Eventer} = riak_core_ring_events:start_link(),
    {ok, RingMgr} = riak_core_ring_manager:start_link(test),

    % needed by the leaders
    meck:new(riak_core_node_watcher_events, [no_link]),
    meck:expect(riak_core_node_watcher_events, add_sup_callback, fun(_fn) ->
        ok
    end),
    meck:new(riak_core_node_watcher, [no_link]),
    meck:expect(riak_core_node_watcher, nodes, fun(_) ->
        [node()]
    end),

    {ok, Leader} = riak_repl2_leader:start_link(),

    ok = application:start(ranch),
    %% we also need to start the other connection servers
    {ok, Pid1} = riak_core_service_mgr:start_link(ClusterAddr),
    {ok, Pid2} = riak_core_connection_mgr:start_link(),
    {ok, Pid3} = riak_core_cluster_conn_sup:start_link(),
    %% now start cluster manager
    {ok, Pid4 } = riak_core_cluster_mgr:start_link(),
    start_fake_remote_cluster_service(),
    {Started, [Leader, Eventer, RingMgr, Pid1, Pid2, Pid3, Pid4]}.

watchit(Pid) ->
    proc_lib:spawn(?MODULE, watchit_loop, [Pid]).

watchit_loop(Pid) ->
    Mon = erlang:monitor(process, Pid),
    watchit_loop(Pid, Mon).

watchit_loop(Pid, Mon) ->
    receive
        {'DOWN', Mon, process, Pid, normal} ->
            % not going to worry about normal exits
            ok;
        {'DOWN', Mon, process, Pid, Cause} ->
            ?debugFmt("~n== DEATH OF A MONITORED PROCESS ==~n~p died due to ~p", [Pid, Cause]),
            ok;
        _ ->
            watchit_loop(Pid, Mon)
    end.

cleanup({Apps, Pids}) ->
    process_flag(trap_exit, true),
    stop_fake_remote_cluster_service(),
    riak_core_service_mgr:stop(),
    riak_core_connection_mgr:stop(),
    %% tough to stop a supervisor
    catch exit(riak_core_cluster_conn_sup),
    catch exit(riak_repl2_leader_gs),
    riak_core_cluster_mgr:stop(),
    application:stop(ranch),
    riak_core_ring_manager:stop(),
    catch exit(riak_core_ring_events, kill),
    [catch exit(Pid, kill) || Pid <- Pids],
    meck:unload(),
    ok = riak_repl_test_util:stop_apps(Apps),
    process_flag(trap_exit, false),
    ok.

maybe_start_master() ->
    case node() of
        'nonode@nohost' ->
            net_kernel:start([riak_repl_tests, shortnames]),
            {started, node()};
        Node ->
            {kept, Node}
    end.

setup_nodes(NodeNames) ->
    Started = maybe_start_master(),
    case start_setup_nodes(NodeNames) of
        {ok, Slaves} ->
            {Started, Slaves};
        {error, {Running, Failer, What}} ->
            teardown_nodes(Started, Running ++ [Failer]),
            erlang:error({bad_node_startup, {Failer, What}})
    end.

start_setup_nodes(NodeNames) ->
    Localhost = list_to_atom(net_adm:localhost()),
    CodePath = code:get_path(),
    start_setup_nodes(NodeNames, Localhost, CodePath, []).

start_setup_nodes([], _Host, _CodePath, Acc) ->
    {ok, lists:reverse(Acc)};

start_setup_nodes([Name | Tail], Host, CodePath, Acc) ->
    case start_setup_node(Name, Host) of
        {ok, Node} ->
            true = sync_paths(CodePath, Node),
            %true = rpc:call(Node, code, set_path, [CodePath]),
            start_setup_nodes(Tail, Host, CodePath, [Node | Acc]);
        {error, {Node, What}} ->
            {error, {Acc, Node, What}}
    end.

sync_paths([], _Node) ->
    true;

sync_paths(["." | Paths], Node) ->
    rpc:call(Node, code, set_path, ["."]),
    sync_paths(Paths, Node);

sync_paths([D | Tail], Node) ->
    case rpc:call(Node, code, add_pathz, [D]) of
        true ->
            sync_paths(Tail, Node);
        {error, bad_directory} ->
            ?debugFmt("Hope this directory wasn't important:  ~s", [D]),
            sync_paths(Tail, Node)
    end.

start_setup_node(Name, Host) ->
    Opts = [{monitor_master, true}],
    case ct_slave:start(Host, Name, Opts) of
        {ok, SlaveNode} = O ->
            % to get io:formats and such (body snatch the user proc!)
            User = rpc:call(SlaveNode, erlang, whereis, [user]),
            exit(User, kill),
            rpc:call(SlaveNode, slave, pseudo, [node(), [user]]),
            O;
        {error, Cause, Node} ->
            {error, {Node, Cause}}
    end.

teardown_nodes({Started, _Master}, Slaves) ->
    [ct_slave:stop(S) || S <- Slaves, is_atom(S)],
    case Started of
        kept ->
            ok;
        started ->
            net_kernel:stop()
    end.

register_member_fun_cb(_Addr) ->
    ?REMOTE_MEMBERS.

return_ok(_,_) -> ok.

restore_fun() ->
    [{test_name_locator, ?REMOTE_CLUSTER_ADDR}].

multinode_restore_fun() ->
    [{test_name_locator, ?MULTINODE_REMOTE_ADDR}].

repeat(Term, N) ->
    [Term || _ <- lists:seq(1, N)].

%%--------------------------
%% helper functions
%%--------------------------

start_fake_remote_cluster_service() ->
    %% start our cluster_mgr service under a different protocol id,
    %% which the cluster manager will use during testing to connect to us.
    ServiceProto = {test_cluster_mgr, [{1,0}]},
    ServiceSpec = {ServiceProto, {?CTRL_OPTIONS, ?MODULE, ctrlService, []}},
    riak_core_service_mgr:register_service(ServiceSpec, {round_robin,10}).

stop_fake_remote_cluster_service() ->
    ServiceProto = {test_cluster_mgr, [{1,0}]},
    riak_core_service_mgr:unregister_service(ServiceProto).

become_leader() ->
    riak_core_cluster_mgr:set_leader(node(), self()).

start_fake_remote_cluster_listener() ->
    start_fake_remote_cluster_listener(?MULTINODE_REMOTE_ADDR).

start_fake_remote_cluster_listener({IP, Port}) when is_list(IP) ->
    {ok, IP2} = inet_parse:address(IP),
    start_fake_remote_cluster_listener({IP2, Port});

start_fake_remote_cluster_listener({IP, Port}) ->
    proc_lib:spawn_link(fun() ->
        case gen_tcp:listen(Port, [binary, {ip, IP}, {active, false}, {packet, 4}, {nodelay, true}]) of
            {ok, Listen} ->
                fake_remote_cluster_loop(listen, Listen, undefined);
            What ->
                ?debugFmt("couldn't get fakey: ~p", [What])
        end
    end).

fake_remote_cluster_loop(listen, ListenSock, undefined) ->
    case gen_tcp:accept(ListenSock, 60000) of
        {ok, Sock} ->
            fake_remote_cluster_loop(connected, Sock, undefined);
        {error, What} ->
            ?debugFmt("Fake cluster going down: ~p", [What])
    end;

fake_remote_cluster_loop(connected, Sock, undefined) ->
    case gen_tcp:recv(Sock, 0, 60000) of
        {ok, Data} ->
            Term = binary_to_term(Data),
            ?debugFmt("data got: ~p", [Term]),
            fake_remote_cluster_loop(got_hello, Sock, Term);
        {error, Reason} ->
            ?debugFmt("Fake cluster in state connected going down: ~p", [Reason])
    end;

fake_remote_cluster_loop(got_hello, Sock, {?CTRL_HELLO, ?CTRL_REV, Capabilies}) ->
    gen_tcp:send(Sock, term_to_binary({?CTRL_ACK, ?CTRL_REV, Capabilies})),
    case gen_tcp:recv(Sock, 0, 60000) of
        {ok, Data} ->
            Term = binary_to_term(Data),
            ?debugFmt("data got: ~p", [Term]),
            fake_remote_cluster_loop(got_protocol, Sock, Term);
        What ->
            ?debugFmt("fake cluster exiting: ~p", [What])
    end;

fake_remote_cluster_loop(got_protocol, Sock, {test_cluster_mgr, [?CTRL_REV]} = State) ->
    {Major, Minor} = ?CTRL_REV,
    gen_tcp:send(Sock, term_to_binary({ok, {test_cluster_mgr, {Major, Minor, Minor}}})),
    fake_remote_cluster_loop(service_loop, Sock, State);

fake_remote_cluster_loop(service_loop, Sock, {test_cluster_mgr, [?CTRL_REV]} = State) ->
    case gen_tcp:recv(Sock, 0, 6000) of
        {ok, ?CTRL_ASK_NAME} ->
            ?debugMsg("name requested"),
            gen_tcp:send(Sock, term_to_binary("betty")),
            fake_remote_cluster_loop(service_loop, Sock, State);
        {ok, ?CTRL_ASK_MEMBERS} ->
            ?debugMsg("members requested"),
            read_ip_address(Sock, gen_tcp, undefined),
            gen_tcp:send(Sock, term_to_binary(?REMOTE_MEMBERS)),
            fake_remote_cluster_loop(service_loop, Sock, State);
        {ok, Data} ->
            ?debugFmt("Did not understand message: ~p", [Data]);
        TcpReadErr ->
            ?debugFmt("fake cluster exiting: ~p", [TcpReadErr])
    end.


%%-----------------------------------
%% control channel services EMULATION
%%-----------------------------------

%% Note: this service module matches on test_cluster_mgr, so it will fail with
%% a function_clause error if the cluster manager isn't using our special test
%% protocol-id. Of course, it did once or I wouldn't have written this note :-)

ctrlService(_Socket, _Transport, {error, Reason}, _Args, _Props) ->
    lager:debug("Failed to accept control channel connection: ~p", [Reason]);
ctrlService(Socket, Transport, {ok, {test_cluster_mgr, MyVer, RemoteVer}}, Args, Props) ->
    RemoteClusterName = proplists:get_value(clustername, Props),
    lager:debug("ctrlService: received connection from cluster: ~p", [RemoteClusterName]),
    Pid = proc_lib:spawn_link(?MODULE,
                              ctrlServiceProcess,
                              [Socket, Transport, MyVer, RemoteVer, Args]),
    Transport:controlling_process(Socket, Pid),
    {ok, Pid}.

read_ip_address(Socket, Transport, Remote) ->
    case Transport:recv(Socket, 0, ?CONNECTION_SETUP_TIMEOUT) of
        {ok, BinAddr} ->
            MyAddr = binary_to_term(BinAddr),
            lager:debug("Cluster Manager: remote thinks my addr is ~p", [MyAddr]),
            lager:info("Cluster Manager: remote thinks my addr is ~p", [MyAddr]),
            MyAddr;
        Error ->
            lager:error("Cluster mgr: failed to receive ip addr from remote ~p: ~p",
                        [Remote, Error]),
            undefined
    end.

%% process instance for handling control channel requests from remote clusters.
ctrlServiceProcess(Socket, Transport, MyVer, RemoteVer, Args) ->
    case Transport:recv(Socket, 0, infinity) of
        {ok, ?CTRL_ASK_NAME} ->
            %% remote wants my name
            lager:debug("wants my name"),
            MyName = ?REMOTE_CLUSTER_NAME,
            Transport:send(Socket, term_to_binary(MyName)),
            ctrlServiceProcess(Socket, Transport, MyVer, RemoteVer, Args);
        {ok, ?CTRL_ASK_MEMBERS} ->
            lager:debug("wants my members"),
            %% remote wants list of member machines in my cluster
            MyAddr = read_ip_address(Socket, Transport, ?MY_CLUSTER_ADDR),
            lager:debug("  client thinks my Addr is ~p", [MyAddr]),
            Members = ?REMOTE_MEMBERS,
            Transport:send(Socket, term_to_binary(Members)),
            ctrlServiceProcess(Socket, Transport, MyVer, RemoteVer, Args);
        {error, Reason} ->
            lager:debug("Failed recv on control channel. Error = ~p", [Reason]),
            % nothing to do now but die
            {error, Reason};
        Other ->
            ?debugFmt("Recv'd unknown message on cluster control channel: ~p",
                      [Other]),
            {error, bad_cluster_mgr_message}
    end.

-endif.
