%% Riak EnterpriseDS
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.

%%
%% Replication leader EQC
%%
%% This quickcheck test exercises leadership elections. It attempts
%% to simulate nodes going up and down with different candidates
%% for leaders in a way similar to ring gossips.  The test state
%% keeps a list of nodes and for each whether node should be a
%% candidate (a listener) or a worker (a non-listener).  Nodes
%% have their type toggled at random and the list is updated
%% at random.
%%
%% The test properties make sure that nodes with the same list
%% of candidates/workers elect the same leader.  Nodes are
%% created using slave() which causes some interactions between
%% cover and generates some warning messages
%%

-module(repl_leader_eqc).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_fsm.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

-record(replnode, {node,
                   running=false,
                   type=worker,
                   candidates=[],
                   workers=[]}).
-record(state, { replnodes=[] }). % {name, stopped | running}

-define(TEST_TIMEOUT, 30 * 60).
-define(MAX_NODES, 5).
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).

-define(DBG(Fmt,Args),ok).
%%-define(DBG(Fmt,Args),io:format(user, Fmt, Args)).

qc_test_() ->
    %% try and clean the repl controller before cover ever runs
    %code:purge(riak_repl_controller), 
    %code:delete(riak_repl_controller),
    ?DBG("Cover modules:\n~p\n", [cover:modules()]),
    Prop = ?QC_OUT(eqc:numtests(40, prop_main())),
    case testcase() of
        [] ->
            {timeout, ?TEST_TIMEOUT, fun() -> ?assert(eqc:quickcheck(Prop)) end};
        Testcase ->
            {timeout, ?TEST_TIMEOUT, fun() -> ?assert(eqc:check(Prop, Testcase)) end}
    end.

testcase() ->
    [].

prop_main() ->
    ?FORALL(Cmds, commands(?MODULE),
            begin
                %% Setup
                ?DBG("\n=== Starting ===\n", []),
                os:cmd("rm -rf repl_leader_eqc_data"),
                maybe_start_net_kernel(),
                code:ensure_loaded(riak_repl_leader),
                code:ensure_loaded(riak_repl_leader_helper),
                %% Run tests
                start_slave_driver(),
                try
                    {H, {_State, _StateData}, Res} = run_commands(?MODULE,Cmds),
                    ?WHENFAIL(begin
                                  io:format(user, "Test Failed\n~p\n",
                                            [zip(state_names(H),command_names(Cmds))]),
                                  io:format(user, "State: ~p\nStateData: ~p\nRes: ~p\n",
                                            [_State, _StateData, Res])
                          end,
                              %% Generate statistics
                              aggregate(zip(state_names(H),command_names(Cmds)), Res == ok))
                after
                    stop_slave_driver(),
                    net_kernel:stop()
                end
            end).

%
%% ====================================================================
%% eqc_fsm callbacks
%% ====================================================================

initial_state() ->
    running.

initial_state_data() ->
    #state{}.

running(S) ->
    [{running, {call, ?MODULE, new_node, []}},
     {running, {call, ?MODULE, start_repl, [g_stopped_replnode(S)]}},
     {running, {call, ?MODULE, stop_repl, [g_running_node(S)]}},
     {running, {call, ?MODULE, toggle_type, [g_node(S)]}},
     %% {running, {call, ?MODULE, ping, [g_node(S)]}},
     {running, {call, ?MODULE, set_candidates, [g_running_node(S),
                                                candidates(S),
                                                workers(S),
                                                S]}},
     {running, {call, ?MODULE, check_leaders, [S]}},
     {running, {call, ?MODULE, add_receiver, [g_running_node(S)]}}].

weight(_,_,{call,_,toggle_type,_}) -> 10;
weight(_,_,{call,_,stop_repl,_}) -> 10;
weight(_,_,_) -> 100.

next_state_data(_From, _To, S, _Res, {call, ?MODULE, new_node, []}) ->
    Node = make_node(length(S#state.replnodes)+1),
    add_replnode(Node, S);
next_state_data(_From, _To, S, _Res, {call, ?MODULE, start_repl, [ReplNode]}) ->
    upd_replnode(ReplNode#replnode{running = true}, S);
next_state_data(_From, _To, S, _Res, {call, ?MODULE, stop_repl, [Node]}) ->
    ReplNode = get_replnode(Node, S),
    upd_replnode(ReplNode#replnode{running = false}, S);
next_state_data(_From, _To, S, _Res, {call, ?MODULE, toggle_type, [Node]}) ->
    ReplNode = get_replnode(Node, S),
    case ReplNode#replnode.type of
        worker->
            UpdReplNode=ReplNode#replnode{type = candidate};
        candidate ->
            UpdReplNode=ReplNode#replnode{type = worker}
    end,
    upd_replnode(UpdReplNode, S);
next_state_data(_From, _To, S, _Res, {call, ?MODULE, set_candidates,
                                      [Node, Candidates, Workers, S]}) ->
    ReplNode = get_replnode(Node, S),
    upd_replnode(ReplNode#replnode{candidates = lists:sort(Candidates),
                                   workers = lists:sort(Workers)},
                 S);
next_state_data(_From, _To, S, _Res, _Call) ->
    S.

precondition(_From, _To, S, {call, ?MODULE, new_node, []}) ->
    length(S#state.replnodes) < ?MAX_NODES;
precondition(_From, _To, S, {call, ?MODULE, start_repl, [ReplNode]}) ->
    lists:member(ReplNode, S#state.replnodes) andalso ReplNode#replnode.running =:= false;
precondition(_From, _To, S, {call, ?MODULE, stop_repl, [Node]}) ->
    check_replnode(Node, #replnode.running, true, S);
precondition(_From, _To, S, {call, ?MODULE, toggle_type, [Node]}) ->
    node_exists(Node, S);
precondition(_From, _To, S, {call, ?MODULE, set_candidates, [Node, _C, _W, S]}) ->
    check_replnode(Node, #replnode.running, true, S);
precondition(_From, _To, S, {call, ?MODULE, add_receiver, [Node]}) ->
    check_replnode(Node, #replnode.running, true, S);
precondition(_From, _To, _S, _Call) ->
    true.

postcondition(_From, _To, _S, {call, ?MODULE, start_repl, [_]}, Res) ->
    case Res of
        ok -> true;
        _ -> {start_repl, Res}
    end;
postcondition(_From, _To, S, {call, ?MODULE, ping, [Node]}, Res) ->
    ReplNode = get_replnode(Node, S),
    case ReplNode#replnode.running of 
        true ->
            Res == pong;
        false ->
            Res == pang
    end;
postcondition(_From, _To, S, {call, ?MODULE, check_leaders, _}, LeaderByNode) ->
    NodesByCandidates0 = nodes_by_candidates(running_replnodes(S)),
    NodesByCandidates = lists:filter(fun({{Candidates, _}, Nodes}) ->
                lists:all(fun(Cand) -> lists:member(Cand, Nodes) end, Candidates) 
        end, NodesByCandidates0),
    check_same_leaders(NodesByCandidates, LeaderByNode);
postcondition(_From, _To, _S, {call, ?MODULE, add_receiver, [Node]}, {Leader1, Leader2, Res, _Pid}) when Node == Leader1; Node == Leader2 ->
    %% The node believes itself to be a leader
    case Res of 
        ok ->
            true;
        _ ->
            {add_receiver_expected_ok, Res}
    end;
postcondition(_From, _To, _S, {call, ?MODULE, add_receiver, [_Node]},
              {_Leader1, _Leader2, Res, _Pid}=_R) when _Leader1 /= undefined,
                                                       _Leader2 /= undefined ->
    %% Node thinks somebody else is the leader or not sure who the leader is.
    %% If the candidates/workers is being changed and the node *was* the leader 
    %% then riak_repl_leader continues to act as the leader until the election
    %% completes (but sets the leader undefined).  Will only *definitely*
    %% return {error, not_leader} once the election completes.
    ?DBG("Postcond: n=~p r=~p\n", [_Node, _R]),
    case Res of
        {error, not_leader} ->
            true;
        _ ->
            {add_receiver_expected_err, Res}
    end;
postcondition(_From, _To, _S, _Call, _Res) ->
    true.

%% ====================================================================
%% Generator functions
%% ====================================================================

g_node(S) ->
    ?LET(RN, elements(S#state.replnodes), RN#replnode.node).

g_running_node(S) ->
    elements([RN#replnode.node || 
                 RN <- S#state.replnodes, RN#replnode.running =:= true]).

g_running_candidate(S) ->
    elements([RN#replnode.node || 
                 RN <- S#state.replnodes,
                 RN#replnode.running =:= true,
                 RN#replnode.type =:= candidate ]).

g_stopped_node(S) ->
    elements([RN#replnode.node || 
                 RN <- S#state.replnodes, RN#replnode.running =/= true]).
g_stopped_replnode(S) ->
    elements([RN || RN <- S#state.replnodes, RN#replnode.running =/= true]).
 
%% ====================================================================
%% Actions
%% ====================================================================

new_node() ->
    ok.

start_repl(ReplNode) ->
    Node = ReplNode#replnode.node,
    ?DBG("Starting slave ~p\n", [Node]),
    ok = start_slave(Node),
    pong = net_adm:ping(Node),
    %{ok, _StartedNodes} = cover:start([Node]),
    dbg:n(Node),
    %?DBG("Cover nodes: ~p\n", [_StartedNodes]),
    ?DBG("Started slave ~p\n", [Node]),
    R = rpc:call(Node, ?MODULE, setup_slave, [ReplNode#replnode.candidates,
                                          ReplNode#replnode.workers]),
    ?DBG("slave start returned ~p~n", [R]),
    R.

stop_repl(Node) ->
    ?DBG("Stopping cover on ~p\n", [Node]),
    %cover:stop([Node]),
    ?DBG("Stopping repl on ~p\n", [Node]),
    ok = stop_slave(Node),
    ?DBG("Stopped slave~p\n", [Node]),
    ok.
        
toggle_type(_Node) ->
    ?DBG("Changing type for ~p\n", [_Node]),
    ok.

ping(Node) ->
    ?DBG("Pinging ~p\n", [Node]),
    Res = net_adm:ping(Node),
    ?DBG("Pinged ~p: ~p\n", [Node, Res]),
    Res.

set_candidates(Node, Candidates, Workers, S) ->
    ?DBG("Setting candidates for ~p to {~p, ~p}\n", 
              [Node, Candidates, Workers]),
    pong = net_adm:ping(Node),
    ?DBG("riak_repl_leader pid on ~p is ~p\n", 
              [Node, rpc:call(Node, erlang, whereis, [riak_repl_leader])]),
    ok = rpc:call(Node, riak_repl_leader, set_candidates, [Candidates, Workers]),
 
    %% Request the helper leader node to make any elections stabalize 
    %% before calling the rest of the quickcheck code, otherwise results
    %% are totally unpredictable.

    %% Have to duplicate some work done in next_state here - helper_leader_node
    %% needs an updated [#replnode{}].
    ReplNode = get_replnode(Node, S),
    UpdS = upd_replnode(ReplNode#replnode{candidates = lists:sort(Candidates),
                                          workers = lists:sort(Workers)},
                        S),
    {_HLN, _UpCand} = helper_leader_node(Node, UpdS),
    ?DBG("Set candidates for ~p, HLN=~p, UpCand=~p\n", [Node, _HLN, _UpCand]),
    ok.

check_leaders(S) -> % include a dummy anode from list so
    ?DBG("CheckLeaders - running nodes ~p\n", [shorten(running_nodes(S))]),
    F = fun(RN) ->        % generator will blow up if none running
                {HelperLN, UpCand} = helper_leader_node(RN#replnode.node, S),
                %% Make sure at least one candidate node for the replnode
                %% is running, is a candidate and belongs to the same set of
                %% candidates/workers
                N = RN#replnode.node,
               
                LN = rpc:call(N, riak_repl_leader, leader_node, []),
                ?DBG("  ~p: {~p, ~p} LN=~p HLN=~p UpCand=~p\n",
                          [N, shorten(RN#replnode.candidates),
                           shorten(RN#replnode.workers), LN, HelperLN,
                           UpCand]),
                {N, LN, HelperLN, UpCand}
        end,
    [F(N) || N <- running_replnodes(S)].
 
add_receiver(N) ->
    % R = {Leader1, Leader2, Pid, Res},
    R = rpc:call(N, ?MODULE, register_receiver, []),
    ?DBG("add_receiver: ~p\n", [R]),
    R.

%% ====================================================================
%% Internal functions
%% ====================================================================

nodes_by_candidates(ReplNodes) ->
    %% Build a dict of all nodes with the same config
    %% Check they agree on who the leader is
    F = fun(ReplNode, D) ->
                Key = {lists:sort(ReplNode#replnode.candidates),
                       lists:sort(ReplNode#replnode.workers)},
                orddict:append_list(Key, [ReplNode#replnode.node], D)
        end,
    lists:foldl(F, orddict:new(), ReplNodes).

check_same_leaders([], _LeaderByNode) ->
    true;
check_same_leaders([{{C,W},Nodes}|Rest], LeaderByNode) ->
    Leaders = lookup_leaders(Nodes, LeaderByNode),
    UniqLeaders = lists:usort([Ldr || {_Node,Ldr} <- Leaders]),
    case UniqLeaders of
        [_SingleLeader] ->
            check_same_leaders(Rest, LeaderByNode);
        ManyLeaders ->
            {different_leaders, ManyLeaders,
             {candidates, C},
             {workers, W},
             {nodes, Nodes},
             {leaders, Leaders},
             {all_leader_info, LeaderByNode}}
    end.

%% For each node, lookup the current leader and build a {Node, LeaderName} tuple.
lookup_leaders(Nodes, LeaderByNode) ->
    F = fun(N, A) ->
                try
                    {N, LN, _HLN, _Cs} = lists:keyfind(N, 1, LeaderByNode),
                    [{N, LN} | A]
                catch
                    _:Error ->
                        throw({cannot_find, N, LeaderByNode, Error})
                end
        end,
    lists:foldl(F, [], Nodes).

maybe_start_net_kernel() ->
    [] = os:cmd("epmd -daemon"),
    case net_kernel:start(['repl_leader_eqc@127.0.0.1', longnames]) of
        {ok, _} ->
            ?DBG("Net kernel started as ~p\n", [node()]);
        {error, {already_started, _}} ->
            ok;
        {error, Reason} ->
            throw({start_net_kernel_failed, Reason})
    end.

good_path() ->
    [filename:absname(D) || 
        D <- lists:filter(fun filelib:is_dir/1, code:get_path())].

make_node(N) ->
    list_to_atom("n" ++ integer_to_list(N) ++ "@" ++ get_host(node())).

get_name(Node) ->
    list_to_atom(hd(string:tokens(atom_to_list(Node), "@"))).

get_host(_Node) ->
    "127.0.0.1".

shorten(Nodes) ->
    [get_name(N) || N <- Nodes].

candidates(S) ->
    [RN#replnode.node || RN <- S#state.replnodes, RN#replnode.type =:= candidate].

workers(S) ->
    [RN#replnode.node || RN <- S#state.replnodes, RN#replnode.type =:= worker].

running_nodes(S) ->
    [RN#replnode.node || RN <- running_replnodes(S)].

running_replnodes(S) ->
    [RN || RN <- S#state.replnodes, RN#replnode.running =:= true].

get_replnode(Node, S) ->
    {value, ReplNode} = lists:keysearch(Node, #replnode.node, S#state.replnodes),
    ReplNode.

add_replnode(Node, S) ->
    ReplNode = #replnode{node = Node,
                         candidates = lists:sort(candidates(S)),
                         workers = lists:sort([Node | workers(S)])},
    UpdReplNodes = lists:keystore(Node, #replnode.node,
                                  S#state.replnodes, ReplNode),
    S#state{replnodes = UpdReplNodes}.

upd_replnode(ReplNode, S) ->
    ?DBG("Updating ~p\nin ~p\n", [ReplNode, S]),
    UpdReplNodes = lists:keyreplace(ReplNode#replnode.node, #replnode.node,
                                    S#state.replnodes, ReplNode),
    ?DBG("Updated ~p\n", [UpdReplNodes]),
    S#state{replnodes = UpdReplNodes}.

%% Check if a node exists in the state
node_exists(Node,S) ->
    try
        get_replnode(Node, S),
        true
    catch
        _:_ ->
            false % no match on node name
    end.

%% Check if a node has the replnode element at Pos == Value
check_replnode(Node, Pos, Value, S) ->
    try
        ReplNode = get_replnode(Node, S),
        element(Pos, ReplNode) =:= Value
    catch
        _:_ ->
            false % no match on node name (or bad position)
    end.
            
wait_for_helper(Node) ->
    wait_for_helper(Node, 1000).

wait_for_helper(_Node, 0) ->
    helper_timeout;
wait_for_helper(Node, Retries) ->
    case rpc:call(Node, riak_repl_leader, helper_pid, []) of
        undefined ->
            timer:sleep(10),
            wait_for_helper(Node, Retries -1);
        Pid when is_pid(Pid) ->
            {ok, Pid};
        {killed, _OldPid} ->
            timer:sleep(10),
            wait_for_helper(Node, Retries - 1)
    end.
            
%% Ask the helper who it thinks the leader is and all up candidates
%% that will answer. Nodes must be 'up' *and* have the same worker/candidate
%% lists.
%% Call this makes sure any elections have been propagated to the 
%% riak_repl_helper process.  The helper is only asked when at least on 
%% candidate nodes should be up, otherwise it will block waiting for any
%% candidate.
helper_leader_node(N, S) ->
    ?DBG("Handler leader node ~p\nState:\n~p\n", [N, S]),
    RN = get_replnode(N, S),
    C = RN#replnode.candidates,
    W = RN#replnode.workers,
    CRNs = [get_replnode(X, S) || X <- C],
    ?DBG("Candidate replication nodes\n~p\n", [CRNs]),
    UpCandidates = [CRN#replnode.node || CRN <- CRNs,
                                         CRN#replnode.running =:= true,
                                         lists:member(CRN#replnode.node, C),
                                         CRN#replnode.candidates =:= C,
                                         CRN#replnode.workers =:= W],
    case UpCandidates of
        [] ->
            HelperLN = no_candidates;
        _ ->
            {ok, Helper} = wait_for_helper(N),
            HelperLN = rpc:call(N, riak_repl_leader_helper,
                                leader_node, [Helper, 300000], 305000)
    end,
    ?DBG("UpCandidatess ~p~n", [UpCandidates]),
    {HelperLN, UpCandidates}.
    
%% ====================================================================
%% Slave driver - link all slaves under a single process for easy cleanup
%% ====================================================================

start_slave_driver() ->
    ?DBG("Starting slave driver\n", []),
    ReplyTo = self(),
    spawn(fun() ->
                  true = register(slave_driver, self()),
                  ?DBG("Started slave driver\n", []),
                  ReplyTo ! ready,
                  slave_driver_loop()
          end),
    receive
        ready ->
            ok
    after
        5000 ->
            throw(slave_driver_timeout)
    end.

slave_driver_loop() ->
    receive
        {start, Name, ReplyTo} ->
            ?DBG("starting node ~p@~p from node ~p~n", [Name,
                    get_host(node()), node()]),
            {ok, Node} = slave:start_link(get_host(node()), Name),
            true = rpc:call(Node, code, set_path, [good_path()]),
            ReplyTo ! {ok, Node};
        {stop, Node, ReplyTo} ->
            ok = slave:stop(Node),
            pang = net_adm:ping(Node),
            ReplyTo ! {stopped, Node}
    end,
    slave_driver_loop().

stop_slave_driver() ->
    ?DBG("Stopping slave driver\n", []),
    case whereis(slave_driver) of
        undefined ->
            ?DBG("Slave driver not running\n", []),
            ok;
        Pid ->
            Mref = erlang:monitor(process, Pid),
            exit(Pid, kill),
            receive
                {'DOWN', Mref, process, _Obj, _Info} ->
                    ?DBG("Stopped slave driver\n", []),
                    ok
            end
    end.

start_slave(Node) ->
    Name = get_name(Node),
    slave_driver ! {start, Name, self()},
    receive
        {ok, Node} ->
            ok
    end.
        
stop_slave(Node) ->
    slave_driver ! {stop, Node, self()},
    receive
        {stopped, Node} ->
            ok
    end.

%% ====================================================================
%% Slave functions - code that runs on the slave nodes for setup
%% ====================================================================

setup_slave(Candidates, Workers) ->
    %mock_repl_controller(),
    start_leader(Candidates, Workers).
   
mock_repl_controller() ->
    ?DBG("Mocking riak_repl_controller on ~p\n", [node()]),
    {module, meck} = code:ensure_loaded(meck),
    ok = meck:new(riak_repl_controller, [no_link]),
    ok = meck:expect(riak_repl_controller, set_is_leader, 
                     fun(_Bool) -> ok end),
    ok = riak_repl_controller:set_is_leader(false), % call it, just to prove it works
    ?DBG("Mocked riak_repl_controller on ~p\n", [node()]).

start_leader(Candidates, Workers) ->
    ?DBG("Starting repl on ~p\n", [node()]),

    application:start(ranch),
    %% Set up the application config so multiple leaders do not
    %% tread on one anothers toes
    application:load(riak_repl),
    DataRoot = "repl_leader_eqc_data/"++atom_to_list(node()),
    application:set_env(riak_repl, data_root, DataRoot),

    %% cannot just call rpc:call(Node, riak_repl_leader, start_link, []) as it 
    %% would link to the rex process created for the call.  This creates the
    %% process and unlinks before returning.
    {ok, REPid} = riak_core_ring_events:start_link(),
    ?DBG("Started ring_events at ~p~n", [REPid]),
    unlink(REPid),
    {ok, RMPid} = riak_core_ring_manager:start_link(test),
    ?DBG("Started ring_manager at ~p~n", [RMPid]),
    unlink(RMPid),
    {ok, NWEPid} = riak_core_node_watcher_events:start_link(),
    ?DBG("Started node_watcher_events at ~p~n", [NWEPid]),
    unlink(NWEPid),
    application:set_env(riak_core, gossip_interval, 5000),
    {ok, NWPid} = riak_core_node_watcher:start_link(),
    ?DBG("Started node_watcher at ~p~n", [NWPid]),
    unlink(NWPid),
    {ok, CSPid} = riak_repl_client_sup:start_link(),
    ?DBG("Started repl client_sup at ~p~n", [CSPid]),
    unlink(CSPid),
    {ok, SSPid} = riak_repl_server_sup:start_link(),
    ?DBG("Started repl server_sup at ~p~n", [SSPid]),
    unlink(SSPid),
    {ok, Pid} = riak_repl_leader:start_link(), 
    ?DBG("Started repl leader at ~p", [Pid]),
    unlink(Pid),

    %% set the candidates so that the repl helper is created
    ok = riak_repl_leader:set_candidates(Candidates, Workers),

    ?DBG("Started repl on ~p as ~p with candidates {~p, ~p}\n",
         [node(), Pid, Candidates, Workers]),

    %riak_repl_leader:ensure_sites(),

    %% Check leader completes election
    %{ok, Helper} = wait_for_helper(node()),
    %_HelperLN = riak_repl_leader_helper:leader_node(Helper, 10000),
    ok.


%% Creates a dummy process that waits for the message 'die'
register_receiver() ->
    Pid = spawn(fun() ->
                        receive
                            die ->
                                ok
                        end
                end),
    Leader1 = riak_repl_leader:leader_node(),
    Res = riak_repl_leader:add_receiver_pid(Pid),
    Leader2 = riak_repl_leader:leader_node(),
    {Leader1, Leader2, Res, Pid}.

-endif. % EQC
