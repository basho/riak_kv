-module(riak_core_cluster_mgr_sup_tests).
-compile([export_all, nowarn_export_all]).
-ifdef(EQC).
-include("riak_core_connection.hrl").
-include_lib("eunit/include/eunit.hrl").

murdering_test_() ->
    error_logger:tty(false),
    {spawn,
     [
      {setup, fun() ->
    % if the cluster_mgr is restarted after the leader process is running,
    % it should ask about who the current leader is.
        Kids = [
            {riak_repl_leader, {riak_repl_leader, start_link, []},
                  permanent, 5000, worker, [riak_repl_leader]},
            {riak_repl2_leader, {riak_repl2_leader, start_link, []},
                  permanent, 5000, worker, [riak_repl2_leader]},
            {riak_core_cluster_mgr_sup, {riak_core_cluster_mgr_sup, start_link, []},
                  permanent, infinity, supervisor, [riak_cluster_mgr_sup]}
        ],
        meck:new(riak_repl_sup, [passthrough]),
        meck:expect(riak_repl_sup, init, fun(_Args) ->
            {ok, {{one_for_one, 9, 10}, Kids}}
        end),
        meck:new(riak_core_node_watcher_events),
        meck:expect(riak_core_node_watcher_events, add_sup_callback, fun(_fn) ->
            ok
        end),
        meck:new(riak_core_node_watcher),
        meck:expect(riak_core_node_watcher, nodes, fun(_) ->
            [node()]
        end),
        application:start(ranch),
        application:set_env(riak_repl, data_root, "."),
        {ok, _Eventer} = riak_core_ring_events:start_link(),
        {ok, _RingMgr} = riak_core_ring_manager:start_link(test),
        {ok, _ClientSup} = riak_repl_client_sup:start_link(),
        {ok, TopSup} = riak_repl_sup:start_link(),
        riak_repl2_leader:register_notify_fun(
            fun riak_core_cluster_mgr:set_leader/2),
        riak_repl_leader:set_candidates([node()], []),
        riak_repl2_leader:set_candidates([node()], []),
        wait_for_leader(),
        TopSup
    end,
    fun(TopSup) ->
            process_flag(trap_exit, true),
            catch exit(TopSup, kill),
            catch(exit(whereis(riak_repl_client_sup), kill)),
            riak_core_ring_manager:stop(),
            catch exit(riak_core_ring_events, kill),
            application:stop(ranch),
            meck:unload(),
            process_flag(trap_exit, false),
            ok
    end,
    fun(_) -> [

        {"Cluster Mgr knows of a leader", fun() ->
            ?assertNotEqual(undefined, riak_core_cluster_mgr:get_leader())
        end},

        {"Kill off the cluster manager, it can still find a leader", fun() ->
            OldPid = whereis(riak_core_cluster_manager),
            catch exit(OldPid, kill),
            WaitFun = fun() ->
                case whereis(riak_core_cluster_manager) of
                    OP when OP == OldPid ->
                    false;
                    undefined ->
                        false;
                    _Pid ->
                        Leader = riak_core_cluster_mgr:get_leader(),
                        Leader =/= undefined
                end
            end,
            WaitRes = wait(WaitFun, 10, 400),
            ?assertEqual(ok, WaitRes)
        end}

    ] end}]}.

pester(Fun) ->
    pester(Fun, 10, 100).

pester(Fun, Wait, Times) ->
    pester(Fun, Wait, Times, []).

pester(_Fun, _Wait, Times, Acc) when Times =< 0 ->
    lists:reverse(Acc);
pester(Fun, Wait, Times, Acc) ->
    Res = Fun(),
    timer:sleep(Wait),
    pester(Fun, Wait, Times - 1, [Res | Acc]).

wait_for_leader() ->
    WaitFun = fun() ->
        riak_repl2_leader:leader_node() =/= undefined
    end,
    wait(WaitFun).

wait(WaitFun) ->
    wait(WaitFun, 10, 100).

wait(_WaitFun, _Wait, Itor) when Itor =< 0 ->
    timeout;
wait(WaitFun, Wait, Itor) ->
    case WaitFun() of
        true ->
            ok;
        _ ->
            timer:sleep(Wait),
            wait(WaitFun, Wait, Itor - 1)
    end.

-endif. % EQC
