-module(riak_kv_sweeper_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include("riak_kv_sweeper.hrl").

%%--------------------------------------------------------------------
%% COMMON TEST CALLBACK FUNCTIONS
%%--------------------------------------------------------------------


suite() ->
    [{timetrap, {minutes, 1}}].


init_per_testcase(_TestCase, Config) ->
    meck:unload(),
    file:delete(riak_kv_sweeper:sweep_file()),
    application:set_env(riak_kv, sweep_participants, undefined),
    application:set_env(riak_kv, sweep_window, always),
    application:set_env(riak_kv, sweeper_scheduler, false),
    application:set_env(riak_kv, sweep_tick, 10), % MSecs
    application:set_env(riak_kv, sweep_throttle, {obj_size, 0, 0}), %Disable throttling
    application:set_env(riak_kv, sweep_window_throttle_div, 1),
    application:set_env(riak_kv, sweep_concurrency, 1),

    VNodeIndices = meck_new_riak_core_modules(2),
    riak_kv_sweeper:start_link(),

    [{vnode_indices, VNodeIndices}|Config].


end_per_testcase(_TestCase, _Config) ->
    %% Tests are allowed to shut down the riak_kv_sweeper and start it
    %% up again
    try riak_kv_sweeper:stop() of
         _ -> ok
    catch exit:{noproc,{gen_server,call,[riak_kv_sweeper,stop]}} ->
            ignore
    end,
    meck:unload(),
    ok.


groups() ->
    [].


all() ->
    all_tests().


%%--------------------------------------------------------------------
%% Factory functions
%%--------------------------------------------------------------------
meck_new_riak_core_modules(Partitions) ->
    Ring = ring,
    meck:new(riak_core_node_watcher),
    meck:new(riak_core_ring_manager),
    meck:expect(riak_core_ring_manager, get_my_ring, fun() -> {ok, Ring} end),
    meck:new(riak_core_ring),
    VNodeIndices = meck_new_riak_core_ring(Partitions),
    meck:expect(riak_core_node_watcher, services, fun(_Node) -> [riak_kv] end),
    VNodeIndices.


meck_new_riak_core_ring(Partitions) ->
    VNodeIndices = [N || N <- lists:seq(0, Partitions)],
    meck:expect(riak_core_ring, my_indices, fun(ring) -> VNodeIndices end),
    VNodeIndices.

meck_new_aae_modules(AAEnabled, EstimatedKeys, LockResult) ->
    meck:new(riak_kv_entropy_manager),
    meck:expect(riak_kv_entropy_manager, enabled, fun() -> AAEnabled end),
    meck:new(riak_kv_index_hashtree),
    meck:expect(riak_kv_index_hashtree, get_lock, fun(_, _) -> LockResult end),
    meck:expect(riak_kv_index_hashtree, estimate_keys, fun(_) -> {ok, EstimatedKeys} end).


meck_new_sweep_particpant(Name, TestCasePid) ->
    meck:new(Name, [non_strict, no_link]),
    meck:expect(Name, participate_in_sweep,
                fun(Index, _Pid) ->
                        TestCasePid ! {ok, participate_in_sweep},
                        InitialAcc = 0,
                        {ok, Name:visit_object_fun(Index), InitialAcc}
                end),

    meck_new_visit_function(Name),
    meck_new_successfull_sweep_function(Name, TestCasePid),
    meck_new_failed_sweep_function(Name, TestCasePid),

    #sweep_participant{description = atom_to_list(Name) ++ " sweep participant",
                       module = Name,
                       fun_type = ?OBSERV_FUN,
                       run_interval = 60,       % Secs
                       options = []}.

meck_new_visit_function(Name) ->
    meck_new_visit_function(Name, no_errors).

meck_new_visit_function(Name, Behavior) ->
    meck:expect(Name, visit_object_fun, visit_function(Behavior)).

meck_new_successfull_sweep_function(Name, TestCasePid) ->
    meck:expect(Name, successfull_sweep, successfull_sweep_function(Name, TestCasePid)).

meck_new_failed_sweep_function(Name, TestCasePid) ->
    meck:expect(Name, failed_sweep, failed_sweep_function(Name, TestCasePid)).

visit_function({throw, Error}) ->
    fun(_Index) ->
        fun({{_Bucket, _Key}, _RObj}, _Acc, _Opts = []) ->
                throw(Error)
        end
    end;
visit_function({wait_after_keys, KeyCount, WaitMSecs}) ->
    fun(_Index) ->
        fun({{_Bucket, _Key}, _RObj}, Acc, _Opts = []) ->
                VisitedKeys = Acc,
                case (VisitedKeys rem KeyCount) == 0 of
                    true ->
                        timer:sleep(WaitMSecs),
                        ok;
                    false ->
                        ok
                end,
                {ok, VisitedKeys + 1}
        end
    end;
visit_function({wait, From, Msg}) ->
    fun(Index) ->
        fun({{_Bucket, _Key}, _RObj}, Acc, _Opts = []) ->
                case Acc of
                    {continue, VisitedKeys} ->
                        {ok, {continue, VisitedKeys + 1}};
                    VisitedKeys when is_integer(VisitedKeys) ->
                        From ! {self(), {visit_function_waiting, Index}},
                        receive
                            Msg -> From ! continue
                        end,
                        {ok, {continue, VisitedKeys + 1}}
                end
        end
    end;
visit_function({mutate, Count}) ->
    fun(_Index) ->
        fun({{_Bucket, _Key}, RObj}, Acc, _Opts = []) ->
                NewAcc = Acc + 1,
                case NewAcc =< Count of
                    true ->
                        {mutated, RObj, NewAcc};
                    false ->
                        {ok, NewAcc}
                end
        end
    end;
visit_function(_) ->
    fun(_Index) ->
        fun({{_Bucket, _Key}, _RObj}, Acc, _Opts = []) ->
                VisitedKeys = Acc,
                {ok, VisitedKeys + 1}
        end
    end.

successfull_sweep_function(Name, TestCasePid) ->
    fun(Index, _FinalAcc) ->
            TestCasePid ! {ok, successfull_sweep, Name, Index},
            ok
    end.

failed_sweep_function(Name, TestCasePid) ->
    fun(Index, _Reason) ->
            TestCasePid ! {ok, failed_sweep, Name, Index},
            ok
    end.

meck_new_backend(TestCasePid) ->
    meck_new_backend(TestCasePid, 1000).

meck_new_backend(TestCasePid, NumKeys) ->
    meck_new_backend(TestCasePid, NumKeys, 0).

meck_new_backend(TestCasePid, NumKeys, ObjSizeBytes) ->
    Keys = [integer_to_binary(N) || N <- lists:seq(1, NumKeys)],
    meck:new(meck_new_backend, [non_strict]),
    meck:expect(meck_new_backend, fold_objects,
                fun(CompleteFoldReq, InitialAcc, _, _) ->
                        Result = lists:foldl(fun(NBin, Acc) ->
                                                     InitialObj = riak_object_bin(NBin, NBin, ObjSizeBytes),
                                                     CompleteFoldReq(NBin, NBin, InitialObj, Acc)
                                             end, InitialAcc, Keys),
                        TestCasePid ! {ok, fold_complete},
                        Result
                end),

    meck:new(riak_kv_vnode),
    meck:expect(riak_kv_vnode, sweep,
                fun({Index, _Node}, ActiveParticipants, EstimatedKeys) ->
                        spawn_link(
                          fun() ->
                                  Result = riak_kv_sweeper_fold:do_sweep(ActiveParticipants,
                                                                         EstimatedKeys,
                                                                         _Sender = '?',
                                                                         _Opts = [],
                                                                         Index,
                                                                         meck_new_backend,
                                                                         _ModState = '?',
                                                                         _VnodeState = '?'),
                                  {reply, SA, _} = Result,
                                  TestCasePid ! {ok, SA}

                          end)
                end),
    meck:expect(riak_kv_vnode, local_put, fun(_, _, _) -> ok end),
    meck:expect(riak_kv_vnode, local_reap, fun(_, _, _) -> ok end).


%%--------------------------------------------------------------------
%% TEST CASES
%%--------------------------------------------------------------------


status_test(Config) ->
    Indices = ?config(vnode_indices, Config),
    {_SPs, Sweeps} = riak_kv_sweeper:status(),
    Indices = lists:sort([I0 || #sweep{state = idle, index = I0} <- Sweeps]),
    ok.


status_index_changed_test(Config) ->
    Indices = ?config(vnode_indices, Config),
    [_H|NewIndices0] = Indices,
    NewIndices = [I0*1000 ||I0 <- NewIndices0],
    meck:expect(riak_core_ring, my_indices, fun(ring) -> NewIndices end),
    {_SPs, Sweeps} = riak_kv_sweeper:status(),
    NewIndices = lists:sort([I0 || #sweep{state = idle, index = I0} <- Sweeps]),
    ok.


sweeps_persistent_test(_Config) ->
    {_SPs, Sweeps0} = riak_kv_sweeper:status(),
    Sweeps = lists:sort(Sweeps0),
    false = filelib:is_regular(riak_kv_sweeper:sweep_file()),
    ok = riak_kv_sweeper:stop(),
    true = filelib:is_regular(riak_kv_sweeper:sweep_file()),
    {ok, [PersistedSweepDict]} = file:consult(riak_kv_sweeper:sweep_file()),
    PersistedSweeps = [Sweep|| {_Index, Sweep} <- dict:to_list(PersistedSweepDict)],
    Sweeps = lists:sort(PersistedSweeps),
    ok.


add_participant_test(_Config) ->
    {[], _Sweeps} = riak_kv_sweeper:status(),
    SP = meck_new_sweep_particpant(sweep_observer_1, self()),
    riak_kv_sweeper:add_sweep_participant(SP),
    {[#sweep_participant{module = sweep_observer_1}], _} = riak_kv_sweeper:status(),
    ok.


add_participant_persistent_test(_Config) ->
    SP = meck_new_sweep_particpant(sweep_observer_1, self()),
    riak_kv_sweeper:add_sweep_participant(SP),
    {[#sweep_participant{module = sweep_observer_1}], _} = riak_kv_sweeper:status(),

    ok = riak_kv_sweeper:stop(),
    riak_kv_sweeper:start_link(),
    {[#sweep_participant{module = sweep_observer_1}], _} = riak_kv_sweeper:status(),
    ok.


remove_participant_test(_Config) ->
    SP = meck_new_sweep_particpant(sweep_observer_1, self()),
    riak_kv_sweeper:add_sweep_participant(SP),
    {[#sweep_participant{module = sweep_observer_1}], _} = riak_kv_sweeper:status(),
    riak_kv_sweeper:remove_sweep_participant(sweep_observer_1),
    {[], _} = riak_kv_sweeper:status(),
    ok.


remove_participant_persistent_test(_Config) ->
    SP = meck_new_sweep_particpant(sweep_observer_1, self()),
    riak_kv_sweeper:add_sweep_participant(SP),
    {[#sweep_participant{module = sweep_observer_1}], _} = riak_kv_sweeper:status(),
    riak_kv_sweeper:remove_sweep_participant(sweep_observer_1),
    {[], _} = riak_kv_sweeper:status(),
    riak_kv_sweeper:stop(),
    riak_kv_sweeper:start_link(),
    {[], _} = riak_kv_sweeper:status(),
    ok.


sweep_request_test(Config) ->
    Indices = ?config(vnode_indices, Config),
    SP = meck_new_sweep_particpant(sweep_observer_1, self()),
    meck_new_backend(self()),
    riak_kv_sweeper:add_sweep_participant(SP),

    I0 = pick(Indices),
    riak_kv_sweeper:sweep(I0),
    ok = receive_msg({ok, successfull_sweep, sweep_observer_1, I0}),

    I1 = pick(Indices),
    riak_kv_sweeper:sweep(I1),
    ok = receive_msg({ok, successfull_sweep, sweep_observer_1, I1}).


scheduler_test(Config) ->
    Indices = ?config(vnode_indices, Config),
    meck_new_backend(self()),
    SP = meck_new_sweep_particpant(sweep_observer_1, self()),
    riak_kv_sweeper:add_sweep_participant(SP),
    riak_kv_sweeper:enable_sweep_scheduling(),
    [receive_msg({ok, successfull_sweep, sweep_observer_1, I}) || I <- Indices],
    ok.

%% TODO - Why are we not monitoring the worker process doing the fold ?
scheduler_worker_process_crashed_test(Config) ->
    Indices = ?config(vnode_indices, Config),
    meck_new_backend(self()),
    SP = meck_new_sweep_particpant(sweep_observer_1, self()),
    meck_new_visit_function(sweep_observer_1, {throw, crash}),
    riak_kv_sweeper:add_sweep_participant(SP),
    riak_kv_sweeper:enable_sweep_scheduling(),
    [timeout = receive_msg({ok, successfull_sweep, sweep_observer_1, I}) || I <- Indices],
    {_SPs, Sweeps} = riak_kv_sweeper:status(),
    true = [ 1 || #sweep{state = running} <- Sweeps ] > 0,
    ok.


scheduler_run_interval_test(Config) ->
    Indices = ?config(vnode_indices, Config),
    meck_new_backend(self()),
    SP = meck_new_sweep_particpant(sweep_observer_1, self()),
    SP1 = SP#sweep_participant{run_interval = 1},
    meck_new_visit_function(sweep_observer_1),
    riak_kv_sweeper:add_sweep_participant(SP1),
    riak_kv_sweeper:enable_sweep_scheduling(),

    [ok = receive_msg({ok, successfull_sweep, sweep_observer_1, I}) || I <- Indices],
    [ok = receive_msg({ok, successfull_sweep, sweep_observer_1, I}, min_scheduler_response_time_msecs()) || I <- Indices],

    ok.


scheduler_remove_participant_test(Config) ->
    {ok, SweepTick} = application:get_env(riak_kv, sweep_tick),
    Indices = ?config(vnode_indices, Config),
    WaitIndex = pick(Indices),
    TestCasePid = self(),
    meck_new_backend(TestCasePid, _NumKeys = 5000),
    SP = meck_new_sweep_particpant(sweep_observer_1, TestCasePid),
    SP1 = SP#sweep_participant{run_interval = 1},
    riak_kv_sweeper:add_sweep_participant(SP1),
    riak_kv_sweeper:enable_sweep_scheduling(),
    VisitControllerPid = spawn_link(
                           ?MODULE,
                           visit_function_controller,
                           [TestCasePid, WaitIndex]),
    meck_new_visit_function(
      sweep_observer_1,
      {wait, VisitControllerPid, visit_function_continue}),
    receive {VisitControllerPid, {visit_controller_waiting, WaitIndex}} ->
            riak_kv_sweeper:remove_sweep_participant(sweep_observer_1),
            VisitControllerPid ! {TestCasePid, visit_controller_continue}
    end,

    timer:sleep(2*SweepTick),
    ok = receive_msg({ok, failed_sweep, sweep_observer_1, WaitIndex}, min_scheduler_response_time_msecs()),
    timer:sleep(2*SweepTick), % Wait for result message to be received by the sweeper
    {_SPs, Sweeps} = riak_kv_sweeper:status(),
    #sweep{results = Result} = lists:keyfind(WaitIndex, #sweep.index, Sweeps),
    {ok,{_, fail}} = dict:find(sweep_observer_1, Result),
    ok.


scheduler_queue_test(Config) ->
    Indices = ?config(vnode_indices, Config),
    WaitIndex = pick(Indices),
    WaitIndex1 = pick(Indices -- [WaitIndex]),

    TestCasePid = self(),
    meck_new_backend(TestCasePid, _NumKeys = 5000),
    SP = meck_new_sweep_particpant(sweep_observer_1, TestCasePid),
    SP1 = SP#sweep_participant{run_interval = 1},
    riak_kv_sweeper:add_sweep_participant(SP1),
    riak_kv_sweeper:enable_sweep_scheduling(),
    VisitControllerPid = spawn_link(
                           ?MODULE,
                           visit_function_controller,
                           [TestCasePid, WaitIndex]),
    meck_new_visit_function(
      sweep_observer_1,
      {wait, VisitControllerPid, visit_function_continue}),

    %% Lets sweeps run once so they are not in never run state
    receive {VisitControllerPid, {visit_controller_waiting, WaitIndex}} ->
            VisitControllerPid ! {TestCasePid, visit_controller_continue}
    end,
    receive {VisitControllerPid, {visit_controller_waiting, WaitIndex}} ->
            riak_kv_sweeper:sweep(WaitIndex1),
            Sweep = get_sweep_on_index(WaitIndex1),
            {_, _, _} = Sweep#sweep.queue_time,
            VisitControllerPid ! {TestCasePid, visit_controller_continue}
    end,
    [ ok = receive_msg({ok, successfull_sweep, sweep_observer_1, I}, min_scheduler_response_time_msecs()) || I <- Indices],
    ok.


scheduler_sweep_window_never_test(Config) ->
    Indices = ?config(vnode_indices, Config),
    {ok, SweepTick} = application:get_env(riak_kv, sweep_tick),
    TestCasePid = self(),

    application:set_env(riak_kv, sweep_window, never),
    meck_new_backend(TestCasePid, _NumKeys = 5000),
    SP = meck_new_sweep_particpant(sweep_observer_1, TestCasePid),
    SP1 = SP#sweep_participant{run_interval = 1},
    riak_kv_sweeper:add_sweep_participant(SP1),
    riak_kv_sweeper:enable_sweep_scheduling(),

    StatusBefore = riak_kv_sweeper:status(),
    timer:sleep(2*SweepTick),
    StatusAfter = riak_kv_sweeper:status(),
    StatusAfter = StatusBefore,

    application:set_env(riak_kv, sweep_window, always),
    timer:sleep(2*SweepTick),
    [ ok = receive_msg({ok, successfull_sweep, sweep_observer_1, I}, min_scheduler_response_time_msecs()) || I <- Indices].


scheduler_now_outside_sleep_window_test(Config) ->
    Indices = ?config(vnode_indices, Config),
    {ok, SweepTick} = application:get_env(riak_kv, sweep_tick),
    TestCasePid = self(),

    {_, {Hour, _, _}} = calendar:local_time(),
    Start = add_hours(Hour, 2),
    End = add_hours(Start, 1),

    application:set_env(riak_kv, sweep_window, {Start, End}),
    meck_new_backend(TestCasePid, _NumKeys = 5000),
    SP = meck_new_sweep_particpant(sweep_observer_1, TestCasePid),
    SP1 = SP#sweep_participant{run_interval = 1},
    riak_kv_sweeper:add_sweep_participant(SP1),
    riak_kv_sweeper:enable_sweep_scheduling(),

    StatusBefore = riak_kv_sweeper:status(),
    timer:sleep(2*SweepTick),
    StatusAfter = riak_kv_sweeper:status(),
    StatusAfter = StatusBefore,

    application:set_env(riak_kv, sweep_window, {Hour, add_hours(Hour, 1)}),
    timer:sleep(2*SweepTick),
    [ ok = receive_msg({ok, successfull_sweep, sweep_observer_1, I}, min_scheduler_response_time_msecs()) || I <- Indices],
    ok.


stop_all_scheduled_sweeps_test(Config) ->
    Indices = ?config(vnode_indices, Config),
    SweepRunTimeMsecs = 1000,
    NumKeys = 10000, % sweeper worker receives messages every 1000 keys
    WaitAfterKeys = 1000,
    WaitMSecs = SweepRunTimeMsecs div  (NumKeys div WaitAfterKeys),
    {ok, SweepTick} = application:get_env(riak_kv, sweep_tick),

    meck_new_backend(self(), NumKeys),
    SP = meck_new_sweep_particpant(sweep_observer_1, self()),
    meck_new_visit_function(sweep_observer_1, {wait_after_keys, WaitAfterKeys, WaitMSecs}),
    riak_kv_sweeper:add_sweep_participant(SP),
    riak_kv_sweeper:enable_sweep_scheduling(),
    timer:sleep(2*SweepTick),
    Running = riak_kv_sweeper:stop_all_sweeps(),
    true = Running >= 1,
    [ timeout = receive_msg({ok, successfull_sweep, sweep_observer_1, I}, SweepRunTimeMsecs) || I <- Indices],
    ok.


stop_all_scheduled_sweeps_race_condition_test(Config) ->
    Indices = ?config(vnode_indices, Config),
    SweepRunTimeMsecs = 1000,
    NumKeys = 10000, % sweeper worker receives messages every 1000 keys
    WaitAfterKeys = 1000,
    WaitMSecs = SweepRunTimeMsecs div  (NumKeys div WaitAfterKeys),

    meck_new_backend(self(), NumKeys),
    SP = meck_new_sweep_particpant(sweep_observer_1, self()),
    meck_new_visit_function(sweep_observer_1, {wait_after_keys, WaitAfterKeys, WaitMSecs}),
    riak_kv_sweeper:add_sweep_participant(SP),
    riak_kv_sweeper:enable_sweep_scheduling(),
    Running = riak_kv_sweeper:stop_all_sweeps(),
    Running = 0,
    [timeout = receive_msg({ok, successfull_sweep, sweep_observer_1, I}, SweepRunTimeMsecs) || I <- Indices],
    ok.

scheduler_add_participant_test(Config) ->
    Indices = ?config(vnode_indices, Config),
    meck_new_backend(self()),
    riak_kv_sweeper:enable_sweep_scheduling(),

    SP1 = meck_new_sweep_particpant(sweep_observer_1, self()),
    riak_kv_sweeper:add_sweep_participant(SP1#sweep_participant{run_interval = 1}),
    [ok = receive_msg({ok, successfull_sweep, sweep_observer_1, I}) || I <- Indices],
    [ok = receive_msg({ok, successfull_sweep, sweep_observer_1, I}, min_scheduler_response_time_msecs()) || I <- Indices],

    SP2 = meck_new_sweep_particpant(sweep_observer_2, self()),
    riak_kv_sweeper:add_sweep_participant(SP2#sweep_participant{run_interval = 1}),
    [ok = receive_msg({ok, successfull_sweep, sweep_observer_2, I}) || I <- Indices],
    [ok = receive_msg({ok, successfull_sweep, sweep_observer_2, I}, min_scheduler_response_time_msecs()) || I <- Indices],
    ok.


scheduler_restart_sweep_test(Config) ->
    Indices = ?config(vnode_indices, Config),
    TestCasePid = self(),
    meck_new_backend(TestCasePid, _NumKeys = 5000),
    SP = meck_new_sweep_particpant(sweep_observer_1, TestCasePid),
    SP1 = SP#sweep_participant{run_interval = 1},
    riak_kv_sweeper:add_sweep_participant(SP1),
    WaitIndex = pick(Indices),
    riak_kv_sweeper:enable_sweep_scheduling(),
    VisitControllerPid0 = spawn_link(
                           ?MODULE,
                           visit_function_controller,
                           [TestCasePid, WaitIndex]),

    meck_new_visit_function(
      sweep_observer_1,
      {wait, VisitControllerPid0, visit_function_continue}),
    receive {VisitControllerPid0, {visit_controller_waiting, WaitIndex}} ->
            RunningSweep = get_sweep_on_index(WaitIndex),
            running = RunningSweep#sweep.state,

            riak_kv_sweeper:sweep(WaitIndex),
            RestartingSweep = get_sweep_on_index(WaitIndex),
            restart = RestartingSweep#sweep.state,
            undefined = RestartingSweep#sweep.queue_time,

            riak_kv_sweeper:sweep(WaitIndex),
            RestartingSweep = get_sweep_on_index(WaitIndex),
            undefined = RestartingSweep#sweep.queue_time,
            restart = RestartingSweep#sweep.state,

            RestartingSweep = RunningSweep#sweep{state=restart},
            VisitControllerPid0 ! {TestCasePid, visit_controller_continue}
    end,
    ok = receive_msg({ok, failed_sweep, sweep_observer_1, WaitIndex}),
    receive {VisitControllerPid0, {visit_controller_waiting, WaitIndex}} ->
            VisitControllerPid0 ! {TestCasePid, visit_controller_continue}
    end,
    ok = receive_msg({ok, successfull_sweep, sweep_observer_1, WaitIndex}, min_scheduler_response_time_msecs()),
    ok.


scheduler_estimated_keys_lock_ok_test(Config) ->
    Indices = ?config(vnode_indices, Config),
    TestCasePid = self(),
    meck_new_aae_modules(_AAEnabled = true,
                         _EstimatedKeys = 4200,
                         _LockResult = ok),
    meck_new_backend(TestCasePid, _NumKeys = 5000),
    SP = meck_new_sweep_particpant(sweep_observer_1, TestCasePid),
    SP1 = SP#sweep_participant{run_interval = 1},
    riak_kv_sweeper:add_sweep_participant(SP1),
    riak_kv_sweeper:enable_sweep_scheduling(),
    meck_new_visit_function(sweep_observer_1),
    [ok = receive_msg({ok, successfull_sweep, sweep_observer_1, I}, min_scheduler_response_time_msecs()) || I <- Indices],
    [true = meck:called(riak_kv_index_hashtree, get_lock, [I, estimate]) || I <- Indices],
    [true = meck:called(riak_kv_index_hashtree, estimate_keys, [I]) || I <- Indices],
    ok.


scheduler_estimated_keys_lock_fail_test(Config) ->
    Indices = ?config(vnode_indices, Config),
    TestCasePid = self(),
    meck_new_aae_modules(_AAEnabled = true,
                         _EstimatedKeys = 4200,
                         _LockResult = fail),
    meck_new_backend(TestCasePid, _NumKeys = 5000),
    SP = meck_new_sweep_particpant(sweep_observer_1, TestCasePid),
    SP1 = SP#sweep_participant{run_interval = 1},
    riak_kv_sweeper:add_sweep_participant(SP1),
    riak_kv_sweeper:enable_sweep_scheduling(),
    meck_new_visit_function(sweep_observer_1),
    [ok = receive_msg({ok, successfull_sweep, sweep_observer_1, I}, min_scheduler_response_time_msecs()) || I <- Indices],
    [true = meck:called(riak_kv_index_hashtree, get_lock, [I, estimate]) || I <- Indices],
    [false = meck:called(riak_kv_index_hashtree, estimate_keys, [I]) || I <- Indices],
    ok.


scheduler_sweep_concurrency_test(_Config) ->
    ConcurrentSweeps = 8,
    application:set_env(riak_kv, sweep_concurrency, ConcurrentSweeps),
    meck_new_riak_core_ring(8),
    TestCasePid = self(),
    meck_new_backend(TestCasePid),
    SP = meck_new_sweep_particpant(sweep_observer_1, TestCasePid),
    SP1 = SP#sweep_participant{run_interval = 1},
    riak_kv_sweeper:add_sweep_participant(SP1),
    riak_kv_sweeper:enable_sweep_scheduling(),
    meck_new_visit_function(
      sweep_observer_1,
      {wait, TestCasePid, visit_function_continue}),

    wait_for_concurrent_sweeps(ConcurrentSweeps),
    {_SPs, Sweeps} = riak_kv_sweeper:status(),
    ConcurrentSweeps = length([1 || #sweep{pid = Pid, state = running} <- Sweeps,
                                    process_info(Pid) =/= undefined]),
    [Pid || #sweep{pid = Pid} <- Sweeps],
    %% [ok = receive_msg({ok, successfull_sweep, sweep_observer_1, I}, min_scheduler_response_time_msecs()) || I <- Indices],
    ok.


sweep_throttle_obj_size_test(Config) ->
    Indices = ?config(vnode_indices, Config),
    sweep_throttle_obj_size(
      _Index              = pick(Indices),
      _NumKeys            = 1000,
      _NumMutatedKeys     = 0,
      _ObjSizeBytes       = 100,
      _ThrottleAfterBytes = 100,
      _ThrottleWaitMsecs  = 1).


%% throttling on object size with no mutated keys
sweep_throttle_obj_size1_test(Config) ->
    Indices = ?config(vnode_indices, Config),
    sweep_throttle_obj_size(
      _Index              = pick(Indices),
      _NumKeys            = 1000,
      _NumMutatedKeys     = 0,
      _ObjSizeBytes       = 100,
      _ThrottleAfterBytes = 100,
      _ThrottleWaitMsecs  = 1).


%% throttling on object size with 100 mutated keys - no extra
%% throttles because of mutated keys should be done
sweep_throttle_obj_size2_test(Config) ->
    Indices = ?config(vnode_indices, Config),
    sweep_throttle_obj_size(
      _Index              = pick(Indices),
      _NumKeys            = 1000,
      _NumMutatedKeys     = 100,
      _ObjSizeBytes       = 100,
      _ThrottleAfterBytes = 200,
      _ThrottleWaitMsecs  = 1).


%% throttling on obj_size where number of bytes to throttle after if
%% bigger than object size
sweep_throttle_obj_size3_test(Config) ->
    Indices = ?config(vnode_indices, Config),
    sweep_throttle_obj_size(
      _Index              = pick(Indices),
      _NumKeys            = 1000,
      _NumMutatedKeys     = 100,
      _ObjSizeBytes       = 100,
      _ThrottleAfterBytes = 500,
      _ThrottleWaitMsecs  = 1).


%% throttling on obj_size using a sweep_window_throttle_div
sweep_throttle_obj_size4_test(Config) ->
    Indices = ?config(vnode_indices, Config),
    application:set_env(riak_kv, sweep_window_throttle_div, 2),
    sweep_throttle_obj_size(
      _Index              = pick(Indices),
      _NumKeys            = 1000,
      _NumMutatedKeys     = 100,
      _ObjSizeBytes       = 100,
      _ThrottleAfterBytes = 500,
      _ThrottleWaitMsecs  = 2).


%% throttling on obj_size sweep_window > throttle wait time.
sweep_throttle_obj_size5_test(Config) ->
    Indices = ?config(vnode_indices, Config),
    application:set_env(riak_kv, sweep_window_throttle_div, 2),
    sweep_throttle_obj_size(
      _Index              = pick(Indices),
      _NumKeys            = 1000,
      _NumMutatedKeys     = 100,
      _ObjSizeBytes       = 100,
      _ThrottleAfterBytes = 500,
      _ThrottleWaitMsecs  = 1).


sweep_throttle_obj_size(Index, NumKeys, NumMutatedKeys, ObjSizeBytes, ThrottleAfterBytes, ThrottleWaitMsecs) ->
    application:set_env(riak_kv, sweep_throttle, {obj_size, ThrottleAfterBytes, ThrottleWaitMsecs}),

    SP = meck_new_sweep_particpant(sweep_observer_1, self()),
    RiakObjSizeBytes = byte_size(riak_object_bin(<<>>, <<>>, ObjSizeBytes)),
    meck_new_backend(self(), NumKeys, RiakObjSizeBytes),
    riak_kv_sweeper:add_sweep_participant(SP),
    meck_new_visit_function(sweep_observer_1, {mutate, NumMutatedKeys}),
    ExpectedThrottleMsecs = expected_obj_size_throttle_total_msecs(
                              NumKeys, NumMutatedKeys, RiakObjSizeBytes, ThrottleAfterBytes, ThrottleWaitMsecs),
    SweepTime = (min_scheduler_response_time_msecs() + ExpectedThrottleMsecs),

    riak_kv_sweeper:sweep(Index),
    ok = receive_msg({ok, successfull_sweep, sweep_observer_1, Index}, SweepTime),
    #sa{throttle_total_wait_msecs = ActualThrottleTotalWait} = receive_sweep_result(),
    ActualThrottleTotalWait = ExpectedThrottleMsecs.


%% throttling on pace every 100 keys
sweep_throttle_pace1_test(Config) ->
    Indices = ?config(vnode_indices, Config),
    sweep_throttle_pace(
      _Index              = pick(Indices),
      _NumKeys            = 1000,
      _NumMutatedKeys     = 0,
      _NumKeysPace        = 100,
      _ThrottleWaitMsecs  = 1).


%% throttling on pace every 100 keys with one extra throttle because
%% of mutated objects
sweep_throttle_pace2_test(Config) ->
    Indices = ?config(vnode_indices, Config),
    sweep_throttle_pace(
      _Index              = pick(Indices),
      _NumKeys            = 1000,
      _NumMutatedKeys     = 100,
      _NumKeysPace        = 100,
      _ThrottleWaitMsecs  = 1).

%% throttling on pace with several extra throttles because of
%% mutations
sweep_throttle_pace3_test(Config) ->
    Indices = ?config(vnode_indices, Config),
    sweep_throttle_pace(
      _Index              = pick(Indices),
      _NumKeys            = 5000,
      _NumMutatedKeys     = 2000,
      _NumKeysPace        = 100,
      _ThrottleWaitMsecs  = 1).


%% throttling on pace using sweep_window_throttle_div
sweep_throttle_pace4_test(Config) ->
    Indices = ?config(vnode_indices, Config),
    application:set_env(riak_kv, sweep_window_throttle_div, 2),
    sweep_throttle_pace(
      _Index              = pick(Indices),
      _NumKeys            = 1000,
      _NumMutatedKeys     = 1000,
      _NumKeysPace        = 100,
      _ThrottleWaitMsecs  = 2).


%% throttling on pace using sweep_window_throttle_div where
%% sweep_window_throttle_div > throttle wait time
sweep_throttle_pace5_test(Config) ->
    Indices = ?config(vnode_indices, Config),
    application:set_env(riak_kv, sweep_window_throttle_div, 2),
    sweep_throttle_pace(
      _Index              = pick(Indices),
      _NumKeys            = 1000,
      _NumMutatedKeys     = 1000,
      _NumKeysPace        = 100,
      _ThrottleWaitMsecs  = 1).


sweep_throttle_pace(Index, NumKeys, NumMutatedKeys, NumKeysPace, ThrottleWaitMsecs) ->
    application:set_env(riak_kv, sweep_throttle, {pace, NumKeysPace, ThrottleWaitMsecs}),

    SP = meck_new_sweep_particpant(sweep_observer_1, self()),
    meck_new_backend(self(), NumKeys),
    riak_kv_sweeper:add_sweep_participant(SP),
    meck_new_visit_function(sweep_observer_1, {mutate, NumMutatedKeys}),
    ExpectedThrottleMsecs = expected_pace_throttle_total_msecs(
                              NumKeys, NumMutatedKeys, NumKeysPace, ThrottleWaitMsecs),

    SweepTime = (min_scheduler_response_time_msecs() + ExpectedThrottleMsecs),

    riak_kv_sweeper:sweep(Index),
    ok = receive_msg({ok, successfull_sweep, sweep_observer_1, Index}, SweepTime),
    #sa{throttle_total_wait_msecs = ActualThrottleTotalWait} = receive_sweep_result(),

    ActualThrottleTotalWait = ExpectedThrottleMsecs.


%% ------------------------------------------------------------------------------
%% Internal Functions
%% ------------------------------------------------------------------------------
wait_for_concurrent_sweeps(ConcurrentSweeps) ->
    wait_for_concurrent_sweeps(ConcurrentSweeps, []).

wait_for_concurrent_sweeps(0, Result) ->
    Result;
wait_for_concurrent_sweeps(ConcurrentSweeps, Result) ->
    NewResult =
        receive {_Pid, {visit_function_waiting, Index}} ->
                [Result|Index]
        end,
    wait_for_concurrent_sweeps(ConcurrentSweeps - 1, NewResult).


expected_obj_size_throttle_total_msecs(NumKeys, NumMutatedKeys, RiakObjSizeBytes, ThrottleAfterBytes, ThrottleWaitMsecs) ->
    ThrottleMsecs = expected_throttle_msecs(
                      NumKeys, ThrottleAfterBytes, ThrottleWaitMsecs, RiakObjSizeBytes),
    ThrottleExtraMsecs = expected_throttle_extra_msecs(
                           NumMutatedKeys, ThrottleWaitMsecs),

    {ok, ThrottleDiv} = application:get_env(riak_kv, sweep_window_throttle_div),
    case ThrottleDiv > ThrottleWaitMsecs of
        true ->
            0;
        false ->
            (ThrottleMsecs + ThrottleExtraMsecs) div ThrottleDiv
    end.


expected_pace_throttle_total_msecs(NumKeys, NumMutatedKeys, NumKeysPace, ThrottleWaitMsecs) ->
    ThrottleMsecs =
        (NumKeys div NumKeysPace) * ThrottleWaitMsecs +
        (NumMutatedKeys div NumKeysPace) * ThrottleWaitMsecs,
    {ok, ThrottleDiv} = application:get_env(riak_kv, sweep_window_throttle_div),
    case ThrottleDiv > ThrottleWaitMsecs of
        true ->
            0;
        false ->
            ThrottleMsecs div ThrottleDiv
    end.


expected_throttle_extra_msecs(NumMutatedKeys, WaitTimeMsecs) ->
    (NumMutatedKeys div 100) * WaitTimeMsecs.


expected_throttle_msecs(NumKeys, ThrottleAfterBytes, WaitTimeMsecs, ObjSizeBytes) ->
    case ThrottleAfterBytes div ObjSizeBytes of
        0 ->
            NumKeys * WaitTimeMsecs;
        1 ->
            (NumKeys div 2)  * WaitTimeMsecs;
        _ ->
            (NumKeys div (ThrottleAfterBytes div ObjSizeBytes))  * WaitTimeMsecs
    end.


riak_object_bin(B, K, ObjSizeBytes) ->
    riak_object:to_binary(v1, riak_object:new(B, K, <<0:ObjSizeBytes/unit:8>>)).

add_hours(Hour, Inc)  ->
    (Hour + Inc) rem 24.

visit_function_controller(TestCasePid, WaitIndex) ->
    {Pid, _Index} =
        receive
            {Pid0, {visit_function_waiting, WaitIndex}}  ->
                TestCasePid ! {self(), {visit_controller_waiting, WaitIndex}},
                receive {TestCasePid, visit_controller_continue} -> ok end,
                {Pid0, WaitIndex};
            {Pid0, {visit_function_waiting, Index0}} ->
                {Pid0, Index0}
        end,
    Pid ! visit_function_continue,
    visit_function_controller(TestCasePid, WaitIndex).


%% Waiting for 2500 msecs because at least 1 second must elapse
%% before a sweep is re-run using the run_interval (now - ts) > 1,
%% see kv_sweeper:expired
min_scheduler_response_time_msecs() ->
    2500.

pick(List) when length(List) > 0 ->
    N = random:uniform(length(List)),
    lists:nth(N, List).


receive_msg(Msg) ->
    receive_msg(Msg, 1000).

receive_msg(Msg, TimeoutMsecs) ->
    receive
        RcvMsg when RcvMsg == Msg ->
            ok
    after TimeoutMsecs ->
            timeout
    end.


receive_sweep_result() ->
    receive
        {ok, #sa{} = SA} ->
            SA
    after 5000 ->
            timeout
    end.


get_sweep_on_index(Index) ->
    {_, Sweeps} = riak_kv_sweeper:status(),
    lists:keyfind(Index, #sweep.index, Sweeps).


all_tests() ->
    [F || {F, A} <- ?MODULE:module_info(exports), is_testcase({F, A})].


is_testcase({F, 1}) ->
    match =:= re:run(atom_to_list(F), "_test$", [{capture, none}]);
is_testcase({_, _}) ->
    false.

%% watch:remove_file_changed_action(ct).
%% watch:add_file_changed_action({ct, fun(_) -> ct:run_test({suite, riak_kv_sweeper_SUITE}) end}).
