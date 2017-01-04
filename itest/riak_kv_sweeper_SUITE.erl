-module(riak_kv_sweeper_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").

%%--------------------------------------------------------------------
%% COMMON TEST CALLBACK FUNCTIONS
%%--------------------------------------------------------------------
suite() ->
    [{timetrap, {minutes, 2}}].

init_per_suite(Config) ->
    {ok, Apps} = application:ensure_all_started(exometer_core),
    %% Exometer starts lager. Set lager_console_backend to none to
    %% avoid spamming the console with logs during testing
    lager:set_loglevel(lager_console_backend, none),
    [{exometer_apps, Apps}|Config].

end_per_suite(Config) ->
    [ok = application:stop(App) || App <- ?config(exometer_apps, Config)],
    ok.

init_per_testcase(TestCase, Config) ->
    ct:pal("Running ~p", [TestCase]),
    meck:unload(),
    application:set_env(riak_kv, sweep_participants, undefined),
    application:set_env(riak_kv, sweep_window, always),
    application:set_env(riak_kv, sweeper_scheduler, false),
    application:set_env(riak_kv, sweep_tick, 10), % MSecs
    application:set_env(riak_kv, sweep_throttle, {obj_size, 0, 0}), %Disable throttling
    application:set_env(riak_kv, sweep_window_throttle_div, 1),
    application:set_env(riak_kv, sweep_concurrency, 1),

    VNodeIndices = meck_new_riak_core_modules(2),
    meck:new(chronos),
    meck:expect(chronos, start_timer, fun(_, _, _, _) -> ok end),
    riak_kv_sweeper:start_link(),

    %% Delete all sweeper exometer metrics
    [begin
         exometer:delete([riak, riak_kv, sweeper, Index, keys]),
         exometer:delete([riak, riak_kv, sweeper, Index, bytes]),
         exometer:delete([riak, riak_kv, sweeper, Index, failed, sweep_observer_1]),
         exometer:delete([riak, riak_kv, sweeper, Index, successful, sweep_observer_1]),
         exometer:delete([riak, riak_kv, sweeper, Index, mutated]),
         exometer:delete([riak, riak_kv, sweeper, Index, deleted])
     end || Index <- VNodeIndices],

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
    meck:expect(riak_kv_index_hashtree, estimate_keys, fun(_) -> {ok, EstimatedKeys} end),
    meck:expect(riak_kv_index_hashtree, release_lock, fun(_) -> ok end).

meck_new_sweep_particpant(Name, TestCasePid) ->
    meck:new(Name, [non_strict, no_link]),
    meck:expect(Name, participate_in_sweep,
                fun(Index, _Pid) ->
                        TestCasePid ! {ok, participate_in_sweep},
                        InitialAcc = 0,
                        {ok, Name:visit_object_fun(Index), InitialAcc}
                end),

    meck_new_visit_function(Name),
    meck_new_successful_sweep_function(Name, TestCasePid),
    meck_new_failed_sweep_function(Name, TestCasePid),

    #sweep_participant{description = atom_to_list(Name) ++ " sweep participant",
                       module = Name,
                       fun_type = observe_fun,
                       run_interval = 60,       % Secs
                       options = []}.

meck_new_visit_function(Name) ->
    meck_new_visit_function(Name, no_errors).

meck_new_visit_function(Name, Behavior) ->
    meck:expect(Name, visit_object_fun, visit_function(Behavior)).

meck_new_successful_sweep_function(Name, TestCasePid) ->
    meck:expect(Name, successful_sweep, successful_sweep_function(Name, TestCasePid)).

meck_new_failed_sweep_function(Name, TestCasePid) ->
    meck:expect(Name, failed_sweep, failed_sweep_function(Name, TestCasePid)).

visit_function({throw, Error}) ->
    fun(_Index) ->
        fun({{_Bucket, _Key}, _RObj}, _Acc, _Opts = []) ->
                throw(Error)
        end
    end;

visit_function({wait, From, Indices}) ->
    fun(Index) ->
        fun({{_Bucket, _Key}, _RObj}, Acc, _Opts = []) ->
                NewAcc =
                case Acc of
                    {continue, Index, VisitedKeys} ->
                        {continue, Index, VisitedKeys + 1};
                    VisitedKeys when is_integer(VisitedKeys) ->
                        case lists:member(Index, Indices) of
                            true ->
                                From ! {self(), Index},
                                receive
                                    {From, continue} -> ok
                                end;
                            false ->
                                ok
                        end,
                        {continue, Index, VisitedKeys + 1}
                end,
                {ok, NewAcc}
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

visit_function({delete, Count}) ->
    fun(_Index) ->
        fun({{_Bucket, _Key}, _RObj}, Acc, _Opts = []) ->
                NewAcc = Acc + 1,
                case NewAcc =< Count of
                    true ->
                        {deleted, NewAcc};
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

successful_sweep_function(Name, TestCasePid) ->
    fun(Index, _FinalAcc) ->
            TestCasePid ! {ok, successful_sweep, Name, Index},
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
    meck:new(meck_new_backend, [non_strict]),
    meck_new_fold_objects_function(TestCasePid, NumKeys, ObjSizeBytes),
    meck_new_riak_kv_vnode().

meck_new_fold_objects_function(TestCasePid, NumKeys, ObjSizeBytes) ->
    meck_new_fold_objects_function(async, TestCasePid, NumKeys, ObjSizeBytes).

meck_new_fold_objects_function(async, TestCasePid, NumKeys, ObjSizeBytes) ->
    Keys = [integer_to_binary(N) || N <- lists:seq(1, NumKeys)],
    meck:expect(meck_new_backend, fold_objects,
                fun(CompleteFoldReq, InitialAcc, _, _) ->
                        Work = fun() ->
                                       Result =
                                           lists:foldl(fun(NBin, Acc) ->
                                                               InitialObj = riak_object_bin(NBin, NBin, ObjSizeBytes),
                                                               CompleteFoldReq(NBin, NBin, InitialObj, Acc)
                                                       end, InitialAcc, Keys),
                                       TestCasePid ! {ok, fold_complete},
                                       Result
                               end,
                        {async, Work}
                end);

meck_new_fold_objects_function(sync, TestCasePid, NumKeys, ObjSizeBytes) ->
    Keys = [integer_to_binary(N) || N <- lists:seq(1, NumKeys)],
    meck:expect(meck_new_backend, fold_objects,
                fun(CompleteFoldReq, InitialAcc, _, _) ->
                        SA =
                            lists:foldl(fun(NBin, Acc) ->
                                                InitialObj = riak_object_bin(NBin, NBin, ObjSizeBytes),
                                                CompleteFoldReq(NBin, NBin, InitialObj, Acc)
                                        end, InitialAcc, Keys),
                        TestCasePid ! {ok, fold_complete},
                        {ok, SA}
                end).

meck_new_riak_kv_vnode() ->
    meck:new(riak_kv_vnode),
    meck:expect(riak_kv_vnode, sweep,
                fun({Index, _Node}, ActiveParticipants, EstimatedKeys) ->
                        meck_riak_kv_vnode_sweep_worker(
                          ActiveParticipants, EstimatedKeys, Index)
                end),
    meck:expect(riak_kv_vnode, local_put, fun(_, _, _) -> ok end),
    meck:expect(riak_kv_vnode, local_reap, fun(_, _, _) -> ok end).

meck_riak_kv_vnode_sweep_worker(ActiveParticipants, EstimatedKeys, Index) ->
    spawn_link(fun() ->
                       meck_vnode_worker_func(ActiveParticipants, EstimatedKeys, Index)
               end).

meck_vnode_worker_func(ActiveParticipants, EstimatedKeys, Index) ->
    case
        riak_kv_sweeper_fold:do_sweep(
          ActiveParticipants, EstimatedKeys, _Sender = '?',
          _Opts = [], Index, meck_new_backend, _ModState = '?',
          _VnodeState = '?') of
        {async, {sweep, FoldFun, FinishFun}, _Sender, _VnodeState} ->
            try
                SA0 = FoldFun(),
                FinishFun(SA0)
            catch throw:{stop_sweep, PrematureAcc} ->
                    FinishFun(PrematureAcc),
                    PrematureAcc;
                  throw:PrematureAcc  ->
                    FinishFun(PrematureAcc),
                    PrematureAcc
            end;
        {reply, Acc, _VnodeStat} ->
            Acc
    end.

meck_sleep_for_throttle() ->
    meck:new(riak_kv_sweeper, [passthrough]),
    meck:expect(riak_kv_sweeper, sleep_for_throttle, fun(_) -> ok end).

meck_count_msecs_slept() ->
    ThrottleHistory = meck:history(riak_kv_sweeper),
    MSecsThrottled = [MSecs || {_Pid, {riak_kv_sweeper, sleep_for_throttle, [MSecs]}, ok} <-
                               ThrottleHistory],
    lists:sum(MSecsThrottled).

%%--------------------------------------------------------------------
%% TEST CASES
%%--------------------------------------------------------------------
initiailize_sweep_request_test(Config) ->
    Indices = ?config(vnode_indices, Config),
    assert_all_indices_idle(Indices),
    ok.

status_index_changed_sweep_request_test(Config) ->
    Indices = ?config(vnode_indices, Config),
    assert_all_indices_idle(Indices),

    NewPartitions = 2 * length(Indices),
    NewIndices = meck_new_riak_core_ring(NewPartitions),
    Index = pick(NewIndices -- Indices),
    riak_kv_sweeper:sweep(Index),
    assert_all_indices_idle(NewIndices),
    ok.

status_index_changed_tick_test(Config) ->
    Indices = ?config(vnode_indices, Config),
    assert_all_indices_idle(Indices),

    NewPartitions = 2 * length(Indices),
    NewIndices = meck_new_riak_core_ring(NewPartitions),
    riak_kv_sweeper:sweep_tick(),
    assert_all_indices_idle(NewIndices),
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
    ok = receive_msg({ok, successful_sweep, sweep_observer_1, I0}),

    I1 = pick(Indices),
    riak_kv_sweeper:sweep(I1),
    ok = receive_msg({ok, successful_sweep, sweep_observer_1, I1}).

sweep_request_non_existing_index_test(Config) ->
    Indices = ?config(vnode_indices, Config),
    SP = meck_new_sweep_particpant(sweep_observer_1, self()),
    meck_new_backend(self()),
    riak_kv_sweeper:add_sweep_participant(SP),

    Max = lists:max(Indices),
    NonExisting = Max * Max,

    false = lists:member(NonExisting, Indices),
    riak_kv_sweeper:sweep(NonExisting),
    ok.

scheduler_sync_backend_test(Config) ->
    Indices = ?config(vnode_indices, Config),

    meck_new_backend(self()),
    meck_new_fold_objects_function(sync, self(), 1000, 100),
    SP = meck_new_sweep_particpant(sweep_observer_1, self()),
    riak_kv_sweeper:add_sweep_participant(SP),

    sweep_all_indices(Indices),
    ok.

scheduler_test(Config) ->
    Indices = ?config(vnode_indices, Config),
    meck_new_backend(self()),
    SP = meck_new_sweep_particpant(sweep_observer_1, self()),
    riak_kv_sweeper:add_sweep_participant(SP),

    sweep_all_indices(Indices),
    ok.

%% TODO - Why are we not monitoring the worker process doing the fold ?
scheduler_worker_process_crashed_test(Config) ->
    Indices = ?config(vnode_indices, Config),
    meck_new_backend(self()),
    SP = meck_new_sweep_particpant(sweep_observer_1, self()),
    meck_new_visit_function(sweep_observer_1, {throw, crash}),
    riak_kv_sweeper:add_sweep_participant(SP),

    sweep_all_indices(Indices, timeout),

    {_SPs, Sweeps} = riak_kv_sweeper:status(),
    false = lists:any(fun(S) -> running =:= riak_kv_sweeper_state:sweep_state(S) end, Sweeps),
    ok.

scheduler_run_interval_test(Config) ->
    Indices = ?config(vnode_indices, Config),
    meck_new_backend(self()),
    SP = meck_new_sweep_particpant(sweep_observer_1, self()),
    SP1 = SP#sweep_participant{run_interval = 1},
    meck_new_visit_function(sweep_observer_1),
    riak_kv_sweeper:add_sweep_participant(SP1),

    sweep_all_indices(Indices),
    timer:sleep(min_scheduler_response_time_msecs()),
    sweep_all_indices(Indices),

    ok.

scheduler_remove_participant_test(Config) ->
    Indices = ?config(vnode_indices, Config),
    WaitIndex = pick(Indices),
    TestCasePid = self(),
    meck_new_backend(TestCasePid, _NumKeys = 5000),
    SP = meck_new_sweep_particpant(sweep_observer_1, TestCasePid),
    SP1 = SP#sweep_participant{run_interval = 1},

    meck_new_visit_function(sweep_observer_1, {wait, TestCasePid, [WaitIndex]}),
    riak_kv_sweeper:add_sweep_participant(SP1),
    riak_kv_sweeper:enable_sweep_scheduling(),

    tick_per_index(Indices),
    receive {From, WaitIndex} ->
            riak_kv_sweeper:remove_sweep_participant(sweep_observer_1),
            From ! {TestCasePid, continue}
    end,

    ok = receive_msg({ok, failed_sweep, sweep_observer_1, WaitIndex}).

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
    meck_new_visit_function(sweep_observer_1, {wait, TestCasePid, [WaitIndex]}),

    %% Lets sweeps run once so they are not in never run state
    tick_per_index(Indices),
    receive {From0, WaitIndex} ->
            From0 ! {TestCasePid, continue}
    end,
    [ok = receive_msg({ok, successful_sweep, sweep_observer_1, I}) || I <- Indices],

    application:set_env(riak_kv, sweep_concurrency, 1),
    riak_kv_sweeper:sweep(WaitIndex),
    receive {From1, WaitIndex} ->
            riak_kv_sweeper:sweep(WaitIndex1),
            Sweep = get_sweep_on_index(WaitIndex1),
            {_, _, _} = riak_kv_sweeper_state:sweep_queue_time(Sweep),
            From1 ! {TestCasePid, continue}
    end,

    ok = receive_msg({ok, successful_sweep, sweep_observer_1, WaitIndex}),
    match_retry(fun riak_kv_sweeper:status/0, [{}, {}]),

    riak_kv_sweeper:sweep_tick(),
    ok = receive_msg({ok, successful_sweep, sweep_observer_1, WaitIndex1}).

scheduler_sweep_window_never_test(Config) ->
    Indices = ?config(vnode_indices, Config),
    TestCasePid = self(),

    application:set_env(riak_kv, sweep_window, never),
    meck_new_backend(TestCasePid, _NumKeys = 5000),
    SP = meck_new_sweep_particpant(sweep_observer_1, TestCasePid),
    SP1 = SP#sweep_participant{run_interval = 1},
    riak_kv_sweeper:add_sweep_participant(SP1),
    riak_kv_sweeper:enable_sweep_scheduling(),

    StatusBefore = riak_kv_sweeper:status(),
    riak_kv_sweeper:sweep_tick(),
    StatusAfter = riak_kv_sweeper:status(),
    StatusAfter = StatusBefore,

    application:set_env(riak_kv, sweep_window, always),
    timer:sleep(min_scheduler_response_time_msecs()),

    sweep_all_indices(Indices),
    ok.

scheduler_now_outside_sleep_window_test(Config) ->
    Indices = ?config(vnode_indices, Config),
    TestCasePid = self(),

    {_, {Hour, _, _}} = calendar:local_time(),
    Start = add_hours(Hour, 2),
    End = add_hours(Start, 1),

    application:set_env(riak_kv, sweep_window, {Start, End}),
    meck_new_backend(TestCasePid, _NumKeys = 5000),
    SP = meck_new_sweep_particpant(sweep_observer_1, TestCasePid),
    SP1 = SP#sweep_participant{run_interval = 1},
    riak_kv_sweeper:add_sweep_participant(SP1),

    StatusBefore = riak_kv_sweeper:status(),
    riak_kv_sweeper:sweep_tick(),
    StatusAfter = riak_kv_sweeper:status(),
    StatusAfter = StatusBefore,

    application:set_env(riak_kv, sweep_window, {Hour, add_hours(Hour, 1)}),
    sweep_all_indices(Indices),
    ok.

%%
%% This test runs long ~5 seconds because the it schedulers `expired'
%% and the scheduler has a minimum resolution of 1 second in this
%% case.
stop_all_scheduled_sweeps_test(Config) ->
    Indices = ?config(vnode_indices, Config),
    TestCasePid = self(),
    NumMsgRecvAfterSweptKeys = 1000,
    application:set_env(riak_kv, sweep_concurrency, length(Indices)),
    meck_new_backend(self(), _NumKeys = NumMsgRecvAfterSweptKeys * 5),
    SP = meck_new_sweep_particpant(sweep_observer_1, self()),
    SP1 = SP#sweep_participant{run_interval = 1},
    meck_new_visit_function(sweep_observer_1),
    riak_kv_sweeper:add_sweep_participant(SP1),
    riak_kv_sweeper:enable_sweep_scheduling(),

    %% Lets sweeps run once so they are not in never run state
    sweep_all_indices(Indices),

    meck_new_visit_function(sweep_observer_1, {wait, TestCasePid, Indices}),

    match_retry(fun riak_kv_sweeper:status/0, [{}, {}]),
    riak_kv_sweeper:sweep_tick(),

    receive {From, _AnyIndex} when is_pid(From) ->
            Running = riak_kv_sweeper:stop_all_sweeps(),
            true = Running >= 1,
            From ! {TestCasePid, continue}

    end,
    sweep_all_indices(Indices, timeout),
    ok.

stop_all_scheduled_sweeps_race_condition_test(Config) ->
    Indices = ?config(vnode_indices, Config),
    NumMsgRecvAfterSweptKeys = 1000,

    meck_new_backend(self(), NumMsgRecvAfterSweptKeys*5),
    SP = meck_new_sweep_particpant(sweep_observer_1, self()),
    meck_new_visit_function(sweep_observer_1),
    riak_kv_sweeper:add_sweep_participant(SP),
    riak_kv_sweeper:enable_sweep_scheduling(),
    Running = riak_kv_sweeper:stop_all_sweeps(),
    Running = 0,
    [timeout = receive_msg({ok, successful_sweep, sweep_observer_1, I}) || I <- Indices],
    ok.

scheduler_add_participant_test(Config) ->
    Indices = ?config(vnode_indices, Config),
    meck_new_backend(self()),
    riak_kv_sweeper:enable_sweep_scheduling(),

    SP1 = meck_new_sweep_particpant(sweep_observer_1, self()),
    riak_kv_sweeper:add_sweep_participant(SP1#sweep_participant{run_interval = 1}),

    sweep_all_indices(Indices),
    timer:sleep(min_scheduler_response_time_msecs()),
    sweep_all_indices(Indices),

    SP2 = meck_new_sweep_particpant(sweep_observer_2, self()),
    riak_kv_sweeper:add_sweep_participant(SP2#sweep_participant{run_interval = 1}),

    sweep_all_indices(Indices, ok, sweep_observer_2),
    timer:sleep(min_scheduler_response_time_msecs()),
    sweep_all_indices(Indices, ok, sweep_observer_2),
    ok.

scheduler_restart_sweep_test(Config) ->
    Indices = ?config(vnode_indices, Config),
    TestCasePid = self(),
    WaitIndex = pick(Indices),
    meck_new_backend(TestCasePid, _NumKeys = 5000),
    SP = meck_new_sweep_particpant(sweep_observer_1, TestCasePid),
    meck_new_visit_function(sweep_observer_1, {wait, TestCasePid, [WaitIndex]}),
    SP1 = SP#sweep_participant{run_interval = 1},
    riak_kv_sweeper:add_sweep_participant(SP1),
    riak_kv_sweeper:enable_sweep_scheduling(),

    tick_per_index(Indices),
    receive {From0, WaitIndex} ->
            RunningSweep = get_sweep_on_index(WaitIndex),
            running = riak_kv_sweeper_state:sweep_state(RunningSweep),

            riak_kv_sweeper:sweep(WaitIndex),
            RestartingSweep = get_sweep_on_index(WaitIndex),
            restart = riak_kv_sweeper_state:sweep_state(RestartingSweep),
            undefined = riak_kv_sweeper_state:sweep_queue_time(RestartingSweep),

            riak_kv_sweeper:sweep(WaitIndex),
            RestartingSweep = get_sweep_on_index(WaitIndex),

            From0 ! {TestCasePid, continue}
    end,
    ok = receive_msg({ok, failed_sweep, sweep_observer_1, WaitIndex}),

    match_retry(fun riak_kv_sweeper:status/0, [{}, {}]),
    tick_per_index(Indices),
    receive {From1, WaitIndex} ->
            From1 ! {TestCasePid, continue}
    end,
    ok = receive_msg({ok, successful_sweep, sweep_observer_1, WaitIndex}, min_scheduler_response_time_msecs()).

scheduler_estimated_keys_lock_ok_test(Config) ->
    Indices = ?config(vnode_indices, Config),
    TestCasePid = self(),

    meck_new_aae_modules(_AAEnabled = true,
                         _EstimatedKeys = 4200,
                         _LockResult = ok),
    meck_new_backend(TestCasePid, _NumKeys = 5000),
    SP = meck_new_sweep_particpant(sweep_observer_1, TestCasePid),
    SP1 = SP#sweep_participant{run_interval = 1},
    meck_new_visit_function(sweep_observer_1),

    riak_kv_sweeper:add_sweep_participant(SP1),
    riak_kv_sweeper:enable_sweep_scheduling(),

    sweep_all_indices(Indices),

    [true = meck:called(riak_kv_index_hashtree, get_lock, [I, estimate]) || I <- Indices],
    [true = meck:called(riak_kv_index_hashtree, estimate_keys, [I]) || I <- Indices],
    [true = meck:called(riak_kv_index_hashtree, release_lock, [I]) || I <- Indices],

    ok.

scheduler_estimated_keys_lock_fail_test(Config) ->
    Indices = ?config(vnode_indices, Config),
    TestCasePid = self(),
    meck_new_aae_modules(_AAEnabled = true,
                         _EstimatedKeys = 4200,
                         _LockResult = fail),
    meck_new_backend(TestCasePid, _NumKeys = 5000),
    SP = meck_new_sweep_particpant(sweep_observer_1, TestCasePid),
    meck_new_visit_function(sweep_observer_1),
    SP1 = SP#sweep_participant{run_interval = 1},
    riak_kv_sweeper:add_sweep_participant(SP1),

    sweep_all_indices(Indices),

    [true = meck:called(riak_kv_index_hashtree, get_lock, [I, estimate]) || I <- Indices],
    [false = meck:called(riak_kv_index_hashtree, estimate_keys, [I]) || I <- Indices],
    [false = meck:called(riak_kv_index_hashtree, release_lock, [I]) || I <- Indices],
    ok.

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
    meck_sleep_for_throttle(),

    SP = meck_new_sweep_particpant(sweep_observer_1, self()),
    RiakObjSizeBytes = byte_size(riak_object_bin(<<>>, <<>>, ObjSizeBytes)),
    meck_new_backend(self(), NumKeys, RiakObjSizeBytes),
    riak_kv_sweeper:add_sweep_participant(SP),
    meck_new_visit_function(sweep_observer_1, {mutate, NumMutatedKeys}),
    ExpectedThrottleMsecs = expected_obj_size_throttle_total_msecs(
                              NumKeys, NumMutatedKeys, RiakObjSizeBytes, ThrottleAfterBytes, ThrottleWaitMsecs),
    SweepTime = (min_scheduler_response_time_msecs() + ExpectedThrottleMsecs),

    riak_kv_sweeper:sweep(Index),
    ok = receive_msg({ok, successful_sweep, sweep_observer_1, Index}, SweepTime),

    ActualThrottleTotalWait = meck_count_msecs_slept(),
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
    meck_sleep_for_throttle(),

    SP = meck_new_sweep_particpant(sweep_observer_1, self()),
    meck_new_backend(self(), NumKeys),
    riak_kv_sweeper:add_sweep_participant(SP),
    meck_new_visit_function(sweep_observer_1, {mutate, NumMutatedKeys}),
    ExpectedThrottleMsecs = expected_pace_throttle_total_msecs(
                              NumKeys, NumMutatedKeys, NumKeysPace, ThrottleWaitMsecs),

    riak_kv_sweeper:sweep(Index),
    ok = receive_msg({ok, successful_sweep, sweep_observer_1, Index}),

    ActualThrottleTotalWait = meck_count_msecs_slept(),
    ActualThrottleTotalWait = ExpectedThrottleMsecs.

sweeper_exometer_reporting_test(Config) ->
    Indices = ?config(vnode_indices, Config),
    SP = meck_new_sweep_particpant(sweep_observer_1, self()),
    NumKeys = 5042,
    ObjSizeBytes = 100,
    RiakObjSizeBytes = byte_size(riak_object_bin(<<>>, <<>>, ObjSizeBytes)),
    meck_new_backend(self(), NumKeys, ObjSizeBytes),
    riak_kv_sweeper:add_sweep_participant(SP),

    Index = pick(Indices),
    {error, not_found} = exometer:get_value([riak, riak_kv, sweeper, Index, keys]),
    riak_kv_sweeper:sweep(Index),
    ok = receive_msg({ok, successful_sweep, sweep_observer_1, Index}),

    KeysCount0 = NumKeys,
    KeysOne0 = NumKeys,
    {ok, [{count, KeysCount0}, {one, KeysOne0}]} =
        match_retry(fun() -> exometer:get_value([riak, riak_kv, sweeper, Index, keys]) end,
                    {ok, [{count, KeysCount0}, {one, KeysOne0}]}),

    BytesCount0 = NumKeys * RiakObjSizeBytes,
    BytesOne0 = NumKeys * RiakObjSizeBytes,

    {ok, [{count, BytesCount0}, {one, BytesOne0}]} =
        match_retry(fun() -> exometer:get_value([riak, riak_kv, sweeper, Index, bytes]) end,
                    {ok, [{count, BytesCount0}, {one, BytesOne0}]}),

    riak_kv_sweeper:sweep(Index),
    ok = receive_msg({ok, successful_sweep, sweep_observer_1, Index}),
    KeysCount1 = 2 * NumKeys,
    KeysOne1 = 2 * NumKeys,

    {ok, [{count, KeysCount1}, {one, KeysOne1}]} =
        match_retry(fun() -> exometer:get_value([riak, riak_kv, sweeper, Index, keys]) end,
                    {ok, [{count, KeysCount1}, {one, KeysOne1}]}),

    BytesCount1 = 2 * NumKeys * RiakObjSizeBytes,
    BytesOne1 = 2 * NumKeys * RiakObjSizeBytes,
    {ok, [{count, BytesCount1}, {one, BytesOne1}]} =
        match_retry(fun() -> exometer:get_value([riak, riak_kv, sweeper, Index, bytes]) end,
                    {ok, [{count, BytesCount1}, {one, BytesOne1}]}),
    ok.

sweeper_exometer_successful_sweep_test(Config) ->
    Indices = ?config(vnode_indices, Config),
    I0 = pick(Indices),
    TestCasePid = self(),
    meck_new_backend(TestCasePid, _NumKeys = 5000),
    SP = meck_new_sweep_particpant(sweep_observer_1, TestCasePid),
    SP1 = SP#sweep_participant{run_interval = 1},

    meck_new_visit_function(sweep_observer_1),
    riak_kv_sweeper:add_sweep_participant(SP1),

    sweep_all_indices(Indices),

    {error, not_found} =
        match_retry(fun() ->
                            exometer:get_value([riak, riak_kv, sweeper, I0,
                                                failed, sweep_observer_1])
                    end,
                    {error, not_found}),
    {ok, [{count, 1}, {one, 1}]} =
        match_retry(fun() ->
                            exometer:get_value([riak, riak_kv, sweeper, I0,
                                                successful, sweep_observer_1])
                    end,  {ok, [{count, 1}, {one, 1}]}),

    riak_kv_sweeper:sweep(I0),

    {error, not_found} =
        match_retry(fun() ->
                            exometer:get_value([riak, riak_kv, sweeper, I0,
                                                failed, sweep_observer_1])
                    end,
                    {error, not_found}),
    {ok, [{count, 2}, {one, 2}]} =
        match_retry(fun() ->
                            exometer:get_value([riak, riak_kv, sweeper, I0,
                                                successful, sweep_observer_1])
                    end,  {ok, [{count, 2}, {one, 2}]}),
    ok.

sweeper_exometer_failed_sweep_test(Config) ->
    Indices = ?config(vnode_indices, Config),
    I0 = pick(Indices),
    TestCasePid = self(),
    meck_new_backend(TestCasePid, _NumKeys = 5000),
    SP = meck_new_sweep_particpant(sweep_observer_1, TestCasePid),
    SP1 = SP#sweep_participant{run_interval = 1},

    meck_new_visit_function(sweep_observer_1, {throw, crash}),
    riak_kv_sweeper:add_sweep_participant(SP1),
    riak_kv_sweeper:enable_sweep_scheduling(),

    tick_per_index(Indices),
    [ok = receive_msg({ok, failed_sweep, sweep_observer_1, I}) || I <- Indices],

    {error, not_found} =
        match_retry(fun() ->
                            exometer:get_value([riak, riak_kv, sweeper, I0,
                                                successful, sweep_observer_1])
                    end,
                    {error, not_found}),
    {ok, [{count, 1}, {one, 1}]} =
        match_retry(fun() ->
                            exometer:get_value([riak, riak_kv, sweeper, I0,
                                                failed, sweep_observer_1])
                    end,  {ok, [{count, 1}, {one, 1}]}),

    riak_kv_sweeper:sweep(I0),

    {error, not_found} =
        match_retry(fun() ->
                            exometer:get_value([riak, riak_kv, sweeper, I0,
                                                successful, sweep_observer_1])
                    end,
                    {error, not_found}),
    {ok, [{count, 2}, {one, 2}]} =
        match_retry(fun() ->
                            exometer:get_value([riak, riak_kv, sweeper, I0,
                                                failed, sweep_observer_1])
                    end,  {ok, [{count, 2}, {one, 2}]}),
    ok.

exometer_mutated_object_count(Config) ->
    Indices = ?config(vnode_indices, Config),
    I0 = pick(Indices),

    SP = meck_new_sweep_particpant(sweep_observer_1, self()),
    meck_new_backend(self(), _NumKeys = 5042),
    riak_kv_sweeper:add_sweep_participant(SP),
    meck_new_visit_function(sweep_observer_1, {mutate, MutateKeys = 100}),
    riak_kv_sweeper:sweep(I0),
    ok = receive_msg({ok, successful_sweep, sweep_observer_1, I0}),
    {ok, [{count, MutateKeys},{one, MutateKeys}]} =
        match_retry(fun() -> exometer:get_value([riak, riak_kv, sweeper, I0, mutated]) end,
                    {ok, [{count, MutateKeys}, {one, MutateKeys}]}),
    ok.

exometer_deleted_object_count(Config) ->
    Indices = ?config(vnode_indices, Config),
    I0 = pick(Indices),

    SP = meck_new_sweep_particpant(sweep_observer_1, self()),
    meck_new_backend(self(), _NumKeys = 5042),
    riak_kv_sweeper:add_sweep_participant(SP),
    meck_new_visit_function(sweep_observer_1, {delete, DeleteKeys = 100}),
    riak_kv_sweeper:sweep(I0),
    ok = receive_msg({ok, successful_sweep, sweep_observer_1, I0}),
    {ok, [{count, DeleteKeys},{one, DeleteKeys}]} =
        match_retry(fun() -> exometer:get_value([riak, riak_kv, sweeper, I0, mutated]) end,
                    {ok, [{count, DeleteKeys}, {one, DeleteKeys}]}),
    ok.

%% ------------------------------------------------------------------------------
%% Internal Functions
%% ------------------------------------------------------------------------------

wait_for_concurrent_sweeps(ConcurrentSweeps) ->
    wait_for_concurrent_sweeps(ConcurrentSweeps, []).

wait_for_concurrent_sweeps(0, Result) ->
    Result;

wait_for_concurrent_sweeps(ConcurrentSweeps, Result) ->
    {NewConcurrentSweeps, NewResult} =
        receive {From, Index} when is_pid(From) ->
                {ConcurrentSweeps - 1, [Result|Index]};
                _ ->
                {ConcurrentSweeps, Result}
        end,

    wait_for_concurrent_sweeps(NewConcurrentSweeps, NewResult).

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

%% Waiting for 2500 msecs because at least 1 second must elapse
%% before a sweep is re-run using the run_interval (now - ts) > 1,
%% see kv_sweeper:expired
min_scheduler_response_time_msecs() ->
    2500.

pick(List) when length(List) > 0 ->
    N = random:uniform(length(List)),
    lists:nth(N, List).

sweep_all_indices(Indices) ->
    sweep_all_indices(Indices, ok).

sweep_all_indices(Indices, ExpectedResult) ->
    sweep_all_indices(Indices, ExpectedResult, sweep_observer_1).

sweep_all_indices(Indices, ExpectedResult, SweepParticipant) ->
    riak_kv_sweeper:enable_sweep_scheduling(),
    tick_per_index(Indices),
    ReceiveFun = fun(I) ->
                         ExpectedResult = receive_msg({ok, successful_sweep, SweepParticipant, I})
                 end,
    lists:foreach(ReceiveFun, Indices).

tick_per_index(Indices) ->
    application:set_env(riak_kv, sweep_concurrency, length(Indices)),
    %% Only one sweep scheduled per tick, so send a tick for each index:
    lists:foreach(fun(_) -> riak_kv_sweeper:sweep_tick() end, Indices).

assert_all_indices_idle(Indices) ->
    {_, Sweeps} = riak_kv_sweeper:status(),
    SweepData = [{riak_kv_sweeper_state:sweep_state(S),
                  riak_kv_sweeper_state:sweep_index(S)} ||
                 S <- Sweeps],
    SweepIndices = lists:sort([I || {idle, I} <- SweepData]),
    SweepIndices = Indices.

receive_msg(Msg) ->
    receive_msg(Msg, 1000).

receive_msg(Msg, TimeoutMsecs) ->
    receive
        RcvMsg when RcvMsg == Msg ->
            ok
    after TimeoutMsecs ->
            timeout
    end.

get_sweep_on_index(Index) ->
    {_, Sweeps} = riak_kv_sweeper:status(),
    IndexedSweeps = [{riak_kv_sweeper_state:sweep_index(S), S} || S <- Sweeps],
    {Index, Sweep} = lists:keyfind(Index, 1, IndexedSweeps),
    Sweep.

all_tests() ->
    [F || {F, A} <- ?MODULE:module_info(exports), is_testcase({F, A})].

is_testcase({F, 1}) ->
    match =:= re:run(atom_to_list(F), "_test$", [{capture, none}]);
is_testcase({_, _}) ->
    false.

match_retry(Fun, Result) ->
   match_retry(_Max = 15,  _N = 1, Fun, Result).
match_retry(Max, N, _Fun, max_retries) when N > Max ->
    throw(max_retries);
match_retry(Max, N, _Fun, _Result) when N > Max ->
    max_retries;
match_retry(Max, N, Fun, Result) ->
    case Fun() of
        Result ->
            Result;
        _ ->
            timer:sleep(1000),
            match_retry(Max, N+1, Fun, Result)
    end.
