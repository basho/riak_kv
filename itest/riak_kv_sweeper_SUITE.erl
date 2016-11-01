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
    file:delete(riak_kv_sweeper:sweep_file()),
    application:set_env(riak_kv, sweep_participants, undefined),
    application:set_env(riak_kv, sweep_window, always),
    application:set_env(riak_kv, sweeper_scheduler, false),
    application:set_env(riak_kv, sweep_tick, 10), % MSecs

    VNodeIndices = new_meck_riak_core_modules(2),
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
new_meck_riak_core_modules(Partitions) ->
    Ring = ring,
    VNodeIndices = [N || N <- lists:seq(0, Partitions)],
    meck:new(riak_core_node_watcher),
    meck:new(riak_core_ring_manager),
    meck:expect(riak_core_ring_manager, get_my_ring, fun() -> {ok, Ring} end),
    meck:new(riak_core_ring),
    meck:expect(riak_core_ring, my_indices, fun(ring) -> VNodeIndices end),
    meck:expect(riak_core_node_watcher, services, fun(_Node) -> [riak_kv] end),
    VNodeIndices.

new_meck_sweep_particpant(Name, TestCasePid) ->
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
    meck:expect(Name, failed_sweep_function, failed_sweep_function(Name, TestCasePid)).

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
    Keys = [integer_to_binary(N) || N <- lists:seq(1, NumKeys)],
    meck:new(meck_new_backend, [non_strict]),
    meck:expect(meck_new_backend, fold_objects,
                fun(CompleteFoldReq, InitialAcc, _, _) ->
                        Result = lists:foldl(fun(NBin, Acc) ->
                                                     InitialObj = riak_object:to_binary(v1, riak_object:new(NBin, NBin, <<>>)),
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
                                  riak_kv_sweeper_fold:do_sweep(ActiveParticipants,
                                                                EstimatedKeys,
                                                                _Sender = '?',
                                                                _Opts = [],
                                                                Index,
                                                                meck_new_backend,
                                                                _ModState = '?',
                                                                _VnodeState = '?')
                          end)
                end).


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
    SP = new_meck_sweep_particpant(sweep_observer_1, self()),
    riak_kv_sweeper:add_sweep_participant(SP),
    {[#sweep_participant{module = sweep_observer_1}], _} = riak_kv_sweeper:status(),
    ok.


add_participant_persistent_test(_Config) ->
    SP = new_meck_sweep_particpant(sweep_observer_1, self()),
    riak_kv_sweeper:add_sweep_participant(SP),
    {[#sweep_participant{module = sweep_observer_1}], _} = riak_kv_sweeper:status(),

    ok = riak_kv_sweeper:stop(),
    riak_kv_sweeper:start_link(),
    {[#sweep_participant{module = sweep_observer_1}], _} = riak_kv_sweeper:status(),
    ok.


remove_participant_test(_Config) ->
    SP = new_meck_sweep_particpant(sweep_observer_1, self()),
    riak_kv_sweeper:add_sweep_participant(SP),
    {[#sweep_participant{module = sweep_observer_1}], _} = riak_kv_sweeper:status(),
    riak_kv_sweeper:remove_sweep_participant(sweep_observer_1),
    {[], _} = riak_kv_sweeper:status(),
    ok.


remove_participant_persistent_test(_Config) ->
    SP = new_meck_sweep_particpant(sweep_observer_1, self()),
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
    application:set_env(riak_kv, sweeper_window, always),
    SP = new_meck_sweep_particpant(sweep_observer_1, self()),
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
    SP = new_meck_sweep_particpant(sweep_observer_1, self()),
    riak_kv_sweeper:add_sweep_participant(SP),
    riak_kv_sweeper:enable_sweep_scheduling(),
    [receive_msg({ok, successfull_sweep, sweep_observer_1, I}) || I <- Indices],
    ok.

%% TODO - Why are we not monitoring the worker process doing the fold ?
scheduler_worker_process_crashed_test(Config) ->
    Indices = ?config(vnode_indices, Config),
    meck_new_backend(self()),
    SP = new_meck_sweep_particpant(sweep_observer_1, self()),
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
    SP = new_meck_sweep_particpant(sweep_observer_1, self()),
    SP1 = SP#sweep_participant{run_interval = 1},
    meck_new_visit_function(sweep_observer_1),
    riak_kv_sweeper:add_sweep_participant(SP1),
    riak_kv_sweeper:enable_sweep_scheduling(),

    [ok = receive_msg({ok, successfull_sweep, sweep_observer_1, I}) || I <- Indices],
    %% Waiting for 2500 msecs because at least 1 second must elapse
    %% before a sweep is re-run using the run_interval (now - ts) > 1,
    %% see kv_sweeper:expired
    [ok = receive_msg({ok, successfull_sweep, sweep_observer_1, I}, 2500) || I <- Indices],

    ok.


stop_all_scheduled_sweeps_test(Config) ->
    Indices = ?config(vnode_indices, Config),
    SweepRunTimeMsecs = 1000,
    NumKeys = 10000, % sweeper worker receives messages every 1000 keys
    WaitAfterKeys = 1000,
    WaitMSecs = SweepRunTimeMsecs div  (NumKeys div WaitAfterKeys),
    {ok, SweepTick} = application:get_env(riak_kv, sweep_tick),

    meck_new_backend(self(), NumKeys),
    SP = new_meck_sweep_particpant(sweep_observer_1, self()),
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
    SP = new_meck_sweep_particpant(sweep_observer_1, self()),
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

    SP1 = new_meck_sweep_particpant(sweep_observer_1, self()),
    riak_kv_sweeper:add_sweep_participant(SP1#sweep_participant{run_interval = 1}),
    [ok = receive_msg({ok, successfull_sweep, sweep_observer_1, I}) || I <- Indices],
    [ok = receive_msg({ok, successfull_sweep, sweep_observer_1, I}, min_scheduler_response_time_msecs()) || I <- Indices],

    SP2 = new_meck_sweep_particpant(sweep_observer_2, self()),
    riak_kv_sweeper:add_sweep_participant(SP2#sweep_participant{run_interval = 1}),
    [ok = receive_msg({ok, successfull_sweep, sweep_observer_2, I}) || I <- Indices],
    [ok = receive_msg({ok, successfull_sweep, sweep_observer_2, I}, min_scheduler_response_time_msecs()) || I <- Indices],
    ok.


%% ------------------------------------------------------------------------------
%% Internal Functions
%% ------------------------------------------------------------------------------

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


all_tests() ->
    [F || {F, A} <- ?MODULE:module_info(exports), is_testcase({F, A})].


is_testcase({F, 1}) ->
    match =:= re:run(atom_to_list(F), "_test$", [{capture, none}]);
is_testcase({_, _}) ->
    false.

%% watch:remove_file_changed_action(ct).
%% watch:add_file_changed_action({ct, fun(_) -> ct:run_test({suite, riak_kv_sweeper_SUITE}) end}).
