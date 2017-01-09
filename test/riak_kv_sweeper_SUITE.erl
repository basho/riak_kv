%% -------------------------------------------------------------------
%%
%% riak_kv_sweeper_SUITE: Common tests for sweeper.
%%
%% Copyright (c) 2017 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

-module(riak_kv_sweeper_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").

%%--------------------------------------------------------------------
%% COMMON TEST CALLBACK FUNCTIONS
%%--------------------------------------------------------------------

suite() ->
    [{timetrap, {seconds, 10}}].

init_per_suite(Config) ->
    {ok, Apps} = application:ensure_all_started(exometer_core),
    %% Exometer starts lager. Set lager_console_backend to none to
    %% avoid spamming the console with logs during testing
    lager:set_loglevel(lager_console_backend, none),

    {ok, _Pid} = sweeper_meck_helper:start(),

    [{exometer_apps, Apps}|Config].

end_per_suite(Config) ->
    sweeper_meck_helper:stop(),
    [ok = application:stop(App) || App <- ?config(exometer_apps, Config)],
    ok.

init_per_testcase(_TestCase, Config) ->
    true = register(?MODULE, self()),
    application:set_env(riak_kv, sweep_participants, undefined),
    application:set_env(riak_kv, sweep_participant_testing_expire, undefined),
    application:set_env(riak_kv, sweep_window, always),
    application:set_env(riak_kv, sweeper_scheduler, false),
    application:set_env(riak_kv, sweep_tick, 10), % MSecs
    application:set_env(riak_kv, sweep_throttle, {obj_size, 0, 0}), %Disable throttling
    application:set_env(riak_kv, sweep_window_throttle_div, 1),
    application:set_env(riak_kv, sweep_concurrency, 1),
    sweeper_meck_helper:num_partitions(2),

    riak_kv_sweeper:start_link(),

    meck:reset(riak_kv_sweeper),
    meck:reset(riak_kv_index_hashtree),

    %% Delete all sweeper exometer metrics
    [catch (ets:delete_all_objects(Tab)) || Tab <- exometer_util:tables()],
    VNodeIndices = riak_core_ring:my_indices(ring),
    [{vnode_indices, VNodeIndices}|Config].

end_per_testcase(_TestCase, _Config) ->
    %% Tests are allowed to shut down the riak_kv_sweeper and start it
    %% up again
    try riak_kv_sweeper:stop() of
        _ -> ok
    catch exit:{noproc,{gen_server,call,[riak_kv_sweeper,stop]}} ->
            ignore
    end,
    ok.

groups() ->
    [].

all() ->
    [F || {F, A} <- module_info(exports),
          is_testcase({F, A})].

%%--------------------------------------------------------------------
%% TEST CASES
%%--------------------------------------------------------------------

test_initiailize_sweep_request(Config) ->
    Indices = ?config(vnode_indices, Config),

    assert_all_indices_idle(Indices),
    ok.

test_status_index_changed_sweep_request(Config) ->
    Indices = ?config(vnode_indices, Config),

    assert_all_indices_idle(Indices),
    sweeper_meck_helper:num_partitions(2 * length(Indices)),
    NewIndices = riak_core_ring:my_indices(ring),
    Index = pick(NewIndices -- Indices),
    riak_kv_sweeper:sweep(Index),
    assert_all_indices_idle(NewIndices),
    ok.

test_status_index_changed_tick(Config) ->
    Indices = ?config(vnode_indices, Config),

    assert_all_indices_idle(Indices),
    sweeper_meck_helper:num_partitions(2 * length(Indices)),
    NewIndices = riak_core_ring:my_indices(ring),
    riak_kv_sweeper:sweep_tick(),
    assert_all_indices_idle(NewIndices),
    ok.

test_add_participant(_Config) ->
    {[], _Sweeps} = riak_kv_sweeper:status(),
    new_sweep_participant(sweeper_callback_1),
    {[Participant], _} = riak_kv_sweeper:status(),
    sweeper_callback_1 = riak_kv_sweeper_fold:participant_module(Participant),
    ok.

test_add_participant_persistent(_Config) ->
    new_sweep_participant(sweeper_callback_1),

    {[Participant1], _} = riak_kv_sweeper:status(),
    sweeper_callback_1 = riak_kv_sweeper_fold:participant_module(Participant1),
    ok = riak_kv_sweeper:stop(),
    riak_kv_sweeper:start_link(),
    {[Participant2], _} = riak_kv_sweeper:status(),
    sweeper_callback_1 = riak_kv_sweeper_fold:participant_module(Participant2),
    ok.

test_remove_participant(_Config) ->
    new_sweep_participant(sweeper_callback_1),

    {[Participant], _} = riak_kv_sweeper:status(),
    sweeper_callback_1 = riak_kv_sweeper_fold:participant_module(Participant),
    riak_kv_sweeper:remove_sweep_participant(sweeper_callback_1),
    {[], _} = riak_kv_sweeper:status(),
    ok.

test_remove_participant_persistent(_Config) ->
    new_sweep_participant(sweeper_callback_1),

    {[Participant], _} = riak_kv_sweeper:status(),
    sweeper_callback_1 = riak_kv_sweeper_fold:participant_module(Participant),
    riak_kv_sweeper:remove_sweep_participant(sweeper_callback_1),
    {[], _} = riak_kv_sweeper:status(),
    riak_kv_sweeper:stop(),
    riak_kv_sweeper:start_link(),
    {[], _} = riak_kv_sweeper:status(),
    ok.

test_sweep_request(Config) ->
    Indices = ?config(vnode_indices, Config),
    new_sweep_participant(sweeper_callback_1),
    sweeper_meck_helper:backend_modules(async, _NumKeys=1000, _ObjecSizeBytes=0),

    I0 = pick(Indices),
    riak_kv_sweeper:sweep(I0),
    ok = receive_msg({ok, successful_sweep, sweeper_callback_1, I0}),
    I1 = pick(Indices),
    riak_kv_sweeper:sweep(I1),
    ok = receive_msg({ok, successful_sweep, sweeper_callback_1, I1}).

test_sweep_request_non_existing_index(Config) ->
    Indices = ?config(vnode_indices, Config),
    new_sweep_participant(sweeper_callback_1),
    sweeper_meck_helper:backend_modules(async, _NumKeys=1000, _ObjecSizeBytes=0),

    Max = lists:max(Indices),
    NonExisting = Max * Max,
    false = lists:member(NonExisting, Indices),
    riak_kv_sweeper:sweep(NonExisting),
    ok.

test_scheduler_sync_backend(Config) ->
    Indices = ?config(vnode_indices, Config),
    new_sweep_participant(sweeper_callback_1),
    sweeper_meck_helper:backend_modules(sync, _NumKeys=1000, _ObjecSizeBytes=100),

    sweep_all_indices(Indices),
    ok.

test_scheduler(Config) ->
    Indices = ?config(vnode_indices, Config),
    new_sweep_participant(sweeper_callback_1),
    sweeper_meck_helper:backend_modules(async, _NumKeys=1000, _ObjecSizeBytes=0),

    sweep_all_indices(Indices),
    ok.

test_scheduler_worker_process_crashed(Config) ->
    Indices = ?config(vnode_indices, Config),
    new_sweep_participant(sweeper_callback_crash),
    sweeper_meck_helper:backend_modules(async, _NumKeys=5000, _ObjecSizeBytes=0),

    riak_kv_sweeper:enable_sweep_scheduling(),
    tick_per_index(Indices),
    [receive_msg({ok, failed_sweep, sweeper_callback_crash, I}) || I <- Indices],
    ok = check_sweeps_idle(Indices),
    ok.

test_scheduler_run_interval(Config) ->
    Indices = ?config(vnode_indices, Config),
    new_sweep_participant(sweeper_callback_1, 1),
    sweeper_meck_helper:backend_modules(async, _NumKeys=1000, _ObjecSizeBytes=0),

    sweep_all_indices(Indices),
    timer:sleep(min_scheduler_response_time_msecs()),
    sweep_all_indices(Indices),
    ok.

test_scheduler_remove_participant(Config) ->
    Indices = ?config(vnode_indices, Config),
    new_sweep_participant(sweeper_callback_wait),
    sweeper_meck_helper:backend_modules(async, _NumKeys=5000, _ObjecSizeBytes=0),

    WaitIndex = pick(Indices),
    riak_kv_sweeper:enable_sweep_scheduling(),
    tick_per_index(Indices),
    sweeper_callback_wait:before_visit([WaitIndex],
                                       fun(Index) when Index == WaitIndex ->
                                               riak_kv_sweeper:remove_sweep_participant(sweeper_callback_wait),
                                               ok;
                                          (_Other) ->
                                               ok
                                       end),
    ok = receive_msg({ok, failed_sweep, sweeper_callback_wait, WaitIndex}).

test_scheduler_queue(Config) ->
    Indices = ?config(vnode_indices, Config),
    new_sweep_participant(sweeper_callback_wait),
    sweeper_meck_helper:backend_modules(async, _NumKeys=5000, _ObjecSizeBytes=0),

    WaitIndex = pick(Indices),
    WaitIndex1 = pick(Indices -- [WaitIndex]),
    riak_kv_sweeper:enable_sweep_scheduling(),
    %% Lets sweeps run once so they are not in never run state
    tick_per_index(Indices),
    sweeper_callback_wait:before_visit(Indices),
    [ok = receive_msg({ok, successful_sweep, sweeper_callback_wait, I}) || I <- Indices],
    application:set_env(riak_kv, sweep_concurrency, 1),
    riak_kv_sweeper:sweep(WaitIndex),
    sweeper_callback_wait:before_visit([WaitIndex],
                                       fun(_Index) ->
                                               riak_kv_sweeper:sweep(WaitIndex1),
                                               Sweep = get_sweep_on_index(WaitIndex1),
                                               {_, _, _} = riak_kv_sweeper_state:sweep_queue_time(Sweep)
                                       end),

    ok = receive_msg({ok, successful_sweep, sweeper_callback_wait, WaitIndex}),
    tick_per_index(Indices),
    sweeper_callback_wait:before_visit([WaitIndex1]),
    ok = receive_msg({ok, successful_sweep, sweeper_callback_wait, WaitIndex1}),
    ok.

test_scheduler_sweep_window_never(Config) ->
    Indices = ?config(vnode_indices, Config),
    new_sweep_participant(sweeper_callback_1),
    sweeper_meck_helper:backend_modules(async, _NumKeys=5000, _ObjecSizeBytes=0),

    application:set_env(riak_kv, sweep_window, never),
    riak_kv_sweeper:enable_sweep_scheduling(),
    StatusBefore = riak_kv_sweeper:status(),
    riak_kv_sweeper:sweep_tick(),
    StatusAfter = riak_kv_sweeper:status(),
    StatusAfter = StatusBefore,
    application:set_env(riak_kv, sweep_window, always),
    sweep_all_indices(Indices),
    ok.

test_scheduler_now_outside_sleep_window(Config) ->
    Indices = ?config(vnode_indices, Config),
    new_sweep_participant(sweeper_callback_1),
    sweeper_meck_helper:backend_modules(async, _NumKeys=5000, _ObjecSizeBytes=0),

    {_, {Hour, _, _}} = calendar:local_time(),
    Start = add_hours(Hour, 2),
    End = add_hours(Start, 1),
    application:set_env(riak_kv, sweep_window, {Start, End}),
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
test_stop_all_scheduled_sweeps(Config) ->
    Indices = ?config(vnode_indices, Config),
    new_sweep_participant(sweeper_callback_wait),
    sweeper_meck_helper:backend_modules(async, _NumKeys=5000, _ObjecSizeBytes=0),

    application:set_env(riak_kv, sweep_concurrency, length(Indices)),
    riak_kv_sweeper:enable_sweep_scheduling(),
    %% Lets sweeps run once so they are not in never run state
    tick_per_index(Indices),
    sweeper_callback_wait:before_visit(Indices),
    [ok = receive_msg({ok, successful_sweep, sweeper_callback_wait, I}) || I <- Indices],
    riak_kv_sweeper:sweep_tick(),
    {_, Sweeps} = riak_kv_sweeper:status(),
    sweeper_callback_wait:before_visit(any, fun(_) ->
                                                    Running = riak_kv_sweeper:stop_all_sweeps(),
                                                    true = Running >= 1
                                            end),
    Running = [riak_kv_sweeper_state:sweep_index(Sweep) ||
                  Sweep <- Sweeps,
                  riak_kv_sweeper_state:sweep_state(Sweep) == running],
    [ok = receive_msg({ok, failed_sweep, sweeper_callback_wait, I}) || I <- Running],
    ok = check_sweeps_idle(Indices),
    ok.

test_stop_all_scheduled_sweeps_race_condition(_Config) ->
    new_sweep_participant(sweeper_callback_1),
    sweeper_meck_helper:backend_modules(async, 5000, 0),

    riak_kv_sweeper:enable_sweep_scheduling(),
    Running = riak_kv_sweeper:stop_all_sweeps(),
    Running = 0,
    ok.

test_scheduler_add_participant(Config) ->
    Indices = ?config(vnode_indices, Config),
    new_sweep_participant(sweeper_callback_1),
    sweeper_meck_helper:backend_modules(async, _NumKeys=1000, _ObjecSizeBytes=0),

    riak_kv_sweeper:enable_sweep_scheduling(),
    sweep_all_indices(Indices, ok, sweeper_callback_1),
    sweep_all_indices(Indices, ok, sweeper_callback_1),
    new_sweep_participant(sweeper_callback_2),
    sweep_all_indices(Indices, ok, sweeper_callback_1),
    sweep_all_indices(Indices, ok, sweeper_callback_2),
    ok.

test_scheduler_restart_sweep(Config) ->
    Indices = ?config(vnode_indices, Config),
    new_sweep_participant(sweeper_callback_wait),
    sweeper_meck_helper:backend_modules(async, _NumKeys=5000, _ObjecSizeBytes=0),

    WaitIndex = pick(Indices),
    riak_kv_sweeper:enable_sweep_scheduling(),
    tick_per_index(Indices),
    sweeper_callback_wait:before_visit([WaitIndex],
                                       fun(Index) when Index == WaitIndex ->
                                               RunningSweep = get_sweep_on_index(WaitIndex),
                                               running = riak_kv_sweeper_state:sweep_state(RunningSweep),
                                               riak_kv_sweeper:sweep(WaitIndex),
                                               RestartingSweep = get_sweep_on_index(WaitIndex),
                                               restart = riak_kv_sweeper_state:sweep_state(RestartingSweep),
                                               undefined = riak_kv_sweeper_state:sweep_queue_time(RestartingSweep);
                                          (_Index) ->
                                               ok
                                       end),
    ok = receive_msg({ok, failed_sweep, sweeper_callback_wait, WaitIndex}),
    tick_per_index(Indices),
    sweeper_callback_wait:before_visit([WaitIndex]),
    ok = receive_msg({ok, successful_sweep, sweeper_callback_wait, WaitIndex}).

test_scheduler_estimated_keys_lock_ok(Config) ->
    Indices = ?config(vnode_indices, Config),
    new_sweep_participant(sweeper_callback_1),
    sweeper_meck_helper:backend_modules(async, _NumKeys=5000, _ObjecSizeBytes=0),
    sweeper_meck_helper:aae_modules(_AAEnabled = true, _EstimatedKeys = 4200, _LockResult = ok),

    riak_kv_sweeper:enable_sweep_scheduling(),
    sweep_all_indices(Indices),
    [true = meck:called(riak_kv_index_hashtree, get_lock, [I, estimate]) || I <- Indices],
    [true = meck:called(riak_kv_index_hashtree, estimate_keys, [I]) || I <- Indices],
    [true = meck:called(riak_kv_index_hashtree, release_lock, [I]) || I <- Indices],
    ok.

test_scheduler_estimated_keys_lock_fail(Config) ->
    Indices = ?config(vnode_indices, Config),
    new_sweep_participant(sweeper_callback_1),
    sweeper_meck_helper:backend_modules(async, _NumKeys=5000, _ObjecSizeBytes=0),
    sweeper_meck_helper:aae_modules(_AAEnabled = true, _EstimatedKeys = 4200, _LockResult = fail),

    sweep_all_indices(Indices),
    [true = meck:called(riak_kv_index_hashtree, get_lock, [I, estimate]) || I <- Indices],
    [false = meck:called(riak_kv_index_hashtree, estimate_keys, [I]) || I <- Indices],
    [false = meck:called(riak_kv_index_hashtree, release_lock, [I]) || I <- Indices],
    ok.

%% throttling on object size with no mutated keys
test_sweep_throttle_obj_size1(Config) ->
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
test_sweep_throttle_obj_size2(Config) ->
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
test_sweep_throttle_obj_size3(Config) ->
    Indices = ?config(vnode_indices, Config),
    sweep_throttle_obj_size(
      _Index              = pick(Indices),
      _NumKeys            = 1000,
      _NumMutatedKeys     = 100,
      _ObjSizeBytes       = 100,
      _ThrottleAfterBytes = 500,
      _ThrottleWaitMsecs  = 1).

%% throttling on obj_size using a sweep_window_throttle_div
test_sweep_throttle_obj_size4(Config) ->
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
test_sweep_throttle_obj_size5(Config) ->
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
    RiakObjSizeBytes = byte_size(riak_object_bin(<<>>, <<>>, ObjSizeBytes)),
    new_sweep_participant(sweeper_callback_mutate),
    sweeper_meck_helper:backend_modules(async, NumKeys, RiakObjSizeBytes),
    meck_sleep_for_throttle(),
    application:set_env(riak_kv, sweep_throttle, {obj_size, ThrottleAfterBytes, ThrottleWaitMsecs}),

    ExpectedThrottleMsecs = expected_obj_size_throttle_total_msecs(
                              NumKeys, NumMutatedKeys, RiakObjSizeBytes, ThrottleAfterBytes, ThrottleWaitMsecs),
    riak_kv_sweeper:sweep(Index),
    receive
        {From, wait_for_count} ->
            From ! {mutate, NumMutatedKeys}
    end,
    ok = receive_msg({ok, successful_sweep, sweeper_callback_mutate, Index}),
    ActualThrottleTotalWait = meck_count_msecs_slept(),
    ActualThrottleTotalWait = ExpectedThrottleMsecs.

%% throttling on pace every 100 keys
test_sweep_throttle_pace1(Config) ->
    Indices = ?config(vnode_indices, Config),
    sweep_throttle_pace(
      _Index              = pick(Indices),
      _NumKeys            = 1000,
      _NumMutatedKeys     = 0,
      _NumKeysPace        = 100,
      _ThrottleWaitMsecs  = 1).

%% throttling on pace every 100 keys with one extra throttle because
%% of mutated objects
test_sweep_throttle_pace2(Config) ->
    Indices = ?config(vnode_indices, Config),
    sweep_throttle_pace(
      _Index              = pick(Indices),
      _NumKeys            = 1000,
      _NumMutatedKeys     = 100,
      _NumKeysPace        = 100,
      _ThrottleWaitMsecs  = 1).

%% throttling on pace with several extra throttles because of
%% mutations
test_sweep_throttle_pace3(Config) ->
    Indices = ?config(vnode_indices, Config),
    sweep_throttle_pace(
      _Index              = pick(Indices),
      _NumKeys            = 5000,
      _NumMutatedKeys     = 2000,
      _NumKeysPace        = 100,
      _ThrottleWaitMsecs  = 1).

%% throttling on pace using sweep_window_throttle_div
test_sweep_throttle_pace4(Config) ->
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
test_sweep_throttle_pace5(Config) ->
    Indices = ?config(vnode_indices, Config),
    application:set_env(riak_kv, sweep_window_throttle_div, 2),
    sweep_throttle_pace(
      _Index              = pick(Indices),
      _NumKeys            = 1000,
      _NumMutatedKeys     = 1000,
      _NumKeysPace        = 100,
      _ThrottleWaitMsecs  = 1).

sweep_throttle_pace(Index, NumKeys, NumMutatedKeys, NumKeysPace, ThrottleWaitMsecs) ->
    new_sweep_participant(sweeper_callback_mutate),
    sweeper_meck_helper:backend_modules(async, NumKeys, 0),
    meck_sleep_for_throttle(),
    application:set_env(riak_kv, sweep_throttle, {pace, NumKeysPace, ThrottleWaitMsecs}),

    ExpectedThrottleMsecs = expected_pace_throttle_total_msecs(
                              NumKeys, NumMutatedKeys, NumKeysPace, ThrottleWaitMsecs),
    riak_kv_sweeper:sweep(Index),
    receive
        {From, wait_for_count} ->
            From ! {mutate, NumMutatedKeys}
    end,
    ok = receive_msg({ok, successful_sweep, sweeper_callback_mutate, Index}),

    ActualThrottleTotalWait = meck_count_msecs_slept(),
    ActualThrottleTotalWait = ExpectedThrottleMsecs.

test_sweeper_exometer_reporting(Config) ->
    Indices = ?config(vnode_indices, Config),
    NumKeys = 5042,
    ObjSizeBytes = 100,
    RiakObjSizeBytes = byte_size(riak_object_bin(<<>>, <<>>, ObjSizeBytes)),
    new_sweep_participant(sweeper_callback_1),
    sweeper_meck_helper:backend_modules(async, NumKeys, ObjSizeBytes),

    Index = pick(Indices),
    {error, not_found} = exometer:get_value([riak, riak_kv, sweeper, Index, keys]),
    riak_kv_sweeper:sweep(Index),
    ok = receive_msg({ok, successful_sweep, sweeper_callback_1, Index}),

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
    ok = receive_msg({ok, successful_sweep, sweeper_callback_1, Index}),
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

test_sweeper_exometer_successful_sweep(Config) ->
    Indices = ?config(vnode_indices, Config),
    sweeper_meck_helper:backend_modules(async, _NumKeys=5000, 0),
    new_sweep_participant(sweeper_callback_1),

    I0 = pick(Indices),
    sweep_all_indices(Indices),
    {error, not_found} =
        match_retry(fun() ->
                            exometer:get_value([riak, riak_kv, sweeper, I0,
                                                failed, sweeper_callback_1])
                    end,
                    {error, not_found}),
    {ok, [{count, 1}, {one, 1}]} =
        match_retry(fun() ->
                            exometer:get_value([riak, riak_kv, sweeper, I0,
                                                successful, sweeper_callback_1])
                    end,  {ok, [{count, 1}, {one, 1}]}),
    riak_kv_sweeper:sweep(I0),
    {error, not_found} =
        match_retry(fun() ->
                            exometer:get_value([riak, riak_kv, sweeper, I0,
                                                failed, sweeper_callback_1])
                    end,
                    {error, not_found}),
    {ok, [{count, 2}, {one, 2}]} =
        match_retry(fun() ->
                            exometer:get_value([riak, riak_kv, sweeper, I0,
                                                successful, sweeper_callback_1])
                    end,  {ok, [{count, 2}, {one, 2}]}),
    ok.

test_sweeper_exometer_failed_sweep(Config) ->
    Indices = ?config(vnode_indices, Config),
    new_sweep_participant(sweeper_callback_crash),
    sweeper_meck_helper:backend_modules(async,_NumKeys= 5000, 0),

    I0 = pick(Indices),
    riak_kv_sweeper:enable_sweep_scheduling(),
    tick_per_index(Indices),
    [ok = receive_msg({ok, failed_sweep, sweeper_callback_crash, I}) || I <- Indices],

    {error, not_found} =
        match_retry(fun() ->
                            exometer:get_value([riak, riak_kv, sweeper, I0,
                                                successful, sweeper_callback_crash])
                    end,
                    {error, not_found}),
    {ok, [{count, 1}, {one, 1}]} =
        match_retry(fun() ->
                            exometer:get_value([riak, riak_kv, sweeper, I0,
                                                failed, sweeper_callback_crash])
                    end,  {ok, [{count, 1}, {one, 1}]}),

    riak_kv_sweeper:sweep(I0),

    {error, not_found} =
        match_retry(fun() ->
                            exometer:get_value([riak, riak_kv, sweeper, I0,
                                                successful, sweeper_callback_crash])
                    end,
                    {error, not_found}),
    {ok, [{count, 2}, {one, 2}]} =
        match_retry(fun() ->
                            exometer:get_value([riak, riak_kv, sweeper, I0,
                                                failed, sweeper_callback_crash])
                    end,  {ok, [{count, 2}, {one, 2}]}),
    ok.

test_exometer_mutated_object_count(Config) ->
    Indices = ?config(vnode_indices, Config),
    new_sweep_participant(sweeper_callback_mutate),
    sweeper_meck_helper:backend_modules(async, _NumKeys = 5042, 0),

    I0 = pick(Indices),
    riak_kv_sweeper:sweep(I0),
    MutateKeys = 100,
    receive
        {From, wait_for_count} ->
            From ! {mutate, MutateKeys}
    end,
    ok = receive_msg({ok, successful_sweep, sweeper_callback_mutate, I0}),
    {ok, [{count, MutateKeys},{one, MutateKeys}]} =
        match_retry(fun() -> exometer:get_value([riak, riak_kv, sweeper, I0, mutated]) end,
                    {ok, [{count, MutateKeys}, {one, MutateKeys}]}),
    ok.

test_exometer_deleted_object_count(Config) ->
    Indices = ?config(vnode_indices, Config),
    new_sweep_participant(sweeper_callback_delete),
    sweeper_meck_helper:backend_modules(async, _NumKeys = 5042, 0),

    I0 = pick(Indices),
    DeleteKeys = 100,
    riak_kv_sweeper:sweep(I0),
    receive
        {From, wait_for_count} ->
            From ! {delete, DeleteKeys}
    end,

    ok = receive_msg({ok, successful_sweep, sweeper_callback_delete, I0}),
    {ok, [{count, DeleteKeys},{one, DeleteKeys}]} =
        match_retry(fun() -> exometer:get_value([riak, riak_kv, sweeper, I0, deleted]) end,
                    {ok, [{count, DeleteKeys}, {one, DeleteKeys}]}),
    ok.


test_scheduler_sweep_concurrency(_Config) ->
    new_sweep_participant(sweeper_callback_wait),
    sweeper_meck_helper:backend_modules(async, _NumKeys=5000, 0),

    ConcurrentSweeps = 4,
    RingSize = 8,
    true = RingSize > ConcurrentSweeps,
    sweeper_meck_helper:num_partitions(RingSize),
    application:set_env(riak_kv, sweep_concurrency, ConcurrentSweeps),
    riak_kv_sweeper:enable_sweep_scheduling(),
    Indices = riak_core_ring:my_indices(ring),

    [ spawn_link(riak_kv_sweeper, sweep_tick, [])
      || _ <- Indices],
    Running0 = check_sweeps_running(ConcurrentSweeps),
    sweeper_callback_wait:before_visit(Running0),
    ok = check_sweeps_idle(Indices),

    [ spawn_link(riak_kv_sweeper, sweep_tick, [])
      || _ <- Indices],
    Running1 = check_sweeps_running(ConcurrentSweeps),
    sweeper_callback_wait:before_visit(Running1),
    ok = check_sweeps_idle(Indices),
    Indices = lists:sort(Running0 ++ Running1),
    ok.

%% ------------------------------------------------------------------------------
%% Internal Functions
%% ------------------------------------------------------------------------------

is_running_sweep(Sweep) ->
    State = riak_kv_sweeper_state:sweep_state(Sweep),
    Pid = riak_kv_sweeper_state:sweep_pid(Sweep),
    State == running andalso
        process_info(Pid) =/= undefined.

new_sweep_participant(Name) ->
    application:set_env(riak_kv, sweep_participant_testing_expire, immediate),
    new_sweep_participant(Name, 1).

new_sweep_participant(Name, RunInterval) ->
    riak_kv_sweeper:add_sweep_participant(
      _Description = atom_to_list(Name) ++ " sweep participant",
      _Module = Name,
      _FunType = observe_fun,
      RunInterval).

meck_sleep_for_throttle() ->
    meck:expect(riak_kv_sweeper, sleep_for_throttle, fun(_) -> ok end).

meck_count_msecs_slept() ->
    ThrottleHistory = meck:history(riak_kv_sweeper),
    MSecsThrottled = [MSecs || {_Pid, {riak_kv_sweeper, sleep_for_throttle, [MSecs]}, ok} <-
                                   ThrottleHistory],
    lists:sum(MSecsThrottled).

check_sweeps_idle(Indices) ->
    Sorted = lists:sort(Indices),
    Sorted =
        match_retry(fun() ->
                            {_SPs, Sweeps} = riak_kv_sweeper:status(),
                            lists:sort([riak_kv_sweeper_state:sweep_index(Sweep) ||
                                           Sweep <- Sweeps,
                                           riak_kv_sweeper_state:sweep_state(Sweep) == idle])
                    end,
                    Sorted),
    ok.

check_sweeps_running(ConcurrentSweeps) ->
    ConcurrentSweeps =
        match_retry(fun() ->
                            {_SPs, Sweeps} = riak_kv_sweeper:status(),
                            riak_core_util:count(fun is_running_sweep/1, Sweeps)
                    end,
                    ConcurrentSweeps),
    {_SPs, Sweeps} = riak_kv_sweeper:status(),
    [riak_kv_sweeper_state:sweep_index(Sweep) ||
        Sweep <- Sweeps,
        riak_kv_sweeper_state:sweep_state(Sweep) == running].

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
    2000.

pick(List) when length(List) > 0 ->
    N = random:uniform(length(List)),
    lists:nth(N, List).

sweep_all_indices(Indices) ->
    sweep_all_indices(Indices, ok).

sweep_all_indices(Indices, ExpectedResult) ->
    sweep_all_indices(Indices, ExpectedResult, sweeper_callback_1).

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
    ok = match_retry(
           fun() ->
                   receive Msg0 when Msg0 == Msg ->
                           ok
                   after 0 ->
                           not_ok
                   end
           end, ok).

get_sweep_on_index(Index) ->
    {_, Sweeps} = riak_kv_sweeper:status(),
    IndexedSweeps = [{riak_kv_sweeper_state:sweep_index(S), S} || S <- Sweeps],
    {Index, Sweep} = lists:keyfind(Index, 1, IndexedSweeps),
    Sweep.

is_testcase({F, 1}) ->
    match =:= re:run(atom_to_list(F), "^test_", [{capture, none}]);
is_testcase({_, _}) ->
    false.

match_retry(Fun, Result) ->
    match_retry(_Max = 100,  _N = 1, Fun, Result).

match_retry(Max, N, _Fun, max_retries) when N > Max ->
    throw(max_retries);

match_retry(Max, N, _Fun, _Result) when N > Max ->
    max_retries;

match_retry(Max, N, Fun, Result) ->
    case Fun() of
        Result ->
            Result;
        _ ->
            timer:sleep(100),
            match_retry(Max, N+1, Fun, Result)
    end.
