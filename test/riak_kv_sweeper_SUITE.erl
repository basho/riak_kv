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
    meck:reset(riak_core_bucket),

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

%%
%% Assert that riak_kv_sweeper initializes with a #sweep{} entry for
%% all partition indices owned by the node.
%%
test_initiailize_sweep_request(Config) ->
    Indices = ?config(vnode_indices, Config),

    assert_all_indices_idle(Indices),
    ok.

%%
%% Assert that a sweep request, riak_kv_sweeper:sweep(...), has the
%% effect of updating it's the state when partition indices ownded
%% by the node change.
%%
test_status_index_changed_sweep_request(Config) ->
    Indices = ?config(vnode_indices, Config),

    assert_all_indices_idle(Indices),
    sweeper_meck_helper:num_partitions(2 * length(Indices)),
    NewIndices = riak_core_ring:my_indices(ring),
    Index = pick(NewIndices -- Indices),
    riak_kv_sweeper:sweep(Index),
    assert_all_indices_idle(NewIndices),
    ok.

%%
%% Assert that scheduling of a sweep, has the effect of updating it's
%% the state when partition indices ownded by the node change.
%%
test_status_index_changed_tick(Config) ->
    Indices = ?config(vnode_indices, Config),

    assert_all_indices_idle(Indices),
    sweeper_meck_helper:num_partitions(2 * length(Indices)),
    NewIndices = riak_core_ring:my_indices(ring),
    riak_kv_sweeper:sweep_tick(),
    assert_all_indices_idle(NewIndices),
    ok.

%%
%% Assert adding a new sweep participant to the riak_kv_sweeper,
%% includes in the sweeper state.
%%
test_add_participant(_Config) ->
    {[], _Sweeps} = riak_kv_sweeper:status(),
    new_sweep_participant(sweeper_callback_1),
    {[Participant], _} = riak_kv_sweeper:status(),
    sweeper_callback_1 = riak_kv_sweeper_fold:participant_module(Participant),
    ok.

%%
%% Assert sweep participants persist across riak_kv_sweeper
%% restarts.
%%
test_add_participant_persistent(_Config) ->
    new_sweep_participant(sweeper_callback_1),

    {[Participant1], _} = riak_kv_sweeper:status(),
    sweeper_callback_1 = riak_kv_sweeper_fold:participant_module(Participant1),
    ok = riak_kv_sweeper:stop(),
    riak_kv_sweeper:start_link(),
    {[Participant2], _} = riak_kv_sweeper:status(),
    sweeper_callback_1 = riak_kv_sweeper_fold:participant_module(Participant2),
    ok.

%%
%% Assert removing a sweep participant removes it from
%% riak_kv_sweeper state.
%%
test_remove_participant(_Config) ->
    new_sweep_participant(sweeper_callback_1),

    {[Participant], _} = riak_kv_sweeper:status(),
    sweeper_callback_1 = riak_kv_sweeper_fold:participant_module(Participant),
    riak_kv_sweeper:remove_sweep_participant(sweeper_callback_1),
    {[], _} = riak_kv_sweeper:status(),
    ok.

%%
%% Assert that removing a sweep participant from riak_kv_sweeper removes it
%% from persistent storage for sweep participants and participant
%% removed do not re-appear on sweeper restart.
%%
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

%%
%% riak_kv_kv_sweeper:sweep(...)  sweeps an index.
%%
%% The test uses sweep participant sweeper_callback_1 that completes
%% successfully as soon as it is scheduled.
%%
%% The test asserts receipt of successful_sweep messages for each
%% swept index.
%%
test_sweep_request(Config) ->
    Indices = ?config(vnode_indices, Config),
    new_sweep_participant(sweeper_callback_1),
    sweeper_meck_helper:backend_modules(async, _NumKeys=1000, _ObjectSizeBytes=0),

    I0 = pick(Indices),
    riak_kv_sweeper:sweep(I0),
    ok = receive_msg({ok, successful_sweep, sweeper_callback_1, I0}),
    I1 = pick(Indices),
    riak_kv_sweeper:sweep(I1),
    ok = receive_msg({ok, successful_sweep, sweeper_callback_1, I1}).

%%
%% riak_kv_sweeper:sweep(...) on an invalid index returns an error.
%%
%% The test uses sweep participant sweeper_callback_1 that completes
%% successfully as soon as it is scheduled.
%%
%% Test that asserts that a sweep request for a non-existing index
%% returns an error and ignores the request. Since the
%% riak_kv_sweeper:sweep(...) may be called manually to start a sweep,
%% so we don't want an invalid argument to the function to terminate
%% the riak_kv_sweeper process.
%%
test_sweep_request_non_existing_index(Config) ->
    Indices = ?config(vnode_indices, Config),
    new_sweep_participant(sweeper_callback_1),
    sweeper_meck_helper:backend_modules(async, _NumKeys=1000, _ObjectSizeBytes=0),

    Max = lists:max(Indices),
    NonExisting = Max * Max,
    false = lists:member(NonExisting, Indices),
    {error, sweep_request_failed} = riak_kv_sweeper:sweep(NonExisting),
    ok.

%%
%% Test the sync behaviour of the backend, i.e., when it fold's keys
%% synchronously.
%%
%% The test uses sweep participant sweeper_callback_1 that completes
%% successfully as soon as it is scheduled and a mocked backend with
%% sync behaviour.
%%
%% The test asserts receipt of successful_sweep messages for each
%% swept index.
%%
test_scheduler_sync_backend(Config) ->
    Indices = ?config(vnode_indices, Config),
    new_sweep_participant(sweeper_callback_1),
    sweeper_meck_helper:backend_modules(sync, _NumKeys=1000, _ObjectSizeBytes=100),

    sweep_all_indices(Indices),
    ok.

%%
%% Test the async behaviour of the backend, i.e., when it returns a
%% Work which is run in the using a process from the vnode worker from
%% the pool.
%%
%% The test uses sweep participant sweeper_callback_1 that completes
%% successfully as soon as it is scheduled with and a mocked backend
%% with async behaviour.
%%
%% The test asserts receipt of successful_sweep messages for each
%% swept index.
%%
test_scheduler_async_backend(Config) ->
    Indices = ?config(vnode_indices, Config),
    new_sweep_participant(sweeper_callback_1),
    sweeper_meck_helper:backend_modules(async, _NumKeys=1000, _ObjectSizeBytes=100),

    sweep_all_indices(Indices),
    ok.

%%
%% Test riak_kv_sweeper's scheduling of sweeps.
%%
%% The test uses sweep participant sweeper_callback_1 that completes
%% successfully as soon as it is scheduled.
%%
%% The test asserts receipt of successful_sweep messages for each
%% swept index after sending sweep_tick's to schedule the sweeps on all
%% indices.
%%
test_scheduler(Config) ->
    Indices = ?config(vnode_indices, Config),
    new_sweep_participant(sweeper_callback_1),
    sweeper_meck_helper:backend_modules(async, _NumKeys=1000, _ObjectSizeBytes=0),

    sweep_all_indices(Indices),
    ok.

%%
%% Test scheduling of failing sweeps.
%%
%% The test uses sweep participant sweeper_callback_crash that fails
%% by throwing a crash as soon as it is scheduled.
%%
%% The test asserts that the sweep has failed by receipt of a
%% failed_sweep message. Test also asserts that the state all sweeps
%% returns to idle.
%%
test_scheduler_worker_process_crashed(Config) ->
    Indices = ?config(vnode_indices, Config),
    new_sweep_participant(sweeper_callback_crash),
    sweeper_meck_helper:backend_modules(async, _NumKeys=5000, _ObjectSizeBytes=0),

    riak_kv_sweeper:enable_sweep_scheduling(),
    tick_per_index(Indices),
    [receive_msg({ok, failed_sweep, sweeper_callback_crash, I}) || I <- Indices],
    ok = check_sweeps_idle(Indices),
    ok.

%%
%% Test riak_kv_sweeper's re-scheduling of sweeps, configured by the
%% run_interval property. run_interval is an integer.
%%
%% The test uses sweep participant sweeper_callback_1 that completes
%% successfully as soon as it is scheduled.
%%
%% The test asserts receipt of successful_sweep messages after sending
%% sweep_ticks and then waiting run_interval+1 seconds, and then
%% re-sending sweep_tick's.
%%
test_scheduler_run_interval(Config) ->
    Indices = ?config(vnode_indices, Config),
    RunInterval = 1,
    new_sweep_participant(sweeper_callback_1, RunInterval),
    sweeper_meck_helper:backend_modules(async, _NumKeys=1000, _ObjectSizeBytes=0),

    sweep_all_indices(Indices),
    sweeper_meck_helper:advance_time({seconds, RunInterval+1}),
    sweep_all_indices(Indices),
    ok.

%%
%% Test riak_kv_sweeper's re-scheduling of sweeps, configured by the
%% run_interval property. run_interval is a fun.
%%
%% The test uses sweep participant sweeper_callback_1 that completes
%% successfully as soon as it is scheduled.
%%
%% The test asserts receipt of successful_sweep messages after sending
%% sweep_ticks and then waiting run_interval+1 seconds, and then
%% re-sending sweep_tick's.
%%
test_scheduler_run_interval_as_fun(Config) ->
    Indices = ?config(vnode_indices, Config),
    RunIntervalFun = fun() -> 1 end,
    new_sweep_participant(sweeper_callback_1, RunIntervalFun),
    sweeper_meck_helper:backend_modules(async, _NumKeys=1000, _ObjectSizeBytes=0),

    sweep_all_indices(Indices),
    sweeper_meck_helper:advance_time({seconds, RunIntervalFun()+1}),
    sweep_all_indices(Indices),
    ok.

%%
%% Test removing a sweep participant during a sweep.
%%
%% The test uses sweep participant sweeper_callback_wait that calls
%% back a Fun before visiting an keys. The backend used must have >
%% 1000 keys, here 5000, because messages, e.g, stop, sent to the
%% process running a sweep are only received once every 1000 keys.
%%
%% The test cases uses the sweeper_callback_wait:before_visit to
%% remove the participant while it is active in a sweep and asserts
%% that we receive a failed_sweep message.
%%
test_scheduler_remove_participant(Config) ->
    Indices = ?config(vnode_indices, Config),
    new_sweep_participant(sweeper_callback_wait),
    sweeper_meck_helper:backend_modules(async, _NumKeys=5000, _ObjectSizeBytes=0),

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

%%
%% Test that riak_kv_sweeper:sweep(...) queues requests if the
%% sweep_concurrency limit has been reached.
%%
%% The test uses sweep participant sweeper_callback_wait that calls
%% back a Fun before visiting an keys.
%%
%% The test cases uses the sweeper_callback_wait:before_visit to make
%% a sweep request on an idle index when we have reached the max
%% concurrency limit, and asserts that the requested sweep is queued.
%%
test_scheduler_queue(Config) ->
    Indices = ?config(vnode_indices, Config),
    new_sweep_participant(sweeper_callback_wait),
    sweeper_meck_helper:backend_modules(async, _NumKeys=1000, _ObjectSizeBytes=0),

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
                                               Sweep0 = get_sweep_on_index(WaitIndex1),
                                               undefined = riak_kv_sweeper_state:sweep_queue_time(Sweep0),
                                               riak_kv_sweeper:sweep(WaitIndex1),
                                               Sweep = get_sweep_on_index(WaitIndex1),
                                               {_, _, _} = riak_kv_sweeper_state:sweep_queue_time(Sweep)
                                       end),

    ok = receive_msg({ok, successful_sweep, sweeper_callback_wait, WaitIndex}),
    tick_per_index(Indices),
    sweeper_callback_wait:before_visit([WaitIndex1]),
    ok = receive_msg({ok, successful_sweep, sweeper_callback_wait, WaitIndex1}),
    ok.

%%
%% Test that sweep_window == never disables scheduling of sweeps by
%% the riak_kv_sweeper.
%%
%% The test uses sweep participant sweeper_callback_1 that completes
%% successfully as soon as it is scheduled.
%%
%% This tests asserts that the state of the riak_kv_sweeper does not
%% change even after a sweep_tick, when sweep_window is never. A sweep
%% on an index, caused by a sweep_tick, changes the state. Note that
%% we use the sync backend here to avoid race conditions; the sync
%% behaviour ensures that the sweep on the index complets when the
%% sweep_tick function returns.
%%
test_scheduler_sweep_window_never(Config) ->
    Indices = ?config(vnode_indices, Config),
    new_sweep_participant(sweeper_callback_1),
    sweeper_meck_helper:backend_modules(sync, _NumKeys=5000, _ObjectSizeBytes=0),

    application:set_env(riak_kv, sweep_window, never),
    riak_kv_sweeper:enable_sweep_scheduling(),
    StatusBefore = riak_kv_sweeper:status(),
    riak_kv_sweeper:sweep_tick(),
    StatusAfter = riak_kv_sweeper:status(),
    StatusAfter = StatusBefore,
    application:set_env(riak_kv, sweep_window, always),
    sweep_all_indices(Indices),
    ok.

%%
%% Test riak_kv_sweeper scheduling when the current time is outside
%% the sweep window.
%%
%% The test uses sweep participant sweeper_callback_1 that completes
%% successfully as soon as it is scheduled.
%%
%% When a sweep_window described by a {start, end} hour is set then
%% sweep tick events scheduled outside this period will not schedule a
%% sweep. This tests asserts that the state of the riak_kv_sweeper
%% does not change even after a sweep_tick. A sweep on an index,
%% caused by a sweep_tick, changes the state. Note that we use the
%% sync backend here to avoid race conditions; the sync behaviour
%% ensures that the sweep on the index complets when the sweep_tick
%% function returns.
test_scheduler_now_outside_sleep_window(Config) ->
    Indices = ?config(vnode_indices, Config),
    new_sweep_participant(sweeper_callback_1),
    sweeper_meck_helper:backend_modules(sync, _NumKeys=5000, _ObjectSizeBytes=0),

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
%% riak_kv_kv_sweeper:stop_all_sweeps() stops all running sweeps
%% causing them to return failed as their result.
%%
%% The test uses sweep participant sweeper_callback_wait that calls
%% back a Fun before visiting an keys. The backend used must have >
%% 1000 keys, here 5000, because messages, e.g, stop, sent to the
%% process running a sweep are only received once every 1000 keys.
%%
%% The stop_all_sweeps request is made in this call back Fun
%% guaranteeing that we have at least one running sweep. The test
%% asserts that the sweep has failed by receipt of a failed_sweep
%% message. Test also asserts that the state all sweeps returns to
%% idle.
%%
test_stop_all_scheduled_sweeps(Config) ->
    Indices = ?config(vnode_indices, Config),
    new_sweep_participant(sweeper_callback_wait, RunInterval=1),
    sweeper_meck_helper:backend_modules(async, _NumKeys=5000, _ObjectSizeBytes=0),

    application:set_env(riak_kv, sweep_concurrency, length(Indices)),
    riak_kv_sweeper:enable_sweep_scheduling(),
    %% Lets sweeps run once so they are not in never run state
    tick_per_index(Indices),
    sweeper_callback_wait:before_visit(Indices),
    [ok = receive_msg({ok, successful_sweep, sweeper_callback_wait, I}) || I <- Indices],
    sweeper_meck_helper:advance_time({seconds, RunInterval+1}),
    riak_kv_sweeper:sweep_tick(),
    {_, Sweeps} = riak_kv_sweeper:status(),
    sweeper_callback_wait:before_visit(any, fun(_) ->
                                                    Running = riak_kv_sweeper:stop_all_sweeps(),
                                                    true = Running >= 1
                                            end),
    Running = [_|_] = [riak_kv_sweeper_state:sweep_index(Sweep) ||
                          Sweep <- Sweeps,
                          riak_kv_sweeper_state:sweep_state(Sweep) == running],
    [ok = receive_msg({ok, failed_sweep, sweeper_callback_wait, I}) || I <- Running],
    ok = check_sweeps_idle(Indices).


%%
%% riak_kv_kv_sweeper:stop_all_sweeps() only stop sweeps that are
%% running when it is called. Sweeps before or after the
%% stop_all_sweeps requests complete successfully.
%%
%% The test uses sweep participant sweeper_callback_wait that calls
%% back a Fun before visiting an keys. The backend used must have >
%% 1000 keys, here 5000, because messages, e.g, stop, sent to the
%% process running a sweep are only received once every 1000 keys.
%%
%% The test asserts that a stop_all_sweeps returns 0 running sweeps
%% stopped when there are no running sweeps and subsequent sweeps
%% complete successfully.
test_stop_all_scheduled_sweeps_race_condition(Config) ->
    Indices = ?config(vnode_indices, Config),
    new_sweep_participant(sweeper_callback_1, RunInterval=1),
    sweeper_meck_helper:backend_modules(async, 5000, 0),

    riak_kv_sweeper:enable_sweep_scheduling(),
    sweep_all_indices(Indices),
    0 = _Running = riak_kv_sweeper:stop_all_sweeps(),
    sweeper_meck_helper:advance_time({seconds, RunInterval+1}),
    sweep_all_indices(Indices),
    ok.

%%
%% The sweep scheduler includes a new participant in sweeps.
%%
%% The test uses sweep participant sweeper_callback_1, and
%% sweeper_callback_2 that complete successfully as soon they are
%% scheduled.
%%
%% The test asserts receipt of successful_sweep messages for the
%% previous and new sweep participant sweep_callback_2 after the have
%% been scheduled to run.
%%
test_scheduler_add_participant(Config) ->
    Indices = ?config(vnode_indices, Config),
    new_sweep_participant(sweeper_callback_1),
    sweeper_meck_helper:backend_modules(async, _NumKeys=1000, _ObjectSizeBytes=0),

    riak_kv_sweeper:enable_sweep_scheduling(),
    sweep_all_indices(Indices, ok, sweeper_callback_1),
    new_sweep_participant(sweeper_callback_2),
    sweep_all_indices(Indices, ok, sweeper_callback_1),
    sweep_all_indices(Indices, ok, sweeper_callback_2),
    ok.


%%
%% riak_kv_sweeper:sweep(...) request restarts running sweep.
%%
%% The test uses sweep participant sweeper_callback_wait that calls
%% back a Fun before visiting an keys. The backend used must have >
%% 1000 keys, here 5000, because messages, e.g, stop, sent to the
%% process running a sweep are only received once every 1000 keys.
%%
%% The sweep request is made in this call back Fun guaranteeing that
%% we have a running sweep. The test asserts that the sweep has failed
%% by receipt of a failed_sweep message. Test also asserts that the
%% state of the restarted sweep changes to restart and then completes
%% successfully.
%%
test_scheduler_restart_sweep(Config) ->
    Indices = ?config(vnode_indices, Config),
    new_sweep_participant(sweeper_callback_wait, RunInterval=1),
    sweeper_meck_helper:backend_modules(async, _NumKeys=5000, _ObjectSizeBytes=0),

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
    sweeper_meck_helper:advance_time({seconds, RunInterval+1}),
    tick_per_index(Indices),
    sweeper_callback_wait:before_visit([WaitIndex]),
    ok = receive_msg({ok, successful_sweep, sweeper_callback_wait, WaitIndex}).

%%
%% Test sweep behaviour when we can get a lock on the
%% riak_kv_index_hashtree.
%%
%% The test uses sweep participant sweeper_callback_1 that completes
%% successfully as soon as it is scheduled and mocks riak_kv_vnode to
%% return ok for lock result.
%%
%% The test asserts that all the mocked calls for
%% riak_kv_index_hashtree are called with expected arguments and that
%% the mocked call to riak_kv_vnode sweep function is called with the
%% value of EstimatedKeys returned by riak_kv_index_hashtree.
%%
test_scheduler_estimated_keys_lock_ok(Config) ->
    Indices = ?config(vnode_indices, Config),
    new_sweep_participant(sweeper_callback_1),
    sweeper_meck_helper:backend_modules(async, _NumKeys=5000, _ObjectSizeBytes=0),
    sweeper_meck_helper:aae_modules(_AAEnabled = true, EstimatedKeys = 4200, _LockResult = ok),

    sweep_all_indices(Indices),
    [true = meck:called(riak_kv_vnode, sweep, [{I, '_'}, '_', EstimatedKeys]) || I <- Indices],
    [true = meck:called(riak_kv_index_hashtree, get_lock, [I, estimate]) || I <- Indices],
    [true = meck:called(riak_kv_index_hashtree, estimate_keys, [I]) || I <- Indices],
    [true = meck:called(riak_kv_index_hashtree, release_lock, [I]) || I <- Indices],
    ok.

%%
%% Test sweep behaviour when we can not get a lock on the
%% riak_kv_index_hashtree.
%%
%% The test uses sweep participant sweeper_callback_1 that completes
%% successfully as soon as it is scheduled and mocks riak_kv_vnode to
%% return fail for lock result.
%%
%% The test asserts that all the mocked calls for
%% riak_kv_index_hashtree are called with expected arguments and we do
%% not make a request to estimate_keys or release the lock.  the
%% mocked call to riak_kv_vnode sweep function is called with the
%% value of 0 for Estimated Keys.
%%
test_scheduler_estimated_keys_lock_fail(Config) ->
    Indices = ?config(vnode_indices, Config),
    new_sweep_participant(sweeper_callback_1),
    sweeper_meck_helper:backend_modules(async, _NumKeys=5000, _ObjectSizeBytes=0),
    sweeper_meck_helper:aae_modules(_AAEnabled = true, _EstimatedKeys = 4200, _LockResult = fail),

    sweep_all_indices(Indices),
    [true = meck:called(riak_kv_vnode, sweep, [{I, '_'}, '_', 0]) || I <- Indices],
    [true = meck:called(riak_kv_index_hashtree, get_lock, [I, estimate]) || I <- Indices],
    [false = meck:called(riak_kv_index_hashtree, estimate_keys, [I]) || I <- Indices],
    [false = meck:called(riak_kv_index_hashtree, release_lock, [I]) || I <- Indices],
    ok.

%%
%% Test throttling on obj_size with no mutated keys, see
%% sweep_throttle_obj_size.
%%
test_sweep_throttle_obj_size1(Config) ->
    Indices = ?config(vnode_indices, Config),
    sweep_throttle_obj_size(
      _Index              = pick(Indices),
      _NumKeys            = 1000,
      _NumMutatedKeys     = 0,
      _ObjSizeBytes       = 100,
      _ThrottleAfterBytes = 100,
      _ThrottleWaitMsecs  = 1).

%%
%% Test throttling on obj_size with 100 mutated keys - no extra
%% throttles because of mutated keys should be done, see
%% sweep_throttle_obj_size.
%%
test_sweep_throttle_obj_size2(Config) ->
    Indices = ?config(vnode_indices, Config),
    sweep_throttle_obj_size(
      _Index              = pick(Indices),
      _NumKeys            = 1000,
      _NumMutatedKeys     = 100,
      _ObjSizeBytes       = 100,
      _ThrottleAfterBytes = 200,
      _ThrottleWaitMsecs  = 1).

%%
%% Test throttling on obj_size where number of bytes to throttle after
%% is bigger than object size, see sweep_throttle_obj_size.
%%
test_sweep_throttle_obj_size3(Config) ->
    Indices = ?config(vnode_indices, Config),
    sweep_throttle_obj_size(
      _Index              = pick(Indices),
      _NumKeys            = 1000,
      _NumMutatedKeys     = 100,
      _ObjSizeBytes       = 100,
      _ThrottleAfterBytes = 500,
      _ThrottleWaitMsecs  = 1).

%%
%% Test throttling on obj_size using a sweep_window_throttle_div, see
%% sweep_throttle_obj_size.
%%
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

%%
%% Test throttling on obj_size sweep_window > throttle wait time, see
%% sweep_throttle_obj_size
%%
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

%%
%% Helper function to test throttling of obj_size throttling strategy.
%%
%% The test uses sweep participant sweeper_callback_mutate that
%% mutates a configured count of keys. The number of keys to mutate
%% are sent to the participant before it starts visiting the keys.
%%
%% The test calculates the expected throttle msecs and asserts it
%% matches the actual throttle msecs.
%%
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

%%
%% Test throttling on pace every 100 keys, see sweep_throttle_pace.
%%
test_sweep_throttle_pace1(Config) ->
    Indices = ?config(vnode_indices, Config),
    sweep_throttle_pace(
      _Index              = pick(Indices),
      _NumKeys            = 1000,
      _NumMutatedKeys     = 0,
      _NumKeysPace        = 100,
      _ThrottleWaitMsecs  = 1).

%%
%% Test throttling on pace every 100 keys with one extra throttle
%% because of mutated objects, see sweep_throttle_pace.
%%
test_sweep_throttle_pace2(Config) ->
    Indices = ?config(vnode_indices, Config),
    sweep_throttle_pace(
      _Index              = pick(Indices),
      _NumKeys            = 1000,
      _NumMutatedKeys     = 100,
      _NumKeysPace        = 100,
      _ThrottleWaitMsecs  = 1).

%%
%% Test throttling on pace with several extra throttles because of
%% mutations, see sweep_throttle_pace.
%%
test_sweep_throttle_pace3(Config) ->
    Indices = ?config(vnode_indices, Config),
    sweep_throttle_pace(
      _Index              = pick(Indices),
      _NumKeys            = 5000,
      _NumMutatedKeys     = 2000,
      _NumKeysPace        = 100,
      _ThrottleWaitMsecs  = 1).

%%
%% Test throttling on pace using sweep_window_throttle_div, see
%% sweep_throttle_pace.
%%
test_sweep_throttle_pace4(Config) ->
    Indices = ?config(vnode_indices, Config),
    application:set_env(riak_kv, sweep_window_throttle_div, 2),
    sweep_throttle_pace(
      _Index              = pick(Indices),
      _NumKeys            = 1000,
      _NumMutatedKeys     = 1000,
      _NumKeysPace        = 100,
      _ThrottleWaitMsecs  = 2).

%%
%% Test throttling on pace using sweep_window_throttle_div where
%% sweep_window_throttle_div > throttle wait time, see
%% sweep_throttle_pace.
%%
test_sweep_throttle_pace5(Config) ->
    Indices = ?config(vnode_indices, Config),
    application:set_env(riak_kv, sweep_window_throttle_div, 2),
    sweep_throttle_pace(
      _Index              = pick(Indices),
      _NumKeys            = 1000,
      _NumMutatedKeys     = 1000,
      _NumKeysPace        = 100,
      _ThrottleWaitMsecs  = 1).

%%
%% Helper function to test throttling of pace throttling strategy.
%%
%% The test uses sweep participant sweeper_callback_mutate that
%% mutates a configured count of keys. The number of keys to mutate
%% are sent to the participant before it starts visiting the keys.
%%
%% The test calculates the expected throttle msecs and asserts it
%% matches the actual throttle msecs.
%%
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

%%
%% Test sweeper reporting of swept keys and bytes swept to exometer.
%%
%% The test uses sweep participant sweeper_callback_1 that completes
%% successfully as soon as it is scheduled.
%%
%% Asserts that the number of swept keys and size of bytes swept are
%% correctly reported to exometer.
%%
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

%%
%% Test sweeper reporting of successful_sweep counts per participant
%% to exometer.
%%
%% The test uses sweep participant sweeper_callback_1 that completes
%% successfully as soon as it is scheduled.
%%
%% Asserts that the number of successful sweeps to sweeper_callback_1
%% are reported correctly to exometer.
%%
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

%%
%% Test sweeper reporting of swept keys and bytes swept to exometer.
%%
%% The test uses sweep participant sweeper_callback_crash that fails
%% the sweep as soon as it is scheduled.
%%
%% Asserts that the number of failed sweeps to sweeper_callback_crash
%% are reported correctly to exometer.
%%
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

%%
%% Test sweeper reporting of mutated objects during sweep to exometer.
%%
%% The test uses sweep participant sweeper_callback_mutate that
%% mutates a configured count of keys. The number of keys to mutate
%% are sent to the participant before it starts visiting the keys.
%%
%% Asserts that the number of mutaed keys are reported correctly to
%% exometer.
%%
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

%%
%% Test sweeper reporting of mutated objects during sweep to exometer.
%%
%% The test uses sweep participant sweeper_callback_delete that
%% mutates a configured count of keys. The number of keys to delete
%% are sent to the participant before it starts visiting the keys.
%%
%% Asserts that the number of deleted keys are reported correctly to
%% exometer.
%%
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


%%
%% Test scheduling of concurrent sweeps, controlled by the
%% sweep_concurrency application environment variable.
%%
%% The test uses sweep participant sweeper_callback_wait that calls
%% back a Fun before visiting an keys to pause and then signal to the
%% sweeps to continue. See raik_kv_sweeper:before_visit(...), the Fun
%% for this test case defaults to no-op.
%%
%% Test asserts that we have a RingSize > ConcurrentSweeps and that
%% have exactly ConcurrentSweeps number of sweeps in the running state
%% after sending RingSize number of tick events.
%%
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

%%
%% Test that the bucket props are collected as part of the sweep if
%% the bucket_props options is set for the sweep participant.
%%
%% Assert that the number of requests to get bucket_props equals to
%% the number of keys, because the mocked backends treat each key as
%% though in a separate bucket. Accumlated bucket props are not
%% observable outside a sweep right and we are assuming it is
%% sufficent to count the number of get_bucket calls.
%%
test_bucket_props(Config) ->
    Indices = ?config(vnode_indices, Config),
    new_sweep_participant(sweeper_callback_bucket_props, _RunInterval = 1, _Options = [bucket_props]),
    sweeper_meck_helper:backend_modules(async, NumKeys=16, _ObjectSizeBytes=0),

    Index = pick(Indices),
    riak_kv_sweeper:sweep(Index),
    ok = receive_msg({ok, successful_sweep, sweeper_callback_bucket_props, Index}),
    true = NumKeys == length(meck_bucket_props()),
    ok.

%%
%% Test that the bucket props are not collected as part of the sweep
%% if the bucket_props option is not set set for the sweep_participant.
%%
%% Assert that the number of requests to get bucket_props equals to
%% 0. Accumlated bucket props are not observable outside a sweep
%% right and we are assuming it is sufficent to count the number of
%% get_bucket calls.
%%
test_no_bucket_props(Config) ->
    Indices = ?config(vnode_indices, Config),
    new_sweep_participant(sweeper_callback_1, _RunInterval = 1, _Options = []),
    sweeper_meck_helper:backend_modules(async, _NumKeys=1000, _ObjectSizeBytes=0),

    Index = pick(Indices),
    riak_kv_sweeper:sweep(Index),
    ok = receive_msg({ok, successful_sweep, sweeper_callback_1, Index}),
    [] = meck_bucket_props(),
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
    new_sweep_participant(Name, 1).

new_sweep_participant(Name, RunInterval) ->
    new_sweep_participant(Name, RunInterval, []).

new_sweep_participant(Name, RunInterval, Options) ->
    riak_kv_sweeper:add_sweep_participant(
      _Description = atom_to_list(Name) ++ " sweep participant",
      _Module = Name,
      _FunType = observe_fun,
      RunInterval,
      Options).


meck_sleep_for_throttle() ->
    meck:expect(riak_kv_sweeper, sleep_for_throttle, fun(_) -> ok end).

meck_count_msecs_slept() ->
    ThrottleHistory = meck:history(riak_kv_sweeper),
    MSecsThrottled = [MSecs || {_Pid, {riak_kv_sweeper, sleep_for_throttle, [MSecs]}, ok} <-
                                   ThrottleHistory],
    lists:sum(MSecsThrottled).

meck_bucket_props() ->
    [B || {_Pid, {riak_core_bucket, get_bucket, [B]}, _ReturnValue} <-
              meck:history(riak_core_bucket)].

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

%% When running tests on buildbot I have observed that sometimes
%% messages are not received by the waiting process but are in the
%% message queue, causing the test case to fail. Adding debug
%% printing, ct:pal(...), to the receive loop causes the messages to
%% be delivered. So, I suspect it's a scheduling issue and adding the
%% match_retry and sleeping is to facilitate the other process being
%% scheduled.
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
    %% Put Result in a tuple to allow result to be max_retries
    %% match_retry(Fun, max_retries)
    match_retry(_Max = 100, _N = 1, _Sleep = 100, Fun, {'_', Result}).

match_retry(Max, N, _Sleep, _Fun, {'_', _Result}) when N > Max ->
    max_retries;

match_retry(Max, N, Sleep, Fun, {'_', Result}) ->
    case Fun() of
        Result ->
            Result;
        _ ->
            timer:sleep(trunc(Sleep)),
            match_retry(Max, N+1, max(1000, Sleep*1.1), Fun, {'_', Result})
    end.
