-module(riak_kv_sweeper_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include("riak_kv_sweeper.hrl").

%%--------------------------------------------------------------------
%% COMMON TEST CALLBACK FUNCTIONS
%%--------------------------------------------------------------------


suite() ->
    [{timetrap, {minutes, 1}}].


init_per_suite(Config) ->
    Config.


end_per_suite(_Config) ->
    ok.


init_per_group(_GroupName, Config) ->
    Config.


end_per_group(_GroupName, _Config) ->
    ok.


init_per_testcase(_TestCase, Config) ->
    file:delete(riak_kv_sweeper:sweep_file()),
    application:set_env(riak_kv, sweep_participants, undefined),
    application:set_env(riak_kv, sweep_window, always),
    application:set_env(riak_kv, sweeper_scheduler, false),
    application:set_env(riak_kv, sweep_tick, 100),

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
    meck:expect(Name, visit_object_fun,
                fun(_Index) ->
                        fun({{_Bucket, _Key}, _RObj}, Acc, _Opts = []) ->
                                {ok, Acc}
                        end
                end),
    meck:expect(Name, successfull_sweep,
                fun(Index, _FinalAcc) ->
                        TestCasePid ! {ok, successfull_sweep, Index},
                        ok
                end),

    meck:expect(Name, failed_sweep,
                fun(Index, _Reason) ->
                        TestCasePid ! {ok, failed_sweep, Index},
                        ok
                end),

    #sweep_participant{description = atom_to_list(Name) ++ " sweep participant",
                       module = Name,
                       fun_type = ?OBSERV_FUN,
                       run_interval = 60,       % TODO this is secondsb
                       options = []}.


meck_new_backend(TestCasePid) ->
    Keys = [integer_to_binary(N) || N <- lists:seq(1, 100)],
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
                                  riak_kv_sweeper:do_sweep(ActiveParticipants,
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
    ok = receive_msg({ok, successfull_sweep, I0}),

    I1 = pick(Indices),
    riak_kv_sweeper:sweep(I1),
    ok = receive_msg({ok, successfull_sweep, I1}).


scheduler_test(Config) ->
    Indices = ?config(vnode_indices, Config),
    meck_new_backend(self()),
    SP = new_meck_sweep_particpant(sweep_observer_1, self()),
    riak_kv_sweeper:add_sweep_participant(SP),
    riak_kv_sweeper:enable_sweep_scheduling(),
    [receive_msg({ok, successfull_sweep, I}) || I <- Indices],
    ok.

%% ------------------------------------------------------------------------------
%% Internal Functions
%% ------------------------------------------------------------------------------

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
    match =:= re:run(atom_to_list(F), "scheduler_test$", [{capture, none}]);
is_testcase({_, _}) ->
    false.

%% watch:remove_file_changed_action(ct).
%% watch:add_file_changed_action({ct, fun(_) -> ct:run_test({suite, riak_kv_sweeper_SUITE}) end}).
