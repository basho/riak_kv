-module(get_put_monitor_eqc).

-ifdef(EQC).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile([export_all]).

-record(state, {
    get_fsm = [],
    put_fsm = [],
    get_errors = 0,
    put_errors = 0
}).

-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).

eqc_test_() ->
    {timeout, 120, ?_assertEqual(true, quickcheck(numtests(100, ?QC_OUT(prop()))))}.

test() ->
    test(100).

test(N) ->
    quickcheck(numtests(N, prop())).

check() ->
    check(prop(), current_counterexample()).

prop() ->
    ?FORALL(Cmds, commands(?MODULE), begin
        crypto:start(),
        application:start(folsom),
        {ok, Pid} = riak_kv_get_put_monitor:start_link(),
        {_,_,Res} = run_commands(?MODULE, Cmds),
        unlink(Pid),
        Monref = erlang:monitor(process, Pid),
        riak_kv_get_put_monitor:stop(),
        receive
            {'DOWN', Monref, process, Pid, _} ->
                ok
        end,
        case Res of
            ok -> ok;
            _ -> io:format(user, "QC result: ~p\n", [Res])
        end,
        aggregate(command_names(Cmds), Res == ok)
    end).

%% ====================================================================
%% eqc_statem callbacks
%% ====================================================================

initial_state() ->
    #state{}.

command(S) ->
    oneof([
        {call, ?MODULE, get_fsm_started, []},
        {call, ?MODULE, get_fsm_exit_normal, [get, S]},
        {call, ?MODULE, get_fsm_exit_shutdown, [get, S]},
        {call, ?MODULE, get_fsm_exit_error, [get, S]},
        {call, ?MODULE, put_fsm_started, []},
        {call, ?MODULE, put_fsm_exit_normal, [put, S]},
        {call, ?MODULE, put_fsm_exit_shutdown, [put, S]},
        {call, ?MODULE, put_fsm_exit_error, [put, S]}
    ]).

precondition(S, {call, _, _Command, [get, S]}) ->
    [] =/= S#state.get_fsm;
precondition(S, {call, _, _Command, [put, S]}) ->
    [] =/= S#state.put_fsm;
precondition(_,_) ->
    true.


next_state(S, Res, {call, _, get_fsm_started, []}) ->
    Gets2 = ordsets:add_element(Res, S#state.get_fsm),
    S#state{get_fsm = Gets2};

next_state(S, Res, {call, _, put_fsm_started, []}) ->
    Puts2 = ordsets:add_element(Res, S#state.put_fsm),
    S#state{put_fsm = Puts2};

next_state(S, Res, {call, _, get_fsm_exit_error, [get, _]}) ->
    Gets2 = ordsets:del_element(Res, S#state.get_fsm),
    ErrCount = S#state.get_errors + 1,
    S#state{get_fsm = Gets2, get_errors = ErrCount};

next_state(S, Res, {call, _, _, [get, _]}) ->
    Gets2 = ordsets:del_element(Res, S#state.get_fsm),
    S#state{get_fsm = Gets2};
    
next_state(S, Res, {call, _, put_fsm_exit_error, [put, _]}) ->
    Puts2 = ordsets:del_element(Res, S#state.put_fsm),
    ErrCount = S#state.put_errors + 1,
    S#state{put_fsm = Puts2, put_errors = ErrCount};

next_state(S, Res, {call, _, _, [put, _]}) ->
    Puts2 = ordsets:del_element(Res, S#state.put_fsm),
    S#state{put_fsm = Puts2}.


postcondition(S, {call, _Mod, put_fsm_started, _Args}, Res) ->
    check_state(S#state{put_fsm = [Res | S#state.put_fsm]});

postcondition(S, {call, _Mod, get_fsm_started, _Args}, Res) ->
    check_state(S#state{get_fsm = [Res | S#state.get_fsm]});

postcondition(S, {call, _Mod, put_fsm_exit_error, _Args}, Res) ->
    S2 = S#state{
        put_fsm = ordsets:del_element(Res, S#state.put_fsm),
        put_errors = S#state.put_errors + 1
    },
    check_state(S2);

postcondition(S, {call, _Mod, get_fsm_exit_error, _Args}, Res) ->
    S2 = S#state{
        get_fsm = ordsets:del_element(Res, S#state.get_fsm),
        get_errors = S#state.get_errors + 1
    },
    check_state(S2);

postcondition(S, {call, _Mod, _NiceShutdown, [put, _]}, Res) ->
    S2 = S#state{
        put_fsm = ordsets:del_element(Res, S#state.put_fsm)
    },
    check_state(S2);

postcondition(S, {call, _Mod, _NiceShutdown, [get, _]}, Res) ->
    S2 = S#state{
        get_fsm = ordsets:del_element(Res, S#state.get_fsm)
    },
    check_state(S2).


check_state(S) ->
    #state{put_errors = PutErrCount, get_errors = GetErrCount,
        put_fsm = PutList, get_fsm = GetList} = S,
    ?debugFmt("state of thing:  ~p", [gen_server:call(riak_kv_get_put_monitor, dump_state)]),
    % with a timetrap of 60 seconds, the spiral will never have values slide off
    ?assertMatch([{count, PutErrCount},_], folsom_metrics:get_metric_value(put_fsm_errors_minute)),
    ?assertMatch([{count, GetErrCount},_], folsom_metrics:get_metric_value(get_fsm_errors_minute)),
    ?assertEqual(PutErrCount, folsom_metrics:get_metric_value(put_fsm_errors_since_start)),
    ?assertEqual(GetErrCount, folsom_metrics:get_metric_value(get_fsm_errors_since_start)),
    PutCount = length(PutList),
    ?assertEqual(PutCount, folsom_metrics:get_metric_value(put_fsm_in_progress)),
    GetCount = length(GetList),
    ?assertEqual(GetCount, folsom_metrics:get_metric_value(get_fsm_in_progress)),
    true.

%% ====================================================================
%% Calls
%% ====================================================================

get_fsm_started() ->
    Pid = fake_fsm(),
    riak_kv_get_put_monitor:get_fsm_spawned(Pid),
    Pid.

get_fsm_exit_normal(get, S) ->
    Pid = lists_random(S#state.get_fsm),
    end_and_wait(Pid, normal),
    Pid.

get_fsm_exit_shutdown(get, S) ->
    Pid = lists_random(S#state.get_fsm),
    end_and_wait(Pid, shutdown),
    Pid.

get_fsm_exit_error(get, S) ->
    Pid = lists_random(S#state.get_fsm),
    end_and_wait(Pid, unnatural),
    Pid.

put_fsm_started() ->
    Pid = fake_fsm(),
    riak_kv_get_put_monitor:put_fsm_spawned(Pid),
    Pid.

put_fsm_exit_normal(put, S) ->
    Pid = lists_random(S#state.put_fsm),
    end_and_wait(Pid, normal),
    Pid.

put_fsm_exit_shutdown(put, S) ->
    Pid = lists_random(S#state.put_fsm),
    end_and_wait(Pid, shutdown),
    Pid.

put_fsm_exit_error(put, S) ->
    Pid = lists_random(S#state.put_fsm),
    end_and_wait(Pid, unnatural),
    Pid.

%% ====================================================================
%% Helpers
%% ====================================================================

fake_fsm() -> proc_lib:spawn(?MODULE, fake_fsm_loop, []).

fake_fsm_loop() ->
    receive
        _ ->
            fake_fsm_loop()
    end.

end_and_wait(Pid, Cause) ->
    exit(Pid, Cause),
    Monref = erlang:monitor(process, Pid),
    receive
        {'DOWN', Monref, process, Pid, _} ->
            ok
    end.

lists_random([]) ->
    erlang:error(badarg);
lists_random([E]) ->
    E;
lists_random(List) ->
    Max = length(List),
    Nth = crypto:rand_uniform(1, Max),
    lists:nth(Nth, List).
-endif.
