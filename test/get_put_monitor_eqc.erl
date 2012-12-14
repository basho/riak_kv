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
    {setup, fun() ->
                    ok
            end,
     fun(_) ->
             [maybe_stop_and_wait(Server) || Server <- [riak_core_stat_cache, riak_kv_stat]]
     end, [
           {timeout, 120, ?_assertEqual(true, quickcheck(numtests(100, ?QC_OUT(prop()))))}
          ]}.

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
        {_,State,Res} = run_commands(?MODULE, Cmds),
        case Res of
            ok ->
                %% what about all the prcesses started?
                %% can we wait for the all to end, please?
                exit_gracefully(State),
                ok;
            _ -> io:format(user, "QC result: ~p\n", [Res])
        end,
        aggregate(command_names(Cmds), Res == ok)
    end).

%% ====================================================================
%% eqc_statem callbacks
%% ====================================================================

initial_state() ->
    [maybe_stop_and_wait(Server) || Server <- [riak_core_stat_cache, riak_kv_stat]],
    application:start(folsom),
    {ok, Cache} = riak_core_stat_cache:start_link(),
    {ok, Pid} = riak_kv_stat:start_link(),
    unlink(Pid),
    unlink(Cache),
    #state{}.

command(S) ->
    frequency([
        {3, {call, ?MODULE, put_fsm_started, []}},
        {3, {call, ?MODULE, get_fsm_started, []}},
        {1, {call, ?MODULE, put_fsm_noproc, []}},
        {1, {call, ?MODULE, get_fsm_noproc, []}},
        {1, {call, ?MODULE, put_fsm_exit_normal, [put, g_put_pid(S)]}},
        {1, {call, ?MODULE, put_fsm_exit_shutdown, [put, g_put_pid(S)]}},
        {1, {call, ?MODULE, put_fsm_exit_error, [put, g_put_pid(S)]}},
        {1, {call, ?MODULE, get_fsm_exit_normal, [get, g_get_pid(S)]}},
        {1, {call, ?MODULE, get_fsm_exit_shutdown, [get, g_get_pid(S)]}},
        {1, {call, ?MODULE, get_fsm_exit_error, [get, g_get_pid(S)]}}
    ]).

g_put_pid(#state{put_fsm = []}) ->
    undefined;
g_put_pid(#state{put_fsm = L}) ->
    oneof(L).

g_get_pid(#state{get_fsm = []}) ->
    undefined;
g_get_pid(#state{get_fsm = L}) ->
    oneof(L).

precondition(#state{get_fsm = []}, {call, _, _Command, [get, _]}) ->
    false;
precondition(#state{put_fsm = []}, {call, _, _Command, [put, _]}) ->
    false;
precondition(_,_) ->
    true.

next_state(S, Res, {call, _, get_fsm_started, []}) ->
    Gets2 = ordsets:add_element(Res, S#state.get_fsm),
    S#state{get_fsm = Gets2};

next_state(S, Res, {call, _, put_fsm_started, []}) ->
    Puts2 = ordsets:add_element(Res, S#state.put_fsm),
    S#state{put_fsm = Puts2};

next_state(S, _Res, {call, _, get_fsm_noproc, []}) ->
    S;

next_state(S, _Res, {call, _, put_fsm_noproc, []}) ->
    S;

next_state(S, _Res, {call, _, get_fsm_exit_error, [get, Pid]}) ->
    Gets2 = ordsets:del_element(Pid, S#state.get_fsm),
    ErrCount = S#state.get_errors + 1,
    S#state{get_fsm = Gets2, get_errors = ErrCount};

next_state(S, _Res, {call, _, _, [get, Pid]}) ->
    Gets2 = ordsets:del_element(Pid, S#state.get_fsm),
    S#state{get_fsm = Gets2};
    
next_state(S, _Res, {call, _, put_fsm_exit_error, [put, Pid]}) ->
    Puts2 = ordsets:del_element(Pid, S#state.put_fsm),
    ErrCount = S#state.put_errors + 1,
    S#state{put_fsm = Puts2, put_errors = ErrCount};

next_state(S, _Res, {call, _, _, [put, Pid]}) ->
    Puts2 = ordsets:del_element(Pid, S#state.put_fsm),
    S#state{put_fsm = Puts2}.


postcondition(S, {call, _Mod, put_fsm_started, _Args}, Res) ->
    check_state(S#state{put_fsm = [Res | S#state.put_fsm]});

postcondition(S, {call, _Mod, get_fsm_started, _Args}, Res) ->
    check_state(S#state{get_fsm = [Res | S#state.get_fsm]});

postcondition(S, {call, _Mod, put_fsm_noproc, _Args}, _Res) ->
    check_state(S);

postcondition(S, {call, _Mod, get_fsm_noproc, _Args}, _Res) ->
    check_state(S);

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
    % wait for folsom stats to settle; if we check too quick, folsom won't have
    % finished updating. Timers suck, so maybe something better will come along
    timer:sleep(10),

    % with a timetrap of 60 seconds, the spiral will never have values slide off
    MetricExpects = [
        {{riak_kv, node, puts, fsm, active}, length(PutList)},
        {{riak_kv, node, gets, fsm, active}, length(GetList)},
        {{riak_kv, node, puts, fsm, errors}, [{count, PutErrCount}, {one, PutErrCount}]},
        {{riak_kv, node, gets, fsm, errors}, [{count, GetErrCount}, {one, GetErrCount}]}
    ],

    [ begin
        ?assertEqual(Expected, folsom_metrics:get_metric_value(Metric))
    end || {Metric, Expected} <- MetricExpects],

    true.

%% wait for all fake fsms to finish, so the monitors
%% get to finish before folsom is stopped.
exit_gracefully(S) ->
    #state{put_fsm = PutList, get_fsm = GetList} = S,
    [end_and_wait(Pid, normal) || Pid <- PutList ++ GetList].

%% ====================================================================
%% Calls
%% ====================================================================

get_fsm_started() ->
    Pid = fake_fsm(),
    riak_kv_get_put_monitor:get_fsm_spawned(Pid),
    Pid.

get_fsm_noproc() ->
    Pid = fake_fsm(),
    end_and_wait(Pid, normal),
    riak_kv_get_put_monitor:get_fsm_spawned(Pid),
    % ugh, sleep; need to give time for the down message to get to the monitor.
    timer:sleep(10),
    Pid.

get_fsm_exit_normal(get, Pid) ->
    end_and_wait(Pid, normal),
    Pid.

get_fsm_exit_shutdown(get, Pid) ->
    end_and_wait(Pid, shutdown),
    Pid.

get_fsm_exit_error(get, Pid) ->
    end_and_wait(Pid, unnatural),
    Pid.

put_fsm_started() ->
    Pid = fake_fsm(),
    riak_kv_get_put_monitor:put_fsm_spawned(Pid),
    Pid.

put_fsm_noproc() ->
    Pid = fake_fsm(),
    end_and_wait(Pid, normal),
    riak_kv_get_put_monitor:put_fsm_spawned(Pid),
    timer:sleep(20),
    Pid.

put_fsm_exit_normal(put, Pid) ->
    end_and_wait(Pid, normal),
    Pid.

put_fsm_exit_shutdown(put, Pid) ->
    end_and_wait(Pid, shutdown),
    Pid.

put_fsm_exit_error(put, Pid) ->
    end_and_wait(Pid, unnatural),
    Pid.

%% ====================================================================
%% Helpers
%% ====================================================================

fake_fsm() -> proc_lib:spawn(?MODULE, fake_fsm_loop, []).

fake_fsm_loop() ->
    receive
        ExitCause ->
            exit(ExitCause)
    end.

end_and_wait(Pid, Cause) ->
    Pid ! Cause,
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

%% Make sure `Server' is not running
%% `Server' _MUST_ have an exported fun `stop/0'
%% that stopes the server
maybe_stop_and_wait(Server) ->
    case whereis(Server) of
        undefined ->
            ok;
        Pid ->
            Server:stop(),
            riak_kv_test_util:wait_for_pid(Pid)
    end.
-endif.
