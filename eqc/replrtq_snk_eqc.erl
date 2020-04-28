%%% @author Thomas Arts <thomas@SpaceGrey.local>
%%% @copyright (C) 2019, Thomas Arts
%%% @doc The replrtq_snk server is started with a one-to-one strategy in the
%%%      supervisor tree riak_kv_sup. In other words, it should handle the
%%%      cases in which one of its communication parties is down for a while.
%%%
%%%      Since we do want to test fault tolerance w.r.t. the other modules,
%%%      we model stopping, starting and crashing here.
%%%
%%% @end
%%% Created : 04 Jun 2019 by Thomas Arts <thomas@SpaceGrey.local>

-module(replrtq_snk_eqc).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_component.hrl").

-compile([export_all, nowarn_export_all]).

-define(TEST_DURATION, 100). %% milliseconds

%% -- State ------------------------------------------------------------------

get_sink(#{sinks := Sinks}, QueueName) ->
    maps:get(QueueName, Sinks).

on_sink(S = #{sinks := Sinks}, QueueName, Fun) ->
    case maps:get(QueueName, Sinks, undefined) of
        undefined -> S;
        Sink      -> S#{sinks := Sinks#{QueueName => Fun(Sink)}}
    end.

peers(S, Q) ->
    case get_sink(S, Q) of
        #{ peers := Peers } -> [ {P, A} || {P, {A, _}} <- Peers ];
        _                   -> []
    end.

workers(S, Q) ->
    case get_sink(S, Q) of
        #{ workers := N } -> N;
        _                 -> 0
    end.

active_sinks(#{sinks := Sinks}) ->
    [ Q || {Q, Sink} <- maps:to_list(Sinks),
           not maps:get(suspended, Sink, false) ];
active_sinks(_) -> [].

suspended_sinks(#{sinks := Sinks}) ->
    [ Q || {Q, Sink} <- maps:to_list(Sinks),
           maps:get(suspended, Sink, false) ];
suspended_sinks(_) -> [].

initial_state() ->
    #{time => 0}.

%% -- Operations -------------------------------------------------------------

%% --- Operation: config ---

config_pre(S) ->
    not maps:is_key(config, S).

%% We configure the same amount of workes for all sinks
config_args(_S) ->
    [weighted_default({1, disabled}, {19, enabled}), sink_gen()].

config_pre(_, [_Enabled, #{queue := _Q, peerlimit:= PL, workers := N}]) ->
    PL =< N.

config(Enabled, #{queue := Q, peers := Peers, peerlimit:= PL, workers := N} = Sink) ->
    application:set_env(riak_kv, replrtq_enablesink, Enabled == enabled),
    %% default Queue will be called "default" in tests
    application:set_env(riak_kv, replrtq_sinkqueue, default),
    %% We presupose that all queues have the same number of workers....
    application:set_env(riak_kv, replrtq_sinkworkers, N),
    application:set_env(riak_kv, replrtq_sinkpeerlimit, PL),
    application:set_env(riak_kv, replrtq_sinkpeers, pp_peers(Q, Peers)),
    [ setup_sink(Sink) || Enabled == enabled ],
    ok.


setup_sink(#{queue := Q, peers := Peers, workers := N, peerlimit := PL}) ->
    %% possibly take peerlimit into account here?
    replrtq_snk_monitor:add_queue(Q, Peers, N, PL).

config_next(S, _Value, [Enabled, Sink]) ->
    S#{ enabled => Enabled == enabled,
        config  => [Sink]}.


%% --- Operation: start ---

start_pre(S) ->
    not maps:is_key(pid, S) andalso maps:is_key(config, S).

start_args(_S) ->
    [].

start() ->
    {ok, Pid} = riak_kv_replrtq_snk:start_link(),
    unlink(Pid),
    %% we don't yet test auto starting
    riak_kv_replrtq_snk:prompt_work(),
    Pid.

start_next(#{enabled := false} = S, Pid, []) -> S#{ pid => Pid, sinks => #{} };
start_next(#{config  := Sinks} = S, Pid, _Args) ->
    S#{pid   => Pid,
       sinks => maps:from_list([ {Q, Sink} || #{queue := Q} = Sink <- Sinks ])}.

start_post(_S, _Args, Res) ->
    is_pid(Res) andalso is_process_alive(Res).

%% start_process(_S, []) ->
%%     worker.



%% --- Operation: sleep ---

sleep_pre(#{time := Time, sinks := _}) -> Time < ?TEST_DURATION;
sleep_pre(_) -> false.

sleep_args(_) ->
    [weighted_default(
      {3, choose(1, 10)},
      {1, choose(10, 40)})].

sleep_pre(#{time := Time}, [N]) -> Time + N =< ?TEST_DURATION.

sleep(N) -> timer:sleep(N).

sleep_next(S = #{time := T}, _, [N]) -> S#{time => T + N}.

%% --- Operation: suspend ---

suspend_pre(S) -> maps:is_key(sinks, S).

suspend_args(S) -> [maybe_active_queue_gen(S)].

suspend(QueueName) ->
    R = riak_kv_replrtq_snk:suspend_snkqueue(QueueName),
    replrtq_snk_monitor:suspend(QueueName),
    timer:sleep(1),
    R.

suspend_next(S, _, [QueueName]) ->
    on_sink(S, QueueName, fun(Sink) -> Sink#{suspended => true} end).

%% --- Operation: resume ---

resume_pre(S) -> maps:is_key(sinks, S).

resume_args(S) -> [maybe_suspended_queue_gen(S)].

resume(QueueName) ->
    replrtq_snk_monitor:resume(QueueName),
    riak_kv_replrtq_snk:resume_snkqueue(QueueName).

resume_next(S, _, [QueueName]) ->
    on_sink(S, QueueName, fun(Sink) -> Sink#{suspended => false} end).

%% --- Operation: add ---

add_pre(S) -> maps:is_key(sinks, S).

add_args(_S) -> [sink_gen()].

add_pre(#{sinks := Sinks}, [#{queue := Q, workers := N}]) ->
    not maps:is_key(Q, Sinks) andalso N > 0.

add(#{queue := Q, peers := Peers, workers := N, peerlimit := PL}) ->
    PeerInfo = fun(I, {{Host, Port, Protocol}, _}) -> {I, 8, Host, Port, Protocol} end,
    replrtq_snk_monitor:add_queue(Q, Peers, N, PL),
    PeerInfos = [PeerInfo(I, Peer) ||
                    {I, Peer} <- lists:zip(lists:seq(1, length(Peers)), Peers) ],
    if PL == N ->
            riak_kv_replrtq_snk:add_snkqueue(Q, PeerInfos, N);
       PL =/= N ->
            riak_kv_replrtq_snk:add_snkqueue(Q, PeerInfos, N, PL)
    end.

add_next(#{sinks := Sinks} = S, _V, [#{queue := Q} = Sink]) ->
  Removed = maps:get(removed, S, []),
  S#{ sinks => Sinks#{ Q => Sink },
      reused => maps:get(reused, S, []) ++ [Q || lists:member(Q, Removed)] }.

%% --- remove ---

remove_pre(S) -> maps:is_key(sinks, S).

remove_args(S) -> [maybe_active_queue_gen(S)].

remove(Q) ->
    R = riak_kv_replrtq_snk:remove_snkqueue(Q),
    replrtq_snk_monitor:remove_queue(Q),
    R.

remove_next(#{sinks := Sinks} = S, _, [Q]) ->
    Removed = maps:get(removed, S, []),
    S#{sinks => maps:remove(Q, Sinks), removed => [Q | Removed]}.

%% --- Operation: set the numebr of workers for a queue ---
nrworkers_pre(S) -> maps:is_key(sinks, S).

nrworkers_args(S) ->
    [maybe_active_queue_gen(S), ?LET(N, nat(), N + 1)].

nrworkers_pre(#{sinks := Sinks}, [Q, _Workers]) ->
    maps:is_key(Q, Sinks).

nrworkers(Q, Workers) ->
    replrtq_snk_monitor:update_workers(Q, Workers),
    riak_kv_replrtq_snk:set_workercount(Q, Workers).

nrworkers_next(#{sinks := Sinks} = S, _Value, [Q, Workers]) ->
    Sink = maps:get(Q, Sinks),
    S#{sinks => Sinks#{Q => Sink#{workers => Workers, peerlimit => Workers}},
       with_dynamic_workers => [Q | maps:get(with_dynamic_workers, S, [])]}.


%% -- Generators -------------------------------------------------------------

%% -- Helper functions

pp_peers(Q, Peers) ->
    string:join([ pp_peer(Q, Peer) || {Peer, _Cfg} <- Peers], "|").

pp_peer(default, {Name, Port, Protocol}) ->
    lists:concat([Name, ":", Port, ":", Protocol]);
pp_peer(Q, {Name, Port, Protocol}) ->
    lists:concat([Q, ":", Name, ":", Port, ":", Protocol]).


queue_names() ->
   [ queue1, queue2, a, b, c, banana, sledge_hammer ].

sink_queue_gen() ->
    weighted_default({1, default}, {9, elements(queue_names())}).

%% Workers  =< Peerlimit * nr(Peers)
sink_gen() ->
    ?LET(Peers, peers_gen(),
    ?LET(Workers, choose(1, 30),
    ?LET(Peerlimit, choose((Workers + length(Peers) - 1) div length(Peers), Workers),
         #{ queue   => sink_queue_gen(),
            peers   => Peers,
            peerlimit => Peerlimit,
            workers => Workers}))).


workers_gen() ->
    frequency([{1, 0}, {10, choose(1, 5)}]).

peers_gen() ->
    ?LET(N, choose(1, 8), peers_gen(N)).

peers_gen(N) ->
    vector(N, {{url(), ?LET(P, nat(), P + 8000), http}, peer_config_gen()}).

elements_or([], Other) -> Other;
elements_or(Xs, Other) -> weighted_default({4, elements(Xs)}, {1, Other}).

maybe_active_queue_gen(S) ->
    elements_or(active_sinks(S), elements(queue_names())).

maybe_suspended_queue_gen(S) ->
    elements_or(suspended_sinks(S), elements(queue_names())).

url() ->
    noshrink(
    ?LET({[Name], Suf}, {eqc_erlang_program:words(1),
                         elements(["com", "org", "co.uk", "edu", "de"])},
    return(Name ++ "." ++ Suf))).

peer_config_gen() ->
    {frequency([{7, active}, {2, inactive}, {1, error}]),
     choose(5, 20)}.    %% Response time


%% -- Property ---------------------------------------------------------------

weight(_S, suspend) -> 10;
weight(_S, remove)  -> 10;
weight(_S, nrworkers) -> 5;
weight(_S, _Cmd)    -> 50.

prop_repl() ->
    ?SETUP(
        fun() ->
                %% Setup mocking, etc.
                eqc_mocking:start_mocking(api_spec()),
                %% Return the teardwown function
                fun() -> ok end
        end,
    eqc:dont_print_counterexample(
    ?FORALL(Cmds, commands(?MODULE),
    begin
        replrtq_snk_monitor:start_link(),
        {H, S = #{time := Time}, Res} = run_commands(Cmds),
        timer:sleep(max(0, ?TEST_DURATION - Time)),
        Traces = replrtq_snk_monitor:stop(),
        Crashed =
            case maps:get(pid, S, undefined) of
                undefined -> false;
                Pid when is_pid(Pid) ->
                    HasCrashed = not is_process_alive(Pid),
                    exit(Pid, kill),
                    HasCrashed;
                Other -> {not_pid, Other}
            end,
        WithDynamicWorkers = maps:get(with_dynamic_workers, S, []),
        ReUsedQueues = maps:get(reused, S, []),
        check_command_names(Cmds,
            collect({changed_workers_count, length(WithDynamicWorkers) > 0},
            collect({removed_and_reused, length(ReUsedQueues) > 0},
            measure(length, commands_length(Cmds),
            measure(trace, [length(T) || {_, _, _, T} <- Traces],
                pretty_commands(?MODULE, Cmds, {H, S, Res},
                                conjunction([{result, equals(Res, ok)},
                                             {alive, equals(Crashed, false)},
                                             {trace, check_traces(Traces, WithDynamicWorkers ++
                                                                      ReUsedQueues)}])
                               ))))))
    end))).

check_traces(Traces, WithDynamicWorkers) ->
    conjunction([ {QueueName, check_trace(QueueName, Peerlimit, Peers, get_workers(Workers, Trace))}
                  || {QueueName, Peerlimit, Peers, Workers, Trace} <- Traces,
                      not lists:member(QueueName, WithDynamicWorkers), QueueName /= no_queue ]).

%% We only need this if we have non dynamic worker count changes
get_workers(Workers, Trace) ->
    case Trace of
        [{_, {workers, N}} | Rest ] -> {N, Rest};
        _ -> {Workers, Trace}
    end.

check_trace(QueueName, Peerlimit, Peers, {Workers, Trace}) ->
    Concurrent = concurrent_fetches(Trace),
    {Suspended, Active} = split_suspended(Concurrent),
    Max        = lists:max([0 | [N || {_, N} <- Active ++ Suspended]]),
    Avg        = weighted_average(Active),
    ActiveTime = lists:sum([ T || {T, _} <- Active ]),
    ActivePeers = length([ x || {_, {active, _}} <- Peers ]),
    AnyActive  = ActivePeers > 0,
    ActiveWorkers = min(ActivePeers * Peerlimit, Workers),
    MinActiveWorkers = min(ActiveWorkers - 1, 0.8 * ActiveWorkers),
    ?WHENFAIL(eqc:format("Trace for ~p:\n  ~p\nworkers ~p\n", [QueueName, Trace, Workers]),
    conjunction([{max_workers, case Trace of
                                   [{_, stop}] -> true;
                                   _ -> equals(Workers, Max)
                               end},
                 {avg_workers,
                    %% Check that the average number of active workers is >= WorkerCount - 1
                    ?WHENFAIL(eqc:format("Concurrent =\n~s\n~.1f < ~p\n",
                                         [format_concurrent(Active), Avg, MinActiveWorkers]),
                        Avg >= MinActiveWorkers
                            orelse not AnyActive        %% Don't check saturation if no work
                            orelse ActiveTime < 40000   %% or if mostly suspended
                        )},
                 {suspended,
                    ?WHENFAIL(eqc:format("Suspended =\n  ~p\n", [Suspended]),
                              check_suspended(Suspended))}])).

format_concurrent(Xs) ->
    [ io_lib:format("  ~p for ~5.1fms\n", [C, T / 1000])
      || {T, C} <- Xs, T >= 100 ].

-define(SLACK, 900). %% allow fetches 900µs after suspension

drop_slack(Chunk) -> drop_slack(0, Chunk).
drop_slack(_, []) -> [];
drop_slack(T, [E = {DT, _} | Es]) when T + DT > ?SLACK -> [E | Es];
drop_slack(T, [{DT, _} | Es]) -> drop_slack(T + DT, Es).

%% Check that number of concurrent fetches is decreasing
check_suspended(Chunks) ->
    lists:all(fun(Chunk) ->
        Chunk1 = drop_slack(Chunk),
        Cs = [ C || {_, C} <- Chunk1 ],
        lists:reverse(Cs) == lists:sort(Cs) end, Chunks).

split_suspended(Xs) ->
    {suspended_chunks(Xs), [ {W, X} || {W, X, active} <- Xs ]}.

suspended_chunks(Xs) -> suspended_chunks(Xs, []).
suspended_chunks([], Acc) ->
    lists:reverse(Acc);
suspended_chunks(Xs, Acc) ->
    Susp = fun(Ss) -> fun({_, _, S}) -> lists:member(S, Ss) end end,
    {_, Xs1} = lists:splitwith(Susp([active]), Xs),
    {Ys, Zs} = lists:splitwith(Susp([suspended, removed]), Xs1),
    suspended_chunks(Zs, [[{X, W} || {X, W, _} <- Ys] | Acc]).

weighted_average([]) -> 0;
weighted_average(Xs) ->
    lists:sum([ W * X || {W, X} <- Xs]) /
    lists:sum([ W     || {W, _} <- Xs]).

concurrent_fetches([]) -> [];
concurrent_fetches(Trace = [{T0, _} | _]) ->
    concurrent_fetches(Trace, T0, 0, active, []).

concurrent_fetches([], _, _, _, Acc) ->
    lists:reverse(Acc);
concurrent_fetches([{T1, E} | Trace], T0, N, Status, Acc) ->
    N1 =
        case E of
            {return, _} -> N - 1;
            {fetch, _}  -> N + 1;
             _          -> N
        end,
    Status1 =
        case E of
            _ when Status == removed -> removed;
            remove                   -> removed;
            suspend                  -> suspended;
            resume                   -> active;
            _                        -> Status
        end,
    DT = timer:now_diff(T1, T0),
    concurrent_fetches(Trace, T1, N1, Status1, [{DT, N, Status} || DT > 0] ++ Acc).

%% -- API-spec ---------------------------------------------------------------
api_spec() ->
    #api_spec{ language = erlang, mocking = eqc_mocking,
               modules = [ app_helper_spec(), lager_spec(), rhc_spec(),
                           riak_client_spec(), mock_spec(), riak_kv_stat_spec() ] }.

app_helper_spec() ->
    #api_module{ name = app_helper, fallback = ?MODULE }.

get_env(App, Key, Default) ->
    application:get_env(App, Key, Default).

get_env(App, Key) -> get_env(App, Key, undefined).

lager_spec() ->
    #api_module{ name = lager, fallback = ?MODULE }.

warning(_F, _As) ->
    %% io:format(F++"\n", As),
    ok.

rhc_spec() ->
    #api_module{ name = rhc, fallback = replrtq_snk_monitor }.

riak_client_spec() ->
    #api_module{ name = riak_client, fallback = ?MODULE }.

riak_kv_stat_spec() ->
    #api_module{ name = riak_kv_stat, fallback = ?MODULE}.

update(_) ->
    ok.

new(_, _) -> riak_client.

push(RObj, Bool, List, LocalClient) ->
    replrtq_snk_monitor:push(RObj, Bool, List, LocalClient).

mock_spec() ->
    #api_module{ name = replrtq_mock,
                 functions = [ #api_fun{name = error, arity = 1} ] }.
