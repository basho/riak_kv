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

config_args(_S) ->
    [weighted_default({1, disabled}, {19, enabled}),
     vector(2, sink_gen())].

config_pre(_, [_Enabled, Sinks]) ->
    Queues = [ Q || #{queue := Q} <- Sinks, Q /= disabled ],
    lists:usort(Queues) == lists:sort(Queues).

config(Enabled, Sinks) ->
    application:set_env(riak_kv, replrtq_enablesink, Enabled == enabled),
    [ lists:zipwith(fun setup_sink/2, lists:seq(1, length(Sinks)), Sinks) || Enabled == enabled ],
    ok.

setup_sink(I, #{queue := Q, peers := Peers, workers := N}) ->
    Key = fun(A) -> list_to_atom(lists:concat(["replrtq_sink", I, A])) end,
    application:set_env(riak_kv, Key(queue), Q),
    application:set_env(riak_kv, Key(peers), pp_peers(Peers)),
    application:set_env(riak_kv, Key(workers), N),
    replrtq_snk_monitor:add_queue(Q, Peers, N).

config_next(S, _Value, [Enabled, Sinks]) ->
    S#{ enabled => Enabled == enabled,
        config  => Sinks }.


%% --- Operation: start ---

start_pre(S) ->
    not maps:is_key(pid, S) andalso maps:is_key(config, S).

start_args(_S) ->
    [].

start() ->
    {ok, Pid} = riak_kv_replrtq_snk:start_link(),
    unlink(Pid),
    riak_kv_replrtq_snk:prompt_work(),
    Pid.

start_callouts(#{enabled := false}, _Args) -> ?EMPTY;
start_callouts(#{config := Sinks}, _Args) ->
    ?SEQ([ ?APPLY(setup_sink, [Sink]) || Sink <- Sinks ]).

setup_sink_callouts(_S, [#{queue := disabled}]) -> ?EMPTY;
setup_sink_callouts(_S, [#{peers := Peers}]) ->
    ?SEQ([ ?CALLOUT(rhc, create, [?WILDCARD, ?WILDCARD, ?WILDCARD, ?WILDCARD], {Host, Port})
         || {{Host, Port}, _} <- Peers]).

start_next(#{enabled := false} = S, Pid, []) -> S#{ pid => Pid, sinks => #{} };
start_next(#{config  := Sinks} = S, Pid, _Args) ->
    S#{pid   => Pid,
       sinks => maps:from_list([ {Q, Sink} || #{queue := Q} = Sink <- Sinks, Q /= disabled ])}.

start_post(_S, _Args, Res) ->
    is_pid(Res) andalso is_process_alive(Res).

%% --- Operation: sleep ---

sleep_pre(#{time := Time, sinks := _}) -> Time < ?TEST_DURATION;
sleep_pre(_) -> false.

sleep_args(_) ->
    [weighted_default(
      {3, choose(1, 10)},
      {1, choose(10, 40)})].

sleep_pre(#{time := Time}, [N]) -> Time + N =< ?TEST_DURATION.

sleep(N) -> timer:sleep(N).

sleep_next(S = #{time := T}, _, [N]) -> S#{time := T + N}.

%% --- Operation: suspend ---

suspend_pre(S) -> maps:is_key(sinks, S).

suspend_args(S) -> [maybe_active_queue_gen(S)].

suspend(QueueName) ->
    R = riak_kv_replrtq_snk:suspend_snkqueue(QueueName),
    replrtq_snk_monitor:suspend(QueueName),
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

add_pre(#{sinks := Sinks}, [#{queue := Q}]) ->
    Q /= disabled andalso
    not maps:is_key(Q, Sinks).

add(#{queue := Q, peers := Peers, workers := N}) ->
    PeerInfo = fun({{Host, Port}, _}) -> {Host, 8, Host, Port} end,
    replrtq_snk_monitor:add_queue(Q, Peers, N),
    riak_kv_replrtq_snk:add_snkqueue(Q, lists:map(PeerInfo, Peers), N).

add_callouts(_S, [Sink]) -> ?APPLY(setup_sink, [Sink]).

add_next(#{sinks := Sinks} = S, _V, [#{queue := Q} = Sink]) ->
  S#{ sinks := Sinks#{ Q => Sink } }.

%% --- remove ---

remove_pre(S) -> maps:is_key(sinks, S).

remove_args(S) -> [maybe_active_queue_gen(S)].

remove(Q) ->
    R = riak_kv_replrtq_snk:remove_snkqueue(Q),
    replrtq_snk_monitor:remove_queue(Q),
    R.

remove_next(#{sinks := Sinks} = S, _, [Q]) ->
    Removed = maps:get(removed, S, []),
    S#{sinks := maps:remove(Q, Sinks), removed => [Q | Removed]}.

%% -- Generators -------------------------------------------------------------

%% -- Helper functions

pp_peers(Peers) ->
    string:join([ pp_peer(Peer) || {Peer, _Cfg} <- Peers], "|").

pp_peer({Name, Port}) ->
    lists:concat([Name, ":", Port]).

queue_names() ->
   [ queue1, queue2, a, aa, b, c, banana, sledge_hammer ].

sink_queue_gen() ->
    weighted_default({1, disabled}, {9, elements(queue_names())}).

sink_gen() ->
    #{ queue   => sink_queue_gen(),
       peers   => peers_gen(),
       workers => workers_gen() }.

workers_gen() ->
    frequency([{1, 0}, {10, choose(1, 5)}]).

peers_gen() ->
    ?LET(N, choose(1, 8), peers_gen(N)).

peers_gen(N) ->
    vector(N, {{url(), ?LET(P, nat(), P + 8000)}, peer_config_gen()}).

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
    {elements([inactive, active]),
     choose(1, 10)}.    %% Response time


%% -- Property ---------------------------------------------------------------

weight(_S, suspend) -> 1;
weight(_S, remove)  -> 1;
weight(_S, _Cmd)    -> 5.

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
        check_command_names(Cmds,
            measure(length, commands_length(Cmds),
            measure(trace, [length(T) || {_, _, _, T} <- Traces],
                pretty_commands(?MODULE, Cmds, {H, S, Res},
                                conjunction([{result, equals(Res, ok)},
                                             {trace, check_traces(Traces)},
                                             {alive, equals(Crashed, false)}])))))
    end))).

check_traces(Traces) ->
    conjunction([ {QueueName, check_trace(QueueName, Peers, Workers, Trace)}
                  || {QueueName, Peers, Workers, Trace} <- Traces ]).

check_trace(QueueName, Peers, Workers, Trace) ->
    Concurrent = concurrent_fetches(Trace),
    {Suspended, Active} = split_suspended(Concurrent),
    Max        = lists:max([0 | [N || {_, N} <- Active ++ Suspended]]),
    Avg        = weighted_average(Active),
    ActiveTime = lists:sum([ T || {T, _} <- Active ]),
    AnyActive  = lists:any(fun({_, {A, _}}) -> A == active end, Peers),
    ?WHENFAIL(eqc:format("Trace for ~p:\n  ~p\n", [QueueName, Trace]),
    conjunction([{max_workers, equals(Workers, Max)},
                 {avg_workers,
                    %% Check that the average number of active workers is >= WorkerCount - 1
                    ?WHENFAIL(eqc:format("Concurrent =\n~s\n~.1f < ~p\n",
                                         [format_concurrent(Active), Avg, Workers - 1]),
                        Avg >= Workers - 1
                            orelse not AnyActive        %% Don't check saturation if no work
                            orelse ActiveTime < 40000   %% or if mostly suspended
                        )},
                 {suspended,
                    ?WHENFAIL(eqc:format("Suspended =\n  ~p\n", [Suspended]),
                              check_suspended(Suspended))}])).

format_concurrent(Xs) ->
    [ io_lib:format("  ~p for ~5.1fms\n", [C, T / 1000])
      || {T, C} <- Xs, T >= 100 ].

-define(SLACK, 20). %% allow fetches 20µs after suspension

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
                           riak_client_spec(), mock_spec() ] }.

app_helper_spec() ->
    #api_module{ name = app_helper, fallback = ?MODULE }.

get_env(riak_kv, Key, Default) ->
    application:get_env(riak_kv, Key, Default).

get_env(riak_kv, Key) -> get_env(riak_kv, Key, undefined).

lager_spec() ->
    #api_module{ name = lager, fallback = ?MODULE }.

warning(_, _) ->
    ok.

rhc_spec() ->
    #api_module{ name = rhc, fallback = replrtq_snk_monitor,
                 functions = [ #api_fun{name = create, arity = 4} ] }.

riak_client_spec() ->
    #api_module{ name = riak_client, fallback = ?MODULE }.

new(_, _) -> riak_client.

push(RObj, Bool, List, LocalClient) ->
    replrtq_snk_monitor:push(RObj, Bool, List, LocalClient).

mock_spec() ->
    #api_module{ name = replrtq_mock,
                 functions = [ #api_fun{name = error, arity = 1} ] }.

