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

%% -- State ------------------------------------------------------------------
initial_state() ->
    #{}.

%% -- Operations -------------------------------------------------------------

%% --- Operation: config ---
config_pre(S) ->
    %% Do not change config while running
    not maps:is_key(pid, S) andalso not maps:is_key(config, S).

config_args(_S) ->
    [[{replrtq_enablesink, weighted_default({1, false}, {19, true})},
      { replrtq_sink1queue, sink_queue_gen()},
      { replrtq_sink1peers, peers_gen() },
      { replrtq_sink1workers, workers_gen() },
      { replrtq_sink2queue, disabled}
     ]].

config_pre(_, [Config]) ->
    Sink1 = proplists:get_value(replrtq_sink1queue, Config),
    Sink2 = proplists:get_value(replrtq_sink2queue, Config),
    Sink1 /= Sink2 orelse Sink1 == disabled.

config(Config) ->
    Format = fun(replrtq_sink1peers, Peers) -> pp_peers(Peers);
                (replrtq_sink2peers, Peers) -> pp_peers(Peers);
                (_, Val) -> Val end,
    [ application:set_env(riak_kv, Key, Format(Key, Val)) || {Key, Val} <- Config ],
    Sink1Name = proplists:get_value(replrtq_sink1queue, Config),
    Sink2Name = proplists:get_value(replrtq_sink2queue, Config),
    Peers = [ {Peer, Name, Cfg}
              || {Name, Key} <- [{Sink1Name, replrtq_sink1peers}, {Sink2Name, replrtq_sink2peers}],
                 Name /= disabled,
                 {Peer, Cfg} <- proplists:get_value(Key, Config) ],
    replrtq_snk_monitor:setup_peers(Peers),
    ok.

config_next(S, _Value, [Config]) ->
    S#{config => maps:from_list(Config)}.


%% --- Operation: start ---
start_pre(S) ->
    not maps:is_key(pid, S) andalso maps:is_key(config, S).

start_args(_S) ->
    [].

start() ->
    {ok, Pid} = riak_kv_replrtq_snk:start_link(),
    unlink(Pid),
    riak_kv_replrtq_snk:prompt_work(),
    timer:sleep(100),
    Pid.

start_callouts(#{config := Config}, _Args) ->
    case maps:get(replrtq_enablesink, Config) of
        false -> ?EMPTY;
        true  ->
            ?APPLY(setup_sink, [replrtq_sink1queue, replrtq_sink1peers]),
            ?APPLY(setup_sink, [replrtq_sink2queue, replrtq_sink2peers])
    end.

setup_sink_callouts(#{config := Config}, [Queue, Peers]) ->
    ?SEQ([ ?CALLOUT(rhc, create, [?WILDCARD, ?WILDCARD, ?WILDCARD, ?WILDCARD], {Host, Port})
         || maps:get(Queue, Config) /= disabled,
            {{Host, Port}, _} <- maps:get(Peers, Config)]).

start_next(#{config := Config} = S, Pid, _Args) ->
    case maps:get(replrtq_enablesink, Config) of
        true ->
            Sinks = [ {maps:get(replrtq_sink1queue, Config),
                       #{peers => maps:get(replrtq_sink1peers, Config),
                         workers => maps:get(replrtq_sink1workers, Config)}}
                    || maps:get(replrtq_sink1queue, Config) =/= disabled ] ++
                    [ {maps:get(replrtq_sink2queue, Config),
                       #{peers => maps:get(replrtq_sink2peers, Config),
                         workers => maps:get(replrtq_sink2workers, Config)}}
                    || maps:get(replrtq_sink2queue, Config) =/= disabled ],
            S#{pid => Pid, sinks => Sinks};
        false ->
            S#{pid => Pid, sinks => []}
    end.

start_post(_S, _Args, Res) ->
    is_pid(Res) andalso is_process_alive(Res).

start_process(_S, []) ->
    worker.

%% -- Generators -------------------------------------------------------------

%% -- Helper functions

pp_peers(Peers) ->
    string:join([ pp_peer(Peer) || {Peer, _Cfg} <- Peers], "|").

pp_peer({Name, Port}) ->
    lists:concat([Name, ":", Port]).

sink_names() ->
   [ sink1, sink2, a, aa, b, c ].

sink_queue_gen() ->
    weighted_default({1, disabled}, {9, elements(sink_names())}).

workers_gen() ->
    frequency([{1, 0}, {10, choose(1, 5)}]).

peers_gen() ->
    ?LET(N, choose(1, 8), peers_gen(N)).

peers_gen(N) ->
    ?LET(Words, eqc_erlang_program:words(N),
         [ {{Word, ?LET(P, nat(), P + 8000)}, peer_config_gen()} || Word <- Words]).

peer_config_gen() ->
    {elements([inactive, active]),
     choose(1, 10)}.    %% Response time

workitems(#{sinks := Sinks}) ->
    lists:foldl(fun({_, #{workitems := Ws}}, Acc) ->
                        Acc ++ Ws
                end, [], Sinks).



%% -- Property ---------------------------------------------------------------

%% invariant(_S) ->
%% true.

weight(_S, start) -> 1;
weight(_S, stop)  -> 2;
weight(_S, crash) -> 1;
weight(_S, config) -> 1;
weight(_S, _Cmd) -> 10.

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
        {H, S, Res} = run_commands(Cmds),
        Trace = replrtq_snk_monitor:stop(),
        Crashed =
            case maps:get(pid, S, undefined) of
                undefined -> false;
                Pid when is_pid(Pid) ->
                    HasCrashed = not is_process_alive(Pid),
                    exit(Pid, kill),
                    HasCrashed;
                Other -> {not_pid, Other}
            end,
        ?WHENFAIL(eqc:format("Trace:\n~p\n", [Trace]),
        check_command_names(Cmds,
            measure(length, commands_length(Cmds),
            measure(trace, length(Trace),
                pretty_commands(?MODULE, Cmds, {H, S, Res},
                                conjunction([{result, equals(Res, ok)},
                                             {trace, check_trace(S, Trace)},
                                             {alive, equals(Crashed, false)}]))))))
    end))).

peers(#{sinks := [{_, #{peers := Peers}}]}) ->
    [ {P, A} || {P, {A, _}} <- Peers ];
peers(_) -> [].

workers(#{sinks := [{_, #{workers := N}}]}) -> N;
workers(_) -> 0.

check_trace(S, Trace) ->
    Workers    = workers(S),
    Concurrent = concurrent_fetches(Trace),
    Max        = lists:max([0 | [N || {_, N} <- Concurrent]]),
    Avg        = weighted_average(Concurrent),
    AnyActive  = lists:keymember(active, 2, peers(S)),
    measure(idle, if AnyActive -> Workers - Avg; true -> 0 end,
    conjunction([{max_workers, equals(Workers, Max)},
                 {avg_workers,
                    %% Check that the average number of active workers is >= WorkerCount - 1
                    ?WHENFAIL(eqc:format("Concurrent = ~p\n~.1f < ~p\n", [Concurrent, Avg, Workers - 1]),
                        Avg >= Workers - 1 orelse not AnyActive)}])).

weighted_average([]) -> 0;
weighted_average(Xs) ->
    lists:sum([ W * X || {W, X} <- Xs]) /
    lists:sum([ W     || {W, _} <- Xs]).

concurrent_fetches([]) -> [];
concurrent_fetches(Trace = [E | _]) ->
    T0 = element(2, E),
    concurrent_fetches(Trace, T0, 0, []).

concurrent_fetches([], _, _, Acc) ->
    lists:reverse(Acc);
concurrent_fetches([E | Trace], T0, N, Acc) ->
    T1 = element(2, E),
    N1 = case element(1, E) of
            return -> N - 1;
            fetch  -> N + 1
         end,
    concurrent_fetches(Trace, T1, N1, [{timer:now_diff(T1, T0), N} | Acc]).

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

