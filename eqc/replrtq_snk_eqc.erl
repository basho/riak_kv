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
    not maps:is_key(pid, S).

config_args(_S) ->
    [[{replrtq_enablesink, bool()},
      { replrtq_sink1queue, elements([disabled | sink_names()])},
      { replrtq_sink1peers, ?LET(N, nat(), peers_gen(N+1)) },
      { replrtq_sink1workers, nat() },
      { replrtq_sink2queue, disabled },
      { replrtq_sink2peers, [] },
      { replrtq_sink2workers, nat() }
     ]].

config(Config) ->
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
    Pid.

start_callouts(#{config := Config}, _Args) ->
    ?CALLOUT(app_helper, get_env, [riak_kv, replrtq_enablesink, ?WILDCARD], maps:get(replrtq_enablesink, Config)),
    ?WHEN(maps:get(replrtq_enablesink, Config),
    ?SEQ([?CALLOUT(app_helper, get_env, [riak_kv, replrtq_sink1queue, ?WILDCARD], maps:get(replrtq_sink1queue, Config)),
          ?WHEN(maps:get(replrtq_sink1queue, Config) =/= disabled,
                ?SEQ([?CALLOUT(app_helper, get_env, [riak_kv, replrtq_sink1peers], pp_peers(maps:get(replrtq_sink1peers, Config))),
                      ?CALLOUT(app_helper, get_env, [riak_kv, replrtq_sink1workers], maps:get(replrtq_sink1workers, Config)) ] ++
                         [ ?SEQ(?CALLOUT(riak_client, new, [?WILDCARD, undefined], riak_client),
                                ?CALLOUT(rhc, create, [?WILDCARD, ?WILDCARD, ?WILDCARD, ?WILDCARD], {Host, Port})) ||
                             {Host, Port} <- maps:get(replrtq_sink1peers, Config)])),
          ?CALLOUT(app_helper, get_env, [riak_kv, replrtq_sink2queue, ?WILDCARD], maps:get(replrtq_sink2queue, Config)),
          ?WHEN(maps:get(replrtq_sink2queue, Config) =/= disabled,
                ?SEQ(?OPTIONAL(?CALLOUT(app_helper, get_env, [riak_kv, replrtq_sink2peers], pp_peers(maps:get(replrtq_sink2peers, Config)))),
                     ?OPTIONAL(?CALLOUT(app_helper, get_env, [riak_kv, replrtq_sink2workers], maps:get(replrtq_sink2workers, Config)))))])).

start_next(#{config := Config} = S, Value, _Args) ->
    case maps:get(replrtq_enablesink, Config) of
        true ->
            Sinks = [ {maps:get(replrtq_sink1queue, Config),
                       #{peers => maps:get(replrtq_sink1peers, Config),
                         workers => maps:get(replrtq_sink1workers, Config),
                         workitems => []}} || maps:get(replrtq_sink1queue, Config) =/= disabled ] ++
                [ {maps:get(replrtq_sink2queue, Config),
                   #{peers => maps:get(replrtq_sink2peers, Config),
                     workers => maps:get(replrtq_sink2workers, Config),
                     workitems => []}} || maps:get(replrtq_sink2queue, Config) =/= disabled ],
            S#{pid => Value, sinks => Sinks};
        false ->
            S#{pid => Value, sinks => []}
    end.

start_post(_S, _Args, Res) ->
    is_pid(Res) andalso is_process_alive(Res).

start_process(_S, []) ->
    worker.


%% --- Operation: crash ---
crash_pre(S) ->
    maps:is_key(pid, S).

crash_args(S) ->
    [maps:get(pid, S)].

crash_pre(S, [Pid]) ->
    %% for shrinking
    maps:get(pid, S) == Pid.

crash(Pid) ->
    exit(Pid, kill).

crash_next(S, Value, [_]) ->
    maps:remove(pid, S).


%% --- Operation: prompt_work ---
prompt_work_pre(S) ->
    maps:is_key(pid, S).

prompt_work_args(_S) ->
    [].

prompt_work() ->
    riak_kv_replrtq_snk:prompt_work(),
    timer:sleep(10).

prompt_work_callouts(S, []) ->
   ?APPLY(work, []).

prompt_work_post(_S, [], Res) ->
    eq(Res, ok).


%% --- Operation: fetcher ---
fetcher_pre(S) ->
    maps:is_key(pid, S) andalso workitems(S) =/= [].

fetcher_args(S) ->
    [elements(workitems(S))].

fetcher_pre(S, [WorkItem]) ->
    lists:member(WorkItem, workitems(S)).

fetcher(WorkItem) ->
    riak_kv_replrtq_snk:repl_fetcher(WorkItem).

fetcher_callouts(_S, [WorkItem]) ->
    ?APPLY(fetchwork, []),
    ?APPLY(donework, [WorkItem]),
    ?APPLY(work, []).


work_callouts(S, []) ->
    ?SEQ([?CALLOUTS( ?MATCH({WorkItem, _}, ?CALLOUT(replrtq_mock, work, [?VAR], ok)),
                     ?APPLY(workitem, [Sink, WorkItem])) ||
             {Sink, #{workers := W, workitems := Ws}} <- maps:get(sinks, S),
             _ <- lists:seq(1, max(0, W - length(Ws))) ]).


workitem_next(#{sinks := Sinks} = S, _, [Sink, WorkItem]) ->
    S#{sinks => [ case S == Sink of
                      false -> {S, Map};
                      true -> {S, Map#{workitems => maps:get(workitems, Map) ++ [WorkItem]}}
                  end || {S, Map} <- Sinks]}.


fetchwork_callouts(S, []) ->
    ?MATCH({Sink, Ret},
           ?CALLOUT(rhc, fetch, [{?WILDCARD, ?WILDCARD}, ?VAR],
                    oneof([{ok, queue_empty},
                           {ok, {deleted, 0, object}},
                           {ok, object},
                           error]))),
    case Ret of
        {ok, queue_empty} ->
            ?CALLOUT(replrtq_mock, adjust_wait, [], 1);
        {ok, {deleted, _, Obj}} ->
            ?CALLOUT(riak_client, push, [ Obj, true, ?WILDCARD, ?WILDCARD], {ok, {nat(), nat(), nat()}}),
            ?CALLOUT(replrtq_mock, adjust_wait, [], 0);
        {ok, Obj} ->
            ?CALLOUT(riak_client, push, [ Obj, false, ?WILDCARD, ?WILDCARD], {ok, {nat(), nat(), nat()}}),
            ?CALLOUT(replrtq_mock, adjust_wait, [], 0);
        error ->
            ?CALLOUT(lager, warning, [?WILDCARD, ?WILDCARD], ok),
            ?CALLOUT(replrtq_mock, adjust_wait, [], 5)
    end.

donework_next(#{sinks := Sinks} = S, _Value, [WorkItem]) ->
    NewSinks = rm_first(WorkItem, Sinks),
    S#{sinks => NewSinks}.

rm_first(WorkItem, [{Sink, #{workitems := Ws} = Map}|Rest]) ->
    case lists:member(WorkItem, Ws) of
        true ->
            [{Sink, Map#{workitems => Ws -- [WorkItem]}} | Rest ];
        false ->
            [{Sink, Map} | rm_first(WorkItem, Rest) ]
    end;
rm_first(_, []) ->
    [].


%% -- Generators -------------------------------------------------------------

%% -- Helper functions

pp_peers(Peers) ->
    string:join([ pp_peer(Peer) || Peer <- Peers], "|").

pp_peer({Name, Port}) ->
    lists:concat([Name, ":", Port]).

sink_names() ->
   [ sink1, sink2, a, aa, b, c ].

peers_gen(N) ->
    ?LET(Words, eqc_erlang_program:words(N),
         [ {Word, ?LET(P, nat(), P+8000)} || Word <- Words]).

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
        {H, S, Res} = run_commands(Cmds),
        Crashed =
            case maps:get(pid, S, undefined) of
                undefined -> false;
                Pid ->
                    HasCrashed = not is_process_alive(Pid),
                    exit(Pid, kill),
                    HasCrashed
            end,
        check_command_names(Cmds,
            measure(length, commands_length(Cmds),
                pretty_commands(?MODULE, Cmds, {H, S, Res},
                                conjunction([{result, Res == ok},
                                             {alive, not Crashed}]))))
    end))).



%% -- API-spec ---------------------------------------------------------------
api_spec() ->
    #api_spec{ language = erlang, mocking = eqc_mocking,
               modules = [ app_helper_spec(), lager_spec(), rhc_spec(), riak_client_spec(), mock_spec() ] }.

app_helper_spec() ->
    #api_module{ name = app_helper, fallback = undefined,
                 functions =
                     [  #api_fun{ name = get_env, arity = 3, matched = all, fallback = false},
                        #api_fun{ name = get_env, arity = 2, matched = all, fallback = false} ] }.

lager_spec() ->
    #api_module{ name = lager, fallback = undefined,
                 functions =
                     [  #api_fun{ name = warning, arity = 2, matched = all, fallback = false} ] }.

rhc_spec() ->
    #api_module{ name = rhc, fallback = undefined,
                 functions =
                     [  #api_fun{ name = create, arity = 4, matched = all, fallback = false},
                        #api_fun{ name = fetch, arity = 2, matched = all, fallback = false} ] }.

riak_client_spec() ->
    #api_module{ name = riak_client, fallback = undefined,
                 functions =
                     [  #api_fun{ name = push, arity = 4, matched = all, fallback = false},
                        #api_fun{ name = new, arity = 2, matched = all, fallback = false} ] }.

mock_spec() ->
    #api_module{ name = replrtq_mock, fallback = undefined,
                 functions =
                     [  #api_fun{ name = work, arity = 1, matched = all, fallback = false},
                        #api_fun{ name = adjust_wait, arity = 0, matched = all, fallback = false} ] }.


%% This is a pure data structure, no need to mock it, rather use it
priority_queue_spec() ->
    #api_module{ name = riak_core_priority_queue, fallback = undefined,
                 functions =
                     [  #api_fun{ name = new, arity = 0, matched = all, fallback = false},
                        #api_fun{ name = in, arity = 3, matched = all, fallback = false}  ] }.
