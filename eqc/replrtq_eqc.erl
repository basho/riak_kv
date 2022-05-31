%%% @author Thomas Arts <thomas@SpaceGrey.local>
%%% @copyright (C) 2019, Thomas Arts
%%% @doc The replrtq_src server is started with a one-to-one strategy in the
%%%      supervisor tree riak_kv_sup. In other words, it should handle the
%%%      cases in which one of its communication parties is down for a while.
%%%
%%%      Since we do want to test fault tolerance w.r.t. the other modules,
%%%      we model stopping, starting and crashing here.
%%%
%%% @end
%%% Created : 27 May 2019 by Thomas Arts <thomas@SpaceGrey.local>

-module(replrtq_eqc).

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
    [[{replrtq_srcqueue, queuedef()},
      {replrtq_overflow_limit, choose(0,100)}  %% length of each individual queue
     ]].

config(Config) ->
    QueueDefs = pp_queuedefs(proplists:get_value(replrtq_srcqueue, Config)),
    application:set_env(riak_kv, replrtq_srcqueue,
                        QueueDefs),
    application:set_env(riak_kv, replrtq_overflow_limit,
                        proplists:get_value(replrtq_overflow_limit, Config)),
    application:set_env(riak_kv, replrtq_logfrequency, 500000),
    maps:from_list(Config),
    QueueDefs.

config_next(S, _Value, [Config]) ->
    S#{config => maps:from_list(Config)}.


%% --- Operation: start ---
start_pre(S) ->
    not maps:is_key(pid, S) andalso maps:is_key(config, S).

start_args(_S) ->
    [].

start() ->
    FilePath = riak_kv_test_util:get_test_dir("replrtq_eqc"),
    {ok, Pid} = riak_kv_replrtq_src:start_link(FilePath),
    unlink(Pid),
    Pid.

start_next(S, Value, _Args) ->
    Queues = [ {Name, #{filter => Filter, status => active}} ||
                 {Name, Filter} <- maps:get(replrtq_srcqueue, maps:get(config, S, #{}), [])],
    S#{pid => Value, priority_queues => maps:from_list(Queues)}.

start_post(_S, _Args, Res) ->
    is_pid(Res) andalso is_process_alive(Res).

%% --- Operation: stop ---
stop_pre(S) ->
    maps:is_key(pid, S).

stop_args(_S) ->
    [].

stop() ->
    riak_kv_replrtq_src:stop().

stop_next(S, _Value, []) ->
    maps:remove(pid, S).

stop_post(_S, [], Res) ->
    eq(Res, ok).

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
    stop_next(S, Value, []).


%% --- Operation: coordput ---
%% Adds the enrty to any queue that it passes the filter for
coordput_pre(S) ->
    maps:is_key(pid, S).

coordput_args(S) ->
    Queues = maps:get(priority_queues, S),
    entries_gen(Queues, 1).

coordput({{Type, Bucket}, Key, VClock, ObjRef}) ->
    riak_kv_replrtq_src:replrtq_coordput({{list_to_binary(Type), list_to_binary(Bucket)}, Key, VClock, ObjRef});
coordput({Bucket, Key, VClock, ObjRef}) ->
    riak_kv_replrtq_src:replrtq_coordput({list_to_binary(Bucket), Key, VClock, ObjRef}).


coordput_next(S, _Value, [{Bucket, _, _, _} = Entry]) ->
    Prio = 3,  %% coordinated put has prio 3
    ApplicableQueues = applicable_queues(S, Prio, Bucket),
    Queues = maps:get(priority_queues, S, #{}),
    NewQueues =
        lists:foldl(fun(QueueName, Acc) ->
                            PQueues = maps:get(QueueName, Acc),
                            PQueue = maps:get(Prio, PQueues, []),
                            Acc#{QueueName => PQueues#{Prio => add_to_buckets(PQueue,[Entry])}}
                    end, Queues, ApplicableQueues),
    S#{priority_queues => NewQueues}.

coordput_post(_S, [_Entry], Res) ->
    eq(Res, ok).

%% --- Operation: ticktac ---
tictac_pre(S) ->
    maps:is_key(pid, S).

tictac_args(S) ->
    Queues = maps:get(priority_queues, S),
    [elements(queuenames()),  ?LET(N, nat(), entries_gen(Queues, N))].

tictac(QueueName, Entries) ->
    ReplEntries =
        lists:map(fun({{Type, Bucket}, Key, VClock, ObjRef}) ->
                          {{list_to_binary(Type), list_to_binary(Bucket)}, Key, VClock, ObjRef};
                     ({Bucket, Key, VClock, ObjRef}) ->
                          {list_to_binary(Bucket), Key, VClock, ObjRef}
                  end, Entries),
    riak_kv_replrtq_src:replrtq_ttaaefs(QueueName, ReplEntries).

tictac_callouts(_S, [QueueName, Entries]) ->
    ?APPLY(put_prio, [2, QueueName, Entries]).

put_prio_post(S, [_Prio, QueueName, _Entries], Res) ->
    Queues = maps:get(priority_queues, S),
    case maps:get(QueueName, Queues, undefined) of
        #{status := active} ->
            eq(Res, ok)
    end.


put_prio_callouts(S, [Prio, QueueName, Entries]) ->
    Queues = maps:get(priority_queues, S),
    Limit = maps:get(replrtq_overflow_limit, maps:get(config, S)),
    case maps:get(QueueName, Queues, undefined) of
        #{status := active} = PQueues ->
            PQueue = maps:get(Prio, PQueues, []),
            {_NextC, ToAdd} =
                lists:foldr(
                    fun(X, {C, Acc}) ->
                        case C of
                            Limit ->
                                {C, Acc};
                            _ ->
                                {C + 1, [X|Acc]}
                        end
                    end,
                    {length(PQueue), []},
                    Entries),
            ?WHEN(length(ToAdd) + length(PQueue) =< Limit,
                  ?APPLY(add_prio, [Prio, QueueName, ToAdd]));
        _ ->
            ?EMPTY
    end.

add_prio_next(S, _Value, [Prio, QueueName, Entries]) ->
    Queues = maps:get(priority_queues, S),
    PQueues = maps:get(QueueName, Queues),
    PQueue = maps:get(Prio, PQueues, []),
    S#{priority_queues => Queues#{QueueName => PQueues#{Prio => add_to_buckets(PQueue, Entries)}}}.


%% --- Operation: ticktac ---
aaefold_pre(S) ->
    maps:is_key(pid, S).

aaefold_args(S) ->
    Queues = maps:get(priority_queues, S),
    [elements(queuenames()),  ?LET(N, nat(), entries_gen(Queues, N))].

aaefold(QueueName, Entries) ->
    ReplEntries =
        lists:map(fun({{Type, Bucket}, Key, VClock, ObjRef}) ->
                          {{list_to_binary(Type), list_to_binary(Bucket)}, Key, VClock, ObjRef};
                     ({Bucket, Key, VClock, ObjRef}) ->
                          {list_to_binary(Bucket), Key, VClock, ObjRef}
                  end, Entries),
    riak_kv_replrtq_src:replrtq_aaefold(QueueName, ReplEntries).

aaefold_callouts(_S, [QueueName, Entries]) ->
    ?APPLY(put_prio, [1, QueueName, Entries]).



%% --- Operation: lengths ---
%% Suggestion: let length return a map... more future proof.
%% E.g. #{1 => 12, 2 => 5, 3 => 0}. Or a list with implicit priority [12, 5, 0]
lengths_pre(S) ->
    maps:is_key(pid, S).

lengths_args(_S) ->
    [elements(queuenames())].

lengths(QueueName) ->
    riak_kv_replrtq_src:length_rtq(QueueName).

lengths_post(S, [QueueName], Res) ->
    Queues = maps:get(priority_queues, S, #{}),
    case maps:get(QueueName, Queues, undefined) of
        undefined ->
            eq(Res, false);
        Queue ->
            eq(Res, {QueueName,
                     {length(maps:get(1, Queue, [])),
                      length(maps:get(2, Queue, [])),
                      length(maps:get(3, Queue, []))}})
    end.

%% --- Operation: register_rtq ---
register_rtq_pre(S) ->
    maps:is_key(pid, S).

register_rtq_args(_S) ->
    [elements(queuenames()), queuefilter()].

register_rtq(Name, {Kind, Word}) ->
    riak_kv_replrtq_src:register_rtq(Name, {Kind, list_to_binary(Word)});
register_rtq(Name, Kind) ->
    riak_kv_replrtq_src:register_rtq(Name, Kind).

register_rtq_next(S, _Value, [Name, Filter]) ->
    Queues = maps:get(priority_queues, S),
    %% Use merge to create a NOP if queue name already taken
    S#{priority_queues => maps:merge(#{Name => #{filter => Filter,
                                                 status => active}}, Queues)}.

register_rtq_post(S, [Name, _Filter], Res) ->
    Queues = maps:get(priority_queues, S),
    case Res of
        true -> not maps:is_key(Name, Queues);
        false -> maps:is_key(Name, Queues)
    end.

%% --- Operation: delist_rtq ---
delist_rtq_pre(S) ->
    maps:is_key(pid, S).

delist_rtq_args(_S) ->
    [elements(queuenames())].

delist_rtq(Name) ->
    riak_kv_replrtq_src:delist_rtq(Name).

delist_rtq_next(S, _Value, [Name]) ->
    Queues = maps:get(priority_queues, S),
    S#{priority_queues => maps:remove(Name, Queues)}.

delist_rtq_post(_S, [_Name], Res) ->
    eq(Res, ok).


%% --- Operation: suspend_rtq ---
suspend_rtq_pre(S) ->
    maps:is_key(pid, S).

suspend_rtq_args(_S) ->
    [elements(queuenames())].

suspend_rtq(Name) ->
    riak_kv_replrtq_src:suspend_rtq(Name).

suspend_rtq_next(S, _Value, [Name]) ->
    Queues = maps:get(priority_queues, S),
    case maps:get(Name, Queues, undefined) of
        undefined -> S;
        PQueue ->
            S#{priority_queues => Queues#{Name => PQueue#{status => suspended}}}
    end.

suspend_rtq_post(S, [Name], Res) ->
    Queues = maps:get(priority_queues, S),
    case Res of
        not_found -> not maps:is_key(Name, Queues);
        ok -> maps:is_key(Name, Queues)
    end.

%% --- Operation: resume_rtq ---
resume_rtq_pre(S) ->
    maps:is_key(pid, S).

resume_rtq_args(_S) ->
    [elements(queuenames())].

resume_rtq(Name) ->
    riak_kv_replrtq_src:resume_rtq(Name).

resume_rtq_next(S, _Value, [Name]) ->
    Queues = maps:get(priority_queues, S),
    case maps:get(Name, Queues, undefined) of
        undefined -> S;
        PQueue ->
            S#{priority_queues => Queues#{Name => PQueue#{status => active}}}
    end.

resume_rtq_post(S, [Name], Res) ->
    Queues = maps:get(priority_queues, S),
    case Res of
        not_found -> not maps:is_key(Name, Queues);
        ok -> maps:is_key(Name, Queues)
    end.

%% --- Operation: pop_rtq ---
pop_rtq_pre(S) ->
    maps:is_key(pid, S).

pop_rtq_args(_S) ->
    [elements(queuenames())].

pop_rtq(Name) ->
    riak_kv_replrtq_src:popfrom_rtq(Name).

pop_rtq_next(S, _Value, [Name]) ->
    Queues = maps:get(priority_queues, S),
    case maps:get(Name, Queues, undefined) of
        undefined -> S;
        PQueue ->
            {_, NewPQueue} = queue_pop_highest(PQueue),
            S#{priority_queues => Queues#{Name => NewPQueue}}
    end.

pop_rtq_post(S, [Name], Res) ->
    Queues = maps:get(priority_queues, S),
    case maps:get(Name, Queues, undefined) of
        undefined -> eq(queue_empty, Res);  %% This is weird., rather not_found as in other API calls
        PQueue ->
            case queue_pop_highest(PQueue) of
                {empty, _} -> eq(Res, queue_empty);
                {{{T, B}, K, V, Obj}, _} -> eq(Res, {{list_to_binary(T), list_to_binary(B)}, K, V, Obj});
                {{B, K, V, Obj}, _} -> eq(Res, {list_to_binary(B), K, V, Obj})
            end
    end.


%% -- Generators -------------------------------------------------------------

%% Generate symbolic and then make string out of it
queuedef() ->
    ?LET(Qs, [{QN, queuefilter()} || QN <- queuenames()],
    sublist(Qs)).

%% Queue names are atoms, only a limited, known subset to choose from
%% to simplify verification of the filters
queuenames() ->
    [ a, aa, b, c, long, short ].

queuefilter() ->
    ?LET([Word], noshrink(eqc_erlang_program:words(1)),
         elements([any,
                   block_rtq,   %% Seems not to have any effect
                   {bucketname, Word},
                   {bucketprefix, Word},
                   {buckettype, Word}])).
                %% no fault injection yet: nonsense, {nonsense, Word}.

entries_gen(Queues, N) ->
    Buckets = Types =
        [ Word || #{filter := {_, Word}} <- maps:values(Queues) ] ++ ["anything", "type"],
    vector(N,
           ?LET({Bucket, PostFix}, {weighted_default({10, elements(Buckets)}, {1, list(choose($a, $z))}),
                                    weighted_default({10, <<>>}, {2, utf8()})},
                frequency([{10, {Bucket ++ binary_to_list(PostFix), binary(), vclock, obj_ref}},
                           {1, {{elements(Types), Bucket ++ binary_to_list(PostFix)}, binary(), vclock, obj_ref}}]))).

%% -- Helper functions

pp_queuedefs(Qds) ->
    string:join([ pp_queuedef(Qd) || Qd <- Qds], "|").

pp_queuedef({Name, {Filter, Arg}}) ->
    lists:concat([Name, ":", Filter, ".", Arg]);
pp_queuedef({Name, Filter}) ->
    lists:concat([Name, ":", Filter]).

queue_pop_highest(PQueue) ->
    case lists:reverse(
           lists:sort([ {Prio, maps:get(Prio, PQueue)} || Prio <- maps:keys(PQueue),
                                                        is_integer(Prio),
                                                        maps:get(Prio, PQueue) =/= []])) of
        [] -> {empty, PQueue};
        [{Prio, [E|Rest]}|_] -> {E, PQueue#{Prio => Rest}}
    end.



applicable_queues(S, Priority, Bucket) ->
    Limit = maps:get(replrtq_overflow_limit, maps:get(config, S)),
    Queues = maps:get(priority_queues, S, #{}),
    QueueDefs = [ {Name, Filter} || {Name, #{filter := Filter, status := active}} <- maps:to_list(Queues) ],
    %% if filter matches and limit is not reached
    %% it filters out block_rtq
    ApplicableQueues =
        [ Name || {Name, any} <- QueueDefs] ++
        [ Name || {Name, {bucketname, Word}} <- QueueDefs,   is_equal(Bucket, Word) ] ++
        [ Name || {Name, {bucketprefix, Word}} <- QueueDefs, is_prefix(Bucket, Word) ] ++
        [ Name || {Name, {buckettype, Word}} <- QueueDefs,   is_tuple(Bucket), string:equal(element(1, Bucket), Word) ],
    lists:filter(fun(QueueName) ->
                         PQueues = maps:get(QueueName, Queues),
                         length(maps:get(Priority, PQueues, [])) < Limit
                    end, ApplicableQueues).

%% Tricky unicode issues
is_prefix({_Type, String}, Prefix) ->
    is_prefix(String, Prefix);
is_prefix(String, Prefix) ->
    %% string:prefix(String, Prefix) =/= nomatch.
    %% If the bucket is a unicode string, then it may happen that
    %% string:prefix returns nomatch, even though the binary bytes are a prefix:
    %% string:prefix(<<108, 97, 219, 159>>, "la") --> nomatch  "la" == <<108,97>>
    lists:prefix(Prefix, String).

is_equal({_Type, String}, Word) ->
    string:equal(String, Word);
is_equal(String, Word) ->
    string:equal(String, Word).


add_to_buckets(Queue, Entries) ->
    Queue ++ lists:reverse(Entries).


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
                fun() -> eqc_mocking:stop_mocking() end
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
                    catch riak_kv_replrtq_src:stop(),
                    HasCrashed
            end,
        check_command_names(Cmds,
            measure(length, commands_length(Cmds),
            measure(queues, maps:size(maps:get(priority_queues, S, #{})),
            aggregate(call_features(H),
                pretty_commands(?MODULE, Cmds, {H, S, Res},
                                conjunction([{result, Res == ok},
                                             {alive, not Crashed}]))))))
    end))).



%% -- API-spec ---------------------------------------------------------------
api_spec() ->
    #api_spec{ language = erlang, mocking = eqc_mocking,
               modules = [ app_helper_spec(), lager_spec() ] }.

app_helper_spec() ->
    #api_module{ name = app_helper, fallback = ?MODULE }.

get_env(riak_kv, Key, Default) ->
  application:get_env(riak_kv, Key, Default).

lager_spec() ->
    #api_module{ name = lager, fallback = ?MODULE }.

warning(_, _) -> ok.

