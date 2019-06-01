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

%% -- Common pre-/post-conditions --------------------------------------------
command_precondition_common(_S, _Cmd) ->
    true.

precondition_common(_S, _Call) ->
    true.

%% postcondition_common(S, Call, Res) ->
%%     eq(Res, return_value(S, Call)). %% Check all return values

%% -- Operations -------------------------------------------------------------

%% --- Operation: config ---
config_pre(S) ->
    %% Do not change config while running
    not maps:is_key(pid, S).

config_args(_S) ->
    [[{replrtq_srcqueue, queuedef()},
      {replrtq_srcqueuelimit, choose(0,100)}  %% length of each individual queue
     ]].

config_pre(_S, [_Config]) ->
    true.

config(Config) ->
    QueueDefs = pp_queuedefs(proplists:get_value(replrtq_srcqueue, Config)),
    application:set_env(riak_kv, replrtq_srcqueue,
                        QueueDefs),
    application:set_env(riak_kv, replrtq_srcqueuelimit,
                        proplists:get_value(replrtq_srcqueuelimit, Config)),
    maps:from_list(Config),
    QueueDefs.

config_next(S, _Value, [Config]) ->
    S#{config => maps:from_list(Config)}.

config_post(_S, [_], _Res) ->
    true.


%% --- Operation: start ---
start_pre(S) ->
    not maps:is_key(pid, S) andalso maps:is_key(config, S).

start_args(_S) ->
    [].

start() ->
    {ok, Pid} = riak_kv_replrtq_src:start_link(),
    unlink(Pid),
    Pid.

start_callouts(#{config := Config}, _Args) ->
    %% Mocking... in OTP20 app_helper can be replaced and then these can be removed
    ?CALLOUT(app_helper, get_env, [riak_kv, replrtq_srcqueue, ?WILDCARD],
             pp_queuedefs(maps:get(replrtq_srcqueue, Config))),
    ?CALLOUT(app_helper, get_env, [riak_kv, replrtq_srcqueuelimit, ?WILDCARD],
             maps:get(replrtq_srcqueuelimit, Config)),
    ?CALLOUT(app_helper, get_env, [riak_kv, replrtq_logfrequency, ?WILDCARD], 500000).  %% lager not defined and crashes if called

start_next(S, Value, _Args) ->
    Queues = [ {Name, #{filter => Filter}} ||
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

crash_post(_S, [_], _Res) ->
    true.


%% --- Operation: coordput ---
%% Adds the enrty to any queue that it passes the filter for
coordput_pre(S) ->
    maps:is_key(pid, S).

coordput_args(S) ->
    Queues = maps:get(priority_queues, S, #{}),
    Buckets = Types =
        [ Word || #{filter := {_, Word}} <- maps:values(Queues) ] ++ ["anything", "type"],
    [?LET({Bucket, PostFix}, {weighted_default({10, elements(Buckets)}, {1, list(choose($a, $z))}),
                              weighted_default({10, <<>>}, {2, utf8()})},
          frequency([{10, {Bucket ++ binary_to_list(PostFix), binary(), vclock, obj_ref}},
                    {1, {{elements(Types), Bucket ++ binary_to_list(PostFix)}, binary(), vclock, obj_ref}}]))].

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
                            Acc#{QueueName => PQueues#{Prio => PQueue ++ [Entry]}}
                    end, Queues, ApplicableQueues),
    S#{priority_queues => NewQueues}.

coordput_post(_S, [_Entry], Res) ->
    eq(Res, ok).


%% --- Operation: lengths ---
%% Suggestion: let length return a map... more future proof.
%% E.g. #{1 => 12, 2 => 5, 3 => 0}. Or a list with implicit priority [12, 5, 0]
lengths_pre(S) ->
    maps:is_key(pid, S).

lengths_args(S) ->
    case maps:keys(maps:get(priority_queues, S, #{})) of
        [] -> ["none_existing_queue"];
        Names ->
            [elements(Names)]
    end.

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






%% --- ... more operations

%% -- Generators -------------------------------------------------------------

%% Generate symbolic and then make string out of it
queuedef() ->
    ?LET(QueueNames, sublist(queuenames()),
        [{QN, queuefilter()} || QN <- QueueNames]).

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

%% -- Helper functions

pp_queuedefs(Qds) ->
    string:join([ pp_queuedef(Qd) || Qd <- Qds], "|").

pp_queuedef({Name, {Filter, Arg}}) ->
    lists:concat([Name, ":", Filter, ".", Arg]);
pp_queuedef({Name, Filter}) ->
    lists:concat([Name, ":", Filter]).

applicable_queues(S, Priority, Bucket) ->
    Limit = maps:get(replrtq_srcqueuelimit, maps:get(config, S)),
    Queues = maps:get(priority_queues, S, #{}),
    QueueDefs = [ {Name, Filter} || {Name, #{filter := Filter}} <- maps:to_list(Queues) ],
    %% if filter matches and limit is not reached
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
%% This might be privacy issue. Assume there is a unicode string that one searches for
%% that only occurs once, but you find many more that clearly are no real matches...
%% Thus, a patient with a weird character in the name and then many johnssons are returned
%% as well.
%% In particular, if someone has a HEBREW ACCENT ETNAHTA in the name, then searching up to
%% that accent could give additional matches.
%% But actually, this is not a real issue, one could also serach for any shorter prefix and
%% then succeed.
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





%% -- Property ---------------------------------------------------------------
%% invariant(_S) ->
%% true.

weight(_S, start) -> 1;
weight(_S, stop)  -> 2;
weight(_S, crash) -> 1;
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
                    catch riak_kv_replrtq_src:stop(),
                    HasCrashed
            end,
        %% timer:sleep(500),
        check_command_names(Cmds,
            measure(length, commands_length(Cmds),
                pretty_commands(?MODULE, Cmds, {H, S, Res},
                                conjunction([{result, Res == ok},
                                             {alive, not Crashed}]))))
    end))).



%% -- API-spec ---------------------------------------------------------------
api_spec() ->
    #api_spec{ language = erlang, mocking = eqc_mocking,
               modules = [ app_helper_spec(), lager_spec() ] }.

app_helper_spec() ->
    #api_module{ name = app_helper, fallback = undefined,
                 functions =
                     [  #api_fun{ name = get_env, arity = 3, matched = all, fallback = false} ] }.

lager_spec() ->
    #api_module{ name = lager, fallback = undefined,
                 functions =
                     [  #api_fun{ name = warning, arity = 2, matched = all, fallback = false} ] }.

%% This is a pure data structure, no need to mock it, rather use it
priority_queue_spec() ->
    #api_module{ name = riak_core_priority_queue, fallback = undefined,
                 functions =
                     [  #api_fun{ name = new, arity = 0, matched = all, fallback = false},
                        #api_fun{ name = in, arity = 3, matched = all, fallback = false}  ] }.
