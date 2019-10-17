-module(rt_source_eqc).

-compile([export_all, nowarn_export_all]).

-ifdef(EQC).
-include("rt_source_eqc.hrl").
-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(P(EXPR), PPP = (EXPR), case PPP of true -> ok; _ -> io:format(user, "PPP ~p at line ~p\n", [PPP, ?LINE]) end, PPP).
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) ->
                              io:format(user, Str, Args) end, P)).

-record(pushed, {
    seq,
    v1_seq,
    push_res,
    skips,
    remotes_up
}).


%% ===================================================================
%% Helper Funcs
%% ===================================================================

-record(ctx, {
    fs_pid,
    fs_port
}).

setup() ->
    % ?debugMsg("enter setup()"),
    application:load(sasl),
    application:set_env(sasl, sasl_error_logger, {file, "rt_source_eqc_sasl.log"}),
    error_logger:tty(false),
    application:start(lager),
    riak_repl_test_util:stop_test_ring(),
    riak_repl_test_util:start_test_ring(),
    riak_repl_test_util:abstract_gen_tcp(),
    riak_repl_test_util:abstract_stats(),
    riak_repl_test_util:abstract_stateful(),
    kill_rtq(),
    {ok, _RTPid} = rt_source_helpers:start_rt(),
    {ok, _RTQPid} = rt_source_helpers:start_rtq(),
    {ok, _TCPMonPid} = rt_source_helpers:start_tcp_mon(),
    %% {ok, _Pid1} = riak_core_service_mgr:start_link(ClusterAddr),
    %% {ok, _Pid2} = riak_core_connection_mgr:start_link(),
    %% {ok, _Pid3} = riak_core_cluster_conn_sup:start_link(),
    %% {ok, _Pid4 } = riak_core_cluster_mgr:start_link(),
    {ok, FsPid, FsPort} = rt_source_helpers:init_fake_sink(),
    ok = rt_source_helpers:abstract_connection_mgr(FsPort),
    R = #ctx{fs_pid = FsPid, fs_port = FsPort},
    % ?debugFmt("leave setup() -> ~p", [R]),
    R.

cleanup(_Ctx) ->
    % ?debugFmt("enter cleanup(~p)", [_Ctx]),
    rt_source_helpers:kill_fake_sink(),
    riak_repl_test_util:kill_and_wait(riak_core_tcp_mon),
    riak_repl2_rtq:stop(),
    riak_repl_test_util:kill_and_wait(riak_repl2_rt),
    riak_repl_test_util:stop_test_ring(),
    meck:unload(),
    % ?debugMsg("leave cleanup(~p)", [_Ctx]),
    ok.

prop_test_() ->
    {spawn,
     [
      {setup,
       fun setup/0,
       fun cleanup/1,
       [%% Run the quickcheck tests
        {timeout, 120,
         ?_assertEqual(true, eqc:quickcheck(eqc:numtests(5, ?QC_OUT(prop_main()))))}
       ]
      }
     ]
    }.

prop_main() ->
    ?FORALL(Cmds, noshrink(commands(?MODULE)),
        aggregate(command_names(Cmds), begin
             {H, S, Res} = run_commands(?MODULE, Cmds),
             process_flag(trap_exit, false),
            pretty_commands(?MODULE, Cmds, {H,S,Res}, Res == ok)
        end)).

%% ====================================================================
%% Generators (including commands)
%% ====================================================================

command(S) ->
    oneof(
        [{call, ?MODULE, connect_to_v1, [remote_name(S), S#state.master_queue]} || S#state.remotes_available /= []] ++
        [{call, ?MODULE, connect_to_v2, [remote_name(S), S#state.master_queue]} || S#state.remotes_available /= []] ++
        [{call, ?MODULE, disconnect, [elements(S#state.sources)]} || S#state.sources /= []] ++
        % push an object that may already have been rt'ed
        [{call, ?MODULE, push_object, [g_unique_remotes(), g_riak_object(), S]} || S#state.sources /= []] ++
        [?LET({_Remote, SrcState} = Source, elements(S#state.sources), {call, ?MODULE, ack_objects, [g_up_to_length(SrcState#src_state.unacked_objects), Source]}) || S#state.sources /= []]
    ).

g_riak_object() ->
    ?LET({Bucket, Key, Content}, {binary(), binary(), binary()}, riak_object:new(Bucket, Key, Content)).

g_up_to_length([]) ->
    0;
g_up_to_length(List) ->
    Max = length(List),
    choose(0, Max).

g_unique_remotes() ->
    ?LET(Remotes, list(g_remote_name()), lists:usort(Remotes)).

g_remote_name() ->
    oneof(?all_remotes).

% name of a remote
remote_name(#state{remotes_available = []}) ->
    erlang:error(no_name_available);
remote_name(#state{remotes_available = Remotes}) ->
    oneof(Remotes).

precondition(#state{master_queue = MasterQ} = S, {call, _, Connect, [Remote, MasterQ]}) when Connect =:= connect_to_v1; Connect =:= connect_to_v2 ->
    %% ?debugFmt("Remote requested: ~p Remotes available: ~p", [Remote, S#state.remotes_available]),
    lists:member(Remote, S#state.remotes_available);
precondition(#state{sources = []}, {call, _, disconnect, _Args}) ->
    false;
precondition(S, {call, _, disconnect, _Args}) ->
    S#state.sources /= [];
precondition(_S, {call, _, ack_objects, [0, _]}) ->
    false;
precondition(#state{sources = []}, {call, _, ack_objects, _Args}) ->
    false;
precondition(S, {call, _, ack_objects, _Args}) ->
    S#state.sources /= [];
precondition(_S, {call, _, push_object, [[], _, _]}) ->
    false;
precondition(S, {call, _, push_object, [_, _, S]}) ->
    S#state.sources /= [];
precondition(_S, {call, _, push_object, [_, _, _NotS]}) ->
    %% ?debugFmt("Bad states.~n    State: ~p~nArg: ~p", [S, NotS]),
    false;
precondition(_S, _Call) ->
    true.

dynamic_precondition(#state{sources = Sources}, {call, _, disconnect, [{Remote, _}]}) ->
    is_tuple(lists:keyfind(Remote, 1, Sources));
dynamic_precondition(#state{sources = Sources}, {call, _, ack_objects, [NumAck, {Remote, SrcState}]}) ->
    case lists:keyfind(Remote, 1, Sources) of
        {_, #src_state{unacked_objects = UnAcked}} when length(UnAcked) >= NumAck ->
            UnAcked =:= SrcState#src_state.unacked_objects;
            %true;
        _ ->
            false
    end;
dynamic_precondition(_S, _Call) ->
    true.
%% dynamic_precondition(_S, {call, _, Connect, [_Remote, _MasterQ]}) when Connect =:= connect_to_v1; Connect =:= connect_to_v2 ->
%%     false;

%% ====================================================================
%% state generation
%% ====================================================================

initial_state() ->
    %% catch unlink(whereis(fake_sink)),
    %% catch exit(whereis(fake_sink), kill),
    process_flag(trap_exit, true),
    %% riak_repl_test_util:stop_test_ring(),
    %% riak_repl_test_util:start_test_ring(),
    %% riak_repl_test_util:abstract_gen_tcp(),
    %% riak_repl_test_util:abstract_stats(),
    %% riak_repl_test_util:abstract_stateful(),
    %% abstract_connection_mgr(),
    %% kill_rtq(),
    %% {ok, _RTPid} = start_rt(),
    %% {ok, _RTQPid} = start_rtq(),
    %% {ok, _TCPMonPid} = start_tcp_mon(),
    %% {ok, _FakeSinkPid} = start_fake_sink(),
    #state{}.

kill_rtq() ->
    case whereis(riak_repl2_rtq) of
        undefined ->
            ok;
        _ ->
            riak_repl2_rtq:stop(),
            catch exit(riak_repl2_rtq, kill)
    end.

next_state(S, Res, {call, _, connect_to_v1, [Remote, _MQ]}) ->
    SrcState = #src_state{pids = Res, version = 1},
    next_state_connect(Remote, SrcState, S);

next_state(S, Res, {call, _, connect_to_v2, [Remote, _MQ]}) ->
    SrcState = #src_state{pids = Res, version = 2},
    next_state_connect(Remote, SrcState, S);

next_state(S, _Res, {call, _, disconnect, [{Remote, _}]}) ->
    %% ?debugFmt("Removing ~p from sources", [Remote]),
    Sources = lists:keydelete(Remote, 1, S#state.sources),
    Master = if
        Sources == [] ->
            [];
        true ->
            MasterKeys = ordsets:from_list([Seq || {Seq, _, _, _} <- S#state.master_queue]),
            SourceQueues = [Source#src_state.unacked_objects || {_, Source} <- Sources],
            ExtractSeqs = fun(Elem, Acc) ->
                SrcSeqs = ordsets:from_list([PushedThang#pushed.seq || PushedThang <- Elem]),
                ordsets:union(Acc, SrcSeqs)
            end,
            ActiveSeqs = lists:foldl(ExtractSeqs, [], SourceQueues),
            RemoveSeqs = ordsets:subtract(MasterKeys, ActiveSeqs),
            UpdateMaster = fun(RemoveSeq, Acc) ->
                lists:keystore(RemoveSeq, 1, Acc, {RemoveSeq, tombstone})
            end,
            lists:foldl(UpdateMaster, S#state.master_queue, RemoveSeqs)
    end,
    %% ?debugFmt("Updated remotes after disconnect: ~p", [[Remote | S#state.remotes_available]]),
    S#state{master_queue = Master, sources = Sources, remotes_available = [Remote | S#state.remotes_available]};

next_state(S, Res, {call, _, push_object, [Remotes, RiakObj, _S]}) ->
    Seq = S#state.seq + 1,
    Sources = update_unacked_objects(Remotes, Seq, Res, S),
    RoutingSources = [R || {R, _} <- Sources, not lists:member(R, Remotes)],
    Master2 = case RoutingSources of
        [] ->
            [{Seq, tombstone} | S#state.master_queue];
        _ ->
            Master = S#state.master_queue,
            [{Seq, Remotes, RiakObj, Res} | Master]
    end,
    S#state{sources = Sources, master_queue = Master2, seq = Seq};

next_state(S, _Res, {call, _, ack_objects, [NumAcked, {Remote, _Source}]}) ->
    case lists:keytake(Remote, 1, S#state.sources) of
        false ->
            S;
        {value, {Remote, RealSource}, Sources} ->
            UnAcked = RealSource#src_state.unacked_objects,
            {Updated, Chopped} = model_ack_objects(NumAcked, UnAcked),
            SrcState2 = RealSource#src_state{unacked_objects = Updated},
            Sources2 = [{Remote, SrcState2} | Sources],
            %% ?debugFmt("Updated ack sources: ~p", [[R || {R, _} <- Sources2]]),
            Master2 = remove_fully_acked(S#state.master_queue, Chopped, Sources2),
            S#state{sources = Sources2, master_queue = Master2}
    end.

next_state_connect(Remote, SrcState, State) ->
    case lists:keyfind(Remote, 1, State#state.sources) of
        false ->
            Remotes = lists:delete(Remote, State#state.remotes_available),
            {NewQueue, Skips, Offset} = generate_unacked_from_master(State, Remote),
            SrcState2 = SrcState#src_state{unacked_objects = NewQueue, skips = Skips, offset = Offset},
            %% ?debugFmt("Adding ~p to sources", [Remote]),
            Sources = [{Remote, SrcState2} | State#state.sources],
            %% ?debugFmt("Updated sources: ~p", [[R || {R, _} <- Sources]]),
            State#state{sources = Sources, remotes_available = Remotes};
        _Tuple ->
            State
    end.

generate_unacked_from_master(State, Remote) ->
    UpRemotes = running_remotes(State),
    generate_unacked_from_master(lists:reverse(State#state.master_queue), UpRemotes, Remote, undefined, 0, []).

generate_unacked_from_master([], _UpRemotes, _Remote, Skips, Offset, Acc) ->
    {Acc, Skips, Offset};
generate_unacked_from_master([{_Seq, tombstone} | Tail], UpRemotes, Remote, undefined, Offset, Acc) ->
    generate_unacked_from_master(Tail, UpRemotes, Remote, undefined, Offset, Acc);
generate_unacked_from_master([{_Seq, tombstone} | Tail], UpRemotes, Remote, Skips, Offset, Acc) ->
    generate_unacked_from_master(Tail, UpRemotes, Remote, Skips + 1, Offset, Acc);
generate_unacked_from_master([{Seq, Remotes, _Binary, Res} | Tail], UpRemotes, Remote, Skips, Offset, Acc) ->
    case {lists:member(Remote, Remotes), Skips} of
        {true, undefined} ->
            % on start up, we don't worry about skips until we've sent at least
            % one; the sink doesn't care what the first seq number is anyway.
            generate_unacked_from_master(Tail, UpRemotes, Remote, Skips, Offset, Acc);
        {true, _} ->
            generate_unacked_from_master(Tail, UpRemotes, Remote, Skips + 1, Offset, Acc);
        {false, _} ->
            NextSkips = case Skips of
                undefined -> 0;
                _ -> Skips
            end,
            Offset2 = Offset + NextSkips,
            Obj = #pushed{seq = Seq, v1_seq = Seq - Offset2, push_res = Res, skips = NextSkips,
                remotes_up = UpRemotes},
            Acc2 = [Obj | Acc],
            generate_unacked_from_master(Tail, UpRemotes, Remote, 0, Offset2, Acc2)
    end.

remove_fully_acked(Master, [], _Sources) ->
    Master;

remove_fully_acked(Master, [Pushed | Chopped], Sources) ->
    #pushed{seq = Seq} = Pushed,
    case is_in_a_source(Seq, Sources) of
        true ->
            remove_fully_acked(Master, Chopped, Sources);
        false ->
            Master2 = lists:keystore(Seq, 1, Master, {Seq, tombstone}),
            remove_fully_acked(Master2, Chopped, Sources)
    end.

is_in_a_source(_Seq, []) ->
    false;
is_in_a_source(Seq, [{_Remote, #src_state{unacked_objects = UnAcked}} | Tail]) ->
    case lists:keymember(Seq, #pushed.seq, UnAcked) of
        true ->
            true;
        false ->
            is_in_a_source(Seq, Tail)
    end.

running_remotes(State) ->
    Remotes = [Remote || {Remote, _} <- State#state.sources],
    ordsets:from_list(Remotes).

update_unacked_objects(Remotes, Seq, Res, State) when is_record(State, state) ->
    UpRemotes = running_remotes(State),
    update_unacked_objects(Remotes, Seq, Res, UpRemotes, State#state.sources, []).

update_unacked_objects(_Remotes, _Seq, _Res, _UpRemotes, [], Acc) ->
    lists:reverse(Acc);

update_unacked_objects(Remotes, Seq, Res, UpRemotes, [{Remote, Source} | Tail], Acc) ->
    case lists:member(Remote, Remotes) of
        true ->
            Skipped = case Source#src_state.skips of
                undefined -> undefined;
                _ -> Source#src_state.skips + 1
            end,
            update_unacked_objects(Remotes, Seq, Res, UpRemotes, Tail, [{Remote, Source#src_state{skips = Skipped}} | Acc]);
        false ->
            Entry = model_push_object(Seq, Res, UpRemotes, Source),
            update_unacked_objects(Remotes, Seq, Res, UpRemotes, Tail, [{Remote, Entry} | Acc])
    end.

model_push_object(Seq, Res, UpRemotes, SrcState = #src_state{unacked_objects = ObjQueue}) ->
    {Seq2, Offset2} = case SrcState#src_state.skips of
        undefined ->
            {Seq, SrcState#src_state.offset};
        _ ->
            Off2 = SrcState#src_state.offset + SrcState#src_state.skips,
            {Seq - Off2, Off2}
    end,
    Obj = #pushed{seq = Seq, v1_seq = Seq2, push_res = Res, skips = SrcState#src_state.skips, remotes_up = UpRemotes},
    SrcState#src_state{unacked_objects = [Obj | ObjQueue], offset = Offset2, skips = 0}.

insert_skip_meta({Seq, {Count, Bin, Meta}}, #src_state{skips = SkipCount}) ->
    Meta2 = orddict:from_list(Meta),
    Meta3 = orddict:store(skip_count, SkipCount, Meta2),
    {Seq, {Count, Bin, Meta3}}.

model_ack_objects(_Num, []) ->
    {[], []};
model_ack_objects(NumAcked, Unacked) when length(Unacked) >= NumAcked ->
    NewLength = length(Unacked) - NumAcked,
    lists:split(NewLength, Unacked).

%% ====================================================================
%% postcondition
%% ====================================================================

postcondition(_State, {call, _, connect_to_v1, _Args}, {error, _}) ->
    ?P(false);
postcondition(_State, {call, _, connect_to_v1, _Args}, {Source, Sink}) ->
    ?P(is_pid(Source) andalso is_pid(Sink));

postcondition(_State, {call, _, connect_to_v2, _Args}, {error, _}) ->
    ?P(false);
postcondition(_State, {call, _, connect_to_v2, _Args}, {Source, Sink}) ->
    ?P(is_pid(Source) andalso is_pid(Sink));

postcondition(_State, {call, _, disconnect, [_SourceState]}, {Source, _Sink}) ->
    %% ?P(lists:all(fun(ok) -> true; (_) -> false end, Waits));
    %% ?debugFmt("Source alive? ~p", [is_process_alive(Source)]),
    ?P(is_process_alive(Source) =:= false);

postcondition(_State, {call, _, push_object, [Remotes, _RiakObj, State]}, Res) ->
    ?P(assert_sink_bugs(Res, Remotes, State#state.sources));

postcondition(State, {call, _, ack_objects, [NumAck, {Remote, Source}]}, AckedStack) ->
    RemoteLives = is_tuple(lists:keyfind(Remote, 1, State#state.sources)),
    #src_state{unacked_objects = UnAcked} = Source,
    {_, Acked} = lists:split(length(UnAcked) - NumAck, UnAcked),
    if
        RemoteLives == false ->
            ?P(AckedStack == []);
        length(Acked) =/= length(AckedStack) ->
            ?debugMsg("Acked length is not the same as AckedStack length"),
            ?P(false);
        true ->
            ?P(assert_sink_ackings(Remote, Source#src_state.version, Acked, AckedStack))
    end;

postcondition(_S, _C, _R) ->
    ?debugMsg("fall through postcondition"),
    false.

assert_sink_bugs(Object, Remotes, Sources) ->
    Active = [A || {A, _} <- Sources],
    assert_sink_bugs(Object, Remotes, Active, Sources, []).

assert_sink_bugs(_Object, _Remotes, _Active, [], Acc) ->
    lists:all(fun(true) -> true; (_) -> false end, Acc);

assert_sink_bugs(Object, Remotes, Active, [{Remote, SrcState} | Tail], Acc) ->
    Truthiness = assert_sink_bug(Object, Remotes, Remote, Active, SrcState),
    assert_sink_bugs(Object, Remotes, Active, Tail, [Truthiness | Acc]).

assert_sink_bug({_Num, RiakObj, BadMeta}, Remotes, Remote, Active, SrcState) ->
    {_, Sink} = SrcState#src_state.pids,
    Version = SrcState#src_state.version,
    History = gen_server:call(Sink, history),
    ShouldSkip = lists:member(Remote, Remotes),
    BadMeta2 = set_skip_meta(BadMeta, SrcState),
    Routed = ordsets:from_list(Remotes ++ [Remote] ++ Active ++ ["undefined"]),
    Meta = orddict:store(routed_clusters, Routed, BadMeta2),
    ObjBin = case SrcState#src_state.version of
        1 ->
            term_to_binary([RiakObj]);
        2 ->
            riak_repl_util:to_wire(w1, [RiakObj])
    end,
    if
        ShouldSkip andalso length(History) == length(SrcState#src_state.unacked_objects) ->
            true;
        ShouldSkip ->
            ?debugFmt("assert sink history length failure!~n"
                "    Remote: ~p~n"
                "    Length Sink Hist: ~p~n"
                "    Length Model Hist: ~p~n"
                "    Sink:~n"
                "~p~n"
                "    Model:~n"
                "~p",[Remote, length(History), length(SrcState#src_state.unacked_objects), History, SrcState#src_state.unacked_objects]),
            false;
        true ->
            Frame = hd(History),
            case {Version, Frame} of
                {1, {objects, {_Seq, ObjBin}}} ->
                    true;
                {2, {objects_and_meta, {_Seq, ObjBin, _M}}} ->
                    true;
                _ ->
                    ?debugFmt("assert sink bug failure!~n"
                        "    Remote: ~p~n"
                        "    Remotes: ~p~n"
                        "    Active: ~p~n"
                        "    ObjBin: ~p~n"
                        "    Meta: ~p~n"
                        "    ShouldSkip: ~p~n"
                        "    Version: ~p~n"
                        "    Frame: ~p~n"
                        "    Length Sink Hist: ~p~n"
                        "    History: ~p~n"
                        "    Length Model Hist: ~p", [Remote, Remotes, Active, ObjBin, Meta, ShouldSkip, Version, Frame, length(History), History, length(SrcState#src_state.unacked_objects)]),
                    false
            end
    end.

set_skip_meta(Meta, #src_state{skips = Skips}) ->
    set_skip_meta(Meta, Skips);
set_skip_meta(Meta, undefined) ->
    set_skip_meta(Meta, 0);
set_skip_meta(Meta, Skips) ->
    orddict:store(skip_count, Skips, Meta).

assert_sink_ackings(Remote, Version, Expecteds, Gots) ->
    assert_sink_ackings(Remote, Version, Expecteds, Gots, []).

assert_sink_ackings(_Remote, _Version, [], [], Acc) ->
    lists:all(fun(true) -> true; (_) -> false end, Acc);

assert_sink_ackings(Remote, Version, [Expected | ETail], [Got | GotTail], Acc) ->
    AHead = assert_sink_acking(Remote, Version, Expected, Got),
    assert_sink_ackings(Remote, Version, ETail, GotTail, [AHead | Acc]).

assert_sink_acking(Remote, Version, Pushed, Got) ->
    #pushed{seq = Seq, v1_seq = V1Seq, push_res = {1, RiakObj, PushMeta}, skips = Skip,
        remotes_up = UpRemotes} = Pushed,
    PushMeta1 = set_skip_meta(PushMeta, Skip),
    PushMeta2 = fix_routed_meta(PushMeta1, ordsets:add_element(Remote, UpRemotes)),
    Meta = PushMeta2,
    ObjBin = case Version of
        1 -> riak_repl_util:to_wire(w0, [RiakObj]);
        2 -> riak_repl_util:to_wire(w1, [RiakObj])
    end,
    case {Version, Got} of
        {1, {objects, {V1Seq, ObjBin}}} ->
            true;
        {2, {objects_and_meta, {Seq, ObjBin, Meta}}} ->
            true;
        _ ->
            ?debugFmt("Sink ack failure!~n"
                "    Remote: ~p~n"
                "    UpRemotes: ~p~n"
                "    Version: ~p~n"
                "    ObjBin: ~p~n"
                "    Seq: ~p~n"
                "    V1Seq: ~p~n"
                "    Skip: ~p~n"
                "    PushMeta: ~p~n"
                "    Meta: ~p~n"
                "    Got: ~p", [Remote, UpRemotes, Version, ObjBin, Seq, V1Seq, Skip, PushMeta, Meta, Got]),
            false
    end.

fix_routed_meta(Meta, AdditionalRemotes) ->
    Routed1 = case orddict:find(routed_clusters, Meta) of
        error -> [];
        {ok, V} -> V
    end,
    Routed2 = ordsets:union(AdditionalRemotes, Routed1),
    Routed3 = ordsets:add_element("undefined", Routed2),
    orddict:store(routed_clusters, Routed3, Meta).

%% ====================================================================
%% test callbacks
%% ====================================================================

connect_to_v1(RemoteName, MasterQueue) ->
    %% ?debugFmt("connect_to_v1: ~p", [RemoteName]),
    stateful:set(version, {realtime, {1,0}, {1,0}}),
    connect(RemoteName, MasterQueue).

connect_to_v2(RemoteName, MasterQueue) ->
    %% ?debugFmt("connect_to_v2: ~p", [RemoteName]),
    stateful:set(version, {realtime, {2,0}, {2,0}}),
    connect(RemoteName, MasterQueue).

connect(RemoteName, MasterQueue) ->
    %% ?debugFmt("connect: ~p", [RemoteName]),
    stateful:set(remote, RemoteName),
    %% ?debugMsg("Starting rtsource link"),
    {ok, SourcePid} = riak_repl2_rtsource_conn:start_link(RemoteName),
    %% ?debugFmt("rtsource pid: ~p", [SourcePid]),
    %% ?debugMsg("Waiting for sink_started"),
    _ = wait_for_rtsource_helper(SourcePid),
    FakeSink = whereis(fake_sink),
    %% ?debugFmt("fake_sink pid: ~p", [FakeSink]),
    FakeSink ! {status, self()},
    receive
        {sink_started, SinkPid} ->
            erlang:monitor(process, SinkPid),
            %% ?debugFmt("V1 SinkPid: ~p", [SinkPid]),
            rt_source_helpers:wait_for_valid_sink_history(SinkPid, RemoteName, MasterQueue),
            ok = rt_source_helpers:ensure_registered(RemoteName),
            {SourcePid, SinkPid}
    after 5000 ->
            {error, timeout}
    end.

wait_for_rtsource_helper(SourcePid) ->
    Status = riak_repl2_rtsource_conn:status(SourcePid),
    wait_for_rtsource_helper(SourcePid, 20, 1000, Status).

wait_for_rtsource_helper(_SourcePid, 0, _Wait, _Status) ->
    {error, rtsource_helper_failed};
wait_for_rtsource_helper(SourcePid, RetriesLeft, Wait, Status) ->
    case lists:keyfind(helper_pid, 1, Status) of
        false ->
            timer:sleep(Wait),
            NewStatus = riak_repl2_rtsource_conn:status(SourcePid),
            wait_for_rtsource_helper(SourcePid, RetriesLeft-1, Wait, NewStatus);
        _ ->
            ok
    end.

disconnect(ConnectState) ->
    {Remote, SrcState} = ConnectState,
    %% ?debugFmt("Disconnecting ~p", [Remote]),
    #src_state{pids = {Source, Sink}} = SrcState,
    %% ?debugFmt("is Source alive: ~p", [is_process_alive(Source)]),
    %% ?debugFmt("is Sink ~p alive: ~p", [Sink, is_process_alive(Sink)]),
    %% Stop the source, but no need to stop our fake sink
    riak_repl2_rtsource_conn:stop(Source),
    Sink ! stop, %% Reset the fake sink history
    riak_repl2_rtq:unregister(Remote),
    {Source, Sink}.
    %% [riak_repl_test_util:wait_for_pid(P, 3000) || P <- [Source, Sink]].

push_object(Remotes, RiakObj, State) ->
    %% ?debugFmt("push_object remotes: ~p", [Remotes]),
    Meta = [{routed_clusters, Remotes}],
    riak_repl2_rtq:push(1, riak_repl_util:to_wire(w1, [RiakObj]), Meta),
    rt_source_helpers:wait_for_pushes(State, Remotes),
    {1, RiakObj, Meta}.

ack_objects(NumToAck, {Remote, SrcState}) ->
    {_, Sink} = SrcState#src_state.pids,
    ProcessAlive = is_process_alive(Sink),
    if
        ProcessAlive ->
            {ok, Acked} = gen_server:call(Sink, {ack, NumToAck}),
            case Acked of
                [] ->
                    ok;
                [{objects_and_meta, {Seq, _, _}} | _] ->
                    riak_repl2_rtq:ack(Remote, Seq);
                [{objects, {Seq, _}} | _] ->
                    riak_repl2_rtq:ack(Remote, Seq)
            end,
            Acked;
        true ->
            []
    end.
-endif.
