-module(rt_source_helpers).

-compile([export_all, nowarn_export_all]).

-ifdef(TEST).

-include("rt_source_eqc.hrl").

-include_lib("eunit/include/eunit.hrl").

%% ====================================================================
%% helpful utility functions
%% ====================================================================

ensure_registered(RemoteName) ->
    ensure_registered(RemoteName, 10).

ensure_registered(_RemoteName, N) when N < 1 ->
    {error, registration_timeout};
ensure_registered(RemoteName, N) ->
    Status = riak_repl2_rtq:status(),
    %% ?debugFmt("RTQ Status: ~p~n", [Status]),
    Consumers = proplists:get_value(consumers, Status),
    case proplists:get_value(RemoteName, Consumers) of
        undefined ->
            timer:sleep(1000),
            ensure_registered(RemoteName, N - 1);
        _ ->
            ok
    end.

wait_for_valid_sink_history(Pid, Remote, MasterQueue) ->
    NewQueue = [{Seq, Queued} || {Seq, RoutedRemotes, _Binary, Queued}
               <- MasterQueue, not lists:member(Remote, RoutedRemotes)],
    if
        length(NewQueue) > 0 ->
            gen_server:call(Pid, {block_until, length(NewQueue)}, 30000);
        true ->
            ok
    end.

wait_for_pushes(State, Remotes) ->
    [wait_for_push(SrcState, Remotes) || SrcState <- State#state.sources].

wait_for_push({Remote, SrcState}, Remotes) ->
    case lists:member(Remote, Remotes) of
        true -> ok;
        _ ->
            WaitLength = length(SrcState#src_state.unacked_objects) + 1,
            {_, Sink} = SrcState#src_state.pids,
            gen_server:call(Sink, {block_until, WaitLength}, 30000)
    end.

plant_bugs(_Remotes, []) ->
    ok;
plant_bugs(Remotes, [{Remote, SrcState} | Tail]) ->
    case lists:member(Remote, Remotes) of
        true ->
            plant_bugs(Remotes, Tail);
        false ->
            {_, Sink} = SrcState#src_state.pids,
            ok = gen_server:call(Sink, bug),
            plant_bugs(Remotes, Tail)
    end.

abstract_connection_mgr(RemotePort) when is_integer(RemotePort) ->
    abstract_connection_mgr({"localhost", RemotePort});
abstract_connection_mgr({RemoteHost, RemotePort} = RemoteName) ->
    riak_repl_test_util:reset_meck(riak_core_connection_mgr, [no_link, passthrough]),
    meck:expect(riak_core_connection_mgr, connect, fun(_ServiceAndRemote, ClientSpec) ->
        proc_lib:spawn_link(fun() ->
            %% ?debugFmt("connection_mgr connect for ~p", [ServiceAndRemote]),
            Version = stateful:version(),
            {_Proto, {TcpOpts, Module, Pid}} = ClientSpec,
             %% ?debugFmt("connection_mgr callback module: ~p", [Module]),
            {ok, Socket} = gen_tcp:connect(RemoteHost, RemotePort, [binary | TcpOpts]),
            %% ?debugFmt("connection_mgr calling connection callback for ~p", [Pid]),
            ok = Module:connected(Socket, gen_tcp, RemoteName, Version, Pid, [])
        end),
        {ok, make_ref()}
    end).

start_rt() ->
    riak_repl_test_util:reset_meck(riak_repl2_rt, [no_link, passthrough]),
    WhoToTell = self(),
    meck:expect(riak_repl2_rt, register_sink, fun(SinkPid) ->
        WhoToTell ! {sink_pid, SinkPid},
        meck:passthrough([SinkPid])
    end),
    riak_repl_test_util:kill_and_wait(riak_repl2_rt),
    riak_repl2_rt:start_link().

start_rtq() ->
    %% ?debugFmt("where is riak_repl2_rtq: ~p", [whereis(riak_repl2_rtq)]),
    %% riak_repl_test_util:kill_and_wait(riak_repl2_rtq),
    riak_repl2_rtq:start_link().

start_tcp_mon() ->
    riak_repl_test_util:kill_and_wait(riak_core_tcp_mon),
    riak_core_tcp_mon:start_link().

-spec init_fake_sink() ->
          {ok, FakeSinkPid :: pid(), ListeningPort :: inet:port_number()}
        | {error, wait_for_sink_failed}.
init_fake_sink() ->
    riak_repl_test_util:reset_meck(riak_core_service_mgr, [no_link, passthrough]),
    WhoToTell = self(),
    meck:expect(riak_core_service_mgr, sync_register_service, fun(HostSpec, _Strategy) ->
        %% riak_repl_test_util:kill_and_wait(fake_sink),
        {_Proto, {TcpOpts, _Module, _StartCB, _CBArgs}} = HostSpec,
        sink_listener(TcpOpts, WhoToTell)
    end),
    riak_repl2_rtsink_conn:sync_register_service(),
    wait_for_sink().

kill_fake_sink() ->
    case whereis(fake_sink) of
        undefined ->
            ok;
        FsPid ->
            FsPid ! kill
    end.

sink_listener(TcpOpts, WhoToTell) ->
    % ?debugMsg("sink_listener"),
    TcpOpts2 = [binary, {reuseaddr, true} | TcpOpts],
    Pid = proc_lib:spawn_link(fun() ->
        {ok, Listen} = gen_tcp:listen(0, TcpOpts2),
        % {ok, LName} = inet:sockname(Listen),
        % ?debugFmt("fake sink listener: ~p", [LName]),
        proc_lib:spawn(?MODULE, sink_acceptor, [Listen, WhoToTell]),
        receive
            _ -> ok
        end
    end),
    %% {ok, Listen} = gen_tcp:listen(0, TcpOpts2),
    %% AcceptorPid = proc_lib:spawn(?MODULE, sink_acceptor, [Listen, WhoToTell]),
    %% sink_acceptor(Listen, WhoToTell),
    %% register(fake_sink, Pid),
    %% {ok, AcceptorPid}.
    {ok, Pid}.

wait_for_sink() ->
    receive
        {fake_sink_pid, Pid} ->
            % ?debugFmt("fake_sink started at ~p", [Pid]),
            case gen_server:call(Pid, listen_port, 2000) of
                {listen_port, Port} ->
                    % ?debugFmt("fake_sink ~p listening on port ~B", [Pid, Port]),
                    {ok, Pid, Port};
                Reply ->
                    % ?debugFmt("fake_sink ~p bad reply ~p", [Pid, Reply]),
                    Reply
            end
    after 30000 ->
        % ?debugMsg("wait_for_sink timed out"),
        {error, wait_for_sink_failed}
    end.

%% wait_for_sink(0, Wait) ->
%%     timer:sleep(Wait),
%%     case whereis(fake_sink) of
%%         undefined ->
%%             {error, wait_for_sink_failed};
%%         _ ->
%%             ok
%%     end;
%% wait_for_sink(Retries, Wait) ->
%%     timer:sleep(Wait),
%%     case whereis(fake_sink) of
%%         undefined ->
%%             wait_for_sink(Retries-1, Wait);
%%         _ ->
%%             ok
%%     end.

wait_for_pid(Pid) ->
    wait_for_pid(Pid, 5, 100).

wait_for_pid(Pid, 0, WaitPeriod) ->
    timer:sleep(WaitPeriod),
    case is_process_alive(Pid) of
        true ->
            ok;
        false ->
            {error, wait_for_pid_failed}
    end;
wait_for_pid(Pid, RetriesLeft, WaitPeriod) ->
    timer:sleep(WaitPeriod),
    case is_process_alive(Pid) of
        true ->
            ok;
        false ->
            wait_for_pid(Pid, RetriesLeft - 1, WaitPeriod)
    end.

sink_acceptor(Listen, WhoToTell) ->
    %% ?debugFmt("sink_acceptor: ~p", [self()]),
    %% {ok, Socket} = gen_tcp:accept(Listen),
    %% Version = stateful:version(),
    Pid = proc_lib:spawn(?MODULE, fake_sink, [undefined, Listen, undefined, undefined, []]),
    %% ?debugFmt("Waiting for ~p", [Pid]),
    wait_for_pid(Pid),

    %% ?debugFmt("Fake Sink pid: ~p Alive?: ~p Fake Sink pid: ~p", [Pid, is_process_alive(Pid), whereis(fake_sink)]),
    %% ok = gen_tcp:controlling_process(Socket, Pid),
    %% ?debugFmt("Started fake_sink ~p, telling ~p", [Pid, WhoToTell]),
    Pid ! {start, WhoToTell},
    %% sink_acceptor(Listen, WhoToTell).
    ok.

fake_sink(undefined, Listen, Version, Bug, History) ->
    receive
        {start, WhoToTell} ->
            % ?debugMsg("registering fake_sink"),
            register(fake_sink, self()),
            % ?debugFmt("Sending fake_sink pid to ~p", [WhoToTell]),
            WhoToTell ! {fake_sink_pid, self()},
            fake_sink(undefined, Listen, Version, Bug, History);
        {status, WhoToTell} ->
            {ok, Socket} = gen_tcp:accept(Listen),
            inet:setopts(Socket, [{active, once}]),
            % ?debugFmt("Sending fake_sink status to ~p", [WhoToTell]),
            WhoToTell ! {sink_started, self()},
            Version1 = stateful:version(),
            fake_sink(Socket, Listen, Version1, Bug, History);
        {'$gen_call', From, history} ->
            gen_server:reply(From, History),
            fake_sink(undefined, Listen, Version, Bug, History);
        {'$gen_call', From, listen_port} ->
            {ok, Port} = inet:port(Listen),
            % ?debugFmt("Sending fake_sink port to ~p", [From]),
            gen_server:reply(From, {listen_port, Port}),
            fake_sink(undefined, Listen, Version, Bug, History);
        {'$gen_call', From, listen_name} ->
            {ok, Name} = inet:sockname(Listen),
            % ?debugFmt("Sending fake_sink name to ~p", [From]),
            gen_server:reply(From, {listen_name, Name}),
            fake_sink(undefined, Listen, Version, Bug, History);
        kill ->
            % {ok, Name} = inet:sockname(Listen),
            % ?debugFmt("killing fake_sink ~p at ~p", [self(), Name]),
            gen_tcp:close(Listen),
            ok
    end;
fake_sink(Socket, Listen, Version, Bug, History) ->
    %% ?debugFmt("fake_sinnk: ~p", [self()]),
    receive
        %% {start, Listen, WhoToTell} ->
        %%     {ok, Socket1} = gen_tcp:accept(Listen),
        %%     inet:setopts(Socket1, [{active, once}]),
        %%     ?debugMsg("registering fake_sink"),
        %%     register(fake_sink, self()),
        %%     WhoToTell ! {fake_sink_pid, self()},
        %%     fake_sink(Socket1, Version, Bug, History);
        {status, WhoToTell} ->
            % ?debugFmt("Sending fake_sink pid to ~p", [WhoToTell]),
            WhoToTell ! {sink_started, self()},
            Version1 = stateful:version(),
            fake_sink(Socket, Listen, Version1, Bug, History);
        stop ->
            %% ?debugMsg("\nfake_sink stop\n"),
            fake_sink(undefined, Listen, undefined, undefined, History);
        kill ->
            % {ok, LName} = inet:sockname(Listen),
            % {ok, SName} = inet:sockname(Socket),
            % ?debugFmt("killing fake_sink ~p at ~p (~p)", [self(), LName, SName]),
            gen_tcp:close(Socket),
            gen_tcp:close(Listen),
            ok;
        {'$gen_call', From, history} ->
            gen_server:reply(From, History),
            fake_sink(Socket, Listen, Version, Bug, History);
        {'$gen_call', From, {ack, Num}} when Num =< length(History) ->
            %% ?debugFmt("Got ~p acks History length: ~p", [Num, length(History)]),
            {NewHistory, Return} = lists:split(length(History) - Num, History),
            gen_server:reply(From, {ok, Return}),
            fake_sink(Socket, Listen, Version, Bug, NewHistory);
        {'$gen_call', {NewBug, _Tag} = From, bug} ->
            gen_server:reply(From, ok),
            fake_sink(Socket, Listen, Version, {once_bug, NewBug}, History);
        {'$gen_call', From, {block_until, HistoryLength}} when length(History) >= HistoryLength ->
            %% ?debugMsg("Got block_until call1"),
            gen_server:reply(From, ok),
            fake_sink(Socket, Listen, Version, Bug, History);
        {'$gen_call', From, {block_until, HistoryLength}} ->
            %% ?debugMsg("Got block_until call2"),
            %% ?debugFmt("History info: ~p ~p", [length(History), HistoryLength]),
            fake_sink(Socket, Listen, Version, {block_until, From, HistoryLength}, History);
        {'$gen_call', From, _Msg} ->
            gen_server:reply(From, {error, badcall}),
            fake_sink(Socket, Listen, Version, Bug, History);
        {tcp, Socket, Bin} ->
            History2 = fake_sink_nom_frames(Bin, History),
            % hearbeats can come at any time, but we don't actually test for
            % them (yet).
            % TODO extend this so a sink can be made to act badly (ie, no
            % hearbeat sent back)
            %% ?debugFmt("Bug: ~p", [Bug]),
            History3 = fake_sink_heartbeats(History2, Socket),
            %% ?debugFmt("New History length: ~p", [length(History3)]),
            NewBug = case Bug of
                {once_bug, Target} ->
                    Self = self(),
                    [Frame | _] = History3,
                    Target ! {got_data, Self, Frame},
                    undefined;
                {block_until, From, Length} when length(History3) >= Length ->
                    gen_server:reply(From, ok),
                    undefined;
                _ ->
                    Bug
            end,
            inet:setopts(Socket, [{active, once}]),
            fake_sink(Socket, Listen, Version, NewBug, History3);
        {tcp, _, _Bin} ->
            %% ?debugMsg("Ignoring tcp msg for old socket"),
            fake_sink(Socket, Listen, Version, Bug, History);
        {tcp_error, Socket, Err} ->
            %% ?debugFmt("Exiting from TCP error: ~p", [Err]),
            exit(Err);
        {tcp_closed, Socket} ->
            %% ?debugMsg("tcp_closed, resetting"),
            fake_sink(undefined, Listen, undefined, undefined, History);
        {tcp_closed, _} ->
            %% ?debugMsg("ignoring old tcp_closed"),
            fake_sink(Socket, Listen, Version, Bug, History)
    end.

fake_sink_heartbeats(History, Socket) ->
    FoldFun = fun
        (heartbeat, Acc) ->
            gen_tcp:send(Socket, riak_repl2_rtframe:encode(heartbeat, undefined)),
            Acc;
        (Frame, Acc) ->
            Acc ++ [Frame]
    end,
    lists:foldl(FoldFun, [], History).

fake_sink_nom_frames({ok, undefined, <<>>}, History) ->
    History;
fake_sink_nom_frames({ok, undefined, Rest}, History) ->
    ?debugFmt("Frame issues: binary left over: ~p", [Rest]),
    History;
fake_sink_nom_frames({ok, Frame, Rest}, History) ->
    fake_sink_nom_frames(Rest, [Frame | History]);
fake_sink_nom_frames(Bin, History) ->
    fake_sink_nom_frames(riak_repl2_rtframe:decode(Bin), History).

-endif.
