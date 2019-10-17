%%
%% Eunit tests for source/sink communictation between protocol versions.
%%

-module(riak_repl2_rt_source_sink_tests).

-include_lib("eunit/include/eunit.hrl").

-compile([export_all, nowarn_export_all]).

-define(SINK_PORT, 5006).
-define(SOURCE_PORT, 4006).
-define(VER1, {1,0}).
-define(VER2, {2,0}).
-define(VER3, {3,0}).
-define(PROTOCOL(NegotiatedVer), {realtime, NegotiatedVer, NegotiatedVer}).
-define(PROTOCOL_V1, ?PROTOCOL(?VER1)).
-define(PROTO_V1_SOURCE_V1_SINK, ?PROTOCOL(?VER1)).

-record(connection_tests, {
    tcp_mon, rt
}).

setup() ->
    error_logger:tty(false),
    riak_repl_test_util:start_test_ring(),
    riak_repl_test_util:abstract_gen_tcp(),
    abstract_stats(),
    abstract_util(),
    abstract_rt(),
    riak_repl_test_util:kill_and_wait(riak_repl2_rt),
    {ok, RT} = riak_repl2_rt:start_link(),
    riak_repl_test_util:kill_and_wait(riak_repl2_rtq),
    {ok, _} = riak_repl2_rtq:start_link(),
    riak_repl_test_util:kill_and_wait(riak_core_tcp_mon),
    {ok, TCPMon} = riak_core_tcp_mon:start_link(),
    #connection_tests{tcp_mon = TCPMon, rt = RT}.

cleanup(State) ->
    process_flag(trap_exit, true),
    #connection_tests{tcp_mon = TCPMon, rt = RT} = State,
    %% riak_repl_test_util:kill_and_wait(riak_repl2_rt),
    %% riak_repl_test_util:kill_and_wait(riak_repl2_rtq),
    %% riak_repl_test_util:kill_and_wait(riak_core_tcp_mon),
    [kill_proc(P) || P <- [TCPMon, RT, riak_repl2_rtq]],
    riak_repl_test_util:stop_test_ring(),
    process_flag(trap_exit, false),
    meck:unload().

kill_proc(undefined) ->
    ok;
kill_proc(Name) when is_atom(Name) ->
    kill_proc(whereis(Name));
kill_proc(Pid) when is_pid(Pid) ->
    catch exit(Pid, kill).

connection_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      fun v1_to_v1_comms/1,
      fun v2_to_v2_comms/1
     ]}.

v2_to_v2_comms(_State) ->
    {spawn,
     [
    {"v2 to v2 communication",
     setup,
     fun v2_to_v2_comms_setup/0,
     fun v2_to_v2_comms_cleanup/1,
     fun({Source, Sink}) ->
             [
              {"everything started okay",
               fun() ->
                       assert_living_pids([Source, Sink])
               end},
              {timeout, 120,
               {"sending objects",
               fun() ->
%                       meck:new(riak_repl_fullsync_worker, [passthrough]),
                       catch(meck:unload(riak_repl_fullsync_worker)),
                       meck:new(riak_repl_fullsync_worker),

                       Bin = <<"data data data">>,
                       Key = <<"key">>,
                       Bucket = <<"kicked">>,
                       _DefaultObj = riak_object:new(Bucket, Key, Bin),

                       Self = self(),
                       %% the w1 below indicates wireformat version 1, used in
                       %% realtime protocol v2.
                       SyncWorkerFun =
                           fun(_Worker, ObjBins, DoneFun, riak_repl2_rtsink_pool, w1) ->
                                   ?assertEqual([<<"der object">>], binary_to_term(ObjBins)),
                                   Self ! continue,
                                   Self ! {state, DoneFun},
                                   ok
                           end,
                       meck:expect(riak_repl_fullsync_worker, do_binputs, SyncWorkerFun),
                       riak_repl2_rtq:push(1, term_to_binary([<<"der object">>]), []),
                       MeckOk = wait_for_continue(),
                       ?assertEqual(ok, MeckOk),
                       meck:unload(riak_repl_fullsync_worker)
               end}},

              {"assert done",
               fun() ->
                       {ok, DoneFun} = extract_state_msg(),
                       DoneFun(),
                       ?assert(riak_repl2_rtq:all_queues_empty())
               end}
             ]
     end}]}.

v2_to_v2_comms_setup() ->
    {ok, _ListenPid} = start_sink(?VER2),
    {ok, {Source, Sink}} = start_source(?VER2),
    meck:new(poolboy, [passthrough]),
    meck:expect(poolboy, checkout, fun(_ServName, _SomeBool, _Timeout) ->
                                           spawn(fun() -> ok end)
                                   end),
    {Source, Sink}.

v2_to_v2_comms_cleanup({Source, Sink}) ->
    meck:unload(poolboy),
    connection_test_teardown_pids(Source, Sink).

v1_to_v1_comms(_State) ->
    {spawn,
     [
    {"v1 to v1 communication",
     setup,
     fun v1_to_v1_setup/0,
     fun v1_to_v1_cleanup/1,
     fun({Source, Sink}) ->
             [
              {"everything started okay",
               fun() ->
                       assert_living_pids([Source, Sink])
               end},

              {timeout, 60,
               {"sending objects",
               fun() ->
                       Self = self(),
                       meck:new(riak_repl_fullsync_worker),
                       %% The w0 defines wireformat 0, used in realtime v1
                       %% protocol.
                       SyncWorkerFun =
                           fun(_Worker, BinObjs, DoneFun, riak_repl2_rtsink_pool, w0) ->
                                   ?assertEqual([<<"der object">>], binary_to_term(BinObjs)),
                                   Self ! continue,
                                   Self ! {state, DoneFun},
                                   ok
                           end,
                       meck:expect(riak_repl_fullsync_worker, do_binputs, SyncWorkerFun),
                       riak_repl2_rtq:push(1, term_to_binary([<<"der object">>])),
                       MeckOk = wait_for_continue(),
                       ?assertEqual(ok, MeckOk),
                       meck:unload(riak_repl_fullsync_worker)
               end}},

              {"assert done",
               fun() ->
                       {ok, DoneFun} = extract_state_msg(),
                       %%?assert(is_function(DoneFun)),
                       DoneFun(),
                       ?assert(riak_repl2_rtq:all_queues_empty())
               end}
             ]
     end}]}.

v1_to_v1_setup() ->
    {ok, _ListenPid} = start_sink(?VER1),
    {ok, {Source, Sink}} = start_source(?VER1),
    meck:new(poolboy, [passthrough]),
    meck:expect(poolboy, checkout, fun(_ServName, _SomeBool, _Timeout) ->
                                           spawn(fun() -> ok end)
                                   end),
    {Source, Sink}.

v1_to_v1_cleanup({Source, Sink}) ->
    meck:unload(poolboy),
    connection_test_teardown_pids(Source, Sink).

assert_living_pids([]) ->
    true;
assert_living_pids([Pid | Tail]) ->
    ?assert(is_pid(Pid)),
    ?assert(is_process_alive(Pid)),
    assert_living_pids(Tail).

%% The start for both source and sink start up the mecks, which link to
%% the calling process. This means the meck needs to be unloaded before that
%% process exits, or there will be a big dump in the console for no reason.
connection_test_teardown_pids(Source, Sink) ->
    meck:unload(riak_core_service_mgr),
    meck:unload(riak_core_connection_mgr),
    %% unlink(Source),
    %% unlink(Sink),
    process_flag(trap_exit, true),
    [kill_proc(P) || P <- [Source, Sink]],
    process_flag(trap_exit, false),
    ok.
    %% riak_repl2_rtsource_conn:stop(Source),
    %% riak_repl2_rtsink_conn:stop(Sink),
    %% wait_for_pid(Source),
    %% wait_for_pid(Sink).

abstract_gen_tcp() ->
    meck:new(gen_tcp, [unstick, passthrough]),
    meck:expect(gen_tcp, setopts, fun(Socket, Opts) ->
        inet:setopts(Socket, Opts)
    end).

abstract_rt() ->
    meck:new(riak_repl2_rt, [passthrough]),
    meck:expect(riak_repl2_rt, register_sink, fun(_SinkPid) ->
        ok
    end).

abstract_stats() ->
    meck:new(riak_repl_stats),
    meck:expect(riak_repl_stats, rt_source_errors, fun() -> ok end),
    meck:expect(riak_repl_stats, objects_sent, fun() -> ok end).

abstract_util() ->
    catch(meck:unload(riak_repl_util)),
    meck:new(riak_repl_util, [passthrough]),
    meck:expect(riak_repl_util, from_wire, fun
        (<<131, _Rest/binary>> = Obj) ->
            binary_to_term(Obj);
        (Obj) ->
            Obj
    end),
    meck:expect(riak_repl_util, from_wire, fun
        (_Ver, <<131, _Rest/binary>> = Obj) ->
            binary_to_term(Obj);
        (_Ver, Obj) ->
            Obj
    end).

start_sink() ->
    start_sink(?VER1).

start_sink(Version) ->
    TellMe = self(),
    catch(meck:unload(riak_core_service_mgr)),
    meck:new(riak_core_service_mgr, [passthrough]),
    meck:expect(riak_core_service_mgr, sync_register_service, fun(HostSpec, _Strategy) ->
        {_Proto, {TcpOpts, _Module, _StartCB, _CBArg}} = HostSpec,
        {ok, Listen} = gen_tcp:listen(?SINK_PORT, [binary, {reuseaddr, true} | TcpOpts]),
        TellMe ! sink_listening,
        {ok, Socket} = gen_tcp:accept(Listen),
        {ok, Pid} = riak_repl2_rtsink_conn:start_link({ok, ?PROTOCOL(Version)}, "source_cluster"),
        %unlink(Pid),
        ok = gen_tcp:controlling_process(Socket, Pid),
        ok = riak_repl2_rtsink_conn:set_socket(Pid, Socket, gen_tcp),
        TellMe ! {sink_started, Pid}
    end),
    Pid = proc_lib:spawn_link(?MODULE, listen_sink, []),
    receive
        sink_listening ->
            {ok, Pid}
    after 10000 ->
            {error, timeout}
    end.

listen_sink() ->
    riak_repl2_rtsink_conn:sync_register_service().

start_source() ->
    start_source(?VER1).

start_source(NegotiatedVer) ->
    catch(meck:unload(riak_core_connection_mgr)),
    meck:new(riak_core_connection_mgr, [passthrough]),
    meck:expect(riak_core_connection_mgr, connect, fun(_ServiceAndRemote, ClientSpec) ->
        spawn_link(fun() ->
            {_Proto, {TcpOpts, Module, Pid}} = ClientSpec,
            {ok, Socket} = gen_tcp:connect("localhost", ?SINK_PORT, [binary | TcpOpts]),
            ok = Module:connected(Socket, gen_tcp, {"localhost", ?SINK_PORT}, ?PROTOCOL(NegotiatedVer), Pid, [])
        end),
        {ok, make_ref()}
    end),
    {ok, SourcePid} = riak_repl2_rtsource_conn:start_link("sink_cluster"),
    %unlink(SourcePid),
    receive
        {sink_started, SinkPid} ->
            {ok, {SourcePid, SinkPid}}
    after 1000 ->
        {error, timeout}
    end.

wait_for_pid(Pid) ->
    Mref = erlang:monitor(process, Pid),
    receive
        {'DOWN',Mref,process,_,_} ->
            ok
    after
        5000 ->
            {error, didnotexit, Pid, erlang:process_info(Pid)}
    end.

wait_for_continue() ->
    wait_for_continue(5000).

wait_for_continue(Timeout) ->
    receive
        continue ->
            ok
    after Timeout ->
        {error, timeout}
    end.

% yes, this is nasty hack to get side effects between tests.
extract_state_msg() ->
    receive
        {state, Data} ->
            {ok, Data}
    after 0 ->
        {error, nostate}
    end.
