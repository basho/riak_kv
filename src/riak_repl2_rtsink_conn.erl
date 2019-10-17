%% Riak EnterpriseDS
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
-module(riak_repl2_rtsink_conn).

%% @doc Realtime replication sink connection module
%%
%% High level responsibility...
%%  consider moving out socket responsibilities to another process
%%  to keep this one responsive (but it would pretty much just do status)
%%

%% API
-include("riak_repl.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([sync_register_service/0,
         start_service/5]).

-export([start_link/2,
         stop/1,
         set_socket/3,
         status/1, status/2,
         legacy_status/1, legacy_status/2]).

%% Export for intercept use in testing
-export([send_heartbeat/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

% how long to wait to reschedule socket reactivate.
-define(REACTIVATE_SOCK_INT_MILLIS, 10).
-define(DEFAULT_INTERVAL_MILLIS, 10000).
-define(DEFAULT_BUCKET_TYPE, <<"default">>).

-record(state, {remote,           %% Remote site name
                transport,        %% Module for sending
                socket,           %% Socket
                proto,            %% Protocol version negotiated
                peername,         %% peername of socket
                ver,              %% wire format agreed with rt source
                max_pending,      %% Maximum number of operations
                active = true,    %% If socket is set active
                deactivated = 0,  %% Count of times deactivated
                source_drops = 0, %% Count of upstream drops
                helper,           %% Helper PID
                hb_last,          %% os:timestamp last heartbeat message received
                seq_ref,          %% Sequence reference for completed/acked
                expect_seq = undefined,%% Next expected sequence number
                acked_seq = undefined, %% Last sequence number acknowledged
                completed = [],   %% Completed sequence numbers that need to be sent
                cont = <<>>,      %% Continuation from previous TCP buffer
                bt_drops,         %% drops due to bucket type mis-matches
                bt_interval,      %% how often (in ms) to report bt_drops
                bt_timer          %% timer reference for interval   
               }).

%% Register with service manager
sync_register_service() ->
    %% version {3,0} supports typed bucket replication
    ProtoPrefs = {realtime,[{3,0}, {2,0}, {1,4}, {1,1}, {1,0}]},
    TcpOptions = [{keepalive, true}, % find out if connection is dead, this end doesn't send
                  {packet, 0},
                  {nodelay, true}],
    HostSpec = {ProtoPrefs, {TcpOptions, ?MODULE, start_service, undefined}},
    riak_core_service_mgr:sync_register_service(HostSpec, {round_robin, undefined}).

%% Callback from service manager
start_service(Socket, Transport, Proto, _Args, Props) ->
    SocketTag = riak_repl_util:generate_socket_tag("rt_sink", Transport, Socket),
    lager:debug("Keeping stats for " ++ SocketTag),
    riak_core_tcp_mon:monitor(Socket, {?TCP_MON_RT_APP, sink, SocketTag},
                              Transport),
    Remote = proplists:get_value(clustername, Props),
    {ok, Pid} = riak_repl2_rtsink_conn_sup:start_child(Proto, Remote),
    ok = Transport:controlling_process(Socket, Pid),
    ok = set_socket(Pid, Socket, Transport),
    {ok, Pid}.

start_link(Proto, Remote) ->
    gen_server:start_link(?MODULE, [Proto, Remote], []).

stop(Pid) ->
    gen_server:call(Pid, stop, infinity).

%% Call after control handed over to socket
set_socket(Pid, Socket, Transport) ->
    gen_server:call(Pid, {set_socket, Socket, Transport}, infinity).

status(Pid) ->
    status(Pid, ?LONG_TIMEOUT).

status(Pid, Timeout) ->
    gen_server:call(Pid, status, Timeout).

legacy_status(Pid) ->
    legacy_status(Pid, ?LONG_TIMEOUT).

legacy_status(Pid, Timeout) ->
    gen_server:call(Pid, legacy_status, Timeout).

%% Callbacks
init([OkProto, Remote]) ->
    %% TODO: remove annoying 'ok' from service mgr proto
    {ok, Proto} = OkProto,
    Ver = riak_repl_util:deduce_wire_version_from_proto(Proto),
    lager:debug("RT sink connection negotiated ~p wire format from proto ~p", [Ver, Proto]),
    {ok, Helper} = riak_repl2_rtsink_helper:start_link(self()),
    riak_repl2_rt:register_sink(self()),
    MaxPending = app_helper:get_env(riak_repl, rtsink_max_pending, 100),
    Report = app_helper:get_env(riak_repl, bucket_type_drop_report_interval, ?DEFAULT_INTERVAL_MILLIS),

    {ok, #state{remote = Remote, proto = Proto, max_pending = MaxPending,
                helper = Helper, ver = Ver, bt_drops = dict:new(), bt_interval = Report}}.

handle_call(status, _From, State = #state{remote = Remote,
                                          transport = T, socket = _S, helper = Helper,
                                          hb_last = HBLast,
                                          active = Active, deactivated = Deactivated,
                                          source_drops = SourceDrops,
                                          expect_seq = ExpSeq, acked_seq = AckedSeq}) ->
    Pending = pending(State),
    SocketStats = riak_core_tcp_mon:socket_status(State#state.socket),
    Status = [{source, Remote},
              {pid, riak_repl_util:safe_pid_to_list(self())},
              {connected, true},
              {transport, T},
              %%{socket_raw, S},
              {socket,
               riak_core_tcp_mon:format_socket_stats(SocketStats,[])},
              {hb_last, HBLast},
              %%{peer, peername(State)},
              {helper, riak_repl_util:safe_pid_to_list(Helper)},
              {helper_msgq_len, riak_repl_util:safe_get_msg_q_len(Helper)},
              {active, Active},
              {deactivated, Deactivated},
              {source_drops, SourceDrops},
              {expect_seq, ExpSeq},
              {acked_seq, AckedSeq},
              {pending, Pending}],
    {reply, Status, State};
handle_call(legacy_status, _From, State = #state{remote = Remote,
                                                 socket = Socket}) ->
    {IPAddr, Port} = peername(State),
    Pending = pending(State),
    SocketStats = riak_core_tcp_mon:socket_status(Socket),
    Status = [{node, node()},
              {site, Remote},
              {strategy, realtime},
              {put_pool_size, Pending}, % close enough
              {connected, IPAddr, Port},
              {socket, riak_core_tcp_mon:format_socket_stats(SocketStats,[])}
             ],
    {reply, {status, Status}, State};
handle_call({set_socket, Socket, Transport}, _From, State) ->
    Transport:setopts(Socket, [{active, once}]), % pick up errors in tcp_error msg
    lager:debug("Starting realtime connection service"),
    {reply, ok, State#state{socket=Socket, transport=Transport, peername = peername(Transport, Socket)}};
handle_call(stop, _From, State) ->
    {stop, normal, ok, State}.

%% Note pattern patch on Ref
handle_cast({ack, Ref, Seq, Skips}, State = #state{transport = T, socket = S,
                                            seq_ref = Ref,
                                            acked_seq = AckedTo,
                                            completed = Completed}) ->
    %% Worker pool has completed the put, check the completed
    %% list and work out where we can ack back to
    %case ack_to(AckedTo, ordsets:add_element(Seq, Completed)) of
    case ack_to(AckedTo, insert_completed(Seq, Skips, Completed)) of
        {AckedTo, Completed2} ->
            {noreply, State#state{completed = Completed2}};
        {AckTo, Completed2}  ->
            TcpIOL = riak_repl2_rtframe:encode(ack, AckTo),
            T:send(S, TcpIOL),
            {noreply, State#state{acked_seq = AckTo, completed = Completed2}}
    end;
handle_cast({ack, Ref, Seq, _Skips}, State) ->
    %% Nothing to send, it's old news.
    lager:debug("Received ack ~p for previous sequence ~p\n", [Seq, Ref]),
    {noreply, State};
handle_cast({drop, BucketType}, #state{bt_timer = undefined, bt_interval = Interval} = State) ->
    {ok, Timer} = timer:send_after(Interval, report_bt_drops),
    {noreply, bt_dropped(BucketType, State#state{bt_timer = Timer})};
handle_cast({drop, BucketType}, State) ->
    {noreply, bt_dropped(BucketType, State)}.

handle_info({Proto, _S, TcpBin}, State= #state{cont = Cont})
        when Proto == tcp; Proto == ssl ->
    recv(<<Cont/binary, TcpBin/binary>>, State);
handle_info({Closed, _S}, State = #state{cont = Cont})
        when Closed == tcp_closed; Closed == ssl_closed ->
    case size(Cont) of
        0 ->
            ok;
        NumBytes ->
            riak_repl_stats:rt_sink_errors(),
            %% cached_peername not caclulated from socket, so should be valid
            lager:warning("Realtime connection from ~p closed with partial receive of ~b bytes\n",
                          [peername(State), NumBytes])
    end,
    {stop, normal, State};
handle_info({Error, _S, Reason}, State= #state{cont = Cont}) when
        Error == tcp_error; Error == ssl_error ->
    %% peername not calculated from socket, so should be valid
    riak_repl_stats:rt_sink_errors(),
    lager:warning("Realtime connection from ~p network error ~p - ~b bytes pending\n",
                  [peername(State), Reason, size(Cont)]),
    {stop, normal, State};
handle_info(reactivate_socket, State = #state{remote = Remote, transport = T, socket = S,
                                              max_pending = MaxPending}) ->
    case pending(State) > MaxPending of
        true ->
            {noreply, schedule_reactivate_socket(State#state{active = false})};
        _ ->
            lager:debug("Realtime sink recovered - reactivating transport ~p socket ~p\n",
                        [T, S]),
            %% Check the socket is ok
            case T:peername(S) of
                {ok, _} ->
                    T:setopts(S, [{active, once}]), % socket could die, pick it up on tcp_error msgs
                    {noreply, State#state{active = true}};
                {error, Reason} ->
                    riak_repl_stats:rt_sink_errors(),
                    lager:error("Realtime replication sink for ~p had socket error - ~p\n",
                                [Remote, Reason]),
                    {stop, normal, State}
            end
    end;
handle_info(report_bt_drops, State=#state{bt_drops = DropDict}) ->
    Total = dict:fold(fun(BucketType, Counter, TotalCount) ->
                          lager:error("drops due to missing or mismatched type ~p: ~p", 
                          [BucketType, Counter]),
                          TotalCount + Counter
                      end,
                      0,
                      DropDict),
    lager:error("total bucket type drop count: ~p", [Total]),
    Report = app_helper:get_env(riak_repl, bucket_type_drop_report_interval, ?DEFAULT_INTERVAL_MILLIS),
    {noreply, State#state{bt_drops = dict:new(), bt_timer = undefined, bt_interval = Report}}.

terminate(_Reason, State) ->
    %% TODO: Consider trying to do something graceful with poolboy?
    catch riak_repl2_rtsink_helper:stop(State#state.helper),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

send_heartbeat(Transport, Socket) ->
    Transport:send(Socket, riak_repl2_rtframe:encode(heartbeat, undefined)).

%% Receive TCP data - decode framing and dispatch
recv(TcpBin, State = #state{transport = T, socket = S}) ->
    case riak_repl2_rtframe:decode(TcpBin) of
        {ok, undefined, Cont} ->
            case State#state.active of
                true ->
                    T:setopts(S, [{active, once}]);
                _ ->
                    ok
            end,
            {noreply, State#state{cont = Cont}};
        {ok, {objects, {Seq, BinObjs}}, Cont} ->
            recv(Cont, do_write_objects(Seq, BinObjs, State));
        {ok, heartbeat, Cont} ->
            send_heartbeat(T, S),
            recv(Cont, State#state{hb_last = os:timestamp()});
        {ok, {objects_and_meta, {Seq, BinObjs, Meta}}, Cont} ->
            recv(Cont, do_write_objects(Seq, {BinObjs, Meta}, State))
    end.

make_donefun({Binary, Meta}, Me, Ref, Seq) ->
    Done = fun() ->
        Skips = orddict:fetch(skip_count, Meta),
        gen_server:cast(Me, {ack, Ref, Seq, Skips}),
        maybe_push(Binary, Meta)
    end,
    {Done, Binary, Meta};
make_donefun(Binary, Me, Ref, Seq) when is_binary(Binary) ->
    Done = fun() ->
        gen_server:cast(Me, {ack, Ref, Seq, 0})
    end,
    {Done, Binary}.

maybe_push(Binary, Meta) ->
    case app_helper:get_env(riak_repl, realtime_cascades, always) of
        never ->
            lager:debug("Skipping cascade due to app env setting"),
            ok;
        always ->
          lager:debug("app env either set to always, or in default; doing cascade"),
          List = riak_repl_util:from_wire(Binary),
          Meta2 = orddict:erase(skip_count, Meta),
          riak_repl2_rtq:push(length(List), Binary, Meta2)
    end.

%% Note match on Seq
do_write_objects(Seq, BinObjsMeta, State = #state{max_pending = MaxPending,
                                              helper = Helper,
                                              seq_ref = Ref,
                                              expect_seq = Seq,
                                              acked_seq = AckedSeq,
                                              ver = Ver}) ->
    Me = self(),
    case make_donefun(BinObjsMeta, Me, Ref, Seq) of
        {DoneFun, BinObjs, Meta} ->
            case riak_repl_bucket_type_util:bucket_props_match(Meta) of
                true ->
                    riak_repl2_rtsink_helper:write_objects(Helper, BinObjs, DoneFun, Ver);
                false ->
                    BucketType = riak_repl_bucket_type_util:prop_get(?BT_META_TYPE, ?DEFAULT_BUCKET_TYPE, Meta),
                    lager:debug("Bucket type:~p is not equal on both the source and sink; not writing object.",
                                [BucketType]),
                    gen_server:cast(Me, {drop, BucketType}),
                    DoneFun()
            end;
        {DoneFun, BinObjs} ->
            %% this is for backwards compatibility with Repl version before metadata support (> 1.4)
            riak_repl2_rtsink_helper:write_objects(Helper, BinObjs, DoneFun, Ver)
    end,
    State2 = case AckedSeq of
                 undefined ->
                     %% Handle first received sequence number
                     State#state{acked_seq = Seq - 1, expect_seq = Seq + 1};
                 _ ->
                     State#state{expect_seq = Seq + 1}
             end,
    %% If the socket is too backed up, take a breather
    %% by setting {active, false} then casting a message to ourselves
    %% to enable it once under the limit
    case pending(State2) > MaxPending of
        true ->
            schedule_reactivate_socket(State2);
        _ ->
            State2
    end;
do_write_objects(Seq, BinObjs, State) ->
    %% Did not get expected sequence.
    %%
    %% If the source dropped (rtq consumer behind tail of queue), there
    %% is no point acknowledging any more if it is unable to resend anyway.
    %% Reset seq ref/completion array and start over at new sequence.
    %%
    %% If the sequence number wrapped?  don't worry about acks, happens infrequently.
    %%
    State2 = reset_ref_seq(Seq, State),
    do_write_objects(Seq, BinObjs, State2).

insert_completed(Seq, Skipped, Completed) ->
    Foldfun = fun(N, Acc) ->
        ordsets:add_element(N, Acc)
    end,
    lists:foldl(Foldfun, Completed, lists:seq(Seq - Skipped, Seq)).

reset_ref_seq(Seq, State) ->
    #state{source_drops = SourceDrops, expect_seq = ExpSeq} = State,
    NewSeqRef = make_ref(),
    SourceDrops2 = case ExpSeq of
        undefined -> % no need to tell user about first time through
            SourceDrops;
        _ ->
            SourceDrops + Seq - ExpSeq
    end,
    State#state{seq_ref = NewSeqRef, expect_seq = Seq, acked_seq = Seq - 1,
        completed = [], source_drops = SourceDrops2}.

%% Work out the highest sequence number that can be acked
%% and return it, completed always has one or more elements on first
%% call.
ack_to(Acked, []) ->
    {Acked, []};
ack_to(Acked, [LessThanAck | _] = Completed) when LessThanAck =< Acked ->
    ack_to(LessThanAck - 1, Completed);
ack_to(Acked, [Seq | Completed2] = Completed) ->
    case Acked + 1 of
        Seq ->
            ack_to(Seq, Completed2);
        _ ->
            {Acked, Completed}
    end.

%% Work out how many requests are pending (not writted yet, may not
%% have been acked)
pending(#state{acked_seq = undefined}) ->
    0; % nothing received yet
pending(#state{expect_seq = ExpSeq, acked_seq = AckedSeq,
               completed = Completed}) ->
    ExpSeq - AckedSeq - length(Completed) - 1.

%% get the peername from the transport; this will fail if the socket
%% is closed
peername(Transport, Socket) ->
    case Transport:peername(Socket) of
        {ok, Res} ->
            Res;
        {error, Reason} ->
            riak_repl_stats:rt_sink_errors(),
            {lists:flatten(io_lib:format("error:~p", [Reason])), 0}
    end.
%% get the peername from #state; this should have been stashed at
%% initialization.
peername(#state{peername = P}) ->
    P.

schedule_reactivate_socket(State = #state{transport = T,
                                          socket = S,
                                          active = Active,
                                          deactivated = Deactivated}) ->
    case Active of
        true ->
            lager:debug("Realtime sink overloaded - deactivating transport ~p socket ~p\n",
                        [T, S]),
            T:setopts(S, [{active, false}]),
            self() ! reactivate_socket,
            State#state{active = false, deactivated = Deactivated + 1};
        false ->
            %% already deactivated, try again in configured interval, or default
            ReactivateSockInt = get_reactivate_socket_interval(),
            lager:debug("reactivate_socket_interval_millis: ~sms.",
              [ReactivateSockInt]),

            erlang:send_after(ReactivateSockInt, self(), reactivate_socket),
            State#state{active = {false, scheduled}};
        {false, scheduled} ->
            %% have a check scheduled already
            State
    end.
get_reactivate_socket_interval() ->
    app_helper:get_env(riak_repl, reactivate_socket_interval_millis, ?REACTIVATE_SOCK_INT_MILLIS).

bt_dropped(BucketType, #state{bt_drops = BucketDict} = State) ->
    State#state{bt_drops = dict:update_counter(BucketType, 1, BucketDict)}.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

-define(SINK_PORT, 5008).
-define(LOOPBACK_TEST_PEER, {127,0,0,1}).
-define(VER1, {1,0}).
-define(PROTOCOL(NegotiatedVer), {realtime, NegotiatedVer, NegotiatedVer}).
-define(PROTOCOL_V1, ?PROTOCOL(?VER1)).
-define(REACTIVATE_SOCK_INT_MILLIS_TEST_VAL, 20).
-define(PORT_RANGE, 999999).

-compile([export_all, nowarn_export_all]).

riak_repl2_rtsink_conn_test_() ->
    {spawn,
     [
      {setup,
        fun setup/0,
        fun cleanup/1,
        [
         fun cache_peername_test_case/0,
         fun reactivate_socket_interval_test_case/0
        ]
      }]}.

setup() ->
    riak_repl_test_util:start_test_ring(),
    riak_repl_test_util:abstract_gen_tcp(),
    riak_repl_test_util:kill_and_wait(riak_repl2_rt),
    {ok, _RT} = riak_repl2_rt:start_link(),
    riak_repl_test_util:kill_and_wait(riak_repl2_rtq),
    {ok, _} = riak_repl2_rtq:start_link(),
    ok.

cleanup(_Ctx) ->
    riak_repl_test_util:kill_and_wait(riak_core_tcp_mon),
    riak_repl_test_util:kill_and_wait(riak_repl2_rtq),
    riak_repl_test_util:kill_and_wait(riak_repl2_rt),
    riak_repl_test_util:stop_test_ring(),
    riak_repl_test_util:maybe_unload_mecks(
      [riak_core_service_mgr,
       riak_core_connection_mgr,
       riak_repl_util,
       riak_core_tcp_mon,
       gen_tcp]),
    meck:unload(),
    ok.

%% test for https://github.com/basho/riak_repl/issues/247
%% cache the peername so that when the local socket is closed
%% peername will still be around for logging
cache_peername_test_case() ->

    TellMe = self(),

    catch(meck:unload(riak_core_service_mgr)),
    meck:new(riak_core_service_mgr, [passthrough]),
    meck:expect(riak_core_service_mgr, sync_register_service, fun(HostSpec, _Strategy) ->
        {_Proto, {TcpOpts, _Module, _StartCB, _CBArg}} = HostSpec,
        {ok, Listen} = gen_tcp:listen(?SINK_PORT, [binary, {reuseaddr, true} | TcpOpts]),
        TellMe ! sink_listening,
        {ok, Socket} = gen_tcp:accept(Listen),
        {ok, Pid} = riak_repl2_rtsink_conn:start_link({ok, ?PROTOCOL(?VER1)}, "source_cluster"),

        ok = gen_tcp:controlling_process(Socket, Pid),
        ok = riak_repl2_rtsink_conn:set_socket(Pid, Socket, gen_tcp),

        % Socket is set, close it to simulate error
        inet:close(Socket),

        % grab the State from the rtsink_conn process
        {status,Pid,_,[_,_,_,_,[_,_,{data,[{_,State}]}]]} = sys:get_status(Pid),

        % check to make sure peername is cached, not calculated from (now closed) Socket
        ?assertMatch({?LOOPBACK_TEST_PEER, _Port}, peername(State)),

        TellMe ! {sink_started, Pid}
    end),

    catch(meck:unload(riak_repl_util)),
    meck:new(riak_repl_util, [passthrough]),
    meck:expect(riak_repl_util, generate_socket_tag, fun(Prefix, _Transport, _Socket) ->
         Portnum = rand:uniform(?PORT_RANGE),
         lists:flatten(io_lib:format("~s_~p -> ~p:~p",[
                Prefix,
                Portnum,
                ?LOOPBACK_TEST_PEER,
                ?SINK_PORT]))
         end),

    catch(meck:unload(riak_core_tcp_mon)),
    meck:new(riak_core_tcp_mon, [passthrough]),
    meck:expect(riak_core_tcp_mon, monitor, fun(Socket, _Tag, Transport) ->
                {reply, ok,  #state{transport=Transport, socket=Socket}}
                end),

    {ok, _SinkPid} = start_sink(),
    {ok, {_Source, _Sink}} = start_source(?VER1).

% test case for https://github.com/basho/riak_repl/issues/252
reactivate_socket_interval_test_case() ->
    ?assertEqual(?REACTIVATE_SOCK_INT_MILLIS, get_reactivate_socket_interval()),

    application:set_env(riak_repl, reactivate_socket_interval_millis, ?REACTIVATE_SOCK_INT_MILLIS_TEST_VAL),
    ?assertEqual(?REACTIVATE_SOCK_INT_MILLIS_TEST_VAL, get_reactivate_socket_interval()).

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
            ok = Module:connected(Socket, gen_tcp, {"localhost", ?SINK_PORT},
              ?PROTOCOL(NegotiatedVer), Pid, [])
        end),
        {ok, make_ref()}
    end),
    meck:expect(riak_core_connection_mgr, disconnect, fun(_Remote) ->
        ok
    end),
    {ok, SourcePid} = riak_repl2_rtsource_conn:start_link("sink_cluster"),
    receive
        {sink_started, SinkPid} ->
            {ok, {SourcePid, SinkPid}}
    after 1000 ->
        {error, timeout}
    end.

start_sink() ->

    Pid = proc_lib:spawn_link(?MODULE, listen_sink, []),
    receive
        sink_listening ->
            {ok, Pid}
    after 10000 ->
            {error, timeout}
    end.
unload_mecks() ->
    riak_repl_test_util:maybe_unload_mecks([
        stateful, riak_core_ring_manager, riak_core_ring,
        riak_repl2_rtsink_helper, gen_tcp, fake_source, riak_repl2_rtq]).


-endif.
