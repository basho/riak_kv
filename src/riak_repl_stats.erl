%% Riak EnterpriseDS
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
-module(riak_repl_stats).
-author('Andy Gross <andy@basho.com>').
-behaviour(gen_server).
-include("riak_repl.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).
-export([start_link/0,
         stop/0,
         client_bytes_sent/1,
         client_bytes_recv/1,
         client_connects/0,
         client_connect_errors/0,
         client_redirect/0,
         server_bytes_sent/1,
         server_bytes_recv/1,
         server_connects/0,
         server_connect_errors/0,
         server_fullsyncs/0,
         objects_dropped_no_clients/0,
         objects_dropped_no_leader/0,
         objects_sent/0,
         objects_forwarded/0,
         elections_elected/0,
         elections_leader_changed/0,
         register_stats/0,
         get_stats/0,
         produce_stats/0,
         rt_source_errors/0,
         rt_sink_errors/0,
         clear_rt_dirty/0,
         touch_rt_dirty_file/0,
         remove_rt_dirty_file/0,
         is_rt_dirty/0]).

-define(APP, riak_repl).

start_link() -> gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

stop() ->
    gen_server:cast(?MODULE, stop).

register_stats() ->
    _ = [reregister_stat(Name, Type) || {Name, Type} <- stats()],
    riak_core_stat_cache:register_app(?APP, {?MODULE, produce_stats, []}),
    ok = folsom_metrics:notify_existing_metric({?APP, last_report}, tstamp(), gauge).

reregister_stat(Name, Type) ->
    catch folsom_metrics:delete_metric({?APP, Name}),
    register_stat(Name, Type).

client_bytes_sent(Bytes) ->
    increment_counter(client_bytes_sent, Bytes).

client_bytes_recv(Bytes) ->
    increment_counter(client_bytes_recv, Bytes).

client_connects() ->
    increment_counter(client_connects).

client_connect_errors() ->
    increment_counter(client_connect_errors).

client_redirect() ->
    increment_counter(client_redirect).

server_bytes_sent(Bytes) ->
    increment_counter(server_bytes_sent, Bytes).

server_bytes_recv(Bytes) ->
    increment_counter(server_bytes_recv, Bytes).

server_connects() ->
    increment_counter(server_connects).

server_connect_errors() ->
    increment_counter(server_connect_errors).

server_fullsyncs() ->
    increment_counter(server_fullsyncs).

objects_dropped_no_clients() ->
    increment_counter(objects_dropped_no_clients).

objects_dropped_no_leader() ->
    increment_counter(objects_dropped_no_leader).

objects_sent() ->
    increment_counter(objects_sent).

objects_forwarded() ->
    increment_counter(objects_forwarded).

elections_elected() ->
    increment_counter(elections_elected).

elections_leader_changed() ->
    increment_counter(elections_leader_changed).

%% If any source errors are detected, write a file out to persist this status
%% across restarts
rt_source_errors() ->
    increment_counter(rt_source_errors),
    rt_dirty().

%% If any sink errors are detected, write a file out to persist this status
%% across restarts
rt_sink_errors() ->
    increment_counter(rt_sink_errors),
    rt_dirty().

rt_dirty() ->
    touch_rt_dirty_file(),
    increment_counter(rt_dirty),
    Stat = lookup_stat(rt_dirty) + 1,  % we know it's at least 1
    % increment counter is a cast, so if the number is relatively small
    % then notify the server. otherwise, we don't want to spam the
    % coordinator. Once this value is reset to 0, we'll notify the
    % coordinator again.
    case Stat > 0 andalso Stat < 5 of
        true ->
            %% the coordinator might not be up yet
            lager:debug("Notifying coordinator of rt_dirty"),
            % notify the coordinator at a later time, hopefully
            % after the fscoordinator is started. If it's not, then just
            % log a warning.
            spawn(fun() ->
                      timer:sleep(30000),
                      try
                        riak_repl2_fscoordinator:node_dirty(node())
                      catch
                        _:_ ->
                         lager:debug("Failed to notify coordinator of rt_dirty status")
                      end
            end),
            ok;
        false -> ok
    end.


get_stats() ->
    case erlang:whereis(riak_repl_stats) of
        Pid when is_pid(Pid) ->
            {ok, Stats, _TS} = riak_core_stat_cache:get_stats(?APP),
            Stats;
        _ -> []
    end.

produce_stats() ->
    lists:flatten([backwards_compat(Stat, Type) ||
        {Stat, Type} <- stats()]).

init([]) ->
    register_stats(),
    schedule_report_bw(),
    case is_rt_dirty() of
        true ->
            lager:warning("RT marked as dirty upon startup"),
            ok = folsom_metrics:notify_existing_metric({?APP, rt_dirty}, {inc, 1}, counter),
            % let the coordinator know about the dirty state when the node
            % comes back up
            lager:debug("Notifying coordinator of rt_dirty state"),
            riak_repl2_fscoordinator:node_dirty(node());
        false ->
            lager:debug("RT is NOT dirty")
    end,
    {ok, ok}.

register_stat(Name, counter) ->
    ok = folsom_metrics:new_counter({?APP, Name});
register_stat(Name, history) ->
    BwHistoryLen =  get_bw_history_len(),
    ok = folsom_metrics:new_history({?APP, Name}, BwHistoryLen);
register_stat(Name, gauge) ->
    ok = folsom_metrics:new_gauge({?APP, Name}).

stats() ->
    [{server_bytes_sent, counter},
     {server_bytes_recv, counter},
     {server_connects, counter},
     {server_connect_errors, counter},
     {server_fullsyncs, counter},
     {client_bytes_sent, counter},
     {client_bytes_recv, counter},
     {client_connects, counter},
     {client_connect_errors, counter},
     {client_redirect, counter},
     {objects_dropped_no_clients, counter},
     {objects_dropped_no_leader, counter},
     {objects_sent, counter},
     {objects_forwarded, counter},
     {elections_elected, counter},
     {elections_leader_changed, counter},
     {client_rx_kbps, history},
     {client_tx_kbps, history},
     {server_rx_kbps, history},
     {server_tx_kbps, history},
     {last_report, gauge},
     {last_client_bytes_sent, gauge},
     {last_client_bytes_recv, gauge},
     {last_server_bytes_sent, gauge},
     {last_server_bytes_recv, gauge},
     {rt_source_errors, counter},
     {rt_sink_errors, counter},
     {rt_dirty, counter}].

increment_counter(Name) ->
    increment_counter(Name, 1).

increment_counter(Name, IncrBy) when is_atom(Name) andalso is_integer(IncrBy) ->
    gen_server:cast(?MODULE, {increment_counter, Name, IncrBy}).

handle_call(_Req, _From, State) ->
    {reply, ok, State}.

handle_cast({increment_counter, Name, IncrBy}, State) ->
    ok = folsom_metrics:notify_existing_metric({?APP, Name}, {inc, IncrBy}, counter),
    {noreply, State};
handle_cast(stop, State) ->
    {stop, normal, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(report_bw, State) ->
    ThisClientBytesSent=lookup_stat(client_bytes_sent),
    ThisClientBytesRecv=lookup_stat(client_bytes_recv),
    ThisServerBytesSent=lookup_stat(server_bytes_sent),
    ThisServerBytesRecv=lookup_stat(server_bytes_recv),

    Now = tstamp(),
    DeltaSecs = now_diff(Now, lookup_stat(last_report)),
    ClientTx = bytes_to_kbits_per_sec(ThisClientBytesSent, lookup_stat(last_client_bytes_sent), DeltaSecs),
    ClientRx = bytes_to_kbits_per_sec(ThisClientBytesRecv, lookup_stat(last_client_bytes_recv), DeltaSecs),
    ServerTx = bytes_to_kbits_per_sec(ThisServerBytesSent, lookup_stat(last_server_bytes_sent), DeltaSecs),
    ServerRx = bytes_to_kbits_per_sec(ThisServerBytesRecv, lookup_stat(last_server_bytes_recv), DeltaSecs),

    _ = [ok = folsom_metrics:notify_existing_metric({?APP, Metric}, Reading, history)
     || {Metric, Reading} <- [{client_tx_kbps, ClientTx},
                              {client_rx_kbps, ClientRx},
                              {server_tx_kbps, ServerTx},
                              {server_rx_kbps, ServerRx}]],

    _ = [ok = folsom_metrics:notify_existing_metric({?APP, Metric}, Reading, gauge)
     || {Metric, Reading} <- [{last_client_bytes_sent, ThisClientBytesSent},
                              {last_client_bytes_recv, ThisClientBytesRecv},
                              {last_server_bytes_sent, ThisServerBytesSent},
                              {last_server_bytes_recv, ThisServerBytesRecv}]],

    schedule_report_bw(),
    ok = folsom_metrics:notify_existing_metric({?APP, last_report}, Now, gauge),
    {noreply, State};
handle_info(_Info, State) -> {noreply, State}.

terminate(_Reason, _State) -> ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

schedule_report_bw() ->
    BwHistoryInterval = app_helper:get_env(riak_repl, bw_history_interval, 60000),
    erlang:send_after(BwHistoryInterval, self(), report_bw).

%% Convert two values in bytes to a kbits/sec
bytes_to_kbits_per_sec(This, Last, Delta) when is_number(This), is_number(Last), is_number(Delta), Delta > 1.0e-6 ->
    trunc((This - Last) / (128 * Delta));  %% x8/1024 = x/128
bytes_to_kbits_per_sec(_, _, _) ->
    undefined.

lookup_stat(Name) ->
    folsom_metrics:get_metric_value({?APP, Name}).

now_diff(NowSecs, ThenSecs) when is_number(NowSecs), is_number(ThenSecs) ->
    NowSecs - ThenSecs;
now_diff(_, _) ->
    undefined.

tstamp() ->
    folsom_utils:now_epoch().

get_bw_history_len() ->
    app_helper:get_env(riak_repl, bw_history_len, 8).

backwards_compat(Name, history) ->
    Stats = folsom_metrics:get_history_values({?APP, Name}, get_bw_history_len()),
    Readings = [[Reading || {event, Reading} <- Events] || {_Moment, Events} <- Stats],
    {Name, lists:flatten(Readings)};
backwards_compat(_Name, gauge) ->
    [];
backwards_compat(Name,  _Type) ->
    {Name, lookup_stat(Name)}.

rt_dirty_filename() ->
    %% or riak_repl/work_dir?
    P_DataDir = app_helper:get_env(riak_core, platform_data_dir),
    filename:join([P_DataDir, "riak_repl", "rt_dirty"]).

touch_rt_dirty_file() ->
    DirtyRTFile = rt_dirty_filename(),
    ok = filelib:ensure_dir(DirtyRTFile),
    case file:write_file(DirtyRTFile, "") of
        ok -> lager:debug("RT dirty file written to ~p", [DirtyRTFile]);
        ER -> lager:warning("Can't write to file ~p due to ~p",[DirtyRTFile,
                                                                ER])
    end.

remove_rt_dirty_file() ->
    DirtyRTFile = rt_dirty_filename(),
    case file:delete(DirtyRTFile) of
        ok -> lager:debug("RT dirty flag cleared");
        {error, Reason} ->
            %% this is a lager:debug because each fullsync
            %% would display this warning.
            lager:debug("Can't clear RT dirty flag: ~p",
                                       [Reason])
    end.


clear_rt_dirty() ->
    remove_rt_dirty_file(),
    %% folsom_metrics:notify_existing_metric doesn't support clear yet
    folsom_metrics_counter:clear({riak_repl, rt_dirty}).

is_rt_dirty() ->
    DirtyRTFile = rt_dirty_filename(),
    case file:read_file_info(DirtyRTFile) of
        {error, enoent} -> false;
        {error, Reason} ->
                  lager:warning("Error reading RT dirty file: ~p", [Reason]),
                  %% assume it's not there
                  false;
        {ok, _Data} -> true
    end.

-ifdef(TEST).

repl_stats_test_() ->
    error_logger:tty(false),
    {"stats test", setup, fun() ->
                    meck:new(folsom_utils, [passthrough]),
                    application:start(bear),
                    ok = folsom:start(),
                    meck:new(riak_core_cluster_mgr, [passthrough]),
                    meck:new(riak_repl2_fscoordinator_sup, [passthrough]),
                    meck:expect(riak_core_cluster_mgr, get_leader, fun() ->
                                node() end),
                    meck:expect(riak_repl2_fscoordinator_sup, started,
                        fun(_Node) -> [] end),
                    {ok, _CPid} = riak_core_stat_cache:start_link(),
                    {ok, Pid} = riak_repl_stats:start_link(),
                    tick(1000, 0),
                    Pid
            end,
     fun(_Pid) ->
             folsom:stop(),
             riak_repl_stats:stop(),
             riak_core_stat_cache:stop(),
             meck:unload(folsom_utils),
             meck:unload(riak_core_cluster_mgr),
             meck:unload(riak_repl2_fscoordinator_sup)
     end,
     [{"Register stats", fun test_register_stats/0},
      {"Populate stats", fun test_populate_stats/0},
      {"Check stats", fun test_check_stats/0},
      {"check report", fun  test_report/0}]
    }.

test_register_stats() ->
   error_logger:tty(false),
    register_stats(),
    RegisteredReplStats = [Stat || {App, Stat} <- folsom_metrics:get_metrics(),
                                   App == riak_repl],
    {Stats, _Types} = lists:unzip(stats()),
    ?assertEqual(lists:sort(Stats), lists:sort(RegisteredReplStats)).

test_populate_stats() ->
    error_logger:tty(false),
    Bytes = 1000,
    ok = client_bytes_sent(Bytes),
    ok = client_bytes_recv(Bytes),
    ok = client_connects(),
    ok = client_connect_errors(),
    ok = client_redirect(),
    ok = server_bytes_sent(Bytes),
    ok = server_bytes_recv(Bytes),
    ok = server_connects(),
    ok = server_connect_errors(),
    ok = server_fullsyncs(),
    ok = objects_dropped_no_clients(),
    ok = objects_dropped_no_leader(),
    ok = objects_sent(),
    ok = objects_forwarded(),
    ok = elections_elected(),
    ok = elections_leader_changed(),
    ok = rt_source_errors(),
    ok = rt_sink_errors().

test_check_stats() ->
   error_logger:tty(false),
    Expected = [
        {server_bytes_sent,1000},
        {server_bytes_recv,1000},
        {server_connects,1},
        {server_connect_errors,1},
        {server_fullsyncs,1},
        {client_bytes_sent,1000},
        {client_bytes_recv,1000},
        {client_connects,1},
        {client_connect_errors,1},
        {client_redirect,1},
        {objects_dropped_no_clients,1},
        {objects_dropped_no_leader,1},
        {objects_sent,1},
        {objects_forwarded,1},
        {elections_elected,1},
        {elections_leader_changed,1},
        {client_rx_kbps,[]},
        {client_tx_kbps,[]},
        {server_rx_kbps,[]},
        {server_tx_kbps,[]},
        {rt_source_errors,1},
        {rt_sink_errors, 1},
        {rt_dirty, 2}],
    Result = get_stats(),

    ?assertEqual(Expected,
        [{K1, V} || {K1, V} <- Result, {K2, _} <- Expected, K1 == K2]).


test_report() ->
    Bytes = 1024 * 60,
    Now = 10000,
    ReportMinutes = 10,
    for_n_minutes(ReportMinutes, Now, Bytes),
    %% I think the cache is getting in the way here?
    timer:sleep(1000),
    %% Rates = [Stat || {Name, _Val}=Stat <- get_stats(),
    %%                  lists:member(Name, [client_rx_kbps,
    %%                                      client_tx_kbps,
    %%                                      server_rx_kbps,
    %%                                      server_tx_kbps])],
    [begin
         ?assertEqual(8, length(Values)),
         [?assertEqual(8, Val) || Val <- Values]
     end || {Name, Values} <- get_stats(),
            lists:member(Name, [client_rx_kbps,
                                client_tx_kbps,
                                server_rx_kbps,
                                server_tx_kbps])].

for_n_minutes(0, _Time, _Bytes) ->
    ok;
for_n_minutes(Minutes, Time0, Bytes) ->
    Time = tick(Time0, 60),
    [folsom_metrics:notify_existing_metric({?APP, Name},
                                           {inc, Bytes},
                                           counter) || Name <-
                                                           [client_bytes_sent,
                                                            client_bytes_recv,
                                                            server_bytes_sent,
                                                            server_bytes_recv]],

    riak_repl_stats:handle_info(report_bw, ok),
    for_n_minutes(Minutes - 1, Time, Bytes).

tick(Time, Interval) ->
    NewTime = Time+Interval,
    meck:expect(folsom_utils, now_epoch, fun() ->
                                                 NewTime end),
    NewTime.

-endif.
