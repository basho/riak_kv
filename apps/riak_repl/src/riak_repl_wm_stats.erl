%% -------------------------------------------------------------------
%%
%% riak_repl_wm_stats: publishing Riak replication stats via HTTP
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

-module(riak_repl_wm_stats).

-export([
         init/1,
         encodings_provided/2,
         content_types_provided/2,
         service_available/2,
         forbidden/2,
         produce_body/2,
         pretty_print/2,
         jsonify_stats/2
        ]).
-include_lib("webmachine/include/webmachine.hrl").
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(ctx, {}).

init(_) ->
    {ok, #ctx{}}.

%% @spec encodings_provided(webmachine:wrq(), context()) ->
%%         {[encoding()], webmachine:wrq(), context()}
%% @doc Get the list of encodings this resource provides.
%%      "identity" is provided for all methods, and "gzip" is
%%      provided for GET as well
encodings_provided(ReqData, Context) ->
    case wrq:method(ReqData) of
        'GET' ->
            {[{"identity", fun(X) -> X end},
              {"gzip", fun(X) -> zlib:gzip(X) end}], ReqData, Context};
        _ ->
            {[{"identity", fun(X) -> X end}], ReqData, Context}
    end.

%% @spec content_types_provided(webmachine:wrq(), context()) ->
%%          {[ctype()], webmachine:wrq(), context()}
%% @doc Get the list of content types this resource provides.
%%      "application/json" and "text/plain" are both provided
%%      for all requests.  "text/plain" is a "pretty-printed"
%%      version of the "application/json" content.
content_types_provided(ReqData, Context) ->
    {[{"application/json", produce_body},
      {"text/plain", pretty_print}],
     ReqData, Context}.

service_available(ReqData, Ctx) ->
    {true, ReqData, Ctx}.

forbidden(RD, Ctx) ->
    {riak_kv_wm_utils:is_forbidden(RD), RD, Ctx}.

produce_body(ReqData, Ctx) ->
    Stats = get_stats(),
    lager:debug("STATS = ~p", [Stats]),
    Body = mochijson2:encode({struct,
                              Stats
                              }),
    {Body, ReqData, Ctx}.

%% @spec pretty_print(webmachine:wrq(), context()) ->
%%          {string(), webmachine:wrq(), context()}
%% @doc Format the response JSON object in a "pretty-printed" style.
pretty_print(RD1, C1=#ctx{}) ->
    {Json, RD2, C2} = produce_body(RD1, C1),
    {json_pp:print(binary_to_list(list_to_binary(Json))), RD2, C2}.

get_stats() ->
    RTRemotesStatus = riak_repl_console:rt_remotes_status(),
    FSRemotesStatus = riak_repl_console:fs_remotes_status(),
    Stats1 = riak_repl_stats:get_stats(),
    CMStats = riak_repl_console:cluster_mgr_stats(),
    LeaderStats = riak_repl_console:leader_stats(),
    Servers = riak_repl_console:server_stats(),
    Clients = riak_repl_console:client_stats(),
    Coord = riak_repl_console:coordinator_stats(),
    CoordSrv = riak_repl_console:coordinator_srv_stats(),
    RTQ = [{realtime_queue_stats, riak_repl2_rtq:status()}],
    PGStats = riak_repl2_pg:status(),

    Most = lists:append([RTRemotesStatus, FSRemotesStatus, Stats1, CMStats,
      LeaderStats, Servers, Clients, Coord, CoordSrv, RTQ]),
    KbpsSums = riak_repl_console:extract_rt_fs_send_recv_kbps(Most),
    jsonify_stats(RTRemotesStatus,[]) ++ jsonify_stats(FSRemotesStatus,[]) ++ CMStats ++ Stats1 ++ LeaderStats
        ++ jsonify_stats(Clients, [])
        ++ jsonify_stats(Servers, [])
    ++ RTQ
    ++ jsonify_stats(Coord,[])
    ++ jsonify_stats(CoordSrv,[]) ++ PGStats ++ jsonify_stats(KbpsSums, []).

format_pid(Pid) ->
    list_to_binary(riak_repl_util:safe_pid_to_list(Pid)).

format_pid_stat({Name, Value}) when is_pid(Value) ->
    {Name, format_pid(Value)};

format_pid_stat({Name, Pid, IP, Port}) when is_pid(Pid) ->
    {Name, format_pid(Pid), IP, Port};

format_pid_stat(Pair) ->
    Pair.


jsonify_stats([], Acc) ->
    lists:flatten(lists:reverse(Acc));

jsonify_stats([{fullsync, Num, _Left}|T], Acc) ->
    jsonify_stats(T, [{"partitions_left", Num} | Acc]);


jsonify_stats([{S,PidAsBinary, IP,Port}|T], Acc) when is_atom(S) andalso
        is_list(IP) andalso is_integer(Port) andalso is_binary(PidAsBinary) ->
    jsonify_stats(T, [{S,
                    [{connecting_pid, PidAsBinary},
                     {connecting_ip,
                      list_to_binary(IP++":"++integer_to_list(Port))}]
                    }|Acc]);

jsonify_stats([{S,IP,Port}|T], Acc) when is_atom(S) andalso is_list(IP) andalso is_integer(Port) ->
    jsonify_stats(T, [{S, list_to_binary(IP++":"++integer_to_list(Port))}|Acc]);
jsonify_stats([{K,V}|T], Acc) when is_pid(V) ->
    jsonify_stats(T, [{K,list_to_binary(riak_repl_util:safe_pid_to_list(V))}|Acc]);

jsonify_stats([{K,V=[{_,_}|_Tl]}|T], Acc) when is_list(V) ->
    NewV = jsonify_stats(V,[]),
    jsonify_stats(T, [{K,NewV}|Acc]);

jsonify_stats([{K, V}|T], Acc) when is_atom(K) and is_tuple(V)
        andalso (K == active) ->
    case V of
      {false, scheduled} ->
         jsonify_stats([{active, false}, {reactivation_scheduled, true} | T], Acc)
    end;
jsonify_stats([{K,V}|T], Acc) when is_atom(K)
        andalso (K == server_stats orelse K == client_stats), is_list(V) ->
    Filtered = lists:filter(fun
        ({_Pid, _Mq, {status, _Stats}}) -> true;
        (_) -> false
    end, V),
    StackedStats = lists:map(fun({Pid, Mq, {status, Stats}}) ->
        FormattedPid = format_pid(Pid),
        SectionName = proplists:get_value(site, Stats, FormattedPid),
        NewStats = {status, jsonify_stats(lists:map(fun format_pid_stat/1, Stats), [])},
        {list_to_binary(SectionName), {struct, [{pid, FormattedPid}, Mq, NewStats]}}
    end, Filtered),
    jsonify_stats(StackedStats ++ T, Acc);
jsonify_stats([{S,IP,Port}|T], Acc) when is_atom(S) andalso is_list(IP) andalso is_integer(Port) ->
    jsonify_stats(T, [{S,
                       list_to_binary(IP++":"++integer_to_list(Port))}|Acc]);
jsonify_stats([{S,{A,B,C,D},Port}|T], Acc) when is_atom(S) andalso is_integer(Port) ->
    jsonify_stats(T, [{S,
                       iolist_to_binary(io_lib:format("~b.~b.~b.~b:~b",[A,B,C,D,Port]))}|Acc]);
jsonify_stats([{K,{Mega,Secs,Micro}=Now}|T], Acc) when is_integer(Mega), is_integer(Secs), is_integer(Micro) ->
    StrDate = httpd_util:rfc1123_date(calendar:now_to_local_time(Now)),
    jsonify_stats(T, [{K, list_to_binary(StrDate)} | Acc]);
jsonify_stats([{K,V}|T], Acc) when is_list(V) ->
    jsonify_stats(T, [{K,list_to_binary(V)}|Acc]);
jsonify_stats([{K, {{Year, Month, Day}, {Hour, Min, Second}} = DateTime } | T], Acc) when is_integer(Year), is_integer(Month), is_integer(Day), is_integer(Hour), is_integer(Min), is_integer(Second) ->
    % the guard clause may be insane, but I just want to be very sure it's a
    % date time tuple that's being converted.
    StrDate = httpd_util:rfc1123_date(calendar:universal_time_to_local_time(DateTime)),
    jsonify_stats(T, [{K, list_to_binary(StrDate)} | Acc]);
jsonify_stats([{K,V}|T], Acc) ->
    jsonify_stats(T, [{K,V}|Acc]);
jsonify_stats([KV|T], Acc) ->
    lager:error("Could not encode stats: ~p", [KV]),
    jsonify_stats(T, Acc).
-ifdef(TEST).

% The lines line the following:
%
% _Result = mochijson2:encode({struct, Expected})
%
% are there to test for faulty json-ables. If the result cannot be
% jsonified, it will crash, causing the test to fail.

jsonify_stats_test_() ->
    [
	 {"correctly handle utc datetimes in rfc1123",
	  fun() ->
            %% this datetime doesn't exist in DST, without correct handling
            %% causes rfc1123 to explode.
			Actual = [{date, {{2017,3,26},{1,0,0}}}],
			Expected = [{date, <<"Sun, 26 Mar 2017 01:00:00 GMT">>}],
			?assertEqual(Expected, jsonify_stats(Actual, [])),
			_Result = mochijson2:encode({struct, Expected})
	  end
	 },
     %% test with bad stat to make sure
     %% the catch-all works
     {"catch-all",
      fun() ->
            Actual = [{this_is_a_bad_stat_key, "bar"}],
            Expected = [{this_is_a_bad_stat_key, <<"bar">>}],
            ?assertEqual(Expected, jsonify_stats(Actual, [])),
            _Result = mochijson2:encode({struct, Expected}) % fail if crash
      end},
     %% simple stats w/out a ton of useful data
     {"client stats",
      fun() ->
              Actual = [{client_stats,[]},{sinks,[]}],
              Expected = [{sinks,<<>>}],
              ?assertEqual(Expected, jsonify_stats(Actual, [])),
              _Result = mochijson2:encode({struct, Expected}) % fail if crash
      end},


     {"Coord, simple",
      fun() ->
              Actual = [{fullsync_coordinator,[{"bar",
                         [{cluster,"bar"},
                          {queued,0},
                          {in_progress,0},
                          {starting,0},
                          {successful_exits,0},
                          {error_exits,0},
                          {busy_nodes,0},
                          {running_stats,[]},
                          {socket,[]},
                          {fullsync_suggested,"dev1@127.0.0.1"},
                          {fullsync_suggested_during_fs,[]}]}]}],
              Expected = [{fullsync_coordinator,[{"bar",
                         [{cluster,<<"bar">>},
                          {queued,0},
                          {in_progress,0},
                          {starting,0},
                          {successful_exits,0},
                          {error_exits,0},
                          {busy_nodes,0},
                          {running_stats,<<>>},
                          {socket,<<>>},
                          {fullsync_suggested,<<"dev1@127.0.0.1">>},
                          {fullsync_suggested_during_fs,<<>>}]}]}],
              ?assertEqual(Expected, jsonify_stats(Actual, [])),
              _Result = mochijson2:encode({struct, Expected}) % fail if crash
      end},

     {"Coord during fullsync",
      fun() ->
             Date = {{2013, 9, 19}, {20, 51, 7}},
             BinDate = list_to_binary(httpd_util:rfc1123_date(calendar:universal_time_to_local_time(Date))),
             Input = [{fullsync_coordinator, [{"bar", [
                          {last_fullsync_started, Date}
                      ]}]}],
             Expected = [{fullsync_coordinator, [{"bar", [
                          {last_fullsync_started, BinDate}
                        ]}]}],
             Got = jsonify_stats(Input, []),
             %?debugFmt("Expected: ~p~nGot: ~p", [Expected, Got]),
             ?assertEqual(Expected, Got),
             _Result = mochijson2:encode({struct, Got}) % fail if crash
      end},

     {"Socket active, false scheduled",
      fun() ->
             Input = [{active, {false, scheduled}}],
             Expected = [{active, false}, {reactivation_scheduled, true}],
             Got = jsonify_stats(Input, []),
             ?assertEqual(Expected, Got),
             _Result = mochijson2:encode({struct, Expected}) % fail if crash
      end},

     {"Test catch-all",
      fun() ->
             Input = [{foo, bar, baz, 100, 200}],
             Got = jsonify_stats(Input, []),
             Expected = [],
             ?assertEqual(Expected, Got),
             _Result = mochijson2:encode({struct, Expected}) % fail if crash
      end},

     {"Test pid match",
      fun() ->
             Input = [{foo, self()}],
             Got = jsonify_stats(Input, []),
             MyPid = erlang:list_to_binary(erlang:pid_to_list(self())),
             Expected = [{foo,MyPid}],
             ?assertEqual(Expected, Got),
             _Result = mochijson2:encode({struct, Expected}) % fail if crash
      end},

     {"Test ip:port",
      fun() ->
             Input = [{foo, "127.0.0.1", 9010}],
             Got = jsonify_stats(Input, []),
             Expected = [{foo,<<"127.0.0.1:9010">>}],
             ?assertEqual(Expected, Got),
             _Result = mochijson2:encode({struct, Expected}) % fail if crash
      end},

     {"Test a,b,c,d:port",
      fun() ->
             Input = [{foo, {127,0,0,1}, 9010}],
             Got = jsonify_stats(Input, []),
             Expected = [{foo,<<"127.0.0.1:9010">>}],
             ?assertEqual(Expected, Got),
             _Result = mochijson2:encode({struct, Expected}) % fail if crash
      end},


     {"Coordsrv, empty",
      fun() ->
              Actual = [{fullsync_coordinator_srv,[]}],
              Expected = [{fullsync_coordinator_srv,<<>>}],
              ?assertEqual(Expected, jsonify_stats(Actual, [])),
              _Result = mochijson2:encode({struct, Expected}) % fail if crash
      end},


     {"Fullsyncs",
      fun() ->
              Actual = [{fullsync_enabled,"bar"},{fullsync_running,[]}],
              Expected = [{fullsync_enabled,<<"bar">>},{fullsync_running,<<>>}],
              ?assertEqual(Expected, jsonify_stats(Actual, [])),
              _Result = mochijson2:encode({struct, Expected}) % fail if crash
      end},


     {"Realtime remotes",
      fun() ->
              Actual = [{realtime_enabled,"bar"},{realtime_started,"bar"}],
              Expected = [{realtime_enabled,<<"bar">>},{realtime_started,<<"bar">>}],
              ?assertEqual(Expected, jsonify_stats(Actual, [])),
              _Result = mochijson2:encode({struct, Expected}) % fail if crash
      end},


     {"Servers",
      fun() ->
              Actual = [{server_stats,[]},
                        {sources,
                         [{source_stats,
                           [{pid,"<0.2157.0>"},
                            {message_queue_len,0},
                            {rt_source_connected_to,
                             [{sink,"bar"},{pid,"<0.2157.0>"},
                              {connected,false}]}]}]}],
                  Expected = [{sources,
                               [{source_stats,
                                 [{pid,<<"<0.2157.0>">>},
                                  {message_queue_len,0},
                                  {rt_source_connected_to,
                                   [{sink,<<"bar">>},
                                    {pid,<<"<0.2157.0>">>},
                                    {connected,false}]}]}]}],
              ?assertEqual(Expected, jsonify_stats(Actual, [])),
              _Result = mochijson2:encode({struct, Expected}) % fail if crash
      end},

     {"cmstats",
      fun() ->
              Actual = [{cluster_name,<<"foo">>},
                        {cluster_leader,'dev1@127.0.0.1'},
                        {connected_clusters,[<<"bar">>]}],
               _Result = mochijson2:encode({struct, Actual}) % fail if crash
      end},

     {"leaderstats",
      fun() ->
              Actual = [{leader,'dev1@127.0.0.1'},
                        {"leader_message_queue_len",0},
                        {"leader_total_heap_size",233},
                        {"leader_heap_size",233},
                        {"leader_stack_size",9},
                        {"leader_reductions",8555},
                        {"leader_garbage_collection",
                         [{min_bin_vheap_size,46368},
                          {min_heap_size,233},
                          {fullsweep_after,0},
                          {minor_gcs,0}]},
                        {"local_leader_message_queue_len",0},
                        {"local_leader_heap_size",233}],
              _Result = mochijson2:encode({struct, Actual}) % fail if crash
      end},

     {"rtqstats",
      fun() ->
              Actual = [{realtime_queue_stats,
                         [{percent_bytes_used, 0.001},
                          {bytes,768},
                          {max_bytes,104857600},
                          {consumers,
                           [{"bar",
                             [{pending,0},
                              {unacked,0},
                              {drops,0},
                              {errs,0}]}]}]}],
              _Result = mochijson2:encode({struct, Actual}) % fail if crash
      end},

     {"general stats",
      fun() ->
              Actual = [{server_bytes_sent,0},
                        {server_bytes_recv,0},
                        {server_connects,0},
                        {server_connect_errors,0},
                        {server_fullsyncs,0},
                        {client_bytes_sent,0},
                        {client_bytes_recv,0},
                        {client_connects,0},
                        {client_connect_errors,0},
                        {client_redirect,0},
                        {objects_dropped_no_clients,0},
                        {objects_dropped_no_leader,0},
                        {objects_sent,0},
                        {objects_forwarded,0},
                        {elections_elected,0},
                        {elections_leader_changed,0},
                        {client_rx_kbps,[0,0,0,0]},
                        {client_tx_kbps,[0,0,0,0]},
                        {server_rx_kbps,[0,0,0,0]},
                        {server_tx_kbps,[0,0,0,0]},
                        {rt_source_errors,0},
                        {rt_sink_errors,0},
                        {rt_dirty,1}],
              _Result = mochijson2:encode({struct, Actual}) % fail if crash
      end},

    {"multiple fs client connections",
     fun() ->
             Pid = spawn(fun() -> ok end),
             Actual = [{client_stats, [
                {Pid, {message_queue_len,0}, {status, [
                    {node,'riak@test-riak-1'},
                    {site,"test3"},
                    {strategy,riak_repl_keylist_server},
                    {fullsync_worker,Pid},
                    {queue_pid,Pid},
                    {dropped_count,0},
                    {queue_length,0},
                    {queue_byte_size,0},
                    {queue_max_size,104857600},
                    {queue_percentage,0},
                    {queue_pending,0},
                    {queue_max_pending,5},
                    {state,wait_for_partition}
                ]}},
                {Pid, {message_queue_len,0}, {status, [
                    {node,'riak@test-riak-1'},
                    {site,"site2"},
                    {strategy,riak_repl_keylist_server},
                    {fullsync_worker,Pid},
                    {queue_pid,Pid},
                    {dropped_count,0},
                    {queue_length,0},
                    {queue_byte_size,0},
                    {queue_max_size,104857600},
                    {queue_percentage,0},
                    {queue_pending,0},
                    {queue_max_pending,5},
                    {state,wait_for_partition}
                ]}}
            ]}],
            Jsonified = jsonify_stats(Actual, []),
            _ = mochijson2:encode({struct, Jsonified})
       end},

    {"multiple fs client connections - structure",
     fun() ->
             Pid = spawn(fun() -> ok end),
             Actual = [{client_stats, [
                {Pid, {message_queue_len,0}, {status, [
                    {node,'riak@test-riak-1'},
                    {site,"test3"},
                    {strategy,riak_repl_keylist_server},
                    {fullsync_worker,Pid},
                    {queue_pid,Pid},
                    {dropped_count,0},
                    {queue_length,0},
                    {queue_byte_size,0},
                    {queue_max_size,104857600},
                    {queue_percentage,0},
                    {queue_pending,0},
                    {queue_max_pending,5},
                    {state,wait_for_partition}
                ]}},
                {Pid, {message_queue_len,0}, {status, [
                    {node,'riak@test-riak-1'},
                    {site,"site2"},
                    {strategy,riak_repl_keylist_server},
                    {fullsync_worker,Pid},
                    {queue_pid,Pid},
                    {dropped_count,0},
                    {queue_length,0},
                    {queue_byte_size,0},
                    {queue_max_size,104857600},
                    {queue_percentage,0},
                    {queue_pending,0},
                    {queue_max_pending,5},
                    {state,wait_for_partition}
                ]}}
            ]}],
            Jsonified = jsonify_stats(Actual, []),
            Json = mochijson2:encode({struct, Jsonified}),
            Decoded = mochijson2:decode(Json),
            ?assertMatch({struct, _}, Decoded)
       end},

     %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
     %%% tests during basho bench, bnw fullsync, 1.2 + 1.3 realtime, source side

     {"FSCoord - busy",
      fun() ->
              Actual = [{fullsync_coordinator,
                         [{"bar",
                           [{cluster,"bar"},
                            {queued,11},
                            {in_progress,0},
                            {starting,0},
                            {successful_exits,53},
                            {error_exits,0},
                            {busy_nodes,1},
                            {running_stats,[]},
                            {socket,[{peername,"127.0.0.1:10056"},
                                     {sockname,"127.0.0.1:56511"},
                                     {recv_avg,"[67, 66, 66, 66, 66]"},
                                     {recv_cnt,"[14, 16, 16, 12]"},
                                     {recv_dvi,"[1, 1, 1, 1, 1]"},
                                     {recv_kbps,"[0, 0, 0, 0]"},
                                     {recv_max,"[75, 75, 75, 75, 75]"},
                                     {send_cnt,"[14, 16, 16, 12]"},
                                     {send_kbps,"[0, 0, 0, 0]"},
                                     {send_pend,"[0, 0, 0, 0, 0]"}]},
                            {fullsync_suggested,"dev1@127.0.0.1"},
                            {fullsync_suggested_during_fs,[]}]}]}],
              Expected = [{fullsync_coordinator,
                           [{"bar",
                             [{cluster,<<"bar">>},
                              {queued,11},
                              {in_progress,0},
                              {starting,0},
                              {successful_exits,53},
                              {error_exits,0},
                              {busy_nodes,1},
                              {running_stats,<<>>},
                              {socket,[{peername,<<"127.0.0.1:10056">>},
                                       {sockname,<<"127.0.0.1:56511">>},
                                       {recv_avg,<<"[67, 66, 66, 66, 66]">>},
                                       {recv_cnt,<<"[14, 16, 16, 12]">>},
                                       {recv_dvi,<<"[1, 1, 1, 1, 1]">>},
                                       {recv_kbps,<<"[0, 0, 0, 0]">>},
                                       {recv_max,<<"[75, 75, 75, 75, 75]">>},
                                       {send_cnt,<<"[14, 16, 16, 12]">>},
                                       {send_kbps,<<"[0, 0, 0, 0]">>},
                                       {send_pend,<<"[0, 0, 0, 0, 0]">>}]},
                              {fullsync_suggested,<<"dev1@127.0.0.1">>},
                              {fullsync_suggested_during_fs,<<>>}]}]}],
              ?assertEqual(Expected, jsonify_stats(Actual, [])),
               _Result = mochijson2:encode({struct, Expected}) % fail if crash
      end},

     {"FSRemotes - busy",
      fun() ->
              Actual = [{fullsync_enabled,"bar"},{fullsync_running,"bar"}],
              Expected = [{fullsync_enabled,<<"bar">>},{fullsync_running,<<"bar">>}],
              ?assertEqual(Expected, jsonify_stats(Actual, [])),
              _Result = mochijson2:encode({struct, Expected}) % fail if crash
      end},


     {"Servers - busy",
      fun() ->
              DummyPid = self(),
              StringPid = riak_repl_util:safe_pid_to_list(DummyPid),
              FormattedPid = format_pid(DummyPid),
              Actual =
                  [{server_stats,
                    [{DummyPid,
                      {message_queue_len,0},
                      {status,
                       [{node,'dev1@127.0.0.1'},
                        {site,"foo"},
                        {strategy,riak_repl_keylist_server},
                        {fullsync_worker,DummyPid},
                        {queue_pid,DummyPid},
                        {dropped_count,0},
                        {queue_length,0},
                        {queue_byte_size,0},
                        {queue_max_size,104857600},
                        {queue_percentage,0},
                        {queue_pending,1},
                        {queue_max_pending,5},
                        {state,wait_for_partition}]}}]},
                   {sources,
                    [{source_stats,
                      [{pid,StringPid},
                       {message_queue_len,0},
                       {rt_source_connected_to,
                        [{sink,"bar"},
                         {pid,StringPid},
                         {connected,true},
                         {transport,ranch_tcp},
                         {socket,
                          [{peername,"127.0.0.1:10046"},
                           {sockname,"127.0.0.1:56510"},
                           {recv_avg,"[13, 13, 13, 13, 13]"},
                           {recv_cnt,"[0, 663, 3172, 2395]"},
                           {recv_dvi,"[0, 0, 0, 0, 0]"},
                           {recv_kbps,"[0, 6, 32, 24]"},
                           {recv_max,"[59, 59, 59, 59, 59]"},
                           {send_cnt,"[0, 686, 3237, 2470]"},
                           {send_kbps,"[0, 5577, 26318, 20080]"},
                           {send_pend,"[0, 0, 0, 0, 0]"}]},
                         {helper_pid,StringPid},
                         {sent_seq,9841},
                         {objects,9841}]}]}]}],
              Expected =
                  [{<<"foo">>, {struct, [
                   {pid,FormattedPid},
                   {message_queue_len,0},
                   {status,
                    [{node,'dev1@127.0.0.1'},
                     {site,<<"foo">>},
                     {strategy,riak_repl_keylist_server},
                     {fullsync_worker,FormattedPid},
                     {queue_pid,FormattedPid},
                     {dropped_count,0},
                     {queue_length,0},
                     {queue_byte_size,0},
                     {queue_max_size,104857600},
                     {queue_percentage,0},
                     {queue_pending,1},
                     {queue_max_pending,5},
                     {state,wait_for_partition}]}]}},
                   {sources,
                    [{source_stats,
                      [{pid,FormattedPid},
                       {message_queue_len,0},
                       {rt_source_connected_to,
                        [{sink,<<"bar">>},
                         {pid,FormattedPid},
                         {connected,true},
                         {transport,ranch_tcp},
                         {socket,
                          [{peername,<<"127.0.0.1:10046">>},
                           {sockname,<<"127.0.0.1:56510">>},
                           {recv_avg,<<"[13, 13, 13, 13, 13]">>},
                           {recv_cnt,<<"[0, 663, 3172, 2395]">>},
                           {recv_dvi,<<"[0, 0, 0, 0, 0]">>},
                           {recv_kbps,<<"[0, 6, 32, 24]">>},
                           {recv_max,<<"[59, 59, 59, 59, 59]">>},
                           {send_cnt,<<"[0, 686, 3237, 2470]">>},
                           {send_kbps,<<"[0, 5577, 26318, 20080]">>},
                           {send_pend,<<"[0, 0, 0, 0, 0]">>}]},
                         {helper_pid,FormattedPid},
                         {sent_seq,9841},
                         {objects,9841}]}]}]}],
              ?assertEqual(Expected, jsonify_stats(Actual, [])),
              _Result = mochijson2:encode({struct, Expected}) % fail if crash
      end},

     {"General Stats - busy",
      fun() ->
              Actual = [{server_bytes_sent,102475507},
                        {server_bytes_recv,826684},
                        {server_connects,0},
                        {server_connect_errors,0},
                        {server_fullsyncs,1},
                        {client_bytes_sent,0},
                        {client_bytes_recv,0},
                        {client_connects,0},
                        {client_connect_errors,0},
                        {client_redirect,0},
                        {objects_dropped_no_clients,0},
                        {objects_dropped_no_leader,0},
                        {objects_sent,19348},
                        {objects_forwarded,0},
                        {elections_elected,0},
                        {elections_leader_changed,0},
                        {client_rx_kbps,[0]},
                        {client_tx_kbps,[0]},
                        {server_rx_kbps,"k"},
                        {server_tx_kbps,[12489]},
                        {rt_source_errors,0},
                        {rt_sink_errors,0},
                        {rt_dirty,1}],
              _Result = mochijson2:encode({struct, Actual}) % fail if crash
      end},

     %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
     %%% tests during basho bench, bnw fullsync, 1.2 + 1.3 realtime, sink side


     {"Clients - busy, sink side",
      fun() ->
              Actual = [{client_stats,[]},
                        {sinks,
                         [{sink_stats,
                           [{pid,"<0.3133.0>"},
                            {message_queue_len,0},
                            {rt_sink_connected_to,
                             [{source,"foo"},
                              {pid,"<0.3133.0>"},
                              {connected,true},
                              {transport,ranch_tcp},
                              {socket,
                               [{peername,"127.0.0.1:57901"},
                                {sockname,"127.0.0.1:10046"},
                                {recv_avg,"[45, 45, 45]"},
                                {recv_cnt,"[0, 0]"},
                                {recv_dvi,"[7, 7, 7]"},
                                {recv_kbps,"[0, 0]"},
                                {recv_max,"[61, 61, 61]"},
                                {send_cnt,"[0, 0]"},
                                {send_kbps,"[0, 0]"},
                                {send_pend,"[0, 0, 0]"}]},
                              {helper,"<0.3134.0>"},
                              {active,true},
                              {deactivated,0},
                              {source_drops,0},
                              {expect_seq,undefined},
                              {acked_seq,undefined},
                              {pending,0}]}]},
                          {sink_stats,
                           [{pid,"<0.3032.0>"},
                            {message_queue_len,0},
                            {rt_sink_connected_to,
                             [{source,"foo"},
                              {pid,"<0.3032.0>"},
                              {connected,true},
                              {transport,ranch_tcp},
                              {socket,
                               [{peername,"127.0.0.1:57896"},
                                {sockname,"127.0.0.1:10046"},
                                {recv_avg,"[45, 45, 45, 45]"},
                                {recv_cnt,"[0, 0, 0]"},
                                {recv_dvi,"[7, 7, 7, 7]"},
                                {recv_kbps,"[0, 0, 0]"},
                                {recv_max,"[61, 61, 61, 61]"},
                                {send_cnt,"[0, 0, 0]"},
                                {send_kbps,"[0, 0, 0]"},
                                {send_pend,"[0, 0, 0, 0]"}]},
                              {helper,"<0.3033.0>"},
                              {active,true},
                              {deactivated,0},
                              {source_drops,0},
                              {expect_seq,935},
                              {acked_seq,933},
                              {pending,1}]}]}]}],
              Expected = [{sinks,
                           [{sink_stats,
                             [{pid,<<"<0.3133.0>">>},
                              {message_queue_len,0},
                              {rt_sink_connected_to,
                               [{source,<<"foo">>},
                                {pid,<<"<0.3133.0>">>},
                                {connected,true},
                                {transport,ranch_tcp},
                                {socket,
                                 [{peername,<<"127.0.0.1:57901">>},
                                  {sockname,<<"127.0.0.1:10046">>},
                                  {recv_avg,<<"[45, 45, 45]">>},
                                  {recv_cnt,<<"[0, 0]">>},
                                  {recv_dvi,<<"[7, 7, 7]">>},
                                  {recv_kbps,<<"[0, 0]">>},
                                  {recv_max,<<"[61, 61, 61]">>},
                                  {send_cnt,<<"[0, 0]">>},
                                  {send_kbps,<<"[0, 0]">>},
                                  {send_pend,<<"[0, 0, 0]">>}]},
                                {helper,<<"<0.3134.0>">>},
                                {active,true},
                                {deactivated,0},
                                {source_drops,0},
                                {expect_seq,undefined},
                                {acked_seq,undefined},
                                {pending,0}]}]},
                            {sink_stats,
                             [{pid,<<"<0.3032.0>">>},
                              {message_queue_len,0},
                              {rt_sink_connected_to,
                               [{source,<<"foo">>},
                                {pid,<<"<0.3032.0>">>},
                                {connected,true},
                                {transport,ranch_tcp},
                                {socket,
                                 [{peername,<<"127.0.0.1:57896">>},
                                  {sockname,<<"127.0.0.1:10046">>},
                                  {recv_avg,<<"[45, 45, 45, 45]">>},
                                  {recv_cnt,<<"[0, 0, 0]">>},
                                  {recv_dvi,<<"[7, 7, 7, 7]">>},
                                  {recv_kbps,<<"[0, 0, 0]">>},
                                  {recv_max,<<"[61, 61, 61, 61]">>},
                                  {send_cnt,<<"[0, 0, 0]">>},
                                  {send_kbps,<<"[0, 0, 0]">>},
                                  {send_pend,<<"[0, 0, 0, 0]">>}]},
                                {helper,<<"<0.3033.0>">>},
                                {active,true},
                                {deactivated,0},
                                {source_drops,0},
                                {expect_seq,935},
                                {acked_seq,933},
                                {pending,1}]}]}]}],
              ?assertEqual(Expected, jsonify_stats(Actual, [])),
              _Result = mochijson2:encode({struct, Expected}) % fail if crash
      end},

     {"CoordSrv - busy, sink side",
      fun() ->
              Actual = [{fullsync_coordinator_srv,
                         [{"127.0.0.1:58467",
                           [{socket,
                             [{peername,"127.0.0.1:58467"},
                              {sockname,"127.0.0.1:10046"},
                              {recv_avg,"[54, 54, 54, 54, 54, 54, 54]"},
                              {recv_cnt,"[8, 15, 16, 17, 17, 18]"},
                              {recv_dvi,"[0, 0, 0, 0, 0, 0, 0]"},
                              {recv_kbps,"[0, 0, 0, 0, 0, 0]"},
                              {recv_max,"[61, 61, 61, 61, 61, 61, 61]"},
                              {send_cnt,"[8, 15, 16, 17, 17, 18]"},
                              {send_kbps,"[0, 0, 0, 0, 0, 0]"},
                              {send_pend,"[0, 0, 0, 0, 0, 0, 0]"}]}]}]}],
              Expected = [{fullsync_coordinator_srv,
                           [{"127.0.0.1:58467",
                             [{socket,
                               [{peername,<<"127.0.0.1:58467">>},
                                {sockname,<<"127.0.0.1:10046">>},
                                {recv_avg,<<"[54, 54, 54, 54, 54, 54, 54]">>},
                                {recv_cnt,<<"[8, 15, 16, 17, 17, 18]">>},
                                {recv_dvi,<<"[0, 0, 0, 0, 0, 0, 0]">>},
                                {recv_kbps,<<"[0, 0, 0, 0, 0, 0]">>},
                                {recv_max,<<"[61, 61, 61, 61, 61, 61, 61]">>},
                                {send_cnt,<<"[8, 15, 16, 17, 17, 18]">>},
                                {send_kbps,<<"[0, 0, 0, 0, 0, 0]">>},
                                {send_pend,<<"[0, 0, 0, 0, 0, 0, 0]">>}]}]}]}],
              ?assertEqual(Expected, jsonify_stats(Actual, [])),
              _Result = mochijson2:encode({struct, Expected}) % fail if crash
      end},

     %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
     %%% tests during basho bench, 1.2 fullsync, 1.2 + 1.3 realtime, source side


     {"Servers, busy, listener side",
      fun() ->
              DummyPid = self(),
              StringPid = riak_repl_util:safe_pid_to_list(DummyPid),
              FormattedPid = format_pid(DummyPid),
              Actual =
                  [{server_stats,
     [{DummyPid,
       {message_queue_len,0},
       {status,
           [{node,'dev1@127.0.0.1'},
            {site,"foo"},
            {strategy,riak_repl_keylist_server},
            {fullsync_worker,DummyPid},
            {queue_pid,DummyPid},
            {dropped_count,0},
            {queue_length,0},
            {queue_byte_size,0},
            {queue_max_size,104857600},
            {queue_percentage,0},
            {queue_pending,4},
            {queue_max_pending,5},
            {state,build_keylist},
            {fullsync,593735040165679310520246963290989976735222595584},
            {partition_start,0.28},
            {stage_start,0.28},
            {get_pool_size,5}]}}]},
 {sources,
     [{source_stats,
          [{pid,StringPid},
           {message_queue_len,0},
           {rt_source_connected_to,
               [{sink,"bar"},
                {pid,StringPid},
                {connected,true},
                {transport,ranch_tcp},
                {socket,
                    [{peername,"127.0.0.1:10046"},
                     {sockname,"127.0.0.1:59147"},
                     {recv_avg,"[46, 46, 46, 46, 46, 46, 46]"},
                     {recv_cnt,"[0, 0, 0, 0, 0, 0]"},
                     {recv_dvi,"[6, 6, 6, 6, 6, 6, 6]"},
                     {recv_kbps,"[0, 0, 0, 0, 0, 0]"},
                     {recv_max,"[59, 59, 59, 59, 59, 59, 59]"},
                     {send_cnt,"[0, 0, 0, 0, 0, 0]"},
                     {send_kbps,"[0, 0, 0, 0, 0, 0]"},
                     {send_pend,"[0, 0, 0, 0, 0, 0, 0]"}]},
                {helper_pid,StringPid},
                {sent_seq,229},
                {objects,229}]}]}]}],

              Expected = [
                    {<<"foo">>, {struct, [
                        {pid,FormattedPid},
                        {message_queue_len,0},
                        {status, [
                            {node,'dev1@127.0.0.1'},
                            {site,<<"foo">>},
                            {strategy,riak_repl_keylist_server},
                            {fullsync_worker,FormattedPid},
                            {queue_pid,FormattedPid},
                            {dropped_count,0},
                            {queue_length,0},
                            {queue_byte_size,0},
                            {queue_max_size,104857600},
                            {queue_percentage,0},
                            {queue_pending,4},
                            {queue_max_pending,5},
                            {state,build_keylist},
                            {fullsync,593735040165679310520246963290989976735222595584},
                            {partition_start,0.28},
                            {stage_start,0.28},
                            {get_pool_size,5}
                        ]}
                    ]}},
                   {sources,
                    [{source_stats,
                      [{pid,FormattedPid},
                       {message_queue_len,0},
                       {rt_source_connected_to,
                        [{sink,<<"bar">>},
                         {pid,FormattedPid},
                         {connected,true},
                         {transport,ranch_tcp},
                         {socket,
                          [{peername,<<"127.0.0.1:10046">>},
                           {sockname,<<"127.0.0.1:59147">>},
                           {recv_avg,<<"[46, 46, 46, 46, 46, 46, 46]">>},
                           {recv_cnt,<<"[0, 0, 0, 0, 0, 0]">>},
                           {recv_dvi,<<"[6, 6, 6, 6, 6, 6, 6]">>},
                           {recv_kbps,<<"[0, 0, 0, 0, 0, 0]">>},
                           {recv_max,<<"[59, 59, 59, 59, 59, 59, 59]">>},
                           {send_cnt,<<"[0, 0, 0, 0, 0, 0]">>},
                           {send_kbps,<<"[0, 0, 0, 0, 0, 0]">>},
                           {send_pend,<<"[0, 0, 0, 0, 0, 0, 0]">>}]},
                         {helper_pid,FormattedPid},
                         {sent_seq,229},
                         {objects,229}]}]}]}],
              ?assertEqual(Expected, jsonify_stats(Actual, [])),
              _Result = mochijson2:encode({struct, Expected}) % fail if crash
      end},



     %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
     %%% tests during basho bench, 1.2 fullsync, 1.2 + 1.3 realtime, sink side


     {"Clients, busy, site side",

      fun() ->
              DummyPid = self(),
              StringPid = riak_repl_util:safe_pid_to_list(DummyPid),
              FormattedPid = format_pid(DummyPid),
              Actual = [{client_stats,
     [{DummyPid,
       {message_queue_len,0},
       {status,
           [{node,'dev4@127.0.0.1'},
            {site,"foo"},
            {strategy,riak_repl_keylist_client},
            {fullsync_worker,DummyPid},
            {put_pool_size,5},
            {connected,"127.0.0.1",5666},
            {cluster_name,<<"{'dev1@127.0.0.1',{1359,730694,756806}}">>},
            {state,wait_for_fullsync}]}}]},
 {sinks,
     [{sink_stats,
          [{pid,StringPid},
           {message_queue_len,0},
           {rt_sink_connected_to,
               [{source,"foo"},
                {pid,StringPid},
                {connected,true},
                {transport,ranch_tcp},
                {socket,
                    [{peername,"127.0.0.1:63893"},
                     {sockname,"127.0.0.1:10046"},
                     {recv_avg,"[45]"},
                     {recv_cnt,"[]"},
                     {recv_dvi,"[7]"},
                     {recv_kbps,"[]"},
                     {recv_max,"[61]"},
                     {send_cnt,"[]"},
                     {send_kbps,"[]"},
                     {send_pend,"[0]"}]},
                {helper,StringPid},
                {active,true},
                {deactivated,0},
                {source_drops,0},
                {expect_seq,undefined},
                {acked_seq,undefined},
                {pending,0}]}]},
      {sink_stats,
          [{pid,StringPid},
           {message_queue_len,0},
           {fs_connected_to,
               [{node,'dev4@127.0.0.1'},
                {site,"foo"},
                {strategy,fullsync},
                {fullsync_worker,StringPid},
                {socket,
                    [{peername,"127.0.0.1:63925"},
                     {sockname,"127.0.0.1:10046"}]},
                {state,request_partition}]}]}]}],


              Expected = [
                {<<"foo">>, {struct, [
                    {pid,FormattedPid},
                    {message_queue_len,0},
                    {status, [
                        {node,'dev4@127.0.0.1'},
                        {site,<<"foo">>},
                        {strategy,riak_repl_keylist_client},
                        {fullsync_worker,FormattedPid},
                        {put_pool_size,5},
                        {connected,<<"127.0.0.1:5666">>},
                        {cluster_name,<<"{'dev1@127.0.0.1',{1359,730694,756806}}">>},
                        {state,wait_for_fullsync}
                    ]}
                ]}},
 {sinks,
     [{sink_stats,
          [{pid,FormattedPid},
           {message_queue_len,0},
           {rt_sink_connected_to,
               [{source,<<"foo">>},
                {pid,FormattedPid},
                {connected,true},
                {transport,ranch_tcp},
                {socket,
                    [{peername,<<"127.0.0.1:63893">>},
                     {sockname,<<"127.0.0.1:10046">>},
                     {recv_avg,<<"[45]">>},
                     {recv_cnt,<<"[]">>},
                     {recv_dvi,<<"[7]">>},
                     {recv_kbps,<<"[]">>},
                     {recv_max,<<"[61]">>},
                     {send_cnt,<<"[]">>},
                     {send_kbps,<<"[]">>},
                     {send_pend,<<"[0]">>}]},
                {helper,FormattedPid},
                {active,true},
                {deactivated,0},
                {source_drops,0},
                {expect_seq,undefined},
                {acked_seq,undefined},
                {pending,0}]}]},
      {sink_stats,
          [{pid,FormattedPid},
           {message_queue_len,0},
           {fs_connected_to,
               [{node,'dev4@127.0.0.1'},
                {site,<<"foo">>},
                {strategy,fullsync},
                {fullsync_worker,FormattedPid},
                {socket,
                    [{peername,<<"127.0.0.1:63925">>},
                     {sockname,<<"127.0.0.1:10046">>}]},
                {state,request_partition}]}]}]}],
              ?assertEqual(Expected, jsonify_stats(Actual, [])),
              _Result = mochijson2:encode({struct, Expected}) % fail if crash
      end},


     %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
     %%% Misc tests


     %% random tests that broke stats
     {"fullsync partitions left",
      fun() ->
              Stats = [{fullsync,63,left},
                       {partition,251195593916248939066258330623111144003363405824},
                       {partition_start,0.88},{stage_start,0.88}],
              Expected = [{"partitions_left",63},
                          {partition,251195593916248939066258330623111144003363405824},
                          {partition_start,0.88}, {stage_start,0.88}],
              ?assertEqual(Expected, jsonify_stats(Stats, []))
      end},
     {"legacy server_stats",
      fun() ->
              DummyPid = self(),
              Stats = [{server_stats,
                        [{DummyPid,
                          {message_queue_len,0},
                          {status,
                           [{node,'dev1@127.0.0.1'},
                            {site,"foo"},
                            {strategy,riak_repl_keylist_server},
                            {fullsync_worker,DummyPid},
                            {queue_pid,DummyPid},
                            {dropped_count,0},
                            {queue_length,0},
                            {queue_byte_size,0},
                            {queue_max_size,104857600},
                            {queue_percentage,0},
                            {queue_pending,0},
                            {queue_max_pending,5},
                            {state,wait_for_partition}]}}]},
                       {sources,[]}],
              FormattedPid = format_pid(DummyPid),
              Expected =
                  [{<<"foo">>, {struct, [
                    {pid,FormattedPid},
                    {message_queue_len,0},
                    {status,[{node,'dev1@127.0.0.1'},
                            {site,<<"foo">>},
                            {strategy,riak_repl_keylist_server},
                            {fullsync_worker,FormattedPid},
                            {queue_pid,FormattedPid},
                            {dropped_count,0},
                            {queue_length,0},
                            {queue_byte_size,0},
                            {queue_max_size,104857600},
                            {queue_percentage,0},
                            {queue_pending,0},
                            {queue_max_pending,5},
                            {state,wait_for_partition}]}
                    ]}},
                   {sources,<<>>}],
                  ?assertEqual(Expected, jsonify_stats(Stats, [])),
                  _Result = mochijson2:encode({struct, Expected}) % fail if crash
      end},


     {"borken connecting stat",
      fun() ->
              DummyPid = self(),
              FormattedPid = format_pid(DummyPid),
              %% test this [{server_stats,[]},{sources,[]}],
              Stats = [{client_stats,
                        [{DummyPid,{message_queue_len,0},
                          {status,
                           [{node,'dev4@127.0.0.1'},
                            {site,"foo"},
                            {strategy,undefined},
                            {fullsync_worker,undefined},
                            {connecting,DummyPid,"127.0.0.1",5666}]}}]},
                       {sinks,[]}],
              Expected =
                  [{<<"foo">>, {struct, [
                   {pid,FormattedPid},
                   {message_queue_len,0},
                   {status,[{node,'dev4@127.0.0.1'},
                            {site,<<"foo">>},
                            {strategy,undefined},
                            {fullsync_worker,undefined},
                            {connecting,[{connecting_pid,FormattedPid},
                                         {connecting_ip,<<"127.0.0.1:5666">>}]}]}]}},
                   {sinks,<<>>}],
              ?assertEqual(Expected, jsonify_stats(Stats, [])),
              _Result = mochijson2:encode({struct, Expected}) % fail if crash
            end
            },
     {"mochijson test",
      fun() ->
              DummyPid = self(),
              FormattedPid = format_pid(DummyPid),

              Stats =  [{realtime_enabled,<<"bar">>},
                        {realtime_started,<<"bar">>},
                        {fullsync_enabled,<<"bar">>},
                        {fullsync_running,<<>>},
                        {cluster_name,<<"foo">>},
                        {cluster_leader,'dev1@127.0.0.1'},
                        {connected_clusters,[<<"bar">>]},
                        {server_bytes_sent,0},
                        {server_bytes_recv,0},
                        {server_connects,0},
                        {server_connect_errors,0},
                        {server_fullsyncs,0},
                        {client_bytes_sent,0},
                        {client_bytes_recv,0},
                        {client_connects,0},
                        {client_connect_errors,0},
                        {client_redirect,0},
                        {objects_dropped_no_clients,0},
                        {objects_dropped_no_leader,0},
                        {objects_sent,0},
                        {objects_forwarded,0},
                        {elections_elected,0},
                        {elections_leader_changed,0},
                        {client_rx_kbps,[]},
                        {client_tx_kbps,[]},
                        {server_rx_kbps,[]},
                        {server_tx_kbps,[]},
                        {rt_source_errors,0},
                        {rt_sink_errors,0},
                        {rt_dirty,1},
                        {leader,'dev1@127.0.0.1'},
                        {"leader_message_queue_len",0},
                        {"leader_total_heap_size",987},
                        {"leader_heap_size",987},
                        {"leader_stack_size",9},
                        {"leader_reductions",6867},
                        {"leader_garbage_collection",
                         [ {min_bin_vheap_size,46368},
                           {min_heap_size,233},
                           {fullsweep_after,0},
                           {minor_gcs,0}]},
                        {"local_leader_message_queue_len",0},
                        {"local_leader_heap_size",987},
                        {sinks,<<>>},
                        {sources,
                         [{source_stats,
                           [{pid,FormattedPid},
                            {message_queue_len,0},
                            {rt_source_connected_to,
                             [{sink,<<"bar">>},
                              {pid,FormattedPid},
                              {connected,false}]}]}]},
                        {realtime_queue_stats,
                         [{bytes,768},
                          {max_bytes,104857600},
                          {consumers,
                           [{"bar",
                             [{pending,0},
                              {unacked,0},
                              {drops,0},
                              {errs,0}]}]}]},
                        {fullsync_coordinator,
                         [{"bar",
                           [{cluster,<<"bar">>},
                            {queued,0},
                            {in_progress,0},
                            {starting,0},
                            {successful_exits,0},
                            {error_exits,0},
                            {busy_nodes,0},
                            {running_stats,<<>>},
                            {socket,<<>>},
                            {fullsync_suggested,<<"dev1@127.0.0.1">>},
                            {fullsync_suggested_during_fs,<<>>}]}]},
                        {fullsync_coordinator_srv,<<>>}],
              %% This shouldn't blow up
              mochijson2:encode({struct, Stats}) % fail if crash
end}
                  ].

-endif.
