%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 Basho Technologies, Inc.  All Rights Reserved.
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

-module(tracer_fsm_init).
-compile(export_all).

start() ->
    start(1*1000).

start(Interval) ->
    %%% Count the get, put, buckets, keys, exchange, and index FSM init() calls
    dbg:tracer(process, {fun trace/2, new_stats()}),
    dbg:p(all, call),
    [dbg:tpl(Mod, init, 1, [{'_', [], []}]) ||
         Mod <- [riak_kv_buckets_fsm, riak_kv_exchange_fsm, riak_kv_get_fsm, riak_kv_index_fsm, riak_kv_keys_fsm, riak_kv_put_fsm]],
    dbg:tpl(riak_kv_put_fsm, start_link, 3, [{'_', [], []}]),
    
    %% Don't need return_trace events for this use case, but here's
    %% how to do it if needed.
    %%dbg:tpl(bitcask, merge_single_entry, 6, [{'_', [], [{return_trace}]}]).

    {ok, TPid} = dbg:get_tracer(),
    io:format("Tracer pid: ~p, use ~p:stop() to stop\n", [TPid, ?MODULE]),
    timer:send_interval(Interval, TPid, print_report),
    {started, TPid}.

stop() ->
    dbg:stop_clear(),
    catch exit(element(2,dbg:get_tracer()), kill),
    stopped.

trace({trace, _Pid, call, {riak_kv_put_fsm, start_link, _}},
      {Pstart_link, R, P, B, E, I, K}) ->
    {Pstart_link+1, R, P, B, E, I, K};
trace({trace, _Pid, call, {riak_kv_get_fsm, init, _}},
      {Pstart_link, R, P, B, E, I, K}) ->
    {Pstart_link, R+1, P, B, E, I, K};
trace({trace, _Pid, call, {riak_kv_put_fsm, init, _}},
      {Pstart_link, R, P, B, E, I, K}) ->
    {Pstart_link, R, P+1, B, E, I, K};
trace({trace, _Pid, call, {riak_kv_buckets_fsm, init, _}},
      {Pstart_link, R, P, B, E, I, K}) ->
    {Pstart_link, R, P, B+1, E, I, K};
trace({trace, _Pid, call, {riak_kv_exchange_fsm, init, _}},
      {Pstart_link, R, P, B, E, I, K}) ->
    {Pstart_link, R, P, B, E+1, I, K};
trace({trace, _Pid, call, {riak_kv_index_fsm, init, _}},
      {Pstart_link, R, P, B, E, I, K}) ->
    {Pstart_link, R, P, B, E, I+1, K};
trace({trace, _Pid, call, {riak_kv_keys_fsm, init, _}},
      {Pstart_link, R, P, B, E, I, K}) ->
    {Pstart_link, R, P, B, E, I, K+1};
trace(print_report, Stats) ->
    print_stats(Stats),
    new_stats();
trace(Unknown, Stats) ->
    erlang:display(wha),
    io:format("Unknown! ~P\n", [Unknown, 20]),
    Stats.

new_stats() ->
    {0,
     0, 0, 0, 0, 0, 0}.

print_stats({Pstart_link, Get, Put, Buckets, Exchange, Index, Keys}) ->
    Stats = [{put_start, Pstart_link},
             {get, Get},
             {put, Put},
             {buckets, Buckets},
             {exchange, Exchange},
             {index, Index},
             {keys, Keys}],
    io:format("~p ~p: ~p\n", [date(), time(), Stats]).
