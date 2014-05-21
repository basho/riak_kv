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

-module(tracer_eleveldb_put_size).
-compile(export_all).

start() ->
    start(10*1000).

start(Interval) ->
    Stats = {StatName, _} = new_stats(),
    reset_metric(StatName),

    dbg:tracer(process, {fun trace/2, Stats}),
    dbg:p(all, [call]),
    dbg:tpl(eleveldb, write, 3, [{'_', [], []}]),
    
    {ok, TPid} = dbg:get_tracer(),
    io:format("Tracer pid: ~p, use ~p:stop() to stop\n", [TPid, ?MODULE]),
    timer:send_interval(Interval, TPid, print_report),
    {started, TPid}.

stop() ->
    dbg:stop_clear(),
    catch exit(element(2,dbg:get_tracer()), kill),
    stopped.

trace({trace, _Pid, call, {eleveldb, write, [_, PutList, _]}},
      {StatName, SumBytes}) ->
    Bs = [begin
              Bs = size(K) + size(V),
              folsom_metrics_histogram:update(StatName, Bs),
              Bs
          end || {put, K, V} <- PutList],
    {StatName, SumBytes + lists:sum(Bs)};
trace(print_report, Stats = {StatName, _}) ->
    print_stats(Stats),
    reset_metric(StatName),
    new_stats();
trace(_Unknown, Stats) ->
    erlang:display(wha),
    %% io:format("Unknown! ~P\n", [Unknown, 20]),
    Stats.

new_stats() ->
    {foo, 0}.

print_stats({StatName, SumBytes}) ->
    if SumBytes == 0 ->
            io:format("~p ~p: 0 bytes\n", [date(), time()]);
       true ->
            Ps = folsom_metrics:get_histogram_statistics(StatName),
            io:format("~p ~p: ~p bytes\n  ~p\n", [date(), time(), SumBytes, Ps])
    end.

reset_metric(Stats) ->
    catch folsom_metrics:delete_metric(Stats),
    folsom_metrics:new_histogram(Stats, uniform, 9981239823).
