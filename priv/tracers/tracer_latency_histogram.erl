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

%% For example: create a histogram of call latencies for bitcask:put/3.
%% Watch for 10 seconds.
%%
%% > latency_histogram_tracer:start(bitcask, put, 3, 10).
%% 
%% Tracer pid: <0.2108.18>, use latency_histogram_tracer:stop() to stop
%% Otherwise, tracing stops in 10 seconds
%% Current date & time: {2013,9,19} {18,14,13}
%% {started,<0.2108.18>}
%% Histogram stats:     
%% [{min,0},
%%  {max,48},
%%  {arithmetic_mean,2.765411819271055},
%%  {geometric_mean,2.527103493663478},
%%  {harmonic_mean,2.2674039086593973},
%%  {median,3},
%%  {variance,3.5629207473971585},
%%  {standard_deviation,1.8875700642352746},
%%  {skewness,2.0360354571500774},
%%  {kurtosis,18.529695846728423},
%%  {percentile,[{50,3},{75,4},{90,5},{95,6},{99,8},{999,14}]},
%%  {histogram,[{1,13436},
%%              {2,12304},
%%              {3,10789},
%%              {4,7397},
%%              {5,4191},
%%              {6,1929},
%%              {7,873},
%%              {8,420},
%%              {9,163},
%%              {10,79},
%%              {11,42},
%%              {12,47},
%%              {13,11},
%%              {14,16},
%%              {15,7},
%%              {16,5},
%%              {17,3},
%%              {18,4},
%%              {19,2},
%%              {20,4},
%%              {21,1},
%%              {22,11},
%%              {23,2},
%%              {24,1},
%%              {25,2},
%%              {26,1},
%%              {27,0},
%%              {28,1},
%%              {29,2},
%%              {30,0},
%%              {31,0},
%%              {40,2},
%%              {50,1}]},
%%  {n,51746}]

-module(tracer_latency_histogram).
-compile(export_all).

start(Mod, Func, Arity, RunSeconds) ->
    catch folsom_metrics:delete_metric(foo),
    folsom_metrics:new_histogram(foo, uniform, 50*1000*1000),
    dbg:tracer(process, {fun trace/2, new_stats(0)}),
    dbg:p(all, [call, timestamp, arity]),
    dbg:tpl(Mod, Func, Arity, [{'_', [], [{return_trace}]}]),
    
    {ok, TPid} = dbg:get_tracer(),
    io:format("Tracer pid: ~p, use ~p:stop() to stop\n", [TPid, ?MODULE]),
    io:format("Otherwise, tracing stops in ~p seconds\n", [RunSeconds]),
    io:format("Current date & time: ~p ~p\n", [date(), time()]),
    spawn(fun() -> timer:sleep(RunSeconds * 1000), stop() end),
    {started, TPid}.

stop() ->
    io:format("Histogram stats:\n~p\n", [catch folsom_metrics:get_histogram_statistics(foo)]),
    dbg:stop_clear(),
    catch exit(element(2,dbg:get_tracer()), kill),
    timer:sleep(100),
    catch folsom_metrics:delete_metric(foo),
    stopped.

trace({trace_ts, Pid, call, {_, _, _}, TS}, {Dict, LMS}) ->
    {dict:store(Pid, TS, Dict), LMS};
trace({trace_ts, Pid, return_from, {_, _, _}, _Res, TS}, {Dict, LatencyMS}) ->
    DKey = Pid,
    Start = case dict:find(DKey, Dict) of
                {ok, StTime} -> StTime;
                error        -> now()
            end,
    Elapsed = timer:now_diff(TS, Start) div 1000,
    folsom_metrics_histogram:update(foo, Elapsed),
    {dict:erase(DKey, Dict), LatencyMS};
trace(print_report, DictStats) ->
    DictStats;
trace(Unknown, DictStats) ->
    erlang:display(wha),
    io:format("Unknown! ~P\n", [Unknown, 20]),
    DictStats.

new_stats(LatencyMS) ->
    {dict:new(), LatencyMS}.

print_stats(_DictStats) ->
    ok.

