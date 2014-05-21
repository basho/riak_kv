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

-module(tracer_gc_latency).
-compile(export_all).

start(LatencyMS) ->
    catch folsom_metrics:delete_metric(foo),
    folsom_metrics:new_histogram(foo, uniform, 50*1000*1000),
    dbg:tracer(process, {fun trace/2, new_stats(LatencyMS)}),
    {ok, _} = dbg:p(all, [timestamp, garbage_collection, running]),
    
    {ok, TPid} = dbg:get_tracer(),
    io:format("Tracer pid: ~p, use ~p:stop() to stop\n", [TPid, ?MODULE]),
    io:format("Current date & time: ~p ~p local time\n", [date(), time()]),
    {started, TPid}.

stop() ->
    dbg:stop_clear(),
    catch exit(element(2,dbg:get_tracer()), kill),
    timer:sleep(100),
    catch folsom_metrics:delete_metric(foo),
    stopped.

trace({trace_ts, Pid, gc_start, _Stats, TS}, {Dict, LMS}) ->
    {dict:store(Pid, TS, Dict), LMS};
trace({trace_ts, Pid, gc_end, _Stats, TS}, {Dict, LMS}=Acc) ->
    DKey = Pid,
    case dict:find(DKey, Dict) of
        {ok, GcStart} ->
            Elapsed = erlang:max(-1, (timer:now_diff(TS, GcStart) div 1000)),
            if Elapsed > LMS ->
                    io:format("~p: GC of ~p elapsed time ~p > threshold ~p\n",
                              [time(), Pid, Elapsed, LMS]),
                    io:format("    ~w,~w\n", [process_info(Pid, message_queue_len), _Stats]);
               true ->
                    ok
            end,
            {dict:erase(DKey, Dict), LMS};
        error ->
            Acc
    end;
trace({trace_ts, Pid, InOrOut, _MFA, TS}, {Dict, _LMS}=Acc) ->
    DKey = Pid,
    case dict:find(DKey, Dict) of
        {ok, GcStart} ->
            io:format("Hey, pid ~p scheduled ~p but started GC ~p msec ago\n",
                      [Pid, InOrOut, timer:now_diff(TS, GcStart)]);
        _ ->
            ok
    end,
    Acc;
trace(Unknown, DictStats) ->
    erlang:display(wha),
    io:format("Unknown! ~P\n\t~P", [Unknown, 20, DictStats,7]),
    DictStats.

new_stats(LatencyMS) ->
    {dict:new(), LatencyMS}.

print_stats(_DictStats) ->
    ok.

