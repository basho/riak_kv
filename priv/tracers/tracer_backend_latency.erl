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

-module(tracer_backend_latency).
-compile(export_all).

start() ->
    start(500).

start(LatencyMS) ->
    start(LatencyMS, [get_fsm, put_fsm,
                      bitcask, eleveldb, file, prim_file, riak_kv_fs2_backend]).

start(LatencyMS, Modules) ->
    %% catch folsom_metrics:delete_metric(foo),
    %% folsom_metrics:new_histogram(foo, uniform, 9981239823),

    dbg:tracer(process, {fun trace/2, new_stats(LatencyMS)}),
    dbg:p(all, [call, timestamp, arity]),

    [catch dbg:tpl(Mod, Func, Arity, [{'_', [], []}]) ||
        lists:member(put_fsm, Modules),
        Mod <- [riak_kv_put_fsm],
        {Func, Arity} <- [{init, 1}, {finish, 2}]],
    [catch dbg:tpl(Mod, Func, Arity, [{'_', [], []}]) ||
        lists:member(get_fsm, Modules),
        Mod <- [riak_kv_get_fsm],
        {Func, Arity} <- [{init, 1}, {finalize, 1}]],

    [catch dbg:tpl(Mod, Func, Arity, [{'_', [], [{return_trace}]}]) ||
        lists:member(bitcask, Modules),
        Mod <- [bitcask],
        {Func, Arity} <- [
                          {open,1}, {open,2},
                           {close,1},
                           {close_write_file,1},
                           {get,2},
                           {put,3},
                           {delete,2},
                           {sync,1},
                           {iterator,3}, {iterator_next,1}, {iterator_release,1},
                           {needs_merge,1},
                           {is_empty_estimate,1},
                           {status,1}]],

   [catch dbg:tpl(Mod, Func, Arity, [{'_', [], [{return_trace}]}]) ||
       lists:member(eleveldb, Modules),
       Mod <- [eleveldb],
       {Func, Arity} <- [
                         {open,2},
                         {close,1},
                         {get,3},
                         {put,4},
                         {delete,3},
                         {write,3},
                         {status,2},
                         {destroy,2},
                         {is_empty,1},
                         {iterator,2},
                         {iterator,3},
                         {iterator_move,2},
                         {iterator_close,1}]],

    [catch dbg:tpl(Mod, Func, Arity, [{'_', [], [{return_trace}]}]) ||
        lists:member(file, Modules),
        Mod <- [file],
        {Func, Arity} <- [
                          {open,2},
                          {close,1},
                          {pread,2},
                          {pread,3},
                          {read,2},
                          {write,2},
                          {pwrite,2},
                          {pwrite,3},
                          {truncate,1},
                          {delete,1},
                          {position,2},
                          {sync,1}
                         ]],

    [catch dbg:tpl(Mod, Func, Arity, [{'_', [], [{return_trace}]}]) ||
        lists:member(prim_file, Modules),
        Mod <- [prim_file],
        {Func, Arity} <- [
                          {list_dir,2},
                          {read_file_info,1},
                          {write_file_info,1}
                         ]],

    [catch dbg:tpl(Mod, Func, Arity, [{'_', [], [{return_trace}]}]) ||
        lists:member(riak_kv_fs2_backend, Modules),
        Mod <- [riak_kv_fs2_backend],
        {Func, Arity} <- [
                          {get_object,4},
                          {put_object,5},
                          {delete,4}]],
    
    %% Don't need return_trace events for this use case, but here's
    %% how to do it if needed.
    %%dbg:tpl(bitcask, merge_single_entry, 6, [{'_', [], [{return_trace}]}]).

    {ok, TPid} = dbg:get_tracer(),
    io:format("Tracer pid: ~p, use ~p:stop() to stop\n", [TPid, ?MODULE]),
    %% timer:send_interval(Interval, TPid, print_report),
    io:format("Not using timer:send_interval...\n"),
    {started, TPid}.

stop() ->
    %% io:format("Histogram stats:\n~p\n", [catch folsom_metrics:get_histogram_statistics(foo)]),
    %% catch folsom_metrics:delete_metric(foo),

    dbg:stop_clear(),
    catch exit(element(2,dbg:get_tracer()), kill),
    stopped.

trace({trace_ts, Pid, call, {riak_kv_put_fsm, init, _}, TS}, {Dict, LMS}) ->
    {dict:store({put, Pid}, TS, Dict), LMS};
trace({trace_ts, Pid, call, {riak_kv_put_fsm, finish, _}, TS}, {Dict, LatencyMS}) ->
    Start = case dict:find({put, Pid}, Dict) of
                {ok, StTime} -> StTime;
                error        -> now()
            end,
    case timer:now_diff(TS, Start) div 1000 of
        Elapsed when Elapsed > LatencyMS ->
            io:format("~p ~p: put_fsm: ~p msec @ ~p ~p\n", [date(), time(), Elapsed, node(), Pid]);
        _Elapsed ->
            ok
    end,
    {dict:erase(Pid, Dict), LatencyMS};
trace({trace_ts, Pid, call, {riak_kv_get_fsm, init, _}, TS}, {Dict, LMS}) ->
    {dict:store({get, Pid}, TS, Dict), LMS};
trace({trace_ts, Pid, call, {riak_kv_get_fsm, finalize, _}, TS}, {Dict, LatencyMS}) ->
    Start = case dict:find({get, Pid}, Dict) of
                {ok, StTime} -> StTime;
                error        -> now()
            end,
    case timer:now_diff(TS, Start) div 1000 of
        Elapsed when Elapsed > LatencyMS ->
            io:format("~p ~p: get_fsm: ~p msec @ ~p ~p\n", [date(), time(), Elapsed, node(), Pid]);
        _Elapsed ->
            ok
    end,
    {dict:erase(Pid, Dict), LatencyMS};
trace({trace_ts, Pid, call, {Mod, _, _}, TS}, {Dict, LMS}) ->
    {dict:store({Mod, Pid}, TS, Dict), LMS};
trace({trace_ts, Pid, return_from, {Mod, Func, _}, _Res, TS}, {Dict, LatencyMS}) ->
    DKey = {Mod, Pid},
    Start = case dict:find(DKey, Dict) of
                {ok, StTime} -> StTime;
                error        -> now()
            end,
    case timer:now_diff(TS, Start) div 1000 of
        Elapsed when Elapsed > LatencyMS ->
            io:format("~p ~p: ~p ~p: ~p msec\n", [date(), time(), Mod,
                                                  Func, Elapsed]);
        _Elapsed ->
            ok
    end,
    %% if Mod == file, Func == pread ->
    %%         folsom_metrics_histogram:update(foo, Elapsed);
    %%    true ->
    %%         ok
    %% end,
    {dict:erase(DKey, Dict), LatencyMS};
trace(print_report, DictStats) ->
    %% print_stats(DictStats),
    %% new_stats();
    DictStats;
trace(Unknown, DictStats) ->
    erlang:display(wha),
    io:format("Unknown! ~P\n", [Unknown, 20]),
    DictStats.

new_stats(LatencyMS) ->
    {dict:new(), LatencyMS}.

print_stats(_DictStats) ->
    ok.

