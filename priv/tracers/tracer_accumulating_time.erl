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

-module(tracer_accumulating_time).
-compile(export_all).

start(Pid_list, MFA_list, IntervalMS) ->
    dbg:tracer(process, {fun trace/2, new_stats()}),
    [dbg:p(Pid, [call, timestamp, arity]) || Pid <- Pid_list],
    [catch dbg:tpl(Mod, Func, Arity, [{'_', [], [{return_trace}]}]) ||
        {Mod, Func, Arity} <- MFA_list],
    
    {ok, TPid} = dbg:get_tracer(),
    io:format("Tracer pid: ~p, use ~p:stop() to stop\n", [TPid, ?MODULE]),
    timer:send_interval(IntervalMS, TPid, print_report),
    {started, TPid}.

stop() ->
    dbg:stop_clear(),
    catch exit(element(2,dbg:get_tracer()), kill),
    stopped.

trace({trace_ts, Pid, call, {Mod, Func, Arity}, TS}, {Dict}) ->
    MFA = {Mod, Func, Arity},
    DKey = {Pid, MFA},
    {dict:store(DKey, TS, Dict)};
trace({trace_ts, Pid, return_from, {Mod, Func, Arity}, _Res, TS}, {Dict}) ->
    MFA = {Mod, Func, Arity},
    DKey = {Pid, MFA},
    Start = case dict:find(DKey, Dict) of
                {ok, StTime} -> StTime;
                error        -> now()
            end,
    Elapsed = timer:now_diff(TS, Start),
    SumKey = {sum, MFA},
    {OldCount, OldTime} = case dict:find(SumKey, Dict) of
                              error ->
                                  {0, 0};
                              {ok, Else} ->
                                  Else
                          end,
    Dict2 = dict:erase(DKey, Dict),
    {dict:store(SumKey, {OldCount+1, OldTime+Elapsed}, Dict2)};
trace(print_report, {Dict}) ->
    print_stats(Dict),
    {dict:from_list([X || {K, _V} = X <- dict:to_list(Dict),
                          element(1, K) /= sum])};
trace(Unknown, {Dict}) ->
    erlang:display(wha),
    io:format("Unknown! ~P\n", [Unknown, 20]),
    {Dict}.

new_stats() ->
    {dict:new()}.

print_stats(Dict) ->
    Reports = lists:sort([{MFA, X} || {{sum, MFA}, X} <- dict:to_list(Dict)]),
    [io:format("~p MFA ~p count ~p elapsed_msec ~p\n",
               [time(), MFA, Count, Sum div 1000]) ||
        {MFA, {Count, Sum}} <- Reports].
