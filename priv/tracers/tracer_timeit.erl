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

-module(tracer_timeit).
-compile(export_all).

%% @doc Dynamically add timing to MFA.  There are various types of
%% timing.
%%
%% all - time latency of all calls to MFA
%%
%% {sample, N, Max} - sample every N calls and stop sampling after Max
%%
%% {threshold, Millis, Max} - count # of calls where latency is > Millis
%% and count # of calls total, thus percentage of calls over threshold
timeit(Mod, Fun, Arity, Type) ->
    Type2 = case Type of
                {sample, N, Max} -> {sample, {N, Max}, {0, 0, 0}};
                {threshold, Millis, Max} -> {threshold, {Millis, Max}, {0, 0}};
                {all, Max} -> {all, {0, Max}}
            end,
    dbg:tracer(process, {fun trace/2, {orddict:new(), Type2}}),
    dbg:p(all, call),
    dbg:tpl(Mod, Fun, Arity, [{'_', [], [{return_trace}]}]).

stop() -> dbg:stop_clear().

trace({trace, Pid, call, {Mod, Fun, _}}, {D, {all, {Count, Max}}}) ->
    D2 = orddict:store({Pid, Mod, Fun}, now(), D),
    {D2, {all, {Count, Max}}};
trace({trace, Pid, call, {Mod, Fun, _}},
      {D, {sample, {N, Max}, {M, K, Total}}}) ->
    M2 = M+1,
    Total2 = Total+1,
    if N == M2 ->
            D2 = orddict:store({Pid, Mod, Fun}, now(), D),
            {D2, {sample, {N, Max}, {0, K, Total2}}};
       true ->
            {D, {sample, {N, Max}, {M2, K, Total2}}}
    end;
trace({trace, Pid, call, {Mod, Fun, _}},
      {D, {threshold, {Millis, Max}, {Over, Total}}}) ->
    D2 = orddict:store({Pid, Mod, Fun}, now(), D),
    {D2, {threshold, {Millis, Max}, {Over, Total+1}}};

trace({trace, Pid, return_from, {Mod, Fun, _}, _Result},
      Acc={D, {all, {Count, Max}}}) ->
    Key = {Pid, Mod, Fun},
    case orddict:find(Key, D) of
        {ok, StartTime} ->
            Count2 = Count+1,
            ElapsedUs = timer:now_diff(now(), StartTime),
            ElapsedMs = ElapsedUs/1000,
            io:format(user, "~p:~p:~p: ~p ms\n", [Pid, Mod, Fun, ElapsedMs]),
            if Count2 == Max -> stop();
               true ->
                    D2 = orddict:erase(Key, D),
                    {D2, {all, {Count2, Max}}}
            end;
        error -> Acc
    end;
trace({trace, Pid, return_from, {Mod, Fun, _}, _Result},
      Acc={D, {sample, {N, Max}, {M, K, Total}}}) ->
    Key = {Pid, Mod, Fun},
    case orddict:find(Key, D) of
        {ok, StartTime} ->
            K2 = K+1,
            ElapsedUs = timer:now_diff(now(), StartTime),
            ElapsedMs = ElapsedUs/1000,
            io:format(user, "[sample ~p/~p] ~p:~p:~p: ~p ms\n",
                      [K2, Total, Pid, Mod, Fun, ElapsedMs]),
            if K2 == Max -> stop();
               true ->
                    D2 = orddict:erase(Key, D),
                    {D2, {sample, {N, Max}, {M, K2, Total}}}
            end;
        error -> Acc
    end;
trace({trace, Pid, return_from, {Mod, Fun, _}, _Result},
      Acc={D, {threshold, {Millis, Max}, {Over, Total}}}) ->
    Key = {Pid, Mod, Fun},
    case orddict:find(Key, D) of
        {ok, StartTime} ->
            ElapsedUs = timer:now_diff(now(), StartTime),
            ElapsedMs = ElapsedUs / 1000,
            if ElapsedMs > Millis ->
                    Over2 = Over+1,
                    io:format(user, "[over threshold ~p, ~p/~p] ~p:~p:~p: ~p ms\n",
                              [Millis, Over2, Total, Pid, Mod, Fun, ElapsedMs]);
               true ->
                    Over2 = Over
            end,
            if Max == Over -> stop();
               true ->
                    D2 = orddict:erase(Key, D),
                    {D2, {threshold, {Millis, Max}, {Over2, Total}}}
            end;
        error -> Acc
    end.
