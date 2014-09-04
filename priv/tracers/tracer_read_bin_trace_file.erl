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

-module(tracer_read_bin_trace_file).
-compile(export_all).

read(Path) ->
    read(Path, 1).

read(Path, LatencyMS) ->
    {ok, FH} = file:open(Path, [read, binary, raw]),
    read(file:read(FH, 5), FH, LatencyMS, []).

read(eof, _FH, _, _) ->
    ok;
read({ok, <<Size:40>>}, FH, LatencyMS, Hist) ->
    {ok, Bin} = file:read(FH, Size),
    case binary_to_term(Bin) of
        {trace_ts, _, call, {M,F,A}, Time} ->
            %%io:format("call MFA = ~p:~p/~p, ", [M, F, length(A)]),
            read(file:read(FH, 5), FH, LatencyMS, [{{M,F,length(A)}, Time, A}|Hist]);
        {trace_ts, _, return_from, MFA, Res, EndTime} ->
            %%io:format("MFA ~p Hist ~p\n", [MFA, Hist]),
            try
                {value, {_, StartTime, A}, NewHist} = lists:keytake(MFA, 1, Hist),
                MSec = timer:now_diff(EndTime, StartTime)/1000,
                if MSec > LatencyMS ->
                        io:format("~p ~p msec\nArgs: (~p/~p) ~P\nRes: ~P\n\n",
                                  [MFA, MSec,
                                   erts_debug:flat_size(A), erts_debug:size(A),
                                   A, 20, Res, 20]);
                   true ->
                        ok
                end,
                read(file:read(FH, 5), FH, LatencyMS, NewHist)
            catch
                error:{badmatch,false} ->
                    read(file:read(FH, 5), FH, LatencyMS, Hist);
                X:Y ->
                    io:format("ERR ~p ~p @ ~p\n", [X, Y, erlang:get_stacktrace()]),
                    read(file:read(FH, 5), FH, LatencyMS, Hist)
            end
    end.

%% read(eof, _FH) ->
%%     ok;
%% read({ok, <<Size:40>>}, FH) ->
%%     {ok, Bin} = file:read(FH, Size),
%%     io:format("~P\n", [binary_to_term(Bin), 15]),
%%     read(file:read(FH, 5), FH).

