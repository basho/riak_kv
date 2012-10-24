%% -------------------------------------------------------------------
%%
%% riak_kv_fsm_util: Somewhere to put that code you cut and paste between
%% get / put fsms
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc code that would otherwise be duplicated in both fsms

-module(riak_kv_fsm_util).

-compile(export_all).

%% add timing information of {State, erlang:now()} to the Timings
add_timing(State, Timings) when is_list(Timings) ->
    [{State, os:timestamp()}|Timings].

%% Calc timing information - stored as {Stage, StageStart} in reverse order.
%% ResponseUsecs is calculated as time from reply to start.
calc_timing([{Stage, Now} | Timing]) ->
    ReplyNow = case Stage of
                   reply ->
                       Now;
                   _ ->
                       undefined
               end,
    calc_timing(Timing, Now, ReplyNow, []).

%% Each timing stage has a start time.
calc_timing([], StageEnd, ReplyNow, Stages) ->
    %% StageEnd is prepare time
    {timer:now_diff(ReplyNow, StageEnd), Stages};
calc_timing([{reply, ReplyNow}|_]=Timing, StageEnd, undefined, Stages) ->
    %% Populate ReplyNow then handle normally.
    calc_timing(Timing, StageEnd, ReplyNow, Stages);
calc_timing([{Stage, StageStart} | Rest], StageEnd, ReplyNow, Stages) ->
    calc_timing(Rest, StageStart, ReplyNow,
                [{Stage, timer:now_diff(StageEnd, StageStart)} | Stages]).
