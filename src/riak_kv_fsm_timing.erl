%% -------------------------------------------------------------------
%%
%% riak_kv_fsm_timing: Common code for timing fsm states
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
%% functions for gathering and calculating timing information
%% for fsm states.

-module(riak_kv_fsm_timing).

-export([add_timing/2, calc_timing/1]).

-type timing() :: {StageName::atom(), StageStartTime::erlang:timestamp()}.
-type timings() :: [timing()].
-type duration() :: {StageName::atom(), StageDuration::non_neg_integer()}.
-type durations() :: {ResponseUSecs::non_neg_integer(), [duration()]}.

%% @doc add timing information of `{State, erlang:now()}' to the Timings

-spec add_timing(atom(), timings()) -> timings().
add_timing(State, Timings) when is_list(Timings) ->
    [{State, os:timestamp()}|Timings].

%% ---------------------------------------------------------------------

%% @doc Calc timing information - stored as `{Stage, StageStart}'
%% in reverse order.
%%
%% ResponseUsecs is calculated as time from reply to start of first stage.
%% If `reply' is in `stages' more than once, the earliest value is used.
%% If `reply' is not in `stages' fails with `badarg'
%% Since a stage's duration is the difference between it's start time
%% and the next stages start time, we don't calculate the duration of
%% the final stage, it is just there as the end time of the
%% penultimate stage

-spec calc_timing(timings()) ->
                               durations().
calc_timing(Stages0) ->
    case proplists:get_value(reply, Stages0) of
        undefined ->
            erlang:error(badarg);
        ReplyTime ->
            [{_FinalStage, StageEnd}|Stages] = Stages0,
            calc_timing(Stages, StageEnd, ReplyTime, orddict:new())
    end.

%% A stages duration is the difference between it's start time
%% and the next stages start time.
-spec calc_timing(timings(), erlang:timestamp(),
                        erlang:timestamp(),
                        orddict:orddict()) ->
                               durations().
calc_timing([], FirstStageStart, ReplyTime, Acc) ->
    %% Time from first stage start until reply sent
    {timer:now_diff(ReplyTime, FirstStageStart), orddict:to_list(Acc)};
calc_timing([{Stage, StageStart} | Rest], StageEnd, ReplyTime, Acc0) ->
    StageDuration = timer:now_diff(StageEnd, StageStart),
    %% When the same stage appears more than once in
    %% a list of timings() aggregate the times into
    %% a total for that stage
    Acc = orddict:update_counter(Stage, StageDuration, Acc0),
    calc_timing(Rest, StageStart, ReplyTime, Acc).
