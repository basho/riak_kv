%% -------------------------------------------------------------------
%%
%% riak_kv_ttaefs_manager: coordination of full-sync replication
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

%% @doc coordination of full-sync replication

-module(riak_kv_ttaaefs_manager).
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

% -behaviour(gen_server).

% -export([init/1,
        % handle_call/3,
        % handle_cast/2,
        % handle_info/2,
        % terminate/2,
        % code_change/3]).

-export([take_next_workitem/4]).

-define(SLICE_COUNT, 100).
-define(SLICE_SECONDS, 864). % e.g. 86400/100.

% -record(state, {slice_allocations = [] :: list(allocation()),
%                 slice_set_start = os:timestmap() :: erlang:timestamp(),
%                 schedule :: schedule_wants(),
%                 peer_ip,
%                 peer_port,
%                 peer_protocol :: http|pb
%                 }).

% -type fullsync_state() :: #state{}.

-type work_item() :: no_sync|all_sync|day_sync|hour_sync.
-type schedule_want() :: {work_item(), non_neg_integer()}.
-type slice() :: pos_integer().
-type allocation() :: {slice(), work_item()}.
-type schedule_wants() :: [schedule_want()].
-type node_info() :: {pos_integer(), pos_integer()}. % Node and node count

%%%============================================================================
%%% API
%%%============================================================================




%%%============================================================================
%%% gen_server callbacks
%%%============================================================================




%%%============================================================================
%%% Internal functions
%%%============================================================================


-spec take_next_workitem(list(allocation()), schedule_wants(),
                            erlang:timestamp(), node_info()) ->
                                {work_item(), pos_integer(),
                                    list(allocation()), erlang:timestamp()}.
%% @doc
%% Take the next work item from the list of allocations, assuming that the
%% starting time for that work item has not alreasy passed.  If there are no
%% more items queue, start a new queue based on the wants for the schedule.
take_next_workitem([], Wants, ScheduleStartTime, SlotInfo) ->
    NewAllocations = choose_schedule(Wants),
    % Should be 24 hours after ScheduleStartTime - so add 24 hours to
    % ScheduleStartTime
    {Mega, Sec, _Micro} = ScheduleStartTime,
    Seconds = Mega * 1000 + Sec + 86400,
    RevisedStartTime = {Seconds div 1000, Seconds rem 1000, 0},
    take_next_workitem(NewAllocations, Wants, RevisedStartTime, SlotInfo);
take_next_workitem([NextAlloc|T], Wants, ScheduleStartTime, SlotInfo) ->
    {NodeNumber, NodeCount} = SlotInfo,
    SlotSeconds =
        NodeNumber * (?SLICE_SECONDS div NodeCount) 
        + random:uniform(?SLICE_SECONDS),
    {SliceNumber, NextAction} = NextAlloc,
    {Mega, Sec, _Micro} = ScheduleStartTime,
    ScheduleSeconds = 
        Mega * 1000 + Sec + SlotSeconds + SliceNumber * ?SLICE_SECONDS,
    {MegaNow, SecNow, _MicroNow} = os:timestamp(),
    NowSeconds = MegaNow * 1000 + SecNow,
    case ScheduleSeconds > NowSeconds of
        true ->
            {NextAction, ScheduleSeconds - NowSeconds, T, ScheduleStartTime};
        false ->
            lager:info("Tictac AAE skipping action ~w as manager running"
                        ++ "~w seconds late",
                        [NextAction, NowSeconds - ScheduleSeconds]),
            take_next_workitem(T, Wants, ScheduleStartTime, SlotInfo)
    end.


-spec choose_schedule(schedule_wants()) -> list(allocation()).
%% @doc
%% Calculate an allocation of activity for the next 24 hours based on the
%% configured scheudle needs.
choose_schedule(ScheduleWants) ->
    [{no_sync, NoSync}, {all_sync, AllSync},
        {day_sync, DaySync}, {hour_sync, HourSync}] = ScheduleWants,
    Slices = lists:seq(1, ?SLICE_COUNT),
    Allocations = [],
    ?SLICE_COUNT = NoSync + AllSync + DaySync + HourSync, 
    lists:sort(choose_schedule(Slices,
                                Allocations,
                                {NoSync, AllSync, DaySync, HourSync})).

choose_schedule([], Allocations, {0, 0, 0, 0}) ->
    lists:ukeysort(1, Allocations);
choose_schedule(Slices, Allocations, {NoSync, 0, 0, 0}) ->
    {HL, [Allocation|TL]} =
        lists:split(random:uniform(length(Slices)) - 1, Slices),
    choose_schedule(HL ++ TL,
                    [{Allocation, no_sync}|Allocations],
                    {NoSync - 1, 0, 0, 0});
choose_schedule(Slices, Allocations, {NoSync, AllSync, 0, 0}) ->
    {HL, [Allocation|TL]} =
        lists:split(random:uniform(length(Slices)) - 1, Slices),
    choose_schedule(HL ++ TL,
                    [{Allocation, all_sync}|Allocations],
                    {NoSync, AllSync - 1, 0, 0});
choose_schedule(Slices, Allocations, {NoSync, AllSync, DaySync, 0}) ->
    {HL, [Allocation|TL]} =
        lists:split(random:uniform(length(Slices)) - 1, Slices),
    choose_schedule(HL ++ TL,
                    [{Allocation, day_sync}|Allocations],
                    {NoSync, AllSync, DaySync - 1, 0});
choose_schedule(Slices, Allocations, {NoSync, AllSync, DaySync, HourSync}) ->
    {HL, [Allocation|TL]} =
        lists:split(random:uniform(length(Slices)) - 1, Slices),
    choose_schedule(HL ++ TL,
                    [{Allocation, hour_sync}|Allocations],
                    {NoSync, AllSync, DaySync, HourSync - 1}).


%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).

choose_schedule_test() ->
    NoSyncAllSchedule =
        [{no_sync, 100}, {all_sync, 0}, {day_sync, 0}, {hour_sync, 0}],
    NoSyncAll = choose_schedule(NoSyncAllSchedule),
    ExpNoSyncAll = lists:map(fun(I) -> {I, no_sync} end, lists:seq(1, 100)),
    ?assertMatch(NoSyncAll, ExpNoSyncAll),

    AllSyncAllSchedule =
        [{no_sync, 0}, {all_sync, 100}, {day_sync, 0}, {hour_sync, 0}],
    AllSyncAll = choose_schedule(AllSyncAllSchedule),
    ExpAllSyncAll = lists:map(fun(I) -> {I, all_sync} end, lists:seq(1, 100)),
    ?assertMatch(AllSyncAll, ExpAllSyncAll),
    
    MixedSyncSchedule = 
        [{no_sync, 0}, {all_sync, 1}, {day_sync, 4}, {hour_sync, 95}],
    MixedSync = choose_schedule(MixedSyncSchedule),
    ?assertMatch(100, length(MixedSync)),
    IsSyncFun = fun({_I, Type}) -> Type == hour_sync end,
    SliceForHourFun = fun({I, hour_sync}) -> I end,
    HourWorkload =
        lists:map(SliceForHourFun, lists:filter(IsSyncFun, MixedSync)),
    ?assertMatch(95, length(lists:usort(HourWorkload))),
    FoldFun = 
        fun(I, Acc) ->
            true = I > Acc,
            I
        end,
    BiggestI = lists:foldl(FoldFun, 0, HourWorkload),
    ?assertMatch(true, BiggestI >= 95).

take_first_workitem_test() ->
    Wants = [{no_sync, 100}, {all_sync, 0}, {day_sync, 0}, {hour_sync, 0}],
    {Mega, Sec, Micro} = os:timestamp(),
    TwentyFourHoursAgo = Mega * 1000 + Sec - (60 * 60 * 24),
    {NextAction, PromptSeconds, _T, ScheduleStartTime} = 
        take_next_workitem([], Wants,
                            {TwentyFourHoursAgo div 1000,
                                TwentyFourHoursAgo rem 1000, Micro},
                            {1, 8}),
    ?assertMatch(no_sync, NextAction),
    ?assertMatch(true, ScheduleStartTime > {Mega, Sec, Micro}),
    ?assertMatch(true, PromptSeconds > 0),
    {NextAction, PromptMoreSeconds, _T, ScheduleStartTime} = 
        take_next_workitem([], Wants,
                            {TwentyFourHoursAgo div 1000,
                                TwentyFourHoursAgo rem 1000, Micro},
                            {2, 8}),
    ?assertMatch(true, PromptMoreSeconds > PromptSeconds),
    {NextAction, PromptEvenMoreSeconds, T, ScheduleStartTime} = 
        take_next_workitem([], Wants,
                            {TwentyFourHoursAgo div 1000,
                                TwentyFourHoursAgo rem 1000, Micro},
                            {7, 8}),
    ?assertMatch(true, PromptEvenMoreSeconds > PromptMoreSeconds),
    {NextAction, PromptYetMoreSeconds, _T0, ScheduleStartTime} = 
        take_next_workitem(T, Wants, ScheduleStartTime, {1, 8}),
    ?assertMatch(true, PromptYetMoreSeconds > PromptEvenMoreSeconds).



-endif.
