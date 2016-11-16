%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016 Basho Technologies, Inc.  All Rights Reserved.
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
%% @doc This module encapsulates the command line interface for the
%% sweeper commands:
%%
%% status
%%
-module(riak_kv_sweeper_cli).

-behaviour(clique_handler).

-include("riak_kv_sweeper.hrl").

-export([
         format_sweeps/2,
         register_cli/0,
         status/3
        ]).

register_cli() ->
    register_all_usage(),
    register_all_commands().

register_all_usage() ->
    clique:register_usage(["riak-admin", "sweeper"], sweeper_usage()),
    clique:register_usage(["riak-admin", "sweeper", "status"], status_usage()).

register_all_commands() ->
    lists:foreach(fun(Args) -> apply(clique, register_command, Args) end,
                  [status_register()]).

status_register() ->
    [["riak-admin", "sweeper", "status"], % Cmd
     [],                                  % KeySpecs
     [],                                  % FlagSpecs
     fun status/3].                       % Implementation callback.

sweeper_usage() ->
    [
     "riak-admin sweeper <sub-command>\n\n",
     "  Sub-commands:\n",
     "    status           Display sweeper status\n",
     "  Use --help after a sub-command for more details.\n"
    ].

status_usage() ->
    ["riak-admin sweeper status\n\n",
     "  Display sweeper status\n"].

status(_CmdBase, [], []) ->
    {Participants, Sweeps} = riak_kv_sweeper:status(),
    SortedParticipants = lists:keysort(1, Participants),
    IndexedParticipants =
        lists:zip(lists:seq(1, length(Participants)), SortedParticipants),
    ParticipantsTable = format_participants(IndexedParticipants),
    SweepsTable = format_sweeps(Sweeps, IndexedParticipants),
    ActiveSweeps = format_active_sweeps(Sweeps, IndexedParticipants),
    ParticipantsTable ++ SweepsTable ++ ActiveSweeps.

format_sweeps(Sweeps, IndexedParticipants) ->
    Now = os:timestamp(),
    Header = io_lib:format("~s", ["Sweep results:"]),
    SortedSweeps = lists:keysort(#sweep.index, Sweeps),
    Rows =
        [begin
             StartTime = Sweep#sweep.start_time,
             EndTime = Sweep#sweep.end_time,
             LastSweep = format_timestamp(Now, StartTime),
             Duration = format_timestamp(EndTime, StartTime),
             ResultsColumns  =
                 format_results(Now, Sweep#sweep.results, IndexedParticipants),
             [{"Index", Sweep#sweep.index},
              {"Last sweep", LastSweep},
              {"Duration", Duration}
             ] ++ ResultsColumns
         end
         || Sweep <- SortedSweeps],
    [clique_status:text(Header),
     clique_status:table(Rows)].

format_active_sweeps(Sweeps, IndexedParticipants) ->
    Rows = [format_progress(Sweep, IndexedParticipants)
              || #sweep{state = State} = Sweep <- Sweeps, State == running],
    case Rows of
        [] ->
            [];
        Rows ->
            Header = io_lib:format("~s", ["Running sweeps:"]),
            [clique_status:text(Header),
             clique_status:table(Rows)]
    end.

format_progress(#sweep{index = Index,
                       active_participants = AcitvePart,
                       estimated_keys = {EstimatedKeys, _TS},
                       swept_keys = SweptKeys},
                IndexedParticipants) ->
    case EstimatedKeys of
        EstimatedKeys when is_integer(EstimatedKeys) andalso EstimatedKeys > 0 ->
            [{"Index", Index},
             {"Swept keys", SweptKeys},
             {"Estimated Keys", EstimatedKeys}] ++
                format_active_participants(AcitvePart, IndexedParticipants);
        _ ->
            [{"Index", Index},
             {"Swept keys", SweptKeys}] ++
                format_active_participants(AcitvePart, IndexedParticipants)
    end;

format_progress(_, _) ->
    "".

format_active_participants(AcitvePart, IndexedParticipants) ->
    [begin
         IndexString = integer_to_list(Index),
         {"Active " ++ IndexString,
          lists:keymember(Mod, #sweep_participant.module, AcitvePart)}
     end
     || {Index, #sweep_participant{module = Mod}} <- IndexedParticipants].

format_results(Now, Results, IndexedParticipants) ->
    lists:flatten([get_result(Index, Participant, Results, Now)
                     || {Index, Participant} <- IndexedParticipants]).

get_result(Index, Participant, Results, Now) ->
    case dict:find(Participant#sweep_participant.module, Results) of
        {ok, {TimeStamp, Outcome}} when Outcome == succ orelse Outcome == fail ->
            LastRun = format_timestamp(Now, TimeStamp),
            OutcomeString = string:to_upper(atom_to_list(Outcome));
        _ ->
            LastRun = "-",
            OutcomeString = "-"
    end,
    IndexString = integer_to_list(Index),
    [{"Last run " ++ IndexString, LastRun},
     {"Result "   ++ IndexString, OutcomeString}].

format_participants(IndexedParticipants) ->
    Header = io_lib:format("~s", ["Sweep participants:"]),
    Rows = [format_sweep_participant(Index, Participant)
              || {Index, Participant} <- IndexedParticipants],
    [clique_status:text(Header),
     clique_status:table(Rows)].

format_sweep_participant(Index, #sweep_participant{module = Mod,
                                                   description = Description,
                                                   run_interval = Interval}) ->
    IntervalValue = riak_kv_sweeper:get_run_interval(Interval),
    IntervalString = format_interval(IntervalValue * 1000000),
    [{"ID", Index},
     {"Module" ,atom_to_list(Mod)},
     {"Description", Description},
     {"Interval",  IntervalString}].

format_interval(Interval) ->
    riak_core_format:human_time_fmt("~.1f", Interval).

format_timestamp(_Now, undefined) ->
    "--";
format_timestamp(undefined, _) ->
    "--";
format_timestamp(Now, TS) ->
    riak_core_format:human_time_fmt("~.1f", timer:now_diff(Now, TS)).
