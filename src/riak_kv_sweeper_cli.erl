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
         format_sweeps/1,
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
    ParticipantsTable = format_participants(Participants),
    SweepsTable = format_sweeps(Sweeps),
    ActiveSweeps = format_active_sweeps(Sweeps),
    [ParticipantsTable,
     SweepsTable]
    ++ ActiveSweeps.

format_sweeps(Sweeps) ->
    Now = os:timestamp(),
    SortedSweeps = lists:keysort(#sweep.index, Sweeps),
    Rows =
        [begin
         StartTime = Sweep#sweep.start_time,
         EndTime = Sweep#sweep.end_time,
         LastSweep = format_timestamp(Now, StartTime),
         Duration = format_timestamp(EndTime, StartTime),
         ResultsString = format_results(Now, Sweep#sweep.results),
         [{"Index", Sweep#sweep.index},
          {"Last sweep", LastSweep},
          {"Duration", Duration},
          {"Results", ResultsString}
          ]
     end
     || Sweep <- SortedSweeps],
    clique_status:table(Rows).

format_active_sweeps(Sweeps) ->
     Header = io_lib:format("~n~s~n", [string:centre(" Active sweeps ", 79, $=)]),
      Rows = [begin
             format_progress(Sweep)
         end
         || #sweep{state = State} = Sweep <- Sweeps, State == running],
     case Rows of
         [] ->
             [];
         Rows ->
             [clique_status:text(Header),
              clique_status:table(Rows)]
     end.

format_progress(#sweep{index = Index,
                       active_participants = AcitvePart,
                       estimated_keys = {EstimatedKeys, _TS},
                       swept_keys = SweptKeys}) ->
    case EstimatedKeys of
        EstimatedKeys when is_integer(EstimatedKeys) andalso EstimatedKeys > 0 ->
            [{"Index", Index},
             {"Swept keys", SweptKeys},
             {"Estimated Keys", EstimatedKeys},
             {"Active", format_active_participants(AcitvePart)}];
        _ ->
            [{"Index", Index},
             {"Swept keys", SweptKeys},
             {"Active", format_active_participants(AcitvePart)}]
    end;

format_progress(_) ->
    "".

format_active_participants(AcitvePart) ->
    [io_lib:format("| ~s", [atom_to_list(Mod)])
     || #sweep_participant{module = Mod} <- AcitvePart].

format_results(Now, Results) ->
    ResultList = dict:to_list(Results),
    SortedResultList = lists:keysort(1, ResultList),
    [begin
        LastResult = format_timestamp(Now, TimeStamp),
        OutcomeString = string:to_upper(atom_to_list(Outcome)),
        io_lib:format(" ~s ~-4s ~-8s", [atom_to_list(Mod), OutcomeString, LastResult])
     end
    || {Mod, {TimeStamp, Outcome}} <- SortedResultList, lists:member(Outcome, [succ, fail])].

format_participants(SweepParticipants) ->
    SortedSweepParticipants = lists:keysort(1, SweepParticipants),
    Rows = [format_sweep_participant(SweepParticipant)
     || SweepParticipant <- SortedSweepParticipants],
    clique_status:table(Rows).

format_sweep_participant(#sweep_participant{module = Mod,
                                            description = Desciption,
                                            run_interval = Interval}) ->
    IntervalValue = riak_kv_sweeper:get_run_interval(Interval),
    IntervalString = format_interval(IntervalValue * 1000000),
    [{"Module" ,atom_to_list(Mod)},
     {"Desciption" ,Desciption},
     {"Interval",  IntervalString}].

format_interval(Interval) ->
    riak_core_format:human_time_fmt("~.1f", Interval).

format_timestamp(_Now, undefined) ->
    "--";
format_timestamp(undefined, _) ->
    "--";
format_timestamp(Now, TS) ->
    riak_core_format:human_time_fmt("~.1f", timer:now_diff(Now, TS)).
