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
    ActiveSweeps = riak_kv_sweeper_state:format_active_sweeps(Sweeps, IndexedParticipants),
    ParticipantsTable ++ SweepsTable ++ ActiveSweeps.

format_sweeps(Sweeps, IndexedParticipants) ->
    Now = os:timestamp(),
    Header = io_lib:format("~s", ["Sweep results:"]),
    Rows = [riak_kv_sweeper_state:format_sweep(Sweep, IndexedParticipants, Now) || Sweep <- Sweeps],
    [clique_status:text(Header),
     clique_status:table(Rows)].

format_participants(IndexedParticipants) ->
    Header = io_lib:format("~s", ["Sweep participants:"]),
    Rows = [riak_kv_sweeper_fold:format_sweep_participant(Index, Participant)
              || {Index, Participant} <- IndexedParticipants],
    [clique_status:text(Header),
     clique_status:table(Rows)].
