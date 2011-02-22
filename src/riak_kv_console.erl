%% -------------------------------------------------------------------
%%
%% riak_console: interface for Riak admin commands
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

%% @doc interface for Riak admin commands

-module(riak_kv_console).

-export([join/1, leave/1, remove/1, status/1, reip/1, ringready/1, transfers/1,
         cluster_info/1]).

join([NodeStr]) ->
    case riak:join(NodeStr) of
        ok ->
            io:format("Sent join request to ~s\n", [NodeStr]),
            ok;
        {error, not_reachable} ->
            io:format("Node ~s is not reachable!\n", [NodeStr]),
            error;
        {error, different_ring_sizes} ->
            io:format("Failed: ~s has a different ring_creation_size~n",
                      [NodeStr]),
            error
    end;
join(_) ->
    io:format("Join requires a node to join with.\n"),
    error.

leave([]) ->
    remove_node(node()).

remove([Node]) ->
    remove_node(list_to_atom(Node)).

remove_node(Node) when is_atom(Node) ->
    {ok, C} = riak:local_client(),
    Res = C:remove_from_cluster(Node),
    io:format("~p\n", [Res]).

-spec(status([]) -> ok).
status([]) ->
    case riak_kv_status:statistics() of
        [] ->
            io:format("riak_kv_stat is not enabled.\n", []);
        Stats ->
            StatString = format_stats(Stats,
                                      ["-------------------------------------------\n",
                                       io_lib:format("1-minute stats for ~p~n",[node()])]),
            io:format("~s\n", [StatString])
    end.

reip([OldNode, NewNode]) ->
    application:load(riak_core),
    RingStateDir = app_helper:get_env(riak_core, ring_state_dir),
    {ok, RingFile} = riak_core_ring_manager:find_latest_ringfile(),
    BackupFN = filename:join([RingStateDir, filename:basename(RingFile)++".BAK"]),
    {ok, _} = file:copy(RingFile, BackupFN),
    io:format("Backed up existing ring file to ~p~n", [BackupFN]),
    F = fun(Ring, []) ->
                case riak_core_ring:rename_node(Ring, OldNode, NewNode) of
                    Ring ->
                        ignore;
                    NewRing ->
                        {new_ring, NewRing}
                end
        end,
    {ok, _NewRing} = riak_core_ring_manager:ring_trans(F, []),
    io:format("New ring file written to ~p~n", 
              [element(2, riak_core_ring_manager:find_latest_ringfile())]).

%% Check if all nodes in the cluster agree on the partition assignment
-spec(ringready([]) -> ok | error).
ringready([]) ->
    case riak_kv_status:ringready() of
        {ok, Nodes} ->
            io:format("TRUE All nodes agree on the ring ~p\n", [Nodes]);
        {error, {different_owners, N1, N2}} ->
            io:format("FALSE Node ~p and ~p list different partition owners\n", [N1, N2]),
            error;
        {error, {nodes_down, Down}} ->
            io:format("FALSE ~p down.  All nodes need to be up to check.\n", [Down]),
            error
    end.

%% Provide a list of nodes with pending partition transfers (i.e. any secondary vnodes)
%% and list any owned vnodes that are *not* running
-spec(transfers([]) -> ok).
transfers([]) ->
    {DownNodes, Pending} = riak_kv_status:transfers(),
    case DownNodes of
        [] -> ok;
        _  -> io:format("Nodes ~p are currently down.\n", [DownNodes])
    end,
    F = fun({waiting_to_handoff, Node, Count}, Acc) ->
                io:format("~p waiting to handoff ~p partitions\n", [Node, Count]),
                Acc + 1;
           ({stopped, Node, Count}, Acc) ->
                io:format("~p does not have ~p primary partitions running\n", [Node, Count]),
                Acc + 1
        end,
    case lists:foldl(F, 0, Pending) of
        0 ->
            io:format("No transfers active\n"),
            ok;
        _ ->
            error
    end.

cluster_info([OutFile|Rest]) ->
    case lists:reverse(atomify_nodestrs(Rest)) of
        [] ->
            cluster_info:dump_all_connected(OutFile);
        Nodes ->
            cluster_info:dump_nodes(Nodes, OutFile)
    end.

format_stats([], Acc) ->
    lists:reverse(Acc);
format_stats([{vnode_gets, V}|T], Acc) ->
    format_stats(T, [io_lib:format("vnode gets : ~p~n", [V])|Acc]);
format_stats([{Stat, V}|T], Acc) ->
    format_stats(T, [io_lib:format("~p : ~p~n", [Stat, V])|Acc]).

atomify_nodestrs(Strs) ->
    lists:foldl(fun("local", Acc) -> [node()|Acc];
                   (NodeStr, Acc) -> try
                                         [list_to_existing_atom(NodeStr)|Acc]
                                     catch error:badarg ->
                                         io:format("Bad node: ~s\n", [NodeStr]),
                                         Acc
                                     end
                end, [], Strs).
