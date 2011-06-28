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
    try
        case riak_core:join(NodeStr) of
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
        end
    catch
        Exception:Reason ->
            log_to_riak_node("Join failed ~p:~p~n", [Exception,
                    Reason]),
            io:format("Join failed, see log for details~n"),
            error
    end.


leave([]) ->
    remove_node(node()).

remove([Node]) ->
    remove_node(list_to_atom(Node)).

remove_node(Node) when is_atom(Node) ->
    try 
        case catch(riak_core:remove_from_cluster(Node)) of
            {'EXIT', {badarg, [{erlang, hd, [[]]}|_]}} ->
                %% This is a workaround because
                %% riak_core_gossip:remove_from_cluster doesn't check if
                %% the result of subtracting the current node from the
                %% cluster member list results in the empty list. When
                %% that code gets refactored this can probably go away.
                io:format("Leave failed, this node is the only member.~n"),
                error;
            Res ->
                io:format(" ~p\n", [Res])
        end
    catch
        Exception:Reason ->
            log_to_riak_node("Leave failed ~p:~p~n", [Exception,
                    Reason]),
            io:format("Leave failed, see log for details~n"),
            error
    end.


-spec(status([]) -> ok).
status([]) ->
    try
        case riak_kv_status:statistics() of
            [] ->
                io:format("riak_kv_stat is not enabled.\n", []);
            Stats ->
                StatString = format_stats(Stats,
                    ["-------------------------------------------\n",
                        io_lib:format("1-minute stats for ~p~n",[node()])]),
                io:format("~s\n", [StatString])
        end
    catch
        Exception:Reason ->
            log_to_riak_node("Status failed ~p:~p~n", [Exception,
                    Reason]),
            io:format("Status failed, see log for details~n"),
            error
    end.


reip([OldNode, NewNode]) ->
    try
        %% reip is called when node is down (so riak_core_ring_manager is not running),
        %% so it has to use the basic ring operations.
        %%
        %% Do *not* convert to use riak_core_ring_manager:ring_trans.
        %%
        application:load(riak_core),
        RingStateDir = app_helper:get_env(riak_core, ring_state_dir),
        {ok, RingFile} = riak_core_ring_manager:find_latest_ringfile(),
        BackupFN = filename:join([RingStateDir, filename:basename(RingFile)++".BAK"]),
        {ok, _} = file:copy(RingFile, BackupFN),
        io:format("Backed up existing ring file to ~p~n", [BackupFN]),
        Ring = riak_core_ring_manager:read_ringfile(RingFile),
        NewRing = riak_core_ring:rename_node(Ring, OldNode, NewNode),
        riak_core_ring_manager:do_write_ringfile(NewRing),
        io:format("New ring file written to ~p~n", 
            [element(2, riak_core_ring_manager:find_latest_ringfile())])
    catch
        Exception:Reason ->
            log_to_riak_node("Reip failed ~p:~p~n", [Exception,
                    Reason]),
            io:format("Reip failed, see log for details~n"),
            error
    end.

%% Check if all nodes in the cluster agree on the partition assignment
-spec(ringready([]) -> ok | error).
ringready([]) ->
    try
        case riak_core_status:ringready() of
            {ok, Nodes} ->
                io:format("TRUE All nodes agree on the ring ~p\n", [Nodes]);
            {error, {different_owners, N1, N2}} ->
                io:format("FALSE Node ~p and ~p list different partition owners\n", [N1, N2]),
                error;
            {error, {nodes_down, Down}} ->
                io:format("FALSE ~p down.  All nodes need to be up to check.\n", [Down]),
                error
        end
    catch
        Exception:Reason ->
            log_to_riak_node("Ringready failed ~p:~p~n", [Exception,
                    Reason]),
            io:format("Ringready failed, see log for details~n"),
            error
    end.

%% Provide a list of nodes with pending partition transfers (i.e. any secondary vnodes)
%% and list any owned vnodes that are *not* running
-spec(transfers([]) -> ok).
transfers([]) ->
    try
        {DownNodes, Pending} = riak_core_status:transfers(),
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
        end
    catch
        Exception:Reason ->
            log_to_riak_node("Transfers failed ~p:~p~n", [Exception,
                    Reason]),
            io:format("Transfers failed, see log for details~n"),
            error
    end.


cluster_info([OutFile|Rest]) ->
    try
        case lists:reverse(atomify_nodestrs(Rest)) of
            [] ->
                cluster_info:dump_all_connected(OutFile);
            Nodes ->
                cluster_info:dump_nodes(Nodes, OutFile)
        end
    catch
        error:{badmatch, {error, eacces}} ->
            io:format("Cluster_info failed, permission denied writing to ~p~n", [OutFile]);
        error:{badmatch, {error, enoent}} ->
            io:format("Cluster_info failed, no such directory ~p~n", [filename:dirname(OutFile)]);
        error:{badmatch, {error, enotdir}} ->
            io:format("Cluster_info failed, not a directory ~p~n", [filename:dirname(OutFile)]);
        Exception:Reason ->
            log_to_riak_node("Cluster_info failed ~p:~p~n",
                [Exception, Reason]),
            io:format("Cluster_info failed, see log for details~n"),
            error
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

%% This function changes the group leader around a call to error_logger. The
%% reason this code is needed is that when these functions are called via
%% rpc:call, the group leader is on the node making the rpc call, in the case
%% of riak-admin, this is nodetool running as an escript. Since nodetool has
%% error logging configured, this makes sure that the log message ends up in
%% riaks's log like we want.
log_to_riak_node(Format, Args) ->
    GL = erlang:group_leader(),
    erlang:group_leader(whereis(user), self()),
    error_logger:error_msg(Format, Args),
    erlang:group_leader(GL, self()).
