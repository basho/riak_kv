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

-export([join/1,
         leave/1,
         remove/1,
         status/1,
         vnode_status/1,
         reip/1,
         ringready/1,
         transfers/1,
         cluster_info/1,
         down/1,
         reload_code/1]).

join([NodeStr]) ->
    try
        case riak_core:join(NodeStr) of
            ok ->
                io:format("Sent join request to ~s~n", [NodeStr]),
                ok;
            {error, not_reachable} ->
                io:format("Node ~s is not reachable!~n", [NodeStr]),
                error;
            {error, different_ring_sizes} ->
                io:format("Failed: ~s has a different ring_creation_size~n",
                          [NodeStr]),
                error;
            {error, unable_to_get_join_ring} ->
                io:format("Failed: Unable to get ring from ~s~n", [NodeStr]),
                error;
            {error, not_single_node} ->
                io:format("Failed: This node is already a member of a "
                          "cluster~n"),
                error;
            {error, _} ->
                io:format("Join failed. Try again in a few moments.~n", []),
                error
        end
    catch
        Exception:Reason ->
            lager:error("Join failed ~p:~p", [Exception, Reason]),
            io:format("Join failed, see log for details~n"),
            error
    end.


leave([]) ->
    try
        case riak_core:leave() of
            ok ->
                io:format("Success: ~p will shutdown after handing off "
                          "its data~n", [node()]),
                ok;
            {error, already_leaving} ->
                io:format("~p is already in the process of leaving the "
                          "cluster.~n", [node()]),
                ok;
            {error, not_member} ->
                io:format("Failed: ~p is not a member of the cluster.~n",
                          [node()]),
                error;
            {error, only_member} ->
                io:format("Failed: ~p is the only member.~n", [node()]),
                error
        end
    catch
        Exception:Reason ->
            lager:error("Leave failed ~p:~p", [Exception, Reason]),
            io:format("Leave failed, see log for details~n"),
            error
    end.

remove([Node]) ->
    try
        case riak_core:remove(list_to_atom(Node)) of
            ok ->
                io:format("Success: ~p removed from the cluster~n", [Node]),
                ok;
            {error, not_member} ->
                io:format("Failed: ~p is not a member of the cluster.~n",
                          [Node]),
                error;
            {error, only_member} ->
                io:format("Failed: ~p is the only member.~n", [Node]),
                error
        end
    catch
        Exception:Reason ->
            lager:error("Remove failed ~p:~p", [Exception, Reason]),
            io:format("Remove failed, see log for details~n"),
            error
    end.

down([Node]) ->
    try
        case riak_core:down(list_to_atom(Node)) of
            ok ->
                io:format("Success: ~p marked as down~n", [Node]),
                ok;
            {error, legacy_mode} ->
                io:format("Cluster is currently in legacy mode~n"),
                ok;
            {error, is_up} ->
                io:format("Failed: ~s is up~n", [Node]),
                error;
            {error, not_member} ->
                io:format("Failed: ~p is not a member of the cluster.~n",
                          [Node]),
                error;
            {error, only_member} ->
                io:format("Failed: ~p is the only member.~n", [Node]),
                error
        end
    catch
        Exception:Reason ->
            lager:error("Down failed ~p:~p", [Exception, Reason]),
            io:format("Down failed, see log for details~n"),
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
            lager:error("Status failed ~p:~p", [Exception,
                    Reason]),
            io:format("Status failed, see log for details~n"),
            error
    end.

-spec(vnode_status([]) -> ok).
vnode_status([]) ->
    try
        case riak_kv_status:vnode_status() of
            [] ->
                io:format("There are no active vnodes.~n");
            Statuses ->
                io:format("~s~n-------------------------------------------~n~n",
                          ["Vnode status information"]),
                print_vnode_statuses(lists:sort(Statuses))
        end
    catch
        Exception:Reason ->
            lager:error("Backend status failed ~p:~p", [Exception,
                    Reason]),
            io:format("Backend status failed, see log for details~n"),
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
            lager:error("Reip failed ~p:~p", [Exception,
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
            lager:error("Ringready failed ~p:~p", [Exception,
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
            lager:error("Transfers failed ~p:~p", [Exception,
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
            lager:error("Cluster_info failed ~p:~p",
                [Exception, Reason]),
            io:format("Cluster_info failed, see log for details~n"),
            error
    end.

reload_code([]) ->
    case app_helper:get_env(riak_kv, add_paths) of
        List when is_list(List) ->
            [ reload_path(filename:absname(Path)) || Path <- List ],
            ok;
        _ -> ok
    end.

reload_path(Path) ->
    {ok, Beams} = file:list_dir(Path),
    [ reload_file(filename:absname(Beam, Path)) || Beam <- Beams, ".beam" == filename:extension(Beam) ].

reload_file(Filename) ->
    Mod = list_to_atom(filename:basename(Filename, ".beam")),
    case code:is_loaded(Mod) of
        {file, Filename} ->
            code:soft_purge(Mod),
            code:load_file(Mod),
            io:format("Reloaded module ~w from ~s.~n", [Mod, Filename]);
        {file, Other} ->
            io:format("CONFLICT: Module ~w originally loaded from ~s, won't reload from ~s.~n", [Mod, Other, Filename]);
        _ ->
            io:format("Module ~w not yet loaded, skipped.~n", [Mod])
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

print_vnode_statuses([]) ->
    ok;
print_vnode_statuses([{VNodeIndex, StatusData} | RestStatuses]) ->
    io:format("VNode: ~p~n", [VNodeIndex]),
    print_vnode_status(StatusData),
    io:format("~n"),
    print_vnode_statuses(RestStatuses).

print_vnode_status([]) ->
    ok;
print_vnode_status([{backend_status,
                     Backend,
                     StatusItem} | RestStatusItems]) ->
    if is_binary(StatusItem) ->
            StatusString = binary_to_list(StatusItem),
            io:format("Backend: ~p~nStatus: ~n~s~n",
                      [Backend, string:strip(StatusString)]);
       true ->
            io:format("Backend: ~p~nStatus: ~n~p~n",
                      [Backend, StatusItem])
    end,
    print_vnode_status(RestStatusItems);
print_vnode_status([StatusItem | RestStatusItems]) ->
    if is_binary(StatusItem) ->
            StatusString = binary_to_list(StatusItem),
            io:format("Status: ~n~s~n",
                      [string:strip(StatusString)]);
       true ->
            io:format("Status: ~n~p~n", [StatusItem])
    end,
    print_vnode_status(RestStatusItems).
