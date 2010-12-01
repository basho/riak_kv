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

-export([join/1, leave/1, remove/1, status/1, reip/1, ringready/1, transfers/1]).

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

status([]) ->
    status2(true);
status(quiet) ->                                % Used by riak_kv_cinfo
    status2(false).

status2(Verbose) ->
    case whereis(riak_kv_stat) of
        undefined ->
            verbose(Verbose, "riak_kv_stat is not enabled.~n", []),
            not_enabled;
        _ ->
            Stats = riak_kv_stat:get_stats(),
            StatString =
                format_stats(Stats, 
                             ["-------------------------------------------\n",
                              io_lib:format("1-minute stats for ~p~n",[node()])]),
            verbose(Verbose, "~s~n", [StatString]),
            Stats
    end.

reip([OldNode, NewNode]) ->
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
              [element(2, riak_core_ring_manager:find_latest_ringfile())]).

%% Check if all nodes in the cluster agree on the partition assignment
ringready([]) ->
    ringready2(true);
ringready(quiet) ->                                % Used by riak_kv_cinfo
    ringready2(false).

ringready2(Verbose) ->
    {Res, Extra} = 
        case get_rings() of
            {[], Rings} ->
                {N1,R1}=hd(Rings),
                case rings_match(hash_ring(R1), tl(Rings)) of
                    true ->
                        Nodes = [N || {N,_} <- Rings],
                        verbose(Verbose, "TRUE All nodes agree on the ring ~p\n", [Nodes]),
                        {true, {true, all_agree, Nodes}};
                    {false, N2} ->
                        verbose(Verbose, "FALSE Node ~p and ~p list different partition owners\n", [N1, N2]),
                        {false, {false, different_owners, N1, N2}}
                end;
            {Down, _Rings} ->
                verbose(Verbose, "FALSE ~p down.  All nodes need to be up to check.\n", [Down]),
                {false, {false, nodes_down, Down}}
        end,
    if Verbose == true ->
            Res; % make nodetool exit 0 or 1
       true ->
            Extra
    end.

%% Provide a list of nodes with pending partition transfers (i.e. any secondary vnodes)
%% and list any owned vnodes that are *not* running

transfers([]) ->
    transfers2(true);
transfers(quiet) ->                                % Used by riak_kv_cinfo
    transfers2(false).

transfers2(Verbose) ->
    {Down, Rings} = get_rings(),
    case Down of
        [] ->
            ok;
        _ ->
            verbose(Verbose, "Nodes ~p are currently down.\n", [Down])
    end,

    %% Work out which vnodes are running and which partitions they claim
    F = fun({N,R}, Acc) ->
                {_Pri, Sec, Stopped} = partitions(N, R),
                case Sec of
                    [] ->
                        [];
                    _ ->
                        verbose(Verbose, "~p waiting to handoff ~p partitions\n", [N, length(Sec)]),
                        [{waiting_to_handoff, N, length(Sec)}]
                end ++
                    case Stopped of
                        [] ->
                            [];
                        _ ->
                            verbose(Verbose, "~p does not have ~p primary partitions running\n",
                                      [N, length(Stopped)]),
                            [{stopped, N}]
                    end ++
                    Acc
        end,
    Res = lists:foldl(F, [], Rings),
    case Res of
        [] ->
            verbose(Verbose, "No transfers active\n", []);
        _ ->
            ok
    end,
    Res.


format_stats([], Acc) ->
    lists:reverse(Acc);
format_stats([{vnode_gets, V}|T], Acc) ->
    format_stats(T, [io_lib:format("vnode gets : ~p~n", [V])|Acc]);
format_stats([{Stat, V}|T], Acc) ->
    format_stats(T, [io_lib:format("~p : ~p~n", [Stat, V])|Acc]).

%% Retrieve the rings for all other nodes by RPC
get_rings() ->
    {RawRings, Down} = riak_core_util:rpc_every_member(
                         riak_core_ring_manager, get_my_ring, [], 30000),
    Rings = orddict:from_list([{riak_core_ring:owner_node(R), R} || {ok, R} <- RawRings]),
    {lists:sort(Down), Rings}.      

%% Produce a hash of the 'chash' portion of the ring
hash_ring(R) ->
    erlang:phash2(riak_core_ring:all_owners(R)).

%% Check if all rings match given a hash and a list of [{N,P}] to check
rings_match(_, []) ->
    true;
rings_match(R1hash, [{N2, R2} | Rest]) ->
    case hash_ring(R2) of
        R1hash ->
            rings_match(R1hash, Rest);
        _ ->
            {false, N2}
    end.
    
%% Get a list of active partition numbers - regardless of vnode type
active_partitions(Node) ->
    lists:foldl(fun({_,P}, Ps) -> 
                        ordsets:add_element(P, Ps)
                end, [], running_vnodes(Node)).
                            

%% Get a list of running vnodes for a node
running_vnodes(Node) ->
    Pids = vnode_pids(Node),
    [rpc:call(Node, riak_core_vnode, get_mod_index, [Pid], 30000) || Pid <- Pids].
        
%% Get a list of vnode pids for a node
vnode_pids(Node) ->
    [Pid || {_,Pid,_,_} <- supervisor:which_children({riak_core_vnode_sup, Node})].

%% Return a list of active primary partitions, active secondary partitions (to be handed off)
%% and stopped partitions that should be started
partitions(Node, Ring) ->
    Owners = riak_core_ring:all_owners(Ring),
    Owned = ordsets:from_list(owned_partitions(Owners, Node)),
    Active = ordsets:from_list(active_partitions(Node)),
    Stopped = ordsets:subtract(Owned, Active),
    Secondary = ordsets:subtract(Active, Owned),
    Primary = ordsets:subtract(Active, Secondary),
    {Primary, Secondary, Stopped}.

%% Return the list of partitions owned by a node
owned_partitions(Owners, Node) ->
    [P || {P, Owner} <- Owners, Owner =:= Node].          

verbose(false, _Fmt, _Args) ->
    ok;
verbose(true, Fmt, Args) ->
    io:format(Fmt, Args).
