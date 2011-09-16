%% -------------------------------------------------------------------
%%
%% Riak: A lightweight, decentralized key-value store.
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
-module(riak_kv_status).

-export([statistics/0,
         ringready/0,
         transfers/0,
         vnode_status/0]).

-include("riak_kv_vnode.hrl").

%% ===================================================================
%% Public API
%% ===================================================================

-spec(statistics() -> [any()]).
statistics() ->
    case whereis(riak_kv_stat) of
        undefined ->
            [];
        _ ->
            riak_kv_stat:get_stats()
    end.

ringready() ->
    riak_core_status:ringready().

transfers() ->
    riak_core_status:transfers().

%% @doc Get status information about the node local vnodes.
-spec vnode_status() -> [{atom(), term()}].
vnode_status() ->
    %% Get the kv vnode indexes and the associated pids for the node.
    IndexPids = riak_core_vnode_master:all_index_pid(riak_kv_vnode),
    %% Get the status of each vnode
    [riak_core_vnode_master:command({Index, Pid},
                                    ?KV_VNODE_STATUS_REQ{},
                                    {raw, Index, self()},
                                    riak_kv_vnode_master) ||
        {Index, Pid} <- IndexPids],
    wait_for_vnode_status_results(IndexPids, []).

%% ===================================================================
%% Internal functions
%% ===================================================================

%% @private
wait_for_vnode_status_results([], Acc) ->
    Acc;
wait_for_vnode_status_results(IndexPids, Acc) ->
    receive
        {Index, {vnode_status, Status}} ->
            UpdIndexPids = proplists:delete(Index, IndexPids),
            wait_for_vnode_status_results(UpdIndexPids, [{Index, Status} | Acc]);
         _ ->
            wait_for_vnode_status_results(IndexPids, Acc)
    end.


