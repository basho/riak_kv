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
         get_stats/1,
         ringready/0,
         transfers/0,
         vnode_status/0,
         fixed_index_status/0]).

-include("riak_kv_vnode.hrl").

%% ===================================================================
%% Public API
%% ===================================================================

-spec(statistics() -> [any()]).
statistics() ->
    get_stats(console).

ringready() ->
    riak_core_status:ringready().

transfers() ->
    riak_core_status:transfers().

%% @doc Get status information about the node local vnodes.
-spec vnode_status() -> [{atom(), term()}].
vnode_status() ->
    %% Get the kv vnode indexes and the associated pids for the node.
    PrefLists = riak_core_vnode_manager:all_index_pid(riak_kv_vnode),
    riak_kv_vnode:vnode_status(PrefLists).

%% @doc Get status of 2i reformat. If the backend requires reformatting, a boolean
%%      value is returned indicating if all partitions on the node have completed
%%      upgrading (if downgrading then false indicates all partitions have been downgraded.
%%      If the backend does not require reformatting, undefined is returned
-spec fixed_index_status() -> boolean() | undefined.
fixed_index_status() ->
    Backend = app_helper:get_env(riak_kv, storage_backend),
    fixed_index_status(Backend).

fixed_index_status(Affected) when Affected =:= riak_kv_eleveldb_backend orelse
                                  Affected =:= riak_kv_multi_backend ->
    Statuses = vnode_status(),
    fixed_index_status(Affected, Statuses);
fixed_index_status(_) ->
    undefined.

fixed_index_status(Affected, Statuses) ->
    lists:foldl(fun(Elem, Acc) -> Acc andalso are_indexes_fixed(Affected, Elem) end,
                true, Statuses).

are_indexes_fixed(riak_kv_eleveldb_backend, {_Idx, [{backend_status,_,Status}]}) ->
    are_indexes_fixed(riak_kv_eleveldb_backend, Status);
are_indexes_fixed(riak_kv_eleveldb_backend, Status) ->
    case proplists:get_value(fixed_indexes, Status) of
        Bool when is_boolean(Bool) -> Bool;
        _ -> false
    end;
are_indexes_fixed(riak_kv_multi_backend, {_Idx, [{backend_status,_,Status}]}) ->
    Statuses = [S || {_, S} <- Status, lists:member({mod, riak_kv_eleveldb_backend}, Status)],
    fixed_index_status(riak_kv_eleveldb_backend, Statuses).



get_stats(web) ->
    legacy_stats(legacy_stat_map1())
        ++ riak_kv_stat_bc:read_repair_stats()
        ++ riak_kv_stat_bc:level_stats()
        ++ legacy_stats(legacy_pipe_stat_map())
        ++ riak_kv_stat_bc:cpu_stats()
        ++ riak_kv_stat_bc:mem_stats()
        ++ riak_kv_stat_bc:system_stats()
        ++ riak_kv_stat_bc:app_stats()
        ++ riak_kv_stat_bc:memory_stats()
        ++ expand_disk_stats(riak_kv_stat_bc:disk_stats())
        ++ legacy_stats(legacy_core_stat_map())
        ++ transform_vnodeq(riak_core_stat:vnodeq_stats());
get_stats(console) ->
    legacy_stats(legacy_stat_map1())
        ++ riak_kv_stat_bc:read_repair_stats()
        ++ riak_kv_stat_bc:level_stats()
        ++ legacy_stats(legacy_pipe_stat_map())
        ++ riak_kv_stat_bc:cpu_stats()
        ++ riak_kv_stat_bc:mem_stats()
        ++ riak_kv_stat_bc:system_stats()
        ++ riak_kv_stat_bc:app_stats()
        ++ riak_kv_stat_bc:memory_stats().

expand_disk_stats([{disk, Stats}]) ->
    [{disk, [{struct, [{id, list_to_binary(Id)}, {size, Size}, {used, Used}]}
             || {Id, Size, Used} <- Stats]}].

legacy_stats(Map) ->
    P = riak_core_stat:prefix(),
    lists:foldr(
      fun({K, DPs}, Acc) ->
              case exometer:get_value([P|K], [D || {D,_} <- DPs]) of
                  {ok, Vs} when is_list(Vs) ->
                      lists:foldr(fun({D,V}, Acc1) ->
                                          {_,N} = lists:keyfind(D,1,DPs),
                                          [{N,V}|Acc1]
                                  end, Acc, Vs);
                  _ ->
                      lists:foldr(fun({_,N}, Acc1) ->
                                          [{N,0}|Acc1]
                                  end, Acc, DPs)
              end
      end, [], Map).


legacy_stat_map1() ->
    [{[riak_kv,vnode,gets], [{one, vnode_gets},
                             {count, vnode_gets_total}]},
     {[riak_kv,vnode,puts], [{one, vnode_puts},
                             {count, vnode_puts_total}]},
     {[riak_kv,vnode,index,reads], [{one, vnode_index_reads},
                                    {count, vnode_index_reads_total}]},
     {[riak_kv,vnode,index,writes], [{one, vnode_index_writes},
                                     {count, vnode_index_writes_total}]},
     {[riak_kv,vnode,index,writes,postings], [{one, vnode_index_writes_postings},
                                              {count, vnode_index_writes_postings_total}]},
     {[riak_kv,vnode,index,deletes], [{one, vnode_index_deletes},
                                      {count, vnode_index_deletes_total}]},
     {[riak_kv,vnode,index,deletes,postings], [{one, vnode_index_deletes_postings},
                                               {count, vnode_index_deletes_postings_total}]},
     {[riak_kv,node,gets], [{one, node_gets},
                            {count, node_gets_total}]},
     {[riak_kv,node,gets,siblings], [{mean, node_get_fsm_siblings_mean},
                                     {median, node_get_fsm_siblings_median},
                                     {95, node_get_fsm_siblings_95},
                                     {99, node_get_fsm_siblings_99},
                                     {max, node_get_fsm_siblings_100}]},
     {[riak_kv,node,gets,objsize], [{mean,node_get_fsm_objsize_mean},
                                    {median,node_get_fsm_objsize_median},
                                    {95,node_get_fsm_objsize_95},
                                    {99,node_get_fsm_objsize_99},
                                    {max,node_get_fsm_objsize_100}]},
     {[riak_kv,node,gets,time], [{mean,node_get_fsm_time_mean},
                                 {median,node_get_fsm_time_median},
                                 {95,node_get_fsm_time_95},
                                 {99,node_get_fsm_time_99},
                                 {max,node_get_fsm_time_100}]},
     {[riak_kv,node,puts], [{one, node_puts},
                            {count, node_puts_total}]},
     {[riak_kv,node,puts,time], [{mean, node_put_fsm_time_mean},
                                 {median, node_put_fsm_time_median},
                                 {95, node_put_fsm_time_95},
                                 {99, node_put_fsm_time_99},
                                 {max, node_put_fsm_time_100}]},
     {[riak_kv,node,gets,read_repairs], [{one, read_repairs},
                                         {count, read_repairs_total}]},
     {[riak_kv,node,puts,coord_redirs], [{value,coord_redirs_total}]},
     {[riak_kv,mapper_count], [{value, executing_mappers}]},
     {[riak_kv,precommit_fail], [{value, precommit_fail}]},
     {[riak_kv,postcommit_fail], [{value, postcommit_fail}]},
     {[riak_kv,index,fsm,create], [{one, index_fsm_create}]},
     {[riak_kv,index,fsm,create,error], [{one, index_fsm_create_error}]},
     {[riak_kv,index,fsm,active], [{value, index_fsm_active}]},
     {[riak_kv,list,fsm,active], [{value, list_fsm_active}]},
     {[riak_api,pbc_connects,active], [{value, pbc_active}]},
     {[riak_api,pbc_connects], [{one, pbc_connects},
                                {count, pbc_connects_total}]},
     {[riak_kv,get_fsm], [{usage, node_get_fsm_active},
                          {usage_60s, node_get_fsm_active_60s},
                          {in_rate, node_get_fsm_in_rate},
                          {out_rate, node_get_fsm_out_rate},
                          {rejected, node_get_fsm_rejected},
                          {rejected_60s, node_get_fsm_rejected_60s},
                          {rejected_total, node_get_fsm_rejected_total}]},
     {[riak_kv,put_fsm], [{usage, node_put_fsm_active},
                          {usage_60s, node_put_fsm_active_60s},
                          {in_rate, node_put_fsm_in_rate},
                          {out_rate, node_put_fsm_out_rate},
                          {rejected, node_put_fsm_rejected},
                          {rejected_60s, node_put_fsm_rejected_60s},
                          {rejected_total, node_put_fsm_rejected_total}]}].


legacy_pipe_stat_map() ->
    [{[riak_pipe,pipeline,create], [{count, pipeline_create_count},
                                    {one, pipeline_create_one}]},
     {[riak_pipe,pipeline,create,error], [{count, pipeline_create_error_count},
                                          {one, pipeline_create_error_one}]},
     {[riak_pipe,pipeline,active], [{value, pipeline_active}]}].

legacy_core_stat_map() ->
    [{[riak_core,ignored_gossip_total], [{value, ignored_gossip_total}]},
     {[riak_core,rings_reconciled], [{count, rings_reconciled_total},
                                     {one, rings_reconciled}]},
     {[riak_core,gossip_received], [{one, gossip_received}]},
     {[riak_core,rejected_handoffs], [{value, rejected_handoffs}]},
     {[riak_core,handoff_timeouts], [{value, handoff_timeouts}]},
     {[riak_core,dropped_vnode_requests_total], [{value,dropped_vnode_requests_total}]},
     {[riak_core,converge_delay], [{min, converge_delay_min},
                                   {max, converge_delay_max},
                                   {last, converge_delay_last}]},
     {[riak_core,rebalance_delay], [{min, rebalance_delay_min},
                                    {max, rebalance_delay_max},
                                    {mean, rebalance_delay_mean},
                                    {last, rebalance_delay_last}]}].

transform_vnodeq(Stats) ->
    lists:flatmap(
      fun({[_, riak_core,N], [{value, V}]}) ->
              [{N, V}];
         ({[_, riak_core, N], Vals}) ->
              [{join(N, K), V} || {K, V} <- Vals]
      end, Stats).

join(A, B) ->
    binary_to_list(<< (atom_to_binary(A, latin1))/binary, "_",
                      (atom_to_binary(B, latin1))/binary >>).
