%% -------------------------------------------------------------------
%%
%% riak_sup: supervise the core Riak services
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

%% @doc supervise the core Riak services

-module(riak_kv_sup).

-include_lib("riak_kv_js_pools.hrl").

-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

-define (IF (Bool, A, B), if Bool -> A; true -> B end).

%% @spec start_link() -> ServerRet
%% @doc API for starting the supervisor.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% @spec init([]) -> SupervisorTree
%% @doc supervisor callback.
init([]) ->
    VMaster = {riak_kv_vnode_master,
               {riak_core_vnode_master, start_link,
                [riak_kv_vnode, riak_kv_legacy_vnode]},
               permanent, 5000, worker, [riak_core_vnode_master]},
    RiakPb = [ {riak_kv_pb_socket_sup, {riak_kv_pb_socket_sup, start_link, []},
                permanent, infinity, supervisor, [riak_kv_pb_socket_sup]},
               {riak_kv_pb_listener, {riak_kv_pb_listener, start_link, []},
               permanent, 5000, worker, [riak_kv_pb_listener]}
              ],
    RiakStat = {riak_kv_stat,
                {riak_kv_stat, start_link, []},
                permanent, 5000, worker, [riak_kv_stat]},
    MapJSPool = {?JSPOOL_MAP,
                 {riak_kv_js_manager, start_link,
                  [?JSPOOL_MAP, read_js_pool_size(map_js_vm_count, "map")]},
                 permanent, 30000, worker, [riak_kv_js_manager]},
    ReduceJSPool = {?JSPOOL_REDUCE,
                    {riak_kv_js_manager, start_link,
                     [?JSPOOL_REDUCE, read_js_pool_size(reduce_js_vm_count, "reduce")]},
                    permanent, 30000, worker, [riak_kv_js_manager]},
    HookJSPool = {?JSPOOL_HOOK,
                  {riak_kv_js_manager, start_link,
                  [?JSPOOL_HOOK, read_js_pool_size(hook_js_vm_count, "hook callback")]},
                  permanent, 30000, worker, [riak_kv_js_manager]},
    JSSup = {riak_kv_js_sup,
             {riak_kv_js_sup, start_link, []},
             permanent, infinity, supervisor, [riak_kv_js_sup]},
    %% @TODO This code is only here to support
    %% rolling upgrades and will be removed.
    KLMaster = {riak_kv_keylister_master,
                 {riak_kv_keylister_master, start_link, []},
                 permanent, 30000, worker, [riak_kv_keylister_master]},
    %% @TODO This code is only here to support
    %% rolling upgrades and will be removed.
    KLSup = {riak_kv_keylister_legacy_sup,
             {riak_kv_keylister_legacy_sup, start_link, []},
             permanent, infinity, supervisor, [riak_kv_keylister_sup]},
    MapCache = {riak_kv_mapred_cache,
                 {riak_kv_mapred_cache, start_link, []},
                 permanent, 30000, worker, [riak_kv_mapred_cache]},
    MapMaster = {riak_kv_map_master,
                 {riak_kv_map_master, start_link, []},
                 permanent, 30000, worker, [riak_kv_map_master]},
    MapperSup = {riak_kv_mapper_sup,
                 {riak_kv_mapper_sup, start_link, []},
                 permanent, infinity, supervisor, [riak_kv_mapper_sup]},
    GetFsmSup = {riak_kv_get_fsm_sup,
                 {riak_kv_get_fsm_sup, start_link, []},
                 permanent, infinity, supervisor, [riak_kv_get_fsm_sup]},
    PutFsmSup = {riak_kv_put_fsm_sup,
                 {riak_kv_put_fsm_sup, start_link, []},
                 permanent, infinity, supervisor, [riak_kv_put_fsm_sup]},
    DeleteSup = {riak_kv_delete_sup,
                 {riak_kv_delete_sup, start_link, []},
                 permanent, infinity, supervisor, [riak_kv_delete_sup]},
    BucketsFsmSup = {riak_kv_buckets_fsm_sup,
                 {riak_kv_buckets_fsm_sup, start_link, []},
                 permanent, infinity, supervisor, [riak_kv_buckets_fsm_sup]},
    KeysFsmSup = {riak_kv_keys_fsm_sup,
                 {riak_kv_keys_fsm_sup, start_link, []},
                 permanent, infinity, supervisor, [riak_kv_keys_fsm_sup]},
    IndexFsmSup = {riak_kv_index_fsm_sup,
                   {riak_kv_index_fsm_sup, start_link, []},
                   permanent, infinity, supervisor, [riak_kv_index_fsm_sup]},
    %% @TODO This code is only here to support
    %% rolling upgrades and will be removed.
    LegacyKeysFsmSup = {riak_kv_keys_fsm_legacy_sup,
                 {riak_kv_keys_fsm_legacy_sup, start_link, []},
                 permanent, infinity, supervisor, [riak_kv_keys_fsm_legacy_sup]},

    % Figure out which processes we should run...
    IsPbConfigured = (app_helper:get_env(riak_kv, pb_ip) /= undefined)
        andalso (app_helper:get_env(riak_kv, pb_port) /= undefined),
    HasStorageBackend = (app_helper:get_env(riak_kv, storage_backend) /= undefined),
    IsStatEnabled = (app_helper:get_env(riak_kv, riak_kv_stat) == true),

    % Build the process list...
    Processes = lists:flatten([
        ?IF(HasStorageBackend, VMaster, []),
        ?IF(IsPbConfigured, RiakPb, []),
        ?IF(IsStatEnabled, RiakStat, []),
        GetFsmSup,
        PutFsmSup,
        DeleteSup,
        BucketsFsmSup,
        KeysFsmSup,
        IndexFsmSup,
        LegacyKeysFsmSup,
        KLSup,
        KLMaster,
        JSSup,
        MapJSPool,
        ReduceJSPool,
        HookJSPool,
        MapperSup,
        MapMaster,
        MapCache
    ]),

    % Run the proesses...
    {ok, {{one_for_one, 10, 10}, Processes}}.

%% Internal functions
read_js_pool_size(Entry, PoolType) ->
    case app_helper:get_env(riak_kv, Entry, undefined) of
        undefined ->
            OldSize = app_helper:get_env(riak_kv, js_vm_count, 0),
            lager:warning("js_vm_count has been deprecated. "
                            "Please use ~p to configure the ~s pool.", [Entry, PoolType]),
            case OldSize > 8 of
                true ->
                    OldSize div 3;
                false ->
                    OldSize
            end;
        Size ->
            Size
    end.
