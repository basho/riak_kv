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
    catch dtrace:init(),                   % NIF load trigger (R14B04)
    catch dyntrace:p(),                    % NIF load trigger (R15B01+)
    riak_kv_entropy_info:create_table(),
    VMaster = {riak_kv_vnode_master,
               {riak_core_vnode_master, start_link,
                [riak_kv_vnode, riak_kv_legacy_vnode, riak_kv]},
               permanent, 5000, worker, [riak_core_vnode_master]},
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
    SinkFsmSup = {riak_kv_mrc_sink_sup,
                  {riak_kv_mrc_sink_sup, start_link, []},
                  permanent, infinity, supervisor, [riak_kv_mrc_sink_sup]},
    EntropyManager = {riak_kv_entropy_manager,
                      {riak_kv_entropy_manager, start_link, []},
                      permanent, 30000, worker, [riak_kv_entropy_manager]},

    % Figure out which processes we should run...
    HasStorageBackend = (app_helper:get_env(riak_kv, storage_backend) /= undefined),

    % Build the process list...
    Processes = lists:flatten([
        ?IF(HasStorageBackend, VMaster, []),
        GetFsmSup,
        PutFsmSup,
        DeleteSup,
        SinkFsmSup,
        BucketsFsmSup,
        KeysFsmSup,
        IndexFsmSup,
        EntropyManager,
        JSSup,
        MapJSPool,
        ReduceJSPool,
        HookJSPool
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
