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
    riak_kv_entropy_info:create_table(),
    riak_kv_hooks:create_table(),
    VMaster = {riak_kv_vnode_master,
               {riak_core_vnode_master, start_link,
                [riak_kv_vnode, riak_kv_legacy_vnode, riak_kv]},
               permanent, 5000, worker, [riak_core_vnode_master]},
    HTTPCache = {riak_kv_http_cache,
		 {riak_kv_http_cache, start_link, []},
		 permanent, 5000, worker, [riak_kv_http_cache]},
    FastPutSup = {riak_kv_w1c_sup,
                 {riak_kv_w1c_sup, start_link, []},
                 permanent, infinity, supervisor, [riak_kv_w1c_sup]},
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
    ClusterAAEFsmSup = {riak_kv_clusteraae_fsm_sup,
                   {riak_kv_clusteraae_fsm_sup, start_link, []},
                   permanent, infinity, supervisor, [riak_kv_clusteraae_fsm_sup]},
    HotBackupAAEFsmSup = {riak_kv_hotbackup_fsm_sup,
                   {riak_kv_hotbackup_fsm_sup, start_link, []},
                   permanent, infinity, supervisor, [riak_kv_hotbackup_fsm_sup]},
    SinkFsmSup = {riak_kv_mrc_sink_sup,
                  {riak_kv_mrc_sink_sup, start_link, []},
                  permanent, infinity, supervisor, [riak_kv_mrc_sink_sup]},
    EntropyManager = {riak_kv_entropy_manager,
                      {riak_kv_entropy_manager, start_link, []},
                      permanent, 30000, worker, [riak_kv_entropy_manager]},
    TictacFSManager = {riak_kv_ttaaefs_manager,
                      {riak_kv_ttaaefs_manager, start_link, []},
                      permanent, 30000, worker, [riak_kv_ttaaefs_manager]},
    ReplRTQSrc = {riak_kv_replrtq_src,
                      {riak_kv_replrtq_src, start_link, []},
                      permanent, 30000, worker, [riak_kv_replrtq_src]},
    ReplRTQSnk = {riak_kv_replrtq_snk,
                      {riak_kv_replrtq_snk, start_link, []},
                      permanent, 30000, worker, [riak_kv_replrtq_snk]},
    Reaper = {riak_kv_reaper,
                {riak_kv_reaper, start_link, []},
                permanent, 30000, worker, [riak_kv_reaper]},
    Eraser = {riak_kv_eraser,
                {riak_kv_eraser, start_link, []},
                permanent, 30000, worker, [riak_kv_eraser]},

    EnsemblesKV =  {riak_kv_ensembles,
                    {riak_kv_ensembles, start_link, []},
                    permanent, 30000, worker, [riak_kv_ensembles]},

    % Figure out which processes we should run...
    HasStorageBackend = (app_helper:get_env(riak_kv, storage_backend) /= undefined),

    % Build the process list...
    Processes = lists:flatten([
        EntropyManager,
        TictacFSManager,
        ReplRTQSrc,
        ReplRTQSnk,
        Reaper,
        Eraser,
        ?IF(HasStorageBackend, VMaster, []),
        FastPutSup,
        DeleteSup,
        SinkFsmSup,
        BucketsFsmSup,
        KeysFsmSup,
        IndexFsmSup,
        ClusterAAEFsmSup,
        HotBackupAAEFsmSup,
        [EnsemblesKV || riak_core_sup:ensembles_enabled()],
        HTTPCache
    ]),

    % Run the proesses...
    {ok, {{one_for_one, 10, 10}, Processes}}.
