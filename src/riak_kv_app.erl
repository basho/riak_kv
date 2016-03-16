%% -------------------------------------------------------------------
%%
%% riak_app: application startup for Riak
%%
%% Copyright (c) 2007-2015 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc Bootstrapping the Riak application.

-module(riak_kv_app).

-behaviour(application).
-export([start/2, prep_stop/1, stop/1]).
-export([check_kv_health/1]).

-include_lib("riak_kv_types.hrl").

-define(SERVICES, [{riak_kv_pb_object, 3, 6}, %% ClientID stuff
                   {riak_kv_pb_object, 9, 14}, %% Object requests
                   {riak_kv_pb_bucket, 15, 18}, %% Bucket requests
                   {riak_kv_pb_mapred, 23, 24}, %% MapReduce requests
                   {riak_kv_pb_index, 25, 26},   %% Secondary index requests
                   {riak_kv_pb_bucket_key_apl, 33, 34}, %% (Active) Preflist requests
                   {riak_kv_pb_csbucket, 40, 41}, %%  CS bucket folding support
                   {riak_kv_pb_counter, 50, 53}, %% counter requests
                   {riak_kv_pb_coverage, 70, 71}, %% coverage requests
                   {riak_kv_pb_crdt, 80, 83}, %% CRDT requests
                   {riak_kv_pb_timeseries, 90, 104} %% time series requests
                  ]).
-define(MAX_FLUSH_PUT_FSM_RETRIES, 10).

-define(DEFAULT_FSM_LIMIT, 10000).

%% @spec start(Type :: term(), StartArgs :: term()) ->
%%          {ok,Pid} | ignore | {error,Error}
%% @doc The application:start callback for riak.
%%      Arguments are ignored as all configuration is done via the erlenv file.
start(_Type, _StartArgs) ->
    ok = start_dependent_apps(),
    ok = start_sidejobs(),
    ok = check_epoch(),
    ok = add_user_paths(),
    ok = append_bucket_defaults(),
    ok = check_storage_backend(),

    %% Register our cluster_info app callback modules, with catch if
    %% the app is missing or packaging is broken.
    catch cluster_info:register_app(riak_kv_cinfo),

    %% print out critical env limits for support/debugging purposes
    catch riak_kv_env:doc_env(),

    %% Spin up supervisor
    %% TODO isn't this a problem? the capabilities should be registered BEFORE
    %% the top sup starts, surely?
    %% (don't call me Shirley)
    case riak_kv_sup:start_link() of
        {ok, Pid} ->
            ok = register_capabilities(),
            ok = add_webmachine_routes(),
            {ok, Pid};
        {error, Reason} ->
            {error, Reason}
    end.

start_dependent_apps() ->
    ok = riak_core_util:start_app_deps(riak_kv).

start_sidejobs() ->
    FSM_Limit = app_helper:get_env(riak_kv, fsm_limit, ?DEFAULT_FSM_LIMIT),
    Status = case FSM_Limit of
                 undefined ->
                     disabled;
                 _ ->
                     sidejob:new_resource(riak_kv_put_fsm_sj, sidejob_supervisor, FSM_Limit),
                     sidejob:new_resource(riak_kv_get_fsm_sj, sidejob_supervisor, FSM_Limit),
                     enabled
             end,
    Base = [riak_core_stat:prefix(), riak_kv],
    riak_kv_exometer_sidejob:new_entry(Base ++ [put_fsm, sidejob],
                                       riak_kv_put_fsm_sj, "node_put_fsm",
                                       [{status, Status}]),
    riak_kv_exometer_sidejob:new_entry(Base ++ [get_fsm, sidejob],
                                       riak_kv_get_fsm_sj, "node_get_fsm",
                                       [{status, Status}]),

    case app_helper:get_env(riak_kv, direct_stats, false) of
        true ->
            ok;
        false ->
            sidejob:new_resource(riak_kv_stat_sj, riak_kv_stat_worker, 10000)
    end,
    ok.

add_user_paths() ->
    %% Append user-provided code paths
    case app_helper:get_env(riak_kv, add_paths) of
        List when is_list(List) ->
            ok = code:add_paths(List);
        _ ->
            ok
    end.

append_bucket_defaults() ->
        %% Append defaults for riak_kv buckets to the bucket defaults
    %% TODO: Need to revisit this. Buckets are typically created
    %% by a specific entity; seems lame to append a bunch of unused
    %% metadata to buckets that may not be appropriate for the bucket.
    ok = riak_core_bucket:append_bucket_defaults(
           [{linkfun,      {modfun, riak_kv_wm_link_walker, mapreduce_linkfun}},
            {old_vclock,   86400},
            {young_vclock, 20},
            {big_vclock,   50},
            {small_vclock, 50},
            {pr,           0},
            {r,            quorum},
            {w,            quorum},
            {pw,           0},
            {dw,           quorum},
            {rw,           quorum},
            {basic_quorum, false},
            {notfound_ok,  true},
            {write_once,   false}
           ]).

register_capabilities() ->    
    %% Register capabilities
    riak_core_capability:register({riak_kv, vnode_vclocks},
                                  [true, false],
                                  false,
                                  {riak_kv,
                                   vnode_vclocks,
                                   [{true, true}, {false, false}]}),
    
    riak_core_capability:register({riak_kv, legacy_keylisting},
                                  [false],
                                  false,
                                  {riak_kv,
                                   legacy_keylisting,
                                   [{false, false}]}),
    
    riak_core_capability:register({riak_kv, listkeys_backpressure},
                                  [true, false],
                                  false,
                                  {riak_kv,
                                   listkeys_backpressure,
                                   [{true, true}, {false, false}]}),
    
    riak_core_capability:register({riak_kv, index_backpressure},
                                  [true, false],
                                  false),
    
    %% mapred_system should remain until no nodes still exist
    %% that would propose 'legacy' as the default choice
    riak_core_capability:register({riak_kv, mapred_system},
                                  [pipe],
                                  pipe,
                                  {riak_kv,
                                   mapred_system,
                                   [{pipe, pipe}]}),
    
    riak_core_capability:register({riak_kv, mapred_2i_pipe},
                                  [true, false],
                                  false,
                                  {riak_kv,
                                   mapred_2i_pipe,
                                   [{true, true}, {false, false}]}),
    
    riak_core_capability:register({riak_kv, anti_entropy},
                                  [enabled_v1, disabled],
                                  disabled),
    
    riak_core_capability:register({riak_kv, handoff_data_encoding},
                                  [encode_raw, encode_zlib],
                                  encode_zlib),
    
    riak_core_capability:register({riak_kv, object_format},
                                  get_object_format_modes(),
                                  v0),
    
    riak_core_capability:register({riak_kv, secondary_index_version},
                                  [v3, v2, v1],
                                  v1),
    
    riak_core_capability:register({riak_kv, vclock_data_encoding},
                                  [encode_zlib, encode_raw],
                                  encode_raw),
    
    riak_core_capability:register({riak_kv, crdt},
                                  [?TOP_LEVEL_TYPES, [pncounter], []],
                                  []),
    
    riak_core_capability:register({riak_kv, crdt_epoch_versions},
                                  [?E2_DATATYPE_VERSIONS, ?E1_DATATYPE_VERSIONS],
                                  ?E1_DATATYPE_VERSIONS),
    
    riak_core_capability:register({riak_kv, put_fsm_ack_execute},
                                  [enabled, disabled],
                                  disabled),
    
%    %% register the pipeline capabilities
    riak_core_capability:register({query_pipeline, create_table},
                                  [riak_ql_create_table_pipeline:get_version()],
                                  "1.2"),

    riak_core_capability:register({query_pipeline, ddl_compiler},
                                  [riak_ql_ddl_compiler:get_version()],
                                  "1.2"),

    riak_core_capability:register({query_pipeline, ddl_validate},
                                  [riak_ql_ddl_validate_pipeline:get_version()],
                                  "1.2"),

    riak_core_capability:register({query_pipeline, describe},
                                  [riak_ql_describe_pipeline:get_version()],
                                  "1.2"),

    riak_core_capability:register({query_pipeline, insert},
                                  [riak_ql_insert_pipeline:get_version()],
                                  "1.2"),

    riak_core_capability:register({query_pipeline, parser},
                                  [riak_ql_parser:get_version()],
                                  "1.2"),

    riak_core_capability:register({query_pipeline, select},
                                  [riak_ql_select_pipeline:get_version()],
                                  "1.2"),

    riak_core_capability:register({query_pipeline, where},
                                  [riak_ql_where_pipeline:get_version()],
                                  "1.2"),

    HealthCheckOn = app_helper:get_env(riak_kv, enable_health_checks, false),
    %% Go ahead and mark the riak_kv service as up in the node watcher.
    %% The riak_core_ring_handler blocks until all vnodes have been started
    %% synchronously.
    riak_core:register(riak_kv, [
                                 {vnode_module, riak_kv_vnode},
                                 {bucket_validator, riak_kv_bucket},
                                 {stat_mod, riak_kv_stat},
                                 {permissions, [get, put, delete, list_keys, list_buckets,
                                                mapreduce, index, get_preflist]}
                                ]
                       ++ [{health_check, {?MODULE, check_kv_health, []}} || HealthCheckOn]),
    
    ok = riak_api_pb_service:register(?SERVICES).

add_webmachine_routes() ->
    %% Add routes to webmachine
    [ ok = webmachine_router:add_route(R)
                || R <- lists:reverse(riak_kv_web:dispatch_table()) ],
    ok.

check_storage_backend() ->
    %% Check the storage backend
    StorageBackend = app_helper:get_env(riak_kv, storage_backend),
    case code:ensure_loaded(StorageBackend) of
        {error,nofile} ->
            lager:critical("storage_backend ~p is non-loadable.",
                           [StorageBackend]),
            throw({error, invalid_storage_backend});
        _ ->
            ok
    end.

%% @doc Prepare to stop - called before the supervisor tree is shutdown
prep_stop(_State) ->
    try %% wrap with a try/catch - application carries on regardless,
        %% no error message or logging about the failure otherwise.

        lager:info("Stopping application riak_kv - marked service down.\n", []),
        riak_core_node_watcher:service_down(riak_kv),

        ok = riak_api_pb_service:deregister(?SERVICES),
        lager:info("Unregistered pb services"),

        %% Gracefully unregister riak_kv webmachine endpoints.
        [ webmachine_router:remove_route(R) || R <-
            riak_kv_web:dispatch_table() ],
        lager:info("unregistered webmachine routes"),
        wait_for_put_fsms(),
        lager:info("all active put FSMs completed"),
        ok
    catch
        Type:Reason ->
            lager:error("Stopping application riak_api - ~p:~p.\n", [Type, Reason])
    end,
    stopping.

%% @spec stop(State :: term()) -> ok
%% @doc The application:stop callback for riak.
stop(_State) ->
    lager:info("Stopped  application riak_kv.\n", []),
    ok.

%% 719528 days from Jan 1, 0 to Jan 1, 1970
%%  *86400 seconds/day
-define(SEC_TO_EPOCH, 62167219200).

%% @spec check_epoch() -> ok
%% @doc Look at the epoch and generating an error message if it doesn't match up
%% to our expectations
check_epoch() ->
    %% doc for erlang:now/0 says return value is platform-dependent
    %% -> let's emit an error if this platform doesn't think the epoch
    %%    is Jan 1, 1970
    {MSec, Sec, _} = os:timestamp(),
    GSec = calendar:datetime_to_gregorian_seconds(
             calendar:universal_time()),
    case GSec - ((MSec*1000000)+Sec) of
        N when (N < ?SEC_TO_EPOCH+5 andalso N > ?SEC_TO_EPOCH-5);
               (N < -?SEC_TO_EPOCH+5 andalso N > -?SEC_TO_EPOCH-5) ->
            %% if epoch is within 10 sec of expected, accept it
            ok;
        N ->
            Epoch = calendar:gregorian_seconds_to_datetime(N),
            lager:error("Riak expects your system's epoch to be Jan 1, 1970,"
                        "but your system says the epoch is ~p", [Epoch]),
            ok
    end.

check_kv_health(_Pid) ->
    VNodes = riak_core_vnode_manager:all_index_pid(riak_kv_vnode),
    {Low, High} = app_helper:get_env(riak_kv, vnode_mailbox_limit, {1, 5000}),
    case lists:member(riak_kv, riak_core_node_watcher:services(node())) of
        true ->
            %% Service active, use high watermark
            Mode = enabled,
            Threshold = High;
        false ->
            %% Service disabled, use low watermark
            Mode = disabled,
            Threshold = Low
    end,

    SlowVNs =
        [{Idx,Len} || {Idx, Pid} <- VNodes,
                      Info <- [process_info(Pid, [message_queue_len])],
                      is_list(Info),
                      {message_queue_len, Len} <- Info,
                      Len > Threshold],
    Passed = (SlowVNs =:= []),

    case {Passed, Mode} of
        {false, enabled} ->
            lager:info("Disabling riak_kv due to large message queues. "
                       "Offending vnodes: ~p", [SlowVNs]);
        {true, disabled} ->
            lager:info("Re-enabling riak_kv after successful health check");
        _ ->
            ok
    end,
    Passed.

wait_for_put_fsms(N) ->
    case riak_kv_util:exact_puts_active() of
        0 -> ok;
        Count ->
            case N of
                0 ->
                    lager:warning("Timed out waiting for put FSMs to flush"),
                    ok;
                _ -> lager:info("Waiting for ~p put FSMs to complete",
                                [Count]),
                     timer:sleep(1000),
                     wait_for_put_fsms(N-1)
            end
    end.

wait_for_put_fsms() ->
    wait_for_put_fsms(?MAX_FLUSH_PUT_FSM_RETRIES).

get_object_format_modes() ->
    %% TODO: clearly, this isn't ideal if we have more versions
    case app_helper:get_env(riak_kv, object_format, v0) of
        v0 -> [v0,v1];
        v1 -> [v1,v0]
    end.
