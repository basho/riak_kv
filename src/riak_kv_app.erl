%% -------------------------------------------------------------------
%%
%% riak_app: application startup for Riak
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

%% @doc Bootstrapping the Riak application.

-module(riak_kv_app).

-behaviour(application).
-export([start/2, prep_stop/1, stop/1]).
-export([check_kv_health/1]).

-define(SERVICES, [{riak_kv_pb_object, 3, 6}, %% ClientID stuff
                   {riak_kv_pb_object, 9, 14}, %% Object requests
                   {riak_kv_pb_bucket, 15, 18}, %% Bucket requests
                   {riak_kv_pb_mapred, 23, 24}, %% MapReduce requests
                   {riak_kv_pb_index, 25, 26},   %% Secondary index requests
                   {riak_kv_pb_csbucket, 40, 41}, %%  CS bucket folding support
                   {riak_kv_pb_counter, 50, 53} %% counter requests
                  ]).
-define(MAX_FLUSH_PUT_FSM_RETRIES, 10).

-define(DEFAULT_FSM_LIMIT, 10000).

%% @spec start(Type :: term(), StartArgs :: term()) ->
%%          {ok,Pid} | ignore | {error,Error}
%% @doc The application:start callback for riak.
%%      Arguments are ignored as all configuration is done via the erlenv file.
start(_Type, _StartArgs) ->
    riak_core_util:start_app_deps(riak_kv),

    FSM_Limit = app_helper:get_env(riak_kv, fsm_limit, ?DEFAULT_FSM_LIMIT),
    case FSM_Limit of
        undefined ->
            ok;
        _ ->
            sidejob:new_resource(riak_kv_put_fsm_sj, sidejob_supervisor, FSM_Limit),
            sidejob:new_resource(riak_kv_get_fsm_sj, sidejob_supervisor, FSM_Limit)
    end,

    case app_helper:get_env(riak_kv, direct_stats, false) of
        true ->
            ok;
        false ->
            sidejob:new_resource(riak_kv_stat_sj, riak_kv_stat_worker, 10000)
    end,

    %% Look at the epoch and generating an error message if it doesn't match up
    %% to our expectations
    check_epoch(),

    %% Append user-provided code paths
    case app_helper:get_env(riak_kv, add_paths) of
        List when is_list(List) ->
            ok = code:add_paths(List);
        _ ->
            ok
    end,

    %% Append defaults for riak_kv buckets to the bucket defaults
    %% TODO: Need to revisit this. Buckets are typically created
    %% by a specific entity; seems lame to append a bunch of unused
    %% metadata to buckets that may not be appropriate for the bucket.
    riak_core_bucket:append_bucket_defaults(
      [{linkfun, {modfun, riak_kv_wm_link_walker, mapreduce_linkfun}},
       {old_vclock, 86400},
       {young_vclock, 20},
       {big_vclock, 50},
       {small_vclock, 50},
       {pr, 0},
       {r, quorum},
       {w, quorum},
       {pw, 0},
       {dw, quorum},
       {rw, quorum},
       {basic_quorum, false},
       {notfound_ok, true}
   ]),

    %% Check the storage backend
    StorageBackend = app_helper:get_env(riak_kv, storage_backend),
    case code:ensure_loaded(StorageBackend) of
        {error,nofile} ->
            lager:critical("storage_backend ~p is non-loadable.",
                           [StorageBackend]),
            throw({error, invalid_storage_backend});
        _ ->
            ok
    end,

    %% Register our cluster_info app callback modules, with catch if
    %% the app is missing or packaging is broken.
    catch cluster_info:register_app(riak_kv_cinfo),

    %% print out critical env limits for support/debugging purposes
    catch riak_kv_env:doc_env(),

    %% Spin up supervisor
    case riak_kv_sup:start_link() of
        {ok, Pid} ->
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
                                          [v2, v1],
                                          v1),

            riak_core_capability:register({riak_kv, vclock_data_encoding},
                                          [encode_zlib, encode_raw],
                                          encode_zlib),

            riak_core_capability:register({riak_kv, crdt},
                                          [[pncounter],[]],
                                          []),

            HealthCheckOn = app_helper:get_env(riak_kv, enable_health_checks, false),
            %% Go ahead and mark the riak_kv service as up in the node watcher.
            %% The riak_core_ring_handler blocks until all vnodes have been started
            %% synchronously.
            riak_core:register(riak_kv, [
                {vnode_module, riak_kv_vnode},
                {bucket_validator, riak_kv_bucket},
                {stat_mod, riak_kv_stat}
            ]
            ++ [{health_check, {?MODULE, check_kv_health, []}} || HealthCheckOn]),

            ok = riak_api_pb_service:register(?SERVICES),

            %% Add routes to webmachine
            [ webmachine_router:add_route(R)
              || R <- lists:reverse(riak_kv_web:dispatch_table()) ],
            {ok, Pid};
        {error, Reason} ->
            {error, Reason}
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
%% @doc
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
