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
-export([start/2,stop/1]).

%% @spec start(Type :: term(), StartArgs :: term()) ->
%%          {ok,Pid} | ignore | {error,Error}
%% @doc The application:start callback for riak.
%%      Arguments are ignored as all configuration is done via the erlenv file.
start(_Type, _StartArgs) ->
    riak_core_util:start_app_deps(riak_kv),

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
                                          [false, true],
                                          true,
                                          {riak_kv,
                                           legacy_keylisting,
                                           [{true, true}, {false, false}]}),

            riak_core_capability:register({riak_kv, listkeys_backpressure},
                                          [true, false],
                                          false,
                                          {riak_kv,
                                           listkeys_backpressure,
                                           [{true, true}, {false, false}]}),

            riak_core_capability:register({riak_kv, mapred_system},
                                          [pipe, legacy],
                                          legacy,
                                          {riak_kv,
                                           mapred_system,
                                           [{pipe, pipe}, {legacy, legacy}]}),

            riak_core_capability:register({riak_kv, mapred_2i_pipe},
                                          [true, false],
                                          false,
                                          {riak_kv,
                                           mapred_2i_pipe,
                                           [{true, true}, {false, false}]}),

            %% Go ahead and mark the riak_kv service as up in the node watcher.
            %% The riak_core_ring_handler blocks until all vnodes have been started
            %% synchronously.
            riak_core:register(riak_kv, [
                {vnode_module, riak_kv_vnode},
                {bucket_validator, riak_kv_bucket},
                {stat_mod, riak_kv_stat}
            ]),

            ok = riak_api_pb_service:register([{riak_kv_pb_object, 3, 6}, %% ClientID stuff
                                               {riak_kv_pb_object, 9, 14}, %% Object requests
                                               {riak_kv_pb_bucket, 15, 22}, %% Bucket requests
                                               {riak_kv_pb_mapred, 23, 24}, %% MapReduce requests
                                               {riak_kv_pb_index, 25, 26} %% Secondary index requests
                                               ]),

            %% Add routes to webmachine
            [ webmachine_router:add_route(R)
              || R <- lists:reverse(riak_kv_web:dispatch_table()) ],
            {ok, Pid};
        {error, Reason} ->
            {error, Reason}
    end.

%% @spec stop(State :: term()) -> ok
%% @doc The application:stop callback for riak.
stop(_State) ->
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
    {MSec, Sec, _} = erlang:now(),
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

