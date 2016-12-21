%%%-------------------------------------------------------------------
%%%
%%% riak_kv_qry_sup: suprvervise the Riak query planner, etc
%%%
%%% Copyright (C) 2016 Basho Technologies, Inc. All rights reserved
%%%
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
%%%
%%%-------------------------------------------------------------------

%% @doc supervise the Riak query planner

-module(riak_kv_qry_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(TIMESERIES_MAX_CONCURRENT_QUERIES, 3).
-define(TIMESERIES_QUERY_QUEUE_LENGTH, 15).
-define(TIMESERIES_QUERY_MAX_RETURNED_DATA_SIZE, 10*1000*1000).
-define(TIMESERIES_QUERY_BUFFERS_ROOT_PATH_SUFFIX, "query_buffers").  %% relative to platform_data_dir
-define(TIMESERIES_QUERY_BUFFERS_SOFT_WATERMARK, 1*1024*1024*1024).
-define(TIMESERIES_QUERY_BUFFERS_HARD_WATERMARK, 4*1024*1024*1024).
-define(TIMESERIES_QUERY_BUFFERS_EXPIRE_MS, 0).
-define(TIMESERIES_QUERY_BUFFERS_INCOMPLETE_RELEASE_MS, 9000).

-define(SERVER, ?MODULE).

%%%===================================================================
%%% API functions
%%%===================================================================

%% @spec start_link() -> ServerRet
start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

%% @spec init([]) -> SupervisorTree
init([]) ->
    RestartStrategy = one_for_one,
    MaxRestarts = 10,
    MaxSecondsBetweenRestarts = 10,

    SupFlags = {RestartStrategy, MaxRestarts, MaxSecondsBetweenRestarts},

    Restart = permanent,
    Shutdown = 2000,
    Type = worker,

    NumFSMs = app_helper:get_env(riak_kv, timeseries_max_concurrent_queries,
                                 ?TIMESERIES_MAX_CONCURRENT_QUERIES),

    MakeNamesFn = fun(N) ->
                          Int   = integer_to_list(N),
                          _Name = list_to_atom("riak_kv_qry_" ++ Int)
                  end,
    Names = [MakeNamesFn(X) || X <- lists:seq(1, NumFSMs)],
    Riak_kv_qrys = [{X, {riak_kv_qry_worker, start_link, [X]},
                     Restart, Shutdown, Type, [riak_kv_qry_worker]} || X <- Names],

    Riak_kv_qry_q = {riak_kv_qry_queue,
                     {riak_kv_qry_queue, start_link,
                      [
                       app_helper:get_env(riak_kv, timeseries_query_queue_length,
                                          ?TIMESERIES_QUERY_QUEUE_LENGTH)
                      ]},
                     Restart, Shutdown, Type, [riak_kv_qry_queue]},

    Riak_kv_qry_b = {riak_kv_qry_buffers,
                     {riak_kv_qry_buffers, start_link,
                      [
                       [app_helper:get_env(riak_kv, timeseries_query_buffers_root_path,
                                           filename:join(
                                             app_helper:get_env(riak_core, platform_data_dir),
                                             ?TIMESERIES_QUERY_BUFFERS_ROOT_PATH_SUFFIX)),
                        app_helper:get_env(riak_kv, timeseries_query_max_returned_data_size,
                                           ?TIMESERIES_QUERY_MAX_RETURNED_DATA_SIZE),
                        app_helper:get_env(riak_kv, timeseries_query_buffers_soft_watermark,
                                           ?TIMESERIES_QUERY_BUFFERS_SOFT_WATERMARK),
                        app_helper:get_env(riak_kv, timeseries_query_buffers_hard_watermark,
                                           ?TIMESERIES_QUERY_BUFFERS_HARD_WATERMARK),
                        app_helper:get_env(riak_kv, timeseries_query_buffers_expire_ms,
                                           ?TIMESERIES_QUERY_BUFFERS_EXPIRE_MS),
                        app_helper:get_env(riak_kv, timeseries_query_buffers_incomplete_release_ms,
                                           ?TIMESERIES_QUERY_BUFFERS_INCOMPLETE_RELEASE_MS)
                       ]]},
                     Restart, Shutdown, Type, [riak_kv_qry_buffers]},

    {ok, {SupFlags, [
                     Riak_kv_qry_q, Riak_kv_qry_b |
                     Riak_kv_qrys
                    ]}}.
