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

-module(riak_kv_cinfo).
-export([cluster_info_init/0, cluster_info_generator_funs/0]).
-export([get_aae_throttle/0]).

%% @spec () -> term()
%% @doc Required callback function for cluster_info: initialization.
%%
%% This function doesn't have to do anything.

cluster_info_init() ->
    ok.

%% @spec () -> list({string(), fun()})
%% @doc Required callback function for cluster_info: return list of
%%      {NameForReport, FunOfArity_1} tuples to generate ASCII/UTF-8
%%      formatted reports.

cluster_info_generator_funs() ->
    [
     {"Riak KV status", fun status/1},
     {"Riak KV ringready", fun ringready/1},
     {"Riak KV transfers", fun transfers/1},
     {"Riak KV anti-entropy throttle", fun aae_throttle/1}
    ].

aae_throttle(CPid) ->
    Props = get_aae_throttle(),
    Throttle = proplists:get_value(current_throttle, Props),
    ThrottleLimits = proplists:get_value(limits, Props),
    cluster_info:format(CPid, "Current throttle: ~p msec\n", [Throttle]),
    cluster_info:format(CPid, "Limit AAE throttle parameters: ~p\n",
                        [ThrottleLimits]).

get_aae_throttle() ->
    [{throttle_kill, riak_kv_entropy_manager:get_aae_throttle_kill()},
     {current_throttle, riak_kv_entropy_manager:get_aae_throttle()},
     {limits, riak_kv_entropy_manager:get_aae_throttle_limits()}].

status(CPid) -> % CPid is the data collector's pid.
    cluster_info:format(CPid, "~p\n", [riak_kv_status:statistics()]).

ringready(CPid) ->
    cluster_info:format(CPid, "~p\n", [riak_kv_status:ringready()]).

transfers(CPid) ->
    cluster_info:format(CPid, "~p\n", [riak_kv_status:transfers()]).

