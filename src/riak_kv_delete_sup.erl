%% -------------------------------------------------------------------
%%
%% riak_kv_delete_sup: supervise the riak_kv delete state machines.
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc supervise the riak_kv delete state machines

-module(riak_kv_delete_sup).

-behaviour(supervisor).

-export([start_delete/2]).
-export([start_link/0]).
-export([init/1]).
-export([maybe_add_sweep_participant/0]).

start_delete(Node, Args) ->
    supervisor:start_child({?MODULE, Node}, Args).

%% @spec start_link() -> ServerRet
%% @doc API for starting the supervisor.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% @spec init([]) -> SupervisorTree
%% @doc supervisor callback.
init([]) ->
    maybe_add_sweep_participant(),
    DeleteSpec = {undefined,
               {riak_kv_delete, start_link, []},
               temporary, 5000, worker, [riak_kv_delete]},

    {ok, {{simple_one_for_one, 10, 10}, [DeleteSpec]}}.

maybe_add_sweep_participant() ->
    maybe_add_reap_sweep_participant(),
    maybe_add_obj_ttl_sweep_participant().

maybe_add_obj_ttl_sweep_participant() ->
    RunIntervalFun =
        fun() -> app_helper:get_env(riak_kv, obj_ttl_sweep_interval, false) end,
    case RunIntervalFun() of
        false ->
            false;
        _ ->
            add_obj_ttl_sweep_participant(RunIntervalFun)
    end.

add_obj_ttl_sweep_participant(RunInterval) ->
    riak_kv_sweeper:add_sweep_participant(_Description = "Object TTL",
                                          _Module = riak_kv_object_ttl,
                                          _FunType = delete_fun,
                                          RunInterval,
                                          _Options = [bucket_props]).

maybe_add_reap_sweep_participant() ->
    RunIntervalFun =
        fun() -> app_helper:get_env(riak_kv, reap_sweep_interval, undefined) end,
    case app_helper:get_env(riak_kv, tombstone_grace_period, disabled) of
        disabled ->
            do_nothing;
        _ ->
            add_reap_sweep_participant(RunIntervalFun)
    end.

add_reap_sweep_participant(RunInterval) ->
    riak_kv_sweeper:add_sweep_participant(_Description = "Tombstone Reaper",
                                          _Module = riak_kv_delete,
                                          _FunType = delete_fun,
                                          RunInterval).
