%% -------------------------------------------------------------------
%%
%% riak_kv_group_keys_fsm_sup: supervise the riak_kv keys state machines.
%%
%% Copyright (c) 2017 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc supervise the riak_kv keys state machines

-module(riak_kv_group_list_fsm_sup).

-behaviour(supervisor).

-export([start_group_list_fsm/2]).
-export([start_link/0]).
-export([init/1]).

start_group_list_fsm(Node, Args) ->
    case supervisor:start_child({?MODULE, Node}, Args) of
        {ok, Pid} ->
            ok = riak_kv_stat:update({list_group_create, Pid}),
            {ok, Pid};
        Error ->
            ok = riak_kv_stat:update(list_group_create_error),
            Error
    end.

%% @spec start_link() -> ServerRet
%% @doc API for starting the supervisor.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% @spec init([]) -> SupervisorTree
%% @doc supervisor callback.
init([]) ->
    KeysFsmSpec = {
        undefined,
        {riak_core_coverage_fsm, start_link, [riak_kv_group_list_fsm]},
        temporary, 5000, worker, [riak_kv_group_list_fsm]
    },
    {ok, {{simple_one_for_one, 10, 10}, [KeysFsmSpec]}}.
