%% -------------------------------------------------------------------
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

%% @doc Supervise the riak_kv backend status state machines.

-module(riak_kv_backend_status_fsm_sup).

-behaviour(supervisor).

-export([start_backend_status_fsm/2]).
-export([start_link/0]).
-export([init/1]).

start_backend_status_fsm(Node, Args) ->
    supervisor:start_child({?MODULE, Node}, Args).

%% @spec start_link() -> ServerRet
%% @doc API for starting the supervisor.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% @spec init([]) -> SupervisorTree
%% @doc supervisor callback.
init([]) ->
    BackendStatusFsmSpec = {undefined,
                            {riak_core_coverage_fsm,
                             start_link,
                             [riak_kv_backend_status_fsm]
                            },
                            temporary,
                            5000,
                            worker,
                            [riak_kv_backend_status_fsm]},
    {ok, {{simple_one_for_one, 10, 10}, [BackendStatusFsmSpec]}}.
