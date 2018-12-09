%% -------------------------------------------------------------------
%%
%% riak_kv_hotbackup_fsm_sup: supervise the hotbackup state machine.
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

%% @doc supervise the riak_kv cluster aae state machine

-module(riak_kv_hotbackup_fsm_sup).

-behaviour(supervisor).

-export([start_hotbackup_fsm/2]).
-export([start_link/0]).
-export([init/1]).

start_hotbackup_fsm(Node, Args) ->
    % No stat is updated on this, there will be a log for every backup instead
    % of incrementing a stat.  This is not expected to be a regular event that
    % requires counting
    supervisor:start_child({?MODULE, Node}, Args).

%% @spec start_link() -> ServerRet
%% @doc API for starting the supervisor.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% @spec init([]) -> SupervisorTree
%% @doc supervisor callback.
init([]) ->
    HotBackupFsmSpec = 
        {undefined,
            {riak_core_coverage_fsm, start_link, [riak_kv_hotbackup_fsm]},
            temporary, 5000, worker, [riak_kv_hotbackup_fsm]},

    {ok, {{simple_one_for_one, 10, 10}, [HotBackupFsmSpec]}}.
