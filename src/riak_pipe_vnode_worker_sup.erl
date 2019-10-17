%% -------------------------------------------------------------------
%%
%% Copyright (c) 2011 Basho Technologies, Inc.
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

%% @doc Supervisor for a vnode's worker processes.  One of these is
%%      started per vnode.
-module(riak_pipe_vnode_worker_sup).

-behaviour(supervisor).

%% API
-export([start_link/2]).
-export([start_worker/2,
         terminate_worker/2]).

%% Supervisor callbacks
-export([init/1]).

-include("riak_pipe.hrl").

%%%===================================================================
%%% API functions
%%%===================================================================

%% @doc Start the supervisor.
-spec start_link(riak_pipe_vnode:partition(), pid()) ->
         {ok, pid()} | ignore | {error, term()}.
start_link(Partition, VnodePid) ->
    supervisor:start_link(?MODULE, [Partition, VnodePid]).

%% @doc Start a new worker under the supervisor.
-spec start_worker(pid(), riak_pipe_fitting:details()) -> {ok, pid()}.
start_worker(Supervisor, Details) ->
    supervisor:start_child(Supervisor, [Details]).

%% @doc Stop a worker immediately
-spec terminate_worker(pid(), pid()) -> ok | {error, term()}.
terminate_worker(Supervisor, WorkerPid) ->
    supervisor:terminate_child(Supervisor, WorkerPid).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

%% @doc Initialize the supervisor.  This is a `simple_one_for_one',
%%      whose child spec is for starting `riak_pipe_vnode_worker'
%%      FSMs.
-spec init([riak_pipe_vnode:partition() | pid()]) ->
         {ok, {{supervisor:strategy(),
                pos_integer(),
                pos_integer()},
               [ supervisor:child_spec() ]}}.
init([Partition, VnodePid]) ->
    RestartStrategy = simple_one_for_one,
    MaxRestarts = 1000,
    MaxSecondsBetweenRestarts = 3600,

    SupFlags = {RestartStrategy, MaxRestarts, MaxSecondsBetweenRestarts},

    Restart = temporary,
    Shutdown = 2000,
    Type = worker,

    AChild = {undefined, % no registered name
              {riak_pipe_vnode_worker, start_link, [Partition, VnodePid]},
              Restart, Shutdown, Type, [riak_pipe_vnode_worker]},

    {ok, {SupFlags, [AChild]}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
