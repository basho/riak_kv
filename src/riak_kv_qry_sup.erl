%%%-------------------------------------------------------------------
%%%
%%% riak_kv_qry_sup: suprvervise the Riak query planner, etc
%%%
%%% Copyright (C) 2015 Basho Technologies, Inc. All rights reserved
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

-define(SERVER,     ?MODULE).

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

    % {ok, NumFSMs} = application:get_env(riak_kv, concurrent_queries),
    % {ok, MaxQ}    = application:get_env(riak_kv, query_queue_length),

    NumFSMs = 3,
    MaxQ    = 3,

    MakeNamesFn = fun(N) ->
			  Int   = integer_to_list(N),
			  _Name = list_to_atom("riak_kv_qry_" ++ Int)
		  end,
    Names = [MakeNamesFn(X) || X <- lists:seq(1, NumFSMs)],
    Riak_kv_qrys = [{X, {riak_kv_qry_worker, start_link, [X]},
		     Restart, Shutdown, Type, [riak_kv_qry_worker]} || X <- Names],

    Riak_kv_qry_q = {riak_kv_qry_queue,
		     {riak_kv_qry_queue, start_link, [MaxQ]},
		     Restart, Shutdown, Type, [riak_kv_qry_queue]},

    {ok, {SupFlags, [
		     Riak_kv_qry_q |
		     Riak_kv_qrys
		    ]}}.
