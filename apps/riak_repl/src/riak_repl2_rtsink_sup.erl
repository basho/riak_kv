%% Riak EnterpriseDS
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
-module(riak_repl2_rtsink_sup).
-author('Andy Gross <andy@basho.com>').
-behaviour(supervisor).

%% External exports
-export([start_link/0]).
%% supervisor callbacks
-export([init/1]).

%% @spec start_link() -> ServerRet
%% @doc API for starting the supervisor.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% @spec init([]) -> SupervisorTree
%% @doc supervisor callback.
init([]) ->
    MinPool = app_helper:get_env(riak_repl, rtsink_min_workers, 5),
    MaxPool = app_helper:get_env(riak_repl, rtsink_max_workers, 100),
    PoolArgs = [{name, {local, riak_repl2_rtsink_pool}},
                {worker_module, riak_repl_fullsync_worker},
                {worker_args, []},
                {size, MinPool}, {max_overflow, MaxPool}],
    Processes =
        [
          {riak_repl2_rtsink_pool, {poolboy, start_link, [PoolArgs]},
           permanent, 5000, worker, [poolboy]},

          {riak_repl2_rtsink_conn_sup, {riak_repl2_rtsink_conn_sup, start_link, []},
           permanent, infinity, supervisor, [riak_repl2_rtsink_conn_sup]} ],
    {ok, {{rest_for_one, 9, 10}, Processes}}.
