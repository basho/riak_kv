%% Riak EnterpriseDS
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved.
-module(riak_repl2_pg_sup).
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
  Processes =
  [ {riak_repl2_pg_block_provider_sup,
    { riak_repl2_pg_block_provider_sup, start_link, []},
    permanent, infinity, supervisor, [
    riak_repl_pg_block_provider_sup]},

  {riak_repl2_pg_block_requester_sup,
    {riak_repl2_pg_block_requester_sup, start_link, []},
    permanent, infinity, supervisor,
    [riak_repl2_pg_block_requester_sup]},

  {riak_repl2_pg, {riak_repl2_pg, start_link, []},
    permanent, 50000, worker, [riak_repl2_pg]} ],
    {ok, {{one_for_one, 9, 10}, Processes}}.
