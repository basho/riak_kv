%% Riak EnterpriseDS
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
-module(riak_repl_sup).
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
    Processes = [
                 {riak_repl_client_sup, {riak_repl_client_sup, start_link, []},
                  permanent, infinity, supervisor, [riak_repl_client_sup]},

                 {riak_repl_server_sup, {riak_repl_server_sup, start_link, []},
                  permanent, infinity, supervisor, [riak_repl_server_sup]},

                 {riak_repl_leader, {riak_repl_leader, start_link, []},
                  permanent, 5000, worker, [riak_repl_leader]},

                 {riak_repl2_leader, {riak_repl2_leader, start_link, []},
                  permanent, 5000, worker, [riak_repl2_leader]},

                 {riak_core_cluster_mgr_sup, {riak_core_cluster_mgr_sup, start_link, []},
                  permanent, infinity, supervisor, [riak_cluster_mgr_sup]},

                 {riak_repl2_fs_node_reserver, {riak_repl2_fs_node_reserver, start_link, []},
                  permanent, infinity, worker, [riak_repl2_fs_node_reserver]},

                 {riak_repl2_rt_sup, {riak_repl2_rt_sup, start_link, []},
                  permanent, infinity, supervisor, [riak_repl2_rt_sup]},

                 {riak_repl2_fscoordinator_sup, {riak_repl2_fscoordinator_sup, start_link, []},
                  permanent, infinity, supervisor, [riak_repl2_fscoordinator_sup]},

                 {riak_repl2_fscoordinator_serv_sup, {riak_repl2_fscoordinator_serv_sup, start_link, []},
                  permanent, infinity, supervisor, [riak_repl2_fscoordinator_serv_sup]},

                 {riak_repl2_fssource_sup, {riak_repl2_fssource_sup, start_link, []},
                  permanent, infinity, supervisor, [riak_repl2_fssource_sup]},

                 {riak_repl2_fssink_pool, {riak_repl2_fssink_pool, start_link, []},
                  permanent, 5000, worker, [riak_repl2_fssink_pool, poolboy]},

                 {riak_repl2_fssink_sup, {riak_repl2_fssink_sup, start_link, []},
                  permanent, infinity, supervisor, [riak_repl2_fssink_sup]},

                 {riak_repl2_pg_proxy_sup, {riak_repl2_pg_proxy_sup, start_link, []},
                  permanent, infinity, supervisor, [riak_repl2_pg_proxy_sup]},

                 {riak_repl2_pg_sup, {riak_repl2_pg_sup, start_link, []},
                  permanent, infinity, supervisor, [riak_repl2_pg_sup]}

                ],

    {ok, {{one_for_one, 9, 10}, Processes}}.

