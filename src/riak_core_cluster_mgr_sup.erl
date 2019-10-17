%% Riak Core Cluster Manager Supervisor
%% Copyright 2007-2012 Basho Technologies, Inc. All Rights Reserved.
%%
-module(riak_core_cluster_mgr_sup).
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
    ClusterMgrDefaults = [fun riak_repl_app:cluster_mgr_member_fun/1,
                          fun riak_repl_app:cluster_mgr_all_member_fun/1,
                          fun riak_repl_app:cluster_mgr_write_cluster_members_to_ring/2,
                          fun riak_repl_app:cluster_mgr_read_cluster_targets_from_ring/0],
    Processes =
        [%% TODO: move these to riak core
         %% Service Manager (inbound connections)
         {riak_core_service_mgr, {riak_core_service_mgr, start_link, []},
          permanent, 5000, worker, [riak_core_service_mgr]},
         %% Connection Manager Stats - don't start it from here. It gets registered
         %% {riak_core_connection_mgr_stats, {riak_core_connection_mgr_stats, start_link, []},
          %% permanent, 5000, worker, [riak_core_connection_mgr_stats]},
         %% Connection Manager (outbound connections)
         {riak_core_connection_mgr, {riak_core_connection_mgr, start_link, []},
          permanent, 5000, worker, [riak_core_connection_mgr]},
         %% Cluster Client Connection Supervisor
         {riak_core_cluster_conn_sup, {riak_core_cluster_conn_sup, start_link, []},
          permanent, infinity, supervisor, [riak_core_cluster_conn_sup]},
         %% Cluster Manager
         {riak_repl_cluster_mgr, {riak_core_cluster_mgr, start_link, ClusterMgrDefaults},
          permanent, 5000, worker, [riak_core_cluster_mgr]},
         {riak_core_tcp_mon, {riak_core_tcp_mon, start_link, []},
          permanent, 5000, worker, [riak_core_tcp_mon]}
        ],
    {ok, {{rest_for_one, 9, 10}, Processes}}.
