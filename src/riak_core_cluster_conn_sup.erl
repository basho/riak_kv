%% Riak Core Cluster Manager Connection Supervisor
%% Copyright 2007-2012 Basho Technologies, Inc. All Rights Reserved.
%%
%% Mission: ensure connections from a cluster manager to other clusters
%% in order to resolve ip addresses into known clusters or to refresh
%% the list of remote cluster members and observe their status.

-module(riak_core_cluster_conn_sup).
-behaviour(supervisor).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([start_link/0,
         add_remote_connection/1, remove_remote_connection/1,
         connections/0, is_connected/1
        ]).
-export([init/1]).

-define(SHUTDOWN, 5000). % how long to give cluster_conn processes to shutdown

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% a remote connection if we don't already have one
add_remote_connection(Remote) ->
    case is_connected(Remote) of
        false ->
            lager:info("Connecting to remote cluster: ~p", [Remote]),
            ChildSpec = make_remote(Remote),
            supervisor:start_child(?MODULE, ChildSpec);
        _ ->
            lager:debug("Already connected to remote cluster: ~p", [Remote]),
            ok
    end.

remove_remote_connection(Remote) ->
    lager:debug("Disconnecting from remote cluster at: ~p", [Remote]),
    %% remove supervised cluster connection
    _ = supervisor:terminate_child(?MODULE, Remote),
    _ = supervisor:delete_child(?MODULE, Remote),
    %% This seems hacky, but someone has to tell the connection manager to stop
    %% trying to reach this target if it hasn't connected yet. It's the supervised
    %% cluster connection that requests the connection, but it's going to die, so
    %% it can't un-connect itself.
    riak_core_connection_mgr:disconnect(Remote).

connections() ->
    [{Remote, Pid} || {Remote, Pid, _, _} <- supervisor:which_children(?MODULE), is_pid(Pid)].

is_connected(Remote) ->
    Connections = connections(),
    lists:any(fun({R,_Pid}) -> R == Remote end, Connections).
    %not ([] == lists:filter(fun({R,_Pid}) -> R == Remote end, connections())).

%% @private
init([]) ->
    %% %% TODO: remote list of test addresses.
    %% Remotes = initial clusters or ip addrs from ring
    %% Children = [make_remote(Remote) || Remote <- Remotes],
    Children = [],
    {ok, {{one_for_one, 10, 10}, Children}}.

make_remote(Remote) ->
    {Remote, {riak_core_cluster_conn, start_link, [Remote]},
        transient, ?SHUTDOWN, worker, [riak_core_cluster_conn]}.
