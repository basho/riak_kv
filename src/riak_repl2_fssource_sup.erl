%% Riak EnterpriseDS
%% Copyright 2007-2012 Basho Technologies, Inc. All Rights Reserved.
-module(riak_repl2_fssource_sup).
-behaviour(supervisor).
-export([start_link/0, enable/3, disable/2, enabled/0, enabled/1]).
-export([init/1]).

-define(SHUTDOWN, 5000). % how long to give rtsource processes to persist queue/shutdown

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%%TODO: Rename enable/disable something better - start/stop is a bit overloaded
enable(Node, Partition, IP) ->
    lager:info("Starting replication fullsync source for ~p from ~p to ~p",
        [Partition, Node, IP]),
    ChildSpec = make_childspec(Partition, IP),
    supervisor:start_child({?MODULE, Node}, ChildSpec).

disable(Node, Partition) ->
    lager:info("Stopping replication fullsync source for ~p", [Partition]),
    _ = supervisor:terminate_child({?MODULE, Node}, Partition),
    _ = supervisor:delete_child({?MODULE, Node}, Partition).

enabled() ->
    [{Remote, Pid} || {Remote, Pid, _, _} <-
        supervisor:which_children(?MODULE), is_pid(Pid)].

enabled(Node) ->
    [{Partition, Pid} || {Partition, Pid, _, _} <-
        supervisor:which_children({?MODULE, Node}), is_pid(Pid)].

%% @private
init([]) ->
    {ok, {{one_for_one, 10, 10}, []}}.

make_childspec(Partition, IP) ->
    {Partition, {riak_repl2_fssource, start_link, [Partition, IP]},
        temporary, ?SHUTDOWN, worker, [riak_repl2_fssource]}.
