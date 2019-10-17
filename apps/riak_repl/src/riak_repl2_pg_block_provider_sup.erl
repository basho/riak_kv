%% Riak EnterpriseDS
%% Copyright 2007-2013 Basho Technologies, Inc. All Rights Reserved.
-module(riak_repl2_pg_block_provider_sup).
-behaviour(supervisor).
-export([start_link/0, enable/1, enabled/0, enabled/1, disable/1]).
-export([init/1]).

-define(SHUTDOWN, 5000).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

enable(Remote) ->
    ChildSpec = make_remote(Remote),
    supervisor:start_child(?MODULE, ChildSpec).

disable(Remote) ->
    _ = supervisor:terminate_child(?MODULE, Remote),
    _ = supervisor:delete_child(?MODULE, Remote).

enabled() ->
    [{Remote, Pid} || {Remote, Pid, _, _} <-
                      supervisor:which_children(?MODULE), is_pid(Pid)].

enabled(Node) ->
    [{Remote, Pid} || {Remote, Pid, _, _} <-
                      supervisor:which_children({?MODULE, Node}), is_pid(Pid)].

%% @private
init([]) ->
    riak_repl2_pg:register_remote_locator(),
    {ok, Ring} = riak_core_ring_manager:get_raw_ring(),
    Remotes = riak_repl_ring:pg_enabled(Ring),
    Children = [make_remote(Remote) || Remote <- Remotes],
    {ok, {{one_for_one, 10, 10}, Children}}.

make_remote(Remote) ->
    {Remote, {riak_repl2_pg_block_provider, start_link, [Remote]},
        transient, ?SHUTDOWN, worker, [riak_repl2_pg_block_provider]}.

