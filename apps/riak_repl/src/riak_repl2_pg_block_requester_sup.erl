%% Riak EnterpriseDS
%% Copyright 2007-2013 Basho Technologies, Inc. All Rights Reserved.
-module(riak_repl2_pg_block_requester_sup).
-behaviour(supervisor).
-export([start_link/0, start_child/4, started/0, started/1]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_child(Socket, Transport, Proto, Props) ->
    supervisor:start_child(?MODULE, [Socket, Transport, Proto, Props]).

started() ->
    [Pid || {_, Pid, _, _} <- supervisor:which_children(?MODULE)].

started(Node) ->
    [{Remote, Pid} || {Remote, Pid, _, _} <-
                      supervisor:which_children({?MODULE, Node}), is_pid(Pid)].

%% @private
init([]) ->
    ChildSpec = {riak_repl2_pg_block_requester, {riak_repl2_pg_block_requester, start_link, []},
    temporary, 5000, worker, [riak_repl2_pg_block_requester]},
    {ok, {{simple_one_for_one, 10, 10}, [ChildSpec]}}.

