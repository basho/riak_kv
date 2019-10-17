%% Riak EnterpriseDS
%% Copyright 2007-2012 Basho Technologies, Inc. All Rights Reserved.
-module(riak_repl2_fssink_sup).
-behaviour(supervisor).
-export([start_link/0, start_child/4, started/0]).
-export([init/1]).

-define(SHUTDOWN, 5000). % how long to give rtsource processes to persist queue/shutdown

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_child(Socket, Transport, Proto, Props) ->
    supervisor:start_child(?MODULE, [Socket, Transport, Proto, Props]).

started() ->
    [Pid || {_, Pid, _, _} <- supervisor:which_children(?MODULE)].

%% @private
init([]) ->
    ChildSpec = {undefined, {riak_repl2_fssink, start_link, []},
                 temporary, 5000, worker, [riak_repl2_fssink]},
    {ok, {{simple_one_for_one, 10, 10}, [ChildSpec]}}.

