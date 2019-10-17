-module(riak_repl2_fscoordinator_serv_sup).
-behavior(supervisor).

-export([init/1]).

-export([start_link/0, start_child/4, started/0, started/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_child(Socket, Transport, Proto, Props) ->
    supervisor:start_child(?MODULE, [Socket, Transport, Proto, Props]).

init(_) ->
    ChildSpec = {id, {riak_repl2_fscoordinator_serv, start_link, []},
        temporary, brutal_kill, worker, [riak_repl2_fscoordinator_serv]},
    {ok, {{simple_one_for_one, 10, 10}, [ChildSpec]}}.

started() ->
    [{Remote, Pid} || {Remote, Pid, _, _} <-
        supervisor:which_children(?MODULE), is_pid(Pid)].

started(Node) ->
    [{Remote, Pid} || {Remote, Pid, _, _} <-
        supervisor:which_children({?MODULE, Node}), is_pid(Pid)].

