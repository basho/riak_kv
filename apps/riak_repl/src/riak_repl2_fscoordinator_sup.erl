%% @doc simple supervisor for the fscoordinator, starting/stopping it
%% in response to leader changes.
-module(riak_repl2_fscoordinator_sup).
-export([start_link/0, set_leader/2, start_coord/2, stop_coord/2,
    started/0, started/1, coord_for_cluster/1]).
-export([init/1]).

-define(SHUTDOWN, 5000).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

set_leader(Node, _Pid) ->
    case node() of
        Node ->
            {ok, Ring} = riak_core_ring_manager:get_my_ring(),
            Fullsyncs = riak_repl_ring:fs_enabled(Ring),
            [start_coord(node(), Remote) || Remote <- Fullsyncs];
        _ ->
            [stop_coord(node(), Remote) || Remote <- started()]
    end.

start_coord(Node, Remote) ->
    lager:info("Starting replication coordination ~p", [Remote]),
    Childspec = make_remote(Remote),
    supervisor:start_child({?MODULE, Node}, Childspec).

stop_coord(Node, Remote) ->
    lager:info("Stopping replication coordination ~p", [Remote]),
    _ = supervisor:terminate_child({?MODULE, Node}, Remote),
    _ = supervisor:delete_child({?MODULE, Node}, Remote).

started() ->
    [{Remote, Pid} || {Remote, Pid, _, _} <-
        supervisor:which_children(?MODULE), is_pid(Pid)].

started(Node) ->
    [{Remote, Pid} || {Remote, Pid, _, _} <-
        supervisor:which_children({?MODULE, Node}), is_pid(Pid)].

coord_for_cluster(Cluster) ->
    Coords = [{Remote, Pid} || {Remote, Pid, _, _} <- supervisor:which_children(?MODULE),
                      is_pid(Pid), Remote == Cluster],
    case Coords of
        [] -> undefined;
        [{_,CoordPid}|_] -> CoordPid
    end.

%% @private
init(_) ->
    {ok, {{one_for_one, 10, 5}, []}}.

make_remote(Remote) ->
    {Remote, {riak_repl2_fscoordinator, start_link, [Remote]},
        transient, ?SHUTDOWN, worker, [riak_repl2, fscoordinator]}.
