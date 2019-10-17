%% Fullsync pool to be shared by all sinks.  Globally bounded in size in case multiple
%% fullsyncs are running.

-module(riak_repl2_fssink_pool).
-export([start_link/0, status/0, bin_put/1]).

start_link() ->
    MinPool = app_helper:get_env(riak_repl, fssink_min_workers, 5),
    MaxPool = app_helper:get_env(riak_repl, fssink_max_workers, 100),
    PoolArgs = [{name, {local, ?MODULE}},
                {worker_module, riak_repl_fullsync_worker},
                {worker_args, []},
                {size, MinPool}, {max_overflow, MaxPool}],
    poolboy:start_link(PoolArgs).

%% @doc Return the poolboy status
status() ->
    {StateName, WorkerQueueLen, Overflow, NumMonitors} = poolboy:status(?MODULE),
    [{statename, StateName},
     {worker_queue_len, WorkerQueueLen},
     {overflow, Overflow},
     {num_monitors, NumMonitors}].

%% @doc Send a replication wire-encoded binary to the worker pool
%% for running a put against.  No guarantees of completion.
bin_put(BinObj) ->
    Pid = poolboy:checkout(?MODULE, true, infinity),
    riak_repl_fullsync_worker:do_binput(Pid, BinObj, ?MODULE).
