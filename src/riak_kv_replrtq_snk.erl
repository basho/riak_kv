%% -------------------------------------------------------------------
%%
%% riak_kv_replrtq_snk: coordination of full-sync replication
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

%% @doc coordination of full-sync replication

-module(riak_kv_replrtq_snk).
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-behaviour(gen_server).

-export([init/1,
        handle_call/3,
        handle_cast/2,
        handle_info/2,
        terminate/2,
        code_change/3]).

-export([start_link/0,
            prompt_work/0,
            done_work/3,
            requeue_work/1,
            suspend_snkqueue/1,
            resume_snkqueue/1,
            remove_snkqueue/1,
            set_workercount/2,
            set_workercount/3,
            add_snkqueue/3,
            add_snkqueue/4]).

-export([repl_fetcher/1]).

-define(LOG_TIMER_SECONDS, 60).
-define(ZERO_STATS,
        {{success, 0},
            {failure, 0},
            {replfetch_time, 0},
            {replpush_time, 0},
            {replmod_time, 0},
            {modified_time, 0, 0, 0, 0, 0}}).
-define(STARTING_DELAYMS, 8).
-define(MAX_SUCCESS_DELAYMS, 1024).
-define(ON_ERROR_DELAYMS, 65536).
-define(INACTIVITY_TIMEOUT_MS, 60000).
-define(DEFAULT_WORKERCOUNT, 1).

-record(sink_work, {queue_name :: queue_name(),
                        %% The name of the remote queue to be fetched from
                    work_queue = [] :: list(work_item()),
                    minimum_queue_length = 0 :: non_neg_integer(),
                        %% The amount of work which must not be in progress
                        %% in order to maintain the constraints around
                        %% concurrent sink workers
                    deferred_queue_length = 0 :: non_neg_integer(),
                        %% The volume of work awaiting requeue
                    peer_list = [] :: list(peer_info()),
                    max_worker_count = 1 :: pos_integer(),
                        %% The target number of sink workers
                    queue_stats = ?ZERO_STATS :: queue_stats(),
                    suspended = false :: boolean()}).


-record(state, {work = [] :: list(sink_work()),
                iteration = 1 :: iteration(),
                    %% The iteration number increments with each operator
                    %% triggered change to the running configuration.  This
                    %% is the iteration number when this configuration was
                    %% setup.  Suspensions/resumes do not bump the
                    %% iteration
                enabled = false :: boolean()}).

-type queue_name() :: atom().
-type peer_id() :: pos_integer().
-type iteration() :: pos_integer().
-type remote_fun() ::
    fun((queue_name()) ->
        {ok, queue_empty}|{ok, {deleted, any(), binary()}}|{ok, binary()}).
-type renew_fun() :: fun(() -> any()).


-type work_item() ::
    {{queue_name(), iteration(), peer_id()},
        riak_client:riak_client(),
        remote_fun(), renew_fun()}.
    % Identifier for the work item  - and the local and remote clients to use

-type sink_work() ::
    {queue_name(), iteration(), #sink_work{}}.
    % Mapping between queue names and the work records

-type peer_info() ::
    {peer_id(), non_neg_integer(), string(), pos_integer(), http|pb}.
    % {Identifier for Peer,
    %   Next delay to use on work_item in ms,
    %   Peer IP,
    %   Peer Listener,
    %   Peer protocol}

-type queue_stats() ::
    {{success, non_neg_integer()}, {failure, non_neg_integer()},
        {replfetch_time, non_neg_integer()},
        {replpush_time, non_neg_integer()},
        {replmod_time, non_neg_integer()},
        {modified_time,
            non_neg_integer(),
            non_neg_integer(),
            non_neg_integer(),
            non_neg_integer(),
            non_neg_integer()}}.
    % {Successes, Failures,
    % Total Repl Completion Time,
    % Modified time by bucket - second, minute, hour, day, longer}

-type reply_tuple() ::
    {queue_empty, non_neg_integer()} |
        {tomb, non_neg_integer(), non_neg_integer(), non_neg_integer()} |
        {object, non_neg_integer(), non_neg_integer(), non_neg_integer()} |
        {error, any(), any()}.

%%%============================================================================
%%% API
%%%============================================================================

-spec start_link() -> {ok, pid()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc
%% Manually prompt for any replication work that is queued to be distributed to
%% available workers.  This should not be necessary in normal operations.
-spec prompt_work() -> ok.
prompt_work() ->
    gen_server:cast(?MODULE, prompt_work).

%% @doc
%% Allows workers to notify the snk that work has been completed, so that the
%% WorkItem can be requeued for another worker (potentially delayed if this
%% work had failed or revealed an empty queue).  Expected to be used only by
%% the spawned snk workers.
-spec done_work(work_item(), boolean(), reply_tuple()) -> ok.
done_work(WorkItem, Success, ReplyTuple) ->
    gen_server:cast(?MODULE, {done_work, WorkItem, Success, ReplyTuple}).

%% @doc
%% Allows a work item to be re-queued.  Used internally.
-spec requeue_work(work_item()) -> ok.
requeue_work(WorkItem) ->
    gen_server:cast(?MODULE, {requeue_work, WorkItem}).

%% @doc
%% Suspend the activity asociated with a given queue (for all peers)
-spec suspend_snkqueue(queue_name()) -> ok|not_found.
suspend_snkqueue(QueueName) ->
    gen_server:call(?MODULE, {suspend, QueueName}).

%% @doc
%% Resume the activity asociated with a given queue (for all peers)
-spec resume_snkqueue(queue_name()) -> ok|not_found.
resume_snkqueue(QueueName) ->
    gen_server:call(?MODULE, {resume, QueueName}).

%% @doc
%% Remove temporarily from this process the configuration for a given queue
%% name.  If the configuration remains in riak.conf, it will be re-introduced
%% on restart
-spec remove_snkqueue(queue_name()) -> ok.
remove_snkqueue(QueueName) ->
    gen_server:call(?MODULE, {remove, QueueName}).

%% @doc
%% Add temporarily to this process a configuration to reach a given queue via
%% a passed-in list of peers, using a given count of workers.
%% This added-in configuration will not be preserved between process restarts.
-spec add_snkqueue(queue_name(), list(peer_info()), pos_integer()) -> ok.
add_snkqueue(QueueName, Peers, WorkerCount) ->
    add_snkqueue(QueueName, Peers, WorkerCount, WorkerCount).

%% @doc
%% Add a queue restricting the number of workers per peer, as well as the
%% number of workers overall
-spec add_snkqueue(queue_name(), list(peer_info()),
                    pos_integer(), pos_integer()) -> ok.
add_snkqueue(QueueName, Peers, WorkerCount, PerPeerLimit) 
                                            when PerPeerLimit =< WorkerCount ->
    gen_server:call(?MODULE,
                    {add, QueueName, Peers, WorkerCount, PerPeerLimit}).



%% @doc
%% Change the number of concurrent workers supporting a given queue.  Changing
%% the count is equivalent to starting a new set of workers, and waiting for
%% the old set of workers to expire once they have completed any outstanding
%% work.  So for an initial period there may be more concurrent work ongoing
%% until all in-flight work is finished.
-spec set_workercount(queue_name(), pos_integer()) -> ok|not_found.
set_workercount(QueueName, WorkerCount) ->
    set_workercount(QueueName, WorkerCount, WorkerCount).

%% @doc
%% Change the number of concurrent workers whilst limiting the number of
%% workers per peer
-spec set_workercount(queue_name(), pos_integer(), pos_integer())
                                                            -> ok|not_found.
set_workercount(QueueName, WorkerCount, PerPeerLimit)
                                            when PerPeerLimit =< WorkerCount ->
    gen_server:call(?MODULE,
                    {worker_count, QueueName, WorkerCount, PerPeerLimit}).

%%%============================================================================
%%% gen_server callbacks
%%%============================================================================

init([]) ->
    SinkEnabled = app_helper:get_env(riak_kv, replrtq_enablesink, false),
    case SinkEnabled of
        true ->
            SinkPeers = app_helper:get_env(riak_kv, replrtq_sinkpeers, ""),
            DefaultQueue = app_helper:get_env(riak_kv, replrtq_sinkqueue),
            SnkQueuePeerInfo = tokenise_peers(DefaultQueue, SinkPeers),
            SnkWorkerCount =
                app_helper:get_env(riak_kv,
                                    replrtq_sinkworkers,
                                    ?DEFAULT_WORKERCOUNT),
            PerPeerLimit =
                app_helper:get_env(riak_kv,
                                        replrtq_sinkpeerlimit,
                                        SnkWorkerCount),
            Iteration = 1,
            MapPeerInfoFun =
                fun({SnkQueueName, SnkPeerInfo}) ->
                    {SnkQueueLength, SnkWorkQueue} =
                        determine_workitems(SnkQueueName,
                                            Iteration,
                                            SnkPeerInfo,
                                            SnkWorkerCount,
                                            min(SnkWorkerCount, PerPeerLimit)),
                    SnkW =
                        #sink_work{queue_name = SnkQueueName,
                                    work_queue = SnkWorkQueue,
                                    minimum_queue_length = SnkQueueLength,
                                    peer_list = SnkPeerInfo,
                                    max_worker_count = SnkWorkerCount},
                    {SnkQueueName, Iteration, SnkW}
                end,
            Work = lists:map(MapPeerInfoFun, SnkQueuePeerInfo),
            {ok, #state{enabled = true, work = Work}, ?INACTIVITY_TIMEOUT_MS};
        false ->
            {ok, #state{}}
    end.

handle_call({suspend, QueueN}, _From, State) ->
    case lists:keyfind(QueueN, 1, State#state.work) of
        false ->
            {reply, not_found, State};
        {QueueN, I, SinkWork} ->
            SW0 = SinkWork#sink_work{suspended = true},
            W0 = lists:keyreplace(QueueN, 1, State#state.work, {QueueN, I, SW0}),
            {reply, ok, State#state{work = W0}}
    end;
handle_call({resume, QueueN}, _From, State) ->
    case lists:keyfind(QueueN, 1, State#state.work) of
        false ->
            {reply, not_found, State};
        {QueueN, I, SinkWork} ->
            SW0 = SinkWork#sink_work{suspended = false},
            W0 = lists:keyreplace(QueueN, 1, State#state.work, {QueueN, I, SW0}),
            prompt_work(),
            {reply, ok, State#state{work = W0}}
    end;
handle_call({remove, QueueN}, _From, State) ->
    W0 = lists:keydelete(QueueN, 1, State#state.work),
    Iteration = State#state.iteration + 1,
    case W0 of
        [] ->
            {reply,
                ok,
                State#state{work = W0, iteration = Iteration, enabled = false}};
        _ ->
            {reply,
                ok,
                State#state{work = W0, iteration = Iteration, enabled = true}}
    end;
handle_call({add, QueueN, Peers, WorkerCount, PerPeerLimit}, _From, State) ->
    Iteration = State#state.iteration + 1,
    {QueueLength, WorkQueue} =
        determine_workitems(QueueN,
                            Iteration,
                            Peers,
                            WorkerCount,
                            min(WorkerCount, PerPeerLimit)),
    SnkW =
        #sink_work{queue_name = QueueN,
                    work_queue = WorkQueue,
                    minimum_queue_length = QueueLength,
                    peer_list = Peers,
                    max_worker_count = WorkerCount},
    W0 =
        lists:keystore(QueueN, 1, State#state.work, {QueueN, Iteration, SnkW}),
    prompt_work(),
    {reply, ok, State#state{work = W0, iteration = Iteration, enabled = true}};
handle_call({worker_count, QueueN, WorkerCount, PerPeerLimit}, _From, State) ->
    case lists:keyfind(QueueN, 1, State#state.work) of
        false ->
            {reply, not_found, State};
        {QueueN, _I, SinkWork} ->
            Iteration = State#state.iteration + 1,
            {QueueLength, WorkQueue} =
                determine_workitems(QueueN,
                                    Iteration,
                                    SinkWork#sink_work.peer_list,
                                    WorkerCount,
                                    min(WorkerCount, PerPeerLimit)),
            SinkWork0 =
                SinkWork#sink_work{work_queue = WorkQueue,
                                    minimum_queue_length = QueueLength,
                                    max_worker_count = WorkerCount},
            W0 =
                lists:keyreplace(QueueN, 1, State#state.work,
                                    {QueueN, Iteration, SinkWork0}),
            prompt_work(),
            {reply, ok, State#state{work = W0, iteration = Iteration}}
    end.


handle_cast(prompt_work, State) ->
    Work0 = lists:map(fun do_work/1, State#state.work),
    {noreply, State#state{work = Work0}};
handle_cast({done_work, WorkItem, Success, ReplyTuple}, State) ->
    {{QueueName, Iteration, PeerID}, _LC, RC, _RNCF} = WorkItem,
    case lists:keyfind(QueueName, 1, State#state.work) of
        {QueueName, Iteration, SinkWork} ->
            QS = SinkWork#sink_work.queue_stats,
            QS0 = increment_queuestats(QS, ReplyTuple),
            PL = SinkWork#sink_work.peer_list,
            {PW0, PL0} = adjust_wait(Success, ReplyTuple, PeerID, PL),
            DQL0 = SinkWork#sink_work.deferred_queue_length + 1,
            UpdSW = SinkWork#sink_work{peer_list = PL0,
                                        queue_stats = QS0,
                                        deferred_queue_length = DQL0},
            UpdWork = lists:keyreplace(QueueName, 1, State#state.work,
                                        {QueueName, Iteration, UpdSW}),
            case PW0 of
                0 ->
                    requeue_work(WorkItem);
                W ->
                    prompt_work(),
                    erlang:send_after(rand:uniform(W),
                                        self(),
                                        {prompt_requeue, WorkItem})
            end,
            {noreply, State#state{work = UpdWork}};
        _ ->
            RC(close),
            % Work profile has changed since this work was prompted
            {noreply, State}
    end;
handle_cast({requeue_work, WorkItem}, State) ->
    {{QueueName, Iteration, _PeerID}, _LC, RC, _RNCF} = WorkItem,
    case lists:keyfind(QueueName, 1, State#state.work) of
        {QueueName, Iteration, SinkWork} ->
            UpdWorkQueue = [WorkItem|SinkWork#sink_work.work_queue],
            DQL = SinkWork#sink_work.deferred_queue_length - 1,
            UpdSW = SinkWork#sink_work{work_queue = UpdWorkQueue,
                                        deferred_queue_length = DQL},
            UpdWork = lists:keyreplace(QueueName, 1, State#state.work,
                                        {QueueName, Iteration, UpdSW}),
            prompt_work(),
            {noreply, State#state{work = UpdWork}};
        _ ->
            RC(close),
            % Work profile has changed since this work was requeued
            {noreply, State}
    end.

handle_info(timeout, State) ->
    prompt_work(),
    erlang:send_after(?LOG_TIMER_SECONDS * 1000, self(), log_stats),
    {noreply, State};
handle_info(log_stats, State) ->
    erlang:send_after(?LOG_TIMER_SECONDS * 1000, self(), log_stats),
    SinkWork0 =
        case State#state.enabled of
            true ->
                lists:map(fun log_mapfun/1, State#state.work);
            false ->
                State#state.work
        end,
    {noreply, State#state{work = SinkWork0}};
handle_info({prompt_requeue, WorkItem}, State) ->
    requeue_work(WorkItem),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%============================================================================
%%% Internal functions
%%%============================================================================

%% @doc calculate the mean avoiding divide by 0
-spec calc_mean(non_neg_integer(), non_neg_integer()) -> string().
calc_mean(_, 0) ->
    "no_result";
calc_mean(Time, Count) ->
    lists:flatten(io_lib:format("~.3f",[(Time / Count) / 1000])).

%% @doc convert the tokenised string of peers from the configuration into actual
%% usable peer information.
%% tokenised string expected to be of form:
%% "192.168.10.1:8098:http|192.168.10.2:8098:http etc"
%% Optionally the tokenised string may include a queue name, if more than the
%% default queue name is to be used.  The queue name should be a prefix e.g.:
%% "q1_ttaaefs:192.168.10.1:8097:pb|passive:192.168.10.2:8097:pb etc"
-spec tokenise_peers(queue_name(), string()) -> list({queue_name(), peer_info()}).
tokenise_peers(DefaultQueue, PeersString) ->
    PeerL0 = string:tokens(PeersString, "|"),
    SplitHostPortFun =
        fun(PeerString, Acc) ->
            {QueueName, Host, Port, Protocol} =
                case string:tokens(PeerString, ":") of
                    [H, P, ProtStr] ->
                        format_peer(DefaultQueue, H, P, ProtStr);
                    [QN, H, P, ProtStr] ->
                        format_peer(list_to_atom(QN), H, P, ProtStr)
                end,
            case lists:keytake(QueueName, 1, Acc) of
                {value, {QueueName, PeerList}, Acc0} ->
                    Peer =
                        {length(PeerList) + 1,
                            ?STARTING_DELAYMS,
                            Host, Port, Protocol},
                    lists:ukeysort(1, [{QueueName, PeerList ++ [Peer]}|Acc0]);
                false ->
                    Peer =
                        {1,
                            ?STARTING_DELAYMS,
                            Host, Port, Protocol},
                    lists:ukeysort(1, [{QueueName, [Peer]}|Acc])
            end
        end,
    lists:foldl(SplitHostPortFun, [], PeerL0).

format_peer(QN, H, P, "http") ->
    {QN, H, list_to_integer(P), http};
format_peer(QN, H, P, "pb") ->
    {QN, H, list_to_integer(P), pb}.


%% @doc
%% Calculates the queue of work items and the minimum length of queue to be
%% kept by the process, in order to deliver the desired number of concurrent
%% worker processes.
%% No one peer may occupy more than per-peer limit of workers
-spec determine_workitems(queue_name(), iteration(), list(peer_info()),
                            pos_integer(), pos_integer())
                                    -> {non_neg_integer(), list(work_item())}.
determine_workitems(QueueName, Iteration, PeerInfo, WorkerCount, PerPeerLimit)
                                            when PerPeerLimit =< WorkerCount ->
    ClientList =
        lists:flatten(lists:duplicate(PerPeerLimit, PeerInfo)),
    WorkItems =
        lists:map(fun map_peer_to_wi_fun/1,
                    lists:map(fun(PI) ->
                                    {QueueName, Iteration, PI}
                                end,
                                ClientList)),
    {length(WorkItems) - WorkerCount, WorkItems}.


-spec map_peer_to_wi_fun({queue_name(), iteration(), peer_info()}) -> work_item().
map_peer_to_wi_fun({QueueName, Iteration, PeerInfo}) ->
    {PeerID, _Delay, Host, Port, Protocol} = PeerInfo,
    LocalClient = riak_client:new(node(), undefined),
    GenClientFun = 
        case Protocol of
            http ->
                InitClientFun =
                    fun() -> rhc:create(Host, Port, "riak", []) end,
                fun() ->
                    HTC = InitClientFun(),
                    fun(Request) ->
                        case Request of
                            {consume, QN} ->
                                rhc:fetch(HTC, QN);
                            _ ->
                                ok
                        end
                    end
                end;
            pb ->
                CaCertificateFilename =
                    app_helper:get_env(riak_kv, repl_cacert_filename),
                CertfiicateFilename =
                    app_helper:get_env(riak_kv, repl_cert_filename),
                KeyFilename =
                    app_helper:get_env(riak_kv, repl_key_filename),
                SecuritySitename = 
                    app_helper:get_env(riak_kv, repl_username),
                Opts = 
                    case CaCertificateFilename of
                        undefined ->
                            [{silence_terminate_crash, true}];
                        CaCert ->
                            [{silence_terminate_crash, true},
                                {credentials, SecuritySitename, ""},
                                {cacertfile, CaCert},
                                {certfile, CertfiicateFilename},
                                {keyfile, KeyFilename}]
                    end,
                InitClientFun =
                    fun() ->
                        case riakc_pb_socket:start(Host, Port, Opts) of
                            {ok, PBpid} ->
                                PBpid;
                            _ ->
                                lager:info("No client initialised -"
                                                ++ " not reachable ~s ~w",
                                            [Host, Port]),
                                no_pid
                        end
                    end,
                fun() ->
                    PBC = InitClientFun(),
                    fun(Request) ->
                        case Request of
                            {consume, QN} ->
                                case check_pbc_client(PBC) of
                                    true ->
                                        QNB = atom_to_binary(QN, utf8),
                                        riakc_pb_socket:fetch(PBC, QNB);
                                    _ ->
                                        {error, no_client}
                                end;
                            close ->
                                close_pbc_client(PBC)
                        end
                    end
                end
        end,
    {{QueueName, Iteration, PeerID},
        LocalClient, GenClientFun(), GenClientFun}.

check_pbc_client(no_pid) ->
    false;
check_pbc_client(PBC) ->
    is_process_alive(PBC).

close_pbc_client(no_pid) ->
    ok;
close_pbc_client(PBC) ->
    case is_process_alive(PBC) of
        true ->
            riakc_pb_socket:stop(PBC);
        false ->
            ok
    end.

%% @doc
%% For an item of work which has been removed from the work queue, spawn a
%% snk worker (using the repl_fetcher fun) to manage that item of work.  The
%% worker must ensure the wortk_item is delivered back on completion.
-spec do_work(sink_work()) -> sink_work().
do_work({QueueName, Iteration, SinkWork}) ->
    WorkQueue = SinkWork#sink_work.work_queue,
    MinQL = (SinkWork#sink_work.minimum_queue_length -
                SinkWork#sink_work.deferred_queue_length),
    IsSuspended = SinkWork#sink_work.suspended,
    case IsSuspended of
        true ->
            {QueueName, Iteration, SinkWork};
        false ->
            case length(WorkQueue) - MinQL of
                0 ->
                    {QueueName, Iteration, SinkWork};
                _ ->
                    {Rem, Work} = lists:split(lists:max([0, MinQL]), WorkQueue),
                    lists:foreach(fun work/1, Work),
                    {QueueName,
                        Iteration,
                        SinkWork#sink_work{work_queue = Rem}}
            end
    end.

-spec work(work_item()) -> ok.
work(WorkItem) ->
    %% possibly we need a spawn link here to survive a restart
    %% But we then we need a TRAPEXIT in the server to be immune.
    _P = spawn(?MODULE, repl_fetcher, [WorkItem]),
    ok.

%% Should always under all circumstances end with calling done_work
-spec repl_fetcher(work_item()) -> ok.
repl_fetcher(WorkItem) ->
    SW = os:timestamp(),
    {{QueueName, _Iter, Peer}, LocalClient, RemoteFun, RenewClientFun}
        = WorkItem,
    try
        case RemoteFun({consume, QueueName}) of
            {ok, queue_empty} ->
                SW0 = os:timestamp(),
                EmptyFetchSplit = timer:now_diff(SW0, SW),
                ok = riak_kv_stat:update(ngrrepl_empty),
                done_work(WorkItem, true,
                            {queue_empty, EmptyFetchSplit});
            {ok, {deleted, _TC, RObj}} ->
                SWFetched = os:timestamp(),
                {ok, LMD} = riak_client:push(RObj, true, [], LocalClient),
                SWPushed = os:timestamp(),
                ModSplit = timer:now_diff(SWPushed, LMD),
                FetchSplit = timer:now_diff(SWFetched, SW),
                PushSplit = timer:now_diff(SWPushed, SWFetched),
                ok = riak_kv_stat:update(ngrrepl_object),
                done_work(WorkItem, true,
                            {tomb, FetchSplit, PushSplit, ModSplit});
            {ok, RObj} ->
                SWFetched = os:timestamp(),
                {ok, LMD} = riak_client:push(RObj, false, [], LocalClient),
                SWPushed = os:timestamp(),
                ModSplit = timer:now_diff(SWPushed, LMD),
                FetchSplit = timer:now_diff(SWFetched, SW),
                PushSplit = timer:now_diff(SWPushed, SWFetched),
                ok = riak_kv_stat:update(ngrrepl_object),
                done_work(WorkItem, true,
                            {object, FetchSplit, PushSplit, ModSplit});
            {error, no_client} ->
                RemoteFun(close),
                UpdWorkItem = setelement(3, WorkItem, RenewClientFun()),
                ok = riak_kv_stat:update(ngrrepl_error),
                done_work(UpdWorkItem, false, {error, error, no_client});
            {error, {conn_failed, {error, econnrefused}}} ->
                lager:info("Snk worker connection refused to peer ~w", [Peer]),
                RemoteFun(close),
                UpdWorkItem = setelement(3, WorkItem, RenewClientFun()),
                ok = riak_kv_stat:update(ngrrepl_error),
                done_work(UpdWorkItem, false, {error, error, econnrefused});
            {error, Bin} when is_binary(Bin) ->
                lager:warning("Snk worker for peer ~w " ++
                                    "failed due to remote exception ~p",
                                [Peer, binary_to_list(Bin)]),
                RemoteFun(close),
                UpdWorkItem = setelement(3, WorkItem, RenewClientFun()),
                ok = riak_kv_stat:update(ngrrepl_error),
                done_work(UpdWorkItem, false, {error, error, remote_error})
        end
    catch
        Type:Exception ->
            lager:warning("Snk worker failed at Peer ~w due to ~w error ~w",
                            [Peer, Type, Exception]),
            RemoteFun(close),
            UpdWorkItem0 = setelement(3, WorkItem, RenewClientFun()),
            ok = riak_kv_stat:update(ngrrepl_error),
            done_work(UpdWorkItem0, false, {error, Type, Exception})
    end.

%% @doc
%% Keep some stats on the queues, to be logged out periodically
-spec increment_queuestats(queue_stats(), reply_tuple()) -> queue_stats().
increment_queuestats(QueueStats, ReplyTuple) ->
    case ReplyTuple of
        {tomb, FetchSplit, PushSplit, ModSplit} ->
            add_modtime(add_repltime(add_success(QueueStats),
                            {FetchSplit, PushSplit, ModSplit}),
                        ModSplit);
        {object, FetchSplit, PushSplit, ModSplit} ->
            add_modtime(add_repltime(add_success(QueueStats),
                            {FetchSplit, PushSplit, ModSplit}),
                        ModSplit);
        {queue_empty, _TS} ->
            QueueStats;
        _ ->
            add_failure(QueueStats)
    end.

mod_split_element(ModSplit) when ModSplit < 1000 ->
    1;
mod_split_element(ModSplit) when ModSplit < 60000 ->
    2;
mod_split_element(ModSplit) when ModSplit < 3600000 ->
    3;
mod_split_element(ModSplit) when ModSplit < 86400000 ->
    4;
mod_split_element(_) ->
    5.

-spec add_success(queue_stats()) -> queue_stats().
add_success({{success, Success}, F, FT, PT, RT, MT}) ->
    {{success, Success + 1}, F, FT, PT, RT, MT}.

-spec add_failure(queue_stats()) -> queue_stats().
add_failure({S, {failure, Failure}, FT, PT, RT, MT}) ->
    {S, {failure, Failure + 1}, FT, PT, RT, MT}.

-spec add_repltime(queue_stats(),
                    {integer(), integer(), integer()}) -> queue_stats().
add_repltime({S, 
                F,
                {replfetch_time, FT}, {replpush_time, PT}, {replmod_time, RT},
                MT},
            {FT0, PT0, RT0}) ->
    {S, F,
        {replfetch_time, FT + FT0},
        {replpush_time, PT + PT0},
        {replmod_time, RT + RT0},
        MT}.

-spec add_modtime(queue_stats(), integer()) -> queue_stats().
add_modtime({S, F, FT, PT, RT, MT}, ModTime) ->
    E = mod_split_element(ModTime div 1000) +  1,
    C = element(E, MT),
    {S, F, FT, PT, RT, setelement(E, MT, C + 1)}.

%% @doc
%% Depending on the result of the request, adjust the wait time before this
%% work item is due to be re-processed.  If the queue is commonly empty, then
%% back of the workload exponentially, but if it is consistently yielding
%% results from the queue - the reduce the wait time exponentially tending to
%% 0.
%% On an error, the wait time should leap to avoid all workers being locked
%% attempting to communicate with a peer to which requests are timing out.
-spec adjust_wait(boolean(), reply_tuple(), peer_id(), list(peer_info()))
                                    -> {non_neg_integer(), list(peer_info())}.
adjust_wait(true, {queue_empty, _T}, PeerID, PeerList) ->
    Peer = lists:keyfind(PeerID, 1, PeerList),
    Delay0 = increment_delay(element(2, Peer)),
    PeerList0 =
        lists:keyreplace(PeerID, 1, PeerList, setelement(2, Peer, Delay0)),
    {Delay0, PeerList0};
adjust_wait(true, _, PeerID, PeerList) ->
    Peer = lists:keyfind(PeerID, 1, PeerList),
    Delay0 = element(2, Peer) bsr 1,
    PeerList0 =
        lists:keyreplace(PeerID, 1, PeerList, setelement(2, Peer, Delay0)),
    {Delay0, PeerList0};
adjust_wait(false, _, PeerID, PeerList) ->
    Peer = lists:keyfind(PeerID, 1, PeerList),
    Delay0 = ?ON_ERROR_DELAYMS,
    PeerList0 =
        lists:keyreplace(PeerID, 1, PeerList, setelement(2, Peer, Delay0)),
    {Delay0, PeerList0}.

-spec increment_delay(non_neg_integer()) -> non_neg_integer().
increment_delay(0) ->
    1;
increment_delay(N) ->
    min(N bsl 1, ?MAX_SUCCESS_DELAYMS).


%% @doc
%% Log details of the replication counts and times, and also the current delay
%% to requeue work items for each peer (consistently low delay may indicate
%% the need to configure more workers.
%% At runtime changes to the number of workers can be managed via the
%% remove_snkqueue/1 and add_snkqueue/3 api.
-spec log_mapfun(sink_work()) -> sink_work().
log_mapfun({QueueName, Iteration, SinkWork}) ->
    {{success, SC}, {failure, EC},
        {replfetch_time, FT},
        {replpush_time, PT},
        {replmod_time, RT},
        {modified_time, MTS, MTM, MTH, MTD, MTL}}
        = SinkWork#sink_work.queue_stats,
    lager:info("Queue=~w success_count=~w error_count=~w" ++
                " mean_fetchtime_ms=~s" ++
                " mean_pushtime_ms=~s" ++
                " mean_repltime_ms=~s" ++
                " lmdin_s=~w lmdin_m=~w lmdin_h=~w lmdin_d=~w lmd_over=~w",
                [QueueName, SC, EC,
                    calc_mean(FT, SC), calc_mean(PT, SC), calc_mean(RT, SC),
                    MTS, MTM, MTH, MTD, MTL]),
    FoldPeerInfoFun =
        fun({_PeerID, D, IP, Port, _P}, Acc) ->
            Acc ++ lists:flatten(io_lib:format(" ~s:~w=~w", [IP, Port, D]))
        end,
    PeerDelays =
        lists:foldl(FoldPeerInfoFun, "", SinkWork#sink_work.peer_list),
    lager:info("Queue=~w has peer delays of~s", [QueueName, PeerDelays]),
    {QueueName, Iteration, SinkWork#sink_work{queue_stats = ?ZERO_STATS}}.


%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).

tokenise_test() ->
    String1 =
        "active:127.0.0.1:12008:http|active:127.0.0.1:12009:pb|"
            ++ "active:127.0.0.1:12009:pb",
    Peer1A = {1, ?STARTING_DELAYMS, "127.0.0.1", 12008, http},
    Peer2A = {2, ?STARTING_DELAYMS, "127.0.0.1", 12009, pb},
    Peer3A = {3, ?STARTING_DELAYMS, "127.0.0.1", 12009, pb},
        % references not de-duped, allows certain peers to double-up on worker
        % time
    ?assertMatch([{active, [Peer1A, Peer2A, Peer3A]}],
                    tokenise_peers(q1_ttaaefs, String1)),
    String2 =
        "127.0.0.1:12008:pb|qb:127.0.0.1:12009:pb|qb:127.0.0.1:12009:pb",
    Peer1B = {1, ?STARTING_DELAYMS, "127.0.0.1", 12008, pb},
    Peer2B = {1, ?STARTING_DELAYMS, "127.0.0.1", 12009, pb},
    Peer3B = {2, ?STARTING_DELAYMS, "127.0.0.1", 12009, pb},
        % references not de-duped, allows certain peers to double-up on worker
        % time
    ?assertMatch([{qa, [Peer1B]}, {qb, [Peer2B, Peer3B]}],
                    tokenise_peers(qa, String2)).

determine_workitems_test() ->
    Peer1 = {1, ?STARTING_DELAYMS, "127.0.0.1", 12008, http},
    Peer2 = {2, ?STARTING_DELAYMS, "127.0.0.1", 12009, http},
    Peer3 = {3, ?STARTING_DELAYMS, "127.0.0.1", 12009, http},
    WC1 = 5,
    {MQL1, WIL1} =
        determine_workitems(queue1, 1, [Peer1, Peer2, Peer3], WC1, 3),
    ?assertMatch(4, MQL1), % 3 clients per peer, worker count of 5
    ?assertMatch(9, length(WIL1)),
    WIL1A = lists:map(fun({K, _LC, _RC, _RNCF}) -> K end, WIL1),
    ?assertMatch([{queue1, 1, 1}, {queue1, 1, 2}, {queue1, 1, 3}],
                    lists:sublist(WIL1A, 3)),

    {MQL2, WIL2} =
        determine_workitems(queue1, 2, [Peer1], WC1, WC1),
    ?assertMatch(0, MQL2),
    ?assertMatch(5, length(WIL2)),
    WIL2A = lists:map(fun({K, _LC, _RC, _RNCF}) -> K end, WIL2),
    ?assertMatch([{queue1, 2, 1}, {queue1, 2, 1}, {queue1, 2, 1}],
                    lists:sublist(WIL2A, 3)).

adjust_wait_test() ->
    NullTS = {0, 0, 0},
    Peer1 = {1, ?STARTING_DELAYMS, "127.0.0.1", 12008},
    Peer2 = {2, ?STARTING_DELAYMS, "127.0.0.1", 12009},
    Peer3 = {3, 0, "127.0.0.1", 12009},
    PL = [Peer1, Peer2, Peer3],
    {W0, PL0} = adjust_wait(true, {queue_empty, NullTS}, 1, PL),
    ?assertMatch(2 * ?STARTING_DELAYMS, W0),
    {W1, PL1} = adjust_wait(true, {queue_empty, NullTS}, 1, PL0),
    ?assertMatch(4 * ?STARTING_DELAYMS, W1),
    {W2, PL2} = adjust_wait(true, {tomb, NullTS, NullTS}, 1, PL1),
    ?assertMatch(2 * ?STARTING_DELAYMS, W2),
    {W3, PL3} = adjust_wait(true, {queue_empty, NullTS}, 3, PL2),
    ?assertMatch(1, W3),
    {W4, PL4} = adjust_wait(false, {error, tcp, closed}, 2, PL3),
    ?assertMatch(?ON_ERROR_DELAYMS, W4),
    {W5, _PL5} = adjust_wait(true, {queue_empty, NullTS}, 2, PL4),
    ?assertMatch(?MAX_SUCCESS_DELAYMS, W5).

log_dont_blow_test() ->
    SW0 = #sink_work{queue_name = queue1},
    {queue1, 1, SW1} = log_mapfun({queue1, 1, SW0}),
    ?assertMatch(SW0, SW1),
    QS0 = SW1#sink_work.queue_stats,
    QS1 = increment_queuestats(QS0, {no_queue, 100}),
    QS2 = increment_queuestats(QS1, {tomb, 150, 50, 180}),
    QS3 = increment_queuestats(QS2, {object, 110, 10, 180}),
    QS4 = increment_queuestats(QS3, {error, tcp, closed}),
    QS5 = increment_queuestats(QS4, {no_queue, 100}),
    {queue1, 5, SW2} =
        log_mapfun({queue1, 5, SW1#sink_work{queue_stats = QS5}}),
    ?assertMatch(?ZERO_STATS, SW2#sink_work.queue_stats),
    QSMT1 = add_modtime(?ZERO_STATS, 86400001 * 1000),
    QSMT2 = add_modtime(QSMT1, 0),
    ?assertMatch({modified_time, 1, 0, 0, 0, 1}, element(6, QSMT2)).


-endif.
