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
            add_snkqueue/3]).

-export([repl_fetcher/1]).

-define(LOG_TIMER_SECONDS, 60).
-define(ZERO_STATS,
        {{success, 0}, {failure, 0}, {repl_time, 0},
        {modified_time, 0, 0, 0, 0, 0}}).
-define(STARTING_DELAYMS, 8).
-define(MAX_SUCCESS_DELAYMS, 256).
-define(ON_ERROR_DELAYMS, 65536).
-define(INACTIVITY_TIMEOUT_MS, 60000).

-record(sink_work, {queue_name :: queue_name(),
                    work_queue = [] :: list(work_item()),
                    minimum_queue_length = 0 :: non_neg_integer(),
                    deferred_queue_length = 0 :: non_neg_integer(),
                    peer_list = [] :: list(peer_info()),
                    max_worker_count = 1 :: pos_integer(),
                    queue_stats = ?ZERO_STATS :: queue_stats(),
                    suspended = false :: boolean()}).


-record(state, {work = [] :: list(sink_work()),
                enabled = false :: boolean()}).

-type queue_name() :: atom().
-type peer_id() :: pos_integer().

-type work_item() ::
    {{queue_name(), peer_id()}, riak_client:riak_client(), rhc:rhc()}.
    % Identifier for the work item  - and the local and remote clients to use

-type sink_work() ::
    {queue_name(), #sink_work{}}.
    % Mapping between queue names and the work records

-type peer_info() ::
    {peer_id(), non_neg_integer(), string(), pos_integer()}.
    % {Identifier for Peer,
    %   Next delay to use on work_item in ms,
    %   Peer IP,
    %   Peer Listener}

-type queue_stats() ::
    {{success, non_neg_integer()}, {failure, non_neg_integer()},
        {repl_time, non_neg_integer()},
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
        {tomb, non_neg_integer(), non_neg_integer()} |
        {object, non_neg_integer(), non_neg_integer()} |
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
%% This API may be used to support more than the two queue names for which
%% configuration is permissable via riak.conf.
-spec add_snkqueue(queue_name(), list(peer_info()), pos_integer()) -> ok.
add_snkqueue(QueueName, Peers, WorkerCount) ->
    gen_server:call(?MODULE, {add, QueueName, Peers, WorkerCount}).

%%%============================================================================
%%% gen_server callbacks
%%%============================================================================

init([]) ->
    SinkEnabled = app_helper:get_env(riak_kv, replrtq_enablesink, false),
    case SinkEnabled of
        true ->
            Sink1 = app_helper:get_env(riak_kv, replrtq_sink1queue, disabled),
            State1 =
                case Sink1 of
                    disabled ->
                        #state{};
                    Snk1QueueName ->
                        Sink1Peers =
                            app_helper:get_env(riak_kv, replrtq_sink1peers),
                        Snk1PeerInfo =
                            tokenise_peers(Sink1Peers),
                        Snk1WorkerCount =
                            app_helper:get_env(riak_kv, replrtq_sink1workers),
                        {Snk1QueueLength, Snk1WorkQueue} =
                            determine_workitems(Snk1QueueName,
                                                Snk1PeerInfo,
                                                Snk1WorkerCount),
                        Snk1W =
                            #sink_work{queue_name = Snk1QueueName,
                                        work_queue = Snk1WorkQueue,
                                        minimum_queue_length = Snk1QueueLength,
                                        peer_list = Snk1PeerInfo,
                                        max_worker_count = Snk1WorkerCount},
                        #state{work = [{Snk1QueueName, Snk1W}]}
                end,
            Sink2 = app_helper:get_env(riak_kv, replrtq_sink2queue, disabled),
            State2 =
                case Sink2 of
                    disabled ->
                        State1;
                    Snk2QueueName ->
                        Sink2Peers =
                            app_helper:get_env(riak_kv, replrtq_sink2peers),
                        Snk2PeerInfo =
                            tokenise_peers(Sink2Peers),
                        Snk2WorkerCount =
                            app_helper:get_env(riak_kv, replrtq_sink2workers),
                        {Snk2QueueLength, Snk2WorkQueue} =
                            determine_workitems(Snk2QueueName,
                                                Snk2PeerInfo,
                                                Snk2WorkerCount),
                        Snk2W =
                            #sink_work{queue_name = Snk2QueueName,
                                        work_queue = Snk2WorkQueue,
                                        minimum_queue_length = Snk2QueueLength,
                                        peer_list = Snk2PeerInfo,
                                        max_worker_count = Snk2WorkerCount},
                        UpdatedWork =
                            [{Snk2QueueName, Snk2W}|State1#state.work],
                        State1#state{work = UpdatedWork}
                end,
            {ok, State2#state{enabled = true}, ?INACTIVITY_TIMEOUT_MS};
        false ->
            {ok, #state{}}
    end.


handle_call({suspend, QueueN}, _From, State) ->
    case lists:keyfind(QueueN, 1, State#state.work) of
        false ->
            {reply, not_found, State};
        {QueueN, SinkWork} ->
            SW0 = SinkWork#sink_work{suspended = true},
            W0 = lists:keyreplace(QueueN, 1, State#state.work, {QueueN, SW0}),
            {reply, ok, State#state{work = W0}}
    end;
handle_call({resume, QueueN}, _From, State) ->
    case lists:keyfind(QueueN, 1, State#state.work) of
        false ->
            {reply, not_found, State};
        {QueueN, SinkWork} ->
            SW0 = SinkWork#sink_work{suspended = false},
            W0 = lists:keyreplace(QueueN, 1, State#state.work, {QueueN, SW0}),
            prompt_work(),
            {reply, ok, State#state{work = W0}}
    end;
handle_call({remove, QueueN}, _From, State) ->
    W0 = lists:keydelete(QueueN, 1, State#state.work),
    case W0 of
        [] ->
            {reply, ok, State#state{work = W0, enabled = false}};
        _ ->
            {reply, ok, State#state{work = W0, enabled = true}}
    end;
handle_call({add, QueueN, Peers, WorkerCount}, _From, State) ->
    {QueueLength, WorkQueue} = determine_workitems(QueueN, Peers, WorkerCount),
    SnkW =
        #sink_work{queue_name = QueueN,
                    work_queue = WorkQueue,
                    minimum_queue_length = QueueLength,
                    peer_list = Peers,
                    max_worker_count = WorkerCount},
    W0 = lists:keystore(QueueN, 1, State#state.work, {QueueN, SnkW}),
    prompt_work(),
    {reply, ok, State#state{work = W0, enabled = true}}.


handle_cast(prompt_work, State) ->
    Work0 = lists:map(fun do_work/1, State#state.work),
    {noreply, State#state{work = Work0}};
handle_cast({done_work, WorkItem, Success, ReplyTuple}, State) ->
    {{QueueName, PeerID}, _LC, _RC} = WorkItem,
    case lists:keyfind(QueueName, 1, State#state.work) of
        false ->
            % Work profile has changed since this work was prompted
            {noreply, State};
        {QueueName, SinkWork} ->
            QS = SinkWork#sink_work.queue_stats,
            QS0 = increment_queuestats(QS, ReplyTuple),
            PL = SinkWork#sink_work.peer_list,
            {PW0, PL0} = adjust_wait(Success, ReplyTuple, PeerID, PL),
            DQL0 = SinkWork#sink_work.deferred_queue_length + 1,
            UpdSW = SinkWork#sink_work{peer_list = PL0,
                                        queue_stats = QS0,
                                        deferred_queue_length = DQL0},
            UpdWork = lists:keyreplace(QueueName, 1, State#state.work,
                                        {QueueName, UpdSW}),
            case PW0 of
                0 ->
                    requeue_work(WorkItem);
                W ->
                    erlang:send_after(W, self(), {prompt_requeue, WorkItem})
            end,
            {noreply, State#state{work = UpdWork}}
    end;
handle_cast({requeue_work, WorkItem}, State) ->
    {{QueueName, _PeerID}, _LC, _RC} = WorkItem,
    case lists:keyfind(QueueName, 1, State#state.work) of
        false ->
            % Work profile has changed since this work was requeued
            {noreply, State};
        {QueueName, SinkWork} ->
            UpdWorkQueue = [WorkItem|SinkWork#sink_work.work_queue],
            DQL = SinkWork#sink_work.deferred_queue_length - 1,
            UpdSW = SinkWork#sink_work{work_queue = UpdWorkQueue,
                                        deferred_queue_length = DQL},
            UpdWork = lists:keyreplace(QueueName, 1, State#state.work,
                                        {QueueName, UpdSW}),
            prompt_work(),
            {noreply, State#state{work = UpdWork}}
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

terminate(normal, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%============================================================================
%%% Internal functions
%%%============================================================================

% @doc calculate the mean avoiding divide by 0
-spec calc_mean(non_neg_integer(), non_neg_integer()) -> string().
calc_mean(_, 0) ->
    "no_result";
calc_mean(Time, Count) ->
    [Mean] = io_lib:format("~.3f",[Time / Count]),
    Mean.

% @doc convert the toeknised string of peers from the configuration into actual
% usable peer information.
% tokenised string expected to be of form:
% "192.168.10.1:8098|192.168.10.2:8098 etc"
-spec tokenise_peers(string()) -> list(peer_info()).
tokenise_peers(PeersString) ->
    PeerL0 = string:tokens(PeersString, "|"),
    SplitHostPortFun =
        fun(PeerString, Acc0) ->
            [Host, Port] = string:tokens(PeerString, ":"),
            {{Acc0, ?STARTING_DELAYMS, Host, list_to_integer(Port)}, Acc0 + 1}
        end,
    {PeerList, _Acc} = lists:mapfoldl(SplitHostPortFun, 1, PeerL0),
    PeerList.


%% @doc
%% Caluclates the queue of work items and the minimum length of queue to be
%% kept by the process, in order to deliver the desired number of concurrent
%% worker processes.
%% The count of work items should allow for all workers to be busy with one
%% peer yielding work, when all other peers have no work
-spec determine_workitems(queue_name(), list(peer_info()), pos_integer())
                                    -> {non_neg_integer(), list(work_item())}.
determine_workitems(QueueName, PeerInfo, WorkerCount) ->
    MapPeerToWIFun =
        fun({PeerID, _Delay, Host, Port}) ->
            LocalClient = riak_client:new(node(), undefined),
            RemoteClient = rhc:create(Host, Port, "riak", []),
            {{QueueName, PeerID}, LocalClient, RemoteClient}
        end,
    WorkItems0 = lists:map(MapPeerToWIFun, PeerInfo),
    WorkItems =
        lists:foldl(fun(_I, Acc) -> Acc ++ WorkItems0 end,
                    [],
                    lists:seq(1, WorkerCount)),
    {length(WorkItems) - WorkerCount, WorkItems}.

%% @doc
%% For an item of work which has been removed from the work queue, spawn a
%% snk worker (using the repl_fetcher fun) to manage that item of work.  The
%% worker must ensure the wortk_item is delivered back on completion.
-spec do_work(sink_work()) -> sink_work().
do_work({QueueName, SinkWork}) ->
    WorkQueue = SinkWork#sink_work.work_queue,
    MinQL = (SinkWork#sink_work.minimum_queue_length -
                SinkWork#sink_work.deferred_queue_length),
    IsSuspended = SinkWork#sink_work.suspended,
    case IsSuspended of
        true ->
            {QueueName, SinkWork};
        false ->
            case length(WorkQueue) - MinQL of
                0 ->
                    {QueueName, SinkWork};
                _ ->
                    {Rem, Work} = lists:split(MinQL, WorkQueue),
                    lists:foreach(fun work/1, Work),
                    {QueueName, SinkWork#sink_work{work_queue = Rem}}
            end
    end.

-spec work(work_item()) -> ok.
work(WorkItem) ->
    _P = spawn(?MODULE, repl_fetcher, [WorkItem]),
    ok.

%% Should always under all circumstances end with calling done_work
-spec repl_fetcher(work_item()) -> ok.
repl_fetcher(WorkItem) ->
    SW = os:timestamp(),
    try
        {{QueueName, _PeerID}, LocalClient, RemoteClient} = WorkItem,
        case rhc:fetch(RemoteClient, QueueName) of
            {ok, queue_empty} ->
                SW0 = os:timestamp(),
                EmptyFetchSplit = timer:now_diff(SW0, SW) div 1000,
                done_work(WorkItem, true, {queue_empty, EmptyFetchSplit});
            {ok, {deleted, _TC, RObj}} ->
                {ok, LMD} = riak_client:push(RObj, true, [], LocalClient),
                SW0 = os:timestamp(),
                ModSplit = timer:now_diff(SW0, LMD) div 1000,
                FetchSplit = timer:now_diff(SW0, SW) div 1000,
                done_work(WorkItem, true, {tomb, FetchSplit, ModSplit});
            {ok, RObj} ->
                {ok, LMD} = riak_client:push(RObj, false, [], LocalClient),
                SW0 = os:timestamp(),
                ModSplit = timer:now_diff(SW0, LMD) div 1000,
                FetchSplit = timer:now_diff(SW0, SW) div 1000,
                done_work(WorkItem, true, {object, FetchSplit, ModSplit})
        end
    catch
        Type:Exception ->
            lager:warning("Snk worker failed due to ~w ~w", [Type, Exception]),
            done_work(WorkItem, false, {error, Type, Exception})
    end.

%% @doc
%% Keep some stats on the queues, to be logged out periodically
-spec increment_queuestats(queue_stats(), reply_tuple()) -> queue_stats().
increment_queuestats(QueueStats, ReplyTuple) ->
    case ReplyTuple of
        {tomb, FetchSplit, ModSplit} ->
            add_modtime(add_repltime(add_success(QueueStats),
                            FetchSplit),
                        ModSplit);
        {object, FetchSplit, ModSplit} ->
            add_modtime(add_repltime(add_success(QueueStats),
                            FetchSplit),
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
add_success({{success, Success}, F, RT, MT}) ->
    {{success, Success + 1}, F, RT, MT}.

-spec add_failure(queue_stats()) -> queue_stats().
add_failure({S, {failure, Failure}, RT, MT}) ->
    {S, {failure, Failure + 1}, RT, MT}.

-spec add_repltime(queue_stats(), integer()) -> queue_stats().
add_repltime({S, F, {repl_time, TotalReplTime}, MT}, ReplTime) ->
    {S, F, {repl_time, TotalReplTime + ReplTime}, MT}.

-spec add_modtime(queue_stats(), integer()) -> queue_stats().
add_modtime({S, F, RT, MT}, ModTime) ->
    E = mod_split_element(ModTime) +  1,
    C = element(E, MT),
    {S, F, RT, setelement(E, MT, C + 1)}.

%% @doc
%% Depending on the result of the request, adjust the wit time before this work
%% item is due to be re-processed.  If the queue is commonly empty, then back
%% of the workload exponentially, but if it is consistently yielding results
%% from the queue - the reduce the wait time exponentially tending to 0.
%% On an error, the wait time should leap to avoid all workers being locked
%% attempting to communicate with a peer to which requests are timing out.
-spec adjust_wait(boolean(), reply_tuple(), peer_id(), list(peer_info()))
                                    -> {non_neg_integer(), list(peer_info())}.
adjust_wait(true, {queue_empty, _T}, PeerID, PeerList) ->
    {PeerID, Delay, H, P} = lists:keyfind(PeerID, 1, PeerList),
    Delay0 = increment_delay(Delay),
    PeerList0 = lists:keyreplace(PeerID, 1, PeerList, {PeerID, Delay0, H, P}),
    {Delay0, PeerList0};
adjust_wait(true, _, PeerID, PeerList) ->
    {PeerID, Delay, H, P} = lists:keyfind(PeerID, 1, PeerList),
    Delay0 = Delay bsr 1,
    PeerList0 = lists:keyreplace(PeerID, 1, PeerList, {PeerID, Delay0, H, P}),
    {Delay0, PeerList0};
adjust_wait(false, _, PeerID, PeerList) ->
    {PeerID, _Delay, H, P} = lists:keyfind(PeerID, 1, PeerList),
    Delay0 = ?ON_ERROR_DELAYMS,
    PeerList0 = lists:keyreplace(PeerID, 1, PeerList, {PeerID, Delay0, H, P}),
    {Delay0, PeerList0}.

-spec increment_delay(non_neg_integer()) -> non_neg_integer().
increment_delay(0) ->
    1;
increment_delay(N) ->
    min(N bsl 1, ?MAX_SUCCESS_DELAYMS).

%% @doc
%% Log details of the replictaion counts and times, and also the current delay
%% to requeue work items for each peer (consistently low delay may indicate
%% the need to configure more workers.
%% At runtime changes to the number of workers can be managed via the
%% remove_snkqueue/1 and add_snkqueue/3 api.
-spec log_mapfun(sink_work()) -> sink_work().
log_mapfun({QueueName, SinkWork}) ->
    {{success, SC}, {failure, EC},
        {repl_time, RT},
        {modified_time, MTS, MTM, MTH, MTD, MTL}}
        = SinkWork#sink_work.queue_stats,
    lager:info("Queue=~w success_count=~w error_count=~w" ++
                " mean_repltime=~s" ++
                " lmdin_s=~w lmdin_m=~w lmdin_h=~w lmdin_d=~w lmd_over=~w",
                [QueueName, SC, EC, calc_mean(RT, SC),
                    MTS, MTM, MTH, MTD, MTL]),
    FoldPeerInfoFun =
        fun({_PeerID, D, IP, Port}, Acc) ->
            Acc ++ lists:flatten(io_lib:format(" ~s:~w=~w", [IP, Port, D]))
        end,
    PeerDelays =
        lists:foldl(FoldPeerInfoFun, "", SinkWork#sink_work.peer_list),
    lager:info("Queue=~w has peer delays of~s", [QueueName, PeerDelays]),
    {QueueName, SinkWork#sink_work{queue_stats = ?ZERO_STATS}}.


%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).

tokenise_test() ->
    String1 = "127.0.0.1:12008|127.0.0.1:12009|127.0.0.1:12009",
    Peer1 = {1, ?STARTING_DELAYMS, "127.0.0.1", 12008},
    Peer2 = {2, ?STARTING_DELAYMS, "127.0.0.1", 12009},
    Peer3 = {3, ?STARTING_DELAYMS, "127.0.0.1", 12009},
        % references not de-duped, allows certain peers to double-up on worker
        % time
    ?assertMatch([Peer1, Peer2, Peer3], tokenise_peers(String1)).

determine_workitems_test() ->
    Peer1 = {1, ?STARTING_DELAYMS, "127.0.0.1", 12008},
    Peer2 = {2, ?STARTING_DELAYMS, "127.0.0.1", 12009},
    Peer3 = {3, ?STARTING_DELAYMS, "127.0.0.1", 12009},
    WC1 = 5,
    {MQL1, WIL1} = determine_workitems(queue1, [Peer1, Peer2, Peer3], WC1),
    ?assertMatch(10, MQL1),
    ?assertMatch(15, length(WIL1)),
    WIL1A = lists:map(fun({K, _LC, _RC}) -> K end, WIL1),
    ?assertMatch([{queue1, 1}, {queue1, 2}, {queue1, 3}], lists:sublist(WIL1A, 3)),

    {MQL2, WIL2} = determine_workitems(queue1, [Peer1], WC1),
    ?assertMatch(0, MQL2),
    ?assertMatch(5, length(WIL2)),
    WIL2A = lists:map(fun({K, _LC, _RC}) -> K end, WIL2),
    ?assertMatch([{queue1, 1}, {queue1, 1}, {queue1, 1}], lists:sublist(WIL2A, 3)).

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
    {queue1, SW1} = log_mapfun({queue1, SW0}),
    ?assertMatch(SW0, SW1),
    QS0 = SW1#sink_work.queue_stats,
    QS1 = increment_queuestats(QS0, {no_queue, 100}),
    QS2 = increment_queuestats(QS1, {tomb, 150, 180}),
    QS3 = increment_queuestats(QS2, {object, 150, 180}),
    QS4 = increment_queuestats(QS3, {error, tcp, closed}),
    QS5 = increment_queuestats(QS4, {no_queue, 100}),
    {queue1, SW2} = log_mapfun({queue1, SW1#sink_work{queue_stats = QS5}}),
    ?assertMatch(?ZERO_STATS, SW2#sink_work.queue_stats),
    QSMT1 = add_modtime(?ZERO_STATS, 86400001),
    QSMT2 = add_modtime(QSMT1, 0),
    ?assertMatch({modified_time, 1, 0, 0, 0, 1}, element(4, QSMT2)).


-endif.
