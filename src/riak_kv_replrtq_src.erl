%% -------------------------------------------------------------------
%%
%% riak_kv_replrtq_src: Source of replication updates from this node
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

%% @doc Queue any replication changes emitting from this node due to
%% a PUT being co-ordinated on this node, a full-sync exchange initiated on
%% this node, or an aae_fold replication-fold running on vnodes on this
%% node.


-module(riak_kv_replrtq_src).
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-behaviour(gen_server).

-export([init/1,
        handle_call/3,
        handle_cast/2,
        handle_info/2,
        terminate/2,
        code_change/3,
        format_status/2]).

-export([start_link/0,
    start_link/1,
    replrtq_aaefold/2,
    replrtq_ttaaefs/2,
    replrtq_coordput/1,
    register_rtq/2,
    delist_rtq/1,
    suspend_rtq/1,
    resume_rtq/1,
    length_rtq/1,
    popfrom_rtq/1,
    waitforpop_rtq/2,
    clear_rtq/1,
    stop/0]).

-ifdef(TEST).

-define(OBJECT_LIMIT, 1000).
-define(QUEUE_LIMIT, 100000).
-define(MEMORY_LIMIT, 2000).

-else.
-define(OBJECT_LIMIT, 1000).
    % If the priority queue is bigger than the object limit, and the object
    % reference is {object, Object} then the object reference will be converted
    % into a fetch request.  Manages the number of objects being held in
    % memory.  The object limit can be altered using:
    % riak_kv.replrtq_srcobjectlimit
    %
    % The object limit is implemented by limiting the size of the local queue
    % cache, and not allowing any objects onto the riak_kv_overflow_queue.  As
    % there may be non-objects on the local queue cache, this means that it is
    % possible that objects may be stripped below the limit.
    %
    % The object limit is tracked separately for each queue name.

-define(QUEUE_LIMIT, 1000000).
    % This is the limit on the size of the queue for a given queue name and
    % priority.  the queue limit can be altered using:
    % riak_kv.replrtq_overflow_limit

-define(MEMORY_LIMIT, 10000).
    % This is the limit on the size of the queue in-memory within the overflow
    % queue for a given queue name and priority

-endif.


-define(RTQ_PRIORITY, 3).
    % Priority for queueing real-time replication of PUTs co-ordinated on this
    % node
-define(AAE_PRIORITY, 2).
    % Priority for queueing replication event prompted by a Tictac AAE fold
-define(FLD_PRIORITY, 1).
    % Priority for queueing replication event prompted by an AAE fold (e.g.
    % replicating all keys in a given range, or modified date range)
-define(LOG_TIMER_SECONDS, 30).
    % Log the queue sizes every 30 seconds
-define(CONSUME_DELAY, 4).
-define(BATCH_SIZE, 32).

-record(state,  {
            queue_filtermap = [] :: list(queue_filtermap()),
            queue_overflow = [] :: list(queue_overflow())|not_logged,
            queue_local = [] :: list(queue_local())|not_logged,
            object_limit = ?OBJECT_LIMIT :: non_neg_integer(),
            queue_limit = ?QUEUE_LIMIT :: non_neg_integer(),
            log_frequency_in_ms = ?LOG_TIMER_SECONDS * 1000 :: pos_integer(),
            root_path :: string()
}).

-type queue_name() :: atom().
-type object_ref() ::
    {tomb, riak_object:riak_object()}|
        {object, riak_object:riak_object()}|
        to_fetch.
    % The object reference can be the actual object or a request to fetch the
    % actual object using the Bucket, Key and Clock in the repl_entry
-type repl_entry() ::
    {riak_object:bucket(), riak_object:key(), vclock:vclock(), object_ref()}.
    % If the object is a tombstone which had been PUT, then the actual
    % object may be queued to reduce the chance that the PULL from the queue
    % on the replicating cluster loses a race with a delete_mode timeout.
    % Where the repl_entry is set to to_fetch it will need to be fetched from
    % a vnode as part of the operation that PULLs from the queue.
-type type_filter() :: {buckettype, binary()}.
-type bucket_filter() :: {bucketname, binary()}.
-type prefix_filter() :: {bucketprefix, binary()}.
-type all_filter() :: any.
-type blockrtq_filter() :: block_rtq.
-type queue_priority() :: 1..3 .
-type queue_filter() ::
    type_filter()|bucket_filter()|all_filter()|
        blockrtq_filter()|prefix_filter().
-type queue_filtermap() :: {queue_name(), queue_filter(), active|suspended}.
-type queue_length() ::
    {non_neg_integer(), non_neg_integer(), non_neg_integer()}.
    % The length of the cache queue, the main queue and the overflow queue
-type queue_overflow() ::
    {queue_name(), riak_kv_overflow_queue:overflowq()}.
-type queue_local_detail() ::
    {queue:queue(), non_neg_integer(), non_neg_integer()}.
    % Local queue, and two integers tracking the size of first the local queue
    % and second the overflow queue
-type queue_local() ::
    {queue_name(),
        {
            queue_local_detail(),
            queue_local_detail(),
            queue_local_detail()
        }
    }.
    % The local queue has a queue_cache, the current queue_cache length (to
    % avoid having to perform order n operation to check the length), and the
    % current overflow queue length.  There are three such tuples, one for
    % each of the 3 priorities supported.  The queue_cache is a cache of the
    % next entries to be sent.
    %
    % If a priority 3 item is pushed to the queue and the length of the
    % queue_cache is less than the object limit, and the overflowq is empty
    % for this priority, the item will be added to the queue_cache.
    % 
    % If a priority 3 item is pushed to the queue and the length of the
    % queue_cache is at/over the object limit, or the overflowq is non-empty
    % then the item will be added to the overflowq, and if the object_ref is
    % an object - this will be sripped back to to_fetch.
    %
    % If a priority 1 or 2 repl_entry is received, then it will always be
    % added first to the overflowq .
    %
    % If a fetch request is received and the priority 3 queue_cache is
    % non-empty then the next entry from this queue will be returned.  
    % If the overflow queue is empty, then an attempt will be made to
    % return a batch from the overflowq, to add to the queue_cache.
    %
    % If the Priority 3 queue is empty (both cache an overflow), then the
    % lower priority queues can be tried in turn.

-export_type([repl_entry/0, queue_name/0]).

%%%============================================================================
%%% API
%%%============================================================================

start_link() ->
    start_link(app_helper:get_env(riak_kv, replrtq_dataroot)).

start_link(FilePath) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [FilePath], []).

%% @doc
%% Add a list of repl entrys to the real-time queue with the given queue name.
%% The filter for that queue name will not be checked.
%% These entries will be added to the queue with priority 1 (higher number is
%% higher priority).
%% This should be used for folds to prompt re-replication (e.g. folds to
%% replicate all changes within a given modified date range)
%%
%% Note that it is assumed that the list of repl_entries has been built
%% efficiently using [Add|L] - so the first added element will be the element
%% retrieved first from the queue
-spec replrtq_aaefold(queue_name(), list(repl_entry())) -> ok.
replrtq_aaefold(QueueName, ReplEntries) ->
    % This is a call as we don't want this process to be able to overload the src
    gen_server:call(
        ?MODULE,
        {bulk_add, ?FLD_PRIORITY, QueueName, ReplEntries}, infinity).

%% @doc
%% Add a list of repl entrys to the real-time queue with the given queue name.
%% The filter for that queue name will not be checked.
%% These entries will be added to the queue with priority 2 (higher number is
%% higher priority).
%% This should be use to replicate the outcome of Tictac AAE full-sync
%% aae_exchange.
-spec replrtq_ttaaefs(queue_name(), list(repl_entry())) -> ok.
replrtq_ttaaefs(QueueName, ReplEntries) ->
    gen_server:call(
        ?MODULE,
        {bulk_add, ?AAE_PRIORITY, QueueName, ReplEntries}, infinity).

%% @doc
%% Add a single repl_entry associated with a PUT coordinated on this node.
%% Never wait for the response or backoff - replictaion should be asynchronous
%% and never slow the PUT path on the src cluster.
-spec replrtq_coordput(repl_entry()) -> ok.
replrtq_coordput({Bucket, _, _, _} = ReplEntry) when is_binary(Bucket); is_tuple(Bucket) ->
    gen_server:cast(?MODULE, {rtq_coordput, Bucket, ReplEntry}).

%% @doc
%% Setup a queue with a given queuename, which will take coordput repl_entries
%% that pass the given filter.
-spec register_rtq(queue_name(), queue_filter()) -> boolean().
register_rtq(QueueName, QueueFilter) ->
    gen_server:call(?MODULE, {register_rtq, QueueName, QueueFilter}, infinity).

%% @doc
%% Remove the registered queue from the configuration, deleting any outstanding
%% items on the queue
-spec delist_rtq(queue_name()) -> ok.
delist_rtq(QueueName) ->
    gen_server:call(?MODULE, {delist_rtq, QueueName}, infinity).

%% @doc
%% Suspend a queue form receiving further updates, all attempted updates after
%% the suspension will be discarded.  The queue will remain listed, and
%% consumption from the queue may continue
-spec suspend_rtq(queue_name()) -> ok|not_found.
suspend_rtq(QueueName) ->
    gen_server:call(?MODULE, {suspend_rtq, QueueName}, infinity).

%% @doc
%% Where a queue has been suspended, allow again for new updates to be added
%% to the queue.  A null operation if the queue is not suspended.
-spec resume_rtq(queue_name()) -> ok|not_found.
resume_rtq(QueueName) ->
    gen_server:call(?MODULE, {resume_rtq, QueueName}, infinity).

%% @doc
%% Return the {fold_length, aae_length, put_length} to show the length of the
%% queue under each priority for that queue
-spec length_rtq(queue_name()) -> {queue_name(), queue_length()}|false.
length_rtq(QueueName) ->
    gen_server:call(?MODULE, {length_rtq, QueueName}).

%% @doc
%% Clear an existing queue and overflow queue, and start a new queue.  This
%% will also reset the size of the overflow queue to the current configuration
%% based on the riak_kv.replrtq_overflow_limit
-spec clear_rtq(queue_name()) -> ok.
clear_rtq(QueueName) ->
    gen_server:call(?MODULE, {clear_rtq, QueueName}).

%% @doc
%% Pop an entry from the queue with given queue name.  The entry will be taken
%% from the highest priority queue with entries
-spec popfrom_rtq(queue_name()) -> repl_entry()|queue_empty.
popfrom_rtq(QueueName) ->
    gen_server:call(?MODULE, {popfrom_rtq, QueueName}, infinity).

%% @doc
%% Wait for N 1 ms loops to see if an object is available on the queue before
%% potentially returning queue_empty
-spec waitforpop_rtq(queue_name, non_neg_integer()) -> repl_entry()|queue_empty.
waitforpop_rtq(QueueName, 0) ->
    popfrom_rtq(QueueName);
waitforpop_rtq(QueueName, N) ->
    case popfrom_rtq(QueueName) of
        queue_empty ->
            timer:sleep(?CONSUME_DELAY),
            % Maybe a shutdown during sleep - so check process is alive when
            % emerging from sleep to avoid noisy shutdown
            case whereis(?MODULE) of
                undefined ->
                    queue_empty;
                _ ->
                    waitforpop_rtq(QueueName, N - 1)
            end;
        R ->
            R
    end.

-spec stop() -> ok.
stop() ->
    gen_server:call(?MODULE, stop).

%%%============================================================================
%%% gen_server callbacks
%%%============================================================================

init([FilePath]) ->
    QueueDefnString = app_helper:get_env(riak_kv, replrtq_srcqueue, ""),
    QFM = tokenise_queuedefn(QueueDefnString),

    {OL, QL} = get_limits(),

    MapToQOverflow =
        fun({QueueName, _QF, _QA}) ->
            {QueueName, empty_overflow_queue(QueueName, FilePath)}
        end,
    MaptoQCache =
        fun({QueueName, _QF, _QA}) ->
            {QueueName, empty_local_queue()}
        end,
    QO = lists:map(MapToQOverflow, QFM),
    QC = lists:map(MaptoQCache, QFM),
    LogFreq =
        app_helper:get_env(
            riak_kv,
            replrtq_logfrequency,
            ?LOG_TIMER_SECONDS * 1000),
    erlang:send_after(LogFreq, self(), log_queue),

    {ok, #state{queue_filtermap = QFM,
                queue_overflow = QO,
                queue_local = QC,
                object_limit = OL,
                queue_limit = QL,
                log_frequency_in_ms = LogFreq,
                root_path = FilePath}}.

handle_call({bulk_add, Priority, QueueName, ReplEntries}, _From, State) ->
    case lists:keyfind(QueueName, 1, State#state.queue_filtermap) of
        {QueueName, _QueueFilter, active} ->
            {QueueName, OverflowQueue} =
                lists:keyfind(QueueName, 1, State#state.queue_overflow),
            {QueueName, LocalQueues} =
                lists:keyfind(QueueName, 1, State#state.queue_local),
            {Q, LC, OC} = element(Priority, LocalQueues),
            EntriesToAdd =
                case LC + OC + length(ReplEntries) of
                    TC when TC =< State#state.queue_limit ->
                        ReplEntries;
                    TC ->
                        OverLimit = TC - State#state.queue_limit,
                        {ToThrow, ToQueue} =
                            lists:split(max(OverLimit, 0), ReplEntries),
                        _ = riak_kv_stat:update(
                            {ngrrepl_srcdiscard, length(ToThrow)}),
                        ToQueue
                end,
            UpdOverflowQueue =
                lists:foldr(
                    fun(RE, Acc) ->
                        riak_kv_overflow_queue:addto_queue(Priority, RE, Acc)
                    end,
                    OverflowQueue,
                    EntriesToAdd),
            UpdOverflowQueues =
                lists:keyreplace(
                    QueueName,
                    1,
                    State#state.queue_overflow,
                    {QueueName, UpdOverflowQueue}),
            UpdLocalQueues =
                lists:keyreplace(
                        QueueName,
                        1,
                        State#state.queue_local,
                        {QueueName,
                            setelement(
                                Priority,
                                LocalQueues,
                                {Q, LC, OC + length(EntriesToAdd)})}),
            {reply,
                ok,
                State#state{
                    queue_local = UpdLocalQueues,
                    queue_overflow = UpdOverflowQueues}};
        _ ->
            {reply, ok, State}
    end;
handle_call({length_rtq, QueueName}, _From, State) ->
    case lists:keyfind(QueueName, 1, State#state.queue_local) of
        {QueueName, LocalQueues} ->
            MapFun =
                fun(I) ->
                    queue_lengths(LocalQueues, I)
                end,
            {reply,
                {QueueName,
                    list_to_tuple(
                        lists:map(
                            MapFun,
                            [?FLD_PRIORITY, ?AAE_PRIORITY, ?RTQ_PRIORITY]))},
                State};
        false ->
            lager:warning(
                "Attempt to get length of undefined queue ~w",
                [QueueName]),
            {reply, false, State}
        end;
handle_call({popfrom_rtq, QName}, _From, State) ->
    case lists:keyfind(QName, 1, State#state.queue_local) of
        {QName, LocalQueues} ->
            case LocalQueues of
                {{_Q1, 0, 0}, {_Q2, 0, 0}, {_Q3, 0, 0}} ->
                    {reply, queue_empty, State};
                {P1, P2, {Q3, LC, OC}} when LC > 0 ->
                    {{value, RE}, UpdQ} = queue:out(Q3),
                    UpdLQs =
                        lists:keyreplace(
                            QName,
                            1,
                            State#state.queue_local,
                            {QName, {P1, P2, {UpdQ, LC - 1, OC}}}),
                    {reply, RE, State#state{queue_local = UpdLQs}};
                {P1, P2, {Q3, 0, OC}} when OC > 0 ->
                    {RE, UpdLQ, UpdOFlowQs} =
                        fetch_from_overflow(
                            ?RTQ_PRIORITY,
                            QName,
                            State#state.queue_overflow,
                            {Q3, 0, OC}),
                    UpdLQs =
                        lists:keyreplace(
                            QName,
                            1,
                            State#state.queue_local,
                            {QName, {P1, P2, UpdLQ}}),
                    {reply,
                        RE,
                        State#state{
                            queue_local = UpdLQs,
                            queue_overflow = UpdOFlowQs}};
                {P1, {Q2, LC, OC}, P3} when LC > 0 ->
                    {{value, RE}, UpdQ} = queue:out(Q2),
                    UpdLQs =
                        lists:keyreplace(
                            QName,
                            1,
                            State#state.queue_local,
                            {QName, {P1, {UpdQ, LC - 1, OC}, P3}}),
                    {reply, RE, State#state{queue_local = UpdLQs}};
                {P1, {Q2, 0, OC}, P3} when OC > 0 ->
                    {RE, UpdLQ, UpdOFlowQs} =
                        fetch_from_overflow(
                            ?AAE_PRIORITY,
                            QName,
                            State#state.queue_overflow,
                            {Q2, 0, OC}),
                    UpdLQs =
                        lists:keyreplace(
                            QName,
                            1,
                            State#state.queue_local,
                            {QName, {P1, UpdLQ, P3}}),
                    {reply,
                        RE,
                        State#state{
                            queue_local = UpdLQs,
                            queue_overflow = UpdOFlowQs}};
                {{Q1, LC, OC}, P2, P3} when LC > 0 ->
                    {{value, RE}, UpdQ} = queue:out(Q1),
                    UpdLQs =
                        lists:keyreplace(
                            QName,
                            1,
                            State#state.queue_local,
                            {QName, {{UpdQ, LC - 1, OC}, P2, P3}}),
                    {reply, RE, State#state{queue_local = UpdLQs}};
                {{Q1, 0, OC}, P2, P3} when OC > 0 ->
                    {RE, UpdLQ, UpdOFlowQs} =
                        fetch_from_overflow(
                            ?FLD_PRIORITY,
                            QName,
                            State#state.queue_overflow,
                            {Q1, 0, OC}),
                    UpdLQs =
                        lists:keyreplace(
                            QName,
                            1,
                            State#state.queue_local,
                            {QName, {UpdLQ, P2, P3}}),
                    {reply,
                        RE,
                        State#state{
                            queue_local = UpdLQs,
                            queue_overflow = UpdOFlowQs}}
            end;
        false ->
            {reply, queue_empty, State}
    end;
handle_call({register_rtq, QueueName, QueueFilter}, _From, State) ->
    QFilter = State#state.queue_filtermap,
    case lists:keyfind(QueueName, 1, QFilter) of
        {QueueName, _, _} ->
            lager:warning("Attempt to register queue already present ~w",
                            [QueueName]),
            {reply, false, State};
        false ->
            LQs = State#state.queue_local,
            OQs = State#state.queue_overflow,
            QFilter0 = [{QueueName, QueueFilter, active}|QFilter],
            LQs0 = [{QueueName, empty_local_queue()}|LQs],
            OQs0 =
                [{QueueName,
                    empty_overflow_queue(QueueName, State#state.root_path)}
                    |OQs],
            {reply,
                true,
                State#state{
                    queue_filtermap = QFilter0,
                    queue_local = LQs0,
                    queue_overflow = OQs0}}
    end;
handle_call({delist_rtq, QueueName}, _From, State) ->
    QFilter = lists:keydelete(QueueName, 1, State#state.queue_filtermap),
    QLs0 = lists:keydelete(QueueName, 1, State#state.queue_local),
    QOs0 = lists:keydelete(QueueName, 1, State#state.queue_overflow),
    {reply,
        ok,
        State#state{
            queue_filtermap = QFilter,
            queue_local = QLs0,
            queue_overflow = QOs0}};
handle_call({suspend_rtq, QueueName}, _From, State) ->
    case lists:keyfind(QueueName, 1, State#state.queue_filtermap) of
        {QueueName, Filter, _} ->
            QF0 =
                lists:keyreplace(
                    QueueName,
                    1,
                    State#state.queue_filtermap,
                    {QueueName, Filter, suspended}),
            {reply, ok, State#state{queue_filtermap = QF0}};
        false ->
            {reply, not_found, State}
    end;
handle_call({resume_rtq, QueueName}, _From, State) ->
    case lists:keyfind(QueueName, 1, State#state.queue_filtermap) of
        {QueueName, Filter, _} ->
            QF0 =
                lists:keyreplace(
                    QueueName,
                    1,
                    State#state.queue_filtermap,
                    {QueueName, Filter, active}),
            {reply, ok, State#state{queue_filtermap = QF0}};
        false ->
            {reply, not_found, State}
    end;
handle_call({clear_rtq, QueueName}, _From, State) ->
    case lists:keyfind(QueueName, 1, State#state.queue_filtermap) of
        false ->
            lager:warning("Attempt to clear queue not present ~w",
                            [QueueName]),
            {reply, ok, State};
        {QueueName, _, _} ->
            {QueueName, _LQ} =
                lists:keyfind(QueueName, 1, State#state.queue_local),
            {QueueName, OQ} =
                lists:keyfind(QueueName, 1, State#state.queue_overflow),
            LQs =
                lists:keyreplace(
                    QueueName,
                    1,
                    State#state.queue_local,
                    {QueueName, empty_local_queue()}),
            RootPath = State#state.root_path,
            ok = riak_kv_overflow_queue:close(RootPath, OQ),
            OQs =
                lists:keyreplace(
                    QueueName,
                    1,
                    State#state.queue_overflow,
                    {QueueName,
                        empty_overflow_queue(QueueName, RootPath)}),
            {OL, QL} = get_limits(),
            {reply,
                ok,
                State#state{
                    queue_local = LQs,
                    queue_overflow = OQs,
                    object_limit = OL,
                    queue_limit = QL}}
    end;
handle_call(stop, _From, State) ->
    {stop, normal, ok, State}.


handle_cast({rtq_coordput, Bucket, ReplEntry}, State) ->
    QueueNames =
        find_queues(Bucket, State#state.queue_filtermap, []),
    AddFun =
        fun(QueueName, AccState) ->
            {QueueName, LQ} =
                lists:keyfind(QueueName, 1, AccState#state.queue_local),
            case element(?RTQ_PRIORITY, LQ) of
                {_Q, LC, OC} when (LC + OC) >= State#state.queue_limit ->
                    _ = riak_kv_stat:update({ngrrepl_srcdiscard, 1}),
                    AccState;
                {Q, LC, OC} when LC >= State#state.object_limit; OC > 0 ->
                    {QueueName, OverflowQ} =
                        lists:keyfind(
                            QueueName,
                            1,
                            AccState#state.queue_overflow),
                    UpdOverflowQ =
                        riak_kv_overflow_queue:addto_queue(
                            ?RTQ_PRIORITY,
                            filter_on_objectlimit(ReplEntry),
                            OverflowQ),
                    UpdOverflowQueues =
                        lists:keyreplace(
                            QueueName,
                            1,
                            AccState#state.queue_overflow,
                            {QueueName, UpdOverflowQ}),
                    UpdLQs =
                        lists:keyreplace(
                            QueueName,
                            1,
                            AccState#state.queue_local,
                            {QueueName,
                                setelement(
                                    ?RTQ_PRIORITY,
                                    LQ,
                                    {Q, LC, OC + 1})}),
                    AccState#state{
                        queue_overflow = UpdOverflowQueues,
                        queue_local = UpdLQs};
                {Q, LC, 0} ->
                    UpdLQs =
                        lists:keyreplace(
                            QueueName,
                            1,
                            AccState#state.queue_local,
                            {QueueName,
                                setelement(
                                    ?RTQ_PRIORITY,
                                    LQ,
                                    {queue:in(ReplEntry, Q), LC + 1, 0})}),
                    AccState#state{queue_local = UpdLQs}
            end
        end,
    {noreply, lists:foldl(AddFun, State, QueueNames)}.

handle_info(log_queue, State) ->
    LogFun =
        fun({QueueName, _QF, _Status}) ->
            {QueueName, QLs} =
                lists:keyfind(QueueName, 1, State#state.queue_local),
            MapFun =
                fun(I) ->
                    queue_lengths(QLs, I)
                end,
            [P1L, P2L, P3L] =
                lists:map(
                    MapFun,
                    [?FLD_PRIORITY, ?AAE_PRIORITY, ?RTQ_PRIORITY]),
            lager:info(
                "QueueName=~w has queue sizes p1=~w p2=~w p3=~w",
                [QueueName, P1L, P2L, P3L])
        end,
    lists:foreach(LogFun, State#state.queue_filtermap),
    erlang:send_after(State#state.log_frequency_in_ms, self(), log_queue),
    {noreply, State}.

format_status(normal, [_PDict, State]) ->
    State;
format_status(terminate, [_PDict, State]) ->
    State#state{
        queue_local = not_logged,
        queue_overflow = not_logged}.

terminate(_Reason, State) ->
    lists:foreach(
        fun({_QN, OQ}) ->
            riak_kv_overflow_queue:close(State#state.root_path, OQ)
        end,
        State#state.queue_overflow),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%============================================================================
%%% Internal functions
%%%============================================================================

%% @doc
%% Return the queue names which are active for this bucket
-spec find_queues(riak_object:bucket(),
                    list(queue_filtermap()),
                    list(queue_name())) -> list(queue_name()).
find_queues(_Bucket, [], ActiveQueues) ->
    ActiveQueues;
find_queues(Bucket, [{_QN, _QF, suspended}|Rest], ActiveQueues) ->
    find_queues(Bucket, Rest, ActiveQueues);
find_queues(Bucket, [{QN, any, _}|Rest], ActiveQueues) ->
    find_queues(Bucket, Rest, [QN|ActiveQueues]);
find_queues(Bucket, [{QN, {bucketname, Bucket}, _}|Rest], ActiveQueues) ->
    find_queues(Bucket, Rest, [QN|ActiveQueues]);
find_queues({T, Bucket}, [{QN, {bucketname, Bucket}, _}|Rest], ActiveQueues) ->
    find_queues({T, Bucket}, Rest, [QN|ActiveQueues]);
find_queues({Type, B}, [{QN, {buckettype, Type}, _}|Rest], ActiveQueues) ->
    find_queues({Type, B}, Rest, [QN|ActiveQueues]);
find_queues({T, Bucket},
            [{QN, {bucketprefix, Prefix}, _}|Rest], ActiveQueues) ->
    PS = byte_size(Prefix),
    case Bucket of
        <<Prefix:PS/binary, _R/binary>> ->
            find_queues({T, Bucket}, Rest, [QN|ActiveQueues]);
        _ ->
            find_queues({T, Bucket}, Rest, ActiveQueues)
    end;
find_queues(Bucket,
            [{QN, {bucketprefix, Prefix}, _}|Rest], ActiveQueues) ->
    PS = byte_size(Prefix),
    case Bucket of
        <<Prefix:PS/binary, _R/binary>> ->
            find_queues(Bucket, Rest, [QN|ActiveQueues]);
        _ ->
            find_queues(Bucket, Rest, ActiveQueues)
    end;
find_queues(Bucket, [_H|Rest], ActiveQueues) ->
    find_queues(Bucket, Rest, ActiveQueues).


-spec filter_on_objectlimit(repl_entry()) -> repl_entry().
filter_on_objectlimit({B, K, VC, {object, _RObj}}) ->
    {B, K, VC, to_fetch};
filter_on_objectlimit(ReplEntry) ->
    ReplEntry.

%% @doc convert the tokenised string of queue definitions into actual queue
%% tokenised string expected to be of form:
% "queuename:filter_type.filter_defn|queuename:filter_type.filter_defn etc"
-spec tokenise_queuedefn(string()) -> list(queue_filtermap()).
tokenise_queuedefn(QueueDefnString) ->
    QueueStrings = string:tokens(QueueDefnString, "|"),
    SplitQueueDefnFun =
        fun(QueueString, Acc) ->
            case string:tokens(QueueString, ":") of
                [QueueName, QueueFilter] ->
                    case string:tokens(QueueFilter, ".") of
                        ["any"] ->
                            [{list_to_atom(QueueName), any, active}|Acc];
                        ["block_rtq"] ->
                            [{list_to_atom(QueueName), block_rtq, active}|Acc];
                        ["bucketname", BucketName] ->
                            [{list_to_atom(QueueName),
                                {bucketname, list_to_binary(BucketName)},
                                active}|Acc];
                        ["bucketprefix", Prefix] ->
                            [{list_to_atom(QueueName),
                                {bucketprefix,
                                    list_to_binary(Prefix)},
                                active}|Acc];
                        ["buckettype", Type] ->
                            [{list_to_atom(QueueName),
                                {buckettype, list_to_binary(Type)},
                                active}|Acc];
                        Unexpected ->
                            lager:warning(
                                "Unsupported queue definition ~w ignored",
                                [Unexpected]),
                            Acc
                    end;
                Unexpected ->
                    lager:warning(
                                "Unsupported queue definition ~w ignored",
                                [Unexpected]),
                    Acc
            end
        end,
    lists:foldl(SplitQueueDefnFun, [], QueueStrings).

-spec queue_lengths(
    {queue_local_detail(), queue_local_detail(), queue_local_detail()},
    queue_priority()) -> non_neg_integer().
queue_lengths(LocalQueues, Priority) ->
    {_Q, LC, OC} = element(Priority, LocalQueues),
    LC + OC.

-spec fetch_from_overflow(
    queue_priority(),
    queue_name(),
    list(queue_overflow()),
    queue_local_detail()) ->
        {repl_entry(),
            queue_local_detail(),
            list(queue_overflow())}.
fetch_from_overflow(Priority, QName, OFlowQs, {Q, 0, OC}) ->
    {QName, OFlowQ} = lists:keyfind(QName, 1, OFlowQs),
    case riak_kv_overflow_queue:fetch_batch(Priority, ?BATCH_SIZE, OFlowQ) of
        {[Head|Rest], UpdOFlowQ} ->
            UpdLQ =
                lists:foldl(fun(RE, AccQ) -> queue:in(RE, AccQ) end, Q, Rest),
            UpdOFlowQs =
                lists:keyreplace(QName, 1, OFlowQs, {QName, UpdOFlowQ}),
            {Head,
                {UpdLQ, length(Rest), OC - length(Rest) - 1},
                UpdOFlowQs};
        {empty, UpdOFlowQ} ->
            UpdOFlowQs =
                lists:keyreplace(QName, 1, OFlowQs, {QName, UpdOFlowQ}),
            {queue_empty,
                {Q, 0, 0},
                UpdOFlowQs}
    end.

-spec empty_local_queue() ->
    {queue_local_detail(), queue_local_detail(), queue_local_detail()}.
empty_local_queue() ->
    {{queue:new(), 0, 0}, {queue:new(), 0, 0}, {queue:new(), 0, 0}}.

-spec empty_overflow_queue(queue_name(), string())
        -> riak_kv_overflow_queue:overflowq(). 
empty_overflow_queue(QueueName, FilePath) ->
    {_OL, QL} = get_limits(),
    Priorities = [?FLD_PRIORITY, ?AAE_PRIORITY, ?RTQ_PRIORITY],
    MemLimit = min(?MEMORY_LIMIT, QL div 10),
    riak_kv_overflow_queue:new(
                    Priorities,
                    filename:join(FilePath, atom_to_list(QueueName)),
                    MemLimit,
                    QL).

-spec get_limits() -> {non_neg_integer(), non_neg_integer()}.
get_limits() ->
    QL = app_helper:get_env(riak_kv, replrtq_overflow_limit, ?QUEUE_LIMIT),
    OL = min(app_helper:get_env(
                riak_kv,
                replrtq_srcobjectlimit,
                ?OBJECT_LIMIT), QL),
    {OL, QL}.

%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).

-define(TB1, <<"Bucket1">>).
-define(TB2, <<"Bucket2">>).
-define(TB3, <<"Bucket3">>).
-define(TT1, <<"BkType1">>).
-define(TT2, <<"BkType2">>).
-define(QN1, pablo).
-define(QN2, patto).
-define(QN3, ponto).
-define(QN4, posko).
-define(QN5, punto).

generate_replentryfun(Bucket) ->
    fun(SQN) ->
        {Bucket, <<SQN:32/integer>>, vclock:fresh(test, SQN), to_fetch}
    end.


start_rtq() ->
    FilePath = riak_kv_test_util:get_test_dir("replrtq_eunit"),
    gen_server:start({local, ?MODULE}, ?MODULE, [FilePath], []).

format_status_test() ->
    start_rtq(),
    {status, _, {module, gen_server}, SItemL} =
        sys:get_status(riak_kv_replrtq_src),
    S = lists:keyfind(state, 1, lists:nth(5, SItemL)),
    ?assert(is_list(S#state.queue_local)),
    ?assert(is_list(S#state.queue_overflow)),
    ST = format_status(terminate, [dict:new(), S]),
    ?assertMatch(not_logged, ST#state.queue_local),
    ?assertMatch(not_logged, ST#state.queue_overflow),
    stop().

basic_singlequeue_test() ->
    start_rtq(),
    Grp1 = lists:map(generate_replentryfun(?TB1), lists:seq(1, 10)),
    Grp2 = lists:map(generate_replentryfun(?TB2), lists:seq(11, 20)),
    Grp3 = lists:map(generate_replentryfun(?TB3), lists:seq(21, 30)),
    Grp4 = lists:map(generate_replentryfun(?TB1), lists:seq(31, 40)),
    Grp5 = lists:map(generate_replentryfun(?TB2), lists:seq(41, 50)),
    ?assertMatch(true, register_rtq(?QN1, any)),
    lists:foreach(fun(RE) -> replrtq_coordput(RE) end, Grp1),
    ok = replrtq_ttaaefs(?QN1, lists:reverse(Grp2)),
    {?TB1, <<1:32/integer>>, _VC1, to_fetch} = popfrom_rtq(?QN1),
    ?assertMatch({?QN1, {0, 10, 9}}, length_rtq(?QN1)),
    {?TB1, <<2:32/integer>>, _VC2, to_fetch} = popfrom_rtq(?QN1),
    ?assertMatch({?QN1, {0, 10, 8}}, length_rtq(?QN1)),
    ok = replrtq_ttaaefs(?QN1, lists:reverse(Grp3)),
    {?TB1, <<3:32/integer>>, _VC3, to_fetch} = popfrom_rtq(?QN1),
    ?assertMatch({?QN1, {0, 20, 7}}, length_rtq(?QN1)),
    lists:foreach(fun(_I) -> popfrom_rtq(?QN1) end, lists:seq(1, 7)),
    ?assertMatch({?QN1, {0, 20, 0}}, length_rtq(?QN1)),
    {?TB2, <<11:32/integer>>, _VC11, to_fetch} = popfrom_rtq(?QN1),
    ?assertMatch({?QN1, {0, 19, 0}}, length_rtq(?QN1)),
    lists:foreach(fun(_I) -> popfrom_rtq(?QN1) end, lists:seq(1, 9)),
    {?TB3, <<21:32/integer>>, _VC21, to_fetch} = popfrom_rtq(?QN1),
    ?assertMatch({?QN1, {0, 9, 0}}, length_rtq(?QN1)),
    lists:foreach(fun(RE) -> replrtq_coordput(RE) end, Grp4),
    {?TB1, <<31:32/integer>>, _VC31, to_fetch} = popfrom_rtq(?QN1),
    ?assertMatch({?QN1, {0, 9, 9}}, length_rtq(?QN1)),
    lists:foreach(fun(_I) -> popfrom_rtq(?QN1) end, lists:seq(1, 17)),
    {?TB3, <<30:32/integer>>, _VC30, to_fetch} = popfrom_rtq(?QN1),
    ?assertMatch({?QN1, {0, 0, 0}}, length_rtq(?QN1)),
    ?assertMatch(queue_empty, popfrom_rtq(?QN1)),
    ok = replrtq_ttaaefs(?QN1, lists:reverse(Grp5)),
    ?assertMatch({?QN1, {0, 10, 0}}, length_rtq(?QN1)),
    {?TB2, <<41:32/integer>>, _VC41, to_fetch} = popfrom_rtq(?QN1),
    ?assertMatch({?QN1, {0, 9, 0}}, length_rtq(?QN1)),
    stop().


basic_multiqueue_test() ->
    start_rtq(),
    ?assertMatch(true, register_rtq(?QN1, any)),
    ?assertMatch(true, register_rtq(?QN2, {buckettype, ?TT1})),
    ?assertMatch(true, register_rtq(?QN3, {buckettype, ?TT2})),
    ?assertMatch(true, register_rtq(?QN4, {bucketname, ?TB1})),

    GenB1 = generate_replentryfun({?TT1, ?TB1}),
    GenB2 = generate_replentryfun({?TT1, ?TB2}),
    GenB3 = generate_replentryfun({?TT2, ?TB2}),
    GenB4 = generate_replentryfun({<<"default">>, ?TB1}),
    GenB5 = generate_replentryfun(?TB1),
    GenB6 = generate_replentryfun(?TB3),
    Grp1 = lists:map(GenB1, lists:seq(1, 10)),
    Grp2 = lists:map(GenB2, lists:seq(11, 20)),
    Grp3 = lists:map(GenB3, lists:seq(21, 30)),
    Grp4 = lists:map(GenB4, lists:seq(31, 40)),
    Grp5 = lists:map(GenB5, lists:seq(41, 50)),
    Grp6 = lists:map(GenB6, lists:seq(51, 60)),

    lists:foreach(fun replrtq_coordput/1, Grp1),
    lists:foreach(fun replrtq_coordput/1, Grp2),
    lists:foreach(fun replrtq_coordput/1, Grp3),
    lists:foreach(fun replrtq_coordput/1, Grp4),
    lists:foreach(fun replrtq_coordput/1, Grp5),
    lists:foreach(fun replrtq_coordput/1, Grp6),

    % filters are applied to coordinated PUT receipts and items
    % are placed on correct queues (and sometimes multiple queues)
    ?assertMatch({?QN1, {0, 0, 60}}, length_rtq(?QN1)),
    ?assertMatch({?QN2, {0, 0, 20}}, length_rtq(?QN2)),
    ?assertMatch({?QN3, {0, 0, 10}}, length_rtq(?QN3)),
    ?assertMatch({?QN4, {0, 0, 30}}, length_rtq(?QN4)),

    % Adding a bulk based on an AAE job will be applied to the actual
    % queue regardless of filter
    Grp5A = lists:map(GenB5, lists:seq(61, 70)),
    ok = replrtq_ttaaefs(?QN2, lists:reverse(Grp5A)),

    ?assertMatch({?QN1, {0, 0, 60}}, length_rtq(?QN1)),
    ?assertMatch({?QN2, {0, 10, 20}}, length_rtq(?QN2)),
    ?assertMatch({?QN3, {0, 0, 10}}, length_rtq(?QN3)),
    ?assertMatch({?QN4, {0, 0, 30}}, length_rtq(?QN4)),

    ?assertMatch(ok, delist_rtq(?QN1)),
    ?assertMatch(false, length_rtq(?QN1)),
    ?assertMatch(queue_empty, popfrom_rtq(?QN1)),

    Grp6A = lists:map(GenB6, lists:seq(71, 80)),
    lists:foreach(fun replrtq_coordput/1, Grp6A),

    ?assertMatch(false, length_rtq(?QN1)),
    ?assertMatch({?QN2, {0, 10, 20}}, length_rtq(?QN2)),
    ?assertMatch({?QN3, {0, 0, 10}}, length_rtq(?QN3)),
    ?assertMatch({?QN4, {0, 0, 30}}, length_rtq(?QN4)),

    % Prompt the log, and the process doesn't crash
    ?MODULE ! log_queue,

    % Delisting a non-existent queue doens't cause an error
    ?assertMatch(ok, delist_rtq(?QN1)),

    % Re-register queue, but should now be empty
    ?assertMatch(true, register_rtq(?QN1, {bucketname, ?TB3})),
    ?assertMatch(queue_empty, popfrom_rtq(?QN1)),
    ?assertMatch({?QN1, {0, 0, 0}}, length_rtq(?QN1)),

    % Add more onto the queue, confirm we can pop an element off it still
    lists:foreach(fun replrtq_coordput/1, Grp6A),
    ?assertMatch({?QN1, {0, 0, 10}}, length_rtq(?QN1)),
    {?TB3, <<71:32/integer>>, _VC71, to_fetch} = popfrom_rtq(?QN1),
    ?assertMatch({?QN1, {0, 0, 9}}, length_rtq(?QN1)),

    ?assertMatch({?QN2, {0, 10, 20}}, length_rtq(?QN2)),
    ?assertMatch({?QN3, {0, 0, 10}}, length_rtq(?QN3)),
    ?assertMatch({?QN4, {0, 0, 30}}, length_rtq(?QN4)),

    % Now suspend the queue, rather than delist it
    ?assertMatch(ok, suspend_rtq(?QN1)),

    % Can still pop from the queue whilst suspended
    {?TB3, <<72:32/integer>>, _VC72, to_fetch} = popfrom_rtq(?QN1),
    ?assertMatch({?QN1, {0, 0, 8}}, length_rtq(?QN1)),
    % But don't write to it when suspended
    Grp6B = lists:map(GenB6, lists:seq(81, 90)),
    Grp6C = lists:map(GenB6, lists:seq(91, 100)),
    lists:foreach(fun(RE) -> replrtq_coordput(RE) end, Grp6B),
    ok = replrtq_ttaaefs(?QN1, lists:reverse(Grp6C)),
    {?TB3, <<73:32/integer>>, _VC73, to_fetch} = popfrom_rtq(?QN1),
    ?assertMatch({?QN1, {0, 0, 7}}, length_rtq(?QN1)),

    % No errors if you suspend it twice
    ?assertMatch(ok, suspend_rtq(?QN1)),
    ?assertMatch({?QN1, {0, 0, 7}}, length_rtq(?QN1)),

    % Resume and can continue to pop, but also now write
    ?assertMatch(ok, resume_rtq(?QN1)),
    {?TB3, <<74:32/integer>>, _VC74, to_fetch} = popfrom_rtq(?QN1),
    ?assertMatch({?QN1, {0, 0, 6}}, length_rtq(?QN1)),
    lists:foreach(fun replrtq_coordput/1, Grp6B),
    ok = replrtq_ttaaefs(?QN1, lists:reverse(Grp6C)),
    {?TB3, <<75:32/integer>>, _VC75, to_fetch} = popfrom_rtq(?QN1),
    ?assertMatch({?QN1, {0, 10, 15}}, length_rtq(?QN1)),

    % Shrug your shoulders if it is resumed twice
    ?assertMatch(ok, resume_rtq(?QN1)),
    % and if something which isn't defined is resumed
    ?assertMatch(not_found, resume_rtq(?QN5)),
    % An undefined queue is also empty
    ?assertMatch(queue_empty, popfrom_rtq(?QN5)),
    % An undefined queue will not be suspended
    ?assertMatch(not_found, suspend_rtq(?QN5)),
    stop().

limit_coordput_test() ->
    start_rtq(),
    ?assertMatch(true, register_rtq(?QN1, any)),
    GenB1 = generate_replentryfun({?TT1, ?TB1}),
    Grp1 = lists:map(GenB1, lists:seq(1, 100000)),

    lists:foreach(fun replrtq_coordput/1, Grp1),
    ?assertMatch({?QN1, {0, 0, 100000}}, length_rtq(?QN1)),

    % At the limit so the next addition should be ignored
    NextAddition = GenB1(100001),
    ok = replrtq_coordput(NextAddition),
    ?assertMatch({?QN1, {0, 0, 100000}}, length_rtq(?QN1)),

    % If we now consume from the queue, the next addition can be made
    {{?TT1, ?TB1}, <<1:32/integer>>, _VC1, to_fetch} = popfrom_rtq(?QN1),
    ?assertMatch({?QN1, {0, 0, 99999}}, length_rtq(?QN1)),
    ok = replrtq_coordput(NextAddition),
    ?assertMatch({?QN1, {0, 0, 100000}}, length_rtq(?QN1)),
    stop().

limit_aaefold_test() ->
    start_rtq(),
    ?assertMatch(true, register_rtq(?QN1, any)),
    GenB1 = generate_replentryfun({?TT1, ?TB1}),
    Grp1 = lists:map(GenB1, lists:seq(1, 90000)),
    Grp2 = lists:map(GenB1, lists:seq(1, 9000)),
    Grp3 = lists:map(GenB1, lists:seq(1, 2000)),
    Grp4 = lists:map(GenB1, lists:seq(1, 2000)),

    ok = replrtq_aaefold(?QN1, Grp1),
    ?assertMatch({?QN1, {90000, 0, 0}}, length_rtq(?QN1)),
    ok = replrtq_aaefold(?QN1, Grp2),
    ?assertMatch({?QN1, {99000, 0, 0}}, length_rtq(?QN1)),

    lists:foreach(fun replrtq_coordput/1, Grp3),
    ?assertMatch({?QN1, {99000, 0, 2000}}, length_rtq(?QN1)),

    ok = replrtq_aaefold(?QN1, Grp4),
    ?assertMatch({?QN1, {100000, 0, 2000}}, length_rtq(?QN1)),
    
    lists:foreach(fun(_I) -> _ = popfrom_rtq(?QN1) end, lists:seq(1, 4000)),
    ?assertMatch({?QN1, {98000, 0, 0}}, length_rtq(?QN1)),

    ok = replrtq_aaefold(?QN1, Grp4),
    ?assertMatch({?QN1, {100000, 0, 0}}, length_rtq(?QN1)),
    stop().

limit_ttaaefs_test() ->
    start_rtq(),
    ?assertMatch(true, register_rtq(?QN1, any)),
    GenB1 = generate_replentryfun({?TT1, ?TB1}),
    Grp1 = lists:map(GenB1, lists:seq(1, 90000)),
    Grp2 = lists:map(GenB1, lists:seq(1, 9000)),
    Grp3 = lists:map(GenB1, lists:seq(1, 2000)),
    Grp4 = lists:map(GenB1, lists:seq(1, 2000)),

    ok = replrtq_ttaaefs(?QN1, Grp1),
    ?assertMatch({?QN1, {0, 90000, 0}}, length_rtq(?QN1)),
    ok = replrtq_ttaaefs(?QN1, Grp2),
    ?assertMatch({?QN1, {0, 99000, 0}}, length_rtq(?QN1)),

    lists:foreach(fun replrtq_coordput/1, Grp3),
    ?assertMatch({?QN1, {0, 99000, 2000}}, length_rtq(?QN1)),

    ok = replrtq_ttaaefs(?QN1, Grp4),
    ?assertMatch({?QN1, {0, 100000, 2000}}, length_rtq(?QN1)),
    
    lists:foreach(fun(_I) -> _ = popfrom_rtq(?QN1) end, lists:seq(1, 4000)),
    ?assertMatch({?QN1, {0, 98000, 0}}, length_rtq(?QN1)),

    ok = replrtq_ttaaefs(?QN1, Grp4),
    ?assertMatch({?QN1, {0, 100000, 0}}, length_rtq(?QN1)),

    lists:foreach(fun(_I) -> _ = popfrom_rtq(?QN1) end, lists:seq(1, 100000)),
    ?assertMatch({?QN1, {0, 0, 0}}, length_rtq(?QN1)),

    ok = replrtq_ttaaefs(?QN1, Grp4),
    ?assertMatch({?QN1, {0, 2000, 0}}, length_rtq(?QN1)),
    
    lists:foreach(fun(_I) -> _ = popfrom_rtq(?QN1) end, lists:seq(1, 2000)),
    ?assertMatch({?QN1, {0, 0, 0}}, length_rtq(?QN1)),

    ?assertMatch(queue_empty, popfrom_rtq(?QN1)),

    stop().

clear_queue_test() ->
    start_rtq(),
    ?assertMatch(true, register_rtq(?QN1, any)),
    GenB1 = generate_replentryfun({?TT1, ?TB1}),
    Grp1 = lists:map(GenB1, lists:seq(1, 100000)),
    Grp2 = lists:map(GenB1, lists:seq(1, 100000)),

    ok = replrtq_ttaaefs(?QN1, Grp1),
    ?assertMatch({?QN1, {0, 100000, 0}}, length_rtq(?QN1)),

    ok = clear_rtq(?QN1),
    ?assertMatch({?QN1, {0, 0, 0}}, length_rtq(?QN1)),

    ok = replrtq_ttaaefs(?QN1, Grp2),
    ?assertMatch({?QN1, {0, 100000, 0}}, length_rtq(?QN1)),

    lists:foreach(fun(_I) -> _ = popfrom_rtq(?QN1) end, lists:seq(1, 100000)),
    ?assertMatch({?QN1, {0, 0, 0}}, length_rtq(?QN1)),

    ?assertMatch(queue_empty, popfrom_rtq(?QN1)),

    stop().

wrong_overflowq_size_test() ->
    FilePath = riak_kv_test_util:get_test_dir("replrtq_eunit"),
    OFlowQs = [{?QN1, empty_overflow_queue(?QN1, FilePath)}],
    fetch_from_overflow(?RTQ_PRIORITY, ?QN1, OFlowQs, {queue:new(), 0, 2}),
    {queue_empty, {_Q, 0, 0}, _OFQ} =
        fetch_from_overflow(?RTQ_PRIORITY, ?QN1, OFlowQs, {queue:new(), 0, 2}).



parse_queuedefinition_test() ->
    Str1 = "cluster_a:bucketprefix.user|cluster_b:any|" ++
            "cluster_c:buckettype.repl|cluster_d:bucketname,wrongdelim|" ++
            "cluster_e.bucketname.name",
    QFM = tokenise_queuedefn(Str1),
    ?assertMatch([cluster_a, cluster_b],
                    find_queues(<<"userdetails">>, QFM, [])),
    ?assertMatch([cluster_a, cluster_b],
                    find_queues({<<"type">>, <<"userdetails">>}, QFM, [])),
    ?assertMatch([cluster_a, cluster_b, cluster_c],
                    find_queues({<<"repl">>, <<"userdetails">>}, QFM, [])).



-endif.
