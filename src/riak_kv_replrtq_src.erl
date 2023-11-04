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

%% @doc Queue any replictaion changes emitting from this node due to
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
        code_change/3]).

-export([start_link/0,
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
            stop/0]).

-ifdef(TEST).
-export([ replrtq_aaefold/3,
          replrtq_ttaaefs/3 ]).
-endif.

-define(BACKOFF_PAUSE, 1000).
    % Pause in ms in the case the last addition to the queue took the queue's
    % size for that priority over the maximum
-define(RTQ_PRIORITY, 3).
    % Priority for queueing real-time replication of PUTs co-ordinated on this
    % node
-define(AAE_PRIORITY, 2).
    % Priority for queueing replication event prompted by a Tictac AAE fold
-define(FLD_PRIORITY, 1).
    % Priority for queueing replication event prompted by an AAE fold (e.g.
    % replicating all keys in a given range, or modified date range)
-define(QUEUE_LIMIT, 100000).
    % Maximum size of a queue for a given priority
    % Real-time replication will tail-drop when over the limit, whereas batch
    % replication queues will prompt a pause in the process queueing the
    % repl references
-define(OBJECT_LIMIT, 1000).
    % If the priority queue is bigger than the object limit, and the object
    % reference is {object, Object} then the object reference will be converted
    % into a fetch request.  Manages the number of objects being held in
    % memory.  The object limit can be altered using
    % riak_kv.replrtq_srcobjectlimit
-define(LOG_TIMER_SECONDS, 30).
    % Log the queue sizes every 30 seconds
-define(CONSUME_DELAY, 4).

-record(state,  {
            queue_filtermap = [] :: list(queue_filtermap()),
            queue_countmap = [] :: list(queue_countmap()),
            queue_map = [] :: list(queue_map()),
            queue_limit = ?QUEUE_LIMIT :: pos_integer(),
            object_limit = ?OBJECT_LIMIT :: non_neg_integer(),
            log_frequency_in_ms = ?LOG_TIMER_SECONDS * 1000 :: pos_integer()
}).

-type priority() :: integer().
-type squeue() :: {queue, [any()], [any()]}.
-type pqueue() ::  squeue() | {pqueue, [{priority(), squeue()}]}.

-type queue_name() :: atom().
-type object_ref() ::
    {tomb, riak_object:riak_object()}|
        {object, riak_object:riak_object()}|
        to_fetch.
    % The object reference can be the actual object or a request to fetch the
    % actual object using the Bucket, Key and Clock in the repl_entry
-type repl_entry() ::
    {riak_object:bucket(), riak_object:key(), vclock:vclock(), object_ref()}.
    % Normally the repl_entry will not include the object to save space on the
    % queue.  If the object is a tombstone which had been PUT, then the actual
    % object may be queued to reduce the chance that the PULL from the queue
    % on the replicating cluster loses a race with a delete_mode timeout.
    % Where the repl_entry is set to not_cached it will need to be fetched from
    % a vnode as part of the operation that PULLs from the queue.
-type type_filter() :: {buckettype, binary()}.
-type bucket_filter() :: {bucketname, binary()}.
-type prefix_filter() :: {bucketprefix, binary()}.
-type all_filter() :: any.
-type blockrtq_filter() :: block_rtq.
-type queue_filter() ::
    type_filter()|bucket_filter()|all_filter()|
        blockrtq_filter()|prefix_filter().
-type queue_length() ::
    {non_neg_integer(), non_neg_integer(), non_neg_integer()}.
-type queue_filtermap() :: {queue_name(), queue_filter(), active|suspended}.
-type queue_countmap() :: {queue_name(), queue_length()}.
-type queue_map() :: {queue_name(), pqueue()}.

-export_type([repl_entry/0, queue_name/0]).

%%%============================================================================
%%% API
%%%============================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

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
-spec replrtq_aaefold(queue_name(), list(repl_entry())) -> ok | pause.
replrtq_aaefold(QueueName, ReplEntries) ->
    replrtq_aaefold(QueueName, ReplEntries, ?BACKOFF_PAUSE).

-spec replrtq_aaefold(queue_name(), list(repl_entry()), pos_integer()) -> ok | pause.
%% @hidden
%% Used for testing if we want to have control over the length of pausing.
replrtq_aaefold(QueueName, ReplEntries, BackoffPause) ->
    % This is a call as we don't want this process to be able to overload the src
    case gen_server:call(?MODULE,
                            {rtq_aaefold, QueueName, ReplEntries},
                            infinity) of
        pause ->
            timer:sleep(BackoffPause),
            pause;
        ok ->
            ok
    end.

%% @doc
%% Add a list of repl entrys to the real-time queue with the given queue name.
%% The filter for that queue name will not be checked.
%% These entries will be added to the queue with priority 2 (higher number is
%% higher priority).
%% This should be use to replicate the outcome of Tictac AAE full-sync
%% aae_exchange.
-spec replrtq_ttaaefs(queue_name(), list(repl_entry())) -> ok | pause.
replrtq_ttaaefs(QueueName, ReplEntries) ->
    replrtq_ttaaefs(QueueName, ReplEntries,  ?BACKOFF_PAUSE).


-spec replrtq_ttaaefs(queue_name(), list(repl_entry()), pos_integer())
                                                                -> ok | pause.
%% @hidden
%% Used for testing if we want to have control over the length of pausing.
replrtq_ttaaefs(QueueName, ReplEntries, BackoffPause) ->
    % This is a call as we don't want this process to be able to overload
    % the src
    case gen_server:call(?MODULE,
                            {rtq_ttaaefs, QueueName, ReplEntries},
                            infinity) of
        pause ->
            timer:sleep(BackoffPause),
            pause;
        ok ->
            ok
    end.

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
%% consumption form the queue may continue
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

init([]) ->
    QueueDefnString = app_helper:get_env(riak_kv, replrtq_srcqueue, ""),
    QFM = tokenise_queuedefn(QueueDefnString),
    MapToQM =
        fun({QueueName, _QF, _QA}) ->
            {QueueName, riak_core_priority_queue:new()}
        end,
    MaptoQC =
        fun({QueueName, _QF, _QA}) ->
            {QueueName, {0, 0, 0}}
        end,
    QM = lists:map(MapToQM, QFM),
    QC = lists:map(MaptoQC, QFM),
    QL = app_helper:get_env(riak_kv, replrtq_srcqueuelimit, ?QUEUE_LIMIT),
    OL = app_helper:get_env(riak_kv, replrtq_srcobjectlimit, ?OBJECT_LIMIT),
    LogFreq = app_helper:get_env(riak_kv, queue_manager_log_frequency, ?LOG_TIMER_SECONDS),
    erlang:send_after(LogFreq, self(), log_queue),
    {ok, #state{queue_filtermap = QFM,
                queue_map = QM,
                queue_countmap = QC,
                queue_limit = QL,
                object_limit = OL,
                log_frequency_in_ms = LogFreq * 1000}}.

handle_call({rtq_ttaaefs, QueueName, ReplEntries}, _From, State) ->
    {ApproachingLimit, QueueMap, QueueCountMap} =
        bulkaddto_queue(ReplEntries, ?AAE_PRIORITY, QueueName,
                        State#state.queue_map,
                        State#state.queue_countmap,
                        State#state.queue_filtermap,
                        State#state.queue_limit),
    R = case ApproachingLimit of true -> pause; _ -> ok end,
    {reply, R, State#state{queue_map = QueueMap,
                            queue_countmap = QueueCountMap}};
handle_call({rtq_aaefold, QueueName, ReplEntries}, _From, State) ->
    {ApproachingLimit, QueueMap, QueueCountMap} =
        bulkaddto_queue(ReplEntries, ?FLD_PRIORITY, QueueName,
                        State#state.queue_map,
                        State#state.queue_countmap,
                        State#state.queue_filtermap,
                        State#state.queue_limit),
    R = case ApproachingLimit of true -> pause; _ -> ok end,
    {reply, R, State#state{queue_map = QueueMap,
                            queue_countmap = QueueCountMap}};
handle_call({length_rtq, QueueName}, _From, State) ->
    {reply, lists:keyfind(QueueName, 1, State#state.queue_countmap), State};
handle_call({popfrom_rtq, QueueName}, _From, State) ->
    case lists:keyfind(QueueName, 1, State#state.queue_map) of
        {QueueName, Queue} ->
            case riak_core_priority_queue:out(Queue) of
                {{value, ReplEntry}, Queue0} ->
                    QueueMap0 =
                        lists:keyreplace(QueueName, 1,
                                            State#state.queue_map,
                                            {QueueName, Queue0}),
                    {QueueName, QueueCounts} =
                        lists:keyfind(QueueName, 1,
                                        State#state.queue_countmap),
                    QueueCountMap0 =
                        lists:keyreplace(QueueName, 1,
                                            State#state.queue_countmap,
                                            {QueueName,
                                                update_counts(QueueCounts)}),
                    {reply,
                        ReplEntry,
                        State#state{queue_map = QueueMap0,
                                    queue_countmap = QueueCountMap0}};
                {empty, Queue0} ->
                    QueueMap0 =
                        lists:keyreplace(QueueName, 1,
                                            State#state.queue_map,
                                            {QueueName, Queue0}),
                    {reply, queue_empty, State#state{queue_map = QueueMap0}}
            end;
        false ->
            {reply, queue_empty, State}
    end;
handle_call({register_rtq, QueueName, QueueFilter}, _From, State) ->
    QFilter = State#state.queue_filtermap,
    QMap = State#state.queue_map,
    QCount = State#state.queue_countmap,
    case lists:keyfind(QueueName, 1, QFilter) of
        {QueueName, _, _} ->
            lager:warning("Attempt to register queue already present ~w",
                            [QueueName]),
            {reply, false, State};
        false ->
            QFilter0 = [{QueueName, QueueFilter, active}|QFilter],
            QMap0 = [{QueueName, riak_core_priority_queue:new()}|QMap],
            QCount0 = [{QueueName, {0, 0, 0}}|QCount],
            {reply, true, State#state{queue_filtermap = QFilter0,
                                        queue_map = QMap0,
                                        queue_countmap = QCount0}}
    end;
handle_call({delist_rtq, QueueName}, _From, State) ->
    QFilter = lists:keydelete(QueueName, 1, State#state.queue_filtermap),
    QMap = lists:keydelete(QueueName, 1, State#state.queue_map),
    QCount = lists:keydelete(QueueName, 1, State#state.queue_countmap),
    {reply, ok, State#state{queue_filtermap = QFilter,
                                queue_map = QMap,
                                queue_countmap = QCount}};
handle_call({suspend_rtq, QueueName}, _From, State) ->
    case lists:keyfind(QueueName, 1, State#state.queue_filtermap) of
        {QueueName, Filter, _} ->
            QF0 = lists:keyreplace(QueueName, 1, State#state.queue_filtermap,
                                    {QueueName, Filter, suspended}),
            {reply, ok, State#state{queue_filtermap = QF0}};
        false ->
            {reply, not_found, State}
    end;
handle_call({resume_rtq, QueueName}, _From, State) ->
    case lists:keyfind(QueueName, 1, State#state.queue_filtermap) of
        {QueueName, Filter, _} ->
            QF0 = lists:keyreplace(QueueName, 1, State#state.queue_filtermap,
                                    {QueueName, Filter, active}),
            {reply, ok, State#state{queue_filtermap = QF0}};
        false ->
            {reply, not_found, State}
    end;
handle_call(stop, _From, State) ->
    {stop, normal, ok, State}.


handle_cast({rtq_coordput, Bucket, ReplEntry}, State) ->
    QueueNames =
        find_queues(Bucket, State#state.queue_filtermap, []),
    {QueueMap, QueueCountMap} =
        addto_queues(ReplEntry, ?RTQ_PRIORITY,
                        QueueNames,
                        State#state.queue_map, State#state.queue_countmap,
                        State#state.queue_limit, State#state.object_limit),
    {noreply, State#state{queue_map = QueueMap,
                            queue_countmap = QueueCountMap}}.

handle_info(log_queue, State) ->
    LogFun =
        fun({_QueueName, {0, 0, 0}}) ->
                ok;
           ({QueueName, {P1Q, P2Q, P3Q}}) ->
                lager:info(
                  [{queue_name, QueueName}, {p1q, P1Q}, {p2q, P2Q}, {p3q, P3Q}],
                  "QueueName=~w has queue sizes p1=~w p2=~w p3=~w",
                  [QueueName, P1Q, P2Q, P3Q])
        end,
    lists:foreach(LogFun, State#state.queue_countmap),
    erlang:send_after(State#state.log_frequency_in_ms, self(), log_queue),
    {noreply, State}.

terminate(_Reason, _State) ->
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


%% @doc
%% Add the replication entry to any matching queue (upping the appropriate
%% counter)
-spec addto_queues(repl_entry(),
                    non_neg_integer(),
                    list(queue_name()),
                    list(queue_map()),
                    list(queue_countmap()),
                    pos_integer(),
                    non_neg_integer())
                                -> {list(queue_map()), list(queue_countmap())}.
addto_queues(_ReplEntry, _P, [], QueueMap, QueueCountMap, _QLimit, _OLimit) ->
    {QueueMap, QueueCountMap};
addto_queues(ReplEntry, P, [QueueName|Rest],
                QueueMap, QueueCountMap,
                QLimit, OLimit) ->
    {QueueName, Counters} = lists:keyfind(QueueName, 1, QueueCountMap),
    case element(P, Counters) of
        C when C >= QLimit ->
            addto_queues(ReplEntry, P, Rest,
                            QueueMap, QueueCountMap,
                            QLimit, OLimit);
        C ->
            C0 = setelement(P, Counters, C + 1),
            QCM0 =
                lists:keyreplace(QueueName, 1, QueueCountMap, {QueueName, C0}),
            {QueueName, Q} = lists:keyfind(QueueName, 1, QueueMap),
            FilteredEntry = filter_on_objectlimit(ReplEntry, C, OLimit),
            Q0 = riak_core_priority_queue:in(FilteredEntry, P, Q),
            QM0 =
                lists:keyreplace(QueueName, 1, QueueMap, {QueueName, Q0}),
            addto_queues(ReplEntry, P, Rest, QM0, QCM0, QLimit, OLimit)
    end.

-spec filter_on_objectlimit(repl_entry(), non_neg_integer(), non_neg_integer()) -> repl_entry().
filter_on_objectlimit(ReplEntry, C, OLimit) when OLimit > C ->
    ReplEntry;
filter_on_objectlimit({B, K, VC, {object, _Obj}}, _C, _OLimit) ->
    {B, K, VC, to_fetch};
filter_on_objectlimit(ReplEntry, _C, _OLimit) ->
    ReplEntry.

%% @doc
%% Add a list of repl_entries to the back of a queue.  Only add if the queue
%% is active.  do not respect the queue filter, which is only applied for
%% singular additions that have not been targetted at a named queue.
-spec bulkaddto_queue(list(repl_entry()), non_neg_integer(), queue_name(),
                        list(queue_map()),
                        list(queue_countmap()),
                        list(queue_filtermap()),
                        pos_integer()) ->
                        {boolean(), list(queue_map()), list(queue_countmap())}.
bulkaddto_queue(ReplEntries, P, QueueName,
                    QueueMap, QueueCountMap, QueueFilterMap, Limit) ->
    case lists:keyfind(QueueName, 1, QueueFilterMap) of
        {QueueName, _Filter, active} ->
            {QueueName, C} = lists:keyfind(QueueName, 1, QueueCountMap),
            {QueueName, Q} = lists:keyfind(QueueName, 1, QueueMap),
            NewQLength = element(P, C) + length(ReplEntries),
            C0 = setelement(P, C, NewQLength),
            QueueAddition = riak_core_priority_queue_from_list(ReplEntries, P),
            Q0 = riak_core_priority_queue:join(Q, QueueAddition),
            QCM0 =
                lists:keyreplace(QueueName, 1, QueueCountMap, {QueueName, C0}),
            QM0 =
                lists:keyreplace(QueueName, 1, QueueMap, {QueueName, Q0}),
            case NewQLength > Limit of
                true ->
                    {true, QueueMap, QueueCountMap};
                false ->
                    {(NewQLength > Limit div 2), QM0, QCM0}
            end;
        _ ->
            {false, QueueMap, QueueCountMap}
    end.

riak_core_priority_queue_from_list(Entries, P) ->
    lists:foldr(fun(Entry, Acc) ->
                        riak_core_priority_queue:in(Entry, P, Acc)
                end, riak_core_priority_queue:new(), Entries).

%% @doc
%% Update the count after an item has been fetched from the queue.  The
%% queue is trusted to have given up an item based on priority correctly.
-spec update_counts(queue_length()) -> queue_length().
update_counts({0, 0, 0}) ->
    % Not possible!  Dare we let it crash?
    {0, 0, 0};
update_counts({P1, 0, 0}) ->
    {P1 - 1, 0, 0};
update_counts({P1, P2, 0}) ->
    {P1, P2 - 1, 0};
update_counts({P1, P2, P3}) ->
    {P1, P2, P3 - 1}.

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

basic_singlequeue_test() ->
    gen_server:start({local, ?MODULE}, ?MODULE, [], []),
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
    gen_server:start({local, ?MODULE}, ?MODULE, [], []),
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
    gen_server:start({local, ?MODULE}, ?MODULE, [], []),
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
    gen_server:start({local, ?MODULE}, ?MODULE, [], []),
    ?assertMatch(true, register_rtq(?QN1, any)),
    GenB1 = generate_replentryfun({?TT1, ?TB1}),
    Grp1 = lists:map(GenB1, lists:seq(1, 20000)),
    Grp2 = lists:map(GenB1, lists:seq(1, 20000)),
    Grp3 = lists:map(GenB1, lists:seq(1, 20000)),
    Grp4 = lists:map(GenB1, lists:seq(1, 20000)),
    Grp5 = lists:map(GenB1, lists:seq(1, 20000)),
    Grp6 = lists:map(GenB1, lists:seq(1, 1000)),

    SW1 = os:timestamp(),
    ok = replrtq_aaefold(?QN1, lists:reverse(Grp1)),
    _SW2 = os:timestamp(),
    ok = replrtq_aaefold(?QN1, lists:reverse(Grp2)),
    SW3 = os:timestamp(),
    pause = replrtq_aaefold(?QN1, lists:reverse(Grp3)),
    _SW4 = os:timestamp(),
    pause = replrtq_aaefold(?QN1, lists:reverse(Grp4)),
    _SW5 = os:timestamp(),
    pause = replrtq_aaefold(?QN1, lists:reverse(Grp5)),
    SW6 = os:timestamp(),
    UnPaused = timer:now_diff(SW3, SW1) div 1000,
    Paused = timer:now_diff(SW6, SW3) div 1000,
    ?assertMatch(true, Paused >= 3000),
    ?assertMatch(true, UnPaused < 2000),
    ?assertMatch({?QN1, {100000, 0, 0}}, length_rtq(?QN1)),

    % After we are over the limit, new PUTs are paused even though the change
    % is rejected
    pause = replrtq_aaefold(?QN1, lists:reverse(Grp6)),
    SW7 = os:timestamp(),
    StillPaused = timer:now_diff(SW7, SW6) div 1000,
    ?assertMatch(true, StillPaused >= 1000),
    ?assertMatch({?QN1, {100000, 0, 0}}, length_rtq(?QN1)),

    % Unload enough space for an unpaused addition
    {{?TT1, ?TB1}, <<1:32/integer>>, _VC1, to_fetch} = popfrom_rtq(?QN1),
    lists:foreach(fun(_I) -> popfrom_rtq(?QN1) end, lists:seq(1, 51000)),
    SW8 = os:timestamp(),
    ok = replrtq_aaefold(?QN1, lists:reverse(Grp6)),
    SW9 = os:timestamp(),
    NowUnPaused = timer:now_diff(SW9, SW8) div 1000,
    ?assertMatch(true, NowUnPaused < 1000),
    ?assertMatch({?QN1, {49999, 0, 0}}, length_rtq(?QN1)),
    stop().

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
