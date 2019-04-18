%% -------------------------------------------------------------------
%%
%% riak_kv_replrt1_src: Source of replication updates from this node
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

%% @doc Queue any replictaion changes emitting from thsis node due to
%% a PUT being co-ordinated on this node, a full-sync exchange initiated on
%% this node, or an aae_fold replication fold running on vnodes on this
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
            replrtq_aaefold/3,
            replrtq_ttaefs/3,
            replrtq_coordput/2,
            register_rtq/3,
            delist_rtq/2,
            suspend_rtq/2,
            resume_rtq/2,
            length_rtq/2,
            popfrom_rtq/2]).

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
-define(LOG_TIMER_SECONDS, 30).
    % Log the queue sizes every 30 seconds

-record(state,  {
            queue_filtermap = [] :: list(queue_filtermap()),
            queue_countmap = [] :: list(queue_countmap()),
            queue_map = [] :: list(queue_map()),
            queue_limit = ?QUEUE_LIMIT :: pos_integer()
}).

-type(priority() :: integer()).
-type(squeue() :: {queue, [any()], [any()]}).
-type(pqueue() ::  squeue() | {pqueue, [{priority(), squeue()}]}).

-type ref() :: pid()|atom().
-type queue_name() :: atom().
-type object_ref() ::
    {object, riak_object:riak_object()}|{vnode, {integer(), atom()}}|to_fetch.
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
-type all_filter() :: any.
-type queue_filter() :: type_filter()|bucket_filter()|all_filter().
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
-spec replrtq_aaefold(ref(), queue_name(), list(repl_entry())) -> ok.
replrtq_aaefold(Ref, QueueName, ReplEntries) ->
    % This is a call as we don't want this process to be able to overload the src
    case gen_server:call(Ref, {rtq_aaefold, QueueName, ReplEntries}, infinity) of
        pause ->
            timer:sleep(?BACKOFF_PAUSE),
            ok;
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
-spec replrtq_ttaefs(ref(), queue_name(), list(repl_entry())) -> ok.
replrtq_ttaefs(Ref, QueueName, ReplEntries) ->
    % This is a call as we don't want this process to be able to overload the src
    case gen_server:call(Ref, {rtq_ttaaefs, QueueName, ReplEntries}, infinity) of
        pause ->
            timer:sleep(?BACKOFF_PAUSE),
            ok;
        ok ->
            ok
    end.

%% @doc
%% Add a single repl_entry associated with a PUT coordinated on this node. 
%% Never wait for the response or backoff - replictaion should be asynchronous
%% and never slow the PUT path on the src cluster.
-spec replrtq_coordput(ref(), repl_entry()) -> ok.
replrtq_coordput(Ref, ReplEntry) ->
    gen_server:cast(Ref, {rtq_coordput, ReplEntry}).

%% @doc
%% Setup a queue with a given queuename, whcih will take coordput repl_entries
%% that pass the given filter.
-spec register_rtq(ref(), queue_name(), queue_filter()) -> boolean().
register_rtq(Ref, QueueName, QueueFilter) ->
    gen_server:call(Ref, {register_rtq, QueueName, QueueFilter}, infinity).

%% @doc
%% Remove the registered queue from the configuration, deleting any outstanding
%% items on the queue
-spec delist_rtq(ref(), queue_name()) -> ok.
delist_rtq(Ref, QueueName) ->
    gen_server:call(Ref, {delist_rtq, QueueName}, infinity).

%% @doc
%% Suspend a queue form receiving further updates, all attempted updates after
%% the suspension will be discarded.  The queue will remain listed, and
%% consumption form the queue may continue
-spec suspend_rtq(ref(), queue_name()) -> boolean().
suspend_rtq(Ref, QueueName) ->
    gen_server:call(Ref, {suspend_rtq, QueueName}, infinity).

%% @doc
%% Where a queue has been suspended, allow again for new updates to be added
%% to the queue.  A null operation if the queue is not suspended.
-spec resume_rtq(ref(), queue_name()) -> boolean().
resume_rtq(Ref, QueueName) ->
    gen_server:call(Ref, {resume_rtq, QueueName}, infinity).

%% @doc
%% Return the {fold_lengrh, aae_length, put_length} to show the length of the
%% queue under each priority for that queue
-spec length_rtq(ref(), queue_name()) -> {queue_name(), queue_length()}|false.
length_rtq(Ref, QueueName) ->
    gen_server:call(Ref, {length_rtq, QueueName}).

%% @doc
%% Pop an entry from the queue with given queue name.  The entry will be taken
%% from the hihgest priority queue with entries
-spec popfrom_rtq(ref(), queue_name()) -> repl_entry()|queue_empty.
popfrom_rtq(Ref, QueueName) ->
    gen_server:call(Ref, {popfrom_rtq, QueueName}, infinity).


%%%============================================================================
%%% gen_server callbacks
%%%============================================================================

init([]) ->
    erlang:send_after(?LOG_TIMER_SECONDS * 1000, self(), log_queue),
    {ok, #state{}}.

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
                    {reply, empty, State#state{queue_map = QueueMap0}}
            end;
        false ->
            {reply, empty, State}
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
            {reply, true, State#state{queue_filtermap = QF0}};
        false ->
            {reply, false, State}
    end;
handle_call({resume_rtq, QueueName}, _From, State) ->
    case lists:keyfind(QueueName, 1, State#state.queue_filtermap) of
        {QueueName, Filter, _} ->
            QF0 = lists:keyreplace(QueueName, 1, State#state.queue_filtermap,
                                    {QueueName, Filter, active}),
            {reply, true, State#state{queue_filtermap = QF0}};
        false ->
            {reply, false, State}
    end.


handle_cast({rtq_coordput, ReplEntry}, State) ->
    QueueNames =
        find_queues(element(1, ReplEntry), State#state.queue_filtermap, []),
    {QueueMap, QueueCountMap} =
        addto_queues(ReplEntry, ?RTQ_PRIORITY,
                        QueueNames,
                        State#state.queue_map, State#state.queue_countmap,
                        State#state.queue_limit),
    {noreply, State#state{queue_map = QueueMap,
                            queue_countmap = QueueCountMap}}.

handle_info(log_queue, State) ->
    LogFun = 
        fun({QueueName, {P1Q, P2Q, P3Q}}) ->
            lager:info("QueueName=~w has queue sizes p1=~w p2=~w p3=~w",
                        [QueueName, P1Q, P2Q, P3Q])
        end,
    lists:foreach(LogFun, State#state.queue_countmap),
    erlang:send_after(?LOG_TIMER_SECONDS * 1000, self(), log_queue),
    {noreply, State}.

terminate(normal, _State) ->
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
                    pos_integer())
                                -> {list(queue_map()), list(queue_countmap())}.
addto_queues(_ReplEntry, _P, [], QueueMap, QueueCountMap, _Limit) ->
    {QueueMap, QueueCountMap};
addto_queues(ReplEntry, P, [QueueName|Rest], QueueMap, QueueCountMap, Limit) ->
    {QueueName, Counters} = lists:keyfind(QueueName, 1, QueueCountMap),
    case element(P, Counters) of
        C when C >= Limit ->
            addto_queues(ReplEntry, P, Rest, QueueMap, QueueCountMap, Limit);
        C ->
            C0 = setelement(P, Counters, C + 1),
            QCM0 =
                lists:keyreplace(QueueName, 1, QueueCountMap, {QueueName, C0}),
            {QueueName, Q} = lists:keyfind(QueueName, 1, QueueMap),
            Q0 = riak_core_priority_queue:in(ReplEntry, P, Q),
            QM0 = 
                lists:keyreplace(QueueName, 1, QueueMap, {QueueName, Q0}),
            addto_queues(ReplEntry, P, Rest, QM0, QCM0, Limit)
    end.

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
            % TODO: Incorporate this step into a riak_core_priority_queue
            % function, rather than have secret knowledge of internals here
            % Secret knowledge includes priority is inversed
            Adds = {pqueue, [{-P, {queue, ReplEntries, []}}]},
            Q0 = riak_core_priority_queue:join(Q, Adds),
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
    {ok, P} = gen_server:start(?MODULE, [], []),
    Grp1 = lists:map(generate_replentryfun(?TB1), lists:seq(1, 10)),
    Grp2 = lists:map(generate_replentryfun(?TB2), lists:seq(11, 20)),
    Grp3 = lists:map(generate_replentryfun(?TB3), lists:seq(21, 30)),
    Grp4 = lists:map(generate_replentryfun(?TB1), lists:seq(31, 40)),
    Grp5 = lists:map(generate_replentryfun(?TB2), lists:seq(41, 50)),
    ?assertMatch(true, register_rtq(P, ?QN1, any)),
    lists:foreach(fun(RE) -> replrtq_coordput(P, RE) end, Grp1),
    ok = replrtq_ttaefs(P, ?QN1, lists:reverse(Grp2)),
    {?TB1, <<1:32/integer>>, _VC1, to_fetch} = popfrom_rtq(P, ?QN1),
    ?assertMatch({?QN1, {0, 10, 9}}, length_rtq(P, ?QN1)),
    {?TB1, <<2:32/integer>>, _VC2, to_fetch} = popfrom_rtq(P, ?QN1),
    ?assertMatch({?QN1, {0, 10, 8}}, length_rtq(P, ?QN1)),
    ok = replrtq_ttaefs(P, ?QN1, lists:reverse(Grp3)),
    {?TB1, <<3:32/integer>>, _VC3, to_fetch} = popfrom_rtq(P, ?QN1),
    ?assertMatch({?QN1, {0, 20, 7}}, length_rtq(P, ?QN1)),
    lists:foreach(fun(_I) -> popfrom_rtq(P,?QN1) end, lists:seq(1, 7)),
    ?assertMatch({?QN1, {0, 20, 0}}, length_rtq(P, ?QN1)),
    {?TB2, <<11:32/integer>>, _VC11, to_fetch} = popfrom_rtq(P, ?QN1),
    ?assertMatch({?QN1, {0, 19, 0}}, length_rtq(P, ?QN1)),
    lists:foreach(fun(_I) -> popfrom_rtq(P,?QN1) end, lists:seq(1, 9)),
    {?TB3, <<21:32/integer>>, _VC21, to_fetch} = popfrom_rtq(P, ?QN1),
    ?assertMatch({?QN1, {0, 9, 0}}, length_rtq(P, ?QN1)),
    lists:foreach(fun(RE) -> replrtq_coordput(P, RE) end, Grp4),
    {?TB1, <<31:32/integer>>, _VC31, to_fetch} = popfrom_rtq(P, ?QN1),
    ?assertMatch({?QN1, {0, 9, 9}}, length_rtq(P, ?QN1)),
    lists:foreach(fun(_I) -> popfrom_rtq(P,?QN1) end, lists:seq(1, 17)),
    {?TB3, <<30:32/integer>>, _VC30, to_fetch} = popfrom_rtq(P, ?QN1),
    ?assertMatch({?QN1, {0, 0, 0}}, length_rtq(P, ?QN1)),
    ?assertMatch(empty, popfrom_rtq(P, ?QN1)),
    ok = replrtq_ttaefs(P, ?QN1, lists:reverse(Grp5)),
    ?assertMatch({?QN1, {0, 10, 0}}, length_rtq(P, ?QN1)),
    {?TB2, <<41:32/integer>>, _VC41, to_fetch} = popfrom_rtq(P, ?QN1),
    ?assertMatch({?QN1, {0, 9, 0}}, length_rtq(P, ?QN1)).


basic_multiqueue_test() ->
    {ok, P} = gen_server:start(?MODULE, [], []),
    ?assertMatch(true, register_rtq(P, ?QN1, any)),
    ?assertMatch(true, register_rtq(P, ?QN2, {buckettype, ?TT1})),
    ?assertMatch(true, register_rtq(P, ?QN3, {buckettype, ?TT2})),
    ?assertMatch(true, register_rtq(P, ?QN4, {bucketname, ?TB1})),
    
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

    lists:foreach(fun(RE) -> replrtq_coordput(P, RE) end, Grp1),
    lists:foreach(fun(RE) -> replrtq_coordput(P, RE) end, Grp2),
    lists:foreach(fun(RE) -> replrtq_coordput(P, RE) end, Grp3),
    lists:foreach(fun(RE) -> replrtq_coordput(P, RE) end, Grp4),
    lists:foreach(fun(RE) -> replrtq_coordput(P, RE) end, Grp5),
    lists:foreach(fun(RE) -> replrtq_coordput(P, RE) end, Grp6),

    % filters are applied to coordinated PUT receipts and items
    % are placed on correct queues (and sometimes multiple queues)
    ?assertMatch({?QN1, {0, 0, 60}}, length_rtq(P, ?QN1)),
    ?assertMatch({?QN2, {0, 0, 20}}, length_rtq(P, ?QN2)),
    ?assertMatch({?QN3, {0, 0, 10}}, length_rtq(P, ?QN3)),
    ?assertMatch({?QN4, {0, 0, 30}}, length_rtq(P, ?QN4)),
    
    % Adding a bulk base don an AAE job will be applied to the actual
    % queue regardless of filter
    Grp5A = lists:map(GenB5, lists:seq(61, 70)),
    ok = replrtq_ttaefs(P, ?QN2, lists:reverse(Grp5A)),

    ?assertMatch({?QN1, {0, 0, 60}}, length_rtq(P, ?QN1)),
    ?assertMatch({?QN2, {0, 10, 20}}, length_rtq(P, ?QN2)),
    ?assertMatch({?QN3, {0, 0, 10}}, length_rtq(P, ?QN3)),
    ?assertMatch({?QN4, {0, 0, 30}}, length_rtq(P, ?QN4)),
    
    ?assertMatch(ok, delist_rtq(P, ?QN1)),
    ?assertMatch(false, length_rtq(P, ?QN1)),
    ?assertMatch(empty, popfrom_rtq(P, ?QN1)),

    Grp6A = lists:map(GenB6, lists:seq(71, 80)),
    lists:foreach(fun(RE) -> replrtq_coordput(P, RE) end, Grp6A),

    ?assertMatch(false, length_rtq(P, ?QN1)),
    ?assertMatch({?QN2, {0, 10, 20}}, length_rtq(P, ?QN2)),
    ?assertMatch({?QN3, {0, 0, 10}}, length_rtq(P, ?QN3)),
    ?assertMatch({?QN4, {0, 0, 30}}, length_rtq(P, ?QN4)),

    % Prompt the log, and the process doesn't crash
    P ! log_queue,

    % Delisting a non-existent queue doens't cause an error
    ?assertMatch(ok, delist_rtq(P, ?QN1)),
    
    % Re-register queue, but should now be empty
    ?assertMatch(true, register_rtq(P, ?QN1, {bucketname, ?TB3})),
    ?assertMatch(empty, popfrom_rtq(P, ?QN1)),
    ?assertMatch({?QN1, {0, 0, 0}}, length_rtq(P, ?QN1)),

    % Add more onto the queue, confirm we can pop an element off it still
    lists:foreach(fun(RE) -> replrtq_coordput(P, RE) end, Grp6A),
    ?assertMatch({?QN1, {0, 0, 10}}, length_rtq(P, ?QN1)),
    {?TB3, <<71:32/integer>>, _VC71, to_fetch} = popfrom_rtq(P, ?QN1),
    ?assertMatch({?QN1, {0, 0, 9}}, length_rtq(P, ?QN1)),

    ?assertMatch({?QN2, {0, 10, 20}}, length_rtq(P, ?QN2)),
    ?assertMatch({?QN3, {0, 0, 10}}, length_rtq(P, ?QN3)),
    ?assertMatch({?QN4, {0, 0, 30}}, length_rtq(P, ?QN4)),
    
    % Now suspend the queue, rather than delist it
    ?assertMatch(true, suspend_rtq(P, ?QN1)),

    % Can still pop from the queue whilst suspended
    {?TB3, <<72:32/integer>>, _VC72, to_fetch} = popfrom_rtq(P, ?QN1),
    ?assertMatch({?QN1, {0, 0, 8}}, length_rtq(P, ?QN1)),
    % But don't write to it when suspended
    Grp6B = lists:map(GenB6, lists:seq(81, 90)),
    Grp6C = lists:map(GenB6, lists:seq(91, 100)),
    lists:foreach(fun(RE) -> replrtq_coordput(P, RE) end, Grp6B),
    ok = replrtq_ttaefs(P, ?QN1, lists:reverse(Grp6C)),
    {?TB3, <<73:32/integer>>, _VC73, to_fetch} = popfrom_rtq(P, ?QN1),
    ?assertMatch({?QN1, {0, 0, 7}}, length_rtq(P, ?QN1)),
    
    % No errors if you suspend it twice
    ?assertMatch(true, suspend_rtq(P, ?QN1)),
    ?assertMatch({?QN1, {0, 0, 7}}, length_rtq(P, ?QN1)),

    % Resume and can continue to pop, but also now write
    ?assertMatch(true, resume_rtq(P, ?QN1)),
    {?TB3, <<74:32/integer>>, _VC74, to_fetch} = popfrom_rtq(P, ?QN1),
    ?assertMatch({?QN1, {0, 0, 6}}, length_rtq(P, ?QN1)),
    lists:foreach(fun(RE) -> replrtq_coordput(P, RE) end, Grp6B),
    ok = replrtq_ttaefs(P, ?QN1, lists:reverse(Grp6C)),
    {?TB3, <<75:32/integer>>, _VC75, to_fetch} = popfrom_rtq(P, ?QN1),
    ?assertMatch({?QN1, {0, 10, 15}}, length_rtq(P, ?QN1)),
    
    % Shrug your shoulders if it is resumed twice
    ?assertMatch(true, resume_rtq(P, ?QN1)),
    % and if something which isn't defined is resumed
    ?assertMatch(false, resume_rtq(P, ?QN5)),
    % An undefined queue is also empty
    ?assertMatch(empty, popfrom_rtq(P, ?QN5)),
    % An undefined queue will not be suspended
    ?assertMatch(false, suspend_rtq(P, ?QN5)).

limit_coordput_test() ->
    {ok, P} = gen_server:start(?MODULE, [], []),
    ?assertMatch(true, register_rtq(P, ?QN1, any)),
    GenB1 = generate_replentryfun({?TT1, ?TB1}),
    Grp1 = lists:map(GenB1, lists:seq(1, 100000)),

    lists:foreach(fun(RE) -> replrtq_coordput(P, RE) end, Grp1),
    ?assertMatch({?QN1, {0, 0, 100000}}, length_rtq(P, ?QN1)),
    
    % At the limit so the next addition should be ignored
    NextAddition = GenB1(100001),
    ok = replrtq_coordput(P, NextAddition),
    ?assertMatch({?QN1, {0, 0, 100000}}, length_rtq(P, ?QN1)),

    % If we now consume from the queue, the next addition can be made
    {{?TT1, ?TB1}, <<1:32/integer>>, _VC1, to_fetch} = popfrom_rtq(P, ?QN1),
    ?assertMatch({?QN1, {0, 0, 99999}}, length_rtq(P, ?QN1)),
    ok = replrtq_coordput(P, NextAddition),
    ?assertMatch({?QN1, {0, 0, 100000}}, length_rtq(P, ?QN1)).

limit_aaefold_test() ->
    {ok, P} = gen_server:start(?MODULE, [], []),
    ?assertMatch(true, register_rtq(P, ?QN1, any)),
    GenB1 = generate_replentryfun({?TT1, ?TB1}),
    Grp1 = lists:map(GenB1, lists:seq(1, 20000)),
    Grp2 = lists:map(GenB1, lists:seq(1, 20000)),
    Grp3 = lists:map(GenB1, lists:seq(1, 20000)),
    Grp4 = lists:map(GenB1, lists:seq(1, 20000)),
    Grp5 = lists:map(GenB1, lists:seq(1, 20000)),
    Grp6 = lists:map(GenB1, lists:seq(1, 1000)),

    SW1 = os:timestamp(),
    ok = replrtq_aaefold(P, ?QN1, lists:reverse(Grp1)),
    _SW2 = os:timestamp(),
    ok = replrtq_aaefold(P, ?QN1, lists:reverse(Grp2)),
    SW3 = os:timestamp(),
    ok = replrtq_aaefold(P, ?QN1, lists:reverse(Grp3)),
    _SW4 = os:timestamp(),
    ok = replrtq_aaefold(P, ?QN1, lists:reverse(Grp4)),
    _SW5 = os:timestamp(),
    ok = replrtq_aaefold(P, ?QN1, lists:reverse(Grp5)),
    SW6 = os:timestamp(),
    UnPaused = timer:now_diff(SW3, SW1) div 1000,
    Paused = timer:now_diff(SW6, SW3) div 1000,
    ?assertMatch(true, Paused >= 3000),
    ?assertMatch(true, UnPaused < 2000),
    ?assertMatch({?QN1, {100000, 0, 0}}, length_rtq(P, ?QN1)),

    % After we are over the limit, new PUTs are paused even though the change
    % is rejected
    ok = replrtq_aaefold(P, ?QN1, lists:reverse(Grp6)),
    SW7 = os:timestamp(),
    StillPaused = timer:now_diff(SW7, SW6) div 1000,
    ?assertMatch(true, StillPaused >= 1000),
    ?assertMatch({?QN1, {100000, 0, 0}}, length_rtq(P, ?QN1)),
    
    % Unload enough space for an unpaused addition
    {{?TT1, ?TB1}, <<1:32/integer>>, _VC1, to_fetch} = popfrom_rtq(P, ?QN1),
    lists:foreach(fun(_I) -> popfrom_rtq(P, ?QN1) end, lists:seq(1, 51000)),
    SW8 = os:timestamp(),
    ok = replrtq_aaefold(P, ?QN1, lists:reverse(Grp6)),
    SW9 = os:timestamp(),
    NowUnPaused = timer:now_diff(SW9, SW8) div 1000,
    ?assertMatch(true, NowUnPaused < 1000),
    ?assertMatch({?QN1, {49999, 0, 0}}, length_rtq(P, ?QN1)).




-endif.