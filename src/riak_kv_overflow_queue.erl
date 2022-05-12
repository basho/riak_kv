%% -------------------------------------------------------------------
%%
%% riak_kv_overflow_queue: A version of queue which overflows to disk
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

%% @doc A wrap around queue so that when a queue limit is
%% reached, the queue is kept on disk to reduce the memory overheads


-module(riak_kv_overflow_queue).

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").
-export([get_mqueue/1]).

-endif.

-export([new/4,
        log/5,
        addto_queue/3,
        work/4,
        stats/1,
        format_state/1,
        close/2,
        fetch_batch/3]).

-define(QUEUE_LIMIT, 1000).
-define(OVERFLOW_LIMIT, 1000000).
-define(OVERFLOW_BATCH, 256).
-define(DISKLOG_EXT, ".dlg").

-type priority() :: pos_integer().
-type continuation() :: start|disk_log:continuation().
-type filename() :: file:filename()|none.
-type queue_stats() :: list({pos_integer(), non_neg_integer()}).

-record(overflowq,  
    {
        mqueues
            :: list({priority(), queue:queue()}) | not_logged,
        mqueue_lengths
            :: queue_stats(),
        overflow_lengths
            :: queue_stats(),
        overflow_discards
            :: queue_stats(),
        mqueue_limit
            :: non_neg_integer(),
        overflow_limit
            :: non_neg_integer(),
        overflow_filepath
            :: string()|undefined,
        overflow_files
            :: list({priority(), {filename(), continuation()}})
}).

-type overflowq() :: #overflowq{}.

-export_type([overflowq/0, queue_stats/0]).


%%%============================================================================
%%% Exported functions
%%%============================================================================

%% @doc Setup a new overflow queue
%% It is important to specify the priorities correctly, a crash will be caused
%% if an unspecified priority is used in any other functions within this
%% module.
-spec new(list(pos_integer()), string(), pos_integer(), pos_integer())
                -> overflowq().
new(Priorities, FilePath, QueueLimit, OverflowLimit) ->
    InitCounts = lists:map(fun(P) -> {P, 0} end, Priorities),
    InitFiles = lists:map(fun(P) -> {P, {none, start}} end, Priorities),
    InitQueues = lists:map(fun(P) -> {P, queue:new()} end, Priorities),
    ok = filelib:ensure_dir(FilePath ++ "/"),

    #overflowq{mqueues = InitQueues,
                mqueue_limit = QueueLimit,
                overflow_limit = OverflowLimit,
                overflow_filepath = FilePath,
                mqueue_lengths = InitCounts,
                overflow_lengths = InitCounts,
                overflow_discards = InitCounts,
                overflow_files = InitFiles}.

%% @doc Log the current counters, and reset the discard counts
-spec log(atom(), non_neg_integer(),
            non_neg_integer(), non_neg_integer(),
            overflowq()) -> overflowq().
log(Type, JobID, Attempts, Aborts, Queue) ->
    QueueLengths =
        lists:foldl(fun({P, L}, Acc) ->
                        [Acc, io_lib:format("queue_p~w=~w ", [P, L])]
                    end,
                    "Queue lengths ",
                    Queue#overflowq.mqueue_lengths),
    OverflowLengths =
        lists:foldl(fun({P, L}, Acc) ->
                        [Acc, io_lib:format("overflow_p~w=~w ", [P, L])]
                    end,
                    "Overflow lengths ",
                    Queue#overflowq.overflow_lengths),
    DiscardCounts =
        lists:foldl(fun({P, L}, Acc) ->
                        [Acc, io_lib:format("discard_p~w=~w ", [P, L])]
                    end,
                    "Discard counts ",
                    Queue#overflowq.overflow_discards),

    _ = lager:info(lists:flatten(["~p job_id=~p has ",
                        "attempts=~w aborts=~w ",
                        QueueLengths,
                        OverflowLengths,
                        DiscardCounts]),
                    [Type, JobID, Attempts, Aborts]),
    
    ResetDiscards =
        lists:map(fun({P, _L}) -> {P, 0} end,
                    Queue#overflowq.overflow_discards),

    Queue#overflowq{overflow_discards = ResetDiscards}.


-spec stats(overflowq()) -> list({atom(), queue_stats()}).
stats(Queue) ->
    [{mqueue_lengths, Queue#overflowq.mqueue_lengths},
        {overflow_lengths, Queue#overflowq.overflow_lengths},
        {overflow_discards, Queue#overflowq.overflow_discards}].

%% @doc add an item to the queue, adding to the overflow file if an overflow
%% file is in use at this priority, or if the addition will take the current
%% queue length over the limit
-spec addto_queue(pos_integer(), term(), overflowq()) -> overflowq().
addto_queue(Priority, Item, FlowQ) ->
    MQueueLimit = FlowQ#overflowq.mqueue_limit,
    MQueueLengths = FlowQ#overflowq.mqueue_lengths,
    {Priority, CurrentQL} = 
        lists:keyfind(Priority, 1, MQueueLengths),
    OverflowFiles = FlowQ#overflowq.overflow_files,
    MQueues = FlowQ#overflowq.mqueues,
    {Priority, {OverflowFile, OverflowC}} =
        lists:keyfind(Priority, 1, OverflowFiles),
    
    case {OverflowFile, CurrentQL} of
        {none, CurrentQL} when CurrentQL < MQueueLimit ->
            UpdQueueLengths =
                update_plist({Priority, CurrentQL + 1}, MQueueLengths),
            {Priority, MQueue} =
                lists:keyfind(Priority, 1, MQueues),
            UpdMQueues =
                update_plist({Priority, queue:in(Item, MQueue)}, MQueues),
            FlowQ#overflowq{mqueue_lengths = UpdQueueLengths,
                            mqueues = UpdMQueues};
        {OverflowFile, CurrentQL} ->
            {MaybeNewFile, MaybeNewCont} =
                case OverflowFile of
                    none ->
                        open_overflow(FlowQ#overflowq.overflow_filepath);
                    _ ->
                        {OverflowFile, OverflowC}
                end,
            OverflowCounts = FlowQ#overflowq.overflow_lengths,
            {Priority, OflowCount} =
                lists:keyfind(Priority, 1, OverflowCounts),
            case OflowCount >= FlowQ#overflowq.overflow_limit of
                true ->
                    OflowDiscards = FlowQ#overflowq.overflow_discards,
                    {Priority, DiscardCount} =
                        lists:keyfind(Priority, 1, OflowDiscards),
                    UpdOflowDiscards =
                        update_plist({Priority, DiscardCount + 1},
                            OflowDiscards),
                    FlowQ#overflowq{overflow_discards = UpdOflowDiscards};
                false ->
                    ok = disk_log:alog(MaybeNewFile, Item),
                    UpdOverflowFiles =
                        update_plist({Priority, {MaybeNewFile, MaybeNewCont}},
                            OverflowFiles),
                    UpdOverflowCounts =
                        update_plist({Priority, OflowCount + 1},
                            OverflowCounts),
                    FlowQ#overflowq{overflow_lengths = UpdOverflowCounts,
                                    overflow_files = UpdOverflowFiles}
            end
    end.


%% @doc Look at queue by priority and batch size, and apply the passed in
%% function to a batch of up to batch size available on the queue for that
%% priority.
%% If the ApplyFun returns true to indicate an attempt has been made, count
%% as such.  Otherwise, count as an abort and requeue.
-spec work(list({pos_integer(), pos_integer()}),
            fun((term()) -> boolean()),
            fun((term()) -> ok),
            overflowq()) ->
                        {pos_integer()|none,
                            non_neg_integer(),
                            non_neg_integer(),
                            overflowq()}.
work([], _ApplyFun, _RequeueFun, FlowQ) ->
    {none, 0, 0, FlowQ};
work([{P, BatchSize}|Rest], ApplyFun, RequeueFun, FlowQ) ->
    BatchFoldFun =
        fun(DelRef, {AT, AB}) ->
            case ApplyFun(DelRef) of
                true ->
                    {AT + 1, AB};
                false ->
                    RequeueFun(DelRef),
                    {AT, AB + 1}
            end
        end,
    {Batch, UpdFlowQ} = fetch_batch(P, BatchSize, FlowQ),
    case Batch of
        Batch when is_list(Batch) ->
            {Attempts, Aborts} = lists:foldl(BatchFoldFun, {0, 0}, Batch),
            {P, Attempts, Aborts, UpdFlowQ};
        empty ->
            work(Rest, ApplyFun, RequeueFun, UpdFlowQ)
    end.

-spec format_state(overflowq()) -> overflowq().
format_state(FlowQ) ->
    FlowQ#overflowq{mqueues = not_logged}.

-spec close(string(), overflowq()) -> ok.
close(FilePath, FlowQ) ->
    CloseFun =
        fun({_P, {DL, _DC}}) ->
            case DL of
                none ->
                    ok;
                _ ->
                    disk_log:close(DL),
                    file:delete(disklog_filename(FilePath, DL))
            end
        end,
    lists:foreach(CloseFun, FlowQ#overflowq.overflow_files).

%% @doc Fetch a batch of items from the queue.  If there are less than batch
%% size items on the queue, then the queue may first be re-loaded with items
%% from the overflow file, if such a file exists.
%% The batch will be taken from the given priority queue with items available.
%% An incomplete batch does not mean there are not items still ready to be
%% processed.
%% In some cases, when overflow files are not in use, items may also come from
%% a lower priority queue
-spec fetch_batch(pos_integer(), pos_integer(), overflowq()) ->
                    {empty|list(term()), overflowq()}.
fetch_batch(Priority, MaxBatchSize, FlowQ) ->
    UpdFlowQ = maybereload_queue(Priority, MaxBatchSize, FlowQ),
    case lists:keyfind(Priority, 1, UpdFlowQ#overflowq.mqueue_lengths) of
        {Priority, 0} -> 
            {empty, UpdFlowQ};
        {Priority, MQueueL} ->
            BatchSize = min(MQueueL, MaxBatchSize),
            MQueues = UpdFlowQ#overflowq.mqueues,
            {Priority, MQueue} =
                lists:keyfind(Priority, 1, MQueues),
            {UpdMQueue, UpdMQueueL, Batch} =
                lists:foldl(fun(_X, {Q, QL, Acc}) ->
                                case queue:out(Q) of
                                    {{value, Item}, Q0} ->
                                        {Q0, QL - 1, [Item|Acc]};
                                    {empty, Q0} ->
                                        {Q0, 0, Acc}
                                end
                            end,
                            {MQueue, MQueueL, []},
                            lists:seq(1, BatchSize)),
            UpdMQueueLengths =
                update_plist({Priority, UpdMQueueL},
                                UpdFlowQ#overflowq.mqueue_lengths),
            UpdMQueues =
                update_plist({Priority, UpdMQueue}, MQueues),
            {lists:reverse(Batch),
                UpdFlowQ#overflowq{mqueues = UpdMQueues,
                                    mqueue_lengths=UpdMQueueLengths}}
    end.

%%%============================================================================
%%% Internal functions
%%%============================================================================

-spec maybereload_queue(pos_integer(), pos_integer(), overflowq()) ->
                            overflowq().
maybereload_queue(Priority, BatchSize, FlowQ) ->
    MQueueLengths = FlowQ#overflowq.mqueue_lengths,
    OverflowFiles = FlowQ#overflowq.overflow_files,
    case {lists:keyfind(Priority, 1, MQueueLengths),
            lists:keyfind(Priority, 1, OverflowFiles)} of
        {{Priority, N}, _} when N > BatchSize ->
            %% There are enough items on the queue, don't reload
            FlowQ;
        {{Priority, _N}, {Priority, {none, start}}} ->
            %% There are no overflow files to reload from 
            FlowQ;
        {{Priority, N}, {Priority, {File, Continuation}}} ->
            %% Attempt to refill the queue from the overflow file
            OverflowCounts = FlowQ#overflowq.overflow_lengths,
            {Priority, OflowCount} =
                lists:keyfind(Priority, 1, OverflowCounts),
            ChunkSize = max(?OVERFLOW_BATCH, BatchSize - N),
            GetChunk = disk_log:chunk(File, Continuation, ChunkSize),
            case GetChunk of
                eof ->
                    %% The overflow file has now been emptied, and so can be
                    %% deleted
                    FilePath = FlowQ#overflowq.overflow_filepath,
                    ok = disk_log:close(File),
                    ok = file:delete(disklog_filename(FilePath, File)),
                    FlowQ#overflowq{
                        overflow_lengths =
                            update_plist({Priority, 0}, OverflowCounts),
                        overflow_files =
                            update_plist({Priority, {none, start}},
                                OverflowFiles)
                        };
                {UpdContinuation, Items} when is_list(Items) ->
                    %% There are items in the overflow fileto be added to the
                    %% queue
                    ItemCount = length(Items),
                    MQueueAdditions = queue:from_list(Items),
                    MQueues = FlowQ#overflowq.mqueues,
                    {Priority, MQueue} =
                        lists:keyfind(Priority, 1, MQueues),
                    UpdMQueue = queue:join(MQueue, MQueueAdditions),
                    UpdMQueues =
                        update_plist({Priority, UpdMQueue}, MQueues),
                    UpdOverflowFiles =
                        update_plist({Priority, {File, UpdContinuation}},
                            OverflowFiles),
                    UpdOverflowCounts =
                        update_plist({Priority, OflowCount - ItemCount},
                            OverflowCounts),
                    {Priority, MQueueCount} =
                        lists:keyfind(Priority, 1, MQueueLengths),
                    UpdMQueueCounts =
                        update_plist({Priority, MQueueCount + ItemCount},
                            MQueueLengths),
                    FlowQ#overflowq{
                        mqueues = UpdMQueues,
                        mqueue_lengths = UpdMQueueCounts,
                        overflow_lengths = UpdOverflowCounts,
                        overflow_files = UpdOverflowFiles}     
            end
    end.


-spec update_plist(
    {pos_integer(), term()}, list({pos_integer(), term()})) ->
        list({pos_integer(), term()}).
update_plist(UpdateTuple, PriorityList) ->
    lists:ukeysort(1, [UpdateTuple|PriorityList]).


-spec open_overflow(string()) -> {filename(), start}.
open_overflow(RootPath) ->
    GUID = generate_ordered_guid(),
    {ok, OverflowQueue} =
        disk_log:open([{name, GUID},
            {file, disklog_filename(RootPath, GUID)},
            {repair, false}]),
    {OverflowQueue, start}.

%% @doc Use a doctored GUID that includes creation time, to make it easier
%% for the operator to clean any accumulated garbage.
-spec generate_ordered_guid() -> iolist().
generate_ordered_guid() ->
    {{Year, Month, Day}, {H, M, _S}} =
        calendar:now_to_datetime(os:timestamp()),
    <<C:16, D:16, E:48>> = crypto:strong_rand_bytes(10),
    io_lib:format(
        "~4.10.0B~2.10.0B~2.10.0B-~2.10.0B~2.10.0B-4~3.16.0b-~4.16.0b-~12.16.0b",
        [Year, Month, Day, H, M, C band 16#0fff, D band 16#3fff bor 16#8000, E]).


-spec disklog_filename(string(), string()) -> filename:filename().
disklog_filename(RootPath, GUID) ->
    filename:join(RootPath, GUID ++ ?DISKLOG_EXT).


%%%============================================================================
%%% Unit tests
%%%============================================================================


-ifdef(TEST).

clean_dir(DirPath) ->
    ok = filelib:ensure_dir(DirPath ++ "/"),
    {ok, Files} = file:list_dir(DirPath),
    lists:foreach(fun(FN) ->
                        File = filename:join(DirPath, FN),
                        _ = file:delete(File)
                    end,
                    Files).

get_mqueue(OverflowQ) ->
    OverflowQ#overflowq.mqueues.

basic_inmemory_test() ->
    RootPath = riak_kv_test_util:get_test_dir("overflow_inmem"),
    clean_dir(RootPath),
    io:format("~p", [RootPath]),
    FlowQ0 = new([1, 2], RootPath, 1000, 5000),
    Refs = lists:seq(1, 100),
    FlowQ1 =
        lists:foldl(fun(R, FQ) -> addto_queue(1, R, FQ) end, FlowQ0, Refs),
    {B1, FlowQ2} = fetch_batch(2, 16, FlowQ1),
    ?assertMatch(empty, B1),

    {mqueue_lengths, MQL1} = lists:keyfind(mqueue_lengths, 1, stats(FlowQ2)),
    ?assertMatch([{1, 100}, {2, 0}], MQL1),

    {B2, FlowQ3} = fetch_batch(1, 1, FlowQ2),
    ?assertMatch([1], B2),
    {mqueue_lengths, MQL2} = lists:keyfind(mqueue_lengths, 1, stats(FlowQ3)),
    ?assertMatch([{1, 99}, {2, 0}], MQL2),
    
    {B3, FlowQ4} = fetch_batch(1, 99, FlowQ3),
    ExpB = lists:seq(2, 100),
    ?assertMatch(ExpB, B3),

    {empty, _FlowQ5} = fetch_batch(1, 1, FlowQ4),
    
    ok = filelib:ensure_dir(RootPath),
    {ok, Files} = file:list_dir(RootPath),
    
    ?assertMatch([], Files).

basic_overflow_test() ->
    RootPath = riak_kv_test_util:get_test_dir("overflow_disk/"),
    clean_dir(RootPath),
    FlowQ0 = new([1, 2], RootPath, 1000, 5000),
    Refs = lists:seq(1, 2000),
    FlowQ1 =
        lists:foldl(fun(R, FQ) -> addto_queue(1, R, FQ) end, FlowQ0, Refs),
    {B1, FlowQ2} = fetch_batch(2, 16, FlowQ1),
    ?assertMatch(empty, B1),

    {mqueue_lengths, MQL1} =
        lists:keyfind(mqueue_lengths, 1, stats(FlowQ2)),
    ?assertMatch([{1, 1000}, {2, 0}], MQL1),
    {overflow_lengths, OQL1} =
        lists:keyfind(overflow_lengths, 1, stats(FlowQ2)),
    ?assertMatch([{1, 1000}, {2, 0}], OQL1),

    close(RootPath, FlowQ2),

    ok = filelib:ensure_dir(RootPath),
    {ok, Files1} = file:list_dir(RootPath),
    ?assertMatch(0, length(Files1)),
    
    FlowQ_NEW = new([1, 2], RootPath, 1000, 5000),
    {mqueue_lengths, MQL2} =
        lists:keyfind(mqueue_lengths, 1, stats(FlowQ_NEW)),
    ?assertMatch([{1, 0}, {2, 0}], MQL2),
    
    ok = filelib:ensure_dir(RootPath),
    {ok, Files2} = file:list_dir(RootPath),
    ?assertMatch(0, length(Files2)).


underover_overflow_test() ->
    RootPath = riak_kv_test_util:get_test_dir("underover_disk"),
    clean_dir(RootPath),
    FlowQ0 = new([1, 2], RootPath, 1000, 5000),
    Refs = lists:seq(1, 7000),
    FlowQ1 =
        lists:foldl(fun(R, FQ) -> addto_queue(1, R, FQ) end, FlowQ0, Refs),
    {B1, FlowQ2} = fetch_batch(2, 16, FlowQ1),
    ?assertMatch(empty, B1),

    {mqueue_lengths, MQL1} =
        lists:keyfind(mqueue_lengths, 1, stats(FlowQ2)),
    ?assertMatch([{1, 1000}, {2, 0}], MQL1),
    {overflow_lengths, OQL1} =
        lists:keyfind(overflow_lengths, 1, stats(FlowQ2)),
    ?assertMatch([{1, 5000}, {2, 0}], OQL1),
    {overflow_discards, OQD1} =
        lists:keyfind(overflow_discards, 1, stats(FlowQ2)),
    ?assertMatch([{1, 1000}, {2, 0}], OQD1),

    {B2, FlowQ3} = fetch_batch(1, 900, FlowQ2),
    ExpB2 = lists:seq(1, 900),
    ?assertMatch(ExpB2, B2),

    {B3, FlowQ4} = fetch_batch(1, 200, FlowQ3),
    ExpB3 = lists:seq(901, 1100),
    ?assertMatch(ExpB3, B3),

    {B4, FlowQ5} = fetch_batch(1, 200, FlowQ4),
    ExpB4 = lists:seq(1101, 1300),
    ?assertMatch(ExpB4, B4),

    {B5, FlowQ6} = fetch_batch(1, 200, FlowQ5),
    ExpB5 = lists:seq(1301, 1500),
    ?assertMatch(ExpB5, B5),

    {B6, FlowQ7} = fetch_batch(1, 300, FlowQ6),
    ExpB6 = lists:seq(1501, 1800),
    ?assertMatch(ExpB6, B6),

    Refs2 = lists:seq(7001, 8000),
    FlowQ8 =
        lists:foldl(fun(R, FQ) -> addto_queue(1, R, FQ) end, FlowQ7, Refs2),
    
    {B7, FlowQ9} = fetch_batch(1, 1200, FlowQ8),
    ExpB7 = lists:seq(1801, 3000),
    ?assertMatch(ExpB7, B7),

    {B8, FlowQ10} = fetch_batch(1, 2800, FlowQ9),
    ExpB8 = lists:seq(3001, 5681),
    %% Batch size may be limited by 64KB limit!
    ?assertMatch(ExpB8, B8),

    {B9, FlowQ11} = fetch_batch(1, 219, FlowQ10),
    ExpB9 = lists:seq(5682, 5900),
    ?assertMatch(ExpB9, B9),

    {B10, FlowQ12} = fetch_batch(1, 200, FlowQ11),
    ExpB10 = lists:seq(5901, 6000) ++ lists:seq(7001, 7100),
    ?assertMatch(ExpB10, B10),

    {B10, FlowQ12} = fetch_batch(1, 200, FlowQ11),
    ExpB10 = lists:seq(5901, 6000) ++ lists:seq(7001, 7100),
    ?assertMatch(ExpB10, B10),

    {B11, FlowQ13} = fetch_batch(1, 800, FlowQ12),
    ExpB11 = lists:seq(7101, 7800),
    %% There were only 800 spaces on the queue last time
    ?assertMatch(ExpB11, B11),

    ok = filelib:ensure_dir(RootPath),
    {ok, Files1} = file:list_dir(RootPath),
    io:format("Files1 ~p", [Files1]),
    ?assertMatch(1, length(Files1)),

    {B12, FlowQ14} = fetch_batch(1, 800, FlowQ13),
    ?assertMatch(empty, B12),

    {ok, Files2} = file:list_dir(RootPath),
    %% The on disk component is empty - so the file should be removed
    ?assertMatch(0, length(Files2)),

    Refs3 = lists:seq(8001, 10000),
    FlowQ15 =
        lists:foldl(fun(R, FQ) -> addto_queue(1, R, FQ) end, FlowQ14, Refs3),
    
    {mqueue_lengths, MQL2} =
        lists:keyfind(mqueue_lengths, 1, stats(FlowQ15)),
    ?assertMatch([{1, 1000}, {2, 0}], MQL2),
    {overflow_lengths, OQL2} =
        lists:keyfind(overflow_lengths, 1, stats(FlowQ15)),
    ?assertMatch([{1, 1000}, {2, 0}], OQL2),
    {overflow_discards, OQD2} =
        lists:keyfind(overflow_discards, 1, stats(FlowQ15)),
    ?assertMatch([{1, 1200}, {2, 0}], OQD2),

    close(RootPath, FlowQ15).


-endif.