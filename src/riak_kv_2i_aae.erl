%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc Code related to repairing 2i data.
-module(riak_kv_2i_aae).
-behaviour(gen_fsm).

-include("riak_kv_wm_raw.hrl").

-export([start/2, stop/1, get_status/0, to_report/1]).

-export([next_partition/2, wait_for_aae_pid/2, wait_for_repair/3,
         wait_for_repair/2]).

%% gen_fsm callbacks
-export([init/1,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         format_status/2,
         terminate/3,
         code_change/4]).

-export_type([status/0, partition_result/0, worker_status/0]).

%% How many items to scan before possibly pausing to obey speed throttle
-define(DEFAULT_SCAN_BATCH, 1000).
-define(MAX_DUTY_CYCLE, 100).
-define(MIN_DUTY_CYCLE, 1).
-define(LOCK_RETRIES, 10).
-define(AAE_PID_TIMEOUT, 30000).
-define(INDEX_SCAN_TIMEOUT, 300000). % 5 mins, in case fold dies timeout
-define(INDEX_REFRESH_TIMEOUT, 60000).

-type worker_status() :: starting | scanning_indexes | populating_hashtree |
                         exchanging.

-type partition_result() :: {Partition :: integer(),
                             {ok,  term()} | {error, term()}}.

-type status() :: [{speed, integer()} |
                   {partitions, [integer()]} |
                   {remaining, [integer()]} |
                   {results, [partition_result()]}|
                   {current_partition, integer()} |
                   {current_partition_status, worker_status()} |
                   {index_scan_count, integer()} |
                   {hashtree_population_count, integer()} |
                   {exchange_count, integer()}].

-record(state,
        {
         speed :: integer(),
         partitions :: [integer()],
         remaining :: [integer()],
         results = [],
         caller :: pid(),
         reply_to,
         early_reply,
         aae_pid_req_id :: pid() | undefined,
         worker_monitor :: reference() | undefined,
         worker_status :: worker_status(),
         index_scan_count = 0 :: integer(),
         hashtree_population_count = 0 :: integer(),
         exchange_count = 0 :: integer(),
         open_dbs = [] :: [reference()]
        }).

-record(partition_result, {
          status :: ok | error,
          error,
          index_scan_count = 0 :: integer(),
          hashtree_population_count = 0:: integer(),
          exchange_count = 0 :: integer()
         }).


%% @doc Starts a process that will issue a repair for each partition in the
%% list, sequentially.
%% The Speed parameter controls how fast we go. 100 means full speed,
%% 50 means to do a unit of work and pause for the duration of that unit
%% to achieve a 50% duty cycle, etc.
%% If a single partition is requested, this function will block until that
%% partition has been blocked or that fails to notify the user immediately.
-spec start([integer()], integer()) -> {ok, pid()} | {error, term()}.
start(Partitions, Speed)
  when Speed >= ?MIN_DUTY_CYCLE, Speed =< ?MAX_DUTY_CYCLE ->
    Res = gen_fsm:start({local, ?MODULE}, ?MODULE,
                        [Partitions, Speed, self()], []),
    case {Res, length(Partitions)} of
        {{ok, _Pid}, 1} ->
            try
                EarlyAck = gen_fsm:sync_send_all_state_event(?MODULE,
                                                             early_ack,
                                                             infinity),
                case EarlyAck of
                    lock_acquired ->
                        Res;
                    _ ->
                        EarlyAck
                end
            catch
                exit:_ ->
                    {error, early_exit}
            end;

        _ ->
            Res
    end.

%% @doc Will stop any worker process and try to close leveldb databases.
stop(TimeOut) ->
    gen_fsm:sync_send_all_state_event(?MODULE, stop, TimeOut).

-spec get_status() -> [{Key :: atom(), Val :: term()}].
get_status() ->
    gen_fsm:sync_send_all_state_event(?MODULE, status, infinity).

init([Partitions, Speed, Caller]) ->
    process_flag(trap_exit, true),
    lager:info("Starting 2i repair at speed ~p for partitions ~p",
               [Speed, Partitions]),
    {ok, next_partition, #state{partitions=Partitions, remaining=Partitions,
                                speed=Speed, caller=Caller}, 0}.

%% @doc Notifies caller for certain cases involving a single partition
%% so the user sees an error condition from the command line.
maybe_notify(State=#state{reply_to=undefined}, Reply) ->
    State#state{early_reply=Reply};
maybe_notify(State=#state{reply_to=From}, Reply) ->
    gen_fsm:reply(From, Reply),
    State#state{reply_to=undefined}.


%% @doc Process next partition or finish if no more remaining.
next_partition(timeout, State=#state{remaining=[]}) ->
    %% We are done. Report results
    Status = to_simple_state(State),
    Report = to_report(Status),
    lager:info("Finished 2i repair:\n~s", [Report]),
    {stop, normal, State};
next_partition(timeout, State=#state{remaining=[Partition|_]}) ->
    ReqId = make_ref(),
    riak_kv_vnode:request_hashtree_pid(Partition, {fsm, ReqId, self()}),
    {next_state, wait_for_aae_pid, State#state{aae_pid_req_id=ReqId}, ?AAE_PID_TIMEOUT}.

%% @doc Waiting for vnode to send back the Pid of the AAE tree process.
wait_for_aae_pid(timeout, State) ->
    State2 = maybe_notify(State, {error, no_aae_pid}),
    {stop, no_aae_pid, State2};
wait_for_aae_pid({ReqId, {error, Err}}, State=#state{aae_pid_req_id=ReqId}) ->
    State2 = maybe_notify(State, {error, {no_aae_pid, Err}}),
    {stop, no_aae_pid, State2};
wait_for_aae_pid({ReqId, {ok, TreePid}},
                 State=#state{aae_pid_req_id=ReqId, speed=Speed,
                              remaining=[Partition|_]}) ->
    WorkerFun =
    fun() ->
            Res =
            repair_partition(Partition, Speed, ?MODULE, TreePid),
            gen_fsm:sync_send_event(?MODULE, {repair_result, Res}, infinity)
    end,
    Mon = monitor(process, spawn_link(WorkerFun)),
    {next_state, wait_for_repair, State#state{worker_monitor=Mon}}.

%% @doc Waiting for a partition repair process to finish
wait_for_repair({lock_acquired, Partition},
                _From,
                State=#state{remaining=[Partition|_]}) ->
    State2 = maybe_notify(State, lock_acquired),
    {reply, ok, wait_for_repair, State2};
wait_for_repair({repair_result, Res},
                _From,
                State=#state{remaining=[Partition|Rem],
                             results=Results,
                             worker_monitor=Mon}) ->
    State2 =
    case Res of
        {error, {lock_failed, LockErr}} ->
            maybe_notify(State, {lock_failed, LockErr});
        _ ->
            State
    end,
    demonitor(Mon, [flush]),
    {reply, ok, next_partition,
     State2#state{remaining=Rem,
                  results=[{Partition, Res}|Results],
                  worker_monitor=undefined,
                  worker_status=starting,
                  index_scan_count=0,
                  hashtree_population_count=0,
                  exchange_count=0},
     0}.

%% @doc Process async worker status updates during a repair
wait_for_repair({index_scan_update, Count}, State) ->
    {next_state, wait_for_repair,
     State#state{worker_status=scanning_indexes,
                 index_scan_count=Count}};
wait_for_repair({hashtree_population_update, Count}, State) ->
    {next_state, wait_for_repair,
     State#state{worker_status=populating_hashtree,
                 hashtree_population_count=Count}};
wait_for_repair({repair_count_update, Count}, State) ->
    {next_state, wait_for_repair,
     State#state{worker_status=exchanging,
                 exchange_count=Count}};
wait_for_repair({open_db, DBRef}, State = #state{open_dbs=DBs}) ->
    {next_state, wait_for_repair,
     State#state{open_dbs=[DBRef|DBs]}};
wait_for_repair({close_db, DBRef}, State = #state{open_dbs=DBs}) ->
    {next_state, wait_for_repair,
     State#state{open_dbs=lists:delete(DBRef, DBs)}}.

%% @doc Performs the actual repair work, called from a spawned process.
-spec repair_partition(integer(), integer(), {pid(), any()}, pid()) ->
    #partition_result{}.
repair_partition(Partition, DutyCycle, From, TreePid) ->
    case get_hashtree_lock(TreePid, ?LOCK_RETRIES) of
        ok ->
            lager:info("Acquired lock on partition ~p", [Partition]),
            gen_fsm:sync_send_event(From, {lock_acquired, Partition}, infinity),
            lager:info("Repairing indexes in partition ~p", [Partition]),
            case create_index_data_db(Partition, DutyCycle) of
                {ok, {DBDir, DBRef, IndexDBCount}} ->
                    Res =
                    case build_tmp_tree(Partition, DBRef, DutyCycle) of
                        {ok, {TmpTree, TmpTreeCount}} ->
                            Res0 = do_exchange(Partition, DBRef, TmpTree,
                                               TreePid),
                            remove_tmp_tree(Partition, TmpTree),
                            case Res0 of
                                {ok, CmpCount} ->
                                    #partition_result{
                                       status=ok,
                                       index_scan_count=IndexDBCount,
                                       hashtree_population_count=TmpTreeCount,
                                       exchange_count=CmpCount};
                                {error, TmpTreeErr} ->
                                    #partition_result{
                                       status=error,
                                       error=TmpTreeErr,
                                       index_scan_count=IndexDBCount,
                                       hashtree_population_count=TmpTreeCount}
                            end;
                        {error, BuildTreeErr} ->
                            #partition_result{status=error,
                                              error=BuildTreeErr,
                                              index_scan_count=IndexDBCount}
                    end,
                    destroy_index_data_db(DBDir, DBRef),
                    lager:info("Finished repairing indexes in partition ~p",
                               [Partition]),
                    Res;
                {error, CreateDBErr} ->
                    #partition_result{status=error, error=CreateDBErr}
            end;
        LockErr ->
            lager:error("Failed to acquire hashtree lock on partition ~p",
                        [Partition]),
            #partition_result{status=error, error=LockErr}
    end.

%% No wait for speed 100, wait equal to work time when speed is 50,
%% wait equal to 99 times the work time when speed is 1.
wait_factor(DutyCycle) ->
    if
        %% Avoid potential floating point funkiness for full speed.
        DutyCycle >= ?MAX_DUTY_CYCLE -> 0;
        true -> (100 - DutyCycle) / DutyCycle
    end.

scan_batch() ->
    app_helper:get_env(riak_kv, aae_2i_batch_size, ?DEFAULT_SCAN_BATCH).

%% @doc Create temporary DB holding 2i index data from disk.
create_index_data_db(Partition, DutyCycle) ->
    DBDir = filename:join(data_root(), "tmp_db"),
    (catch eleveldb:destroy(DBDir, [])),
    case filelib:ensure_dir(DBDir) of
        ok ->
            create_index_data_db(Partition, DutyCycle, DBDir);
        _ ->
            {error, io_lib:format("Could not create directory ~s", [DBDir])}
    end.

create_index_data_db(Partition, DutyCycle, DBDir) ->
    lager:info("Creating temporary database of 2i data in ~s", [DBDir]),
    DBOpts = leveldb_opts(),
    case eleveldb:open(DBDir, DBOpts) of
        {ok, DBRef} ->
            create_index_data_db(Partition, DutyCycle, DBDir, DBRef);
        {error, DBErr} ->
            {error, DBErr}
    end.

create_index_data_db(Partition, DutyCycle, DBDir, DBRef) ->
    Client = self(),
    BatchRef = make_ref(),
    WaitFactor = wait_factor(DutyCycle),
    ScanBatch = scan_batch(),
    Fun =
    fun(Bucket, Key, Field, Term, Count) ->
            BKey = term_to_binary({Bucket, Key}),
            OldVal = fetch_index_data(BKey, DBRef),
            Val = [{Field, Term}|OldVal],
            ok = eleveldb:put(DBRef, BKey, term_to_binary(Val), []),
            Count2 = Count + 1,
            case Count2 rem ScanBatch of
                0 when DutyCycle < ?MAX_DUTY_CYCLE ->
                    Mon = monitor(process, Client),
                    Client ! {BatchRef, self(), Count2},
                    receive
                        {BatchRef, continue} ->
                            ok;
                        {'DOWN', Mon, _, _, _} ->
                            throw(fold_receiver_died)
                    end,
                    demonitor(Mon, [flush]);
                _ ->
                    ok
            end,
            Count2
    end,
    lager:info("Grabbing all index data for partition ~p", [Partition]),
    Ref = make_ref(),
    Sender = {raw, Ref, Client},
    StartTime = now(),
    riak_core_vnode_master:command({Partition, node()},
                                   {fold_indexes, Fun, 0},
                                   Sender,
                                   riak_kv_vnode_master),
    NumFound = wait_for_index_scan(Ref, BatchRef, StartTime, WaitFactor),
    case NumFound of
        {error, _} = Err ->
            destroy_index_data_db(DBDir, DBRef),
            Err;
        _ ->
            lager:info("Grabbed ~p index data entries from partition ~p",
                       [NumFound, Partition]),
            {ok, {DBDir, DBRef, NumFound}}
    end.

leveldb_opts() ->
    ConfigOpts = app_helper:get_env(riak_kv,
                                    anti_entropy_leveldb_opts,
                                    [{write_buffer_size, 20 * 1024 * 1024},
                                     {max_open_files, 20}]),
    Config0 = orddict:from_list(ConfigOpts),
    ForcedOpts = [{create_if_missing, true}, {error_if_exists, true}],
    Config =
    lists:foldl(fun({K,V}, Cfg) ->
                        orddict:store(K, V, Cfg)
                end, Config0, ForcedOpts),
    Config.

duty_cycle_pause(WaitFactor, StartTime) ->
    case WaitFactor > 0 of
        true ->
            Now = now(),
            ElapsedMicros = timer:now_diff(Now, StartTime),
            WaitMicros = ElapsedMicros * WaitFactor,
            WaitMillis = trunc(WaitMicros / 1000 + 0.5),
            timer:sleep(WaitMillis);
        false ->
            ok
    end.

%% @doc Async notify this FSM of an update.
-spec send_event(any()) -> ok.
send_event(Event) ->
    gen_fsm:send_event(?MODULE, Event).

%% @doc Waiting for 2i data scanning updates and final result.
wait_for_index_scan(Ref, BatchRef, StartTime, WaitFactor) ->
    receive
        {BatchRef, Pid, Count} ->
            duty_cycle_pause(WaitFactor, StartTime),
            Pid ! {BatchRef, continue},
            send_event({index_scan_update, Count}),
            wait_for_index_scan(Ref, BatchRef, now(), WaitFactor);
        {Ref, Result} ->
            Result
    after
        ?INDEX_SCAN_TIMEOUT ->
            {error, index_scan_timeout}
    end.

-spec fetch_index_data(BK :: binary(), reference()) -> term().
fetch_index_data(BK, DBRef) ->
    % Let it crash on leveldb error
    case eleveldb:get(DBRef, BK, []) of
        {ok, BinVal} ->
            binary_to_term(BinVal);
        not_found ->
            []
    end.

%% @doc Remove all traces of the temporary 2i index DB
destroy_index_data_db(DBDir, DBRef) ->
    catch eleveldb:close(DBRef),
    eleveldb:destroy(DBDir, []).

%% @doc Returns the base data directory to use inside the AAE directory.
data_root() ->
    BaseDir = riak_kv_index_hashtree:determine_data_root(),
    filename:join(BaseDir, "2i").

%% @doc Builds a temporary hashtree based on the 2i data fetched from the
%% backend.
build_tmp_tree(Index, DBRef, DutyCycle) ->
    lager:info("Building tree for 2i data on disk for partition ~p", [Index]),
    %% Build temporary hashtree with this index data.
    TreeId = <<0:176/integer>>,
    Path = filename:join(data_root(), integer_to_list(Index)),
    catch eleveldb:destroy(Path, []),
    case filelib:ensure_dir(Path) of
        ok ->
            Tree = hashtree:new({Index, TreeId}, [{segment_path, Path}]),
            WaitFactor = wait_factor(DutyCycle),
            BatchSize = scan_batch(),
            FoldFun =
            fun({K, V}, {Count, TreeAcc, StartTime}) ->
                    Indexes = binary_to_term(V),
                    Hash = riak_kv_index_hashtree:hash_index_data(Indexes),
                    Tree2 = hashtree:insert(K, Hash, TreeAcc),
                    Count2 = Count + 1,
                    StartTime2 =
                    case Count2 rem BatchSize of
                        0 ->
                            send_event({hashtree_population_update, Count2}),
                            duty_cycle_pause(WaitFactor, StartTime),
                            now();
                        _ ->
                            StartTime
                    end,
                    {Count2, Tree2, StartTime2}
            end,
            send_event({hashtree_population_update, 0}),
            {Count, Tree2, _} = eleveldb:fold(DBRef, FoldFun,
                                              {0, Tree, erlang:now()}, []),
            lager:info("Done building temporary tree for 2i data "
                       "with ~p entries",
                       [Count]),
            {ok, {Tree2, Count}};
        _ ->
            {error, io_lib:format("Could not create directory ~s", [Path])}
    end.

%% @doc Remove all traces of the temporary hashtree for 2i index data.
remove_tmp_tree(Partition, Tree) ->
    lager:debug("Removing temporary AAE 2i tree for partition ~p", [Partition]),
    hashtree:close(Tree),
    hashtree:destroy(Tree).

%% @doc Run exchange between the temporary 2i tree and the vnode's 2i tree.
-spec do_exchange(integer(), reference(), hashtree:hashtree(), pid()) ->
    {ok, integer()} | {error, any()}.
do_exchange(Partition, DBRef, TmpTree0, TreePid) ->
    lager:info("Reconciling 2i data"),
    IndexN2i = riak_kv_index_hashtree:index_2i_n(),
    lager:debug("Updating the vnode tree"),
    ok = riak_kv_index_hashtree:update(IndexN2i, TreePid),
    lager:debug("Updating the temporary 2i AAE tree"),
    TmpTree = hashtree:update_tree(TmpTree0),
    Remote =
    fun(get_bucket, {L, B}) ->
            R1 = riak_kv_index_hashtree:exchange_bucket(IndexN2i, L, B,
                                                        TreePid),
            R1;
       (key_hashes, Segment) ->
            R2 = riak_kv_index_hashtree:exchange_segment(IndexN2i, Segment,
                                                         TreePid),
            R2;
       (A, B) ->
            throw({riak_kv_2i_aae_internal_error, A, B})
    end,
    AccFun =
    fun(KeyDiff, Acc) ->
            lists:foldl(
              fun({_DiffReason, BKeyBin}, Count) ->
                      BK = binary_to_term(BKeyBin),
                      IdxData = fetch_index_data(BKeyBin, DBRef),
                      % Sync refresh it. Not in a rush here.
                      riak_kv_vnode:refresh_index_data(Partition, BK, IdxData,
                                                      ?INDEX_REFRESH_TIMEOUT),
                      Count2 = Count + 1,
                      send_event({repair_count_update, Count2}),
                      Count2
              end,
              Acc, KeyDiff)
    end,
    try
        NumDiff = hashtree:compare(TmpTree, Remote, AccFun, 0),
        lager:info("Found ~p differences", [NumDiff]),
        {ok, NumDiff}
    catch
        CmpErrClass:CmpErr ->
            lager:error("Hashtree exchange failed ~p, ~p", [CmpErrClass, CmpErr]),
            case CmpErrClass of
                error ->
                    {error, CmpErr};
                _ ->
                    {error, {CmpErrClass, CmpErr}}
            end
    end.

get_hashtree_lock(TreePid, Retries) ->
    case riak_kv_index_hashtree:get_lock(TreePid, local_fsm) of
        Reply = already_locked ->
            case Retries > 0 of
                true ->
                    timer:sleep(1000),
                    get_hashtree_lock(TreePid, Retries-1);
                false ->
                    Reply
            end;
        Reply ->
            Reply
    end.


%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

to_simple_partition_result(#partition_result{
                              status=Status,
                              error=Err,
                              index_scan_count=IdxCount,
                              hashtree_population_count=TreePopCount,
                              exchange_count=CmpCount}) ->
    [{status, Status}] ++
    [{error, Err} || Err /= undefined] ++
    [{index_scan_count, IdxCount},
     {hashtree_population_count, TreePopCount},
     {exchange_count, CmpCount}].

%% @doc Returns a proplist simplified version of the state.
to_simple_state(State) ->
    #state{
         speed=Speed,
         partitions=Partitions,
         remaining=Rem,
         worker_status=WStatus,
         index_scan_count=IdxCount,
         hashtree_population_count=PopCount,
         results = Results0
      } = State,
    Results =
    [{Partition, to_simple_partition_result(Res)}
     || {Partition, Res} <- Results0],
    {TotIdxCount, TotPopCount, TotXchgCount} =
    lists:foldl(fun({_, #partition_result{
                       index_scan_count=NI,
                       hashtree_population_count=NP,
                       exchange_count=NX}}, {I, P, X}) ->
                        {I+NI, P+NP, X+NX}
                end, {0,0,0}, Results0),
    PartStatus =
    case length(Rem) > 0 of
        true ->
            [{current_partition, hd(Rem)},
             {current_partition_status, WStatus},
             {current_index_scan_count, IdxCount},
             {current_hashtree_population_count, PopCount}];
        false ->
            []
    end,
    [{speed, Speed},
     {partitions, Partitions},
     {remaining, Rem},
     {index_scan_count, TotIdxCount},
     {hashtree_population_count, TotPopCount},
     {exchange_count, TotXchgCount},
     {results, Results}]
    ++ PartStatus.

format_status(_Opt, [_PDict, State]) ->
    [{detailed_status, to_simple_state(State)}].

%% @doc Human readable status report.
to_report(Status) ->
    Speed = proplists:get_value(speed, Status),
    IdxScanCount = proplists:get_value(index_scan_count, Status),
    PopCount = proplists:get_value(hashtree_population_count, Status),
    XchgCount = proplists:get_value(exchange_count, Status),
    Partitions = proplists:get_value(partitions, Status),
    Rem = proplists:get_value(remaining, Status),
    NParts = length(Partitions),
    NDone = NParts - length(Rem),
    Report0 = io_lib:format("\tTotal partitions: ~p\n"
                            "\tFinished partitions: ~p\n"
                            "\tSpeed: ~p\n"
                            "\tTotal 2i items scanned: ~p\n"
                            "\tTotal tree objects: ~p\n"
                            "\tTotal objects fixed: ~p\n",
                            [NParts, NDone, Speed, IdxScanCount, PopCount,
                             XchgCount]),
    Results = proplists:get_value(results, Status),
    Errors =
    [{P, proplists:get_value(error, R)}
     || {P, R} <- Results,
        proplists:get_value(status, R) == error],
    case Errors of
        [] ->
            Report0;
        _ ->
            lists:flatten(
             [Report0, io_lib:format("With errors:\n", []) |
              [io_lib:format("Partition: ~p\nError: ~p\n\n", [P, E])
               || {P, E} <- Errors]])
    end.

%% @doc Handles repair worker death notification
handle_info({'EXIT', _From, _Reason}, StateName, State) ->
    %% We handle the monitor down message instead.
    {next_state, StateName, State};
handle_info({'DOWN', Mon, _, _, Reason}, _StateName,
            State=#state{worker_monitor=Mon, remaining=[Partition|_]}) ->
    lager:error("Index repair of partition ~p failed : ~p",
                [Partition, Reason]),
    {stop, {partition_failed, Reason}, State};
handle_info({'DOWN', _Mon, _, _, _Reason}, StateName, State) ->
    % Proces died after we got its result, so ignore.
    {next_state, StateName, State}.

handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

%% @doc Handles early reply and status requests.
%% A client may request an early acknowledgement, so early
%% errors can be returned immediately to the user.
handle_sync_event(early_ack, From, StateName,
                  State=#state{early_reply=undefined}) ->
    {next_state, StateName, State#state{reply_to=From}};
handle_sync_event(early_ack, _From, StateName,
                  State=#state{early_reply=Reply}) ->
    {reply, Reply, StateName, State};
handle_sync_event(status, _From, StateName, State) ->
    {reply, to_simple_state(State), StateName, State};
handle_sync_event(stop, From, _StateName, State=#state{open_dbs=DBs}) ->
    [try
         eleveldb:close(DB)
     catch
         _:_ ->
             ok
     end || DB <- DBs],
    gen_fsm:reply(From, ok),
    {stop, user_request, State}.

terminate(_Reason, _StateName, _State) ->
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.
