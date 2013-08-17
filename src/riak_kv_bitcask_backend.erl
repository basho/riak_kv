%% -------------------------------------------------------------------
%%
%% riak_kv_bitcask_backend: Bitcask Driver for Riak
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
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

-module(riak_kv_bitcask_backend).
-behavior(riak_kv_backend).
-author('Andy Gross <andy@basho.com>').
-author('Dave Smith <dizzyd@basho.com>').

%% KV Backend API
-export([api_version/0,
         capabilities/1,
         capabilities/2,
         start/2,
         stop/1,
         get/3,
         put/5,
         delete/4,
         drop/1,
         fold_buckets/4,
         fold_keys/4,
         fold_objects/4,
         is_empty/1,
         status/1,
         callback/3]).

-export([data_size/1]).

%% Helper API
-export([key_counts/0,
         key_counts/1]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include_lib("bitcask/include/bitcask.hrl").

-define(MERGE_CHECK_INTERVAL, timer:minutes(3)).
-define(API_VERSION, 1).
-define(CAPABILITIES, [async_fold,size]).

-record(state, {ref :: reference(),
                data_dir :: string(),
                opts :: [{atom(), term()}],
                partition :: integer(),
                root :: string()}).

-type state() :: #state{}.
-type config() :: [{atom(), term()}].

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Return the major version of the
%% current API.
-spec api_version() -> {ok, integer()}.
api_version() ->
    {ok, ?API_VERSION}.

%% @doc Return the capabilities of the backend.
-spec capabilities(state()) -> {ok, [atom()]}.
capabilities(_) ->
    {ok, ?CAPABILITIES}.

%% @doc Return the capabilities of the backend.
-spec capabilities(riak_object:bucket(), state()) -> {ok, [atom()]}.
capabilities(_, _) ->
    {ok, ?CAPABILITIES}.

%% @doc Start the bitcask backend
-spec start(integer(), config()) -> {ok, state()} | {error, term()}.
start(Partition, Config) ->
    %% Get the data root directory
    case app_helper:get_prop_or_env(data_root, Config, bitcask) of
        undefined ->
            lager:error("Failed to create bitcask dir: data_root is not set"),
            {error, data_root_unset};
        DataRoot ->
            %% Check if a directory exists for the partition
            PartitionStr = integer_to_list(Partition),
            case get_data_dir(DataRoot, PartitionStr) of
                {ok, DataDir} ->
                    BitcaskOpts = set_mode(read_write, Config),
                    case bitcask:open(filename:join(DataRoot, DataDir), BitcaskOpts) of
                        Ref when is_reference(Ref) ->
                            check_fcntl(),
                            schedule_merge(Ref),
                            maybe_schedule_sync(Ref),
                            {ok, #state{ref=Ref,
                                        data_dir=DataDir,
                                        root=DataRoot,
                                        opts=BitcaskOpts,
                                        partition=Partition}};
                        {error, Reason1} ->
                            lager:error("Failed to start bitcask backend: ~p\n",
                                        [Reason1]),
                            {error, Reason1}
                    end;
                {error, Reason} ->
                    lager:error("Failed to start bitcask backend: ~p\n",
                                [Reason]),
                    {error, Reason}
            end
    end.

%% @doc Stop the bitcask backend
-spec stop(state()) -> ok.
stop(#state{ref=Ref}) ->
    case Ref of
        undefined ->
            ok;
        _ ->
            bitcask:close(Ref)
    end.

%% @doc Retrieve an object from the bitcask backend
-spec get(riak_object:bucket(), riak_object:key(), state()) ->
                 {ok, any(), state()} |
                 {error, not_found, state()} |
                 {error, term(), state()}.
get(Bucket, Key, #state{ref=Ref}=State) ->
    BitcaskKey = term_to_binary({Bucket, Key}),
    case bitcask:get(Ref, BitcaskKey) of
        {ok, Value} ->
            {ok, Value, State};
        not_found  ->
            {error, not_found, State};
        {error, bad_crc}  ->
            lager:warning("Unreadable object ~p/~p discarded",
                          [Bucket,Key]),
            {error, not_found, State};
        {error, nofile}  ->
            {error, not_found, State};
        {error, Reason} ->
            {error, Reason, State}
    end.

%% @doc Insert an object into the bitcask backend.
%% NOTE: The bitcask backend does not currently support
%% secondary indexing and the_IndexSpecs parameter
%% is ignored.
-type index_spec() :: {add, Index, SecondaryKey} | {remove, Index, SecondaryKey}.
-spec put(riak_object:bucket(), riak_object:key(), [index_spec()], binary(), state()) ->
                 {ok, state()} |
                 {error, term(), state()}.
put(Bucket, PrimaryKey, _IndexSpecs, Val, #state{ref=Ref}=State) ->
    BitcaskKey = term_to_binary({Bucket, PrimaryKey}),
    case bitcask:put(Ref, BitcaskKey, Val) of
        ok ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end.

%% @doc Delete an object from the bitcask backend
%% NOTE: The bitcask backend does not currently support
%% secondary indexing and the_IndexSpecs parameter
%% is ignored.
-spec delete(riak_object:bucket(), riak_object:key(), [index_spec()], state()) ->
                    {ok, state()} |
                    {error, term(), state()}.
delete(Bucket, Key, _IndexSpecs, #state{ref=Ref}=State) ->
    BitcaskKey = term_to_binary({Bucket, Key}),
    case bitcask:delete(Ref, BitcaskKey) of
        ok ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end.

%% @doc Fold over all the buckets.
-spec fold_buckets(riak_kv_backend:fold_buckets_fun(),
                   any(),
                   [],
                   state()) -> {ok, any()} | {async, fun()} | {error, term()}.
fold_buckets(FoldBucketsFun, Acc, Opts, #state{opts=BitcaskOpts,
                                               data_dir=DataFile,
                                               ref=Ref,
                                               root=DataRoot}) ->
    FoldFun = fold_buckets_fun(FoldBucketsFun),
    case lists:member(async_fold, Opts) of
        true ->
            ReadOpts = set_mode(read_only, BitcaskOpts),
            BucketFolder =
                fun() ->
                        case bitcask:open(filename:join(DataRoot, DataFile),
                                          ReadOpts) of
                            Ref1 when is_reference(Ref1) ->
                                try
                                    {Acc1, _} =
                                        bitcask:fold_keys(Ref1,
                                                          FoldFun,
                                                          {Acc, sets:new()}),
                                        Acc1
                                after
                                    bitcask:close(Ref1)
                                end;
                            {error, Reason} ->
                                {error, Reason}
                        end
                end,
            {async, BucketFolder};
        false ->
            {FoldResult, _Bucketset} =
                bitcask:fold_keys(Ref, FoldFun, {Acc, sets:new()}),
            case FoldResult of
                {error, _} ->
                    FoldResult;
                _ ->
                    {ok, FoldResult}
            end
    end.

%% @doc Fold over all the keys for one or all buckets.
-spec fold_keys(riak_kv_backend:fold_keys_fun(),
                any(),
                [{atom(), term()}],
                state()) -> {ok, term()} | {async, fun()} | {error, term()}.
fold_keys(FoldKeysFun, Acc, Opts, #state{opts=BitcaskOpts,
                                         data_dir=DataFile,
                                         ref=Ref,
                                         root=DataRoot}) ->
    Bucket =  proplists:get_value(bucket, Opts),
    FoldFun = fold_keys_fun(FoldKeysFun, Bucket),
    case lists:member(async_fold, Opts) of
        true ->
            ReadOpts = set_mode(read_only, BitcaskOpts),
            KeyFolder =
                fun() ->
                        case bitcask:open(filename:join(DataRoot, DataFile),
                                          ReadOpts) of
                            Ref1 when is_reference(Ref1) ->
                                try
                                    bitcask:fold_keys(Ref1, FoldFun, Acc)
                                after
                                    bitcask:close(Ref1)
                                end;
                            {error, Reason} ->
                                {error, Reason}
                        end
                end,
            {async, KeyFolder};
        false ->
            FoldResult = bitcask:fold_keys(Ref, FoldFun, Acc),
            case FoldResult of
                {error, _} ->
                    FoldResult;
                _ ->
                    {ok, FoldResult}
            end
    end.

%% @doc Fold over all the objects for one or all buckets.
-spec fold_objects(riak_kv_backend:fold_objects_fun(),
                   any(),
                   [{atom(), term()}],
                   state()) -> {ok, any()} | {async, fun()} | {error, term()}.
fold_objects(FoldObjectsFun, Acc, Opts, #state{opts=BitcaskOpts,
                                               data_dir=DataFile,
                                               ref=Ref,
                                               root=DataRoot}) ->
    Bucket =  proplists:get_value(bucket, Opts),
    FoldFun = fold_objects_fun(FoldObjectsFun, Bucket),
    case lists:member(async_fold, Opts) of
        true ->
            ReadOpts = set_mode(read_only, BitcaskOpts),
            ObjectFolder =
                fun() ->
                        case bitcask:open(filename:join(DataRoot, DataFile),
                                          ReadOpts) of
                            Ref1 when is_reference(Ref1) ->
                                try
                                    bitcask:fold(Ref1, FoldFun, Acc)
                                after
                                    bitcask:close(Ref1)
                                end;
                            {error, Reason} ->
                                {error, Reason}
                        end
                end,
            {async, ObjectFolder};
        false ->
            FoldResult = bitcask:fold(Ref, FoldFun, Acc),
            case FoldResult of
                {error, _} ->
                    FoldResult;
                _ ->
                    {ok, FoldResult}
            end
    end.

%% @doc Delete all objects from this bitcask backend
%% @TODO once bitcask has a more friendly drop function
%%  of its own, use that instead.
-spec drop(state()) -> {ok, state()} | {error, term(), state()}.
drop(#state{ref=Ref,
            partition=Partition,
            root=DataRoot}=State) ->
    %% Close the bitcask reference
    bitcask:close(Ref),

    PartitionStr = integer_to_list(Partition),
    PartitionDir = filename:join([DataRoot, PartitionStr]),
    %% Check for any existing directories for the partition
    %% and select the most recent as the active data directory
    %% if any exist.
    PartitionDirs = existing_partition_dirs(PartitionDir),

    %% Move the older data directories to an automatic cleanup
    %% directory and log their existence.
    CleanupDir = check_for_cleanup_dir(DataRoot, auto),
    move_unused_dirs(CleanupDir, PartitionDirs),

    %% Spawn a process to cleanup the old data files.
    %% The use of spawn is intentional. We do not
    %% care if this process dies since any lingering
    %% files will be cleaned up on the next drop.
    %% The worst case is that the files hang
    %% around and take up some disk space.
    spawn(drop_data_cleanup(PartitionStr, CleanupDir)),

    %% Make sure the data directory is now empty
    data_directory_cleanup(PartitionDir),
    {ok, State#state{ref = undefined}}.

%% @doc Returns true if this bitcasks backend contains any
%% non-tombstone values; otherwise returns false.
-spec is_empty(state()) -> boolean().
is_empty(#state{ref=Ref}) ->
    %% Estimate if we are empty or not as determining for certain
    %% requires a fold over the keyspace that may block. The estimate may
    %% return false when this bitcask is actually empty, but it will never
    %% return true when the bitcask has data.
    bitcask:is_empty_estimate(Ref).

%% @doc Get the status information for this bitcask backend
-spec status(state()) -> [{atom(), term()}].
status(#state{ref=Ref}) ->
    {KeyCount, Status} = bitcask:status(Ref),
    [{key_count, KeyCount}, {status, Status}].

%% @doc Get the size of the bitcask backend (in number of keys)
-spec data_size(state()) -> undefined | {non_neg_integer(), objects}.
data_size(State) ->
    Status = status(State),
    case proplists:get_value(key_count, Status) of
        undefined -> undefined;
        KeyCount -> {KeyCount, objects}
    end.

%% @doc Register an asynchronous callback
-spec callback(reference(), any(), state()) -> {ok, state()}.
callback(Ref,
         {sync, SyncInterval},
         #state{ref=Ref}=State) when is_reference(Ref) ->
    bitcask:sync(Ref),
    schedule_sync(Ref, SyncInterval),
    {ok, State};
callback(Ref,
         merge_check,
         #state{ref=Ref,
                data_dir=DataDir,
                opts=BitcaskOpts,
                root=DataRoot}=State) when is_reference(Ref) ->
    case bitcask:needs_merge(Ref) of
        {true, Files} ->
            BitcaskRoot = filename:join(DataRoot, DataDir),
            bitcask_merge_worker:merge(BitcaskRoot, BitcaskOpts, Files);
        false ->
            ok
    end,
    schedule_merge(Ref),
    {ok, State};
%% Ignore callbacks for other backends so multi backend works
callback(_Ref, _Msg, State) ->
    {ok, State}.

-spec key_counts() -> [{string(), non_neg_integer()}] |
                      {error, data_root_not_set}.
key_counts() ->
    case application:get_env(bitcask, data_root) of
        {ok, RootDir} ->
            key_counts(RootDir);
        undefined ->
            {error, data_root_not_set}
    end.

-spec key_counts(string()) -> [{string(), non_neg_integer()}].
key_counts(RootDir) ->
    [begin
         [{key_count, Keys} | _] = status(#state{root=filename:join(RootDir, Dir)}),
         {Dir, Keys}
     end || Dir <- element(2, file:list_dir(RootDir))].

%% ===================================================================
%% Internal functions
%% ===================================================================

%% @private
%% On linux there is a kernel bug that won't allow fcntl to add O_SYNC
%% to an already open file descriptor.
check_fcntl() ->
    Logged=application:get_env(riak_kv,o_sync_warning_logged),
    Strategy=application:get_env(bitcask,sync_strategy),
    case {Logged,Strategy} of
        {undefined,{ok,o_sync}} ->
            case riak_core_util:is_arch(linux) of
                true ->
                    lager:warning("{sync_strategy,o_sync} not implemented on Linux"),
                    application:set_env(riak_kv,o_sync_warning_logged,true);
                _ ->
                    ok
            end;
        _ ->
            ok
    end.

%% @private
%% Return a function to fold over the buckets on this backend
fold_buckets_fun(FoldBucketsFun) ->
    fun(#bitcask_entry{key=BK}, {Acc, BucketSet}) ->
            {Bucket, _} = binary_to_term(BK),
            case sets:is_element(Bucket, BucketSet) of
                true ->
                    {Acc, BucketSet};
                false ->
                    {FoldBucketsFun(Bucket, Acc),
                     sets:add_element(Bucket, BucketSet)}
            end
    end.

%% @private
%% Return a function to fold over keys on this backend
fold_keys_fun(FoldKeysFun, undefined) ->
    fun(#bitcask_entry{key=BK}, Acc) ->
            {Bucket, Key} = binary_to_term(BK),
            FoldKeysFun(Bucket, Key, Acc)
    end;
fold_keys_fun(FoldKeysFun, Bucket) ->
    fun(#bitcask_entry{key=BK}, Acc) ->
            {B, Key} = binary_to_term(BK),
            case B =:= Bucket of
                true ->
                    FoldKeysFun(B, Key, Acc);
                false ->
                    Acc
            end
    end.

%% @private
%% Return a function to fold over keys on this backend
fold_objects_fun(FoldObjectsFun, undefined) ->
    fun(BK, Value, Acc) ->
            {Bucket, Key} = binary_to_term(BK),
            FoldObjectsFun(Bucket, Key, Value, Acc)
    end;
fold_objects_fun(FoldObjectsFun, Bucket) ->
    fun(BK, Value, Acc) ->
            {B, Key} = binary_to_term(BK),
            case B =:= Bucket of
                true ->
                    FoldObjectsFun(B, Key, Value, Acc);
                false ->
                    Acc
            end
    end.

%% @private
%% Schedule sync (if necessary)
maybe_schedule_sync(Ref) when is_reference(Ref) ->
    case application:get_env(bitcask, sync_strategy) of
        {ok, {seconds, Seconds}} ->
            SyncIntervalMs = timer:seconds(Seconds),
            schedule_sync(Ref, SyncIntervalMs);
        %% erlang:send_after(SyncIntervalMs, self(),
        %%                   {?MODULE, {sync, SyncIntervalMs}});
        {ok, none} ->
            ok;
        {ok, o_sync} ->
            ok;
        BadStrategy ->
            lager:notice("Ignoring invalid bitcask sync strategy: ~p",
                         [BadStrategy]),
            ok
    end.

schedule_sync(Ref, SyncIntervalMs) when is_reference(Ref) ->
    riak_kv_backend:callback_after(SyncIntervalMs, Ref, {sync, SyncIntervalMs}).

schedule_merge(Ref) when is_reference(Ref) ->
    riak_kv_backend:callback_after(?MERGE_CHECK_INTERVAL, Ref, merge_check).

%% @private
get_data_dir(DataRoot, Partition) ->
    PartitionDir = filename:join([DataRoot, Partition]),
    %% Check for any existing directories for the partition
    %% and select the most recent as the active data directory
    %% if any exist.
    ExistingPartitionDirs = existing_partition_dirs(PartitionDir),
    case ExistingPartitionDirs of
        [] ->
            make_data_dir(PartitionDir);
        PartitionDirs ->
            %% Sort the existing data directories
            [MostRecentDataDir | RestDataDirs] = sort_data_dirs(PartitionDirs),

            %% Move the older data directories to a manual cleanup directory and
            %% log their existence. These will not be automatically cleaned up to
            %% avoid data loss in the rare case where the heuristic for selecting
            %% the most recent data directory fails.
            CleanupDir = check_for_cleanup_dir(DataRoot, manual),

            move_unused_dirs(CleanupDir, RestDataDirs),
            log_unused_partition_dirs(Partition, RestDataDirs),

            %% Rename the most recent data directory to the bare
            %% partition name if it is not already so named.
            case MostRecentDataDir == PartitionDir of
                true ->
                    ok;
                false ->
                    file:rename(MostRecentDataDir, PartitionDir)
            end,
            {ok, filename:basename(PartitionDir)}
    end.

%% @private
existing_partition_dirs(PartitionDir) ->
    ExistingDataDirs = filelib:wildcard(PartitionDir ++ "-*"),
    case filelib:is_dir(PartitionDir) of
        true ->
            [PartitionDir | ExistingDataDirs];
        false ->
            ExistingDataDirs
    end.

%% @private
sort_data_dirs(PartitionDirs) ->
    LastModSortFun =
        fun(Dir1, Dir2) ->
                Dir1LastMod = filelib:last_modified(Dir1),
                Dir2LastMod = filelib:last_modified(Dir2),
                case Dir1LastMod == Dir2LastMod of
                    true ->
                        try
                            [_ | [Dir1TS]] =
                                string:tokens(filename:basename(Dir1), "-"),
                            [_ | [Dir2TS]] =
                                string:tokens(filename:basename(Dir2), "-"),
                            if Dir1TS == [] ->
                                    true;
                               Dir2TS == [] ->
                                    false;
                               true ->
                                    list_to_integer(Dir1TS) <
                                        list_to_integer(Dir2TS)
                            end
                        catch _:_ ->
                                Dir1 < Dir2
                        end;
                    false ->
                        Dir1LastMod < Dir2LastMod
                end
        end,
    LastModSortResults = lists:reverse(lists:sort(LastModSortFun, PartitionDirs)),
    TimestampSortResults = lists:reverse(lists:sort(PartitionDirs)),
    %% Check if the head of the last-modified sort results
    %% is the same as the head of the timestamp sort results.
    %% This is to achieve a better correlation that the most
    %% recent data directory has been found. In the case that they
    %% do no match the head of the last-modified sort is chosen
    %% and a warning is logged.
    case hd(LastModSortResults) == hd(TimestampSortResults) of
        true ->
            ok;
        false ->
            %% Log the mismatch
            lager:warning("The most recently modified data directory is not the \
same as the data directory with the most recent timestamp appended. The most \
recently modified directory has been selected, but the other data directories\
 have been preserved in the manual_cleanup directory.")
    end,
    LastModSortResults.

%% @private
check_for_cleanup_dir(PartitionDir, Type) ->
    CleanupDir = filename:join([PartitionDir, atom_to_list(Type) ++ "_cleanup"]),
    filelib:ensure_dir(filename:join([CleanupDir, dummy])),
    CleanupDir.

%% @private
move_unused_dirs(_, []) ->
    ok;
move_unused_dirs(DestinationDir, [PartitionDir | RestPartitionDirs]) ->
    case file:rename(PartitionDir,
                     filename:join([DestinationDir,
                                    filename:basename(PartitionDir)])) of
        ok ->
            move_unused_dirs(DestinationDir, RestPartitionDirs);
        {error, Reason} ->
            lager:error("Failed to move unused data directory ~p. Reason: ~p",
                        [PartitionDir, Reason]),
            move_unused_dirs(DestinationDir, RestPartitionDirs)
    end.

%% @private
log_unused_partition_dirs(Partition, PartitionDirs) ->
    case PartitionDirs of
        [] ->
            ok;
        _ ->
            %% Inform the user in case they want to do some cleanup.
            lager:notice("Unused data directories exist for partition ~p: ~p",
                         [Partition, PartitionDirs])
    end.

%% @private
make_data_dir(PartitionFile) ->
    AbsPath = filename:absname(PartitionFile),
    DataDir = filename:basename(PartitionFile),
    case filelib:ensure_dir(filename:join([AbsPath, dummy])) of
        ok ->
            {ok, DataDir};
        {error, Reason} ->
            lager:error("Failed to create bitcask dir ~s: ~p",
                        [DataDir, Reason]),
            {error, Reason}
    end.

%% @private
drop_data_cleanup(Partition, CleanupDir) ->
    fun() ->
            %% List all the directories in the cleanup directory
            case file:list_dir(CleanupDir) of
                {ok, Dirs} ->
                    %% Delete the contents of each directory and
                    %% the directory itself excluding the
                    %% current data directory.
                    [data_directory_cleanup(filename:join(CleanupDir, Dir)) ||
                        Dir <- Dirs,
                        (Dir =:= Partition) or
                        (string:left(Dir, length(Partition) + 1) =:= (Partition ++ "-"))];
                {error, _} ->
                    ignore
            end
    end.

%% @private
data_directory_cleanup(DirPath) ->
    case file:list_dir(DirPath) of
        {ok, Files} ->
            [file:delete(filename:join([DirPath, File])) || File <- Files],
            file:del_dir(DirPath);
        _ ->
            ignore
    end.

%% @private
set_mode(read_only, Config) ->
    Config1 = lists:keystore(read_only, 1, Config, {read_only, true}),
    lists:keydelete(read_write, 1, Config1);
set_mode(read_write, Config) ->
    Config1 = lists:keystore(read_write, 1, Config, {read_write, true}),
    lists:keydelete(read_only, 1, Config1).

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

simple_test_() ->
    ?assertCmd("rm -rf test/bitcask-backend"),
    application:set_env(bitcask, data_root, ""),
    riak_kv_backend:standard_test(?MODULE,
                                        [{data_root, "test/bitcask-backend"}]).

custom_config_test_() ->
    ?assertCmd("rm -rf test/bitcask-backend"),
    application:set_env(bitcask, data_root, ""),
    riak_kv_backend:standard_test(?MODULE,
                                        [{data_root, "test/bitcask-backend"}]).

startup_data_dir_test() ->
    os:cmd("rm -rf test/bitcask-backend/*"),
    Path = "test/bitcask-backend",
    Config = [{data_root, Path}],
    %% Create a set of timestamped partition directories
    TSPartitionDirs =
        [filename:join(["42-" ++ integer_to_list(X)]) ||
                          X <- lists:seq(1, 10)],
    [begin
         filelib:ensure_dir(filename:join([Path, Dir, dummy]))
     end || Dir <- TSPartitionDirs],
    %% Start the backend
    {ok, State} = start(42, Config),
    %% Stop the backend
    ok = stop(State),
    %% Ensure the timestamped directories have been moved
    {ok, DataDirs} = file:list_dir(Path),
    {ok, RemovalDirs} = file:list_dir(filename:join([Path, "manual_cleanup"])),
    [_ | RemovalPartitionDirs] = lists:reverse(TSPartitionDirs),
    os:cmd("rm -rf test/bitcask-backend/*"),
    ?assertEqual(["42", "manual_cleanup"], lists:sort(DataDirs)),
    ?assertEqual(lists:sort(RemovalPartitionDirs), lists:sort(RemovalDirs)).

drop_test() ->
    os:cmd("rm -rf test/bitcask-backend/*"),
    Path = "test/bitcask-backend",
    Config = [{data_root, Path}],
    %% Start the backend
    {ok, State} = start(42, Config),
    %% Drop the backend
    {ok, State1} = drop(State),
    %% Ensure the timestamped directories have been moved
    {ok, DataDirs} = file:list_dir(Path),
    {ok, RemovalDirs} = file:list_dir(filename:join([Path, "auto_cleanup"])),
    %% RemovalPartitionDirs = lists:reverse(TSPartitionDirs),
    %% Stop the backend
    ok = stop(State1),
    os:cmd("rm -rf test/bitcask-backend/*"),
    ?assertEqual(["auto_cleanup"], lists:sort(DataDirs)),
    %% The drop cleanup happens in a separate process so
    %% there is no guarantee it has happened yet when
    %% this test runs.
    case RemovalDirs of
        [] ->
            ?assert(true);
        ["42"] ->
            ?assert(true);
        _ ->
            ?assert(false)
    end.

get_data_dir_test() ->
    %% Cleanup
    os:cmd("rm -rf test/bitcask-backend/*"),
    Path = "test/bitcask-backend",
    %% Create a set of timestamped partition directories
    %% plus some base directories for other partitions
    TSPartitionDirs =
        [filename:join(["21-" ++ integer_to_list(X)]) ||
                          X <- lists:seq(1, 10)],
    OtherPartitionDirs = [integer_to_list(X) || X <- lists:seq(1,10)],
    [filelib:ensure_dir(filename:join([Path, Dir, dummy]))
     || Dir <- TSPartitionDirs ++ OtherPartitionDirs],
    %% Check the results
    ?assertEqual({ok, "21"}, get_data_dir(Path, "21")).

existing_partition_dirs_test() ->
    %% Cleanup
    os:cmd("rm -rf test/bitcask-backend/*"),
    Path = "test/bitcask-backend",
    %% Create a set of timestamped partition directories
    %% plus some base directories for other partitions
    TSPartitionDirs =
        [filename:join([Path, "21-" ++ integer_to_list(X)]) ||
                          X <- lists:seq(1, 10)],
    OtherPartitionDirs = [integer_to_list(X) || X <- [2, 23, 210]],
    [filelib:ensure_dir(filename:join([Dir, dummy]))
     || Dir <- TSPartitionDirs ++ OtherPartitionDirs],
    %% Check the results
    ?assertEqual(lists:sort(TSPartitionDirs),
                 existing_partition_dirs(filename:join([Path, "21"]))).


-ifdef(EQC).

eqc_test_() ->
    {spawn,
     [{inorder,
       [{setup,
         fun setup/0,
         fun cleanup/1,
         [
          {timeout, 60000,
           [?_assertEqual(true,
                          backend_eqc:test(?MODULE,
                                           false,
                                           [{data_root,
                                             "test/bitcask-backend"}]))]}
         ]}]}]}.

setup() ->
    application:load(sasl),
    application:set_env(sasl, sasl_error_logger,
                        {file, "riak_kv_bitcask_backend_eqc_sasl.log"}),
    error_logger:tty(false),
    error_logger:logfile({open, "riak_kv_bitcask_backend_eqc.log"}),

    application:load(bitcask),
    application:set_env(bitcask, merge_window, never),
    ok.

cleanup(_) ->
    os:cmd("rm -rf test/bitcask-backend/*").

-endif. % EQC

-endif.
