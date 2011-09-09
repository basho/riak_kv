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

%% Helper API
-export([key_counts/0,
         key_counts/1]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include_lib("bitcask/include/bitcask.hrl").

-define(MERGE_CHECK_INTERVAL, timer:minutes(3)).
-define(API_VERSION, 1).
-define(CAPABILITIES, [async_fold]).

-record(state, {ref :: reference(),
                data_dir :: string(),
                opts :: [{atom(), term()}],
                partition :: integer(),
                root :: string(),
                async_folds :: boolean()}).

-type state() :: #state{}.
-type config() :: [{atom(), term()}].

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Return the major version of the
%% current API and a capabilities list.
-spec api_version() -> {integer(), [atom()]}.
api_version() ->
    {?API_VERSION, ?CAPABILITIES}.

%% @doc Start the bitcask backend
-spec start(integer(), config()) -> {ok, state()} | {error, term()}.
start(Partition, Config) ->
    %% Get the data root directory
    case config_value(data_root, Config) of
        undefined ->
            lager:error("Failed to create bitcask dir: data_root is not set"),
            {error, data_root_unset};
        DataRoot ->
            %% Check if a directory exists for the partition
            case get_data_dir(DataRoot, Partition) of
                {ok, DataDir} ->
                    BitcaskOpts = set_mode(read_write, Config),
                    case bitcask:open(filename:join(DataRoot, DataDir), BitcaskOpts) of
                        Ref when is_reference(Ref) ->
                            check_fcntl(),
                            schedule_merge(Ref),
                            maybe_schedule_sync(Ref),
                            AsyncFolds = config_value(async_folds, Config, true),
                            {ok, #state{ref=Ref,
                                        data_dir=DataDir,
                                        root=DataRoot,
                                        opts=BitcaskOpts,
                                        partition=Partition,
                                        async_folds=AsyncFolds}};
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
    bitcask:close(Ref).

%% @doc Retrieve an object from the bitcask backend
-spec get(riak_object:bucket(), riak_object:key(), state()) ->
                 {ok, any(), state()} |
                 {ok, not_found, state()} |
                 {error, term(), state()}.
get(Bucket, Key, #state{ref=Ref}=State) ->
    BitcaskKey = term_to_binary({Bucket, Key}),
    case bitcask:get(Ref, BitcaskKey) of
        {ok, Value} ->
            {ok, Value, State};
        not_found  ->
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
fold_buckets(FoldBucketsFun, Acc, _Opts, #state{opts=BitcaskOpts,
                                                data_dir=DataFile,
                                                root=DataRoot,
                                                async_folds=true}) ->
    FoldFun = fold_buckets_fun(FoldBucketsFun),
    ReadOpts = set_mode(read_only, BitcaskOpts),
    BucketFolder =
        fun() ->
                case bitcask:open(filename:join(DataRoot, DataFile),
                                  ReadOpts) of
                    Ref when is_reference(Ref) ->
                        {Acc1, _} =
                            bitcask:fold_keys(Ref,
                                              FoldFun,
                                              {Acc, ordsets:new()}),
                        Acc1;
                    {error, Reason} ->
                        {error, Reason}
                end
        end,
    {async, BucketFolder};
fold_buckets(FoldBucketsFun, Acc, _Opts, #state{ref=Ref}) ->
    FoldFun = fold_buckets_fun(FoldBucketsFun),
    {FoldResult, _Bucketset} =
        bitcask:fold_keys(Ref, FoldFun, {Acc, ordsets:new()}),
    case FoldResult of
        {error, _} ->
            FoldResult;
        _ ->
            {ok, FoldResult}
    end.

%% @doc Fold over all the keys for one or all buckets.
-spec fold_keys(riak_kv_backend:fold_keys_fun(),
                any(),
                [{atom(), term()}],
                state()) -> {ok, term()} | {async, fun()} | {error, term()}.
fold_keys(FoldKeysFun, Acc, Opts, #state{opts=BitcaskOpts,
                                         data_dir=DataFile,
                                         root=DataRoot,
                                         async_folds=true}) ->
    Bucket =  proplists:get_value(bucket, Opts),
    FoldFun = fold_keys_fun(FoldKeysFun, Bucket),
    ReadOpts = set_mode(read_only, BitcaskOpts),
    KeyFolder =
        fun() ->
                case bitcask:open(filename:join(DataRoot, DataFile),
                                  ReadOpts) of
                    Ref when is_reference(Ref) ->
                        bitcask:fold_keys(Ref, FoldFun, Acc);
                    {error, Reason} ->
                        {error, Reason}
                end
        end,
    {async, KeyFolder};
fold_keys(FoldKeysFun, Acc, Opts, #state{ref=Ref}) ->
    Bucket =  proplists:get_value(bucket, Opts),
    FoldFun = fold_keys_fun(FoldKeysFun, Bucket),
    FoldResult = bitcask:fold_keys(Ref, FoldFun, Acc),
    case FoldResult of
        {error, _} ->
            FoldResult;
        _ ->
            {ok, FoldResult}
    end.

%% @doc Fold over all the objects for one or all buckets.
-spec fold_objects(riak_kv_backend:fold_objects_fun(),
                   any(),
                   [{atom(), term()}],
                   state()) -> {ok, any()} | {async, fun()} | {error, term()}.
fold_objects(FoldObjectsFun, Acc, Opts, #state{opts=BitcaskOpts,
                                               data_dir=DataFile,
                                               root=DataRoot,
                                               async_folds=true}) ->
    Bucket =  proplists:get_value(bucket, Opts),
    FoldFun = fold_objects_fun(FoldObjectsFun, Bucket),
    ReadOpts = set_mode(read_only, BitcaskOpts),
    ObjectFolder =
        fun() ->
                case bitcask:open(filename:join(DataRoot, DataFile),
                                  ReadOpts) of
                    Ref when is_reference(Ref) ->
                        bitcask:fold(Ref, FoldFun, Acc);
                    {error, Reason} ->
                        {error, Reason}
                end
        end,
    {async, ObjectFolder};
fold_objects(FoldObjectsFun, Acc, Opts, #state{ref=Ref}) ->
    Bucket =  proplists:get_value(bucket, Opts),
    FoldFun = fold_objects_fun(FoldObjectsFun, Bucket),
    FoldResult = bitcask:fold(Ref, FoldFun, Acc),
    case FoldResult of
        {error, _} ->
            FoldResult;
        _ ->
            {ok, FoldResult}
    end.

%% @doc Delete all objects from this bitcask backend
%% @TODO once bitcask has a more friendly drop function
%%  of its own, use that instead.
-spec drop(state()) -> {ok, state()} | {error, term(), state()}.
drop(#state{ref=Ref,
            partition=Partition,
            root=DataRoot,
            opts=BitcaskOpts}=State) ->
    %% Close the bitcask reference
    bitcask:close(Ref),

    case make_data_dir(filename:join([DataRoot,
                                      integer_to_list(Partition)])) of
        {ok, DataDir} ->
            %% Spawn a process to cleanup the old data files.
            %% The use of spawn is intentional. We do not
            %% care if this process dies since any lingering
            %% files will be cleaned up on the next drop.
            %% The worst case is that the files hang
            %% around and take up some disk space.
            spawn(drop_data_cleanup(DataRoot, Partition, DataDir)),

            %% Now open the bitcask and return an updated state
            %% so this backend can continue processing.
            case bitcask:open(filename:join(DataRoot, DataDir), BitcaskOpts) of
                Ref1 when is_reference(Ref1) ->
                    {ok, State#state{data_dir=DataDir,
                                     ref=Ref1}};
                {error, Reason} ->
                    {error, Reason, State#state{data_dir=DataDir}}
            end;
        {error, Reason1} ->
            {error, Reason1, State}
    end.

%% @doc Returns true if this bitcasks backend contains any
%% non-tombstone values; otherwise returns false.
-spec is_empty(state()) -> boolean().
is_empty(#state{ref=Ref}) ->
    %% Determining if a bitcask is empty requires us to find at least
    %% one value that is NOT a tombstone. Accomplish this by doing a fold_keys
    %% that forcibly bails on the very first key encountered.
    F = fun(_K, _Acc0) ->
                throw(found_one_value)
        end,
    (catch bitcask:fold_keys(Ref, F, undefined)) /= found_one_value.

%% @doc Get the status information for this bitcask backend
-spec status(state()) -> [{atom(), term()}].
status(#state{ref=Ref}) ->
    {KeyCount, Status} = bitcask:status(Ref),
    [{key_count, KeyCount}, {status, Status}].


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
                root=BitcaskRoot}=State) when is_reference(Ref) ->
    case bitcask:needs_merge(Ref) of
        {true, Files} ->
            bitcask_merge_worker:merge(BitcaskRoot, [], Files);
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
            case ordsets:is_element(Bucket, BucketSet) of
                true ->
                    {Acc, BucketSet};
                false ->
                    {FoldBucketsFun(Bucket, Acc),
                     ordsets:add_element(Bucket, BucketSet)}
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
config_value(Key, Config) ->
    config_value(Key, Config, undefined).

%% @private
config_value(Key, Config, Default) ->
    case proplists:get_value(Key, Config) of
        undefined ->
            app_helper:get_env(bitcask, Key, Default);
        Value ->
            Value
    end.

%% @private
get_data_dir(DataRoot, PartitionI) ->
    Partition = integer_to_list(PartitionI),
    PartitionPath = filename:join([DataRoot, Partition]),
    case filelib:is_dir(PartitionPath) of
        true ->
            {ok, Partition};
        false ->
            %% Check for any existing directories for the partition
            %% and select the most recent as the active data directory
            %% if any exist.
            case filelib:wildcard(PartitionPath ++ "-*") of
                [] ->
                    make_data_dir(PartitionPath);
                PartitionDirs ->
                    [DataDir | RestPartitionDirs] =
                        lists:reverse(PartitionDirs),
                    log_unused_partition_dirs(Partition,
                                              RestPartitionDirs),
                    {ok, filename:basename(DataDir)}
            end
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
    {MegaSecs, Secs, MicroSecs} = erlang:now(),
    AbsPath = filename:absname(PartitionFile),
    DataDir = [filename:basename(PartitionFile),
               "-",
               integer_to_list(MegaSecs),
               integer_to_list(Secs),
               integer_to_list(MicroSecs)],
    case filelib:ensure_dir([AbsPath, DataDir]) of
        ok ->
            {ok, DataDir};
        {error, Reason} ->
            lager:error("Failed to create bitcask dir ~s: ~p",
                        [DataDir, Reason]),
            {error, Reason}
    end.

%% @private
drop_data_cleanup(DataRoot, Partition, DataDir) ->
    fun() ->
            %% List all the directories in data root
            case file:list_dir(DataRoot) of
                {ok, Dirs} ->
                    PartitionStr = integer_to_list(Partition),
                    %% Delete the contents of each directory and
                    %% the directory itself excluding the
                    %% current data directory.
                    [data_directory_cleanup(filename:join(DataRoot, Dir)) ||
                        Dir <- Dirs,
                        Dir /= lists:flatten(DataDir),
                        string:left(Dir, length(PartitionStr) + 1) =:= (PartitionStr ++ "-")];
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
                          backend_eqc:test(?MODULE, false,
                                           [{data_root,
                                             "test/bitcask-backend"},
                                            {async_folds, false}])),
            ?_assertEqual(true,
                          backend_eqc:test(?MODULE, false,
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
    ?_assertCmd("rm -rf test/bitcask-backend").

-endif. % EQC

-endif.
