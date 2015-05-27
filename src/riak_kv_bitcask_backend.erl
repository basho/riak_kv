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

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include_lib("bitcask/include/bitcask.hrl").

-define(MERGE_CHECK_INTERVAL, timer:minutes(3)).
-define(MERGE_CHECK_JITTER, 0.3).
-define(UPGRADE_CHECK_INTERVAL, timer:seconds(10)).
-define(UPGRADE_MERGE_BATCH_SIZE, 5).
-define(UPGRADE_FILE, "upgrade.txt").
-define(MERGE_FILE, "merge.txt").
-define(VERSION_FILE, "version.txt").
-define(API_VERSION, 1).
-define(CAPABILITIES, [async_fold,size]).

%% must not be 131, otherwise will match t2b in error
%% yes, I know that this is horrible.
-define(VERSION_1, 1).
-define(VERSION_BYTE, ?VERSION_1).
-define(CURRENT_KEY_TRANS, fun key_transform_to_1/1).

-record(state, {ref :: reference(),
                data_dir :: string(),
                opts :: [{atom(), term()}],
                partition :: integer(),
                root :: string(),
                key_vsn :: integer()}).

-type state() :: #state{}.
-type config() :: [{atom(), term()}].
-type version() :: {non_neg_integer(), non_neg_integer(), non_neg_integer()}.

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

%% @doc Transformation functions for the keys coming off the disk.
key_transform_to_1(<<?VERSION_1:7, _:1, _Rest/binary>> = Key) ->
    Key;
key_transform_to_1(<<131:8,_Rest/bits>> = Key0) ->
    {Bucket, Key} = binary_to_term(Key0),
    make_bk(?VERSION_BYTE, Bucket, Key).

key_transform_to_0(<<?VERSION_1:7,_Rest/bits>> = Key0) ->
    term_to_binary(bk_to_tuple(Key0));
key_transform_to_0(<<131:8,_Rest/binary>> = Key) ->
    Key.

bk_to_tuple(<<?VERSION_1:7, HasType:1, Sz:16/integer,
             TypeOrBucket:Sz/bytes, Rest/binary>>) ->
    case HasType of
        0 ->
            %% no type, first field is bucket
            {TypeOrBucket, Rest};
        1 ->
            %% has a tyoe, extract bucket as well
            <<BucketSz:16/integer, Bucket:BucketSz/bytes, Key/binary>> = Rest,
            {{TypeOrBucket, Bucket}, Key}
    end;
bk_to_tuple(<<131:8,_Rest/binary>> = BK) ->
    binary_to_term(BK).

make_bk(0, Bucket, Key) ->
    term_to_binary({Bucket, Key});
make_bk(1, {Type, Bucket}, Key) ->
    TypeSz = size(Type),
    BucketSz = size(Bucket),
    <<?VERSION_BYTE:7, 1:1, TypeSz:16/integer, Type/binary,
      BucketSz:16/integer, Bucket/binary, Key/binary>>;
make_bk(1, Bucket, Key) ->
    BucketSz = size(Bucket),
    <<?VERSION_BYTE:7, 0:1, BucketSz:16/integer,
     Bucket/binary, Key/binary>>.

%% @doc Start the bitcask backend
-spec start(integer(), config()) -> {ok, state()} | {error, term()}.
start(Partition, Config0) ->
    random:seed(erlang:now()),
    {Config, KeyVsn} =
        case app_helper:get_prop_or_env(small_keys, Config0, bitcask) of
            false ->
                C0 = proplists:delete(small_keys, Config0),
                C1 = C0 ++ [{key_transform, fun key_transform_to_0/1}],
                {C1, 0};
            _ ->
                C0 = proplists:delete(small_keys, Config0),
                C1 = C0 ++ [{key_transform, ?CURRENT_KEY_TRANS}],
                {C1, ?VERSION_BYTE}
        end,

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
                    BitcaskDir = filename:join(DataRoot, DataDir),
                    UpgradeRet = maybe_start_upgrade(BitcaskDir),
                    BitcaskOpts = set_mode(read_write, Config),
                    case bitcask:open(BitcaskDir, BitcaskOpts) of
                        Ref when is_reference(Ref) ->
                            check_fcntl(),
                            schedule_merge(Ref),
                            maybe_schedule_sync(Ref),
                            maybe_schedule_upgrade_check(Ref, UpgradeRet),
                            {ok, #state{ref=Ref,
                                        data_dir=DataDir,
                                        root=DataRoot,
                                        opts=BitcaskOpts,
                                        partition=Partition,
                                        key_vsn=KeyVsn}};
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
get(Bucket, Key, #state{ref=Ref, key_vsn=KVers}=State) ->
    BitcaskKey = make_bk(KVers, Bucket, Key),
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
put(Bucket, PrimaryKey, _IndexSpecs, Val,
    #state{ref=Ref, key_vsn=KeyVsn}=State) ->
    BitcaskKey = make_bk(KeyVsn, Bucket, PrimaryKey),
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
                    {ok, state()}.
delete(Bucket, Key, _IndexSpecs,
       #state{ref=Ref, key_vsn=KeyVsn}=State) ->
    BitcaskKey = make_bk(KeyVsn, Bucket, Key),
    ok = bitcask:delete(Ref, BitcaskKey),
    {ok, State}.

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
                        case bitcask:open(filename:join(DataRoot, DataFile), ReadOpts) of
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
                        case bitcask:open(filename:join(DataRoot, DataFile), ReadOpts) of
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
                        case bitcask:open(filename:join(DataRoot, DataFile), ReadOpts) of
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
    case lists:member(riak_kv, riak_core_node_watcher:services(node())) of
        true ->
            BitcaskRoot = filename:join(DataRoot, DataDir),
            merge_check(Ref, BitcaskRoot, BitcaskOpts);
        false ->
            lager:debug("Skipping merge check: KV service not yet up"),
            ok
    end,
    schedule_merge(Ref),
    {ok, State};
callback(Ref,
         upgrade_check,
         #state{ref=Ref,
                data_dir=DataDir,
                root=DataRoot}=State) when is_reference(Ref) ->
    BitcaskRoot = filename:join(DataRoot, DataDir),
    case check_upgrade(BitcaskRoot) of
        finished ->
            case finalize_upgrade(BitcaskRoot) of
                {ok, Vsn} ->
                    lager:info("Finished upgrading to Bitcask ~s in ~s",
                               [version_to_str(Vsn), BitcaskRoot]),
                    {ok, State};
                {error, EndErr} ->
                    lager:error("Finalizing backend upgrade : ~p", [EndErr]),
                    {ok, State}
            end;
        pending ->
            _ = schedule_upgrade_check(Ref),
            {ok, State};
        {error, Reason} ->
            lager:error("Aborting upgrade in ~s : ~p", [BitcaskRoot, Reason]),
            {ok, State}
    end;
%% Ignore callbacks for other backends so multi backend works
callback(_Ref, _Msg, State) ->
    {ok, State}.

%% ===================================================================
%% Internal functions
%% ===================================================================

% @private If no pending merges, check if files need to be merged.
merge_check(Ref, BitcaskRoot, BitcaskOpts) ->
    case bitcask_merge_worker:status() of
        {0, _} ->
            MaxMergeSize = app_helper:get_env(riak_kv,
                                              bitcask_max_merge_size),
            case bitcask:needs_merge(Ref, [{max_merge_size, MaxMergeSize}]) of
                {true, Files} ->
                    bitcask_merge_worker:merge(BitcaskRoot, BitcaskOpts, Files);
                false ->
                    ok
            end;
        _ ->
            lager:debug("Skipping merge check: Pending merges"),
            ok
    end.

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
            {Bucket, _} = bk_to_tuple(BK),
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
            {Bucket, Key} = bk_to_tuple(BK),
            FoldKeysFun(Bucket, Key, Acc)
    end;
fold_keys_fun(FoldKeysFun, Bucket) ->
    fun(#bitcask_entry{key=BK}, Acc) ->
            {B, Key} = bk_to_tuple(BK),
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
            {Bucket, Key} = bk_to_tuple(BK),
            FoldObjectsFun(Bucket, Key, Value, Acc)
    end;
fold_objects_fun(FoldObjectsFun, Bucket) ->
    fun(BK, Value, Acc) ->
            {B, Key} = bk_to_tuple(BK),
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
    Interval = app_helper:get_env(riak_kv, bitcask_merge_check_interval,
                                  ?MERGE_CHECK_INTERVAL),
    JitterPerc = app_helper:get_env(riak_kv, bitcask_merge_check_jitter,
                                    ?MERGE_CHECK_JITTER),
    Jitter = Interval * JitterPerc,
    FinalInterval = Interval + trunc(2 * random:uniform() * Jitter - Jitter),
    lager:debug("Scheduling Bitcask merge check in ~pms", [FinalInterval]),
    riak_kv_backend:callback_after(FinalInterval, Ref, merge_check).

-spec schedule_upgrade_check(reference()) -> reference().
schedule_upgrade_check(Ref) when is_reference(Ref) ->
    riak_kv_backend:callback_after(?UPGRADE_CHECK_INTERVAL, Ref,
                                   upgrade_check).

-spec maybe_schedule_upgrade_check(reference(),
                                   {upgrading, version()} | no_upgrade) -> ok.
maybe_schedule_upgrade_check(Ref, {upgrading, _}) ->
    _ = schedule_upgrade_check(Ref),
    ok;
maybe_schedule_upgrade_check(_Ref, no_upgrade) ->
    ok.

%% @private
get_data_dir(DataRoot, Partition) ->
    PartitionDir = filename:join([DataRoot, Partition]),
    make_data_dir(PartitionDir).

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

-spec read_version(File::string()) -> undefined | version().
read_version(File) ->
    case file:read_file(File) of
        {ok, VsnContents} ->
            % Crude parser to ignore common user introduced variations
            VsnLines = [binary:replace(B, [<<" ">>, <<"\t">>], <<>>, [global])
                        || B <- binary:split(VsnContents,
                                             [<<"\n">>, <<"\r">>, <<"\r\n">>])],
            VsnLine = [L || L <- VsnLines, L /= <<>>],
            case VsnLine of
                [VsnStr] ->
                    try
                        version_from_str(binary_to_list(VsnStr))
                    catch
                        _:_ ->
                            undefined
                    end;
                _ ->
                    undefined
            end;
        {error, _} ->
            undefined
    end.

-spec bitcask_files(string()) -> {ok, [string()]} | {error, term()}.
bitcask_files(Dir) ->
    case file:list_dir(Dir) of
        {ok, Files} ->
            BitcaskFiles = [F || F <- Files, lists:suffix(".bitcask.data", F)],
            {ok, BitcaskFiles};
        {error, Err} ->
            {error, Err}
    end.

-spec has_bitcask_files(string()) -> boolean() | {error, term()}.
has_bitcask_files(Dir) ->
    case bitcask_files(Dir) of
        {ok, Files} ->
            case Files of
                [] -> false;
                _ -> true
            end;
        {error, Err} ->
            {error, Err}
    end.

-spec needs_upgrade(version() | undefined, version()) -> boolean().
needs_upgrade(undefined, _) ->
    true;
needs_upgrade({A1, B1, _}, {A2, B2, _})
  when {A1, B1} =< {1, 6}, {A2, B2} > {1, 6} ->
    true;
needs_upgrade(_, _) ->
    false.

-spec maybe_start_upgrade(string()) -> no_upgrade | {upgrading, version()}.
maybe_start_upgrade(Dir) ->
    % Are we already upgrading?
    UpgradeFile = filename:join(Dir, ?UPGRADE_FILE),
    UpgradingVsn = read_version(UpgradeFile),
    case UpgradingVsn of
        undefined ->
            % No, maybe if previous data needs upgrade
            maybe_start_upgrade_if_bitcask_files(Dir);
        _ ->
            lager:info("Continuing upgrade to version ~s in ~s",
                       [version_to_str(UpgradingVsn), Dir]),
            {upgrading, UpgradingVsn}
    end.

-spec maybe_start_upgrade_if_bitcask_files(string()) ->
    no_upgrade | {upgrading, version()}.
maybe_start_upgrade_if_bitcask_files(Dir) ->
    NewVsn = bitcask_version(),
    VersionFile = filename:join(Dir, ?VERSION_FILE),
    CurrentVsn = read_version(VersionFile),
    case has_bitcask_files(Dir) of
        true ->
            case needs_upgrade(CurrentVsn, NewVsn) of
                true ->
                    NewVsnStr = version_to_str(NewVsn),
                    case start_upgrade(Dir, CurrentVsn, NewVsn) of
                        ok ->
                            UpgradeFile = filename:join(Dir, ?UPGRADE_FILE),
                            write_version(UpgradeFile, NewVsn),
                            lager:info("Starting upgrade to version ~s in ~s",
                                       [NewVsnStr, Dir]),
                            {upgrading, NewVsn};
                        {error, UpgradeErr} ->
                            lager:error("Failed to start upgrade to version ~s"
                                        " in ~s : ~p",
                                        [NewVsnStr, Dir, UpgradeErr]),
                            no_upgrade
                    end;
                false ->
                    case CurrentVsn == NewVsn of
                        true ->
                            no_upgrade;
                        false ->
                            write_version(VersionFile, NewVsn),
                            no_upgrade
                    end
            end;
        false ->
            write_version(VersionFile, NewVsn),
            no_upgrade;
        {error, Err} ->
            lager:error("Failed to check for bitcask files in ~s."
                        " Can not determine if an upgrade is needed : ~p",
                        [Dir, Err]),
            no_upgrade
    end.

% @doc Start the upgrade process for the given old/new version pair.
-spec start_upgrade(string(), version() | undefined, version()) ->
    ok | {error, term()}.
start_upgrade(Dir, OldVsn, NewVsn)
  when OldVsn < {1, 7, 0}, NewVsn >= {1, 7, 0} ->
    % NOTE: The guard handles old version being undefined, as atom < tuple
    % That is always the case with versions < 1.7.0 anyway.
    case bitcask_files(Dir) of
        {ok, Files} ->
            % Write merge.txt with a list of all bitcask files to merge
            MergeFile = filename:join(Dir, ?MERGE_FILE),
            MergeList = to_merge_list(Files, ?UPGRADE_MERGE_BATCH_SIZE),
            case riak_core_util:replace_file(MergeFile, MergeList) of
                ok ->
                    ok;
                {error, WriteErr} ->
                    {error, WriteErr}
            end;
        {error, BitcaskReadErr} ->
            {error, BitcaskReadErr}
    end.

% @doc Transform to contents of merge.txt, with an empty line every
% Batch files, which delimits the merge batches.
-spec to_merge_list([string()], pos_integer()) -> iodata().
to_merge_list(L, Batch) ->
    N = length(L),
    LN = lists:zip(L, lists:seq(1, N)),
    [case I rem Batch == 0 of
         true ->
             [E, "\n\n"];
         false ->
             [E, "\n"]
     end || {E, I} <- LN].

%% @doc Checks progress of an ongoing backend upgrade.
-spec check_upgrade(Dir :: string()) -> pending | finished | {error, term()}.
check_upgrade(Dir) ->
    UpgradeFile = filename:join(Dir, ?UPGRADE_FILE),
    case filelib:is_regular(UpgradeFile) of
        true ->
            % Look for merge file. When all merged, Bitcask deletes it.
            MergeFile = filename:join(Dir, ?MERGE_FILE),
            case filelib:is_regular(MergeFile) of
                true ->
                    pending;
                false ->
                    finished
            end;
        false ->
            % Flattening so it prints like a string
            Reason = lists:flatten(io_lib:format("Missing upgrade file ~s",
                                                 [UpgradeFile])),
            {error, Reason}
    end.

-spec version_from_str(string()) -> version().
version_from_str(VsnStr) ->
    [Major, Minor, Patch] =
        [list_to_integer(Tok) || Tok <- string:tokens(VsnStr, ".")],
    {Major, Minor, Patch}.

-spec bitcask_version() -> version().
bitcask_version() ->
    version_from_str(bitcask_version_str()).

-spec bitcask_version_str() -> string().
bitcask_version_str() ->
    Apps = application:which_applications(),
    % For tests, etc without the app, use big version number to avoid upgrades
    BitcaskVsn = hd([Vsn || {bitcask, _, Vsn} <- Apps] ++ ["999.999.999"]),
    BitcaskVsn.

-spec version_to_str(version()) -> string().
version_to_str({Major, Minor, Patch}) ->
    io_lib:format("~p.~p.~p", [Major, Minor, Patch]).

-spec write_version(File::string(), Vsn::string() | version()) ->
    ok | {error, term()}.
write_version(File, {_, _, _} = Vsn) ->
    write_version(File, version_to_str(Vsn));
write_version(File, Vsn) ->
    riak_core_util:replace_file(File, Vsn).

-spec finalize_upgrade(Dir :: string()) -> {ok, version()} | {error, term()}.
finalize_upgrade(Dir) ->
    UpgradeFile = filename:join(Dir, ?UPGRADE_FILE),
    case read_version(UpgradeFile) of
        undefined ->
            {error, no_upgrade_version};
        Vsn ->
            VsnFile = filename:join(Dir, ?VERSION_FILE),
            case write_version(VsnFile, Vsn) of
                ok ->
                    case file:delete(UpgradeFile) of
                        ok ->
                            {ok, Vsn};
                        {error, DelErr} ->
                            {error, DelErr}
                    end;
                {error, WriteErr} ->
                    {error, WriteErr}
            end
    end.
%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

simple_test_() ->
    ?assertCmd("rm -rf test/bitcask-backend"),
    application:set_env(bitcask, data_root, ""),
    backend_test_util:standard_test(?MODULE,
                                    [{data_root, "test/bitcask-backend"}]).

custom_config_test_() ->
    ?assertCmd("rm -rf test/bitcask-backend"),
    application:set_env(bitcask, data_root, ""),
    backend_test_util:standard_test(?MODULE,
                                    [{data_root, "test/bitcask-backend"}]).

startup_data_dir_test() ->
    os:cmd("rm -rf test/bitcask-backend/*"),
    Path = "test/bitcask-backend",
    Config = [{data_root, Path}],
    %% Start the backend
    {ok, State} = start(42, Config),
    %% Stop the backend
    ok = stop(State),
    %% Ensure the timestamped directories have been moved
    {ok, DataDirs} = file:list_dir(Path),
    os:cmd("rm -rf test/bitcask-backend/*"),
    ?assertEqual(["42"], DataDirs).

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
    %% RemovalPartitionDirs = lists:reverse(TSPartitionDirs),
    %% Stop the backend
    ok = stop(State1),
    os:cmd("rm -rf test/bitcask-backend/*"),
    ?assertEqual([], DataDirs).

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

key_version_test() ->
    FoldKeysFun =
        fun(Bucket, Key, Acc) ->
                [{Bucket, Key} | Acc]
        end,

    ?assertCmd("rm -rf test/bitcask-backend"),
    application:set_env(bitcask, data_root, "test/bitcask-backend"),
    application:set_env(bitcask, small_keys, true),
    {ok, S} = ?MODULE:start(42, []),
    ?MODULE:put(<<"b1">>, <<"k1">>, [], <<"v1">>, S),
    ?MODULE:put(<<"b2">>, <<"k1">>, [], <<"v2">>, S),
    ?MODULE:put(<<"b3">>, <<"k1">>, [], <<"v3">>, S),

    ?assertMatch({ok, <<"v2">>, _}, ?MODULE:get(<<"b2">>, <<"k1">>, S)),

    ?MODULE:stop(S),
    application:set_env(bitcask, small_keys, false),
    {ok, S1} = ?MODULE:start(42, []),
    %%{ok, L0} = ?MODULE:fold_keys(FoldKeysFun, [], [], S1),
    %%io:format("~p~n", [L0]),

    ?assertMatch({ok, <<"v2">>, _}, ?MODULE:get(<<"b2">>, <<"k1">>, S1)),

    ?MODULE:put(<<"b4">>, <<"k1">>, [], <<"v4">>, S1),
    ?MODULE:put(<<"b5">>, <<"k1">>, [], <<"v5">>, S1),

    ?MODULE:stop(S1),
    application:set_env(bitcask, small_keys, true),
    {ok, S2} = ?MODULE:start(42, []),

    {ok, L0} = ?MODULE:fold_keys(FoldKeysFun, [], [], S2),
    L = lists:sort(L0),
    ?_assertEqual([
                   {<<"b1">>, <<"k1">>},
                   {<<"b2">>, <<"k1">>},
                   {<<"b3">>, <<"k1">>},
                   {<<"b4">>, <<"k1">>},
                   {<<"b5">>, <<"k1">>}
                  ],
                  L).

-ifdef(EQC).

eqc_test_() ->
    {spawn,
     [{inorder,
       [{setup,
         fun setup/0,
         fun cleanup/1,
         [
          {timeout, 180,
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
