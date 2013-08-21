%% -------------------------------------------------------------------
%%
%% riak_kv_eleveldb_backend: Backend Driver for LevelDB
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
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

-module(riak_kv_eleveldb_backend).
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
         fix_index/3,
         mark_indexes_fixed/2,
         set_legacy_indexes/2,
         fixed_index_status/1,
         fold_buckets/4,
         fold_keys/4,
         fold_objects/4,
         is_empty/1,
         status/1,
         callback/3]).

-export([data_size/1]).

-compile({inline, [
                   to_object_key/2, from_object_key/1,
                   to_index_key/4, from_index_key/1
                  ]}).

-include("riak_kv_index.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(API_VERSION, 1).
-define(CAPABILITIES, [async_fold, indexes, index_reformat, size]).
-define(FIXED_INDEXES_KEY, fixed_indexes).

-record(state, {ref :: reference(),
                data_root :: string(),
                open_opts = [],
                config :: config(),
                read_opts = [],
                write_opts = [],
                fold_opts = [{fill_cache, false}],
                fixed_indexes = false, %% true if legacy indexes have be rewritten
                legacy_indexes = false %% true if new writes use legacy indexes (downgrade)
               }).


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

%% @doc Start the eleveldb backend
-spec start(integer(), config()) -> {ok, state()} | {error, term()}.
start(Partition, Config) ->
    %% Initialize random seed
    random:seed(now()),

    %% Get the data root directory
    DataDir = filename:join(app_helper:get_prop_or_env(data_root, Config, eleveldb),
                            integer_to_list(Partition)),

    %% Initialize state
    S0 = init_state(DataDir, Config),
    case open_db(S0) of
        {ok, State} ->
            determine_fixed_index_status(State);
        {error, Reason} ->
            {error, Reason}
    end.

determine_fixed_index_status(State) ->
    case indexes_fixed(State) of
        {error, Reason} ->
            {error, Reason};
        true ->
            {ok, State#state{fixed_indexes=true}};
        false ->
            %% call eleveldb directly to circumvent extra check
            %% for fixed indexes entry. if entry is present we
            %% don't want to ignore becasue it occurs on downgrade.
            %% ignoring is not dangerous but reports confusing results
            %% (empty downgraded partitions still returning fixed = true)
            case eleveldb:is_empty(State#state.ref) of
                true -> mark_indexes_fixed_on_start(State);
                false -> {ok, State#state{fixed_indexes=false}}
            end
    end.

mark_indexes_fixed_on_start(State) ->
    case mark_indexes_fixed(State, true) of
        {error, Reason, _} -> {error, Reason};
        Res -> Res
    end.

%% @doc Stop the eleveldb backend
-spec stop(state()) -> ok.
stop(State) ->
    case State#state.ref of
        undefined ->
            ok;
        _ ->
            eleveldb:close(State#state.ref)
    end,
    ok.

%% @doc Retrieve an object from the eleveldb backend
-spec get(riak_object:bucket(), riak_object:key(), state()) ->
                 {ok, any(), state()} |
                 {ok, not_found, state()} |
                 {error, term(), state()}.
get(Bucket, Key, #state{read_opts=ReadOpts,
                        ref=Ref}=State) ->
    StorageKey = to_object_key(Bucket, Key),
    case eleveldb:get(Ref, StorageKey, ReadOpts) of
        {ok, Value} ->
            {ok, Value, State};
        not_found  ->
            {error, not_found, State};
        {error, Reason} ->
            {error, Reason, State}
    end.

%% @doc Insert an object into the eleveldb backend.
-type index_spec() :: {add, Index, SecondaryKey} | {remove, Index, SecondaryKey}.
-spec put(riak_object:bucket(), riak_object:key(), [index_spec()], binary(), state()) ->
                 {ok, state()} |
                 {error, term(), state()}.
put(Bucket, PrimaryKey, IndexSpecs, Val, #state{ref=Ref,
                                                write_opts=WriteOpts,
                                                legacy_indexes=WriteLegacy,
                                                fixed_indexes=FixedIndexes}=State) ->
    %% Create the KV update...
    StorageKey = to_object_key(Bucket, PrimaryKey),
    Updates1 = [{put, StorageKey, Val}],

    %% Convert IndexSpecs to index updates...
    F = fun({add, Field, Value}) ->
                case WriteLegacy of
                    true ->
                        [{put, to_legacy_index_key(Bucket, PrimaryKey, Field, Value), <<>>}];
                    false ->
                        [{put, to_index_key(Bucket, PrimaryKey, Field, Value), <<>>}]
                end;
           ({remove, Field, Value}) ->
                index_deletes(FixedIndexes, Bucket, PrimaryKey, Field, Value)
        end,
    Updates2 = lists:flatmap(F, IndexSpecs),

    %% Perform the write...
    case eleveldb:write(Ref, Updates1 ++ Updates2, WriteOpts) of
        ok ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end.

indexes_fixed(#state{ref=Ref,read_opts=ReadOpts}) ->
    case eleveldb:get(Ref, to_md_key(?FIXED_INDEXES_KEY), ReadOpts) of
        {ok, <<1>>} ->
            true;
        {ok, <<0>>} ->
            false;
        not_found ->
            false;
        {error, Reason} ->
            {error, Reason}
    end.

index_deletes(FixedIndexes, Bucket, PrimaryKey, Field, Value) ->
    IndexKey = to_index_key(Bucket, PrimaryKey, Field, Value),
    LegacyKey = to_legacy_index_key(Bucket, PrimaryKey, Field, Value),
    KeyDelete = [{delete, IndexKey}],
    LegacyDelete = [{delete, LegacyKey}
                    || FixedIndexes =:= false andalso IndexKey =/= LegacyKey],
    KeyDelete ++ LegacyDelete.

fix_index(IndexKeys, ForUpgrade, #state{ref=Ref,
                                                read_opts=ReadOpts,
                                                write_opts=WriteOpts} = State)
                                        when is_list(IndexKeys) ->
    FoldFun =
        fun(ok, {Success, Ignore, Error}) ->
               {Success+1, Ignore, Error};
           (ignore, {Success, Ignore, Error}) ->
               {Success, Ignore+1, Error};
           ({error, _}, {Success, Ignore, Error}) ->
               {Success, Ignore, Error+1}
        end,
    Totals =
        lists:foldl(FoldFun, {0,0,0},
                    [fix_index(IndexKey, ForUpgrade, Ref, ReadOpts, WriteOpts)
                        || {_Bucket, IndexKey} <- IndexKeys]),
    {reply, Totals, State};
fix_index(IndexKey, ForUpgrade, #state{ref=Ref,
                                                read_opts=ReadOpts,
                                                write_opts=WriteOpts} = State) ->
    case fix_index(IndexKey, ForUpgrade, Ref, ReadOpts, WriteOpts) of
        Atom when is_atom(Atom) ->
            {Atom, State};
        {error, Reason} ->
            {error, Reason, State}
    end.

fix_index(IndexKey, ForUpgrade, Ref, ReadOpts, WriteOpts) ->
    case eleveldb:get(Ref, IndexKey, ReadOpts) of
        {ok, _} ->
            case from_index_key(IndexKey) of 
                {Bucket, Key, Field, Value} ->
                    
                    NewKey = case ForUpgrade of
                                 true -> to_index_key(Bucket, Key, Field, Value);
                                 false -> to_legacy_index_key(Bucket, Key, Field, Value)
                             end,
                    Updates = [{delete, IndexKey}, {put, NewKey, <<>>}],
                    case eleveldb:write(Ref, Updates, WriteOpts) of
                        ok ->
                            ok;
                        {error, Reason} ->
                            {error, Reason}
                    end;
                ignore ->
                    ignore
            end;
        not_found ->
            ignore;
        {error, Reason} ->
            {error, Reason}
    end.

mark_indexes_fixed(State=#state{fixed_indexes=true}, true) ->
    {ok, State};
mark_indexes_fixed(State=#state{fixed_indexes=false}, false) ->
    {ok, State};
mark_indexes_fixed(State=#state{ref=Ref, write_opts=WriteOpts}, ForUpgrade) ->
    Value = case ForUpgrade of
                true -> <<1>>;
                false -> <<0>>
            end,
    Updates = [{put, to_md_key(?FIXED_INDEXES_KEY), Value}],
    case eleveldb:write(Ref, Updates, WriteOpts) of
        ok ->
            {ok, State#state{fixed_indexes=ForUpgrade}};
        {error, Reason} ->
            {error, Reason, State}
    end.

set_legacy_indexes(State, WriteLegacy) ->
    State#state{legacy_indexes=WriteLegacy}.

-spec fixed_index_status(state()) -> boolean().
fixed_index_status(#state{fixed_indexes=Fixed}) ->
    Fixed.

%% @doc Delete an object from the eleveldb backend
-spec delete(riak_object:bucket(), riak_object:key(), [index_spec()], state()) ->
                    {ok, state()} |
                    {error, term(), state()}.
delete(Bucket, PrimaryKey, IndexSpecs, #state{ref=Ref,
                                              write_opts=WriteOpts,
                                              fixed_indexes=FixedIndexes}=State) ->

    %% Create the KV delete...
    StorageKey = to_object_key(Bucket, PrimaryKey),
    Updates1 = [{delete, StorageKey}],

    %% Convert IndexSpecs to index deletes...
    F = fun({remove, Field, Value}) ->
                index_deletes(FixedIndexes, Bucket, PrimaryKey, Field, Value)
        end,
    Updates2 = lists:flatmap(F, IndexSpecs),

    case eleveldb:write(Ref, Updates1 ++ Updates2, WriteOpts) of
        ok ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end.

%% @doc Fold over all the buckets
-spec fold_buckets(riak_kv_backend:fold_buckets_fun(),
                   any(),
                   [],
                   state()) -> {ok, any()} | {async, fun()}.
fold_buckets(FoldBucketsFun, Acc, Opts, #state{fold_opts=FoldOpts,
                                               ref=Ref}) ->
    FoldFun = fold_buckets_fun(FoldBucketsFun),
    FirstKey = to_first_key(undefined),
    FoldOpts1 = [{first_key, FirstKey} | FoldOpts],
    BucketFolder =
        fun() ->
                try
                    {FoldResult, _} =
                        eleveldb:fold_keys(Ref, FoldFun, {Acc, []}, FoldOpts1),
                    FoldResult
                catch
                    {break, AccFinal} ->
                        AccFinal
                end
        end,
    case lists:member(async_fold, Opts) of
        true ->
            {async, BucketFolder};
        false ->
            {ok, BucketFolder()}
    end.

%% @doc Fold over all the keys for one or all buckets.
-spec fold_keys(riak_kv_backend:fold_keys_fun(),
                any(),
                [{atom(), term()}],
                state()) -> {ok, term()} | {async, fun()}.
fold_keys(FoldKeysFun, Acc, Opts, #state{fold_opts=FoldOpts,
                                         fixed_indexes=FixedIdx,
                                         legacy_indexes=WriteLegacyIdx,
                                         ref=Ref}) ->
    %% Figure out how we should limit the fold: by bucket, by
    %% secondary index, or neither (fold across everything.)
    Bucket = lists:keyfind(bucket, 1, Opts),
    Index = lists:keyfind(index, 1, Opts),

    %% Multiple limiters may exist. Take the most specific limiter.
    Limiter =
        if Index /= false  -> Index;
           Bucket /= false -> Bucket;
           true            -> undefined
        end,

    %% Set up the fold...
    FirstKey = to_first_key(Limiter),
    FoldFun = fold_keys_fun(FoldKeysFun, Limiter),
    FoldOpts1 = [{first_key, FirstKey} | FoldOpts],
    ExtraFold = not FixedIdx orelse WriteLegacyIdx,
    KeyFolder =
        fun() ->
            %% Do the fold. ELevelDB uses throw/1 to break out of a fold...
            AccFinal =
                       try
                           eleveldb:fold_keys(Ref, FoldFun, Acc, FoldOpts1)
                       catch
                           {break, BrkResult} ->
                               BrkResult
                       end,
            case ExtraFold of
                true ->
                    legacy_key_fold(Ref, FoldFun, AccFinal, FoldOpts1, Limiter);
                false ->
                    AccFinal
            end
        end,
    case lists:member(async_fold, Opts) of
        true ->
            {async, KeyFolder};
        false ->
            {ok, KeyFolder()}
    end.

legacy_key_fold(Ref, FoldFun, Acc, FoldOpts0, Query={index, _, _}) ->
    {_, FirstKey} = lists:keyfind(first_key, 1, FoldOpts0),
    LegacyKey = to_legacy_first_key(Query),
    case LegacyKey =/= FirstKey of
        true ->
            try
                FoldOpts = lists:keyreplace(first_key, 1, FoldOpts0, {first_key, LegacyKey}),
                eleveldb:fold_keys(Ref, FoldFun, Acc, FoldOpts)
            catch
                {break, AccFinal} ->
                    AccFinal
            end;
        false ->
            Acc
    end;
legacy_key_fold(_Ref, _FoldFun, Acc, _FoldOpts, _Query) ->
    Acc.

%% @doc Fold over all the objects for one or all buckets.
-spec fold_objects(riak_kv_backend:fold_objects_fun(),
                   any(),
                   [{atom(), term()}],
                   state()) -> {ok, any()} | {async, fun()}.
fold_objects(FoldObjectsFun, Acc, Opts, #state{fold_opts=FoldOpts,
                                               ref=Ref}) ->
    %% Figure out how we should limit the fold: by bucket, by
    %% secondary index, or neither (fold across everything.)
    Bucket = lists:keyfind(bucket, 1, Opts),
    Index = lists:keyfind(index, 1, Opts),

    %% Multiple limiters may exist. Take the most specific limiter.
    Limiter =
        if Index /= false  -> Index;
           Bucket /= false -> Bucket;
           true            -> undefined
        end,

    %% Set up the fold...
    FirstKey = to_first_key(Limiter),
    FoldOpts1 = [{first_key, FirstKey} | FoldOpts],
    FoldFun = fold_objects_fun(FoldObjectsFun, Limiter),

    ObjectFolder =
        fun() ->
                try
                    eleveldb:fold(Ref, FoldFun, Acc, FoldOpts1)
                catch
                    {break, AccFinal} ->
                        AccFinal
                end
        end,
    case lists:member(async_fold, Opts) of
        true ->
            {async, ObjectFolder};
        false ->
            {ok, ObjectFolder()}
    end.

%% @doc Delete all objects from this eleveldb backend
%% and return a fresh reference.
-spec drop(state()) -> {ok, state()} | {error, term(), state()}.
drop(State0) ->
    eleveldb:close(State0#state.ref),
    case eleveldb:destroy(State0#state.data_root, []) of
        ok ->
            {ok, State0#state{ref = undefined}};
        {error, Reason} ->
            {error, Reason, State0}
    end.

%% @doc Returns true if this eleveldb backend contains any
%% non-tombstone values; otherwise returns false.
-spec is_empty(state()) -> boolean() | {error, term()}.
is_empty(#state{ref=Ref, read_opts=ReadOpts, write_opts=WriteOpts}) ->
    case eleveldb:is_empty(Ref) of
        true ->
            true;
        false ->
            is_empty_but_md(Ref, ReadOpts, WriteOpts)
    end.

is_empty_but_md(Ref, _ReadOpts, _WriteOpts) ->
    MDKey = to_md_key(?FIXED_INDEXES_KEY),
    %% fold, and if any key (except md key) is found, not
    %% empty
    FF = fun(Key, _) when Key == MDKey -> true;
            (_K, _) -> throw({break, false})
         end,
    try
        eleveldb:fold_keys(Ref, FF, true, [{fill_cache, false}])
    catch {break, Empty} ->
            Empty
    end.

%% @doc Get the status information for this eleveldb backend
-spec status(state()) -> [{atom(), term()}].
status(State=#state{fixed_indexes=FixedIndexes}) ->
    {ok, Stats} = eleveldb:status(State#state.ref, <<"leveldb.stats">>),
    {ok, ReadBlockError} = eleveldb:status(State#state.ref, <<"leveldb.ReadBlockError">>),
    [{stats, Stats}, {read_block_error, ReadBlockError}, {fixed_indexes, FixedIndexes}].

%% @doc Register an asynchronous callback
-spec callback(reference(), any(), state()) -> {ok, state()}.
callback(_Ref, _Msg, State) ->
    {ok, State}.

%% @doc Get the size of the eleveldb backend in bytes
-spec data_size(state()) -> undefined | {non_neg_integer(), bytes}.
data_size(State) ->
    try {ok, <<SizeStr/binary>>} = eleveldb:status(State#state.ref, <<"leveldb.total-bytes">>),
         list_to_integer(binary_to_list(SizeStr)) of
        Size -> {Size, bytes}
    catch
        error:_ -> undefined
    end.

%% ===================================================================
%% Internal functions
%% ===================================================================

%% @private
init_state(DataRoot, Config) ->
    %% Get the data root directory
    filelib:ensure_dir(filename:join(DataRoot, "dummy")),

    %% Merge the proplist passed in from Config with any values specified by the
    %% eleveldb app level; precedence is given to the Config.
    MergedConfig = orddict:merge(fun(_K, VLocal, _VGlobal) -> VLocal end,
                                 orddict:from_list(Config), % Local
                                 orddict:from_list(application:get_all_env(eleveldb))), % Global

    %% Use a variable write buffer size in order to reduce the number
    %% of vnodes that try to kick off compaction at the same time
    %% under heavy uniform load...
    WriteBufferMin = config_value(write_buffer_size_min, MergedConfig, 30 * 1024 * 1024),
    WriteBufferMax = config_value(write_buffer_size_max, MergedConfig, 60 * 1024 * 1024),
    WriteBufferSize = WriteBufferMin + random:uniform(1 + WriteBufferMax - WriteBufferMin),

    %% Update the write buffer size in the merged config and make sure create_if_missing is set
    %% to true
    FinalConfig = orddict:store(write_buffer_size, WriteBufferSize,
                                orddict:store(create_if_missing, true, MergedConfig)),

    %% Parse out the open/read/write options
    {OpenOpts, _BadOpenOpts} = eleveldb:validate_options(open, FinalConfig),
    {ReadOpts, _BadReadOpts} = eleveldb:validate_options(read, FinalConfig),
    {WriteOpts, _BadWriteOpts} = eleveldb:validate_options(write, FinalConfig),

    %% Use read options for folding, but FORCE fill_cache to false
    FoldOpts = lists:keystore(fill_cache, 1, ReadOpts, {fill_cache, false}),

    %% Warn if block_size is set
    SSTBS = proplists:get_value(sst_block_size, OpenOpts, false),
    BS = proplists:get_value(block_size, OpenOpts, false),
    case BS /= false andalso SSTBS == false of
        true ->
            lager:warning("eleveldb block_size has been renamed sst_block_size "
                          "and the current setting of ~p is being ignored.  "
                          "Changing sst_block_size is strongly cautioned "
                          "against unless you know what you are doing.  Remove "
                          "block_size from app.config to get rid of this "
                          "message.\n", [BS]);
        _ ->
            ok
    end,

    %% Generate a debug message with the options we'll use for each operation
    lager:debug("Datadir ~s options for LevelDB: ~p\n",
                [DataRoot, [{open, OpenOpts}, {read, ReadOpts}, {write, WriteOpts}, {fold, FoldOpts}]]),
    #state { data_root = DataRoot,
             open_opts = OpenOpts,
             read_opts = ReadOpts,
             write_opts = WriteOpts,
             fold_opts = FoldOpts,
             config = FinalConfig }.

%% @private
open_db(State) ->
    RetriesLeft = app_helper:get_env(riak_kv, eleveldb_open_retries, 30),
    open_db(State, max(1, RetriesLeft), undefined).

open_db(_State0, 0, LastError) ->
    {error, LastError};
open_db(State0, RetriesLeft, _) ->
    case eleveldb:open(State0#state.data_root, State0#state.open_opts) of
        {ok, Ref} ->
            {ok, State0#state { ref = Ref }};
        %% Check specifically for lock error, this can be caused if
        %% a crashed vnode takes some time to flush leveldb information
        %% out to disk.  The process is gone, but the NIF resource cleanup
        %% may not have completed.
        {error, {db_open, OpenErr}=Reason} ->
            case lists:prefix("IO error: lock ", OpenErr) of
                true ->
                    SleepFor = app_helper:get_env(riak_kv, eleveldb_open_retry_delay, 2000),
                    lager:debug("Leveldb backend retrying ~p in ~p ms after error ~s\n",
                                [State0#state.data_root, SleepFor, OpenErr]),
                    timer:sleep(SleepFor),
                    open_db(State0, RetriesLeft - 1, Reason);
                false ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

%% @private
config_value(Key, Config, Default) ->
    case orddict:find(Key, Config) of
        error ->
            Default;
        {ok, Value} ->
            Value
    end.

%% @private
%% Return a function to fold over the buckets on this backend
fold_buckets_fun(FoldBucketsFun) ->
    fun(BK, {Acc, LastBucket}) ->
            case from_object_key(BK) of
                {LastBucket, _} ->
                    {Acc, LastBucket};
                {Bucket, _} ->
                    {FoldBucketsFun(Bucket, Acc), Bucket};
                ignore ->
                    {Acc, LastBucket};
                _ ->
                    throw({break, Acc})
            end
    end.

%% @private
%% Return a function to fold over keys on this backend
fold_keys_fun(FoldKeysFun, undefined) ->
    %% Fold across everything...
    fun(StorageKey, Acc) ->
            case from_object_key(StorageKey) of
                {Bucket, Key} ->
                    FoldKeysFun(Bucket, Key, Acc);
                ignore ->
                    Acc;
                _ ->
                    throw({break, Acc})
            end
    end;
fold_keys_fun(FoldKeysFun, {bucket, FilterBucket}) ->
    %% Fold across a specific bucket...
    fun(StorageKey, Acc) ->
            case from_object_key(StorageKey) of
                {Bucket, Key} when Bucket == FilterBucket ->
                    FoldKeysFun(Bucket, Key, Acc);
                ignore ->
                    Acc;
                _ ->
                    throw({break, Acc})
            end
    end;
%% 2i queries
fold_keys_fun(FoldKeysFun, {index, FilterBucket, Q=?KV_INDEX_Q{filter_field=FilterField}})
  when FilterField =:= <<"$bucket">>;
       FilterField =:= <<"$key">> ->
    %% Inbuilt indexes
    fun(StorageKey, Acc) ->
            ObjectKey = from_object_key(StorageKey),
            case riak_index:object_key_in_range(ObjectKey, FilterBucket, Q) of
                {true, {Bucket, Key}} ->
                    stoppable_fold(FoldKeysFun, Bucket, Key, Acc);
                {skip, _BK} ->
                    Acc;
                _ ->
                    throw({break, Acc})
            end
    end;
fold_keys_fun(FoldKeysFun, {index, FilterBucket, Q=?KV_INDEX_Q{return_terms=Terms}}) ->
    %% User indexes
    fun(StorageKey, Acc) ->
            IndexKey = from_index_key(StorageKey),
            case riak_index:index_key_in_range(IndexKey, FilterBucket, Q) of
                {true, {Bucket, Key, _Field, Term}} ->
                    Val = if
                              Terms -> {Term, Key};
                              true -> Key
                          end,
                    stoppable_fold(FoldKeysFun, Bucket, Val, Acc);
                {skip, _IK} ->
                    Acc;
                _ ->
                    throw({break, Acc})
            end
    end;
fold_keys_fun(FoldKeysFun, {index, incorrect_format, ForUpgrade}) when is_boolean(ForUpgrade) ->
    %% Over incorrectly formatted 2i index values
    fun(StorageKey, Acc) ->
            Action =
                case from_index_key(StorageKey) of
                    {Bucket, Key, Field, Term} ->
                        NewKey = case ForUpgrade of
                                     true ->
                                         to_index_key(Bucket, Key, Field, Term);
                                     false ->
                                         to_legacy_index_key(Bucket, Key, Field, Term)
                                 end,
                        case NewKey =:= StorageKey of
                            true  ->
                                ignore;
                            false ->
                                {fold, Bucket, StorageKey}
                        end;
                    ignore ->
                        ignore;
                    _ ->
                        stop
                end,
            case Action of
                {fold, B, K} ->
                    FoldKeysFun(B, K, Acc);
                ignore ->
                    Acc;
                stop ->
                    throw({break, Acc})
            end
    end;
fold_keys_fun(FoldKeysFun, {index, Bucket, V1Q}) ->
    %% Handle legacy queries
    Q = riak_index:upgrade_query(V1Q),
    fold_keys_fun(FoldKeysFun, {index, Bucket, Q});
fold_keys_fun(_FoldKeysFun, Other) ->
    throw({unknown_limiter, Other}).

%% @private
%% To stop a fold in progress when pagination limit is reached.
stoppable_fold(Fun, Bucket, Item, Acc) ->
    try
        Fun(Bucket, Item, Acc)
    catch
        stop_fold ->
            throw({break, Acc})
    end.



%% @private
%% Return a function to fold over the objects on this backend
fold_objects_fun(FoldObjectsFun, {index, FilterBucket, Q=?KV_INDEX_Q{}}) ->
    %% 2I query on $key or $bucket field with return_body
    fun({StorageKey, Value}, Acc) ->
            ObjectKey = from_object_key(StorageKey),
            case riak_index:object_key_in_range(ObjectKey, FilterBucket, Q) of
                {true, {Bucket, Key}} ->
                    stoppable_fold(FoldObjectsFun, Bucket, {o, Key, Value}, Acc);
                {skip, _BK} ->
                    Acc;
                _ ->
                    throw({break, Acc})
            end
    end;
fold_objects_fun(FoldObjectsFun, {bucket, FilterBucket}) ->
    fun({StorageKey, Value}, Acc) ->
            case from_object_key(StorageKey) of
                {Bucket, Key} when Bucket == FilterBucket ->
                    FoldObjectsFun(Bucket, Key, Value, Acc);
                ignore ->
                    Acc;
                _ ->
                    throw({break, Acc})
            end
    end;
fold_objects_fun(FoldObjectsFun, undefined) ->
    fun({StorageKey, Value}, Acc) ->
            case from_object_key(StorageKey) of
                {Bucket, Key} ->
                    FoldObjectsFun(Bucket, Key, Value, Acc);
                ignore ->
                    Acc;
                _ ->
                    throw({break, Acc})
            end
    end.

%% @private Given a scope limiter, use sext to encode an expression
%% that represents the starting key for the scope. For example, since
%% we store objects under {o, Bucket, Key}, the first key for the
%% bucket "foo" would be `sext:encode({o, <<"foo">>, <<>>}).`
to_first_key(undefined) ->
    %% Start at the first object in LevelDB...
    to_object_key(<<>>, <<>>);
to_first_key({bucket, Bucket}) ->
    %% Start at the first object for a given bucket...
    to_object_key(Bucket, <<>>);
to_first_key({index, incorrect_format, ForUpgrade}) when is_boolean(ForUpgrade) ->
    %% Start at first index entry
    to_index_key(<<>>, <<>>, <<>>, <<>>);
%% V2 indexes
to_first_key({index, Bucket,
              ?KV_INDEX_Q{filter_field=Field,
                          start_key=StartKey}}) when Field == <<"$key">>;
                                                     Field == <<"$bucket">> ->
    to_object_key(Bucket, StartKey);
to_first_key({index, Bucket, ?KV_INDEX_Q{filter_field=Field,
                                         start_key=StartKey,
                                         start_term=StartTerm}}) ->
    to_index_key(Bucket, StartKey, Field, StartTerm);
%% Upgrade legacy queries to current version
to_first_key({index, Bucket, Q}) ->
    UpgradeQ = riak_index:upgrade_query(Q),
    to_first_key({index, Bucket, UpgradeQ});
to_first_key(Other) ->
    erlang:throw({unknown_limiter, Other}).

% @doc If index query, encode key using legacy sext format.
to_legacy_first_key({index, Bucket, {eq, Field, Term}}) ->
    to_legacy_first_key({index, Bucket, {range, Field, Term, Term}});
to_legacy_first_key({index, Bucket, {range, Field, StartTerm, _EndTerm}}) ->
    to_legacy_index_key(Bucket, <<>>, Field, StartTerm);
to_legacy_first_key(Other) ->
    to_first_key(Other).

to_object_key(Bucket, Key) ->
    sext:encode({o, Bucket, Key}).

from_object_key(LKey) ->
    case (catch sext:decode(LKey)) of
        {'EXIT', _} -> 
            lager:warning("Corrupted object key, discarding"),
            ignore;
        {o, Bucket, Key} ->
            {Bucket, Key};
        _ ->
            undefined
    end.

to_index_key(Bucket, Key, Field, Term) ->
    sext:encode({i, Bucket, Field, Term, Key}).

to_legacy_index_key(Bucket, Key, Field, Term) -> %% encode with legacy bignum encoding
    sext:encode({i, Bucket, Field, Term, Key}, true).

from_index_key(LKey) ->
    case (catch sext:decode(LKey)) of
        {'EXIT', _} -> 
            lager:warning("Corrupted index key, discarding"),
            ignore;
        {i, Bucket, Field, Term, Key} ->
            {Bucket, Key, Field, Term};
        _ ->
            undefined
    end.

%% @doc Encode a key to store partition meta-data attributes.
to_md_key(Key) ->
    sext:encode({md, Key}).


%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

simple_test_() ->
    ?assertCmd("rm -rf test/eleveldb-backend"),
    application:set_env(eleveldb, data_root, "test/eleveldb-backend"),
    riak_kv_backend:standard_test(?MODULE, []).

custom_config_test_() ->
    ?assertCmd("rm -rf test/eleveldb-backend"),
    application:set_env(eleveldb, data_root, ""),
    riak_kv_backend:standard_test(?MODULE, [{data_root, "test/eleveldb-backend"}]).

retry_test() ->
    Root = "/tmp/eleveldb_retry_test",
    try
        {ok, State1} = start(42, [{data_root, Root}]),
        Me = self(),
        Pid1 = spawn_link(fun() ->
                                  receive
                                      stop ->
                                          Me ! {1, stop(State1)}
                                  end
                          end),
        _Pid2 = spawn_link(
                 fun() ->
                         Me ! {2, running},
                         Me ! {2, start(42, [{data_root, Root}])}
                 end),
        %% Ensure Pid2 is runnng and  give it 10ms to get into the open
        %% so we know it has a lock clash
        receive
            {2, running} ->
                timer:sleep(10);
            X ->
                throw({unexpected, X})
        after
            5000 ->
                throw(timeout1)
        end,
        %% Tell Pid1 to shut it down
        Pid1 ! stop,
        receive
            {1, ok} ->
                ok;
            X2 ->
                throw({unexpected, X2})
        after
            5000 ->
                throw(timeout2)
        end,
        %% Wait for Pid2
        receive
            {2, {ok, _State2}} ->
                ok;
            {2, Res} ->
                throw({notok, Res});
            X3 ->
                throw({unexpected, X3})
        end
    after
        os:cmd("rm -rf " ++ Root)
    end.

retry_fail_test() ->
    Root = "/tmp/eleveldb_fail_retry_test",
    try
        application:set_env(riak_kv, eleveldb_open_retries, 3), % 3 times, 1ms a time
        application:set_env(riak_kv, eleveldb_open_retry_delay, 1),
        {ok, State1} = start(42, [{data_root, Root}]),
        Me = self(),
        spawn_link(
          fun() ->
                  Me ! {2, running},
                  Me ! {2, start(42, [{data_root, Root}])}
          end),
        %% Ensure Pid2 is runnng and  give it 10ms to get into the open
        %% so we know it has a lock clash
        receive
            {2, running} ->
                ok;
            X ->
                throw({unexpected, X})
        after
            5000 ->
                throw(timeout1)
        end,
        %% Wait for Pid2 to fail
        receive
            {2, {error, {db_open, _Why}}} ->
                ok;
            {2, Res} ->
                throw({expect_fail, Res});
            X3 ->
                throw({unexpected, X3})
        end,
        %% Then close and reopen, just for kicks to prove it was the locking
        ok = stop(State1),
        {ok, State2} = start(42, [{data_root, Root}]),
        ok = stop(State2)
    after
        os:cmd("rm -rf " ++ Root),
        application:unset_env(riak_kv, eleveldb_open_retries),
        application:unset_env(riak_kv, eleveldb_open_retry_delay)
    end.


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
                                             "test/eleveldb-backend"}]))]}
         ]}]}]}.

setup() ->
    application:load(sasl),
    application:set_env(sasl, sasl_error_logger, {file, "riak_kv_eleveldb_backend_eqc_sasl.log"}),
    error_logger:tty(false),
    error_logger:logfile({open, "riak_kv_eleveldb_backend_eqc.log"}),

    ok.

cleanup(_) ->
    ?_assertCmd("rm -rf test/eleveldb-backend").

-endif. % EQC

-endif.
