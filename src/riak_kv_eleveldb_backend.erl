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
         fold_buckets/4,
         fold_keys/4,
         fold_objects/4,
         is_empty/1,
         status/1,
         callback/3]).

-compile({inline, [
                   to_object_key/2, from_object_key/1,
                   to_index_key/4, from_index_key/1
                  ]}).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(API_VERSION, 1).
-define(CAPABILITIES, [async_fold, indexes]).

-record(state, {ref :: reference(),
                data_root :: string(),
                open_opts = [],
                config :: config(),
                read_opts = [],
                write_opts = [],
                fold_opts = [{fill_cache, false}]
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
            {ok, State};
        {error, Reason} ->
            {error, Reason}
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
                                                write_opts=WriteOpts}=State) ->
    %% Create the KV update...
    StorageKey = to_object_key(Bucket, PrimaryKey),
    Updates1 = [{put, StorageKey, Val}],

    %% Convert IndexSpecs to index updates...
    F = fun({add, Field, Value}) ->
                {put, to_index_key(Bucket, PrimaryKey, Field, Value), <<>>};
           ({remove, Field, Value}) ->
                {delete, to_index_key(Bucket, PrimaryKey, Field, Value)}
        end,
    Updates2 = [F(X) || X <- IndexSpecs],

    %% Perform the write...
    case eleveldb:write(Ref, Updates1 ++ Updates2, WriteOpts) of
        ok ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end.


%% @doc Delete an object from the eleveldb backend
-spec delete(riak_object:bucket(), riak_object:key(), [index_spec()], state()) ->
                    {ok, state()} |
                    {error, term(), state()}.
delete(Bucket, PrimaryKey, IndexSpecs, #state{ref=Ref,
                                              write_opts=WriteOpts}=State) ->

    %% Create the KV delete...
    StorageKey = to_object_key(Bucket, PrimaryKey),
    Updates1 = [{delete, StorageKey}],

    %% Convert IndexSpecs to index deletes...
    F = fun({remove, Field, Value}) ->
                {delete, to_index_key(Bucket, PrimaryKey, Field, Value)}
        end,
    Updates2 = [F(X) || X <- IndexSpecs],

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
    KeyFolder =
        fun() ->
                %% Do the fold. ELevelDB uses throw/1 to break out of a fold...
                try
                    eleveldb:fold_keys(Ref, FoldFun, Acc, FoldOpts1)
                catch
                    {break, AccFinal} ->
                        AccFinal
                end
        end,
    case lists:member(async_fold, Opts) of
        true ->
            {async, KeyFolder};
        false ->
            {ok, KeyFolder()}
    end.

%% @doc Fold over all the objects for one or all buckets.
-spec fold_objects(riak_kv_backend:fold_objects_fun(),
                   any(),
                   [{atom(), term()}],
                   state()) -> {ok, any()} | {async, fun()}.
fold_objects(FoldObjectsFun, Acc, Opts, #state{fold_opts=FoldOpts,
                                               ref=Ref}) ->
    Bucket =  proplists:get_value(bucket, Opts),
    FoldOpts1 = fold_opts(Bucket, FoldOpts),
    FoldFun = fold_objects_fun(FoldObjectsFun, Bucket),
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
is_empty(#state{ref=Ref}) ->
    eleveldb:is_empty(Ref).

%% @doc Get the status information for this eleveldb backend
-spec status(state()) -> [{atom(), term()}].
status(State) ->
    {ok, Stats} = eleveldb:status(State#state.ref, <<"leveldb.stats">>),
    [{stats, Stats}].

%% @doc Register an asynchronous callback
-spec callback(reference(), any(), state()) -> {ok, state()}.
callback(_Ref, _Msg, State) ->
    {ok, State}.

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
open_db(State0) ->
    case eleveldb:open(State0#state.data_root, State0#state.open_opts) of
        {ok, Ref} ->
            {ok, State0#state { ref = Ref }};
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
                _ ->
                    throw({break, Acc})
            end
    end;
fold_keys_fun(FoldKeysFun, {index, FilterBucket, {eq, <<"$bucket">>, _}}) ->
    %% 2I exact match query on special $bucket field...
    fold_keys_fun(FoldKeysFun, {bucket, FilterBucket});
fold_keys_fun(FoldKeysFun, {index, FilterBucket, {eq, FilterField, FilterTerm}}) ->
    %% Rewrite 2I exact match query as a range...
    NewQuery = {range, FilterField, FilterTerm, FilterTerm},
    fold_keys_fun(FoldKeysFun, {index, FilterBucket, NewQuery});
fold_keys_fun(FoldKeysFun, {index, FilterBucket, {range, <<"$key">>, StartKey, EndKey}}) ->
    %% 2I range query on special $key field...
    fun(StorageKey, Acc) ->
            case from_object_key(StorageKey) of
                {Bucket, Key} when FilterBucket == Bucket,
                                   StartKey =< Key,
                                   EndKey >= Key ->
                    FoldKeysFun(Bucket, Key, Acc);
                _ ->
                    throw({break, Acc})
            end
    end;
fold_keys_fun(FoldKeysFun, {index, FilterBucket, {range, FilterField, StartTerm, EndTerm}}) ->
    %% 2I range query...
    fun(StorageKey, Acc) ->
            case from_index_key(StorageKey) of
                {Bucket, Key, Field, Term} when FilterBucket == Bucket,
                                                FilterField == Field,
                                                StartTerm =< Term,
                                                EndTerm >= Term ->
                    FoldKeysFun(Bucket, Key, Acc);
                _ ->
                    throw({break, Acc})
            end
    end;
fold_keys_fun(_FoldKeysFun, Other) ->
    throw({unknown_limiter, Other}).

%% @private
%% Return a function to fold over the objects on this backend
fold_objects_fun(FoldObjectsFun, FilterBucket) ->
    %% 2I does not support fold objects at this time, so this is much
    %% simpler than fold_keys_fun.
    fun({StorageKey, Value}, Acc) ->
            case from_object_key(StorageKey) of
                {Bucket, Key} when FilterBucket == undefined;
                                   Bucket == FilterBucket ->
                    FoldObjectsFun(Bucket, Key, Value, Acc);
                _ ->
                    throw({break, Acc})
            end
    end.

%% @private
%% Augment the fold options list if a
%% bucket is defined.
fold_opts(undefined, FoldOpts) ->
    FoldOpts;
fold_opts(Bucket, FoldOpts) ->
    BKey = sext:encode({Bucket, <<>>}),
    [{first_key, BKey} | FoldOpts].


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
to_first_key({index, Bucket, {eq, <<"$bucket">>, _Term}}) ->
    %% 2I exact match query on special $bucket field...
    to_first_key({bucket, Bucket});
to_first_key({index, Bucket, {eq, Field, Term}}) ->
    %% Rewrite 2I exact match query as a range...
    to_first_key({index, Bucket, {range, Field, Term, Term}});
to_first_key({index, Bucket, {range, <<"$key">>, StartTerm, _EndTerm}}) ->
    %% 2I range query on special $key field...
    to_object_key(Bucket, StartTerm);
to_first_key({index, Bucket, {range, Field, StartTerm, _EndTerm}}) ->
    %% 2I range query...
    to_index_key(Bucket, <<>>, Field, StartTerm);
to_first_key(Other) ->
    erlang:throw({unknown_limiter, Other}).


to_object_key(Bucket, Key) ->
    sext:encode({o, Bucket, Key}).

from_object_key(LKey) ->
    case sext:decode(LKey) of
        {o, Bucket, Key} ->
            {Bucket, Key};
        _ ->
            undefined
    end.

to_index_key(Bucket, Key, Field, Term) ->
    sext:encode({i, Bucket, Field, Term, Key}).

from_index_key(LKey) ->
    case sext:decode(LKey) of
        {i, Bucket, Field, Term, Key} ->
            {Bucket, Key, Field, Term};
        _ ->
            undefined
    end.

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
                                             "test/eleveldb-backend"},
                                         {async_folds, false}]))]},
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
