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
         start/2,
         stop/1,
         get/3,
         put/5,
         delete/3,
         drop/1,
         fold_buckets/4,
         fold_keys/4,
         fold_objects/4,
         is_empty/1,
         status/1,
         callback/3]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(API_VERSION, 1).
-define(CAPABILITIES, []).

-record(state, {ref :: reference(),
                data_root :: string(),
                read_opts = [],
                write_opts = [],
                fold_opts = [{fill_cache, false}]}).

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

%% @doc Start the eleveldb backend
-spec start(integer(), config()) -> {ok, state()} | {error, term()}.
start(Partition, Config) ->
    %% Get the data root directory
    DataDir = filename:join(config_value(data_root, Config),
                            integer_to_list(Partition)),
    filelib:ensure_dir(filename:join(DataDir, "dummy")),
    case eleveldb:open(DataDir, [{create_if_missing, true}]) of
        {ok, Ref} ->
            {ok, #state { ref = Ref,
                          data_root = DataDir }};
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Stop the eleveldb backend
-spec stop(state()) -> ok.
stop(_State) ->
    %% No-op; GC handles cleanup
    ok.

%% @doc Retrieve an object from the eleveldb backend
-spec get(riak_object:bucket(), riak_object:key(), state()) ->
                 {ok, any(), state()} |
                 {ok, not_found, state()} |
                 {error, term(), state()}.
get(Bucket, Key, #state{read_opts=ReadOpts,
                        ref=Ref}=State) ->
    StorageKey = sext:encode({Bucket, Key}),
    case eleveldb:get(Ref, StorageKey, ReadOpts) of
        {ok, Value} ->
            {ok, Value, State};
        not_found  ->
            {error, not_found, State};
        {error, Reason} ->
            {error, Reason, State}
    end.

%% @doc Insert an object into the eleveldb backend.
%% NOTE: The eleveldb backend does not currently support
%% secondary indexing. This function is only here
%% to conform to the backend API specification.
-type index_spec() :: {add, Index, SecondaryKey} | {remove, Index, SecondaryKey}.
-spec put(riak_object:bucket(), riak_object:key(), [index_spec()], binary(), state()) ->
                 {ok, state()} |
                 {error, term(), state()}.
put(Bucket, PrimaryKey, _IndexSpecs, Val, #state{ref=Ref,
                                                 write_opts=WriteOpts}=State) ->
    StorageKey = sext:encode({Bucket, PrimaryKey}),
    case eleveldb:put(Ref, StorageKey, Val, WriteOpts) of
        ok ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end.


%% @doc Delete an object from the eleveldb backend
-spec delete(riak_object:bucket(), riak_object:key(), state()) ->
                    {ok, state()} |
                    {error, term(), state()}.
delete(Bucket, Key, #state{ref=Ref,
                           write_opts=WriteOpts}=State) ->
    StorageKey = sext:encode({Bucket, Key}),
    case eleveldb:delete(Ref, StorageKey, WriteOpts) of
        ok ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end.

%% @doc Fold over all the buckets
-spec fold_buckets(riak_kv_backend:fold_buckets_fun(),
                   any(),
                   [],
                   state()) -> {ok, any()}.
fold_buckets(FoldBucketsFun, Acc, _Opts, #state{fold_opts=FoldOpts,
                                                ref=Ref}) ->
    FoldFun = fold_buckets_fun(FoldBucketsFun),
    {Acc0, _LastBucket} =
        eleveldb:fold_keys(Ref, FoldFun, {Acc, []}, FoldOpts),
    {ok, Acc0}.

%% @doc Fold over all the keys for one or all buckets.
-spec fold_keys(riak_kv_backend:fold_keys_fun(),
                any(),
                [{atom(), term()}],
                state()) -> {ok, term()}.
fold_keys(FoldKeysFun, Acc, Opts, #state{fold_opts=FoldOpts1,
                                         ref=Ref}) ->
    Bucket =  proplists:get_value(bucket, Opts),
    FoldOpts = fold_opts(Bucket, FoldOpts1),
    FoldFun = fold_keys_fun(FoldKeysFun, Bucket),
    try
        Acc0 = eleveldb:fold_keys(Ref, FoldFun, Acc, FoldOpts),
        {ok, Acc0}
    catch
        {break, AccFinal} ->
            {ok, AccFinal}
    end.

%% @doc Fold over all the objects for one or all buckets.
-spec fold_objects(riak_kv_backend:fold_objects_fun(),
                   any(),
                   [{atom(), term()}],
                   state()) -> {ok, any()}.
fold_objects(FoldObjectsFun, Acc, Opts, #state{fold_opts=FoldOpts1,
                                               ref=Ref}) ->
    Bucket = proplists:get_value(bucket, Opts),
    FoldOpts = fold_opts(Bucket, FoldOpts1),
    FoldFun = fold_objects_fun(FoldObjectsFun, Bucket),
    try
        Acc0 = eleveldb:fold(Ref, FoldFun, Acc, FoldOpts),
        {ok, Acc0}
    catch
        {break, AccFinal} ->
            {ok, AccFinal}
    end.

%% @doc Delete all objects from this eleveldb backend
%% and return a fresh reference.
-spec drop(state()) -> {ok, state()} | {error, term(), state()}.
drop(#state{data_root=DataRoot}=State) ->
    case eleveldb:destroy(DataRoot, []) of
        ok ->
            filelib:ensure_dir(filename:join(DataRoot, "dummy")),
            case eleveldb:open(DataRoot, [{create_if_missing, true}]) of
                {ok, Ref} ->
                    {ok, State#state{ref = Ref}};
                {error, Reason} ->
                    {error, Reason, State}
            end;
        {error, Reason} ->
            {error, Reason, State}
    end.

%% @doc Returns true if this eleveldb backend contains any
%% non-tombstone values; otherwise returns false.
-spec is_empty(state()) -> boolean() | {error, term()}.
is_empty(#state{ref=Ref}) ->
    eleveldb:is_empty(Ref).

%% @doc Get the status information for this eleveldb backend
-spec status(state()) -> [{atom(), term()}].
status(#state{data_root=DataRoot}) ->
    [{Dir, get_status(filename:join(DataRoot, Dir))} ||
        Dir <- element(2, file:list_dir(DataRoot))].

%% @doc Register an asynchronous callback
-spec callback(reference(), any(), state()) -> {ok, state()}.
callback(_Ref, _Msg, State) ->
    {ok, State}.

%% ===================================================================
%% Internal functions
%% ===================================================================

%% @private
config_value(Key, Config) ->
    case proplists:get_value(Key, Config) of
        undefined ->
            app_helper:get_env(eleveldb, Key);
        Value ->
            Value
    end.

%% @private
%% Return a function to fold over the buckets on this backend
fold_buckets_fun(FoldBucketsFun) ->
    fun(BK, {Acc, LastBucket}) ->
            case sext:decode(BK) of
                {LastBucket, _} ->
                    {Acc, LastBucket};
                {Bucket, _} ->
                    {FoldBucketsFun(Bucket, Acc), Bucket}
            end
    end.

%% @private
%% Return a function to fold over keys on this backend
fold_keys_fun(FoldKeysFun, undefined) ->
    fun(BK, Acc) ->
            {Bucket, Key} = sext:decode(BK),
            FoldKeysFun(Bucket, Key, Acc)
    end;
fold_keys_fun(FoldKeysFun, Bucket) ->
    fun(BK, Acc) ->
            {B, Key} = sext:decode(BK),
            if B =:= Bucket ->
                    FoldKeysFun(B, Key, Acc);
               true ->
                    throw({break, Acc})
            end
    end.

%% @private
%% Return a function to fold over the objects on this backend
fold_objects_fun(FoldObjectsFun, undefined) ->
    fun({BK, Value}, Acc) ->
            {Bucket, Key} = sext:decode(BK),
            FoldObjectsFun(Bucket, Key, Value, Acc)
    end;
fold_objects_fun(FoldObjectsFun, Bucket) ->
    fun({BK, Value}, Acc) ->
            {B, Key} = sext:decode(BK),
            %% Take advantage of the fact that sext-encoding ensures all
            %% keys in a bucket occur sequentially. Once we encounter a
            %% different bucket, the fold is complete
            if B =:= Bucket ->
                    FoldObjectsFun(B, Key, Value, Acc);
               true ->
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

%% @private
get_status(Dir) ->
    case eleveldb:open(Dir, [{create_if_missing, true}]) of
        {ok, Ref} ->
            {ok, Status} = eleveldb:status(Ref, <<"leveldb.stats">>),
            Status;
        {error, Reason} ->
            {error, Reason}
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
