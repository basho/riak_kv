%% -------------------------------------------------------------------
%%
%% riak_memory_backend: storage engine using ETS tables
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

%% @doc riak_kv_memory_backend is a Riak storage backend that uses ets
%% tables to store all data in memory.
%%
%% === Configuration Options ===
%%
%% The following configuration options are available for the memory backend.
%% The options should be specified in the `memory_backend' section of your
%% app.config file.
%%
%% <ul>
%% <li>`ttl' - The time in seconds that an object should live before being expired.</li>
%% <li>`max_memory' - The amount of memory in megabytes to limit the backend to.</li>
%% </ul>
%%

-module(riak_kv_memory_backend).
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

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(API_VERSION, 1).
-define(CAPABILITIES, [async_fold]).

-record(state, {data_ref :: integer() | atom(),
                time_ref :: integer() | atom(),
                max_memory :: undefined | integer(),
                used_memory=0 :: integer(),
                ttl :: integer()}).

-type state() :: #state{}.
-type config() :: [].

%% ===================================================================
%% Public API
%% ===================================================================

%% KV Backend API

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

%% @doc Start the memory backend
-spec start(integer(), config()) -> {ok, state()}.
start(Partition, Config) ->
    TTL = app_helper:get_prop_or_env(ttl, Config, memory_backend),
    MemoryMB = app_helper:get_prop_or_env(max_memory, Config, memory_backend),
    case MemoryMB of
        undefined ->
            MaxMemory = undefined,
            TimeRef = undefined;
        _ ->
            MaxMemory = MemoryMB * 1024 * 1024,
            TimeRef = ets:new(list_to_atom(integer_to_list(Partition)), [ordered_set])
    end,
    DataRef = ets:new(list_to_atom(integer_to_list(Partition)), []),
    {ok, #state{data_ref=DataRef,
                max_memory=MaxMemory,
                time_ref=TimeRef,
                ttl=TTL}}.

%% @doc Stop the memory backend
-spec stop(state()) -> ok.
stop(#state{data_ref=DataRef,
            max_memory=MaxMemory,
            time_ref=TimeRef}) ->
    catch ets:delete(DataRef),
    case MaxMemory of
        undefined ->
            ok;
        _ ->
            catch ets:delete(TimeRef)
    end,
    ok.

%% @doc Retrieve an object from the memory backend
-spec get(riak_object:bucket(), riak_object:key(), state()) ->
                 {ok, any(), state()} |
                 {ok, not_found, state()} |
                 {error, term(), state()}.
get(Bucket, Key, State=#state{data_ref=DataRef,
                              ttl=TTL}) ->
    case ets:lookup(DataRef, {Bucket, Key}) of
        [] -> {error, not_found, State};
        [{{Bucket, Key}, {{ts, Timestamp}, Val}}] ->
            case exceeds_ttl(Timestamp, TTL) of
                true ->
                    delete(Bucket, Key, undefined, State),
                    {error, not_found, State};
                false ->
                    {ok, Val, State}
            end;
        [{{Bucket, Key}, Val}] ->
            {ok, Val, State};
        Error ->
            {error, Error, State}
    end.

%% @doc Insert an object into the memory backend.
%% NOTE: The memory backend does not currently
%% support secondary indexing and the _IndexSpecs
%% parameter is ignored.
-type index_spec() :: {add, Index, SecondaryKey} | {remove, Index, SecondaryKey}.
-spec put(riak_object:bucket(), riak_object:key(), [index_spec()], binary(), state()) ->
                 {ok, state()} |
                 {error, term(), state()}.
put(Bucket, PrimaryKey, _IndexSpecs, Val, State=#state{data_ref=DataRef,
                                                       max_memory=MaxMemory,
                                                       time_ref=TimeRef,
                                                       ttl=TTL,
                                                       used_memory=UsedMemory}) ->
    Now = now(),
    case TTL of
        undefined ->
            Val1 = Val;
        _ ->
            Val1 = {{ts, Now}, Val}
    end,
    case do_put(Bucket, PrimaryKey, Val1, DataRef) of
        {ok, Size} ->
            %% If the memory is capped update timestamp table
            %% and check if the memory usage is over the cap.
            case MaxMemory of
                undefined ->
                    UsedMemory1 = UsedMemory;
                _ ->
                    time_entry(Bucket, PrimaryKey, Now, TimeRef),
                    Freed = trim_data_table(MaxMemory,
                                            UsedMemory + Size,
                                            DataRef,
                                            TimeRef,
                                            0),
                    UsedMemory1 = UsedMemory + Size - Freed
            end,
            {ok, State#state{used_memory=UsedMemory1}};
        {error, Reason} ->
            {error, Reason, State}
    end.

%% @doc Delete an object from the memory backend
%% NOTE: The memory backend does not currently
%% support secondary indexing and the _IndexSpecs
%% parameter is ignored.
-spec delete(riak_object:bucket(), riak_object:key(), [index_spec()], state()) ->
                    {ok, state()}.
delete(Bucket, Key, _IndexSpecs, State=#state{data_ref=DataRef,
                                              time_ref=TimeRef,
                                              used_memory=UsedMemory}) ->
    case TimeRef of
        undefined ->
            UsedMemory1 = UsedMemory;
        _ ->
            %% Lookup the object so we can delete its
            %% entry from the time table and account
            %% for the memory used.
            [Object] = ets:lookup(DataRef, {Bucket, Key}),
            case Object of
                {_, {{ts, Timestamp}, _}} ->
                    ets:delete(TimeRef, Timestamp),
                    UsedMemory1 = UsedMemory - object_size(Object);
                _ ->
                    UsedMemory1 = UsedMemory
            end
    end,
    ets:delete(DataRef, {Bucket, Key}),
    {ok, State#state{used_memory=UsedMemory1}}.

%% @doc Fold over all the buckets.
-spec fold_buckets(riak_kv_backend:fold_buckets_fun(),
                   any(),
                   [],
                   state()) -> {ok, any()}.
fold_buckets(FoldBucketsFun, Acc, Opts, #state{data_ref=DataRef}) ->
    FoldFun = fold_buckets_fun(FoldBucketsFun),
    case lists:member(async_fold, Opts) of
        true ->
            BucketFolder =
                fun() ->
                        {Acc0, _} = ets:foldl(FoldFun, {Acc, sets:new()}, DataRef),
                        Acc0
                end,
            {async, BucketFolder};
        false ->
            {Acc0, _} = ets:foldl(FoldFun, {Acc, sets:new()}, DataRef),
            {ok, Acc0}
    end.

%% @doc Fold over all the keys for one or all buckets.
-spec fold_keys(riak_kv_backend:fold_keys_fun(),
                any(),
                [{atom(), term()}],
                state()) -> {ok, term()} | {async, fun()}.
fold_keys(FoldKeysFun, Acc, Opts, #state{data_ref=DataRef}) ->
    Bucket =  proplists:get_value(bucket, Opts),
    FoldFun = fold_keys_fun(FoldKeysFun, Bucket),
    case lists:member(async_fold, Opts) of
        true ->
            {async, get_folder(FoldFun, Acc, DataRef)};
        false ->
            Acc0 = ets:foldl(FoldFun, Acc, DataRef),
            {ok, Acc0}
    end.

%% @doc Fold over all the objects for one or all buckets.
-spec fold_objects(riak_kv_backend:fold_objects_fun(),
                   any(),
                   [{atom(), term()}],
                   state()) -> {ok, any()} | {async, fun()}.
fold_objects(FoldObjectsFun, Acc, Opts, #state{data_ref=DataRef}) ->
    Bucket =  proplists:get_value(bucket, Opts),
    FoldFun = fold_objects_fun(FoldObjectsFun, Bucket),
    case lists:member(async_fold, Opts) of
        true ->
            {async, get_folder(FoldFun, Acc, DataRef)};
        false ->
            Acc0 = ets:foldl(FoldFun, Acc, DataRef),
            {ok, Acc0}
    end.

%% @doc Delete all objects from this memory backend
-spec drop(state()) -> {ok, state()}.
drop(State=#state{data_ref=DataRef,
                  time_ref=TimeRef}) ->
    ets:delete_all_objects(DataRef),
    case TimeRef of
        undefined ->
            ok;
        _ ->
            ets:delete_all_objects(TimeRef)
    end,
    {ok, State}.

%% @doc Returns true if this memory backend contains any
%% non-tombstone values; otherwise returns false.
-spec is_empty(state()) -> boolean().
is_empty(#state{data_ref=DataRef}) ->
    ets:info(DataRef, size) =:= 0.

%% @doc Get the status information for this memory backend
-spec status(state()) -> [{atom(), term()}].
status(#state{data_ref=DataRef,
              time_ref=TimeRef}) ->
    DataStatus = ets:info(DataRef),
    case TimeRef of
        undefined ->
            [{data_table_status, DataStatus}];
        _ ->
            TimeStatus = ets:info(TimeRef),
            [{data_table_status, DataStatus},
             {time_table_status, TimeStatus}]
    end.

%% @doc Register an asynchronous callback
-spec callback(reference(), any(), state()) -> {ok, state()}.
callback(_Ref, _Msg, State) ->
    {ok, State}.

%% ===================================================================
%% Internal functions
%% ===================================================================

%% @TODO Some of these implementations may be suboptimal.
%% Need to do some measuring and testing to refine the
%% implementations.

%% @private
%% Return a function to fold over the buckets on this backend
fold_buckets_fun(FoldBucketsFun) ->
    fun({{Bucket, _}, _}, {Acc, BucketSet}) ->
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
    fun({{Bucket, Key}, _}, Acc) ->
            FoldKeysFun(Bucket, Key, Acc)
    end;
fold_keys_fun(FoldKeysFun, Bucket) ->
    fun({{B, Key}, _}, Acc) ->
            case B =:= Bucket of
                true ->
                    FoldKeysFun(Bucket, Key, Acc);
                false ->
                    Acc
            end
    end.

%% @private
%% Return a function to fold over keys on this backend
fold_objects_fun(FoldObjectsFun, undefined) ->
    fun({{Bucket, Key}, Value}, Acc) ->
            FoldObjectsFun(Bucket, Key, Value, Acc)
    end;
fold_objects_fun(FoldObjectsFun, Bucket) ->
    fun({{B, Key}, Value}, Acc) ->
            case B =:= Bucket of
                true ->
                    FoldObjectsFun(Bucket, Key, Value, Acc);
                false ->
                    Acc
            end
    end.

%% @private
get_folder(FoldFun, Acc, DataRef) ->
    fun() ->
            ets:foldl(FoldFun, Acc, DataRef)
    end.

%% @private
do_put(Bucket, Key, Val, Ref) ->
    Object = {{Bucket, Key}, Val},
    true = ets:insert(Ref, Object),
    {ok, object_size(Object)}.

%% Check if this timestamp is past the ttl setting.
exceeds_ttl(Timestamp, TTL) ->
    Diff = (timer:now_diff(now(), Timestamp) / 1000 / 1000),
    Diff > TTL.

%% @private
time_entry(Bucket, Key, Now, TimeRef) ->
    ets:insert(TimeRef, {Now, {Bucket, Key}}).

%% @private
%% @doc Dump some entries if the max memory size has
%% been breached.
trim_data_table(MaxMemory, UsedMemory, _, _, Freed) when
      (UsedMemory - Freed) =< MaxMemory ->
    Freed;
trim_data_table(MaxMemory, UsedMemory, DataRef, TimeRef, Freed) ->
    %% Delete the oldest object
    OldestSize = delete_oldest(DataRef, TimeRef),
    trim_data_table(MaxMemory,
                    UsedMemory,
                    DataRef,
                    TimeRef,
                    Freed + OldestSize).

%% @private
delete_oldest(DataRef, TimeRef) ->
    OldestTime = ets:first(TimeRef),
    case OldestTime of
        '$end_of_table' ->
            0;
        _ ->
            OldestKey = ets:lookup_element(TimeRef, OldestTime, 2),
            ets:delete(TimeRef, OldestTime),
            case ets:lookup(DataRef, OldestKey) of
                [] ->
                    delete_oldest(DataRef, TimeRef);
                [Object] ->
                    ets:delete(DataRef, OldestKey),
                    object_size(Object)
            end
    end.

%% @private
object_size(Object) ->
    case Object of
        {{Bucket, Key}, {{ts, _}, Val}} ->
            ok;
        {{Bucket, Key}, Val} ->
            ok
    end,
    size(Bucket) + size(Key) + size(Val).

%% ===================================================================
%% EUnit tests
%% ===================================================================

-ifdef(TEST).

simple_test_() ->
    riak_kv_backend:standard_test(?MODULE, []).

ttl_test_() ->
    Config = [{ttl, 15}],
    {ok, State} = start(42, Config),

    Bucket = <<"Bucket">>,
    Key = <<"Key">>,
    Value = <<"Value">>,

    [
     %% Put an object
     ?_assertEqual({ok, State}, put(Bucket, Key, [], Value, State)),
     %% Wait 1 second to access it
     ?_assertEqual(ok, timer:sleep(1000)),
     ?_assertEqual({ok, Value, State}, get(Bucket, Key, State)),
     %% Wait 3 seconds and access it again
     ?_assertEqual(ok, timer:sleep(3000)),
     ?_assertEqual({ok, Value, State}, get(Bucket, Key, State)),
     %% Wait 15 seconds and it should expire
     {timeout, 30000, ?_assertEqual(ok, timer:sleep(15000))},
     %% This time it should be gone
     ?_assertEqual({error, not_found, State}, get(Bucket, Key, State))
    ].

%% @private
max_memory_test_() ->
    %% Set max size to 1.5kb
    Config = [{max_memory, 1.5 * (1 / 1024)}],
    {ok, State} = start(42, Config),

    Bucket = <<"Bucket">>,
    Key1 = <<"Key1">>,
    Value1 = list_to_binary(string:copies("1", 1024)),
    Key2 = <<"Key2">>,
    Value2 = list_to_binary(string:copies("2", 1024)),

    %% Write Key1 to the datastore
    {ok, State1} = put(Bucket, Key1, [], Value1, State),
    timer:sleep(timer:seconds(1)),
    %% Write Key2 to the datastore
    {ok, State2} = put(Bucket, Key2, [], Value2, State1),

    [
     %% Key1 should be kicked out
     ?_assertEqual({error, not_found, State2}, get(Bucket, Key1, State2)),
     %% Key2 should still be present
     ?_assertEqual({ok, Value2, State2}, get(Bucket, Key2, State2))
    ].

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
                         backend_eqc:test(?MODULE, true))]}
        ]}]}]}.

setup() ->
    application:load(sasl),
    application:set_env(sasl, sasl_error_logger, {file, "riak_kv_memory_backend_eqc_sasl.log"}),
    error_logger:tty(false),
    error_logger:logfile({open, "riak_kv_memory_backend_eqc.log"}),
    ok.

cleanup(_) ->
    ok.

-endif. % EQC

-endif. % TEST
