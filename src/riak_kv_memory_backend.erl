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
%% <li>`test' - When true, allow public access to ETS tables so they can be cleared efficiently.</li>
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

-export([data_size/1]).

%% "Testing" backend API
-export([reset/0]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-compile([export_all]).
-endif.

-define(API_VERSION, 1).
-define(CAPABILITIES, [async_fold, indexes, size]).

%% Macros for working with indexes
-define(DELETE_PTN(B,K), {{B,'_','_',K},'_'}).

%% ETS table name macros so we can break encapsulation for testing
%% mode
-define(DNAME(P), list_to_atom("riak_kv_"++integer_to_list(P))).
-define(INAME(P), list_to_atom("riak_kv_"++integer_to_list(P)++"_i")).
-define(TNAME(P), list_to_atom("riak_kv_"++integer_to_list(P)++"_t")).

-record(state, {data_ref :: ets:tid(),
                index_ref :: ets:tid(),
                time_ref :: ets:tid(),
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
    TableOpts = case app_helper:get_prop_or_env(test, Config, memory_backend) of
                    true ->
                        [ordered_set, public, named_table];
                    _ ->
                        [ordered_set]
                end,
    case MemoryMB of
        undefined ->
            MaxMemory = undefined,
            TimeRef = undefined;
        _ ->
            MaxMemory = MemoryMB * 1024 * 1024,
            TimeRef = ets:new(?TNAME(Partition), TableOpts)
    end,
    IndexRef = ets:new(?INAME(Partition), TableOpts),
    DataRef = ets:new(?DNAME(Partition), TableOpts),
    {ok, #state{data_ref=DataRef,
                index_ref=IndexRef,
                max_memory=MaxMemory,
                time_ref=TimeRef,
                ttl=TTL}}.

%% @doc Stop the memory backend
-spec stop(state()) -> ok.
stop(#state{data_ref=DataRef,
            index_ref=IndexRef,
            max_memory=MaxMemory,
            time_ref=TimeRef}) ->
    catch ets:delete(DataRef),
    catch ets:delete(IndexRef),
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
                              index_ref=IndexRef,
                              used_memory=UsedMemory,
                              max_memory=MaxMemory,
                              ttl=TTL}) ->
    case ets:lookup(DataRef, {Bucket, Key}) of
        [] -> {error, not_found, State};
        [{{Bucket, Key}, {{ts, Timestamp}, Val}}=Object] ->
            case exceeds_ttl(Timestamp, TTL) of
                true ->
                    %% Because we do not have the IndexSpecs, we must
                    %% delete the object directly and all index
                    %% entries blindly using match_delete.
                    ets:delete(DataRef, {Bucket, Key}),
                    ets:match_delete(IndexRef, ?DELETE_PTN(Bucket, Key)),
                    case MaxMemory of
                        undefined ->
                            UsedMemory1 = UsedMemory;
                        _ ->
                            UsedMemory1 = UsedMemory - object_size(Object)
                    end,
                    {error, not_found, State#state{used_memory=UsedMemory1}};
                false ->
                    {ok, Val, State}
            end;
        [{{Bucket, Key}, Val}] ->
            {ok, Val, State};
        Error ->
            {error, Error, State}
    end.

%% @doc Insert an object into the memory backend.
-type index_spec() :: {add, Index, SecondaryKey} | {remove, Index, SecondaryKey}.
-spec put(riak_object:bucket(), riak_object:key(), [index_spec()], binary(), state()) ->
                 {ok, state()}.
put(Bucket, PrimaryKey, IndexSpecs, Val, State=#state{data_ref=DataRef,
                                                      index_ref=IndexRef,
                                                      max_memory=MaxMemory,
                                                      time_ref=TimeRef,
                                                      ttl=TTL,
                                                      used_memory=UsedMemory}) ->
    Now = os:timestamp(),
    case TTL of
        undefined ->
            Val1 = Val;
        _ ->
            Val1 = {{ts, Now}, Val}
    end,
    {ok, Size} = do_put(Bucket, PrimaryKey, Val1, IndexSpecs, DataRef, IndexRef),
    case MaxMemory of
        undefined ->
            UsedMemory1 = UsedMemory;
        _ ->
            time_entry(Bucket, PrimaryKey, Now, TimeRef),
            Freed = trim_data_table(MaxMemory,
                                    UsedMemory + Size,
                                    DataRef,
                                    TimeRef,
                                    IndexRef,
                                    0),
            UsedMemory1 = UsedMemory + Size - Freed
    end,
    {ok, State#state{used_memory=UsedMemory1}}.

%% @doc Delete an object from the memory backend
-spec delete(riak_object:bucket(), riak_object:key(), [index_spec()], state()) ->
                    {ok, state()}.
delete(Bucket, Key, IndexSpecs, State=#state{data_ref=DataRef,
                                             index_ref=IndexRef,
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
    update_indexes(Bucket, Key, IndexSpecs, IndexRef),
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
fold_keys(FoldKeysFun, Acc, Opts, #state{data_ref=DataRef,
                                         index_ref=IndexRef}) ->

    %% Figure out how we should limit the fold: by bucket, by
    %% secondary index, or neither (fold across everything.)
    Bucket = lists:keyfind(bucket, 1, Opts),
    Index = lists:keyfind(index, 1, Opts),

    %% Multiple limiters may exist. Take the most specific limiter,
    %% get an appropriate folder function.
    Folder = if
                 Index /= false  ->
                     FoldFun = fold_keys_fun(FoldKeysFun, Index),
                     get_index_folder(FoldFun, Acc, Index, DataRef, IndexRef);
                 Bucket /= false ->
                     FoldFun = fold_keys_fun(FoldKeysFun, Bucket),
                     get_folder(FoldFun, Acc, DataRef);
                 true ->
                     FoldFun = fold_keys_fun(FoldKeysFun, undefined),
                     get_folder(FoldFun, Acc, DataRef)
             end,

    case lists:member(async_fold, Opts) of
        true ->
            {async, Folder};
        false ->
            {ok, Folder()}
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
                  index_ref=IndexRef,
                  time_ref=TimeRef}) ->
    ets:delete_all_objects(DataRef),
    ets:delete_all_objects(IndexRef),
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
              index_ref=IndexRef,
              time_ref=TimeRef}) ->
    DataStatus = ets:info(DataRef),
    IndexStatus = ets:info(IndexRef),
    case TimeRef of
        undefined ->
            [{data_table_status, DataStatus},
             {index_table_status, IndexStatus}];
        _ ->
            TimeStatus = ets:info(TimeRef),
            [{data_table_status, DataStatus},
             {index_table_status, IndexStatus},
             {time_table_status, TimeStatus}]
    end.

%% @doc Get the size of the memory backend. Returns a dynamic size
%%      since new writes may appear in an ets fold
-spec data_size(state()) -> undefined | {function(), dynamic}.
data_size(#state{data_ref=DataRef}) ->
    F = fun() ->
                DataStatus = ets:info(DataRef),
                case proplists:get_value(size, DataStatus) of
                    undefined -> undefined;
                    ObjCount -> {ObjCount, objects}
                end
        end,
    {F, dynamic}.

%% @doc Register an asynchronous callback
-spec callback(reference(), any(), state()) -> {ok, state()}.
callback(_Ref, _Msg, State) ->
    {ok, State}.

%% @doc Resets state of all running memory backends on the local
%% node. The `riak_kv' environment variable `memory_backend' must
%% contain the `test' property, set to `true' for this to work.
-spec reset() -> ok | {error, reset_disabled}.
reset() ->
    reset(app_helper:get_env(memory_backend, test, app_helper:get_env(riak_kv, test)), app_helper:get_env(riak_kv, storage_backend)).

reset(true, ?MODULE) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    [ begin
          catch ets:delete_all_objects(?DNAME(I)),
          catch ets:delete_all_objects(?INAME(I)),
          catch ets:delete_all_objects(?TNAME(I))
      end || I <- riak_core_ring:my_indices(Ring) ],
    ok;
reset(_, _) ->
    {error, reset_disabled}.

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
            FoldKeysFun(Bucket, Key, Acc);
       (_, Acc) ->
            Acc
    end;
fold_keys_fun(FoldKeysFun, {bucket, FilterBucket}) ->
    fun({{Bucket, Key}, _}, Acc) when Bucket == FilterBucket ->
            FoldKeysFun(Bucket, Key, Acc);
       (_, Acc) ->
            Acc
    end;
fold_keys_fun(FoldKeysFun, {index, FilterBucket, {eq, <<"$bucket">>, _}}) ->
    %% 2I exact match query on special $bucket field...
    fold_keys_fun(FoldKeysFun, {bucket, FilterBucket});
fold_keys_fun(FoldKeysFun, {index, FilterBucket, {range, <<"$key">>, _, _}}) ->
    %% 2I range query on special $key field...
    fold_keys_fun(FoldKeysFun, {bucket, FilterBucket});
fold_keys_fun(FoldKeysFun, {index, FilterBucket, {eq, <<"$key">>, _}}) ->
    %% 2I eq query on special $key field...
    fold_keys_fun(FoldKeysFun, {bucket, FilterBucket});
fold_keys_fun(FoldKeysFun, {index, _FilterBucket, _Query}) ->
    fun({{Bucket, _FilterField, _FilterTerm, Key}, _}, Acc) ->
            FoldKeysFun(Bucket, Key, Acc);
       (_, Acc) ->
            Acc
    end.


%% @private
%% Return a function to fold over keys on this backend
fold_objects_fun(FoldObjectsFun, undefined) ->
    fun({{Bucket, Key}, Value}, Acc) ->
            FoldObjectsFun(Bucket, Key, Value, Acc);
       (_, Acc) ->
            Acc
    end;
fold_objects_fun(FoldObjectsFun, FilterBucket) ->
    fun({{Bucket, Key}, Value}, Acc) when Bucket == FilterBucket->
            FoldObjectsFun(Bucket, Key, Value, Acc);
       (_, Acc) ->
            Acc
    end.

%% @private
get_folder(FoldFun, Acc, DataRef) ->
    fun() ->
            ets:foldl(FoldFun, Acc, DataRef)
    end.

%% @private
get_index_folder(Folder, Acc0, {index, Bucket, {eq, <<"$bucket">>, _}}, DataRef, _) ->
    %% For the special $bucket index, turn it into a fold over the
    %% data table.
    fun() ->
            key_range_folder(Folder, Acc0, DataRef, {Bucket, <<>>}, Bucket)
    end;
get_index_folder(Folder, Acc0, {index, Bucket, {range, <<"$key">>, Min, Max}}, DataRef, _) ->
    %% For the special range lookup on the $key index, turn it into a
    %% fold on the data table
    fun() ->
            key_range_folder(Folder, Acc0, DataRef, {Bucket, Min}, {Bucket, Min, Max})
    end;
get_index_folder(Folder, Acc0, {index, Bucket, {eq, <<"$key">>, Val}}, DataRef, IndexRef) ->
    get_index_folder(Folder, Acc0, {index, Bucket, {range, <<"$key">>, Val, Val}}, DataRef, IndexRef);
get_index_folder(Folder, Acc0, {index, Bucket, {eq, Field, Term}}, _, IndexRef) ->
    fun() ->
            index_range_folder(Folder, Acc0, IndexRef, {Bucket, Field, Term, undefined}, {Bucket, Field, Term, Term})
    end;
get_index_folder(Folder, Acc0, {index, Bucket, {range, Field, Min, Max}}, _, IndexRef) ->
    fun() ->
            index_range_folder(Folder, Acc0, IndexRef, {Bucket, Field, Min, undefined}, {Bucket, Field, Min, Max})
    end.


%% Iterates over a range of keys, for the special $key and $bucket
%% indexes.
%% @private
-spec key_range_folder(function(), term(), ets:tid(), {riak_object:bucket(), riak_object:key()}, binary() | {riak_object:bucket(), term(), term()}) -> term().
key_range_folder(Folder, Acc0, DataRef, {B,_}=DataKey, B) ->
    case ets:lookup(DataRef, DataKey) of
        [] ->
            key_range_folder(Folder, Acc0, DataRef, ets:next(DataRef, DataKey), B);
        [Object] ->
            Acc = Folder(Object, Acc0),
            key_range_folder(Folder, Acc, DataRef, ets:next(DataRef, DataKey), B)
    end;
key_range_folder(Folder, Acc0, DataRef, {B,K}=DataKey, {B, Min, Max}=Query) when K >= Min, K =< Max ->
    case ets:lookup(DataRef, DataKey) of
        [] ->
            key_range_folder(Folder, Acc0, DataRef, ets:next(DataRef, DataKey), Query);
        [Object] ->
            Acc = Folder(Object, Acc0),
            key_range_folder(Folder, Acc, DataRef, ets:next(DataRef, DataKey), Query)
    end;
key_range_folder(_Folder, Acc, _DataRef, _DataKey, _Query) ->
    Acc.

%% Iterates over a range of index postings
index_range_folder(Folder, Acc0, IndexRef, {B, I, V, _K}=IndexKey, {B, I, Min, Max}=Query) when V >= Min, V =< Max ->
    case ets:lookup(IndexRef, IndexKey) of
        [] ->
            %% This will happen on the first iteration, where the key
            %% does not exist. In all other cases, ETS will give us a
            %% real key from next/2.
            index_range_folder(Folder, Acc0, IndexRef, ets:next(IndexRef, IndexKey), Query);
        [Posting] ->
            Acc = Folder(Posting, Acc0),
            index_range_folder(Folder, Acc, IndexRef, ets:next(IndexRef, IndexKey), Query)
    end;
index_range_folder(_Folder, Acc, _IndexRef, _IndexKey, _Query) ->
    Acc.


%% @private
do_put(Bucket, Key, Val, IndexSpecs, DataRef, IndexRef) ->
    Object = {{Bucket, Key}, Val},
    true = ets:insert(DataRef, Object),
    update_indexes(Bucket, Key, IndexSpecs, IndexRef),
    {ok, object_size(Object)}.

%% Check if this timestamp is past the ttl setting.
exceeds_ttl(Timestamp, TTL) ->
    Diff = (timer:now_diff(os:timestamp(), Timestamp) / 1000 / 1000),
    Diff > TTL.

update_indexes(_Bucket, _Key, undefined, _IndexRef) ->
    ok;
update_indexes(_Bucket, _Key, [], _IndexRef) ->
    ok;
update_indexes(Bucket, Key, [{remove, Field, Value}|Rest], IndexRef) ->
    true = ets:delete(IndexRef, {Bucket, Field, Value, Key}),
    update_indexes(Bucket, Key, Rest, IndexRef);
update_indexes(Bucket, Key, [{add, Field, Value}|Rest], IndexRef) ->
    true = ets:insert(IndexRef, {{Bucket, Field, Value, Key}, <<>>}),
    update_indexes(Bucket, Key, Rest, IndexRef).

%% @private
time_entry(Bucket, Key, Now, TimeRef) ->
    ets:insert(TimeRef, {Now, {Bucket, Key}}).

%% @private
%% @doc Dump some entries if the max memory size has
%% been breached.
trim_data_table(MaxMemory, UsedMemory, _, _, _, Freed) when
      (UsedMemory - Freed) =< MaxMemory ->
    Freed;
trim_data_table(MaxMemory, UsedMemory, DataRef, TimeRef, IndexRef, Freed) ->
    %% Delete the oldest object
    OldestSize = delete_oldest(DataRef, TimeRef, IndexRef),
    trim_data_table(MaxMemory,
                    UsedMemory,
                    DataRef,
                    TimeRef,
                    IndexRef,
                    Freed + OldestSize).

%% @private
delete_oldest(DataRef, TimeRef, IndexRef) ->
    OldestTime = ets:first(TimeRef),
    case OldestTime of
        '$end_of_table' ->
            0;
        _ ->
            OldestKey = ets:lookup_element(TimeRef, OldestTime, 2),
            ets:delete(TimeRef, OldestTime),
            case ets:lookup(DataRef, OldestKey) of
                [] ->
                    delete_oldest(DataRef, TimeRef, IndexRef);
                [Object] ->
                    {Bucket, Key} = OldestKey,
                    ets:match_delete(IndexRef, ?DELETE_PTN(Bucket, Key)),
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

regression_367_key_range_test_() ->
    {ok, State} = start(142, []),
    Keys = [begin
                Bin = list_to_binary(integer_to_list(I)),
                if I < 10 ->
                        <<"obj0", Bin/binary>>;
                   true -> <<"obj", Bin/binary>>
                end
            end || I <- lists:seq(1,30) ],
    Bucket = <<"keyrange">>,
    Value = <<"foobarbaz">>,
    State1 = lists:foldl(fun(Key, IState) ->
                                 {ok, NewState} = put(Bucket, Key, [], Value, IState),
                                 NewState
                         end, State, Keys),
    Folder = fun(_B, K, Acc) ->
                     Acc ++ [K]
             end,
    [
     ?_assertEqual({ok, [<<"obj01">>]}, fold_keys(Folder, [], [{index, Bucket, {range, <<"$key">>, <<"obj01">>, <<"obj01">>}}], State1)),
     ?_assertEqual({ok, [<<"obj10">>,<<"obj11">>]}, fold_keys(Folder, [], [{index, Bucket, {range, <<"$key">>, <<"obj10">>, <<"obj11">>}}], State1)),
     ?_assertEqual({ok, [<<"obj01">>]}, fold_keys(Folder, [], [{index, Bucket, {range, <<"$key">>, <<"obj00">>, <<"obj01">>}}], State1)),
     ?_assertEqual({ok, lists:sort(Keys)}, fold_keys(Folder, [], [{index, Bucket, {range, <<"$key">>, <<"obj0">>, <<"obj31">>}}], State1)),
     ?_assertEqual({ok, []}, fold_keys(Folder, [], [{index, Bucket, {range, <<"$key">>, <<"obj31">>, <<"obj32">>}}], State1)),
     ?_assertEqual({ok, [<<"obj01">>]}, fold_keys(Folder, [], [{index, Bucket, {eq, <<"$key">>, <<"obj01">>}}], State1)),
     ?_assertEqual(ok, stop(State1))
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
