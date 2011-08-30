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
%% <li>`object_expiry_enabled' - Boolean option to enable time-based object
%%    expiration. Defaults to `false'.</li>
%% <li>`ttl' - The time in seconds that an object should live before being expired.
%%    Default is 3600 seconds or 1 hour. This option is ignored unless 
%%    `object_expiry_enabled' is `true'.</li>
%% <li>`memory_cap_enabled' - Boolean option to enable a cap on the amount
%%    of data that can be stored by the memory backend. When the memory
%%    limit is reached, the objects that have been least recently accessed
%%    are expunged until the amount of memory used is under the limit.
%%    Default is `false'.</li>
%% <li>`max_memory' - The amount of memory in megabytes to limit the backend to.
%%    Default is 100 MB. This option is ignored unless 
%%    `memory_cap_enabled' is `true'.</li>
%% </ul>
%%

-module(riak_kv_memory_backend).
-behavior(riak_kv_backend).
-behavior(gen_server).

%% KV Backend API
-export([api_version/0,
         start/2,
         stop/1,
         get/3,
         put/4,
         delete/3,
         drop/1,
         fold_buckets/4,
         fold_keys/4,
         fold_objects/4,
         is_empty/1,
         status/1,
         callback/3]).

%% gen_server API
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(API_VERSION, 1).
-define(CAPABILITIES, []).
-define (DEFAULT_TTL, 3600). % 1 hour
-define (DEFAULT_MEMORY,  100).  % 100MB

-record(server_state, {data_ref :: integer() | atom(),
                       time_ref :: integer() | atom(),
                       expiry_enabled :: boolean(),
                       memory_cap_enabled :: boolean(),
                       max_memory :: undefined | integer(),
                       used_memory=0 :: integer(),
                       ttl :: integer()}).
-type state() :: pid().
-type config() :: [].

%% ===================================================================
%% Public API
%% ===================================================================

%% KV Backend API

%% @doc Return the major version of the
%% current API and a capabilities list.
-spec api_version() -> {integer(), [atom()]}.
api_version() ->
    {?API_VERSION, ?CAPABILITIES}.

%% @doc Start the memory backend
-spec start(integer(), config()) -> {ok, state()} | {error, term()}.
start(Partition, Config) ->
    case gen_server:start_link(?MODULE, [Partition, Config], []) of
        {ok, Pid} ->
            {ok, Pid};
        {error, Reason} ->
            {error, Reason};
        ignore ->
            {error, ignore}
    end.

%% @doc Stop the memory backend
-spec stop(state()) -> ok.
stop(State) ->
    gen_server:call(State, stop).

%% @doc Retrieve an object from the memory backend
-spec get(riak_object:bucket(), riak_object:key(), state()) ->
                 {ok, any(), state()} |
                 {ok, not_found, state()} |
                 {error, term(), state()}.
get(Bucket, Key, State) ->
    case gen_server:call(State, {get, Bucket, Key}) of
        {ok, Value} ->
            {ok, Value, State};
        {error, Reason} ->
            {error, Reason, State}
    end.

%% @doc Insert an object into the memory backend
-spec put(riak_object:bucket(), riak_object:key(), binary(), state()) ->
                 {ok, state()} |
                 {error, term(), state()}.
put(Bucket, Key, Val, State) ->
    case gen_server:call(State, {put, Bucket, Key, Val}) of
        ok ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end.

%% @doc Delete an object from the memory backend
-spec delete(riak_object:bucket(), riak_object:key(), state()) ->
                    {ok, state()} |
                    {error, term(), state()}.
delete(Bucket, Key, State) ->
    case gen_server:call(State, {delete, Bucket, Key}) of
        ok ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end.

%% @doc Fold over all the buckets.
-spec fold_buckets(riak_kv_backend:fold_buckets_fun(),
                   any(),
                   [],
                   state()) -> {ok, any()} | {error, term()}.
fold_buckets(FoldBucketsFun, Acc, _Opts, State) ->
    FoldFun = fold_buckets_fun(FoldBucketsFun),
    gen_server:call(State, {fold_buckets, FoldFun, Acc}).

%% @doc Fold over all the keys for one or all buckets.
-spec fold_keys(riak_kv_backend:fold_keys_fun(),
                any(),
                [{atom(), term()}],
                state()) -> {ok, term()} | {error, term()}.
fold_keys(FoldKeysFun, Acc, Opts, State) ->
    Bucket =  proplists:get_value(bucket, Opts),
    FoldFun = fold_keys_fun(FoldKeysFun, Bucket),
    gen_server:call(State, {fold_keys, FoldFun, Bucket, Acc}).

%% @doc Fold over all the objects for one or all buckets.
-spec fold_objects(riak_kv_backend:fold_objects_fun(),
                   any(),
                   [{atom(), term()}],
                   state()) -> {ok, any()} | {error, term()}.
fold_objects(FoldObjectsFun, Acc, Opts, State) ->
    Bucket =  proplists:get_value(bucket, Opts),
    FoldFun = fold_objects_fun(FoldObjectsFun, Bucket),
    gen_server:call(State, {fold_objects, FoldFun, Bucket, Acc}).

%% @doc Delete all objects from this memory backend
-spec drop(state()) -> {ok, state()} | {error, term(), state()}.
drop(State) ->
    gen_server:call(State, drop).

%% @doc Returns true if this memory backend contains any
%% non-tombstone values; otherwise returns false.
-spec is_empty(state()) -> boolean() | {error, term()}.
is_empty(State) ->
    gen_server:call(State, is_empty).

%% @doc Get the status information for this memory backend
-spec status(state()) -> [{atom(), term()}].
status(State) ->
    gen_server:call(State, status).

%% @doc Register an asynchronous callback
-spec callback(reference(), any(), state()) -> {ok, state()}.
callback(_Ref, _Msg, State) ->
    {ok, State}.

%% gen_server API

%% @private
init([Partition, Config]) ->
    ExpiryEnabled = config_value(object_expiry_enabled, Config, false),
    TTL = config_value(ttl, Config, ?DEFAULT_TTL),
    MemoryCapEnabled = config_value(memory_cap_enabled, Config, false),
    MaxMemory = config_value(max_memory, Config, ?DEFAULT_MEMORY),
    case MemoryCapEnabled of
        true ->
            TimeRef = ets:new(list_to_atom(integer_to_list(Partition)), [ordered_set]);
        false ->
            TimeRef = undefined
    end,
    DataRef = ets:new(list_to_atom(integer_to_list(Partition)), []),
    {ok, #server_state{data_ref=DataRef,
                       expiry_enabled=ExpiryEnabled,
                       max_memory=MaxMemory * 1024 * 1024,
                       memory_cap_enabled=MemoryCapEnabled,
                       time_ref=TimeRef,
                       ttl=TTL}}.

%% @private
handle_call(stop, _From, #server_state{data_ref=Ref}=State) ->
    {reply, srv_stop(Ref), State};
handle_call({get, Bucket, Key},
            _From,
            #server_state{expiry_enabled=ExpiryEnabled,
                          memory_cap_enabled=MemoryCapEnabled,
                          ttl=TTL,
                          data_ref=Ref}=State) when ExpiryEnabled =:= true;
                                                    MemoryCapEnabled =:= true ->
    Result = srv_get(Bucket, Key, Ref),
    case Result of
        {ok, {{ts, Timestamp}, Val}} ->
            case exceeds_ttl(Timestamp, TTL) of
                true ->
                    srv_delete(Bucket, Key, Ref),
                    Reply = {error, not_found};
                false ->
                    Reply = {ok, Val}
            end;
        _ ->
            Reply = Result
    end,
    {reply, Reply, State};
handle_call({get, Bucket, Key}, _From, #server_state{data_ref=Ref}=State) ->
    {reply, srv_get(Bucket, Key, Ref), State};
handle_call({put, Bucket, Key, Val},
            _From,
            #server_state{data_ref=DataRef,
                          expiry_enabled=ExpiryEnabled,
                          max_memory=MaxMemory,
                          memory_cap_enabled=MemoryCapEnabled,
                          time_ref=TimeRef,
                          used_memory=UsedMemory}=State) when ExpiryEnabled =:= true;
                                                              MemoryCapEnabled =:= true ->
    Now = now(),
    {Reply, Size} = srv_put(Bucket, Key, {{ts, Now}, Val}, DataRef),

    %% If the memory cap is enabled update timestamp table
    %% and check if the memory usage is over the cap.
    case MemoryCapEnabled of
        true ->
            time_entry(Bucket, Key, Now, TimeRef),
            Freed = trim_data_table(MaxMemory,
                                    UsedMemory + Size,
                                    DataRef,
                                    TimeRef,
                                    0),
            UsedMemory1 = UsedMemory + Size - Freed;
        false ->
            UsedMemory1 = UsedMemory
    end,
    {reply, Reply, State#server_state{used_memory=UsedMemory1}};
handle_call({put, Bucket, Key, Val}, _From, #server_state{data_ref=Ref}=State) ->
    {Reply, _} = srv_put(Bucket, Key, Val, Ref),
    {reply, Reply, State};
handle_call({delete, Bucket, Key}, _From, #server_state{data_ref=Ref}=State) ->
    {reply, srv_delete(Bucket, Key, Ref), State};
handle_call({fold_buckets, FoldFun, Acc},
            _From,
            #server_state{data_ref=Ref}=State) ->
    {reply, srv_fold_buckets(FoldFun, Acc, Ref), State};
handle_call({fold_keys, FoldFun, Bucket, Acc},
            _From,
            #server_state{data_ref=Ref}=State) ->
    {reply, srv_fold_keys(FoldFun, Bucket, Acc, Ref), State};
handle_call({fold_objects, FoldFun, Bucket, Acc},
            _From,
            #server_state{data_ref=Ref}=State) ->
    {reply, srv_fold_objects(FoldFun, Bucket, Acc, Ref), State};
handle_call(drop, _From, #server_state{data_ref=Ref}=State) ->
    ets:delete_all_objects(Ref),
    {reply, {ok, self()}, State};
handle_call(is_empty, _From, #server_state{data_ref=Ref}=State) ->
    {reply, ets:info(Ref, size) =:= 0, State};
handle_call(status, _From, #server_state{data_ref=Ref}=State) ->
    {reply, ets:info(Ref), State}.


%% @private
handle_cast(_, State) -> {noreply, State}.

%% @private
handle_info(_Msg, State) -> {noreply, State}.

%% @private
terminate(_Reason, _State) -> ok.

%% @private
code_change(_OldVsn, State, _Extra) -> {ok, State}.

%% ===================================================================
%% Internal functions
%% ===================================================================

%% @TODO Some of these implementations may be suboptimal.
%% Need to do some measuring and testing to refine the
%% implementations.

%% @private
%% Return a function to fold over the buckets on this backend
fold_buckets_fun(FoldBucketsFun) ->
    fun([Bucket], Acc) ->
            FoldBucketsFun(Bucket, Acc)
    end.

%% @private
%% Return a function to fold over keys on this backend
fold_keys_fun(FoldKeysFun, undefined) ->
    fun([Bucket, Key], Acc) ->
            FoldKeysFun(Bucket, Key, Acc)
    end;
fold_keys_fun(FoldKeysFun, Bucket) ->
    fun([Key], Acc) ->
            FoldKeysFun(Bucket, Key, Acc)
    end.

%% @private
%% Return a function to fold over keys on this backend
fold_objects_fun(FoldObjectsFun, _) ->
    fun({{Bucket, Key}, Value}, Acc) ->
            FoldObjectsFun(Bucket, Key, Value, Acc)
    end.

%% @private
srv_stop(Ref) ->
    catch ets:delete(Ref),
    ok.

%% @private
srv_get(Bucket, Key, Ref) ->
    case ets:lookup(Ref, {Bucket, Key}) of
        [] -> {error, not_found};
        [{{Bucket, Key}, Val}] -> {ok, Val};
        Error -> {error, Error}
    end.

%% @private
srv_put(Bucket, Key, Val, Ref) ->
    Object = {{Bucket, Key}, Val},
    true = ets:insert(Ref, Object),
    {ok, object_size(Object)}.

%% @private
srv_delete(Bucket, Key, Ref) ->
    true = ets:delete(Ref, {Bucket, Key}),
    ok.

%% @private
srv_fold_buckets(FoldFun, Acc, Ref) ->
    BucketList = ets:match(Ref, {{'$1', '_'}, '_'}),
    Acc0 = lists:foldl(FoldFun, Acc, BucketList),
    {ok, Acc0}.

%% @private
srv_fold_keys(FoldFun, Bucket, Acc, Ref) ->
    case Bucket of
        undefined ->
            KeyList = ets:match(Ref, {{'$1', '$2'}, '_'});
        _ ->
            KeyList = ets:match(Ref, {{Bucket, '$1'}, '_'})
    end,
    Acc0 = lists:foldl(FoldFun, Acc, KeyList),
    {ok, Acc0}.

%% @private
srv_fold_objects(FoldFun, Bucket, Acc, Ref) ->
    case Bucket of
        undefined ->
            ObjectList = ets:match_object(Ref, {{'_', '_'}, '_'});
        _ ->
            ObjectList = ets:match_object(Ref, {{Bucket, '_'}, '_'})
    end,
    Acc0 = lists:foldl(FoldFun, Acc, ObjectList),
    {ok, Acc0}.

%% @private
config_value(Key, Config, Default) ->
    case proplists:get_value(Key, Config) of
        undefined ->
            app_helper:get_env(memory_backend, Key, Default);
        Value ->
            Value
    end.

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
    Config = [{object_expiry_enabled, true},
              {ttl, 15}],
    {ok, State} = start(42, Config),

    Bucket = <<"Bucket">>,
    Key = <<"Key">>,
    Value = <<"Value">>,

    {spawn,
     [
      %% Put an object
      ?_assertEqual({ok, State}, put(Bucket, Key, Value, State)),
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
     ]}.

%% @private
max_memory_test_() ->
    %% Set max size to 1.5kb
    Config = [{max_memory, 1.5 * (1 / 1024)},
              {memory_cap_enabled, true}],
    {ok, State} = start(42, Config),

    Bucket = <<"Bucket">>,
    Key1 = <<"Key1">>,
    Value1 = list_to_binary(string:copies("1", 1024)),
    Key2 = <<"Key2">>,
    Value2 = list_to_binary(string:copies("2", 1024)),

    {spawn,
     [
      %% Write Key1 to the datastore
      ?_assertEqual({ok, State}, put(Bucket, Key1, Value1, State)),
      %% Fetch it
      ?_assertEqual({ok, Value1, State}, get(Bucket, Key1, State)),
      %% Pause for a second to let clocks increment
      ?_assertEqual(ok, timer:sleep(timer:seconds(1))),
      %% Write Key2 to the datastore
      ?_assertEqual({ok, State}, put(Bucket, Key2, Value2, State)),
      %% Key1 should be kicked out
      ?_assertEqual({error, not_found, State}, get(Bucket, Key1, State)),
      %% Key2 should still be present
      ?_assertEqual({ok, Value2, State}, get(Bucket, Key2, State))
     ]}.

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
