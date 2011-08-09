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

-record(state, {ref :: integer() | atom()}).

%% ===================================================================
%% Public API
%% ===================================================================

%% KV Backend API

%% @doc Return the major version of the
%% current API and a capabilities list.
api_version() ->
    {?API_VERSION, ?CAPABILITIES}.

%% @doc Start the memory backend
%% @spec start(Partition :: integer(), Config :: proplist()) ->
%%                        {ok, state()} | {{error, Reason :: term()}, state()}
start(Partition, _Config) ->
    gen_server:start_link(?MODULE, [Partition], []).

%% @spec stop(state()) -> ok | {error, Reason :: term()}
stop(State) ->
    gen_server:call(State, stop).

%% get(riak_object:bucket(), riak_object:key(), state()) ->
%%   {ok, Val :: binary()} | {error, Reason :: term()}
%% key must be 160b
get(Bucket, Key, State) ->
    case gen_server:call(State, {get, Bucket, Key}) of
        {ok, Value} ->
            {ok, Value, State};
        {error, Reason} ->
            {error, Reason, State}
    end.

%% put(riak_object:bucket(), riak_object:key(), binary(), state()) ->
%%   ok | {error, Reason :: term()}
%% key must be 160b
put(Bucket, Key, Val, State) ->
    case gen_server:call(State, {put, Bucket, Key, Val}) of
        ok ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end.

%% delete(riak_object:bucket(), riak_object:key(), state()) ->
%%   ok | {error, Reason :: term()}
%% key must be 160b
delete(Bucket, Key, State) ->
    case gen_server:call(State, {delete, Bucket, Key}) of
        ok ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end.

%% @doc Fold over all the buckets.
fold_buckets(FoldBucketsFun, Acc, _Opts, State) ->
    FoldFun = fold_buckets_fun(FoldBucketsFun),
    gen_server:call(State, {fold_buckets, FoldFun, Acc}).

%% @doc Fold over all the keys for a bucket.
fold_keys(FoldKeysFun, Acc, Opts, State) ->
    Bucket =  proplists:get_value(bucket, Opts),
    FoldFun = fold_keys_fun(FoldKeysFun, Bucket),
    gen_server:call(State, {fold_keys, FoldFun, Bucket, Acc}).

%% @doc Fold over all the objects for a bucket.
fold_objects(FoldObjectsFun, Acc, Opts, State) ->
    Bucket =  proplists:get_value(bucket, Opts),
    FoldFun = fold_objects_fun(FoldObjectsFun, Bucket),
    gen_server:call(State, {fold_objects, FoldFun, Bucket, Acc}).

%% @doc Returns true if this memory backend contains any
%% non-tombstone values; otherwise returns false.
is_empty(State) ->
    gen_server:call(State, is_empty).

%% @doc Delete all objects from this memory backend
drop(State) ->
    gen_server:call(State, drop).

%% @doc Get the status information for this memory backend
status(State) ->
    gen_server:call(State, status).

%% @doc Register an asynchronous callback
callback(_Ref, _Msg, _State) ->
    ok.

%% gen_server API

%% @private
init([Partition]) ->
    TableRef = ets:new(list_to_atom(integer_to_list(Partition)),[]),
    {ok, #state{ref=TableRef}}.

%% @private
handle_call(stop, _From, #state{ref=Ref}=State) ->
    {reply, srv_stop(Ref), State};
handle_call({get, Bucket, Key}, _From, #state{ref=Ref}=State) ->
    {reply, srv_get(Bucket, Key, Ref), State};
handle_call({put, Bucket, Key, Val}, _From, #state{ref=Ref}=State) ->
    {reply, srv_put(Bucket, Key, Val, Ref), State};
handle_call({delete, Bucket, Key}, _From, #state{ref=Ref}=State) ->
    {reply, srv_delete(Bucket, Key, Ref), State};
handle_call({fold_buckets, FoldFun, Acc}, _From, #state{ref=Ref}=State) ->
    {reply, srv_fold_buckets(FoldFun, Acc, Ref), State};
handle_call({fold_keys, FoldFun, Bucket, Acc}, _From, #state{ref=Ref}=State) ->
    {reply, srv_fold_keys(FoldFun, Bucket, Acc, Ref), State};
handle_call({fold_objects, FoldFun, Bucket, Acc}, _From, #state{ref=Ref}=State) ->
    {reply, srv_fold_objects(FoldFun, Bucket, Acc, Ref), State};
handle_call(drop, _From, #state{ref=Ref}=State) ->
    ets:delete_all_objects(Ref),
    {reply, {ok, self()}, State};
handle_call(is_empty, _From, #state{ref=Ref}=State) ->
    {reply, ets:info(Ref, size) =:= 0, State};
handle_call(status, _From, #state{ref=Ref}=State) ->
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
        [] -> {error, notfound};
        [{{Bucket, Key}, Val}] -> {ok, Val};
        Error -> {error, Error}
    end.

%% @private
srv_put(Bucket, Key, Val, Ref) ->
    true = ets:insert(Ref, {{Bucket, Key}, Val}),
    ok.

%% @private
srv_delete(Bucket, Key, Ref) ->
    true = ets:delete(Ref, {Bucket, Key}),
    ok.

%% @private
srv_fold_buckets(FoldFun, Acc, Ref) ->
    BucketList = ets:match(Ref, {{'$1', '_'}, '_'}),
    lists:foldl(FoldFun, Acc, BucketList).

%% @private
srv_fold_keys(FoldFun, Bucket, Acc, Ref) ->
    case Bucket of
        undefined ->
            KeyList = ets:match(Ref, {{'$1', '$2'}, '_'});
        _ ->
            KeyList = ets:match(Ref, {{Bucket, '$1'}, '_'})
    end,
    lists:foldl(FoldFun, Acc, KeyList).

%% @private
srv_fold_objects(FoldFun, Bucket, Acc, Ref) ->
    case Bucket of
        undefined ->
            ObjectList = ets:match_object(Ref, {{'_', '_'}, '_'});
        _ ->
            ObjectList = ets:match_object(Ref, {{Bucket, '_'}, '_'})
    end,
    lists:foldl(FoldFun, Acc, ObjectList).

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

simple_test() ->
    riak_kv_backend:standard_test(?MODULE, []).

-ifdef(EQC).
%% eqc_test() ->
%%     ?assertEqual(true, backend_eqc:test(?MODULE, true)).
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
