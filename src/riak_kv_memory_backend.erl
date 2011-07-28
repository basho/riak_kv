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
         fold_buckets/3,
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
-define(CAPABILITIES, [api_version,
                       start,
                       stop,
                       get,
                       put,
                       delete,
                       drop,
                       fold_buckets,
                       fold_keys,
                       fold_objects,
                       is_empty,
                       status,
                       callback]).




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
% @spec start(Partition :: integer(), Config :: proplist()) ->
%                        {ok, state()} | {{error, Reason :: term()}, state()}
start(Partition, _Config) ->
    gen_server:start_link(?MODULE, [Partition], []).

% @spec stop(state()) -> ok | {error, Reason :: term()}
stop(State) ->
    gen_server:call(State, stop).

% get(riak_object:bucket(), riak_object:key(), state()) ->
%   {ok, Val :: binary()} | {error, Reason :: term()}
% key must be 160b
get(Bucket, Key, State) ->
    gen_server:call(State, {get, Bucket, Key}).

% put(riak_object:bucket(), riak_object:key(), binary(), state()) ->
%   ok | {error, Reason :: term()}
% key must be 160b
put(Bucket, Key, Val, State) ->
    gen_server:call(State, {put, Bucket, Key, Val}).

% delete(riak_object:bucket(), riak_object:key(), state()) ->
%   ok | {error, Reason :: term()}
% key must be 160b
delete(Bucket, Key, State) -> 
    gen_server:call(State, {delete, Bucket, Key}).

%% @doc Fold over all the buckets. If the fold
%% function is `none' just list all of the buckets.
fold_buckets(none, Acc, State) ->
    gen_server:call(State, {list_buckets, Acc});
fold_buckets(FoldBucketsFun, Acc, State) ->
    gen_server:call(State, {fold_buckets, FoldBucketsFun, Acc}).

%% @doc Fold over all the keys for a bucket. If the
%% fold function is `none' just list all of the keys.
fold_keys(none, Acc, Opts, State) ->
    case proplists:get_value(bucket, Opts) of
        undefined ->
            %% Return a list of all keys in
            %% all buckets: [{bucket(), key()}]
            gen_server:call(State, {list_keys, Acc});
        Bucket ->
            %% Return a list of the keys in
            %% the specified bucket: [{bucket(), key()}]
            gen_server:call(State, {list_keys, Bucket, Acc})
    end;
fold_keys(FoldKeysFun, Acc, Opts, State) ->
    case proplists:get_value(bucket, Opts) of
        undefined ->
            %% Fold across all keys in all buckets
            gen_server:call(State, {fold_keys, FoldKeysFun, Acc});
        Bucket ->
            %% Fold across the keys in the specified bucket
            gen_server:call(State, {fold_keys, FoldKeysFun, Bucket, Acc})
    end.

%% @doc Fold over all the objects for a bucket. If the
%% fold function is `none' just list all of the objects.
fold_objects(none, Acc, Opts, State) ->
    case proplists:get_value(bucket, Opts) of
        undefined ->
            %% Return a list of all objects in
            %% all buckets.
            gen_server:call(State, {list_objects, Acc});
        Bucket ->
            %% Return a list of the objects in
            %% the specified bucket.
            gen_server:call(State, {list_objects, Bucket, Acc})
    end;
fold_objects(FoldObjectsFun, Acc, Opts, State) ->
    case proplists:get_value(bucket, Opts) of
        undefined ->
            %% Fold across all objects in all buckets
            gen_server:call(State, {fold_objects, FoldObjectsFun, Acc});
        Bucket ->
            %% Fold across the objects in the specified bucket
            gen_server:call(State, {fold_objects, FoldObjectsFun, Bucket, Acc})
    end.

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
handle_call({list_buckets, Acc}, _From, #state{ref=Ref}=State) ->
    {reply, srv_list_buckets(Acc, Ref), State};
handle_call({fold_buckets, FoldBucketsFun, Acc}, _From, #state{ref=Ref}=State) ->
    {reply, srv_fold_buckets(FoldBucketsFun, Acc, Ref), State};
handle_call({list_keys, Acc}, _From, #state{ref=Ref}=State) ->
    {reply, srv_list_keys(Acc, Ref), State};
handle_call({list_keys, Bucket, Acc}, _From, #state{ref=Ref}=State) ->
    {reply, srv_list_keys(Bucket, Acc, Ref), State};
handle_call({fold_keys, FoldKeysFun, Acc}, _From, #state{ref=Ref}=State) ->
    {reply, srv_fold_keys(FoldKeysFun, Acc, Ref), State};
handle_call({fold_keys, FoldKeysFun, Bucket, Acc}, _From, #state{ref=Ref}=State) ->
    {reply, srv_fold_keys(FoldKeysFun, Bucket, Acc, Ref), State};
handle_call({list_objects, Acc}, _From, #state{ref=Ref}=State) ->
    {reply, srv_list_objects(Acc, Ref), State};
handle_call({list_objects, Bucket, Acc}, _From, #state{ref=Ref}=State) ->
    {reply, srv_list_objects(Bucket, Acc, Ref), State};
handle_call({fold_objects, FoldObjectsFun, Acc}, _From, #state{ref=Ref}=State) ->
    {reply, srv_fold_objects(FoldObjectsFun, Acc, Ref), State};
handle_call({fold_objects, FoldObjectsFun, Bucket, Acc}, _From, #state{ref=Ref}=State) ->
    {reply, srv_fold_objects(FoldObjectsFun, Bucket, Acc, Ref), State};
handle_call(drop, _From, #state{ref=Ref}=State) -> 
    ets:delete(Ref),
    {reply, ok, State};
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
srv_list_buckets(_Acc, Ref) ->
    BucketList = ets:match(Ref, {{'$1', '_'}, '_'}),
    lists:usort(lists:flatten(BucketList)).

%% @private
srv_fold_buckets(FoldBucketsFun, Acc, Ref) ->
    FoldFun = fun([Bucket], Acc1) ->
                      case lists:member(Bucket, Acc1) of
                          true ->
                              Acc1;
                          false ->
                              FoldBucketsFun(Bucket, Acc1)
                      end
              end,
    BucketList = ets:match(Ref, {{'$1', '_'}, '_'}),
    lists:foldl(FoldFun, Acc, BucketList).

%% @private
srv_list_keys(_Acc, Ref) ->
    KeyList = ets:match(Ref, {{'$1', '$2'}, '_'}),
    lists:flatten(KeyList).

%% @private
srv_list_keys(Bucket, _Acc, Ref) ->
    KeyList = ets:match(Ref, {{Bucket, '$1'}, '_'}),
    lists:flatten(KeyList).

%% @private
srv_fold_keys(FoldKeysFun, Acc, Ref) ->
    FoldFun = fun([Bucket, Key], Acc1) ->
                      FoldKeysFun(Bucket, Key, Acc1)
              end,
    KeyList = ets:match(Ref, {{'$1', '_'}, '_'}),
    lists:foldl(FoldFun, Acc, KeyList).

%% @private
srv_fold_keys(FoldKeysFun, Bucket, Acc, Ref) ->
    FoldFun = fun([B, K], Acc1) ->
                      FoldKeysFun(B, K, Acc1)
              end,
    KeyList = ets:match(Ref, {{Bucket, '_'}, '_'}),
    lists:foldl(FoldFun, Acc, KeyList).

%% @private
srv_list_objects(_Acc, Ref) ->
    ets:match_object(Ref, {{'_', '_'}, '_'}).

%% @private
srv_list_objects(Bucket, _Acc, Ref) ->
    ets:match_object(Ref, {{Bucket, '_'}, '_'}).

%% @private
srv_fold_objects(FoldObjectsFun, Acc, Ref) ->
    FoldFun = fun({{Bucket, Key}, Value}, Acc1) ->
                      FoldObjectsFun(Bucket, Key, Value, Acc1)
              end,
    ets:foldl(FoldFun, Acc, Ref).

%% @private
srv_fold_objects(FoldObjectsFun, Bucket, Acc, Ref) ->
    ObjectList = ets:match_object(Ref, {{Bucket, '_'}, '_'}),
    FoldFun = fun({{B, Key}, Value}, Acc1) ->
                      FoldObjectsFun(B, Key, Value, Acc1)
              end,
    lists:foldl(FoldFun, Acc, ObjectList).

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

simple_test() ->
    riak_kv_backend:standard_test(?MODULE, []).

-ifdef(EQC).
eqc_test() ->
    ?assertEqual(true, backend_eqc:test(?MODULE, true)).

-endif. % EQC
-endif. % TEST
