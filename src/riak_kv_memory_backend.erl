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
fold_buckets(none, _Acc, #state{ref=_Ref}) ->
    ok;
fold_buckets(_FoldBucketsFun, _Acc, #state{ref=_Ref}) ->
    ok.

%% @doc Fold over all the keys for a bucket. If the
%% fold function is `none' just list all of the keys.
fold_keys(none, _Acc, _Opts, _State) ->
    %% Return a list of all keys in all buckets: [{bucket(), key()}]
    ok;
fold_keys(_FoldKeysFun, _Acc, _Opts, #state{ref=_Ref}) ->
    ok.

%% @doc Fold over all the objects for a bucket. If the
%% fold function is `none' just list all of the objects.
fold_objects(none, _Acc, _Opts, #state{ref=_Ref}) ->
    ok;
fold_objects(_FoldObjectsFun, _Acc, _Opts, #state{ref=_Ref}) ->
    ok.


% list(state()) -> [riak_object:bkey()]
%% list(SrvRef) -> gen_server:call(SrvRef,list).

%% list([],Acc) -> Acc;
%% list([[K]|Rest],Acc) -> list(Rest,[K|Acc]).

%% % list_bucket(term(), Bucket :: riak_object:bucket()) -> [Key :: binary()]
%% list_bucket(SrvRef, Bucket) ->
%%     gen_server:call(SrvRef,{list_bucket, Bucket}).

%% @doc Returns true if this memory backend contains any 
%% non-tombstone values; otherwise returns false.
is_empty(State) ->
    gen_server:call(State, is_empty).

%% @doc Delete all objects from this memory backend
drop(State) ->
    gen_server:call(State, drop).
    
%% fold(SrvRef, Fun, Acc0) -> gen_server:call(SrvRef, {fold, Fun, Acc0}, infinity).

%% fold_bucket_keys(SrvRef, Bucket, Fun, Acc0) ->
%%     gen_server:call(SrvRef, {fold_bucket_keys, Bucket, Fun, Acc0}, infinity).

%% @doc Register an asynchronous callback
callback(_Ref, _Msg, _State) ->
    ok.

%% @doc Get the status information for this memory backend
status(_State) ->
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
%% handle_call(list,_From,State) ->
%%     {reply, srv_list(State), State};
%% handle_call({list_bucket, Bucket}, _From, #state{ref=Ref}=State) ->
%%     {reply, srv_list_bucket(Bucket, Ref), State};
handle_call(is_empty, _From, #state{ref=Ref}=State) ->
    {reply, ets:info(Ref, size) =:= 0, State};
handle_call(drop, _From, #state{ref=Ref}=State) -> 
    ets:delete(Ref),
    {reply, ok, State}.
%% handle_call({fold, Fun0, Acc}, _From, #state{ref=Ref}=State) ->
%%     Fun = fun({{B,K}, V}, AccIn) -> Fun0({B,K}, V, AccIn) end,
%%     Reply = ets:foldl(Fun, Acc, Ref),
%%     {reply, Reply, State};
%% handle_call({fold_bucket_keys, _Bucket, Fun0, Acc}, From, State) ->
%%     %% We could do something with the Bucket arg, but for this backend
%%     %% there isn't much point, so we'll do the same thing as the older
%%     %% API fold.
%%     handle_call({fold, Fun0, Acc}, From, State).

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

srv_stop(Ref) ->
    catch ets:delete(Ref),
    ok.

srv_get(Bucket, Key, Ref) ->
    case ets:lookup(Ref, {Bucket, Key}) of
        [] -> {error, notfound};
        [{{Bucket, Key}, Val}] -> {ok, Val};
        Error -> {error, Error}
    end.

srv_put(Bucket, Key, Val, Ref) ->
    true = ets:insert(Ref, {{Bucket, Key}, Val}),
    ok.

srv_delete(Bucket, Key, Ref) ->
    true = ets:delete(Ref, {Bucket, Key}),
    ok.

%% srv_list(Ref) ->
%%     MList = ets:match(Ref, {'$1','_'}),
%%     list(MList,[]).

%% srv_list_bucket({filter, Bucket, Fun}, Ref) ->
%%     MList = lists:filter(Fun, ets:match(Ref, {{Bucket,'$1'},'_'})),
%%     list(MList,[]);
%% srv_list_bucket(State, Bucket) ->
%%     case Bucket of
%%         '_' -> MatchSpec = {{'$1','_'},'_'};
%%         _ -> MatchSpec = {{Bucket,'$1'},'_'}
%%     end,
%%     MList = ets:match(Ref, MatchSpec),
%%     list(MList,[]).


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
