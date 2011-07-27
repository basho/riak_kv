%% -------------------------------------------------------------------
%%
%% riak_kv_index_backend: joins bitcask and merge_index to create an
%%                        indexing backend.
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

-module (riak_kv_index_backend).
-behavior(riak_kv_backend).

-define(PRINT(Var), io:format("DEBUG: ~p:~p - ~p~n~n ~p~n~n", [?MODULE, ?LINE, ??Var, Var])).

%% KV Backend API
-export([start/2,
         stop/1,
         get/2,
         put/3,
         delete/2,
         list/1,
         list_bucket/2,
         fold/3,
         fold_keys/3,
         fold_bucket_keys/4,
         drop/1,
         is_empty/1,
         callback/3]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(state, {
          kv_mod,       % The KV backend module.
          kv_state,     % The KV backend state.
          index_mod,    % The Index backend module.
          index_state   % The Index backend state.
         }).

%% @spec start(Partition :: integer(), Config :: integer()) ->
%%                        {ok, state()} | {{error, Reason :: term()}, state()}.
%%
%% @doc Start an instance of riak_kv_bitcask_backend and riak_index_mi_backend.
start(Partition, Config) ->
    %% For now, just use bitcask and merge_index. In the future, make
    %% this configurable.
    KVMod = riak_kv_bitcask_backend,
    IndexMod = riak_index_mi_backend,

    %% Fire up the backends...
    {ok, KVState} = KVMod:start(Partition, Config),
    {ok, IndexState} = IndexMod:start(Partition, Config),

    %% Create the state and return...
    State = #state { 
      kv_mod = KVMod,
      kv_state = KVState,
      index_mod = IndexMod,
      index_state = IndexState
     },
    {ok, State}.

%% @spec stop(state()) -> ok | {error, Reason :: term()}.
%%
%% @doc Stop backends, bubble up any errors.
stop(State) -> 
    #state { kv_mod = KVMod, kv_state = KVState, index_mod = IndexMod, index_state = IndexState } = State,
    KVResult = KVMod:stop(KVState),
    IndexResult = IndexMod:stop(IndexState),
    Results = [KVResult, IndexResult],
    ErrorResults = [X || X <- Results, X /= ok],
    case ErrorResults of
        [] -> ok;
        _ -> {error, ErrorResults}
    end.

%% @spec get(state(), Key :: binary()) -> {ok, Val :: binary()} | {error, Reason :: term()}.
%% 
%% @doc Pass a get request through to the KV backend.
get(State, BKey) ->
    #state { kv_mod = KVMod, kv_state = KVState } = State,
    KVMod:get(KVState, BKey).

%% @spec put(state(), Key :: binary(), Val :: binary()) -> ok | {error, Reason :: term()}.
%%
%% @doc Index the Riak object using fields stored in metadata under
%%       <<"index">>, then store the object in the KV backend.
put(State, BKey, Val) -> 
    #state { kv_mod = KVMod, kv_state = KVState, index_mod = IndexMod, index_state = IndexState } = State,

    %% Since the two backends are separate, we can't have a true
    %% transaction. The best we can do for now is perform the index
    %% (which is the newer and more complicated code) and if that
    %% works, then perform the KV put.
    case do_index_put(IndexMod, IndexState, BKey, Val) of
        ok ->
            do_kv_put(KVMod, KVState, BKey, Val);
        Other ->
            Other
    end.

%% @spec delete(state(), Key :: binary()) -> ok | {error, Reason :: term()}.
%% 
%% @doc Delete the object from the Index backend and the KV backend.
delete(State, BKey) -> 
    #state { kv_mod = KVMod, kv_state = KVState, index_mod = IndexMod, index_state = IndexState } = State,

    %% Since the two backends are separate, we can't have a true
    %% transaction. The best we can do for now is delete from the index
    %% (which is the newer and more complicated code) and if that
    %% works, then delete from KV.
    case do_index_delete(IndexMod, IndexState, BKey) of
        ok ->
            do_kv_delete(KVMod, KVState, BKey);
        Other ->
            Other
    end.

%% @spec list(state()) -> [Key :: binary()]
%%
%% @doc Pass a list request through to the KV backend.
list(State) ->
    #state { kv_mod = KVMod, kv_state = KVState } = State,
    KVMod:list(KVState).

%% @spec list_bucket(state(), list_bucket_def()) -> [Key :: binary()].
%% @type list_bucket_def() -> Bucket :: binary() 
%%                          | Any :: '_'
%%                          | Filter :: {filter, Bucket::binary(), FilterFun :: function()}
%%
%% @doc Pass list_bucket requests through to the KV backend.
list_bucket(State, Bucket) ->
    #state { kv_mod = KVMod, kv_state = KVState } = State,
    KVMod:list_bucket(KVState, Bucket).

%% @spec is_empty(state()) -> boolean().
%%
%% @doc Pass is_empty requests through to the KV backend.
is_empty(State) ->
    #state { kv_mod = KVMod, kv_state = KVState } = State,
    KVMod:is_empty(KVState).

%% @spec drop(state()) -> ok.
%%
%% @doc Drop all data from the Index and KV backends.
drop(State) ->
    #state { kv_mod = KVMod, kv_state = KVState, index_mod = IndexMod, index_state = IndexState } = State,
    IndexMod:drop(IndexState),
    KVMod:drop(KVState).

%% @spec fold(state(), function(), term()) -> [Key :: binary()].
%%
%% @doc Pass fold requests through to the KV backend.
fold(State, Fun, Extra) ->    
    #state { kv_mod = KVMod, kv_state = KVState } = State,
    KVMod:fold(KVState, Fun, Extra).

%% @spec fold_keys(state(), function(), term()) -> [Key :: binary()].
%% 
%% @doc Pass fold_keys requests through to the KV backend.
fold_keys(State, Fun, Extra) ->
    #state { kv_mod = KVMod, kv_state = KVState } = State,
    KVMod:fold_keys(KVState, Fun, Extra).


%% HACK - This is how we're funneling Riak Index queries until we add
%% "covering cast" functionality to riak_core. For now, we're going to
%% focus on getting the request to the Index backend with the correct
%% arguments roughly in place and running basic queries.
fold_bucket_keys(State, {index_query, Bucket, Query}, _Fun, _Extra) ->
    #state { index_mod = IndexMod, index_state = IndexState } = State,

    %% Update riak_kv_stat...
    riak_kv_stat:update(vnode_index_read),

    %% This is needs discussion. Our design notes call for results to
    %% feed into SKFun using {sk, SecondaryKey, PrimaryKey}, but this
    %% doesn't account for Properties. We also need to add a clause
    %% for when we reach the limit.
    SKFun = fun({results, Results}, Acc) ->
                    {ok, Results ++ Acc};
               ({error, Reason}, _Acc) ->
                    {error, Reason};
               (done, Acc) ->
                    {ok, Acc}
            end,

    %% Also, for this round of changes, FinalFun is going to just
    %% return the output of SKFun. Normally, this would call
    %% riak_core_vnode:reply/N. Here, we just transform the list of
    %% results from [{Key, Props}] to [Key].
    FinalFun = fun({error, Reason}, _Acc) ->
                       {error, Reason};
                  (done, Acc) ->
                       [K || {K, _} <- Acc]
               end,

    %% Pass the fold_index request through to the Index backend.
    IndexMod:fold_index(IndexState, Bucket, Query, SKFun, [], FinalFun);

%% @spec fold_bucket_keys(state(), binary(), function(), term()) -> [Key :: binary()].
%%
%% @doc Pass fold_bucket_keys requests through to the KV backend.
fold_bucket_keys(State, Bucket, Fun, Extra) ->
    #state { kv_mod = KVMod, kv_state = KVState } = State,

    %% Pass fold_bucket_keys request to the KV backend using the
    %% latest known interface, falling back as appropriate.
    try
        WrapFun = fun(K, Acc) -> Fun({Bucket, K}, unused_val, Acc) end,
        KVMod:fold_bucket_keys(KVState, Bucket, WrapFun)
    catch error:undef ->
            try
                KVMod:fold_bucket_keys(KVState, Bucket, Fun, Extra)
            catch error:undef ->
                    %% Backend doesn't support newer API, use older API
                    KVMod:fold(KVState, Fun, Extra)
            end
    end.

%% @spec callback(state(), ref(), term()) -> ok.
%%
%% @doc Pass any callbacks through to the KV backend. 
callback(State, Ref, Msg) ->
    %% Callbacks are handled using the same method as multi-backend:
    %% call the callback function on each backend, don't check for any
    %% response, no error handling, return 'ok'.
    #state { kv_mod = KVMod, kv_state = KVState, index_mod = IndexMod, index_state = IndexState } = State,
    KVMod:callback(KVState, Ref, Msg),
    IndexMod:callback(IndexState, Ref, Msg),
    ok.


%%% PRIVATE FUNCTIONS %%%


%% @private 
%% @spec do_index_put(IndexMod :: module(), IndexState :: term(), BKey :: riak_object:bkey(), Val :: binary()) -> ok.
%%       
%% @doc Index the BKey/Value in the Index backend.
do_index_put(IndexMod, IndexState, BKey, Val) ->
    %% Get the old proxy object. If it exists, then delete all of its
    %% postings.
    {Bucket, Key} = BKey,
    case IndexMod:lookup_sync(IndexState, Bucket, "_proxy", Key) of
        [{Key, OldPostings}] ->
            TS1 = riak_index:timestamp(),
            OldPostings1 = [{Bucket, Field, Token, Key, TS1} || {Field, Token, _} <- OldPostings],
            %% io:format("DEBUG: Deleting old postings for ~p:~n~p~n", [BKey, OldPostings1]),
            ok = IndexMod:delete(IndexState, OldPostings1),
            riak_kv_stat:update({vnode_index_delete, length(OldPostings1)});
        _ ->
            skip
    end,

    %% We need the unserialized Riak Object in order to index. This is
    %% extra overhead.
    Obj = binary_to_term(Val),

    %% Pull the IndexFields from the object. At this point, we've
    %% already validated that the object parses correctly, so if we
    %% get anything back except for {ok, IndexFields} then fail
    %% loudly, as it's an unanticipated code path.
    IndexFields = case riak_index:parse_object(Obj) of
                      {ok, IFs} -> 
                          %% No properties for now. This is here for future planning.
                          EmptyProps = [],
                          [{Field, Value, EmptyProps} || {Field, Value} <- IFs];
                      {error, Reasons} -> 
                          throw({error_parsing_index_fields, Reasons})
                  end,

    %% Store the new proxy object. This is a single entry written to
    %% merge_index under {Index, Field, Term} of {Bucket, "_proxy",
    %% Key}. The DocID is also the Key, and the properties is a list
    %% of postings.
    TS2 = riak_index:timestamp(),
    %% io:format("DEBUG: Indexing proxy object for ~p~n", [BKey]),
    ok = IndexMod:index(IndexState, [{Bucket, "_proxy", Key, Key, IndexFields, TS2}]),

    %% Store the new postings...
    Postings = [{Bucket, Field, Token, Key, Props, TS2} || {Field, Token, Props} <- IndexFields],
    try 
        %% io:format("DEBUG: Indexing new postings for ~p:~n~p~n", [BKey, Postings]),
        IndexMod:index(IndexState, Postings),
        riak_kv_stat:update({vnode_index_write, length(Postings)}),
        ok
    catch 
        _Type : Reason ->
            {error, Reason}
    end.

%% @private 
%% @spec do_kv_put(KVMod :: module(), KVState :: term(), BKey :: riak_object:bkey(), Val :: binary()) -> ok.
%% 
%% @doc Store the BKey/Value in the KV backend.
do_kv_put(KVMod, KVState, BKey, Val) ->
    KVMod:put(KVState, BKey, Val).

%% @private 
%% @spec do_index_delete(IndexMod :: module(), IndexState :: term(), BKey :: riak_object:bkey()) -> ok | {error, Reason}
%%
%% @doc Delete the BKey from the Index backend.
do_index_delete(IndexMod, IndexState, BKey) ->
    %% Look up the old proxy object. If it exists, then delete all of
    %% its postings.
    {Bucket, Key} = BKey,
    case IndexMod:lookup_sync(IndexState, Bucket, "_proxy", Key) of
        [{Key, OldPostings}] ->
            TS1 = riak_index:timestamp(),
            OldPostings1 = [{Bucket, Field, Token, Key, TS1} || {Field, Token, _} <- OldPostings],
            try
                %% io:format("DEBUG: Deleting postings for ~p:~n~p~n", [BKey, OldPostings1]),
                ok = IndexMod:delete(IndexState, OldPostings1),
                riak_kv_stat:update({vnode_index_delete, length(OldPostings1)}),
                ok
            catch _Type : Reason ->
                    {error, Reason}
            end;
        _ ->
            ok
    end.

%% @private 
%% @spec do_kv_delete(KVMod :: module(), KVState :: term(), BKey :: riak_object:bkey()) -> ok.
%% 
%% @doc Delete the BKey from the KV backend.
do_kv_delete(KVMod, KVState, BKey) ->
    KVMod:delete(KVState, BKey).
