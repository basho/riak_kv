%% -------------------------------------------------------------------
%%
%% riak_kv_index_backend: joins bitcask and merge_index to create an
%%                        indexing backend.
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

-module (riak_kv_index_backend).
-behavior(riak_kv_backend).

-define(PRINT(Var), io:format("DEBUG: ~p:~p - ~p~n~n ~p~n~n", [?MODULE, ?LINE, ??Var, Var])).

%% KV Backend API
-export([api_version/0,
         start/2,
         stop/1,
         get/3,
         put/4,
         put/5,
         delete/3,
         drop/1,
         fold_buckets/4,
         fold_keys/4,
         fold_objects/4,
         is_empty/1,
         status/1,
         callback/3]).

%% Secondary index query support
-export([fold_index/3]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(API_VERSION, 1).
-define(CAPABILITIES, [indexes]).

-record(state, {
          kv_mod,       % The KV backend module.
          kv_state,     % The KV backend state.
          index_mod,    % The Index backend module.
          index_state   % The Index backend state.
         }).

-opaque(state() :: #state{}).
-type config() :: [{atom(), term()}].
-type index_spec() :: {add, Index, SecondaryKey} | {remove, Index, SecondaryKey}.

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Return the major version of the
%% current API and a capabilities list.
-spec api_version() -> {integer(), [atom()]}.
api_version() ->
    {?API_VERSION, ?CAPABILITIES}.

%% @doc Start an instance of riak_kv_bitcask_backend and riak_index_mi_backend.
-spec start(integer(), config()) -> {ok, state()} | {error, term()}.
start(Partition, Config) ->
    %% For now, just use bitcask and merge_index. In the future, make
    %% this configurable.
    KVMod = riak_kv_bitcask_backend,
    IndexMod = riak_index_mi_backend,

    %% Check for module-specific configs
    case proplists:get_value(bitcask, Config) of
        undefined ->
            KVConfig = [];
        KVConfig ->
            ok
    end,
    case proplists:get_value(merge_index, Config) of
        undefined ->
            IndexConfig = [];
        IndexConfig ->
            ok
    end,
    %% Fire up the backends...
    case KVMod:start(Partition, KVConfig) of
        {ok, KVState} ->
            case IndexMod:start(Partition, IndexConfig) of
                {ok, IndexState} ->
                    %% Create the state and return...
                    State = #state{
                      kv_mod = KVMod,
                      kv_state = KVState,
                      index_mod = IndexMod,
                      index_state = IndexState
                     },
                    {ok, State};
                {error, Reason1} ->
                    {error, Reason1}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Stop the backends
-spec stop(state()) -> ok.
stop(State) ->
    #state{kv_mod = KVMod,
           kv_state = KVState,
           index_mod = IndexMod,
           index_state = IndexState}=State,
    KVMod:stop(KVState),
    IndexMod:stop(IndexState),
    ok.

%% @doc Pass a get request through to the KV backend.
-spec get(riak_object:bucket(), riak_object:key(), state()) ->
                 {ok, any(), state()} |
                 {ok, not_found, state()} |
                 {error, term(), state()}.
get(Bucket, Key, #state{kv_mod = KVMod,
                        kv_state = KVState}) ->
    KVMod:get(Bucket, Key, KVState).

%% @doc Just store the object in the KV backend.
-spec put(riak_object:bucket(), riak_object:key(), binary(), state()) ->
                 {ok, state()} |
                 {error, term(), state()}.
put(Bucket, Key, Val, #state{kv_mod = KVMod,
                             kv_state = KVState}=State) ->
    case do_kv_put(Bucket, Key, Val, KVMod, KVState) of
        {ok, UpdKVState} ->
            {ok, State#state{kv_state=UpdKVState}};
        {error, Reason, UpdKVState} ->
            {error, Reason, State#state{kv_state=UpdKVState}}
    end.

%% @doc Insert an object with secondary index 
%% information into the bitcask backend
-spec put(riak_object:bucket(), riak_object:key(), [index_spec()], binary(), state()) ->
                 {ok, state()} |
                 {error, term(), state()}.
put(Bucket, PrimaryKey, IndexSpecs, Val, State=
        #state{kv_mod = KVMod,
               kv_state = KVState,
               index_mod = IndexMod,
               index_state = IndexState}) ->
    %% Since the two backends are separate, we can't have a true
    %% transaction. The best we can do for now is perform the index
    %% (which is the newer and more complicated code) and if that
    %% works, then perform the KV put.
    case do_index_put(Bucket, PrimaryKey, IndexSpecs, Val, IndexMod, IndexState) of
        ok ->
            case do_kv_put(Bucket, PrimaryKey, Val, KVMod, KVState) of
                {ok, UpdKVState} ->
                    {ok, State#state{kv_state=UpdKVState}};
                {error, Reason, UpdKVState} ->
                    {error, Reason, State#state{kv_state=UpdKVState}}
            end;
        {error, Reason1} ->
            {error, Reason1, State}
    end.


%% @doc Delete the object from the Index backend and the KV backend.
-spec delete(riak_object:bucket(), riak_object:key(), state()) ->
                    {ok, state()} |
                    {error, term(), state()}.
delete(Bucket, Key, #state{kv_mod = KVMod,
                           kv_state = KVState,
                           index_mod = IndexMod,
                           index_state = IndexState}=State) ->
    %% Since the two backends are separate, we can't have a true
    %% transaction. The best we can do for now is delete from the index
    %% (which is the newer and more complicated code) and if that
    %% works, then delete from KV.
    case do_index_delete(Bucket, Key, IndexMod, IndexState) of
        ok ->
            case do_kv_delete(Bucket, Key, KVMod, KVState) of
                {ok, UpdKVState} ->
                    {ok, State#state{kv_state=UpdKVState}};
                {error, Reason, UpdKVState} ->
                    {error, Reason, State#state{kv_state=UpdKVState}}
            end;
        {error, Reason1} ->
            {error, Reason1, State}
    end.

%% @doc Fold over all the buckets.
-spec fold_buckets(riak_kv_backend:fold_buckets_fun(),
                   any(),
                   [],
                   state())-> {ok, any()} | {error, term()}.
fold_buckets(FoldBucketsFun, Acc, Opts, #state{kv_mod = KVMod,
                                               kv_state = KVState}) ->
    KVMod:fold_buckets(FoldBucketsFun, Acc, Opts, KVState).

%% @doc Fold over all the keys for one or all buckets.
-spec fold_keys(riak_kv_backend:fold_keys_fun(),
                any(),
                [{atom(), term()}],
                state()) -> {ok, term()} | {error, term()}.
fold_keys(FoldKeysFun, Acc, Opts, #state{kv_mod = KVMod,
                                         kv_state = KVState}) ->
    KVMod:fold_keys(FoldKeysFun, Acc, Opts, KVState).

%% @doc Fold over all the objects for one or all buckets.
-spec fold_objects(riak_kv_backend:fold_objects_fun(),
                   any(),
                   [{atom(), term()}],
                   state()) -> {ok, any()} | {error, term()}.
fold_objects(FoldObjectsFun, Acc, Opts, #state{kv_mod = KVMod,
                                               kv_state = KVState}) ->
    KVMod:fold_objects(FoldObjectsFun, Acc, Opts, KVState).

%% @doc Drop all data from the Index and KV backends.
-spec drop(state()) -> {ok, state()} | {error, term(), state()}.
drop(#state{kv_mod = KVMod,
            kv_state = KVState,
            index_mod = IndexMod,
            index_state = IndexState}=State) ->
    case KVMod:drop(KVState) of
        {ok, UpdKVState} ->
            KVError = none;
        {error, KVError, UpdKVState} ->
            ok
    end,
    case IndexMod:drop(IndexState) of
        ok ->
            UpdIndexState = IndexState,
            IndexError = none;
        {ok, UpdIndexState} ->
            IndexError = none;
        {error, IndexError, UpdIndexState} ->
            ok
    end,
    Errors = [KVError, IndexError],
    case Errors of
        [none, none] ->
            {ok, State#state{kv_state=UpdKVState,
                             index_state=UpdIndexState}};
        _ ->
            {error, Errors, State#state{kv_state=UpdKVState,
                                        index_state=UpdIndexState}}
    end.

%% @doc Returns true if the kv backend contains any
%% non-tombstone values; otherwise returns false.
-spec is_empty(state()) -> boolean().
is_empty(#state{kv_mod = KVMod,
                kv_state = KVState}) ->
    KVMod:is_empty(KVState).

%% @doc Get the status information for the backends
-spec status(state()) -> [{atom(), term()}].
status(#state{kv_mod = KVMod,
              kv_state = KVState,
              index_mod = IndexMod,
              index_state = IndexState}) ->
    KVStatus = {KVMod, KVMod:status(KVState)},
    IndexStatus = {IndexMod, IndexMod:status(IndexState)},
    [{KVMod, KVStatus},  {IndexMod, IndexStatus}].

%% @doc Pass any callbacks through to the KV backend.
-spec callback(reference(), any(), state()) -> {ok, state()}.
callback(Ref, Msg, #state{kv_mod = KVMod,
                          kv_state = KVState,
                          index_mod = IndexMod,
                          index_state = IndexState}=State) ->
    %% Callbacks are handled using the same method as multi-backend:
    %% call the callback function on each backend, don't check for any
    %% response, no error handling, return 'ok'.

    {ok, UpdKVState} = KVMod:callback(Ref, Msg, KVState),
    ok = IndexMod:callback(Ref, Msg, IndexState),
    {ok, State#state{kv_state=UpdKVState}}.

%% @doc Pass the fold_index request through to the Index backend.
%% @TODO This function is temporary and will be removed in the next
%% phase of backend refactoring.
fold_index(#state{index_mod = IndexMod,
                  index_state = IndexState},
           Bucket, Query) ->
    %% Update riak_kv_stat...
    riak_kv_stat:update(vnode_index_read),

    %% This is needs discussion. Our design notes call for results to
    %% feed into SKFun using {sk, SecondaryKey, PrimaryKey}, but this
    %% doesn't account for Properties. We also need to add a clause
    %% for when we reach the limit.
    SKFun = fun({results, Results1}, Acc1) ->
                    {ok, Results1 ++ Acc1};
               ({error, Reason}, _Acc1) ->
                    {error, Reason};
               (done, Acc1) ->
                    {ok, Acc1}
            end,

    %% Also, for this round of changes, FinalFun is going to just
    %% return the output of SKFun. Normally, this would call
    %% riak_core_vnode:reply/N. Here, we just transform the list of
    %% results from [{Key, Props}] to [Key].
    FinalFun = fun({error, Reason}, _Acc1) ->
                       {error, Reason};
                  (done, Acc1) ->
                       [K || {K, _} <- Acc1]
               end,

    %% Call fold_index on the Index backend
    IndexMod:fold_index(IndexState, Bucket, Query, SKFun, [], FinalFun).

%% ===================================================================
%% Internal functions
%% ===================================================================

%% @private
%% @doc Index the BKey/Value in the Index backend.
-spec do_index_put(riak_object:bucket(),
                   riak_object:key(),
                   [index_spec()],
                   binary(),
                   module(),
                   term()) -> ok | {error, term()}.
do_index_put(Bucket, PrimaryKey, IndexSpecs, _Val, IndexMod, IndexState) ->
    AssemblePostings = 
        fun({Op, Index, SecondaryKey}) ->
                TS = riak_index:timestamp(),
                case Op of
                    add ->
                        {Bucket, Index, SecondaryKey, PrimaryKey, [], TS};
                    remove ->
                        {Bucket, Index, SecondaryKey, PrimaryKey, undefined, TS}
                end
        end,
    Postings = [AssemblePostings(IndexSpec) || IndexSpec <- IndexSpecs],
    try
        IndexMod:index(IndexState, Postings),
        riak_kv_stat:update({vnode_index_write, length(Postings)}),
        ok
    catch
        _Type : Reason ->
            {error, Reason}
    end.
                       
%% @private
%% @doc Store the BKey/Value in the KV backend.
-spec do_kv_put(riak_object:bucket(),
                riak_object:key(),
                binary(),
                module(),
                term()) -> {ok, term()} | {error, term(), term()}.
do_kv_put(Bucket, Key, Val, KVMod, KVState) ->
    KVMod:put(Bucket, Key, Val, KVState).

%% @private
%% @doc Delete the BKey from the Index backend.
-spec do_index_delete(riak_object:bucket(),
                      riak_object:key(),
                      module(),
                      term()) -> ok | {error, term()}.
do_index_delete(Bucket, Key, IndexMod, IndexState) ->
    %% Look up the old proxy object. If it exists, then delete all of
    %% its postings.
    case IndexMod:lookup_sync(IndexState, Bucket, "_proxy", Key) of
        [{Key, OldPostings}] ->
            TS1 = riak_index:timestamp(),
            OldPostings1 = [{Bucket, Field, Token, Key, TS1} || {Field, Token, _} <- OldPostings],
            try
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
%% @doc Delete the {Bucket, Key} from the KV backend.
-spec do_kv_delete(riak_object:bucket(),
                   riak_object:key(),
                   module(),
                   term()) -> {ok, term()} | {error, term(), term()}.
do_kv_delete(Bucket, Key, KVMod, KVState) ->
    KVMod:delete(Bucket, Key, KVState).

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

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
                                           [{bitcask, [{data_root, "test/bitcask-backend"}]},
                                            {merge_index,
                                             [{data_root_2i, "test/merge_index-backend"}]}]))]}
         ]}]}]}.

setup() ->
    application:load(sasl),
    application:set_env(sasl, sasl_error_logger, {file, "riak_kv_index_backend_eqc_sasl.log"}),
    error_logger:tty(false),
    error_logger:logfile({open, "riak_kv_index_backend_eqc.log"}),

    application:load(bitcask),
    application:set_env(bitcask, merge_window, never),
    application:load(merge_index),
    application:set_env(merge_index, buffer_delayed_write_size, 1024),
    application:set_env(merge_index, buffer_delayed_write_ms, 100),
    ok.

cleanup(_) ->
    os:cmd("rm -rf test/bitcask-backend"),
    os:cmd("rm -rf test/merge_index-backend").

-endif. % EQC

-endif.
