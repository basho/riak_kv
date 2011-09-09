%% -------------------------------------------------------------------
%%
%% riak_multi_backend: switching between multiple storage engines
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

-module (riak_kv_multi_backend).
-behavior(riak_kv_backend).

%% KV Backend API
-export([api_version/0,
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

-record (state, {async_backends :: [{atom(), atom(), term()}],
                 sync_backends :: [{atom(), atom(), term()}],
                 default_backend :: atom()}).

-type state() :: #state{}.
-type config() :: [{atom(), term()}].
-type fold_result() :: {{sync, [term()]}, {async, [fun()]}} | {error, term()}.

%% @doc riak_kv_multi_backend allows you to run multiple backends within a
%% single Riak instance. The 'backend' property of a bucket specifies
%% the backend in which the object should be stored. If no 'backend'
%% is specified, then the 'multi_backend_default' setting is used.
%% If this is unset, then the first defined backend is used.
%%
%% === Configuration ===
%%
%%     {storage_backend, riak_kv_multi_backend},
%%     {multi_backend_default, first_backend},
%%     {multi_backend, [
%%       % format: {name, module, [Configs]}
%%       {first_backend, riak_xxx_backend, [
%%         {config1, ConfigValue1},
%%         {config2, ConfigValue2}
%%       ]},
%%       {second_backend, riak_yyy_backend, [
%%         {config1, ConfigValue1},
%%         {config2, ConfigValue2}
%%       ]}
%%     ]}
%%
%%
%% Then, tell a bucket which one to use...
%%
%%     riak_core_bucket:set_bucket(&lt;&lt;"MY_BUCKET"&gt;&gt;, [{backend, second_backend}])
%%
%%

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Return the major version of the
%% current API and a capabilities list.
-spec api_version() -> {integer(), [atom()]}.
api_version() ->
    {?API_VERSION, ?CAPABILITIES}.

%% @doc Start the backends
-spec start(integer(), config()) -> {ok, state()} | {error, term()}.
start(Partition, Config) ->
    %% Sanity checking
    Defs =  config_value(multi_backend, Config),
    if Defs =:= undefined ->
            {error, multi_backend_config_unset};
       not is_list(Defs) ->
            {error, {invalid_config_setting,
                     multi_backend,
                     list_expected}};
       length(Defs) =< 0 ->
            {error, {invalid_config_setting,
                     multi_backend,
                     list_is_empty}};
       true ->
            {First, _, _} = hd(Defs),

            %% Check if async folds have been disabled
            AsyncFolds = config_value(async_folds, Config, true),
            %% Get the default
            DefaultBackend = config_value(multi_backend_default, Config, First),
            case lists:keymember(DefaultBackend, 1, Defs) of
                true ->
                    %% Start the backends
                    BackendFun = start_backend_fun(Partition, AsyncFolds),
                    {AsyncBackends, SyncBackends, Errors} =
                        lists:foldl(BackendFun, {[], [], []}, Defs),
                    case Errors of
                        [] ->
                            {ok, #state{async_backends=AsyncBackends,
                                        sync_backends=SyncBackends,
                                        default_backend=DefaultBackend}};
                        _ ->
                            {error, Errors}
                    end;
                false ->
                    {error, {invalid_config_setting,
                             multi_backend_default,
                             backend_not_found}}
            end
    end.

%% @private
start_backend_fun(Partition, AsyncFolds) ->
    fun({Name, Module, ModConfig}, {AsyncBackends, SyncBackends, Errors}) ->
            {_, Capabilities} = Module:api_version(),
            case AsyncFolds andalso
                lists:member(async_fold, Capabilities) of
                true ->
                    ModConfig1 = [{async_folds, true}
                                  | ModConfig],
                        case start_backend(Name,
                                           Module,
                                           Partition,
                                           ModConfig1) of
                            {Module, Reason} ->
                                {AsyncBackends,
                                 SyncBackends,
                                 [{Module, Reason} | Errors]};
                            AsyncBackend ->
                                {[AsyncBackend | AsyncBackends],
                                 SyncBackends,
                                 Errors}
                        end;
                false ->
                    ModConfig1 = [{async_folds, false}
                                  | ModConfig],
                    case start_backend(Name,
                                       Module,
                                       Partition,
                                       ModConfig1) of
                        {Module, Reason} ->
                            {AsyncBackends,
                             SyncBackends,
                             [{Module, Reason} | Errors]};
                        SyncBackend ->
                            {AsyncBackends,
                             [SyncBackend | SyncBackends],
                             Errors}
                    end
            end
    end.

%% @private
start_backend(Name, Module, Partition, Config) ->
    try
        case Module:start(Partition, Config) of
            {ok, State} ->
                {Name, Module, State};
            {error, Reason} ->
                {Module, Reason}
        end
    catch
        _:Reason1 ->
             {Module, Reason1}
    end.

%% @doc Stop the backends
-spec stop(state()) -> ok.
stop(#state{async_backends=AsyncBackends,
            sync_backends=SyncBackends}) ->
    Backends = AsyncBackends ++ SyncBackends,
    [Module:stop(SubState) || {_, Module, SubState} <- Backends],
    ok.

%% @doc Retrieve an object from the backend
-spec get(riak_object:bucket(), riak_object:key(), state()) ->
                 {ok, any(), state()} |
                 {ok, not_found, state()} |
                 {error, term(), state()}.
get(Bucket, Key, State) ->
    {Name, Module, SubState} = get_backend(Bucket, State),
    case Module:get(Bucket, Key, SubState) of
        {ok, Value, NewSubState} ->
            NewState = update_backend_state(Name, Module, NewSubState, State),
            {ok, Value, NewState};
        {error, Reason, NewSubState} ->
            NewState = update_backend_state(Name, Module, NewSubState, State),
            {error, Reason, NewState}
    end.

%% @doc Insert an object with secondary index
%% information into the kv backend
-type index_spec() :: {add, Index, SecondaryKey} | {remove, Index, SecondaryKey}.
-spec put(riak_object:bucket(), riak_object:key(), [index_spec()], binary(), state()) ->
                 {ok, state()} |
                 {error, term(), state()}.
put(Bucket, PrimaryKey, IndexSpecs, Value, State) ->
    {Name, Module, SubState} = get_backend(Bucket, State),
    case Module:put(Bucket, PrimaryKey, IndexSpecs, Value, SubState) of
        {ok, NewSubState} ->
            NewState = update_backend_state(Name, Module, NewSubState, State),
            {ok, NewState};
        {error, Reason, NewSubState} ->
            NewState = update_backend_state(Name, Module, NewSubState, State),
            {error, Reason, NewState}
    end.

%% @doc Delete an object from the backend
-spec delete(riak_object:bucket(), riak_object:key(), [index_spec()], state()) ->
                    {ok, state()} |
                    {error, term(), state()}.
delete(Bucket, Key, IndexSpecs, State) ->
    {Name, Module, SubState} = get_backend(Bucket, State),
    case Module:delete(Bucket, Key, IndexSpecs, SubState) of
        {ok, NewSubState} ->
            NewState = update_backend_state(Name, Module, NewSubState, State),
            {ok, NewState};
        {error, Reason, NewSubState} ->
            NewState = update_backend_state(Name, Module, NewSubState, State),
            {error, Reason, NewState}
    end.

%% @doc Fold over all the buckets
-spec fold_buckets(riak_kv_backend:fold_buckets_fun(),
                   any(),
                   [],
                   state()) -> fold_result().
fold_buckets(FoldBucketsFun, Acc, Opts, State) ->
    fold(undefined, fold_buckets, FoldBucketsFun, Acc, Opts, State).

%% @doc Fold over all the keys for one or all buckets.
-spec fold_keys(riak_kv_backend:fold_keys_fun(),
                any(),
                [{atom(), term()}],
                state()) -> fold_result().
fold_keys(FoldKeysFun, Acc, Opts, State) ->
    Bucket = proplists:get_value(bucket, Opts),
    fold(Bucket, fold_keys, FoldKeysFun, Acc, Opts, State).

%% @doc Fold over all the objects for one or all buckets.
-spec fold_objects(riak_kv_backend:fold_objects_fun(),
                   any(),
                   [{atom(), term()}],
                   state()) -> fold_result().
fold_objects(FoldObjectsFun, Acc, Opts, State) ->
    Bucket = proplists:get_value(bucket, Opts),
    fold(Bucket, fold_objects, FoldObjectsFun, Acc, Opts, State).

%% @doc Delete all objects from the different backends
-spec drop(state()) -> {ok, state()} | {error, term(), state()}.
drop(#state{async_backends=AsyncBackends,
            sync_backends=SyncBackends}=State) ->
    Fun = fun({Name, Module, SubState}) ->
                  case Module:drop(SubState) of
                      {ok, NewSubState} ->
                          {Name, Module, NewSubState};
                      {error, Reason, NewSubState} ->
                          {error, Reason, NewSubState}
                  end
          end,
    AsyncDropResults = [Fun(Backend) || Backend <- AsyncBackends],
    SyncDropResults = [Fun(Backend) || Backend <- SyncBackends],
    {Errors, UpdAsyncBackends} =
        lists:splitwith(fun error_filter/1, AsyncDropResults),
    {Errors, UpdSyncBackends} =
        lists:splitwith(fun error_filter/1, SyncDropResults),
    case Errors of
        [] ->
            {ok, State#state{async_backends=UpdAsyncBackends,
                             sync_backends=UpdSyncBackends}};
        _ ->
            {error, Errors, State#state{async_backends=UpdAsyncBackends,
                                        sync_backends=UpdSyncBackends}}
    end.

%% @doc Returns true if the backend contains any
%% non-tombstone values; otherwise returns false.
-spec is_empty(state()) -> boolean().
is_empty(#state{async_backends=AsyncBackends,
                sync_backends=SyncBackends}) ->
    Fun = fun({_, Module, SubState}) ->
                  Module:is_empty(SubState)
          end,
    lists:all(Fun, AsyncBackends ++ SyncBackends).

%% @doc Get the status information for this backend
-spec status(state()) -> [{atom(), term()}].
status(#state{async_backends=AsyncBackends,
              sync_backends=SyncBackends}) ->
    %% @TODO Reexamine how this is handled
    Backends = AsyncBackends ++ SyncBackends,
    [{N, Mod:status(ModState)} || {N, Mod, ModState} <- Backends].

%% @doc Register an asynchronous callback
-spec callback(reference(), any(), state()) -> {ok, state()}.
callback(Ref, Msg, #state{async_backends=AsyncBackends,
                          sync_backends=SyncBackends}=State) ->
    %% Pass the callback on to all submodules - their responsbility to
    %% filter out if they need it.
    Backends = AsyncBackends ++ SyncBackends,
    [Mod:callback(Ref, Msg, ModState) || {_N, Mod, ModState} <- Backends],
    {ok, State}.

%% ===================================================================
%% Internal functions
%% ===================================================================

%% @private
%% Given a Bucket name and the State, return the
%% backend definition. (ie: {Name, Module, SubState})
get_backend(Bucket, #state{async_backends=AsyncBackends,
                           sync_backends=SyncBackends,
                           default_backend=DefaultBackend}) ->
    BucketProps = riak_core_bucket:get_bucket(Bucket),
    BackendName = proplists:get_value(backend, BucketProps, DefaultBackend),
    %% Ensure that a backend by that name exists...
    Backends = AsyncBackends ++ SyncBackends,
    case lists:keyfind(BackendName, 1, Backends) of
        false -> throw({?MODULE, undefined_backend, BackendName});
        Backend -> Backend
    end.

%% @private
%% @doc Update the state for one of the
%% composing backends of this multi backend.
update_backend_state(Backend, Module, ModState, State) ->
    AsyncBackends = State#state.async_backends,

    case lists:keymember(Backend, 1, AsyncBackends) of
        true ->
            NewAsyncBackends = lists:keyreplace(Backend,
                                                1,
                                                AsyncBackends,
                                                {Backend, Module, ModState}),
            State#state{async_backends=NewAsyncBackends};
        false ->
            SyncBackends = State#state.sync_backends,
            NewSyncBackends = lists:keyreplace(Backend,
                                               1,
                                               SyncBackends,
                                               {Backend, Module, ModState}),
            State#state{sync_backends=NewSyncBackends}
    end.

%% @private
%% @doc Shared code used by all the backend fold functions.
fold(Bucket, ModFun, FoldFun, Acc, Opts, State=#state{async_backends=AsyncBackends,
                                                      sync_backends=SyncBackends}) ->
    case Bucket of
        undefined ->
            BackendFoldFun =
                fun({_, Module, SubState}, FoldAcc) ->
                        Result = Module:ModFun(FoldFun,
                                               FoldAcc,
                                               Opts,
                                               SubState),
                        case Result of
                            {ok, FoldAcc1} ->
                                FoldAcc1;
                            {async, AsyncWork} ->
                                AsyncWork();
                            {error, Reason} ->
                                throw({error, {Module, Reason}})
                        end
                end,
            try
                case SyncBackends of 
                    [] ->
                        Acc0 = Acc;
                    _ ->
                        Acc0 = lists:foldl(BackendFoldFun, Acc, SyncBackends)
                end,
                %% We have now accumulated the results for all of the
                %% synchronous backends. The next step is to wrap the
                %% asynchronous work in a function that passes the accumulator
                %% to each successive piece of asynchronous work.
                case AsyncBackends of
                    [] ->
                        %% Just return the synchronous results
                        {ok, Acc0};
                    _ ->
                        AsyncWork =
                            fun() ->
                                    lists:foldl(BackendFoldFun, Acc0, AsyncBackends)
                            end,
                        {async, AsyncWork}
                end
            catch
                Error ->
                    Error
            end;
        _ ->
            {_Name, Module, SubState} = get_backend(Bucket, State),
            Module:ModFun(FoldFun,
                          Acc,
                          Opts,
                          SubState)
    end.

%% @private
%% @doc Function to filter error results when
%% calling a function on the entire list of
%% backends composing this multi backend.
error_filter({error, _, _}) ->
    true;
error_filter(_) ->
    false.

%% @private
config_value(Key, Config) ->
    config_value(Key, Config, undefined).

%% @private
config_value(Key, Config, Default) ->
    case proplists:get_value(Key, Config) of
        undefined ->
            app_helper:get_env(riak_kv, Key, Default);
        Value ->
            Value
    end.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

%% @private
multi_backend_test_() ->
    {foreach,
     fun() ->
             crypto:start(),

             %% start the ring manager
             {ok, P1} = riak_core_ring_events:start_link(),
             {ok, P2} = riak_core_ring_manager:start_link(test),
             application:load(riak_core),
             application:set_env(riak_core, default_bucket_props, []),

             %% Have to do some prep for bitcask
             application:load(bitcask),
             ?assertCmd("rm -rf test/bitcask-backend"),
             application:set_env(bitcask, data_root, "test/bitcask-backend"),

             [P1, P2]
     end,
     fun([P1, P2]) ->
             crypto:stop(),
             ?assertCmd("rm -rf test/bitcask-backend"),
             unlink(P1),
             unlink(P2),
             catch exit(P1, kill),
             catch exit(P2, kill)
     end,
     [
      fun(_) ->
              {"simple test",
               fun() ->
                       riak_core_bucket:set_bucket(<<"b1">>, [{backend, first_backend}]),
                       riak_core_bucket:set_bucket(<<"b2">>, [{backend, second_backend}]),

                       %% Run the standard backend test...
                       Config = sample_config(),
                       riak_kv_backend:standard_test(?MODULE, Config)
               end
              }
      end,
      fun(_) ->
              {"get_backend_test",
               fun() ->
                       riak_core_bucket:set_bucket(<<"b1">>, [{backend, first_backend}]),
                       riak_core_bucket:set_bucket(<<"b2">>, [{backend, second_backend}]),

                       %% Start the backend...
                       {ok, State} = start(42, sample_config()),

                       %% Check our buckets...
                       {first_backend, riak_kv_memory_backend, _} = get_backend(<<"b1">>, State),
                       {second_backend, riak_kv_memory_backend, _} = get_backend(<<"b2">>, State),

                       %% Check the default...
                       {second_backend, riak_kv_memory_backend, _} = get_backend(<<"b3">>, State),
                       ok
               end
              }
      end,
      fun(_) ->
              {"start error with invalid backend test",
               fun() ->
                       %% Attempt to start the backend with a
                       %% nonexistent backend specified
                       ?assertEqual({error, [{riak_kv_devnull_backend, undef}]},
                                    start(42, bad_backend_config()))
               end
              }
      end
     ]
    }.

-ifdef(EQC).

%% @private
eqc_test_() ->
    {spawn,
     [{inorder,
       [{setup,
         fun setup/0,
         fun cleanup/1,
         [?_assertEqual(true,
                        backend_eqc:test(?MODULE, true, sample_config())),
          ?_assertEqual(true,
                        backend_eqc:test(?MODULE, true, async_fold_config())),
          ?_assertEqual(true,
                        backend_eqc:test(?MODULE,
                                         true,
                                         sync_and_async_fold_config()))
         ]}]}]}.

setup() ->
    %% Start the ring manager...
    crypto:start(),
    {ok, P1} = riak_core_ring_events:start_link(),
    {ok, P2} = riak_core_ring_manager:start_link(test),

    %% Set some buckets...
    application:load(riak_core), % make sure default_bucket_props is set
    application:set_env(riak_core, default_bucket_props, []),
    riak_core_bucket:set_bucket(<<"b1">>, [{backend, first_backend}]),
    riak_core_bucket:set_bucket(<<"b2">>, [{backend, second_backend}]),

    {P1, P2}.

cleanup({P1, P2}) ->
    crypto:stop(),
    application:stop(riak_core),

    unlink(P1),
    unlink(P2),
    catch exit(P1, kill),
    catch exit(P2, kill).

async_fold_config() ->
    [
     {storage_backend, riak_kv_multi_backend},
     {multi_backend_default, second_backend},
      {multi_backend, [
                      {first_backend, riak_kv_memory_backend, []},
                      {second_backend, riak_kv_memory_backend, []}
                     ]}
    ].

sync_and_async_fold_config() ->
    [
     {storage_backend, riak_kv_multi_backend},
     {multi_backend_default, second_backend},
      {multi_backend, [
                      {first_backend, riak_kv_memory_backend, []},
                      {second_backend,
                       riak_kv_memory_backend,
                       [{async_folds, false}]}
                     ]}
    ].

-endif. % EQC

%% Check extra callback messages are ignored by backends
extra_callback_test() ->
    %% Have to do some prep for bitcask
    application:load(bitcask),
    ?assertCmd("rm -rf test/bitcask-backend"),
    application:set_env(bitcask, data_root, "test/bitcask-backend"),

    %% Have to do some prep for eleveldb
    application:load(eleveldb),
    ?assertCmd("rm -rf test/eleveldb-backend"),
    application:set_env(eleveldb, data_root, "test/eleveldb-backend"),

    %% Start up multi backend
    Config = [{async_folds, false},
              {storage_backend, riak_kv_multi_backend},
              {multi_backend_default, memory},
              {multi_backend,
               [{bitcask, riak_kv_bitcask_backend, []},
                {memory, riak_kv_memory_backend, []},
                {eleveldb, riak_kv_eleveldb_backend, []}]}],
    {ok, State} = start(0, Config),
    callback(make_ref(), ignore_me, State),
    stop(State),
    application:stop(bitcask).

bad_config_test() ->
    ErrorReason = multi_backend_config_unset,
    ?assertEqual({error, ErrorReason}, start(0, [])).

sample_config() ->
    [
     {async_folds, false},
     {storage_backend, riak_kv_multi_backend},
     {multi_backend_default, second_backend},
      {multi_backend, [
                      {first_backend, riak_kv_memory_backend, []},
                      {second_backend, riak_kv_memory_backend, []}
                     ]}
    ].

bad_backend_config() ->
    [
     {storage_backend, riak_kv_multi_backend},
     {multi_backend_default, second_backend},
      {multi_backend, [
                      {first_backend, riak_kv_devnull_backend, []},
                      {second_backend, riak_kv_memory_backend, []}
                     ]}
    ].

-endif.
