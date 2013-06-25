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
         data_size/1,
         status/1,
         callback/3,
         fix_index/3,
         set_legacy_indexes/2,
         mark_indexes_fixed/2,
         fixed_index_status/1]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(API_VERSION, 1).
-define(CAPABILITIES, [async_fold, index_reformat]).

-record (state, {backends :: [{atom(), atom(), term()}],
                 default_backend :: atom()}).

-type state() :: #state{}.
-type config() :: [{atom(), term()}].

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
%% current API.
-spec api_version() -> {ok, integer()}.
api_version() ->
    {ok, ?API_VERSION}.

%% @doc Return the capabilities of the backend.
-spec capabilities(state()) -> {ok, [atom()]}.
capabilities(State) ->
    %% Expose ?CAPABILITIES plus the intersection of all child
    %% backends. (This backend creates a shim for any backends that
    %% don't support async_fold.)
    F = fun(Mod, ModState) ->
                {ok, S1} = Mod:capabilities(ModState),
                ordsets:from_list(S1)
        end,
    AllCaps = [F(Mod, ModState) || {_, Mod, ModState} <- State#state.backends],
    Caps1 = ordsets:intersection(AllCaps),
    Caps2 = ordsets:to_list(Caps1),

    Capabilities = lists:usort(?CAPABILITIES ++ Caps2),
    {ok, Capabilities}.

%% @doc Return the capabilities of the backend.
-spec capabilities(riak_object:bucket(), state()) -> {ok, [atom()]}.
capabilities(Bucket, State) when is_binary(Bucket) ->
    {_Name, Mod, ModState} = get_backend(Bucket, State),
    Mod:capabilities(ModState);
capabilities(_Bucket, State) ->
    capabilities(State).

%% @doc Start the backends
-spec start(integer(), config()) -> {ok, state()} | {error, term()}.
start(Partition, Config) ->
    %% Sanity checking
    Defs =  app_helper:get_prop_or_env(multi_backend, Config, riak_kv),
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

            %% Get the default
            DefaultBackend = app_helper:get_prop_or_env(multi_backend_default, Config, riak_kv, First),
            case lists:keymember(DefaultBackend, 1, Defs) of
                true ->
                    %% Start the backends
                    BackendFun = start_backend_fun(Partition),
                    {Backends, Errors} =
                        lists:foldl(BackendFun, {[], []}, Defs),
                    case Errors of
                        [] ->
                            {ok, #state{backends=Backends,
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
start_backend_fun(Partition) ->
    fun({Name, Module, ModConfig}, {Backends, Errors}) ->
            try
                case start_backend(Name,
                                   Module,
                                   Partition,
                                   ModConfig) of
                    {Module, Reason} ->
                        {Backends,
                         [{Module, Reason} | Errors]};
                    Backend ->
                        {[Backend | Backends],
                         Errors}
                end
            catch _:Error ->
                    {Backends,
                     [{Module, Error} | Errors]}
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
stop(#state{backends=Backends}) ->
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
                   [{atom(), term()}],
                   state()) -> {ok, any()} | {async, fun()} | {error, term()}.
fold_buckets(FoldBucketsFun, Acc, Opts, State) ->
    fold_all(fold_buckets, FoldBucketsFun, Acc, Opts, State).

%% @doc Fold over all the keys for one or all buckets.
-spec fold_keys(riak_kv_backend:fold_keys_fun(),
                any(),
                [{atom(), term()}],
                state()) -> {ok, any()} | {async, fun()} | {error, term()}.
fold_keys(FoldKeysFun, Acc, Opts, State) ->
    case proplists:get_value(bucket, Opts) of
        undefined ->
            fold_all(fold_keys, FoldKeysFun, Acc, Opts, State);
        Bucket ->
            fold_in_bucket(Bucket, fold_keys, FoldKeysFun, Acc, Opts, State)
    end.

%% @doc Fold over all the objects for one or all buckets.
-spec fold_objects(riak_kv_backend:fold_objects_fun(),
                   any(),
                   [{atom(), term()}],
                   state()) -> {ok, any()} | {async, fun()} | {error, term()}.
fold_objects(FoldObjectsFun, Acc, Opts, State) ->
    case proplists:get_value(bucket, Opts) of
        undefined ->
            fold_all(fold_objects, FoldObjectsFun, Acc, Opts, State);
        Bucket ->
            fold_in_bucket(Bucket, fold_objects, FoldObjectsFun, Acc, Opts, State)
    end.

%% @doc Delete all objects from the different backends
-spec drop(state()) -> {ok, state()} | {error, term(), state()}.
drop(#state{backends=Backends}=State) ->
    Fun = fun({Name, Module, SubState}) ->
                  case Module:drop(SubState) of
                      {ok, NewSubState} ->
                          {Name, Module, NewSubState};
                      {error, Reason, NewSubState} ->
                          {error, Reason, NewSubState}
                  end
          end,
    DropResults = [Fun(Backend) || Backend <- Backends],
    {Errors, UpdBackends} =
        lists:splitwith(fun error_filter/1, DropResults),
    case Errors of
        [] ->
            {ok, State#state{backends=UpdBackends}};
        _ ->
            {error, Errors, State#state{backends=UpdBackends}}
    end.

%% @doc Returns true if the backend contains any
%% non-tombstone values; otherwise returns false.
-spec is_empty(state()) -> boolean().
is_empty(#state{backends=Backends}) ->
    Fun = fun({_, Module, SubState}) ->
                  Module:is_empty(SubState)
          end,
    lists:all(Fun, Backends).

%% @doc Not currently supporting data size
%% @todo Come up with a way to reflect mixed backend data sizes,
%% as level reports in bytes, bitcask in # of keys.
-spec data_size(state()) -> undefined.
data_size(_) ->
    undefined.

%% @doc Get the status information for this backend
-spec status(state()) -> [{atom(), term()}].
status(#state{backends=Backends}) ->
    %% @TODO Reexamine how this is handled
    %% all backend mods return a proplist from Mod:status/1
    %% So as to tag the backend with its mod, without
    %% breaking this API list of two tuples return,
    %% add the tuple {mod, Mod} to the status for each
    %% backend.
    [{N, [{mod, Mod} | Mod:status(ModState)]} || {N, Mod, ModState} <- Backends].

%% @doc Register an asynchronous callback
-spec callback(reference(), any(), state()) -> {ok, state()}.
callback(Ref, Msg, #state{backends=Backends}=State) ->
    %% Pass the callback on to all submodules - their responsbility to
    %% filter out if they need it.
    [Mod:callback(Ref, Msg, ModState) || {_N, Mod, ModState} <- Backends],
    {ok, State}.

set_legacy_indexes(State=#state{backends=Backends}, WriteLegacy) ->
    NewBackends = [{I, Mod, maybe_set_legacy_indexes(Mod, ModState, WriteLegacy)} ||
                      {I, Mod, ModState} <- Backends],
    State#state{backends=NewBackends}.

maybe_set_legacy_indexes(Mod, ModState, WriteLegacy) ->
    case backend_can_index_reformat(Mod, ModState) of
        true -> Mod:set_legacy_indexes(ModState, WriteLegacy);
        false -> ModState
    end.

mark_indexes_fixed(State=#state{backends=Backends}, ForUpgrade) ->
    NewBackends = mark_indexes_fixed(Backends, [], ForUpgrade),
    {ok, State#state{backends=NewBackends}}.

mark_indexes_fixed([], NewBackends, _) ->
    lists:reverse(NewBackends);
mark_indexes_fixed([{I, Mod, ModState} | Backends], NewBackends, ForUpgrade) ->
    Res = maybe_mark_indexes_fixed(Mod, ModState, ForUpgrade),
    case Res of
        {error, Reason} ->
            {error, Reason};
        {ok, NewModState} ->
            mark_indexes_fixed(Backends, [{I, Mod, NewModState} | NewBackends], ForUpgrade)
    end.

maybe_mark_indexes_fixed(Mod, ModState, ForUpgrade) ->
    case backend_can_index_reformat(Mod, ModState) of
        true -> Mod:mark_indexes_fixed(ModState, ForUpgrade);
        false -> {ok, ModState}
    end.

fix_index(BKeys, ForUpgrade, State) ->
    % Group keys per bucket 
    PerBucket = lists:foldl(fun(BK={B,_},D) -> dict:append(B,BK,D) end, dict:new(), BKeys),
    Result = 
        dict:fold(
            fun(Bucket, StorageKey, Acc = {Success, Ignore, Errors}) ->
                {_, Mod,  ModState} = Backend = get_backend(Bucket, State),
                case backend_can_index_reformat(Mod, ModState) of
                    true -> 
                            {S, I, E} = backend_fix_index(Backend, Bucket, 
                                                          StorageKey, ForUpgrade),
                            {Success + S, Ignore + I, Errors + E};
                    false -> 
                            Acc
                end
            end, {0, 0, 0}, PerBucket),
    {reply, Result, State}.

backend_fix_index({_, Mod, ModState}, Bucket, StorageKey, ForUpgrade) ->
    case Mod:fix_index(StorageKey, ForUpgrade, ModState) of
        {reply, Reply, _UpModState} -> 
            Reply;
        {error, Reason} ->
           lager:error("Failed to fix index for bucket ~p, key ~p, backend ~p: ~p",
                       [Bucket, StorageKey, Mod, Reason]),
            {0, 0, length(StorageKey)}
    end.

-spec fixed_index_status(state()) -> boolean().
fixed_index_status(#state{backends=Backends}) ->
    lists:foldl(fun({_N, Mod, ModState}, Acc) ->
                        Status = Mod:status(ModState),
                        case fixed_index_status(Mod, ModState, Status) of
                            undefined -> Acc;
                            Res ->
                                case Acc of
                                    undefined -> Res;
                                    _ -> Res andalso Acc
                                end
                        end
                end,
                undefined,
                Backends).

fixed_index_status(Mod, ModState, Status) ->
    case backend_can_index_reformat(Mod, ModState) of
        true -> proplists:get_value(fixed_indexes, Status);
        false -> undefined
    end.



%% ===================================================================
%% Internal functions
%% ===================================================================

%% @private
%% Given a Bucket name and the State, return the
%% backend definition. (ie: {Name, Module, SubState})
get_backend(Bucket, #state{backends=Backends,
                           default_backend=DefaultBackend}) ->
    BucketProps = riak_core_bucket:get_bucket(Bucket),
    BackendName = proplists:get_value(backend, BucketProps, DefaultBackend),
    %% Ensure that a backend by that name exists...
    case lists:keyfind(BackendName, 1, Backends) of
        false -> throw({?MODULE, undefined_backend, BackendName});
        Backend -> Backend
    end.

%% @private
%% @doc Update the state for one of the
%% composing backends of this multi backend.
update_backend_state(Backend,
                     Module,
                     ModState,
                     State=#state{backends=Backends}) ->
    NewBackends = lists:keyreplace(Backend,
                                   1,
                                   Backends,
                                   {Backend, Module, ModState}),
    State#state{backends=NewBackends}.

%% @private
%% @doc Shared code used by all the backend fold functions.
fold_all(ModFun, FoldFun, Acc, Opts, State) ->
    Backends = State#state.backends,
    try
        AsyncFold = lists:member(async_fold, Opts),
        {Acc0, AsyncWorkList} =
            lists:foldl(backend_fold_fun(ModFun,
                                         FoldFun,
                                         Opts,
                                         AsyncFold),
                        {Acc, []},
                        Backends),

        %% We have now accumulated the results for all of the
        %% synchronous backends. The next step is to wrap the
        %% asynchronous work in a function that passes the accumulator
        %% to each successive piece of asynchronous work.
        case AsyncWorkList of
            [] ->
                %% Just return the synchronous results
                {ok, Acc0};
            _ ->
                AsyncWork =
                    fun() ->
                            lists:foldl(async_fold_fun(), Acc0, AsyncWorkList)
                    end,
                {async, AsyncWork}
        end
    catch
        Error ->
            Error
    end.

fold_in_bucket(Bucket, ModFun, FoldFun, Acc, Opts, State) ->
    {_Name, Module, SubState} = get_backend(Bucket, State),
    Module:ModFun(FoldFun,
                  Acc,
                  Opts,
                  SubState).

%% @private
backend_fold_fun(ModFun, FoldFun, Opts, AsyncFold) ->
    fun({_, Module, SubState}, {Acc, WorkList}) ->
            %% Get the backend capabilities to determine
            %% if it supports asynchronous folding.
            {ok, ModCaps} = Module:capabilities(SubState),
            DoAsync = AsyncFold andalso lists:member(async_fold, ModCaps),
            Indexes = lists:keyfind(index, 1, Opts),
            case Indexes of
                {index, incorrect_format, _ForUpgrade} ->
                    case lists:member(index_reformat, ModCaps) of
                        true -> backend_fold_fun(Module, ModFun, SubState, FoldFun,
                                                 Opts, {Acc, WorkList}, DoAsync);
                        false -> {Acc, WorkList}
                    end;
                _ ->
                    backend_fold_fun(Module,
                                     ModFun,
                                     SubState,
                                     FoldFun,
                                     Opts,
                                     {Acc, WorkList},
                                     DoAsync)
            end
    end.

backend_fold_fun(Module, ModFun, SubState, FoldFun, Opts, {Acc, WorkList}, true) ->
    AsyncWork =
        fun(Acc1) ->
                Module:ModFun(FoldFun,
                              Acc1,
                              Opts,
                              SubState)
        end,
    {Acc, [AsyncWork | WorkList]};
backend_fold_fun(Module, ModFun, SubState, FoldFun, Opts, {Acc, WorkList}, false) ->
    Result = Module:ModFun(FoldFun,
                           Acc,
                           Opts,
                           SubState),
    case Result of
        {ok, Acc1} ->
            {Acc1, WorkList};
        {error, Reason} ->
            throw({error, {Module, Reason}})
    end.

async_fold_fun() ->
    fun(AsyncWork, Acc) ->
            case AsyncWork(Acc) of
                {ok, Acc1} ->
                    Acc1;
                {async, AsyncFun} ->
                    AsyncFun();
                {error, Reason} ->
                    throw({error, Reason})
            end
    end.

%% @private
%% @doc Function to filter error results when
%% calling a function on the entire list of
%% backends composing this multi backend.
error_filter({error, _, _}) ->
    true;
error_filter(_) ->
    false.

backend_can_index_reformat(Mod, ModState) ->
    {ok, Caps} = Mod:capabilities(ModState),
    lists:member(index_reformat, Caps).

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
             catch exit(P2, kill),
             wait_until_dead(P1),
             wait_until_dead(P2)
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
         [{timeout, 60000, [?_assertEqual(true,
                        backend_eqc:test(?MODULE, true, sample_config()))]},
          {timeout, 60000, [?_assertEqual(true,
                        backend_eqc:test(?MODULE, true, async_fold_config()))]}
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
    catch exit(P2, kill),
    wait_until_dead(P1),
    wait_until_dead(P2).

async_fold_config() ->
    [
     {storage_backend, riak_kv_multi_backend},
     {multi_backend_default, second_backend},
      {multi_backend, [
                      {first_backend, riak_kv_memory_backend, []},
                      {second_backend, riak_kv_memory_backend, []}
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
    Config = [{storage_backend, riak_kv_multi_backend},
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
    application:unset_env(riak_kv, multi_backend),
    ErrorReason = multi_backend_config_unset,
    ?assertEqual({error, ErrorReason}, start(0, [])).

sample_config() ->
    [
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

%% Minor sin of cut-and-paste....
wait_until_dead(Pid) when is_pid(Pid) ->
    Ref = monitor(process, Pid),
    receive
        {'DOWN', Ref, process, _Obj, Info} ->
            Info
    after 10*1000 ->
            exit({timeout_waiting_for, Pid})
    end;
wait_until_dead(_) ->
    ok.

-endif.
