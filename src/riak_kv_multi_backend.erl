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
         put/4,
         delete/3,
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
-define(CAPABILITIES, []).

-record (state, {backends :: {atom(), atom(), term()},
                 default_backend :: atom()}).

-opaque(state() :: #state{}).
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
%% current API and a capabilities list.
-spec api_version() -> {integer(), [atom()]}.
api_version() ->
    {?API_VERSION, ?CAPABILITIES}.

%% @doc Start the backends
-spec start(integer(), config()) -> {ok, state()} | {error, term()}.
start(Partition, Config) ->
                                                % Sanity checking...
    Defs = proplists:get_value(multi_backend, Config),
    assert(is_list(Defs), {invalid_config_setting, multi_backend, list_expected}),
    assert(length(Defs) > 0, {invalid_config_setting, multi_backend, list_is_empty}),
    {First, _, _} = hd(Defs),

                                                % Get the default...
    DefaultBackend = proplists:get_value(multi_backend_default, Config, First),
    assert(lists:keymember(DefaultBackend, 1, Defs), {invalid_config_setting, multi_backend_default, backend_not_found}),

                                                % Start the backends...
    Backends = [begin
                    {ok, State} = Module:start(Partition, SubConfig),
                    {Name, Module, State}
                end || {Name, Module, SubConfig} <- Defs],

    {ok, #state { backends=Backends, default_backend=DefaultBackend}}.

%% @doc Stop the backends
-spec stop(state()) -> ok.
stop(State) ->
    Backends = State#state.backends,
    Results = [Module:stop(SubState) || {_, Module, SubState} <- Backends],
    ErrorResults = [X || X <- Results, X /= ok],
    case ErrorResults of
        [] -> ok;
        _ -> {error, ErrorResults}
    end.

%% @doc Retrieve an object from the backend
-spec get(riak_object:bucket(), riak_object:key(), state()) ->
                 {ok, any(), state()} |
                 {ok, not_found, state()} |
                 {error, term(), state()}.
get(Bucket, Key, State) ->
    {_Name, Module, SubState} = get_backend(Bucket, State),
    Module:get(Bucket, Key, SubState).

%% @doc Insert an object into the eleveldb backend
-spec put(riak_object:bucket(), riak_object:key(), binary(), state()) ->
                 {ok, state()} |
                 {error, term(), state()}.
put(Bucket, Key, Value, State) ->
    {_Name, Module, SubState} = get_backend(Bucket, State),
    Module:put(Bucket, Key, Value, SubState).

%% @doc Delete an object from the backend
-spec delete(riak_object:bucket(), riak_object:key(), state()) ->
                    {ok, state()} |
                    {error, term(), state()}.
delete(Bucket, Key, State) ->
    {_Name, Module, SubState} = get_backend(Bucket, State),
    Module:delete(Bucket, Key, SubState).

%% @doc Fold over all the buckets
-spec fold_buckets(riak_kv_backend:fold_buckets_fun(),
                   any(),
                   [],
                   state()) -> {ok, any()} | {error, term()}.
fold_buckets(FoldBucketsFun, Acc, Opts, #state{backends=Backends}) ->
    FoldFun = fun({_, Module, SubState}, Acc1) ->
                      Module:fold_buckets(FoldBucketsFun, Acc1, Opts, SubState)
              end,
    lists:foldl(FoldFun, Acc, Backends).

%% @doc Fold over all the keys for one or all buckets.
-spec fold_keys(riak_kv_backend:fold_keys_fun(),
                any(),
                [{atom(), term()}],
                state()) -> {ok, term()} | {error, term()}.
fold_keys(FoldKeysFun, Acc, Opts, #state{backends=Backends}) ->
    FoldFun = fun({_, Module, SubState}, Acc1) ->
                      Module:fold_keys(FoldKeysFun, Acc1, Opts, SubState)
              end,
    lists:foldl(FoldFun, Acc, Backends).

%% @doc Fold over all the objects for one or all buckets.
-spec fold_objects(riak_kv_backend:fold_objects_fun(),
                   any(),
                   [{atom(), term()}],
                   state()) -> {ok, any()} | {error, term()}.
fold_objects(FoldObjectsFun, Acc, Opts, #state{backends=Backends}) ->
    FoldFun = fun({_, Module, SubState}, Acc1) ->
                      Module:fold_objects(FoldObjectsFun, Acc1, Opts, SubState)
              end,
    lists:foldl(FoldFun, Acc, Backends).

%% @doc Delete all objects from the different backends
-spec drop(state()) -> {ok, state()} | {error, term(), state()}.
drop(#state{backends=Backends}) ->
    Fun = fun({_, Module, SubState}) ->
                  Module:drop(SubState)
          end,
    [Fun(Backend) || Backend <- Backends],
    ok.

%% @doc Returns true if the backend contains any
%% non-tombstone values; otherwise returns false.
-spec is_empty(state()) -> boolean().
is_empty(#state{backends=Backends}) ->
    Fun = fun({_, Module, SubState}) ->
                  Module:is_empty(SubState)
          end,
    lists:all(Fun, Backends).

%% @doc Get the status information for this backend
-spec status(state()) -> [{atom(), term()}].
status(#state{backends=Backends}) ->
    %% @TODO Reexamine how this is handled
    [{N, Mod:status(ModState)} || {N, Mod, ModState} <- Backends].

%% @doc Register an asynchronous callback
-spec callback(reference(), any(), state()) -> {ok, state()}.
callback(Ref, Msg, #state{backends=Backends}=State) ->
    %% Pass the callback on to all submodules - their responsbility to
    %% filter out if they need it.
    [Mod:callback(Ref, Msg, ModState) || {_N, Mod, ModState} <- Backends],
    {ok, State}.

%% ===================================================================
%% Internal functions
%% ===================================================================

%% Given a Bucket name and the State, return the
%% backend definition. (ie: {Name, Module, SubState})
get_backend(Bucket, State) ->
    %% Get the name of the backend...
    DefaultBackend = State#state.default_backend,
    BucketProps = riak_core_bucket:get_bucket(Bucket),
    BackendName = proplists:get_value(backend, BucketProps, DefaultBackend),
    %% Ensure that a backend by that name exists...
    Backends = State#state.backends,
    case lists:keyfind(BackendName, 1, Backends) of
        false -> throw({?MODULE, undefined_backend, BackendName});
        Backend -> Backend
    end.

assert(true, _) -> ok;
assert(false, Error) -> throw({?MODULE, Error}).


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
             application:unload(bitcask),
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

                                                % Run the standard backend test...
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
      end
     ]
    }.

-ifdef(EQC).
%% @private

eqc_test() ->
    %% Start the ring manager...
    crypto:start(),
    {ok, P1} = riak_core_ring_events:start_link(),
    {ok, P2} = riak_core_ring_manager:start_link(test),

    %% Set some buckets...
    application:load(riak_core), % make sure default_bucket_props is set
    application:set_env(riak_core, default_bucket_props, []),
    riak_core_bucket:set_bucket(<<"b1">>, [{backend, first_backend}]),
    riak_core_bucket:set_bucket(<<"b2">>, [{backend, second_backend}]),

    %% @TODO Get this to work with bitcask
    %% %% Have to do some prep for bitcask
    %% application:load(bitcask),
    %% ?assertCmd("rm -rf test/bitcask-backend"),
    %% application:set_env(bitcask, data_root, "test/bitcask-backend"),

    %% Run the standard backend test
    Config = sample_config(),
    ?assertEqual(true, backend_eqc:test(?MODULE, true, Config)),

    %% cleanup
    crypto:stop(),
    application:unload(riak_core),

    unlink(P1),
    unlink(P2),
    catch exit(P1, kill),
    catch exit(P2, kill).

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
    application:unload(bitcask),
    application:unload(eleveldb).


bad_config_test() ->
    %% {invalid_config_setting, multi_backend_default, backend_not_found}
    ?assertThrow({riak_kv_multi_backend,
                  {invalid_config_setting,multi_backend,list_expected}},
                 start(0, [])).


sample_config() ->
    [
     {storage_backend, riak_kv_multi_backend},
     {multi_backend_default, second_backend},
     {multi_backend, [
                      {first_backend, riak_kv_memory_backend, []},
                      {second_backend, riak_kv_memory_backend, []}
                     ]}
    ].

-endif.
