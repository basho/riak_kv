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
api_version() ->
    {?API_VERSION, ?CAPABILITIES}.

%% @spec start(Partition :: integer(), Config :: integer()) ->
%%                        {ok, state()} | {{error, Reason :: term()}, state()}
%% @doc Start the backends
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

%% @spec stop(state()) -> ok | {error, Reason :: term()}
%% @doc Stop the backends
stop(State) -> 
    Backends = State#state.backends,
    Results = [Module:stop(SubState) || {_, Module, SubState} <- Backends],
    ErrorResults = [X || X <- Results, X /= ok],
    case ErrorResults of
        [] -> ok;
        _ -> {error, ErrorResults}
    end.

%% get(riak_object:bucket(), riak_object:key(), state()) ->
%%   {ok, Val :: binary()} | {error, Reason :: term()}
get(Bucket, Key, State) ->
    {_Name, Module, SubState} = get_backend(Bucket, State),
    Module:get(SubState, {Bucket, Key}).

%% put(riak_object:bucket(), riak_object:key(), binary(), state()) ->
%%   ok | {error, Reason :: term()}
put(Bucket, Key, Value, State) -> 
    {_Name, Module, SubState} = get_backend(Bucket, State),
    Module:put(SubState, {Bucket, Key}, Value).

%% delete(riak_object:bucket(), riak_object:key(), state()) ->
%%   ok | {error, Reason :: term()}
delete(Bucket, Key, State) -> 
    {_Name, Module, SubState} = get_backend(Bucket, State),
    Module:delete(SubState, {Bucket, Key}).

%% @doc Fold over all the buckets. 
fold_buckets(FoldBucketsFun, Acc, Opts, #state{backends=Backends}) ->
    FoldFun = fun({_, Module, SubState}, Acc1) ->
                      Module:fold_buckets(FoldBucketsFun, Acc1, Opts, SubState)
              end,
    lists:foldl(FoldFun, Acc, Backends).

%% @doc Fold over all the keys for a bucket or all keys for
%% all buckets if no bucket is specified in Opts.
fold_keys(FoldKeysFun, Acc, Opts, #state{backends=Backends}) ->
    FoldFun = fun({_, Module, SubState}, Acc1) ->
                      Module:fold_keys(FoldKeysFun, Acc1, Opts, SubState)
              end,
    lists:foldl(FoldFun, Acc, Backends).

%% @doc Fold over all the objects for a bucket or all objects for
%% all buckets if no bucket is specified in Opts.
fold_objects(FoldObjectsFun, Acc, Opts, #state{backends=Backends}) ->
    FoldFun = fun({_, Module, SubState}, Acc1) ->
                      Module:fold_objects(FoldObjectsFun, Acc1, Opts, SubState)
              end,
    lists:foldl(FoldFun, Acc, Backends).

%% @doc Delete all objects from the different backends
drop(#state{backends=Backends}) ->
    Fun = fun({_, Module, SubState}) ->
        Module:drop(SubState)
    end,
    [Fun(Backend) || Backend <- Backends],
    ok.

%% @doc Returns true if the backend contains any 
%% non-tombstone values; otherwise returns false.
is_empty(#state{backends=Backends}) ->
    Fun = fun({_, Module, SubState}) ->
        Module:is_empty(SubState)
    end,
    lists:all(Fun, Backends).

%% @doc Register an asynchronous callback
callback(Ref, Msg, #state{backends=Backends}) ->
    %% Pass the callback on to all submodules - their responsbility to
    %% filter out if they need it.
    [Mod:callback(Ref, Msg, ModState) || {_N, Mod, ModState} <- Backends],
    ok.

%% @doc Get the status information for this backend
status(#state{backends=Backends}) ->
    %% @TODO Reexamine how this is handled
    [{N, Mod:status(ModState)} || {N, Mod, ModState} <- Backends].


%% ===================================================================
%% Internal functions
%% ===================================================================

% Given a Bucket name and the State, return the
% backend definition. (ie: {Name, Module, SubState})
get_backend(Bucket, State) ->
    % Get the name of the backend...
    DefaultBackend = State#state.default_backend,
    BucketProps = riak_core_bucket:get_bucket(Bucket),
    BackendName = proplists:get_value(backend, BucketProps, DefaultBackend),

    % Ensure that a backend by that name exists...
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

% @private
multi_backend_test_() ->
    {foreach,
        fun() ->
            crypto:start(),

            % start the ring manager
            {ok, P1} = riak_core_ring_events:start_link(),
            {ok, P2} = riak_core_ring_manager:start_link(test),
            application:load(riak_core),
            application:set_env(riak_core, default_bucket_props, []),
            [P1, P2]
        end,
        fun([P1, P2]) ->
            crypto:stop(),
            unlink(P1),
            unlink(P2),
            [begin
                 catch exit(Proc, kill),
                 wait_until_dead(Proc)
             end || Proc <- [P1, P2, whereis(riak_core_ring_events),
                             whereis(riak_core_ring_manager)]]
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

                        % Start the backend...    
                        {ok, State} = start(42, sample_config()),

                        % Check our buckets...
                        {first_backend, riak_kv_gb_trees_backend, _} = get_backend(<<"b1">>, State),
                        {second_backend, riak_kv_ets_backend, _} = get_backend(<<"b2">>, State),

                        % Check the default...
                        {second_backend, riak_kv_ets_backend, _} = get_backend(<<"b3">>, State),
                            
                        % Stop the backend
                        stop(State),
                        ok
                    end
                }
            end
        ]
    }.

-ifdef(EQC).
%% @private

eqc_test() ->
    % Start the ring manager...
    crypto:start(),
    {ok, P1} = riak_core_ring_events:start_link(),
    {ok, P2} = riak_core_ring_manager:start_link(test),

    % Set some buckets...
    application:load(riak_core), % make sure default_bucket_props is set
    riak_core_bucket:set_bucket(<<"b1">>, [{backend, first_backend}]),
    riak_core_bucket:set_bucket(<<"b2">>, [{backend, second_backend}]),

    % Run the standard backend test... sample is volatile as it uses
    % gb_trees and ets
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

    %% And for dets
    application:stop(dets),
    ?assertCmd("rm -rf test/dets-backend"),
    DetsConfig = [{riak_kv_dets_backend_root, "test/dets-backend"}],

    %% and for fs
   ?assertCmd("rm -rf test/fs-backend"),
   FsConfig = [{riak_kv_fs_backend_root, "test/fs-backend"}],


    %% Start up multi backend
    Config = [{storage_backend, riak_kv_multi_backend},
              {multi_backend_default, ets},
              {multi_backend, 
               [{bitcask, riak_kv_bitcask_backend, []},
                {cache, riak_kv_cache_backend, []},
                {dets, riak_kv_dets_backend, DetsConfig},
                {ets, riak_kv_ets_backend, []},
                {fs, riak_kv_fs_backend, FsConfig},
                {gb_trees, riak_kv_gb_trees_backend, []}]}],
    {ok, State} = start(0, Config),
    callback(State, make_ref(), ignore_me),
    stop(State),
    application:unload(bitcask).
    
           
bad_config_test() ->     
    % {invalid_config_setting, multi_backend_default, backend_not_found}
    ?assertThrow({riak_kv_multi_backend,
                  {invalid_config_setting,multi_backend,list_expected}},
                 start(0, [])).


sample_config() ->
    [
        {storage_backend, riak_kv_multi_backend},
        {multi_backend_default, second_backend},
        {multi_backend, [
            {first_backend, riak_kv_gb_trees_backend, []},
            {second_backend, riak_kv_ets_backend, []}
        ]}
    ].

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
