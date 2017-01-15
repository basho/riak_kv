%% -------------------------------------------------------------------
%%
%% sweeper_meck_helper: Meck wrapper for sweeper testing
%%
%% Copyright (c) 2017 Basho Technologies, Inc.  All Rights Reserved.
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
%% This module wraps all calls to meck in a separate gen_server
%% process so that we can call `meck:new(...)` in ct's
%% `init_per_suite` callback. `init_per_suite` runs in a separate
%% process, which on termination unloads all mocked modules created
%% using `meck:new(...)`.
%%
%% `meck:new(...)` calls are slow (500 mescs) so we want to do them
%% them once per suite rather than of once per testcase. Meck's
%% `no_link` option, i.e., doing a `meck:new(<module>, [no_link])` in
%% the `init_per_suite function` doesn't help, the process that
%% creates the mocked modules needs to live as long the mocked modules
%% are required.
%%
-module(sweeper_meck_helper).

-behaviour(gen_server).

%% API
-export([start/0,
         stop/0,
         aae_modules/3,
         backend_modules/3,
         num_partitions/1,
         advance_time/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {}).

%%%===================================================================
%%% API
%%%===================================================================

start() ->
    gen_server:start({local, ?SERVER}, ?MODULE, [], []).

stop() ->
    gen_server:call(?SERVER, stop).

aae_modules(AAEnabled, EstimatedKeys, LockResult) ->
    gen_server:call(?SERVER, {aae_modules, AAEnabled, EstimatedKeys, LockResult}).

backend_modules(Type, NumKeys, ObjSizeBytes) ->
    gen_server:call(?SERVER, {backend_modules, Type, NumKeys, ObjSizeBytes}).

num_partitions(N) ->
    gen_server:call(?SERVER, {num_partitions, N}).

advance_time({seconds, Secs}) ->
    gen_server:call(?SERVER, {advance_time, Secs}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    Now = os:timestamp(),
    meck_init_new(),
    meck_init_expect(),
    meck_set_sweeper_timestamp(Now),
    {ok, #state{}}.

handle_call(stop, _From, State) ->
    meck:unload(),
    {reply, _Reason = normal, _Reply = ok, State};

handle_call({aae_modules, AAEnabled, EstimatedKeys, LockResult}, _From, State) ->
    meck_new_aae_modules(AAEnabled, EstimatedKeys, LockResult),
    {reply, ok, State};

handle_call({backend_modules, Type, NumKeys, ObjSizeBytes}, _From, State) ->
    meck_new_backend1(Type, NumKeys, ObjSizeBytes),
    {reply, ok, State};

handle_call({num_partitions, N}, _From, State) ->
    application:set_env('MECK CTRL', 'PARTITIONS', N),
    {reply, ok, State};

handle_call({advance_time, DeltaSecs}, _From, State) ->
    {Mega, Secs, Msecs} = riak_kv_sweeper:timestamp(),
    meck:expect(riak_kv_sweeper, timestamp, fun() -> {Mega, Secs+DeltaSecs, Msecs} end),
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
get_my_indices(ring) ->
    {ok, Partitions} = application:get_env('MECK CTRL', 'PARTITIONS'),
    [N || N <- lists:seq(1, Partitions)].

meck_init_new() ->
    meck:new(riak_core_node_watcher),
    meck:new(riak_core_ring_manager),
    meck:new(riak_kv_entropy_manager),
    meck:new(riak_kv_index_hashtree),
    meck:new(riak_core_ring),
    meck:new(chronos),
    meck:new(riak_kv_vnode),
    meck:new(meck_new_backend, [non_strict]),
    meck:new(riak_kv_sweeper, [passthrough]),
    meck:new(riak_core_bucket).

meck_init_expect() ->
    Ring = ring,
    meck:expect(riak_kv_entropy_manager, enabled, fun() -> false end),
    meck:expect(chronos, start_timer, fun(_, _, _, _) -> ok end),
    meck:expect(riak_core_ring_manager, get_my_ring, fun() -> {ok, Ring} end),
    meck:expect(riak_core_node_watcher, services, fun(_Node) -> [riak_kv] end),
    meck:expect(riak_core_ring, my_indices, fun get_my_indices/1),
    meck:expect(riak_core_bucket, get_bucket, fun(BucketName) -> [{name, BucketName}] end).

meck_set_sweeper_timestamp(Now) ->
    meck:expect(riak_kv_sweeper, timestamp, fun() -> Now end).

meck_new_aae_modules(AAEnabled, EstimatedKeys, LockResult) ->
    meck:expect(riak_kv_entropy_manager, enabled, fun() -> AAEnabled end),
    meck:expect(riak_kv_index_hashtree, get_lock, fun(_, _) -> LockResult end),
    meck:expect(riak_kv_index_hashtree, estimate_keys, fun(_) -> {ok, EstimatedKeys} end),
    meck:expect(riak_kv_index_hashtree, release_lock, fun(_) -> ok end).


meck_new_backend1(Type, NumKeys, ObjSizeBytes) ->
    meck_new_fold_objects_function(Type, NumKeys, ObjSizeBytes),
    meck_new_riak_kv_vnode().

meck_new_fold_objects_function(async, NumKeys, ObjSizeBytes) ->
    meck:expect(meck_new_backend, fold_objects,
                fun(CompleteFoldReq, InitialAcc, _, _) ->
                        fold_objects(async, NumKeys, ObjSizeBytes, CompleteFoldReq, InitialAcc)
                end);

meck_new_fold_objects_function(sync, NumKeys, ObjSizeBytes) ->
    meck:expect(meck_new_backend, fold_objects,
                fun(CompleteFoldReq, InitialAcc, _, _) ->
                        fold_objects(sync, NumKeys, ObjSizeBytes, CompleteFoldReq, InitialAcc)
                end).

fold_objects(Behavior, NumKeys, ObjSizeBytes, CompleteFoldReq, InitialAcc) ->
    Keys = [integer_to_binary(N) || N <- lists:seq(1, NumKeys)],

    Work = fun() ->
                   Result =
                       lists:foldl(fun(NBin, Acc) ->
                                           InitialObj = riak_kv_sweeper_SUITE:riak_object_bin(NBin, NBin, ObjSizeBytes),
                                           CompleteFoldReq(NBin, NBin, InitialObj, Acc)
                                   end, InitialAcc, Keys),
                   Result
           end,
    case Behavior of
        sync ->
            {ok, Work()};
        async ->
            {async, Work}
    end.


meck_new_riak_kv_vnode() ->
    meck:expect(riak_kv_vnode, sweep,
                fun({Index, _Node}, ActiveParticipants, EstimatedKeys) ->
                        meck_riak_kv_vnode_sweep_worker(
                          ActiveParticipants, EstimatedKeys, Index)
                end),
    meck:expect(riak_kv_vnode, local_put, fun(_, _, _) -> ok end),
    meck:expect(riak_kv_vnode, local_reap, fun(_, _, _) -> ok end).

meck_riak_kv_vnode_sweep_worker(ActiveParticipants, EstimatedKeys, Index) ->
    spawn_link(fun() ->
                       meck_vnode_worker_func(ActiveParticipants, EstimatedKeys, Index)
               end).

meck_vnode_worker_func(ActiveParticipants, EstimatedKeys, Index) ->
    case
        riak_kv_sweeper_fold:do_sweep(
          ActiveParticipants, EstimatedKeys, _Sender = '?',
          _Opts = [], Index, meck_new_backend, _ModState = '?',
          _VnodeState = '?') of
        {async, {sweep, FoldFun, FinishFun}, _Sender, _VnodeState} ->
            try
                SA0 = FoldFun(),
                FinishFun(SA0)
            catch throw:{stop_sweep, PrematureAcc} ->
                    FinishFun(PrematureAcc),
                    PrematureAcc;
                  throw:PrematureAcc  ->
                    FinishFun(PrematureAcc),
                    PrematureAcc
            end;
        {reply, Acc, _VnodeStat} ->
            Acc
    end.
