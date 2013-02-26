%% -------------------------------------------------------------------
%%
%% Copyright (c) 2012 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc
%% This module implements a gen_server process that manages a set of hashtrees
%% (see {@link hashtree}) containing key/hash pairs for all data owned by a
%% given partition. Each riak_kv vnode spawns its own index_hashtree. These
%% hashtrees are used for active anti-entropy exchange between vnodes.

-module(riak_kv_index_hashtree).
-behaviour(gen_server).

-include_lib("riak_kv_vnode.hrl").

%% API
-export([start/3, start_link/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export([get_lock/2,
         compare/3,
         compare/4,
         exchange_bucket/4,
         exchange_segment/3,
         update/2,
         start_exchange_remote/4,
         delete/2,
         insert/4,
         insert/5,
         insert_object/3,
         async_insert_object/3,
         stop/1,
         destroy/1]).

-export([poke/1]).

-type index() :: non_neg_integer().
-type index_n() :: {index(), pos_integer()}.
-type orddict() :: orddict:orddict().
-type proplist() :: proplists:proplist().
-type riak_object_t2b() :: binary().
-type hashtree() :: hashtree:hashtree().

-record(state, {index,
                vnode_pid,
                built,
                lock :: undefined | reference(),
                path,
                build_time,
                trees}).

-type state() :: #state{}.

%% Time from build to expiration of tree, in millseconds
-define(DEFAULT_EXPIRE, 604800000). %% 1 week

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Spawn an index_hashtree process that manages the hashtrees (one
%%      for each `index_n') for the specified partition index.
-spec start(index(), nonempty_list(index_n()), pid()) -> {ok, pid()} | 
                                                         {error, term()}.
start(Index, IndexNs=[_|_], VNPid) ->
    gen_server:start(?MODULE, [Index, IndexNs, VNPid], []).

%% @doc Spawn an index_hashtree process that manages the hashtrees (one
%%      for each `index_n') for the specified partition index.
-spec start_link(index(), nonempty_list(index_n()), pid()) -> {ok, pid()} |
                                                              {error, term()}.
start_link(Index, IndexNs=[_|_], VNPid) ->
    gen_server:start_link(?MODULE, [Index, IndexNs, VNPid], []).

%% @doc Add a key/hash pair to the tree identified by the given tree id
%%      that is managed by the provided index_hashtree pid.
-spec insert(index_n(), binary(), binary(), pid()) -> ok.
insert(Id, Key, Hash, Tree) ->
    insert(Id, Key, Hash, Tree, []).

%% @doc A variant of {@link insert/4} that accepts a list of options.
%%      Valid options:
%%       ``if_missing'' :: Only insert the key/hash pair if the key does not
%%                         already exist in the hashtree.
-spec insert(index_n(), binary(), binary(), pid(), proplist()) -> ok.
insert(_Id, _Key, _Hash, undefined, _Options) ->
    ok;
insert(Id, Key, Hash, Tree, Options) ->
    catch gen_server:call(Tree, {insert, Id, Key, Hash, Options}, infinity).

%% @doc Add an encoded (binary) riak_object associated with a given
%%      bucket/key to the appropriate hashtree managed by the provided
%%      index_hashtree pid. The hash value is generated using
%%      {@link hash_object/2}. Any encoding version is supported. The encoding
%%      will be changed to the appropriate version before hashing the object.
-spec insert_object({binary(), binary()}, riak_object_t2b(), pid()) -> ok.
insert_object(_BKey, _RObj, undefined) ->
    ok;
insert_object(BKey, RObj, Tree) ->
    catch gen_server:call(Tree, {insert_object, BKey, RObj}, infinity).

%% @doc Asynchronous version of {@link insert_object/3}.
async_insert_object(BKey, RObj, Tree) ->
    gen_server:cast(Tree, {insert_object, BKey, RObj}).

%% @doc Remove the key/hash pair associated with a given bucket/key from the
%%      appropriate hashtree managed by the provided index_hashtree pid.
-spec delete({binary(), binary()}, pid()) -> ok.
delete(_BKey, undefined) ->
    ok;
delete(BKey, Tree) ->
    catch gen_server:call(Tree, {delete, BKey}, infinity).

%% @doc Called by the entropy manager to finish the process used to acquire
%%      remote vnode locks when starting an exchange. For more details,
%%      see {@link riak_kv_entropy_manager:start_exchange_remote/3}
-spec start_exchange_remote(pid(), term(), index_n(), pid()) -> ok.
start_exchange_remote(FsmPid, From, IndexN, Tree) ->
    gen_server:cast(Tree, {start_exchange_remote, FsmPid, From, IndexN}).

%% @doc Update all hashtrees managed by the provided index_hashtree pid.
-spec update(index_n(), pid()) -> ok | not_responsible.
update(Id, Tree) ->
    gen_server:call(Tree, {update_tree, Id}, infinity).

%% @doc Return a hash bucket from the tree identified by the given tree id
%%      that is managed by the provided index_hashtree.
-spec exchange_bucket(index_n(), integer(), integer(), pid()) -> orddict().
exchange_bucket(Id, Level, Bucket, Tree) ->
    gen_server:call(Tree, {exchange_bucket, Id, Level, Bucket}, infinity).

%% @doc Return a segment from the tree identified by the given tree id that
%%      is managed by the provided index_hashtree.
-spec exchange_segment(index_n(), integer(), pid()) -> orddict().
exchange_segment(Id, Segment, Tree) ->
    gen_server:call(Tree, {exchange_segment, Id, Segment}, infinity).

%% @doc Start the key exchange between a given tree managed by the
%%      provided index_hashtree and a remote tree accessed through the
%%      provided remote function.
-spec compare(index_n(), hashtree:remote_fun(), pid()) -> [hashtree:keydiff()].
compare(Id, Remote, Tree) ->
    compare(Id, Remote, undefined, Tree).

%% @doc A variant of {@link compare/3} that takes a key difference accumulator
%%      function as an additional parameter.
-spec compare(index_n(), hashtree:remote_fun(),
              undefined | hashtree:acc_fun(T), pid()) -> T.
compare(Id, Remote, AccFun, Tree) ->
    gen_server:call(Tree, {compare, Id, Remote, AccFun}, infinity).

%% @doc Acquire the lock for the specified index_hashtree if not already
%%      locked, and associate the lock with the calling process.
-spec get_lock(pid(), any()) -> ok | not_built | already_locked.
get_lock(Tree, Type) ->
    get_lock(Tree, Type, self()).

%% @doc Acquire the lock for the specified index_hashtree if not already
%%      locked, and associate the lock with the provided pid.
-spec get_lock(pid(), any(), pid()) -> ok | not_built | already_locked.
get_lock(Tree, Type, Pid) ->
    gen_server:call(Tree, {get_lock, Type, Pid}, infinity).

%% @doc Poke the specified index_hashtree to ensure the tree is
%%      built/rebuilt as needed. This is periodically called by the
%%      {@link riak_kv_entropy_manager}.
-spec poke(pid()) -> ok.
poke(Tree) ->
    gen_server:cast(Tree, poke).

%% @doc Terminate the specified index_hashtree.
stop(Tree) ->
    gen_server:cast(Tree, stop).

%% @doc Destroy the specified index_hashtree, which will destroy all
%%      associated hashtrees and terminate.
-spec destroy(pid()) -> ok.
destroy(Tree) ->
    gen_server:call(Tree, destroy, infinity).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Index, IndexNs, VNPid]) ->
    case determine_data_root() of
        undefined ->
            case riak_kv_entropy_manager:enabled() of
                true ->
                    lager:warning("Neither riak_kv/anti_entropy_data_dir or "
                                  "riak_core/platform_data_dir are defined. "
                                  "Disabling active anti-entropy."),
                    riak_kv_entropy_manager:disable();
                false ->
                    ok
            end,
            ignore;
        Root ->
            Path = filename:join(Root, integer_to_list(Index)),
            monitor(process, VNPid),

            State = #state{index=Index,
                           vnode_pid=VNPid,
                           trees=orddict:new(),
                           built=false,
                           path=Path},
            State2 = init_trees(IndexNs, State),
            {ok, State2}
    end.

handle_call({new_tree, Id}, _From, State) ->
    State2 = do_new_tree(Id, State),
    {reply, ok, State2};

handle_call({get_lock, Type, Pid}, _From, State) ->
    {Reply, State2} = do_get_lock(Type, Pid, State),
    {reply, Reply, State2};

handle_call({insert, Id, Key, Hash, Options}, _From, State) ->
    State2 = do_insert(Id, Key, Hash, Options, State),
    {reply, ok, State2};
handle_call({insert_object, BKey, RObj}, _From, State) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    IndexN = riak_kv_util:get_index_n(BKey, Ring),
    State2 = do_insert(IndexN, term_to_binary(BKey), hash_object(BKey, RObj), [], State),
    {reply, ok, State2};
handle_call({delete, BKey}, _From, State) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    IndexN = riak_kv_util:get_index_n(BKey, Ring),
    State2 = do_delete(IndexN, term_to_binary(BKey), State),
    {reply, ok, State2};

handle_call({update_tree, Id}, From, State) ->
    lager:debug("Updating tree: (vnode)=~p (preflist)=~p", [State#state.index, Id]),
    apply_tree(Id,
               fun(Tree) ->
                       {SnapTree, Tree2} = hashtree:update_snapshot(Tree),
                       spawn_link(fun() ->
                                          hashtree:update_perform(SnapTree),
                                          gen_server:reply(From, ok)
                                  end),
                       {noreply, Tree2}
               end,
               State);

handle_call({exchange_bucket, Id, Level, Bucket}, _From, State) ->
    apply_tree(Id,
               fun(Tree) ->
                       Result = hashtree:get_bucket(Level, Bucket, Tree),
                       {Result, Tree}
               end,
               State);

handle_call({exchange_segment, Id, Segment}, _From, State) ->
    apply_tree(Id,
               fun(Tree) ->
                       [{_, Result}] = hashtree:key_hashes(Tree, Segment),
                       {Result, Tree}
               end,
               State);

handle_call({compare, Id, Remote, AccFun}, From, State) ->
    do_compare(Id, Remote, AccFun, From, State),
    {noreply, State};

handle_call(destroy, _From, State) ->
    State2 = destroy_trees(State),
    {stop, normal, ok, State2};

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(poke, State) ->
    State2 = do_poke(State),
    {noreply, State2};

handle_cast(stop, State) ->
    close_trees(State),
    {stop, normal, State};

handle_cast({insert_object, BKey, RObj}, State) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    IndexN = riak_kv_util:get_index_n(BKey, Ring),
    State2 = do_insert(IndexN, term_to_binary(BKey), hash_object(BKey, RObj), [], State),
    {noreply, State2};

handle_cast(build_failed, State) ->
    riak_kv_entropy_manager:requeue_poke(State#state.index),
    State2 = State#state{built=false},
    {noreply, State2};
handle_cast(build_finished, State) ->
    State2 = do_build_finished(State),
    {noreply, State2};

handle_cast({start_exchange_remote, FsmPid, From, _IndexN}, State) ->
    %% Concurrency lock already acquired, try to acquire tree lock.
    case do_get_lock(remote_fsm, FsmPid, State) of
        {ok, State2} ->
            gen_server:reply(From, {remote_exchange, self()}),
            {noreply, State2};
        {Reply, State2} ->
            gen_server:reply(From, {remote_exchange, Reply}),
            {noreply, State2}
    end;

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', _, _, Pid, _}, State) when Pid == State#state.vnode_pid ->
    %% vnode has terminated, exit as well
    close_trees(State),
    {stop, normal, State};
handle_info({'DOWN', Ref, _, _, _}, State) ->
    State2 = maybe_release_lock(Ref, State),
    {noreply, State2};
   
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

determine_data_root() ->
    case application:get_env(riak_kv, anti_entropy_data_dir) of
        {ok, EntropyRoot} ->
            EntropyRoot;
        undefined ->
            case application:get_env(riak_core, platform_data_dir) of
                {ok, PlatformRoot} ->
                    Root = filename:join(PlatformRoot, "anti_entropy"),
                    lager:warning("Config riak_kv/anti_entropy_data_dir is "
                                  "missing. Defaulting to: ~p", [Root]),
                    application:set_env(riak_kv, anti_entropy_data_dir, Root),
                    Root;
                undefined ->
                    undefined
            end
    end.

-spec init_trees([index_n()], state()) -> state().
init_trees(IndexNs, State) ->
    State2 = lists:foldl(fun(Id, StateAcc) ->
                                 do_new_tree(Id, StateAcc)
                         end, State, IndexNs),
    State2#state{built=false}.

-spec load_built(state()) -> boolean().
load_built(#state{trees=Trees}) ->
    {_,Tree0} = hd(Trees),
    case hashtree:read_meta(<<"built">>, Tree0) of
        {ok, <<1>>} ->
            true;
        _ ->
            false
    end.

%% Generate a hash value for a binary-encoded `riak_object'
-spec hash_object({riak_object:bucket(), riak_object:key()}, riak_object_t2b()) -> binary().
hash_object({Bucket, Key}, RObjBin) ->
    %% Normalize the `riak_object' vector clock before hashing
    RObj = riak_object:from_binary(Bucket, Key, RObjBin),
    Hash = riak_object:hash(RObj),
    term_to_binary(Hash).

%% Fold over a given vnode's data, inserting each object into the appropriate
%% hashtree. Use the `if_missing' option to only insert the key/hash pair if
%% the key does not already exist in the tree. This allows real-time updates
%% to the hashtree to occur concurrently with the fold. For example, if an
%% incoming write triggers a real-time insert of a key/hash pair for an object
%% before the fold reaches the now out-of-date version of the object, the old
%% key/hash pair will be ignored.
-spec fold_keys(index(), pid()) -> ok.
fold_keys(Partition, Tree) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Req = ?FOLD_REQ{foldfun=fun(BKey={Bucket,Key}, RObj, _) ->
                                    IndexN = riak_kv_util:get_index_n({Bucket, Key}, Ring),
                                    insert(IndexN, term_to_binary(BKey), hash_object(BKey, RObj),
                                           Tree, [if_missing]),
                                    ok
                            end,
                    acc0=ok},
    riak_core_vnode_master:sync_command({Partition, node()},
                                        Req,
                                        riak_kv_vnode_master, infinity),
    ok.

%% Generate a new {@link hashtree} for the specified `index_n'. If this is
%% the first hashtree created by this index_hashtree, then open/create a new
%% on-disk store at `segment_path'. Otherwise, re-use the store from the first
%% tree. In other words, all hashtrees for a given index_hashtree are stored in
%% the same on-disk store.
-spec do_new_tree(index_n(), state()) -> state().
do_new_tree(Id, State=#state{trees=Trees, path=Path}) ->
    Index = State#state.index,
    IdBin = tree_id(Id),
    NewTree = case Trees of
                  [] ->
                      hashtree:new({Index,IdBin}, [{segment_path, Path}]);
                  [{_,Other}|_] ->
                      hashtree:new({Index,IdBin}, Other)
              end,
    Trees2 = orddict:store(Id, NewTree, Trees),
    State#state{trees=Trees2}.

-spec do_get_lock(any(), pid(), state()) -> {not_built | ok | already_locked, state()}.
do_get_lock(_, _, State) when State#state.built /= true ->
    lager:debug("Not built: ~p :: ~p", [State#state.index, State#state.built]),
    {not_built, State};
do_get_lock(_Type, Pid, State=#state{lock=undefined}) ->
    Ref = monitor(process, Pid),
    State2 = State#state{lock=Ref},
    {ok, State2};
do_get_lock(_, _, State) ->
    lager:debug("Already locked: ~p", [State#state.index]),
    {already_locked, State}.

-spec maybe_release_lock(reference(), state()) -> state().
maybe_release_lock(Ref, State) ->
    case State#state.lock of
        Ref ->
            State#state{lock=undefined};
        _ ->
            State
    end.

%% Utility function for passing a specific hashtree into a provided function
%% and storing the possibly-modified hashtree back in the index_hashtree state.
-spec apply_tree(index_n(),
                 fun((hashtree()) -> {'noreply' | any(), hashtree()}),
                 state())
                -> {'reply', 'not_responsible', state()} |
                   {'reply', any(), state()} |
                   {'noreply', state()}.
apply_tree(Id, Fun, State=#state{trees=Trees}) ->
    case orddict:find(Id, Trees) of
        error ->
            {reply, not_responsible, State};
        {ok, Tree} ->
            {Result, Tree2} = Fun(Tree),
            Trees2 = orddict:store(Id, Tree2, Trees),
            State2 = State#state{trees=Trees2},
            case Result of
                noreply ->
                    {noreply, State2};
                _ ->
                    {reply, Result, State2}
            end
    end.

-spec do_build_finished(state()) -> state().
do_build_finished(State=#state{index=Index, built=_Pid}) ->
    lager:debug("Finished build (b): ~p", [Index]),
    {_,Tree0} = hd(State#state.trees),
    BuildTime = get_build_time(Tree0),
    hashtree:write_meta(<<"built">>, <<1>>, Tree0),
    hashtree:write_meta(<<"build_time">>, term_to_binary(BuildTime), Tree0),
    riak_kv_entropy_info:tree_built(Index, BuildTime),
    State#state{built=true, build_time=BuildTime}.

%% Determine the build time for all trees associated with this
%% index. The build time is stored as metadata in the on-disk file. If
%% the tree was rehashed after a restart, this function should return
%% the original build time. If this is a newly created tree (or if the
%% on-disk time is invalid), the function returns the current time.
-spec get_build_time(hashtree()) -> calendar:t_now().
get_build_time(Tree) ->
    Time = case hashtree:read_meta(<<"build_time">>, Tree) of
               {ok, TimeBin} ->
                   binary_to_term(TimeBin);
               _ ->
                   undefined
           end,
    case valid_time(Time) of
        true ->
            Time;
        false ->
            os:timestamp()
    end.

valid_time({X,Y,Z}) when is_integer(X) and is_integer(Y) and is_integer(Z) ->
    true;
valid_time(_) ->
    false.

-spec do_insert(index_n(), binary(), binary(), proplist(), state()) -> state().
do_insert(Id, Key, Hash, Opts, State=#state{trees=Trees}) ->
    case orddict:find(Id, Trees) of
        {ok, Tree} ->
            Tree2 = hashtree:insert(Key, Hash, Tree, Opts),
            Trees2 = orddict:store(Id, Tree2, Trees),
            State#state{trees=Trees2};
        _ ->
            State2 = handle_unexpected_key(Id, Key, State),
            State2
    end.

-spec do_delete(index_n(), binary(), state()) -> state().
do_delete(Id, Key, State=#state{trees=Trees}) ->
    case orddict:find(Id, Trees) of
        {ok, Tree} ->
            Tree2 = hashtree:delete(Key, Tree),
            Trees2 = orddict:store(Id, Tree2, Trees),
            State#state{trees=Trees2};
        _ ->
            State2 = handle_unexpected_key(Id, Key, State),
            State2
    end.

-spec handle_unexpected_key(index_n(), binary(), state()) -> state().
handle_unexpected_key(Id, Key, State=#state{index=Partition}) ->
    RP = riak_kv_util:responsible_preflists(Partition),
    case lists:member(Id, RP) of
        false ->
            %% The encountered object does not belong to any preflists thata
            %% this partition is associated with. Under normal Riak operation,
            %% this should only happen when the `n_val' for an object is
            %% reduced. For example, write an object with N=3, then change N to
            %% 2. There will be an extra replica of the object that is no
            %% longer needed. We should probably just delete these objects, but
            %% to be safe rather than sorry, the first version of AAE simply
            %% ignores these objects.
            %%
            %% TODO: We should probably remove these warnings before final
            %%       release, as reducing N will result in a ton of log/console
            %%       spam.
            %% lager:warning("Object ~p encountered during fold over partition "
            %%               "~p, but key does not hash to an index handled by "
            %%               "this partition", [Key, Partition]),
            State;
        true ->
            %% The encountered object belongs to a preflist that is currently
            %% associated with this partition, but was not when the
            %% index_hashtree process was created. This occurs when increasing
            %% the `n_val' for an object. For example, write an object with N=3
            %% and it will map to the index/n preflist `{<index>, 3}'. Increase
            %% N to 4, and the object now maps to preflist '{<index>, 4}' which
            %% may not have an existing hashtree if there were previously no
            %% objects with N=4.
            lager:info("Partition/tree ~p/~p does not exist to hold object ~p",
                       [Partition, Id, Key]),
            case State#state.built of
                true ->
                    %% If the tree is already built, clear the tree to trigger
                    %% a rebuild that will re-distribute objects into the
                    %% proper hashtrees based on current N values.
                    lager:info("Clearing tree to trigger future rebuild"),
                    clear_tree(State);
                _ ->
                    %% Initialize a new index_n tree to prevent future errors.
                    %% The various hashtrees will likely be inconsistent, with
                    %% some trees containing key/hash pairs that should be in
                    %% other trees (eg. due to a change in N value). This will
                    %% be resolved whenever trees are eventually rebuilt, either
                    %% after normal expiration or after a future unexpected value
                    %% triggers the alternate case clause above.
                    State2 = do_new_tree(Id, State),
                    State2
            end
    end.

-spec tree_id(index_n()) -> hashtree:tree_id_bin().
tree_id({Index, N}) ->
    %% hashtree is hardcoded for 22-byte (176-bit) tree id
    <<Index:160/integer,N:16/integer>>;
tree_id(_) ->
    erlang:error(badarg).

-spec do_compare(index_n(), hashtree:remote_fun(), hashtree:acc_fun(any()),
                 term(), state()) -> ok.
do_compare(Id, Remote, AccFun, From, State) ->
    case orddict:find(Id, State#state.trees) of
        error ->
            %% This case shouldn't happen, but might as well safely handle it.
            lager:warning("Tried to compare nonexistent tree "
                          "(vnode)=~p (preflist)=~p", [State#state.index, Id]),
            gen_server:reply(From, []);
        {ok, Tree} ->
            spawn_link(fun() ->
                               Result = case AccFun of
                                            undefined ->
                                                hashtree:compare(Tree, Remote);
                                            _ ->
                                                hashtree:compare(Tree, Remote, AccFun)
                                        end,
                               gen_server:reply(From, Result)
                       end)
    end,
    ok.

-spec do_poke(state()) -> state().
do_poke(State) ->
    State1 = maybe_clear(State),
    State2 = maybe_build(State1),
    State2.

%% If past expiration, clear all hashtrees.
-spec maybe_clear(state()) -> state().
maybe_clear(State=#state{lock=undefined, built=true}) ->
    Diff = timer:now_diff(os:timestamp(), State#state.build_time),
    Expire = app_helper:get_env(riak_kv,
                                anti_entropy_expire,
                                ?DEFAULT_EXPIRE),
    %% Need to convert from millsec to microsec
    case Diff > (Expire * 1000) of
        true ->
            clear_tree(State);
        false ->
            State
    end;
maybe_clear(State) ->
    State.

-spec clear_tree(state()) -> state().
clear_tree(State=#state{index=Index}) ->
    lager:debug("Clearing tree ~p", [State#state.index]),
    State2 = destroy_trees(State),
    IndexNs = riak_kv_util:responsible_preflists(Index),
    State3 = init_trees(IndexNs, State2#state{trees=orddict:new()}),
    State3#state{built=false}.

destroy_trees(State) ->
    State2 = close_trees(State),
    {_,Tree0} = hd(State2#state.trees),
    hashtree:destroy(Tree0),
    State2.

-spec maybe_build(state()) -> state().
maybe_build(State=#state{built=false}) ->
    Self = self(),
    Pid = spawn_link(fun() ->
                             build_or_rehash(Self, State)
                     end),
    State#state{built=Pid};
maybe_build(State) ->
    %% Already built or build in progress
    State.

%% If the on-disk data is not marked as previously being built, then trigger
%% a fold/build. Otherwise, trigger a rehash to ensure the hashtrees match the
%% current on-disk segments.
-spec build_or_rehash(pid(), state()) -> ok.
build_or_rehash(Self, State=#state{index=Index, trees=Trees}) ->
    Type = case load_built(State) of
               false -> build;
               true  -> rehash
           end,
    Lock = riak_kv_entropy_manager:get_lock(Type),
    case {Lock, Type} of
        {ok, build} ->
            lager:debug("Starting build: ~p", [Index]),
            fold_keys(Index, Self),
            lager:debug("Finished build (a): ~p", [Index]), 
            gen_server:cast(Self, build_finished);
        {ok, rehash} ->
            lager:debug("Starting rehash: ~p", [Index]),
            [hashtree:rehash_tree(T) || {_,T} <- Trees],
            lager:debug("Finished rehash (a): ~p", [Index]),
            gen_server:cast(Self, build_finished);
        {_Error, _} ->
            gen_server:cast(Self, build_failed)
    end.

close_trees(State=#state{trees=Trees}) ->
    Trees2 = [{IdxN, hashtree:close(Tree)} || {IdxN, Tree} <- Trees],
    State#state{trees=Trees2}.
