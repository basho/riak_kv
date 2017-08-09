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

-module(riak_kv_entropy_manager).
-behaviour(gen_server).

%% API
-export([start_link/0,
         manual_exchange/1,
         enabled/0,
         enable/0,
         disable/0,
         set_mode/1,
         set_debug/1,
         cancel_exchange/1,
         cancel_exchanges/0,
         get_lock/1,
         get_lock/2,
         release_lock/1,
         requeue_poke/1,
         start_exchange_remote/3,
         start_exchange_remote/4,
         exchange_status/4,
         expire_trees/0,
         clear_trees/0,
         get_version/0,
         get_partition_version/1,
         get_pending_version/0,
         get_upgraded/0,
         get_trees_version/0]).
-export([all_pairwise_exchanges/2]).
-export([throttle/0,
         get_aae_throttle/0,
         set_aae_throttle/1,
         is_aae_throttle_enabled/0,
         disable_aae_throttle/0,
         enable_aae_throttle/0,
         get_aae_throttle_limits/0,
         set_aae_throttle_limits/1,
         get_max_local_vnodeq/0]).
-export([multicall/5]).                         % for meck twiddle-testing

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-ifdef(TEST).
-export([query_and_set_aae_throttle/1]).        % for eunit twiddle-testing
-export([make_state/0, get_last_throttle/1]).   % for eunit twiddle-testing
-include_lib("eunit/include/eunit.hrl").
-endif.

-type index() :: non_neg_integer().
-type index_n() :: {index(), pos_integer()}.
-type vnode() :: {index(), node()}.
-type exchange() :: {index(), index(), index_n()}.
-type riak_core_ring() :: riak_core_ring:riak_core_ring().
-type version() :: legacy | non_neg_integer().
-type orddict(Key, Val) :: [{Key, Val}].

-record(state, {mode           = automatic :: automatic | manual,
                trees          = []        :: orddict(index(), pid()),
                tree_queue     = []        :: orddict(index(), pid()),
                trees_version  = []        :: orddict(index(), version()),
                locks          = []        :: [{pid(), reference()}],
                build_tokens   = 0         :: non_neg_integer(),
                exchange_queue = []        :: [exchange()],
                exchanges      = []        :: [{index(), reference(), pid()}],
                vnode_status_pid = undefined :: 'undefined' | pid(),
                last_throttle  = undefined :: 'undefined' | non_neg_integer(),
                version        = legacy :: version(),
                pending_version = legacy :: version()
               }).

-type state() :: #state{}.

-define(DEFAULT_CONCURRENCY, 2).
-define(DEFAULT_BUILD_LIMIT, {1, 3600000}). %% Once per hour
-define(KV_ENTROPY_LOCK_TIMEOUT, app_helper:get_env(riak_kv, anti_entropy_lock_timeout, 10000)).
-define(AAE_THROTTLE_KEY, aae_throttle).
-define(DEFAULT_AAE_THROTTLE_LIMITS,
        [{-1,0}, {200,10}, {500,50}, {750,250}, {900,1000}, {1100,5000}]).
-define(ETS, entropy_manager_ets).

%%%===================================================================
%%% API
%%%===================================================================

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Acquire an exchange concurrency lock if available, and associate
%%      the lock with the calling process.
-spec get_lock(any()) -> ok | max_concurrency.
get_lock(Type) ->
    get_lock(Type, self()).

%% @doc Acquire an exchange concurrency lock if available, and associate
%%      the lock with the provided pid.
-spec get_lock(any(), pid()) -> ok | max_concurrency.
get_lock(Type, Pid) ->
    gen_server:call(?MODULE, {get_lock, Type, Pid}, ?KV_ENTROPY_LOCK_TIMEOUT).

-spec release_lock(pid()) -> ok.
release_lock(Pid) ->
    gen_server:cast(?MODULE, {release_lock, Pid}).

%% @doc Acquire the necessary locks for an entropy exchange with the specified
%%      remote vnode. The request is sent to the remote entropy manager which
%%      will try to acquire a concurrency lock. If successful, the request is
%%      then forwarded to the relevant index_hashtree to acquire a tree lock.
%%      If both locks are acquired, the pid of the remote index_hashtree is
%%      returned. This function assumes a legacy version of the hashtree
%%      and is left over for support for mixed clusters with pre-2.2 nodes
-spec start_exchange_remote({index(), node()}, index_n(), pid())
                           -> {remote_exchange, pid()} |
                              {remote_exchange, anti_entropy_disabled} |
                              {remote_exchange, max_concurrency} |
                              {remote_exchange, not_built} |
                              {remote_exchange, already_locked} |
                              {remote_exchange, bad_version}.
start_exchange_remote(VNode, IndexN, FsmPid) ->
    start_exchange_remote(VNode, IndexN, FsmPid, legacy).

-spec start_exchange_remote({index(), node()}, index_n(), pid(), version())
                           -> {remote_exchange, pid()} |
                              {remote_exchange, anti_entropy_disabled} |
                              {remote_exchange, max_concurrency} |
                              {remote_exchange, not_built} |
                              {remote_exchange, already_locked} |
                              {remote_exchange, bad_version}.
start_exchange_remote(_VNode={Index, Node}, IndexN, FsmPid, legacy) ->
    gen_server:call({?MODULE, Node},
                    {start_exchange_remote, FsmPid, Index, IndexN},
                    infinity);                        
start_exchange_remote(_VNode={Index, Node}, IndexN, FsmPid, Version) ->
    gen_server:call({?MODULE, Node},
                    {start_exchange_remote, FsmPid, Index, IndexN, Version},
                    infinity).

%% @doc Used by {@link riak_kv_index_hashtree} to requeue a poke on
%%      build failure.
-spec requeue_poke(index()) -> ok.
requeue_poke(Index) ->
    gen_server:cast(?MODULE, {requeue_poke, Index}).

%% @doc Used by {@link riak_kv_exchange_fsm} to inform the entropy
%%      manager about the status of an exchange (ie. completed without
%%      issue, failed, etc)
-spec exchange_status(vnode(), vnode(), index_n(), any()) -> ok.
exchange_status(LocalVN, RemoteVN, IndexN, Reply) ->
    gen_server:cast(?MODULE,
                    {exchange_status,
                     self(), LocalVN, RemoteVN, IndexN, Reply}).

%% @doc Returns true of AAE is enabled, false otherwise.
-spec enabled() -> boolean().
enabled() ->
    {Enabled, _} = settings(),
    Enabled.

%% @doc Set AAE to either `automatic' or `manual' mode. In automatic mode, the
%%      entropy manager triggers all necessary hashtree exchanges. In manual
%%      mode, exchanges must be triggered using {@link manual_exchange/1}.
%%      Regardless of exchange mode, the entropy manager will always ensure
%%      local hashtrees are built and rebuilt as necessary.
-spec set_mode(automatic | manual) -> ok.
set_mode(Mode=automatic) ->
    ok = gen_server:call(?MODULE, {set_mode, Mode}, infinity);
set_mode(Mode=manual) ->
    ok = gen_server:call(?MODULE, {set_mode, Mode}, infinity).

%% @doc Toggle debug mode, which prints verbose AAE information to the console.
-spec set_debug(boolean()) -> ok.
set_debug(Enabled) ->
    Modules = [riak_kv_index_hashtree,
               riak_kv_entropy_manager,
               riak_kv_exchange_fsm],
    case Enabled of
        true ->
            [lager:trace_console([{module, Mod}]) || Mod <- Modules];
        false ->
            [begin
                 {ok, Trace} = lager:trace_console([{module, Mod}]),
                 lager:stop_trace(Trace)
             end || Mod <- Modules]
    end,
    ok.

-spec enable() -> ok.
enable() ->
    gen_server:call(?MODULE, enable, infinity).

-spec disable() -> ok.
disable() ->
    gen_server:call(?MODULE, disable, infinity).


-spec expire_trees() -> ok.
expire_trees() ->
    gen_server:cast(?MODULE, expire_trees).

-spec clear_trees() -> ok.
clear_trees() ->
    gen_server:cast(?MODULE, clear_trees).

-spec get_version() -> version().
get_version() ->
    case ets:lookup(?ETS, version) of
        [] ->
            legacy;
        [{version, Version}] ->
            Version
    end.

-spec get_partition_version(index()) -> version().
get_partition_version(Index) ->
    case ets:lookup(?ETS, Index) of
        [] ->
            legacy;
        [{Index, Version}] ->
            Version
    end.

-spec get_pending_version() -> version().
get_pending_version() ->
    case ets:lookup(?ETS, pending_version) of
        [] ->
            legacy;
        [{pending_version, Version}] ->
            Version
    end.

-spec get_upgraded() -> boolean().
get_upgraded() ->
    case {get_version(), get_pending_version()} of
        {legacy, legacy} ->
            false;
        _ ->
            true
    end.

%% For testing to quickly verify tree versions match up with manager version
-spec get_trees_version() -> [{index(), version()}].
get_trees_version() ->
    gen_server:call(?MODULE, get_trees_version, infinity).

%% @doc Manually trigger hashtree exchanges.
%%      -- If an index is provided, trigger exchanges between the index and all
%%         sibling indices for all index_n.
%%      -- If both an index and index_n are provided, trigger exchanges between
%%         the index and all sibling indices associated with the specified
%%         index_n.
%%      -- If an index, remote index, and index_n are provided, trigger an
%%         exchange between the index and remote index for the specified
%%         index_n.
-spec manual_exchange(index() |
                      {index(), index_n()} |
                      {index(), index(), index_n()}) -> ok.
manual_exchange(Exchange) ->
    gen_server:call(?MODULE, {manual_exchange, Exchange}, infinity).

-spec cancel_exchange(index()) -> ok | undefined.
cancel_exchange(Index) ->
    gen_server:call(?MODULE, {cancel_exchange, Index}, infinity).

-spec cancel_exchanges() -> [index()].
cancel_exchanges() ->
    gen_server:call(?MODULE, cancel_exchanges, infinity).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

-spec init([]) -> {'ok',state()}.
init([]) ->
    register_capabilities(),
    riak_core_throttle:init(riak_kv,
                            ?AAE_THROTTLE_KEY,
                            {aae_throttle_limits, ?DEFAULT_AAE_THROTTLE_LIMITS},
                            {aae_throttle_enabled, true}),
    ?ETS = ets:new(?ETS, [named_table, {read_concurrency, true}]),

    schedule_tick(),

    {_, Opts} = settings(),
    Mode = case proplists:is_defined(manual, Opts) of
               true ->
                   manual;
               false ->
                   automatic
           end,
    set_debug(proplists:is_defined(debug, Opts)),
    State = #state{mode=Mode,
                   trees=[],
                   tree_queue=[],
                   locks=[],
                   exchanges=[],
                   exchange_queue=[]},
    State2 = reset_build_tokens(State),
    schedule_reset_build_tokens(),
    {ok, State2}.

register_capabilities() ->
    riak_core_capability:register({riak_kv, object_hash_version},
                                  [0, legacy],
                                  legacy),
    riak_core_capability:register({riak_kv, anti_entropy},
                                  [enabled_v1, disabled],
                                  disabled).

handle_call({set_mode, Mode}, _From, State=#state{mode=CurrentMode}) ->
    State2 = case {CurrentMode, Mode} of
                 {automatic, manual} ->
                     %% Clear exchange queue when switching to manual mode
                     State#state{exchange_queue=[]};
                 _ ->
                     State
             end,
    {reply, ok, State2#state{mode=Mode}};
handle_call({manual_exchange, Exchange}, _From, State) ->
    State2 = enqueue_exchange(Exchange, State),
    {reply, ok, State2};
handle_call(enable, _From, State) ->
    {_, Opts} = settings(),
    application:set_env(riak_kv, anti_entropy, {on, Opts}),
    {reply, ok, State};
handle_call(disable, _From, State) ->
    {_, Opts} = settings(),
    application:set_env(riak_kv, anti_entropy, {off, Opts}),
    _ = [riak_kv_index_hashtree:stop(T) || {_,T} <- State#state.trees],
    {reply, ok, State};
handle_call(get_trees_version, _From, State=#state{trees=Trees}) ->
    VTrees = [{Idx, riak_kv_index_hashtree:get_version(Pid)} || {Idx, Pid} <- Trees],
    {reply, VTrees, State};
handle_call({get_lock, Type, Pid}, _From, State) ->
    {Reply, State2} = do_get_lock(Type, Pid, State),
    {reply, Reply, State2};
%% To support compatibility with pre 2.2 nodes.
handle_call({start_exchange_remote, FsmPid, Index, IndexN}, From, State) ->
    do_start_exchange_remote(FsmPid, Index, IndexN, legacy, From, State);
handle_call({start_exchange_remote, FsmPid, Index, IndexN, Version}, From, State) ->
    do_start_exchange_remote(FsmPid, Index, IndexN, Version, From, State);
handle_call({cancel_exchange, Index}, _From, State) ->
    case lists:keyfind(Index, 1, State#state.exchanges) of
        false ->
            {reply, undefined, State};
        {Index, _Ref, Pid} ->
            exit(Pid, kill),
            {reply, ok, State}
    end;
handle_call(cancel_exchanges, _From, State=#state{exchanges=Exchanges}) ->
    Indices = [begin
                   exit(Pid, kill),
                   Index
               end || {Index, _Ref, Pid} <- Exchanges],
    {reply, Indices, State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({release_lock, Pid}, S) ->
    S2 = maybe_release_lock(Pid, S),
    {noreply, S2};

handle_cast({requeue_poke, Index}, State) ->
    State2 = requeue_poke(Index, State),
    {noreply, State2};
handle_cast({exchange_status, Pid, LocalVN, RemoteVN, IndexN, Reply}, State) ->
    State2 = do_exchange_status(Pid, LocalVN, RemoteVN, IndexN, Reply, State),
    {noreply, State2};
handle_cast(clear_trees, S) ->
    clear_all_exchanges(S#state.exchanges),
    clear_all_trees(S#state.trees),
    {noreply, S};
handle_cast(expire_trees, S) ->
    ok = expire_all_trees(S#state.trees),
    {noreply, S};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(tick, State) ->
    State1 = maybe_tick(State),
    {noreply, State1};
handle_info(reset_build_tokens, State) ->
    State2 = reset_build_tokens(State),
    schedule_reset_build_tokens(),
    {noreply, State2};
handle_info({{hashtree_pid, Index}, Reply}, State) ->
    case Reply of
        {ok, Pid} when is_pid(Pid) ->
            State2 = add_hashtree_pid(Index, Pid, State),
            {noreply, State2};
        _ ->
            {noreply, State}
    end;
handle_info({'DOWN', _, _, Pid, Status}, #state{vnode_status_pid=Pid}=State) ->
    case Status of
        {result, _} = RES ->
            State2 = query_and_set_aae_throttle3(RES, State#state{vnode_status_pid=undefined}),
            {noreply, State2};
        Else ->
            lager:error("query_and_set_aae_throttle error: ~p",[Else]),
            {noreply, State}
    end;
handle_info({'DOWN', Ref, _, Obj, Status}, State) ->
    State2 = maybe_release_lock(Ref, State),
    State3 = maybe_clear_exchange(Ref, Status, State2),
    State4 = maybe_clear_registered_tree(Obj, State3),
    {noreply, State4};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

clear_all_exchanges(Exchanges) ->
    [begin
         exit(Pid, kill),
         Index
     end || {Index, _Ref, Pid} <- Exchanges].

clear_all_trees(Trees) ->
    [riak_kv_index_hashtree:clear(TPid) || {_, TPid} <- Trees].

expire_all_trees(Trees) ->
    _ = [riak_kv_index_hashtree:expire(TPid) || {_, TPid} <- Trees],
    ok.

schedule_reset_build_tokens() ->
    {_, Reset} = app_helper:get_env(riak_kv, anti_entropy_build_limit,
                                    ?DEFAULT_BUILD_LIMIT),
    erlang:send_after(Reset, self(), reset_build_tokens).

reset_build_tokens(State) ->
    {Tokens, _} = app_helper:get_env(riak_kv, anti_entropy_build_limit,
                                     ?DEFAULT_BUILD_LIMIT),
    State#state{build_tokens=Tokens}.

-spec settings() -> {boolean(), proplists:proplist()}.
settings() ->
    case app_helper:get_env(riak_kv, anti_entropy, {off, []}) of
        {on, Opts} ->
            {true, Opts};
        {off, Opts} ->
            {false, Opts};
        X ->
            lager:warning("Invalid setting for riak_kv/anti_entropy: ~p", [X]),
            application:set_env(riak_kv, anti_entropy, {off, []}),
            {false, []}
    end.

-spec maybe_reload_hashtrees(riak_core_ring(), state()) -> state().
maybe_reload_hashtrees(Ring, State) ->
    case lists:member(riak_kv, riak_core_node_watcher:services(node())) of
        true ->
            reload_hashtrees(Ring, State);
        false ->
            State
    end.

%% Determine the index_hashtree pid for each running primary vnode. This
%% function is called each tick to ensure that any newly spawned vnodes are
%% queried.
-spec reload_hashtrees(riak_core_ring(), state()) -> state().
reload_hashtrees(Ring, State=#state{trees=Trees}) ->
    Indices = riak_core_ring:my_indices(Ring),
    Existing = dict:from_list(Trees),
    MissingIdx = [Idx || Idx <- Indices,
                         not dict:is_key(Idx, Existing)],
    _ = [riak_kv_vnode:request_hashtree_pid(Idx) || Idx <- MissingIdx],
    State.

add_hashtree_pid(Index, Pid, State) ->
    add_hashtree_pid(enabled(), Index, Pid, State).

add_hashtree_pid(false, _Index, Pid, State) ->
    riak_kv_index_hashtree:stop(Pid),
    State;
add_hashtree_pid(true, Index, Pid, State=#state{trees=Trees, trees_version=VTrees}) ->
    case orddict:find(Index, Trees) of
        {ok, Pid} ->
            %% Already know about this hashtree
            State;
        _ ->
            monitor(process, Pid),
            Version = riak_kv_index_hashtree:get_version(Pid),
            Trees2 = orddict:store(Index, Pid, Trees),
            VTrees2 = orddict:store(Index, Version, VTrees),
            ets:insert(?ETS, {Index, Version}),
            State2 = State#state{trees=Trees2, trees_version=VTrees2},
            State3 = add_index_exchanges(Index, State2),
            State4 = check_upgrade(State3),
            State4
    end.

-spec do_get_lock(any(),pid(),state())
                 -> {ok | max_concurrency | build_limit_reached, state()}.
do_get_lock(Type, Pid, State=#state{locks=Locks}) ->
    Concurrency = app_helper:get_env(riak_kv,
                                     anti_entropy_concurrency,
                                     ?DEFAULT_CONCURRENCY),
    case length(Locks) >= Concurrency of
        true ->
            {max_concurrency, State};
        false ->
            case check_lock_type(Type, State) of
                {ok, State2} ->
                    Ref = monitor(process, Pid),
                    State3 = State2#state{locks=[{Pid,Ref}|Locks]},
                    {ok, State3};
                Error ->
                    {Error, State}
            end
    end.

-spec do_start_exchange_remote(pid(), index(), index_n(), version(), term(), state())
                  -> {reply, term(), state()} | {noreply, state()}.
do_start_exchange_remote(FsmPid, Index, IndexN, Version, From, State) ->
    Enabled = enabled(),
    TreeResult = orddict:find(Index, State#state.trees),
    maybe_start_exchange_remote(Enabled, TreeResult, FsmPid, IndexN, Version, From, State).

-spec maybe_start_exchange_remote(Enabled::boolean(),
                                  TreeResult::{ok, term()} | error,
                                  FsmPid::pid(),
                                  IndexN::index_n(),
                                  Version::version(),
                                  From::term(),
                                  State::state()) ->
    {noreply, state()} | {reply, term(), state()}.
maybe_start_exchange_remote(false, _, _FsmPid, _IndexN, _Version, _From, State) ->
    {reply, {remote_exchange, anti_entropy_disabled}, State};
maybe_start_exchange_remote(_, error, _FsmPid,  _IndexN, _Version, _From, State) ->
    {reply, {remote_exchange, not_built}, State};
maybe_start_exchange_remote(true, {ok, Tree}, FsmPid, IndexN, Version, From, State) ->
    case do_get_lock(exchange_remote, FsmPid, State) of
        {ok, State2} ->
            %% Concurrency lock acquired, now forward to index_hashtree
            %% to acquire tree lock.
            riak_kv_index_hashtree:start_exchange_remote(FsmPid, Version, From, IndexN, Tree),
            {noreply, State2};
        {Reply, State2} ->
            {reply, {remote_exchange, Reply}, State2}
    end.


-spec check_lock_type(any(), state()) -> build_limit_reached | {ok, state()}.
check_lock_type(Type, State) ->
    case Type of
        build->
            do_get_build_token(State);
        % upgrade ->
        %     do_get_build_token(State);
        % Upgrade doesn't actually do any work, so don't spend a token on it
        % this reduces the time a cluster spends in lock-down waiting for 
        % rebuilds to spread through the cluster, and the tokens can be spent 
        % building these upgraded backends.
        _ ->
            {ok, State}
    end.

-spec do_get_build_token(state()) -> build_limit_reached | {ok, state()}.
do_get_build_token(State=#state{build_tokens=Tokens}) ->
    if Tokens > 0 ->
            {ok, State#state{build_tokens=Tokens-1}};
       true ->
            build_limit_reached
    end.

-spec maybe_release_lock(reference(), state()) -> state().
maybe_release_lock(Ref, State) ->
    Locks = lists:keydelete(Ref, 2, State#state.locks),
    State#state{locks=Locks}.

-spec maybe_clear_exchange(reference(), term(), state()) -> state().
maybe_clear_exchange(Ref, Status, State) ->
    case lists:keytake(Ref, 2, State#state.exchanges) of
        false ->
            State;
        {value, {Idx,Ref,_Pid}, Exchanges} ->
            lager:debug("Untracking exchange: ~p :: ~p", [Idx, Status]),
            State#state{exchanges=Exchanges}
    end.

-spec maybe_clear_registered_tree(pid(), state()) -> state().
maybe_clear_registered_tree(Pid, State) when is_pid(Pid) ->
    case lists:keytake(Pid, 2, State#state.trees) of
        false ->
            State;
        {value, {Index, Pid}, Trees} ->
            ets:delete(?ETS, Index),
            VTrees = orddict:erase(Index, State#state.trees_version),
            State#state{trees=Trees, trees_version=VTrees}
    end;
maybe_clear_registered_tree(_, State) ->
    State.

-spec next_tree(state()) -> {pid(), state()}.
next_tree(#state{trees=[]}) ->
    throw(no_trees_registered);
next_tree(State=#state{tree_queue=[], trees=Trees}) ->
    State2 = State#state{tree_queue=Trees},
    next_tree(State2);
next_tree(State=#state{tree_queue=Queue}) ->
    [{_Index,Pid}|Rest] = Queue,
    State2 = State#state{tree_queue=Rest},
    {Pid, State2}.

-spec schedule_tick() -> ok.
schedule_tick() ->
    %% Perform tick every 15 seconds
    DefaultTick = 15000,
    Tick = app_helper:get_env(riak_kv,
                              anti_entropy_tick,
                              DefaultTick),
    erlang:send_after(Tick, ?MODULE, tick),
    ok.

-spec maybe_tick(state()) -> state().
maybe_tick(State) ->
    case enabled() of
        true ->
            case riak_core_capability:get({riak_kv, anti_entropy}, disabled) of
                disabled ->
                    NextState = State;
                enabled_v1 ->
                    NextState = tick(State)
            end;
        false ->
            %% Ensure we do not have any running index_hashtrees, which can
            %% happen when disabling anti-entropy on a live system.
            _ = [riak_kv_index_hashtree:stop(T) || {_,T} <- State#state.trees],
            NextState = State
    end,
    schedule_tick(),
    NextState.

-spec tick(state()) -> state().
tick(State) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    State1 = query_and_set_aae_throttle(State),
    State2 = maybe_reload_hashtrees(Ring, State1),
    State3 = maybe_start_upgrade(Ring, State2),
    State4 = lists:foldl(fun(_,S) ->
                                 maybe_poke_tree(S)
                         end, State3, lists:seq(1,10)),
    State5 = maybe_exchange(Ring, State4),
    State5.

-spec maybe_poke_tree(state()) -> state().
maybe_poke_tree(State=#state{trees=[]}) ->
    State;
maybe_poke_tree(State) ->
    {Tree, State2} = next_tree(State),
    riak_kv_index_hashtree:poke(Tree),
    State2.

-spec maybe_start_upgrade(riak_core_ring(),state()) -> state().
maybe_start_upgrade(Ring, State=#state{trees=Trees, version=legacy, pending_version=legacy}) ->
    Indices = riak_core_ring:my_indices(Ring),
    case riak_core_capability:get({riak_kv, object_hash_version}, legacy) of
        0 when length(Trees) == length(Indices) ->
            maybe_upgrade(State);
        _ ->
            State
    end;
maybe_start_upgrade(_Ring, State) ->
    State.

-spec maybe_upgrade(state()) -> state().
maybe_upgrade(State=#state{trees_version = []}) ->
    %% No hashtrees have registered with the manager, skip upgrade check
    State;
maybe_upgrade(State=#state{trees_version = VTrees}) ->
    case [Idx || {Idx, legacy} <- VTrees] of
        %% Upgrade is done already, set version in state
        [] ->
            ets:insert(?ETS, {version, 0}),
            State#state{version=0};
        %% No trees have been upgraded, check version
        %% on other nodes to see if we should immediately start upgrade.
        %% Otherwise wait for all local and remote exchanges to complete.
        Trees when length(Trees) == length(VTrees) ->
            check_remote_upgraded(State);
        %% Upgrade already started, set pending_version
        _ ->
            lager:notice("Hashtree upgrade already in-process, setting pending_version to 0"),
            ets:insert(?ETS, {pending_version, 0}),
            State#state{pending_version=0}
    end.

check_remote_upgraded(State) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Nodes = riak_core_ring:all_members(Ring),
    case lists:any(fun do_check_remote_upgraded/1, Nodes) of
        true ->
            lager:notice("Starting AAE hashtree upgrade"),
            ets:insert(?ETS, {pending_version, 0}),
            State#state{pending_version=0};
        _ ->
            check_exchanges_and_upgrade(State, Nodes)
    end.

do_check_remote_upgraded(Node) ->
    case riak_core_util:safe_rpc(Node, riak_kv_entropy_manager, get_upgraded, [], 10000) of
        Result when is_boolean(Result) ->
            Result;
        {badrpc, _Reason} ->
            false;
        _ ->
            false
    end.

-spec check_exchanges_and_upgrade(state(), list()) -> state().
check_exchanges_and_upgrade(State, Nodes) ->
    case riak_kv_entropy_info:all_sibling_exchanges_complete() of
        true ->
            %% Now check nodes who havent reported success in the ETS table
            case check_all_remote_exchanges_complete(Nodes) of
                true ->
                    lager:notice("Starting AAE hashtree upgrade"),
                    ets:insert(?ETS, {pending_version, 0}),
                    State#state{pending_version=0};
                _ ->
                    State
            end;
        _ ->
            State
    end.

-spec check_all_remote_exchanges_complete(list()) -> boolean().
check_all_remote_exchanges_complete(Nodes) ->
    lists:all(fun maybe_check_and_record_remote_exchange/1, Nodes).

-spec maybe_check_and_record_remote_exchange(node()) -> boolean().
maybe_check_and_record_remote_exchange(Node) ->
    case ets:lookup(?ETS, Node) of
        [{Node, true}] ->
            true;
        _ ->
            Result = check_remote_exchange(Node),
            ets:insert(?ETS, {Node, Result}),
            Result
    end.

-spec check_remote_exchange(node()) -> boolean().
check_remote_exchange(Node) ->
    case riak_core_util:safe_rpc(Node, riak_kv_entropy_info, all_sibling_exchanges_complete, [], 10000) of
        Result when is_boolean(Result) ->
            Result;
        {badrpc, _Reason} ->
            false;
        _ ->
            false
    end.

-spec check_upgrade(state()) -> state().
check_upgrade(State=#state{pending_version=legacy}) ->
    State;
check_upgrade(State=#state{pending_version=PendingVersion,trees_version=VTrees}) ->
    %% Verify we have all partitions registered with a hashtree, version
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Indices = riak_core_ring:my_indices(Ring),
    MissingIdx = [Idx || Idx <- Indices,
        not orddict:is_key(Idx, VTrees)],
    case MissingIdx of
        [] ->
            case [Idx || {Idx, V} <- VTrees, V == PendingVersion] of
                [] ->
                    State;
                Trees when length(Trees) == length(VTrees) ->
                    lager:notice("Local AAE hashtrees have completed upgrade to version: ~p",[PendingVersion]),
                    ets:insert(?ETS, {version, PendingVersion}),
                    ets:insert(?ETS, {pending_version, legacy}),
                    State#state{version=PendingVersion, pending_version=legacy};
                _Trees ->
                    State
            end;
        _ ->
            State
    end.


%%%===================================================================
%%% Exchanging
%%%===================================================================

-spec do_exchange_status(pid(), vnode(), vnode(), index_n(), any(), state()) -> state().
do_exchange_status(_Pid, LocalVN, RemoteVN, IndexN, Reply, State) ->
    {LocalIdx, _} = LocalVN,
    {RemoteIdx, RemoteNode} = RemoteVN,
    case Reply of
        ok ->
            State;
        {remote, anti_entropy_disabled} ->
            lager:warning("Active anti-entropy is disabled on ~p", [RemoteNode]),
            State;
        _ ->
            State2 = requeue_exchange(LocalIdx, RemoteIdx, IndexN, State),
            State2
    end.

-spec enqueue_exchange(index() |
                       {index(), index_n()} |
                       {index(), index(), index_n()}, state()) -> state().
enqueue_exchange(E={Index, _RemoteIdx, _IndexN}, State) ->
    %% Verify that the exchange is valid
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Exchanges = all_pairwise_exchanges(Index, Ring),
    case lists:member(E, Exchanges) of
        true ->
            enqueue_exchanges([E], State);
        false ->
            State
    end;
enqueue_exchange({Index, IndexN}, State) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Exchanges = all_pairwise_exchanges(Index, Ring),
    Exchanges2 = [Exchange || Exchange={_, _, IdxN} <- Exchanges,
                              IdxN =:= IndexN],
    enqueue_exchanges(Exchanges2, State);
enqueue_exchange(Index, State) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Exchanges = all_pairwise_exchanges(Index, Ring),
    enqueue_exchanges(Exchanges, State).

-spec enqueue_exchanges([exchange()], state()) -> state().
enqueue_exchanges(Exchanges, State) ->
    EQ = prune_exchanges(State#state.exchange_queue ++ Exchanges),
    State#state{exchange_queue=EQ}.

-spec start_exchange(vnode(),
                     {index(), index_n()},
                     riak_core_ring(),
                     state()) -> {any(), state()}.
start_exchange(LocalVN, {RemoteIdx, IndexN}, Ring, State) ->
    %% in rare cases, when the ring is resized, there may be an
    %% exchange enqueued for an index that no longer exists. catch
    %% the case here and move on
    try riak_core_ring:index_owner(Ring, RemoteIdx) of
        Owner ->
            Nodes = lists:usort([node(), Owner]),
            DownNodes = Nodes -- riak_core_node_watcher:nodes(riak_kv),
            case DownNodes of
                [] ->
                    RemoteVN = {RemoteIdx, Owner},
                    start_exchange(LocalVN, RemoteVN, IndexN, Ring, State);
                _ ->
                    {{riak_kv_down, DownNodes}, State}
            end
    catch
        error:{badmatch,_} ->
            lager:warning("ignoring exchange to non-existent index: ~p", [RemoteIdx]),
            {ok, State}
    end.

start_exchange(LocalVN, RemoteVN, IndexN, Ring, State) ->
    {LocalIdx, _} = LocalVN,
    {RemoteIdx, _} = RemoteVN,
    case riak_core_ring:vnode_type(Ring, LocalIdx) of
        primary ->
            case orddict:find(LocalIdx, State#state.trees) of
                error ->
                    %% The local vnode has not yet registered it's
                    %% index_hashtree. Likewise, the vnode may not even
                    %% be running (eg. after a crash).  Send request to
                    %% the vnode to trigger on-demand start and requeue
                    %% exchange.
                    riak_kv_vnode:request_hashtree_pid(LocalIdx),
                    State2 = requeue_exchange(LocalIdx, RemoteIdx, IndexN, State),
                    {not_built, State2};
                {ok, Tree} ->
                    case riak_kv_exchange_fsm:start(LocalVN, RemoteVN,
                                                    IndexN, Tree, self()) of
                        {ok, FsmPid} ->
                            Ref = monitor(process, FsmPid),
                            Exchanges = State#state.exchanges,
                            Exchanges2 = [{LocalIdx, Ref, FsmPid} | Exchanges],
                            {ok, State#state{exchanges=Exchanges2}};
                        {error, Reason} ->
                            {Reason, State}
                    end
            end;
        _ ->
            %% No longer owner of this partition or partition is
            %% part or larger future ring, ignore exchange
            {not_responsible, State}
    end.

-spec all_pairwise_exchanges(index(), riak_core_ring())
                            -> [exchange()].
all_pairwise_exchanges(Index, Ring) ->
    LocalIndexN = riak_kv_util:responsible_preflists(Index, Ring),
    Sibs = riak_kv_util:preflist_siblings(Index),
    lists:flatmap(
      fun(RemoteIdx) ->
              RemoteIndexN = riak_kv_util:responsible_preflists(RemoteIdx, Ring),
              SharedIndexN = ordsets:intersection(ordsets:from_list(LocalIndexN),
                                                  ordsets:from_list(RemoteIndexN)),
              [{Index, RemoteIdx, IndexN} || IndexN <- SharedIndexN]
      end, Sibs).

-spec all_exchanges(node(), riak_core_ring(), state())
                   -> [exchange()].
all_exchanges(_Node, Ring, #state{trees=Trees}) ->
    Indices = orddict:fetch_keys(Trees),
    lists:flatmap(fun(Index) ->
                          all_pairwise_exchanges(Index, Ring)
                  end, Indices).

-spec add_index_exchanges(index(), state()) -> state().
add_index_exchanges(_Index, State) when State#state.mode == manual ->
    State;
add_index_exchanges(Index, State) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Exchanges = all_pairwise_exchanges(Index, Ring),
    EQ = State#state.exchange_queue ++ Exchanges,
    EQ2 = prune_exchanges(EQ),
    State#state{exchange_queue=EQ2}.

-spec prune_exchanges([exchange()])
                     -> [exchange()].
prune_exchanges(Exchanges) ->
    L = [if A < B ->
                 {A, B, IndexN};
            true ->
                 {B, A, IndexN}
         end || {A, B, IndexN} <- Exchanges],
    lists:usort(L).

-spec already_exchanging(index() ,state()) -> boolean().
already_exchanging(Index, #state{exchanges=E}) ->
    case lists:keyfind(Index, 1, E) of
        false ->
            false;
        {Index,_,_} ->
            true
    end.

-spec maybe_exchange(riak_core_ring(), state()) -> state().
maybe_exchange(Ring, State) ->
    case next_exchange(Ring, State) of
        {none, State2} ->
            State2;
        {NextExchange, State2} ->
            {LocalIdx, RemoteIdx, IndexN} = NextExchange,
            case already_exchanging(LocalIdx, State) of
                true ->
                    requeue_exchange(LocalIdx, RemoteIdx, IndexN, State2);
                false ->
                    LocalVN = {LocalIdx, node()},
                    case start_exchange(LocalVN, {RemoteIdx, IndexN}, Ring, State2) of
                        {ok, State3} ->
                            State3;
                        {_Reason, State3} ->
                            State3
                    end
            end
    end.

-spec next_exchange(riak_core_ring(), state()) -> {'none' | exchange(), state()}.
next_exchange(_Ring, State=#state{exchange_queue=[], trees=[]}) ->
    {none, State};
next_exchange(_Ring, State=#state{exchange_queue=[],
                                  mode=Mode}) when Mode == manual ->
    {none, State};
next_exchange(Ring, State=#state{exchange_queue=[]}) ->
    case prune_exchanges(all_exchanges(node(), Ring, State)) of
        [] ->
            {none, State};
        [Exchange|Rest] ->
            State2 = State#state{exchange_queue=Rest},
            {Exchange, State2}
    end;
next_exchange(_Ring, State=#state{exchange_queue=Exchanges}) ->
    [Exchange|Rest] = Exchanges,
    State2 = State#state{exchange_queue=Rest},
    {Exchange, State2}.

-spec requeue_poke(index(), state()) -> state().
requeue_poke(Index, State=#state{trees=Trees}) ->
    case orddict:find(Index, Trees) of
        {ok, Tree} ->
            Queue = State#state.tree_queue ++ [{Index,Tree}],
            State#state{tree_queue=Queue};
        _ ->
            State
    end.

-spec requeue_exchange(index(), index(), index_n(), state()) -> state().
requeue_exchange(LocalIdx, RemoteIdx, IndexN, State) ->
    Exchange = {LocalIdx, RemoteIdx, IndexN},
    case lists:member(Exchange, State#state.exchange_queue) of
        true ->
            State;
        false ->
            lager:debug("Requeue: ~p", [{LocalIdx, RemoteIdx, IndexN}]),
            Exchanges = State#state.exchange_queue ++ [Exchange],
            State#state{exchange_queue=Exchanges}
    end.

throttle() ->
    riak_core_throttle:throttle(riak_kv, ?AAE_THROTTLE_KEY).

get_aae_throttle() ->
    riak_core_throttle:get_throttle(riak_kv, ?AAE_THROTTLE_KEY).

set_aae_throttle(Milliseconds) when is_integer(Milliseconds), Milliseconds >= 0 ->
    riak_core_throttle:set_throttle(riak_kv, ?AAE_THROTTLE_KEY, Milliseconds).

is_aae_throttle_enabled() ->
    riak_core_throttle:is_throttle_enabled(riak_kv, ?AAE_THROTTLE_KEY).

disable_aae_throttle() ->
    riak_core_throttle:disable_throttle(riak_kv, ?AAE_THROTTLE_KEY).
enable_aae_throttle() ->
    riak_core_throttle:enable_throttle(riak_kv, ?AAE_THROTTLE_KEY).

get_max_local_vnodeq() ->
    try
	{ok, [{max,M}]} =
	    exometer:get_value(
	      [riak_core_stat:prefix(),riak_core,vnodeq,riak_kv_vnode],
	      [max]),
	{M, node()}
    catch _X:_Y ->
            %% This can fail locally if riak_core & riak_kv haven't finished their setup.
            {0, node()}
    end.

get_aae_throttle_limits() ->
    riak_core_throttle:get_limits(riak_kv, ?AAE_THROTTLE_KEY).

%% @doc Set AAE throttle limits list
%%
%% Limit list = [{max_vnode_q_len, per-repair-delay}]
%% A tuple with max_vnode_q_len=-1 must be present.
%% List sorting is not required: the throttle is robust with any ordering.

set_aae_throttle_limits(Limits) ->
    riak_core_throttle:set_limits(riak_kv, ?AAE_THROTTLE_KEY, Limits).

query_and_set_aae_throttle(State) ->
    case is_aae_throttle_enabled() of
        true ->
            query_and_set_aae_throttle2(State);
        false ->
            State#state{last_throttle=0}
    end.

query_and_set_aae_throttle2(#state{vnode_status_pid = undefined} = State) ->
    {Pid, _Ref} = spawn_monitor(fun() ->
        RES = fix_up_rpc_errors(
                ?MODULE:multicall([node()|nodes()],
                                  ?MODULE, get_max_local_vnodeq, [], 10*1000)),
              exit({result, RES})
         end),
    State#state{vnode_status_pid = Pid};
query_and_set_aae_throttle2(State) ->
    State.

query_and_set_aae_throttle3({result, {MaxNds, BadNds}}, State) ->
    %% If a node is really hosed, then this RPC call is going to fail
    %% for that node.  We might also delay the 'tick' processing by
    %% several seconds.  But the tick processing is OK if it's delayed
    %% while we wait for slow nodes here.
    {WorstVMax, _WorstNode} =
        case {BadNds, lists:reverse(lists:sort(MaxNds))} of
            {[], [{VMax, Node}|_]} ->
                {VMax, Node};
            {BadNodes, _} ->
                %% If anyone couldn't respond, let's assume the worst.
                %% If that node is actually down, then the net_kernel
                %% will mark it down for us soon, and then we'll
                %% calculate a real value after that.
                lager:info("Could not determine mailbox sizes for nodes: ~p",
                           [BadNds]),
                {max, BadNodes}
        end,
    NewThrottle = riak_core_throttle:set_throttle_by_load(
                    riak_kv,
                    ?AAE_THROTTLE_KEY,
                    WorstVMax),
    State#state{last_throttle=NewThrottle}.

%% Wrapper for meck interception for testing.
multicall(A, B, C, D, E) ->
    rpc:multicall(A, B, C, D, E).

fix_up_rpc_errors({ResL, BadL}) ->
    lists:foldl(fun({N, _}=R, {Rs, Bs}) when is_integer(N) ->
                        {[R|Rs], Bs};
                   (_, {Rs, Bs}) ->
                        {Rs, [bad_rpc_result|Bs]}
                end, {[], BadL}, ResL).

-ifdef(TEST).

make_state() ->
    #state{}.

get_last_throttle(State) ->
    State2 = wait_for_vnode_status(State),
    {State2#state.last_throttle, State2}.

wait_for_vnode_status(State) ->
    case is_aae_throttle_enabled() of
        false ->
            State;
        true ->
            receive
                {'DOWN',_,_,_,_} = Msg ->
                    element(2, handle_info(Msg, State))
            after 10*1000 ->
                    error({?MODULE, wait_for_vnode_status, timeout})
            end
    end.

-endif.
