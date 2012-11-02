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
         get_lock/1,
         get_lock/2,
         requeue_poke/1,
         exchange_status/4]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3, enabled/0, set_mode/1, set_debug/1,
         cancel_exchange/1, cancel_exchanges/0]).

-type index() :: non_neg_integer().
-type index_n() :: {index(), pos_integer()}.
-type vnode() :: {index(), node()}.
-type exchange() :: {index(), index(), index_n()}.
-type riak_core_ring() :: riak_core_ring:riak_core_ring().

-record(state, {mode           = automatic :: automatic | manual,
                trees          = []        :: [{index(), pid()}],
                tree_queue     = []        :: [{index(), pid()}],
                locks          = []        :: [{pid(), reference()}],
                exchange_queue = []        :: [exchange()],
                exchanges      = []        :: [{index(), reference(), pid()}]
               }).

-type state() :: #state{}.

-define(DEFAULT_CONCURRENCY, 2).

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
%%      the lock with the calling process.
-spec get_lock(any(), pid()) -> ok | max_concurrency.
get_lock(Type, Pid) ->
    gen_server:call(?MODULE, {get_lock, Type, Pid}, infinity).

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
    ok = gen_server:call(?MODULE, {set_mode, Mode});
set_mode(Mode=manual) ->
    ok = gen_server:call(?MODULE, {set_mode, Mode}).

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
    schedule_tick(),
    {_, Opts} = settings(),
    Mode = case lists:member(manual, Opts) of
               true ->
                   manual;
               false ->
                   automatic
           end,
    set_debug(lists:member(debug, Opts)),
    State = #state{mode=Mode,
                   trees=[],
                   tree_queue=[],
                   locks=[],
                   exchanges=[],
                   exchange_queue=[]},
    {ok, State}.

handle_call({set_mode, Mode}, _From, State) ->
    {reply, ok, State#state{mode=Mode}};
handle_call({manual_exchange, Exchange}, _From, State) ->
    State2 = trigger_exchange(Exchange, State),
    {reply, ok, State2};
handle_call({get_lock, Type, Pid}, _From, State) ->
    {Reply, State2} = do_get_lock(Type, Pid, State),
    {reply, Reply, State2};
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

handle_cast({requeue_poke, Index}, State) ->
    State2 = requeue_poke(Index, State),
    {noreply, State2};
handle_cast({exchange_status, Pid, LocalVN, RemoteVN, IndexN, Reply}, State) ->
    State2 = do_exchange_status(Pid, LocalVN, RemoteVN, IndexN, Reply, State),
    {noreply, State2};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(tick, State) ->
    State2 = tick(State),
    {noreply, State2};
handle_info({'DOWN', Ref, _, Obj, _}, State) ->
    State2 = maybe_release_lock(Ref, State),
    State3 = maybe_clear_exchange(Ref, State2),
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

-spec settings() -> {boolean(), proplists:proplist()}.
settings() ->
    case app_helper:get_env(riak_kv, anti_entropy, {off, []}) of
        {on, Opts} ->
            {true, Opts};
        {manual, Opts} ->
            {true, [manual|Opts]};
        {off, Opts} ->
            {false, Opts};
        X ->
            lager:warning("Invalid setting for riak_kv/anti_entropy: ~p", [X]),
            application:set_env(riak_kv, anti_entropy, {off, []}),
            {false, []}
    end.

-spec maybe_reload_hashtrees(state()) -> state().
maybe_reload_hashtrees(State) ->
    case lists:member(riak_kv, riak_core_node_watcher:services(node())) of
        true ->
            reload_hashtrees(State);
        false ->
            State
    end.

%% Determine the index_hashtree pid for each running primary vnode. This
%% function is called each tick to ensure that any newly spawned vnodes are
%% queried.
-spec reload_hashtrees(state()) -> state().
reload_hashtrees(State=#state{trees=Trees}) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Indices = riak_core_ring:my_indices(Ring),
    Existing = dict:from_list(Trees),
    MissingIdx = [Idx || Idx <- Indices,
                         not dict:is_key(Idx, Existing)],
    L = [{Idx, Pid} || Idx <- MissingIdx,
                       {ok,Pid} <- [riak_kv_vnode:hashtree_pid(Idx)],
                       is_pid(Pid) andalso is_process_alive(Pid)],
    Trees2 = orddict:from_list(Trees ++ L),
    State2 = State#state{trees=Trees2},
    State3 = lists:foldl(fun({Idx,Pid}, StateAcc) ->
                                 monitor(process, Pid),
                                 add_index_exchanges(Idx, StateAcc)
                         end, State2, L),
    State3.


-spec do_get_lock(any(),pid(),state()) -> {'max_concurrency',state()}.
do_get_lock(_Type, Pid, State=#state{locks=Locks}) ->
    Concurrency = app_helper:get_env(riak_kv,
                                     anti_entropy_concurrency,
                                     ?DEFAULT_CONCURRENCY),
    case length(Locks) >= Concurrency of
        true ->
            {max_concurrency, State};
        false ->
            Ref = monitor(process, Pid),
            State2 = State#state{locks=[{Pid,Ref}|Locks]},
            {ok, State2}
    end.

-spec maybe_release_lock(reference(), state()) -> state().
maybe_release_lock(Ref, State) ->
    Locks = lists:keydelete(Ref, 2, State#state.locks),
    State#state{locks=Locks}.

-spec maybe_clear_exchange(reference(), state()) -> state().
maybe_clear_exchange(Ref, State) ->
    case lists:keytake(Ref, 2, State#state.exchanges) of
        false ->
            State;
        {value, {Idx,Ref,_Pid}, Exchanges} ->
            lager:debug("Untracking exchange: ~p", [Idx]),
            State#state{exchanges=Exchanges}
    end.

-spec maybe_clear_registered_tree(pid(), state()) -> state().
maybe_clear_registered_tree(Pid, State) when is_pid(Pid) ->
    Trees = lists:keydelete(Pid, 2, State#state.trees),
    State#state{trees=Trees};
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
    DefaultTick = 1000,
    Tick = app_helper:get_env(riak_kv,
                              anti_entropy_tick,
                              DefaultTick),
    erlang:send_after(Tick, ?MODULE, tick),
    ok.

-spec tick(state()) -> state().
tick(State) ->
    case riak_core_capability:get({riak_kv, anti_entropy}, disabled) of
        disabled ->
            NextState = State;
        enabled_v1 ->
            State2 = maybe_reload_hashtrees(State),
            State3 = lists:foldl(fun(_,S) ->
                                         maybe_poke_tree(S)
                                 end, State2, lists:seq(1,10)),
            State4 = maybe_exchange(State3),
            NextState = State4
    end,
    schedule_tick(),
    NextState.

-spec maybe_poke_tree(state()) -> state().
maybe_poke_tree(State=#state{trees=[]}) ->
    State;
maybe_poke_tree(State) ->
    {Tree, State2} = next_tree(State),
    riak_kv_index_hashtree:poke(Tree),
    State2.

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

-spec trigger_exchange(index() |
                       {index(), index_n()} |
                       {index(), index(), index_n()}, state()) -> state().
trigger_exchange(E={Index, _RemoteIdx, _IndexN}, State) ->
    %% Verify that the exchange is valid
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Exchanges = all_pairwise_exchanges(Index, Ring),
    case lists:member(E, Exchanges) of
        true ->
            trigger_exchanges([E], State);
        false ->
            State
    end;
trigger_exchange({Index, IndexN}, State) -> 
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Exchanges = all_pairwise_exchanges(Index, Ring),
    Exchanges2 = [Exchange || Exchange={_, _, IdxN} <- Exchanges,
                              IdxN =:= IndexN],
    trigger_exchanges(Exchanges2, State);
trigger_exchange(Index, State) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Exchanges = all_pairwise_exchanges(Index, Ring),
    trigger_exchanges(Exchanges, State).

-spec trigger_exchanges([exchange()], state()) -> state().
trigger_exchanges(Exchanges, State) ->
    EQ = prune_exchanges(State#state.exchange_queue ++ Exchanges),
    State#state{exchange_queue=EQ}.

-spec start_exchange(vnode(),
                     {index(), index_n()},
                     riak_core_ring(),
                     state()) -> {any(), state()}.
start_exchange(LocalVN, {RemoteIdx, IndexN}, Ring, State) ->
    Owner = riak_core_ring:index_owner(Ring, RemoteIdx),
    RemoteVN = {RemoteIdx, Owner},
    {LocalIdx, _} = LocalVN,
    case riak_core_ring:index_owner(Ring, LocalIdx) == node() of
        false ->
            %% No longer owner of this partition, ignore exchange
            {not_responsible, State};
        true ->
            case orddict:find(LocalIdx, State#state.trees) of
                error ->
                    %% The local vnode has not yet registered it's
                    %% index_hashtree. Likewise, the vnode may not even
                    %% be running (eg. after a crash).  Send request to
                    %% the vnode to trigger on-demand start and requeue
                    %% exchange.
                    spawn(fun() ->
                                  riak_kv_vnode:hashtree_pid(LocalIdx)
                          end),
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
            end
    end.

-spec all_pairwise_exchanges(index(), riak_core_ring())
                            -> [exchange()].
all_pairwise_exchanges(Index, Ring) ->
    LocalIndexN = riak_kv_util:responsible_preflists(Index, Ring),
    Sibs = riak_kv_util:preflist_siblings(Index),
    lists:flatmap(
      fun(RemoteIdx) when RemoteIdx == Index ->
              [];
         (RemoteIdx) ->
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

-spec maybe_exchange(state()) -> state().
maybe_exchange(State) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
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
    [Exchange|Rest] = prune_exchanges(all_exchanges(node(), Ring, State)),
    State2 = State#state{exchange_queue=Rest},
    {Exchange, State2};
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
