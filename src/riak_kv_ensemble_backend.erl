%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013 Basho Technologies, Inc.  All Rights Reserved.
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
%% Implementation of {@link //riak_ensemble/riak_ensemble_backend} behavior that
%% connects riak_ensemble to riak_kv vnodes.
%%

-module(riak_kv_ensemble_backend).
-behaviour(riak_ensemble_backend).

-export([init/3, new_obj/4]).
-export([obj_epoch/1, obj_seq/1, obj_key/1, obj_value/1]).
-export([set_obj_epoch/2, set_obj_seq/2, set_obj_value/2]).
-export([get/3, put/4, tick/5, ping/2, ready_to_start/0]).
-export([synctree_path/2]).
-export([reply/2]).
-export([obj_newer/2]).
-export([handle_down/4]).

-include_lib("riak_ensemble/include/riak_ensemble_types.hrl").

-include_lib("kernel/include/logger.hrl").

-define(STABLE_RING_LEVEL, 20).

-record(state, {ensemble  :: ensemble_id(),
                id        :: peer_id(),
                proxy     :: atom(),
                proxy_ref :: reference(),
                vnode_ref :: reference(),
                async     :: pid() | undefined,
                last_ring_id :: term() | undefined,
                stable_ring_count = 0 :: non_neg_integer()}).

-type obj()    :: riak_object:riak_object().
-type state()  :: #state{}.
-type key()    :: {_,_}.
-type value()  :: any().

%%===================================================================

-spec init(ensemble_id(), peer_id(), [any()]) -> state().
init(Ensemble, Id, []) ->
    {{kv, _PL, _N, Idx}, _} = Id,
    Proxy = riak_core_vnode_proxy:reg_name(riak_kv_vnode, Idx),
    ProxyRef = erlang:monitor(process, Proxy),
    {ok, Vnode} = riak_core_vnode_manager:get_vnode_pid(Idx, riak_kv_vnode),
    VnodeRef = erlang:monitor(process, Vnode),
    #state{ensemble=Ensemble,
           id=Id,
           proxy=Proxy,
           proxy_ref=ProxyRef,
           vnode_ref=VnodeRef}.

%%===================================================================

-spec new_obj(epoch(), seq(), key(), value()) -> obj().
new_obj(Epoch, Seq, {B,K}, Value) ->
    case riak_object:is_robject(Value) of
        true ->
            set_epoch_seq(Epoch, Seq, Value);
        false ->
            RObj = riak_object:new(B, K, Value),
            set_epoch_seq(Epoch, Seq, RObj)
    end.

set_epoch_seq(Epoch, Seq, RObj) ->
    EpochSeq = (Epoch bsl 32) + Seq,
    %% riak_object:set_vclock(RObj, vclock:fresh(Epoch, Seq)),
    RObj2 = riak_object:set_vclock(RObj, vclock:fresh(eseq, EpochSeq)),
    RObj3 = riak_object:update_last_modified(RObj2),
    riak_object:apply_updates(RObj3).

get_epoch_seq(RObj) ->
    EpochSeq = vclock:get_counter(eseq, riak_object:vclock(RObj)),
    <<Epoch:32/integer, Seq:32/integer>> = <<EpochSeq:64/integer>>,
    {Epoch, Seq}.

%%===================================================================

-spec obj_epoch(obj()) -> epoch().
obj_epoch(RObj) ->
    {Epoch, _} = get_epoch_seq(RObj),
    Epoch.

-spec obj_seq(obj()) -> seq().
obj_seq(RObj) ->
    {_, Seq} = get_epoch_seq(RObj),
    Seq.

-spec obj_key(obj()) -> key().
obj_key(RObj) ->
    {riak_object:bucket(RObj), riak_object:key(RObj)}.

-spec obj_value(obj()) -> value().
obj_value(RObj) ->
    riak_object:get_value(RObj).

%% TODO: Move to riak_ensemble_backend or something
obj_newer(TestObj, BaseObj) ->
    A = {obj_epoch(TestObj), obj_seq(TestObj)},
    B = {obj_epoch(BaseObj), obj_seq(BaseObj)},
    A > B.

%%===================================================================

-spec set_obj_epoch(epoch(), obj()) -> obj().
set_obj_epoch(Epoch, RObj) ->
    {_, Seq} = get_epoch_seq(RObj),
    set_epoch_seq(Epoch, Seq, RObj).

-spec set_obj_seq(seq(), obj()) -> obj().
set_obj_seq(Seq, RObj) ->
    {Epoch, _} = get_epoch_seq(RObj),
    set_epoch_seq(Epoch, Seq, RObj).

-spec set_obj_value(value(), obj()) -> obj().
set_obj_value(Value, RObj) ->
    case riak_object:is_robject(Value) of
        true ->
            Contents = riak_object:get_contents(Value),
            riak_object:set_contents(RObj, Contents);
        false ->
            riak_object:apply_updates(riak_object:update_value(RObj, Value))
    end.

%%===================================================================

-spec get(key(), riak_ensemble_backend:from(), state()) -> state().
get(Key, From, State=#state{proxy=Proxy}) ->
    ok = send_msg(Proxy, From, {ensemble_get, Key}),
    State.

-spec put(key(), obj(), riak_ensemble_backend:from(), state()) -> state().
put(Key, Obj, From, State=#state{proxy=Proxy}) ->
    ok = send_msg(Proxy, From, {ensemble_put, Key, Obj}),
    State.

-spec send_msg(atom(), riak_ensemble_backend:from(), any()) -> ok.
send_msg(Proxy, From, Msg) ->
    Msg2 = erlang:append_element(Msg, From),
    catch Proxy ! Msg2,
    ok.

-spec reply(riak_ensemble_backend:from(), any()) -> ok.
reply(From, Reply) ->
    riak_ensemble_backend:reply(From, Reply),
    ok.

%%===================================================================

-spec handle_down(reference(), pid(), term(), state()) -> false |
                                                          {reset, state()}.
handle_down(Ref, _Pid, Reason, #state{id=Id,
                                      proxy=Proxy,
                                      vnode_ref=VnodeRef,
                                      proxy_ref=ProxyRef}=State) ->
    {{kv, _PL, _N, Idx}, _} = Id,
    case Ref of
        VnodeRef ->
            ?LOG_WARNING("Vnode for Idx: ~p crashed with reason: ~p.",
                [Idx, Reason]),
            %% There are some races here. The vnode may not have been restarted yet
            %% or the manager itself could be down.
            %% TODO: Add a timer:sleep()? Something else?
            {ok, Vnode} =
                riak_core_vnode_manager:get_vnode_pid(Idx, riak_kv_vnode),
            VnodeRef2 = erlang:monitor(process, Vnode),
            {reset, State#state{vnode_ref=VnodeRef2}};
        ProxyRef ->
            ?LOG_WARNING("Vnode Proxy for Idx: ~p crashed with reason: ~p.",
                [Idx, Reason]),
            ProxyRef2 = erlang:monitor(process, Proxy),
            {reset, State#state{proxy_ref=ProxyRef2}};
        _ ->
            false
    end.

%%===================================================================

-spec tick(epoch(), seq(), peer_id(), views(), state()) -> state().
tick(_Epoch, _Seq, _Leader, Views, State=#state{id=Id}) ->
    %% TODO: Should this entire function be async?
    CurrentRingID = riak_core_ring_manager:get_ring_id(),
    StableRingCount =
        case State#state.last_ring_id == CurrentRingID of
            true ->
                State#state.stable_ring_count + 1;
            false ->
                0
        end,
    case StableRingCount == ?STABLE_RING_LEVEL of
        true ->
            %% Any changes to the cluster are now stable, and have not been
            %% acted on by the claimant.  So consider if action is necessary
            %% here
            {{kv, Idx, N, _}, _} = Id,
            Latest = hd(Views),
            {ok, Ring, CHBin} = riak_core_ring_manager:get_raw_ring_chashbin(),
            case riak_core_ring:check_lastgasp(Ring) of
                true ->
                    %% See https://github.com/basho/riak_core/issues/943
                    State;
                false ->
                    CHBinIter = chashbin:exact_iterator(Idx, CHBin),
                    {PL, _} = chashbin:itr_pop(N, CHBinIter),
                    %% TODO: Make ensembles/peers use ensemble/peer as actual peer
                    %% name so this is unneeded
                    Peers = [{{kv, Idx, N, Idx2}, Node} || {Idx2, Node} <- PL],
                    Add = Peers -- Latest,
                    Del = Latest -- Peers,
                    Changes =
                        [{add, Peer} || Peer <- Add]
                        ++ [{del, Peer} || Peer <- Del],
                    %% https://github.com/basho/riak_ensemble/issues/129
                    %% Not sure it is safe to do updates this way, as future
                    %% ring changes may not take affect due to Vsn mismatches
                    case Changes of
                        [] ->
                            State;
                        _ ->
                            ?LOG_INFO("Changes ~p prompted by ring update",
                                        [Changes]),
                            State2 = maybe_async_update(Changes, State),
                            State2
                    end
            end;
        false ->
            State#state{last_ring_id = CurrentRingID,
                        stable_ring_count = StableRingCount}
    end.


maybe_async_update(Changes, State=#state{async=Async}) ->
    CurrentAsync = is_pid(Async) andalso is_process_alive(Async),
    case CurrentAsync of
        true ->
            State;
        false ->
            Self = self(),
            Async2 = spawn(fun() ->
                                   riak_ensemble_peer:update_members(Self, Changes, 5000)
                           end),
            State#state{async=Async2}
    end.

%%===================================================================

ping(From, State=#state{proxy=Proxy}) ->
    catch Proxy ! {ensemble_ping, From},
    {async, State}.

-spec ready_to_start() -> boolean().
ready_to_start() ->
    lists:member(riak_kv, riak_core_node_watcher:services(node())).

synctree_path(_Ensemble, Id) ->
    {{kv, PL, N, Idx}, _} = Id,
    Bin = term_to_binary({PL, N}),
    %% Use a prefix byte to leave open the possibility of different
    %% tree id encodings (eg. not term_to_binary) in the future.
    TreeId = <<0, Bin/binary>>,
    {TreeId, "kv_" ++ integer_to_list(Idx)}.
