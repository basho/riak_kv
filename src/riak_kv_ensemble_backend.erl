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
%% Implementation of {@link riak_ensemble_backend} behavior that
%% connects riak_ensemble to riak_kv vnodes. 
%%
%% TODO: Move to riak_kv. Before PR, document more.

-module(riak_kv_ensemble_backend).
-behaviour(riak_ensemble_backend).

-export([init/3, new_obj/4]).
-export([obj_epoch/1, obj_seq/1, obj_key/1, obj_value/1]).
-export([set_obj_epoch/2, set_obj_seq/2, set_obj_value/2]).
-export([get/3, put/4, tick/5]).
-export([reply/2]).

-include_lib("riak_ensemble/include/riak_ensemble_types.hrl").

-record(state, {ensemble  :: ensemble_id(),
                id        :: peer_id(),
                proxy     :: atom()}).

-type obj()    :: riak_object:riak_object().
-type state()  :: #state{}.
-type key()    :: {_,_}.
-type value()  :: any().

%%===================================================================

-spec init(ensemble_id(), peer_id(), [any()]) -> state().
init(Ensemble, Id, []) ->
    {{kv, _PL, _N, Idx}, _} = Id,
    Proxy = riak_core_vnode_proxy:reg_name(riak_kv_vnode, Idx),
    #state{ensemble=Ensemble,
           id=Id,
           proxy=Proxy}.

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

-spec tick(epoch(), seq(), peer_id(), views(), state()) -> state().
tick(_Epoch, _Seq, _Leader, Views, State=#state{id=Id}) ->
    {{kv, Idx, N, _}, _} = Id,
    Latest = hd(Views),
    {ok, CHBin} = riak_core_ring_manager:get_chash_bin(),
    {PL, _} = chashbin:itr_pop(N, chashbin:exact_iterator(Idx, CHBin)),
    %% TODO: Make ensembles/peers use ensemble/peer as actual peer name so this is unneeded
    Peers = [{{kv, Idx, N, Idx2}, Node} || {Idx2, Node} <- PL],
    Add = Peers -- Latest,
    Del = Latest -- Peers,
    Changes = [{add, Peer} || Peer <- Add] ++ [{del, Peer} || Peer <- Del],
    case Changes of
        [] ->
            ok;
        _ ->
            io:format("~p~n", [Changes]),
            spawn(fun() ->
                          riak_ensemble_peer:update_members(self(), Changes, 5000)
                  end),
            ok
    end,
    State.
