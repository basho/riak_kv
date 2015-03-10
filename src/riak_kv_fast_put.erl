%% -------------------------------------------------------------------
%% Copyright (c) 2015 Basho Technologies, Inc. All Rights Reserved.
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
-module(riak_kv_fast_put).

-behaviour(gen_server).

%% API
-export([start_link/3]).
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-record(state, {
    w, pw,
    primaries = 0,
    total = 0,
    from
}).

-define(DEFAULT_TIMEOUT, 60000).

%%
%% Description:
%%
%% This module defines an intermediate proc that stands between
%% riak_client and a collection of riak_kv_vnodes, mediating asynchronous
%% put operations in the special case where the fast_path flag is
%% set on a bucket.
%%
%% This proc will cast a set of ts_put messages to a computed set of
%% vnodes, and wait until a desired set of writes have succeeded.
%% Once the desired number of writes have succeeded (within a specified
%% timeout), the proc will terminate and send an ok message back to
%% the caller.  If the desired number of writes have not responded within
%% a specified amount of time (default: 1sec), the proc will terminate
%% and send an {error, timeout} message back to the caller.  This proc
%% is guaranteed to terminate within the specified timeout in either one
%% of these scenarios.
%%
%% The behavior of the proc is driven in part by the following pieces
%% of configuration:
%%
%% sloppy_quorum
%%      if set to true (default), writes will go to primary and fallback vnodes
%%      otherwise, only primary vnodes will be asked to write
%% w
%% pw
%% dw
%% timeout
%%

%%%===================================================================
%%% API
%%%===================================================================

start_link(Self, RObj, Options) ->
    gen_server:start_link(?MODULE, [Self, RObj, Options], []).

init(Args) ->
    gen_server:cast(self(), {async_init, Args}),
    {ok, #state{}}.

async_init([From, RObj, Options], State) ->
    Bucket = riak_object:bucket(RObj),
    BKey = {Bucket, riak_object:key(RObj)},
    BucketProps = riak_core_bucket:get_bucket(riak_object:bucket(RObj)),
    DocIdx = riak_core_util:chash_key(BKey, BucketProps),
    NVal = proplists:get_value(n_val, BucketProps),
    PW = get(pw, BucketProps, NVal, Options),
    %% W (writes) is max of configured writes and durable writes (if specified)
    W = case get(dw, BucketProps, NVal, Options) of
            error   -> get(w, BucketProps, NVal, Options);
            DW      -> erlang:max(DW, get(w, BucketProps, NVal, Options))
        end,
    case proplists:get_value(timeout, Options, ?DEFAULT_TIMEOUT) of
        N when is_integer(N) andalso N > 0 ->
            timer:send_after(N, timeout);
        _ ->
            % TODO handle bad timoeout
            ok
    end,
    Preflist =
        case proplists:get_value(sloppy_quorum, Options, true) of
            true ->
                UpNodes = riak_core_node_watcher:nodes(riak_kv),
                riak_core_apl:get_apl_ann(DocIdx, NVal, UpNodes);
            false ->
                riak_core_apl:get_primary_apl(DocIdx, NVal, riak_kv)
        end,
    %% Cast a ts_put to all nodes in the selected preflist.  Response will be sent
    %% back to this proc.  See wait({ts_reply, ...})
    {RandomId, _} = random:uniform_s(1000000000, os:timestamp()),
    RObj2 = riak_object:set_vclock(RObj, vclock:fresh(RandomId, 1)),
    RObj3 = riak_object:update_last_modified(RObj2),
    RObj4 = riak_object:apply_updates(RObj3),
    [begin
         Proxy = riak_core_vnode_proxy:reg_name(riak_kv_vnode, Idx),
         {Proxy, Node} ! {ts_put, self(), RObj4, Type},
         ok
     end || {{Idx, Node}, Type} <- Preflist],
    State#state{w = W, pw = PW, from = From}.


handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({async_init, Args}, State) ->
    {noreply, async_init(Args, State)}.

handle_info({ts_reply, _Reply, Type}, #state{
        pw = PW, w = W,
        primaries = Primaries, total = Total,
        from = From
    } = State) ->
    NewPrimaries = case Type of
                       primary ->
                           Primaries + 1;
                       fallback ->
                           Primaries
                   end,
    NewTotal = Total + 1,
    case enoughReplies(W, PW, NewPrimaries, NewTotal) of
        true ->
            reply(From, ok),
            {stop, normal, State};
        false ->
            {noreply, State#state{primaries = NewPrimaries, total = NewTotal}}
    end;
handle_info(timeout, #state{from = From} = State) ->
    reply(From, {error, timeout}),
    {stop, normal, State}.


terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.




%%%===================================================================
%%% Internal functions
%%%===================================================================

enoughReplies(W, PW, Primaries, TotalReplies) ->
    EnoughTotal = W =< TotalReplies,
    EnoughPrimaries = case PW of
                          error ->  true;
                          0     ->  true;
                          _ ->      PW =< Primaries
                      end,
    EnoughTotal andalso EnoughPrimaries.

reply(From, Term) ->
    From ! {self(), Term}.


get(Key, BucketProps, N, Options) ->
    riak_kv_util:expand_rw_value(
        Key,
        proplists:get_value(Key, Options, default),
        BucketProps, N
    ).
