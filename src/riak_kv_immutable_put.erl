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
-module(riak_kv_immutable_put).

%% API
-export([start_link/1]).

-define(SERVER, ?MODULE).

-record(state, {
    w, pw,
    primaries = 0,
    total = 0,
    timeout_at,
    from
}).

-define(ELSE, true). % improves readability of if-then-else clauses


%%
%% Description:
%%
%% This module defines an intermediate process that stands between
%% riak_client and a collection of riak_kv_vnodes, mediating asynchronous
%% put operations in the special case where the immutable flag is
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

start_link(Args) ->
    % TODO add monitor
    From = self(),
    {ok, spawn_link(fun() -> start(Args, From) end)}.



%%%===================================================================
%%% Internal functions
%%%===================================================================

start({RObj, Options, RecvTimeout}, From) ->
    Bucket = riak_object:bucket(RObj),
    BKey = {Bucket, riak_object:key(RObj)},
    BucketProps = riak_core_bucket:get_bucket(riak_object:bucket(RObj)),
    DocIdx = riak_core_util:chash_key(BKey, BucketProps),
    NVal = riak_kv_util:get_option(n_val, BucketProps),
    PW = get(pw, BucketProps, NVal, Options),
    %% W (writes) is max of configured writes and durable writes (if specified)
    W = case get(dw, BucketProps, NVal, Options) of
            error   -> get(w, BucketProps, NVal, Options);
            DW      -> erlang:max(DW, get(w, BucketProps, NVal, Options))
        end,
    Timeout = erlang:max(RecvTimeout, 0),
    Preflist =
        case riak_kv_util:get_option(sloppy_quorum, Options, true) of
            true ->
                UpNodes = riak_core_node_watcher:nodes(riak_kv),
                riak_core_apl:get_apl_ann(DocIdx, NVal, UpNodes);
            false ->
                riak_core_apl:get_primary_apl(DocIdx, NVal, riak_kv)
        end,
    %% Cast a ts_put to all nodes in the selected preflist.  Response will be sent
    %% back to this proc.  See wait({ts_reply, ...})
    [begin
         Proxy = riak_core_vnode_proxy:reg_name(riak_kv_vnode, Idx),
         {Proxy, Node} ! {ts_put, self(), RObj, Type},
         ok
     end || {{Idx, Node}, Type} <- Preflist],
    wait(#state{w = W, pw = PW, timeout_at = current_ms() + Timeout, from = From}).

wait(
    #state{
        pw = PW, w = W,
        primaries = Primaries, total = Total,
        timeout_at = TimeoutAt,
        from = From
    } = State
) ->
    Timeout = erlang:max(TimeoutAt - current_ms(), 0),
    receive
        {ts_reply, _Reply, Type} ->
            NewPrimaries = case Type of
                               primary ->
                                   Primaries + 1;
                               fallback ->
                                   Primaries
                           end,
            NewTotal = Total + 1,
            case enoughReplies(W, PW, NewPrimaries, NewTotal) of
                true ->
                    reply(From, ok);
                false ->
                    wait(State#state{primaries = NewPrimaries, total = NewTotal})
            end;
        Reply ->
            reply(From, {error, spurious_reply, Reply})
    after
        Timeout ->
            reply(From, {error, timeout})
    end.

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
        riak_kv_util:get_option(Key, Options, default),
        BucketProps, N
    ).

current_ms() ->
    % TODO use os:timestamp instead?  Lower overhead?
    {MegaSecs, Secs, MicroSecs} = erlang:now(),
    (MegaSecs*1000000 + Secs)*1000 + erlang:round(MicroSecs/1000).