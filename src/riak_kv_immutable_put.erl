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

% TODO change this to gen_server or simple proc
-behaviour(gen_fsm).

%% API
-export([start_link/1]).

%% gen_fsm callbacks
-export([init/1,
    wait/2,
    handle_event/3,
    handle_sync_event/4,
    handle_info/3,
    terminate/3,
    code_change/4]).

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
%% This FSM will cast a set of ts_put messages to a computed set of
%% vnodes, and wait until a desired set of writes have succeeded.
%% Once the desired number of writes have succeeded (within a specified
%% timeout), this FSM will terminate and send an ok message back to
%% the caller.  If the desired number of writes have not responded within
%% a specified amount of time (default: 1sec), the FSM will terminate
%% and send an {error, timedout} message back to the caller.  This FSM
%% is guaranteed to terminate within the specified timeout in either one
%% of these scenarios.
%%
%% The behavior of this FSM is driven in part by the following pieces
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
    gen_fsm:start_link(?MODULE, [Args], []).

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

init({RObj, Options, From}) ->
    Bucket = riak_object:bucket(RObj),
    BKey = {Bucket, riak_object:key(RObj)},
    BucketProps = riak_core_bucket:get_bucket(riak_object:bucket(RObj)),
    DocIdx = riak_core_util:chash_key(BKey, BucketProps),
    NVal = riak_kv_util:get_option(n_val, BucketProps),
    PW = get(pw, BucketProps, NVal, Options),
    W = case get(dw, BucketProps, NVal, Options) of
        error   -> get(w, BucketProps, NVal, Options);
        DW      -> erlang:max(DW, get(w, BucketProps, NVal, Options))
    end,
    % TODO handle malformed W, PW values; raise error in init ?
    Timeout = riak_kv_util:get_option(timeout, Options, 1000),
    Preflist =
        case riak_kv_util:get_option(sloppy_quorum, Options, true) of
            true ->
                UpNodes = riak_core_node_watcher:nodes(riak_kv),
                riak_core_apl:get_apl_ann(DocIdx, NVal, UpNodes);
            false ->
                riak_core_apl:get_primary_apl(DocIdx, NVal, riak_kv)
        end,
    %% Cast a ts_put to all nodes in the selected preflist.  Response will be sent
    %% back to this proc.  See handle_info({ts_reply, ...})
    [begin
         Proxy = riak_core_vnode_proxy:reg_name(riak_kv_vnode, Idx),
         {Proxy, Node} ! {ts_put, self(), RObj, Type},
         ok
     end || {{Idx, Node}, Type} <- Preflist],
    {
        ok, wait,
        #state{w = W, pw = PW, timeout_at = current_ms() + Timeout, from = From},
        Timeout
    }.

wait(timeout, #state{timeout_at = TimeoutAt} = State) ->
    CurrentMs = current_ms(),
    if
        CurrentMs >= TimeoutAt ->
            {stop, timed_out, State};
        ?ELSE ->
            %% ASSERT TimeoutAt \geq current_ms()
            {next_state, wait, State, TimeoutAt - current_ms()}
    end.

handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

handle_sync_event(_Event, _From, StateName, State) ->
    Reply = ok,
    {reply, Reply, StateName, State}.

handle_info(
    {ts_reply, _Reply, Type}, _StateName,
    #state{
        pw = PW, w = W,
        primaries = Primaries, total = Total,
        timeout_at = TimeoutAt
    } = State) ->

    NewPrimaries = case Type of
        primary ->
            Primaries + 1;
        fallback ->
            Primaries
    end,
    NewTotal = Total + 1,

    if
        PW /= error andalso NewPrimaries >= PW ->
            {stop, ok, State};
        PW =:= error andalso NewTotal >= W ->
            {stop, ok, State};
        ?ELSE ->
            %% ASSERT TimeoutAt \geq current_ms()
            Timeout = TimeoutAt - current_ms(),
            {next_state, wait, State#state{primaries = NewPrimaries, total = NewTotal}, Timeout}
    end;
handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

terminate(Reason, _StateName, #state{from = From} = _State) ->
    From ! {self(), Reason},
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================


get(Key, BucketProps, N, Options) ->
    riak_kv_util:expand_rw_value(
        Key,
        riak_kv_util:get_option(Key, Options, default),
        BucketProps, N
    ).

current_ms() ->
    {MegaSecs, Secs, MicroSecs} = erlang:now(),
    (MegaSecs*1000000 + Secs)*1000000 + MicroSecs.