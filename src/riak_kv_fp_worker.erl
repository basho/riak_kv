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
-module(riak_kv_fp_worker).

-behaviour(gen_server).

%% API
-export([start_link/1, put/2, workers/0]).
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-record(rec, {
    w, pw,
    primaries = 0,
    total = 0,
    from
}).

-define(DICT_TYPE, dict).

-record(state, {
    entries = ?DICT_TYPE:new()
}).

-define(DEFAULT_TIMEOUT, 60000).


%%%===================================================================
%%% API
%%%===================================================================

workers() ->
    {
        riak_kv_fp_worker0,
        riak_kv_fp_worker1,
        riak_kv_fp_worker2,
        riak_kv_fp_worker3,
        riak_kv_fp_worker4,
        riak_kv_fp_worker5,
        riak_kv_fp_worker6,
        riak_kv_fp_worker7
    }.

start_link(Name) ->
    gen_server:start_link({local, Name}, ?MODULE, [], []).

put(RObj, Options) ->
    Bucket = riak_object:bucket(RObj),
    BKey = {Bucket, riak_object:key(RObj)},
    BucketProps = riak_core_bucket:get_bucket(riak_object:bucket(RObj)),
    DocIdx = riak_core_util:chash_key(BKey, BucketProps),
    NVal = proplists:get_value(n_val, BucketProps),
    PW = get(pw, BucketProps, NVal, Options),
    W = case get(dw, BucketProps, NVal, Options) of
            error   -> get(w, BucketProps, NVal, Options);
            DW      -> erlang:max(DW, get(w, BucketProps, NVal, Options))
        end,
    Preflist =
        case proplists:get_value(sloppy_quorum, Options, true) of
            true ->
                UpNodes = riak_core_node_watcher:nodes(riak_kv),
                riak_core_apl:get_apl_ann(DocIdx, NVal, UpNodes);
            false ->
                riak_core_apl:get_primary_apl(DocIdx, NVal, riak_kv)
        end,
    Workers = workers(),
    R = random(size(Workers)),
    Worker = element(R, workers()),
    ReqId = erlang:monitor(process, Worker),
    RObj2 = riak_object:set_vclock(RObj, vclock:fresh(ReqId, 1)),
    RObj3 = riak_object:update_last_modified(RObj2),
    RObj4 = riak_object:apply_updates(RObj3),
    gen_server:cast(Worker,
        {put, RObj4, ReqId, Preflist, #rec{w=W, pw=PW, from=self()}}
    ),
    Timeout = case proplists:get_value(timeout, Options, ?DEFAULT_TIMEOUT) of
        N when is_integer(N) andalso N > 0 ->
            N;
        _ ->
            ?DEFAULT_TIMEOUT
    end,
    receive
        {'DOWN', ReqId, process, _Pid, _Reason} ->
            {error, riak_kv_fast_put_server_crashed};
        {ReqId, Response} ->
            erlang:demonitor(ReqId),
            Response
    after Timeout ->
        gen_server:cast(Worker, {cancel, ReqId}),
        receive
            {ReqId, Response} ->
                Response
        end
    end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init(_) ->
    {ok, #state{}}.

handle_call(_Request, _From, State) ->
    {reply, undefined, State}.

handle_cast({put, RObj, ReqId, Preflist, #rec{from=From}=Rec}, State) ->
    NewState = case do_put(ReqId, Rec, State) of
        {undefined, S} ->
            [begin
                 Proxy = riak_core_vnode_proxy:reg_name(riak_kv_vnode, Idx),
                 {Proxy, Node} ! {ts_put, self(), RObj, ReqId, Type},
                 ok
             end || {{Idx, Node}, Type} <- Preflist],
            S;
        {_AlreadyDefined, S} ->
            reply(From, ReqId, {error, request_id_already_defined}),
            S
    end,
    {noreply, NewState};
handle_cast({cancel, ReqId}, State) ->
    NewState = case do_erase(ReqId, State) of
        {undefined, S} ->
            S;
        {#rec{from=From}, S} ->
            reply(From, ReqId, {error, timeout}),
            S
    end,
    {noreply, NewState};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({ts_reply, ReqId, _Reply, Type}, State) ->
    case do_get(ReqId, State) of
        undefined->
            % the entry was likely purged by the timeout mechanism
            {noreply, State};
        Rec ->
            #rec{
                pw = PW, w = W,
                primaries = Primaries, total = Total,
                from = From
            } = Rec,
            NewPrimaries = case Type of
                               primary ->
                                   Primaries + 1;
                               fallback ->
                                   Primaries
                           end,
            NewTotal = Total + 1,
            NewState = case enoughReplies(W, PW, NewPrimaries, NewTotal) of
                true ->
                    {_, S} = do_erase(ReqId, State),
                    reply(From, ReqId, ok),
                    S;
                false ->
                    {_, S} = do_put(ReqId, Rec#rec{primaries = NewPrimaries, total = NewTotal}, State),
                    S
            end,
            {noreply, NewState}
    end;
handle_info(_Msg, State) ->
    {noreply, State}.

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

reply(From, ReqId, Term) ->
    From ! {ReqId, Term}.


get(Key, BucketProps, N, Options) ->
    riak_kv_util:expand_rw_value(
        Key,
        proplists:get_value(Key, Options, default),
        BucketProps, N
    ).

%% See riak_kv_ensemble_router:random for justification
random(N) ->
    erlang:phash2(erlang:statistics(io), N) + 1.


do_put(ReqId, Rec, #state{entries=Entries} = State) ->
    case do_get(ReqId, State) of
        undefined ->
            {undefined, State#state{entries=?DICT_TYPE:store(ReqId, Rec, Entries)}};
        OldValue ->
            {OldValue, State#state{entries=?DICT_TYPE:store(ReqId, Rec, Entries)}}
    end.

do_get(ReqId, #state{entries=Entries} = _State) ->
    case ?DICT_TYPE:find(ReqId, Entries) of
        {ok, Value} -> Value;
        error -> undefined
    end.

do_erase(ReqId, #state{entries=Entries} = State) ->
    case do_get(ReqId, State) of
        undefined ->
            {undefined, State};
        OldValue ->
            {OldValue, State#state{entries=?DICT_TYPE:erase(ReqId, Entries)}}
    end.
