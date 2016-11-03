%% -------------------------------------------------------------------
%% Copyright (c) 2015, 2016 Basho Technologies, Inc. All Rights Reserved.
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
-module(riak_kv_w1c_worker).

-behaviour(gen_server).

%% API
-export([start_link/1, put/2, async_put/8, async_put_replies/2,
         ts_batch_put/8, ts_batch_put_encoded/2,
         workers/0, validate_options/4]).
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-include_lib("riak_kv_vnode.hrl").
-include("riak_kv_wm_raw.hrl").

-record(rec, {
    w, pw, n_val,
    start_ts,
    size,
    primary_okays = 0,
    fallback_okays = 0,
    errors = [],
    from
}).

-define(DICT_TYPE, dict).

-record(state, {
    entries = ?DICT_TYPE:new(),
    proxies = ?DICT_TYPE:new()
}).

-define(DEFAULT_TIMEOUT, 60000).


%%%===================================================================
%%% API
%%%===================================================================

workers() ->
    {
        riak_kv_w1c_worker00,
        riak_kv_w1c_worker01,
        riak_kv_w1c_worker02,
        riak_kv_w1c_worker03,
        riak_kv_w1c_worker04,
        riak_kv_w1c_worker05,
        riak_kv_w1c_worker06,
        riak_kv_w1c_worker07
    }.

%% @spec start_link(atom()) -> {ok, pid()} | ignore | {error, term()}
start_link(Name) ->
    gen_server:start_link({local, Name}, ?MODULE, [], []).

%% @spec put(RObj :: riak_object:riak_object(), proplists:proplist()) ->
%%        ok |
%%       {error, timeout} |
%%       {error, term()}
put(RObj0, Options) ->
    Bucket = riak_object:bucket(RObj0),
    case riak_core_bucket:get_bucket(Bucket) of
        {error, Reason} ->
            {error, Reason};
        BucketProps ->
            NVal = proplists:get_value(n_val, BucketProps),
            {RObj, Key, EncodeFn} =
                kv_or_ts_details(RObj0, riak_object:get_ts_local_key(RObj0)),
            PartitionIdx = chash_key(Bucket, Key, BucketProps),
            Preflist =
                case proplists:get_value(sloppy_quorum, Options, true) of
                    true ->
                        UpNodes = riak_core_node_watcher:nodes(riak_kv),
                        riak_core_apl:get_apl_ann(PartitionIdx, NVal, UpNodes);
                    false ->
                        riak_core_apl:get_primary_apl(PartitionIdx, NVal, riak_kv)
                end,

            case validate_options(NVal, Preflist, Options, BucketProps) of
                {ok, W, PW} ->
                    synchronize_put(
                      async_put(
                        RObj, W, PW, Bucket, NVal, choose_put_key(Key, PartitionIdx),
                        EncodeFn, Preflist),
                      Options);
                Error ->
                    Error
            end
    end.

%% `put/2' is invoked for timeseries data only when placing a
%% tombstone. In such a case, we need to replace the usual
%% {PartitionKey, LocalKey} tuple with {PartitionIndex, LocalKey} for
%% `async_put'
choose_put_key({_PK, LK}, PartitionIdx) ->
    {PartitionIdx, LK};
choose_put_key(Key, _Idx) ->
    Key.

-spec async_put(RObj :: riak_object:riak_object(),
                W :: pos_integer(),
                PW :: pos_integer(),
                Bucket :: binary()|{binary(), binary()},
                NVal :: pos_integer(),
                Key :: tuple()|{binary(), tuple()},
                EncodeFn :: fun((riak_object:riak_object()) -> binary()),
                Preflist :: term()) ->
                       {ok, {reference(), atom()}}.

async_put(RObj, W, PW, Bucket, NVal, {PartIdx, LK}, EncodeFn, Preflist) when is_tuple(LK) ->
    async_put(RObj, W, PW, Bucket, NVal, PartIdx, LK, EncodeFn, Preflist);
async_put(RObj, W, PW, Bucket, NVal, Key, EncodeFn, Preflist) ->
    async_put(RObj, W, PW, Bucket, NVal, undefined, Key, EncodeFn, Preflist).

async_put(RObj, W, PW, Bucket, NVal, PartitionIdx, Key, EncodeFn, Preflist) ->
    StartTS = os:timestamp(),
    Worker = random_worker(),
    ReqId = erlang:monitor(process, Worker),
    EncodedVal = EncodeFn(w1c_vclock(RObj)),
    BucketProps = riak_core_bucket:get_bucket(Bucket),

    gen_server:cast(
      Worker,
      {put, Bucket, BucketProps, PartitionIdx, Key, EncodedVal, ReqId, Preflist,
       #rec{w=W, pw=PW, n_val=NVal, from=self(),
            start_ts=StartTS,
            size=size(EncodedVal)}}),
    {ok, {ReqId, Worker}}.

-spec ts_batch_put(RObjs :: [{riak_object:key(), riak_object:riak_object()}],
                   W :: pos_integer(),
                   PW :: pos_integer(),
                   Bucket :: binary()|{binary(), binary()},
                   NVal :: pos_integer(),
                   EncodeFn :: fun((riak_object:riak_object()) -> binary()),
                   DocIdx :: chash:index(),
                   Preflist :: term()) ->
                          {ok, {reference(), atom()}}.

ts_batch_put(RObjs, W, PW, Bucket, NVal, EncodeFn, DocIdx, Preflist) ->
    StartTS = os:timestamp(),
    Worker = random_worker(),
    ReqId = erlang:monitor(process, Worker),
    BucketProps = riak_core_bucket:get_bucket(Bucket),
    EncodedVals =
        lists:map(fun({K, O}) -> {{Bucket, K}, EncodeFn(w1c_vclock(O))} end,
                  RObjs),
    Size = lists:sum(lists:map(fun({_BK, O}) -> size(O) end, EncodedVals)),

    gen_server:cast(
      Worker,
      {batch_put, Bucket, BucketProps, EncodedVals, ReqId, DocIdx, Preflist,
       #rec{w=W, pw=PW, n_val=NVal, from=self(),
            start_ts=StartTS,
            size=Size}}),
    {ok, {ReqId, Worker}}.

%% Primarily for repl support. Put a list of objects represented as
%% tuple({Bucket, LocalKey}, msgpack-encoded riak object) into the
%% correct partition
ts_batch_put_encoded([{{Bucket, _LK}, _RObj0}|_Rest]=RObjs, DocIdx) ->
    StartTS = os:timestamp(),
    Worker = random_worker(),
    ReqId = erlang:monitor(process, Worker),
    BucketProps = riak_core_bucket:get_bucket(Bucket),
    NVal = proplists:get_value(n_val, BucketProps),
    W = proplists:get_value(w, BucketProps),
    PW = proplists:get_value(pw, BucketProps),
    UpNodes = riak_core_node_watcher:nodes(riak_kv),
    Preflist = riak_core_apl:get_apl_ann(DocIdx, NVal, UpNodes),
    Size = lists:sum(lists:map(fun({_BK, O}) -> size(O) end, RObjs)),
    gen_server:cast(
      Worker,
      {batch_put, Bucket, BucketProps, RObjs, ReqId, DocIdx, Preflist,
       #rec{w=W, pw=PW, n_val=NVal, from=self(),
            start_ts=StartTS,
            size=Size}}),
    {ok, {ReqId, Worker}}.




w1c_vclock(RObj) ->
    RObj2 = riak_object:set_vclock(RObj, vclock:fresh(<<0:8>>, 1)),
    RObj3 = riak_object:update_last_modified(RObj2),
    riak_object:apply_updates(RObj3).

-spec async_put_replies(ReqIdTuples :: list({reference(), pid()}), proplists:proplist()) ->
                                       list(term()).
async_put_replies(ReqIdTuples, Options) ->
    async_put_reply_loop(ReqIdTuples, [], os:timestamp(),
                         find_put_timeout(Options)).


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init(_) ->
    {ok, #state{}}.

handle_call(_Request, _From, State) ->
    {reply, undefined, State}.

handle_cast({put, Bucket, BucketProps, PartitionIdx, Key, EncodedVal, ReqId, Preflist, #rec{from=From}=Rec}, #state{proxies=Proxies}=State) ->
    NewState = case store_request_record(ReqId, Rec, State) of
                   {undefined, S} ->
                       UpdProxies = send_vnodes(Preflist, Proxies, Bucket, Key, EncodedVal, ReqId),
                       postcommit(data_type_by_key(Key), Bucket,
                                  maybe_ts_markup(PartitionIdx, Bucket, Key, EncodedVal),
                                  BucketProps),
                       S#state{proxies=UpdProxies};
                   {_, S} ->
                       reply(From, ReqId, {error, request_id_already_defined}),
                       S
    end,
    {noreply, NewState};
handle_cast({batch_put, Bucket, BucketProps, EncodedVals, ReqId, PK, Preflist, #rec{from=From}=Rec}, #state{proxies=Proxies}=State) ->
    NewState = case store_request_record(ReqId, Rec, State) of
                   {undefined, S} ->
                       UpdProxies = batch_send_vnodes(Preflist, Proxies, EncodedVals, ReqId),
                       postcommit(ts, Bucket, {PK, EncodedVals}, BucketProps),
                       S#state{proxies=UpdProxies};
                   {_, S} ->
                       reply(From, ReqId, {error, request_id_already_defined}),
                       S
    end,
    {noreply, NewState};
handle_cast({cancel, ReqId}, State) ->
    NewState = case erase_request_record(ReqId, State) of
        {undefined, S} ->
            S;
        {#rec{from=From}, S} ->
            reply(From, ReqId, {error, timeout}),
            S
    end,
    {noreply, NewState};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({ReqId, ?KV_W1C_BATCH_PUT_REPLY{reply=Reply, type=Type}}, State) ->
    handle_put_reply(ReqId, Reply, Type, State);
handle_info({ReqId, ?KV_W1C_PUT_REPLY{reply=Reply, type=Type}}, State) ->
    handle_put_reply(ReqId, Reply, Type, State);
handle_info(_Msg, State) ->
    {noreply, State}.

handle_put_reply(ReqId, Reply, Type, State) ->
    case get_request_record(ReqId, State) of
        undefined->
            % the entry was likely purged by the timeout mechanism
            {noreply, State};
        Rec ->
            #rec{
                pw = PW, w = W, n_val=NVal,
                primary_okays = PrimaryOkays,
                fallback_okays = FallbackOkays,
                errors = Errors,
                from = From,
                start_ts = StartTS,
                size = Size
            } = Rec,
            {PrimaryOkays1, FallbackOkays1, Errors1} =
                case {Reply, Type} of
                    {ok, primary} ->
                        {PrimaryOkays + 1, FallbackOkays, Errors};
                    {ok, fallback} ->
                        {PrimaryOkays, FallbackOkays + 1, Errors};
                    {{error, Reason}, _} ->
                        {PrimaryOkays, FallbackOkays, [Reason | Errors]}
                end,
            % TODO use riak_kv_put_core here instead
            case enough(W, PW, NVal, PrimaryOkays1 + FallbackOkays1, PrimaryOkays1, length(Errors1)) of
                true ->
                    reply(From, ReqId, response(W, PW, NVal, PrimaryOkays1 + FallbackOkays1, PrimaryOkays1, Errors1, length(Errors))),
                    {_, NewState} = erase_request_record(ReqId, State),
                    Usecs = timer:now_diff(os:timestamp(), StartTS),
                    riak_kv_stat:update({write_once_put, Usecs, Size});
                false ->
                    NewRec = Rec#rec{
                        primary_okays = PrimaryOkays1,
                        fallback_okays = FallbackOkays1,
                        errors = Errors1
                    },
                    {_, NewState} = store_request_record(ReqId, NewRec, State)
            end,
            {noreply, NewState}
    end.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.




%%%===================================================================
%%% Internal functions
%%%===================================================================

%% First argument is a binary partition index for timeseries data or
%% `undefined' if this is routed through the standard KV data code
%% path
maybe_ts_markup(undefined, _Bucket, _LK, Val) ->
    Val;
maybe_ts_markup(PartitionIdx, Bucket, LK, Val) ->
    {PartitionIdx, {{Bucket, LK}, Val}}.

%% Timeseries local key will be a tuple of values
data_type_by_key(Key) when is_tuple(Key) ->
    ts;
data_type_by_key(_Key) ->
    kv.

postcommit(kv, _Bucket, _ValTuple, _BucketProps) ->
    %% No plans for general w1c hooks yet
    ok;
postcommit(ts, Bucket, ValTuple, BucketProps) ->
    riak_kv_hooks:call_timeseries_postcommit(ValTuple, Bucket, BucketProps).

random_worker() ->
    Workers = workers(),
    R = random(size(Workers)),
    element(R, workers()).

validate_options(NVal, Preflist, Options, BucketProps) ->
    PW = get_rw_value(pw, BucketProps, NVal, Options),
    W = case get_rw_value(dw, BucketProps, NVal, Options) of
            error   -> get_rw_value(w, BucketProps, NVal, Options);
            DW      -> erlang:max(DW, get_rw_value(w, BucketProps, NVal, Options))
        end,
    NumPrimaries = length([x || {_, primary} <- Preflist]),
    NumVnodes = length(Preflist),
    MinVnodes = lists:max([1, W, PW]),
    if
        PW =:= error ->
            {error, pw_val_violation};
        W =:= error ->
            {error, w_val_violation};
        (W > NVal) or (PW > NVal) ->
            {error, {n_val_violation, NVal}};
        PW > NumPrimaries ->
            {error, {pw_val_unsatisfied, PW, NumPrimaries}};
        NumVnodes < MinVnodes ->
            {error, {insufficient_vnodes, NumVnodes, need, MinVnodes}};
        true ->
            {ok, W, PW}
    end.

send_vnodes(Preflist, Proxies, Bucket, {_PK, LK}, EncodedVal, ReqId) when is_tuple(LK) ->
    send_vnodes(Preflist, Proxies, Bucket, LK, EncodedVal, ReqId);
send_vnodes([], Proxies, _Bucket, _Key, _EncodedVal, _ReqId) ->
    Proxies;
send_vnodes([{{Idx, Node}, Type}|Rest], Proxies, Bucket, Key, EncodedVal, ReqId) ->
    {Proxy, NewProxies} = get_proxy(Idx, Proxies),
    Message = ?KV_W1C_PUT_REQ{bkey={Bucket, Key}, encoded_obj=EncodedVal, type=Type},
    gen_fsm:send_event(
        {Proxy, Node},
        riak_core_vnode_master:make_request(Message, {raw, ReqId, self()}, Idx)
    ),
    send_vnodes(Rest, NewProxies, Bucket, Key, EncodedVal, ReqId).

batch_send_vnodes([], Proxies, _EncodedVals, _ReqId) ->
    Proxies;
batch_send_vnodes([{{Idx, Node}, Type}|Rest], Proxies, EncodedVals, ReqId) ->
    {Proxy, NewProxies} = get_proxy(Idx, Proxies),
    Message = ?KV_W1C_BATCH_PUT_REQ{objs=EncodedVals, type=Type},
    gen_fsm:send_event(
        {Proxy, Node},
        riak_core_vnode_master:make_request(Message, {raw, ReqId, self()}, Idx)
    ),
    batch_send_vnodes(Rest, NewProxies, EncodedVals, ReqId).

get_proxy(Idx, Proxies) ->
    case ?DICT_TYPE:find(Idx, Proxies) of
        {ok, Value} ->
            {Value, Proxies};
        error ->
            Proxy = riak_core_vnode_proxy:reg_name(riak_kv_vnode, Idx),
            {Proxy, ?DICT_TYPE:store(Idx, Proxy, Proxies)}
    end.


enough(W, PW, _NVal, NumW, _NumPW, _NumErrors)  when W =< NumW, PW =< 0 ->
    true;
enough(W, PW, _NVal, NumW, NumPW, _NumErrors)   when W =< NumW, 0 < PW, PW =< NumPW ->
    true;
enough(W, PW, NVal, _NumW, _NumPW, NumErrors)   when PW =< 0, (NVal - NumErrors) < W ->
    true;
enough(_W, PW, NVal, _NumW, _NumPW, NumErrors)  when 0 < PW, (NVal - NumErrors) < PW ->
    true;
enough(_W, _PW, _NVal, _NumW, _NumPW, _NumErrors) ->
    false.

response(W, PW, _NVal, NumW, _NumPW, _Errors, _NumErrors)   when W =< NumW, PW =< 0 ->
    ok;
response(W, PW, _NVal, NumW, NumPW, _Errors, _NumErrors)    when W =< NumW, 0 < PW, PW =< NumPW ->
    ok;
response(W, PW, NVal, NumW, _NumPW, Errors, NumErrors)      when PW =< 0, (NVal - NumErrors) < W ->
    overload_trumps(Errors, {error, {w_val_unsatisfied, W, NumW}});
response(_W, PW, NVal, _NumW, NumPW, Errors, NumErrors)     when 0 < PW, (NVal - NumErrors) < PW ->
    overload_trumps(Errors, {error, {pw_val_unsatisfied, PW, NumPW}}).


overload_trumps(Errors, Default) ->
    case lists:member({error, overload}, Errors) of
        true ->
            {error, overload};
        _->
            Default
    end.

reply(From, ReqId, Term) ->
    From ! {ReqId, Term}.


get_rw_value(Key, BucketProps, N, Options) ->
    riak_kv_util:expand_rw_value(
        Key,
        proplists:get_value(Key, Options, default),
        BucketProps, N
    ).


random(N) ->
    erlang:phash2(erlang:statistics(io), N) + 1.


store_request_record(ReqId, Rec, #state{entries=Entries} = State) ->
    case get_request_record(ReqId, State) of
        undefined ->
            {undefined, State#state{entries=?DICT_TYPE:store(ReqId, Rec, Entries)}};
        OldValue ->
            {OldValue, State#state{entries=?DICT_TYPE:store(ReqId, Rec, Entries)}}
    end.

get_request_record(ReqId, #state{entries=Entries} = _State) ->
    case ?DICT_TYPE:find(ReqId, Entries) of
        {ok, Value} -> Value;
        error -> undefined
    end.

%% Utility function for put/2: is this a TS object with its special
%% requirements or a more traditional KV object?
%%
%% When riak_kv_ts_svc is driving a put request, it can provide
%% all of these details directly to async_put/8, but when a tombstone
%% is being put via riak_kv_delete, these must be extracted from the
%% object.
kv_or_ts_details(RObj, {ok, LocalKey}) ->
    MD  = riak_object:get_metadata(RObj),
    MD1 = dict:erase(?MD_TS_LOCAL_KEY, MD),
    RObj1 = riak_object:update_metadata(RObj, MD1),
    {RObj1, {riak_object:key(RObj), LocalKey},
     fun(O) -> riak_object:to_binary(v1, O, erlang) end};
kv_or_ts_details(RObj, error) ->
    {RObj, riak_object:key(RObj), fun(O) -> riak_object:to_binary(v1, O) end}.

erase_request_record(ReqId, #state{entries=Entries} = State) ->
    case get_request_record(ReqId, State) of
        undefined ->
            {undefined, State};
        OldValue ->
            {OldValue, State#state{entries=?DICT_TYPE:erase(ReqId, Entries)}}
    end.

%%%% Functions to handle asynchronous puts

%% Invoked by put/2 to turn the async request into a synchronous call
synchronize_put({ok, {_ReqId, _Worker}=ReqIdTuple}, Options) ->
    wait_for_put_reply(ReqIdTuple, find_put_timeout(Options)).

%% Invoked by async_put_reply/2 to wait for all responses
async_put_reply_loop([], Responses, _StartTime, _Timeout) ->
    Responses;
async_put_reply_loop([IdTuple|IdTuples], Responses, StartTime, Timeout) ->
    async_put_reply_loop(IdTuples,
                         [wait_for_put_reply(IdTuple,
                                             remaining_put_timeout(
                                               StartTime, Timeout))
                          |Responses],
                         StartTime, Timeout).

wait_for_put_reply({ReqId, Worker}, Timeout) ->
    receive
        {'DOWN', ReqId, process, _Pid, _Reason} ->
            {error, riak_kv_w1c_server_crashed};
        {ReqId, Response} ->
            erlang:demonitor(ReqId, [flush]),
            Response
    after Timeout ->
            erlang:demonitor(ReqId, [flush]),
            gen_server:cast(Worker, {cancel, ReqId}),
            {error, timeout}
    end.

non_neg(X) ->
    max(0, X).

remaining_put_timeout(Start, Timeout) ->
    MicroSecondsSinceStart =
        non_neg(trunc(timer:now_diff(os:timestamp(), Start))),
    %% Convert to milliseconds before calculating the new timeout
    non_neg(Timeout - (MicroSecondsSinceStart div 1000)).

find_put_timeout(Options) ->
    case proplists:get_value(timeout, Options, ?DEFAULT_TIMEOUT) of
        N when is_integer(N) andalso N > 0 ->
            N;
        _ ->
            ?DEFAULT_TIMEOUT
    end.

chash_key(Bucket, {PartitionKey, _LocalKey}, BucketProps) ->
    riak_core_util:chash_key({Bucket, PartitionKey}, BucketProps);
chash_key(Bucket, Key, BucketProps) ->
    riak_core_util:chash_key({Bucket, Key}, BucketProps).
