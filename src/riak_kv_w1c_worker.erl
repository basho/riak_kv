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
-module(riak_kv_w1c_worker).

-behaviour(gen_server).

%% API
-export([start_link/1, put/2, workers/0]).
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-include_lib("riak_kv_vnode.hrl").

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

%% @spec put(RObj :: riak_object:riak_object(), riak_object:options(), riak_object:riak_client()) ->
%%        ok |
%%       {error, timeout} |
%%       {error, term()}
put(RObj, Options) ->
    StartTS = os:timestamp(),
    Bucket = riak_object:bucket(RObj),
    case riak_object:get_ts_local_key(RObj) of
        {ok, LocalKey} ->
            Key = LocalKey,
            EncodeFn =
                fun(XRObj) ->
                    riak_object:to_binary(v1, XRObj, msgpack)
                end;
        error ->
            Key = riak_object:key(RObj),
            EncodeFn = fun(XRObj) -> riak_object:to_binary(v1, XRObj) end
    end,
    BKey = {Bucket, riak_object:key(RObj)},
    BucketProps = riak_core_bucket:get_bucket(Bucket),
    DocIdx = riak_core_util:chash_key(BKey, BucketProps),
    NVal = proplists:get_value(n_val, BucketProps),
    Preflist =
        case proplists:get_value(sloppy_quorum, Options, true) of
            true ->
                UpNodes = riak_core_node_watcher:nodes(riak_kv),
                riak_core_apl:get_apl_ann(DocIdx, NVal, UpNodes);
            false ->
                riak_core_apl:get_primary_apl(DocIdx, NVal, riak_kv)
        end,
    case validate_options(NVal, Preflist, Options, BucketProps) of
        {ok, W, PW} ->
            Workers = workers(),
            R = random(size(Workers)),
            Worker = element(R, workers()),
            ReqId = erlang:monitor(process, Worker),
            RObj2 = riak_object:set_vclock(RObj, vclock:fresh(<<0:8>>, 1)),
            RObj3 = riak_object:update_last_modified(RObj2),
            RObj4 = riak_object:apply_updates(RObj3),
            EncodedVal = EncodeFn(RObj4),
            gen_server:cast(
                Worker,
                {put, Bucket, Key, EncodedVal, ReqId, Preflist,
                 #rec{w=W, pw=PW, n_val=NVal, from=self(),
                      start_ts=StartTS,
                      size=size(EncodedVal)}}),
            Timeout = case proplists:get_value(timeout, Options, ?DEFAULT_TIMEOUT) of
                          N when is_integer(N) andalso N > 0 ->
                              N;
                          _ ->
                              ?DEFAULT_TIMEOUT
                      end,
            % this is not direct gen_server:call because the process that
            % replies is not the one that received the cast
            receive
                {'DOWN', ReqId, process, _Pid, _Reason} ->
                    {error, riak_kv_w1c_server_crashed};
                {ReqId, Response} ->
                    erlang:demonitor(ReqId, [flush]),
                    Response
            after Timeout ->
                gen_server:cast(Worker, {cancel, ReqId}),
                receive
                    {ReqId, Response} ->
                        Response
                end
            end;
        Error ->
            Error
    end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init(_) ->
    {ok, #state{}}.

handle_call(_Request, _From, State) ->
    {reply, undefined, State}.

handle_cast({put, Bucket, Key, EncodedVal, ReqId, Preflist, #rec{from=From}=Rec}, #state{proxies=Proxies}=State) ->
    NewState = case store_request_record(ReqId, Rec, State) of
        {undefined, S} ->
            S#state{proxies=send_vnodes(Preflist, Proxies, Bucket, Key, EncodedVal, ReqId)};
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

handle_info({ReqId, ?KV_W1C_PUT_REPLY{reply=Reply, type=Type}}, State) ->
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

erase_request_record(ReqId, #state{entries=Entries} = State) ->
    case get_request_record(ReqId, State) of
        undefined ->
            {undefined, State};
        OldValue ->
            {OldValue, State#state{entries=?DICT_TYPE:erase(ReqId, Entries)}}
    end.