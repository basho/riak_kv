%% -------------------------------------------------------------------
%%
%% riak_kv_pb_crdt: Expose crdts over Protocol Buffers
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

%% @doc <p>The CRDT PB service for Riak KV. This covers the
%% following request messages:</p>
%%
%% <pre>
%%  80 - DtFetchReq
%%  82 - DtUpdateReq
%% </pre>
%%
%% <p>This service produces the following responses:</p>
%%
%% <pre>
%%  81 - DtFetchResp
%%  83 - DtUpdateResp
%% </pre>
%%
%% @end

-module(riak_kv_pb_crdt).

-include_lib("riak_pb/include/riak_dt_pb.hrl").
-include_lib("riak_pb/include/riak_kv_pb.hrl").
-include_lib("riak_pb/include/riak_pb_kv_codec.hrl").
-include_lib("riak_kv_types.hrl").

-behaviour(riak_api_pb_service).

-export([init/0,
         decode/2,
         encode/1,
         process/2,
         process_stream/3]).

-import(riak_pb_kv_codec, [decode_quorum/1]).

-record(state, {client,
                type,
                mod,
                op,
                op_type,
                return_key,
                return_ctx,
                return_value}).

-define(DEFAULT_TIMEOUT, 60000).
-define(DEFAULT_BUCKET, <<"default">>).

%% @doc init/0 callback. Returns the service internal start
%% state.
-spec init() -> any().
init() ->
    {ok, C} = riak:local_client(),
    #state{client=C}.

%% @doc decode/2 callback. Decodes an incoming message.
decode(Code, Bin) ->
    Msg = riak_pb_codec:decode(Code, Bin),
    {ok, Msg, permission_for(Msg)}.

%% @doc encode/1 callback. Encodes an outgoing response message.
encode(Message) ->
    {ok, riak_pb_codec:encode(Message)}.

%% @doc process/2 callback. Handles an incoming request message.
process(#dtfetchreq{type=?DEFAULT_BUCKET}=Req0, State) ->
    %% Handle a typeless message
    %% @see downgrade_request/1 etc below for details
    Req = downgrade_request(Req0),
    process_legacy_counter(Req, State);
process(#dtfetchreq{bucket=B, type=BType}=Req, State) ->
    %% V2 fetch operation
    fetch_type(bucket_type_to_type(B, BType), Req, State);
process(#dtupdatereq{type=?DEFAULT_BUCKET}=Req0, State) ->
    %% Handle a typeless update message
    %% @see downgrade_request/1 etc below for details
    Req = downgrade_request(Req0),
    process_legacy_counter(Req, State);
process(#dtupdatereq{bucket=B, type=BType}=Req, State) ->
    %% V2 update operation
    update_type(bucket_type_to_type(B, BType), Req, State).

%% @doc process_stream/3 callback. This service does not create any
%% streaming responses and so ignores all incoming messages.
process_stream(_,_,State) ->
    {ignore, State}.

%% ===================================================================
%% Internal functions
%% ===================================================================

%% @private get a Data Type value
fetch_type({ok, {true, Type}}, Req, State) ->
    Mod = riak_kv_crdt:to_mod(Type),
    Supported = riak_kv_crdt:supported(Mod),
    maybe_fetch(Supported, Req, State#state{mod=Mod, type=Type});
fetch_type({ok, {false, _Type}}, _Req, State) ->
    {error, {format, "Bucket must be allow_mult=true"}, State};
fetch_type({error, no_type}, #dtfetchreq{type=BType}, State) ->
    {error, {format, "Error no bucket type `~p`", [BType]}, State}.

maybe_fetch(true, Req, State) ->
    #dtfetchreq{bucket=B, key=K, type=BType, include_context=InclCtx} = Req,
    #state{client=C} = State,
    Options = make_options(Req),
    Resp = C:get({BType, B}, K, [{crdt_op, State#state.mod}|Options]),
    process_fetch_response(Resp, State#state{return_ctx=InclCtx});
maybe_fetch(false, _Req, State) ->
    #state{type=Type} = State,
    {error, {format, "`~p` is not a supported type", [Type]}, State}.

%% @private prepare a repsonse for a fetch operation
process_fetch_response({ok, O}, State) ->
    #state{type=Type, mod=Mod, return_ctx=ReturnCtx} = State,
    {{Ctx0, Value}, Stats} = riak_kv_crdt:value(O, Mod),
    [ riak_kv_stat:update(S) || S <- Stats ],
    Ctx = get_context(Ctx0, ReturnCtx),
    Resp = riak_pb_dt_codec:encode_fetch_response(Type, Value, Ctx, ?MOD_MAP),
    {reply, Resp, State};
process_fetch_response({error, notfound}, State) ->
    #state{type=Type} = State,
    Resp = riak_pb_dt_codec:encode_fetch_response(Type, undefined, undefined),
    {reply, Resp, State};
process_fetch_response({error, Reason}, State) ->
    {error, {format,Reason}, State}.

update_type({ok, {true, Type}}, #dtupdatereq{op=Op0}=Req, State0) ->
    Mod = riak_kv_crdt:to_mod(Type),
    Supported = riak_kv_crdt:supported(Mod),
    Op = riak_pb_dt_codec:decode_operation(Op0, ?MOD_MAP),
    OpType = riak_pb_dt_codec:operation_type(Op0),
    ModsMatch = mods_match(Mod, OpType),
    State = State0#state{mod=Mod, type=Type, op=Op, op_type=OpType},
    maybe_update({Supported, ModsMatch}, Req, State);
update_type({ok, {false, _Type}}, _Req, State) ->
    {error, {format, "Bucket must be allow_mult=true"}, State};
update_type({error, no_type}, #dtupdatereq{type=BType}, State) ->
    {error, {format, "Error no bucket type `~p`", [BType]}, State}.

maybe_update({true, true}, Req, State0) ->
    #dtupdatereq{bucket=B, key=K, type=BType,
                 include_context=InclCtx,
                 context=Ctx} = Req,
    #state{client=C, mod=Mod, op=Op} = State0,
    {Key, ReturnKey} = get_key(K),
    O = riak_kv_crdt:new({BType, B}, Key, Mod),
    Options0 = make_options(Req),
    CrdtOp = make_operation(Mod, Op, Ctx),
    Options = [{crdt_op, CrdtOp},
               {retry_put_coordinator_failure, false}] ++ Options0,
    Resp =  C:put(O, Options),
    State = State0#state{return_key=ReturnKey, return_ctx=InclCtx},
    process_update_response(Resp, State);
maybe_update({false, _}, _Req, State ) ->
    #state{type=Type} = State,
    {error, {format, "Bucket datatype `~p` is not a supported type", [Type]}, State};
maybe_update({_, false}, _Req, State) ->
    #state{type=Type, op_type=OpType} = State,
    {error, {format, "Operation type is `~p` but  bucket type is `~p`.",
             [OpType, Type]}, State}.

process_update_response(ok, State) ->
    #state{return_key=ReturnKey} = State,
    {reply, #dtupdateresp{key=ReturnKey}, State};
process_update_response({ok, RObj}, State) ->
    #state{type=Type, mod=Mod, return_key=Key, return_ctx=ReturnCtx} = State,
    {{Ctx, Value}, Stats} = riak_kv_crdt:value(RObj, Mod),
    [ riak_kv_stat:update(S) || S <- Stats ],
    Resp = riak_pb_dt_codec:encode_update_response(Type, Value, Key,
                                                   get_context(Ctx, ReturnCtx), ?MOD_MAP),
    {reply, Resp, State};
process_update_response({error, notfound}, State) ->
    {reply, #dtupdateresp{}, State};
process_update_response({error, Reason}, State) ->
    {error, {format, Reason}, State}.

%% Make a list of options for the Get / Put ops
make_options(#dtfetchreq{r=R0, pr=PR0,
                         notfound_ok=NFOk, basic_quorum=BQ,
                         sloppy_quorum=SloppyQ, n_val=NVal}) ->
    R = decode_quorum(R0),
    PR = decode_quorum(PR0),
    make_option(r, R) ++
        make_option(pr, PR) ++
        make_option(notfound_ok, NFOk) ++
        make_option(basic_quorum, BQ) ++
        make_option(sloppy_quorum, SloppyQ) ++
        make_option(n_val, NVal);
make_options(#dtupdatereq{w=W0, dw=DW0, pw=PW0,
                          timeout=Timeout, sloppy_quorum=SloppyQ,
                          n_val=NVal, return_body=RetVal}) ->
    W = decode_quorum(W0),
    DW = decode_quorum(DW0),
    PW = decode_quorum(PW0),
    make_option(w, W) ++
        make_option(dw, DW) ++
        make_option(pw, PW) ++
        make_option(timeout, timeout(Timeout)) ++
        make_option(sloppy_quorum, SloppyQ) ++
        make_option(n_val, NVal) ++ return_value(RetVal).

%% return a key/value tuple that we can ++ to other options so long as the
%% value is not default or undefined -- those values are pulled from the
%% bucket by the get/put FSMs.
make_option(_, undefined) ->
    [];
make_option(_, default) ->
    [];
make_option(K, V) ->
    [{K, V}].

timeout(Timeout) when is_integer(Timeout), Timeout > 0 ->
    Timeout;
timeout(_) ->
    ?DEFAULT_TIMEOUT.

%% Generate a key if the user didn't define one.  5 Return a tuple of
%the Key to use and the Key to return to the user. We only return an
%key if we generated one.
get_key(undefined) ->
    %% Generate a key, the user didn't supply one
    Key = list_to_binary(riak_core_util:unique_id_62()),
    {Key, Key};
get_key(K) ->
    {K, undefined}.

bucket_type_to_type(Bucket, BType) ->
    case riak_core_bucket:get_bucket({BType, Bucket}) of
        BProps when is_list(BProps) ->
            DataType = proplists:get_value(datatype, BProps),
            AllowMult = proplists:get_value(allow_mult, BProps),
            {ok, {AllowMult, DataType}};
        {error, no_type}=E ->
            E
    end.

make_operation(Mod, Op, Ctx) ->
    #crdt_op{mod=Mod, op=Op, ctx=Ctx}.

return_value(true) ->
    [returnbody];
return_value(_) ->
    [].

get_context(<<>>, _) ->
    undefined;
get_context(_Ctx, false) ->
   undefined;
get_context(Ctx, true) ->
    Ctx.

mods_match(BucketMod, OpType) ->
    OpMod = riak_kv_crdt:to_mod(OpType),
    OpMod == BucketMod.

%% ===================================================================
%% V1.4 adapter
%%
%% Riak 1.4 included CRDT based counters for the first time. There was
%% no such thing as "bucket types" back then, lad. A user could store
%% a 1.4 counter in any bucket that had the property
%% `allow_mult=true`. With 2.0 Riak has bucket types. We decided that
%% only storing CRDTs of the same type in a bucket made most sense. In
%% Riak 2.0 Data Types may only be stored in buckets with property of
%% `datatype` set to a supported data type atom. Someone decided it
%% woud be "neat" if clients could still access v1.4 counters from the
%% new API. This code enables that, (although there is also a v1.4 API
%% supported for the time being.) Since _this_ feature / API comes out
%% with 2.0 it actually extends the life of typless buckets for
%% counters by another version.
%%
%% If the user supplies a bucket type of <<"default">> we _assume_
%% they mean to perform an v1.4 counter operation (since this is the
%% only possible CRDT operation without a bucket type).
%%
%% For the record I (Russell) don't agree with this feature, and we'll
%% have to remove it one version after we EOL the v1.4 counter
%% interface

%% Downgrade a crdt request to a v1 counter request
downgrade_request(#dtfetchreq{bucket=B, key=K, r=R, pr=PR, notfound_ok=NOFOK,
                              basic_quorum=BQ}) ->
    #rpbcountergetreq{bucket=B, key=K, r=R, pr=PR, notfound_ok=NOFOK, basic_quorum=BQ};
downgrade_request(#dtupdatereq{bucket=B, key=K, w=W, pw=PW, dw=DW, return_body=RB, op=Op}) ->
    case operation_to_amt(Op) of
        Amt when is_integer(Amt) ->
            #rpbcounterupdatereq{bucket=B, key=K, w=W, pw=PW, dw=DW,
                                 returnvalue=RB, amount=Amt};
        Err -> Err
    end.

%% Transform a counter operation to a v1.4 operation.
operation_to_amt(Op0) ->
    Op = riak_pb_dt_codec:decode_operation(Op0, ?MOD_MAP),
    OpType = riak_pb_dt_codec:operation_type(Op0),
    case {mods_match(?COUNTER_TYPE, OpType), Op} of
        {false, _} ->
            {error, "non-counter operation on default bucket"};
        {true, increment} ->
            1;
        {true, {increment, Amt}} ->
            Amt
    end.

%% delegate the legacy counter requests to the legacy pb service
%% module
process_legacy_counter({error, Reason}, State) ->
    {error, {format, Reason}, State};
process_legacy_counter(Req, State) ->
    LegacyState = riak_kv_pb_counter:init(),
    %% Discard the riak_kv_pb_counter state.
    {E1, E2, _LegacyState2} = riak_kv_pb_counter:process(Req, LegacyState),
    upgrade_response(Req, {E1, E2, State}).

%% Transform a v1.4 counter response to a v2.0 dt response
upgrade_response(_, {reply, #rpbcountergetresp{value=Val}, State}) ->
    Resp = riak_pb_dt_codec:encode_fetch_response(riak_kv_crdt:from_mod(?COUNTER_TYPE),
                                                  Val, undefined, ?MOD_MAP),
    {reply, Resp, State};
upgrade_response(#rpbcounterupdatereq{}, {reply, #rpbcounterupdateresp{}, State}) ->
    {reply, #dtupdateresp{}, State};
upgrade_response(_, {reply, #rpbcounterupdateresp{value=Value}, State}) ->
    {reply, #dtupdateresp{counter_value=Value}, State};
upgrade_response(_, {error, Err, State}) ->
    {error, Err, State}.

%% End of v1.4 adapter
%% ===================================================================

permission_for(#dtupdatereq{bucket=B, type=T}) ->
    {"riak_kv.put", {T,B}};
permission_for(#dtfetchreq{bucket=B, type=T}) ->
    {"riak_kv.get", {T,B}}.
