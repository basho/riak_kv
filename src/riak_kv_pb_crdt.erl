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
-include_lib("riak_pb/include/riak_pb_kv_codec.hrl").
-include_lib("riak_kv_types.hrl").

-behaviour(riak_api_pb_service).

-export([init/0,
         decode/2,
         encode/1,
         process/2,
         process_stream/3]).

-import(riak_pb_kv_codec, [decode_quorum/1]).

-record(state, {client}).

-define(DEFAULT_TIMEOUT, 60000).

%% @doc init/0 callback. Returns the service internal start
%% state.
-spec init() -> any().
init() ->
    {ok, C} = riak:local_client(),
    #state{client=C}.

%% @doc decode/2 callback. Decodes an incoming message.
decode(Code, Bin) ->
    {ok, riak_pb_codec:decode(Code, Bin)}.

%% @doc encode/1 callback. Encodes an outgoing response message.
encode(Message) ->
    {ok, riak_pb_codec:encode(Message)}.

%% @doc process/2 callback. Handles an incoming request message.
process(#dtfetchreq{bucket=B, key=K, r=R0, pr=PR0,
                    notfound_ok=NFOk, basic_quorum=BQ,
                    sloppy_quorum=SloppyQ, n_val=NVal,
                    type=BType, include_context=InclCtx},
        #state{client=C} = State) ->
    {AllowMult, Type} = bucket_type_to_type(B, BType),
    Mod = riak_kv_crdt:to_mod(Type),
    case {AllowMult, riak_kv_crdt:supported(Mod)} of
        {true, true} ->
            R = decode_quorum(R0),
            PR = decode_quorum(PR0),
            case C:get({BType, B}, K, make_option(r, R) ++
                           make_option(pr, PR) ++
                           make_option(notfound_ok, NFOk) ++
                           make_option(basic_quorum, BQ) ++
                           make_option(sloppy_quorum, SloppyQ) ++
                           make_option(n_val, NVal)) of
                {ok, O} ->
                    {Ctx0, Value} = riak_kv_crdt:value(O, Mod),
                    Ctx = get_context(Ctx0, InclCtx),
                    Resp = riak_pb_dt_codec:encode_fetch_response(Type, Value, Ctx, ?MOD_MAP),
                    {reply, Resp, State};
                {error, notfound} ->
                    Resp = riak_pb_dt_codec:encode_fetch_response(Type, undefined, undefined),
                    {reply, Resp, State};
                {error, Reason} ->
                    {error, {format,Reason}, State};
                Other -> lager:info("got ~p", [Other])
            end;
        {false, _} ->
            {error, {format, "Bucket must be allow_mult=true"}, State};
        {true, false} ->
            {error, {format, "`~p` is not a supported type", [Type]}, State}
    end;
process(#dtupdatereq{bucket=B, key=K, type=BType,
                     w=W0, dw=DW0, pw=PW0, context=Ctx,
                     timeout=Timeout, sloppy_quorum=SloppyQ,
                     n_val=NVal, include_context=InclCtx,
                     op=Op0, return_body=RetVal},
        #state{client=C} = State) ->
    {AllowMult, Type} = bucket_type_to_type(B, BType),
    Mod = riak_kv_crdt:to_mod(Type),
    Op = riak_pb_dt_codec:decode_operation(Op0, ?MOD_MAP),
    OpType = riak_pb_dt_codec:operation_type(Op0),
    ModsMatch = mods_match(Mod, OpType),
    {Key, ReturnKey} = get_key(K),
    case {AllowMult, riak_kv_crdt:supported(Mod), ModsMatch} of
        {true, true, true} ->
            O = riak_kv_crdt:new({BType, B}, Key, Mod),
            %% erlang_protobuffs encodes as 1/0/undefined
            W = decode_quorum(W0),
            DW = decode_quorum(DW0),
            PW = decode_quorum(PW0),
            CrdtOp = make_operation(Mod, Op, Ctx),
            Options = [{crdt_op, CrdtOp}] ++ return_value(RetVal),
            case C:put(O, make_option(w, W) ++
                           make_option(dw, DW) ++
                           make_option(pw, PW) ++
                           make_option(timeout, timeout(Timeout)) ++
                           make_option(sloppy_quorum, SloppyQ) ++
                           make_option(n_val, NVal) ++
                           Options) of
                ok ->
                    {reply, #dtupdateresp{key=ReturnKey}, State};
                {ok, RObj} ->
                    {ReturnCtx, Value} = riak_kv_crdt:value(RObj, Type),
                    {reply, update_resp(Type, Value, get_context(ReturnCtx, InclCtx)), State};
                {error, notfound} ->
                    {reply, #dtupdateresp{}, State};
                {error, Reason} ->
                    {error, {format, Reason}, State}
            end;
        {false, _, _} ->
            {error, {format, "Bucket must be allow_mult=true"}, State};
        {true, false ,_} ->
            {error, {format, "Bucket datatype `~p` is not a supported type", [Type]}, State};
        {_, _, false} ->
            {error, {format, "Operation type is `~p` but  bucket type is `~p`.",
                     [OpType, Type]}, State}
    end.

%% @doc process_stream/3 callback. This service does not create any
%% streaming responses and so ignores all incoming messages.
process_stream(_,_,State) ->
    {ignore, State}.

%% ===================================================================
%% Internal functions
%% ===================================================================

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

get_key(undefined) ->
    %% Generate a key, the user didn't supply one
    Key = list_to_binary(riak_core_util:unique_id_62()),
    {Key, Key};
get_key(K) ->
    {K, undefined}.

bucket_type_to_type(Bucket, BType) ->
    BProps = riak_core_bucket:get_bucket({BType, Bucket}),
    DataType = proplists:get_value(datatype, BProps),
    AllowMult = proplists:get_value(allow_mult, BProps),
    {AllowMult, DataType}.

make_operation(Mod, Op, Ctx) ->
    #crdt_op{mod=Mod, op=Op, ctx=Ctx}.

update_resp(map, Value, Ctx) ->
    #dtupdateresp{map_value=Value, context=Ctx};
update_resp(counter, Value, Ctx) ->
    #dtupdateresp{counter_value=Value, context=Ctx};
update_resp(set, Value, Ctx) ->
    #dtupdateresp{set_value=Value, context=Ctx}.

return_value(true) ->
    [returnbody];
return_value(_) ->
    [].

get_context(_Ctx, false) ->
   undefined;
get_context(Ctx, true) ->
    Ctx.

mods_match(BucketMod, OpType) ->
    OpMod = riak_kv_crdt:to_mod(OpType),
    OpMod == BucketMod.
