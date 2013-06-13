%% -------------------------------------------------------------------
%%
%% riak_kv_pb_counter: Expose counters over Protocol Buffers
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

%% @doc <p>The Counter PB service for Riak KV. This covers the
%% following request messages:</p>
%%
%% <pre>
%%  29 - RpbCounterUpdateReq
%%  31 - RpbCounterGetReq
%% </pre>
%%
%% <p>This service produces the following responses:</p>
%%
%% <pre>
%%  30 - RpbCounterUpdateResp - 0 length
%%  32 - RpbCounterGetResp
%% </pre>
%%
%% @end

-module(riak_kv_pb_counter).

-include_lib("riak_pb/include/riak_kv_pb.hrl").
-include_lib("riak_pb/include/riak_pb_kv_codec.hrl").

-behaviour(riak_api_pb_service).

-export([init/0,
         decode/2,
         encode/1,
         process/2,
         process_stream/3]).

-import(riak_pb_kv_codec, [decode_quorum/1]).

-record(state, {client}).

-define(DEFAULT_TIMEOUT, 60000).

%% The empty counter that is the body of all new counter objects
-define(NEW_COUNTER, {riak_kv_pncounter, riak_kv_pncounter:new()}).

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
process(#rpbcountergetreq{bucket=B, key=K, r=R0, pr=PR0, notfound_ok=NFOk,
                          basic_quorum=BQ}, #state{client=C} = State) ->
    case riak_kv_counter:supported() of
        true ->
            R = decode_quorum(R0),
            PR = decode_quorum(PR0),
            case C:get(B, K, make_option(r, R) ++
                           make_option(pr, PR) ++
                           make_option(notfound_ok, NFOk) ++
                           make_option(basic_quorum, BQ)) of
                {ok, O} ->
                    Value = riak_kv_counter:value(O),
                    {reply, #rpbcountergetresp{value = Value}, State};
                {error, notfound} ->
                    {reply, #rpbcountergetresp{}, State};
                {error, Reason} ->
                    {error, {format,Reason}, State}
            end;
        false ->
            {error, {format, "Counters are not supported"}, State}
    end;
process(#rpbcounterupdatereq{bucket=B, key=K,  w=W0, dw=DW0, pw=PW0, amount=CounterOp,
                             returnvalue=RetVal},
        #state{client=C} = State) ->
    case {allow_mult(B), riak_kv_counter:supported()} of
        {true, true} ->
            O = riak_kv_counter:new(B, K),

            %% erlang_protobuffs encodes as 1/0/undefined
            W = decode_quorum(W0),
            DW = decode_quorum(DW0),
            PW = decode_quorum(PW0),
            Options = [{counter_op, CounterOp}] ++ return_value(RetVal),
            case C:put(O, make_option(w, W) ++ make_option(dw, DW) ++
                           make_option(pw, PW) ++ [{timeout, default_timeout()} | Options]) of
                ok ->
                    {reply, #rpbcounterupdateresp{}, State};
                {ok, RObj} ->
                    Value = riak_kv_counter:value(RObj),
                    {reply, #rpbcounterupdateresp{value=Value}, State};
                {error, notfound} ->
                    {reply, #rpbcounterupdateresp{}, State};
                {error, Reason} ->
                    {error, {format, Reason}, State}
            end;
        {_, false} ->
            {error, {format, "Counters are not supported"}, State};
        {false, true} ->
            {error, {format, "Counters require bucket property 'allow_mult=true'"}, State}
    end.

return_value(true) ->
    [returnbody];
return_value(_) ->
    [].

allow_mult(Bucket) ->
    proplists:get_value(allow_mult, riak_core_bucket:get_bucket(Bucket)).

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

default_timeout() ->
    ?DEFAULT_TIMEOUT.
