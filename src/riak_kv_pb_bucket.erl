%% -------------------------------------------------------------------
%%
%% riak_kv_pb_bucket: Expose KV bucket functionality to Protocol Buffers
%%
%% Copyright (c) 2012 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc <p>The Bucket PB service for Riak KV. This covers the
%% following request messages in the original protocol:</p>
%%
%% <pre>
%% 15 - RpbListBucketsReq
%% 17 - RpbListKeysReq
%% 19 - RpbGetBucketReq
%% 21 - RpbSetBucketReq
%% </pre>
%%
%% <p>This service produces the following responses:</p>
%%
%% <pre>
%% 16 - RpbListBucketsResp
%% 18 - RpbListKeysResp{1,}
%% 20 - RpbGetBucketResp
%% 22 - RpbSetBucketResp
%% </pre>
%%
%% <p>The semantics are unchanged from their original
%% implementations.</p>
%% @end

-module(riak_kv_pb_bucket).

-include_lib("riak_pb/include/riak_kv_pb.hrl").

-behaviour(riak_api_pb_service).

-export([init/0,
         decode/2,
         encode/1,
         process/2,
         process_stream/3]).

-record(state, {client,    % local client
                req,       % current request (for multi-message requests like list keys)
                req_ctx}). % context to go along with request (partial results, request ids etc)

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
process(rpblistbucketsreq,
        #state{client=C} = State) ->
    case C:list_buckets() of
        {ok, Buckets} ->
            {reply, #rpblistbucketsresp{buckets = Buckets}, State};
        {error, Reason} ->
            {error, {format, Reason}, State}
    end;

%% Start streaming in list keys
process(#rpblistkeysreq{bucket=B}=Req, #state{client=C} = State) ->
    %% stream_list_keys results will be processed by process_stream/3
    {ok, ReqId} = C:stream_list_keys(B),
    {reply, {stream, ReqId}, State#state{req = Req, req_ctx = ReqId}};

%% Get bucket properties
process(#rpbgetbucketreq{bucket=B},
        #state{client=C} = State) ->
    Props = C:get_bucket(B),
    PbProps = riak_pb_kv_codec:encode_bucket_props(Props),
    {reply, #rpbgetbucketresp{props = PbProps}, State};

%% Set bucket properties
process(#rpbsetbucketreq{bucket=B, props = PbProps},
        #state{client=C} = State) ->
    Props = riak_pb_kv_codec:decode_bucket_props(PbProps),
    case C:set_bucket(B, Props) of
        ok ->
            {reply, rpbsetbucketresp, State};
        {error, Details} ->
            {error, {format, "Invalid bucket properties: ~p", [Details]}, State}
    end.

%% @doc process_stream/3 callback. Handles streaming keys messages.
process_stream({ReqId, done}, ReqId,
            State=#state{req=#rpblistkeysreq{}, req_ctx=ReqId}) ->
    {done, #rpblistkeysresp{done = 1}, State};
process_stream({ReqId, From, {keys, []}}, ReqId,
               State=#state{req=#rpblistkeysreq{}, req_ctx=ReqId}) ->
    riak_kv_keys_fsm:ack_keys(From),
    {ignore, State};
process_stream({ReqId, {keys, []}}, ReqId,
               State=#state{req=#rpblistkeysreq{}, req_ctx=ReqId}) ->
    {ignore, State};
process_stream({ReqId, From, {keys, Keys}}, ReqId,
               State=#state{req=#rpblistkeysreq{}, req_ctx=ReqId}) ->
    riak_kv_keys_fsm:ack_keys(From),
    {reply, #rpblistkeysresp{keys = Keys}, State};
process_stream({ReqId, {keys, Keys}}, ReqId,
               State=#state{req=#rpblistkeysreq{}, req_ctx=ReqId}) ->
    {reply, #rpblistkeysresp{keys = Keys}, State};
process_stream({ReqId, Error}, ReqId,
               State=#state{ req=#rpblistkeysreq{}, req_ctx=ReqId}) ->
    {error, {format, Error}, State#state{req = undefined, req_ctx = undefined}}.
