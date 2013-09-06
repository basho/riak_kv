%% -------------------------------------------------------------------
%%
%% riak_kv_pb_index: Expose secondary index queries to Protocol Buffers
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
%% @doc <p> Special service for riak cs. Fold over objects in buckets.
%% This covers the following request messages:</p>
%%
%% <pre>
%%  40 - RpbCSBucketReq
%% </pre>
%%
%% <p>This service produces the following responses:</p>
%%
%% <pre>
%%  41 - RpbCSBucketResp
%% </pre>
%% @end

-module(riak_kv_pb_csbucket).

-include_lib("riak_pb/include/riak_kv_pb.hrl").
-include("riak_kv_index.hrl").

-behaviour(riak_api_pb_service).

-export([init/0,
         decode/2,
         encode/1,
         process/2,
         process_stream/3]).

-record(state, {client, req_id, req, continuation, result_count=0}).

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

process(Req=#rpbcsbucketreq{}, State) ->
    #rpbcsbucketreq{bucket=Bucket, start_key=StartKey,
                    start_incl=StartIncl, continuation=Continuation,
                    end_key=EndKey, end_incl=EndIncl} = Req,
    Query = riak_index:to_index_query(<<"$bucket">>, [Bucket], Continuation, true, {StartKey, StartIncl}, {EndKey, EndIncl}),
    maybe_perform_query(Query, Req, State).

maybe_perform_query({error, Reason}, _Req, State) ->
    {error, {format, Reason}, State};
maybe_perform_query({ok, Query}, Req, State) ->
    #rpbcsbucketreq{bucket=Bucket, max_results=MaxResults, timeout=Timeout} = Req,
    #state{client=Client} = State,
    Opts = riak_index:add_timeout_opt(Timeout, [{max_results, MaxResults}]),
    {ok, ReqId, _FSMPid} = Client:stream_get_index(Bucket, Query, Opts),
    {reply, {stream, ReqId}, State#state{req_id=ReqId, req=Req}}.

%% @doc process_stream/3 callback. Handle streamed responses
process_stream({ReqId, done}, ReqId, State=#state{req_id=ReqId,
                                                  continuation=Continuation,
                                                  req=Req,
                                                  result_count=Count}) ->
    %% Only add the continuation if there may be more results to send
    #rpbcsbucketreq{max_results=MaxResults} = Req,
    Resp = case is_integer(MaxResults) andalso Count >= MaxResults of
               true -> #rpbcsbucketresp{done=1, continuation=Continuation};
               false -> #rpbcsbucketresp{done=1}
           end,
    {done, Resp, State};
process_stream({ReqId, {results, []}}, ReqId, State=#state{req_id=ReqId}) ->
    {ignore, State};
process_stream({ReqId, {results, Results0}}, ReqId, State=#state{req_id=ReqId, req=Req, result_count=Count}) ->
    #rpbcsbucketreq{max_results=MaxResults, bucket=Bucket} = Req,
    Count2 = length(Results0) + Count,
    %% results are {o, Key, Binary} where binary is a riak object
    Continuation = make_continuation(MaxResults, lists:last(Results0), Count2),
    Results = [encode_result(Bucket, {K, V}) || {o, K, V} <- Results0],
    {reply, #rpbcsbucketresp{objects=Results},
     State#state{continuation=Continuation, result_count=Count2}};
process_stream({ReqId, Error}, ReqId, State=#state{req_id=ReqId}) ->
    {error, {format, Error}, State#state{req_id=undefined}};
process_stream(_,_,State) ->
    {ignore, State}.

encode_result(B, {K, V}) ->
    RObj = riak_object:from_binary(B, K, V),
    Contents = riak_pb_kv_codec:encode_contents(riak_object:get_contents(RObj)),
    VClock = pbify_rpbvc(riak_object:vclock(RObj)),
    GetResp = #rpbgetresp{vclock=VClock, content=Contents},
    #rpbindexobject{key=K, object=GetResp}.

pbify_rpbvc(Vc) ->
    riak_object:encode_vclock(Vc).

make_continuation(MaxResults, {o, K, _V}, MaxResults) ->
    riak_index:make_continuation([K]);
make_continuation(_, _, _)  ->
    undefined.
