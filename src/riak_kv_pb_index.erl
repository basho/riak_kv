%% -------------------------------------------------------------------
%%
%% riak_kv_pb_index: Expose secondary index queries to Protocol Buffers
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

%% @doc <p>The Secondary Index PB service for Riak KV. This covers the
%% following request messages:</p>
%%
%% <pre>
%%  25 - RpbIndexReq
%% </pre>
%%
%% <p>This service produces the following responses:</p>
%%
%% <pre>
%%  26 - RpbIndexResp
%% </pre>
%% @end

-module(riak_kv_pb_index).

-include_lib("riak_pb/include/riak_kv_pb.hrl").

-behaviour(riak_api_pb_service).

-export([init/0,
         decode/2,
         encode/1,
         process/2,
         process_stream/3]).

-record(state, {client}).

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
process(#rpbindexreq{qtype=eq, key=SKey}, State)
  when not is_binary(SKey) ->
    {error, {format, "Invalid equality query ~p", [SKey]}, State};
process(#rpbindexreq{qtype=range, range_min=Min, range_max=Max}, State)
  when not (is_binary(Min) andalso is_binary(Max)) ->
    {error, {format, "Invalid range query: ~p -> ~p", [Min, Max]}, State};
process(#rpbindexreq{bucket=Bucket, index=Index, qtype=eq, key=SKey}, #state{client=Client}=State) ->
    case riak_index:to_index_query(Index, [SKey]) of
        {ok, Query} ->
            case Client:get_index(Bucket, Query) of
                {ok, Results} ->
                    {reply, #rpbindexresp{keys=Results}, State};
                {error, QReason} ->
                    {error, {format, QReason}, State}
            end;
        {error, Reason} ->
            {error, {format, Reason}, State}
    end;
process(#rpbindexreq{bucket=Bucket, index=Index, qtype=range,
                     range_min=Min, range_max=Max, return_terms=ReturnTerms0}, #state{client=Client}=State) ->
    CanReturnTerms = riak_core_capability:get({riak_kv, '2i_return_terms'}, false),
    ReturnTerms = (normalize_bool(ReturnTerms0) andalso CanReturnTerms),
    case riak_index:to_index_query(Index, [Min, Max], ReturnTerms) of
        {ok, Query} ->
            case {ReturnTerms, Client:get_index(Bucket, Query)} of
                {false, {ok, Results}} ->
                    {reply, #rpbindexresp{keys=Results}, State};
                {true, {ok, Results0}} ->
                    Results = [riak_pb_kv_codec:encode_index_pair(Res) || Res <- Results0],
                    {reply, #rpbindexresp{results=Results}, State};
                {_, {error, QReason}} ->
                    {error, {format, QReason}, State}
            end;
        {error, Reason} ->
            {error, {format, Reason}, State}
    end.

normalize_bool(B) when is_boolean(B) ->
    B;
normalize_bool(_) ->
    false.

%% @doc process_stream/3 callback. This service does not create any
%% streaming responses and so ignores all incoming messages.
process_stream(_,_,State) ->
    {ignore, State}.
