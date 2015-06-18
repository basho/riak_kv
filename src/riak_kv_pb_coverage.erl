%% -------------------------------------------------------------------
%%
%% riak_kv_pb_coverage: Expose coverage queries to Protocol Buffers
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

%% @doc <p>The Coverage PB service for Riak KV. This covers the
%% following request messages:</p>
%%
%% <pre>
%%  ?? - RpbCoverageReq
%% </pre>
%%
%% <p>This service produces the following responses:</p>
%%
%% <pre>
%%  ?? - RpbCoverageResp
%% </pre>
%% @end

-module(riak_kv_pb_coverage).

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
    Msg = riak_pb_codec:decode(Code, Bin),
    case Msg of
        #rpbcoverreq{type=T, bucket=B} ->
            Bucket = bucket_type(T, B),
            {ok, Msg, {"riak_kv.cover", Bucket}}
    end.

%% @doc encode/1 callback. Encodes an outgoing response message.
encode(Message) ->
    {ok, riak_pb_codec:encode(Message)}.

%% @doc process_stream/3 callback. This service does not create any
%% streaming responses and so ignores all incoming messages.
process_stream(_,_,State) ->
    {ignore, State}.

%% @doc process/2 callback. Handles an incoming request message.
process(#rpbcoverreq{type=T, bucket=B}, #state{client=Client} = State) ->
    Bucket = bucket_type(T, B),
    check_for_error(Client:get_cover(Bucket), State).

check_for_error({error, Error}, State) ->
    {error, Error, State};
check_for_error(Results, State) ->
    %% XXX Have to do something more intelligent here
    {reply, #rpbcoverresp{data=Results}, State}.

%% always construct {Type, Bucket} tuple, filling in default type if needed
bucket_type(undefined, B) ->
    {<<"default">>, B};
bucket_type(T, B) ->
    {T, B}.
