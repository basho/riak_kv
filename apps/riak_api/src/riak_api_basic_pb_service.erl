%% -------------------------------------------------------------------
%%
%% riak_api_basic_pb_service: Simple cluster health service
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

%% @doc <p>The PB service for cluster health messages. This covers
%% the following request messages in the original protocol:</p>
%%
%% <pre>
%%   1 - RpbPingReq
%%   7 - RpbGetServerInfoReq
%% </pre>
%%
%% <p>This service produces the following responses:</p>
%%
%% <pre>
%%   2 - RpbPingResp
%%   8 - RpbGetServerInfoResp
%% </pre>
%%
%% <p>The semantics are unchanged from their original
%% implementations.</p>
%% @end
-module(riak_api_basic_pb_service).

-include_lib("riak_pb/include/riak_pb.hrl").

-behaviour(riak_api_pb_service).

-export([init/0,
         decode/2,
         encode/1,
         process/2,
         process_stream/3]).

%% @doc init/0 callback. Returns the service internal start
%% state. This service has no state.
-spec init() -> undefined.
init() ->
    undefined.

%% @doc decode/2 callback. Decodes an incoming message.
decode(Code, Bin) when Code == 1; Code == 7 ->
    {ok, riak_pb_codec:decode(Code, Bin)}.

%% @doc encode/1 callback. Encodes an outgoing response message.
encode(Message) ->
    {ok, riak_pb_codec:encode(Message)}.

%% @doc process/2 callback. Handles an incoming request message.
process(rpbpingreq, State) ->
    {reply, rpbpingresp, State};
process(rpbgetserverinforeq, State) ->
    {_, Vsn} = init:script_id(),
    Message = #rpbgetserverinforesp{node = riak_pb_codec:to_binary(node()),
                                    server_version = riak_pb_codec:to_binary(Vsn)},
    {reply, Message, State}.

%% @doc process_stream/3 callback. Handles a streaming message
%% received by the server on behalf of the service. This service
%% implements no streaming responses, so all messages are ignored.
process_stream(_,_,State) ->
    {ignore, State}.
