%% -------------------------------------------------------------------
%%
%% riak_api_pb_apiep: Protobuff callbacks providing a `location service'
%%                    to external clients for optimal access to hosts
%%                    with partitions containing known buckets/key
%%
%% Copyright (c) 2015 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc Protobuff callbacks providing a `location service'
%%      to external clients for optimal access to hosts
%%      with partitions containing certain buckets/key
%%
%% This module serves requests (code), returning response (code):
%%      RpbApiEpReq    (90) -> RpbApiEpResp    (91),
%%      RpbApiEpMapReq (92) -> RpbApiEpMapResp (93)

-module(riak_kv_pb_apiep).

-behaviour(riak_api_pb_service).

-export([init/0,
         decode/2,
         encode/1,
         process/2,
         process_stream/3]).

-include_lib("riak_pb/include/riak_kv_pb.hrl").

-spec init() -> undefined.
init() ->
    undefined.

decode(Code, Bin) when Code == 90 ->
    Msg = riak_pb_codec:decode(Code, Bin),
    case Msg of
        #rpbapiepreq{bucket = B, key = K, proto = P} ->
            {ok, Msg, {"riak_kv.apiep", {B, K, P}}}
    end;

decode(Code, Bin) when Code == 92 ->
    Msg = riak_pb_codec:decode(Code, Bin),
    case Msg of
        #rpbapiepmapreq{proto = P} ->
            {ok, Msg, {"riak_kv.apiepmap", {P}}}
    end.


encode(Message) ->
    {ok, riak_pb_codec:encode(Message)}.


process(#rpbapiepreq{bucket = Bucket, key = Key, proto = Proto}, State) ->
    EPList = riak_kv_apiep:get_entrypoints(Proto, {Bucket, Key}),
    {reply, #rpbapiepresp{
               eplist = [#rpbapiep{
                            ip = list_to_binary(IP),
                            ports = Ports} || {IP, Ports} <- EPList]},
     State};

process(#rpbapiepmapreq{proto = Proto}, State) ->
    EPList = riak_kv_apiep:get_entrypoints(Proto),
    {reply, #rpbapiepmapresp{
               eplist = [#rpbapiep{
                            ip = list_to_binary(IP),
                            ports = Ports} || {IP, Ports} <- EPList]},
     State}.


process_stream(_, _, State) ->
    {ignore, State}.
