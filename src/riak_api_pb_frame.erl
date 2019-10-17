%% -------------------------------------------------------------------
%%
%% riak_api_pb_frame: framing assistance for TCP responses
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
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
-module(riak_api_pb_frame).

-export([new/0,
         add/2,
         flush/1]).

-record(buffer, {
          buffer = [] :: iodata(),
          size = 0 :: non_neg_integer(),
          max_size = 1024 :: non_neg_integer()
         }).

-type buffer() :: #buffer{}.
-export_type([buffer/0]).

-spec new() -> buffer().
new() ->
    #buffer{}.

-spec add(iodata(), buffer()) -> {ok, buffer()} | {flush, iodata(), buffer()}.
add(Message, #buffer{buffer = Buffer, size = Size, max_size = Max}) ->
    %% 'Message' should already have the message code prefix on it.
    MessageSize = iolist_size(Message),
    NewSize = MessageSize + 4 + Size,
    NewBuffer = [[<<MessageSize:32/unsigned-big-integer>>,Message]|Buffer],
    if
        NewSize >= Max -> 
            {flush, lists:reverse(NewBuffer), #buffer{}};
        true -> 
            {ok, #buffer{buffer=NewBuffer, size=NewSize}}
    end.

-spec flush(buffer()) -> {ok, iodata(), buffer()}.
flush(#buffer{buffer=[]}=Frame) ->
    {ok, [], Frame};
flush(#buffer{buffer=IoData}) ->
    {ok, lists:reverse(IoData), #buffer{}}.
