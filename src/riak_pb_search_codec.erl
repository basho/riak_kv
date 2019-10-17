%% -------------------------------------------------------------------
%%
%% riak_pb_search_codec: Protocol Buffers encoding/decoding helpers for
%% Riak Search
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

%% @doc Utility functions for Protocol Buffers encoding and decoding
%% of Riak Search-related messages. These are used inside the client
%% and server code and do not normally need to be used in application
%% code.
-module(riak_pb_search_codec).

-include("riak_search_pb.hrl").

-export([encode_search_doc/1,
         decode_search_doc/1]).

-import(riak_pb_codec, [encode_pair/1, decode_pair/1]).

%% @doc Encodes a property-list of indexed-document fields into a
%% search-doc Protocol Buffers message.
-spec encode_search_doc([{binary(), binary()}]) -> #rpbsearchdoc{}.
encode_search_doc(PList) ->
    #rpbsearchdoc{fields=[ encode_pair(Pair) || Pair <- PList]}.

%% @doc Decodes a Protocol Buffers message search-doc into proplist of
%% document fields and values.
-spec decode_search_doc(#rpbsearchdoc{}) -> [{binary(), binary()}].
decode_search_doc(#rpbsearchdoc{fields=Fields}) -> 
    [ decode_pair(Pair) || Pair <- Fields ].
