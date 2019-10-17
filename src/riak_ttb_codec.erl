%% -------------------------------------------------------------------
%%
%% riak_ttb_codec.erl: term-to-binary codec functions for
%%                     Riak messages
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

%% @doc Codec for Riak term-to-binary messages.

-module(riak_ttb_codec).

-include("riak_ts_ttb.hrl").

-export([encode/1,
         decode/1]).

%% ------------------------------------------------------------
%% Encode for TTB simply converts any strings to binary and encodes to
%% erlang binary format
%% ------------------------------------------------------------

encode(Msg) ->
    [?TTB_MSG_CODE, term_to_binary(Msg)].


%% ------------------------------------------------------------
%% Decode does the reverse
%% ------------------------------------------------------------

decode(MsgData) ->
    return_resp(binary_to_term(MsgData)).

%% ------------------------------------------------------------
%% But if the decoded response is empty, just return the atom
%% identifying the message.  This mimics the behavior of the PB
%% decoder, which simply returns msg_type(msg_code) if the message
%% body is empty
%% ------------------------------------------------------------

return_resp({Atom, <<>>}) ->
    Atom;
return_resp(Resp) ->
    Resp.
