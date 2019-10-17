%% -------------------------------------------------------------------
%%
%% riak_pb_kv_codec.hrl: Riak KV PB codec header
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

%% Canonical names of riakc_obj and riak_object metadata fields
-define(MD_CTYPE,    <<"content-type">>).
-define(MD_CHARSET,  <<"charset">>).
-define(MD_ENCODING, <<"content-encoding">>).
-define(MD_VTAG,     <<"X-Riak-VTag">>).
-define(MD_LINKS,    <<"Links">>).
-define(MD_LASTMOD,  <<"X-Riak-Last-Modified">>).
-define(MD_USERMETA, <<"X-Riak-Meta">>).
-define(MD_INDEX,    <<"index">>).
-define(MD_DELETED, <<"X-Riak-Deleted">>).

%% Content-Type for Erlang term_to_binary format
-define(CTYPE_ERLANG_BINARY, "application/x-erlang-binary").

%% Quorum value encodings
-define(UINT_MAX, 16#ffffffff).
-define(RIAKPB_RW_ONE, ?UINT_MAX-1).
-define(RIAKPB_RW_QUORUM, ?UINT_MAX-2).
-define(RIAKPB_RW_ALL, ?UINT_MAX-3).
-define(RIAKPB_RW_DEFAULT, ?UINT_MAX-4).
