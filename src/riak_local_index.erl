%% -------------------------------------------------------------------
%%
%% riak_local_index: central module for local indexing.
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

%% @doc central module for local indexing.

-module(riak_local_index).

-export([
	 get_key_from_li_query/1,
	 get_bucket_from_req/1,
	 get_query_from_req/1,
	 is_li_index_query/1
	]).

-include_lib("riak_ql/include/riak_ql_sql.hrl").
-include("riak_kv_index.hrl").
-include("riak_kv_vnode.hrl").

%% ===================================================================
%% Public API
%% ===================================================================

get_key_from_li_query(?KV_SQL_Q{partition_key = K}) -> K.

get_bucket_from_req(#riak_kv_index_req_v1{bucket = B}) -> B;
get_bucket_from_req(#riak_kv_index_req_v2{bucket = B}) -> B.

get_query_from_req(#riak_kv_index_req_v1{qry = Q}) -> Q;
get_query_from_req(#riak_kv_index_req_v2{qry = Q}) -> Q.

is_li_index_query(#riak_kv_index_req_v1{qry = Q}) -> is_li_i2(Q);
is_li_index_query(#riak_kv_index_req_v2{qry = Q}) -> is_li_i2(Q);
is_li_index_query(_)                              -> false.

is_li_i2(?KV_SQL_Q{}) -> true;
is_li_i2(_)           -> false.
