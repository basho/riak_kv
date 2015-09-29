%%%-------------------------------------------------------------------
%%%
%%% riak_kv_qry_queue.hrl: defines the interfaces of the fsm
%%%
%%% Copyright (C) 2015 Basho Technologies, Inc. All rights reserved
%%%
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
%%%
%%%-------------------------------------------------------------------

-ifndef(RIAK_KV_QRY_QUEUE_HRL).
-define(RIAK_KV_QRY_QUEUE_HRL, included).

-include("riak_kv_index.hrl").
-include_lib("riak_ql/include/riak_ql_sql.hrl").

-record(queue_query, {
          qry = #riak_sql_v1{}
         }).

-type qry_fsm_name() :: atom().
-type query_id()     :: {node(), integer()}.
-type qry_status()   :: available | in_progress.
-type qry()          :: ?KV_SQL_Q{}.
-type timestamp()    :: pos_integer().

-endif.
