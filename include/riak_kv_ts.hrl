%% -------------------------------------------------------------------
%%
%% riak_kv_ts: defines records used in the data description language
%%
%% Copyright (c) 2016 Basho Technologies, Inc.  All Rights Reserved.
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

-ifndef(RIAK_KV_TS_HRL).
-define(RIAK_KV_TS_HRL, included).

%% For riak_pb records
-include_lib("riak_pb/include/riak_ts_pb.hrl").

%% For dialyzer types
-include_lib("riak_ql/include/riak_ql_ddl.hrl").

-define(SQL_SELECT, #riak_select_v1).
-define(SQL_SELECT_RECORD_NAME, riak_select_v1).

%% the result type of a query, rows means to return all matching rows, aggregate
%% returns one row calculated from the result set for the query.
-type select_result_type() :: rows | aggregate.

-record(riak_sel_clause_v1,
        {
          calc_type        = rows :: select_result_type(),
          initial_state    = []   :: [any()],
          col_return_types = []   :: [riak_ql_ddl:simple_field_type()],
          col_names        = []   :: [binary()],
          clause           = []   :: [riak_kv_qry_compiler:compiled_select()],
          finalisers       = []   :: [skip | function()]
        }).

-record(riak_select_v1,
        {
          'SELECT'              :: #riak_sel_clause_v1{},
          'FROM'        = <<>>  :: binary() | {list, [binary()]} | {regex, list()},
          'WHERE'       = []    :: [riak_ql_ddl:filter()],
          'ORDER BY'    = []    :: [riak_kv_qry_compiler:sorter()],
          'LIMIT'       = []    :: [riak_kv_qry_compiler:limit()],
          helper_mod            :: atom(),
          %% will include groups when we get that far
          partition_key = none  :: none | #key_v1{},
          %% indicates whether this query has already been compiled to a sub query
          is_executable = false :: boolean(),
          type          = sql   :: sql | timeseries,
          cover_context = undefined :: term(), %% for parallel queries
          local_key                            %% prolly a mistake to put this here - should be in DDL
        }).

-record(riak_sql_describe_v1,
        {
          'DESCRIBE'    = <<>>  :: binary()
        }).

-record(riak_sql_insert_v1,
        {
          'INSERT'      = <<>>  :: binary(),
          fields                :: [riak_ql_ddl:field_identifier()],
          values                :: [[riak_ql_ddl:data_value()]],
          helper_mod            :: atom()
        }).

-endif.
