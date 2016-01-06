%% -------------------------------------------------------------------
%%
%% riak_kv_index: central module for indexing.
%%
%% Copyright (c) 2007-2015 Basho Technologies, Inc.  All Rights Reserved.
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

-ifndef(RIAK_KV_INDEX_HRL).
-define(RIAK_KV_INDEX_HRL, included).

%% Index query records
-record(riak_kv_index_v2, {
          start_key= <<>> :: binary(),
          filter_field :: binary() | undefined,
          start_term :: binary() | undefined, %% Note, in a $key query, start_key==start_term
          end_term :: binary() | undefined, %% Note, in an eq query, start==end
          return_terms=true :: boolean(), %% Note, should be false for an equals query
          start_inclusive=true :: boolean(),
          end_inclusive=true :: boolean(),
          return_body=false ::boolean() %% Note, only for riak cs bucket folds
         }).

-record(riak_kv_index_v3, {
          start_key= <<>> :: binary(),
          filter_field :: binary() | undefined,
          start_term :: integer() | binary() | undefined, %% Note, in a $key query, start_key==start_term
          end_term :: integer() | binary() | undefined, %% Note, in an eq query, start==end
          return_terms=true :: boolean(), %% Note, should be false for an equals query
          start_inclusive=true :: boolean(),
          end_inclusive=true :: boolean(),
          return_body=false ::boolean(), %% Note, only for riak cs bucket folds
          term_regex :: {'re_pattern', any(), any(), any()} | binary() | undefined,
          max_results :: integer() | undefined
         }).

%% TODO these types will be improved over the duration of the time series project
%% -type selection()  :: term().
%% -type filter()     :: term().
%% -type operator()   :: term().
%% -type sorter()     :: term().
%% -type combinator() :: term().
%% -type limit()      :: any().

%% -record(riak_kv_li_index_v1, {
%% 	  bucket        = <<>>  :: binary(),
%% 	  partition_key = <<>>  :: binary(),
%% 	  is_executable = false :: boolean(),
%% 	  selections    = []    :: [selection()],
%% 	  filters       = []    :: [filter()],
%% 	  operators     = []    :: [operator()],
%% 	  sorters       = []    :: [sorter()],
%% 	  combinators   = []    :: [combinator()],
%% 	  limit         = none  :: limit()
%% 	 }).

-define(KV_INDEX_Q, #riak_kv_index_v3).

-endif.
