%% -------------------------------------------------------------------
%%
%% riak_kv_ddl: defines records used in the data description language
%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved.
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

-record(riak_field_v1, {
	  name     = <<>>  :: list(),
	  position         :: pos_integer(),
	  type             :: field_type(),
	  optional = false :: boolean()
	 }).

-type field_type()         :: simple_field_type() | complex_field_type().
-type simple_field_type()  :: binary | integer | float | timestamp | boolean | set.
-type complex_field_type() :: {map, [#riak_field_v1{}]} | any.

-record(param_v1, {
	  name :: string()
	 }).

-record(hash_fn_v1, {
	  mod       :: atom(),
	  fn        :: atom(),
	  args = [] :: [#param_v1{} | any()]
	 }).

-record(partition_key_v1, {
	  ast = [] :: [#hash_fn_v1{} | #param_v1{}]
	 }).

-record(local_key_v1, {
	  ast = [] :: [#hash_fn_v1{} | #param_v1{}]
	 }).

-record(ddl_v1, {
	  bucket             :: binary(),
	  fields        = [] :: [#riak_field_v1{}],
	  partition_key      :: #partition_key_v1{},
	  local_key          :: #local_key_v1{}
	 }).
