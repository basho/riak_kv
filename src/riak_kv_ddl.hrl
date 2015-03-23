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

%% TODO these types will be improved as the timeseries work progresses
-type riak_field() :: any().
-type field_type() :: binary | int | float | timeseries | boolean | any.
%% TODO fix up type definitions etc, etc
%% -type colocation() :: any().

-record(riak_field, {
	  name = throw("field 'name' in #riak_field{} is required") :: binary(),
	  type = throw("field 'type' in #riak_field{} is required"):: field_type()
	 }).

-record(ddl, {
	  bucket     = throw("field 'bucket' in #ddl{} is required") :: binary(),
	  fields     = []                                            :: [riak_field()],
	  colocation = throw("field 'bucket' in #ddl{} is required") :: colocation()
	 }).
