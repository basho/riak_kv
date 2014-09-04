%% -------------------------------------------------------------------
%%
%% riak_kv_counter: Backwards compatibile access to counters for
%% customer MR
%%
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

%% @doc Backwards compatibility with 1.4 counters `value' function as
%% used in the CRDT cookbook.
%%
%% @see riak_kv_crdt.erl
%% @end

-module(riak_kv_counter).

-export([value/1]).

-include("riak_kv_wm_raw.hrl").
-include_lib("riak_kv_types.hrl").

%% @doc Get the value of V1 (1.4) Counter from an Object. Backwards
%% compatability with 1.4 for MapReduce.
-spec value(riak_object:riak_object()) ->
                   integer().
value(RObj) ->
    {{_Ctx, Count}, _Stats} = riak_kv_crdt:value(RObj, ?V1_COUNTER_TYPE),
    Count.
