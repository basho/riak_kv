%%
%% -------------------------------------------------------------------
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
%%

-module(riak_kv_noop_update_hook).
-behaviour(riak_kv_update_hook).

-export([update/3,
         update_binary/5,
         requires_existing_object/1,
         should_handoff/1]).


-spec update(riak_kv_update_hook:object_pair(),
             riak_kv_update_hook:update_reason(),
             riak_kv_update_hook:partition()) -> ok.
update(_ObjectPair, _UpdateReason, _Partition) ->
    ok.

-spec update_binary(
        riak_core_bucket:bucket(),
        riak_object:key(),
        binary(),
        riak_kv_update_hook:update_reason(),
        riak_kv_update_hook:partition()) -> ok.
update_binary(_Bucket, _Key, _Binary, _UpdateReason, _Partition) ->
    ok.

-spec requires_existing_object(Props :: riak_kv_bucket:props()) -> boolean().
requires_existing_object(_Props) ->
    false.

-spec should_handoff(Destination :: riak_kv_update_hook:handoff_dest()) -> boolean().
should_handoff(_Destionation) ->
    true.