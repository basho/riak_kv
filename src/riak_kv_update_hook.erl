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
-module(riak_kv_update_hook).

-include_lib("riak_core/include/riak_core_vnode.hrl").

-export_type([object_pair/0, update_reason/0, repair/0, partition/0, handoff_dest/0]).

-type object_pair() :: {riak_object:riak_object(), riak_object:riak_object() | no_old_object}.
-type repair() :: full_repair | tree_repair | failed_repair.
-type update_reason() ::
    delete
    | handoff
    | put
    | anti_entropy
    | {delete, repair()}
    | {anti_entropy, repair()}
    | {anti_entropy_delete, repair()}
    | anti_entropy_delete.


%% @doc Update a Riak object, given a reason and partition under which
%%      the object is being indexed.  The object pair contains the new
%%      and old objects, in the case where a read-before-write resulted
%%      in an old object.
-callback update(
    object_pair(),
    update_reason(),
    partition()
) ->
    ok.

%% @doc Update a Riak object encoded as an erlang binary.  This function
%%      is typically called from the write-once path, where there is no
%%      old object to pass.
-callback update_binary(
    riak_core_bucket:bucket(),
    riak_object:key(),
    binary(),
    update_reason(),
    partition()
) ->
    ok.

%% @doc Determine whether a bucket requires an existing object,
%%      based on its properties.  If this function returns true,
%%      this may result in a read-before-write in the vnode.
-callback requires_existing_object(riak_kv_bucket:props()) ->
    boolean().

%% @doc Determine whether handoff should start.
-callback should_handoff(handoff_dest()) ->
    boolean().
