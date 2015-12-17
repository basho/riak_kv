%% -------------------------------------------------------------------
%%
%% riak_kv_object_ttl: This module keep sweeper callbacks for object TTL.
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

-module(riak_kv_object_ttl).
-include("riak_kv_wm_raw.hrl").

%% ====================================================================
%% API functions
%% ====================================================================
-export([participate_in_sweep/2,
         successfull_sweep/2,
         failed_sweep/2]).

%% riak_kv_sweeper callbacks
participate_in_sweep(_Index, _Pid) ->
    InitialAcc = 0,
    {ok, ttl_fun(), InitialAcc}.

ttl_fun() ->
    fun({{Bucket, Key}, RObj}, Acc, [{bucket_props, BucketProps}]) ->
            case expired(RObj, BucketProps) of
                true ->
                    Tombstone = create_tombstone(RObj, Bucket, Key),
                    {mutated, Tombstone, Acc};
                _ ->
                    {ok, Acc}
            end
    end.

create_tombstone(RObj, Bucket, Key) ->
    TombstoneMD = dict:store(?MD_DELETED, "true", dict:new()),
    Obj0 = riak_object:new(Bucket, Key, <<>>, TombstoneMD),
    VClock = riak_object:vclock(RObj),
    Tombstone = riak_object:set_vclock(Obj0, VClock),
    riak_object:update_last_modified(Tombstone).

successfull_sweep(Index, _FinalAcc) ->
    lager:info("successfull_sweep ~p", [Index]),
    ok.

failed_sweep(Index, Reason) ->
    lager:info("failed_sweep ~p ~p", [Index, Reason]),
    ok.

%% ====================================================================
%% Internal functions
%% ====================================================================

expired(RObj, BucketProps) ->
    riak_kv_util:is_x_expired(RObj, BucketProps).
