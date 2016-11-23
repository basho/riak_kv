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

-behavior(riak_kv_sweeper).

%% ====================================================================
%% API functions
%% ====================================================================
-export([participate_in_sweep/2,
         successful_sweep/2,
         failed_sweep/2]).

%% riak_kv_sweeper callbacks
participate_in_sweep(_Index, _Pid) ->
    InitialAcc = 0,
    {ok, ttl_fun(), InitialAcc}.

ttl_fun() ->
    fun({{_Bucket, _Key}, RObj}, Acc, [{bucket_props, BucketProps}]) ->
            case riak_kv_util:is_x_expired(RObj, BucketProps) of
                true ->
                    {deleted, Acc};
                _ ->
                    {ok, Acc}
            end
    end.

successful_sweep(Index, _FinalAcc) ->
    lager:info("successful_sweep ~p", [Index]),
    ok.

failed_sweep(Index, Reason) ->
    lager:info("failed_sweep ~p ~p", [Index, Reason]),
    ok.

%% ====================================================================
%% Internal functions
%% ====================================================================
