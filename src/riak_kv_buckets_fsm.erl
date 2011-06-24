%% -------------------------------------------------------------------
%%
%% riak_buckets_fsm: listing of buckets
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc The buckets fsm manages the listing of buckets.

-module(riak_kv_buckets_fsm).

-behaviour(riak_core_coverage_fsm).

-export([init/0, process_results/3]).

init() ->
    %% Return a tuple containing the ModFun to call per vnode, 
    %% and a coverage factor.
    {ok, {riak_kv_vnode, list_buckets}, 1, riak_kv_vnode_master}.

process_results(Buckets, ClientType, {raw, ReqId, ClientPid}) ->
    case ClientType of
        mapred ->
            try
                luke_flow:add_inputs(ClientPid, Buckets)
            catch _:_ ->
                    exit(self(), normal)
            end;
        plain -> ClientPid ! {ReqId, {buckets, Buckets}}
    end.
