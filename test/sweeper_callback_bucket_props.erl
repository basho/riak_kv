%% -------------------------------------------------------------------
%%
%% sweeper_callback_1: Sweep participant for sweeper testing
%%
%% Copyright (c) 2017 Basho Technologies, Inc.  All Rights Reserved.
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

-module(sweeper_callback_bucket_props).

-export([participate_in_sweep/2,
         successful_sweep/2,
         failed_sweep/3,
         visit_function/3]).

participate_in_sweep(_Index, _Pid) ->
    InitialAcc = 0,
    riak_kv_sweeper_SUITE ! {ok, participate_in_sweep},
    {ok, fun visit_function/3, InitialAcc}.

successful_sweep(Index, _FinalAcc) ->
    riak_kv_sweeper_SUITE ! {ok, successful_sweep, ?MODULE, Index},
    ok.

failed_sweep(Index, _Reason, _Acc) ->
    riak_kv_sweeper_SUITE ! {ok, failed_sweep, ?MODULE, Index},
    ok.

visit_function({{Bucket, _Key}, _RObj}, Acc, _Opts = [{bucket_props, [{name, Bucket}]}]) ->
    Visited = Acc,
    {ok, Visited + 1}.
