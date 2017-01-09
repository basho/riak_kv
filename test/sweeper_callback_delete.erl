%% -------------------------------------------------------------------
%%
%% sweeper_callback_delete: sweep participant for sweeper testing
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

-module(sweeper_callback_delete).

-export([participate_in_sweep/2,
         successful_sweep/2,
         failed_sweep/3,
         visit_object_function/3]).

participate_in_sweep(_Index, _Pid) ->
    riak_kv_sweeper_SUITE ! {ok, participate_in_sweep},
    {ok, fun visit_object_function/3, wait_for_count}.

successful_sweep(Index, _FinalAcc) ->
    riak_kv_sweeper_SUITE ! {ok, successful_sweep, ?MODULE, Index},
    ok.

failed_sweep(Index, _Acc,_Reason) ->
    riak_kv_sweeper_SUITE ! {ok, failed_sweep, ?MODULE, Index},
    ok.

visit_object_function({{_Bucket, _Key}, _RObj}, wait_for_count, _Opts = []) ->
    riak_kv_sweeper_SUITE ! {self(), wait_for_count},
    Count =
        receive
            {delete, Count0} ->
                Count0
        end,
    Acc1 = {delete, Count, _Visited=1},
    if Count > 0 ->
            {deleted, Acc1};
       true ->
            {ok, Acc1}
    end;

visit_object_function({{_Bucket, _Key}, _RObj}, {delete, Count, Visited}, _Opts = [])
  when Visited < Count ->
    Acc1 = {delete, Count, Visited+1},
    {deleted, Acc1};

visit_object_function({{_Bucket, _Key}, _RObj}, Acc = {delete, _Count, _Visited}, _Opts = []) ->
    {ok, Acc}.
