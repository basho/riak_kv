%% -------------------------------------------------------------------
%%
%% sweeper_callback_wait: sweep participant for sweeper testing
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

-module(sweeper_callback_wait).

-export([participate_in_sweep/2,
         successful_sweep/2,
         failed_sweep/3,
         visit_function/3]).

-export([before_visit/1,
         before_visit/2]).

participate_in_sweep(Index, _Pid) ->
    riak_kv_sweeper_SUITE ! {ok, participate_in_sweep},
    {ok, fun visit_function/3, {init, Index}}.

successful_sweep(Index, _FinalAcc) ->
    riak_kv_sweeper_SUITE ! {ok, successful_sweep, ?MODULE, Index},
    ok.

failed_sweep(Index, _Acc, _Reason) ->
    riak_kv_sweeper_SUITE ! {ok, failed_sweep, ?MODULE, Index},
    ok.

visit_function({{_Bucket, _Key}, _RObj}, Acc = continue, _Opts = []) ->
    {ok, Acc};

visit_function({{_Bucket, _Key}, _RObj}, _Acc = {init, Index}, _Opts = []) ->
    riak_kv_sweeper_SUITE ! {self(), waiting, Index},
    receive continue -> ok end,
    Acc1 = continue,
    {ok, Acc1}.


before_visit(Indices) ->
    before_visit(Indices, fun(_) -> ok end).

before_visit([], _Fun) ->
    ok;

before_visit(Indices, Fun) ->
    Index =
        receive
            {From, waiting, Index0} ->
                From ! continue,
                Index0
        end,
    Fun(Index),
    before_visit(update_wait_indices(Indices, Index), Fun).

%% Internal Functions
update_wait_indices(Indices, Index) when is_list(Indices) ->
    Indices -- [Index];
update_wait_indices(any, _Index) ->
    [].
