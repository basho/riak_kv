%% -------------------------------------------------------------------
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

%% @doc This module uses the riak_core_vnode_worker behavior to perform
%% different riak_kv tasks asynchronously.

-module(riak_kv_worker).
-behaviour(riak_core_vnode_worker).

-export([init_worker/3,
         handle_work/3]).

-include_lib("riak_kv_vnode.hrl").

-record(state, {index :: partition()}).

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Initialize the worker. Currently only the VNode index
%% parameter is used.
init_worker(VNodeIndex, _Args, _Props) ->
    {ok, #state{index=VNodeIndex}}.

%% @doc Perform the asynchronous fold operation.
handle_work({fold, FoldFun, FinishFun}, _Sender, State) ->
    try
        FinishFun(FoldFun())
    catch
        throw:receiver_down -> ok;
        throw:stop_fold     -> ok;
        throw:PrematureAcc  -> FinishFun(PrematureAcc)
    end,

    %% Here we're going to terminate the process instead of
    %% continuing with the state machine. We do this because
    %% if there was a lot of memory generated during the fold
    %% (e.g. list keys) then the memory won't be cleaned up
    %% immediately. Instead the process will go back to 'sleep'
    %% in the pool until it's needed again. Once needed it will
    %% GC, but that means there can potentially be N processes
    %% running all eating up unused memory. We could issue an
    %% erlang:collect_garbage() call here instead, but that
    %% would eat up more processor time than just killing and
    %% restarting the worker.

    exit(normal).
