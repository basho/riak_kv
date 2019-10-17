%% -------------------------------------------------------------------
%%
%% riak_pipe_qcover_fsm: enqueueing on a covering set of vnodes
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

%% @doc The qcover fsm manages enqueueing the same work on a set of
%%      vnodes that covers the space of data with a replication factor
%%      of N.  This can be used, for example, to ask 1/Xth of the
%%      cluster to list keys for data stored in Riak KV with N=X.
%%
%%      When using qcover to send `Input', workers for the fitting
%%      will receive input of the form `{cover, FilterVNodes, Input}'.
%%
%%      IMPORTANT: Fittings that will receive their inputs via this
%%      qcover fsm should be started with their nval=1.  Pipe nval
%%      failover should not be used with qcover inputs, because the
%%      vnode filter is meant only for the vnode that initially
%%      receives the request.
%%
%%      Note also that the chashfun fitting parameter has no effect
%%      when using qcover.  Coverage is computed purely via the
%%      qcover's NVal.

-module(riak_pipe_qcover_fsm).

-behaviour(riak_core_coverage_fsm).

-export([init/2,
         process_results/2,
         finish/2]).

-include("riak_pipe.hrl").

-record(state, {from}).

init(From, [#pipe{fittings=[{_Name, Fitting}|_]}, Input, NVal]) ->
    Req = {Fitting, Input},
    {Req,                    %% Request
     all,                    %% VNodeSelector
     NVal,                   %% NVal
     1,                      %% PrimaryVNodeCoverage
     riak_pipe,              %% NodeCheckService
     riak_pipe_vnode_master, %% VNodeMaster
     infinity,               %% Timeout
     #state{from=From}}.     %% State

process_results(ok, State) ->
    %% this 'done' means that the vnode that sent this reply is done,
    %% not that the entire request is done
    {done, State};
process_results({error, Reason}, _State) ->
    {error, Reason}.

finish({error, Reason}, #state{from={raw, Ref, Client}}=State) ->
    Client ! {Ref, {error, Reason}},
    {stop, normal, State};
finish(clean, #state{from={raw, Ref, Client}}=State) ->
    Client ! {Ref, done},
    {stop, normal, State}.
