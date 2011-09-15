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

%% @doc The backend status fsm manages the gathering the status of
%% the backends for each vnode.

-module(riak_kv_backend_status_fsm).

-behaviour(riak_core_coverage_fsm).

-include_lib("riak_kv_vnode.hrl").

-export([init/2,
         process_results/2,
         finish/2]).

-type from() :: {atom(), req_id(), pid()}.
-type req_id() :: non_neg_integer().

-record(state, {from :: from(),
                statuses=[] :: [{partition(), atom(), term()}]}).

%% @doc Return a tuple containing the ModFun to call per vnode,
%% the number of primary preflist vnodes the operation
%% should cover, the service to use to check for available nodes,
%% and the registered name to use to access the vnode master process.
init(From, Timeout) ->
    %% Construct the backend status request
    Req = ?KV_BACKEND_STATUS_REQ{},
    {Req, allup, 1, 1, riak_kv, riak_kv_vnode_master, Timeout,
     #state{from=From}}.

process_results(Status,
                State=#state{statuses=Statuses}) ->
    {done, State#state{statuses=[Status | Statuses]}};
process_results({error, Reason}, _State) ->
    {error, Reason}.

finish({error, Error},
       State=#state{from={raw, ReqId, ClientPid}}) ->
    %% Notify the requesting client that an error
    %% occurred or the timeout has elapsed.
    ClientPid ! {ReqId, Error},
    {stop, normal, State};
finish(clean,
       State=#state{from={raw, ReqId, ClientPid},
                    statuses=Statuses}) ->
    ClientPid ! {ReqId, {statuses, Statuses}},
    {stop, normal, State}.
