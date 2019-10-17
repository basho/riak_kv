%% -------------------------------------------------------------------
%%
%% Copyright (c) 2011 Basho Technologies, Inc.
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

%% @doc This worker always returns `forward_preflist' for each input,
%%      in order to get the worker to send the input on to the next
%%      vnode in the preflist.  It is used by the vnode to disperse
%%      enqueued inputs for a worker, if the worker fails, and then
%%      also fails to restart.
-module(riak_pipe_w_fwd).
-behaviour(riak_pipe_vnode_worker).

-export([init/2,
         process/3,
         done/1]).

-include("riak_pipe.hrl").
-include("riak_pipe_log.hrl").

-record(state, {fd :: riak_pipe_fitting:details()}).
-opaque state() :: #state{}.
-export_type([state/0]).

%% @doc Initialization just stows the fitting details in the module's
%%      state, for sending traces in {@link process/3}.
-spec init(riak_pipe_vnode:partition(),
           riak_pipe_fitting:details()) ->
        {ok, state()}.
init(_Partition, FittingDetails) ->
    {ok, #state{fd=FittingDetails}}.

%% @doc Process just requests that `Input' be forwarded to the next
%%      vnode in its preflist.
-spec process(term(), boolean(), state()) -> {forward_preflist, state()}.
process(_Input, _Last, #state{fd=FittingDetails}=State) ->
    ?T(FittingDetails, [], {forwarding, _Input}),
    {forward_preflist, State}.

%% @doc Unused.
-spec done(state()) -> ok.
done(_State) ->
    ok.
