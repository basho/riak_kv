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

-module(riak_kv_pipe_get).
-behaviour(riak_pipe_vnode_worker).

-export([init/2,
         process/3,
         done/1]).

-include("riak_kv_vnode.hrl").

-record(state, {partition, fd}).

init(Partition, FittingDetails) ->
    {ok, #state{partition=Partition, fd=FittingDetails}}.

process(Input, Last, #state{partition=Partition, fd=FittingDetails}=State) ->
    ReqId = make_req_id(),
    riak_core_vnode_master:command(
      {Partition, node()}, %% assume local partfun was used
      ?KV_GET_REQ{bkey=Input, req_id=ReqId},
      {raw, ReqId, self()},
      riak_kv_vnode_master),
    receive
        {ReqId, {r, {ok, Obj}, _, _}} ->
            riak_pipe_vnode_worker:send_output(
              {ok, Obj}, Partition, FittingDetails),
            {ok, State};
        {ReqId, {r, {error, _} = Error, _, _}} ->
            if Last ->
                    riak_pipe_vnode_worker:send_output(
                      {Error, Input}, Partition, FittingDetails),
                    {ok, State};
               true ->
                    {forward_preflist, State}
            end
    end.

done(_State) ->
    ok.

make_req_id() ->
    erlang:phash2(erlang:now()). % stolen from riak_client
