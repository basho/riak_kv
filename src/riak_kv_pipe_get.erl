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
         process/2,
         done/1]).
-export([partfun/1]).

-include("riak_kv_vnode.hrl").

-record(state, {partition, fd}).

init(Partition, FittingDetails) ->
    {ok, #state{partition=Partition, fd=FittingDetails}}.

process(Input, #state{partition=Partition, fd=FittingDetails}=State) ->
    ReqId = make_req_id(),
    riak_core_vnode_master:command(
      {Partition, node()}, %% assume local partfun was used
      ?KV_GET_REQ{bkey=Input, req_id=ReqId},
      {raw, ReqId, self()},
      riak_kv_vnode_master),
    receive
        {ReqId, {r, {ok, Obj}, _, _}} ->
            riak_pipe_vnode_worker:send_output(
              Obj, Partition, FittingDetails);
        {ReqId, {r, {error, Error}, _, _}} ->
            %%TODO: forward
            riak_pipe_vnode_worker:send_output(
              {error, Error, Input}, Partition, FittingDetails)
    end,
    {ok, State}.

done(_State) ->
    ok.

partfun(Bkey) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    DocIdx = riak_core_util:chash_key(Bkey),
    [{Partition, _}|_] = riak_core_ring:preflist(DocIdx, Ring),
    Partition.

make_req_id() ->
    erlang:phash2(erlang:now()). % stolen from riak_client
