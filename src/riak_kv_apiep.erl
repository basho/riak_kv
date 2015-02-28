%% -------------------------------------------------------------------
%%
%% riak_kv_apiep: Common functions for ring/coverage protobuff
%%                callback and Webmachine resource
%%
%% Copyright (c) 2015 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc Supporting functions shared between riak_kv_{wm,pb}_apiep.

-module(riak_kv_apiep).

-export([get_entrypoints/1, get_entrypoints_json/1,
         get_entrypoints/2, get_entrypoints_json/2]).

-type proto() :: http|pbc.
-type ep() :: {string(), non_neg_integer()}.
-type bkey() :: {Bucket::binary(), Key::binary()}.

-define(RPC_TIMEOUT, 10000).


-spec get_entrypoints_json(proto(), bkey()) -> iolist().
%% @doc Produce API entry points for a given Bucket and Key, in a JSON form.
get_entrypoints_json(Proto, BKey) ->
    mochijson2:encode(
       {struct, get_entrypoints(Proto, BKey)}).

-spec get_entrypoints_json(proto()) -> iolist().
%% @doc Produce all API entry points in a JSON form.
get_entrypoints_json(Proto) ->
    mochijson2:encode(
       {struct, get_entrypoints(Proto)}).


-spec get_entrypoints(proto(), bkey()) -> [ep()].
%% @doc For a given protocol, determine ip:port entry points of hosts
%%      running riak nodes containing requested bucket and key.
get_entrypoints(Proto, BKey) ->
    UpNodes = riak_core_node_watcher:nodes(riak_kv),
    Preflist = riak_core_apl:get_apl_ann(BKey, UpNodes),
    Nodes =
        lists:usort(
          [N || {{_Index, N}, _Type} <- Preflist]),  %% filter on type?
    get_proto_entrypoints(Nodes, Proto).

-spec get_entrypoints(proto()) -> [ep()].
%% @doc Returns all API entry points for a given protocol.
get_entrypoints(Proto) ->
    Nodes = riak_core_node_watcher:nodes(riak_kv),
    %% is it possible that a vnode running riak_kv has no listeners?
    get_proto_entrypoints(Nodes, Proto).


%% ===================================================================
%% Local functions
%% ===================================================================

-spec get_proto_entrypoints([node()], proto()) -> [ep()].
%% @private
get_proto_entrypoints(Nodes, Proto) ->
    %% 0. check that listeners are up, and also get the port numbers
    {ResL, FailedNodes} =
        rpc:multicall(
          Nodes, riak_api_lib, get_active_listener_entrypoints, [Proto], ?RPC_TIMEOUT),
    case FailedNodes of
        [] ->
            fine;
        FailedNodes ->
            lager:log(
              warning, self(), "Failed to get ~p riak api listeners at node(s) ~9999p",
              [Proto, FailedNodes])
    end,
    ResL.
