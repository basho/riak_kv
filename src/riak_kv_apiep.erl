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

-export([get_entrypoints/2, get_entrypoints_json/2]).

-type proto() :: http|pbc.
-type ep() :: [{addr, inet:ip_address() | not_routed} | {port, inet:port_number()} |
               {proto, proto()} | {last_checked, {integer(), integer(), integer()}}].
-type bkey() :: {Bucket::binary(), Key::binary()}.
-type get_entrypoints_option() :: [{force_update, boolean()} | {bkey, bkey()}].

-define(plget, proplists:get_value).


-spec get_entrypoints_json(proto(), get_entrypoints_option()) -> iolist().
%% @doc Produce API entry points (all active or just for a given
%%      {bucket, key}), in a JSON form.
get_entrypoints_json(Proto, Options) ->
    mochijson2:encode(
       {struct, [{struct, EP} ||
                    EP <- prettify(get_entrypoints(Proto, Options))]}).


-spec get_entrypoints(proto(), get_entrypoints_option()) -> [ep()].
get_entrypoints(Proto, Options) ->
    ForceUpdate = ?plget(force_update, Options, false),
    case ?plget(bkey, Options, {<<>>, <<>>}) of
        {<<>>, <<>>} ->
            get_entrypoints_(Proto, all, ForceUpdate);
        BKey ->
            UpNodes = riak_core_node_watcher:nodes(riak_kv),
            Preflist = riak_core_apl:get_apl_ann(BKey, UpNodes),
            Nodes =
                lists:usort(
                  [N || {{_Index, N}, _Type} <- Preflist]),  %% filter on type?
            get_entrypoints_(Proto, Nodes, ForceUpdate)
    end.


%% ===================================================================
%% Local functions
%% ===================================================================

%% @private
get_entrypoints_(Proto, Nodes, ForceUpdate) ->
    {_Nodes, EPLists} =
        lists:unzip(riak_api_lib:get_entrypoints(
                      Proto, [{restrict_nodes, Nodes},
                              {force_update, ForceUpdate}])),
    flatten_one_level(EPLists).

%% @private
prettify(EPList) ->
    %% prettify addresses, convert now's
    [[{addr, prettify_addr(Addr)},
      {port, Port},
      {last_checked, unixtime(LastChecked)}] ||
        [{addr, Addr}, {port, Port}, {last_checked, LastChecked}] <- EPList].

%% @private
prettify_addr(not_routed) ->
    not_routed;
prettify_addr(Addr) ->
    list_to_binary(inet:ntoa(Addr)).

%% @private
unixtime({NowMega, NowSec, _}) ->
    NowMega * 1000 * 1000 + NowSec.

-spec flatten_one_level([[any()]]) -> [any()].
%% @private
flatten_one_level(L) ->
    lists:map(fun([M]) -> M; (M) -> M end, L).
