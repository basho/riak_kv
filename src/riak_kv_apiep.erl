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

-type bkey() :: {Bucket::binary(), Key::binary()}.
-type get_entrypoints_api_options() ::
        [{force_update, boolean()} |
         {bkey, bkey()} |
         {report_unrouted_addresses, boolean()}].  %% this option is not used by clients
%% this type includes options for the user API call, and is therefore
%% different from riak_api_lib:get_entrypoints_options(), which
%% defines options for lower-level internal API

-define(plget, proplists:get_value).
-define(is_addr(A), (is_tuple(A) andalso (size(A) == 8 orelse size(A) == 4))).

-spec get_entrypoints_json(riak_api_lib:proto(), get_entrypoints_api_options())
                          -> iolist().
%% @doc Produce API entry points (all active or just for a given
%%      {bucket, key}), in a JSON form.
get_entrypoints_json(Proto, Options) ->
    mochijson2:encode(
       {array, [{struct, EP} ||
                   EP <- prettify(get_entrypoints(Proto, Options))]}).


-spec get_entrypoints(riak_api_lib:proto(), get_entrypoints_api_options()) ->
                             [riak_api_lib:ep()].
get_entrypoints(Proto, Options) ->
    ForceUpdate = ?plget(force_update, Options, false),
    ReportUnroutedAddresses = ?plget(report_unrouted_addresses, Options, false),
    case ?plget(bkey, Options, {<<>>, <<>>}) of
        {<<>>, <<>>} ->
            get_entrypoints_(Proto, all, ForceUpdate, ReportUnroutedAddresses);
        BKey ->
            UpNodes = riak_core_node_watcher:nodes(riak_kv),
            Preflist = riak_core_apl:get_apl_ann(BKey, UpNodes),
            Nodes =
                lists:usort(
                  [N || {{_Index, N}, _Type} <- Preflist]),  %% filter on type?
            get_entrypoints_(Proto, Nodes, ForceUpdate, ReportUnroutedAddresses)
    end.


%% ===================================================================
%% Local functions
%% ===================================================================

-spec get_entrypoints_(riak_api_lib:proto(), all|[node()],
                       boolean()|any(), boolean()|any()) ->
                              [riak_api_lib:ep()].
%% @private
get_entrypoints_(Proto, Nodes, ForceUpdate, ReportUnroutedAddresses) ->
    {_Nodes, EPLists} =
        lists:unzip(riak_api_lib:get_entrypoints(
                      Proto, [{restrict_nodes, Nodes},
                              {force_update, ForceUpdate}])),
    lists:filtermap(
      fun([]) ->
              false;
         (EP) ->
              case ?plget(addr, EP) of
                  Addr when ?is_addr(Addr) orelse ReportUnroutedAddresses ->
                      {true, EP};
                  _ ->
                      false
              end
      end, flatten_one_level(EPLists)).

-spec flatten_one_level([[any()]]) -> [any()].
%% @private
flatten_one_level(L) ->
    lists:map(fun([M]) -> M; (M) -> M end, L).

%% @private
prettify(EPList) ->
    %% prettify addresses, convert now's
    [[{addr, prettify_addr(Addr)},
      {port, Port},
      {last_checked, strptime(LastChecked)}] ||
        [{addr, Addr}, {port, Port}, {last_checked, LastChecked}] <- EPList].

%% @private
prettify_addr(StatusAtom) when is_atom(StatusAtom) ->
    StatusAtom;
prettify_addr(Addr) ->
    list_to_binary(inet:ntoa(Addr)).

-spec strptime({integer(), integer(), integer()}) -> binary().
%% @private
strptime(Now) ->
    {{Y, O, D}, {H, I, S}} =
        calendar:now_to_universal_time(Now),
    list_to_binary(io_lib:format(
      "~4..0B-~2..0B-~2..0BT~2..0B:~2..0B:~2..0B",
      [Y, O, D, H, I, S])).
