%% -------------------------------------------------------------------
%%
%% riak_api_pb_apiep.erl: Protobuff callbacks providing a `location
%%                        service' to external clients for optimal
%%                        access to hosts with partitions containing
%%                        known buckets/key
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

%% @doc Protobuff callbacks providing a `location service' to external
%%      clients for optimal access to hosts with partitions (all or a
%%      subset containing certain buckets/key).
%%
%%  This module serves requests (code), returning response (code):
%%
%%    RpbApiEpReq (90) -> RpbApiEpResp (91)
%%
%%  If parameter force_update is specified and has a value of 1, this
%%  information is collected via (expensive) rpc calls to all riak_kv
%%  nodes; else, values cached in cluster metadata from a previous
%%  call are returned.
%%

-module(riak_kv_pb_apiep).

-behaviour(riak_api_pb_service).

-export([init/0,
         decode/2,
         encode/1,
         process/2,
         process_stream/3]).

-include_lib("riak_pb/include/riak_kv_pb.hrl").

-define(plget, proplists:get_value).


-spec init() -> undefined.
init() ->
    undefined.

decode(Code, Bin) when Code == 90 ->
    Msg = riak_pb_codec:decode(Code, Bin),
    case Msg of
        #rpbapiepreq{bucket = B, key = K,
                     proto = P,
                     force_update = ForceUpdate,
                     check_key_exist = CheckKeyExist} ->
            {ok, Msg, {"riak_kv.apiep",
                       {{B, K}, P, ForceUpdate, CheckKeyExist}}}
    end.

encode(Message) ->
    {ok, riak_pb_codec:encode(Message)}.


process(#rpbapiepreq{bucket = Bucket, key = Key,
                     proto = Proto,
                     force_update = ForceUpdate,
                     check_key_exist = CheckKeyExist}, State) ->
    EPList = riak_kv_apiep:get_entrypoints(
               Proto, [{bkey, {Bucket, Key}},
                       {force_update, ForceUpdate},
                       {check_key_exist, CheckKeyExist}]),
    APList =
        lists:filtermap(
          fun(EP) ->
                  %% well, we know the elements in this preflist come
                  %% in a certain order (in which case we would just
                  %% match them in function head), but let's treat it
                  %% as if it might not.
                  case {?plget(addr, EP),
                        ?plget(port, EP),
                        ?plget(last_checked, EP)} of
                      {A, _, _} when is_atom(A) ->
                          %% but also, we filter entries like
                          %% no_interfaces or not_routed which are of
                          %% no interest to clients
                          false;
                      APL ->
                          {true, APL}
                  end
               end, EPList),
    {reply, #rpbapiepresp{
               eplist = [#rpbapiep{
                            addr = if is_atom(MaybeAddr) ->
                                           list_to_binary(atom_to_list(MaybeAddr));
                                      el/=se -> list_to_binary(inet:ntoa(MaybeAddr))
                                   end,
                            port = Port,
                            last_checked = unixtime(LastChecked)} ||
                            {MaybeAddr, Port, LastChecked} <- APList]},
     State}.


process_stream(_, _, State) ->
    {ignore, State}.

%% @private
-spec unixtime({non_neg_integer(), integer(), integer()}) -> non_neg_integer().
unixtime({NowMega, NowSec, _}) ->
    NowMega * 1000 * 1000 + NowSec.
