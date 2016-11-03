%% -------------------------------------------------------------------
%%
%% riak_kv_ttb_ts.erl: Riak TS TTB callbacks
%%
%% Copyright (c) 2016 Basho Technologies, Inc.
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
%% @doc Callbacks for TS TTB-Encoded TCP messages [code 104]

-module(riak_kv_ttb_ts).

-include_lib("riak_pb/include/riak_ts_pb.hrl").
-include_lib("riak_pb/include/riak_ts_ttb.hrl").

-include("riak_kv_ts.hrl").
-include("riak_kv_ts_svc.hrl").

-behaviour(riak_api_pb_service).

%% behaviour exports
-export([init/0,
         decode/2,
         encode/1,
         process/2,
         process_stream/3]).

-spec init() -> any().
init() ->
    #state{}.

-spec decode(integer(), binary()) ->
                    {ok, riak_kv_ts_svc:ts_requests(), {PermSpec::string(), Table::binary()}} |
                    {error, _}.
decode(?TTB_MSG_CODE, Bin) ->
    Msg = riak_ttb_codec:decode(Bin),
    DecodedReq =
        case Msg of
            #tsqueryreq{query = Q, cover_context = Cover} ->
                riak_kv_ts_svc:decode_query_common(Q, Cover);
            #tsgetreq{table = Table}->
                {ok, Msg, {riak_kv_ts_api:api_call_to_perm(get), Table}};
            #tsputreq{table = Table} ->
                {ok, Msg, {riak_kv_ts_api:api_call_to_perm(put), Table}}
        end,
    DDLRecCap = riak_core_capability:get({riak_kv, riak_ql_ddl_rec_version}),
    riak_kv_ts_util:check_table_feature_supported(DDLRecCap, DecodedReq).

-spec encode(tuple()) -> {ok, iolist()}.
encode(Message) ->
    {ok, riak_ttb_codec:encode(Message)}.

-spec process(atom() | riak_kv_ts_svc:ts_requests() | riak_kv_ts_svc:ts_query_types(), #state{}) ->
                     {reply, riak_kv_ts_svc:ts_responses(), #state{}}.
process(Request, State) ->
    riak_kv_ts_svc:process(Request, State).

%% TS TTB messages do not support streaming yet
process_stream(_, _, _) ->
    {error, "Not Supported", #state{}}.
