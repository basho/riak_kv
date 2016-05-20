%% -------------------------------------------------------------------
%%
%% riak_kv_pb_ts.erl: Riak TS PB callbacks
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
%% @doc Callbacks for TS PB-Encoded TCP messages [codes 90..103]

-module(riak_kv_pb_ts).

-include_lib("riak_pb/include/riak_kv_pb.hrl").
-include_lib("riak_pb/include/riak_ts_pb.hrl").

-include("riak_kv_ts.hrl").
-include("riak_kv_ts_svc.hrl").

-behaviour(riak_api_pb_service).

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
decode(Code, Bin) when Code >= 90, Code =< 103 ->
    Msg = riak_pb_codec:decode(Code, Bin),
    case Msg of
        #tsqueryreq{query = Q, cover_context = Cover, stream = Stream} ->
            riak_kv_ts_svc:decode_query_common(Q, Cover, Stream);
        #tsgetreq{table = Table}->
            {ok, Msg, {riak_kv_ts_api:api_call_to_perm(get), Table}};
        #tsputreq{table = Table} ->
            {ok, Msg, {riak_kv_ts_api:api_call_to_perm(put), Table}};
        #tsdelreq{table = Table} ->
            {ok, Msg, {riak_kv_ts_api:api_call_to_perm(delete), Table}};
        #tslistkeysreq{table = Table} ->
            {ok, Msg, {riak_kv_ts_api:api_call_to_perm(listkeys), Table}};
        #tscoveragereq{table = Table} ->
            {ok, Msg, {riak_kv_ts_api:api_call_to_perm(coverage), Table}}
    end.

-spec encode(tuple()) -> {ok, iolist()}.
encode(Message) ->
    {ok, riak_pb_codec:encode(Message)}.

-spec process(atom() | riak_kv_ts_svc:ts_requests() | riak_kv_ts_svc:ts_query_types(), #state{}) ->
                     {reply, riak_kv_ts_svc:ts_responses(), #state{}}.
process(Request, State) ->
    encode_response(riak_kv_ts_svc:process(Request, State)).

%% There is no two-tuple variants of process_stream for tslistkeysresp
%% as TS list_keys senders always use backpressure.
process_stream(Message, ReqId, State) ->
    riak_kv_ts_svc:process_stream(Message, ReqId, State).

encode_response({reply, {tsqueryresp, {CNames, CTypes, Rows}}, State}) ->
    Encoded = #tsqueryresp{columns = riak_pb_ts_codec:encode_columns(CNames, CTypes),
                           rows = riak_pb_ts_codec:encode_rows(CTypes, Rows)},
    {reply, Encoded, State};
encode_response({reply, {tsgetresp, {CNames, CTypes, Rows}}, State}) ->
    C = riak_pb_ts_codec:encode_columns(CNames, CTypes),
    R = riak_pb_ts_codec:encode_rows(CTypes, Rows),
    Encoded = #tsgetresp{columns = C, rows = R},
    {reply, Encoded, State};
encode_response({reply, {tscoverageresp, Entries}, State}) ->
    Encoded = #tscoverageresp{entries = riak_pb_ts_codec:encode_cover_list(Entries)},
    {reply, Encoded, State};
encode_response(Response) ->
    Response.
