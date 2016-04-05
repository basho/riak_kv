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
%% @doc Callbacks for TS TCP messages [codes 90..104]

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

-type ts_requests() :: #tsputreq{} | #tsgetreq{} | #tsqueryreq{}.
-type ts_responses() :: #tsputresp{} | #tsgetresp{} | #tsqueryresp{} |
                        #rpberrorresp{}.
-type ts_query_types() :: ?DDL{} | ?SQL_SELECT{} | #riak_sql_describe_v1{} |
                          #riak_sql_insert_v1{}.

-spec init() -> any().
init() ->
    #state{}.

-spec decode(integer(), binary()) ->
                    {ok, ts_requests(), {PermSpec::string(), Table::binary()}} |
                    {error, _}.
decode(?TTB_MSG_CODE, Bin) ->
    Msg = riak_ttb_codec:decode(Bin),
    case Msg of
        #tsqueryreq{query = Q, cover_context = Cover} ->
            riak_kv_ts_svc:decode_query_common(Q, Cover);
        #tsgetreq{table = Table}->
            {ok, Msg, {"riak_kv.ts_get", Table}};
        #tsputreq{table = Table} ->
            {ok, Msg, {"riak_kv.ts_put", Table}}
    end.

-spec encode(tuple()) -> {ok, iolist()}.
encode(Message) ->
    {ok, riak_ttb_codec:encode(Message)}.

-spec process(atom() | ts_requests() | ts_query_types(), #state{}) ->
                     {reply, ts_responses(), #state{}}.
process(Request, State) ->
    encode_response(riak_kv_ts_svc:process(Request, State)).

%% TS TTB messages do not support streaming yet
process_stream(_, _, _) ->
    {error, "Not Supported", #state{}}.

encode_response({reply, {tsqueryresp, {_, _, []}}, State}) ->
    Encoded = #tsqueryresp{columns = {[], []}, rows = []},
    {reply, Encoded, State};
encode_response({reply, {tsqueryresp, {CNames, CTypes, Rows}}, State}) ->
    Encoded = #tsqueryresp{columns = {CNames, CTypes}, rows = Rows},
    {reply, Encoded, State};
encode_response({reply, tsqueryresp, State}) ->
    {reply, #tsqueryresp{}, State};
encode_response({reply, {tsgetresp, {CNames, CTypes, Rows}}, State}) ->
    Encoded = #tsgetresp{columns = {CNames, CTypes}, rows = Rows},
    {reply, Encoded, State};
encode_response(Response) ->
    Response.
