%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2016 Basho Technologies, Inc.
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

%% @doc Webmachine resource for fetching from queue
%%
%% Available operations:
%%
%% ```
%% GET /queuename/QueueName
%%
%% Will return an object id an object present on queue
%%
%% Parameters to pass:
%% object_format - internal (only currently supported object format)
%%
%% ```

-module(riak_kv_wm_queue).

%% webmachine resource exports
-export([
         init/1,
         service_available/2,
         malformed_request/2,
         content_types_provided/2,
         produce_queue_fetch/2
        ]).

-record(ctx, {
            client,       %% riak_client() - the store client
            riak,         %% local | {node(), atom()} - params for riak client
            queuename,  %% Queue Name (from uri)
            object_format = internal :: internal %% object format to be used in response
         }).
-type context() :: #ctx{}.


-include_lib("webmachine/include/webmachine.hrl").
-include("riak_kv_wm_raw.hrl").

%% @doc Initialize this resource.
-spec init(proplists:proplist()) -> {ok, context()}.
init(Props) ->
    {ok, #ctx{riak=proplists:get_value(riak, Props)}}.

-spec service_available(#wm_reqdata{}, context()) ->
    {boolean(), #wm_reqdata{}, context()}.
%% @doc Determine whether or not a connection to Riak
%%      can be established. Also, extract query params.
service_available(RD, Ctx=#ctx{riak=RiakProps}) ->
    ClientID = riak_kv_wm_utils:get_client_id(RD),
    case riak_kv_wm_utils:get_riak_client(RiakProps, ClientID) of
        {ok, C} ->
            {true, RD, Ctx#ctx{client = C}};
        Error ->
            {false,
             wrq:set_resp_body(
               io_lib:format("Unable to connect to Riak: ~p~n", [Error]),
               wrq:set_resp_header(?HEAD_CTYPE, "text/plain", RD)),
             Ctx}
    end.

-spec malformed_request(#wm_reqdata{}, context()) ->
    {boolean(), #wm_reqdata{}, context()}.
%% @doc Determine whether request is well-formed
malformed_request(RD, Ctx) ->
    QueueNameRaw = wrq:path_info(queuename, RD),
    QueueName =
        list_to_atom(riak_kv_wm_utils:maybe_decode_uri(RD, QueueNameRaw)),
    ObjectFormatRaw = wrq:get_qs_value(?Q_OBJECT_FORMAT, "internal", RD),
    ObjectFormat = list_to_atom(ObjectFormatRaw),
    {false, RD, Ctx#ctx{queuename = QueueName, object_format = ObjectFormat}}.


-spec content_types_provided(#wm_reqdata{}, context()) ->
    {[{ContentType::string(), Producer::atom()}], #wm_reqdata{}, context()}.
%% @doc List the content types available for representing this resource.
%%      "application/json" is the content-type for bucket lists.
content_types_provided(RD, Ctx) ->
    {[{"application/octet-stream", produce_queue_fetch}], RD, Ctx}.


-spec produce_queue_fetch(#wm_reqdata{}, context()) ->
    {binary()|{error, any()}, #wm_reqdata{}, context()}.
%% @doc Produce the binary response to a queue fetch request
produce_queue_fetch(RD, Ctx) ->
    Client = Ctx#ctx.client,
    QueueName = Ctx#ctx.queuename,
    format_response(Ctx#ctx.object_format, Client:fetch(QueueName), RD, Ctx).
    
format_response(internal, {ok, queue_empty}, RD, Ctx) ->
    {<<0:8/integer>>, RD, Ctx};
format_response(internal, {ok, {deleted, TombClock, RObj}}, RD, Ctx) ->
    SuccessMark = <<1:8/integer>>,
    IsTombstone = <<1:8/integer>>,
    ObjBin = riak_object:to_binary(v1, RObj),
    CRC = erlang:crc32(ObjBin),
    TombClockBin = term_to_binary(TombClock),
    TCL = byte_size(TombClockBin),
    {<<SuccessMark/binary, IsTombstone/binary,
        TCL:32/integer, TombClockBin/binary,
        CRC:32/integer, ObjBin/binary>>, RD, Ctx};
format_response(internal, {ok, RObj}, RD, Ctx) ->
    SuccessMark = <<1:8/integer>>,
    IsTombstone = <<0:8/integer>>,
    ObjBin = riak_object:to_binary(v1, RObj),
    CRC = erlang:crc32(ObjBin),
    {<<SuccessMark/binary, IsTombstone/binary,
        CRC:32/integer, ObjBin/binary>>, RD, Ctx};
format_response(internal, {error, Reason}, RD, Ctx) ->
    {{error, Reason}, RD, Ctx}.