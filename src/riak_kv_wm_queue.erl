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
%% Will return an object if an object is present on the queue
%%
%% Parameters to pass:
%% object_format - internal (return object in internal repl format)
%%               - internal_aaehash (also return segment hash and vc hash)
%%
%% POST /queuename/QueueName
%%
%% Body should be a JSON in the format `[{Bucket:[{Key, Clock}]}]`
%% ```

-module(riak_kv_wm_queue).

%% webmachine resource exports
-export([
         init/1,
         service_available/2,
         allowed_methods/2,
         malformed_request/2,
         content_types_provided/2,
         produce_queue_fetch/2,
         produce_requeue/2
        ]).

-record(ctx, {
            client,         %% riak_client() - the store client
            riak,           %% local | {node(), atom()} - params for riak client
            queuename,      %% Queue Name (from uri)
            keyclocklist = [] :: list(riak_kv_replrtq_src:repl_entry()),
                            %% List of Bucket, Key, Clock tuples
            object_format = internal :: internal|internal_aaehash,
                            %% object format to be used in response
            method :: 'GET'|'PUT'|'POST'|undefined

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
%%      can be established. 
service_available(RD, Ctx=#ctx{riak=RiakProps}) ->
    ClientID = riak_kv_wm_utils:get_client_id(RD),
    case riak_kv_wm_utils:get_riak_client(RiakProps, ClientID) of
        {ok, C} ->
            {true, RD, Ctx#ctx{client = C, method=wrq:method(RD)}};
        Error ->
            {false,
             wrq:set_resp_body(
               io_lib:format("Unable to connect to Riak: ~p~n", [Error]),
               wrq:set_resp_header(?HEAD_CTYPE, "text/plain", RD)),
             Ctx}
    end.


allowed_methods(RD, Ctx) ->
    {['GET', 'POST'], RD, Ctx}.

-spec malformed_request(#wm_reqdata{}, context()) ->
    {boolean(), #wm_reqdata{}, context()}.
%% @doc Determine whether request is well-formed
malformed_request(RD, Ctx) when Ctx#ctx.method =:= 'GET' ->
    QueueName = malformed_queuename(RD),
    ObjectFormatRaw = wrq:get_qs_value(?Q_OBJECT_FORMAT, "internal", RD),
    ObjectFormat = list_to_atom(ObjectFormatRaw),
    {false, RD, Ctx#ctx{queuename = QueueName, object_format = ObjectFormat}};
malformed_request(RD, Ctx) when Ctx#ctx.method =:= 'POST' ->
    QueueName = malformed_queuename(RD),
    KeyClockList = malformed_keyclocklist(wrq:req_body(RD)),
    {false, RD, Ctx#ctx{queuename = QueueName, keyclocklist = KeyClockList}}.

-spec malformed_queuename(#wm_reqdata{}) -> atom().
malformed_queuename(RD) ->
    QueueNameRaw = wrq:path_info(queuename, RD),
    list_to_atom(riak_kv_wm_utils:maybe_decode_uri(RD, QueueNameRaw)).

-spec malformed_keyclocklist(iolist()) -> list(riak_kv_replrtq_src:rpel_entry()).
malformed_keyclocklist(ReqBody) ->
    DecodeBKLFun =
        fun({B, [{struct, KCL}]}) ->
            ReplEntryFun = 
                fun({K, C}) ->
                    {B,
                        K,
                        riak_object:decode_vclock(base64:decode(C)),
                        to_fetch}
                end,
            lists:map(ReplEntryFun, KCL)
        end,
    {struct, BKL} = mochijson2:decode(ReqBody),
    lists:flatten(lists:map(DecodeBKLFun, BKL)).


-spec content_types_provided(#wm_reqdata{}, context()) ->
    {[{ContentType::string(), Producer::atom()}], #wm_reqdata{}, context()}.
%% @doc List the content types available for representing this resource.
%%      "application/json" is the content-type for bucket lists.
content_types_provided(RD, Ctx) when Ctx#ctx.method =:= 'GET' ->
    {[{"application/octet-stream", produce_queue_fetch}], RD, Ctx};
content_types_provided(RD, Ctx) when Ctx#ctx.method =:= 'POST' ->
    {[{"application/text", produce_requeue}], RD, Ctx}.

-spec produce_requeue(#wm_reqdata{}, context()) ->
    {iolist()|{error, any()}, #wm_reqdata{}, context()}.
produce_requeue(RD, Ctx) ->
    QueueName = Ctx#ctx.queuename,
    KeyClockList = Ctx#ctx.keyclocklist,
    ok = riak_kv_replrtq_src:replrtq_ttaaefs(QueueName, KeyClockList),
    {"ok " ++ integer_to_list(length(KeyClockList)), RD, Ctx}.

-spec produce_queue_fetch(#wm_reqdata{}, context()) ->
    {binary()|{error, any()}, #wm_reqdata{}, context()}.
%% @doc Produce the binary response to a queue fetch request
produce_queue_fetch(RD, Ctx) ->
    Client = Ctx#ctx.client,
    QueueName = Ctx#ctx.queuename,
    format_response(Ctx#ctx.object_format,
                        riak_client:fetch(QueueName, Client),
                        RD,
                        Ctx).
    
format_response(_, {ok, queue_empty}, RD, Ctx) ->
    {<<0:8/integer>>, RD, Ctx};
format_response(_, {error, Reason}, RD, Ctx) ->
    lager:warning("Fetch error ~w", [Reason]),
    {{error, Reason}, RD, Ctx};
format_response(internal_aaehash, {ok, {deleted, TombClock, RObj}}, RD, Ctx) ->
    BK = make_binarykey(riak_object:bucket(RObj), riak_object:key(RObj)),
    {SegmentID, SegmentHash} =
        leveled_tictac:tictac_hash(BK, lists:sort(TombClock)),
    SuccessMark = <<1:8/integer>>,
    IsTombstone = <<1:8/integer>>,
    ObjBin = encode_riakobject(RObj),
    TombClockBin = term_to_binary(TombClock),
    TCL = byte_size(TombClockBin),
    {<<SuccessMark/binary, IsTombstone/binary,
        SegmentID:32/integer, SegmentHash:32/integer,
        TCL:32/integer, TombClockBin/binary,
        ObjBin/binary>>, RD, Ctx};
format_response(internal_aaehash, {ok, RObj}, RD, Ctx) ->
    BK = make_binarykey(riak_object:bucket(RObj), riak_object:key(RObj)),
    {SegmentID, SegmentHash} =
        leveled_tictac:tictac_hash(BK, lists:sort(riak_object:vclock(RObj))),
    SuccessMark = <<1:8/integer>>,
    IsTombstone = <<0:8/integer>>,
    ObjBin = encode_riakobject(RObj),
    {<<SuccessMark/binary, IsTombstone/binary,
        SegmentID:32/integer, SegmentHash:32/integer,
        ObjBin/binary>>, RD, Ctx};
format_response(internal, {ok, {deleted, TombClock, RObj}}, RD, Ctx) ->
    SuccessMark = <<1:8/integer>>,
    IsTombstone = <<1:8/integer>>,
    ObjBin = encode_riakobject(RObj),
    TombClockBin = term_to_binary(TombClock),
    TCL = byte_size(TombClockBin),
    {<<SuccessMark/binary, IsTombstone/binary,
        TCL:32/integer, TombClockBin/binary,
        ObjBin/binary>>, RD, Ctx};
format_response(internal, {ok, RObj}, RD, Ctx) ->
    SuccessMark = <<1:8/integer>>,
    IsTombstone = <<0:8/integer>>,
    ObjBin = encode_riakobject(RObj),
    {<<SuccessMark/binary, IsTombstone/binary,
        ObjBin/binary>>, RD, Ctx}.

encode_riakobject(RObj) ->
    ToCompress = app_helper:get_env(riak_kv, replrtq_compressonwire, false),
    FullObjBin = riak_object:nextgenrepl_encode(repl_v1, RObj, ToCompress),
    CRC = erlang:crc32(FullObjBin),
    <<CRC:32/integer, FullObjBin/binary>>. 

-spec make_binarykey(riak_object:bucket(), riak_object:key()) -> binary().
%% @doc
%% Convert Bucket and Key into a single binary 
make_binarykey({Type, Bucket}, Key)
                    when is_binary(Type), is_binary(Bucket), is_binary(Key) ->
    <<Type/binary, Bucket/binary, Key/binary>>;
make_binarykey(Bucket, Key) when is_binary(Bucket), is_binary(Key) ->
    <<Bucket/binary, Key/binary>>.
