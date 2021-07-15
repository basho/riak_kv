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
%% Will return an object if an object is present in the queue
%%
%% Parameters to pass:
%% object_format - internal (return object in internal repl format)
%%               - internal_aaehash (also return segment hash and vc hash)
%%
%% POST /queuename/QueueName
%%
%% Body should be a JSON in the format returned from
%% riak_kv_clusteraae_fsm:json_encode_results(fetch_clocks_range, KeysNClocks).
%%
%% ```

-module(riak_kv_wm_queue).

%% webmachine resource exports
-export([
         init/1,
         service_available/2,
         allowed_methods/2,
         malformed_request/2,
         content_types_provided/2,
         process_post/2,
         produce_queue_fetch/2
        ]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

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
malformed_request(RD, Ctx) ->
    RDForError = wrq:set_resp_header(?HEAD_CTYPE, "text/plain", RD),
    case wrq:method(RD) of
        'GET' ->
            QueueName = malformed_queuename(RD),
            ObjectFormatRaw =
                wrq:get_qs_value(?Q_OBJECT_FORMAT, "internal", RD),
            case existing_atom(ObjectFormatRaw) of
                false ->
                    ErrorBody =
                        io_lib:format("Format ~w not defined~n",
                            [ObjectFormatRaw]),
                    {true,
                        wrq:append_to_resp_body(ErrorBody, RDForError),
                        Ctx};
                ObjectFormat ->
                    {false,
                        RD,
                        Ctx#ctx{queuename = QueueName,
                                object_format = ObjectFormat}}
            end;
        'POST' ->
            case malformed_queuename(RD) of
                false ->
                    ErrorBody =
                        io_lib:format("Queue name ~w not defined~n",
                            [wrq:path_info(queuename, RD)]),
                    {true,
                        wrq:append_to_resp_body(ErrorBody, RDForError),
                     Ctx};
                QueueName ->
                    case malformed_keyclocklist(wrq:req_body(RD)) of
                        false ->
                            ErrorBody = "Malformed Keyclock list~n",
                            {true,
                                wrq:append_to_resp_body(ErrorBody, RDForError),
                                Ctx};
                        KeyClockList ->
                            {false,
                                RD, 
                                Ctx#ctx{queuename = QueueName,
                                        keyclocklist = KeyClockList}}
                    end
            end
    end.

-spec malformed_queuename(#wm_reqdata{}) -> atom()|false.
malformed_queuename(RD) ->
    QueueNameRaw = wrq:path_info(queuename, RD),
    existing_atom(riak_kv_wm_utils:maybe_decode_uri(RD, QueueNameRaw)).

-spec malformed_keyclocklist(iolist()) ->
        list(riak_kv_replrtq_src:repl_entry())|false.
malformed_keyclocklist(ReqBody) ->
    %% If individual elements of the Key Clock list are malformed
    %% they are filtered, rather than calling the request malformed
    %% with a log raise to indicate that the filtering has occurred 
    case mochijson2:decode(ReqBody) of
        {struct, [{<<"keys-clocks">>, KCL}]} ->
            KeyClockList = 
                lists:foldl(fun decode_bucketkeyclock/2, [], KCL),
            case {length(KeyClockList), length(KCL)} of
                {N, N} ->
                    ok;
                {N, M} ->
                    lager:info(
                        "~w Malformed requests filtered from push of ~w",
                        [M - N, M]),
                    ok
            end,
            KeyClockList;
        _ ->
            false
    end.


-spec content_types_provided(#wm_reqdata{}, context()) ->
    {[{ContentType::string(), Producer::atom()}], #wm_reqdata{}, context()}.
%% @doc List the content types available for representing this resource.
content_types_provided(RD, Ctx) when Ctx#ctx.method =:= 'GET' ->
    {[{"application/octet-stream", produce_queue_fetch}], RD, Ctx};
content_types_provided(RD, Ctx) when Ctx#ctx.method =:= 'POST' ->
    {[{"text/plain", nop}], RD, Ctx}.


-spec process_post(#wm_reqdata{}, context()) ->
    {true, #wm_reqdata{}, context()}.
%% @doc Pass-through for key-level requests to allow POST to function
%%      as PUT for clients that do not support PUT.
process_post(RD, Ctx) -> 
    QueueName = Ctx#ctx.queuename,
    KeyClockList = 
        lists:map(fun({B, K, C}) -> {B, K, C, to_fetch} end,
                    lists:reverse(Ctx#ctx.keyclocklist)),
    ok = riak_kv_replrtq_src:replrtq_ttaaefs(QueueName, KeyClockList),
    R = 
        case riak_kv_replrtq_src:length_rtq(QueueName) of
            {QueueName, {FL, FSL, RTL}} ->
                io_lib:format("Queue ~w: ~w ~w ~w", [QueueName, FL, FSL, RTL]);
            _ ->
                io_lib:format("No queue ~w", [QueueName])
        end,
    {true,
        wrq:set_resp_body(
               iolist_to_binary(R),
               wrq:set_resp_header(?HEAD_CTYPE, "text/plain", RD)),
        Ctx}.

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

decode_bucketkeyclock({struct, BKC}, Acc) ->
    B = 
        case lists:keyfind(<<"bucket">>, 1, BKC) of
            {<<"bucket">>, Bucket} when is_binary(Bucket) ->
                case lists:keyfind(<<"bucket-type">>, 1, BKC) of
                    {<<"bucket-type">>, BucketType} ->
                        {BucketType, Bucket};
                    false ->
                        Bucket
                end;
            false ->
                false
        end,
    case B of
        false ->
            Acc;
        _ ->
            case lists:keyfind(<<"key">>, 1, BKC) of
                {<<"key">>, K} when is_binary(K) ->
                    case lists:keyfind(<<"clock">>, 1, BKC) of
                        {<<"clock">>, EncodedClock} ->
                            case decode_clock(EncodedClock) of
                                false ->
                                    Acc;
                                VC ->
                                    [{B, K, VC}|Acc]
                            end;
                        false ->
                            Acc
                    end;
                false ->
                    Acc
            end
    end;
decode_bucketkeyclock(_, Acc) ->
    Acc.

-spec existing_atom(list()) -> atom()|false.
existing_atom(ListToConvert)->
    try
        list_to_existing_atom(ListToConvert)
    catch
        _:_ ->
            false
    end.

-spec decode_clock(list()) -> vclock:vclock()|false.
decode_clock(EncodedClock) ->
    try
        riak_object:decode_vclock(base64:decode(EncodedClock))
    catch
        _:_ ->
            false
    end.

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

-ifdef(TEST).

test_kcl() ->
    A = vclock:fresh(),
    B = vclock:fresh(),
    A1 = vclock:increment(a, A),
    B1 = vclock:increment(b, B),
    E1 = {<<"B1">>, <<"K1">>, A1},
    E2 = {{<<"T">>, <<"B2">>}, <<"K2">>, B1},
    [E1, E2].

json_encode_keys_test() ->
    KCL = lists:sort(test_kcl()),
    KCEncoded =
        riak_kv_clusteraae_fsm:json_encode_results(fetch_clocks_range, KCL),
    ?assertMatch(KCL, lists:sort(malformed_keyclocklist(KCEncoded))).

malformed_clock_test() ->
    KCL = lists:sort(test_kcl()),
    KCEncoded =
        riak_kv_clusteraae_fsm:json_encode_results(fetch_clocks_range, KCL),
    {struct, [{<<"keys-clocks">>, EncKCL}]} =
        mochijson2:decode(KCEncoded),
    BadEncoded =
        mochijson2:encode({struct, [{<<"keys-xxx-clocks">>, EncKCL}]}),
    ?assertMatch(false, malformed_keyclocklist(BadEncoded)),
    Q = q1_ttaaefs,
    ?assertMatch(Q, existing_atom("q1_ttaaefs")),
    ?assertMatch(false, existing_atom("q1_xxx_ttaaefs")),
    VC = 
        base64:encode(
            riak_object:encode_vclock(
                vclock:increment(a, vclock:fresh()))),
    Garbage =
        <<"zzz">>,
    ?assertMatch(false, decode_clock(<<VC/binary, Garbage/binary>>)).


-endif.
