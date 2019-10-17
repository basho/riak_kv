-module(encoding_test).
-compile([export_all, nowarn_export_all]).
-include_lib("eunit/include/eunit.hrl").
-include("riak_pb_kv_codec.hrl").
-include("riak_dt_pb.hrl").

pb_test_() ->
    [{"content encode decode",
      ?_test(begin
                 MetaData = dict:from_list(
                              [{?MD_CTYPE, "ctype"},
                               {?MD_CHARSET, "charset"},
                               {?MD_ENCODING, "encoding"},
                               {?MD_VTAG, "vtag"},
                               {?MD_LINKS, [{{<<"b1">>, <<"k1">>}, <<"v1">>},
                                            {{<<"b2">>, <<"k2">>}, <<"v2">>}
                                           ]},
                               {?MD_LASTMOD, {1, 2, 3}},
                               {?MD_USERMETA, [{<<"X-Riak-Meta-MyMetaData1">>, <<"here it is">>},
                                               {<<"X-Riak-Meta-MoreMd">>, <<"have some more">>},
                                               {<<"X-Riak-Meta-EvenMoreMd">>, term_to_binary({a,b,c,d,e,f})}
                                              ]},
                               {?MD_INDEX, [{<<"index_bin">>, <<"foo">>}]},
                               {?MD_DELETED, true}
                              ]),
                 Value = <<"test value">>,
                 {MetaData2, Value2} = riak_pb_kv_codec:decode_content(
                                         riak_kv_pb:decode_msg(
                                           iolist_to_binary(riak_kv_pb:encode_msg(
                                             riak_pb_kv_codec:encode_content({MetaData, Value}))),
                                           rpbcontent)),
                 ?assertEqual(lists:sort(dict:to_list(MetaData)), lists:sort(dict:to_list(MetaData2))),
                 ?assertEqual(Value, Value2)
             end)},
     {"deleted header encode decode",
      ?_test(begin
                 InputMD = [dict:from_list([{?MD_DELETED, DelVal}]) ||
                               DelVal <- [true, "true", false, <<"rubbish">>]],
                 Value = <<"test value">>,
                 {OutputMD, _} = lists:unzip(
                                   [riak_pb_kv_codec:decode_content(
                                      riak_kv_pb:decode_msg(
                                        iolist_to_binary(riak_kv_pb:encode_msg(
                                          riak_pb_kv_codec:encode_content({MD, Value}))),
                                        rpbcontent)) ||
                                       MD <- InputMD]),
                 MdSame1 = (lists:sort(dict:to_list(lists:nth(1, OutputMD))) =:=
                                lists:sort(dict:to_list(lists:nth(2, OutputMD)))),
                 MdSame2 = (lists:sort(dict:to_list(lists:nth(3, OutputMD))) =:=
                                lists:sort(dict:to_list(lists:nth(4, OutputMD)))),
                 ?assertEqual(true, MdSame1),
                 ?assertEqual(true, MdSame2)
             end)},
     {"indexes encode decode",
      ?_test(begin
                 InputMD = dict:from_list([{?MD_INDEX, [{"index_bin", "foo"},
                                                        {"index_int", 10}]}]),
                 ExpectedMD = [{?MD_INDEX, [{<<"index_bin">>, <<"foo">>},
                                            {<<"index_int">>, <<"10">>}]}],
                 Value = <<"test value">>,
                 {OutputMD, _} = riak_pb_kv_codec:decode_content(
                                         riak_kv_pb:decode_msg(
                                           iolist_to_binary(riak_kv_pb:encode_msg(
                                             riak_pb_kv_codec:encode_content({InputMD, Value}))),
                                           rpbcontent)),
                 ?assertEqual(ExpectedMD, dict:to_list(OutputMD))
             end)},
     {"empty content encode decode",
      ?_test(begin
                 MetaData = dict:new(),
                 Value = <<"test value">>,
                 {MetaData2, Value2} = riak_pb_kv_codec:decode_content(
                                         riak_kv_pb:decode_msg(
                                           iolist_to_binary(riak_kv_pb:encode_msg(
                                             riak_pb_kv_codec:encode_content({MetaData, Value}))),
                                           rpbcontent)),
                 ?assertEqual([], dict:to_list(MetaData2)),
                 ?assertEqual(Value, Value2)
             end)},
     {"empty repeated metas are removed/ignored",
      ?_test(begin
                 MetaData = dict:from_list([{?MD_LINKS, []}, {?MD_USERMETA, []}, {?MD_INDEX, []}]),
                 Value = <<"test value">>,
                 {MetaData2, Value2} = riak_pb_kv_codec:decode_content(
                                         riak_kv_pb:decode_msg(
                                           iolist_to_binary(riak_kv_pb:encode_msg(
                                             riak_pb_kv_codec:encode_content({MetaData, Value}))),
                                           rpbcontent)),
                 ?assertEqual([], dict:to_list(MetaData2)),
                 ?assertEqual(Value, Value2)
             end)},
     {"riak_dt-dtfetchreq-encode-decode",
      ?_test(begin
                 RBin = <<10,1,97,18,1,97,26,1,97>>,
                 R = riak_dt_pb:decode_msg(RBin, dtfetchreq),
                 ?assertEqual(<<"a">>, R#dtfetchreq.type),
                 ?assertEqual(<<"a">>, R#dtfetchreq.bucket),
                 ?assertEqual(<<"a">>, R#dtfetchreq.key),
                 ?assertEqual(undefined, R#dtfetchreq.r),
                 ?assertEqual(undefined, R#dtfetchreq.pr),
                 ?assertEqual(undefined, R#dtfetchreq.basic_quorum),
                 ?assertEqual(undefined, R#dtfetchreq.notfound_ok),
                 ?assertEqual(undefined, R#dtfetchreq.timeout),
                 ?assertEqual(undefined, R#dtfetchreq.sloppy_quorum),
                 ?assertEqual(undefined, R#dtfetchreq.n_val),
                 ?assertEqual(undefined, R#dtfetchreq.include_context)
             end)},
     {"msg code encode decode",
      ?_test(begin
                 msg_code_encode_decode(0)
             end)}
    ].

msg_code_encode_decode(256) -> ok;
msg_code_encode_decode(N) ->
    case riak_pb_codec:msg_type(N) of
        undefined ->
            ignore;
        MsgType ->
            ?assertEqual(N, riak_pb_codec:msg_code(MsgType))
    end,
    msg_code_encode_decode(N+1).
