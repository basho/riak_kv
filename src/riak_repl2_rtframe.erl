%% API
-module(riak_repl2_rtframe).
-export([encode/2, decode/1]).

-define(MSG_HEARTBEAT, 16#00). %% Heartbeat message
-define(MSG_OBJECTS,   16#10). %% List of objects to write
-define(MSG_ACK,       16#20). %% Ack
-define(MSG_OBJECTS_AND_META, 16#30). %% List of objects and rt meta data

%% Build an IOlist suitable for sending over a socket
encode(Type, Payload) ->
    IOL = encode_payload(Type, Payload),
    Size = iolist_size(IOL),
    [<<Size:32/unsigned-big-integer>> | IOL].

encode_payload(objects_and_meta, {Seq, BinObjs, Meta}) when is_binary(BinObjs) ->
    BinsAndMeta = {BinObjs, Meta},
    BinsAndMetaBin = term_to_binary(BinsAndMeta),
    [?MSG_OBJECTS_AND_META,
    <<Seq:64/unsigned-big-integer>>,
    BinsAndMetaBin];
encode_payload(objects, {Seq, BinObjs}) when is_binary(BinObjs) ->
    [?MSG_OBJECTS,
     <<Seq:64/unsigned-big-integer>>,
     BinObjs];
encode_payload(ack, Seq) ->
    [?MSG_ACK,
     <<Seq:64/unsigned-big-integer>>];
encode_payload(heartbeat, undefined) ->
    [?MSG_HEARTBEAT].

decode(<<Size:32/unsigned-big-integer, 
         Msg:Size/binary, % MsgCode is included in size calc
         Rest/binary>>) ->
    <<MsgCode:8/unsigned, Payload/binary>> = Msg,
    {ok, decode_payload(MsgCode, Payload), Rest};
decode(<<Rest/binary>>) ->
    {ok, undefined, Rest}.

decode_payload(?MSG_OBJECTS_AND_META, <<Seq:64/unsigned-big-integer, BinsAndMeta/binary>>) ->
    {Bins, Meta} = binary_to_term(BinsAndMeta),
    {objects_and_meta, {Seq, Bins, Meta}};
decode_payload(?MSG_OBJECTS, <<Seq:64/unsigned-big-integer, BinObjs/binary>>) ->
    {objects, {Seq, BinObjs}};
decode_payload(?MSG_ACK, <<Seq:64/unsigned-big-integer>>) ->
    {ack, Seq};
decode_payload(?MSG_HEARTBEAT, <<>>) ->
    heartbeat.
