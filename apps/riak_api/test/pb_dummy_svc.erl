-module(pb_dummy_svc).
-behaviour(riak_api_pb_service).
-export([init/0,
         decode/2,
         encode/1,
         process/2,
         process_stream/3]).

init() ->
    undefined.

decode(101, <<>>) ->
    {ok, dummyreq};
decode(_,_) ->
    {error, unknown_message}.

encode(ok) ->
    {ok, <<102,$s,$w,$a,$p>>};
encode(_) ->
    error.

process(dummyreq, State) ->
    {reply, ok, State}.

process_stream(_, _, State) ->
    {ignore, State}.

