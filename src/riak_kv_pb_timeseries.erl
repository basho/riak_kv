-module(riak_kv_pb_timeseries).

-behaviour(riak_api_pb_service).

-export([init/0,
         decode/2,
         encode/1,
         process/2]).

-record(state, {}).

-spec init() -> any().
init() ->
    #state{}.

decode(Code, Bin) ->
    Msg = riak_pb_codec:decode(Code, Bin),
    case Msg of
        #tsqueryreq{query = Q}->
            {ok, Decoded, Table} = decode_query(Q),
            {ok, Decoded, {"riak_kv.ts_query", Table}};
        #tsputreq{table = Table, columns = Columns, rows = Rows} ->
            {ok, prepare_put(Table, Columns, Rows), {"riak_kv.ts_put", Table}}
    end.

encode(Message) ->
    {ok, riak_pb_codec:encode(Message)}.
