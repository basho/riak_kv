-module(riak_kv_pb_timeseries).

-include_lib("riak_pb/include/riak_kv_pb.hrl").
-include_lib("riak_ql/include/riak_ql_sql.hrl").
-include_lib("riak_ql/include/riak_ql_ddl.hrl").
-behaviour(riak_api_pb_service).

-export([init/0,
         decode/2,
         encode/1,
         process/2,
         process_stream/3]).

-record(state, {}).

-spec init() -> any().
init() ->
    #state{}.

decode(Code, Bin) ->
    Msg = riak_pb_codec:decode(Code, Bin),
    case Msg of
        #tsqueryreq{query=Q}->
            DecodedQuery = decode_query(Q),
            PermAndTarget = decode_query_permissions(DecodedQuery),
            {ok, DecodedQuery, PermAndTarget};
        #tsputreq{table=Table, columns=_Columns, rows=_Rows} ->
            {ok, Msg, {"riak_kv.ts_put", Table}}
    end.

encode(Message) ->
    {ok, riak_pb_codec:encode(Message)}.

process(#tsputreq{table=_Table, columns=Columns, rows=Rows}, State) ->
    _Data = make_data(Rows, Columns),
    {reply, tsputresp, State};
process(_Ddl = #ddl_v1{}, State) ->
    {reply, #tsqueryresp{
               columns=[#tscolumndescription{
                           name = <<"asdf">>,
                           type = 'BINARY'}],
               rows=[#tsrow{cells=[#tscell{binary_value = <<"jkl;">>}]}]},
     State};
process({_DecodedQuery}, State) ->
    {reply, #tsqueryresp{
               columns=[#tscolumndescription{
                           name = <<"asdf">>,
                           type = 'BINARY'}],
               rows=[#tsrow{cells=[#tscell{binary_value = <<"jkl;">>}]}]},
     State}.

process_stream(_, _, State)->
    {ignore, State}.

decode_query(Query) ->
    case Query of
        #tsinterpolation{base=BaseQuery, interpolations=_Interpolations} ->
            Lexed = riak_ql_lexer:get_tokens(binary_to_list(BaseQuery)),
            {ok, Parsed} = riak_ql_parser:parse(Lexed),
            Parsed
    end.

decode_query_permissions(#ddl_v1{bucket=NewBucket}) ->
    {"riak_kv.ts_create_table", NewBucket};
decode_query_permissions(#riak_sql_v1{'FROM'=Bucket}) ->
    {"riak_kv.ts_query", Bucket}.

make_data(Rows, Cols) ->
    make_d2(Rows, Cols, []).

make_d2([], _Cols, Acc) ->
    lists:reverse(Acc);
make_d2([{tsrow, Row} | T], Cols, Acc) ->
    make_d2(T, Cols, [make_d3(Row, Cols, []) | Acc]).

make_d3([], _Cols, Acc) ->
    list_to_tuple(lists:reverse(Acc));
make_d3([#tscell{binary_value    = Bin,
		 integer_value   = undefined,
		 numeric_value   = undefined,
		 timestamp_value = undefined,
		 boolean_value   = undefined,
		 set_value       = [],
		 map_value       = undefined} | T], Cols, Acc) ->
    make_d3(T, Cols, [Bin | Acc]);
make_d3([#tscell{binary_value    = undefined,
		 integer_value   = Int,
		 numeric_value   = undefined,
		 timestamp_value = undefined,
		 boolean_value   = undefined,
		 set_value       = [],
		 map_value       = undefined} | T], Cols, Acc) ->
    make_d3(T, Cols, [Int | Acc]);
make_d3([#tscell{binary_value    = undefined,
		 integer_value   = undefined,
		 numeric_value   = Num,
		 timestamp_value = undefined,
		 boolean_value   = undefined,
		 set_value       = [],
		 map_value       = undefined} | T], Cols, Acc) ->
    make_d3(T, Cols, [Num | Acc]);
make_d3([#tscell{binary_value    = undefined,
		 integer_value   = undefined,
		 numeric_value   = undefined,
		 timestamp_value = Timestamp,
		 boolean_value   = undefined,
		 set_value       = [],
		 map_value       = undefined} | T], Cols, Acc) ->
    make_d3(T, Cols, [Timestamp | Acc]);
make_d3([#tscell{binary_value    = undefined,
		 integer_value   = undefined,
		 numeric_value   = undefined,
		 timestamp_value = undefined,
		 boolean_value   = Bool,
		 set_value       = [],
		 map_value       = undefined} | T], Cols, Acc) ->
    make_d3(T, Cols, [Bool | Acc]);
make_d3([#tscell{binary_value    = undefined,
		 integer_value   = undefined,
		 numeric_value   = undefined,
		 timestamp_value = undefined,
		 boolean_value   = undefined,
		 set_value       = Set,
		 map_value       = undefined} | T], Cols, Acc) ->
    make_d3(T, Cols, [Set | Acc]);
make_d3([#tscell{binary_value    = undefined,
		 integer_value   = undefined,
		 numeric_value   = undefined,
		 timestamp_value = undefined,
		 boolean_value   = undefined,
		 set_value       = [],
		 map_value       = Map} | T], Cols, Acc) ->
    make_d3(T, Cols, [Map | Acc]).
