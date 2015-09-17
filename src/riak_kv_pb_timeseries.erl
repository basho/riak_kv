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

process(#tsputreq{table=_Table, columns=_Columns, rows=_Rows}, State) ->
    {reply, tsputresp, State};
process(_Ddl = #ddl_v1{}, State) ->
    {reply, #tsqueryresp{
               columns=[#tscolumndescription{
                           name = <<"asdf">>,
                           type = 'BINARY'}],
               rows=[#tsrow{cells=[#tscell{binary_value = <<"jkl;">>}]}]},
     State};
process(_DecodedQuery = #riak_sql_v1{}, State) ->
    {reply, #tsqueryresp{
               columns=[#tscolumndescription{
                           name = <<"asdf">>,
                           type = 'BINARY'}],
               rows=[#tsrow{cells=[#tscell{binary_value = <<"jkl;">>}]}]},
     State}.

process_stream(_, _, State)->
    {ignore, State}.

decode_query(#tsinterpolation{base=BaseQuery, interpolations=Interpolations}) ->
    Lexed = riak_ql_lexer:get_tokens(binary_to_list(BaseQuery)),
    {ok, Parsed} = riak_ql_parser:parse(Lexed),
    TypedInterpolations = type_interpolations(Interpolations),
    riak_ql_interpolator:interpolate(Parsed, TypedInterpolations).

decode_query_permissions(#ddl_v1{bucket=NewBucket}) ->
    {"riak_kv.ts_create_table", NewBucket};
decode_query_permissions(#riak_sql_v1{'FROM'=Bucket}) ->
    {"riak_kv.ts_query", Bucket}.

type_interpolations(Interpolations) ->
    type_interpolations(Interpolations, []).

type_interpolations([], Acc) ->
    Acc;
type_interpolations([#tskeycell{key=Key, value=Val} | Rest], Acc) ->
    type_interpolations(Rest, [{binary_to_list(Key), type_interpolation(Val)} | Acc]).

type_interpolation(#tscell{binary_value = Val}) when Val =/= undefined ->
    {binary, iolist_to_binary(Val)};
type_interpolation(#tscell{integer_value = Val}) when is_integer(Val) ->
    {integer, Val};
type_interpolation(#tscell{numeric_value = Val}) when Val =/= undefined ->
    {numeric, iolist_to_binary(Val)};
type_interpolation(#tscell{timestamp_value = Val}) when is_integer(Val) ->
    {timestamp, Val};
type_interpolation(#tscell{boolean_value = Val}) when is_boolean(Val) ->
    {boolean, Val}.


