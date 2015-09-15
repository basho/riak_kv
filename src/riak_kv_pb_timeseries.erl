-module(riak_kv_pb_timeseries).

-include_lib("riak_pb/include/riak_kv_pb.hrl").
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

%% @ignore INSERT (as if)
process(#tsputreq{table=Table, columns=_Columns, rows=Rows}, State) ->
    Data = make_data(Rows),
    Mod = riak_ql_ddl:make_module_name(Table),
    _Data2 = Mod:add_column_info(Data),
    {reply, tsputresp, State};

%% @ignore CREATE TABLE
process(_DDL = #ddl_v1{}, State) ->
    %% {module, Module} = riak_ql_ddl_compiler:make_helper_mod(DDL),
    %% and what do we do with this DDL?
    %% bucket creation effected via bucket activation (via riak-admin)
    {reply, #rpberrorresp{errcode = -3,
                          errmsg = "CREATE TABLE not supported via client interface; use riak-admin command instead"},
     State};

%% @ignore SELECT
process(SQL = #riak_sql_v1{'FROM' = Bucket}, State) ->
    Mod = riak_ql_ddl:make_module_name(Bucket),
    DDL = Mod:get_ddl(),
    case riak_kv_qry:submit(SQL, DDL) of
        {ok, QId} ->
            case riak_kv_qry:fetch(QId) of
                {ok, Data} ->
                    {reply, make_tsqueryresp(Data, fun Mod:get_field_type/1), State};
                {error, Reason} ->
                    {reply, #rpberrorresp{errcode = -1,
                                          errmsg = lists:flatten(
                                                     io_lib:format("~p", [Reason]))},
                     State}
            end;
        {error, Reason} ->
            {reply, #rpberrorresp{errcode = -2,
                                  errmsg = lists:flatten(
                                             io_lib:format("~p", [Reason]))},
             State}
    end.

make_tsqueryresp([], _Fun) ->
    #tsqueryresp{columns = [], rows = []};
make_tsqueryresp(Data, GetFieldTypeF) ->
    %% data come in batches
    Rows = lists:flatten(Data),
    %% get types and take the ordering of columns in the batches
    {ColumnNames, _Values} = lists:unzip(hd(Data)),
    ColumnTypes = lists:map(GetFieldTypeF, ColumnNames),
    #tsqueryresp{columns = [#tscolumndescription{name = Name, type = Type}
                            || {Name, Type} <- lists:zip(ColumnNames, ColumnTypes)],
                 rows = make_data(Rows)}.

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

make_data(Rows) ->
    make_d2(Rows, []).

make_d2([], Acc) ->
    lists:reverse(Acc);
make_d2([{tsrow, Row} | T], Acc) ->
    make_d2(T, [make_d3(Row, []) | Acc]).

make_d3([], Acc) ->
    list_to_tuple(lists:reverse(Acc));
make_d3([#tscell{binary_value    = Bin,
		 integer_value   = undefined,
		 numeric_value   = undefined,
		 timestamp_value = undefined,
		 boolean_value   = undefined,
		 set_value       = [],
		 map_value       = undefined} | T], Acc) ->
    make_d3(T, [Bin | Acc]);
make_d3([#tscell{binary_value    = undefined,
		 integer_value   = Int,
		 numeric_value   = undefined,
		 timestamp_value = undefined,
		 boolean_value   = undefined,
		 set_value       = [],
		 map_value       = undefined} | T], Acc) ->
    make_d3(T, [Int | Acc]);
make_d3([#tscell{binary_value    = undefined,
		 integer_value   = undefined,
		 numeric_value   = Num,
		 timestamp_value = undefined,
		 boolean_value   = undefined,
		 set_value       = [],
		 map_value       = undefined} | T], Acc) ->
    make_d3(T, [Num | Acc]);
make_d3([#tscell{binary_value    = undefined,
		 integer_value   = undefined,
		 numeric_value   = undefined,
		 timestamp_value = Timestamp,
		 boolean_value   = undefined,
		 set_value       = [],
		 map_value       = undefined} | T], Acc) ->
    make_d3(T, [Timestamp | Acc]);
make_d3([#tscell{binary_value    = undefined,
		 integer_value   = undefined,
		 numeric_value   = undefined,
		 timestamp_value = undefined,
		 boolean_value   = Bool,
		 set_value       = [],
		 map_value       = undefined} | T], Acc) ->
    make_d3(T, [Bool | Acc]);
make_d3([#tscell{binary_value    = undefined,
		 integer_value   = undefined,
		 numeric_value   = undefined,
		 timestamp_value = undefined,
		 boolean_value   = undefined,
		 set_value       = Set,
		 map_value       = undefined} | T], Acc) ->
    make_d3(T, [Set | Acc]);
make_d3([#tscell{binary_value    = undefined,
		 integer_value   = undefined,
		 numeric_value   = undefined,
		 timestamp_value = undefined,
		 boolean_value   = undefined,
		 set_value       = [],
		 map_value       = Map} | T], Acc) ->
    make_d3(T, [Map | Acc]).
