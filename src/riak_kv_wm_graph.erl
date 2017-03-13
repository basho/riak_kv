%% -------------------------------------------------------------------
%%
%% riak_kv_wm_graph - Grafana integration webmachine for Riak TS
%%
%% Copyright (c) 2016 Basho Technologies, Inc.  All Rights Reserved.
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
-module(riak_kv_wm_graph).

-export([
         init/1,
         allowed_methods/2,
         is_authorized/2,
         forbidden/2,
         content_types_provided/2,
         resource_exists/2
        ]).

-export([to_json/2]).

%% error response formatting functions for riak_kv_ts_api:create_table/3
-export([make_table_create_fail_resp/2,
         make_table_activate_error_timeout_resp/1,
         make_table_created_missing_resp/1]).

-include_lib("webmachine/include/webmachine.hrl").
-include_lib("riak_ql/include/riak_ql_ddl.hrl").
-include("riak_kv_wm_raw.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif. %%<< TEST

-ifdef(debug).
-define(WMTRACE, {trace, "traces"}).
-else.
-define(WMTRACE, ok).
-endif. %%<< debug

-record(ctx, {
         }).

init(_Config) ->
    {?WMTRACE, #ctx{}}.

allowed_methods(RD, Ctx) ->
    {['GET', 'HEAD', 'OPTIONS'], RD, Ctx}.

is_authorized(RD, Ctx) ->
    Table = <<>>,
    Call = show_tables,
    case riak_kv_wm_ts_util:authorize(Call, Table, RD) of
        ok -> {true, RD, Ctx};
        {error, Error} ->
            riak_kv_wm_ts_util:handle_error({not_permitted, Table, Error}, RD, Ctx);
        insecure ->
            riak_kv_wm_ts_util:handle_error(insecure_connection, RD, Ctx)
    end.

forbidden(RD, Ctx) ->
    Result = riak_kv_wm_utils:is_forbidden(RD),
    {Result, RD, Ctx}.

content_types_provided(RD, Ctx) ->
    {[{"application/json", to_json}], RD, Ctx}.

resource_exists(RD, Ctx) ->
    Exists = case q_from_path(RD, Ctx) of
                 undefined -> false;
                 _ -> true
             end,
    {Exists, RD, Ctx}.

to_json(RD, Ctx) ->
    Q = q_from_path(RD, Ctx),
    QR = query_result(Q, RD, Ctx),
    RD1 = wrq:set_resp_header("Access-Control-Allow-Origin", "*", RD),
    {QR, RD1, Ctx}.

q_from_path(RD, _Ctx) ->
    wrq:get_qs_value("q", RD).

query_result(S, RD, Ctx) when is_list(S) ->
    query_result(list_to_binary(S), RD, Ctx);
query_result(<<"SHOW TABLES LIMIT ", Limit/binary>>, RD, Ctx) ->
    Limit1 = binary_to_integer(Limit),
    show_tables(Limit1, RD, Ctx);
query_result(<<"SHOW TABLES", _T/binary>>, RD, Ctx) ->
    show_tables(undefined, RD, Ctx);
query_result(<<"SHOW TAG KEYS FROM ", TableNRest/binary>>, RD, Ctx) ->
    %% NOTE: discarding clauses after FROM intentionally
    Table = hd(string:tokens(binary_to_list(TableNRest), " ")),
    show_table_fields(Table, RD, Ctx);
query_result(<<"SHOW FIELD KEYS FROM ", TableNRest/binary>>, RD, Ctx) ->
    %% NOTE: discarding clauses after FROM intentionally
    Table = hd(string:tokens(binary_to_list(TableNRest), " ")),
    show_table_fields(Table, RD, Ctx);
query_result(<<"SHOW TAG VALUES ", TableNRest/binary>>, RD, Ctx) ->
    case parse_query_table_with_key(TableNRest) of
        {ok, [{table, Table},
              {field, Field}]} ->
            show_tag_values(Table, Field, RD, Ctx);
        {ok, [{table, Table},
              {field, Field},
              {values, _Values}]} ->
            show_tag_values(Table, Field, RD, Ctx)
    end;
query_result(Query = <<"SELECT ", _T/binary>>, RD, Ctx) ->
    %% TODO: ensure values in WHERE filter are in the field type, type coercion
    select(Query, RD, Ctx);
query_result(<<"INSERT TAG VALUES ", TableNRest/binary>>, RD, Ctx) ->
    {ok, [{table, Table},
          {field, Field},
          {values, Values}]} = parse_query_table_with_key(TableNRest),
    insert_tag_values(Table, Field, Values, RD, Ctx);
query_result(<<"DELETE TAG VALUES ", TableNRest/binary>>, RD, Ctx) ->
    {ok, [{table, Table},
          {field, Field},
          {values, Values}]} = parse_query_table_with_key(TableNRest),
    delete_tag_values(Table, Field, Values, RD, Ctx).

select(Query, RD, Ctx) when is_binary(Query) ->
    select(binary_to_list(Query), RD, Ctx);
select(Query, RD, Ctx) ->
    Queries = string:tokens(Query, ";"),
    Results = [ select_each(Query0, RD, Ctx) || Query0 <- Queries],
    Series = series_results(Results, 0, []),
    iolist_to_binary(mochijson2:encode(
        {struct, [{results, [
                    {struct, [{series, Series }]}
                 ]}]})).

select_each(Query, RD, Ctx) ->
    Query1 = replace_time_field(Query, RD, Ctx),
    {ok, {FieldNames, _FieldTypes, Rows}} = ts_query(Query1, RD, Ctx),
    {ok, {FieldNames, Rows}}.

series_results([], _I, Acc) ->
    lists:reverse(Acc);
series_results([{ok, {FieldNames, Rows}}|T], I, Acc) ->
    Series = {struct, [{name, list_to_binary(io_lib:format("~c", [$A + I]))},
                       {columns, FieldNames},
                       {values, Rows}
                      ]},
    series_results(T, I + 1, [Series|Acc]).

replace_time_field(Query, RD, Ctx) ->
    %% if the first field in the query is not the time field, prepend the time
    %% field as the first field in the query.
    QParts = string:tokens(Query, " "),
    Table = table_from_query(QParts),
    TimeField = ts_get_time_field(Table, RD, Ctx),
    replace_time_field1(TimeField, QParts, Query).

replace_time_field1(undefined, _QParts, Query) ->
    Query;
replace_time_field1(TimeField, QParts, _Query) ->
    %% replace time template variable w/ time field.
    QParts1 = [case QPart of
                   %% replace only also first argument to GROUP BY time function
                   "time($time" ++ T -> "time(" ++ TimeField ++ T;
                   "$time" ++ T -> TimeField ++ T;
                   P -> P
               end || QPart <- QParts],
    string:join([hd(QParts1)] ++ [TimeField ++ ","] ++ tl(QParts1), " ").

table_from_query([]) ->
    undefined;
table_from_query(["FROM",Table|_T]) ->
    Table;
table_from_query([_H|T]) ->
    table_from_query(T).

parse_query_table_with_key(TableNRest) when is_binary(TableNRest) ->
    parse_query_table_with_key(binary_to_list(TableNRest));
parse_query_table_with_key(TableNRest) ->
    QParts = string:tokens(TableNRest, " "),
    parse_query_table_with_key1(QParts, []).

parse_query_table_with_key1([], Acc) ->
    {ok, Acc};
parse_query_table_with_key1(["FROM", Table|T], []) ->
    parse_query_table_with_key1(T, [{table, Table}]);
parse_query_table_with_key1(["WITH", "KEY", "=", Column|T], [{table, Table}]) ->
    Column1 = string:strip(Column, both, $"),
    parse_query_table_with_key1(T, [{table, Table},
                                     {field, Column1}]);
parse_query_table_with_key1([_H|T], [{table, Table},
                                     {field, Column}]) ->
    Values = [string:strip(V, both, $,) || V <- T],
    parse_query_table_with_key1([], [{table, Table},
                                     {field, Column},
                                     {values, Values}]);
parse_query_table_with_key1([_H|T], Acc) ->
    parse_query_table_with_key1(T, Acc).

show_tables(Limit, RD, Ctx) ->
    Tables = show_tables_(Limit, RD, Ctx),
    %% filter out inactive tables and tag lookup tables by prefix
    LookupPrefix = "lookup_t_",
    Tables1 = [Table || [Table, <<"Active">>|_T] <- Tables,
                lists:sublist(binary_to_list(Table), 1,
                              length(LookupPrefix)) /= LookupPrefix],
    iolist_to_binary(mochijson2:encode(
        {struct, [{results, [
                    {struct, [{series, [
                                    {struct, [{values, Tables1}]}
                                       ]}
                             ]}
                            ]}
                 ]}
                      )).

ts_get_time_field(Table, RD, Ctx) ->
    Fields = show_table_fields_(Table, RD, Ctx),
    case lists:filter(
           fun([_Column, ColumnType, IsNullable,
                IsInPartitionKey, IsInLocalKey|_RowT]) ->
                   (ColumnType =:= <<"timestamp">> orelse
                    ColumnType =:= timestamp) andalso
                   IsNullable =:= false andalso
                   IsInPartitionKey =:= true andalso
                   IsInLocalKey =:= true
           end, Fields) of
        [] -> undefined;
        [[TimeField|_TFields]|_T] -> binary_to_list(TimeField)
    end.

show_table_fields(Table, RD, Ctx) ->
    Fields = show_table_fields_(Table, RD, Ctx),
    Fields1 = [Column || [Column|_FieldRowT] <- Fields],
    iolist_to_binary(mochijson2:encode(
        {struct, [{results, [
                    {struct, [{series, [
                                    {struct, [{values, Fields1}]}
                                       ]}
                             ]}
                            ]}
                 ]}
                      )).

show_tables_(_Limit = undefined, RD, Ctx) ->
    {ok, {_MetaFieldNames, _MetaFieldTypes, Tables}} =
        ts_query("SHOW TABLES", RD, Ctx),
    Tables;
show_tables_(Limit, RD, Ctx) ->
    Tables = show_tables_(undefined, RD, Ctx),
    case length(Tables) =< Limit of
        true -> Tables;
        _ -> lists:sublist(Tables, Limit)
    end.

show_table_fields_(Table, _RD, _Ctx) ->
    [{table, _Table},
     {mod, _Mod},
     {ddl, DDL},
     {sql, _SQL},
     {props, _WithProps}] = ts_get_query_ddl("DESCRIBE " ++ Table),
    case DDL of
        ?DDL{fields=Fields,
               partition_key=PKey,
               local_key=LKey} ->
            [ [Column, ColumnType, IsNullable,
              is_in_key(Column, PKey), is_in_key(Column, LKey)] ||
              #riak_field_v1{name=Column,
                            type=ColumnType,
                            optional=IsNullable} <- Fields];
        undefined -> []
    end.

is_in_key(_Column, ?DDL_KEY{ast=[]}) ->
    false;
is_in_key(Column, ?DDL_KEY{ast=[
                                #hash_fn_v1{
                                   args=[]
                                }|T
                               ]}) ->
    is_in_key(Column, ?DDL_KEY{ast=T});
is_in_key(Column, ?DDL_KEY{ast=[
                                #hash_fn_v1{
                                   args=[
                                         ?SQL_PARAM{name=[Column]}|_TA
                                        ]}
                               ]}) ->
    true;
is_in_key(Column, ?DDL_KEY{ast=[
                                #hash_fn_v1{
                                   args=[
                                         ?SQL_PARAM{name=[_Column]}|_TA
                                        ]}
                                |T
                               ]}) ->
    is_in_key(Column, ?DDL_KEY{ast=T});
is_in_key(Column, ?DDL_KEY{ast=[
                                ?SQL_PARAM{name=[]}|T
                               ]}) ->
    is_in_key(Column, ?DDL_KEY{ast=T});
is_in_key(Column, ?DDL_KEY{ast=[
                                ?SQL_PARAM{name=[Column]}|_T
                               ]}) ->
    true;
is_in_key(Column, ?DDL_KEY{ast=[
                                ?SQL_PARAM{name=[_Column]}
                                |T
                               ]}) ->
    is_in_key(Column, ?DDL_KEY{ast=T}).

ts_query(Query, RD, Ctx) ->
    try
        [{table, _Table},
         {mod, Mod},
         {ddl, DDL},
         {sql, SQL},
         {props, WithProps}] = ts_get_query_ddl(Query),

        case {Mod, DDL, WithProps, SQL} of
            {_Mod, _DDL, _WithProps, {riak_sql_show_tables_v1, <<>>}} ->
                riak_kv_qry:submit(SQL, DDL);
            {undefined, ?DDL{} = DDL, WithProps, _SQL} ->
                ok = riak_kv_ts_api:create_table(?MODULE, DDL, WithProps),
                result_set_empty();
            {undefined, undefined, _WithProps, _SQL} ->
                result_set_empty();
            _ ->
                riak_kv_qry:submit(SQL, DDL)
        end
    catch
        throw:Condition ->
            riak_kv_wm_ts_util:handle_error(Condition, RD, Ctx)
    end.

ts_query1(Query) ->
    case lex_parse(Query) of
        {error, {_LineNo, riak_ql_parser, Msg}} when is_integer(_LineNo) ->
            throw({query_parse_error, Msg});
        {error, {Token, riak_ql_parser, _Msg}} ->
            throw({query_parse_error, io_lib:format("Unexpected token: '~s'", [Token])});
        {'EXIT', {Reason, _StackTrace}} ->
            throw({query_parse_error, Reason});
        {error, Reason} ->
            throw({query_parse_error, Reason});
        {ok, {DDL, Props}} ->
            {create_table, DDL, Props};
        {ok, [{type, Type}|Compiled]} ->
            {ok, SQL} = riak_kv_ts_util:build_sql_record(Type, Compiled, []),
            {Type, SQL, undefined}
    end.

ts_get_query_ddl(Query) ->
    try
        {SqlType, SQL, WithProps} = ts_query1(Query),
        ts_get_query_ddl1(SqlType, SQL, WithProps)
    catch
        throw:_Condition ->
            [{table, undefined},
             {mod, undefined},
             {ddl, undefined},
             {sql, undefined},
             {props, undefined}]
    end.

ts_get_query_ddl1(create_table, DDL, WithProps) ->
    [{table, undefined},
     {mod, undefined},
     {ddl, DDL},
     {sql, undefined},
     {props, WithProps}];
ts_get_query_ddl1(_SqlType, SQL, WithProps) ->
    Table = riak_kv_ts_util:queried_table(SQL),
    {Mod, DDL} = case Table of
                     <<>> ->
                         {undefined, undefined};
                     Table ->
                         Mod0 = riak_ql_ddl:make_module_name(Table),
                         case catch Mod0:get_ddl() of
                             {'EXIT', _Reason} ->
                                 {undefined, undefined};
                             DDL0 ->
                                 {Mod0, DDL0}
                         end
                 end,
    [{table, Table},
     {mod, Mod},
     {ddl, DDL},
     {sql, SQL},
     {props, WithProps}].

tag_values_table_name(Table, Column) ->
    "lookup_t_" ++ Table ++ "_f_" ++ Column.

table_exists(Table) ->
    TableB = list_to_binary(Table),
    case ts_get_query_ddl("DESCRIBE " ++ Table) of
        [{table, TableB},
         {mod, undefined},
         {ddl, undefined},
         {sql, {riak_sql_describe_v1, TableB}},
         {props, undefined}] -> false;
        _ -> true
    end.

maybe_create_tag_values_table(Table, Column, RD, Ctx) ->
    TagValuesTable = tag_values_table_name(Table, Column),
    maybe_create_tag_values_table1(table_exists(TagValuesTable), TagValuesTable,
                                   RD, Ctx).

maybe_create_tag_values_table1(true, _TagValuesTable, _RD, _Ctx) ->
    ok;
maybe_create_tag_values_table1(_TableExists, TagValuesTable, RD, Ctx) ->
    Query = "CREATE TABLE " ++ TagValuesTable ++
            "(z SINT64 NOT NULL, v VARCHAR NOT NULL, a BOOLEAN NOT NULL," ++
            " PRIMARY KEY((z), z, v))",
    {ok, {_FieldNames, _FieldTypes, _Rows}} = ts_query(Query, RD, Ctx).

show_tag_values(Table, Column, RD, Ctx) ->
    Query1 = "SELECT v FROM " ++ tag_values_table_name(Table, Column) ++
             " WHERE z = 1 AND a = true",
    {ok, {FieldNames, _FieldTypes, Rows}} = ts_query(Query1, RD, Ctx),
    iolist_to_binary(mochijson2:encode(
        {struct, [{results, [
                    {struct, [{series, [
                        {struct, [{name, <<"A">>},
                                  {columns, FieldNames},
                                  {values, Rows}
                                 ]}
                                       ]}
                             ]}
                            ]}
                 ]}
                      )).

insert_tag_values(Table, Column, Values, RD, Ctx) ->
    update_tag_values(Table, Column, Values, true, RD, Ctx).

delete_tag_values(Table, Column, Values, RD, Ctx) ->
    update_tag_values(Table, Column, Values, false, RD, Ctx).

update_tag_values(Table, Column, Values, Active, RD, Ctx) ->
    maybe_create_tag_values_table(Table, Column, RD, Ctx),
    ValuesS0 = lists:flatten(
                 [ "(1, " ++ atom_to_list(Active) ++ ", '" ++ Value ++ "')," ||
                   Value <- Values]),
    ValuesS = lists:sublist(ValuesS0, length(ValuesS0) - 1),
    Query1 = "INSERT INTO " ++ tag_values_table_name(Table, Column) ++
             "(z, a, v) VALUES " ++
             ValuesS,
    {ok, {[], [], []}} = ts_query(Query1, RD, Ctx),
    show_tag_values(Table, Column, RD, Ctx).

lex_parse(Query) ->
    catch riak_ql_parser:parse(
      riak_ql_lexer:get_tokens(Query)).

result_set_empty() ->
    {ok, {[], [], []}}.

%% error response formatting functions for riak_kv_ts_api:create_table/3
flat_format(Format, Args) ->
    lists:flatten(io_lib:format(Format, Args)).

make_table_create_fail_resp(Table, Reason) ->
    flat_format("Failed to create table ~ts: ~p", [Table, Reason]).

make_table_activate_error_timeout_resp(Table) ->
    flat_format("Timed out while attempting to activate table ~ts", [Table]).

make_table_created_missing_resp(Table) ->
    flat_format("Table ~ts has been created but found missing", [Table]).

-ifdef(TEST).
create_test_table(FuT, Condition) ->
    create_test_table(FuT, Condition, true, false, true, true).
create_test_table(FuT, Condition,
                  IsTimestampField, IsNullable, IsInPKey, IsInLKey) ->
    Table = atom_to_list(?MODULE) ++ "_" ++
        FuT ++ "_" ++
        Condition ++ "_test",
    ColumnType = case IsTimestampField of
                     true -> "timestamp";
                     _ -> "varchar"
                 end,
    Nullable = case IsNullable of
                   true -> "";
                   _ -> "NOT NULL"
               end,
    PKey = case IsInPKey of
               true -> "(i, ts)";
               _ -> "(i)"
           end,
    LKey = case IsInLKey of
               true -> "i, ts";
               _ -> "i"
           end,
    CreateSQL = "CREATE TABLE " ++ Table ++
                "(i sint64 NOT NULL," ++
                "ts " ++ ColumnType ++ " " ++ Nullable ++ "," ++
                "PRIMARY KEY(" ++ PKey ++ "," ++ LKey ++ "))",
    {module, Mod} = helper_sql_to_module(CreateSQL),
    ts_query1(CreateSQL),
    {Table, Mod}.

helper_sql_to_module(SQL) ->
    {ok, {DDL, _WithProps}} = lex_parse(SQL),
    {module, _Module} = riak_ql_ddl_compiler:compile_and_load_from_tmp(DDL).

mock_show_tables(ExpectedActiveTables, ExpectedInactiveTables) ->
    ExpectedTablesWithStatus = [[list_to_binary(Table), <<"Active">>] ||
                                Table <- ExpectedActiveTables] ++
                               [[list_to_binary(Table), <<"Not Active">>] ||
                                Table <- ExpectedInactiveTables],
    ExpectedRes = {ok, {[<<"Table">>, <<"Status">>],
                        [varchar, varchar],
                        ExpectedTablesWithStatus}},

    meck:new(riak_kv_qry),
    meck:expect(riak_kv_qry, submit,
                fun({riak_sql_show_tables_v1, <<>>}, undefined) ->
                        ExpectedRes
                end),
    ExpectedRes.

mock_show_tables_unload() ->
    meck:unload(riak_kv_qry).

ts_get_query_ddl_invalid_sql_test() ->
    ?assertEqual([{table, undefined},
                  {mod, undefined},
                  {ddl, undefined},
                  {sql, undefined},
                  {props, undefined}],
                 ts_get_query_ddl("invalid sql")).
ts_get_query_ddl_select_test() ->
    {Table, Mod} = create_test_table("ts_get_query_ddl", "select"),
    TableB = list_to_binary(Table),
    [{table, TableB},
     {mod, Mod},
     {ddl, DDL},
     {sql, SQL},
     {props, undefined}] =
        ts_get_query_ddl("SELECT * FROM " ++ Table ++ " WHERE ts = 1"),
    ?assertEqual(ddl_v2, element(1, DDL)),
    ?assertEqual(riak_select_v3, element(1, SQL)).
ts_get_query_ddl_show_tables_test() ->
    [{table, <<>>},
     {mod, undefined},
     {ddl, undefined},
     {sql, SQL},
     {props, undefined}] =
        ts_get_query_ddl("SHOW TABLES"),
    ?assertEqual(riak_sql_show_tables_v1, element(1, SQL)).
ts_get_query_ddl_describe_table_test() ->
    {Table, _Mod} = create_test_table("ts_get_query_ddl", "describe_table"),
    [{table, undefined},
     {mod, undefined},
     {ddl, undefined},
     {sql, undefined},
     {props, undefined}] =
        ts_get_query_ddl("DESCRIBE TABLE " ++ Table).

ts_query_invalid_sql_test() ->
    ?assertEqual(result_set_empty(), ts_query("invalid sql", {}, {})).
ts_query_empty_select_test() ->
    ?assertEqual(result_set_empty(), ts_query("select * from t1 where ts = 1", {}, {})).
ts_query_create_test() ->
    meck:new(riak_kv_ts_api),
    meck:expect(riak_kv_ts_api, create_table,
                fun(_SvcMod, _DDL, _WithProps) ->
                        ok
                end),
    Res = ts_query("create table t1(ts timestamp not null," ++
                   "primary key((ts),ts))", {}, {}),
    meck:unload(riak_kv_ts_api),
    ?assertEqual(result_set_empty(), Res).
ts_query_non_empty_select_test() ->
    {Table, _Mod} = create_test_table("ts_query", "non_empty_select"),
    ExpectedResultSet = {ok, {[<<"ts">>], [timestamp], [[1]]}},
    meck:new(riak_kv_qry),
    meck:expect(riak_kv_qry, submit,
                fun(_SQL, _DDL) ->
                        ExpectedResultSet
                end),
    Res = ts_query("select * from " ++ Table ++ " where ts = 1", {}, {}),
    meck:unload(riak_kv_qry),
    ?assertEqual(ExpectedResultSet, Res).
ts_query_show_tables_empty_test() ->
    meck:new(riak_kv_qry),
    meck:expect(riak_kv_qry, submit,
                fun(_SQL, _DDL) ->
                        result_set_empty()
                end),
    Res = ts_query("show tables", {}, {}),
    meck:unload(riak_kv_qry),
    ?assertEqual(result_set_empty(), Res).
ts_query_show_tables_non_empty_test() ->
    ExpectedResultSet = {ok, {[<<"Table">>, <<"Status">>],
                              [varchar, varchar],
                              [[<<"t1">>, <<"Active">>]]}},
    meck:new(riak_kv_qry),
    meck:expect(riak_kv_qry, submit,
                fun(_SQL, _DDL) ->
                        ExpectedResultSet
                end),
    Res = ts_query("show tables", {}, {}),
    meck:unload(riak_kv_qry),
    ?assertEqual(ExpectedResultSet, Res).
ts_query_describe_table_not_present_test() ->
    meck:new(riak_kv_qry),
    meck:expect(riak_kv_qry, submit,
                fun(_SQL, _DDL) ->
                        result_set_empty()
                end),
    Res = ts_query("describe table t1", {}, {}),
    meck:unload(riak_kv_qry),
    ?assertEqual(result_set_empty(), Res).

ts_query_describe_table_present_test() ->
    ExpectedResultSet = {ok, {[<<"Column">>, <<"Type">>, <<"Nullable">>,
                               <<"Partition Key">>, <<"Local Key">>,
                               <<"Interval">>, <<"Unit">>, <<"Sort Order">>],
                              [varchar, varchar, boolean,
                               sint64, sint64,
                               sint64, varchar, varchar],
                              [[<<"ts">>, <<"timestamp">>, false,
                               1, 1,
                               [], [], []]]}},
    meck:new(riak_kv_qry),
    meck:expect(riak_kv_qry, submit,
                fun(_SQL, _DDL) ->
                        ExpectedResultSet
                end),
    Res = ts_query("show tables", {}, {}),
    meck:unload(riak_kv_qry),
    ?assertEqual(ExpectedResultSet, Res).

show_tables_empty_test() ->
    {ok, {_ColumnNames, _ColumnTypes, ExpectedRows}} =
        mock_show_tables([], []),
    ?assertEqual(ExpectedRows, show_tables_(undefined, {}, {})),
    mock_show_tables_unload().

show_tables_non_empty_test() ->
    {Table, _Mod} = create_test_table("show_tables", "non_empty"),
    {ok, {_ColumnNames, _ColumnTypes, ExpectedRows}} =
        mock_show_tables([Table], []),
    ?assertEqual(ExpectedRows, show_tables_(undefined, {}, {})),
    mock_show_tables_unload().

ts_get_time_field_table_not_present_test() ->
    ?assertEqual(undefined, ts_get_time_field("t1", {}, {})).
ts_get_time_field_no_ts_field_test() ->
    {Table, _Mod} = create_test_table("ts_get_time_field", "no_ts_field",
                                     false, false, true, true),
    ?assertEqual(undefined, ts_get_time_field(Table, {}, {})).
ts_get_time_field_is_nullable_field_test() ->
    {Table, _Mod} = create_test_table("ts_get_time_field", "is_nullable_field",
                                     true, true, false, false),
    ?assertEqual(undefined, ts_get_time_field(Table, {}, {})).
ts_get_time_field_no_ts_field_in_partition_key_test() ->
     {Table, _Mod} = create_test_table("ts_get_time_field", "no_ts_field_in_pkey",
                                      true, false, false, true),
    ?assertEqual(undefined, ts_get_time_field(Table, {}, {})).
ts_get_time_field_no_ts_field_in_local_key_test() ->
     {Table, _Mod} = create_test_table("ts_get_time_field", "no_ts_field_in_lkey",
                                      true, false, false, false),
    ?assertEqual(undefined, ts_get_time_field(Table, {}, {})).
ts_get_time_field_present_test() ->
     {Table, _Mod} = create_test_table("ts_get_time_field", "present",
                                      true, false, true, true),
    ?assertEqual("ts", ts_get_time_field(Table, {}, {})).

select_result_empty() ->
    <<"{\"results\":[{\"series\":[{\"name\":\"A\","
      "\"columns\":[],\"values\":[]}]}]}">>.

select_result_non_empty(Columns, Values) ->
    ColumnsL = ["\"" ++ Column ++ "\"" || Column <- Columns],
    ValuesL = [io_lib:format("~p", [Row])|| Row <- Values],
    list_to_binary("{\"results\":[{\"series\":[{\"name\":\"A\"," ++
      "\"columns\":[" ++ ColumnsL ++ "]," ++
      "\"values\":[" ++ ValuesL ++ "]}]}]}").

select_no_time_field_test() ->
     {Table, _Mod} = create_test_table("select", "no_time_field",
                                      true, false, false, false),
     ?assertEqual(select_result_empty(),
                  select("SELECT * FROM " ++ Table ++ " WHERE $time=1", {}, {})).

select_time_field_test() ->
    meck:new(riak_kv_qry),
    meck:expect(riak_kv_qry, submit,
                fun(_SQL, _DDL) ->
                    {ok, {[<<"ts">>], [timestamp], [[1]]}}
                end),
     {Table, _Mod} = create_test_table("select", "time_field",
                                      true, false, true, true),
     Res = select("SELECT * FROM " ++ Table ++ " WHERE $time=1", {}, {}),
     meck:unload(riak_kv_qry),
     ?assertEqual(select_result_non_empty(["ts"], [[1]]), Res).

-endif. %%<< TEST
