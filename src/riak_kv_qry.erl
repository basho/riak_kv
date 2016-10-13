%%-------------------------------------------------------------------
%%
%% riak_kv_qry: Riak SQL API
%%
%% Copyright (C) 2016 Basho Technologies, Inc. All rights reserved
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
%%-------------------------------------------------------------------

%% @doc API endpoints for the Riak SQL.  Functions in this module
%%      prepare and validate raw queries, pass them to riak_kv_qry_queue

-module(riak_kv_qry).

-export([
         submit/2,
         empty_result/0,
         format_query_syntax_errors/1
        ]).

-include("riak_kv_ts.hrl").

%% enumerate all current SQL query types
-type query_type() ::
        ddl | explain | describe | insert | select | show_tables.
%% and also their corresponding records (mainly for use in specs)
-type sql_query_type_record() ::
        ?SQL_SELECT{} |
        #riak_sql_describe_v1{} |
        #riak_sql_insert_v1{} |
        #riak_sql_show_tables_v1{}.

-type query_tabular_result() :: {[riak_pb_ts_codec:tscolumnname()],
                                 [riak_pb_ts_codec:tscolumntype()],
                                 [list(riak_pb_ts_codec:ldbvalue())]}.

-export_type([query_type/0, sql_query_type_record/0,
              query_tabular_result/0]).


%% No coverage plan for parallel requests
-spec submit(string() | sql_query_type_record(), ?DDL{}) ->
    {ok, query_tabular_result()} | {error, any()}.
%% @doc Parse, validate against DDL, and submit a query for execution.
%%      To get the results of running the query, use fetch/1.
submit(SQLString, DDL) when is_list(SQLString) ->
    case catch riak_ql_parser:parse(
                 riak_ql_lexer:get_tokens(SQLString)) of
        {error, _Reason} = Error ->
            Error;
        {'EXIT', Reason} ->  %% lexer problem
            {error, Reason};
        {ok, Compiled} ->
            Type = proplists:get_value(type, Compiled),
            {ok, SQL} = riak_kv_ts_util:build_sql_record(
                Type, Compiled, undefined),
            submit(SQL, DDL)
    end;

submit(#riak_sql_describe_v1{}, DDL) ->
    do_describe(DDL);
submit(SQL = #riak_sql_insert_v1{}, _DDL) ->
    do_insert(SQL);
submit(SQL = ?SQL_SELECT{}, DDL) ->
    do_select(SQL, DDL);
submit(#riak_sql_show_tables_v1{}, _DDL) ->
    do_show_tables();
submit(#riak_sql_explain_query_v1{'EXPLAIN' = Select}, DDL) ->
    do_explain(DDL, Select).

%% ---------------------
%% local functions

%%
%% INSERT statements
%%
-spec do_insert(#riak_sql_insert_v1{}) -> {ok, query_tabular_result()} | {error, term()}.
do_insert(#riak_sql_insert_v1{'INSERT' = Table,
                              fields = Fields,
                              values = Values}) ->
    Mod = riak_ql_ddl:make_module_name(Table),
    case lookup_field_positions(Mod, Fields) of
        {ok, Positions} ->
            Empty = make_empty_row(Mod),
            Types = [catch Mod:get_field_type([Column]) || {identifier, [Column]} <- Fields],
            try xlate_insert_to_putdata(Values, Positions, Empty, Types) of
                {ok, Data} ->
                    insert_putreqs(Mod, Table, Data);
                {error, Reason} ->
                    {error, Reason}
            catch
                throw:Reason ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

insert_putreqs(Mod, Table, Data) ->
    case riak_kv_ts_util:validate_rows(Mod, Data) of
        [] ->
            case riak_kv_ts_api:put_data(Data, Table, Mod) of
                ok ->
                    {ok, empty_result()};
                {error, {some_failed, ErrorCount}} ->
                    {error, io_lib:format("Failed to put ~b record(s)", [ErrorCount])};
                {error, no_type} ->
                    {error, io_lib:format("~ts is not an active table", [Table])};
                {error, OtherReason} ->
                    {error, OtherReason}
            end;
        BadRowIdxs when is_list(BadRowIdxs) ->
            {error, {invalid_data, BadRowIdxs}}
    end.

%%
%% Return an all-null empty row ready to be populated by the values
%%
-spec make_empty_row(module()) -> tuple(undefined).
make_empty_row(Mod) ->
    Positions = Mod:get_field_positions(),
    list_to_tuple(lists:duplicate(length(Positions), undefined)).

%%
%% Lookup the index of the field names selected to insert.
%%
%% This *requires* that once schema changes take place the DDL fields are left in order.
%%
-spec lookup_field_positions(module(), [riak_ql_ddl:field_identifier()]) ->
                           {ok, [pos_integer()]} | {error, string()}.
lookup_field_positions(Mod, FieldIdentifiers) ->
    GoodBadPositions =
        lists:foldl(
          fun({identifier, FieldName}, {Good, Bad}) ->
                  case Mod:is_field_valid(FieldName) of
                      false ->
                          {Good, [FieldName | Bad]};
                      true ->
                          {[Mod:get_field_position(FieldName) | Good], Bad}
                  end
          end, {[], []}, FieldIdentifiers),
    case GoodBadPositions of
        {Positions, []} ->
            {ok, lists:reverse(Positions)};
        {_, Errors} ->
            {error, {undefined_fields, Errors}}
    end.

%%
%% Map the list of values from statement order into the correct place in the tuple.
%% If there are less values given than the field list the NULL will carry through
%% and the general validation rules should pick that up.
%% If there are too many values given for the fields it returns an error.
%%
%% Additionally, we will convert Value in {varchar, Value} into an
%% integer when that value is a timestamp. We would do it as we
%% repackage list to tuple here, rather than separately as a
%% post-processing step on Data, to avoid another round of
%% list_to_tuple(tuple_to_list())
-spec xlate_insert_to_putdata([[riak_ql_ddl:data_value()]], [pos_integer()], tuple(undefined),
                              [riak_ql_ddl:simple_field_type()]) ->
                              {ok, [tuple()]} | {error, string()}.
xlate_insert_to_putdata(Values, Positions, Empty, FieldTypes) ->
    ConvFn = fun(RowVals, {Good, Bad, RowNum}) ->
                 case make_insert_row(RowVals, Positions, Empty, FieldTypes) of
                     {ok, Row} ->
                         {[Row | Good], Bad, RowNum + 1};
                     {error, _Reason} ->
                         {Good, [RowNum | Bad], RowNum + 1}
                 end
             end,
    Converted = lists:foldl(ConvFn, {[], [], 1}, Values),
    case Converted of
        {PutData, [], _} ->
            {ok, lists:reverse(PutData)};
        {_, Errors, _} ->
            {error, {too_many_insert_values, lists:reverse(Errors)}}
    end.

-spec make_insert_row([riak_ql_ddl:data_value()], [pos_integer()], tuple(),
                      [riak_ql_ddl:simple_field_type()]) ->
                      {ok, tuple()} | {error, string()}.
make_insert_row(Vals, _Positions, Row, _FieldTypes)
  when length(Vals) > size(Row) ->
    %% diagnose too_many_values before eventual timestamp conversion
    %% errors, so that it is reported with higher precedence than that
    {error, too_many_values};
make_insert_row([], _Positions, Row, _FieldTypes) ->
    %% Out of entries in the value - row is populated with default values
    %% so if we run out of data for implicit/explicit fieldnames can just return
    {ok, Row};
make_insert_row([TypedVal | Values], [Pos | Positions], Row, FieldTypes) ->
    %% Note the Type in TypedVal = {Type, _} is what the value was
    %% parsed into, while its counterpart in FieldTypes is what DDL
    %% expects it to be
    Val = maybe_convert_timestamp(TypedVal, lists:nth(Pos, FieldTypes)),
    make_insert_row(Values, Positions, setelement(Pos, Row, Val), FieldTypes).

maybe_convert_timestamp({binary, String}, timestamp) ->
    riak_kv_ts_util:varchar_to_timestamp(String);
maybe_convert_timestamp({_NonTSType, Val}, _OtherType) ->
    Val.


%% DESCRIBE

-spec do_describe(?DDL{}) ->
                         {ok, query_tabular_result()} | {error, term()}.
do_describe(?DDL{fields = FieldSpecs,
                 partition_key = #key_v1{ast = PKSpec},
                 local_key     = #key_v1{ast = LKSpec}}) ->
    ColumnNames = [<<"Column">>, <<"Type">>, <<"Is Null">>, <<"Primary Key">>, <<"Local Key">>, <<"Interval">>, <<"Unit">>, <<"Sort Order">>],
    ColumnTypes = [   varchar,      varchar,    boolean,       sint64,            sint64,         sint64,         varchar,      varchar],
    Quantum = find_quantum_field(PKSpec),
    Rows =
        [[Name, list_to_binary(atom_to_list(Type)), Nullable,
          column_pk_position_or_blank(Name, PKSpec),
          column_lk_position_or_blank(Name, LKSpec)] ++
          columns_quantum_or_blank(Name, Quantum) ++
          column_lk_order(Name, LKSpec)
         || #riak_field_v1{name = Name,
                           type = Type,
                           optional = Nullable} <- FieldSpecs],
    {ok, {ColumnNames, ColumnTypes, Rows}}.

%% Return the sort order of the local key for this column, or null if it is not
%% a local key or has an undefined sort order.
column_lk_order(Name, LK) when is_binary(Name) ->
    case lists:keyfind([Name], #riak_field_v1.name, LK) of
        ?SQL_PARAM{ ordering = descending } ->
            <<"DESC">>;
        ?SQL_PARAM{ ordering = ascending } ->
            <<"ASC">>;
        _ ->
            []
    end.

%% the following two functions are identical, for the way fields and
%% keys are represented as of 2015-12-18; duplication here is a hint
%% of things to come.
-spec column_pk_position_or_blank(binary(), [?SQL_PARAM{}]) -> integer() | [].
column_pk_position_or_blank(Col, KSpec) ->
    count_to_position(Col, KSpec, 1).

-spec column_lk_position_or_blank(binary(), [?SQL_PARAM{}]) -> integer() | [].
column_lk_position_or_blank(Col, KSpec) ->
    count_to_position(Col, KSpec, 1).

%% Extract the quantum column information, if it exists in the table definition
%% and put in two additional columns
-spec columns_quantum_or_blank(Col :: binary(), PKSpec :: [?SQL_PARAM{}|#hash_fn_v1{}]) ->
      [binary() | []].
columns_quantum_or_blank(Col, #hash_fn_v1{args = [?SQL_PARAM{name = [Col]}, Interval, Unit]}) ->
    [Interval, list_to_binary(io_lib:format("~p", [Unit]))];
columns_quantum_or_blank(_Col, _PKSpec) ->
    [[], []].

%% Find the field associated with the quantum, if there is one
-spec find_quantum_field([?SQL_PARAM{}|#hash_fn_v1{}]) -> [] | #hash_fn_v1{}.
find_quantum_field([]) ->
    [];
find_quantum_field([Q = #hash_fn_v1{}|_]) ->
    Q;
find_quantum_field([_|T]) ->
    find_quantum_field(T).

count_to_position(_, [], _) ->
    [];
count_to_position(Col, [?SQL_PARAM{name = [Col]} | _], Pos) ->
    Pos;
count_to_position(Col, [#hash_fn_v1{args = [?SQL_PARAM{name = [Col]} | _]} | _], Pos) ->
    Pos;
count_to_position(Col, [_ | Rest], Pos) ->
    count_to_position(Col, Rest, Pos + 1).


%% SELECT

-spec do_select(?SQL_SELECT{}, ?DDL{}) ->
                       {ok, query_tabular_result()} | {error, term()}.
do_select(SQL, ?DDL{table = BucketType} = DDL) ->
    Mod = riak_ql_ddl:make_module_name(BucketType),
    MaxSubQueries =
        app_helper:get_env(riak_kv, timeseries_query_max_quanta_span),

    case riak_ql_ddl:is_query_valid(Mod, DDL, riak_kv_ts_util:sql_record_to_tuple(SQL)) of
        true ->
            case riak_kv_qry_compiler:compile(DDL, SQL, MaxSubQueries) of
                {error,_} = Error ->
                    Error;
                {ok, SubQueries} ->
                    maybe_await_query_results(
                      riak_kv_qry_queue:put_on_queue(self(), SubQueries, DDL))
            end;
        {false, Errors} ->
            {error, {invalid_query, format_query_syntax_errors(Errors)}}
    end.

maybe_await_query_results({error,_} = Error) ->
    Error;
maybe_await_query_results(_) ->
    Timeout = app_helper:get_env(riak_kv, timeseries_query_timeout_ms),

    % we can't use a gen_server call here because the reply needs to be
    % from an fsm but one is not assigned if the query is queued.
    receive
        {ok, Result} ->
            {ok, Result};
        {error, Reason} ->
            {error, Reason}
    after
        Timeout ->
            {error, qry_worker_timeout}
    end.

%% Format the multiple syntax errors into a multiline error
%% message.
format_query_syntax_errors(Errors) ->
    iolist_to_binary(
        [["\n", riak_ql_ddl:syntax_error_to_msg(E)] || E <- Errors]).


-spec empty_result() -> query_tabular_result().
empty_result() ->
    {[], [], []}.

%%
%% SHOW TABLES statement
%%
-spec do_show_tables() -> {ok, query_tabular_result()} | {error, term()}.
do_show_tables() ->
    Tables = riak_kv_compile_tab:get_all_table_names(),
    build_show_tables_result(Tables).

-spec build_show_tables_result([binary()]) -> tuple().
build_show_tables_result(Tables) ->
    ColumnNames = [<<"Table">>],
    ColumnTypes = [varchar],
    Rows = [[T] || T <- Tables],
    {ok, {ColumnNames, ColumnTypes, Rows}}.

%%
%% EXPLAIN statement
%%
-spec do_explain(DDL :: ?DDL{},
                 Select :: ?SQL_SELECT{}) ->
                        {ok, query_tabular_result()} | {error, term()}.
do_explain(DDL, Select) ->
    ColumnNames = [
        <<"Subquery">>,
        <<"Coverage Plan">>,
        <<"Range Scan Start Key">>,
        <<"Is Start Inclusive?">>,
        <<"Range Scan End Key">>,
        <<"Is End Inclusive?">>,
        <<"Filter">>],
    ColumnTypes = [
        sint64,
        varchar,
        varchar,
        boolean,
        varchar,
        boolean,
        varchar],
    case riak_kv_ts_util:explain_query(DDL, Select) of
        {error, Error} ->
            {error, Error};
        Rows ->
            {ok, {ColumnNames, ColumnTypes, Rows}}
    end.

%%%===================================================================
%%% Unit tests
%%%===================================================================

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

describe_table_columns_test() ->
    {ddl, DDL, []} =
        riak_ql_parser:ql_parse(
          riak_ql_lexer:get_tokens(
            "CREATE TABLE fafa ("
            " f varchar   not null,"
            " s varchar   not null,"
            " t timestamp not null,"
            " w sint64    not null,"
            " p double,"
            " PRIMARY KEY ((f, s, quantum(t, 15, m)), "
            " f, s, t))")),
    Res = do_describe(DDL),
    ?assertMatch(
       {ok, {_, _,
             [[<<"f">>, <<"varchar">>,   false, 1,  1, [], []],
              [<<"s">>, <<"varchar">>,   false, 2,  2, [], []],
              [<<"t">>, <<"timestamp">>, false, 3,  3, 15, <<"m">>],
              [<<"w">>, <<"sint64">>, false, [], [], [], []],
              [<<"p">>, <<"double">>, true,  [], [], [], []]]}},
       Res).

describe_table_columns_no_quantum_test() ->
    {ddl, DDL, []} =
        riak_ql_parser:ql_parse(
            riak_ql_lexer:get_tokens(
                "CREATE TABLE fafa ("
                " f varchar   not null,"
                " s varchar   not null,"
                " t timestamp not null,"
                " w sint64    not null,"
                " p double,"
                " PRIMARY KEY (f, s, t))")),
    Res = do_describe(DDL),
    ?assertMatch(
        {ok, {_, _,
            [[<<"f">>, <<"varchar">>,   false, 1,  1, [], []],
             [<<"s">>, <<"varchar">>,   false, 2,  2, [], []],
             [<<"t">>, <<"timestamp">>, false, 3,  3, [], []],
             [<<"w">>, <<"sint64">>, false, [], [], [], []],
             [<<"p">>, <<"double">>, true,  [], [], [], []]]}},
        Res).

show_tables_test() ->
    Res = build_show_tables_result([<<"fafa">>,<<"lala">>]),
    ?assertMatch(
        {ok, {[<<"Table">>], [varchar], [[<<"fafa">>], [<<"lala">>]]}},
        Res).

explain_query_test() ->
    {ddl, DDL, []} =
        riak_ql_parser:ql_parse(
            riak_ql_lexer:get_tokens(
                "CREATE TABLE tab ("
                "a SINT64 NOT NULL,"
                "b TIMESTAMP NOT NULL,"
                "c VARCHAR NOT NULL,"
                "d SINT64,"
                "e BOOLEAN,"
                "f VARCHAR,"
                "PRIMARY KEY  ((c, QUANTUM(b, 1, 's')), c,b,a))")),
    riak_ql_ddl_compiler:compile_and_load_from_tmp(DDL),
    SQL = "SELECT a,b,c FROM tab WHERE b > 0 AND b < 2000 AND a=319 AND c='hola' AND (d=15 OR (e=true AND f='adios'))",
    {ok, Q} = riak_ql_parser:parse(riak_ql_lexer:get_tokens(SQL)),
    {ok, Select} = riak_kv_ts_util:build_sql_record(select, Q, undefined),
    meck:new(riak_client),
    meck:new(riak_core_apl),
    meck:new(riak_core_bucket),
    meck:new(riak_core_claimant),
    meck:expect(riak_client, ring_size, fun() -> 8 end),
    meck:expect(riak_core_claimant, get_bucket_type, fun(_, _) -> [{n_val, 3}] end),
    meck:expect(riak_core_bucket, get_bucket, fun(_) -> [{chash_keyfun,{riak_core_util,chash_std_keyfun}}] end),
    meck:expect(riak_core_apl, get_primary_apl,
        fun(_, 3, riak_kv) ->
            [{{35195593916248939066258330623111144003363405826,
                'dev1@127.0.0.1'},
                primary},
             {{274031556999544297163190906134303066185487351809,
                'dev1@127.0.0.1'},
                primary},
             {{296867520082839655260123481645494988367611297792,
                'dev1@127.0.0.1'},
                primary}]
        end),
    ?assert(meck:validate(riak_client)),
    ?assert(meck:validate(riak_core_apl)),
    ?assert(meck:validate(riak_core_bucket)),
    ?assert(meck:validate(riak_core_claimant)),
    Res = do_explain(DDL, Select),
    ExpectedRows =
        [[1,<<"dev1@127.0.0.1/0, dev1@127.0.0.1/1, dev1@127.0.0.1/1">>,
            <<"c = 'hola', b = 1">>,false,<<"c = 'hola', b = 1000">>,false,
            <<"(((d = 15) OR ((e = true) AND (f = 'adios'))) AND (a = 319))">>],
        [2,<<"dev1@127.0.0.1/0, dev1@127.0.0.1/1, dev1@127.0.0.1/1">>,
            <<"c = 'hola', b = 1000">>,false,<<"c = 'hola', b = 2000">>,false,
            <<"(((d = 15) OR ((e = true) AND (f = 'adios'))) AND (a = 319))">>]],
    ?assertMatch(
        {ok, {_, _, ExpectedRows}},
        Res),
    Res1 = submit("EXPLAIN " ++ SQL, DDL),
    ?assertMatch(
        {ok, {_, _, ExpectedRows}},
        Res1),
    meck:unload(riak_client),
    meck:unload(riak_core_apl),
    meck:unload(riak_core_bucket),
    meck:unload(riak_core_claimant).

validate_make_insert_row_basic_test() ->
    Data = [{integer,4}, {binary,<<"bamboozle">>}, {float, 3.14}],
    Positions = [3, 1, 2],
    Row = {undefined, undefined, undefined},
    Result = make_insert_row(Data, Positions, Row, [varchar, double, sint64]),
    ?assertEqual(
        {ok, {<<"bamboozle">>, 3.14, 4}},
        Result
    ).

validate_make_insert_row_too_many_test() ->
    Data = [{integer,4}, {binary,<<"bamboozle">>}, {float, 3.14}, {integer, 8}],
    Positions = [3, 1, 2],
    Row = {undefined, undefined, undefined},
    Result = make_insert_row(Data, Positions, Row, [varchar, double, sint64]),
    ?assertEqual(
        {error, too_many_values},
        Result
    ).


validate_xlate_insert_to_putdata_ok_test() ->
    Empty = list_to_tuple(lists:duplicate(5, undefined)),
    Values = [[{integer, 4}, {binary, <<"babs">>}, {float, 5.67}, {binary, <<"bingo">>}],
              [{integer, 8}, {binary, <<"scat">>}, {float, 7.65}, {binary, <<"yolo!">>}]],
    Positions = [5, 3, 1, 2, 4],
    Result = xlate_insert_to_putdata(Values, Positions, Empty, [double, varchar, varchar, binary, sint64]),
    ?assertEqual(
        {ok,[{5.67,<<"bingo">>,<<"babs">>,undefined,4},
             {7.65,<<"yolo!">>,<<"scat">>,undefined,8}]},
        Result
    ).

good_timestamp_insert_test() ->
    GoodInsert = "insert into table1 values "
        " (1, '2015', 2), "
        " (1, '2015-06', 2), "
        " (1, '2015-06-05', 2), "
        " (1, '2015-06-05 10', 2), "
        " (1, '2015-06-05 10:10:11', 2), "
        " (1, '2015-06-05 10:10:11Z', 2) ",
    {_DDL, _Mod} = helper_compile_def_to_module(
        "CREATE TABLE table1 ("
        "a SINT64 NOT NULL, "
        "b TIMESTAMP NOT NULL, "
        "c SINT64 NOT NULL, "
        "PRIMARY KEY((a, quantum(b, 15, 's')), a, b))"),
    Lexed = riak_ql_lexer:get_tokens(GoodInsert),
    {ok, Q} = riak_ql_parser:parse(Lexed),
    {ok, Insert} = riak_kv_ts_util:build_sql_record(insert, Q, undefined),
    Res = xlate_insert_to_putdata(
            Insert#riak_sql_insert_v1.values,
            [1,2,3], {undefined, undefined, undefined},
            [sint64, timestamp, sint64]),

    ?assertEqual({ok, [{1,1420070400000,2},
                       {1,1433116800000,2},
                       {1,1433462400000,2},
                       {1,1433498400000,2},
                       {1,1433499011000,2},
                       {1,1433499011000,2}]},
                Res).

helper_compile_def_to_module(SQL) ->
    Lexed = riak_ql_lexer:get_tokens(SQL),
    {ok, {DDL, _Props}} = riak_ql_parser:parse(Lexed),
    {module, Mod} = riak_ql_ddl_compiler:compile_and_load_from_tmp(DDL),
    {DDL, Mod}.


validate_xlate_insert_to_putdata_too_many_values_test() ->
    Empty = list_to_tuple(lists:duplicate(4, undefined)),
    Values = [[{integer, 4}, {binary, <<"babs">>}, {float, 5.67}, {binary, <<"bingo">>}, {integer, 7}],
              [{integer, 8}, {binary, <<"scat">>}, {float, 7.65}, {binary, <<"yolo!">>}]],
    Positions = [3, 1, 2, 4],
    Result = xlate_insert_to_putdata(Values, Positions, Empty, [double, varchar, varchar, binary]),
    ?assertEqual(
        {error,{too_many_insert_values, [1]}},
        Result
    ).

-endif.
