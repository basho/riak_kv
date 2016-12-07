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

-define(EMPTYRESPONSE, {[], [], []}).

%% enumerate all current SQL query types
-type query_type() ::
        ddl | explain | describe | insert | select | show_tables | show_create_table.
%% and also their corresponding records (mainly for use in specs)
-type sql_query_type_record() ::
        ?SQL_SELECT{} |
        #riak_sql_describe_v1{} |
        #riak_sql_insert_v1{} |
        #riak_sql_show_tables_v1{} |
        #riak_sql_show_create_table_v1{} |
        #riak_sql_delete_query_v1{}.

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
                Type, Compiled, []),
            submit(SQL, DDL)
    end;

submit(#riak_sql_describe_v1{}, DDL) ->
    riak_ql_describe:describe(DDL);
submit(#riak_sql_show_create_table_v1{}, ?DDL{table = Table} = DDL) ->
    Props = riak_core_bucket:get_bucket({Table, Table}),
    riak_ql_show_create_table:show_create_table(DDL, Props);
submit(SQL = #riak_sql_insert_v1{}, _DDL) ->
    do_insert(SQL);
submit(SQL = ?SQL_SELECT{}, DDL) ->
    do_select(SQL, DDL);
submit(#riak_sql_show_tables_v1{}, _DDL) ->
    do_show_tables();
submit(#riak_sql_explain_query_v1{'EXPLAIN' = Select}, DDL) ->
    do_explain(DDL, Select);
submit(#riak_sql_delete_query_v1{} = Q, _DDL) ->
    do_delete(Q).

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
            Types = [catch riak_ql_ddl:get_storage_type(Mod:get_field_type([Column])) || {identifier, [Column]} <- Fields],
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
                              [riak_ql_ddl:internal_field_type()]) ->
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
                      [riak_ql_ddl:internal_field_type()]) ->
                      {ok, tuple()} | {error, string()} |
                      {error, {atom(), string()}}.
make_insert_row(Vals, _Positions, Row, _FieldTypes)
  when length(Vals) > size(Row) ->
    %% diagnose too_many_values before eventual timestamp conversion
    %% errors, so that it is reported with higher precedence than that
    {error, too_many_values};
make_insert_row([], _Positions, Row, _FieldTypes) ->
    %% Out of entries in the value - row is populated with default values
    %% so if we run out of data for implicit/explicit fieldnames can just return
    {ok, Row};
make_insert_row([{identifier, Identifier} | _Values], _Positions, _Row, _FieldTypes) ->
    {error, {identifier_unexpected, Identifier}};
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





%% SELECT

-spec do_select(?SQL_SELECT{}, ?DDL{}) ->
                       {ok, query_tabular_result()} | {error, term()}.
do_select(SQL, ?DDL{table = BucketType} = DDL) ->
    Mod = riak_ql_ddl:make_module_name(BucketType),

    case riak_ql_ddl:is_query_valid(Mod, DDL, riak_kv_ts_util:sql_record_to_tuple(SQL)) of
        true ->
            case riak_kv_qry_compiler:compile(DDL, SQL) of
                {error,_} = Error ->
                    Error;
                {ok, SubQueries} ->
                    %% these fields are used to create a DDL for the query buffer table;
                    %% we extract them here to avoid doing another compilation of OrigQry
                    %% in riak_kv_qry_buffers:get_or_create_qbuf
                    ?SQL_SELECT{'SELECT'   = CompiledSelect,
                                'ORDER BY' = CompiledOrderBy,
                                'LIMIT'    = Limit,
                                'OFFSET'   = Offset} = hd(SubQueries),
                    FullCycle =
                        fun(QBufRef) ->
                            maybe_await_query_results(
                              riak_kv_qry_queue:put_on_queue(self(), SubQueries, DDL, QBufRef))
                        end,
                    case maybe_create_query_buffer(
                           SQL, length(SubQueries), CompiledSelect, CompiledOrderBy, []) of
                        {error, Reason} ->
                            %% query buffers are non-optional for
                            %% ORDER BY queries
                            {error, {qbuf_create_error, Reason}};
                        {ok, undefined} ->
                            %% query buffer path not advisable (query
                            %% has no ORDER BY)
                            FullCycle(undefined);
                        {ok, {new, QBufRef}} ->
                            %% query is eligible, and new buffer is ready
                            FullCycle(QBufRef);
                        {ok, {existing, QBufRef}} ->
                            %% query is eligible AND it is a follow-up
                            %% query: yay results!
                            try riak_kv_qry_buffers:fetch_limit(
                                  QBufRef,
                                  riak_kv_qry_buffers:limit_to_scalar(Limit),
                                  riak_kv_qry_buffers:offset_to_scalar(Offset)) of
                                Result ->
                                    Result
                            catch
                                Error:Reason ->
                                    lager:warning("Failed to fetch results from qbuf for ~p: ~p:~p",
                                                  [SQL, Error, Reason]),
                                    {error, Reason}
                            end
                    end
            end;
        {false, Errors} ->
            {error, {invalid_query, format_query_syntax_errors(Errors)}}
    end.


-spec maybe_create_query_buffer(?SQL_SELECT{}, pos_integer(),
                                #riak_sel_clause_v1{}, [riak_kv_qry_compiler:sorter()],
                                proplists:proplist()) ->
                                       {ok, {new|existing, riak_kv_qry_buffers:qbuf_ref()} |
                                             undefined} |
                                       {error, any()}.
maybe_create_query_buffer(?SQL_SELECT{'ORDER BY' = [],
                                      'LIMIT'    = [],
                                      'OFFSET'   = []},
                          _NSubQueries, _CompiledSelect, _CompiledOrderBy, _Options) ->
    {ok, undefined};
maybe_create_query_buffer(SQL, NSubqueries, CompiledSelect, CompiledOrderBy, Options) ->
    try riak_kv_qry_buffers:get_or_create_qbuf(
           SQL, NSubqueries, CompiledSelect, CompiledOrderBy, Options) of
        {ok, QBufRefNewOrExisting} ->
            {ok, QBufRefNewOrExisting};
        {error, Reason} ->
            lager:warning("Failed to set up query buffer for ~p: ~p", [SQL, Reason]),
            {error, Reason}
    catch
        Error:Reason ->
            lager:warning("Failed to set up qbuf for ~p: ~p:~p",
                          [SQL, Error, Reason]),
            {error, Reason}
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
    Tables = riak_kv_compile_tab:get_table_status_pairs(),
    build_show_tables_result(Tables).

-spec build_show_tables_result([{binary(),binary()}]) -> {ok, query_tabular_result()}.
build_show_tables_result(Tables) ->
    ColumnNames = [<<"Table">>, <<"Status">>],
    ColumnTypes = [varchar,varchar],
    Rows = [[TableName, Status] || {TableName, Status} <- Tables],
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

%%
%% Single Key DELETE statement
%%

do_delete(#riak_sql_delete_query_v1{'FROM'     = F,
                                    'WHERE'    = W,
                                    helper_mod = Mod}) ->
    case Mod:get_delete_key(W) of
        {ok, Key} ->
            case riak_kv_ts_api:delete_data(Key, F) of
                ok                -> {ok, ?EMPTYRESPONSE};
                {error, notfound} -> {ok, ?EMPTYRESPONSE};
                {error, Err}      -> Msg1 = io_lib:format("Delete failed: ~p", [Err]),
                                     {error, Msg1}
            end;
        {error, Err} ->
            Msg2 = io_lib:format("Unable to get delete key: ~p", [Err]),
            {error, Msg2}
    end.

%%%===================================================================
%%% Unit tests
%%%===================================================================

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

show_tables_test() ->
    Res = build_show_tables_result([
            {<<"fafa">>, <<"Active">>},
            {<<"lala">>, <<"Not Active">>}]),
    ?assertMatch(
        {ok, {[<<"Table">>,<<"Status">>],
                [varchar,varchar],
                [[<<"fafa">>, <<"Active">>],
                 [<<"lala">>, <<"Not Active">>]]}},
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
                "g BLOB,"
                "PRIMARY KEY  ((c, QUANTUM(b, 1, 's')), c,b,a))")),
    riak_ql_ddl_compiler:compile_and_load_from_tmp(DDL),
    SQL = "SELECT a,b,c FROM tab WHERE b > 0 AND b < 2000 AND a=319 AND c='hola' AND (d=15 OR (e=true AND (f='adios' OR g=0xefface)))",
    {ok, Q} = riak_ql_parser:parse(riak_ql_lexer:get_tokens(SQL)),
    {ok, Select} = riak_kv_ts_util:build_sql_record(select, Q, []),
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
            <<"(((d = 15) OR ((e = true) AND ((f = 'adios') OR (g = 0xefface)))) AND (a = 319))">>],
        [2,<<"dev1@127.0.0.1/0, dev1@127.0.0.1/1, dev1@127.0.0.1/1">>,
            <<"c = 'hola', b = 1000">>,false,<<"c = 'hola', b = 2000">>,false,
            <<"(((d = 15) OR ((e = true) AND ((f = 'adios') OR (g = 0xefface)))) AND (a = 319))">>]],
    {ok, {_, _, ActualRows}} = Res,
    ?assertEqual(ExpectedRows, ActualRows),
    Res1 = submit("EXPLAIN " ++ SQL, DDL),
    {ok, {_, _, ActualRows1}} = Res1,
    ?assertEqual(ExpectedRows, ActualRows1),
    meck:unload(riak_client),
    meck:unload(riak_core_apl),
    meck:unload(riak_core_bucket),
    meck:unload(riak_core_claimant).

show_create_table_test() ->
    CreateSQL = "CREATE TABLE creation ("
        "a SINT64 NOT NULL,\n"
        "b TIMESTAMP NOT NULL,\n"
        "c VARCHAR NOT NULL,\n"
        "d SINT64,\n"
        "e BOOLEAN,\n"
        "f VARCHAR,\n"
        "PRIMARY KEY ((c, a, QUANTUM(b, 1, 's')),\nc, a, b))",
    PropsSQL = "\nWITH (active = true,\n"
        "allow_mult = true,\n"
        "dvv_enabled = true,\n"
        "dw = quorum,\n"
        "last_write_wins = false,\n"
        "n_val = 2,\n"
        "notfound_ok = true,\n"
        "postcommit = '',\n"
        "pr = 0,\n"
        "pw = 0,\n"
        "r = quorum,\n"
        "rw = quorum,\n"
        "w = quorum)",
    %NoNewLines = re:replace(CreateSQL, "\n", " ", [global,{return,list}]),
    {ddl, DDL, _Props} =
        riak_ql_parser:ql_parse(
            riak_ql_lexer:get_tokens(
                CreateSQL ++
                "WITH (n_val=2)")),
    Props = [
        {young_vclock, 20},
        {w, quorum},
        {small_vclock, 50},
        {rw, quorum},
        {r, quorum},
        {pw, 0},
        {precommit, []},
        {pr, 0},
        {postcommit, []},
        {old_vclock, 86400},
        {notfound_ok, true},
        {n_val, 2},
        {linkfun, {modfun,riak_kv_wm_link_walker,mapreduce_linkfun}},
        {last_write_wins, false},
        {dw, quorum},
        {dvv_enabled, true},
        {ddl_compiler_version, 319058810386694792992737611904771446619},
        {ddl, {ddl_v2,<<"creation">>,
            [{riak_field_v1,<<"a">>,1,sint64,false},
                {riak_field_v1,<<"b">>,2,timestamp,false},
                {riak_field_v1,<<"c">>,3,varchar,false},
                {riak_field_v1,<<"d">>,4,sint64,true},
                {riak_field_v1,<<"e">>,5,boolean,true},
                {riak_field_v1,<<"f">>,6,varchar,true}],
            {key_v1,[{param_v2,[<<"c">>],undefined},
                {param_v2,[<<"a">>],undefined},
                {hash_fn_v1,riak_ql_quanta,quantum,
                    [{param_v2,[<<"b">>],undefined},1,s],
                    timestamp}]},
            {key_v1,[{param_v2,[<<"c">>],undefined},
                {param_v2,[<<"a">>],undefined},
                {param_v2,[<<"b">>],undefined}]},
            v1}},
        {chash_keyfun, {riak_core_util,chash_std_keyfun}},
        {big_vclock, 50},
        {basic_quorum, false},
        {allow_mult, true},
        {write_once, true},
        {active, true},
        {claimant, 'dev1@127.0.0.1'}
    ],
    Res = riak_ql_show_create_table:show_create_table(DDL, Props),
    ExpectedRows =
        [[CreateSQL ++ PropsSQL]],
    ?assertMatch(
        {ok, {_, _, ExpectedRows}},
        Res).

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
    {ok, Insert} = riak_kv_ts_util:build_sql_record(insert, Q, []),
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

validate_xlate_insert_to_putdata_unexpected_identifier_test() ->
    Empty = list_to_tuple(lists:duplicate(4, undefined)),
    Values = [[{integer, 4}, {identifier, <<"babs">>}, {float, 5.67}, {binary, <<"bingo">>}, {integer, 7}],
              [{integer, 8}, {binary, <<"scat">>}, {float, 7.65}, {binary, <<"yolo!">>}]],
    Positions = [3, 1, 2, 4],
    Result = xlate_insert_to_putdata(Values, Positions, Empty, [double, varchar, varchar, binary]),
    %% TODO: the error for an insert which may span multiple rows  currently
    %% only supports a single reason due to levels above and below, most
    %% constraining being the translation of too_many_insert_values into the
    %% human-readable message:
    %% {error,{1003,<<"Invalid data found at row index(es) 1">>}}
    ?assertEqual(
        {error,{too_many_insert_values, [1]}},
        Result
    ).

-endif.
