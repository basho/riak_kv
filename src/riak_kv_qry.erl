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
        ddl | select | describe | insert.
%% and also their corresponding records (mainly for use in specs)
-type sql_query_type_record() ::
        ?SQL_SELECT{} |
        #riak_sql_describe_v1{} |
        #riak_sql_insert_v1{}.

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
        {ok, SQL} ->
            submit(SQL, DDL)
    end;

submit(#riak_sql_describe_v1{}, DDL) ->
    do_describe(DDL);
submit(SQL = #riak_sql_insert_v1{}, _DDL) ->
    do_insert(SQL);
submit(SQL = ?SQL_SELECT{}, DDL) ->
    do_select(SQL, DDL).


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
            case xlate_insert_to_putdata(Values, Positions, Empty) of
                {error, Reason} ->
                    {error, Reason};
                {ok, Data} ->
                    insert_putreqs(Mod, Table, Data)
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
-spec xlate_insert_to_putdata([[riak_ql_ddl:data_value()]], [pos_integer()], tuple(undefined)) ->
                              {ok, [tuple()]} | {error, string()}.
xlate_insert_to_putdata(Values, Positions, Empty) ->
    ConvFn = fun(RowVals, {Good, Bad, RowNum}) ->
                 case make_insert_row(RowVals, Positions, Empty) of
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

-spec make_insert_row([] | [riak_ql_ddl:data_value()], [] | [pos_integer()], tuple()) ->
                      {ok, tuple()} | {error, string()}.
make_insert_row([], _Positions, Row) when is_tuple(Row) ->
    %% Out of entries in the value - row is populated with default values
    %% so if we run out of data for implicit/explicit fieldnames can just return
    {ok, Row};
make_insert_row(_, [], Row) when is_tuple(Row) ->
    %% Too many values for the field
    {error, too_many_values};
%% Make sure the types match
make_insert_row([{_Type, Val} | Values], [Pos | Positions], Row) when is_tuple(Row) ->
    make_insert_row(Values, Positions, setelement(Pos, Row, Val)).


%% DESCRIBE

-spec do_describe(?DDL{}) ->
                         {ok, query_tabular_result()} | {error, term()}.
do_describe(?DDL{fields = FieldSpecs,
                 partition_key = #key_v1{ast = PKSpec},
                 local_key     = #key_v1{ast = LKSpec}}) ->
    ColumnNames = [<<"Column">>, <<"Type">>, <<"Is Null">>, <<"Primary Key">>, <<"Local Key">>],
    ColumnTypes = [   varchar,     varchar,     boolean,        sint64,             sint64    ],
    Rows =
        [[Name, list_to_binary(atom_to_list(Type)), Nullable,
          column_pk_position_or_blank(Name, PKSpec),
          column_lk_position_or_blank(Name, LKSpec)]
         || #riak_field_v1{name = Name,
                           type = Type,
                           optional = Nullable} <- FieldSpecs],
    {ok, {ColumnNames, ColumnTypes, Rows}}.


%% the following two functions are identical, for the way fields and
%% keys are represented as of 2015-12-18; duplication here is a hint
%% of things to come.
-spec column_pk_position_or_blank(binary(), [#param_v1{}]) -> integer() | [].
column_pk_position_or_blank(Col, KSpec) ->
    count_to_position(Col, KSpec, 1).

-spec column_lk_position_or_blank(binary(), [#param_v1{}]) -> integer() | [].
column_lk_position_or_blank(Col, KSpec) ->
    count_to_position(Col, KSpec, 1).

count_to_position(_, [], _) ->
    [];
count_to_position(Col, [#param_v1{name = [Col]} | _], Pos) ->
    Pos;
count_to_position(Col, [#hash_fn_v1{args = [#param_v1{name = [Col]} | _]} | _], Pos) ->
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
             [[<<"f">>, <<"varchar">>,   false, 1,  1],
              [<<"s">>, <<"varchar">>,   false, 2,  2],
              [<<"t">>, <<"timestamp">>, false, 3,  3],
              [<<"w">>, <<"sint64">>, false, [], []],
              [<<"p">>, <<"double">>, true,  [], []]]}},
       Res).

validate_make_insert_row_basic_test() ->
    Data = [{integer,4}, {binary,<<"bamboozle">>}, {float, 3.14}],
    Positions = [3, 1, 2],
    Row = {undefined, undefined, undefined},
    Result = make_insert_row(Data, Positions, Row),
    ?assertEqual(
        {ok, {<<"bamboozle">>, 3.14, 4}},
        Result
    ).

validate_make_insert_row_too_many_test() ->
    Data = [{integer,4}, {binary,<<"bamboozle">>}, {float, 3.14}, {integer, 8}],
    Positions = [3, 1, 2],
    Row = {undefined, undefined, undefined},
    Result = make_insert_row(Data, Positions, Row),
    ?assertEqual(
        {error, too_many_values},
        Result
    ).


validate_xlate_insert_to_putdata_ok_test() ->
    Empty = list_to_tuple(lists:duplicate(5, undefined)),
    Values = [[{integer, 4}, {binary, <<"babs">>}, {float, 5.67}, {binary, <<"bingo">>}],
              [{integer, 8}, {binary, <<"scat">>}, {float, 7.65}, {binary, <<"yolo!">>}]],
    Positions = [5, 3, 1, 2, 4],
    Result = xlate_insert_to_putdata(Values, Positions, Empty),
    ?assertEqual(
        {ok,[{5.67,<<"bingo">>,<<"babs">>,undefined,4},
             {7.65,<<"yolo!">>,<<"scat">>,undefined,8}]},
        Result
    ).

validate_xlate_insert_to_putdata_too_many_values_test() ->
    Empty = list_to_tuple(lists:duplicate(5, undefined)),
    Values = [[{integer, 4}, {binary, <<"babs">>}, {float, 5.67}, {binary, <<"bingo">>}, {integer, 7}],
           [{integer, 8}, {binary, <<"scat">>}, {float, 7.65}, {binary, <<"yolo!">>}]],
    Positions = [3, 1, 2, 4],
    Result = xlate_insert_to_putdata(Values, Positions, Empty),
    ?assertEqual(
        {error,{too_many_insert_values, [1]}},
        Result
    ).

-endif.
