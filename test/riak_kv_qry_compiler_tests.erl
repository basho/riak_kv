%% -------------------------------------------------------------------
%%
%% riak_kv_qry_compiler_tests: general tests for the query compiler
%%
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
-module(riak_kv_qry_compiler_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("riak_kv/include/riak_kv_ts.hrl").
-include_lib("riak_kv/src/riak_kv_ts_error_msgs.hrl").
-include_lib("riak_ql/include/riak_ql_ddl.hrl").

-define(UTIL, riak_kv_qry_compiler_test_util).

%%
%% complete query passing tests
%%

simplest_test() ->
    DDL = ?UTIL:get_standard_ddl(),
    Query = "select weather from GeoCheckin where time > 3000 and time < 5000 and user = 'user_1' and location = 'San Francisco'",
    {ok, Q} = ?UTIL:get_query(Query),
    true = ?UTIL:is_query_valid(DDL, Q),
    [Where1] =
        test_data_where_clause(<<"San Francisco">>, <<"user_1">>, [{3000, 5000}]),
    Where2 = Where1 ++ [{start_inclusive, false}],
    PK = ?UTIL:get_standard_pk(),
    LK = ?UTIL:get_standard_lk(),
    ?assertMatch(
       {ok, [?SQL_SELECT{ is_executable = true,
                          type          = timeseries,
                          'WHERE'       = Where2,
                          partition_key = PK,
                          local_key     = LK }]},
       riak_kv_qry_compiler:compile(DDL, Q, 5)
      ).

simple_with_filter_1_test() ->
    {ok, Q} = ?UTIL:get_query(
                "SELECT weather FROM GeoCheckin "
                "WHERE time > 3000 AND time < 5000 "
                "AND user = 'user_1' AND location = 'Scotland' "
                "AND weather = 'yankee'"
               ),
    DDL = ?UTIL:get_standard_ddl(),
    true = ?UTIL:is_query_valid(DDL, Q),
    [[StartKey, EndKey |_]] =
        test_data_where_clause(<<"Scotland">>, <<"user_1">>, [{3000, 5000}]),
    Where = [
             StartKey,
             EndKey,
             {filter, {'=', {field, <<"weather">>, varchar}, {const, <<"yankee">>}}},
             {start_inclusive, false}
            ],
    PK = ?UTIL:get_standard_pk(),
    LK = ?UTIL:get_standard_lk(),
    ?assertMatch(
       {ok, [?SQL_SELECT{ is_executable = true,
                          type          = timeseries,
                          'WHERE'       = Where,
                          partition_key = PK,
                          local_key     = LK }]},
       riak_kv_qry_compiler:compile(DDL, Q, 5)
      ).

simple_with_filter_2_test() ->
    {ok, Q} = ?UTIL:get_query(
                "SELECT weather FROM GeoCheckin "
                "WHERE time >= 3000 AND time < 5000 "
                "AND user = 'user_1' AND location = 'Scotland' "
                "AND weather = 'yankee'"
               ),
    DDL = ?UTIL:get_standard_ddl(),
    true = ?UTIL:is_query_valid(DDL, Q),
    [[StartKey, EndKey |_]] =
        test_data_where_clause(<<"Scotland">>, <<"user_1">>, [{3000, 5000}]),
    Where = [
             StartKey,
             EndKey,
             {filter,   {'=', {field, <<"weather">>, varchar}, {const, <<"yankee">>}}}
            ],
    PK = ?UTIL:get_standard_pk(),
    LK = ?UTIL:get_standard_lk(),
    ?assertMatch(
       {ok, [?SQL_SELECT{ is_executable = true,
                          type          = timeseries,
                          'WHERE'       = Where,
                          partition_key = PK,
                          local_key     = LK }]},
       riak_kv_qry_compiler:compile(DDL, Q, 5)
      ).

simple_with_filter_3_test() ->
    {ok, Q} = ?UTIL:get_query(
                "SELECT weather FROM GeoCheckin "
                "WHERE time > 3000 AND time <= 5000 "
                "AND user = 'user_1' AND location = 'Scotland' "
                "AND weather = 'yankee'"
               ),
    DDL = ?UTIL:get_standard_ddl(),
    true = ?UTIL:is_query_valid(DDL, Q),
    [[StartKey, EndKey |_]] =
        test_data_where_clause(<<"Scotland">>, <<"user_1">>, [{3000, 5000}]),
    PK = ?UTIL:get_standard_pk(),
    LK = ?UTIL:get_standard_lk(),
    Where = [
             StartKey,
             EndKey,
             {filter, {'=', {field, <<"weather">>, varchar}, {const, <<"yankee">>}}},
             {start_inclusive, false},
             {end_inclusive,   true}
            ],
    ?assertMatch(
       {ok, [?SQL_SELECT{ is_executable = true,
                          type          = timeseries,
                          'WHERE'       = Where,
                          partition_key = PK,
                          local_key     = LK }]},
       riak_kv_qry_compiler:compile(DDL, Q, 5)
      ).

simple_with_2_field_filter_test() ->
    {ok, Q} = ?UTIL:get_query(
                "select weather from GeoCheckin "
                "where time > 3000 and time < 5000 "
                "and user = 'user_1' and location = 'Scotland' "
                "and weather = 'yankee' "
                "and temperature = 'yelp'"
               ),
    DDL = ?UTIL:get_standard_ddl(),
    true = ?UTIL:is_query_valid(DDL, Q),
    [[StartKey, EndKey |_]] =
        test_data_where_clause(<<"Scotland">>, <<"user_1">>, [{3000, 5000}]),
    PK = ?UTIL:get_standard_pk(),
    LK = ?UTIL:get_standard_lk(),
    Where = [
             StartKey,
             EndKey,
             {filter,
              {and_,
               {'=', {field, <<"weather">>, varchar}, {const, <<"yankee">>}},
               {'=', {field, <<"temperature">>, varchar}, {const, <<"yelp">>}}
              }
             },
             {start_inclusive, false}
            ],
    ?assertMatch(
       {ok, [?SQL_SELECT{ is_executable = true,
                          type          = timeseries,
                          'WHERE'       = Where,
                          partition_key = PK,
                          local_key     = LK }]},
       riak_kv_qry_compiler:compile(DDL, Q, 5)
      ).

complex_with_4_field_filter_test() ->
    Query = "select weather from GeoCheckin where time > 3000 and time < 5000 and user = 'user_1' and location = 'Scotland' and extra = 1 and (weather = 'yankee' or (temperature = 'yelp' and geohash = 'erko'))",
    {ok, Q} = ?UTIL:get_query(Query),
    DDL = ?UTIL:get_long_ddl(),
    true = ?UTIL:is_query_valid(DDL, Q),
    [[Start, End | _]] =
        test_data_where_clause(<<"Scotland">>, <<"user_1">>, [{3000, 5000}]),
    Where2 = [
              Start, End,
              {filter,
               {and_,
                {or_,
                 {'=', {field, <<"weather">>, varchar}, {const, <<"yankee">>}},
                 {and_,
                  {'=', {field, <<"geohash">>,     varchar}, {const, <<"erko">>}},
                  {'=', {field, <<"temperature">>, varchar}, {const, <<"yelp">>}} }
                },
                {'=', {field, <<"extra">>, sint64}, {const, 1}}
               }
              },
              {start_inclusive, false}
             ],
    PK = ?UTIL:get_standard_pk(),
    LK = ?UTIL:get_standard_lk(),
    ?assertMatch(
       {ok, [?SQL_SELECT{ is_executable = true,
                          type          = timeseries,
                          'WHERE'       = Where2,
                          partition_key = PK,
                          local_key     = LK }]},
       riak_kv_qry_compiler:compile(DDL, Q, 5)
      ).

complex_with_boolean_rewrite_filter_test() ->
    DDL = ?UTIL:get_long_ddl(),
    {ok, Q} = ?UTIL:get_query(
                "SELECT weather FROM GeoCheckin "
                "WHERE time > 3000 AND time < 5000 "
                "AND user = 'user_1' AND location = 'Scotland' "
                "AND (myboolean = False OR myboolean = tRue)"),
    true = ?UTIL:is_query_valid(DDL, Q),
    [[StartKey, EndKey |_]] =
        test_data_where_clause(<<"Scotland">>, <<"user_1">>, [{3000, 5000}]),
    PK = ?UTIL:get_standard_pk(),
    LK = ?UTIL:get_standard_lk(),
    Where = [
             StartKey,
             EndKey,
             {filter,
              {or_,
               {'=', {field, <<"myboolean">>, boolean}, {const, false}},
               {'=', {field, <<"myboolean">>, boolean}, {const, true}}
              }
             },
             {start_inclusive, false}
            ],
    ?assertMatch(
       {ok, [?SQL_SELECT{ is_executable  = true,
                          type          = timeseries,
                          'WHERE'       = Where,
                          partition_key = PK,
                          local_key     = LK
                        }]},
       riak_kv_qry_compiler:compile(DDL, Q, 5)
      ).

%% got for 3 queries to get partition ordering problems flushed out
simple_spanning_boundary_test() ->
    DDL = ?UTIL:get_standard_ddl(),
    {ok, Q} = ?UTIL:get_query(
                "select weather from GeoCheckin where time >= 3000 and time < 31000 and user = 'user_1' and location = 'Scotland'"),
    true = ?UTIL:is_query_valid(DDL, Q),
    %% get basic query
    %% now make the result - expecting 3 queries
    [Where1, Where2, Where3] =
        test_data_where_clause(<<"Scotland">>, <<"user_1">>,
                               [{3000, 15000}, {15000, 30000}, {30000, 31000}]),
    PK = ?UTIL:get_standard_pk(),
    LK = ?UTIL:get_standard_lk(),
    ?assertMatch({ok, [
                       ?SQL_SELECT{
                          'WHERE'       = Where1,
                          partition_key = PK,
                          local_key     = LK},
                       ?SQL_SELECT{
                          'WHERE'       = Where2,
                          partition_key = PK,
                          local_key     = LK},
                       ?SQL_SELECT{
                          'WHERE'       = Where3,
                          partition_key = PK,
                          local_key     = LK}
                      ]},
                 riak_kv_qry_compiler:compile(DDL, Q, 5)
                ).

%% Values right at quanta edges are tricky. Make sure we're not
%% missing them: we should be generating two queries instead of just
%% one.
boundary_quanta_test() ->
    DDL = ?UTIL:get_standard_ddl(),
    Query = "select weather from GeoCheckin where time >= 14000 and time <= 15000 and user = 'user_1' and location = 'Scotland'",
    {ok, Q} = ?UTIL:get_query(Query),
    true = ?UTIL:is_query_valid(DDL, Q),
    %% get basic query
    Actual = riak_kv_qry_compiler:compile(DDL, Q, 5),
    ?assertEqual(2, length(element(2, Actual))).

test_data_where_clause(Family, Series, StartEndTimes) ->
    Fn =
        fun({Start, End}) ->
                [
                 {startkey,        [
                                    {<<"location">>, varchar, Family},
                                    {<<"user">>, varchar,    Series},
                                    {<<"time">>, timestamp, Start}
                                   ]},
                 {endkey,          [
                                    {<<"location">>, varchar, Family},
                                    {<<"user">>, varchar,    Series},
                                    {<<"time">>, timestamp, End}
                                   ]},
                 {filter, []}
                ]
        end,
    [Fn(StartEnd) || StartEnd <- StartEndTimes].

%% check for spanning precision (same as above except selection range
%% is exact multiple of quantum size)
simple_spanning_boundary_precision_test() ->
    DDL = ?UTIL:get_standard_ddl(),
    Query = "select weather from GeoCheckin where time >= 3000 and time < 30000 and user = 'user_1' and location = 'Scotland'",
    {ok, Q} = ?UTIL:get_query(Query),
    true = ?UTIL:is_query_valid(DDL, Q),
    %% now make the result - expecting 2 queries
    [Where1, Where2] =
        test_data_where_clause(<<"Scotland">>, <<"user_1">>, [{3000, 15000}, {15000, 30000}]),
    PK = ?UTIL:get_standard_pk(),
    LK = ?UTIL:get_standard_lk(),
    ?assertMatch(
       {ok, [?SQL_SELECT{ 'WHERE'       = Where1,
                          partition_key = PK,
                          local_key     = LK},
             ?SQL_SELECT{ 'WHERE'       = Where2,
                          partition_key = PK,
                          local_key     = LK
                        }]},
       riak_kv_qry_compiler:compile(DDL, Q, 5)
      ).

%%
%% test failures
%%

simplest_compile_once_only_fail_test() ->
    DDL = ?UTIL:get_standard_ddl(),
    Query = "select weather from GeoCheckin where time >= 3000 and time < 5000 and user = 'user_1' and location = 'Scotland'",
    {ok, Q} = ?UTIL:get_query(Query),
    true = ?UTIL:is_query_valid(DDL, Q),
    %% now try and compile twice
    {ok, [Q2]} = riak_kv_qry_compiler:compile(DDL, Q, 5),
    Got = riak_kv_qry_compiler:compile(DDL, Q2, 5),
    ?assertEqual(
       {error, 'query is already compiled'},
       Got).

end_key_not_a_range_test() ->
    DDL = ?UTIL:get_standard_ddl(),
    {ok, Q} = ?UTIL:get_query(
                "SELECT weather FROM GeoCheckin "
                "WHERE time > 3000 AND time != 5000 "
                "AND user = 'user_1' AND location = 'derby'"),
    ?assertEqual(
       {error, {incomplete_where_clause, ?E_TSMSG_NO_UPPER_BOUND}},
       riak_kv_qry_compiler:compile(DDL, Q, 5)
      ).

start_key_not_a_range_test() ->
    DDL = ?UTIL:get_standard_ddl(),
    {ok, Q} = ?UTIL:get_query(
                "SELECT weather FROM GeoCheckin "
                "WHERE time = 3000 AND time < 5000 "
                "AND user = 'user_1' AND location = 'derby'"),
    ?assertEqual(
       {error, {incomplete_where_clause, ?E_TSMSG_NO_LOWER_BOUND}},
       riak_kv_qry_compiler:compile(DDL, Q, 5)
      ).

key_is_all_timestamps_test() ->
    DDL = ?UTIL:get_ddl(
            "CREATE TABLE GeoCheckin ("
            "time_a TIMESTAMP NOT NULL, "
            "time_b TIMESTAMP NOT NULL, "
            "time_c TIMESTAMP NOT NULL, "
            "PRIMARY KEY("
            " (time_a, time_b, QUANTUM(time_c, 15, 's')), time_a, time_b, time_c))"),
    {ok, Q} = ?UTIL:get_query(
                "SELECT time_a FROM GeoCheckin "
                "WHERE time_c > 2999 AND time_c < 5000 "
                "AND time_a = 10 AND time_b = 15"),
    ?assertMatch(
       {ok, [?SQL_SELECT{
                'WHERE' = [
                           {startkey, [
                                       {<<"time_a">>, timestamp, 10},
                                       {<<"time_b">>, timestamp, 15},
                                       {<<"time_c">>, timestamp, 2999}
                                      ]},
                           {endkey, [
                                     {<<"time_a">>, timestamp, 10},
                                     {<<"time_b">>, timestamp, 15},
                                     {<<"time_c">>, timestamp, 5000}
                                    ]},
                           {filter, []},
                           {start_inclusive, false}]
               }]},
       riak_kv_qry_compiler:compile(DDL, Q, 5)
      ).

duplicate_lower_bound_filter_not_allowed_test() ->
    DDL = ?UTIL:get_standard_ddl(),
    {ok, Q} = ?UTIL:get_query(
                "SELECT weather FROM GeoCheckin "
                "WHERE time > 3000 AND  time > 3001 AND time < 5000 "
                "AND user = 'user_1' AND location = 'derby'"),
    ?assertEqual(
       {error, {lower_bound_specified_more_than_once, ?E_TSMSG_DUPLICATE_LOWER_BOUND}},
       riak_kv_qry_compiler:compile(DDL, Q, 5)
      ).

duplicate_upper_bound_filter_not_allowed_test() ->
    DDL = ?UTIL:get_standard_ddl(),
    {ok, Q} = ?UTIL:get_query(
                "SELECT weather FROM GeoCheckin "
                "WHERE time > 3000 AND time < 5000 AND time < 4999 "
                "AND user = 'user_1' AND location = 'derby'"),
    ?assertEqual(
       {error, {upper_bound_specified_more_than_once, ?E_TSMSG_DUPLICATE_UPPER_BOUND}},
       riak_kv_qry_compiler:compile(DDL, Q, 5)
      ).

lower_bound_is_bigger_than_upper_bound_test() ->
    DDL = ?UTIL:get_standard_ddl(),
    {ok, Q} = ?UTIL:get_query(
                "SELECT weather FROM GeoCheckin "
                "WHERE time > 6000 AND time < 5000"
                "AND user = 'user_1' AND location = 'derby'"),
    ?assertEqual(
       {error, {lower_bound_must_be_less_than_upper_bound, ?E_TSMSG_LOWER_BOUND_MUST_BE_LESS_THAN_UPPER_BOUND}},
       riak_kv_qry_compiler:compile(DDL, Q, 5)
      ).

lower_bound_is_same_as_upper_bound_test() ->
    DDL = ?UTIL:get_standard_ddl(),
    {ok, Q} = ?UTIL:get_query(
                "SELECT weather FROM GeoCheckin "
                "WHERE time > 5000 AND time < 5000"
                "AND user = 'user_1' AND location = 'derby'"),
    ?assertEqual(
       {error, {lower_and_upper_bounds_are_equal_when_no_equals_operator, ?E_TSMSG_LOWER_AND_UPPER_BOUNDS_ARE_EQUAL_WHEN_NO_EQUALS_OPERATOR}},
       riak_kv_qry_compiler:compile(DDL, Q, 5)
      ).

query_has_no_AND_operator_1_test() ->
    DDL = ?UTIL:get_standard_ddl(),
    {ok, Q} = ?UTIL:get_query("select * from test1 where time < 5"),
    ?assertEqual(
       {error, {incomplete_where_clause, ?E_TSMSG_NO_LOWER_BOUND}},
       riak_kv_qry_compiler:compile(DDL, Q, 5)
      ).

query_has_no_AND_operator_2_test() ->
    DDL = ?UTIL:get_standard_ddl(),
    {ok, Q} = ?UTIL:get_query("select * from test1 where time > 1 OR time < 5"),
    ?assertEqual(
       {error, {time_bounds_must_use_and_op, ?E_TIME_BOUNDS_MUST_USE_AND}},
       riak_kv_qry_compiler:compile(DDL, Q, 5)
      ).

query_has_no_AND_operator_3_test() ->
    DDL = ?UTIL:get_standard_ddl(),
    {ok, Q} = ?UTIL:get_query("select * from test1 where user = 'user_1' AND time > 1 OR time < 5"),
    ?assertEqual(
       {error, {time_bounds_must_use_and_op, ?E_TIME_BOUNDS_MUST_USE_AND}},
       riak_kv_qry_compiler:compile(DDL, Q, 5)
      ).

query_has_no_AND_operator_4_test() ->
    DDL = ?UTIL:get_standard_ddl(),
    {ok, Q} = ?UTIL:get_query("select * from test1 where user = 'user_1' OR time > 1 OR time < 5"),
    ?assertEqual(
       {error, {time_bounds_must_use_and_op, ?E_TIME_BOUNDS_MUST_USE_AND}},
       riak_kv_qry_compiler:compile(DDL, Q, 5)
      ).

missing_key_field_in_where_clause_test() ->
    DDL = ?UTIL:get_standard_ddl(),
    {ok, Q} = ?UTIL:get_query("select * from test1 where time > 1 and time < 6 and user = '2'"),
    ?assertEqual(
       {error, {missing_key_clause, ?E_KEY_FIELD_NOT_IN_WHERE_CLAUSE("location")}},
       riak_kv_qry_compiler:compile(DDL, Q, 5)
      ).

not_equals_can_only_be_a_filter_test() ->
    DDL = ?UTIL:get_standard_ddl(),
    {ok, Q} = ?UTIL:get_query("select * from test1 where time > 1 and time < 6 and user = '2' and location != '4'"),
    ?assertEqual(
       {error, {missing_key_clause, ?E_KEY_PARAM_MUST_USE_EQUALS_OPERATOR("location", '!=')}},
       riak_kv_qry_compiler:compile(DDL, Q, 5)
      ).

no_where_clause_test() ->
    DDL = ?UTIL:get_standard_ddl(),
    {ok, Q} = ?UTIL:get_query("select * from test1"),
    ?assertEqual(
       {error, {no_where_clause, ?E_NO_WHERE_CLAUSE}},
       riak_kv_qry_compiler:compile(DDL, Q, 5)
      ).

%% Columns are: [geohash, location, user, time, weather, temperature]

-define(ROW, [<<"geodude">>, <<"derby">>, <<"ralph">>, 10, <<"hot">>, 12.2]).

%% this helper function is only for tests testing queries with the
%% query_result_type of 'rows' and _not_ 'aggregate'
testing_compile_row_select(DDL, QueryString) ->
    {ok, [?SQL_SELECT{ 'SELECT' = SelectSpec } | _]} =
        riak_kv_qry_compiler:compile(DDL, element(2, ?UTIL:get_query(QueryString)), 5),
    SelectSpec.

run_select_all_test() ->
    DDL = ?UTIL:get_standard_ddl(),
    Sel = testing_compile_row_select(DDL,
                                     "SELECT * FROM GeoCheckin "
                                     "WHERE time > 1 AND time < 6 AND user = '2' AND location = '4'"),
    #riak_sel_clause_v1{clause = SelectSpec} = Sel,
    ?assertEqual(
       ?ROW,
       riak_kv_qry_compiler:run_select_TEST(SelectSpec, ?ROW)
      ).

run_select_first_test() ->
    DDL = ?UTIL:get_standard_ddl(),
    Sel = testing_compile_row_select(DDL,
                                     "SELECT geohash FROM GeoCheckin "
                                     "WHERE time > 1 AND time < 6 AND user = '2' AND location = '4'"),
    #riak_sel_clause_v1{clause = SelectSpec} = Sel,
    ?assertEqual(
       [<<"geodude">>],
       riak_kv_qry_compiler:run_select_TEST(SelectSpec, ?ROW)
      ).

run_select_last_test() ->
    DDL = ?UTIL:get_standard_ddl(),
    Sel = testing_compile_row_select(DDL,
                                     "SELECT temperature FROM GeoCheckin "
                                     "WHERE time > 1 AND time < 6 AND user = '2' AND location = '4'"),
    #riak_sel_clause_v1{clause = SelectSpec} = Sel,
    ?assertEqual(
       [12.2],
       riak_kv_qry_compiler:run_select_TEST(SelectSpec, ?ROW)
      ).

run_select_all_individually_test() ->
    DDL = ?UTIL:get_standard_ddl(),
    Sel = testing_compile_row_select(DDL,
                                     "SELECT geohash, location, user, time, weather, temperature FROM GeoCheckin "
                                     "WHERE time > 1 AND time < 6 AND user = '2' AND location = '4'"),
    #riak_sel_clause_v1{clause = SelectSpec} = Sel,
    ?assertEqual(
       ?ROW,
       riak_kv_qry_compiler:run_select_TEST(SelectSpec, ?ROW)
      ).

run_select_some_test() ->
    DDL = ?UTIL:get_standard_ddl(),
    Sel = testing_compile_row_select(DDL,
                                     "SELECT  location, weather FROM GeoCheckin "
                                     "WHERE time > 1 AND time < 6 AND user = '2' AND location = '4'"),
    #riak_sel_clause_v1{clause = SelectSpec} = Sel,
    ?assertEqual(
       [<<"derby">>, <<"hot">>],
       riak_kv_qry_compiler:run_select_TEST(SelectSpec, ?ROW)
      ).

select_count_aggregation_test() ->
    DDL = ?UTIL:get_standard_ddl(),
    Sel = testing_compile_row_select(DDL,
                                     "SELECT count(location) FROM GeoCheckin "
                                     "WHERE time > 1 AND time < 6 AND user = '2' AND location = '4'"),
    #riak_sel_clause_v1{clause = SelectSpec} = Sel,
    ?assertEqual(
       [1],
       riak_kv_qry_compiler:run_select_TEST(SelectSpec, ?ROW, [0])
      ).


select_count_aggregation_2_test() ->
    DDL = ?UTIL:get_standard_ddl(),
    Sel = testing_compile_row_select(DDL,
                                     "SELECT count(location), count(location) FROM GeoCheckin "
                                     "WHERE time > 1 AND time < 6 AND user = '2' AND location = '4'"),
    #riak_sel_clause_v1{clause = SelectSpec} = Sel,
    ?assertEqual(
       [1, 10],
       riak_kv_qry_compiler:run_select_TEST(SelectSpec, ?ROW, [0, 9])
      ).

%% FIXME or operators

%% literal_on_left_hand_side_test() ->
%%     DDL = ?UTIL:get_standard_ddl(),
%%     {ok, Q} = ?UTIL:get_query("select * from testtwo where time > 1 and time < 6 and user = '2' and location = '4'"),
%%     ?assertMatch(
%%         [?SQL_SELECT{} | _],
%%         riak_kv_qry_compiler:compile(DDL, Q)
%%     ).

%% FIXME RTS-634
%% or_on_local_key_not_allowed_test() ->
%%     DDL = ?UTIL:get_standard_ddl(),
%%     {ok, Q} = ?UTIL:get_query(
%%         "SELECT weather FROM GeoCheckin "
%%         "WHERE time > 3000 AND time < 5000 "
%%         "AND user = 'user_1' "
%%         "AND location = 'derby' OR location = 'rottingham'"),
%%     ?assertEqual(
%%         {error, {upper_bound_specified_more_than_once, ?E_TSMSG_DUPLICATE_UPPER_BOUND}},
%%         riak_kv_qry_compiler:compile(DDL, Q)
%%     ).

%% TODO support filters on the primary key, this is not currently supported
%% filter_on_quanta_field_test() ->
%%     DDL = ?UTIL:get_standard_ddl(),
%%     {ok, Q} = ?UTIL:get_query(
%%         "SELECT weather FROM GeoCheckin "
%%         "WHERE time > 3000 AND time < 5000 "
%%         "AND time = 3002 AND user = 'user_1' AND location = 'derby'"),
%%     ?assertMatch(
%%         [?SQL_SELECT{
%%             'WHERE' = [
%%                 {startkey, [
%%                     {<<"time_a">>, timestamp, 10},
%%                     {<<"time_b">>, timestamp, 15},
%%                     {<<"time_c">>, timestamp, 2999}
%%                 ]},
%%                 {endkey, [
%%                     {<<"time_a">>, timestamp, 10},
%%                     {<<"time_b">>, timestamp, 15},
%%                     {<<"time_c">>, timestamp, 5000}
%%                 ]},
%%                 {filter, []},
%%                 {start_inclusive, false}]
%%         }],
%%         riak_kv_qry_compiler:compile(DDL, Q)
%%     ).

%%
%% Select Clause Compilation
%%

get_sel_ddl() ->
    ?UTIL:get_ddl(
      "CREATE TABLE GeoCheckin "
      "(location varchar not null, "
      "user varchar not null, "
      "time timestamp not null, "
      "mysint sint64 not null, "
      "mydouble double, "
      "myboolean boolean, "
      "PRIMARY KEY((location, user, quantum(time, 15, 's')), "
      "location, user, time))").

basic_select_test() ->
    DDL = get_sel_ddl(),
    SQL = "SELECT location from mytab WHERE myfamily = 'familyX' and myseries = 'seriesX' and time > 1 and time < 2",
    {ok, Rec} = ?UTIL:get_query(SQL),
    {ok, Sel} = riak_kv_qry_compiler:compile_select_clause_TEST(DDL, Rec),
    ?assertMatch(#riak_sel_clause_v1{calc_type        = rows,
                                     col_return_types = [
                                                         varchar
                                                        ],
                                     col_names        = [
                                                         <<"location">>
                                                        ]
                                    },
                 Sel).

basic_select_wildcard_test() ->
    DDL = get_sel_ddl(),
    SQL = "SELECT * from mytab WHERE myfamily = 'familyX' and myseries = 'seriesX' and time > 1 and time < 2",
    {ok, Rec} = ?UTIL:get_query(SQL),
    {ok, Sel} = riak_kv_qry_compiler:compile_select_clause_TEST(DDL, Rec),
    ?assertMatch(#riak_sel_clause_v1{calc_type        = rows,
                                     col_return_types = [
                                                         varchar,
                                                         varchar,
                                                         timestamp,
                                                         sint64,
                                                         double,
                                                         boolean
                                                        ],
                                     col_names        = [
                                                         <<"location">>,
                                                         <<"user">>,
                                                         <<"time">>,
                                                         <<"mysint">>,
                                                         <<"mydouble">>,
                                                         <<"myboolean">>
                                                        ]
                                    },
                 Sel).

select_all_and_column_test() ->
    {ok, Rec} = ?UTIL:get_query(
                  "SELECT *, location from mytab WHERE myfamily = 'familyX' "
                  "AND myseries = 'seriesX' AND time > 1 AND time < 2"),
    {ok, Selection} = riak_kv_qry_compiler:compile_select_clause_TEST(get_sel_ddl(), Rec),
    ?assertMatch(
       #riak_sel_clause_v1{
          calc_type = rows,
          col_return_types = [varchar, varchar, timestamp, sint64, double,
                              boolean, varchar],
          col_names = [<<"location">>, <<"user">>, <<"time">>,
                       <<"mysint">>, <<"mydouble">>, <<"myboolean">>,
                       <<"location">>]
         },
       Selection
      ).

select_column_and_all_test() ->
    {ok, Rec} = ?UTIL:get_query(
                  "SELECT location, * from mytab WHERE myfamily = 'familyX' "
                  "AND myseries = 'seriesX' AND time > 1 AND time < 2"),
    {ok, Selection} = riak_kv_qry_compiler:compile_select_clause_TEST(get_sel_ddl(), Rec),
    ?assertMatch(
       #riak_sel_clause_v1{
          calc_type = rows,
          col_return_types = [varchar, varchar, varchar, timestamp, sint64, double,
                              boolean],
          col_names = [<<"location">>, <<"location">>, <<"user">>, <<"time">>,
                       <<"mysint">>, <<"mydouble">>, <<"myboolean">>]
         },
       Selection
      ).

basic_select_window_agg_fn_test() ->
    SQL = "SELECT count(location), avg(mydouble), avg(mysint) from mytab WHERE myfamily = 'familyX' and myseries = 'seriesX' and time > 1 and time < 2",
    {ok, Rec} = ?UTIL:get_query(SQL),
    {ok, Sel} = riak_kv_qry_compiler:compile_select_clause_TEST(get_sel_ddl(), Rec),
    ?assertMatch(#riak_sel_clause_v1{calc_type        = aggregate,
                                     col_return_types = [
                                                         sint64,
                                                         double,
                                                         double
                                                        ],
                                     col_names        = [
                                                         <<"COUNT(location)">>,
                                                         <<"AVG(mydouble)">>,
                                                         <<"AVG(mysint)">>
                                                        ]
                                    },
                 Sel).

basic_select_arith_1_test() ->
    SQL = "SELECT 1 + 2 - 3 /4 * 5 from mytab WHERE myfamily = 'familyX' and myseries = 'seriesX' and time > 1 and time < 2",
    {ok, Rec} = ?UTIL:get_query(SQL),
    {ok, Sel} = riak_kv_qry_compiler:compile_select_clause_TEST(get_sel_ddl(), Rec),
    ?assertMatch(
       #riak_sel_clause_v1{
          calc_type        = rows,
          col_return_types = [sint64],
          col_names        = [<<"((1+2)-((3/4)*5))">>] },
       Sel
      ).

varchar_literal_test() ->
    {ok, Rec} = ?UTIL:get_query("SELECT 'hello' from mytab"),
    {ok, Sel} = riak_kv_qry_compiler:compile_select_clause_TEST(get_sel_ddl(), Rec),
    ?assertMatch(
       #riak_sel_clause_v1{
          calc_type        = rows,
          col_return_types = [varchar],
          col_names        = [<<"'hello'">>] },
       Sel
      ).

boolean_true_literal_test() ->
    {ok, Rec} = ?UTIL:get_query("SELECT true from mytab"),
    {ok, Sel} = riak_kv_qry_compiler:compile_select_clause_TEST(get_sel_ddl(), Rec),
    ?assertMatch(
       #riak_sel_clause_v1{
          calc_type        = rows,
          col_return_types = [boolean],
          col_names        = [<<"true">>] },
       Sel
      ).

boolean_false_literal_test() ->
    {ok, Rec} = ?UTIL:get_query("SELECT false from mytab"),
    {ok, Sel} = riak_kv_qry_compiler:compile_select_clause_TEST(get_sel_ddl(), Rec),
    ?assertMatch(
       #riak_sel_clause_v1{
          calc_type        = rows,
          col_return_types = [boolean],
          col_names        = [<<"false">>] },
       Sel
      ).

basic_select_arith_2_test() ->
    SQL = "SELECT 1 + 2.0 - 3 /4 * 5 from mytab WHERE myfamily = 'familyX' and myseries = 'seriesX' and time > 1 and time < 2",
    {ok, Rec} = ?UTIL:get_query(SQL),
    {ok, Sel} = riak_kv_qry_compiler:compile_select_clause_TEST(get_sel_ddl(), Rec),
    ?assertMatch(
       #riak_sel_clause_v1{
          calc_type = rows,
          col_return_types = [double],
          col_names = [<<"((1+2.0)-((3/4)*5))">>] },
       Sel
      ).

rows_initial_state_test() ->
    {ok, Rec} = ?UTIL:get_query(
                  "SELECT * FROM mytab WHERE myfamily = 'familyX' "
                  "AND myseries = 'seriesX' AND time > 1 AND time < 2"),
    {ok, Select} = riak_kv_qry_compiler:compile_select_clause_TEST(get_sel_ddl(), Rec),
    ?assertMatch(
       #riak_sel_clause_v1{ initial_state = [] },
       Select
      ).

function_1_initial_state_test() ->
    {ok, Rec} = ?UTIL:get_query(
                  "SELECT SUM(mydouble) FROM mytab WHERE myfamily = 'familyX' "
                  "AND myseries = 'seriesX' AND time > 1 AND time < 2"),
    {ok, Select} = riak_kv_qry_compiler:compile_select_clause_TEST(get_sel_ddl(), Rec),
    ?assertMatch(
       #riak_sel_clause_v1{ initial_state = [[]] },
       Select
      ).

function_2_initial_state_test() ->
    {ok, Rec} = ?UTIL:get_query(
                  "SELECT SUM(mydouble), SUM(mydouble) FROM mytab WHERE myfamily = 'familyX' "
                  "AND myseries = 'seriesX' AND time > 1 AND time < 2"),
    {ok, Select} = riak_kv_qry_compiler:compile_select_clause_TEST(get_sel_ddl(), Rec),
    ?assertMatch(
       #riak_sel_clause_v1{ initial_state = [[], []] },
       Select
      ).

select_negation_test() ->
    DDL = get_sel_ddl(),
    SQL = "SELECT -1, - 1, -1.0, - 1.0, -mydouble, - mydouble, -(1), -(1.0) from mytab "
        "WHERE myfamily = 'familyX' AND myseries = 'seriesX' "
        "AND time > 1 AND time < 2",
    {ok, Rec} = ?UTIL:get_query(SQL),
    {ok, Sel} = riak_kv_qry_compiler:compile_select_clause_TEST(DDL, Rec),
    ?assertMatch(#riak_sel_clause_v1{calc_type        = rows,
                                     col_return_types = [
                                                         sint64,
                                                         sint64,
                                                         double,
                                                         double,
                                                         double,
                                                         double,
                                                         sint64,
                                                         double
                                                        ],
                                     col_names        = [
                                                         <<"-1">>,
                                                         <<"-1">>,
                                                         <<"-1.0">>,
                                                         <<"-1.0">>,
                                                         <<"-mydouble">>,
                                                         <<"-mydouble">>,
                                                         <<"-1">>,
                                                         <<"-1.0">>
                                                        ]
                                    },
                 Sel).

sum_sum_finalise_test() ->
    {ok, Rec} = ?UTIL:get_query(
        "SELECT mydouble, SUM(mydouble), SUM(mydouble) FROM mytab"),
    {ok, Select} = riak_kv_qry_compiler:compile_select_clause_TEST(get_sel_ddl(), Rec),
    ?assertEqual(
        [1.0,3,7],
        riak_kv_qry_compiler:finalise_aggregate(Select, [1.0, 3, 7])
      ).

extract_stateful_function_1_test() ->
    {ok, #riak_select_v1{ 'SELECT' = #riak_sel_clause_v1{ clause = [Select] } }} =
        ?UTIL:get_query(
        "SELECT COUNT(col1) + COUNT(col2) FROM mytab "
        "WHERE myfamily = 'familyX' "
        "AND myseries = 'seriesX' AND time > 1 AND time < 2"),
    CountFn1 = {{window_agg_fn, 'COUNT'}, [{identifier, [<<"col1">>]}]},
    CountFn2 = {{window_agg_fn, 'COUNT'}, [{identifier, [<<"col2">>]}]},
    ?assertEqual(
        {{'+', {finalise_aggregation, 'COUNT', 1}, {finalise_aggregation, 'COUNT', 2}}, [CountFn1,CountFn2]},
        riak_kv_qry_compiler:extract_stateful_functions_TEST(Select, 0)
    ).

count_plus_count_test() ->
    {ok, Rec} = ?UTIL:get_query(
        "SELECT COUNT(mydouble) + COUNT(mydouble) FROM mytab "
        "WHERE myfamily = 'familyX' "
        "AND myseries = 'seriesX' AND time > 1 AND time < 2"),
    {ok, Select} = riak_kv_qry_compiler:compile_select_clause_TEST(get_sel_ddl(), Rec),
    ?assertMatch(
        #riak_sel_clause_v1{
            initial_state = [0,0],
            finalisers = [_, skip] },
        Select
      ).

count_plus_count_finalise_test() ->
    {ok, Rec} = ?UTIL:get_query(
        "SELECT COUNT(mydouble) + COUNT(mydouble) FROM mytab"),
    {ok, Select} = riak_kv_qry_compiler:compile_select_clause_TEST(get_sel_ddl(), Rec),
    ?assertMatch(
        [6],
        riak_kv_qry_compiler:finalise_aggregate(Select, [3,3])
      ).

count_multiplied_by_count_finalise_test() ->
    {ok, Rec} = ?UTIL:get_query(
        "SELECT COUNT(mydouble) * COUNT(mydouble) FROM mytab"),
    {ok, Select} = riak_kv_qry_compiler:compile_select_clause_TEST(get_sel_ddl(), Rec),
    ?assertMatch(
        [9],
        riak_kv_qry_compiler:finalise_aggregate(Select, [3,3])
      ).

count_plus_seven_finalise_test() ->
    {ok, Rec} = ?UTIL:get_query(
        "SELECT COUNT(mydouble) + 7 FROM mytab"),
    {ok, Select} = riak_kv_qry_compiler:compile_select_clause_TEST(get_sel_ddl(), Rec),
    ?assertMatch(
        [10],
        riak_kv_qry_compiler:finalise_aggregate(Select, [3])
      ).

count_plus_seven_sum__test() ->
    {ok, Rec} = ?UTIL:get_query(
        "SELECT COUNT(mydouble) + 7, SUM(mydouble) FROM mytab"),
    {ok, Select} = riak_kv_qry_compiler:compile_select_clause_TEST(get_sel_ddl(), Rec),
    ?assertMatch(
        #riak_sel_clause_v1{
            initial_state = [0,[]],
            finalisers = [_, _] },
        Select
      ).

count_plus_seven_sum_finalise_1_test() ->
    {ok, Rec} = ?UTIL:get_query(
        "SELECT COUNT(mydouble) + 7, SUM(mydouble) FROM mytab"),
    {ok, Select} = riak_kv_qry_compiler:compile_select_clause_TEST(get_sel_ddl(), Rec),
    ?assertMatch(
        [10, 11.0],
        riak_kv_qry_compiler:finalise_aggregate(Select, [3, 11.0])
      ).

count_plus_seven_sum_finalise_2_test() ->
    {ok, Rec} = ?UTIL:get_query(
        "SELECT COUNT(mydouble+1) + 1 FROM mytab"),
    {ok, Select} = riak_kv_qry_compiler:compile_select_clause_TEST(get_sel_ddl(), Rec),
    ?assertEqual(
        [2],
        riak_kv_qry_compiler:finalise_aggregate(Select, [1])
      ).

avg_finalise_test() ->
    {ok, Rec} = ?UTIL:get_query(
        "SELECT AVG(mydouble) FROM mytab"),
    {ok, #riak_sel_clause_v1{ clause = [AvgFn] } = Select} =
        riak_kv_qry_compiler:compile_select_clause_TEST(get_sel_ddl(), Rec),
    InitialState = riak_ql_window_agg_fns:start_state('AVG'),
    Rows = [[x,x,x,x,N,x] || N <- lists:seq(1, 5)],
    AverageResult = lists:foldl(AvgFn, InitialState, Rows),
    ?assertEqual(
        [lists:sum(lists:seq(1, 5)) / 5],
        riak_kv_qry_compiler:finalise_aggregate(Select, [AverageResult])
    ).

finalise_aggregate_test() ->
    ?assertEqual(
        [1,2,3],
        riak_kv_qry_compiler:finalise_aggregate(
            #riak_sel_clause_v1 {
                calc_type = aggregate,
                finalisers = lists:duplicate(3, fun(_,S) -> S end) },
            [1,2,3]
        )
    ).

infer_col_type_TEST_1_test() ->
    ?assertEqual(
        {sint64, []},
        riak_kv_qry_compiler:infer_col_type_TEST(get_sel_ddl(), {integer, 5}, [])
    ).

infer_col_type_2_test() ->
    ?assertEqual(
        {sint64, []},
        riak_kv_qry_compiler:infer_col_type_TEST(get_sel_ddl(), {{window_agg_fn, 'SUM'}, [{integer, 4}]}, [])
    ).

compile_query_with_function_type_error_1_test() ->
    {ok, Q} = ?UTIL:get_query(
          "SELECT SUM(location) FROM GeoCheckin "
          "WHERE time > 5000 AND time < 10000"
          "AND user = 'user_1' AND location = 'derby'"),
    ?assertEqual(
        {error,{invalid_query,<<"\nFunction 'SUM' called with arguments of the wrong type [varchar].">>}},
        riak_kv_qry_compiler:compile(?UTIL:get_standard_ddl(), Q, 100)
    ).

compile_query_with_function_type_error_2_test() ->
    {ok, Q} = ?UTIL:get_query(
          "SELECT SUM(location), AVG(location) FROM GeoCheckin "
          "WHERE time > 5000 AND time < 10000"
          "AND user = 'user_1' AND location = 'derby'"),
    ?assertEqual(
        {error,{invalid_query,<<"\nFunction 'SUM' called with arguments of the wrong type [varchar].\n"
                                "Function 'AVG' called with arguments of the wrong type [varchar].">>}},
        riak_kv_qry_compiler:compile(?UTIL:get_standard_ddl(), Q, 100)
    ).

compile_query_with_function_type_error_3_test() ->
    {ok, Q} = ?UTIL:get_query(
          "SELECT AVG(location + 1) FROM GeoCheckin "
          "WHERE time > 5000 AND time < 10000"
          "AND user = 'user_1' AND location = 'derby'"),
    ?assertEqual(
        {error,{invalid_query,<<"\nOperator '+' called with mismatched types [varchar vs sint64].">>}},
        riak_kv_qry_compiler:compile(?UTIL:get_standard_ddl(), Q, 100)
    ).

compile_query_with_arithmetic_type_error_1_test() ->
    {ok, Q} = ?UTIL:get_query(
          "SELECT location + 1 FROM GeoCheckin "
          "WHERE time > 5000 AND time < 10000"
          "AND user = 'user_1' AND location = 'derby'"),
    ?assertEqual(
        {error,{invalid_query,<<"\nOperator '+' called with mismatched types [varchar vs sint64].">>}},
        riak_kv_qry_compiler:compile(?UTIL:get_standard_ddl(), Q, 100)
    ).

compile_query_with_arithmetic_type_error_2_test() ->
    {ok, Q} = ?UTIL:get_query(
          "SELECT 2*(location + 1) FROM GeoCheckin "
          "WHERE time > 5000 AND time < 10000"
          "AND user = 'user_1' AND location = 'derby'"),
    ?assertEqual(
        {error,{invalid_query,<<"\nOperator '+' called with mismatched types [varchar vs sint64].">>}},
        riak_kv_qry_compiler:compile(?UTIL:get_standard_ddl(), Q, 100)
    ).

flexible_keys_1_test() ->
    DDL = ?UTIL:get_ddl(
        "CREATE TABLE tab4("
        "a1 SINT64 NOT NULL, "
        "a TIMESTAMP NOT NULL, "
        "b VARCHAR NOT NULL, "
        "c VARCHAR NOT NULL, "
        "d SINT64 NOT NULL, "
        "PRIMARY KEY  ((a1, quantum(a, 15, 's')), a1, a, b, c, d))"),
    {ok, Q} = ?UTIL:get_query(
          "SELECT * FROM tab4 WHERE a > 0 AND a < 1000 AND a1 = 1"),
    {ok, [Select]} = riak_kv_qry_compiler:compile(DDL, Q, 100),
    ?assertEqual(
        [{startkey,[{<<"a1">>,sint64,1}, {<<"a">>,timestamp,0}]},
          {endkey, [{<<"a1">>,sint64,1}, {<<"a">>,timestamp,1000}]},
          {filter,[]},
          {start_inclusive,false}],
        Select#riak_select_v1.'WHERE'
    ).

%% two element key with quantum
flexible_keys_2_test() ->
    DDL = ?UTIL:get_ddl(
        "CREATE TABLE tab4("
        "a TIMESTAMP NOT NULL, "
        "PRIMARY KEY  ((quantum(a, 15, 's')), a))"),
    {ok, Q} = ?UTIL:get_query(
          "SELECT * FROM tab4 WHERE a > 0 AND a < 1000"),
    ?assertMatch(
        {ok, [#riak_select_v1{}]},
        riak_kv_qry_compiler:compile(DDL, Q, 100)
    ).

quantum_field_name_test() ->
    DDL = ?UTIL:get_ddl(
        "CREATE TABLE tab1("
        "a SINT64 NOT NULL, "
        "b TIMESTAMP NOT NULL, "
        "PRIMARY KEY  ((a,quantum(b, 15, 's')), a,b))"),
    ?assertEqual(
        <<"b">>,
        riak_kv_qry_compiler:quantum_field_name_TEST(DDL)
    ).

quantum_field_name_no_quanta_test() ->
    DDL = ?UTIL:get_ddl(
        "CREATE TABLE tab1("
        "a SINT64 NOT NULL, "
        "b TIMESTAMP NOT NULL, "
        "PRIMARY KEY  ((a,b), a,b))"),
    ?assertEqual(
        no_quanta,
        riak_kv_qry_compiler:quantum_field_name_TEST(DDL)
    ).

%% short key, partition and local keys are the same
no_quantum_in_query_1_test() ->
    DDL = ?UTIL:get_ddl(
        "CREATE TABLE tabab("
        "a TIMESTAMP NOT NULL, "
        "b VARCHAR NOT NULL, "
        "PRIMARY KEY  ((a,b), a,b))"),
    {ok, Q} = ?UTIL:get_query(
          "SELECT * FROM tab1 WHERE a = 1 AND b = 1"),
    ?assertMatch(
        {ok, [#riak_select_v1{ 
            'WHERE' = 
                [{startkey,[{<<"a">>,timestamp,1},{<<"b">>,varchar,1}]},
                 {endkey,  [{<<"a">>,timestamp,1},{<<"b">>,varchar,1}]},
                 {filter,[]},
                 {end_inclusive,true}] }]},
        riak_kv_qry_compiler:compile(DDL, Q, 100)
    ).

%% partition and local key are different
no_quantum_in_query_2_test() ->
    DDL = ?UTIL:get_ddl(
        "CREATE TABLE tabab("
        "a SINT64 NOT NULL, "
        "b VARCHAR NOT NULL, "
        "c DOUBLE NOT NULL, "
        "d BOOLEAN NOT NULL, "
        "PRIMARY KEY  ((c,a,b), c,a,b,d))"),
    {ok, Q} = ?UTIL:get_query(
          "SELECT * FROM tabab WHERE a = 1000 AND b = 'bval' AND c = 3.5"),
    {ok, [Select]} = riak_kv_qry_compiler:compile(DDL, Q, 100),
    Key = 
        [{<<"c">>,double,3.5}, {<<"a">>,sint64,1000},{<<"b">>,varchar,<<"bval">>}],
    ?assertEqual(
        [{startkey, Key},
         {endkey, Key},
         {filter,[]},
         {end_inclusive,true}],
        Select#riak_select_v1.'WHERE'
    ).

no_quantum_in_query_3_test() ->
    DDL = ?UTIL:get_ddl(
        "CREATE TABLE tababa("
        "a SINT64 NOT NULL, "
        "b VARCHAR NOT NULL, "
        "c DOUBLE NOT NULL, "
        "d BOOLEAN NOT NULL, "
        "PRIMARY KEY  ((c,a,b), c,a,b,d))"),
    {ok, Q} = ?UTIL:get_query(
          "SELECT * FROM tababa WHERE a = 1000 AND b = 'bval' AND c = 3.5 AND d = true"),
    {ok, [Select]} = riak_kv_qry_compiler:compile(DDL, Q, 100),
    Key =
        [{<<"c">>,double,3.5}, {<<"a">>,sint64,1000},{<<"b">>,varchar,<<"bval">>}],
    ?assertEqual(
        [{startkey, Key},
         {endkey, Key},
         {filter,{'=',{field,<<"d">>,boolean},{const, true}}},
         {end_inclusive,true}],
        Select#riak_select_v1.'WHERE'
    ).

%% one element key
no_quantum_in_query_4_test() ->
    DDL = ?UTIL:get_ddl(
        "CREATE TABLE tab1("
        "a TIMESTAMP NOT NULL, "
        "PRIMARY KEY  ((a), a))"),
    {ok, Q} = ?UTIL:get_query(
          "SELECT * FROM tab1 WHERE a = 1000"),
    {ok, [Select]} = riak_kv_qry_compiler:compile(DDL, Q, 100),
    ?assertEqual(
        [{startkey,[{<<"a">>,timestamp,1000}]},
          {endkey,[{<<"a">>,timestamp,1000}]},
          {filter,[]},
          {end_inclusive,true}],
        Select#riak_select_v1.'WHERE'
    ).

two_element_key_range_cannot_match_test() ->
    DDL = ?UTIL:get_ddl(
        "CREATE TABLE tabab("
        "a TIMESTAMP NOT NULL, "
        "b SINT64 NOT NULL, "
        "PRIMARY KEY  ((a,quantum(b, 15, 's')), a,b))"),
    {ok, Q} = ?UTIL:get_query(
          "SELECT * FROM tab1 WHERE a = 1 AND b > 1 AND b < 1"),
    ?assertMatch(
        {error, {lower_and_upper_bounds_are_equal_when_no_equals_operator, <<_/binary>>}},
        riak_kv_qry_compiler:compile(DDL, Q, 100)
    ).

quantum_is_not_last_element_test() ->
    DDL = ?UTIL:get_ddl(
        "CREATE TABLE tab1("
        "a SINT64 NOT NULL, "
        "b TIMESTAMP NOT NULL, "
        "c SINT64 NOT NULL, "
        "PRIMARY KEY  ((a,quantum(b,1,'s'),c), a,b,c))"),
    {ok, Q} = ?UTIL:get_query(
          "SELECT * FROM tab1 WHERE b >= 1000 AND b <= 3000 AND a = 10 AND c = 20"),
    {ok, SubQueries} = riak_kv_qry_compiler:compile(DDL, Q, 100),
    SubQueryWheres = [S#riak_select_v1.'WHERE' || S <- SubQueries],
    ?assertEqual(
        [
            [{startkey,[{<<"a">>,sint64,10},{<<"b">>,timestamp,1000},{<<"c">>,sint64,20}]},
             {endkey,  [{<<"a">>,sint64,10},{<<"b">>,timestamp,2000},{<<"c">>,sint64,20}]},
             {filter,[]}],
            [{startkey,[{<<"a">>,sint64,10},{<<"b">>,timestamp,2000},{<<"c">>,sint64,20}]},
             {endkey,  [{<<"a">>,sint64,10},{<<"b">>,timestamp,3000},{<<"c">>,sint64,20}]},
             {filter,[]}],
            [{startkey,[{<<"a">>,sint64,10},{<<"b">>,timestamp,3000},{<<"c">>,sint64,20}]},
             {endkey,  [{<<"a">>,sint64,10},{<<"b">>,timestamp,3000},{<<"c">>,sint64,20}]},
             {filter,[]},
             {end_inclusive,true}]
        ],
        SubQueryWheres
    ).

negate_an_aggregation_function_test() ->
    {ok, Rec} = ?UTIL:get_query(
        "SELECT -COUNT(*) FROM mytab"),
    {ok, Select} = riak_kv_qry_compiler:compile_select_clause_TEST(get_sel_ddl(), Rec),
    ?assertMatch(
        [-3],
        riak_kv_qry_compiler:finalise_aggregate(Select, [3])
      ).
