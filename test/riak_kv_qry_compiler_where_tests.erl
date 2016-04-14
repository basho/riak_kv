%% -------------------------------------------------------------------
%%
%% riak_kv_qry_compiler_where_tests: tests pertaining to the where
%% clause as it is being rewritten
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
-module(riak_kv_qry_compiler_where_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("riak_kv/src/riak_kv_ts_error_msgs.hrl").
-include_lib("riak_ql/include/riak_ql_ddl.hrl").
-include("riak_kv_ts.hrl").

-define(UTIL, riak_kv_qry_compiler_test_util).

%%
%% complete query passing tests
%%

simplest_test() ->
    DDL = ?UTIL:get_standard_ddl(),
    Query = "select weather from GeoCheckin where time > 3000 and time < 5000 and user = 'user_1' and location = 'San Francisco'",
    {ok, Q} = ?UTIL:get_query(Query),
    [Where1] =
        test_data_where_clause(<<"San Francisco">>, <<"user_1">>, [{3000, 5000}]),
    Where2 = Where1 ++ [{start_inclusive, false}],
    {ok, Got0} = riak_kv_qry_where_compiler:compile_where_clause(DDL, Q, 5),
    Got = ?UTIL:get_where_clauses(Got0),
    ?assertEqual([Where2], Got).

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

