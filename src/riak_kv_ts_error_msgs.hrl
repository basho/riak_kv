%%% -------------------------------------------------------------------
%%%
%%% riak_kv_ts_error_msgs
%%%
%%% Time Series messages that are returned as a response to requests
%%% that have experienced an error.
%%%
%%% Copyright (c) 2016 Basho Technologies, Inc.  All Rights Reserved.
%%%
%%% This file is provided to you under the Apache License,
%%% Version 2.0 (the "License"); you may not use this file
%%% except in compliance with the License.  You may obtain
%%% a copy of the License at
%%%
%%%   http://www.apache.org/licenses/LICENSE-2.0
%%%
%%% Unless required by applicable law or agreed to in writing,
%%% software distributed under the License is distributed on an
%%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%%% KIND, either express or implied.  See the License for the
%%% specific language governing permissions and limitations
%%% under the License.
%%%
%%% -------------------------------------------------------------------

-define(
    E_NO_WHERE_CLAUSE,
    <<"The query must have a where clause.">>
).

-define(
    E_TSMSG_NO_UPPER_BOUND,
    <<"Where clause has no upper bound.">>
).

-define(
    E_TSMSG_NO_LOWER_BOUND,
    <<"Where clause has no lower bound.">>
).

-define(
    E_TSMSG_NO_BOUNDS_SPECIFIED,
    <<"Neither upper or lower time bounds were specified in the query.">>
).

-define(
    E_TSMSG_DUPLICATE_LOWER_BOUND,
    <<"The lower time bound is specified more than once in the query.">>
).

-define(
    E_TSMSG_DUPLICATE_UPPER_BOUND,
    <<"The upper time bound is specified more than once in the query.">>
).

-define(
    E_MISSING_PARAM_IN_WHERE_CLAUSE(ParamName),
    iolist_to_binary(["Missing parameter ", ParamName, " in where clause."])
).

-define(
    E_TIME_BOUNDS_MUST_USE_AND,
    <<"The time bounds used OR but must use AND.">>
).

-define(
    E_TSMSG_LOWER_BOUND_MUST_BE_LESS_THAN_UPPER_BOUND,
    <<"The lower time bound is greater than the upper time bound.">>
).

-define(
    E_TSMSG_LOWER_AND_UPPER_BOUNDS_ARE_EQUAL_WHEN_NO_EQUALS_OPERATOR,
    <<"The upper and lower boundaries are equal or adjacent. No results are possible.">>
).

-define(
    E_KEY_FIELD_NOT_IN_WHERE_CLAUSE(ParamName),
    iolist_to_binary(
        ["The '", ParamName, "' parameter is part the primary key but not ",
         "specified in the where clause."])
).

-define(
    E_KEY_PARAM_MUST_USE_EQUALS_OPERATOR(ParamName, Op),
    iolist_to_binary(
        ["The '", ParamName, "' parameter is part the primary key, and must have an ",
         "equals clause in the query but the ", atom_to_list(Op), " operator was used."])
).

-define(
   E_TOO_MANY_SUBQUERIES(N),
   iolist_to_binary(
     ["Too many subqueries (",integer_to_list(N),")"])
).

-define(
   E_NON_UNIQUE_ORDERBY_FIELDS(N),
   iolist_to_binary(
     ["Column ",N," appears more than once in ORDER BY list"])
).

-define(
   E_ORDER_BY_WITH_AGGREGATE_CALC_TYPE,
   <<"ORDER BY/LIMIT/OFFSET clauses not supported for queries containing an aggregate function.">>).
