%%% -------------------------------------------------------------------
%%%
%%% riak_kv_ts_error_msgs
%%%
%%% Time Series messages that are returned as a response to requests
%%% that have experienced an error.
%%%
%%% Copyright (c) 2015 Basho Technologies, Inc.  All Rights Reserved.
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
    E_TSMSG_NO_UPPER_BOUND,
    <<"Where clause has no upper bound.">>
).

-define(
    E_TSMSG_NO_LOWER_BOUND,
    <<"Where clause has no lower bound.">>
).

-define(
    E_MISSING_PARAM_IN_WHERE_CLAUSE(ParamName),
    iolist_to_binary(["Missing parameter ", ParamName, " in where clause."])
).

-define(
    E_WHERE_CLAUSE_NOT_A_RANGE(ParamName, InvalidOperator),
    iolist_to_binary([
        "The where clause is not a complete time range e.g. ",
        "\n\n    ", ParamName, ", > 1000 and ", ParamName, " < 2000.\n\n",
        "Operator ", atom_to_list(InvalidOperator), " was used but only '>', '>=', '<' and '<=' are allowed."
    ])).

-define(
    E_WHERE_CLAUSE_NOT_A_RANGE(ParamNameA, InvalidOperatorA, ParamNameB, InvalidOperatorB),
    iolist_to_binary([
        ?E_WHERE_CLAUSE_NOT_A_RANGE(ParamNameA, InvalidOperatorA), "\n",
        ?E_WHERE_CLAUSE_NOT_A_RANGE(ParamNameB, InvalidOperatorB)
    ])).