%% -------------------------------------------------------------------
%%
%% sql_compilation_end_to_end: basic timeseries DDL validation test
%%
%% Copyright (c) 2015 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc A basic test of timeseries that writes a single element to the back end
%%      and checks it is correct

-module(sql_compilation_end_to_end).

-include_lib("eunit/include/eunit.hrl").
-include_lib("riak_ql/include/riak_ql_ddl.hrl").

-define(VALID,   true).
-define(INVALID, false).

-compile(export_all).

-include_lib("riak_ql/include/riak_ql_ddl.hrl").

%%
%% Testing Macros
%%

-define(assert_test(Name, TableCreate, Query, Expected),
        Name() ->
               {ok, DDL} = make_ddl(TableCreate),
               {module, Mod} = riak_ql_ddl_compiler:make_helper_mod(DDL),
               {ok, SQL} = make_sql(Query),
               case riak_ql_ddl:is_query_valid(Mod, DDL, SQL) of
                   true ->
                       Got = riak_kv_qry_compiler:compile(DDL, SQL),
                       ?assertEqual(Expected, Got);
                   false ->
                       ?assertEqual(Expected, invalid_query)
               end).

%%
%% Helper Funs
%%

make_ddl(CreateTable) ->
    Lexed = riak_ql_lexer:get_tokens(CreateTable),
    {ok, _DDL} = riak_ql_parser:parse(Lexed).

make_sql(Query) ->
    Lexed = riak_ql_lexer:get_tokens(Query),
    {ok, _SQL} = riak_ql_parser:parse(Lexed).

get_standard_pk() -> #key_v1{ast = [
                #param_v1{name = [<<"location">>]},
                #param_v1{name = [<<"user">>]},
                                #hash_fn_v1{mod = riak_ql_quanta,
                                            fn = quantum,
                                            args = [
                                                    #param_v1{name = [<<"time">>]},
                                                    15,
                                                    s
                                                   ],
                                            type = timestamp}
                               ]
                        }.

get_standard_lk() -> #key_v1{ast = [
                    #param_v1{name = [<<"location">>]},
                    #param_v1{name = [<<"user">>]},
                                    #param_v1{name = [<<"time">>]}
                                   ]}.

%%
%% Passing Tests
%%

?assert_test(plain_qry_test,
             "CREATE TABLE GeoCheckin ("
             " geohash varchar not null,"
             " location varchar not null,"
             " user varchar not null,"
             " time timestamp not null,"
             " weather varchar not null,"
             " temperature varchar,"
             " PRIMARY KEY ((location, user, quantum(time, 15, 's')),"
             " location, user, time))",
             "select weather from GeoCheckin where time > 3000"
             " and time < 5000 and user = 'gordon' and location = 'Lithgae'",
             [
              #riak_sql_v1{'SELECT'      = [[<<"weather">>]],
                           'FROM'        = <<"GeoCheckin">>,
                           'WHERE'       = [
                                            {startkey, [
                                                        {<<"location">>, varchar, <<"Lithgae">>},
                                                        {<<"user">>, varchar, <<"gordon">>},
                                                        {<<"time">>, timestamp, 3000}
                                                       ]
                                            },
                                            {endkey,   [
                                                        {<<"location">>, varchar, <<"Lithgae">>},
                                                        {<<"user">>, varchar, <<"gordon">>},
                                                        {<<"time">>, timestamp, 5000}
                                                       ]
                                            },
                                            {filter, []},
                                            {start_inclusive,   false}
                                           ],
                           helper_mod    = riak_ql_ddl:make_module_name(<<"GeoCheckin">>),
                           partition_key = get_standard_pk(),
                           is_executable = true,
                           type          = timeseries,
                           local_key     = get_standard_lk()}
             ]).

?assert_test(badarith_regression_test,
             "CREATE TABLE GeoCheckin ("
             " geohash varchar not null,"
             " user varchar not null,"
             " time timestamp not null,"
             " weather varchar not null,"
             " temperature varchar,"
             " PRIMARY KEY ((user, geohash, quantum(time, 15, s)), user, geohash, time))",
             "select weather from GeoCheckin where time > 3000 and time < 5000",
             {error, {missing_param, <<"Missing parameter user in where clause.">>}}).

?assert_test(spanning_qry_test,
             "CREATE TABLE GeoCheckin ("
             " geohash varchar not null, "
             " location varchar not null, "
             " user varchar not null, "
             " time timestamp not null, "
             " weather varchar not null, "
             " temperature varchar, "
             "PRIMARY KEY ((location, user, quantum(time, 15, 's')),"
             " location, user, time))",
             "select weather from GeoCheckin where time > 3000 and "
             " time < 18000 and user = 'gordon' and location = 'Lithgae'",

          [
           #riak_sql_v1{'SELECT'      = [[<<"weather">>]],
                        'FROM'        = <<"GeoCheckin">>,
                        'WHERE'       = [
                                         {startkey, [
                                                     {<<"location">>, varchar, <<"Lithgae">>},
                                                     {<<"user">>, varchar, <<"gordon">>},
                                                     {<<"time">>, timestamp, 3000}
                                                    ]
                                         },
                                         {endkey,   [
                                                     {<<"location">>, varchar, <<"Lithgae">>},
                                                     {<<"user">>, varchar, <<"gordon">>},
                                                     {<<"time">>, timestamp, 15000}
                                                    ]
                                         },
                                         {filter, []},
                                         {start_inclusive,   false}
                                        ],
                        helper_mod    = riak_ql_ddl:make_module_name(<<"GeoCheckin">>),
                        partition_key = get_standard_pk(),
                        is_executable = true,
                        type          = timeseries,
                        local_key     = get_standard_lk()},

           #riak_sql_v1{'SELECT'      = [[<<"weather">>]],
                        'FROM'        = <<"GeoCheckin">>,
                        'WHERE'       = [
                                         {startkey, [
                                                     {<<"location">>, varchar, <<"Lithgae">>},
                                                     {<<"user">>, varchar, <<"gordon">>},
                                                     {<<"time">>, timestamp, 15000}
                                                    ]
                                         },
                                         {endkey,   [
                                                     {<<"location">>, varchar, <<"Lithgae">>},
                                                     {<<"user">>, varchar, <<"gordon">>},
                                                     {<<"time">>, timestamp, 18000}
                                                    ]
                                         },
                                         {filter, []}
                                        ],
                        helper_mod    = riak_ql_ddl:make_module_name(<<"GeoCheckin">>),
                        partition_key = get_standard_pk(),
                        is_executable = true,
                        type          = timeseries,
                        local_key     = get_standard_lk()}
          ]).
