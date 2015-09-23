-module(sql_compilation_end_to_end).

-include_lib("eunit/include/eunit.hrl").
-include_lib("riak_ql/include/riak_ql_ddl.hrl").

%% this is a basic test of timeseries that writes a single element to the back end
%% and checks it is correct

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
               {module, _Mod} = riak_ql_ddl_compiler:make_helper_mod(DDL),
               {ok, SQL} = make_sql(Query),
               case riak_ql_ddl:is_query_valid(DDL, SQL) of
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
				    #param_v1{name = [<<"time">>]},
				    #param_v1{name = [<<"user">>]}
				   ]}.

%%
%% Passing Tests
%%

?assert_test(plain_qry_test,
              "CREATE TABLE GeoCheckin "
              ++ "(geohash varchar not null, "
              ++ "user varchar not null, "
              ++ "time timestamp not null, "
              ++ "weather varchar not null, "
              ++ "temperature varchar, "
              ++ "PRIMARY KEY ((quantum(time, 15, s)), time, user))",
              "select weather from GeoCheckin where time > 3000 and time < 5000 and user = gordon",
             [
              #riak_sql_v1{'SELECT'      = [[<<"weather">>]],
                           'FROM'        = <<"GeoCheckin">>,
                           'WHERE'       = [
                                            {startkey, [
                                                        {<<"time">>,  
							 timestamp, 
							 3000},
                                                        {<<"user">>, 
							 binary,
							 <<"gordon">>}
                                                       ]
                                            },
                                            {endkey,   [
                                                        {<<"time">>, 
							 timestamp, 
							 5000},
                                                        {<<"user">>, 
							 binary,
							 <<"gordon">>}
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

?assert_test(badarith_regression_test,
              "CREATE TABLE GeoCheckin "
              ++ "(geohash varchar not null, "
              ++ "user varchar not null, "
              ++ "time timestamp not null, "
              ++ "weather varchar not null, "
              ++ "temperature varchar, "
              ++ "PRIMARY KEY ((quantum(time, 15, s)), time, user))",
	     "select weather from GeoCheckin where time > 3000 and time < 5000",
	     [{error,
	       {invalid_where_clause,
		{and_, 
		 {'<', <<"time">>, {int, 5000}}, 
		 {'>', <<"time">>, {int, 3000}}
		}
		}
	      }]).

?assert_test(spanning_qry_test, 
	      "CREATE TABLE GeoCheckin " ++
		  "(geohash varchar not null, " ++ 
		  "user varchar not null, " ++
		  "time timestamp not null, " ++ 
		  "weather varchar not null, " ++ 
		  "temperature varchar, " ++ 
		  "PRIMARY KEY((quantum(time, 15, s)), time, user))", 
	      "select weather from GeoCheckin where time > 3000 and time < 18000 "
	      "and user = gordon",
	      [
	      #riak_sql_v1{'SELECT'      = [[<<"weather">>]],
			   'FROM'        = <<"GeoCheckin">>,
			   'WHERE'       = [
					    {startkey, [
                                                        {<<"time">>, 
							 timestamp,
							 3000},
                                                        {<<"user">>,
							 binary,
							 <<"gordon">>}
                                                       ]
                                            },
                                            {endkey,   [
                                                        {<<"time">>, 
							 timestamp,
							 15000},
                                                        {<<"user">>, 
							 binary,
							 <<"gordon">>}
                                                       ]
                                            },
                                            {filter, []}
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
                                                        {<<"time">>,  
							 timestamp,
							 15000},
                                                        {<<"user">>, 
							 binary,
							 <<"gordon">>}
                                                       ]
                                            },
                                            {endkey,   [
                                                        {<<"time">>, 
							 timestamp,
							 18000},
                                                        {<<"user">>,
							 binary,
							 <<"gordon">>}
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
