-module(sql_compilation_end_to_end).

-include_lib("eunit/include/eunit.hrl").
-include_lib("riak_ql/include/riak_ql_sql.hrl").

%% this is a basic test of timeseries that writes a single element to the back end
%% and checks it is correct

-define(VALID,   true).
-define(INVALID, false).

-compile(export_all).


%%
%% Testing Macros
%%

-define(passing_test(Name, TableCreate, Query, Expected), 
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
    
%%
%% Passing Tests
%%

?passing_test(plain_qry_test,
	      "CREATE TABLE GeoCheckin "
	      ++ "(geohash varchar not null, "
	      ++ "user varchar not null, "
	      ++ "time timestamp not null, "
	      ++ "weather varchar not null, "
	      ++ "temperature varchar, "
	      ++ "PRIMARY KEY ((quantum(time, 15, m)), time, user))",
	      "select weather from GeoCheckin where time > 3000 and time < 5000",
	     [
	      #riak_sql_v1{'SELECT'      = [["weather"]],
			   'FROM'        = <<"GeoCheckin">>,
			   'WHERE'       = [
					    {and_,
					     {'>', "time", {int, 3000}},
					     {'<', "time", {int, 5000}}}
					   ],
			   is_executable = true}
	      ]).

?passing_test(spanning_qry_test, 
	      "CREATE TABLE GeoCheckin " ++
		  "(geohash varchar not null, " ++ 
		  "user varchar not null, " ++
		  "time timestamp not null, " ++ 
		  "weather varchar not null, " ++ 
		  "temperature varchar, " ++ 
		  "PRIMARY KEY((quantum(time, 15, s)), time, user))", 
	      "select weather from GeoCheckin where time > 3000 and time < 18000",
	      [
	      #riak_sql_v1{'SELECT'      = [["weather"]],
			   'FROM'        = <<"GeoCheckin">>,
			   'WHERE'       = [
					    {and_,
					     {'>', "time", {int, 3000}},
					     {'<', "time", {int, 15000}}}
					   ],
			   is_executable = true},
	      #riak_sql_v1{'SELECT'      = [["weather"]],
			   'FROM'        = <<"GeoCheckin">>,
			   'WHERE'       = [
					    {and_,
					     {'>=', "time", {int, 15000}},
					     {'<',  "time", {int, 18000}}}
					   ],
			   is_executable = true}
	      ]).

