-module(sql_compilation_end_to_end).

-include_lib("eunit/include/eunit.hrl").

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
	       {ok, SQL} = make_sql(Query),
	       gg:format("DDL is ~p~n", [DDL]),
	       gg:format("SQL is ~p~n", [SQL]),
	       Got = riak_kv_qry_compiler:compile(DDL, SQL),
	       gg:format("Got:~n~p~nExpected:~n~p~n", [Got, Expected]),
	       ?assertEqual(Expected, Got)).

%% -define(failing_test(Name, TableCreate, Query), 
%% 	Name() ->
%% ).

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
%% Tests
%%

?passing_test(plain_qry_test,
	      "create table temperatures " ++
		  "(time timestamp not null, " ++
		  "user_id varchar not null, " ++
		  "primary key (time, user_id))",
	      "select * from temperatures",
	      yando).


    
