%% -------------------------------------------------------------------
%%
%% riak_kv_qry_compiler: generate the coverage for a hashed query
%%
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
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
-module(riak_kv_qry_compiler).

-export([
	 compile/2
	]).

-include_lib("riak_ql/include/riak_ql_ddl.hrl").
-include_lib("riak_ql/include/riak_ql_sql.hrl").
-include("riak_kv_index.hrl").

compile(#ddl_v1{}, #riak_sql_v1{is_executable = true}) ->
    {error, 'query is already compiled'};
compile(#ddl_v1{}, #riak_sql_v1{'SELECT' = []}) ->
    {error, 'full table scan not implmented'};
compile(#ddl_v1{} = DDL, #riak_sql_v1{is_executable = false, type = sql} = Q) ->
    comp2(DDL, Q).

%% adding the local key here is a bodge
%% should be a helper fun in the generated DDL module but I couldn't
%% write that up in time
comp2(#ddl_v1{local_key = LK, partition_key = PK} = DDL,
      #riak_sql_v1{is_executable = false,
		   'WHERE'       = W} = Q) ->
    case compile_where(DDL, W) of
	{error, E} -> [{error, E}];
	NewWs      -> [Q#riak_sql_v1{is_executable = true,
				     type          = timeseries,
	 			     'WHERE'       = X,
				     local_key     = LK,
				     partition_key = PK} || X <- NewWs]
    end.

%% going forward the compilation and restructuring of the queries will be a big piece of work
%% for the moment we just brute force assert that the query is a timeseries SQL request
%% and go with that
compile_where(DDL, Where) ->
    case is_timeseries(DDL, Where) of
	false         -> {error, {where_not_supported, Where}};
	{true, NewWs} -> NewWs
end.

is_timeseries(#ddl_v1{bucket = _B, partition_key = PK, local_key = LK}, 
	      [{and_, _LHS, _RHS} = W]) ->
    try    #key_v1{ast = PAST} = PK,
	   #key_v1{ast = LAST} = LK,
	   LocalFields     = [X || #param_v1{name = X} <- LAST],
	   PartitionFields = [X || #param_v1{name = X} <- PAST],
	   QuantumFields   = [X || #hash_fn_v1{mod = riak_ql_quanta,
					       fn   = quantum,
					       args = [#param_v1{name = X} | _Rest]} <- PAST],
	   StrippedW = strip(W, []),
	   {StartW, EndW, Filter} = break_out_timeseries(StrippedW, LocalFields, QuantumFields),
	   StartKey = rewrite(LK, StartW),
	   EndKey = rewrite(LK, EndW),
	   {true, [[
		   {startkey, StartKey},
		   {endkey,   EndKey},
		   {filter,   Filter}
		  ]]}
    catch  _:_ -> {error, {where_not_timeseries, W}}
    end;
is_timeseries(_DLL, _Where) ->
    false.

break_out_timeseries(Ands, LocalFields, QuantumFields) ->
    {NewAnds, [Ends, Starts]} = get_fields(QuantumFields, Ands, []),
    {Filter, Body} = get_fields(LocalFields, NewAnds, []),
    {[Starts | Body], [Ends | Body], Filter}.

get_fields([], Ands, Acc) ->
    {Ands, lists:sort(Acc)};
get_fields([[H] | T], Ands, Acc) ->
    {NewAnds, Vals} = take(H, Ands, []),
    get_fields(T, NewAnds, Vals ++ Acc).

take(Key, Ands, Acc) ->
    case lists:keytake(Key, 2, Ands) of
	{value, Val, NewAnds} -> take(Key, NewAnds, [Val | Acc]);
	false                 -> {Ands, lists:sort(Acc)}
    end.

strip({and_, B, C}, Acc) -> strip(C, [B | Acc]);
strip(A, Acc)            -> [A | Acc].

rewrite(#key_v1{ast = AST}, W) ->
    rew2(AST, W, []).

rew2([], [], Acc) ->
   lists:reverse(Acc);
%% the rewrite should have consumed all the passed in values
rew2([], _W, _Acc) ->
    {error, invalid_rewrite};
rew2([#param_v1{name = [N]} | T], W, Acc) ->
     case lists:keytake(N, 2, W) of
	 false                           -> {error, invalid_rewrite};
	 {value, {_, _, {_, Val}}, NewW} -> rew2(T, NewW, [{N, Val} | Acc])
     end.

-ifdef(TEST).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

%%
%% Helper Fn for unit tests
%%

make_ddl(Bucket, Fields) when is_binary(Bucket) ->
    make_ddl(Bucket, Fields, #key_v1{}, #key_v1{}).

make_ddl(Bucket, Fields, PK) when is_binary(Bucket) ->
    make_ddl(Bucket, Fields, PK, #key_v1{}).

make_ddl(Bucket, Fields, #key_v1{} = PK, #key_v1{} = LK)
  when is_binary(Bucket) ->
    #ddl_v1{bucket        = Bucket,
	    fields        = Fields,
	    partition_key = PK,
	    local_key     = LK}.

make_query(Bucket, Selections) ->
    make_query(Bucket, Selections, []).

make_query(Bucket, Selections, Where) ->
    #riak_sql_v1{'FROM'   = Bucket,
		 'SELECT' = Selections,
		 'WHERE'  = Where}.

-define(MIN, 60 * 1000).
-define(NAME, "time").

is_query_valid(DDL, Q) ->
    case riak_ql_ddl:is_query_valid(DDL, Q) of
	false -> exit('invalid query');
	true  -> true
    end.    

get_query(String) ->
    Lexed = riak_ql_lexer:get_tokens(String),
    {ok, _Q} = riak_ql_parser:parse(Lexed).

get_standard_ddl() ->
    SQL = "CREATE TABLE GeoCheckin " ++
	"(geohash varchar not null, " ++ 
	"user varchar not null, " ++
	"time timestamp not null, " ++ 
	"weather varchar not null, " ++ 
	"temperature varchar, " ++ 
	"PRIMARY KEY((quantum(time, 15, m)), time, user))", 
    Lexed = riak_ql_lexer:get_tokens(SQL),
    {ok, DDL} = riak_ql_parser:parse(Lexed),
    {module, _Module} = riak_ql_ddl_compiler:make_helper_mod(DDL),
    DDL.

%%
%% Unit tests
%%

%%
%% rewrite passing tests
%%

simple_rewrite_test() ->
    LK  = #key_v1{ast = [
			 #param_v1{name = ["bob"]},
			 #param_v1{name = ["ripple"]}
			]},
    W   = [
	   {'=', "bob",    {word, "yardle"}},
	   {'>', "ripple", {int,  678}}
	  ],
    Exp = [
	   {"bob",    "yardle"}, 
	   {"ripple", 678}
	  ],
    Got = rewrite(LK, W),
    ?assertEqual(Exp, Got).

%%
%% rewrite failing tests
%%

simple_rewrite_fail_1_test() ->
    LK  = #key_v1{ast = [
			 #param_v1{name = ["bob"]},
			 #param_v1{name = ["ripple"]}
			]},
    W   = [
	   {'=', "bob", {"word", "yardle"}}
	  ],
    Exp = {error, invalid_rewrite},
    Got = rewrite(LK, W),
    ?assertEqual(Exp, Got).

simple_rewrite_fail_2_test() ->
    LK  = #key_v1{ast = [
			 #param_v1{name = ["bob"]},
			 #param_v1{name = ["archipelego"]}
			]},
    W   = [
	   {'=', "bob", {"word", "yardle"}}
	  ],
    Exp = {error, invalid_rewrite},
    Got = rewrite(LK, W),
    ?assertEqual(Exp, Got).

simple_rewrite_fail_3_test() ->
    LK  = #key_v1{ast = [
			 #param_v1{name = ["bob"]},
			 #param_v1{name = ["ripple"]},
			 #param_v1{name = ["kumquat"]}
		      ]},
    W   = [
	   {'=', "bob", {"word", "yardle"}}
	  ],
    Exp = {error, invalid_rewrite},
    Got = rewrite(LK, W),
    ?assertEqual(Exp, Got).

%%
%% complete query passing tests
%%

simplest_test() ->
    DDL = get_standard_ddl(),
    Query = "select weather from GeoCheckin where time > 3000 and time < 5000 and user = \"user_1\"",
    {ok, Q} = get_query(Query),
    true = is_query_valid(DDL, Q),
    Got = compile(DDL, Q),
    Expected = [Q#riak_sql_v1{is_executable = true,
			      type          = timeseries,
			      'WHERE'       = [
					       {startkey, [
							   {<<"time">>, 3000},
							   {<<"user">>, <<"user_1">>}
							   ]},
					       {endkey,   [
							   {<<"time">>, 5000},
							   {<<"user">>, <<"user_1">>}
							  ]},
					       {filter,   []}
					      ],
			      partition_key = #key_v1{ast = [
							     #hash_fn_v1{mod = riak_ql_quanta,
									 fn = quantum,
									 args = [
										 #param_v1{name = [<<"time">>]},
										 15, 
										 m
										],
									 type = timestamp}
							    ]
						     },
			      local_key = #key_v1{ast = [
							 #param_v1{name = [<<"time">>]},
							 #param_v1{name = [<<"user">>]}
							]}
		     }],
    ?assertEqual(Expected, Got).

%%
%% test failures
%%

no_where_clause_fail_test() ->
    DDL = get_standard_ddl(),
    Query = "select weather from GeoCheckin where time > 3000 and time < 5000 and user = \"user_1\"",
    {ok, Q} = get_query(Query),
    true = is_query_valid(DDL, Q),
    %% now replace the where clause
    Where = [],
    Q2 = Q#riak_sql_v1{'WHERE' = Where},
    Got = compile(DDL, Q2),
    Expected = [{error, {where_not_supported, Where}}],
    ?assertEqual(Expected, Got).

simplest_fail_test() ->
    DDL = get_standard_ddl(),
    Query = "select weather from GeoCheckin where time > 3000 and time < 5000 and user = \"user_1\"",
    {ok, Q} = get_query(Query),
    true = is_query_valid(DDL, Q),
    Where = [{xor_, {myop, "fakefield", 22}, {notherop, "real_gucci", atombomb}}],
    %% now replace the where clause
    Q2 = Q#riak_sql_v1{'WHERE' = Where},
    Got = compile(DDL, Q2),
    Expected = [{error, {where_not_supported, Where}}],
    ?assertEqual(Expected, Got).

simplest_compile_once_only_fail_test() ->
    DDL = get_standard_ddl(),
    Query = "select weather from GeoCheckin where time > 3000 and time < 5000 and user = \"user_1\"",
    {ok, Q} = get_query(Query),
    true = is_query_valid(DDL, Q),
    %% now try and compile twice
    [Q2] = compile(DDL, Q),
    Got = compile(DDL, Q2),
    Expected = {error, 'query is already compiled'},
    ?assertEqual(Expected, Got).

-endif.
