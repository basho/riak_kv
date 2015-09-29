%% -------------------------------------------------------------------
%%
%% riak_kv_qry_compiler: generate the coverage for a hashed query
%%
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
-module(riak_kv_qry_compiler).

-export([
         compile/2
        ]).

-include_lib("riak_ql/include/riak_ql_ddl.hrl").
-include("riak_kv_index.hrl").

compile(#ddl_v1{}, #riak_sql_v1{is_executable = true}) ->
    {error, 'query is already compiled'};
compile(#ddl_v1{}, #riak_sql_v1{'SELECT' = []}) ->
    {error, 'full table scan not implmented'};
compile(#ddl_v1{} = DDL, #riak_sql_v1{is_executable = false, 
				      type          = sql} = Q) ->
    comp2(DDL, Q).

%% adding the local key here is a bodge
%% should be a helper fun in the generated DDL module but I couldn't
%% write that up in time
comp2(#ddl_v1{} = DDL, #riak_sql_v1{is_executable = false,
				    'WHERE'       = W} = Q) ->
    case compile_where(DDL, W) of
        {error, E} -> [{error, E}];
        NewW       -> expand_query(DDL, Q, NewW)
    end.

%% now break out the query on quantum boundaries
expand_query(#ddl_v1{local_key = LK, partition_key = PK}, Q, NewW) ->
    NewWs = expand_where(NewW, PK),
    [Q#riak_sql_v1{is_executable = true,
		   type          = timeseries,
		   'WHERE'       = X,
		   local_key     = LK,
		   partition_key = PK} || X <- NewWs].
	

expand_where(Where, #key_v1{ast = PAST}) ->
    GetMaxMinFun = fun({startkey, [{_, _, H} | _T]}, {_S, E}) ->
			   {H, E};
		      ({endkey,   [{_, _, H} | _T]}, {S, _E}) ->
			   {S, H};
		      (_, {S, E})  ->
			   {S, E}
		   end,
    {Min, Max} = lists:foldl(GetMaxMinFun, {"", ""}, Where),
    [{[QField], Q, U}] = [{X, Y, Z} 
			  || #hash_fn_v1{mod = riak_ql_quanta,
					 fn   = quantum,
					 args = [#param_v1{name = X}, Y, Z]} 
				 <- PAST],
    {NoSubQueries, Boundaries} = riak_ql_quanta:quanta(Min, Max, Q, U),
    case NoSubQueries of
	1 -> [Where];
	_ -> _NewWs = make_wheres(Where, QField, Min, Max, Boundaries)
    end.

make_wheres(Where, QField, Min, Max, Boundaries) ->
    Starts = [Min | Boundaries],
    Ends   = Boundaries ++ [Max],
    make_w2(Starts, Ends, QField, Where, []).

make_w2([], [], _QField, _Where, Acc) ->
    lists:reverse(Acc);
make_w2([Start | T1], [End | T2], QField, Where, Acc) ->
    Where2 = swap(Where, QField, startkey, Start),
    Where3 = swap(Where2, QField, endkey, End),
    make_w2(T1, T2, QField, Where, [Where3 | Acc]).

%% this rewrite is premised on the fact the the Query field is a timestamp
swap(Where, QField, Key, Val) ->
    {Key, Fields} = lists:keyfind(Key, 1, Where),
    NewFields = lists:keyreplace(QField, 1, Fields, {QField, timestamp, Val}),
    _NewWhere = lists:keyreplace(Key, 1, Where, {Key, NewFields}).

%% going forward the compilation and restructuring of the queries will be a big piece of work
%% for the moment we just brute force assert that the query is a timeseries SQL request
%% and go with that
compile_where(DDL, Where) ->
    case check_if_timeseries(DDL, Where) of
        {error, E}   -> {error, E};
        {true, NewW} -> NewW
end.

check_if_timeseries(#ddl_v1{bucket = B, partition_key = PK, local_key = LK},
              [{and_, _LHS, _RHS} = W]) ->
    try    #key_v1{ast = PAST} = PK,
           #key_v1{ast = LAST} = LK,
           LocalFields     = [X || #param_v1{name = X} <- LAST],
           PartitionFields = [X || #param_v1{name = X} <- PAST],
           [QField] = [X || #hash_fn_v1{mod  = riak_ql_quanta,
					fn   = quantum,
					args = [#param_v1{name = X} | _Rest]} 
				<- PAST],
	   StrippedW = strip(W, []),
	   {StartW, EndW, Filter} = break_out_timeseries(StrippedW, LocalFields, [QField]),
	   Mod = riak_ql_ddl:make_module_name(B),
	   StartKey = rewrite(LK, StartW, Mod),
           EndKey = rewrite(LK, EndW, Mod),
	   HasErrs = [X || {error, _} = X <- [StartKey, EndKey]],
	   case HasErrs of
	       [] ->
		   RewrittenFilter = add_types_to_filter(Filter, Mod),
		   {true, [
			   {startkey, StartKey},
			   {endkey,   EndKey},
			   {filter,   RewrittenFilter}
			  ]};
	       _ ->
		   {error, {invalid_where_clause, W}}
	   end
    catch  _:_ -> {error, {where_not_timeseries, W}}
    end;
check_if_timeseries(_DLL, Where) ->
    {error, {where_not_supported, Where}}.

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

add_types_to_filter(Filter, Mod) ->
    add_types2(Filter, Mod, []).

add_types2([], _Mod, Acc) ->
    make_ands(lists:reverse(Acc));
add_types2([{Op, LHS, RHS} | T], Mod, Acc) when Op =:= and_ orelse
						Op =:= or_  ->
    NewAcc = {Op, add_types2([LHS], Mod, []), add_types2([RHS], Mod, [])},
    add_types2(T, Mod, [NewAcc | Acc]);
add_types2([{Op, Field, {_, Val}} | T], Mod, Acc) ->
    NewAcc = {Op, {field, Field, Mod:get_field_type([Field])}, {const, Val}},
    add_types2(T, Mod, [NewAcc | Acc]).	      

%% I know, not tail recursive could stackbust
%% but not really
make_ands([]) ->
    [];
make_ands([H | []]) ->
    H;
make_ands([H | T]) ->
    {and_, H, make_ands(T)}.

rewrite(#key_v1{ast = AST}, W, Mod) ->
    rew2(AST, W, Mod, []).

rew2([], [], _Mod, Acc) ->
   lists:reverse(Acc);
%% the rewrite should have consumed all the passed in values
rew2([], _W, _Mod, _Acc) ->
    {error, invalid_rewrite};
rew2([#param_v1{name = [N]} | T], W, Mod, Acc) ->
    Type = Mod:get_field_type([N]),
    case lists:keytake(N, 2, W) of
	false                           -> 
	    {error, invalid_rewrite};
	{value, {_, _, {_, Val}}, NewW} -> 
	    rew2(T, NewW, Mod, [{N, Type, Val} | Acc])
    end.

-ifdef(TEST).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

%%
%% Helper Fns for unit tests
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

get_long_ddl() ->
    SQL = "CREATE TABLE GeoCheckin " ++
        "(geohash varchar not null, " ++
        "user varchar not null, " ++
	"extra int not null, " ++
	"more float not null, " ++
        "time timestamp not null, " ++
        "weather varchar not null, " ++
        "temperature varchar, " ++
        "PRIMARY KEY((quantum(time, 15, s)), time, user))",
    get_ddl(SQL).

get_standard_ddl() ->
    SQL = "CREATE TABLE GeoCheckin " ++
        "(geohash varchar not null, " ++
        "user varchar not null, " ++
        "time timestamp not null, " ++
        "weather varchar not null, " ++
        "temperature varchar, " ++
        "PRIMARY KEY((quantum(time, 15, s)), time, user))",
    get_ddl(SQL).

get_ddl(SQL) ->
    Lexed = riak_ql_lexer:get_tokens(SQL),
    {ok, DDL} = riak_ql_parser:parse(Lexed),
    {module, _Module} = riak_ql_ddl_compiler:make_helper_mod(DDL),
    DDL.

get_standard_pk() ->
    #key_v1{ast = [
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

get_standard_lk() ->
    #key_v1{ast = [
		   #param_v1{name = [<<"time">>]},
		   #param_v1{name = [<<"user">>]}
		  ]}.

%%
%% Unit tests
%%

%%
%% tests for adding type information and rewriting filters
%%

simple_filter_typing_test() ->
    #ddl_v1{bucket = B} = get_long_ddl(),
    Mod = riak_ql_ddl:make_module_name(B),
    Filter = [
	      {or_, 
	       {'=', <<"weather">>, {word, <<"yankee">>}},
	       {and_, 
		{'=', <<"geohash">>,     {word, <<"erko">>}},
		{'=', <<"temperature">>, {word, <<"yelp">>}}
	       }
	      },
	      {'=', <<"extra">>, {int, 1}}
	     ],
    Got = add_types_to_filter(Filter, Mod),
    Expected = {and_, 
	    {or_, 
	     {'=', {field, <<"weather">>, binary}, {const, <<"yankee">>}},
	     {and_, 
	      {'=', {field, <<"geohash">>,     binary}, {const, <<"erko">>}},
	      {'=', {field, <<"temperature">>, binary}, {const, <<"yelp">>}}
	     }
	    },
	    {'=', {field, <<"extra">>, integer}, {const, 1}}
	   },
    ?assertEqual(Expected, Got).

%%
%% rewrite passing tests
%%
%% success here is because the where clause covers the entire local key
%% we have enough info to build a range scan
%%
simple_rewrite_test() ->
    #ddl_v1{bucket = B} = get_standard_ddl(),
    Mod = riak_ql_ddl:make_module_name(B),
    LK  = #key_v1{ast = [
                         #param_v1{name = [<<"geohash">>]},
                         #param_v1{name = [<<"time">>]}
                        ]},
    W   = [
           {'=', <<"geohash">>, {word, "yardle"}},
           {'>', <<"time">>,    {int,   678}}
          ],
    Exp = [
           {<<"geohash">>,  binary,   "yardle"},
           {<<"time">>,     timestamp, 678}
          ],
    Got = rewrite(LK, W, Mod),
    ?assertEqual(Exp, Got).

%%
%% rewrite failing tests
%%
%% failure is because the where clause does NOT cover the
%% local key - there is no enough info for a range scan
%%
simple_rewrite_fail_1_test() ->
    #ddl_v1{bucket = B} = get_standard_ddl(),
    Mod = riak_ql_ddl:make_module_name(B),
    LK  = #key_v1{ast = [
                         #param_v1{name = [<<"geohash">>]},
                         #param_v1{name = [<<"user">>]}
                        ]},
    W   = [
           {'=', <<"geohash">>, {"word", "yardle"}}
          ],
    Exp = {error, invalid_rewrite},
    Got = rewrite(LK, W, Mod),
    ?assertEqual(Exp, Got).

simple_rewrite_fail_2_test() ->
    #ddl_v1{bucket = B} = get_standard_ddl(),
    Mod = riak_ql_ddl:make_module_name(B),
    LK  = #key_v1{ast = [
                         #param_v1{name = [<<"geohash">>]},
                         #param_v1{name = [<<"user">>]}
                        ]},
    W   = [
           {'=', <<"user">>, {"word", "yardle"}}
          ],
    Exp = {error, invalid_rewrite},
    Got = rewrite(LK, W, Mod),
    ?assertEqual(Exp, Got).

simple_rewrite_fail_3_test() ->
    #ddl_v1{bucket = B} = get_standard_ddl(),
    Mod = riak_ql_ddl:make_module_name(B),
    LK  = #key_v1{ast = [
                         #param_v1{name = [<<"geohash">>]},
                         #param_v1{name = [<<"user">>]},
                         #param_v1{name = [<<"temperature">>]}
                      ]},
    W   = [
           {'=', <<"geohash">>, {"word", "yardle"}}
          ],
    Exp = {error, invalid_rewrite},
    Got = rewrite(LK, W, Mod),
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
                                                           {<<"time">>, 
							    timestamp, 
							    3000},
                                                           {<<"user">>, 
							    binary,
							    <<"user_1">>}
                                                           ]},
                                               {endkey,   [
                                                           {<<"time">>, 
							    timestamp, 
							    5000},
                                                           {<<"user">>, 
							    binary, 
							    <<"user_1">>}
                                                          ]},
                                               {filter,   []}
                                              ],
                              partition_key = get_standard_pk(),
                              local_key     = get_standard_lk()
                     }],
    ?assertEqual(Expected, Got).

simple_with_filter_test() ->
    DDL = get_standard_ddl(),
    Query = "select weather from GeoCheckin where time > 3000 and time < 5000 and user = \"user_1\" and weather = 'yankee'",
    {ok, Q} = get_query(Query),
    true = is_query_valid(DDL, Q),
    Got = compile(DDL, Q),
    Where = [
	     {startkey, [
			 {<<"time">>, timestamp, 3000},
			 {<<"user">>, binary,    <<"user_1">>}
			]},
	     {endkey,   [
			 {<<"time">>, timestamp, 5000},
			 {<<"user">>, binary,    <<"user_1">>}
			]},
	     {filter,   {'=', {field, <<"weather">>, binary}, 
			 {const, <<"yankee">>}}}
	    ],
    Expected = [Q#riak_sql_v1{is_executable = true,
                              type          = timeseries,
                              'WHERE'       = Where,
                              partition_key = get_standard_pk(),
                              local_key     = get_standard_lk()
                     }],
    ?assertEqual(Expected, Got).

simple_with_2_field_filter_test() ->
    DDL = get_standard_ddl(),
    Query = "select weather from GeoCheckin where time > 3000 and time < 5000 and user = \"user_1\" and weather = 'yankee' and temperature = 'yelp'",
    {ok, Q} = get_query(Query),
    true = is_query_valid(DDL, Q),
    Got = compile(DDL, Q),
    Where = [
	     {startkey, [
			 {<<"time">>, timestamp, 3000},
			 {<<"user">>, binary,    <<"user_1">>}
			]},
	     {endkey,   [
			 {<<"time">>, timestamp, 5000},
			 {<<"user">>, binary,    <<"user_1">>}
			]},
	     {filter,   {and_, 
			 {'=', {field, <<"weather">>,     binary}, 
			  {const, <<"yankee">>}},
			 {'=', {field, <<"temperature">>, binary}, 
			  {const, <<"yelp">>}}
			}
	     }
	    ],
    Expected = [Q#riak_sql_v1{is_executable = true,
                              type          = timeseries,
                              'WHERE'       = Where,
                              partition_key = get_standard_pk(),
                              local_key     = get_standard_lk()
                     }],
    ?assertEqual(Expected, Got).

complex_with_4_field_filter_test() ->
    DDL = get_long_ddl(),
    Query = "select weather from GeoCheckin where time > 3000 and time < 5000 and user = \"user_1\" and extra = 1 and (weather = 'yankee' or (temperature = 'yelp' and geohash = 'erko'))",
    {ok, Q} = get_query(Query),
    true = is_query_valid(DDL, Q),
    Got = compile(DDL, Q),
    Where = [
	     {startkey, [
			 {<<"time">>, timestamp, 3000},
			 {<<"user">>, binary,    <<"user_1">>}
			]},
	     {endkey,   [
			 {<<"time">>, timestamp, 5000},
			 {<<"user">>, binary,    <<"user_1">>}
			]},
	     {filter,   {and_, 
			 {or_, 
			  {'=', {field, <<"weather">>, binary}, 
			   {const, <<"yankee">>}},
			  {and_, 
			   {'=', {field, <<"geohash">>,     binary}, 
			    {const, <<"erko">>}},
			   {'=', {field, <<"temperature">>, binary}, 
			    {const, <<"yelp">>}}
			  }
			 },
			 {'=', {field, <<"extra">>, integer}, 
			  {const, 1}}
			}
	     }
	    ],
    Expected = [Q#riak_sql_v1{is_executable = true,
                              type          = timeseries,
                              'WHERE'       = Where,
                              partition_key = get_standard_pk(),
                              local_key     = get_standard_lk()
                     }],
    ?assertEqual(Expected, Got).

%% got for 3 queries to get partition ordering problems flushed out
simple_spanning_boundary_test() ->
    DDL = get_standard_ddl(),
    Query = "select weather from GeoCheckin where time > 3000 and time < 31000 and user = \"user_1\"",
    {ok, Q} = get_query(Query),
    true = is_query_valid(DDL, Q),
    %% get basic query
    Got = compile(DDL, Q),
    %% now make the result - expecting 3 queries
    W1 = [
	  {startkey, [{<<"time">>, timestamp, 3000},  
		      {<<"user">>, binary, <<"user_1">>}]},
	  {endkey,   [{<<"time">>, timestamp, 15000}, 
		      {<<"user">>, binary, <<"user_1">>}]},
	  {filter, []}
	 ],
    W2 = [
	  {startkey, [{<<"time">>, timestamp, 15000}, 
		      {<<"user">>, binary, <<"user_1">>}]},
	  {endkey,   [{<<"time">>, timestamp, 30000}, 
		      {<<"user">>, binary, <<"user_1">>}]},
	  {filter, []}
	 ],
    W3 = [
	  {startkey, [{<<"time">>, timestamp, 30000}, 
		      {<<"user">>, binary, <<"user_1">>}]},
	  {endkey,   [{<<"time">>, timestamp, 31000}, 
		      {<<"user">>, binary, <<"user_1">>}]},
	  {filter, []}
	 ],
    Expected = [
		Q#riak_sql_v1{is_executable = true,
			      type          = timeseries,
			      'WHERE'       = W1,
                              partition_key = get_standard_pk(),
                              local_key     = get_standard_lk()},
		Q#riak_sql_v1{is_executable = true,
			      type          = timeseries,
			      'WHERE'       = W2,
                              partition_key = get_standard_pk(),
                              local_key     = get_standard_lk()},
		Q#riak_sql_v1{is_executable = true,
			      type          = timeseries,
			      'WHERE'       = W3,
                              partition_key = get_standard_pk(),
                              local_key     = get_standard_lk()}
	       ],
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
