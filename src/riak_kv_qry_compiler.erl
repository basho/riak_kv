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
-include_lib("riak_kv/include/riak_kv_index.hrl").

compile(#ddl_v1{}, #riak_sql_v1{is_executable = true}) ->
    {error, 'query is already compiled'};
compile(#ddl_v1{}, #riak_sql_v1{'SELECT' = []}) ->
    {error, 'full table scan not implmented'};
compile(#ddl_v1{} = DDL, #riak_sql_v1{is_executable = false,
				      'SELECT'      = F} = Q) ->
    %% break out all the 'SELECT' into their own queries and then compile them
    ListQ = [Q#riak_sql_v1{'SELECT' = [X]} || X <- F],
    lists:flatten([comp2(DDL, X) || X <- ListQ]).

comp2(#ddl_v1{partition_key = PK},
      #riak_sql_v1{is_executable = false,
		   'WHERE'      = W} = Q) ->
    case compile_where(PK, W) of
	{error, E} -> {error, E};
	NewWs      -> [Q#riak_sql_v1{is_executable = true,
	 			     'WHERE'        = [X]} || X <- NewWs]
    end.

%% going forward the compilation and restructuring of the queries will be a big piece of work
%% for the moment we just brute force assert that the query is a timeseries SQL request
%% and go with that
%% just flip the 'and' clause
compile_where(#partition_key_v1{} = PK,
	       [{and_,
		 {Op1, "timestamp", T1} = LHS,
		 {Op2, "timestamp", T2} = RHS
		}])
  when
      (is_integer(T1) andalso is_integer(T2))
      andalso
      ((Op1 =:= '<' orelse Op1 =:= '=<')
       andalso
	 (Op2 =:= '>' orelse Op2 =:= '>='))->
    compile_where(PK, [{and_, RHS, LHS}]);
compile_where(#partition_key_v1{} = PK,
	       [{and_,
		 {Op1, "timestamp", T1},
		 {Op2, "timestamp", T2}
		} = W])
  when
      (is_integer(T1) andalso is_integer(T2))
      andalso
      ((Op1 =:= '>' orelse Op1 =:= '>=')
       andalso
	 (Op2 =:= '<' orelse Op2 =:= '=<')) ->
    #partition_key_v1{ast = [#hash_fn_v1{mod  = riak_ql_quanta,
					 fn   = quantum,
					 args = [
						 #param_v1{name = ["timestamp"]},
						 Quantity,
						 Unit
						]}
			    ]} = PK,
    case riak_ql_quanta:quanta(T1, T2, Quantity, Unit) of
	{1, []}          -> [W];
	{_N, Boundaries} -> StartClauses = [{'>=', X} || X <- Boundaries],
			    EndClauses   = [{'<',  X} || X <- Boundaries],
			    Starts = [{Op1, T1} | StartClauses],
			    Ends   = EndClauses ++ [{Op2, T2}],
			    make_wheres(Starts, Ends)
    end;
compile_where(#partition_key_v1{} = _PK, Where) ->
    {error, {where_not_supported, Where}}.

make_wheres(Starts, Ends) ->
    make_w2(Starts, Ends, []).

make_w2([], [], Acc) -> 
    lists:reverse(Acc);
make_w2([{Op1, V1} | T1], [{Op2, V2} | T2], Acc) -> 
    make_w2(T1, T2, [make_where({Op1, V1}, {Op2, V2}) | Acc]).

make_where({Op1, Start}, {Op2, End}) ->
    {and_,
     {Op1, "timestamp", Start},
     {Op2, "timestamp", End}
     }.

-ifdef(TEST).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

%%
%% Helper Fn for unit tests
%%

make_ddl(Bucket, Fields) when is_binary(Bucket) ->
    make_ddl(Bucket, Fields, #partition_key_v1{}, #local_key_v1{}).

make_ddl(Bucket, Fields, PK) when is_binary(Bucket) ->
    make_ddl(Bucket, Fields, PK, #local_key_v1{}).

make_ddl(Bucket, Fields, #partition_key_v1{} = PK, #local_key_v1{} = LK)
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
-define(NAME, "timestamp").

basic_DDL_and_Q(Bucket) when is_binary(Bucket) ->
    PK = #partition_key_v1{ast = [
				  #hash_fn_v1{mod  = riak_ql_quanta,
					      fn   = quantum,
					      args = [
						      #param_v1{name = [?NAME]},
						      15,
						      m
						     ]
					     }
				 ]},
    DDL = make_ddl(Bucket,
		   [
		    #riak_field_v1{name     = ?NAME,
				   position = 1,
				   type     = binary}
		   ],
		   PK),
    {module, _Module} = riak_ql_ddl_compiler:make_helper_mod(DDL),
    Q = make_query(Bucket, [["*"]], [{and_,
				      {'>', ?NAME, 1 * ?MIN},
				      {'<', ?NAME, 2 * ?MIN}
				     }]),
    case riak_ql_ddl:is_query_valid(DDL, Q) of
	false -> exit('invalid query');
	true  -> {DDL, Q}
    end.

%%
%% Unit tests
%%

simplest_test() ->
    {DDL, Q} = basic_DDL_and_Q(<<"simplest_test">>),
    Got = compile(DDL, Q),
    Expected = [Q#riak_sql_v1{is_executable = true}],
    ?assertEqual(Expected, Got).

simple_double_AND_test() ->
    {DDL, Q} = basic_DDL_and_Q(<<"simple_double_AND_test">>),
     #riak_sql_v1{'SELECT' = [F]} = Q,
    Q2 = Q#riak_sql_v1{'SELECT' = [F, F]},
    CompQ = Q#riak_sql_v1{is_executable = true},
    Got = compile(DDL, Q2),
    Expected = [CompQ, CompQ],
    ?assertEqual(Expected, Got).

simplest_fail_test() ->
    {DDL, Q} = basic_DDL_and_Q(<<"simplest_fail_test">>),
    Where = [{xor_, {myop, "fakefield", 22}, {notherop, "real_gucci", atombomb}}],
    Q2 = Q#riak_sql_v1{'WHERE' = Where},
    Got = compile(DDL, Q2),
    Expected = [{error, {where_not_supported, Where}}],
    ?assertEqual(Expected, Got).

simplest_compile_once_only_fail_test() ->
    {DDL, Q} = basic_DDL_and_Q(<<"simplest_compile_once_only_fail_test">>),
    [Q2] = compile(DDL, Q),
    Got = compile(DDL, Q2),
    Expected = {error, 'query is already compiled'},
    ?assertEqual(Expected, Got).

simple_spanning_boundary_test() ->
    %% get basic query
    {DDL, Q} = basic_DDL_and_Q(<<"simplest_test">>),
    %% make new Where clause
    NewW = [
	    {and_,
	     {'>', ?NAME, 1 * ?MIN},
	     {'<', ?NAME, 16 * ?MIN}
	    }
	   ],
    Q2 = Q#riak_sql_v1{'WHERE' = NewW},
    Got = compile(DDL, Q2),
    % now make the result - expecting 2 queries
    W1 = [
	    {and_,
	     {'>', ?NAME, 1 * ?MIN},
	     {'<', ?NAME, 15 * ?MIN}
	    }
	 ],
    W2 = [
	    {and_,
	     {'>=', ?NAME, 15 * ?MIN},
	     {'<',  ?NAME, 16 * ?MIN}
	    }
	 ],
    Expected = [
		Q#riak_sql_v1{is_executable = true,
			      'WHERE'       = W1},
		Q#riak_sql_v1{is_executable = true,
			      'WHERE'       = W2}
	       ],
    ?assertEqual(Expected, Got).

-endif.
