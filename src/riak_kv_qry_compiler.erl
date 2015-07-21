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
					      'SELECT'       = F} = Q) ->
    %% break out all the 'SELECT' into their own queries and then compile them
    ListQ = [Q#riak_sql_v1{'SELECT' = [X]} || X <- F],
    [comp2(DDL, X) || X <- ListQ].

comp2(#ddl_v1{partition_key = PK},
      #riak_sql_v1{is_executable = false,
			   'SELECT'       = F} = Q) ->
    case compile_filter(PK, F) of
	{error, E} -> {error, E};
	NewF       -> Q#riak_sql_v1{is_executable = true,
					    'SELECT'       = NewF}
    end.

%% going forward the compilation and restructuring of the queries will be a big piece of work
%% for the moment we just brute force assert that the query is a timeseries SQL request
%% and go with that
%% just flip the and clause
compile_filter(#partition_key_v1{} = PK,
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
    compile_filter(PK, [{and_, RHS, LHS}]);
compile_filter(#partition_key_v1{} = PK,
	       [{and_,
		 {Op1, "timestamp", T1},
		 {Op2, "timestamp", T2}
		} = F])
  when
      (is_integer(T1) andalso is_integer(T2))
      andalso
      ((Op1 =:= '>' orelse Op1 =:= '>=')
       andalso
	 (Op2 =:= '<' orelse Op2 =:= '=<')) ->
    %% TODO fix up this super shonk
    #partition_key_v1{ast = [#hash_fn_v1{mod  = riak_kv_quanta,
					 fn   = quantum,
					 args = [
						 #param_v1{name = ["timestamp"]},
						 Quantity,
						 Unit
						]}
			    ]} = PK,
    Quanta = riak_kv_quanta:quanta(T1, T2, Quantity, Unit),
    [F];
compile_filter(#partition_key_v1{} = _PK, Filter) ->
    {error, {filter_not_supported, Filter}}.

%-ifdef(TEST).
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

basic_DDL_and_Q(Bucket) when is_binary(Bucket) ->
    Name = "timestamp",
    PK = #partition_key_v1{ast = [
				  #hash_fn_v1{mod  = riak_kv_quanta,
					      fn   = quantum,
					      args = [
						      #param_v1{name = [Name]},
						      15,
						      m
						     ]
					     }
				 ]},
    DDL = make_ddl(Bucket,
		   [
		    #riak_field_v1{name     = Name,
				   position = 1,
				   type     = binary}
		   ],
		   PK),
    {module, _Module} = riak_kv_ddl_compiler:make_helper_mod(DDL, "/tmp"),
    Q = make_query(Bucket, [["*"]], [{and_,
				      {'>', Name, 100000},
				      {'<', Name, 910001}
				     }]),
    case riak_kv_ddl:is_query_valid(DDL, Q) of
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
    Filter = [{xor_, {myop, "fakefield", 22}, {notherop, "real_gucci", atombomb}}],
    Q2 = Q#riak_sql_v1{'SELECT' = Filter},
    Got = compile(DDL, Q2),
    ?assertEqual([{error, {filter_not_supported, Filter}}], Got).

simplest_compile_once_only_fail_test() ->
    {DDL, Q} = basic_DDL_and_Q(<<"simplest_compile_once_only_fail_test">>),
    [Q2] = compile(DDL, Q),
    Got = compile(DDL, Q2),
    Expected = {error, 'query is already compiled'},
    ?assertEqual(Expected, Got).

%-endif.
