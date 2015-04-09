%% -------------------------------------------------------------------
%%
%% riak_kv_ddl: API module for the DDL
%%
%%
%% Copyright (c) 2007-2015 Basho Technologies, Inc.  All Rights Reserved.
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
-module(riak_kv_ddl).

-include("riak_kv_index.hrl").
-include("riak_kv_ddl.hrl").

%% this function can be used to work out which Module to use
-export([
	 make_module_name/1
	]).

%% a helper function for destructuring data objects
%% and testing the validity of field names
%% the generated helper functions cannot contain
%% record definitions because of the build cycle
%% so this function can be called out to to pick
%% apart the DDL records
-export([
	 get_partition_key/2,
	 get_local_key/2,
	 is_valid_field/2,
	 validate_query/2
	]).

-type bucket()     :: atom().
-type modulename() :: atom().

-spec make_module_name(bucket()) -> modulename().
make_module_name(Bucket) when is_binary(Bucket) ->
    Nonce = binary_to_list(base64:encode(crypto:hash(md4, Bucket))),
    Nonce2 = remove_hooky_chars(Nonce),
    ModName = "riak_kv_ddl_helper_mod_" ++ Nonce2,
    list_to_atom(ModName).

-spec get_partition_key(#ddl_v1{}, tuple()) -> tuple().
get_partition_key(#ddl_v1{bucket = B, partition_key = PK}, Obj)
  when is_tuple(Obj) ->
    Mod = make_module_name(B),
    #partition_key_v1{ast = Params} = PK,
    Key = build(Params, Obj, Mod, []),
    _K = sext:encode(Key).

-spec get_local_key(#ddl_v1{}, tuple()) -> tuple().
get_local_key(#ddl_v1{bucket = B, local_key = LK}, Obj) when is_tuple(Obj) ->
    Mod = make_module_name(B),
    #local_key_v1{ast = Params} = LK,
    Key = build(Params, Obj, Mod, []),
    _K = sext:encode(Key).

-spec build([#param_v1{}], tuple(), atom(), any()) -> tuple().
build([], _Obj, _Mod, A) ->
    list_to_tuple(lists:reverse(A));
build([#param_v1{name = Nm} | T], Obj, Mod, A) ->
    Val = Mod:extract(Obj, Nm),
    build(T, Obj, Mod, [Val | A]);
build([#hash_fn_v1{mod  = Md,
		   fn   = Fn,
		   args = Args} | T], Obj, Mod, A) ->
    A2 = convert(Args, Obj, Mod, []),
    Res = erlang:apply(Md, Fn, A2),
    build(T, Obj, Mod, [Res | A]).

-spec convert([#param_v1{}], tuple(), atom(), [any()]) -> any().
convert([], _Obj, _Mod, Acc) ->
    lists:reverse(Acc);
convert([#param_v1{name = Nm} | T], Obj, Mod, Acc) ->
    Val = Mod:extract(Obj, Nm),
    convert(T, Obj, Mod, [Val | Acc]);
convert([Atom | T], Obj, Mod, Acc) ->
    convert(T, Obj, Mod, [Atom | Acc]).

-spec is_valid_field(#ddl_v1{}, list()) -> boolean().
is_valid_field(#ddl_v1{bucket = B}, Fields) when is_list(Fields)->
    Mod = riak_kv_ddl:make_module_name(B),
    case (catch Mod:get_field_type(Fields)) of
	{'EXIT', _} -> false;
	_           -> true
    end.

validate_query(#ddl_v1{bucket = B} = DDL,
	       #riak_kv_li_index_v1{bucket     = B,
				    selections = S,
				    filters    = F}) ->
    ValidSelection = validate_selections(DDL, S),
    ValidFilters = validate_filters(DDL, F),
    case {ValidSelection, ValidFilters} of
	{true, true} -> ok;
	_            -> {error, ValidSelection, ValidFilters}
    end;
validate_query(#ddl_v1{bucket = B1}, #riak_kv_li_index_v1{bucket = B2}) ->
    Msg = io_lib:format("DDL has a bucket of ~p "
			"but query has a bucket of ~p~n", [B1, B2]),
    {error, {ddl_mismatch, lists:flatten(Msg)}}.

validate_filters(#ddl_v1{} = DDL, Filters) ->
    Fields = extract_fields(Filters),
    validate_selections(DDL, Fields).

validate_selections(#ddl_v1{} = DDL, Selections) ->
    CheckFn = fun(X, {Acc, Status}) ->
		      case is_valid_field(DDL, X) of
			  true  -> {Acc, Status};
			  false -> Msg = {invalid_field, X},
				   {[Msg | Acc], false}
		      end
	      end,
    case lists:foldl(CheckFn, {[], true}, Selections) of
	{[],   true}  -> true;
	{Msgs, false} -> {error, Msgs}
    end.

extract_fields(Fields) ->
    extract_f2(Fields, []).

extract_f2([], Acc) ->
    lists:reverse(Acc);
extract_f2([{Op, Field, _Val} | T], Acc) when Op =:= '='   orelse
					      Op =:= 'and' orelse
					      Op =:= 'or'  orelse
					      Op =:= '>'   orelse
					      Op =:= '<'   orelse
					      Op =:= '=<'  orelse
					      Op =:= '>=' ->
    NewAcc = case is_tuple(Field) of
		 true  -> extract_f2([Field], Acc);
		 false -> Acc
	     end,
    extract_f2(T, NewAcc).

remove_hooky_chars(Nonce) ->
    re:replace(Nonce, "[/|\+|\.|=]", "", [global, {return, list}]).

-ifdef(TEST).
-compile(export_all).

-define(VALID,   true).
-define(INVALID, false).

-include_lib("eunit/include/eunit.hrl").

%%
%% Helper Fn for unit tests
%%
partition(A, B, C) ->
    {A, B, C}.

%%
%% get partition_key tests
%%

simplest_partition_key_test_() ->
    Name = "yando",
    PK = #partition_key_v1{ast = [
				  #param_v1{name = [Name]}
				 ]},
    DDL = riak_kv_ddl_compiler:make_ddl(<<"simplest_partition_key_test">>,
					[
					 #riak_field_v1{name     = Name,
							position = 1,
							type     = binary}
					],
					PK),
    {module, _Module} = riak_kv_ddl_compiler:make_helper_mod(DDL),
    Obj = {<<"yarble">>},
    Result = (catch get_partition_key(DDL, Obj)),
    ?_assertEqual({<<"yarble">>}, sext:decode(Result)).

simple_partition_key_test_() ->
    Name1 = "yando",
    Name2 = "buckle",
    PK = #partition_key_v1{ast = [
				  #param_v1{name = [Name1]},
				  #param_v1{name = [Name2]}
				 ]},
    DDL = riak_kv_ddl_compiler:make_ddl(<<"simple_partition_key_test">>,
					[
					 #riak_field_v1{name     = Name2,
							position = 1,
							type     = binary},
					 #riak_field_v1{name     = "sherk",
							position = 2,
							type     = binary},
					 #riak_field_v1{name     = Name1,
							position = 3,
							type     = binary}
					],
					PK),
    {module, _Module} = riak_kv_ddl_compiler:make_helper_mod(DDL),
    Obj = {<<"one">>, <<"two">>, <<"three">>},
    Result = (catch get_partition_key(DDL, Obj)),
    ?_assertEqual({<<"three">>, <<"one">>}, sext:decode(Result)).

function_partition_key_test_() ->
    Name1 = "yando",
    Name2 = "buckle",
    PK = #partition_key_v1{ast = [
				  #param_v1{name = [Name1]},
				  #hash_fn_v1{mod  = ?MODULE,
					      fn   = partition,
					      args = [
						      #param_v1{name = [Name2]},
						      15,
						      m
						     ]
					     }
				 ]},
    DDL = riak_kv_ddl_compiler:make_ddl(<<"function_partition_key_test">>,
					[
					 #riak_field_v1{name     = Name2,
							position = 1,
							type     = timestamp},
					 #riak_field_v1{name     = "sherk",
							position = 2,
							type     = binary},
					 #riak_field_v1{name     = Name1,
							position = 3,
							type     = binary}
					],
					PK),
    {module, _Module} = riak_kv_ddl_compiler:make_helper_mod(DDL),
    Obj = {1234567890, <<"two">>, <<"three">>},
    Result = (catch get_partition_key(DDL, Obj)),
    Expected = {<<"three">>, {1234567890, 15, m}},
    ?_assertEqual(Expected, sext:decode(Result)).

complex_partition_key_test_() ->
    Name0 = "yerp",
    Name1 = "yando",
    Name2 = "buckle",
    Name3 = "doodle",
    PK = #partition_key_v1{ast = [
				  #param_v1{name = [Name0, Name1]},
				  #hash_fn_v1{mod  = ?MODULE,
					      fn   = partition,
					      args = [
						      #param_v1{name = [
									Name0,
									Name2,
									Name3
								       ]},
						      "something",
						      pong
						     ]
					     },
				  #hash_fn_v1{mod  = ?MODULE,
					      fn   = partition,
					      args = [
						      #param_v1{name = [
									Name0,
									Name1
								       ]},
						      #param_v1{name = [
									Name0,
									Name2,
									Name3
								       ]},
						      pang
						     ]
					     }
				 ]},
    Map3 = {map, [
		  #riak_field_v1{name     = "in_Map_2",
				 position = 1,
				 type     = integer}
		 ]},
    Map2 = {map, [
		  #riak_field_v1{name     = Name3,
				 position = 1,
				 type     = integer}
		 ]},
    Map1 = {map, [
		  #riak_field_v1{name     = Name1,
				 position = 1,
				 type     = integer},
		  #riak_field_v1{name     = Name2,
				 position = 2,
				 type     = Map2},
		  #riak_field_v1{name     = "Level_1_map2",
				 position = 3,
				 type     = Map3}
		 ]},
    DDL = riak_kv_ddl_compiler:make_ddl(<<"complex_partition_key_test">>,
					[
					 #riak_field_v1{name     = Name0,
							position = 1,
							type     = Map1}
					],
					PK),
    {module, _Module} = riak_kv_ddl_compiler:make_helper_mod(DDL),
    Obj = {{2, {3}, {4}}},
    Result = (catch get_partition_key(DDL, Obj)),
    Expected = {2, {3, "something", pong}, {2, 3, pang}},
    ?_assertEqual(Expected, sext:decode(Result)).

%%
%% get local_key tests
%%

%% local keys share the same code path as partition keys
%% so only need the lightest tests
simplest_local_key_test_() ->
    Name = "yando",
    PK = #partition_key_v1{ast = [
				  #param_v1{name = [Name]}
				 ]},
    LK = #local_key_v1{ast = [
			      #param_v1{name = [Name]}
			     ]},
    DDL = riak_kv_ddl_compiler:make_ddl(<<"simplest_key_key_test">>,
					[
					 #riak_field_v1{name     = Name,
							position = 1,
							type     = binary}
					],
					PK, LK),
    {module, _Module} = riak_kv_ddl_compiler:make_helper_mod(DDL),
    Obj = {<<"yarble">>},
    Result = (catch get_partition_key(DDL, Obj)),
    ?_assertEqual({<<"yarble">>}, sext:decode(Result)).

%%
%% get type of named field
%%

simplest_valid_get_type_test_() ->
    DDL = riak_kv_ddl_compiler:make_ddl(<<"simplest_valid_get_type_test">>,
					[
					 #riak_field_v1{name     = "yando",
							position = 1,
							type     = binary}
					]),
    {module, Module} = riak_kv_ddl_compiler:make_helper_mod(DDL),
    Result = (catch Module:get_field_type(["yando"])),
    ?_assertEqual(binary, Result).

simple_valid_get_type_test_() ->
    DDL = riak_kv_ddl_compiler:make_ddl(<<"simple_valid_get_type_test">>,
					[
					 #riak_field_v1{name     = "scoobie",
							position = 1,
							type     = integer},
					 #riak_field_v1{name     = "yando",
							position = 2,
							type     = binary}
					]),
    {module, Module} = riak_kv_ddl_compiler:make_helper_mod(DDL),
    Result = (catch Module:get_field_type(["yando"])),
    ?_assertEqual(binary, Result).

simple_valid_map_get_type_1_test_() ->
    Map = {map, [
		 #riak_field_v1{name     = "yarple",
				position = 1,
				type     = integer}
		]},
    DDL = riak_kv_ddl_compiler:make_ddl(<<"simple_valid_map_get_type_1_test">>,
					[
					 #riak_field_v1{name     = "yando",
							position = 1,
							type     = binary},
					 #riak_field_v1{name     = "erko",
							position = 2,
							type     = Map},
					 #riak_field_v1{name     = "erkle",
							position = 3,
							type     = float}
					]),
    {module, Module} = riak_kv_ddl_compiler:make_helper_mod(DDL),
    Result = (catch Module:get_field_type(["erko", "yarple"])),
    ?_assertEqual(integer, Result).

simple_valid_map_get_type_2_test_() ->
    Map = {map, [
		 #riak_field_v1{name     = "yarple",
				position = 1,
				type     = integer}
		]},
    DDL = riak_kv_ddl_compiler:make_ddl(<<"simple_valid_map_get_type_2_test">>,
					[
					 #riak_field_v1{name     = "yando",
							position = 1,
							type     = binary},
					 #riak_field_v1{name     = "erko",
							position = 2,
							type     = Map},
					 #riak_field_v1{name     = "erkle",
							position = 3,
							type     = float}
					]),
    {module, Module} = riak_kv_ddl_compiler:make_helper_mod(DDL),
    Result = (catch Module:get_field_type(["erko"])),
    ?_assertEqual(map, Result).

complex_valid_map_get_type_test_() ->
    Map3 = {map, [
		  #riak_field_v1{name     = "in_Map_2",
				 position = 1,
				 type     = integer}
		 ]},
    Map2 = {map, [
		  #riak_field_v1{name     = "in_Map_1",
				 position = 1,
				 type     = integer}
		 ]},
    Map1 = {map, [
		  #riak_field_v1{name     = "Level_1_1",
				 position = 1,
				 type     = integer},
		  #riak_field_v1{name     = "Level_1_map1",
				 position = 2,
				 type     = Map2},
		  #riak_field_v1{name     = "Level_1_map2",
				 position = 3,
				 type     = Map3}
		 ]},
    DDL = riak_kv_ddl_compiler:make_ddl(<<"complex_valid_map_get_type_test">>,
					[
					 #riak_field_v1{name     = "Top_Map",
							position = 1,
							type     = Map1}
					]),
    {module, Module} = riak_kv_ddl_compiler:make_helper_mod(DDL, "/tmp"),
    Path = ["Top_Map", "Level_1_map1", "in_Map_1"],
    Res = (catch Module:get_field_type(Path)),
    ?_assertEqual(integer, Res).

-endif
