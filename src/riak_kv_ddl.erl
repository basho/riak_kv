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
	 is_query_valid/2
	]).

-ifdef(TEST).
%% these internal funs are exposed for the development period
%% they are NOT part of the final API and the export statement
%% will be removed
-export([
	 make_ddl/2,
	 are_selections_valid/3
	]).
-endif.

-define(CANBEBLANK,  true).
-define(CANTBEBLANK, false).

-type bucket()                 :: binary().
-type modulename()             :: atom().
-type hierarchical_fieldname() :: [string()].

-spec make_module_name(bucket()) -> modulename().
make_module_name(Bucket) when is_binary(Bucket) ->
    Nonce = binary_to_list(base64:encode(crypto:hash(md4, Bucket))),
    Nonce2 = remove_hooky_chars(Nonce),
    ModName = "riak_kv_ddl_helper_mod_" ++ Nonce2,
    list_to_atom(ModName).

-spec get_partition_key(#ddl_v1{}, tuple()) -> binary().
get_partition_key(#ddl_v1{bucket = B, partition_key = PK}, Obj)
  when is_tuple(Obj) ->
    Mod = make_module_name(B),
    #partition_key_v1{ast = Params} = PK,
    Key = build(Params, Obj, Mod, []),
    _K = sext:encode(Key).

-spec get_local_key(#ddl_v1{}, tuple()) -> binary().
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

-spec is_valid_field(#ddl_v1{}, hierarchical_fieldname()) -> boolean().
is_valid_field(#ddl_v1{bucket = B}, Field) when is_list(Field)->
    Mod = riak_kv_ddl:make_module_name(B),
    Mod:is_field_valid(Field).

is_query_valid(#ddl_v1{bucket = B} = DDL,
	       #riak_kv_li_index_v1{bucket     = B,
				    selections = S,
				    filters    = F}) ->
    ValidSelection = are_selections_valid(DDL, S, ?CANTBEBLANK),
    ValidFilters = are_filters_valid(DDL, F),
    case {ValidSelection, ValidFilters} of
	{true, true} -> true;
	_            -> {false, [
				 {are_selections_valid, ValidSelection},
				 {are_filters_valid, ValidFilters}
				]}
    end;
is_query_valid(#ddl_v1{bucket = B1}, #riak_kv_li_index_v1{bucket = B2}) ->
    Msg = io_lib:format("DDL has a bucket of ~p "
			"but query has a bucket of ~p~n", [B1, B2]),
    {false, {ddl_mismatch, lists:flatten(Msg)}}.

are_filters_valid(#ddl_v1{} = DDL, Filters) ->
    Fields = extract_fields(Filters),
    are_selections_valid(DDL, Fields, ?CANBEBLANK).

are_selections_valid(#ddl_v1{}, [], ?CANTBEBLANK) ->
    {false, [{selections_cant_be_blank, []}]};
are_selections_valid(#ddl_v1{} = DDL, Selections, _) ->
    CheckFn = fun(X, {Acc, Status}) ->
		      case is_valid_field(DDL, X) of
			  true  -> {Acc, Status};
			  false -> Msg = {invalid_field, X},
				   {[Msg | Acc], false}
		      end
	      end,
    case lists:foldl(CheckFn, {[], true}, Selections) of
	{[],   true}  -> true;
	{Msgs, false} -> {false, Msgs}
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

%%
%% get partition_key tests
%%

simplest_partition_key_test() ->
    Name = "yando",
    PK = #partition_key_v1{ast = [
				  #param_v1{name = [Name]}
				 ]},
    DDL = make_ddl(<<"simplest_partition_key_test">>,
		   [
		    #riak_field_v1{name     = Name,
				   position = 1,
				   type     = binary}
		   ],
		   PK),
    {module, _Module} = riak_kv_ddl_compiler:make_helper_mod(DDL),
    Obj = {<<"yarble">>},
    Result = (catch get_partition_key(DDL, Obj)),
    ?assertEqual({<<"yarble">>}, sext:decode(Result)).

simple_partition_key_test() ->
    Name1 = "yando",
    Name2 = "buckle",
    PK = #partition_key_v1{ast = [
				  #param_v1{name = [Name1]},
				  #param_v1{name = [Name2]}
				 ]},
    DDL = make_ddl(<<"simple_partition_key_test">>,
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
    ?assertEqual({<<"three">>, <<"one">>}, sext:decode(Result)).

function_partition_key_test() ->
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
    DDL = make_ddl(<<"function_partition_key_test">>,
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
    ?assertEqual(Expected, sext:decode(Result)).

complex_partition_key_test() ->
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
    DDL = make_ddl(<<"complex_partition_key_test">>,
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
    ?assertEqual(Expected, sext:decode(Result)).

%%
%% get local_key tests
%%

%% local keys share the same code path as partition keys
%% so only need the lightest tests
simplest_local_key_test() ->
    Name = "yando",
    PK = #partition_key_v1{ast = [
				  #param_v1{name = [Name]}
				 ]},
    LK = #local_key_v1{ast = [
			      #param_v1{name = [Name]}
			     ]},
    DDL = make_ddl(<<"simplest_key_key_test">>,
		   [
		    #riak_field_v1{name     = Name,
				   position = 1,
				   type     = binary}
		   ],
		   PK, LK),
    {module, _Module} = riak_kv_ddl_compiler:make_helper_mod(DDL),
    Obj = {<<"yarble">>},
    Result = (catch get_partition_key(DDL, Obj)),
    ?assertEqual({<<"yarble">>}, sext:decode(Result)).

%%
%% get type of named field
%%

simplest_valid_get_type_test() ->
    DDL = make_ddl(<<"simplest_valid_get_type_test">>,
		   [
		    #riak_field_v1{name     = "yando",
				   position = 1,
				   type     = binary}
		   ]),
    {module, Module} = riak_kv_ddl_compiler:make_helper_mod(DDL),
    Result = (catch Module:get_field_type(["yando"])),
    ?assertEqual(binary, Result).

simple_valid_get_type_test() ->
    DDL = make_ddl(<<"simple_valid_get_type_test">>,
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
    ?assertEqual(binary, Result).

simple_valid_map_get_type_1_test() ->
    Map = {map, [
		 #riak_field_v1{name     = "yarple",
				position = 1,
				type     = integer}
		]},
    DDL = make_ddl(<<"simple_valid_map_get_type_1_test">>,
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
    ?assertEqual(integer, Result).

simple_valid_map_get_type_2_test() ->
    Map = {map, [
		 #riak_field_v1{name     = "yarple",
				position = 1,
				type     = integer}
		]},
    DDL = make_ddl(<<"simple_valid_map_get_type_2_test">>,
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
    ?assertEqual(map, Result).

complex_valid_map_get_type_test() ->
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
    DDL = make_ddl(<<"complex_valid_map_get_type_test">>,
		   [
		    #riak_field_v1{name     = "Top_Map",
				   position = 1,
				   type     = Map1}
		   ]),
    {module, Module} = riak_kv_ddl_compiler:make_helper_mod(DDL),
    Path = ["Top_Map", "Level_1_map1", "in_Map_1"],
    Res = (catch Module:get_field_type(Path)),
    ?assertEqual(integer, Res).

%%
%% Validate Query Tests
%%

partial_are_selections_valid_test() ->
    Selections  = [["temperature"], ["geohash"]],
    DDL = make_ddl(<<"partial_are_selections_valid_test">>,
		    [
		     #riak_field_v1{name     = "temperature",
				    position = 1,
				    type     = integer},
		     #riak_field_v1{name     = "geohash",
				    position = 2,
				    type     = integer}
		    ]),
    {module, _Module} = riak_kv_ddl_compiler:make_helper_mod(DDL),
    Res = are_selections_valid(DDL, Selections, ?CANTBEBLANK),
    ?assertEqual(true, Res).

partial_wildcard_are_selections_valid_test() ->
    Selections  = [["*"]],
    DDL = make_ddl(<<"partial_wildcard_are_selections_valid_test">>,
		    [
		     #riak_field_v1{name     = "temperature",
				    position = 1,
				    type     = integer},
		     #riak_field_v1{name     = "geohash",
				    position = 2,
				    type     = integer}
		    ]),
    {module, _Module} = riak_kv_ddl_compiler:make_helper_mod(DDL),
    Res = are_selections_valid(DDL, Selections, ?CANTBEBLANK),
    ?assertEqual(true, Res).

partial_are_selections_valid_fail_test() ->
    Selections  = [],
    DDL = make_ddl(<<"partial_are_selections_valid_fail_test">>,
		    [
		     #riak_field_v1{name     = "temperature",
				    position = 1,
				    type     = integer},
		     #riak_field_v1{name     = "geohash",
				    position = 2,
				    type     = integer}
		    ]),
    {Res, _} = are_selections_valid(DDL, Selections, ?CANTBEBLANK),
    ?assertEqual(false, Res).

simple_are_selections_valid_test() ->
    Bucket = <<"simple_are_selections_valid_test">>,
    Selections  = [["temperature"], ["geohash"]],
    Query = #riak_kv_li_index_v1{bucket      = Bucket,
				 selections  = Selections},
    DDL = make_ddl(Bucket,
		    [
		     #riak_field_v1{name     = "temperature",
				    position = 1,
				    type     = integer},
		     #riak_field_v1{name     = "geohash",
				    position = 2,
				    type     = integer}
		    ]),
    {module, _ModName} = riak_kv_ddl_compiler:make_helper_mod(DDL),
    Res = riak_kv_ddl:is_query_valid(DDL, Query),
    ?assertEqual(true, Res).

simple_validate_selections_fail_test() ->
    Bucket = <<"simple_validate_selections_fail_test">>,
    Selections  = [["temperature"], ["yerble"]],
    Query = #riak_kv_li_index_v1{bucket      = Bucket,
				 selections  = Selections},
    DDL = make_ddl(Bucket,
		    [
		     #riak_field_v1{name     = "temperature",
				    position = 1,
				    type     = integer},
		     #riak_field_v1{name     = "geohash",
				    position = 2,
				    type     = integer}
		    ]),
    {module, _ModName} = riak_kv_ddl_compiler:make_helper_mod(DDL),
    {Res, _} = riak_kv_ddl:is_query_valid(DDL, Query),
    ?assertEqual(false, Res).

simple_map_validate_selections_test() ->
    Bucket = <<"simple_map_validate_selections__test">>,
    Name0 = "name",
    Name1 = "temp",
    Name2 = "geo",
    Selections  = [["temp", "geo"], ["name"]],
    Query = #riak_kv_li_index_v1{bucket      = Bucket,
				 selections  = Selections},
    Map = {map, [
		 #riak_field_v1{name     = Name2,
				position = 1,
				type     = integer}
		 ]},
    DDL = make_ddl(Bucket,
		    [
		     #riak_field_v1{name     = Name0,
				    position = 1,
				    type     = integer},
		     #riak_field_v1{name     = Name1,
				    position = 2,
				    type     = Map}
		    ]),
    {module, _ModName} = riak_kv_ddl_compiler:make_helper_mod(DDL),
    Res = riak_kv_ddl:is_query_valid(DDL, Query),
    ?assertEqual(true, Res).

simple_map_wildcard_validate_selections_test() ->
    Bucket = <<"simple_map_wildcard_validate_selections__test">>,
    Name0 = "name",
    Name1 = "temp",
    Name2 = "geo",
    Selections  = [["temp", "*"], ["name"]],
    Query = #riak_kv_li_index_v1{bucket      = Bucket,
				 selections  = Selections},
    Map = {map, [
		 #riak_field_v1{name     = Name2,
				position = 1,
				type     = integer}
		 ]},
    DDL = make_ddl(Bucket,
		    [
		     #riak_field_v1{name     = Name0,
				    position = 1,
				    type     = integer},
		     #riak_field_v1{name     = Name1,
				    position = 2,
				    type     = Map}
		    ]),
    {module, _ModName} = riak_kv_ddl_compiler:make_helper_mod(DDL),
    Res = riak_kv_ddl:is_query_valid(DDL, Query),
    ?assertEqual(true, Res).

-endif.
