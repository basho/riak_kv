%% -------------------------------------------------------------------
%%
%% riak_kv_ddl_compiler: compile the record description in the DDL
%% into code to verify that data conforms to a schema at the boundary
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

%% TODO
%% * are we usings lists to describe fields or what?
%% * turning bucket names into verification modules is a potential
%%   memory leak on the atom table - not much we can do about that...
%% * do we care about the name of the validation module?

%% @doc
-module(riak_kv_ddl_compiler).

-include("riak_kv_index.hrl").
-include("riak_kv_ddl.hrl").

-export([
	 make_validation/1,
	 make_validation/2
	]).

-define(NODEBUGOUTPUT, false).
-define(DEBUGOUTPUT,   true).
-define(MAKEIGNORE,    true).
-define(MAKE,          false).

make_validation(#ddl{} = DDL) ->
    make_validation(DDL, ?NODEBUGOUTPUT, "/tmp").

make_validation(#ddl{} = DDL, OutputDir) ->
    make_validation(DDL, ?DEBUGOUTPUT, OutputDir).

make_validation(#ddl{} = DDL, HasDebugOutput, OutputDir) ->
    {Module, AST} = compile(DDL, HasDebugOutput, OutputDir),
    {ok, Module, Bin} = compile:forms(AST),
    SpoofBeam = "/tmp/" ++ atom_to_list(Module) ++ ".beam",
    {module, Module} = code:load_binary(Module, SpoofBeam, Bin).

compile(#ddl{} = DDL, HasDebugOutput, OutputDir) ->
    #ddl{bucket = Bucket,
	 fields = Fields} = DDL,
    {ModName, Attrs, N} = make_attrs(Bucket, 1),
    {Funs, N2} = recurse_over_fns([Fields], N, 1, [], []),
    AST = Attrs ++ Funs ++ [{eof, N2}],
    if HasDebugOutput ->
	    ASTFileName = filename:join([OutputDir, ModName]) ++ ".ast",
	    write_file(ASTFileName, io_lib:format("~p", [AST]));
       el/=se ->
	    ok
    end,
    case erl_lint:module(AST) of
 	{ok, []} ->
     	    if
     		HasDebugOutput ->
		    write_src(AST, DDL, ModName, OutputDir);
     		el/=se ->
		    ok
     	    end,
     	    _Module = {ModName, AST};
     	Other  -> exit(Other)
    end.

write_src(AST, DDL, ModName, OutputDir) ->
    Syntax = erl_syntax:form_list(AST),
    Header = io_lib:format("%%% Generated Module, do NOT edit~n~n" ++
			       "Validates the DDL~n~p~n",
			   [DDL]),
    Header2 = re:replace(Header, "\\n", "\\\n%%% ", [global, {return, list}]),
    Src = erl_prettypr:format(Syntax),
    Contents  = [Header2 ++ "\n" ++ Src],
    SrcFileName = filename:join([OutputDir, ModName]) ++ ".erl",
    write_file(SrcFileName, Contents).

write_file(FileName, Contents) ->
    ok = filelib:ensure_dir(FileName),
    {ok, Fd} = file:open(FileName, [write]),
    io:fwrite(Fd, "~s~n", [Contents]),
    file:close(Fd).

recurse_over_fns([], N, _, [], Acc2) ->
    {lists:flatten(lists:reverse(Acc2)), N};
recurse_over_fns([], N, M, Acc1, Acc2) ->
    recurse_over_fns(Acc1, N, M, [], Acc2);
recurse_over_fns([Fields | T], N, M, Acc1, Acc2) ->
    {Funs, N2, Maps, M2} = make_funs(Fields, N, M),
    Maps2 = {tuple, [Y || {_X, Y} <- Maps]},
    Acc1a = case Maps of
		[] -> Acc1;
		_  -> [Maps2 |  Acc1]
	    end,
    recurse_over_fns(T, N2, M2, Acc1a, [Funs | Acc2]).

make_attrs(Bucket, N) when is_binary(Bucket)    ->
    ModName = make_module_name(Bucket),
    {ModAttr, N1} = make_module_attr(ModName, N),
    {ExpAttr, N2} = make_export_attr(N1),
    {ModName, [ModAttr, ExpAttr], N2}.

make_funs(Fields, N, M) ->
    {Args, Fields2} = case Fields of
			  {tuple, List} ->
			      Fs = [make_names(X, N) || X <- List],
			      Fs2 = [make_tuple(X, N) || X <- Fs],
			      {make_tuple(Fs2, N), lists:flatten(List)};
			  _ ->
			      Fs = make_names(Fields, N),
			      {make_tuple(Fs, N), Fields}
		      end,
    {Guards, Maps, N2} = make_guards(Fields2, N + 1),
    Success = case Maps of
		  [] -> make_atom(true, N2);
		  _  -> make_map_call(Maps, N2, M + 1)
	      end,
    {ClauseS, _N3} = make_success_clause(Args, Guards, Success, N2),
    {ClauseF, _N4} = make_fail_clause(N2),
    FunName = get_fn_name(M),
    Fun = make_fun2(FunName, [ClauseS, ClauseF], N2),
    {[Fun], N2, Maps, M + 1}.

make_map_call(Fields, N, M) ->
    Tuple = make_tuple([make_var(X, N) || {{_, _, X}, _} <- Fields], N),
    Name = get_fn_name(M),
    Nm = make_atom(Name, N),
    make_call(Nm, Tuple, N).

make_guards(Fields, N) ->
    {_G1, _Maps, _N2} = make_g2(Fields, N, [], []).

make_g2([], N, [], Acc2) ->
    {[], Acc2, N};
make_g2([], N, Acc1, Acc2) ->
    {[[Acc1]], Acc2, N};
make_g2([#riak_field{type = any} | T], N, Acc1, Acc2) ->
    make_g2(T, N, Acc1, Acc2);
make_g2([#riak_field{name     = Name,
		     position = M,
		     type     = {map, L}} | T], N, Acc1, Acc2)
  when is_list(L) ->
    Nm = make_name(Name, ?MAKE, N, M),
    make_g2(T, N, Acc1, [{Nm, L} | Acc2]);
make_g2([#riak_field{name     = Name,
		     position = M,
		     type     = Type} | T], N, Acc1, Acc2)
  when Type =:= binary    orelse
       Type =:= integer   orelse
       Type =:= float     orelse
       Type =:= boolean   orelse
       Type =:= set       orelse
       Type =:= timestamp ->
    Nm = make_name(Name, ?MAKE, N, M),
    Call = make_guard(Nm, Type, N),
    Acc1a = case Acc1 of
		[] -> Call;
		_  -> make_op('andalso', Call, Acc1, N)
	    end,
    make_g2(T, N, Acc1a, Acc2).

make_op(Op, LHS, RHS, N) -> {op, N, Op, LHS, RHS}.

make_guard(Nm, binary,    N) -> {call, N, make_atom(is_binary,  N), [Nm]};
make_guard(Nm, integer,   N) -> {call, N, make_atom(is_integer, N), [Nm]};
make_guard(Nm, float,     N) -> {call, N, make_atom(is_float,   N), [Nm]};
make_guard(Nm, boolean,   N) -> {call, N, make_atom(is_boolean, N), [Nm]};
make_guard(Nm, set,       N) -> {call, N, make_atom(is_list,    N), [Nm]};
make_guard(Nm, timestamp, N) -> LHS = {call, N, make_atom(is_integer, N), [Nm]},
				RHS = make_op('>', Nm, make_integer(0, N), N),
				make_op('andalso', LHS, RHS, N).

make_integer(I, N) when is_integer(I) -> {integer, N, I}.

make_atom(A, N) -> {atom, N, A}.

make_call(FnName, Vars, N) when is_list(Vars) ->
    {call, N, FnName, Vars};
make_call(FnName, Var, N) ->
    {call, N, FnName, [Var]}.

make_names(Fields, N) ->
    Make_fn = fun(#riak_field{name     = Name,
			      position = M,
			      type     = Type}) ->
			      Make = case Type of
					 any -> ?MAKEIGNORE;
					 _   -> ?MAKE
				     end,
			      make_name(Name, Make, N, M)
		      end,
    [Make_fn(X) || X <- Fields].

make_name(Name, HasPrefix, N, M) ->
    Prefix = if
		 HasPrefix -> "_";
		 el/=se    -> ""
	     end,
    Nm = list_to_atom(Prefix ++ "Var" ++ integer_to_list(M) ++ "_" ++ Name),
    make_var(Nm, N).

make_var(Name, N) -> {var, N, Name}.

make_success_clause(Args, Guards, Success, N) ->
    Clause = make_clause(Args, Guards, Success, N),
    {Clause, N + 1}.

make_fail_clause(N) ->
    Var = make_var('_', N),
    False = make_atom(false, N),
    Clause = make_clause(Var, [], False, N),
    {Clause, N + 1}.

get_fn_name(1) ->
    validate;
get_fn_name(N) when is_integer(N) ->
    list_to_atom("validate" ++ integer_to_list(N)).

make_fun2(FunName, Clause, N) -> {function, N, FunName, 1, Clause}.

make_clause(Tuple, Guards, Body, N) -> {clause, N, [Tuple], Guards, [Body]}.

make_tuple(Fields, N) -> {tuple, N, Fields}.

make_module_name(Bucket) ->
    Nonce = binary_to_list(base64:encode(crypto:hash(md4, Bucket))),
    Nonce2 = remove_hooky_chars(Nonce),
    ModName = "riak_ddl_validation_" ++ Nonce2,
    list_to_atom(ModName).

remove_hooky_chars(Nonce) ->
    re:replace(Nonce, "[/|\+|\.|=]", "", [global, {return, list}]).

make_module_attr(ModName, N) ->
    {{attribute, N, module, ModName}, N + 1}.

make_export_attr(N) ->
    {{attribute, N, export, [{validate, 1}]}, N + 1}.

%%-ifdef(TEST).
-compile(export_all).

-define(VALID,   true).
-define(INVALID, false).

-include_lib("eunit/include/eunit.hrl").

make_ddl(Bucket, Fields) ->
    #ddl{bucket     = term_to_binary(Bucket),
	 fields     = Fields,
	 colocation = []}.

simplest_valid_test_() ->
    DDL = make_ddl(<<"simplest_valid_test">>,
		   [
		    #riak_field{name     = "yando",
				position = 1,
				type     = binary}
		   ]),
    {module, Module} = make_validation(DDL),
    Result = Module:validate({<<"ewrewr">>}),
    ?_assertEqual(?VALID, Result).

simple_valid_binary_test_() ->
    DDL = make_ddl(<<"simple_valid_binary_test">>,
		   [
		    #riak_field{name     = "yando",
				position = 1,
				type     = binary},
		    #riak_field{name     = "erko",
				position = 2,
				type     = binary}
		   ]),
    {module, Module} = make_validation(DDL),
    Result = Module:validate({<<"ewrewr">>, <<"werewr">>}),
    ?_assertEqual(?VALID, Result).

simple_valid_integer_test_() ->
    DDL = make_ddl("simple_valid_integer_test",
		   [
		    #riak_field{name     = "yando",
				position = 1,
				type     = integer},
		    #riak_field{name     = "erko",
				position = 2,
				type     = integer},
		    #riak_field{name     = "erkle",
				position = 3,
				type     = integer}
		   ]),
    {module, Module} = make_validation(DDL),
    Result = Module:validate({999, -9999, 0}),
    ?_assertEqual(?VALID, Result).

simple_valid_float_test_() ->
    DDL = make_ddl("simple_valid_float_test",
		   [
		    #riak_field{name     = "yando",
				position = 1,
				type     = float},
		    #riak_field{name     = "erko",
				position = 2,
				type     = float},
		    #riak_field{name     = "erkle",
				position = 3,
				type     = float}
		   ]),
    {module, Module} = make_validation(DDL),
    Result = Module:validate({432.22, -23423.22, -0.0}),
    ?_assertEqual(?VALID, Result).

simple_valid_boolean_test_() ->
    DDL = make_ddl("simple_valid_boolean_test",
		   [
		    #riak_field{name     = "yando",
				position = 1,
				type     = boolean},
		    #riak_field{name     = "erko",
				position = 2,
				type     = boolean}
		   ]),
    {module, Module} = make_validation(DDL),
    Result = Module:validate({true, false}),
    ?_assertEqual(?VALID, Result).

simple_valid_timestamp_test_() ->
    DDL = make_ddl("simple_valid_timestamp_test",
		   [
		    #riak_field{name     = "yando",
				position = 1,
				type     = timestamp},
		    #riak_field{name     = "erko",
				position = 2,
				type     = timestamp},
		    #riak_field{name     = "erkle",
				position = 3,
				type     = timestamp}
		   ]),
    {module, Module} = make_validation(DDL),
    Result = Module:validate({234324, 23424, 34636}),
    ?_assertEqual(?VALID, Result).

simple_valid_map_1_test() ->
    Map = {map, [
		 #riak_field{name     = "yarple",
			     position = 1,
			     type     = binary}
		]},
    DDL = make_ddl("simple_valid_map_1_test",
		   [
		    #riak_field{name     = "yando",
				position = 1,
				type     = binary},
		    #riak_field{name     = "erko",
				position = 2,
				type     = Map},
		    #riak_field{name     = "erkle",
				position = 3,
				type     = float}
		   ]),
    {module, Module} = make_validation(DDL),
    Result = Module:validate({<<"ewrewr">>, {<<"erko">>}, 4.4}),
    ?_assertEqual(?VALID, Result).

simple_valid_map_2_test_() ->
    Map = {map, [
		 #riak_field{name     = "yarple",
			     position = 1,
			     type     = binary},
		 #riak_field{name     = "yoplait",
			     position = 2,
			     type     = integer}
		]},
    DDL = make_ddl("simple_valid_map_2_test",
		   [
		    #riak_field{name     = "yando",
				position = 1,
				type     = binary},
		    #riak_field{name     = "erko",
				position = 2,
				type     = Map},
		    #riak_field{name     = "erkle",
				position = 3,
				type     = float}
		   ]),
    {module, Module} = make_validation(DDL),
    Result = Module:validate({<<"ewrewr">>, {<<"erko">>, -999}, 4.4}),
    ?_assertEqual(?VALID, Result).

complex_valid_map_1_test_() ->
    Map2 = {map, [
		  #riak_field{name     = "dingle",
			      position = 1,
			      type     = integer},
		  #riak_field{name     = "zoomer",
			      position = 2,
			      type     = integer}
		 ]},
    Map1 = {map, [
		  #riak_field{name     = "yarple",
			      position = 1,
			      type     = integer},
		  #riak_field{name     = "yoplait",
			      position = 2,
			      type     = Map2},
		  #riak_field{name     = "yowl",
			      position = 3,
			      type     = integer}
		 ]},
    DDL = make_ddl("complex_valid_map_1_test",
		   [
		    #riak_field{name     = "yando",
				position = 1,
				type     = integer},
		    #riak_field{name     = "erko",
				position = 2,
				type     = Map1},
		    #riak_field{name     = "erkle",
				position = 3,
				type     = integer}
		   ]),
    {module, Module} = make_validation(DDL),
    Result = Module:validate({1, {1, {3, 4}, 1}, 1}),
    ?_assertEqual(?VALID, Result).

complex_valid_map_2_test_() ->
    Map3 = {map, [
		  #riak_field{name     = "yik",
			      position = 1,
			      type     = integer},
		  #riak_field{name     = "banjo",
			      position = 2,
			      type     = integer}
		 ]},
    Map2 = {map, [
		  #riak_field{name     = "dingle",
			      position = 1,
			      type     = integer},
		  #riak_field{name     = "zoomer",
			      position = 2,
			      type     = integer},
		  #riak_field{name     = "dougal",
			      position = 2,
			      type     = Map3}
		 ]},
    Map1 = {map, [
		  #riak_field{name     = "yarple",
			      position = 1,
			      type     = integer},
		  #riak_field{name     = "yoplait",
			      position = 2,
			      type     = Map2},
		  #riak_field{name     = "yowl",
			      position = 3,
			      type     = integer}
		 ]},
    DDL = make_ddl("complex_valid_map_2_test",
		   [
		    #riak_field{name     = "yando",
				position = 1,
				type     = integer},
		    #riak_field{name     = "erko",
				position = 2,
				type     = Map1},
		    #riak_field{name     = "erkle",
				position = 3,
				type     = integer}
		   ]),
    {module, Module} = make_validation(DDL),
    Result = Module:validate({1, {1, {1, 1, {1, 1}}, 1}, 1}),
    ?_assertEqual(?VALID, Result).

complex_valid_map_3_test_() ->
    Map3 = {map, [
		  #riak_field{name     = "in_Map_2",
			      position = 1,
			      type     = integer}
		 ]},
    Map2 = {map, [
		  #riak_field{name     = "in_Map_1",
			      position = 1,
			      type     = integer}
		 ]},
    Map1 = {map, [
		  #riak_field{name     = "Level_1_1",
			      position = 1,
			      type     = integer},
		  #riak_field{name     = "Level_1_map1",
			      position = 2,
			      type     = Map2},
		  #riak_field{name     = "Level_1_map2",
			      position = 3,
			      type     = Map3}
		 ]},
    DDL = make_ddl("complex_valid_map_3_test",
		   [
		    #riak_field{name     = "Top_Map",
				position = 1,
				type     = Map1}
		   ]),
    {module, Module} = make_validation(DDL),
    Result = Module:validate({{2, {3}, {4}}}),
    ?_assertEqual(?VALID, Result).

simple_valid_any_test_() ->
    DDL = make_ddl("simple_valid_any_test",
		   [
		    #riak_field{name     = "yando",
				position = 1,
				type     = binary},
		    #riak_field{name     = "erko",
				position = 2,
				type     = any},
		    #riak_field{name     = "erkle",
				position = 3,
				type     = float}
		   ]),
    {module, Module} = make_validation(DDL),
    Result = Module:validate({<<"ewrewr">>, [a, b, d], 4.4}),
    ?_assertEqual(?VALID, Result).

simple_valid_set_test_() ->
    DDL = make_ddl("simple_valid_any_test",
		   [
		    #riak_field{name     = "yando",
				position = 1,
				type     = binary},
		    #riak_field{name     = "erko",
				position = 2,
				type     = set},
		    #riak_field{name     = "erkle",
				position = 3,
				type     = float}
		   ]),
    {module, Module} = make_validation(DDL),
    Result = Module:validate({<<"ewrewr">>, [a, b, d], 4.4}),
    ?_assertEqual(?VALID, Result).

simple_valid_mixed_test_() ->
    DDL = make_ddl("simple_valid_mixed_test",
		   [
		    #riak_field{name     = "yando",
				position = 1,
				type     = binary},
		    #riak_field{name     = "erko",
				position = 2,
				type     = integer},
		    #riak_field{name     = "erkle",
				position = 3,
				type     = float},
		    #riak_field{name     = "banjo",
				position = 4,
				type     = timestamp}
		   ]),
    {module, Module} = make_validation(DDL),
    Result = Module:validate({<<"ewrewr">>, 99, 4.4, 5555}),
    ?_assertEqual(?VALID, Result).

%% invalid tests
simple_invalid_binary_test_() ->
    DDL = make_ddl("simple_invalid_binary_test",
		   [
		    #riak_field{name     = "yando",
				position = 1,
				type     = binary},
		    #riak_field{name     = "erko",
				position = 2,
				type     = binary}
		   ]),
    {module, Module} = make_validation(DDL),
    Result = Module:validate({<<"ewrewr">>, 55}),
    ?_assertEqual(?INVALID, Result).

simple_invalid_integer_test_() ->
    DDL = make_ddl("simple_invalid_integer_test",
		   [
		    #riak_field{name     = "yando",
				position = 1,
				type     = integer},
		    #riak_field{name     = "erko",
				position = 2,
				type     = integer},
		    #riak_field{name     = "erkle",
				position = 3,
				type     = integer}
		   ]),
    {module, Module} = make_validation(DDL),
    Result = Module:validate({999, -9999, 0,0}),
    ?_assertEqual(?INVALID, Result).

simple_invalid_float_test_() ->
    DDL = make_ddl("simple_invalid_float_test",
		   [
		    #riak_field{name     = "yando",
				position = 1,
				type     = float},
		    #riak_field{name     = "erko",
				position = 2,
				type     = float},
		    #riak_field{name     = "erkle",
				position = 3,
				type     = float}
		   ]),
    {module, Module} = make_validation(DDL),
    Result = Module:validate({432.22, -23423.22, [a, b, d]}),
    ?_assertEqual(?INVALID, Result).

simple_invalid_boolean_test_() ->
    DDL = make_ddl("simple_invalid_boolean_test",
		   [
		    #riak_field{name     = "yando",
				position = 1,
				type     = boolean},
		    #riak_field{name     = "erko",
				position = 2,
				type     = boolean},
		    #riak_field{name     = "erkle",
				position = 3,
				type     = boolean}
		   ]),
    {module, Module} = make_validation(DDL),
    Result = Module:validate({true, false, [a, b, d]}),
    ?_assertEqual(?INVALID, Result).

simple_invalid_set_test_() ->
    DDL = make_ddl("simple_invalid_set_test",
		   [
		    #riak_field{name     = "yando",
				position = 1,
				type     = binary},
		    #riak_field{name     = "erko",
				position = 2,
				type     = set},
		    #riak_field{name     = "erkle",
				position = 3,
				type     = float}
		   ]),
    {module, Module} = make_validation(DDL),
    Result = Module:validate({<<"ewrewr">>, [444.44], 4.4}),
    ?_assertEqual(?VALID, Result).

simple_invalid_timestamp_1_test_() ->
    DDL = make_ddl("simple_invalid_timestamp_1_test",
		   [
		    #riak_field{name     = "yando",
				position = 1,
				type     = timestamp},
		    #riak_field{name     = "erko",
				position = 2,
				type     = timestamp},
		    #riak_field{name     = "erkle",
				position = 3,
				type     = timestamp}
		   ]),
    {module, Module} = make_validation(DDL),
    Result = Module:validate({234.324, 23424, 34636}),
    ?_assertEqual(?INVALID, Result).

simple_invalid_timestamp_2_test_() ->
    DDL = make_ddl("simple_invalid_timestamp_2_test",
		   [
		    #riak_field{name     = "yando",
				position = 1,
				type     = timestamp},
		    #riak_field{name     = "erko",
				position = 2,
				type     = timestamp},
		    #riak_field{name     = "erkle",
				position = 3,
				type     = timestamp}
		   ]),
    {module, Module} = make_validation(DDL),
    Result = Module:validate({234324, -23424, 34636}),
    ?_assertEqual(?INVALID, Result).

simple_invalid_timestamp_3_test_() ->
    DDL = make_ddl("simple_invalid_timestamp_3_test",
		   [
		    #riak_field{name     = "yando",
				position = 1,
				type     = timestamp},
		    #riak_field{name     = "erko",
				position = 2,
				type     = timestamp},
		    #riak_field{name     = "erkle",
				position = 3,
				type     = timestamp}
		   ]),
    {module, Module} = make_validation(DDL),
    Result = Module:validate({234324, 0, 34636}),
    ?_assertEqual(?INVALID, Result).

simple_invalid_map_1_test() ->
    Map = {map, [
		 #riak_field{name     = "yarple",
			     position = 1,
			     type     = binary}
		]},
    DDL = make_ddl("simple_invalid_map_1_test",
		   [
		    #riak_field{name     = "yando",
				position = 1,
				type     = binary},
		    #riak_field{name     = "erko",
				position = 2,
				type     = Map},
		    #riak_field{name     = "erkle",
				position = 3,
				type     = float}
		   ]),
    {module, Module} = make_validation(DDL),
    Result = Module:validate({<<"ewrewr">>, {99}, 4.4}),
    ?_assertEqual(?VALID, Result).

simple_invalid_map_2_test() ->
    Map = {map, [
		 #riak_field{name     = "yarple",
			     position = 1,
			     type     = binary},
		 #riak_field{name     = "yip",
			     position = 2,
			     type     = binary}
		]},
    DDL = make_ddl("simple_invalid_map_2_test",
		   [
		    #riak_field{name     = "yando",
				position = 1,
				type     = binary},
		    #riak_field{name     = "erko",
				position = 2,
				type     = Map},
		    #riak_field{name     = "erkle",
				position = 3,
				type     = float}
		   ]),
    {module, Module} = make_validation(DDL),
    Result = Module:validate({<<"ewrewr">>, {<<"erer">>}, 4.4}),
    ?_assertEqual(?VALID, Result).

simple_invalid_map_3_test() ->
    Map = {map, [
		 #riak_field{name     = "yarple",
			     position = 1,
			     type     = binary},
		 #riak_field{name     = "yip",
			     position = 2,
			     type     = binary}
		]},
    DDL = make_ddl("simple_invalid_map_3_test",
		   [
		    #riak_field{name     = "yando",
				position = 1,
				type     = binary},
		    #riak_field{name     = "erko",
				position = 2,
				type     = Map},
		    #riak_field{name     = "erkle",
				position = 3,
				type     = float}
		   ]),
    {module, Module} = make_validation(DDL),
    Result = Module:validate({<<"ewrewr">>, {<<"bingo">>, <<"bango">>, <<"erk">>}, 4.4}),
    ?_assertEqual(?VALID, Result).

simple_invalid_map_4_test() ->
    Map = {map, [
		 #riak_field{name     = "yarple",
			     position = 1,
			     type     = binary}
		]},
    DDL = make_ddl("simple_invalid_map_4_test",
		   [
		    #riak_field{name     = "yando",
				position = 1,
				type     = binary},
		    #riak_field{name     = "erko",
				position = 2,
				type     = Map},
		    #riak_field{name     = "erkle",
				position = 3,
				type     = float}
		   ]),
    {module, Module} = make_validation(DDL),
    Result = Module:validate({<<"ewrewr">>, [99], 4.4}),
    ?_assertEqual(?VALID, Result).

complex_invalid_map_1_test() ->
    Map2 = {map, [
		  #riak_field{name     = "dingle",
			      position = 1,
			      type     = binary},
		  #riak_field{name     = "zoomer",
			      position = 2,
			      type     = integer}
		 ]},
    Map1 = {map, [
		  #riak_field{name     = "yarple",
			      position = 1,
			      type     = binary},
		  #riak_field{name     = "yoplait",
			      position = 2,
			      type     = Map2},
		  #riak_field{name     = "yowl",
			      position = 3,
			      type     = integer}
		 ]},
    DDL = make_ddl("complex_invalid_map_1_test",
		   [
		    #riak_field{name     = "yando",
				position = 1,
				type     = binary},
		    #riak_field{name     = "erko",
				position = 2,
				type     = Map1},
		    #riak_field{name     = "erkle",
				position = 3,
				type     = float}
		   ]),
    {module, Module} = make_validation(DDL),
    Result = Module:validate({<<"ewrewr">>,
			      {<<"erko">>,
			       {<<"yerk">>, 33.0}
			      },
			      4.4}),
    ?_assertEqual(?VALID, Result).

%%% test the size of the tuples
too_small_tuple_test_() ->
    DDL = make_ddl("simple_too_small_tuple_test",
		   [
		    #riak_field{name     = "yando",
				position = 1,
				type     = float},
		    #riak_field{name     = "erko",
				position = 2,
				type     = float},
		    #riak_field{name     = "erkle",
				position = 3,
				type     = float}
		   ]),
    {module, Module} = make_validation(DDL),
    Result = Module:validate({432.22, -23423.22}),
    ?_assertEqual(?INVALID, Result).

too_big_tuple_test_() ->
    DDL = make_ddl("simple_too_big_tuple_test",
		   [
		    #riak_field{name     = "yando",
				position = 1,
				type     = float},
		    #riak_field{name     = "erko",
				position = 2,
				type     = float},
		    #riak_field{name     = "erkle",
				position = 3,
				type     = float}
		   ]),
    {module, Module} = make_validation(DDL),
    Result = Module:validate({432.22, -23423.22, 44.44, 65.43}),
    ?_assertEqual(?INVALID, Result).

%%-endif.
