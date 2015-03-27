%% -------------------------------------------------------------------
%%
%% riak_kv_ddl_compiler: processes the Data Description in the DDL
%% * verifies that the DDL is valid
%% * compiles the record description in the DDL into a module that can
%%   be used to verify that incoming data conforms to a schema at the boundary
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

-type expr()   :: tuple().
-type exprs()  :: [expr()].
-type guards() :: [exprs()].
-type ast()    :: [expr() | exprs() | guards()].
%% maps() are an aggregration of [map()]
-type maps()   :: {maps, [[#riak_field{}]]}.
-type map()    :: {map,  [#riak_field{}]}.

-spec make_validation(#ddl{}) -> {module, atom()} | {'error', tuple()}.
make_validation(#ddl{} = DDL) ->
    make_validation(DDL, "/tmp", ?NODEBUGOUTPUT).

-spec make_validation(#ddl{}, string()) ->
			     {module, atom()} | {'error', tuple()}.
make_validation(#ddl{} = DDL, OutputDir) ->
    make_validation(DDL, OutputDir, ?DEBUGOUTPUT).

-spec make_validation(#ddl{}, string(), boolean()) ->
			     {module, atom()} | {'error', tuple()}.
make_validation(#ddl{} = DDL, OutputDir, HasDebugOutput) ->
    case validate(DDL) of
	true ->
	    {Module, AST} = compile(DDL, OutputDir, HasDebugOutput),
	    {ok, Module, Bin} = compile:forms(AST),
	    SpoofBeam = "/tmp/" ++ atom_to_list(Module) ++ ".beam",
	    {module, Module} = code:load_binary(Module, SpoofBeam, Bin);
	{false, Errs} ->
	    {error, {invalid_ddl, Errs}}
    end.

validate(#ddl{fields = Fields}) ->
    Ret1 = validate_fields(Fields, []),
    Ret2 = validate_positions(Fields),
    case lists:merge(Ret1, Ret2) of
	[]   -> true;
	Errs -> {false, Errs}
    end.

validate_positions(Fields) ->
    SortFn = fun(#riak_field{position = A}, #riak_field{position = B}) ->
		     if
			 A =< B -> true;
			 A >  B -> false
		     end
	     end,
    Fs2 = lists:sort(SortFn, Fields),
    validate_pos2(Fs2, 1, []).

validate_pos2([], _NPos, Acc) ->
    lists:flatten(lists:reverse(Acc));
validate_pos2([#riak_field{type     = {map, MapFields},
			   position = NPos} | T], NPos, Acc) ->
    NewAcc = case validate_positions(MapFields) of
		 [] -> Acc;
		 A  -> [A | Acc]
	     end,
    validate_pos2(T, NPos + 1, NewAcc);
validate_pos2([#riak_field{position = NPos} | T], NPos, Acc) ->
    validate_pos2(T, NPos + 1, Acc);
validate_pos2([H | T], NPos, Acc) ->
    validate_pos2(T, NPos + 1, [{wrong_position, {expected, NPos}, H} | Acc]).

validate_fields([], []) ->
    [];
validate_fields([], Acc) ->
    lists:flatten(lists:reverse(Acc));
validate_fields([#riak_field{name     = N,
			     position = P,
			     type     = Ty,
			     optional = O} | T], Acc)
  when is_list(N),             % guard 1
       is_integer(P)           % guard 2
       andalso P > 0,
       Ty == binary            % guard 3
       orelse Ty =:= integer
       orelse Ty =:= float
       orelse Ty =:= boolean
       orelse Ty =:= set
       orelse Ty =:= timestamp
       orelse Ty =:= any,
       is_boolean(O) ->        % guard 4
    validate_fields(T, Acc);
validate_fields([#riak_field{name     = N,
			     position = P,
			     type     = {map, MapFields},
			     optional = false} | T], Acc)
  when is_list(N),   % guard 1
       is_integer(P) % guard 2
       andalso P > 0
       ->
    NewAcc = case validate_fields(MapFields, []) of
		 [] -> Acc;
		 A  -> [A | Acc]
	     end,
    validate_fields(T, NewAcc);
validate_fields([#riak_field{name     = N,
			     position = P,
			     type     = {map, _MapFields},
			     optional = true} = H | T], Acc)
  when is_list(N),   % guard 1
       is_integer(P) % guard 2
       andalso P > 0
       ->
    validate_fields(T, [{maps_cant_be_optional, H} | Acc]);
validate_fields([H | T], Acc) ->
    validate_fields(T, [{invalid_field, H} | Acc]).

-spec compile(#ddl{}, string(), boolean()) ->
		     {module, ast()} | {'error', tuple()}.
compile(#ddl{} = DDL, OutputDir, HasDebugOutput) ->
    #ddl{bucket = Bucket,
	 fields = Fields} = DDL,
    {ModName, Attrs, N} = make_attrs(Bucket, 1),
    {Funs, N2} = build_validation_fns([Fields], N, 1, [], []),
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
			       "Validates the DDL~n~n" ++
			       "Bucket     : ~s~n" ++
			       "Fields     : ~p~n" ++
			       "Colocation : ~p~n~n",
			   [
			    binary_to_list(DDL#ddl.bucket),
			    DDL#ddl.fields,
			    DDL#ddl.colocation
			   ]),
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

%%% this is the messiest function because the first validation clause has
%%% a simple tuple as an arguement, but the subsequent ones (that validate
%% the data in embedded maps) take a tuple of tuples
-spec build_validation_fns([[#riak_field{}] | [maps()]],
			   pos_integer(), pos_integer(),
			   [maps()], [ast()]) ->
				  {[ast()], pos_integer()}.
build_validation_fns([], LineNo, _, [], Acc2) ->
    {lists:flatten(lists:reverse(Acc2)), LineNo};
build_validation_fns([], LineNo, FunNo, Acc1, Acc2) ->
    build_validation_fns(Acc1, LineNo, FunNo, [], Acc2);
build_validation_fns([Fields | T], LineNo, FunNo, Acc1, Acc2) ->
    {Funs, LineNo2, Maps, FunNo2} = make_funs(Fields, LineNo, FunNo),
    Maps2 = {maps, [Y || {_X, Y} <- Maps]},
    Acc1a = case Maps of
		[] -> Acc1;
		_  -> [Maps2 |  Acc1]
	    end,
    build_validation_fns(T, LineNo2, FunNo2, Acc1a, [Funs | Acc2]).

-spec make_attrs(binary(), pos_integer()) -> {atom(), ast(), pos_integer()}.
make_attrs(Bucket, LineNo) when is_binary(Bucket)    ->
    ModName = make_module_name(Bucket),
    {ModAttr, LineNo1} = make_module_attr(ModName, LineNo),
    {ExpAttr, LineNo2} = make_export_attr(LineNo1),
    {ModName, [ModAttr, ExpAttr], LineNo2}.

-spec make_funs([#riak_field{} | maps()], pos_integer(), pos_integer()) ->
		       {ast(), pos_integer(), [map()], pos_integer()}.
make_funs(Fields, LineNo, FunNo) ->
    {Args, Fields2} = case Fields of
			  {maps, MapFields} ->
			      Fs  = [make_names(X, LineNo) || X <- MapFields],
			      Fs2 = [make_tuple(X, LineNo) || X <- Fs],
			      FlatFields = lists:flatten(MapFields),
			      {make_tuple(Fs2, LineNo), FlatFields};
			  _ ->
			      Fs = make_names(Fields, LineNo),
			      {make_tuple(Fs, LineNo), Fields}
		      end,
    {Guards, Maps, LineNo2} = make_guards(Fields2, LineNo + 1),
    Success = case Maps of
		  [] -> make_atom(true, LineNo2);
		  _  -> make_map_call(Maps, LineNo2, FunNo + 1)
	      end,
    {ClauseS, _LineNo3} = make_success_clause(Args, Guards, Success, LineNo2),
    {ClauseF, _LineNo4} = make_fail_clause(LineNo2),
    FunName = get_fn_name(FunNo),
    Fun = make_fun2(FunName, [ClauseS, ClauseF], LineNo2),
    {[Fun], LineNo2, Maps, FunNo + 1}.

make_fun2(FunName, Clause, LineNo) -> {function, LineNo, FunName, 1, Clause}.

-spec make_map_call([{expr(), [#riak_field{}]}], pos_integer(), pos_integer()) ->
			   expr().
make_map_call(Fields, LineNo, FunNo) ->
    Tuple = make_tuple([make_var(X, LineNo) || {{_, _, X}, _} <- Fields], LineNo),
    Name = get_fn_name(FunNo),
    Nm = make_atom(Name, LineNo),
    _Expr = make_call(Nm, Tuple, LineNo).

-spec make_guards([#riak_field{}], pos_integer()) ->
			 {ast(), [{expr(), [#riak_field{}]}], pos_integer()}.
make_guards(Fields, LineNo) ->
    {_G1, _Maps, _LineNo2} = make_gds2(Fields, LineNo, [], []).

make_gds2([], LineNo, [], Acc2) ->
    {[], Acc2, LineNo};
make_gds2([], LineNo, Acc1, Acc2) ->
    {[Acc1], Acc2, LineNo};
make_gds2([#riak_field{type = any} | T], LineNo, Acc1, Acc2) ->
    make_gds2(T, LineNo, Acc1, Acc2);
make_gds2([#riak_field{name     = Name,
		       position = NPos,
		       type     = {map, MapFields},
		       optional = false} | T], LineNo, Acc1, Acc2)
  when is_list(MapFields) ->
    Nm = make_name(Name, ?MAKE, LineNo, NPos),
    make_gds2(T, LineNo, Acc1, [{Nm, MapFields} | Acc2]);
make_gds2([#riak_field{name     = Name,
		       position = NPos,
		       type     = Type,
		       optional = IsOp} | T], LineNo, Acc1, Acc2)
  when Type =:= binary    orelse
       Type =:= integer   orelse
       Type =:= float     orelse
       Type =:= boolean   orelse
       Type =:= set       orelse
       Type =:= timestamp ->
    Nm = make_name(Name, ?MAKE, LineNo, NPos),
    Call = case IsOp of
	       false -> make_guard(Nm, Type, LineNo);
	       true  -> LHS = make_op('=:=', Nm, {nil, LineNo}, LineNo),
			RHS = make_guard(Nm, Type, LineNo),
			make_op('orelse', LHS, RHS, LineNo)
	   end,
    Acc1a = case Acc1 of
		[] -> [Call];
		_  -> [Call | Acc1]
	    end,
    make_gds2(T, LineNo, Acc1a, Acc2).

-spec make_op(atom(), expr(), expr(), pos_integer()) -> expr().
make_op(Op, LHS, RHS, LineNo) -> {op, LineNo, Op, LHS, RHS}.

-spec make_guard(expr(), simple_field_type(), pos_integer()) -> expr().
make_guard(Nm, binary,    LNo) -> {call, LNo, make_atom(is_binary,  LNo), [Nm]};
make_guard(Nm, integer,   LNo) -> {call, LNo, make_atom(is_integer, LNo), [Nm]};
make_guard(Nm, float,     LNo) -> {call, LNo, make_atom(is_float,   LNo), [Nm]};
make_guard(Nm, boolean,   LNo) -> {call, LNo, make_atom(is_boolean, LNo), [Nm]};
make_guard(Nm, set,       LNo) -> {call, LNo, make_atom(is_list,    LNo), [Nm]};
make_guard(Nm, timestamp, LNo) ->
    LHS = {call, LNo, make_atom(is_integer, LNo), [Nm]},
    RHS = make_op('>', Nm, make_integer(0, LNo), LNo),
    make_op('andalso', LHS, RHS, LNo).

-spec make_integer(integer(), pos_integer()) -> expr().
make_integer(I, LineNo) when is_integer(I) -> {integer, LineNo, I}.

-spec make_atom(atom(), pos_integer()) -> expr().
make_atom(A, LineNo) when is_atom(A) -> {atom, LineNo, A}.

-spec make_call(expr(), expr(), pos_integer()) -> expr().
make_call(FnName, Var, LineNo) ->
    {call, LineNo, FnName, [Var]}.

-spec make_names([#riak_field{}], pos_integer()) -> [expr()].
make_names(Fields, LineNo) ->
    Make_fn = fun(#riak_field{name     = Name,
			      position = NPos,
			      type     = Type}) ->
			      Make = case Type of
					 any -> ?MAKEIGNORE;
					 _   -> ?MAKE
				     end,
			      make_name(Name, Make, LineNo, NPos)
		      end,
    [Make_fn(X) || X <- Fields].

-spec make_name(string(), boolean(), pos_integer(), pos_integer()) -> expr().
make_name(Name, HasPrefix, LineNo, NPos) ->
    Prefix = if
		 HasPrefix -> "_";
		 el/=se    -> ""
	     end,
    Nm = list_to_atom(Prefix ++ "Var" ++ integer_to_list(NPos) ++ "_" ++ Name),
    make_var(Nm, LineNo).

-spec make_var(atom(), pos_integer()) -> expr().
make_var(Name, LineNo) -> {var, LineNo, Name}.

-spec make_success_clause(tuple(), guards(), expr(), pos_integer()) -> {expr(), pos_integer()}.
make_success_clause(Tuple, Guards, Body, LineNo) ->
    Clause = {clause, LineNo, [Tuple], Guards, [Body]},
    {Clause, LineNo + 1}.

-spec make_fail_clause(pos_integer()) -> expr().
make_fail_clause(LineNo) ->
    Var = make_var('_', LineNo),
    False = make_atom(false, LineNo),
    Clause = make_clause(Var, [], False, LineNo),
    {Clause, LineNo + 1}.

-spec get_fn_name(pos_integer()) -> atom().
get_fn_name(1) ->
    validate;
get_fn_name(FunNo) when is_integer(FunNo) ->
    list_to_atom("validate" ++ integer_to_list(FunNo)).

-spec make_clause(tuple(), guards(), expr(), pos_integer()) -> expr().
make_clause(Tuple, Guards, Body, LineNo) ->
    {clause, LineNo, [Tuple], Guards, [Body]}.

make_tuple(Fields, LineNo) ->
    {tuple, LineNo, Fields}.

make_module_name(Bucket) ->
    Nonce = binary_to_list(base64:encode(crypto:hash(md4, Bucket))),
    Nonce2 = remove_hooky_chars(Nonce),
    ModName = "riak_ddl_validation_" ++ Nonce2,
    list_to_atom(ModName).

remove_hooky_chars(Nonce) ->
    re:replace(Nonce, "[/|\+|\.|=]", "", [global, {return, list}]).

make_module_attr(ModName, LineNo) ->
    {{attribute, LineNo, module, ModName}, LineNo + 1}.

make_export_attr(LineNo) ->
    {{attribute, LineNo, export, [{validate, 1}]}, LineNo + 1}.

-ifdef(TEST).
-compile(export_all).

-define(VALID,   true).
-define(INVALID, false).

-include_lib("eunit/include/eunit.hrl").

make_ddl(Bucket, Fields) when is_binary(Bucket) ->
    #ddl{bucket     = Bucket,
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
    DDL = make_ddl(<<"simple_valid_integer_test">>,
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
    DDL = make_ddl(<<"simple_valid_float_test">>,
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
    DDL = make_ddl(<<"simple_valid_boolean_test">>,
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
    DDL = make_ddl(<<"simple_valid_timestamp_test">>,
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

simple_valid_map_1_test_() ->
    Map = {map, [
		 #riak_field{name     = "yarple",
			     position = 1,
			     type     = binary}
		]},
    DDL = make_ddl(<<"simple_valid_map_1_test">>,
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
    DDL = make_ddl(<<"simple_valid_map_2_test">>,
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

simple_valid_optional_1_test_() ->
    DDL = make_ddl(<<"simple_valid_optional_1_test">>,
		   [
		    #riak_field{name     = "yando",
				position = 1,
				type     = binary,
				optional = true}
		   ]),
    {module, Module} = make_validation(DDL),
    Result = Module:validate({[]}),
    ?_assertEqual(?VALID, Result).

simple_valid_optional_2_test_() ->
    DDL = make_ddl(<<"simple_valid_optional_2_test">>,
		   [
		    #riak_field{name     = "yando",
				position = 1,
				type     = binary,
				optional = true},
		    #riak_field{name     = "erko",
				position = 2,
				type     = integer,
				optional = true},
		    #riak_field{name     = "erkle",
				position = 3,
				type     = float,
				optional = true},
		    #riak_field{name     = "eejit",
				position = 4,
				type     = boolean,
				optional = true},
		    #riak_field{name     = "ergot",
				position = 5,
				type     = boolean,
				optional = true},
		    #riak_field{name     = "epithelion",
				position = 6,
				type     = set,
				optional = true},
		    #riak_field{name     = "endofdays",
		     		position = 7,
		     		type     = timestamp,
		     		optional = true}
		   ]),
    {module, Module} = make_validation(DDL),
    Result = Module:validate({[], [], [], [], [], [], []}),
    ?_assertEqual(?VALID, Result).

simple_valid_optional_3_test_() ->
    DDL = make_ddl(<<"simple_valid_optional_3_test">>,
		   [
		    #riak_field{name     = "yando",
				position = 1,
				type     = binary,
				optional = true},
		    #riak_field{name     = "yerk",
				position = 2,
				type     = binary,
				optional = false}
		   ]),
    {module, Module} = make_validation(DDL),
    Result = Module:validate({[], <<"edible">>}),
    ?_assertEqual(?VALID, Result).

complex_valid_optional_1_test_() ->
    Map = {map, [
		    #riak_field{name     = "yando",
				position = 1,
				type     = integer,
				optional = true},
		    #riak_field{name     = "yando",
				position = 2,
				type     = binary,
				optional = true}
		 ]},
    DDL = make_ddl(<<"complex_valid_optional_1_test">>,
		   [
		    #riak_field{name     = "yando",
				position = 1,
				type     = binary,
				optional = true},
		    #riak_field{name     = "yando",
				position = 2,
				type     = Map,
				optional = false}
		   ]),
    {module, Module} = make_validation(DDL),
    Result = Module:validate({[], {1, []}}),
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
    DDL = make_ddl(<<"complex_valid_map_1_test">>,
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
			      position = 3,
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
    DDL = make_ddl(<<"complex_valid_map_2_test">>,
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
    DDL = make_ddl(<<"complex_valid_map_3_test">>,
		   [
		    #riak_field{name     = "Top_Map",
				position = 1,
				type     = Map1}
		   ]),
    {module, Module} = make_validation(DDL),
    Result = Module:validate({{2, {3}, {4}}}),
    ?_assertEqual(?VALID, Result).

simple_valid_any_test_() ->
    DDL = make_ddl(<<"simple_valid_any_test">>,
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
    DDL = make_ddl(<<"simple_valid_any_test">>,
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
    DDL = make_ddl(<<"simple_valid_mixed_test">>,
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
    DDL = make_ddl(<<"simple_invalid_binary_test">>,
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
    DDL = make_ddl(<<"simple_invalid_integer_test">>,
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
    DDL = make_ddl(<<"simple_invalid_float_test">>,
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
    DDL = make_ddl(<<"simple_invalid_boolean_test">>,
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
    DDL = make_ddl(<<"simple_invalid_set_test">>,
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
    DDL = make_ddl(<<"simple_invalid_timestamp_1_test">>,
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
    DDL = make_ddl(<<"simple_invalid_timestamp_2_test">>,
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
    DDL = make_ddl(<<"simple_invalid_timestamp_3_test">>,
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

simple_invalid_map_1_test_() ->
    Map = {map, [
		 #riak_field{name     = "yarple",
			     position = 1,
			     type     = binary}
		]},
    DDL = make_ddl(<<"simple_invalid_map_1_test">>,
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
    {module, Module} = make_validation(DDL, "/tmp"),
    Result = Module:validate({<<"ewrewr">>, {99}, 4.4}),
    ?_assertEqual(?INVALID, Result).

simple_invalid_map_2_test_() ->
    Map = {map, [
		 #riak_field{name     = "yarple",
			     position = 1,
			     type     = binary},
		 #riak_field{name     = "yip",
			     position = 2,
			     type     = binary}
		]},
    DDL = make_ddl(<<"simple_invalid_map_2_test">>,
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
    ?_assertEqual(?INVALID, Result).

simple_invalid_map_3_test_() ->
    Map = {map, [
		 #riak_field{name     = "yarple",
			     position = 1,
			     type     = binary},
		 #riak_field{name     = "yip",
			     position = 2,
			     type     = binary}
		]},
    DDL = make_ddl(<<"simple_invalid_map_3_test">>,
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
    ?_assertEqual(?INVALID, Result).

simple_invalid_map_4_test_() ->
    Map = {map, [
		 #riak_field{name     = "yarple",
			     position = 1,
			     type     = binary}
		]},
    DDL = make_ddl(<<"simple_invalid_map_4_test">>,
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
    ?_assertEqual(?INVALID, Result).

complex_invalid_map_1_test_() ->
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
    DDL = make_ddl(<<"complex_invalid_map_1_test">>,
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
    ?_assertEqual(?INVALID, Result).

%%% test the size of the tuples
too_small_tuple_test_() ->
    DDL = make_ddl(<<"simple_too_small_tuple_test">>,
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
    DDL = make_ddl(<<"simple_too_big_tuple_test">>,
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

%% invalid DDL tests

simple_invalid_position_test_() ->
    Duff = #riak_field{name     = "erkle",
		       position = 4,
		       type     = float},
    DDL = make_ddl(<<"simple_invalid_position_test">>,
		   [
		    #riak_field{name     = "yando",
				position = 1,
				type     = float},
		    #riak_field{name     = "erko",
				position = 2,
				type     = float},
		    Duff
		   ]),
    {error, Err} = make_validation(DDL),
    Expected = {invalid_ddl, [{wrong_position, {expected, 3}, Duff}]},
    ?_assertEqual(Expected, Err).

simple_invalid_vals_test_() ->
    Duff = #riak_field{name     = oops_atom,
		       position = 1.0,
		       type     = random},
    DDL = make_ddl(<<"simple_invalid_vals_test">>,
		   [
		    Duff
		   ]),
    {error, Err} = make_validation(DDL),
    Expected = {invalid_ddl, [{invalid_field, Duff},
			      {wrong_position, {expected, 1}, Duff}]},
    ?_assertEqual(Expected, Err).

simple_invalid_optional_test_() ->
    Map = {map, [
		 #riak_field{name     = "yando",
			     position = 1,
			     type     = integer,
			     optional = true}
		]},
    Duff = #riak_field{name     = "ribbit",
		       position = 1,
		       type     = Map,
		       optional = true},
    DDL = make_ddl(<<"simple_invalid_optional_test">>,
		   [
		    Duff
		   ]),
    {error, Err} = make_validation(DDL),
    Expected = {invalid_ddl, [{maps_cant_be_optional, Duff}]},
    ?_assertEqual(Expected, Err).


complex_invalid_vals_test_() ->
    Duff =  #riak_field{name     = bert,
			position = 33,
			type     = shubert,
			optional = true},
    Map = {map, [
		 #riak_field{name     = "yando",
			     position = 1,
			     type     = integer,
			     optional = true},
		 Duff
		]},
    DDL = make_ddl(<<"complex_invalid_vals_test">>,
		   [
		    #riak_field{name     = "yando",
				position = 1,
				type     = binary,
				optional = true},
		    #riak_field{name     = "yando",
				position = 2,
				type     = Map,
				optional = false}
		   ]),
    {error, Err} = make_validation(DDL),
    Expected = {invalid_ddl, [{invalid_field, Duff},
			      {wrong_position, {expected, 2}, Duff}]},
    ?_assertEqual(Expected, Err).

-endif.
