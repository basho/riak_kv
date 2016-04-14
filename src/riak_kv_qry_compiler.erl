%% -------------------------------------------------------------------
%%
%% riak_kv_qry_compiler: generate the coverage for a hashed query
%%
%%
%% Copyright (c) 2016 Basho Technologies, Inc.  All Rights Reserved.
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

-export([compile/3]).
-export([finalise_aggregate/2]).
-export([run_select/2, run_select/3]).
-export([get_version/0]).

%% Testing exports
-export([
         add_types_to_filter_TEST/2,
         rewrite_TEST/3,
         run_select_TEST/2,
         run_select_TEST/3,
         compile_select_clause_TEST/2,
         extract_stateful_functions_TEST/2,
         infer_col_type_TEST/3,
         quantum_field_name_TEST/1
        ]).

-type compiled_select() :: fun((_,_) -> riak_pb_ts_codec:ldbvalue()).
-export_type([compiled_select/0]).

-include("riak_kv_ts.hrl").
-include("riak_kv_index.hrl").
-include("riak_kv_ts_error_msgs.hrl").

-type where_props() :: [{startkey, [term()]} |
                        {endkey, [term()]} |
                        {filter, [term()]} |
                        {start_inclusive, boolean()} |
                        {end_inclusive, boolean()}].
-type combinator()       :: [binary()].
-type limit()            :: any().
-type operator()         :: [binary()].
-type sorter()           :: term().


-export_type([
              combinator/0,
              limit/0,
              operator/0,
              sorter/0
             ]).
-export_type([where_props/0]).

-define(W, riak_kv_qry_where_compiler).

get_version() -> 1.

add_types_to_filter_TEST(Filter, Mod) ->
    add_types_to_filter(Filter, Mod).

rewrite_TEST(LocalKey, Where, Mod) ->
    rewrite(LocalKey, Where, Mod).

run_select_TEST(Select, Row) ->
    run_select(Select, Row).

run_select_TEST(Select, Row, InitialState) ->
    run_select(Select, Row, InitialState).

compile_select_clause_TEST(DDL, SelectQuery) ->
    compile_select_clause(DDL, SelectQuery).

extract_stateful_functions_TEST(Selection, Int) ->
    extract_stateful_functions(Selection, Int).

infer_col_type_TEST(DDL, Field, Errors) ->
    infer_col_type(DDL, Field, Errors).

quantum_field_name_TEST(DDL) ->
    quantum_field_name(DDL).

%% 3rd argument is undefined if we should not be concerned about the
%% maximum number of quanta
-spec compile(#ddl_v1{}, ?SQL_SELECT{}, 'undefined'|pos_integer()) ->
    {ok, [?SQL_SELECT{}]} | {error, any()}.
compile(#ddl_v1{}, ?SQL_SELECT{is_executable = true}, _MaxSubQueries) ->
    {error, 'query is already compiled'};
compile(#ddl_v1{} = DDL,
        ?SQL_SELECT{is_executable = false, 'SELECT' = Sel} = Q, MaxSubQueries) ->
    Ret = if Sel#riak_sel_clause_v1.clause == [] ->
                  {error, 'full table scan not implemented'};
             el/=se ->
                  case compile_select_clause(DDL, Q) of
                      {ok, S} ->
                          ?W:compile_where_clause(DDL, Q?SQL_SELECT{'SELECT' = S}, 
                                                  MaxSubQueries);
                {error, _} = Error ->
                          Error
                  end
          end,
    gg:format("Query compiler returns ~p~n", [Ret]),
    Ret.

%% Calculate the final result for an aggregate.
-spec finalise_aggregate(#riak_sel_clause_v1{}, [any()]) -> [any()].
finalise_aggregate(#riak_sel_clause_v1{ calc_type = aggregate,
                                        finalisers = FinaliserFns }, Row) ->
    finalise_aggregate2(FinaliserFns, Row, Row).

%%
finalise_aggregate2([], [], _) ->
    [];
finalise_aggregate2([skip | Fns], [_ | Row], FullRow) ->
    finalise_aggregate2(Fns, Row, FullRow);
finalise_aggregate2([CellFn | Fns], [Cell | Row], FullRow) ->
    [CellFn(FullRow, Cell) | finalise_aggregate2(Fns, Row, FullRow)].

%% Run the selection spec for all selection columns that was created by
-spec run_select(SelectionSpec::[compiled_select()], Row::[any()]) ->
                        [any()].
run_select(Select, Row) ->
    %% the second argument is the state, if we're return row query results then
    %% there is no long running state
    run_select2(Select, Row, undefined, []).

run_select(Select, Row, InitialState) ->
    %% the second argument is the state, if we're return row query results then
    %% there is no long running state
    run_select2(Select, Row, InitialState, []).

%% @priv
run_select2([], _, _, Acc) ->
    lists:reverse(Acc);
run_select2([Fn | SelectTail], Row, [ColState1 | ColStateTail], Acc1) ->
    Acc2 = prepend_select_columns(Fn(Row, ColState1), Acc1),
    run_select2(SelectTail, Row, ColStateTail, Acc2);
run_select2([Fn | SelectTail], Row, RowState, Acc1) ->
    Acc2 = prepend_select_columns(Fn(Row, RowState), Acc1),
    run_select2(SelectTail, Row, RowState, Acc2).

%% Check if the select column is actually multiple columns, as returned by
%% SELECT * FROM, or the corner case SELECT *, my_col FROM. This cannot simply
%% be flattened because nulls are represented as empty lists.
prepend_select_columns([_|_] = MultiCols, Acc) ->
    lists:reverse(MultiCols) ++ Acc;
prepend_select_columns(V, Acc) ->
    [V | Acc].

%% copy pasta mapfoldl from lists.erl, otherwise this reports
%% a warning in dialyzer!
my_mapfoldl(F, Accu0, [Hd|Tail]) ->
    {R,Accu1} = F(Hd, Accu0),
    {Rs,Accu2} = my_mapfoldl(F, Accu1, Tail),
    {[R|Rs],Accu2};
my_mapfoldl(F, Accu, []) when is_function(F, 2) -> {[],Accu}.

%%
compile_select_clause(DDL, ?SQL_SELECT{'SELECT' = #riak_sel_clause_v1{ clause = Sel } } = Q) ->
    CompileColFn =
        fun(ColX, AccX) ->
            select_column_clause_folder(DDL, ColX, AccX)
        end,
    %% compile each select column and put all the calc types into a set, if
    %% any of the results are aggregate then aggregate is the calc type for the
    %% whole query
    Acc = {sets:new(), #riak_sel_clause_v1{ }},
    %% iterate from the right so we can append to the head of lists
    {ResultTypeSet, Q2} = lists:foldl(CompileColFn, Acc, Sel),

    {ColTypes, Errors} = my_mapfoldl(
        fun(ColASTX, Errors) ->
            infer_col_type(DDL, ColASTX, Errors)
        end, [], Sel),

    case sets:is_element(aggregate, ResultTypeSet) of
        true  ->
            Q3 = Q2#riak_sel_clause_v1{
                   calc_type = aggregate,
                   col_names = get_col_names(DDL, Q) };
        false ->
            Q3 = Q2#riak_sel_clause_v1{
                   calc_type = rows,
                   initial_state = [],
                   col_names = get_col_names(DDL, Q) }
    end,
    case Errors of
      [] ->
          {ok, Q3#riak_sel_clause_v1{
              col_names = get_col_names(DDL, Q),
              col_return_types = lists:flatten(ColTypes) }};
      [_|_] ->
          {error, {invalid_query, riak_kv_qry:format_query_syntax_errors(lists:reverse(Errors))}}
    end.

%%
-spec get_col_names(#ddl_v1{}, ?SQL_SELECT{}) -> [binary()].
get_col_names(DDL, ?SQL_SELECT{'SELECT' = #riak_sel_clause_v1{clause = Select}}) ->
    ColNames = riak_ql_to_string:col_names_from_select(Select),
    %% flatten because * gets expanded to multiple columns
    lists:flatten(
      [get_col_names2(DDL, N) || N <- ColNames]
    ).

%%
get_col_names2(DDL, "*") ->
    [X#riak_field_v1.name || X <- DDL#ddl_v1.fields];
get_col_names2(_, Name) ->
    list_to_binary(Name).

%%
-record(single_sel_column, {
          calc_type        :: select_result_type(),
          initial_state    :: any(),
          col_return_types :: [riak_pb_ts_codec:ldbvalue()],
          col_name         :: riak_pb_ts_codec:tscolumnname(),
          clause           :: function(),
          finaliser        :: [function()]
         }).

%%
-spec select_column_clause_folder(#ddl_v1{}, riak_ql_ddl:selection(),
                                  {set(), #riak_sel_clause_v1{}}) ->
                {set(), #riak_sel_clause_v1{}}.
select_column_clause_folder(DDL, ColAST1,
                            {TypeSet1, #riak_sel_clause_v1{ finalisers = Finalisers } = SelClause}) ->
    %% extract the stateful functions then treat them as separate select columns
    LenFinalisers = length(Finalisers),
    case extract_stateful_functions(ColAST1, LenFinalisers) of
        {ColAST2, []} ->
            %% the case where the column contains no functions
            FinaliserFn =
                compile_select_col_stateless(DDL, {return_state, LenFinalisers + 1}),
            ColAstList = [{ColAST2, FinaliserFn}];
        {FinaliserAST, [WindowFnAST | Tail]} ->
            %% the column contains one or more functions that will be separated
            %% into their own columns until finalisation
            FinaliserFn =
                compile_select_col_stateless(DDL, FinaliserAST),
            ActualCol = {WindowFnAST, FinaliserFn},
            TempCols = [{AST, skip} || AST <- Tail],
            ColAstList = [ActualCol | TempCols]
    end,
    FolderFn =
        fun(E, Acc) ->
            select_column_clause_exploded_folder(DDL, E, Acc)
        end,
    lists:foldl(FolderFn, {TypeSet1, SelClause}, ColAstList).


%% When the select column is "exploded" it means that multiple functions that
%% collect state have been extracted and given their own temporary columns
%% which will be merged by the finalisers.
select_column_clause_exploded_folder(DDL, {ColAst, Finaliser}, {TypeSet1, SelClause1}) ->
    #riak_sel_clause_v1{
       initial_state = InitX,
       clause = RunFnX,
       finalisers = Finalisers1 } = SelClause1,
    S = compile_select_col(DDL, ColAst),
    TypeSet2 = sets:add_element(S#single_sel_column.calc_type, TypeSet1),
    Init2   = InitX ++ [S#single_sel_column.initial_state],
    RunFn2  = RunFnX ++ [S#single_sel_column.clause],
    Finalisers2 = Finalisers1 ++ [Finaliser],
    %% ColTypes are messy because <<"*">> represents many
    %% so you need to flatten the list
    SelClause2 = #riak_sel_clause_v1{
                    initial_state    = Init2,
                    clause           = RunFn2,
                    finalisers       = lists:flatten(Finalisers2)},
    {TypeSet2, SelClause2}.

%% Compile a single selection column into a fun that can extract the cell
%% from the row.
-spec compile_select_col(DDL::#ddl_v1{}, ColumnSpec::any()) ->
                                #single_sel_column{}.
compile_select_col(DDL, {{window_agg_fn, FnName}, [FnArg1]}) when is_atom(FnName) ->
    case riak_ql_window_agg_fns:start_state(FnName) of
        stateless ->
            %% TODO this does not run the function! nothing is stateless so far though
            Fn = compile_select_col_stateless(DDL, FnArg1),
            #single_sel_column{ calc_type        = rows,
                                initial_state    = undefined,
                                clause           = Fn };
        Initial_state ->
            Compiled_arg1 = compile_select_col_stateless(DDL, FnArg1),
            % all the windows agg fns so far are arity of 1
            % which we have forced in this clause by matching on a single argument in the
            % function head
            SelectFn =
                fun(Row, State) ->
                        riak_ql_window_agg_fns:FnName(Compiled_arg1(Row, State), State)
                end,
            #single_sel_column{ calc_type        = aggregate,
                                initial_state    = Initial_state,
                                clause           = SelectFn }
    end;
compile_select_col(DDL, Select) ->
    #single_sel_column{ calc_type = rows,
                        initial_state = undefined,
                        clause = compile_select_col_stateless(DDL, Select) }.


%% Returns a one arity fun which is stateless for example pulling a field from a
%% row.
-spec compile_select_col_stateless(#ddl_v1{}, riak_ql_ddl:selection()|{Op::atom(), riak_ql_ddl:selection(), riak_ql_ddl:selection()}|{return_state, integer()}) ->
       compiled_select().
compile_select_col_stateless(_, {identifier, [<<"*">>]}) ->
    fun(Row, _) -> Row end;
compile_select_col_stateless(DDL, {negate, ExprToNegate}) ->
    ValueToNegate = compile_select_col_stateless(DDL, ExprToNegate),
    fun(Row, State) -> -ValueToNegate(Row, State) end;
compile_select_col_stateless(_, {Type, V}) when Type == varchar; Type == boolean; Type == binary; Type == integer; Type == float ->
    fun(_,_) -> V end;
compile_select_col_stateless(_, {return_state, N}) when is_integer(N) ->
    fun(Row,_) -> pull_from_row(N, Row) end;
compile_select_col_stateless(_, {finalise_aggregation, FnName, N}) ->
    fun(Row,_) ->
        ColValue = pull_from_row(N, Row),
        riak_ql_window_agg_fns:finalise(FnName, ColValue)
    end;
compile_select_col_stateless(#ddl_v1{ fields = Fields }, {identifier, ColumnName}) ->
    {Index, _} = col_index_and_type_of(Fields, to_column_name_binary(ColumnName)),
    fun(Row,_) -> pull_from_row(Index, Row) end;
compile_select_col_stateless(DDL, {Op, A, B}) ->
    Arg_a = compile_select_col_stateless(DDL, A),
    Arg_b = compile_select_col_stateless(DDL, B),
    compile_select_col_stateless2(Op, Arg_a, Arg_b).

%%
-spec infer_col_type(#ddl_v1{}, riak_ql_ddl:selection(), Errors1::[any()]) ->
        {riak_ql_ddl:simple_field_type() | error, Errors2::[any()]}.
infer_col_type(_, {Type, _}, Errors) when Type == sint64; Type == varchar;
                                          Type == boolean; Type == double ->
    {Type, Errors};
infer_col_type(_, {binary, _}, Errors) ->
    {varchar, Errors};
infer_col_type(_, {integer, _}, Errors) ->
    {sint64, Errors};
infer_col_type(_, {float, _}, Errors) ->
    {double, Errors};
infer_col_type(#ddl_v1{ fields = Fields }, {identifier, ColName1}, Errors) ->
    case to_column_name_binary(ColName1) of
        <<"*">> ->
            Type = [T || #riak_field_v1{ type = T } <- Fields];
        ColName2 ->
            {_, Type} = col_index_and_type_of(Fields, ColName2)
    end,
    {Type, Errors};
infer_col_type(DDL, {{window_agg_fn, FnName}, [FnArg1]}, Errors1) ->
    case infer_col_type(DDL, FnArg1, Errors1) of
        {error, _} = Error ->
            Error;
        {ArgType, Errors2} ->
            infer_col_function_type(FnName, [ArgType], Errors2)
    end;
infer_col_type(DDL, {Op, A, B}, Errors1) when Op == '/'; Op == '+'; Op == '-'; Op == '*' ->
    {AType, Errors2} = infer_col_type(DDL, A, Errors1),
    {BType, Errors3} = infer_col_type(DDL, B, Errors2),
    maybe_infer_op_type(Op, AType, BType, Errors3);
infer_col_type(DDL, {negate, AST}, Errors) ->
    infer_col_type(DDL, AST, Errors).

%%
infer_col_function_type(FnName, ArgTypes, Errors) ->
    case riak_ql_window_agg_fns:fn_type_signature(FnName, ArgTypes) of
        {error, Reason} ->
            {error, [Reason | Errors]};
        ReturnType ->
            {ReturnType, Errors}
    end.

%%
pull_from_row(N, Row) ->
    lists:nth(N, Row).

%%
-spec extract_stateful_functions(riak_ql_ddl:selection(), integer()) ->
        {riak_ql_ddl:selection() | {return_state, integer()}, [riak_ql_ddl:selection_function()]}.
extract_stateful_functions(Selection1, FinaliserLen) when is_integer(FinaliserLen) ->
    {Selection2, Fns} = extract_stateful_functions2(Selection1, FinaliserLen, []),
    {Selection2, lists:reverse(Fns)}.

%% extract stateful functions from the selection
-spec extract_stateful_functions2(riak_ql_ddl:selection(), integer(), [riak_ql_ddl:selection_function()]) ->
        {riak_ql_ddl:selection() | {finalise_aggregation, FnName::atom(), integer()}, [riak_ql_ddl:selection_function()]}.
extract_stateful_functions2({Op, ArgA1, ArgB1}, FinaliserLen, Fns1) ->
    {ArgA2, Fns2} = extract_stateful_functions2(ArgA1, FinaliserLen, Fns1),
    {ArgB2, Fns3} = extract_stateful_functions2(ArgB1, FinaliserLen, Fns2),
    {{Op, ArgA2, ArgB2}, Fns3};
extract_stateful_functions2({negate, Arg1}, FinaliserLen, Fns1) ->
    {Arg2, Fns2} = extract_stateful_functions2(Arg1, FinaliserLen, Fns1),
    {{negate, Arg2}, Fns2};
extract_stateful_functions2({Tag, _} = Node, _, Fns)
        when Tag == identifier; Tag == sint64; Tag == integer; Tag == float;
             Tag == binary;     Tag == varchar; Tag == boolean ->
    {Node, Fns};
extract_stateful_functions2({{window_agg_fn, FnName}, _} = Function, FinaliserLen, Fns1) ->
    Fns2 = [Function | Fns1],
    {{finalise_aggregation, FnName, FinaliserLen + length(Fns2)}, Fns2}.

%%
maybe_infer_op_type(_, error, _, Errors) ->
    {error, Errors};
maybe_infer_op_type(_, _, error, Errors) ->
    {error, Errors};
maybe_infer_op_type(Op, AType, BType, Errors) ->
    case infer_op_type(Op, AType, BType) of
        {error, Reason} ->
            {error, [Reason | Errors]};
        Type ->
            {Type, Errors}
    end.

%%
infer_op_type('/', sint64, sint64) -> sint64;
infer_op_type('/', double, double) -> double;
infer_op_type('/', sint64, double) -> double;
infer_op_type('/', double, sint64) -> double;
infer_op_type(_, T, T) when T == double orelse T == sint64 ->
    T;
infer_op_type(_, T1, T2) when T1 == double andalso T2 == sint64;
                              T1 == sint64 andalso T2 == double ->
    double;
infer_op_type(Op, T1, T2) ->
    {error, {operator_type_mismatch, Op, T1, T2}}.

%%
compile_select_col_stateless2('+', A, B) ->
    fun(Row, State) ->
        riak_ql_window_agg_fns:add(A(Row, State), B(Row, State))
    end;
compile_select_col_stateless2('*', A, B) ->
    fun(Row, State) ->
        riak_ql_window_agg_fns:multiply(A(Row, State), B(Row, State))
    end;
compile_select_col_stateless2('/', A, B) ->
    fun(Row, State) ->
        riak_ql_window_agg_fns:divide(A(Row, State), B(Row, State))
    end;
compile_select_col_stateless2('-', A, B) ->
    fun(Row, State) ->
        riak_ql_window_agg_fns:subtract(A(Row, State), B(Row, State))
    end.

%%
to_column_name_binary([Name]) when is_binary(Name) ->
    Name;
to_column_name_binary(Name) when is_binary(Name) ->
    Name.

%% Return the index and type of a field in the table definition.
col_index_and_type_of(Fields, ColumnName) ->
    case lists:keyfind(ColumnName, #riak_field_v1.name, Fields) of
        false ->
            FieldNames = [X#riak_field_v1.name || X <- Fields],
            error({unknown_column, {ColumnName, FieldNames}});
        #riak_field_v1{ position = Position, type = Type } ->
            {Position, Type}
    end.


%%
quantum_field_name(DDL) ->
    case find_quantum_fields(DDL) of
        [QFieldName] ->
            QFieldName;
        [] ->
            no_quanta
    end.

%%
find_quantum_fields(#ddl_v1{ partition_key = #key_v1{ ast = PKAST } }) ->
    [quantum_fn_to_field_name(QuantumFunc) || #hash_fn_v1{ } = QuantumFunc <- PKAST].

%%
quantum_fn_to_field_name(#hash_fn_v1{ mod = riak_ql_quanta,
                                      fn = quantum,
                                      args = [#param_v1{name = [Name]}|_ ] }) ->
    Name.

add_types_to_filter(Filter, Mod) ->
    add_types2(Filter, Mod, []).

add_types2([], _Mod, Acc) ->
    make_ands(lists:reverse(Acc));
add_types2([{Op, LHS, RHS} | T], Mod, Acc) when Op =:= and_ orelse
                                                Op =:= or_  ->
    NewAcc = {Op, add_types2([LHS], Mod, []), add_types2([RHS], Mod, [])},
    add_types2(T, Mod, [NewAcc | Acc]);
add_types2([{Op, Field, {_, Val}} | T], Mod, Acc) ->
    NewType = Mod:get_field_type([Field]),
    NewAcc = {Op, {field, Field, NewType}, {const, normalise(Val, NewType)}},
    add_types2(T, Mod, [NewAcc | Acc]).

%% the query is prevalidated so the value can only convert down to one of these
%% two values (but that may fail in the future)
normalise(Val, boolean) when is_binary(Val) ->
    case string:to_lower(binary_to_list(Val)) of
        "true"  -> true;
        "false" -> false
    end;
normalise(Val, boolean) when is_list(Val) ->
    case string:to_lower(Val) of
        "true"  -> true;
        "false" -> false
    end;
normalise(X, _) ->
    X.

%% I know, not tail recursive could stackbust
%% but not really
make_ands([]) ->
    [];
make_ands([H | []]) ->
    H;
make_ands([H | T]) ->
    {and_, H, make_ands(T)}.

%%
rewrite(#key_v1{ast = AST}, W, Mod) ->
    rewrite2(AST, W, Mod, []).

%%
rewrite2([], [], _Mod, Acc) ->
    lists:reverse(Acc);
rewrite2([], _W, _Mod, _Acc) ->
    %% the rewrite should have consumed all the passed in values
    {error, {invalid_rewrite, _W}};
rewrite2([#param_v1{name = [FieldName]} | T], Where1, Mod, Acc) ->
    Type = Mod:get_field_type([FieldName]),
    case lists:keytake(FieldName, 2, Where1) of
        false                           ->
            {error, {missing_param, ?E_MISSING_PARAM_IN_WHERE_CLAUSE(FieldName)}};
        {value, {_, _, {_, Val}}, Where2} ->
            rewrite2(T, Where2, Mod, [{FieldName, Type, Val} | Acc])
    end.
