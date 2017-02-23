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

-export([compile/2]).
-export([compile_select_clause/2,  %% used in riak_kv_qry_buffers;
         compile_order_by/1]).     %% to deliver chunks more efficiently
-export([finalise_aggregate/2]).
-export([run_select/2, run_select/3]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

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
-type limit()            :: pos_integer().
-type offset()           :: non_neg_integer().
-type operator()         :: [binary()].
-type sorter()           :: {binary(), asc|desc, nulls_first|nulls_last}.


-export_type([combinator/0,
              limit/0,
              offset/0,
              operator/0,
              sorter/0]).
-export_type([where_props/0]).

-define(MAX_QUERY_QUANTA, 1000).  %% cap the number of subqueries the compiler will emit
%% Note that when such a query begins to be actually executed, the
%% chunks will need to be really small, for the result to be within
%% max query size.

-spec compile(?DDL{}, ?SQL_SELECT{}) ->
    {ok, [?SQL_SELECT{}]} | {error, any()}.
compile(?DDL{}, ?SQL_SELECT{is_executable = true}) ->
    {error, 'query is already compiled'};
compile(?DDL{table = T} = DDL,
        ?SQL_SELECT{is_executable = false} = Q1) ->
    Mod = riak_ql_ddl:make_module_name(T),
    case compile_order_by(
           maybe_compile_group_by(
             Mod, compile_select_clause(DDL, Q1), Q1)) of
        {ok, Q2} ->
            compile_where_clause(DDL, Q2);
        {error, _} = Error ->
            Error
    end.


-spec compile_order_by({ok, ?SQL_SELECT{}} | {error, any()}) ->
                              {ok, ?SQL_SELECT{}} | {error, any()}.
compile_order_by({error,_} = E) ->
    E;
compile_order_by({ok, ?SQL_SELECT{'ORDER BY' = OrderBy,
                                  'OFFSET'   = Offset,
                                  'LIMIT'    = Limit,
                                  'SELECT' = #riak_sel_clause_v1{calc_type = CalcType}}})
  when (length(OrderBy) > 0 orelse
        length(Limit)   > 0 orelse
        length(Offset)  > 0) andalso CalcType /= rows ->
    {error, {order_by_with_aggregate_calc_type, ?E_ORDER_BY_WITH_AGGREGATE_CALC_TYPE}};
compile_order_by({ok, ?SQL_SELECT{'ORDER BY' = OrderBy} = Q}) ->
    case non_unique_identifiers(OrderBy) of
        [] ->
            {ok, Q?SQL_SELECT{'ORDER BY' = OrderBy}};
        WhichNonUnique ->
            {error, {non_unique_orderby_fields, ?E_NON_UNIQUE_ORDERBY_FIELDS(hd(WhichNonUnique))}}
    end.

non_unique_identifiers(FF) ->
    Occurrences = [{F, occurs(F, FF)} || {F, _, _} <- FF],
    [F || {F, N} <- Occurrences, N > 1].
occurs(F, FF) ->
    lists:foldl(
      fun({A, _, _}, N) when A == F -> N + 1;
         (_, N) -> N
      end,
      0, FF).

%%
maybe_compile_group_by(_, {error,_} = E, _) ->
    E;
maybe_compile_group_by(Mod, {ok, Sel3}, ?SQL_SELECT{ group_by = GroupBy } = Q1) ->
    %% a throw means an error occurred and the function returned early and is
    %% an expected error e.g. column name typo.
    try
        compile_group_by(Mod, GroupBy, [], Q1?SQL_SELECT{ 'SELECT' = Sel3 })
    catch
        throw:Error -> Error
    end.

%%
compile_group_by(_, [], Acc, Q) ->
    {ok, Q?SQL_SELECT{ group_by = lists:reverse(Acc) }};
compile_group_by(Mod, [{identifier,FieldName}|Tail], Acc, Q)
        when is_binary(FieldName) ->
    Pos = get_group_by_field_position(Mod, FieldName),
    compile_group_by(Mod, Tail, [{Pos,FieldName}|Acc], Q);
compile_group_by(Mod, [{time_fn,{_,FieldName},_} = GroupTimeFnAST|Tail], Acc, Q) when is_binary(FieldName) ->
    GroupTimeFn = make_group_by_time_fn(Mod, GroupTimeFnAST),
    compile_group_by(Mod, Tail, [{GroupTimeFn,FieldName}|Acc], Q).

-spec make_group_by_time_fn(module(), group_time_fn()) -> function().
make_group_by_time_fn(Mod, {time_fn, {identifier,FieldName},{integer,GroupSize}}) ->
    Pos = get_group_by_field_position(Mod, FieldName),
    fun(Row) ->
        Time = lists:nth(Pos, Row),
        riak_ql_quanta:quantum(Time, GroupSize, 'ms')
    end.

%% Get the position in the row for a table column, with `group by` specific
%% errors if the column name does not exist in this table.
get_group_by_field_position(Mod, FieldName) when is_binary(FieldName) ->
    case Mod:get_field_position([FieldName]) of
        undefined ->
            throw(group_by_column_does_not_exist_error(Mod, FieldName));
        Pos when is_integer(Pos) ->
            Pos
    end.

group_by_column_does_not_exist_error(Mod, FieldName) ->
    ?DDL{table = TableName} = Mod:get_ddl(),
    {error, {invalid_query, ?E_MISSING_COL_IN_GROUP_BY(FieldName, TableName)}}.

%% adding the local key here is a bodge
%% should be a helper fun in the generated DDL module but I couldn't
%% write that up in time
-spec compile_where_clause(?DDL{}, ?SQL_SELECT{}) ->
                                  {ok, [?SQL_SELECT{}]} | {error, term()}.
compile_where_clause(?DDL{} = DDL,
                     ?SQL_SELECT{helper_mod = Mod,
                                 is_executable = false,
                                 'WHERE'       = W1,
                                 cover_context = Cover} = Q) ->
    {W2,_} = resolve_expressions(Mod, W1),
    case {compile_where(DDL, lists:flatten([W2])), unwrap_cover(Cover)} of
        {{error, E}, _} ->
            {error, E};
        {_, {error, E}} ->
            {error, E};
        {NewW, {ok, {RealCover, WhereModifications}}} ->
            expand_query(DDL, Q?SQL_SELECT{cover_context = RealCover},
                         update_where_for_cover(NewW, WhereModifications))
    end.

%% now break out the query on quantum boundaries
-spec expand_query(?DDL{}, ?SQL_SELECT{}, proplists:proplist()) ->
                          {ok, [?SQL_SELECT{}]} | {error, term()}.
expand_query(?DDL{local_key = LK, partition_key = PK},
             ?SQL_SELECT{helper_mod = Mod} = Q1, Where1) ->
    case expand_where(Where1, PK) of
        {error, E} ->
            {error, E};
        {ok, Where2} ->
            IsDescending = is_last_partition_column_descending(Mod:field_orders(), PK),
            SubQueries1 =
                [Q1?SQL_SELECT{
                       is_executable = true,
                       type          = timeseries,
                       'WHERE'       = maybe_fix_start_order(IsDescending, X),
                       local_key     = LK,
                       partition_key = PK} || X <- Where2],
            SubQueries2 =
                case IsDescending of
                    true ->
                        fix_subquery_order(SubQueries1);
                    false ->
                        SubQueries1
                end,
            {ok, SubQueries2}
    end.

%% Only check the last column in the partition key, otherwise the start/end key
%% does not need to be flipped. The data is not stored in a different order to
%% the start/end key.
%%
%% Checking the last column in the partition key is safe while the other columns
%% must be exactly specified e.g. anything but the QUANTUM column must be
%% covered with an equals operator in the where clause.
is_last_partition_column_descending(FieldOrders, PK) ->
    lists:nth(key_length(PK), FieldOrders) == descending.

key_length(#key_v1{ast = AST}) ->
    length(AST).

local_key_field_orders(FieldOrders, PK) ->
    lists:nthtail(key_length(PK), FieldOrders).

%% Calulate the final result for an aggregate.
-spec finalise_aggregate(#riak_sel_clause_v1{}, [any()]) -> [any()].
finalise_aggregate(#riak_sel_clause_v1{ calc_type = CalcType,
                                        finalisers = FinaliserFns }, Row)
                    when CalcType == aggregate; CalcType == group_by ->
    finalise_aggregate2(FinaliserFns, Row, Row).

%%
finalise_aggregate2([], [], _) ->
    [];
finalise_aggregate2([skip | Fns], [_ | Row], FullRow) ->
    finalise_aggregate2(Fns, Row, FullRow);
finalise_aggregate2([CellFn | Fns], [Cell | Row], FullRow) ->
    [CellFn(FullRow, Cell) | finalise_aggregate2(Fns, Row, FullRow)].

%% Run the selection spec for all selection columns that was created by
-spec run_select(SelectionSpec::[compiled_select()], Row::[riak_pb_ts_codec:ldbvalue()]) ->
                        [riak_pb_ts_codec:ldbvalue()].
run_select(Select, Row) ->
    %% the second argument is the state, if we're return row query results then
    %% there is no long running state
    run_select2(Select, Row, undefined, []).

run_select(Select,  Row, InitialState) ->
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
compile_select_clause(DDL, ?SQL_SELECT{'SELECT' = #riak_sel_clause_v1{clause = Sel}} = Q) ->
    %% compile each select column and put all the calc types into a set, if
    %% any of the results are aggregate then aggregate is the calc type for the
    %% whole query
    CompileColFn =
        fun(ColX, AccX) ->
            select_column_clause_folder(DDL, ColX, AccX)
        end,
    Acc = {sets:new(), #riak_sel_clause_v1{ }},
    %% iterate from the right so we can append to the head of lists
    {ResultTypeSet, Sel1} = lists:foldl(CompileColFn, Acc, Sel),
    {ColTypes, Errors} = my_mapfoldl(
        fun(ColASTX, Errors) ->
            infer_col_type(DDL, ColASTX, Errors)
        end, [], Sel),
    IsGroupBy = (Q?SQL_SELECT.group_by /= []),
    IsAggregate = sets:is_element(aggregate, ResultTypeSet),
    if
        IsGroupBy ->
            Sel2 = Sel1#riak_sel_clause_v1{calc_type = group_by};
        IsAggregate ->
            Sel2 = Sel1#riak_sel_clause_v1{calc_type = aggregate};
        not IsAggregate ->
            Sel2 = Sel1#riak_sel_clause_v1{
                   calc_type = rows,
                   initial_state = []}
    end,
    case Errors of
        [] ->
            Sel3 = Sel2#riak_sel_clause_v1{
                col_return_types = lists:flatten(ColTypes),
                col_names = get_col_names(DDL, Q)},
            {ok, Sel3};
        [_|_] ->
            {error, {invalid_query, riak_kv_qry:format_query_syntax_errors(lists:reverse(Errors))}}
    end.

%%
-spec get_col_names(?DDL{}, ?SQL_SELECT{}) -> [binary()].
get_col_names(DDL, ?SQL_SELECT{'SELECT' = #riak_sel_clause_v1{clause = Select}}) ->
    ColNames = riak_ql_to_string:col_names_from_select(Select),
    %% flatten because * gets expanded to multiple columns
    lists:flatten(
      [get_col_names2(DDL, N) || N <- ColNames]
    ).

%%
get_col_names2(DDL, "*") ->
    [X#riak_field_v1.name || X <- DDL?DDL.fields];
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
-spec select_column_clause_folder(?DDL{}, riak_ql_ddl:selection(),
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
-spec compile_select_col(DDL::?DDL{}, ColumnSpec::any()) ->
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
-spec compile_select_col_stateless(?DDL{}, riak_ql_ddl:selection()
                                   | {Op::atom(), riak_ql_ddl:selection(), riak_ql_ddl:selection()}
                                   | {return_state, integer()}) ->
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
compile_select_col_stateless(DDL, {{sql_select_fn, 'TIME'}, Args1}) ->
    [Argsx1,Argsx2] = [compile_select_col_stateless(DDL,Ax) || Ax <- Args1],
    fun(Row,State) ->
        A1 = Argsx1(Row,State),
        A2 = Argsx2(Row,State),
        riak_ql_quanta:quantum(A1,A2,ms)
    end;
compile_select_col_stateless(_, {finalise_aggregation, FnName, N}) ->
    fun(Row,_) ->
        ColValue = pull_from_row(N, Row),
        riak_ql_window_agg_fns:finalise(FnName, ColValue)
    end;
compile_select_col_stateless(?DDL{ fields = Fields }, {identifier, ColumnName}) ->
    {Index, _} = col_index_and_type_of(Fields, to_column_name_binary(ColumnName)),
    fun(Row,_) -> pull_from_row(Index, Row) end;
compile_select_col_stateless(DDL, {Op, A, B}) ->
    Arg_a = compile_select_col_stateless(DDL, A),
    Arg_b = compile_select_col_stateless(DDL, B),
    compile_select_col_stateless2(Op, Arg_a, Arg_b).

%%
-spec infer_col_type(?DDL{}, riak_ql_ddl:selection(), Errors::[any()]) ->
        {riak_ql_ddl:external_field_type() | error, any()}.
infer_col_type(_, {Type, _}, Errors) when Type == sint64; Type == varchar;
                                          Type == boolean; Type == double ->
    {Type, Errors};
infer_col_type(_, {binary, _}, Errors) ->
    {varchar, Errors};
infer_col_type(_, {integer, _}, Errors) ->
    {sint64, Errors};
infer_col_type(_, {float, _}, Errors) ->
    {double, Errors};
infer_col_type(?DDL{ fields = Fields }, {identifier, ColName1}, Errors) ->
    case to_column_name_binary(ColName1) of
        <<"*">> ->
            Type = [T || #riak_field_v1{ type = T } <- Fields];
        ColName2 ->
            {_, Type} = col_index_and_type_of(Fields, ColName2)
    end,
    {Type, Errors};
infer_col_type(DDL, {{sql_select_fn, 'TIME'}, Args}, Errors1) ->
    {ArgTypes,Errors2} =
        lists:foldr(
            fun(E, {Types, ErrorsX}) ->
                case infer_col_type(DDL, E, ErrorsX) of
                    {error,E}     -> {[error|Types],[E|ErrorsX]};
                    {T, ErrorsX2} -> {[T|Types], ErrorsX2}
                end
            end, {[], Errors1}, Args),
    case ArgTypes of
        [timestamp,sint64] ->
            {timestamp,Errors2};
        _ ->
            FnSigError = {argument_type_mismatch, 'TIME', ArgTypes},
            {error,[FnSigError|Errors2]}
    end;
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
        {riak_ql_ddl:selection() |
         {return_state, integer()}, [riak_ql_ddl:selection_function()]}.
extract_stateful_functions(Selection1, FinaliserLen) when is_integer(FinaliserLen) ->
    {Selection2, Fns} = extract_stateful_functions2(Selection1, FinaliserLen, []),
    {Selection2, lists:reverse(Fns)}.

%% extract stateful functions from the selection
-spec extract_stateful_functions2(riak_ql_ddl:selection(), integer(),
                                  [riak_ql_ddl:selection_function()]) ->
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
extract_stateful_functions2({{sql_select_fn, _}, _} = Node, _, Fns) ->
    {Node, Fns};
extract_stateful_functions2({{window_agg_fn, FnName}, _} = Function, FinaliserLen, Fns1) ->
    Fns2 = [Function | Fns1],
    {{finalise_aggregation, FnName, FinaliserLen + length(Fns2)}, Fns2}.

%% Only change the start order if the query has descending fields, otherwise
%% the natural order is correct.
maybe_fix_start_order(false, W) ->
    W;
maybe_fix_start_order(true, W) ->
    fix_start_order(W).

%%
fix_start_order(W) ->
    {startkey, StartKey0} = lists:keyfind(startkey, 1, W),
    {endkey, EndKey0} = lists:keyfind(endkey, 1, W),
    %% Swap the start/end keys so that the backends will
    %% scan over them correctly.  Likely cause is a descending
    %% timestamp field.
    W1 = lists:keystore(startkey, 1, W, {startkey, EndKey0}),
    W2 = lists:keystore(endkey, 1, W1, {endkey, StartKey0}),
    %% start inclusive defaults true, end inclusive defaults false
    W3 = lists:keystore(start_inclusive, 1, W2,
                        {start_inclusive, proplists:get_value(end_inclusive, W, false)}),
    _W4 = lists:keystore(end_inclusive, 1, W3,
                         {end_inclusive, proplists:get_value(start_inclusive, W2, true)}).

fix_subquery_order(Queries1) ->
    Queries2 = lists:sort(fun fix_subquery_order_compare/2, Queries1),
    case Queries2 of
        [?SQL_SELECT{ 'WHERE' = FirstWhere1 } = FirstQuery|QueryTail] when length(Queries2) > 1 ->
            case lists:keytake(end_inclusive, 1, FirstWhere1) of
                false ->
                    Queries2;
                {value, Flag, _FirstWhere2} ->
                    ?SQL_SELECT{ 'WHERE' = LastWhere } = LastQuery1 = lists:last(Queries2),
                    LastQuery2 = LastQuery1?SQL_SELECT{ 'WHERE' = lists:keystore(end_inclusive, 1, LastWhere, Flag) },
                    Queries3 = QueryTail -- [LastQuery1],
                    [FirstQuery?SQL_SELECT{ 'WHERE' = FirstWhere1 } | Queries3] ++ [LastQuery2]
            end;
        _ ->
            Queries2
    end.

%% Make the subqueries appear in the same order as the keys.  The qry worker
%% returns the results to the client in the order of the subqueries, so
%% if timestamp is descending for equality queries on family/series then
%% the results need to merge in the reverse order.
%%
%% Detect this by the implict order of the start/end keys.  Should be
%% refactored to explicitly understand key order at some future point.
fix_subquery_order_compare(Qa, Qb) ->
    {startkey, Astartkey} = lists:keyfind(startkey, 1, Qa?SQL_SELECT.'WHERE'),
    {endkey, Aendkey}     = lists:keyfind(endkey, 1, Qa?SQL_SELECT.'WHERE'),
    {startkey, Bstartkey} = lists:keyfind(startkey, 1, Qb?SQL_SELECT.'WHERE'),
    {endkey, Bendkey}     = lists:keyfind(endkey, 1, Qb?SQL_SELECT.'WHERE'),

    fix_subquery_order_compare(Astartkey, Aendkey, Bstartkey, Bendkey).

%%
fix_subquery_order_compare(Astartkey, Aendkey, Bstartkey, Bendkey) ->
    if
        (Astartkey == Aendkey) ->
            (Astartkey =< Bstartkey orelse Aendkey =< Bendkey);
        (Bstartkey == Bendkey) ->
            not (Astartkey =< Bstartkey orelse Aendkey =< Bendkey);
        (Astartkey =< Aendkey) ->
            Astartkey =< Bstartkey;
        true ->
            Bstartkey =< Astartkey
    end.

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
-spec expand_where(riak_ql_ddl:filter(), #key_v1{}) ->
                          {ok, [where_props()]} | {error, atom()}.
expand_where(Where, PartitionKey) ->
    case find_quantum_field_index_in_key(PartitionKey) of
        {QField, QSize, QUnit, QIndex} ->
            hash_timestamp_to_quanta(QField, QSize, QUnit, QIndex, Where);
        notfound ->
            {ok, [Where]}
    end.

%% Return the parameters for the quantum function and it's index in the
%% partition key fields.
-spec find_quantum_field_index_in_key(#key_v1{}) ->
    {QName::binary(), QSize::integer(), QUnit::atom(), QIndex::integer()} | notfound.
find_quantum_field_index_in_key(#key_v1{ ast = PKAST }) ->
    find_quantum_field_index_in_key2(PKAST, 1).

%%
find_quantum_field_index_in_key2([], _) ->
    notfound;
find_quantum_field_index_in_key2([#hash_fn_v1{ mod = riak_ql_quanta,
                                               fn = quantum,
                                               args = [?SQL_PARAM{name = [X]}, Y, Z] }|_], Index) ->
    {X,Y,Z,Index};
find_quantum_field_index_in_key2([_|Tail], Index) ->
    find_quantum_field_index_in_key2(Tail, Index+1).

%%
hash_timestamp_to_quanta(QField, QSize, QUnit, QIndex, Where1) ->
    GetMaxMinFun = fun({startkey, List}, {_S, E}) ->
                           {element(3, lists:nth(QIndex, List)), E};
                      ({endkey,   List}, {S, _E}) ->
                           {S, element(3, lists:nth(QIndex, List))};
                      (_, {S, E})  ->
                           {S, E}
                   end,
    {Min1, Max1} = lists:foldl(GetMaxMinFun, {"", ""}, Where1),
    %% if the start range is not inclusive then add one and remove the
    %% start_inclusive flag. This is so that the query start key hashes to the
    %% correct quanta when it is on the boundary since the start_inclusive flag
    %% is not taken into account in the partition hashing. For example given a
    %% one second quantum `mytime > 1999` should return keys with mytime greater
    %% than 2000 but will hash to the quantum before 2000 and receive no results
    %% from it.
    case lists:keytake(start_inclusive, 1, Where1) of
        {value, {start_inclusive, false}, WhereX}  ->
            Where2 = WhereX,
            Min2 = Min1 + 1;
        _ ->
            Where2 = Where1,
            Min2 = Min1
    end,
    Max2 =
        case proplists:get_value(end_inclusive, Where2, false) of
            true  -> Max1 + 1;
            false -> Max1
        end,
    %% sanity check for the number of quanta we can handle
    MaxQueryQuanta = app_helper:get_env(riak_kv, timeseries_query_max_quanta_span, ?MAX_QUERY_QUANTA),
    NQuanta = (Max2 - Min2) div riak_ql_quanta:unit_to_millis(QSize, QUnit),
    case NQuanta < MaxQueryQuanta of
        true ->
            {_NoSubQueries, Boundaries} =
                riak_ql_quanta:quanta(Min2, Max2, QSize, QUnit),
            %% use the maximum value that has not been incremented, we still use
            %% the end_inclusive flag because the end key is not used to hash
            {ok, make_wheres(Where2, QField, Min2, Max1, Boundaries)};
        false ->
            lager:info("query spans too many quanta (~b, max ~b)", [NQuanta, MaxQueryQuanta]),
            {error, {too_many_subqueries, NQuanta, MaxQueryQuanta}}
    end.

make_wheres(Where, QField, LowerBound, UpperBound, Boundaries) when LowerBound > UpperBound ->
    make_wheres(Where, QField, UpperBound, LowerBound, Boundaries);
make_wheres(Where, QField, LowerBound, UpperBound, Boundaries) ->
    {HeadOption, TailOption, NewWhere} = extract_options(Where),
    Starts = [LowerBound | Boundaries],
    Ends   = Boundaries ++ [UpperBound],
    [HdW | Ws] = make_w2(Starts, Ends, QField, NewWhere, []),
    %% add the head options to the head
    %% add the tail options to the tail
    %% reverse again
    [TW | Rest] = lists:reverse([lists:flatten(HdW ++ [HeadOption]) | Ws]),
    _Wheres = lists:reverse([lists:flatten(TW ++ [TailOption]) | Rest]).

make_w2([], [], _QField, _Where, Acc) ->
    lists:reverse(Acc);
make_w2([Start | T1], [End | T2], QField, Where, Acc) ->
    Where2 = swap(Where, QField, startkey, Start),
    Where3 = swap(Where2, QField, endkey, End),
    make_w2(T1, T2, QField, Where, [Where3 | Acc]).

extract_options(Where) ->
    {HeadOption, W1} = case lists:keytake(start_inclusive, 1, Where) of
                           false                  -> {[], Where};
                           {value, HdO, NewWhere} -> {HdO, NewWhere}
                       end,
    {TailOption, W2} = case lists:keytake(end_inclusive, 1, W1) of
                           false                  -> {[], W1};
                           {value, TO, NewWhere2} -> {TO, NewWhere2}
                       end,
    {HeadOption, TailOption, W2}.

%% this rewrite is premised on the fact the the Query field is a timestamp
swap(Where, QField, Key, Val) ->
    {Key, Fields} = lists:keyfind(Key, 1, Where),
    NewFields = lists:keyreplace(QField, 1, Fields, {QField, timestamp, Val}),
    _NewWhere = lists:keyreplace(Key, 1, Where, {Key, NewFields}).

%% going forward the compilation and restructuring of the queries will be a big piece of work
%% for the moment we just brute force assert that the query is a timeseries SQL request
%% and go with that
compile_where(DDL, Where) ->
    try
        case check_if_timeseries(DDL, Where) of
            {error, E}   -> {error, E};
            {true, NewW} -> NewW
        end
    catch throw:V -> V
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
find_quantum_fields(?DDL{ partition_key = #key_v1{ ast = PKAST } }) ->
    [quantum_fn_to_field_name(QuantumFunc) || #hash_fn_v1{ } = QuantumFunc <- PKAST].

%%
quantum_fn_to_field_name(#hash_fn_v1{ mod = riak_ql_quanta,
                                      fn = quantum,
                                      args = [?SQL_PARAM{name = [Name]}|_ ] }) ->
    Name.

check_if_timeseries(?DDL{table = T, partition_key = PK, local_key = LK0} = DDL,
                    [W]) ->
    try
        #key_v1{ast = PartitionKeyAST} = PK,
        PartitionFields = [X || ?SQL_PARAM{name = X} <- PartitionKeyAST],
        LK = LK0#key_v1{ast = lists:sublist(LK0#key_v1.ast, length(PartitionKeyAST))},
        QuantumFieldName = quantum_field_name(DDL),
        StrippedW = strip(W, []),
        {StartW, EndW, Filter} =
            break_out_timeseries(StrippedW, PartitionFields, QuantumFieldName),
        Mod = riak_ql_ddl:make_module_name(T),
        StartKey = rewrite(LK, StartW, Mod),
        EndKey = rewrite(LK, EndW, Mod),
        case has_errors(StartKey, EndKey) of
            [] ->
                %% defaults on startkey and endkey are different
                IncStart = case includes(StartW, '>', Mod) of
                               true  -> [{start_inclusive, false}];
                               false -> []
                           end,
                IncEnd = case includes(EndW, '<', Mod) of
                             true  -> [];
                             false -> [{end_inclusive, true}]
                         end,
                RewrittenFilter = add_types_to_filter(Filter, Mod),
                WhereProps1 = lists:flatten(
                    [{startkey, StartKey},
                     {endkey,   EndKey},
                     {filter,   RewrittenFilter},
                     IncStart,
                     IncEnd]),
                WhereProps2 = check_where_clause_is_possible(DDL, WhereProps1),
                WhereProps3 = rewrite_where_with_additional_filters(
                    Mod:additional_local_key_fields(),
                    local_key_field_orders(Mod:field_orders(), PK),
                    fun Mod:get_field_type/1,
                    WhereProps2),
                {true, WhereProps3};
            Errors ->
                {error, Errors}
        end
    catch
        error:{Reason, Description} = E when is_atom(Reason), is_binary(Description) ->
            {error, E};
        error:Reason ->
            %% if it is not a known error then return the stack trace for
            %% debugging
            {error, {where_not_timeseries, Reason, erlang:get_stacktrace()}}
    end;
check_if_timeseries(?DDL{}, []) ->
    {error, {no_where_clause, ?E_NO_WHERE_CLAUSE}}.

%%
has_errors(StartKey, EndKey) ->
    HasErrors = [EX || {error, EX} <- [StartKey, EndKey]],
    case HasErrors of
        [E,E] -> E;
        [E]   -> E;
        _     -> HasErrors
    end.

%% this is pretty brutal - it is assuming this is a time series query
%% if it isn't this clause is mince
includes([], _Op, _Mod) ->
    false;
includes([{Op1, Field, _} | T], Op2, Mod) ->
    Type = Mod:get_field_type([Field]),
    case Type of
        timestamp ->
            case Op1 of
                Op2 -> true;
                _   -> false
            end;
        _ ->
            includes(T, Op2, Mod)
    end.

%% find the upper and lower bound for the time
find_timestamp_bounds(QuantumField, LocalFields) ->
    find_timestamp_bounds2(QuantumField, LocalFields, [], {undefined, undefined}).

%%
find_timestamp_bounds2(_, [], OtherFilters, BoundsAcc) ->
    {lists:reverse(OtherFilters), BoundsAcc};
find_timestamp_bounds2(QuantumFieldName, [{or_, {_, QuantumFieldName, _}, _} | _], _, _) ->
    %% if this is an or state ment, lookahead at what is being tested, the quanta
    %% cannot be tested with an OR operator
    error({time_bounds_must_use_and_op, ?E_TIME_BOUNDS_MUST_USE_AND});
find_timestamp_bounds2(QuantumFieldName, [{Op, QuantumFieldName, _} = Filter | Tail],
                       OtherFilters, BoundsAcc1) ->
    %% if there are already end bounds throw an error
    if
        Op == '>' orelse Op == '>=' ->
            find_timestamp_bounds2(
              QuantumFieldName, Tail, OtherFilters, acc_lower_bounds(Filter, BoundsAcc1));
        Op == '<' orelse Op == '<=' ->
            find_timestamp_bounds2(
              QuantumFieldName, Tail, OtherFilters, acc_upper_bounds(Filter, BoundsAcc1));
        Op == '=' orelse Op == '!=' ->
            find_timestamp_bounds2(
              QuantumFieldName, Tail, [Filter | OtherFilters], BoundsAcc1)
    end;
find_timestamp_bounds2(QuantumFieldName, [Filter | Tail], OtherFilters, BoundsAcc1) ->
    %% this filter is not on the quantum
    find_timestamp_bounds2(QuantumFieldName, Tail, [Filter | OtherFilters], BoundsAcc1).

%%
acc_lower_bounds(Filter, {undefined, U}) ->
    {Filter, U};
acc_lower_bounds(_Filter, {_L, _}) ->
    error({lower_bound_specified_more_than_once, ?E_TSMSG_DUPLICATE_LOWER_BOUND}).

%%
acc_upper_bounds(Filter, {L, undefined}) ->
    {L, Filter};
acc_upper_bounds(_Filter, {_, _U}) ->
    error({upper_bound_specified_more_than_once, ?E_TSMSG_DUPLICATE_UPPER_BOUND}).

%%
break_out_timeseries(Filters1, PartitionFields1, no_quanta) ->
    {Body, Filters2} = split_key_from_filters(PartitionFields1, Filters1),
    {Body, Body, Filters2};
break_out_timeseries(Filters1, PartitionFields1, QuantumField) when is_binary(QuantumField) ->
    case find_timestamp_bounds(QuantumField, Filters1) of
        {_, {undefined, undefined}} ->
            %% if we don't have a time range then check for a time equality
            %% filter e.g. mytime = 12345, which is rewritten as
            %% mytime >= 12345 AND mytime <= 12345
            {QEqFilters, OtherFilters} =
                lists:partition(fun({Op, F, _}) ->
                                    Op == '=' andalso F == QuantumField
                                end, Filters1),
            case QEqFilters of
                [QEqFilter] ->
                    {Body, Filters2} = split_key_from_filters(PartitionFields1, OtherFilters),
                    Starts = setelement(1, QEqFilter, '>='),
                    Ends = setelement(1, QEqFilter, '<='),
                    {[Starts | Body], [Ends | Body], Filters2};
                [] ->
                    error({incomplete_where_clause, ?E_TSMSG_NO_BOUNDS_SPECIFIED});
                [_|_] ->
                    error(
                        {cannot_have_two_equality_filters_on_quantum_without_range,
                         ?E_CANNOT_HAVE_TWO_EQUALITY_FILTERS_ON_QUANTUM_WITHOUT_RANGE})
            end;
        {_, {_, undefined}} ->
            error({incomplete_where_clause, ?E_TSMSG_NO_UPPER_BOUND});
        {_, {undefined, _}} ->
            error({incomplete_where_clause, ?E_TSMSG_NO_LOWER_BOUND});
        {_, {{_,_,{_,Starts}}, {_,_,{_,Ends}}}} when is_integer(Starts),
                                                     is_integer(Ends),
                                                     Starts > Ends ->
            error({lower_bound_must_be_less_than_upper_bound,
                   ?E_TSMSG_LOWER_BOUND_MUST_BE_LESS_THAN_UPPER_BOUND});
        {_, {{GT,_,{_,Starts}}, {LT,_,{_,Ends}}}} when is_integer(Starts),
                                                       is_integer(Ends),
                                                       ((Starts == Ends andalso (GT /= '>=' orelse LT /= '<='))
                                                        orelse
                                                        ((Starts == (Ends - 1)) andalso GT /= '>=' andalso LT /= '<=')
                                                       ) ->
            %% Two scenarios:
            %% * Upper and lower bounds are the same, in which case
            %%   both comparison operators must include the equal sign
            %% * Upper and lower bounds are adjacent, in which case
            %%   one comparison operator must include the equal sign
            error({lower_and_upper_bounds_are_equal_when_no_equals_operator,
                   ?E_TSMSG_LOWER_AND_UPPER_BOUNDS_ARE_EQUAL_WHEN_NO_EQUALS_OPERATOR});
        {_, {{'>',_,{_,Starts}}, {'<',_,{_,Ends}}}} when is_integer(Starts),
                                                         is_integer(Ends),
                                                         Starts == (Ends - 1) ->
            %% catch when the filter values for time bounds are equal but we're
            %% using greater than or less than so could never match, if >= or <=
            %% were used on either side then
            error({lower_and_upper_bounds_are_equal_when_no_equals_operator,
                   ?E_TSMSG_LOWER_AND_UPPER_BOUNDS_ARE_EQUAL_WHEN_NO_EQUALS_OPERATOR});
        {Filters2, {Starts, Ends}} ->
            %% create the keys by splitting the key filters and prepending it
            %% with the time bound.
            {Body, Filters3} = split_key_from_filters(PartitionFields1, Filters2),
            {[Starts | Body], [Ends | Body], Filters3}
    end.

%% separate the key fields from the other filters
split_key_from_filters(LocalFields, Filters) ->
    lists:mapfoldl(fun split_key_from_filters2/2, Filters, LocalFields).

%%
split_key_from_filters2([FieldName], Filters) when is_binary(FieldName) ->
    take_key_field(FieldName, Filters, []).

%%
take_key_field(FieldName, [], Acc) when is_binary(FieldName) ->
    %% check if the field exists in the clause but used the wrong operator or
    %% it never existed at all. Give a more helpful message if the wrong op was
    %% used.
    case lists:keyfind(FieldName, 2, Acc) of
        false ->
            Reason = ?E_KEY_FIELD_NOT_IN_WHERE_CLAUSE(FieldName);
        {Op, _, _} ->
            Reason = ?E_KEY_PARAM_MUST_USE_EQUALS_OPERATOR(FieldName, Op)
    end,
    error({missing_key_clause, Reason});
take_key_field(FieldName, [{'=', FieldName, _} = Field | Tail], Acc) ->
    {Field, Acc ++ Tail};
take_key_field(FieldName, [Field | Tail], Acc) ->
    take_key_field(FieldName, Tail, [Field | Acc]).

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
add_types2([{NullOp, {identifier, Field}} | T], Mod, Acc) when NullOp =:= is_null orelse
                                                                 NullOp =:= is_not_null ->
    EqOp = case NullOp of
        is_null -> '=';
        is_not_null -> '!='
    end,
    %% cast to varchar since nullable types do not exist w/i the leveldb backend,
    %% otherwise said NULL as [] bleeds due to basic datatype selection.
    NewType = 'varchar',
    NewAcc = {EqOp, {field, Field, NewType}, {const, ?SQL_NULL}},
    add_types2(T, Mod, [NewAcc | Acc]);
add_types2([{Op, Field, {_, Val}} | T], Mod, Acc) ->
    NewType = riak_ql_ddl:get_storage_type(Mod:get_field_type([Field])),
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
rewrite2([?SQL_PARAM{name = [FieldName]} | T], Where1, Mod, Acc) ->
    Type = Mod:get_field_type([FieldName]),
    case lists:keytake(FieldName, 2, Where1) of
        false                           ->
            {error, {missing_param, ?E_MISSING_PARAM_IN_WHERE_CLAUSE(FieldName)}};
        {value, {_, _, {_, Val}}, Where2} ->
            rewrite2(T, Where2, Mod, [{FieldName, Type, Val} | Acc])
    end.

%% Functions to assist with coverage chunks that redefine quanta ranges
-spec unwrap_cover(undefined | binary()) ->
                          {ok, {undefined, undefined} |
                           {OpaqueContext::binary(), {FieldName::binary(), Range::tuple()}}} |
                          {error, invalid_coverage_context_checksum}.
unwrap_cover(undefined) ->
    {ok, {undefined, undefined}};
unwrap_cover(Cover) when is_binary(Cover) ->
    case catch riak_kv_pb_coverage:checksum_binary_to_term(Cover) of
        {ok, {Proplist, {FieldName, RangeTuple}}} ->
            {ok, {riak_kv_pb_coverage:term_to_checksum_binary(Proplist),
             {FieldName, RangeTuple}}};

        %% As of 1.6 or the equivalent merged KV version, we can start
        %% generating simpler coverage chunks for timeseries that are
        %% pure property lists instead of a tuple with the
        %% `vnode_hash'/`node' property list as first element and the
        %% local where clause parameters as second.
        {ok, Proplist} ->
            FieldName = proplists:get_value(ts_where_field, Proplist),
            RangeTuple = proplists:get_value(ts_where_range, Proplist),
            {ok, {Cover, {FieldName, RangeTuple}}};

        {error, invalid_checksum} ->
            {error, invalid_coverage_context_checksum}
    end.

update_where_for_cover(Where, undefined) ->
    Where;
update_where_for_cover(Where, {FieldName, RangeTuple}) ->
    update_where_for_cover(Where, FieldName, RangeTuple).

update_where_for_cover(Props, Field, {{StartVal, StartInclusive},
                                      {EndVal, EndInclusive}}) ->
    %% Sample data structure:
    %% 'WHERE' = [{startkey,[{<<"field1">>,varchar,<<"f1">>},
    %%                       {<<"field2">>,varchar,<<"f2">>},
    %%                       {<<"time">>,timestamp,15000}]},
    %%            {endkey,[{<<"field1">>,varchar,<<"f1">>},
    %%                     {<<"field2">>,varchar,<<"f2">>},
    %%                     {<<"time">>,timestamp,20000}]},
    %%            {filter,[]},
    %%            {end_inclusive,true}],

    %% Changes to apply:
    %%   Modify the Field 3-tuple in the startkey and endkey properties
    %%   Drop end_inclusive, start_inclusive properties
    %%   Add new end_inclusive, start_inclusive properties based on the parameters
    %%   Retain any other properties (currently only `filter')

    NewStartKeyVal = modify_where_key(proplists:get_value(startkey, Props),
                                     Field, StartVal),
    NewEndKeyVal = modify_where_key(proplists:get_value(endkey, Props),
                                   Field, EndVal),

    SlimProps =
        lists:foldl(
          fun(Prop, Acc) -> proplists:delete(Prop, Acc) end,
          Props,
          [startkey, endkey, end_inclusive, start_inclusive]),

    [{startkey, NewStartKeyVal}, {endkey, NewEndKeyVal},
     {start_inclusive, StartInclusive}, {end_inclusive, EndInclusive}] ++
        SlimProps.

modify_where_key(TupleList, Field, NewVal) ->
    {Field, FieldType, _OldVal} = lists:keyfind(Field, 1, TupleList),
    lists:keyreplace(Field, 1, TupleList, {Field, FieldType, NewVal}).

resolve_expressions(Mod, WhereAST) ->
    Acc = acc, %% not used
    riak_ql_ddl:mapfold_where_tree(
        fun (_, Op, Acc_x) when Op == and_; Op == or_ ->
                {ok, Acc_x};
            (_, Filter, Acc_x) ->
                {resolve_expressions_folder(Mod, Filter), Acc_x}
        end, Acc, WhereAST).

resolve_expressions_folder(_Mod, {ExpOp,{_,A},{_,B}}) when ExpOp == '+'; ExpOp == '*';
                                                           ExpOp == '-'; ExpOp == '/' ->
    Value =
        case ExpOp of
            '+' -> riak_ql_window_agg_fns:add(A,B);
            '*' -> riak_ql_window_agg_fns:multiply(A,B);
            '-' -> riak_ql_window_agg_fns:subtract(A,B);
            '/' -> riak_ql_window_agg_fns:divide(A,B)
        end,
    {calculated, Value};
resolve_expressions_folder(Mod, {ExpOp,FieldName,{calculated,V1}}) when is_binary(FieldName) ->
    V2 = cast_value_to_ast(Mod:get_field_type([FieldName]),V1),
    {ExpOp,FieldName,V2};
resolve_expressions_folder(_, AST) ->
    AST.

cast_value_to_ast(T, V) when (T == integer orelse T == timestamp), is_integer(V) -> {T, V};
cast_value_to_ast(T, V) when (T == integer orelse T == timestamp), is_float(V)   -> {T, erlang:round(V)};
cast_value_to_ast(double, V) when is_integer(V) -> {double, float(V)};
cast_value_to_ast(double, V) when is_float(V)   -> {double, V}.

-record(filtercheck, {name,'=','>','>=','<','<='}).

check_where_clause_is_possible(DDL, WhereProps) ->
    Filter1 = proplists:get_value(filter, WhereProps),
    {Filter2, FilterChecks} = riak_ql_ddl:mapfold_where_tree(
        fun (_, and_, Acc) ->
                {ok, Acc};
            (_, or_, Acc) ->
                {skip, Acc};
            (Conditional, Filter, Acc) ->
                check_where_clause_is_possible_fold(DDL, Conditional, Filter, Acc)
        end, [{eliminate_later,[]}], Filter1),
    ElimLater = proplists:get_value(eliminate_later, FilterChecks),
    {Filter3, _} = riak_ql_ddl:mapfold_where_tree(
        fun (_, and_, Acc) ->
                {ok, Acc};
            (_, or_, Acc) ->
                {skip, Acc};
            (_, Filter, Acc) ->
                case lists:member(Filter, ElimLater) of
                  true ->
                      {eliminate, Acc};
                  false ->
                      {Filter, Acc}
                end
        end, [], Filter2),
    lists:keystore(filter, 1, WhereProps, {filter,Filter3}).

check_where_clause_is_possible_fold(DDL, _, {'=',{field,FieldName,_},{const,?SQL_NULL}} = F, Acc) ->
    %% `IS NULL` filters
    %% if a column is marked NOT NULL, but the WHERE clause has IS NULL for that
    %% column then no results will ever be returned, so do not execute!
    case is_field_nullable(FieldName, DDL) of
        true ->
            {F, Acc};
        false ->
            throw({error, {impossible_where_clause, << >>}})
    end;
check_where_clause_is_possible_fold(DDL, _, {'!=',{field,FieldName,_},{const,?SQL_NULL}} = F, Acc) ->
    %% `NOT NULL` filters
    %% if column is marked NOT NULL, and the WHERE clause has IS NOT NULL for
    %% that column then we can eliminate it, and reduce the filter, maybe to
    %% nothing which means leveldb would not have to decode the rows!
    case is_field_nullable(FieldName, DDL) of
        true ->
            {F, Acc};
        false ->
            {eliminate, Acc}
    end;
check_where_clause_is_possible_fold(_, _, {'=',{field,FieldName,_},{const,EqVal}} = F, Acc) ->
    case find_filter_check(FieldName, Acc) of
        #filtercheck{'=' = F} ->
            %% this filter has been specified twice so remove the second occurence
            {eliminate, Acc};
        #filtercheck{'>' = {_,_,{const,GtVal}} = GtF} = Check1 when GtVal < EqVal ->
            %% query like `a = 10 AND a > 8` we can eliminate the greater than
            %% clause because if a to be 10 it must also be greater than 8
            Acc2 = append_to_eliminate_later(GtF, Acc),
            Check2 = Check1#filtercheck{'>' = undefined, '=' = F},
            {F, lists:keystore(FieldName, #filtercheck.name, Acc2, Check2)};
        #filtercheck{'>=' = {_,_,{const,GteVal}} = GteF} = Check1 when GteVal < EqVal ->
            %% query like `a = 10 AND a > 8` we can eliminate the greater than
            %% clause because if a to be 10 it must also be greater than 8
            Acc2 = append_to_eliminate_later(GteF, Acc),
            Check2 = Check1#filtercheck{'>=' = undefined, '=' = F},
            {F, lists:keystore(FieldName, #filtercheck.name, Acc2, Check2)};
        #filtercheck{'<' = {_,_,{const,LtVal}}} when LtVal =< EqVal ->
            %% query requires a column to be equal to a value AND less than that
            %% value which is impossible
            throw({error, {impossible_where_clause, << >>}});
        #filtercheck{'<=' = {_,_,{const,LteVal}} = LteF} = Check1 when LteVal >= EqVal ->
            %% query like `a = 10 AND a <= 10` the less than or equals can be
            %% eliminated because it is already captured in the equality filter
            Acc2 = append_to_eliminate_later(LteF, Acc),
            Check2 = Check1#filtercheck{'<=' = undefined, '=' = F},
            {F, lists:keystore(FieldName, #filtercheck.name, Acc2, Check2)};
        #filtercheck{'=' = F_x} when F_x /= undefined ->
            %% there are two different checks on the same column, for equality
            %% this can never be satisfied.
            throw({error, {impossible_where_clause, << >>}});
        #filtercheck{'<=' = {_,_,{const,LteVal}}} when LteVal < EqVal ->
            %% query like `a = 10 AND a <= 8`
            throw({error, {impossible_where_clause, << >>}});
        #filtercheck{'=' = undefined} = Check1 ->
            %% this is the first equality check so just record it
            Check2 = Check1#filtercheck{'=' = F},
            {F, lists:keystore(FieldName, #filtercheck.name, Acc, Check2)}
    end;
check_where_clause_is_possible_fold(_, _, {'>',{field,FieldName,_},{const,GtVal}} = F, Acc) ->
    case find_filter_check(FieldName, Acc) of
        #filtercheck{'>' = F} ->
            %% this filter has been specified twice so remove the second occurence
            {eliminate, Acc};
        #filtercheck{'=' = {_,_,{const,EqVal}}} when GtVal >= EqVal ->
            %% query like `a = 10 AND a > 10` cannot be satisfied
            throw({error, {impossible_where_clause, << >>}});
        #filtercheck{'=' = {_,_,{const,EqVal}}} when GtVal < EqVal ->
            %% query like `a = 10 AND a > 8` we can eliminate the greater than
            %% clause because if a to be 10 it must also be greater than 8
            {eliminate, Acc};
        #filtercheck{'>=' = {_,_,{const,GteVal}} = F_x} = Check1 when GtVal >= GteVal ->
            %% query like `a >= 8 AND a > 10` we can eliminate the lesser clause
            %% because if the value must be greater than 10 it can't be 8 or 9.
            Check2 = Check1#filtercheck{'>' = F, '>=' = undefined},
            Acc2 = store_filter_check(Check2, Acc),
            Acc3 = append_to_eliminate_later(F_x, Acc2),
            {F,Acc3};
        #filtercheck{'>=' = {_,_,{const,GteVal}}} when GtVal < GteVal ->
            %% query like `b >= 10 AND b > 9`, the previous >= clause should be
            %% eliminated on the second pass
            {eliminate, Acc};
        #filtercheck{'>' = undefined} = Check1 ->
            %% this is the first greater than check so just record it
            Check2 = Check1#filtercheck{'>' = F},
            {F, lists:keystore(FieldName, #filtercheck.name, Acc, Check2)};
        #filtercheck{'>' = F_x} = Check1 when F_x < F ->
            %% eliminate the lower > value for this column
            Check2 = Check1#filtercheck{'>' = F},
            Acc2 = store_filter_check(Check2, Acc),
            Acc3 = append_to_eliminate_later(F_x, Acc2),
            {F,Acc3};
        #filtercheck{'>' = F_x} when F_x > F ->
            %% we already have a filter that is higher than this one, so
            %% eliminate
            {eliminate, Acc}
    end;
check_where_clause_is_possible_fold(_, _, {'>=',{field,FieldName,_},{const,GteVal}} = F, Acc) ->
    case find_filter_check(FieldName, Acc) of
        #filtercheck{'=' = {_,_,{const,EqVal}}} when GteVal =< EqVal ->
            %% query like `a = 10 AND a >= 8` we can eliminate the greater than
            %% or equal to clause because if a to be 10 it must also be greater
            %% than 8
            {eliminate, Acc};
        #filtercheck{'=' = {_,_,{const,EqVal}}} when GteVal > EqVal ->
            %% query like `a = 10 AND a >= 11` cannot be satisfied
            throw({error, {impossible_where_clause, << >>}});
        #filtercheck{'>' = {_,_,{const,GtVal}} = F_x} = Check1 when GtVal < GteVal ->
            %% query like `a > 6 AND a >= 8` we can eliminate the lesser '>' filter
            Check2 = Check1#filtercheck{'>=' = F},
            Acc2 = store_filter_check(Check2, Acc),
            Acc3 = append_to_eliminate_later(F_x, Acc2),
            {F, Acc3};
        #filtercheck{'>' = {_,_,{const,GtVal}}} when GtVal >= GteVal ->
            %% we already have a filter that is higher than the second one
            {eliminate, Acc};
        #filtercheck{'>=' = undefined} = Check1 ->
            %% this is the first equality check so just record it
            {F, store_filter_check(Check1#filtercheck{'>=' = F}, Acc)};
        #filtercheck{'>=' = F} ->
            %% this filter has been specified twice so remove the second occurence
            {eliminate, Acc};
        #filtercheck{'>=' = F_x} = Check1 when F_x < F ->
            %% we already have a filter that is a lower value so eliminate it
            Check2 = Check1#filtercheck{'>=' = F},
            Acc2 = store_filter_check(Check2, Acc),
            Acc3 = append_to_eliminate_later(F_x, Acc2),
            {F, Acc3};
        #filtercheck{'>=' = F_x} when F_x > F ->
            %% we already have a filter that is higher than this one
            {eliminate, Acc}
    end;
check_where_clause_is_possible_fold(_, _, {'<',{field,FieldName,_},{const,LtVal}} = F, Acc) ->
    case find_filter_check(FieldName, Acc) of
        #filtercheck{'<=' = {_,_,{const,LteVal}} = F_x} = Check1 when LteVal >= LtVal ->
            %% query like `a <= 10 AND a < 10`, less than or equal is eliminated
            Check2 = Check1#filtercheck{'<' = F, '<=' = undefined},
            Acc2 = store_filter_check(Check2, Acc),
            Acc3 = append_to_eliminate_later(F_x, Acc2),
            {F, Acc3};
        #filtercheck{'<=' = {_,_,{const,LteVal}}} when LteVal < LtVal ->
            {eliminate, Acc};
        #filtercheck{'=' = {_,_,{const,EqVal}}} when LtVal > EqVal ->
            %% existing equality check is less, eliminate this `>` filter.
            {eliminate, Acc};
        #filtercheck{'=' = {_,_,{const,EqVal}}} when LtVal =< EqVal ->
            %% query requires a column to be equal to a value AND less than that
            %% value which is impossible
            throw({error, {impossible_where_clause, << >>}});
        #filtercheck{'<' = F} ->
            %% duplicate < filter
            {eliminate, Acc};
        #filtercheck{'<' = undefined} = Check1 ->
            %% this is the first less than check so just record it
            Check2 = Check1#filtercheck{'<' = F},
            {F, lists:keystore(FieldName, #filtercheck.name, Acc, Check2)};
        #filtercheck{'<' = F_x} when F_x < F ->
            %% we already have a filter that is higher than this one,
            %% which we can eliminate. Store it and eliminate it on the second
            %% pass since it has already been iterated
            {eliminate, Acc};
        #filtercheck{'<' = F_x} = Check1 when F_x > F ->
            %% we already have a filter that is higher than this one, and so
            %% includes it so we can safely eliminate this one.
            Check2 = Check1#filtercheck{'<' = F},
            Acc2 = store_filter_check(Check2, Acc),
            Acc3 = append_to_eliminate_later(F_x, Acc2),
            {F, Acc3}
    end;
check_where_clause_is_possible_fold(_, _, {'<=',{field,FieldName,_},{const,LteVal}} = F, Acc) ->
    case find_filter_check(FieldName, Acc) of
        #filtercheck{'<=' = F} ->
            %% duplicate <= filter
            {eliminate, Acc};
        #filtercheck{'=' = {_,_,{const,EqVal}}} when LteVal >= EqVal ->
            %% query like `a = 10 AND a <= 10` the less than or equals can be
            %% eliminated because it is already captured in the equality filter
            {eliminate, Acc};
        #filtercheck{'=' = {_,_,{const,EqVal}}} when LteVal < EqVal ->
            %% query like `a = 10 AND a <= 8`
            throw({error, {impossible_where_clause, << >>}});
        #filtercheck{'<' = {_,_,{const,LtVal}}} when LteVal >= LtVal ->
            %% query like `a < 10 AND a <= 10`
            {eliminate, Acc};
        #filtercheck{'<' = {_,_,{const,LtVal}} = F_x} = Check1 when LteVal < LtVal ->
            %% query like `a < 11 AND a <= 10`
            Check2 = Check1#filtercheck{'<' = undefined, '<=' = F},
            Acc2 = store_filter_check(Check2, Acc),
            Acc3 = append_to_eliminate_later(F_x, Acc2),
            {F, Acc3};
        #filtercheck{'<=' = undefined} = Check1 ->
            %% this is the first less than check so just record it
            Check2 = Check1#filtercheck{'<=' = F},
            {F, lists:keystore(FieldName, #filtercheck.name, Acc, Check2)}
    end;
check_where_clause_is_possible_fold(_, _, Filter, Acc) ->
    {Filter, Acc}.

append_to_eliminate_later(Filter, Acc) ->
    ElimLater = proplists:get_value(eliminate_later, Acc),
    lists:keystore(eliminate_later, 1, Acc, {eliminate_later, [Filter|ElimLater]}).

store_filter_check(#filtercheck{name = FieldName} = Check, Acc) ->
    lists:keystore(FieldName, #filtercheck.name, Acc, Check).

%% TODO put this in the table helper module
is_field_nullable(FieldName, ?DDL{fields = Fields}) when is_binary(FieldName)->
    #riak_field_v1{optional = Optional} = lists:keyfind(FieldName, #riak_field_v1.name, Fields),
    (Optional == true).

find_filter_check(FieldName, Acc) when is_binary(FieldName) ->
    case lists:keyfind(FieldName, #filtercheck.name, Acc) of
      false ->
          #filtercheck{name=FieldName};
      Val ->
          Val
    end.

%% tl;dr put filters that test equality of columns in the local key but not in
%% in the partition key, in the star and end key.
%%
%% PRIMARY KEY((quantum(a,1,'m')),a,b)
%%
%% SELECT * FROM table WHERE a > 2 and a < 6 AND b = 5
%%
%% Instead of putting b just in the filter, put it in the start and end keys.
%% This narrows the range from everything between {2,_} and {6,_} to everyting
%% between {2,5} and {6,5}.
%%
%% For equality filters, it is not safe to remove the filter, because {3,7} is
%% also in the range but should not be returned because only rows where b = 5
%% are correct for this query.
rewrite_where_with_additional_filters([], _, _, WhereProps) ->
    WhereProps;
rewrite_where_with_additional_filters(AdditionalFields, FieldOrders, ToKeyTypeFn, WhereProps) when is_list(WhereProps) ->
    SKey1 = proplists:get_value(startkey, WhereProps),
    EKey1 = proplists:get_value(endkey, WhereProps),
    SInc1 = proplists:get_value(start_inclusive, WhereProps),
    EInc1 = proplists:get_value(end_inclusive, WhereProps),
    Filter = proplists:get_value(filter, WhereProps),
    AdditionalFilter = find_filters_on_additional_local_key_fields(
        AdditionalFields, Filter),
    {SKey2, SInc2} = rewrite_start_key_with_filters(
        AdditionalFields, FieldOrders, ToKeyTypeFn, AdditionalFilter, SKey1, SInc1),
    {EKey2, EInc2} = rewrite_end_key_with_filters(
        AdditionalFields, FieldOrders, ToKeyTypeFn, AdditionalFilter, EKey1, EInc1),
    lists:foldl(
        fun({K,V},Acc) -> store_to_where_props(K,V,Acc) end, WhereProps,
        [{startkey,SKey2}, {endkey,EKey2},
         {start_inclusive,SInc2}, {end_inclusive,EInc2}]).

store_to_where_props(Key, Val, WhereProps) when Val /= undefined ->
    lists:keystore(Key, 1, WhereProps, {Key, Val});
store_to_where_props(_, _, WhereProps) ->
    WhereProps.

rewrite_start_key_with_filters([], _, _, _, SKey, SInc) ->
    {SKey, SInc};
rewrite_start_key_with_filters([AddFieldName|Tail], [Order|TailOrder], ToKeyTypeFn, Filter, SKey, SInc1) when is_binary(AddFieldName) ->
    case proplists:get_value(AddFieldName, Filter) of
        {Op,_,_} = F  when Op == '=' orelse (Order == ascending andalso (Op == '>'orelse Op == '>='))
                                     orelse (Order == descending andalso (Op == '<'orelse Op == '<=')) ->
            SKeyElem = to_key_elem(ToKeyTypeFn, F),
            %% if the quantum was inclusive, make sure we stay inclusive or
            %% the quantum boundary is modified later on
            SInc2 = ((SInc1 /= false) or is_inclusive_op(Op)),
            rewrite_start_key_with_filters(
                Tail, TailOrder, ToKeyTypeFn, Filter, SKey++[SKeyElem], SInc2);
        _ ->
            %% there is no filter for the next additional field so give up! The
            %% filters must be consecutive, we can't have a '_' inbetween
            %% values
            {SKey, SInc1}
    end.

rewrite_end_key_with_filters([], _, _, _, EKey, EInc) ->
    {EKey, EInc};
rewrite_end_key_with_filters([AddFieldName|Tail], [Order|TailOrder], ToKeyTypeFn, Filter, EKey, EInc1) when is_binary(AddFieldName) ->
    case proplists:get_value(AddFieldName, Filter) of
        {Op,_,_} = F  when Op == '=' orelse (Order == ascending andalso  (Op == '<' orelse Op == '<='))
                                     orelse (Order == descending andalso  (Op == '>' orelse Op == '>=')) ->
            EKeyElem = to_key_elem(ToKeyTypeFn, F),
            %% if the quantum was inclusive, make sure we stay inclusive or
            %% the quantum boundary is modified later on
            EInc2 = ((EInc1 /= false) or is_inclusive_op(Op)),
            rewrite_end_key_with_filters(
                Tail, TailOrder, ToKeyTypeFn, Filter, EKey++[EKeyElem], EInc2);
        _ ->
            %% there is no filter for the next additional field so give up! The
            %% filters must be consecutive, we can't have a '_' inbetween
            %% values
            {EKey, EInc1}
    end.

is_inclusive_op('=')  -> true;
is_inclusive_op('>')  -> false;
is_inclusive_op('>=') -> true;
is_inclusive_op('<') -> false;
is_inclusive_op('<=') -> true.

%% Convert a filter to a start/end key element
to_key_elem(ToKeyTypeFn, {_,{field,FieldName,_},{_,Val}}) ->
    {FieldName,ToKeyTypeFn([FieldName]),Val}.

%% Return filters where the column is in the local key but not the partition key
find_filters_on_additional_local_key_fields(AdditionalFields, Where) ->
    try
        riak_ql_ddl:fold_where_tree(
            fun(Conditional, Filter, Acc) ->
                find_filters_on_additional_local_key_fields_folder(Conditional, Filter, AdditionalFields, Acc)
            end, [], Where)
    catch throw:Val -> Val %% a throw means the function wanted to exit immediately
    end.

%%
find_filters_on_additional_local_key_fields_folder(or_,_,_,_) ->
    %% we cannot support filters using OR, because keys must be a singular value
    throw([]);
find_filters_on_additional_local_key_fields_folder(_, {'!=',_,_}, _, Acc) ->
    %% we can't support additional NOT filters on the key
    Acc;
find_filters_on_additional_local_key_fields_folder(_, {_,{field,Name,_},_} = F, AdditionalFields, Acc) when is_binary(Name) ->
    case lists:member(Name, AdditionalFields) of
        true ->
            [{Name, F}|Acc];
        false ->
            Acc
    end.

%% -------------------------------------------------------------------
%% TESTS
%% -------------------------------------------------------------------

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-define(
    assertPropsEqual(Expected1, Actual1),
    Expected2 = lists:sort(Expected1),
    Actual2 = lists:sort(Actual1),
    ?assertEqual(lists:sort(Expected2), lists:sort(Actual2))
).

%%
%% Helper Fns for unit tests
%%

-define(MIN, 60 * 1000).
-define(NAME, "time").

is_query_valid(?DDL{table = Table} = DDL, Q) ->
    Mod = riak_ql_ddl:make_module_name(Table),
    riak_ql_ddl:is_query_valid(Mod, DDL, riak_kv_ts_util:sql_record_to_tuple(Q)).

get_query(String) ->
    get_query(String, undefined).
get_query(String, Cover) ->
    Lexed = riak_ql_lexer:get_tokens(String),
    {ok, Q} = riak_ql_parser:parse(Lexed),
    riak_kv_ts_util:build_sql_record(select, Q, [{cover, Cover}]).

get_long_ddl() ->
    SQL = "CREATE TABLE GeoCheckin " ++
        "(geohash varchar not null, " ++
        "location varchar not null, " ++
        "user varchar not null, " ++
        "extra sint64 not null, " ++
        "more double not null, " ++
        "time timestamp not null, " ++
        "myboolean boolean not null," ++
        "weather varchar not null, " ++
        "temperature varchar, " ++
        "PRIMARY KEY((location, user, quantum(time, 15, 's')), " ++
        "location, user, time))",
    get_ddl(SQL).

get_standard_ddl() ->
    get_ddl(
      "CREATE TABLE GeoCheckin "
      "(geohash varchar not null, "
      "location varchar not null, "
      "user varchar not null, "
      "time timestamp not null, "
      "weather varchar not null, "
      "temperature varchar, "
      "PRIMARY KEY((location, user, quantum(time, 15, 's')), "
      "location, user, time))").

get_ddl(SQL) ->
    Lexed = riak_ql_lexer:get_tokens(SQL),
    {ddl, DDL, _WithProps} = riak_ql_parser:ql_parse(Lexed),
    {module, _Module} = riak_ql_ddl_compiler:compile_and_load_from_tmp(DDL),
    DDL.

get_standard_pk() ->
    #key_v1{ast = [
                   ?SQL_PARAM{name = [<<"location">>]},
                   ?SQL_PARAM{name = [<<"user">>]},
                   #hash_fn_v1{mod = riak_ql_quanta,
                               fn = quantum,
                               args = [
                                       ?SQL_PARAM{name = [<<"time">>]},
                                       15,
                                       s
                                      ],
                               type = timestamp}
                  ]
           }.

get_standard_lk() ->
    #key_v1{ast = [
                   ?SQL_PARAM{name = [<<"location">>]},
                   ?SQL_PARAM{name = [<<"user">>]},
                   ?SQL_PARAM{name = [<<"time">>]}
                  ]}.

%%
%% Unit tests
%%

%%
%% tests for adding type information and rewriting filters
%%

simple_filter_typing_test() ->
    ?DDL{table = T} = get_long_ddl(),
    Mod = riak_ql_ddl:make_module_name(T),
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
                 {'=', {field, <<"weather">>, varchar}, {const, <<"yankee">>}},
                 {and_,
                  {'=', {field, <<"geohash">>,     varchar}, {const, <<"erko">>}},
                  {'=', {field, <<"temperature">>, varchar}, {const, <<"yelp">>}}
                 }
                },
                {'=', {field, <<"extra">>, sint64}, {const, 1}}
               },
    ?assertEqual(Expected, Got).

%%
%% test for IS [NOT] NULL filters
%%
is_null_filter_typing_test() ->
    ?DDL{table = T} = get_long_ddl(),
    Mod = riak_ql_ddl:make_module_name(T),
    Filter = [
               {and_,
                   {is_null, {identifier, <<"weather">>}},
                   {is_not_null, {identifier, <<"temperature">>}}
               }
             ],
    Got = add_types_to_filter(Filter, Mod),
    Expected = {and_,
                {'=', {field, <<"weather">>, varchar}, {const, ?SQL_NULL}},
                {'!=', {field, <<"temperature">>, varchar}, {const, ?SQL_NULL}}
               },
    ?assertEqual(Expected, Got).

%%
%% rewrite passing tests
%%
%% success here is because the where clause covers the entire local key
%% we have enough info to build a range scan
%%
simple_rewrite_test() ->
    ?DDL{table = T} = get_standard_ddl(),
    Mod = riak_ql_ddl:make_module_name(T),
    LK  = #key_v1{ast = [
                         ?SQL_PARAM{name = [<<"geohash">>]},
                         ?SQL_PARAM{name = [<<"time">>]}
                        ]},
    W   = [
           {'=', <<"geohash">>, {word, "yardle"}},
           {'>', <<"time">>,    {int,   678}}
          ],
    Exp = [
           {<<"geohash">>,  varchar,   "yardle"},
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
    ?DDL{table = T} = get_standard_ddl(),
    Mod = riak_ql_ddl:make_module_name(T),
    LK  = #key_v1{ast = [
                         ?SQL_PARAM{name = [<<"geohash">>]},
                         ?SQL_PARAM{name = [<<"user">>]}
                        ]},
    W   = [
           {'=', <<"geohash">>, {"word", "yardle"}}
          ],
    ?assertEqual(
       {error, {missing_param, ?E_MISSING_PARAM_IN_WHERE_CLAUSE("user")}},
       rewrite(LK, W, Mod)
      ).

simple_rewrite_fail_2_test() ->
    ?DDL{table = T} = get_standard_ddl(),
    Mod = riak_ql_ddl:make_module_name(T),
    LK  = #key_v1{ast = [
                         ?SQL_PARAM{name = [<<"geohash">>]},
                         ?SQL_PARAM{name = [<<"user">>]}
                        ]},
    W   = [
           {'=', <<"user">>, {"word", "yardle"}}
          ],
    ?assertEqual(
       {error, {missing_param, ?E_MISSING_PARAM_IN_WHERE_CLAUSE("geohash")}},
       rewrite(LK, W, Mod)
      ).

simple_rewrite_fail_3_test() ->
    ?DDL{table = T} = get_standard_ddl(),
    Mod = riak_ql_ddl:make_module_name(T),
    LK  = #key_v1{ast = [
                         ?SQL_PARAM{name = [<<"geohash">>]},
                         ?SQL_PARAM{name = [<<"user">>]},
                         ?SQL_PARAM{name = [<<"temperature">>]}
                        ]},
    W   = [
           {'=', <<"geohash">>, {"word", "yardle"}}
          ],
    %% TODO only returns error info about the first missing param, temperature
    %%      should also be in the error message.
    ?assertEqual(
       {error, {missing_param, ?E_MISSING_PARAM_IN_WHERE_CLAUSE("user")}},
       rewrite(LK, W, Mod)
      ).

%%
%% complete query passing tests
%%

simplest_test() ->
    DDL = get_standard_ddl(),
    Query =
        "select weather from GeoCheckin where time > 3000"
        " and time < 5000 and user = 'user_1' and location = 'San Francisco'",
    {ok, Q} = get_query(Query),
    true = is_query_valid(DDL, Q),
    [ExpectedWhere] =
        test_data_where_clause(<<"San Francisco">>, <<"user_1">>, [{3001, 5000}]),
    {ok, [?SQL_SELECT{ 'WHERE'       = WhereVal,
                       partition_key = PK,
                       local_key     = LK }]} = compile(DDL, Q),
    ?assertEqual(get_standard_pk(), PK),
    ?assertEqual(get_standard_lk(), LK),
    ?assertEqual(ExpectedWhere, WhereVal).

simple_with_filter_1_test() ->
    {ok, Q} = get_query(
                "SELECT weather FROM GeoCheckin "
                "WHERE time > 3000 AND time < 5000 "
                "AND user = 'user_1' AND location = 'Scotland' "
                "AND weather = 'yankee'"
               ),
    DDL = get_standard_ddl(),
    true = is_query_valid(DDL, Q),
    [[StartKey, EndKey |_]] =
        test_data_where_clause(<<"Scotland">>, <<"user_1">>, [{3001, 5000}]),
    ExpectedWhere = [
             StartKey,
             EndKey,
             {filter, {'=', {field, <<"weather">>, varchar}, {const, <<"yankee">>}}}
            ],
    {ok, [?SQL_SELECT{ 'WHERE'       = WhereVal,
                       partition_key = PK,
                       local_key     = LK }]} = compile(DDL, Q),
    ?assertEqual(get_standard_pk(), PK),
    ?assertEqual(get_standard_lk(), LK),
    ?assertEqual(ExpectedWhere, WhereVal).

simple_with_filter_2_test() ->
    {ok, Q} = get_query(
                "SELECT weather FROM GeoCheckin "
                "WHERE time >= 3000 AND time < 5000 "
                "AND user = 'user_1' AND location = 'Scotland' "
                "AND weather = 'yankee'"
               ),
    DDL = get_standard_ddl(),
    true = is_query_valid(DDL, Q),
    [[StartKey, EndKey |_]] =
        test_data_where_clause(<<"Scotland">>, <<"user_1">>, [{3000, 5000}]),
    ExpectedWhere = [
             StartKey,
             EndKey,
             {filter,   {'=', {field, <<"weather">>, varchar}, {const, <<"yankee">>}}}
            ],
    {ok, [?SQL_SELECT{ 'WHERE'       = WhereVal,
                       partition_key = PK,
                       local_key     = LK }]} = compile(DDL, Q),
    ?assertEqual(get_standard_pk(), PK),
    ?assertEqual(get_standard_lk(), LK),
    ?assertEqual(ExpectedWhere, WhereVal).

simple_with_filter_3_test() ->
    {ok, Q} = get_query(
                "SELECT weather FROM GeoCheckin "
                "WHERE time > 3000 AND time <= 5000 "
                "AND user = 'user_1' AND location = 'Scotland' "
                "AND weather = 'yankee'"
               ),
    DDL = get_standard_ddl(),
    true = is_query_valid(DDL, Q),
    [[StartKey, EndKey |_]] =
        test_data_where_clause(<<"Scotland">>, <<"user_1">>, [{3001, 5000}]),
    PK = get_standard_pk(),
    LK = get_standard_lk(),
    ExpectedWhere = [
             StartKey,
             EndKey,
             {filter, {'=', {field, <<"weather">>, varchar}, {const, <<"yankee">>}}},
             {end_inclusive, true}
            ],
    {ok, [?SQL_SELECT{ 'WHERE'       = WhereVal,
                       partition_key = PK,
                       local_key     = LK }]} = compile(DDL, Q),
    ?assertEqual(get_standard_pk(), PK),
    ?assertEqual(get_standard_lk(), LK),
    ?assertEqual(ExpectedWhere, WhereVal).

simple_with_2_field_filter_test() ->
    {ok, Q} = get_query(
                "select weather from GeoCheckin "
                "where time > 3000 and time < 5000 "
                "and user = 'user_1' and location = 'Scotland' "
                "and weather = 'yankee' "
                "and temperature = 'yelp'"
               ),
    DDL = get_standard_ddl(),
    true = is_query_valid(DDL, Q),
    [[StartKey, EndKey |_]] =
        test_data_where_clause(<<"Scotland">>, <<"user_1">>, [{3001, 5000}]),
    ExpectedWhere = [
             StartKey,
             EndKey,
             {filter,
              {and_,
               {'=', {field, <<"weather">>, varchar}, {const, <<"yankee">>}},
               {'=', {field, <<"temperature">>, varchar}, {const, <<"yelp">>}}
              }
             }
            ],
    {ok, [?SQL_SELECT{ 'WHERE'       = WhereVal,
                       partition_key = PK,
                       local_key     = LK }]} = compile(DDL, Q),
    ?assertEqual(get_standard_pk(), PK),
    ?assertEqual(get_standard_lk(), LK),
    ?assertEqual(ExpectedWhere, WhereVal).

complex_with_4_field_filter_test() ->
    Query =
        "select weather from GeoCheckin where"
        " time > 3000 and time < 5000 and user = 'user_1'"
        " and location = 'Scotland' and extra = 1"
        " and (weather = 'yankee' or (temperature = 'yelp' and geohash = 'erko'))",
    {ok, Q} = get_query(Query),
    DDL = get_long_ddl(),
    true = is_query_valid(DDL, Q),
    [[Start, End | _]] =
        test_data_where_clause(<<"Scotland">>, <<"user_1">>, [{3001, 5000}]),
    ExpectedWhere = [
              Start, End,
              {filter,
               {and_,
                {or_,
                 {'=', {field, <<"weather">>, varchar}, {const, <<"yankee">>}},
                 {and_,
                  {'=', {field, <<"geohash">>,     varchar}, {const, <<"erko">>}},
                  {'=', {field, <<"temperature">>, varchar}, {const, <<"yelp">>}} }
                },
                {'=', {field, <<"extra">>, sint64}, {const, 1}}
               }
              }
             ],
    {ok, [?SQL_SELECT{ 'WHERE'       = WhereVal,
                       partition_key = PK,
                       local_key     = LK }]} = compile(DDL, Q),
    ?assertEqual(get_standard_pk(), PK),
    ?assertEqual(get_standard_lk(), LK),
    ?assertEqual(ExpectedWhere, WhereVal).

complex_with_boolean_rewrite_filter_test() ->
    DDL = get_long_ddl(),
    {ok, Q} = get_query(
                "SELECT weather FROM GeoCheckin "
                "WHERE time > 3000 AND time < 5000 "
                "AND user = 'user_1' AND location = 'Scotland' "
                "AND (myboolean = False OR myboolean = tRue)"),
    true = is_query_valid(DDL, Q),
    [[StartKey, EndKey |_]] =
        test_data_where_clause(<<"Scotland">>, <<"user_1">>, [{3001, 5000}]),
    ExpectedWhere = [
             StartKey,
             EndKey,
             {filter,
              {or_,
               {'=', {field, <<"myboolean">>, boolean}, {const, false}},
               {'=', {field, <<"myboolean">>, boolean}, {const, true}}
              }
             }
            ],
    {ok, [?SQL_SELECT{ 'WHERE'       = WhereVal,
                       partition_key = PK,
                       local_key     = LK }]} = compile(DDL, Q),
    ?assertEqual(get_standard_pk(), PK),
    ?assertEqual(get_standard_lk(), LK),
    ?assertEqual(ExpectedWhere, WhereVal).

%% got for 3 queries to get partition ordering problems flushed out
simple_spanning_boundary_test() ->
    DDL = get_standard_ddl(),
    {ok, Q} = get_query(
                "select weather from GeoCheckin"
                " where time >= 3000 and time < 31000"
                " and user = 'user_1' and location = 'Scotland'"),
    true = is_query_valid(DDL, Q),
    %% get basic query
    %% now make the result - expecting 3 queries
    [Where1, Where2, Where3] =
        test_data_where_clause(<<"Scotland">>, <<"user_1">>,
                               [{3000, 15000}, {15000, 30000}, {30000, 31000}]),
    PK = get_standard_pk(),
    LK = get_standard_lk(),
    ?assertMatch({ok, [
                       ?SQL_SELECT{
                          'WHERE'       = Where1,
                          partition_key = PK,
                          local_key     = LK},
                       ?SQL_SELECT{
                          'WHERE'       = Where2,
                          partition_key = PK,
                          local_key     = LK},
                       ?SQL_SELECT{
                          'WHERE'       = Where3,
                          partition_key = PK,
                          local_key     = LK}
                      ]},
                 compile(DDL, Q)
                ).

%% Values right at quanta edges are tricky. Make sure we're not
%% missing them: we should be generating two queries instead of just
%% one.
boundary_quanta_test() ->
    DDL = get_standard_ddl(),
    Query =
        "select weather from GeoCheckin"
        " where time >= 14000 and time <= 15000"
        " and user = 'user_1' and location = 'Scotland'",
    {ok, Q} = get_query(Query),
    true = is_query_valid(DDL, Q),
    %% get basic query
    Actual = compile(DDL, Q),
    ?assertEqual(2, length(element(2, Actual))).

test_data_where_clause(Family, Series, StartEndTimes) ->
    Fn =
        fun({Start, End}) ->
                [
                 {startkey,        [
                                    {<<"location">>, varchar, Family},
                                    {<<"user">>, varchar,    Series},
                                    {<<"time">>, timestamp, Start}
                                   ]},
                 {endkey,          [
                                    {<<"location">>, varchar, Family},
                                    {<<"user">>, varchar,    Series},
                                    {<<"time">>, timestamp, End}
                                   ]},
                 {filter, []}
                ]
        end,
    [Fn(StartEnd) || StartEnd <- StartEndTimes].

%% check for spanning precision (same as above except selection range
%% is exact multiple of quantum size)
simple_spanning_boundary_precision_test() ->
    DDL = get_standard_ddl(),
    Query =
        "select weather from GeoCheckin"
        " where time >= 3000 and time < 30000"
        " and user = 'user_1' and location = 'Scotland'",
    {ok, Q} = get_query(Query),
    true = is_query_valid(DDL, Q),
    %% now make the result - expecting 2 queries
    [Where1, Where2] =
        test_data_where_clause(<<"Scotland">>, <<"user_1">>, [{3000, 15000}, {15000, 30000}]),
    _PK = get_standard_pk(),
    _LK = get_standard_lk(),
    {ok, [Select1, Select2]} = compile(DDL, Q),
    ?assertEqual(
        [Where1, Where2],
        [Select1?SQL_SELECT.'WHERE', Select2?SQL_SELECT.'WHERE']
    ),
    ?assertEqual(
        [get_standard_pk(), get_standard_pk()],
        [Select1?SQL_SELECT.partition_key, Select2?SQL_SELECT.partition_key]
    ),
    ?assertEqual(
        [get_standard_lk(), get_standard_lk()],
        [Select1?SQL_SELECT.local_key, Select2?SQL_SELECT.local_key]
    ).

%%
%% test failures
%%

simplest_compile_once_only_fail_test() ->
    DDL = get_standard_ddl(),
    Query =
        "select weather from GeoCheckin where"
        " time >= 3000 and time < 5000"
        " and user = 'user_1' and location = 'Scotland'",
    {ok, Q} = get_query(Query),
    true = is_query_valid(DDL, Q),
    %% now try and compile twice
    {ok, [Q2]} = compile(DDL, Q),
    Got = compile(DDL, Q2),
    ?assertEqual(
       {error, 'query is already compiled'},
       Got).

end_key_not_a_range_test() ->
    DDL = get_standard_ddl(),
    {ok, Q} = get_query(
                "SELECT weather FROM GeoCheckin "
                "WHERE time > 3000 AND time != 5000 "
                "AND user = 'user_1' AND location = 'derby'"),
    ?assertEqual(
       {error, {incomplete_where_clause, ?E_TSMSG_NO_UPPER_BOUND}},
       compile(DDL, Q)
      ).

start_key_not_a_range_test() ->
    DDL = get_standard_ddl(),
    {ok, Q} = get_query(
                "SELECT weather FROM GeoCheckin "
                "WHERE time = 3000 AND time < 5000 "
                "AND user = 'user_1' AND location = 'derby'"),
    ?assertEqual(
       {error, {incomplete_where_clause, ?E_TSMSG_NO_LOWER_BOUND}},
       compile(DDL, Q)
      ).

key_is_all_timestamps_test() ->
    DDL = get_ddl(
            "CREATE TABLE GeoCheckin ("
            "time_a TIMESTAMP NOT NULL, "
            "time_b TIMESTAMP NOT NULL, "
            "time_c TIMESTAMP NOT NULL, "
            "PRIMARY KEY("
            " (time_a, time_b, QUANTUM(time_c, 15, 's')), time_a, time_b, time_c))"),
    {ok, Q} = get_query(
                "SELECT time_a FROM GeoCheckin "
                "WHERE time_c > 2999 AND time_c < 5000 "
                "AND time_a = 10 AND time_b = 15"),
    {ok, [?SQL_SELECT{ 'WHERE' = Where }]} = compile(DDL, Q),
    ?assertEqual(
        [{startkey, [{<<"time_a">>,timestamp,10}, {<<"time_b">>,timestamp,15}, {<<"time_c">>,timestamp,3000} ]},
         {endkey,   [{<<"time_a">>,timestamp,10}, {<<"time_b">>,timestamp,15}, {<<"time_c">>,timestamp,5000} ]},
         {filter, []} ],
        Where
      ).

duplicate_lower_bound_filter_not_allowed_test() ->
    DDL = get_standard_ddl(),
    {ok, Q} = get_query(
                "SELECT weather FROM GeoCheckin "
                "WHERE time > 3000 AND  time > 3001 AND time < 5000 "
                "AND user = 'user_1' AND location = 'derby'"),
    ?assertEqual(
       {error, {lower_bound_specified_more_than_once, ?E_TSMSG_DUPLICATE_LOWER_BOUND}},
       compile(DDL, Q)
      ).

duplicate_upper_bound_filter_not_allowed_test() ->
    DDL = get_standard_ddl(),
    {ok, Q} = get_query(
                "SELECT weather FROM GeoCheckin "
                "WHERE time > 3000 AND time < 5000 AND time < 4999 "
                "AND user = 'user_1' AND location = 'derby'"),
    ?assertEqual(
       {error, {upper_bound_specified_more_than_once, ?E_TSMSG_DUPLICATE_UPPER_BOUND}},
       compile(DDL, Q)
      ).

lower_bound_is_bigger_than_upper_bound_test() ->
    DDL = get_standard_ddl(),
    {ok, Q} = get_query(
                "SELECT weather FROM GeoCheckin "
                "WHERE time > 6000 AND time < 5000"
                "AND user = 'user_1' AND location = 'derby'"),
    ?assertEqual(
       {error, {lower_bound_must_be_less_than_upper_bound,
                ?E_TSMSG_LOWER_BOUND_MUST_BE_LESS_THAN_UPPER_BOUND}},
       compile(DDL, Q)
      ).

lower_bound_is_same_as_upper_bound_test() ->
    DDL = get_standard_ddl(),
    {ok, Q} = get_query(
                "SELECT weather FROM GeoCheckin "
                "WHERE time > 5000 AND time < 5000"
                "AND user = 'user_1' AND location = 'derby'"),
    ?assertEqual(
       {error, {lower_and_upper_bounds_are_equal_when_no_equals_operator,
                ?E_TSMSG_LOWER_AND_UPPER_BOUNDS_ARE_EQUAL_WHEN_NO_EQUALS_OPERATOR}},
       compile(DDL, Q)
      ).

query_has_no_AND_operator_1_test() ->
    DDL = get_standard_ddl(),
    {ok, Q} = get_query("select * from test1 where time < 5"),
    ?assertEqual(
       {error, {incomplete_where_clause, ?E_TSMSG_NO_LOWER_BOUND}},
       compile(DDL, Q)
      ).

query_has_no_AND_operator_2_test() ->
    DDL = get_standard_ddl(),
    {ok, Q} = get_query("select * from test1 where time > 1 OR time < 5"),
    ?assertEqual(
       {error, {time_bounds_must_use_and_op, ?E_TIME_BOUNDS_MUST_USE_AND}},
       compile(DDL, Q)
      ).

query_has_no_AND_operator_3_test() ->
    DDL = get_standard_ddl(),
    {ok, Q} = get_query("select * from test1 where user = 'user_1' AND time > 1 OR time < 5"),
    ?assertEqual(
       {error, {time_bounds_must_use_and_op, ?E_TIME_BOUNDS_MUST_USE_AND}},
       compile(DDL, Q)
      ).

query_has_no_AND_operator_4_test() ->
    DDL = get_standard_ddl(),
    {ok, Q} = get_query("select * from test1 where user = 'user_1' OR time > 1 OR time < 5"),
    ?assertEqual(
       {error, {time_bounds_must_use_and_op, ?E_TIME_BOUNDS_MUST_USE_AND}},
       compile(DDL, Q)
      ).

missing_key_field_in_where_clause_test() ->
    DDL = get_standard_ddl(),
    {ok, Q} = get_query("select * from test1 where time > 1 and time < 6 and user = '2'"),
    ?assertEqual(
       {error, {missing_key_clause, ?E_KEY_FIELD_NOT_IN_WHERE_CLAUSE("location")}},
       compile(DDL, Q)
      ).

not_equals_can_only_be_a_filter_test() ->
    DDL = get_standard_ddl(),
    {ok, Q} = get_query("select * from test1 where time > 1"
                        " and time < 6 and user = '2' and location != '4'"),
    ?assertEqual(
       {error, {missing_key_clause, ?E_KEY_PARAM_MUST_USE_EQUALS_OPERATOR("location", '!=')}},
       compile(DDL, Q)
      ).

no_where_clause_test() ->
    DDL = get_standard_ddl(),
    {ok, Q} = get_query("select * from test1"),
    ?assertEqual(
       {error, {no_where_clause, ?E_NO_WHERE_CLAUSE}},
       compile(DDL, Q)
      ).

%% Columns are: [geohash, location, user, time, weather, temperature]

-define(ROW, [<<"geodude">>, <<"derby">>, <<"ralph">>, 10, <<"hot">>, 12.2]).

%% this helper function is only for tests testing queries with the
%% query_result_type of 'rows' and _not_ 'aggregate'
testing_compile_row_select(DDL, QueryString) ->
    {ok, [?SQL_SELECT{ 'SELECT' = SelectSpec } | _]} =
        compile(DDL, element(2, get_query(QueryString))),
    SelectSpec.

run_select_all_test() ->
    DDL = get_standard_ddl(),
    Sel = testing_compile_row_select(DDL,
                                     "SELECT * FROM GeoCheckin "
                                     "WHERE time > 1 AND time < 6 AND user = '2' AND location = '4'"),
    #riak_sel_clause_v1{clause = SelectSpec} = Sel,
    ?assertEqual(
       ?ROW,
       run_select(SelectSpec, ?ROW)
      ).

run_select_first_test() ->
    DDL = get_standard_ddl(),
    Sel = testing_compile_row_select(
            DDL,
            "SELECT geohash FROM GeoCheckin "
            "WHERE time > 1 AND time < 6 AND user = '2' AND location = '4'"),
    #riak_sel_clause_v1{clause = SelectSpec} = Sel,
    ?assertEqual(
       [<<"geodude">>],
       run_select(SelectSpec, ?ROW)
      ).

run_select_last_test() ->
    DDL = get_standard_ddl(),
    Sel = testing_compile_row_select(
            DDL,
            "SELECT temperature FROM GeoCheckin "
            "WHERE time > 1 AND time < 6 AND user = '2' AND location = '4'"),
    #riak_sel_clause_v1{clause = SelectSpec} = Sel,
    ?assertEqual(
       [12.2],
       run_select(SelectSpec, ?ROW)
      ).

run_select_all_individually_test() ->
    DDL = get_standard_ddl(),
    Sel = testing_compile_row_select(
            DDL,
            "SELECT geohash, location, user, time, weather, temperature FROM GeoCheckin "
            "WHERE time > 1 AND time < 6 AND user = '2' AND location = '4'"),
    #riak_sel_clause_v1{clause = SelectSpec} = Sel,
    ?assertEqual(
       ?ROW,
       run_select(SelectSpec, ?ROW)
      ).

run_select_some_test() ->
    DDL = get_standard_ddl(),
    Sel = testing_compile_row_select(
            DDL,
            "SELECT  location, weather FROM GeoCheckin "
            "WHERE time > 1 AND time < 6 AND user = '2' AND location = '4'"),
    #riak_sel_clause_v1{clause = SelectSpec} = Sel,
    ?assertEqual(
       [<<"derby">>, <<"hot">>],
       run_select(SelectSpec, ?ROW)
      ).

select_count_aggregation_test() ->
    DDL = get_standard_ddl(),
    Sel = testing_compile_row_select(
            DDL,
            "SELECT count(location) FROM GeoCheckin "
            "WHERE time > 1 AND time < 6 AND user = '2' AND location = '4'"),
    #riak_sel_clause_v1{clause = SelectSpec} = Sel,
    ?assertEqual(
       [1],
       run_select(SelectSpec, ?ROW, [0])
      ).


select_count_aggregation_2_test() ->
    DDL = get_standard_ddl(),
    Sel = testing_compile_row_select(
            DDL,
            "SELECT count(location), count(location) FROM GeoCheckin "
            "WHERE time > 1 AND time < 6 AND user = '2' AND location = '4'"),
    #riak_sel_clause_v1{clause = SelectSpec} = Sel,
    ?assertEqual(
       [1, 10],
       run_select(SelectSpec, ?ROW, [0, 9])
      ).

%% FIXME or operators

%% literal_on_left_hand_side_test() ->
%%     DDL = get_standard_ddl(),
%%     {ok, Q} = get_query("select * from testtwo where time > 1 and time < 6 and user = '2' and location = '4'"),
%%     ?assertMatch(
%%         [?SQL_SELECT{} | _],
%%         compile(DDL, Q)
%%     ).

%% FIXME RTS-634
%% or_on_local_key_not_allowed_test() ->
%%     DDL = get_standard_ddl(),
%%     {ok, Q} = get_query(
%%         "SELECT weather FROM GeoCheckin "
%%         "WHERE time > 3000 AND time < 5000 "
%%         "AND user = 'user_1' "
%%         "AND location = 'derby' OR location = 'rottingham'"),
%%     ?assertEqual(
%%         {error, {upper_bound_specified_more_than_once, ?E_TSMSG_DUPLICATE_UPPER_BOUND}},
%%         compile(DDL, Q)
%%     ).

%% TODO support filters on the primary key, this is not currently supported
%% filter_on_quanta_field_test() ->
%%     DDL = get_standard_ddl(),
%%     {ok, Q} = get_query(
%%         "SELECT weather FROM GeoCheckin "
%%         "WHERE time > 3000 AND time < 5000 "
%%         "AND time = 3002 AND user = 'user_1' AND location = 'derby'"),
%%     ?assertMatch(
%%         [?SQL_SELECT{
%%             'WHERE' = [
%%                 {startkey, [
%%                     {<<"time_a">>, timestamp, 10},
%%                     {<<"time_b">>, timestamp, 15},
%%                     {<<"time_c">>, timestamp, 2999}
%%                 ]},
%%                 {endkey, [
%%                     {<<"time_a">>, timestamp, 10},
%%                     {<<"time_b">>, timestamp, 15},
%%                     {<<"time_c">>, timestamp, 5000}
%%                 ]},
%%                 {filter, []},
%%                 {start_inclusive, false}]
%%         }],
%%         compile(DDL, Q)
%%     ).

%%
%% Select Clause Compilation
%%

get_sel_ddl() ->
    get_ddl(
      "CREATE TABLE GeoCheckin "
      "(location varchar not null, "
      "user varchar not null, "
      "time timestamp not null, "
      "mysint sint64 not null, "
      "mydouble double, "
      "myboolean boolean, "
      "PRIMARY KEY((location, user, quantum(time, 15, 's')), "
      "location, user, time))").

basic_select_test() ->
    DDL = get_sel_ddl(),
    SQL =
        "SELECT location from mytab"
        " WHERE myfamily = 'familyX'"
        " and myseries = 'seriesX' and time > 1 and time < 2",
    {ok, Rec} = get_query(SQL),
    {ok, Sel} = compile_select_clause(DDL, Rec),
    ?assertMatch(#riak_sel_clause_v1{calc_type        = rows,
                                     col_return_types = [
                                                         varchar
                                                        ],
                                     col_names        = [
                                                         <<"location">>
                                                        ]
                                    },
                 Sel).

basic_select_wildcard_test() ->
    DDL = get_sel_ddl(),
    SQL = "SELECT * from mytab WHERE myfamily = 'familyX' and myseries = 'seriesX' and time > 1 and time < 2",
    {ok, Rec} = get_query(SQL),
    {ok, Sel} = compile_select_clause(DDL, Rec),
    ?assertMatch(#riak_sel_clause_v1{calc_type        = rows,
                                     col_return_types = [
                                                         varchar,
                                                         varchar,
                                                         timestamp,
                                                         sint64,
                                                         double,
                                                         boolean
                                                        ],
                                     col_names        = [
                                                         <<"location">>,
                                                         <<"user">>,
                                                         <<"time">>,
                                                         <<"mysint">>,
                                                         <<"mydouble">>,
                                                         <<"myboolean">>
                                                        ]
                                    },
                 Sel).

select_all_and_column_test() ->
    {ok, Rec} = get_query(
                  "SELECT *, location from mytab WHERE myfamily = 'familyX' "
                  "AND myseries = 'seriesX' AND time > 1 AND time < 2"),
    {ok, Selection} = compile_select_clause(get_sel_ddl(), Rec),
    ?assertMatch(
       #riak_sel_clause_v1{
          calc_type = rows,
          col_return_types = [varchar, varchar, timestamp, sint64, double,
                              boolean, varchar],
          col_names = [<<"location">>, <<"user">>, <<"time">>,
                       <<"mysint">>, <<"mydouble">>, <<"myboolean">>,
                       <<"location">>]
         },
       Selection
      ).

select_column_and_all_test() ->
    {ok, Rec} = get_query(
                  "SELECT location, * from mytab WHERE myfamily = 'familyX' "
                  "AND myseries = 'seriesX' AND time > 1 AND time < 2"),
    {ok, Selection} = compile_select_clause(get_sel_ddl(), Rec),
    ?assertMatch(
       #riak_sel_clause_v1{
          calc_type = rows,
          col_return_types = [varchar, varchar, varchar, timestamp, sint64, double,
                              boolean],
          col_names = [<<"location">>, <<"location">>, <<"user">>, <<"time">>,
                       <<"mysint">>, <<"mydouble">>, <<"myboolean">>]
         },
       Selection
      ).

basic_select_window_agg_fn_test() ->
    SQL =
        "SELECT count(location), avg(mydouble), avg(mysint)"
        " from mytab WHERE myfamily = 'familyX'"
        " and myseries = 'seriesX' and time > 1 and time < 2",
    {ok, Rec} = get_query(SQL),
    {ok, Sel} = compile_select_clause(get_sel_ddl(), Rec),
    ?assertMatch(#riak_sel_clause_v1{calc_type        = aggregate,
                                     col_return_types = [
                                                         sint64,
                                                         double,
                                                         double
                                                        ],
                                     col_names        = [
                                                         <<"COUNT(location)">>,
                                                         <<"AVG(mydouble)">>,
                                                         <<"AVG(mysint)">>
                                                        ]
                                    },
                 Sel).

basic_select_arith_1_test() ->
    SQL =
        "SELECT 1 + 2 - 3 /4 * 5 from mytab"
        " WHERE myfamily = 'familyX' and myseries = 'seriesX'"
        " and time > 1 and time < 2",
    {ok, Rec} = get_query(SQL),
    {ok, Sel} = compile_select_clause(get_sel_ddl(), Rec),
    ?assertMatch(
       #riak_sel_clause_v1{
          calc_type        = rows,
          col_return_types = [sint64],
          col_names        = [<<"((1+2)-((3/4)*5))">>] },
       Sel
      ).

varchar_literal_test() ->
    {ok, Rec} = get_query("SELECT 'hello' from mytab"),
    {ok, Sel} = compile_select_clause(get_sel_ddl(), Rec),
    ?assertMatch(
       #riak_sel_clause_v1{
          calc_type        = rows,
          col_return_types = [varchar],
          col_names        = [<<"'hello'">>] },
       Sel
      ).

boolean_true_literal_test() ->
    {ok, Rec} = get_query("SELECT true from mytab"),
    {ok, Sel} = compile_select_clause(get_sel_ddl(), Rec),
    ?assertMatch(
       #riak_sel_clause_v1{
          calc_type        = rows,
          col_return_types = [boolean],
          col_names        = [<<"true">>] },
       Sel
      ).

boolean_false_literal_test() ->
    {ok, Rec} = get_query("SELECT false from mytab"),
    {ok, Sel} = compile_select_clause(get_sel_ddl(), Rec),
    ?assertMatch(
       #riak_sel_clause_v1{
          calc_type        = rows,
          col_return_types = [boolean],
          col_names        = [<<"false">>] },
       Sel
      ).

basic_select_arith_2_test() ->
    SQL =
        "SELECT 1 + 2.0 - 3 /4 * 5 from mytab"
        " WHERE myfamily = 'familyX' and myseries = 'seriesX'"
        " and time > 1 and time < 2",
    {ok, Rec} = get_query(SQL),
    {ok, Sel} = compile_select_clause(get_sel_ddl(), Rec),
    ?assertMatch(
       #riak_sel_clause_v1{
          calc_type = rows,
          col_return_types = [double],
          col_names = [<<"((1+2.0)-((3/4)*5))">>] },
       Sel
      ).

rows_initial_state_test() ->
    {ok, Rec} = get_query(
                  "SELECT * FROM mytab WHERE myfamily = 'familyX' "
                  "AND myseries = 'seriesX' AND time > 1 AND time < 2"),
    {ok, Select} = compile_select_clause(get_sel_ddl(), Rec),
    ?assertMatch(
       #riak_sel_clause_v1{ initial_state = [] },
       Select
      ).

function_1_initial_state_test() ->
    {ok, Rec} = get_query(
                  "SELECT SUM(mydouble) FROM mytab WHERE myfamily = 'familyX' "
                  "AND myseries = 'seriesX' AND time > 1 AND time < 2"),
    {ok, Select} = compile_select_clause(get_sel_ddl(), Rec),
    ?assertMatch(
       #riak_sel_clause_v1{ initial_state = [[]] },
       Select
      ).

function_2_initial_state_test() ->
    {ok, Rec} = get_query(
                  "SELECT SUM(mydouble), SUM(mydouble) FROM mytab WHERE myfamily = 'familyX' "
                  "AND myseries = 'seriesX' AND time > 1 AND time < 2"),
    {ok, Select} = compile_select_clause(get_sel_ddl(), Rec),
    ?assertMatch(
       #riak_sel_clause_v1{ initial_state = [[], []] },
       Select
      ).

select_negation_test() ->
    DDL = get_sel_ddl(),
    SQL = "SELECT -1, - 1, -1.0, - 1.0, -mydouble, - mydouble, -(1), -(1.0) from mytab "
        "WHERE myfamily = 'familyX' AND myseries = 'seriesX' "
        "AND time > 1 AND time < 2",
    {ok, Rec} = get_query(SQL),
    {ok, Sel} = compile_select_clause(DDL, Rec),
    ?assertMatch(#riak_sel_clause_v1{calc_type        = rows,
                                     col_return_types = [
                                                         sint64,
                                                         sint64,
                                                         double,
                                                         double,
                                                         double,
                                                         double,
                                                         sint64,
                                                         double
                                                        ],
                                     col_names        = [
                                                         <<"-1">>,
                                                         <<"-1">>,
                                                         <<"-1.0">>,
                                                         <<"-1.0">>,
                                                         <<"-mydouble">>,
                                                         <<"-mydouble">>,
                                                         <<"-1">>,
                                                         <<"-1.0">>
                                                        ]
                                    },
                 Sel).

sum_sum_finalise_test() ->
    {ok, Rec} = get_query(
        "SELECT mydouble, SUM(mydouble), SUM(mydouble) FROM mytab"),
    {ok, Select} = compile_select_clause(get_sel_ddl(), Rec),
    ?assertEqual(
        [1.0,3,7],
        finalise_aggregate(Select, [1.0, 3, 7])
      ).

extract_stateful_function_1_test() ->
    {ok, ?SQL_SELECT{ 'SELECT' = #riak_sel_clause_v1{ clause = [Select] } }} =
        get_query(
        "SELECT COUNT(col1) + COUNT(col2) FROM mytab "
        "WHERE myfamily = 'familyX' "
        "AND myseries = 'seriesX' AND time > 1 AND time < 2"),
    CountFn1 = {{window_agg_fn, 'COUNT'}, [{identifier, [<<"col1">>]}]},
    CountFn2 = {{window_agg_fn, 'COUNT'}, [{identifier, [<<"col2">>]}]},
    ?assertEqual(
        {{'+',
          {finalise_aggregation, 'COUNT', 1},
          {finalise_aggregation, 'COUNT', 2}},
         [CountFn1,CountFn2]},
        extract_stateful_functions(Select, 0)
    ).

count_plus_count_test() ->
    {ok, Rec} = get_query(
        "SELECT COUNT(mydouble) + COUNT(mydouble) FROM mytab "
        "WHERE myfamily = 'familyX' "
        "AND myseries = 'seriesX' AND time > 1 AND time < 2"),
    {ok, Select} = compile_select_clause(get_sel_ddl(), Rec),
    ?assertMatch(
        #riak_sel_clause_v1{
            initial_state = [0,0],
            finalisers = [_, skip] },
        Select
      ).

count_plus_count_finalise_test() ->
    {ok, Rec} = get_query(
        "SELECT COUNT(mydouble) + COUNT(mydouble) FROM mytab"),
    {ok, Select} = compile_select_clause(get_sel_ddl(), Rec),
    ?assertMatch(
        [6],
        finalise_aggregate(Select, [3,3])
      ).

count_multiplied_by_count_finalise_test() ->
    {ok, Rec} = get_query(
        "SELECT COUNT(mydouble) * COUNT(mydouble) FROM mytab"),
    {ok, Select} = compile_select_clause(get_sel_ddl(), Rec),
    ?assertMatch(
        [9],
        finalise_aggregate(Select, [3,3])
      ).

count_plus_seven_finalise_test() ->
    {ok, Rec} = get_query(
        "SELECT COUNT(mydouble) + 7 FROM mytab"),
    {ok, Select} = compile_select_clause(get_sel_ddl(), Rec),
    ?assertMatch(
        [10],
        finalise_aggregate(Select, [3])
      ).

count_plus_seven_sum__test() ->
    {ok, Rec} = get_query(
        "SELECT COUNT(mydouble) + 7, SUM(mydouble) FROM mytab"),
    {ok, Select} = compile_select_clause(get_sel_ddl(), Rec),
    ?assertMatch(
        #riak_sel_clause_v1{
            initial_state = [0,[]],
            finalisers = [_, _] },
        Select
      ).

count_plus_seven_sum_finalise_1_test() ->
    {ok, Rec} = get_query(
        "SELECT COUNT(mydouble) + 7, SUM(mydouble) FROM mytab"),
    {ok, Select} = compile_select_clause(get_sel_ddl(), Rec),
    ?assertMatch(
        [10, 11.0],
        finalise_aggregate(Select, [3, 11.0])
      ).

count_plus_seven_sum_finalise_2_test() ->
    {ok, Rec} = get_query(
        "SELECT COUNT(mydouble+1) + 1 FROM mytab"),
    {ok, Select} = compile_select_clause(get_sel_ddl(), Rec),
    ?assertEqual(
        [2],
        finalise_aggregate(Select, [1])
      ).

avg_finalise_test() ->
    {ok, Rec} = get_query(
        "SELECT AVG(mydouble) FROM mytab"),
    {ok, #riak_sel_clause_v1{ clause = [AvgFn] } = Select} =
        compile_select_clause(get_sel_ddl(), Rec),
    InitialState = riak_ql_window_agg_fns:start_state('AVG'),
    Rows = [[x,x,x,x,N,x] || N <- lists:seq(1, 5)],
    AverageResult = lists:foldl(AvgFn, InitialState, Rows),
    ?assertEqual(
        [lists:sum(lists:seq(1, 5)) / 5],
        finalise_aggregate(Select, [AverageResult])
    ).

finalise_aggregate_test() ->
    ?assertEqual(
        [1,2,3],
        finalise_aggregate(
            #riak_sel_clause_v1 {
                calc_type = aggregate,
                finalisers = lists:duplicate(3, fun(_,S) -> S end) },
            [1,2,3]
        )
    ).

infer_col_type_1_test() ->
    ?assertEqual(
        {sint64, []},
        infer_col_type(get_sel_ddl(), {integer, 5}, [])
    ).

infer_col_type_2_test() ->
    ?assertEqual(
        {sint64, []},
        infer_col_type(get_sel_ddl(), {{window_agg_fn, 'SUM'}, [{integer, 4}]}, [])
    ).

compile_query_with_function_type_error_1_test() ->
    {ok, Q} = get_query(
          "SELECT SUM(location) FROM GeoCheckin "
          "WHERE time > 5000 AND time < 10000"
          "AND user = 'user_1' AND location = 'derby'"),
    ?assertEqual(
        {error,{invalid_query,<<"\nFunction 'SUM' called with arguments of the wrong type [varchar].">>}},
        compile(get_standard_ddl(), Q)
    ).

compile_query_with_function_type_error_2_test() ->
    {ok, Q} = get_query(
          "SELECT SUM(location), AVG(location) FROM GeoCheckin "
          "WHERE time > 5000 AND time < 10000"
          "AND user = 'user_1' AND location = 'derby'"),
    ?assertEqual(
        {error,{invalid_query,<<"\nFunction 'SUM' called with arguments of the wrong type [varchar].\n"
                                "Function 'AVG' called with arguments of the wrong type [varchar].">>}},
        compile(get_standard_ddl(), Q)
    ).

compile_query_with_function_type_error_3_test() ->
    {ok, Q} = get_query(
          "SELECT AVG(location + 1) FROM GeoCheckin "
          "WHERE time > 5000 AND time < 10000"
          "AND user = 'user_1' AND location = 'derby'"),
    ?assertEqual(
        {error,{invalid_query,<<"\nOperator '+' called with mismatched types [varchar vs sint64].">>}},
        compile(get_standard_ddl(), Q)
    ).

compile_query_with_arithmetic_type_error_1_test() ->
    {ok, Q} = get_query(
          "SELECT location + 1 FROM GeoCheckin "
          "WHERE time > 5000 AND time < 10000"
          "AND user = 'user_1' AND location = 'derby'"),
    ?assertEqual(
        {error,{invalid_query,<<"\nOperator '+' called with mismatched types [varchar vs sint64].">>}},
        compile(get_standard_ddl(), Q)
    ).

compile_query_with_arithmetic_type_error_2_test() ->
    {ok, Q} = get_query(
          "SELECT 2*(location + 1) FROM GeoCheckin "
          "WHERE time > 5000 AND time < 10000"
          "AND user = 'user_1' AND location = 'derby'"),
    ?assertEqual(
        {error,{invalid_query,<<"\nOperator '+' called with mismatched types [varchar vs sint64].">>}},
        compile(get_standard_ddl(), Q)
    ).

flexible_keys_1_test() ->
    DDL = get_ddl(
        "CREATE TABLE tab4("
        "a1 SINT64 NOT NULL, "
        "a TIMESTAMP NOT NULL, "
        "b VARCHAR NOT NULL, "
        "c VARCHAR NOT NULL, "
        "d SINT64 NOT NULL, "
        "PRIMARY KEY  ((a1, quantum(a, 15, 's')), a1, a, b, c, d))"),
    {ok, Q} = get_query(
          "SELECT * FROM tab4 WHERE a > 0 AND a < 1000 AND a1 = 1"),
    {ok, [Select]} = compile(DDL, Q),
    ?assertPropsEqual(
        [{startkey,[{<<"a1">>,sint64,1}, {<<"a">>,timestamp,1}]},
          {endkey, [{<<"a1">>,sint64,1}, {<<"a">>,timestamp,1000}]},
          {filter,[]}],
        Select?SQL_SELECT.'WHERE'
    ).

%% two element key with quantum
flexible_keys_2_test() ->
    DDL = get_ddl(
        "CREATE TABLE tab4("
        "a TIMESTAMP NOT NULL, "
        "PRIMARY KEY  ((quantum(a, 15, 's')), a))"),
    {ok, Q} = get_query(
          "SELECT * FROM tab4 WHERE a > 0 AND a < 1000"),
    ?assertMatch(
        {ok, [?SQL_SELECT{}]},
        compile(DDL, Q)
    ).

quantum_field_name_test() ->
    DDL = get_ddl(
        "CREATE TABLE tab1("
        "a SINT64 NOT NULL, "
        "b TIMESTAMP NOT NULL, "
        "PRIMARY KEY  ((a,quantum(b, 15, 's')), a,b))"),
    ?assertEqual(
        <<"b">>,
        quantum_field_name(DDL)
    ).

quantum_field_name_no_quanta_test() ->
    DDL = get_ddl(
        "CREATE TABLE tab1("
        "a SINT64 NOT NULL, "
        "b TIMESTAMP NOT NULL, "
        "PRIMARY KEY  ((a,b), a,b))"),
    ?assertEqual(
        no_quanta,
        quantum_field_name(DDL)
    ).

%% short key, partition and local keys are the same
no_quantum_in_query_1_test() ->
    DDL = get_ddl(
        "CREATE TABLE tabab("
        "a TIMESTAMP NOT NULL, "
        "b VARCHAR NOT NULL, "
        "PRIMARY KEY  ((a,b), a,b))"),
    {ok, Q} = get_query(
          "SELECT * FROM tab1 WHERE a = 1 AND b = 1"),
    {ok, [?SQL_SELECT{ 'WHERE' = Where }]} = compile(DDL, Q),
    ?assertEqual(
        [{startkey,[{<<"a">>,timestamp,1},{<<"b">>,varchar,1}]},
         {endkey,  [{<<"a">>,timestamp,1},{<<"b">>,varchar,1}]},
         {filter,[]},
         {end_inclusive,true}],
        Where
    ).

%% partition and local key are different
no_quantum_in_query_2_test() ->
    DDL = get_ddl(
        "CREATE TABLE tabab("
        "a SINT64 NOT NULL, "
        "b VARCHAR NOT NULL, "
        "c DOUBLE NOT NULL, "
        "d BOOLEAN NOT NULL, "
        "PRIMARY KEY  ((c,a,b), c,a,b,d))"),
    {ok, Q} = get_query(
          "SELECT * FROM tabab WHERE a = 1000 AND b = 'bval' AND c = 3.5"),
    {ok, [Select]} = compile(DDL, Q),
    Key =
        [{<<"c">>,double,3.5}, {<<"a">>,sint64,1000},{<<"b">>,varchar,<<"bval">>}],
    ?assertPropsEqual(
        [{startkey, Key},
         {endkey, Key},
         {filter,[]},
         {end_inclusive,true}],
        Select?SQL_SELECT.'WHERE'
    ).


no_quantum_in_query_3_test() ->
    DDL = get_ddl(
        "CREATE TABLE tababa("
        "a SINT64 NOT NULL, "
        "b VARCHAR NOT NULL, "
        "c DOUBLE NOT NULL, "
        "d BOOLEAN NOT NULL, "
        "PRIMARY KEY  ((c,a,b), c,a,b,d))"),
    {ok, Q} = get_query(
          "SELECT * FROM tababa WHERE a = 1000 AND b = 'bval' AND c = 3.5 AND d = true"),
    {ok, [Select]} = compile(DDL, Q),
    Key =
        [{<<"c">>,double,3.5}, {<<"a">>,sint64,1000},{<<"b">>,varchar,<<"bval">>},{<<"d">>,boolean,true}],
    ?assertPropsEqual(
        [{startkey, Key},
         {endkey, Key},
         {filter,{'=',{field,<<"d">>,boolean},{const, true}}},
         {start_inclusive,true},
         {end_inclusive,true}],
        Select?SQL_SELECT.'WHERE'
    ).

%% one element key
no_quantum_in_query_4_test() ->
    DDL = get_ddl(
        "CREATE TABLE tab1("
        "a TIMESTAMP NOT NULL, "
        "PRIMARY KEY  ((a), a))"),
    {ok, Q} = get_query(
          "SELECT * FROM tab1 WHERE a = 1000"),
    {ok, [Select]} = compile(DDL, Q),
    ?assertEqual(
        [{startkey,[{<<"a">>,timestamp,1000}]},
          {endkey,[{<<"a">>,timestamp,1000}]},
          {filter,[]},
          {end_inclusive,true}],
        Select?SQL_SELECT.'WHERE'
    ).

eqality_filter_on_quantum_specifies_start_and_end_range_test() ->
    DDL = get_ddl(
        "CREATE TABLE tab1("
        "a TIMESTAMP NOT NULL, "
        "PRIMARY KEY  ((quantum(a, 15, 's')), a))"),
    {ok, Q} = get_query(
          "SELECT * FROM tab1 WHERE a = 1000"),
    {ok, [Select]} = compile(DDL, Q),
    ?assertEqual(
        [{startkey,[{<<"a">>,timestamp,1000}]},
         {endkey,[{<<"a">>,timestamp,1000}]},
         {filter,[]},
         {end_inclusive,true}],
        Select?SQL_SELECT.'WHERE'
    ).

eqality_filter_on_quantum_specifies_start_and_end_range_2_test() ->
    DDL = get_ddl(
        "CREATE TABLE tab123 ("
        "StationId     VARCHAR   NOT NULL,"
        "ReadingTimeStamp  TIMESTAMP NOT NULL,"
        "Temperature     SINT64,"
        "Humidity      DOUBLE,"
        "WindSpeed     DOUBLE,"
        "WindDirection   DOUBLE,"
        "PRIMARY KEY ((StationId, QUANTUM(ReadingTimeStamp, 1, 'd')),"
        "   StationId, ReadingTimeStamp));"),
    {ok, Q} = get_query(
          "SELECT * FROM tab123 "
          "WHERE StationId = 'Station-1001' "
          "AND ReadingTimeStamp = 1469204877;"),
    {ok, [Select]} = compile(DDL, Q),
    ?assertEqual(
        [{startkey,[{<<"StationId">>,varchar,<<"Station-1001">>},{<<"ReadingTimeStamp">>,timestamp,1469204877}]},
         {endkey,[  {<<"StationId">>,varchar,<<"Station-1001">>},{<<"ReadingTimeStamp">>,timestamp,1469204877}]},
         {filter,[]},
         {end_inclusive,true}],
        Select?SQL_SELECT.'WHERE'
    ).

cannot_have_two_equality_filters_on_quantum_without_range_test() ->
    DDL = get_ddl(
        "CREATE TABLE tab1("
        "a TIMESTAMP NOT NULL, "
        "PRIMARY KEY  ((quantum(a, 15, 's')), a))"),
    {ok, Q} = get_query(
          "SELECT * FROM tab1 WHERE a = 1000 AND a = 1"),
    ?assertEqual(
        {error, {cannot_have_two_equality_filters_on_quantum_without_range,
                 ?E_CANNOT_HAVE_TWO_EQUALITY_FILTERS_ON_QUANTUM_WITHOUT_RANGE}},
        compile(DDL, Q)
    ).

two_element_key_range_cannot_match_test() ->
    DDL = get_ddl(
        "CREATE TABLE tabab("
        "a TIMESTAMP NOT NULL, "
        "b TIMESTAMP NOT NULL, "
        "PRIMARY KEY  ((a,quantum(b, 15, 's')), a,b))"),
    {ok, Q} = get_query(
          "SELECT * FROM tabab WHERE a = 1 AND b > 1 AND b < 1"),
    ?assertMatch(
        {error, {lower_and_upper_bounds_are_equal_when_no_equals_operator, <<_/binary>>}},
        compile(DDL, Q)
    ).

group_by_one_field_test() ->
    DDL = get_ddl(
        "CREATE TABLE mytab("
        "a TIMESTAMP NOT NULL, "
        "b TIMESTAMP NOT NULL, "
        "PRIMARY KEY  ((a,b), a,b))"),
    {ok, Q1} = get_query(
        "SELECT b FROM mytab "
        "WHERE a = 1 AND b = 2 GROUP BY b"),
    {ok, [Q2]} = compile(DDL, Q1),
    ?assertEqual(
        [{2,<<"b">>}],
        Q2?SQL_SELECT.group_by
    ).

group_by_two_fields_test() ->
    DDL = get_ddl(
        "CREATE TABLE mytab("
        "a TIMESTAMP NOT NULL, "
        "b TIMESTAMP NOT NULL, "
        "PRIMARY KEY  ((a,b), a,b))"),
    {ok, Q1} = get_query(
        "SELECT b FROM mytab "
        "WHERE a = 1 AND b = 2 GROUP BY b, a"),
    {ok, [Q2]} = compile(DDL, Q1),
    ?assertEqual(
        [{2,<<"b">>},{1,<<"a">>}],
        Q2?SQL_SELECT.group_by
    ).

group_by_column_not_in_the_table_test() ->
    DDL = get_ddl(
        "CREATE TABLE mytab("
        "a TIMESTAMP NOT NULL, "
        "b TIMESTAMP NOT NULL, "
        "PRIMARY KEY  ((a,b), a,b))"),
    {ok, Q1} = get_query(
        "SELECT x FROM mytab "
        "WHERE a = 1 AND b = 2 GROUP BY x"),
    ?assertError(
        {unknown_column,{<<"x">>,[<<"a">>,<<"b">>]}},
        compile(DDL, Q1)
    ).

order_by_with_pass_1_test() ->
    DDL = get_ddl(
        "CREATE TABLE t("
        "a TIMESTAMP NOT NULL, "
        "b sint64, "
        "PRIMARY KEY ((a), a))"),
    {ok, Q1} = get_query(
        "SELECT b FROM t "
        "WHERE a = 1 LIMIT 6"),
    {ok, [Q2]} = compile(DDL, Q1),
    ?assertEqual(
        [],
        Q2?SQL_SELECT.'ORDER BY'
    ),
    ?assertEqual(
        [6],
        Q2?SQL_SELECT.'LIMIT'
    ),
    ?assertEqual(
        [],
        Q2?SQL_SELECT.'OFFSET'
    ).

order_by_with_pass_2_test() ->
    DDL = get_ddl(
        "CREATE TABLE t("
        "a TIMESTAMP NOT NULL, "
        "b sint64, "
        "c sint64, "
        "PRIMARY KEY ((a), a))"),
    {ok, Q1} = get_query(
        "SELECT b FROM t "
        "WHERE a = 1 ORDER BY a asc, c, b nulls first limit 11 offset 3"),
    {ok, [Q2]} = compile(DDL, Q1),
    ?assertEqual(
        [{<<"a">>, asc, nulls_last}, {<<"c">>, asc, nulls_last}, {<<"b">>, asc, nulls_first}],
        Q2?SQL_SELECT.'ORDER BY'
    ),
    ?assertEqual(
        [11],
        Q2?SQL_SELECT.'LIMIT'
    ),
    ?assertEqual(
        [3],
        Q2?SQL_SELECT.'OFFSET'
    ).

order_by_with_aggregate_calc_type_test() ->
    DDL = get_ddl(
        "CREATE TABLE t("
        "a TIMESTAMP NOT NULL, "
        "b sint64, "
        "PRIMARY KEY ((a), a))"),
    {ok, Q} = get_query(
        "SELECT min(b) FROM t "
        "WHERE a = 1 LIMIT 6"),
    ?assertMatch(
        {error, {order_by_with_aggregate_calc_type, <<_/binary>>}},
        compile(DDL, Q)
    ).


negate_an_aggregation_function_test() ->
    {ok, Rec} = get_query(
        "SELECT -COUNT(*) FROM mytab"),
    {ok, Select} = compile_select_clause(get_sel_ddl(), Rec),
    ?assertMatch(
        [-3],
        finalise_aggregate(Select, [3])
      ).

coverage_context_not_a_tuple_or_invalid_checksum_test() ->
    NotACheckSum = 34,
    OfThisTerm = <<"f,a,f,a">>,
    {ok, Q} = get_query("select a from t where a>0 and a<2", term_to_binary({NotACheckSum, OfThisTerm})),
    ?assertEqual(
       {error, invalid_coverage_context_checksum},
       compile(get_ddl("create table t (a timestamp not null, primary key ((quantum(a,1,d)), a))"), Q)).

helper_desc_order_on_quantum_ddl() ->
    get_ddl(
        "CREATE TABLE table1 ("
        "a SINT64 NOT NULL, "
        "b SINT64 NOT NULL, "
        "c TIMESTAMP NOT NULL, "
        "PRIMARY KEY ((a,b,quantum(c, 1, 's')), a,b,c DESC))").

query_desc_order_on_quantum_at_quanta_boundaries_test() ->
    {ok, Q} = get_query(
          "SELECT * FROM table1 "
          "WHERE a = 1 AND b = 1 AND c >= 4000 AND c <= 5000"),
    {ok, SubQueries} = compile(helper_desc_order_on_quantum_ddl(), Q),
    SubQueryWheres = [S?SQL_SELECT.'WHERE' || S <- SubQueries],
    ?assertEqual(
        [
            [{startkey,[{<<"a">>,sint64,1},{<<"b">>,sint64,1},{<<"c">>,timestamp,5000}]},
             {endkey,  [{<<"a">>,sint64,1},{<<"b">>,sint64,1},{<<"c">>,timestamp,5000}]},
             {filter,[]},
             {end_inclusive,true},
             {start_inclusive,true}]
            ,
            [{startkey,[{<<"a">>,sint64,1},{<<"b">>,sint64,1},{<<"c">>,timestamp,5000}]},
             {endkey,  [{<<"a">>,sint64,1},{<<"b">>,sint64,1},{<<"c">>,timestamp,4000}]},
             {filter,[]},
             {start_inclusive,false},
             {end_inclusive,true}]
        ],
        SubQueryWheres
    ).

fix_subquery_order_test() ->
    {ok, Q} = get_query(
          "SELECT * FROM table1 "
          "WHERE a = 1 AND b = 1 AND c >= 4000 AND c <= 5000"),
    {ok, SubQueries} = compile(helper_desc_order_on_quantum_ddl(), Q),
    ?assertEqual(
        [
            [{startkey,[{<<"a">>,sint64,1},{<<"b">>,sint64,1},{<<"c">>,timestamp,5000}]},
             {endkey,  [{<<"a">>,sint64,1},{<<"b">>,sint64,1},{<<"c">>,timestamp,5000}]},
             {filter,[]},
             {end_inclusive,true},
             {start_inclusive,true}]
            ,
            [{startkey,[{<<"a">>,sint64,1},{<<"b">>,sint64,1},{<<"c">>,timestamp,5000}]},
             {endkey,  [{<<"a">>,sint64,1},{<<"b">>,sint64,1},{<<"c">>,timestamp,4000}]},
             {filter,[]},
             {start_inclusive,false},
             {end_inclusive,true}]
        ],
        [S?SQL_SELECT.'WHERE' || S <- fix_subquery_order(SubQueries)]
    ).

query_desc_order_on_quantum_at_quantum_across_quanta_test() ->
    {ok, Q} = get_query(
          "SELECT * FROM table1 "
          "WHERE a = 1 AND b = 1 AND c >= 3500 AND c <= 5500"),
    {ok, SubQueries} = compile(helper_desc_order_on_quantum_ddl(), Q),
    SubQueryWheres = [S?SQL_SELECT.'WHERE' || S <- SubQueries],
    ?assertEqual(
        [
            [{startkey,[{<<"a">>,sint64,1},{<<"b">>,sint64,1},{<<"c">>,timestamp,5500}]},
             {endkey,  [{<<"a">>,sint64,1},{<<"b">>,sint64,1},{<<"c">>,timestamp,5000}]},
             {filter,[]},
             {end_inclusive,true},
             {start_inclusive,true}]
            ,
            [{startkey,[{<<"a">>,sint64,1},{<<"b">>,sint64,1},{<<"c">>,timestamp,5000}]},
             {endkey,  [{<<"a">>,sint64,1},{<<"b">>,sint64,1},{<<"c">>,timestamp,4000}]},
             {filter,[]},
             {start_inclusive,false},
             {end_inclusive,true}]
            ,
            [{startkey,[{<<"a">>,sint64,1},{<<"b">>,sint64,1},{<<"c">>,timestamp,4000}]},
             {endkey,  [{<<"a">>,sint64,1},{<<"b">>,sint64,1},{<<"c">>,timestamp,3500}]},
             {filter,[]},
             {start_inclusive,false},
             {end_inclusive,true}]
        ],
        SubQueryWheres
    ).

group_by_time_adds_a_column_test() ->
    DDL = get_ddl(
        "CREATE TABLE t("
        "a TIMESTAMP NOT NULL, "
        "b sint64, "
        "PRIMARY KEY ((a), a))"),
    {ok, Q} = get_query(
        "SELECT time(a,1m), COUNT(*) FROM t
         WHERE a = 1
         GROUP BY time(a,1m);"),
    {ok,[?SQL_SELECT{'SELECT'=SelClause}]} = compile(DDL, Q),
    ?assertEqual(group_by, SelClause#riak_sel_clause_v1.calc_type),
    ?assertEqual([<<"TIME(a, 60000)">>, <<"COUNT(*)">>], SelClause#riak_sel_clause_v1.col_names),
    ?assertEqual([timestamp,sint64], SelClause#riak_sel_clause_v1.col_return_types),
    %% these are funs so just check we have the right number
    ?assertMatch([_,_], SelClause#riak_sel_clause_v1.clause),
    ?assertMatch([_,_], SelClause#riak_sel_clause_v1.finalisers),
    ?assertEqual([timestamp,sint64], SelClause#riak_sel_clause_v1.col_return_types).

group_by_time_run_select_test() ->
    DDL = get_ddl(
        "CREATE TABLE t("
        "a TIMESTAMP NOT NULL, "
        "PRIMARY KEY ((QUANTUM(a, 1, 's')), a))"),
    Sel = testing_compile_row_select(
        DDL,
        "SELECT time(a, 1s), COUNT(*) FROM t "
        "WHERE a > 1 AND a < 6 "
        "GROUP BY time(a,1s);"),
    #riak_sel_clause_v1{clause = SelectSpec} = Sel,
    ?assertEqual(
       [1000,1],
       run_select(SelectSpec, [1001], [[],0])
    ).

time_fn_in_select_clause_invalid_arg_types_returns_error_test() ->
    DDL = get_ddl(
        "CREATE TABLE t("
        "a TIMESTAMP NOT NULL, "
        "PRIMARY KEY ((QUANTUM(a, 1, 's')), a))"),
    {ok, Query} = get_query(
        "SELECT time('lol', true), COUNT(*) FROM t "
        "WHERE a > 1 AND a < 6 "
        "GROUP BY time(a,1s);"),
    ?assertMatch(
       {error,{invalid_query,<<_/binary>>}},
       compile(DDL,Query)
    ).

group_by_time_on_non_existing_column_returns_error_test() ->
    DDL = get_ddl(
        "CREATE TABLE t("
        "a TIMESTAMP NOT NULL, "
        "PRIMARY KEY ((QUANTUM(a, 1, 's')), a))"),
    {ok, Query} = get_query(
        "SELECT time(a, 1s), COUNT(*) FROM t "
        "WHERE a > 1 AND a < 6 "
        "GROUP BY time(x,1s);"),
    ?assertMatch(
       {error,{invalid_query,<<_/binary>>}},
       compile(DDL,Query)
    ).

group_by_on_non_existing_column_returns_error_test() ->
    DDL = get_ddl(
        "CREATE TABLE t("
        "a TIMESTAMP NOT NULL, "
        "PRIMARY KEY ((QUANTUM(a, 1, 's')), a))"),
    {ok, Query} = get_query(
        "SELECT time(a, 1s), COUNT(*) FROM t "
        "WHERE a > 1 AND a < 6 "
        "GROUP BY x;"),
    ?assertMatch(
       {error,{invalid_query,<<_/binary>>}},
       compile(DDL,Query)
    ).

find_filters_on_additional_local_key_fields_test() ->
    ?assertEqual(
        [{<<"a">>,{'=',{field,<<"a">>,integer},{const, 100}}}],
        find_filters_on_additional_local_key_fields(
            [<<"a">>],
            {'=',{field,<<"a">>,integer},{const, 100}})
    ).

rewrite_where_with_additional_filters_test() ->
    ?assertPropsEqual(
        [{startkey,[{<<"c">>,timestamp,5500},{<<"a">>,sint64,100}]},
         {endkey,  [{<<"c">>,timestamp,5000},{<<"a">>,sint64,100}]},
         {filter, {'=',{field,<<"a">>,integer},{const, 100}}},
         {end_inclusive, true},
         {start_inclusive, true}],
        rewrite_where_with_additional_filters(
            [<<"a">>],
            [ascending],
            fun([<<"a">>]) -> sint64 end,
            [{startkey,[{<<"c">>,timestamp,5500}]},
             {endkey,  [{<<"c">>,timestamp,5000}]},
             {filter,   {'=',{field,<<"a">>,integer},{const, 100}}},
             {end_inclusive,true},
             {start_inclusive,true}])
    ).

additional_local_key_cols_added_to_query_keys_test() ->
    DDL = get_ddl(
        "CREATE TABLE table1("
        "a TIMESTAMP NOT NULL, "
        "b SINT64 NOT NULL, "
        "PRIMARY KEY ((quantum(a,1,'s')),a,b))"),
    {ok, Q} = get_query(
        "SELECT * FROM table1 "
        "WHERE b = 1 AND a >= 4200 AND a <= 4600"),
    {ok, [S|_]} = compile(DDL, Q),
    ?assertPropsEqual(
        [{startkey,[{<<"a">>,timestamp,4200},{<<"b">>,sint64,1}]},
         {endkey,[{<<"a">>,timestamp,4600},{<<"b">>,sint64,1}]},
         {filter,{'=',{field,<<"b">>,sint64},{const, 1}}},
         {end_inclusive,true},
         {start_inclusive,true}],
        S?SQL_SELECT.'WHERE'
    ).

greater_than_filter_added_to_start_key_if_it_part_of_local_key_test() ->
    DDL = get_ddl(
        "CREATE TABLE table1("
        "a TIMESTAMP NOT NULL, "
        "b SINT64 NOT NULL, "
        "PRIMARY KEY ((quantum(a,1,'s')),a,b))"),
    {ok, Q} = get_query(
        "SELECT * FROM table1 "
        "WHERE b > 1 AND a >= 4200 AND a <= 4600"),
    {ok, [S|_]} = compile(DDL, Q),
    ?assertPropsEqual(
        [{startkey,[{<<"a">>,timestamp,4200},{<<"b">>,sint64,1}]},
         {endkey,[{<<"a">>,timestamp,4600}]},
         {filter,{'>',{field,<<"b">>,sint64},{const, 1}}},
         {start_inclusive,true},
         {end_inclusive,true}],
        S?SQL_SELECT.'WHERE'
    ).

greater_than_filter_added_to_start_key_if_it_part_of_local_key_when_start_inclusive_false_test() ->
    DDL = get_ddl(
        "CREATE TABLE table1("
        "a TIMESTAMP NOT NULL, "
        "b SINT64 NOT NULL, "
        "PRIMARY KEY ((quantum(a,1,'s')),a,b))"),
    {ok, Q} = get_query(
        "SELECT * FROM table1 "
        "WHERE b > 1 AND a > 4200 AND a <= 4600"),
    {ok, [S|_]} = compile(DDL, Q),
    ?assertPropsEqual(
        [{startkey,[{<<"a">>,timestamp,4201},{<<"b">>,sint64,1}]},
         {endkey,[{<<"a">>,timestamp,4600}]},
         {filter,{'>',{field,<<"b">>,sint64},{const, 1}}},
         {end_inclusive,true}],
        S?SQL_SELECT.'WHERE'
    ).

greater_than_or_equal_filter_added_to_start_key_if_it_part_of_local_key_test() ->
    DDL = get_ddl(
        "CREATE TABLE table1("
        "a TIMESTAMP NOT NULL, "
        "b SINT64 NOT NULL, "
        "PRIMARY KEY ((quantum(a,1,'s')),a,b))"),
    {ok, Q} = get_query(
        "SELECT * FROM table1 "
        "WHERE b >= 1 AND a >= 4200 AND a <= 4600"),
    {ok, [S|_]} = compile(DDL, Q),
    ?assertPropsEqual(
        [{startkey,[{<<"a">>,timestamp,4200},{<<"b">>,sint64,1}]},
         {endkey,[{<<"a">>,timestamp,4600}]},
         {filter,{'>=',{field,<<"b">>,sint64},{const, 1}}},
         {start_inclusive,true},
         {end_inclusive,true}],
        S?SQL_SELECT.'WHERE'
    ).

%% if the pk is (a) and lk is (a,b,c) then b AND c must have filters for c
%% to be added to the key. If b does not have a filter but c does, then neither
%% is added.
additional_local_key_cols_must_be_specified_consecutively_test() ->
    DDL = get_ddl(
        "CREATE TABLE table1("
        "a TIMESTAMP NOT NULL, "
        "b SINT64 NOT NULL, "
        "c SINT64 NOT NULL, "
        "PRIMARY KEY ((quantum(a,1,'s')),a,b,c))"),
    {ok, Q} = get_query(
        "SELECT * FROM table1 "
        "WHERE c = 2 AND a >= 4200 AND a <= 4600"),
    {ok, [S|_]} = compile(DDL, Q),
    ?assertPropsEqual(
        [{startkey,[{<<"a">>,timestamp,4200}]},
         {endkey,[{<<"a">>,timestamp,4600}]},
         {filter,{'=',{field,<<"c">>,sint64},{const, 2}}},
         {end_inclusive,true}],
        S?SQL_SELECT.'WHERE'
    ).

is_null_clause_on_non_null_column_is_impossible_test() ->
    DDL = get_ddl(
        "CREATE TABLE table1("
        "a TIMESTAMP NOT NULL, "
        "b SINT64 NOT NULL, "
        "PRIMARY KEY ((a),a))"),
    {ok, Q} = get_query(
        "SELECT * FROM table1 "
        "WHERE b IS NULL AND a = 4600"),
    ?assertEqual(
        {error,{impossible_where_clause, << >>}},
        compile(DDL, Q)
    ).

same_equality_check_twice_has_duplicate_removed_test() ->
    DDL = get_ddl(
        "CREATE TABLE table1("
        "a TIMESTAMP NOT NULL, "
        "b SINT64 NOT NULL, "
        "PRIMARY KEY ((a),a))"),
    {ok, Q} = get_query(
        "SELECT * FROM table1 "
        "WHERE a = 5 AND b = 10 AND b = 10"),
    {ok, [S|_]} = compile(DDL, Q),
    ?assertPropsEqual(
        [{startkey,[{<<"a">>,timestamp,5}]},
         {endkey,  [{<<"a">>,timestamp,5}]},
         {filter, {'=',{field,<<"b">>,sint64},{const, 10}}},
         {end_inclusive,true}],
        S?SQL_SELECT.'WHERE'
    ).

same_equality_check_on_additional_local_key_col_is_not_in_filter_twice_test() ->
    DDL = get_ddl(
        "CREATE TABLE table1("
        "a TIMESTAMP NOT NULL, "
        "b SINT64 NOT NULL, "
        "PRIMARY KEY ((a),a,b))"),
    {ok, Q} = get_query(
        "SELECT * FROM table1 "
        "WHERE a = 5 AND b = 10 AND b = 10"),
    {ok, [S|_]} = compile(DDL, Q),
    ?assertPropsEqual(
        [{startkey,[{<<"a">>,timestamp,5},{<<"b">>,sint64,10}]},
         {endkey,  [{<<"a">>,timestamp,5},{<<"b">>,sint64,10}]},
         {filter, {'=',{field,<<"b">>,sint64},{const, 10}}},
         {start_inclusive,true},
         {end_inclusive,true}],
        S?SQL_SELECT.'WHERE'
    ).

checks_for_different_values_on_the_same_col_is_impossible_test() ->
    DDL = get_ddl(
        "CREATE TABLE table1("
        "a TIMESTAMP NOT NULL, "
        "b SINT64 NOT NULL, "
        "PRIMARY KEY ((a),a))"),
    {ok, Q} = get_query(
        "SELECT * FROM table1 "
        "WHERE b = 5 AND b = 6 AND a = 4600"),
    ?assertEqual(
        {error,{impossible_where_clause, << >>}},
        compile(DDL, Q)
    ).

second_greater_than_check_which_is_a_greater_value_is_removed_test() ->
    DDL = get_ddl(
        "CREATE TABLE table1("
        "a TIMESTAMP NOT NULL, "
        "b SINT64 NOT NULL, "
        "PRIMARY KEY ((a),a))"),
    {ok, Q} = get_query(
        "SELECT * FROM table1 "
        "WHERE a = 5 AND b > 10 AND b > 11"),
    {ok, [S|_]} = compile(DDL, Q),
    ?assertPropsEqual(
        [{startkey,[{<<"a">>,timestamp,5}]},
         {endkey,  [{<<"a">>,timestamp,5}]},
         {filter, {'>',{field,<<"b">>,sint64},{const, 11}}},
         {end_inclusive,true}],
        S?SQL_SELECT.'WHERE'
    ).


second_greater_than_check_which_is_a_lesser_value_removes_the_first_test() ->
    DDL = get_ddl(
        "CREATE TABLE table1("
        "a TIMESTAMP NOT NULL, "
        "b SINT64 NOT NULL, "
        "PRIMARY KEY ((a),a))"),
    {ok, Q} = get_query(
        "SELECT * FROM table1 "
        "WHERE a = 5 AND b > 11 AND b > 10"),
    {ok, [S|_]} = compile(DDL, Q),
    ?assertPropsEqual(
        [{startkey,[{<<"a">>,timestamp,5}]},
         {endkey,  [{<<"a">>,timestamp,5}]},
         {filter, {'>',{field,<<"b">>,sint64},{const, 11}}},
         {end_inclusive,true}],
        S?SQL_SELECT.'WHERE'
    ).

clause_for_equality_and_greater_than_equality_is_not_possible_test() ->
    DDL = get_ddl(
        "CREATE TABLE table1("
        "a TIMESTAMP NOT NULL, "
        "b SINT64 NOT NULL, "
        "PRIMARY KEY ((a),a))"),
    {ok, Q} = get_query(
        "SELECT * FROM table1 "
        "WHERE b = 5 AND b > 6 AND a = 4600"),
    ?assertEqual(
        {error,{impossible_where_clause, << >>}},
        compile(DDL, Q)
    ).

clause_for_equality_and_greater_than_is_not_possible_swap_lhs_rhs_test() ->
    DDL = get_ddl(
        "CREATE TABLE table1("
        "a TIMESTAMP NOT NULL, "
        "b SINT64 NOT NULL, "
        "PRIMARY KEY ((a),a))"),
    {ok, Q} = get_query(
        "SELECT * FROM table1 "
        "WHERE b > 6 AND b = 5 AND a = 4600"),
    ?assertEqual(
        {error,{impossible_where_clause, << >>}},
        compile(DDL, Q)
    ).

greater_than_value_lower_than_equality_is_eliminated_test() ->
    DDL = get_ddl(
        "CREATE TABLE table1("
        "a TIMESTAMP NOT NULL, "
        "b SINT64 NOT NULL, "
        "PRIMARY KEY ((a),a))"),
    {ok, Q} = get_query(
        "SELECT * FROM table1 "
        "WHERE a = 5 AND b = 10 AND b > 5"),
    {ok, [S|_]} = compile(DDL, Q),
    ?assertPropsEqual(
        [{startkey,[{<<"a">>,timestamp,5}]},
         {endkey,  [{<<"a">>,timestamp,5}]},
         {filter, {'=',{field,<<"b">>,sint64},{const, 10}}},
         {end_inclusive,true}],
        S?SQL_SELECT.'WHERE'
    ).

greater_than_value_lower_than_equality_is_eliminated_swap_lhs_rhs_test() ->
    DDL = get_ddl(
        "CREATE TABLE table1("
        "a TIMESTAMP NOT NULL, "
        "b SINT64 NOT NULL, "
        "PRIMARY KEY ((a),a))"),
    {ok, Q} = get_query(
        "SELECT * FROM table1 "
        "WHERE a = 5 AND b > 5 AND b = 10"),
    {ok, [S|_]} = compile(DDL, Q),
    ?assertPropsEqual(
        [{startkey,[{<<"a">>,timestamp,5}]},
         {endkey,  [{<<"a">>,timestamp,5}]},
         {filter, {'=',{field,<<"b">>,sint64},{const, 10}}},
         {end_inclusive,true}],
        S?SQL_SELECT.'WHERE'
    ).

same_great_than_or_equals_check_is_not_in_filter_twice_test() ->
    DDL = get_ddl(
        "CREATE TABLE table1("
        "a TIMESTAMP NOT NULL, "
        "b SINT64 NOT NULL, "
        "PRIMARY KEY ((a),a))"),
    {ok, Q} = get_query(
        "SELECT * FROM table1 "
        "WHERE a = 5 AND b >= 10 AND b >= 10"),
    {ok, [S|_]} = compile(DDL, Q),
    ?assertPropsEqual(
        [{startkey,[{<<"a">>,timestamp,5}]},
         {endkey,  [{<<"a">>,timestamp,5}]},
         {filter, {'>=',{field,<<"b">>,sint64},{const, 10}}},
         {end_inclusive,true}],
        S?SQL_SELECT.'WHERE'
    ).

second_greater_than_or_equal_check_which_is_a_greater_value_is_removed_test() ->
    DDL = get_ddl(
        "CREATE TABLE table1("
        "a TIMESTAMP NOT NULL, "
        "b SINT64 NOT NULL, "
        "PRIMARY KEY ((a),a))"),
    {ok, Q} = get_query(
        "SELECT * FROM table1 "
        "WHERE a = 5 AND b >= 10 AND b >= 11"),
    {ok, [S|_]} = compile(DDL, Q),
    ?assertPropsEqual(
        [{startkey,[{<<"a">>,timestamp,5}]},
         {endkey,  [{<<"a">>,timestamp,5}]},
         {filter, {'>=',{field,<<"b">>,sint64},{const, 11}}},
         {end_inclusive,true}],
        S?SQL_SELECT.'WHERE'
    ).

second_greater_than_or_equal_check_which_is_a_lesser_value_removes_the_first_test() ->
    DDL = get_ddl(
        "CREATE TABLE table1("
        "a TIMESTAMP NOT NULL, "
        "b SINT64 NOT NULL, "
        "PRIMARY KEY ((a),a))"),
    {ok, Q} = get_query(
        "SELECT * FROM table1 "
        "WHERE a = 5 AND b >= 11 AND b >= 10"),
    {ok, [S|_]} = compile(DDL, Q),
    ?assertPropsEqual(
        [{startkey,[{<<"a">>,timestamp,5}]},
         {endkey,  [{<<"a">>,timestamp,5}]},
         {filter, {'>=',{field,<<"b">>,sint64},{const, 11}}},
         {end_inclusive,true}],
        S?SQL_SELECT.'WHERE'
    ).

greater_than_or_equal_to_value_lower_than_equality_is_eliminated_test() ->
    DDL = get_ddl(
        "CREATE TABLE table1("
        "a TIMESTAMP NOT NULL, "
        "b SINT64 NOT NULL, "
        "PRIMARY KEY ((a),a))"),
    {ok, Q} = get_query(
        "SELECT * FROM table1 "
        "WHERE a = 5 AND b = 10 AND b >= 5"),
    {ok, [S|_]} = compile(DDL, Q),
    ?assertPropsEqual(
        [{startkey,[{<<"a">>,timestamp,5}]},
         {endkey,  [{<<"a">>,timestamp,5}]},
         {filter, {'=',{field,<<"b">>,sint64},{const, 10}}},
         {end_inclusive,true}],
        S?SQL_SELECT.'WHERE'
    ).

greater_than_or_equal_to_value_lower_than_equality_is_eliminated_swap_lhs_rhs_test() ->
    DDL = get_ddl(
        "CREATE TABLE table1("
        "a TIMESTAMP NOT NULL, "
        "b SINT64 NOT NULL, "
        "PRIMARY KEY ((a),a))"),
    {ok, Q} = get_query(
        "SELECT * FROM table1 "
        "WHERE a = 5 AND b >= 5 AND b = 10"),
    {ok, [S|_]} = compile(DDL, Q),
    ?assertPropsEqual(
        [{startkey,[{<<"a">>,timestamp,5}]},
         {endkey,  [{<<"a">>,timestamp,5}]},
         {filter, {'=',{field,<<"b">>,sint64},{const, 10}}},
         {end_inclusive,true}],
        S?SQL_SELECT.'WHERE'
    ).

clause_for_equality_and_greater_than_or_equalit_to_is_not_possible_swap_lhs_rhs_test() ->
    DDL = get_ddl(
        "CREATE TABLE table1("
        "a TIMESTAMP NOT NULL, "
        "b SINT64 NOT NULL, "
        "PRIMARY KEY ((a),a))"),
    {ok, Q} = get_query(
        "SELECT * FROM table1 "
        "WHERE b >= 6 AND b = 5 AND a = 4600"),
    ?assertEqual(
        {error,{impossible_where_clause, << >>}},
        compile(DDL, Q)
    ).

greater_than_or_equal_to_is_less_than_previous_greater_than_clause_removes_greater_than_clause_test() ->
    DDL = get_ddl(
        "CREATE TABLE table1("
        "a TIMESTAMP NOT NULL, "
        "b SINT64 NOT NULL, "
        "PRIMARY KEY ((a),a))"),
    {ok, Q} = get_query(
        "SELECT * FROM table1 "
        "WHERE a = 5 AND b > 10 AND b >= 10"),
    {ok, [S|_]} = compile(DDL, Q),
    ?assertPropsEqual(
        [{startkey,[{<<"a">>,timestamp,5}]},
         {endkey,  [{<<"a">>,timestamp,5}]},
         {filter, {'>',{field,<<"b">>,sint64},{const, 10}}},
         {end_inclusive,true}],
        S?SQL_SELECT.'WHERE'
    ).

greater_than_or_equal_to_is_less_than_previous_greater_than_clause_removes_greater_than_clause_swap_lhs_rhs_test() ->
    DDL = get_ddl(
        "CREATE TABLE table1("
        "a TIMESTAMP NOT NULL, "
        "b SINT64 NOT NULL, "
        "PRIMARY KEY ((a),a))"),
    {ok, Q} = get_query(
        "SELECT * FROM table1 "
        "WHERE a = 5 AND b >= 10 AND b > 10"),
    {ok, [S|_]} = compile(DDL, Q),
    ?assertPropsEqual(
        [{startkey,[{<<"a">>,timestamp,5}]},
         {endkey,  [{<<"a">>,timestamp,5}]},
         {filter, {'>',{field,<<"b">>,sint64},{const, 10}}},
         {end_inclusive,true}],
        S?SQL_SELECT.'WHERE'
    ).

greater_than_is_less_than_previous_greater_than_or_equal_to_clause_removes_greater_than_or_equal_to_clause_test() ->
    DDL = get_ddl(
        "CREATE TABLE table1("
        "a TIMESTAMP NOT NULL, "
        "b SINT64 NOT NULL, "
        "PRIMARY KEY ((a),a))"),
    {ok, Q} = get_query(
        "SELECT * FROM table1 "
        "WHERE a = 5 AND b > 9 AND b >= 10"),
    {ok, [S|_]} = compile(DDL, Q),
    ?assertPropsEqual(
        [{startkey,[{<<"a">>,timestamp,5}]},
         {endkey,  [{<<"a">>,timestamp,5}]},
         {filter, {'>=',{field,<<"b">>,sint64},{const, 10}}},
         {end_inclusive,true}],
        S?SQL_SELECT.'WHERE'
    ).

greater_than_is_less_than_previous_greater_than_or_equal_to_clause_removes_greater_than_or_equal_to_clause_swap_lhs_rhs_test() ->
    DDL = get_ddl(
        "CREATE TABLE table1("
        "a TIMESTAMP NOT NULL, "
        "b SINT64 NOT NULL, "
        "PRIMARY KEY ((a),a))"),
    {ok, Q} = get_query(
        "SELECT * FROM table1 "
        "WHERE a = 5 AND b >= 10 AND b > 9"),
    {ok, [S|_]} = compile(DDL, Q),
    ?assertPropsEqual(
        [{startkey,[{<<"a">>,timestamp,5}]},
         {endkey,  [{<<"a">>,timestamp,5}]},
         {filter, {'>=',{field,<<"b">>,sint64},{const, 10}}},
         {end_inclusive,true}],
        S?SQL_SELECT.'WHERE'
    ).

less_than_filters_get_reduced_test() ->
    DDL = get_ddl(
        "CREATE TABLE table1("
        "a TIMESTAMP NOT NULL, "
        "b SINT64 NOT NULL, "
        "PRIMARY KEY ((a),a))"),
    {ok, Q} = get_query(
        "SELECT * FROM table1 "
        "WHERE a = 5 AND b < 10 AND b < 9"),
    {ok, [S|_]} = compile(DDL, Q),
    ?assertPropsEqual(
        [{startkey,[{<<"a">>,timestamp,5}]},
         {endkey,  [{<<"a">>,timestamp,5}]},
         {filter, {'<',{field,<<"b">>,sint64},{const,9}}},
         {end_inclusive,true}],
        S?SQL_SELECT.'WHERE'
    ).

less_than_filters_get_reduced_swap_filter_order_test() ->
    DDL = get_ddl(
        "CREATE TABLE table1("
        "a TIMESTAMP NOT NULL, "
        "b SINT64 NOT NULL, "
        "PRIMARY KEY ((a),a))"),
    {ok, Q} = get_query(
        "SELECT * FROM table1 "
        "WHERE a = 5 AND b < 9 AND b < 10"),
    {ok, [S|_]} = compile(DDL, Q),
    ?assertPropsEqual(
        [{startkey,[{<<"a">>,timestamp,5}]},
         {endkey,  [{<<"a">>,timestamp,5}]},
         {filter, {'<',{field,<<"b">>,sint64},{const,9}}},
         {end_inclusive,true}],
        S?SQL_SELECT.'WHERE'
    ).

less_than_eliminated_if_it_is_greater_than_equals_to_on_same_column_test() ->
    DDL = get_ddl(
        "CREATE TABLE table1("
        "a TIMESTAMP NOT NULL, "
        "b SINT64 NOT NULL, "
        "PRIMARY KEY ((a),a))"),
    {ok, Q} = get_query(
        "SELECT * FROM table1 "
        "WHERE a = 5 AND b = 9 AND b < 10"),
    {ok, [S|_]} = compile(DDL, Q),
    ?assertPropsEqual(
        [{startkey,[{<<"a">>,timestamp,5}]},
         {endkey,  [{<<"a">>,timestamp,5}]},
         {filter, {'=',{field,<<"b">>,sint64},{const,9}}},
         {end_inclusive,true}],
        S?SQL_SELECT.'WHERE'
    ).

less_than_duplicates_are_eliminated_test() ->
    DDL = get_ddl(
        "CREATE TABLE table1("
        "a TIMESTAMP NOT NULL, "
        "b SINT64 NOT NULL, "
        "PRIMARY KEY ((a),a))"),
    {ok, Q} = get_query(
        "SELECT * FROM table1 "
        "WHERE a = 5 AND b < 10 AND b < 10"),
    {ok, [S|_]} = compile(DDL, Q),
    ?assertPropsEqual(
        [{startkey,[{<<"a">>,timestamp,5}]},
         {endkey,  [{<<"a">>,timestamp,5}]},
         {filter, {'<',{field,<<"b">>,sint64},{const,10}}},
         {end_inclusive,true}],
        S?SQL_SELECT.'WHERE'
    ).

query_impossible_if_less_than_is_equal_to_equality_filter_on_same_column_test() ->
    DDL = get_ddl(
        "CREATE TABLE table1("
        "a TIMESTAMP NOT NULL, "
        "b SINT64 NOT NULL, "
        "PRIMARY KEY ((a),a))"),
    {ok, Q} = get_query(
        "SELECT * FROM table1 "
        "WHERE a = 5 AND b = 9 AND b < 9"),
    ?assertEqual(
        {error,{impossible_where_clause, << >>}},
        compile(DDL, Q)
    ).

query_impossible_if_less_than_is_equal_to_equality_filter_on_same_column_swap_filter_order_test() ->
    DDL = get_ddl(
        "CREATE TABLE table1("
        "a TIMESTAMP NOT NULL, "
        "b SINT64 NOT NULL, "
        "PRIMARY KEY ((a),a))"),
    {ok, Q} = get_query(
        "SELECT * FROM table1 "
        "WHERE a = 5 AND b < 9 AND b = 9"),
    ?assertEqual(
        {error,{impossible_where_clause, << >>}},
        compile(DDL, Q)
    ).

query_impossible_if_less_than_is_less_than_equality_filter_on_same_column_test() ->
    DDL = get_ddl(
        "CREATE TABLE table1("
        "a TIMESTAMP NOT NULL, "
        "b SINT64 NOT NULL, "
        "PRIMARY KEY ((a),a))"),
    {ok, Q} = get_query(
        "SELECT * FROM table1 "
        "WHERE a = 5 AND b = 9 AND b < 8"),
    ?assertEqual(
        {error,{impossible_where_clause, << >>}},
        compile(DDL, Q)
    ).

query_impossible_if_less_than_is_less_than_equality_filter_on_same_column_swap_filter_order_test() ->
    DDL = get_ddl(
        "CREATE TABLE table1("
        "a TIMESTAMP NOT NULL, "
        "b SINT64 NOT NULL, "
        "PRIMARY KEY ((a),a))"),
    {ok, Q} = get_query(
        "SELECT * FROM table1 "
        "WHERE a = 5 AND b < 8 AND b = 9"),
    ?assertEqual(
        {error,{impossible_where_clause, << >>}},
        compile(DDL, Q)
    ).

less_than_on_column_in_local_key_is_added_to_endkey_test() ->
    DDL = get_ddl(
        "CREATE TABLE table1("
        "a TIMESTAMP NOT NULL, "
        "b SINT64 NOT NULL, "
        "PRIMARY KEY ((a),a,b))"),
    {ok, Q} = get_query(
        "SELECT * FROM table1 "
        "WHERE a = 5 AND b < 10"),
    {ok, [S|_]} = compile(DDL, Q),
    ?assertPropsEqual(
        [{startkey,[{<<"a">>,timestamp,5}]},
         {endkey,  [{<<"a">>,timestamp,5},{<<"b">>,sint64,10}]},
         {filter, {'<',{field,<<"b">>,sint64},{const,10}}},
         {end_inclusive,true}],
        S?SQL_SELECT.'WHERE'
    ).

duplicate_less_than_or_equal_to_filters_are_eliminated_test() ->
    DDL = get_ddl(
        "CREATE TABLE table1("
        "a TIMESTAMP NOT NULL, "
        "b SINT64 NOT NULL, "
        "PRIMARY KEY ((a),a))"),
    {ok, Q} = get_query(
        "SELECT * FROM table1 "
        "WHERE a = 5 AND b <= 8 AND b <= 8"),
    {ok, [S|_]} = compile(DDL, Q),
    ?assertPropsEqual(
        [{startkey,[{<<"a">>,timestamp,5}]},
         {endkey,  [{<<"a">>,timestamp,5}]},
         {filter, {'<=',{field,<<"b">>,sint64},{const,8}}},
         {end_inclusive,true}],
        S?SQL_SELECT.'WHERE'
    ).

less_than_or_equal_to_filters_are_eliminated_when_equality_filter_value_is_equal_test() ->
    DDL = get_ddl(
        "CREATE TABLE table1("
        "a TIMESTAMP NOT NULL, "
        "b SINT64 NOT NULL, "
        "PRIMARY KEY ((a),a))"),
    {ok, Q} = get_query(
        "SELECT * FROM table1 "
        "WHERE a = 5 AND b = 8 AND b <= 8"),
    {ok, [S|_]} = compile(DDL, Q),
    ?assertPropsEqual(
        [{startkey,[{<<"a">>,timestamp,5}]},
         {endkey,  [{<<"a">>,timestamp,5}]},
         {filter, {'=',{field,<<"b">>,sint64},{const,8}}},
         {end_inclusive,true}],
        S?SQL_SELECT.'WHERE'
    ).

less_than_or_equal_to_filters_are_eliminated_when_equality_filter_value_is_equal_swap_filter_order_test() ->
    DDL = get_ddl(
        "CREATE TABLE table1("
        "a TIMESTAMP NOT NULL, "
        "b SINT64 NOT NULL, "
        "PRIMARY KEY ((a),a))"),
    {ok, Q} = get_query(
        "SELECT * FROM table1 "
        "WHERE a = 5 AND b <= 8 AND b = 8"),
    {ok, [S|_]} = compile(DDL, Q),
    ?assertPropsEqual(
        [{startkey,[{<<"a">>,timestamp,5}]},
         {endkey,  [{<<"a">>,timestamp,5}]},
         {filter, {'=',{field,<<"b">>,sint64},{const,8}}},
         {end_inclusive,true}],
        S?SQL_SELECT.'WHERE'
    ).

less_than_or_equal_to_filters_are_eliminated_when_equality_filter_value_is_greater_test() ->
    DDL = get_ddl(
        "CREATE TABLE table1("
        "a TIMESTAMP NOT NULL, "
        "b SINT64 NOT NULL, "
        "PRIMARY KEY ((a),a))"),
    {ok, Q} = get_query(
        "SELECT * FROM table1 "
        "WHERE a = 5 AND b = 8 AND b <= 9"),
    {ok, [S|_]} = compile(DDL, Q),
    ?assertPropsEqual(
        [{startkey,[{<<"a">>,timestamp,5}]},
         {endkey,  [{<<"a">>,timestamp,5}]},
         {filter, {'=',{field,<<"b">>,sint64},{const,8}}},
         {end_inclusive,true}],
        S?SQL_SELECT.'WHERE'
    ).

less_than_or_equal_to_filters_are_eliminated_when_less_than_filter_value_is_greater_test() ->
    DDL = get_ddl(
        "CREATE TABLE table1("
        "a TIMESTAMP NOT NULL, "
        "b SINT64 NOT NULL, "
        "PRIMARY KEY ((a),a))"),
    {ok, Q} = get_query(
        "SELECT * FROM table1 "
        "WHERE a = 5 AND b < 8 AND b <= 8"),
    {ok, [S|_]} = compile(DDL, Q),
    ?assertPropsEqual(
        [{startkey,[{<<"a">>,timestamp,5}]},
         {endkey,  [{<<"a">>,timestamp,5}]},
         {filter, {'<',{field,<<"b">>,sint64},{const,8}}},
         {end_inclusive,true}],
        S?SQL_SELECT.'WHERE'
    ).

less_than_or_equal_to_filters_are_eliminated_when_less_than_filter_value_is_greater_swap_filter_order_test() ->
    DDL = get_ddl(
        "CREATE TABLE table1("
        "a TIMESTAMP NOT NULL, "
        "b SINT64 NOT NULL, "
        "PRIMARY KEY ((a),a))"),
    {ok, Q} = get_query(
        "SELECT * FROM table1 "
        "WHERE a = 5 AND b <= 8 AND b < 8"),
    {ok, [S|_]} = compile(DDL, Q),
    ?assertPropsEqual(
        [{startkey,[{<<"a">>,timestamp,5}]},
         {endkey,  [{<<"a">>,timestamp,5}]},
         {filter, {'<',{field,<<"b">>,sint64},{const,8}}},
         {end_inclusive,true}],
        S?SQL_SELECT.'WHERE'
    ).

less_than_or_equal_to_on_column_in_local_key_is_added_to_endkey_test() ->
    DDL = get_ddl(
        "CREATE TABLE table1("
        "a TIMESTAMP NOT NULL, "
        "b SINT64 NOT NULL, "
        "PRIMARY KEY ((a),a,b))"),
    {ok, Q} = get_query(
        "SELECT * FROM table1 "
        "WHERE a = 5 AND b <= 10"),
    {ok, [S|_]} = compile(DDL, Q),
    ?assertPropsEqual(
        [{startkey,[{<<"a">>,timestamp,5}]},
         {endkey,  [{<<"a">>,timestamp,5},{<<"b">>,sint64,10}]},
         {filter, {'<=',{field,<<"b">>,sint64},{const,10}}},
         {end_inclusive,true}],
        S?SQL_SELECT.'WHERE'
    ).

% less_than_or_equal_to_is_less_than_equality_filter_on_same_column_is_impossible_test

'< eliminated when greater than <= on same column_test'() ->
    DDL = get_ddl(
        "CREATE TABLE table1("
        "a TIMESTAMP NOT NULL, "
        "b SINT64 NOT NULL, "
        "PRIMARY KEY ((a),a,b))"),
    {ok, Q} = get_query(
        "SELECT * FROM table1 "
        "WHERE a = 5 AND b <= 10 AND b < 11"),
    {ok, [S]} = compile(DDL, Q),
    ?assertPropsEqual(
        [{startkey,[{<<"a">>,timestamp,5}]},
         {endkey,  [{<<"a">>,timestamp,5},{<<"b">>,sint64,10}]},
         {filter, {'<=',{field,<<"b">>,sint64},{const,10}}},
         {end_inclusive,true}],
        S?SQL_SELECT.'WHERE'
    ).

'< eliminated when greater than <= on same column_swap_filter_order_test'() ->
    DDL = get_ddl(
        "CREATE TABLE table1("
        "a TIMESTAMP NOT NULL, "
        "b SINT64 NOT NULL, "
        "PRIMARY KEY ((a),a,b))"),
    {ok, Q} = get_query(
        "SELECT * FROM table1 "
        "WHERE a = 5 AND b < 11 AND b <= 10"),
    {ok, [S]} = compile(DDL, Q),
    ?assertPropsEqual(
        [{startkey,[{<<"a">>,timestamp,5}]},
         {endkey,  [{<<"a">>,timestamp,5},{<<"b">>,sint64,10}]},
         {filter, {'<=',{field,<<"b">>,sint64},{const,10}}},
         {end_inclusive,true}],
        S?SQL_SELECT.'WHERE'
    ).

less_than_or_equal_to_is_less_than_equality_filter_on_same_column_is_impossible_test() ->
    DDL = get_ddl(
        "CREATE TABLE table1("
        "a TIMESTAMP NOT NULL, "
        "b SINT64 NOT NULL, "
        "PRIMARY KEY ((a),a,b))"),
    {ok, Q} = get_query(
        "SELECT * FROM table1 "
        "WHERE a = 1 AND b <= 8 AND b = 10"),
    ?assertEqual(
        {error,{impossible_where_clause, << >>}},
        compile(DDL, Q)
    ).

less_than_or_equal_to_is_less_than_equality_filter_on_same_column_is_impossible_swap_filter_order_test() ->
    DDL = get_ddl(
        "CREATE TABLE table1("
        "a TIMESTAMP NOT NULL, "
        "b SINT64 NOT NULL, "
        "PRIMARY KEY ((a),a,b))"),
    {ok, Q} = get_query(
        "SELECT * FROM table1 "
        "WHERE a = 1 AND b = 10 AND b <= 8"),
    ?assertEqual(
        {error,{impossible_where_clause, << >>}},
        compile(DDL, Q)
    ).

desc_query_with_additional_column_in_local_key_test() ->
    DDL = get_ddl(
        "CREATE TABLE table3 ("
        "a VARCHAR NOT NULL,"
        "b TIMESTAMP NOT NULL,"
        "c VARCHAR NOT NULL,"
        "PRIMARY KEY ((a, QUANTUM(b, 1, 'm')),a, b DESC, c))"
    ),
    {ok, Q} = get_query(
          "SELECT * FROM table3 "
          "WHERE a = 'dby' AND b >= 3500 AND b <= 5500"),
    {ok, [?SQL_SELECT{'WHERE' = W}]} = compile(DDL, Q),
    ?assertEqual(
        [{startkey,[{<<"a">>,varchar,<<"dby">>},{<<"b">>,timestamp,5500}]},
         {endkey,  [{<<"a">>,varchar,<<"dby">>},{<<"b">>,timestamp,3500}]},
         {filter,[]},
         {end_inclusive,true},
         {start_inclusive,true}],
        W
    ).

desc_query_without_additional_column_in_local_key_test() ->
    DDL = get_ddl(
        "CREATE TABLE table3 ("
        "a VARCHAR NOT NULL,"
        "b TIMESTAMP NOT NULL,"
        "c VARCHAR NOT NULL,"
        "PRIMARY KEY ((a, QUANTUM(b, 1, 'm')),a, b DESC))"
    ),
    {ok, Q} = get_query(
          "SELECT * FROM table3 "
          "WHERE a = 'dby' AND b >= 3500 AND b <= 5500"),
    {ok, [?SQL_SELECT{'WHERE' = W}]} = compile(DDL, Q),
    ?assertEqual(
        [{startkey,[{<<"a">>,varchar,<<"dby">>},{<<"b">>,timestamp,5500}]},
         {endkey,  [{<<"a">>,varchar,<<"dby">>},{<<"b">>,timestamp,3500}]},
         {filter,[]},
         {end_inclusive,true},
         {start_inclusive,true}],
        W
    ).

desc_query_with_column_names_test() ->
    DDL = get_ddl(
        "CREATE TABLE table3 ("
        "b VARCHAR NOT NULL,"
        "a TIMESTAMP NOT NULL,"
        "PRIMARY KEY ((b, QUANTUM(a, 1, 'm')),b,a DESC))"
    ),
    {ok, Q} = get_query(
          "SELECT * FROM table3 "
          "WHERE b = 'dby' AND a >= 3500 AND a <= 5500"),
    {ok, [?SQL_SELECT{'WHERE' = W}]} = compile(DDL, Q),
    ?assertEqual(
        [{startkey,[{<<"b">>,varchar,<<"dby">>},{<<"a">>,timestamp,5500}]},
         {endkey,  [{<<"b">>,varchar,<<"dby">>},{<<"a">>,timestamp,3500}]},
         {filter,[]},
         {end_inclusive,true},
         {start_inclusive,true}],
        W
    ).

query_with_desc_on_local_key_additional_column_test() ->
    DDL = get_ddl(
        "CREATE TABLE table3("
        "a SINT64 NOT NULL, "
        "b VARCHAR NOT NULL, "
        "c TIMESTAMP NOT NULL, "
        "d VARCHAR NOT NULL, "
        "PRIMARY KEY ((a,b,quantum(c, 1, 'm')), a,b, c, d DESC))"
    ),
    {ok, Q} = get_query(
          "SELECT * FROM table3 "
          "WHERE a = 1 AND b = 'dby' AND c >= 3500 AND c <= 5500"),
    {ok, [?SQL_SELECT{'WHERE' = W}]} = compile(DDL, Q),
    ?assertEqual(
        [{startkey,[{<<"a">>,sint64,1},{<<"b">>,varchar,<<"dby">>},{<<"c">>,timestamp,3500}]},
         {endkey,  [{<<"a">>,sint64,1},{<<"b">>,varchar,<<"dby">>},{<<"c">>,timestamp,5500}]},
         {filter,[]},
         {end_inclusive,true}],
        W
    ).

query_with_desc_on_local_key_additional_column_multi_quanta_test() ->
    DDL = get_ddl(
        "CREATE TABLE table3("
        "a SINT64 NOT NULL, "
        "b VARCHAR NOT NULL, "
        "c TIMESTAMP NOT NULL, "
        "d VARCHAR NOT NULL, "
        "PRIMARY KEY ((a,b,quantum(c, 1, 's')), a,b, c, d DESC))"
    ),
    {ok, Q} = get_query(
          "SELECT * FROM table3 "
          "WHERE a = 1 AND b = 'dby' AND c >= 3500 AND c <= 5500"),
    {ok, SubQueries} = compile(DDL, Q),
    ?assertEqual(
        [[{startkey,[{<<"a">>,sint64,1},{<<"b">>,varchar,<<"dby">>},{<<"c">>,timestamp,3500}]},
          {endkey,  [{<<"a">>,sint64,1},{<<"b">>,varchar,<<"dby">>},{<<"c">>,timestamp,4000}]},
          {filter,[]}],
         [{startkey,[{<<"a">>,sint64,1},{<<"b">>,varchar,<<"dby">>},{<<"c">>,timestamp,4000}]},
          {endkey,  [{<<"a">>,sint64,1},{<<"b">>,varchar,<<"dby">>},{<<"c">>,timestamp,5000}]},
          {filter,[]}],
         [{startkey,[{<<"a">>,sint64,1},{<<"b">>,varchar,<<"dby">>},{<<"c">>,timestamp,5000}]},
          {endkey,  [{<<"a">>,sint64,1},{<<"b">>,varchar,<<"dby">>},{<<"c">>,timestamp,5500}]},
          {filter,[]},
          {end_inclusive,true}]],
        [W || ?SQL_SELECT{'WHERE' = W} <- SubQueries]
    ).

query_with_desc_on_not_last_local_key_column_test() ->
    DDL = get_ddl(
        "CREATE table desc1 ("
        "a VARCHAR NOT NULL,"
        "b TIMESTAMP NOT NULL,"
        "PRIMARY KEY ((a, QUANTUM(b, 1, 's')), a DESC, b));"
    ),
    {ok, Q} = get_query(
          "SELECT * FROM desc1 "
          "WHERE a = 'hi' AND b >= 3500 AND b <= 5500"),
    {ok, SubQueries} = compile(DDL, Q),
    ?assertEqual(
        [[{startkey,[{<<"a">>,varchar,<<"hi">>},{<<"b">>,timestamp,3500}]},
          {endkey,  [{<<"a">>,varchar,<<"hi">>},{<<"b">>,timestamp,4000}]},
          {filter,[]}],
         [{startkey,[{<<"a">>,varchar,<<"hi">>},{<<"b">>,timestamp,4000}]},
          {endkey,  [{<<"a">>,varchar,<<"hi">>},{<<"b">>,timestamp,5000}]},
          {filter,[]}],
         [{startkey,[{<<"a">>,varchar,<<"hi">>},{<<"b">>,timestamp,5000}]},
          {endkey,  [{<<"a">>,varchar,<<"hi">>},{<<"b">>,timestamp,5500}]},
          {filter,[]},
          {end_inclusive,true}]],
        [W || ?SQL_SELECT{'WHERE' = W} <- SubQueries]
    ).

query_with_desc_on_not_last_local_key_column_no_quantum_test() ->
    DDL = get_ddl(
        "CREATE table desc1 ("
        "a VARCHAR NOT NULL,"
        "b TIMESTAMP NOT NULL,"
        "PRIMARY KEY ((a, b), a DESC, b));"
    ),
    {ok, Q} = get_query(
          "SELECT * FROM desc1 "
          "WHERE a = 'hi' AND b = 4001"),
    {ok, SubQueries} = compile(DDL, Q),
    ?assertEqual(
        [[{startkey,[{<<"a">>,varchar,<<"hi">>},{<<"b">>,timestamp,4001}]},
          {endkey,  [{<<"a">>,varchar,<<"hi">>},{<<"b">>,timestamp,4001}]},
          {filter,[]},
          {end_inclusive, true}]],
        [W || ?SQL_SELECT{'WHERE' = W} <- SubQueries]
    ).

query_with_desc_last_local_key_column_no_quantum_test() ->
    DDL = get_ddl(
        "CREATE table desc1 ("
        "a VARCHAR NOT NULL,"
        "b TIMESTAMP NOT NULL,"
        "PRIMARY KEY ((a, b), a, b DESC));"
    ),
    {ok, Q} = get_query(
          "SELECT * FROM desc1 "
          "WHERE a = 'hi' AND b = 4001"),
    {ok, SubQueries} = compile(DDL, Q),
    ?assertEqual(
        [[{startkey,[{<<"a">>,varchar,<<"hi">>},{<<"b">>,timestamp,4001}]},
          {endkey,  [{<<"a">>,varchar,<<"hi">>},{<<"b">>,timestamp,4001}]},
          {filter,[]},
          {end_inclusive, true},
          {start_inclusive, true}]],
        [W || ?SQL_SELECT{'WHERE' = W} <- SubQueries]
    ).

query_with_arithmetic_in_where_clause_test() ->
    DDL = get_ddl(
        "CREATE table table1 ("
        "a VARCHAR NOT NULL,"
        "b TIMESTAMP NOT NULL,"
        "PRIMARY KEY ((a, b), a, b));"
    ),
    {ok, Q} = get_query(
          "SELECT * FROM table1 WHERE a = 'hi' AND b = 4000 + 1s"),
    {ok, SubQueries} = compile(DDL, Q),
    ?assertEqual(
        [[{startkey,[{<<"a">>,varchar,<<"hi">>},{<<"b">>,timestamp,5000}]},
          {endkey,  [{<<"a">>,varchar,<<"hi">>},{<<"b">>,timestamp,5000}]},
          {filter,[]},
          {end_inclusive, true}]],
        [W || ?SQL_SELECT{'WHERE' = W} <- SubQueries]
    ).

query_with_arithmetic_in_where_clause_expresson_on_lhs_test() ->
    DDL = get_ddl(
        "CREATE table table1 ("
        "a VARCHAR NOT NULL,"
        "b TIMESTAMP NOT NULL,"
        "PRIMARY KEY ((a, b), a, b));"
    ),
    {ok, Q} = get_query(
          "SELECT * FROM table1 WHERE a = 'hi' AND 4000 + 1s = b"),
    {ok, SubQueries} = compile(DDL, Q),
    ?assertEqual(
        [[{startkey,[{<<"a">>,varchar,<<"hi">>},{<<"b">>,timestamp,5000}]},
          {endkey,  [{<<"a">>,varchar,<<"hi">>},{<<"b">>,timestamp,5000}]},
          {filter,[]},
          {end_inclusive, true}]],
        [W || ?SQL_SELECT{'WHERE' = W} <- SubQueries]
    ).

query_with_arithmetic_in_where_clause_add_double_to_integer_test() ->
    DDL = get_ddl(
        "CREATE table table1 ("
        "a VARCHAR NOT NULL,"
        "b TIMESTAMP NOT NULL,"
        "PRIMARY KEY ((a, b), a, b));"
    ),
    {ok, Q} = get_query(
          "SELECT * FROM table1 WHERE a = 'hi' AND b = 4000 + 2.4"),
    {ok, SubQueries} = compile(DDL, Q),
    ?assertEqual(
        [[{startkey,[{<<"a">>,varchar,<<"hi">>},{<<"b">>,timestamp,4002}]},
          {endkey,  [{<<"a">>,varchar,<<"hi">>},{<<"b">>,timestamp,4002}]},
          {filter,[]},
          {end_inclusive, true}]],
        [W || ?SQL_SELECT{'WHERE' = W} <- SubQueries]
    ).

query_with_arithmetic_in_where_clause_multiple_additions_test() ->
    DDL = get_ddl(
        "CREATE table table1 ("
        "a VARCHAR NOT NULL,"
        "b TIMESTAMP NOT NULL,"
        "PRIMARY KEY ((a, b), a, b));"
    ),
    {ok, Q} = get_query(
          "SELECT * FROM table1 WHERE a = 'hi' AND b = 4000 + 2.4 + 7"),
    {ok, SubQueries} = compile(DDL, Q),
    ?assertEqual(
        [[{startkey,[{<<"a">>,varchar,<<"hi">>},{<<"b">>,timestamp,4009}]},
          {endkey,  [{<<"a">>,varchar,<<"hi">>},{<<"b">>,timestamp,4009}]},
          {filter,[]},
          {end_inclusive, true}]],
        [W || ?SQL_SELECT{'WHERE' = W} <- SubQueries]
    ).

query_with_arithmetic_in_where_clause_operator_precedence_test() ->
    DDL = get_ddl(
        "CREATE table table1 ("
        "a VARCHAR NOT NULL,"
        "b TIMESTAMP NOT NULL,"
        "PRIMARY KEY ((a, b), a, b));"
    ),
    {ok, Q} = get_query(
          "SELECT * FROM table1 WHERE a = 'hi' AND b = 1+3*2/4 - 0.5"),
    {ok, SubQueries} = compile(DDL, Q),
    ?assertEqual(
        [[{startkey,[{<<"a">>,varchar,<<"hi">>},{<<"b">>,timestamp, round(1+3*2/4-0.5)}]},
          {endkey,  [{<<"a">>,varchar,<<"hi">>},{<<"b">>,timestamp, round(1+3*2/4-0.5)}]},
          {filter,[]},
          {end_inclusive, true}]],
        [W || ?SQL_SELECT{'WHERE' = W} <- SubQueries]
    ).

cast_integer_expression_to_double_column_test() ->
    DDL = get_ddl(
        "CREATE table table1 ("
        "a TIMESTAMP NOT NULL,"
        "b DOUBLE NOT NULL,"
        "PRIMARY KEY ((a), a));"
    ),
    {ok, Q} = get_query(
          "SELECT * FROM table1 WHERE a = 10 AND b = 1+5"),
    {ok, SubQueries} = compile(DDL, Q),
    ?assertEqual(
        [[{startkey,[{<<"a">>,timestamp,10}]},
          {endkey,  [{<<"a">>,timestamp,10}]},
          {filter, {'=',{field,<<"b">>,double},{const,6.0}}},
          {end_inclusive, true}]],
        [W || ?SQL_SELECT{'WHERE' = W} <- SubQueries]
    ).

query_with_arithmetic_in_where_clause_operator_precedence_queries_equal_test() ->
    DDL = get_ddl(
        "CREATE table table1 ("
        "a VARCHAR NOT NULL,"
        "b TIMESTAMP NOT NULL,"
        "PRIMARY KEY ((a, b), a, b));"
    ),
    {ok, QA} = get_query(
          "SELECT * FROM table1 WHERE a = 'hi' AND b = 2+3*4"),
    {ok, QB} = get_query(
          "SELECT * FROM table1 WHERE a = 'hi' AND b = 3*4+2"),
    {ok, SubQueriesA} = compile(DDL, QA),
    {ok, SubQueriesB} = compile(DDL, QB),
    ?assertEqual(
        [W || ?SQL_SELECT{'WHERE' = W} <- SubQueriesA],
        [W || ?SQL_SELECT{'WHERE' = W} <- SubQueriesB]
    ).

select_with_arithmetic_on_identifier_throws_an_error_test() ->
    DDL = get_ddl(
        "CREATE table table1 ("
        "a SINT64 NOT NULL,"
        "PRIMARY KEY ((a), a));"
    ),
    {ok, Q} = get_query(
        "SELECT * FROM table1 "
        "WHERE a+1 = 10"),
    ?assertEqual(
        {false,[{arithmetic_on_identifier,<<"a">>}]},
        is_query_valid(DDL, Q)
    ).

select_with_arithmetic_on_identifier_throws_an_error_literal_on_lhs_test() ->
    DDL = get_ddl(
        "CREATE table table1 ("
        "a SINT64 NOT NULL,"
        "PRIMARY KEY ((a), a));"
    ),
    {ok, Q} = get_query(
        "SELECT * FROM table1 "
        "WHERE 10 = a+1"),
    ?assertEqual(
        {false,[{arithmetic_on_identifier,<<"a">>}]},
        is_query_valid(DDL, Q)
    ).



-endif.
