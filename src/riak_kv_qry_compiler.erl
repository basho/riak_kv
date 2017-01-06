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
-compile(export_all).
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
    compile_group_by(Mod, GroupBy, [], Q1?SQL_SELECT{ 'SELECT' = Sel3 }).

%%
compile_group_by(_, [], Acc, Q) ->
    {ok, Q?SQL_SELECT{ group_by = lists:reverse(Acc) }};
compile_group_by(Mod, [{identifier,FieldName}|Tail], Acc, Q)
        when is_binary(FieldName) ->
    Pos = Mod:get_field_position([FieldName]),
    compile_group_by(Mod, Tail, [{Pos,FieldName}|Acc], Q).

%% adding the local key here is a bodge
%% should be a helper fun in the generated DDL module but I couldn't
%% write that up in time
-spec compile_where_clause(?DDL{}, ?SQL_SELECT{}) ->
                                  {ok, [?SQL_SELECT{}]} | {error, term()}.
compile_where_clause(?DDL{} = DDL,
                     ?SQL_SELECT{is_executable = false,
                                 'WHERE'       = W,
                                 cover_context = Cover} = Q) ->
    case {compile_where(DDL, W), unwrap_cover(Cover)} of
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
    {ResultTypeSet, Sel1} = lists:foldl(CompileColFn, Acc, Sel),

    {ColTypes, Errors} = my_mapfoldl(
        fun(ColASTX, Errors) ->
            infer_col_type(DDL, ColASTX, Errors)
        end, [], Sel),

    IsGroupBy = (Q?SQL_SELECT.group_by /= []),
    IsAggregate = sets:is_element(aggregate, ResultTypeSet),
    if
        IsGroupBy ->
            Sel2 = Sel1#riak_sel_clause_v1{
                   calc_type = group_by,
                   col_names = get_col_names(DDL, Q) };
        IsAggregate ->
            Sel2 = Sel1#riak_sel_clause_v1{
                   calc_type = aggregate,
                   col_names = get_col_names(DDL, Q) };
        not IsAggregate ->
            Sel2 = Sel1#riak_sel_clause_v1{
                   calc_type = rows,
                   initial_state = [],
                   col_names = get_col_names(DDL, Q) }
    end,
    case Errors of
      [] ->
          {ok, Sel2#riak_sel_clause_v1{
              col_names = get_col_names(DDL, Q),
              col_return_types = lists:flatten(ColTypes) }};
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
-spec infer_col_type(?DDL{}, riak_ql_ddl:selection(), Errors1::[any()]) ->
        {Type::riak_ql_ddl:external_field_type() | error, Errors2::[any()]}.
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
        {riak_ql_ddl:selection() | {finalise_aggregation, FnName::atom(), integer()},
         [riak_ql_ddl:selection_function()]}.
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
    case check_if_timeseries(DDL, Where) of
        {error, E}   -> {error, E};
        {true, NewW} -> NewW
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
                {true, lists:flatten([
                                      {startkey, StartKey},
                                      {endkey,   EndKey},
                                      {filter,   RewrittenFilter}
                                     ] ++ IncStart ++ IncEnd
                                    )};
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

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

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
    ?assertEqual(
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
    ?assertEqual(
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
        [{<<"c">>,double,3.5}, {<<"a">>,sint64,1000},{<<"b">>,varchar,<<"bval">>}],
    ?assertEqual(
        [{startkey, Key},
         {endkey, Key},
         {filter,{'=',{field,<<"d">>,boolean},{const, true}}},
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

-endif.
