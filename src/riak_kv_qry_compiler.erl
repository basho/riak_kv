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
         compile/2,
         run_select/2,
         run_select/3  %% what is this for?
        ]).

-type compiled_select() :: fun((_,_) -> riak_pb_ts_codec:ldbvalue()).
-export_type([compiled_select/0]).

-include_lib("riak_ql/include/riak_ql_ddl.hrl").
-include("riak_kv_index.hrl").
-include("riak_kv_ts_error_msgs.hrl").

-spec compile(#ddl_v1{}, ?SQL_SELECT{}) ->
                     {ok, InitialState::[any()], SubQueries::[?SQL_SELECT{}]} | {error, any()}.
compile(#ddl_v1{}, ?SQL_SELECT{is_executable = true}) ->
    {error, 'query is already compiled'};
compile(#ddl_v1{}, ?SQL_SELECT{'SELECT' = #riak_sel_clause_v1{ clause = [] } }) ->
    {error, 'full table scan not implmented'};
compile(#ddl_v1{} = DDL, ?SQL_SELECT{is_executable = false,
                                     type          = sql} = Q) ->
    {ok, S} = compile_select_clause(DDL, Q),
    compile_where_clause(DDL, Q?SQL_SELECT{'SELECT' = S}).

%% adding the local key here is a bodge
%% should be a helper fun in the generated DDL module but I couldn't
%% write that up in time
compile_where_clause(#ddl_v1{} = DDL, ?SQL_SELECT{is_executable = false,
                                                  'WHERE'       = W} = Q) ->
    case compile_where(DDL, W) of
        {error, E} -> {error, E};
        NewW       -> expand_query(DDL, Q, NewW)
    end.

%% now break out the query on quantum boundaries
expand_query(#ddl_v1{local_key = LK, partition_key = PK},
             ?SQL_SELECT{} = Q1, Where1) ->
    case expand_where(Where1, PK) of
        {error, E} ->
            {error, E};
        Where2 ->
            Q2 = Q1?SQL_SELECT{is_executable = true,
                               type          = timeseries,
                               local_key     = LK,
                               partition_key = PK},
            SubQueries = [Q2?SQL_SELECT{ 'WHERE' = X } || X <- Where2],
            {ok, SubQueries}
    end.

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
    lists:flatten(lists:reverse(Acc));
run_select2([Fn | SelectTail], Row, [ColState1 | ColStateTail], Acc1) ->
    ColState2 = Fn(Row, ColState1),
    Acc2 = [ColState2 | Acc1],
    run_select2(SelectTail, Row, ColStateTail, Acc2);
run_select2([Fn | SelectTail], Row, RowState, Acc1) ->
    Value = Fn(Row, RowState),
    Acc2 = [Value | Acc1],
    run_select2(SelectTail, Row, RowState, Acc2).

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
    {ColTypeSet, Q2} = lists:foldr(CompileColFn, Acc, Sel),
    case sets:is_element(aggregate, ColTypeSet) of
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
    {ok, Q3}.

%%
-record(single_sel_column, {
          calc_type        :: select_result_type(),
          initial_state    :: any(),
          col_return_types :: [riak_pb_ts_codec:ldbvalue()],
          col_name         :: riak_pb_ts_codec:tscolumnname(),
          clause           :: function(),
          is_valid         :: true | {error, [any()]},
          finaliser        :: [function()]
         }).

%%
-spec select_column_clause_folder(#ddl_v1{}, selection(), {set(), #riak_sel_clause_v1{}}) ->
                                         {set(), #riak_sel_clause_v1{}}.
select_column_clause_folder(DDL, ColX, {TypeSet1, SelClause1}) ->
    #riak_sel_clause_v1{
       initial_state = InitX,
       col_return_types = ColRetX,
       clause = RunFnX,
       is_valid = IsValid1,
       finalisers = Finalisers1 } = SelClause1,
    S = compile_select_col(DDL, ColX),
    TypeSet2 = sets:add_element(S#single_sel_column.calc_type, TypeSet1),
    Init2   = [S#single_sel_column.initial_state | InitX],
    ColRet2 = [S#single_sel_column.col_return_types | ColRetX],
    RunFn2  = [S#single_sel_column.clause | RunFnX],
    Finalisers2 =[S#single_sel_column.finaliser | Finalisers1],
    IsValid2 = merge_validation(S#single_sel_column.is_valid, IsValid1),
    %% ColTypes are messy because <<"*">> represents many
    %% so you need to flatten the list
    SelClause2 = #riak_sel_clause_v1{
                    initial_state    = Init2,
                    col_return_types = lists:flatten(ColRet2),
                    clause           = RunFn2,
                    is_valid         = IsValid2,
                    finalisers       = lists:flatten(Finalisers2)},
    {TypeSet2, SelClause2}.

%%
-spec get_col_names(#ddl_v1{}, ?SQL_SELECT{}) -> [binary()].
get_col_names(DDL, Q) ->
    ColNames = riak_ql_to_string:col_names_from_select(Q),
    %% flatten because * gets expanded to multiple columns
    lists:flatten(
      [get_col_names2(DDL, N) || N <- ColNames]
     ).

%%
get_col_names2(DDL, "*") ->
    [X#riak_field_v1.name || X <- DDL#ddl_v1.fields];
get_col_names2(_, Name) ->
    list_to_binary(Name).

%% Compile a single selection column into a fun that can extract the cell
%% from the row.
-spec compile_select_col(DDL::#ddl_v1{}, ColumnSpec::any()) ->
                                #single_sel_column{}.
compile_select_col(DDL, {{window_agg_fn, FnName}, [FnArg1]}) ->
    case riak_ql_window_agg_fns:start_state(FnName) of
        stateless ->
            {ColTypes1, IsValid1, Fn} = compile_select_col_stateless(DDL, FnArg1),
            #single_sel_column{ calc_type        = rows,
                                initial_state    = undefined,
                                col_return_types = ColTypes1,
                                clause           = Fn,
                                is_valid         = IsValid1};
        Initial_state ->
            {ColTypes2, IsValid2, Compiled_arg1} =
                compile_select_col_stateless(DDL, FnArg1),
            %% all the windows agg fns so far are arity of 1
            %% which we have forced in this clause by matching on a single argument in the
            %% function head
            FnSig = riak_ql_window_agg_fns:get_arity_and_type_sig(FnName),
            {IsValid, ColRet} = check_types(FnName, FnSig, ColTypes2),

            IsValid3 = merge_validation(IsValid2, IsValid),
            SelectFn =
                fun(Row, State) ->
                        riak_ql_window_agg_fns:FnName(Compiled_arg1(Row), State)
                end,
            FinaliserFn =
                fun(State) ->
                        riak_ql_window_agg_fns:finalise(FnName, State)
                end,
            #single_sel_column{ calc_type        = aggregate,
                                initial_state    = Initial_state,
                                col_return_types = ColRet,
                                clause           = SelectFn,
                                is_valid         = IsValid3,
                                finaliser        = [FinaliserFn] }
    end;
compile_select_col(DDL, Select) ->
    {ColTypes, IsValid, Fn} = compile_select_col_stateless(DDL, Select),
    %% to support aggregates that have a running state all top level funs must be
    %% arity two, when we know the function is stateless wrap it in a two arity
    %% fun and ignore the state arg
    Finalisers = lists:duplicate(length(ColTypes), fun(State) -> State end),
    #single_sel_column{ calc_type        = rows,
                        initial_state    = undefined,
                        col_return_types = ColTypes,
                        clause           = fun(Row, _) -> Fn(Row) end,
                        is_valid         = IsValid,
                        finaliser        = Finalisers }.

%% Returns a one arity fun which is stateless for example pulling a field from a
%% row.
-spec compile_select_col_stateless(#ddl_v1{}, selection()|{Op::atom(), selection(), selection()}) ->
       {ColTypes::[simple_field_type()], IsValid::true|any(), function()}.
compile_select_col_stateless(DDL, {identifier, [<<"*">>]}) ->
    ColTypes = [X#riak_field_v1.type || X <- DDL#ddl_v1.fields],
    {ColTypes, true, fun(Row) -> Row end};
compile_select_col_stateless(DDL, {negate, ExprToNegate}) ->
    {TypeToNegate, ValidityToNegate, ValueToNegate} =
        compile_select_col_stateless(DDL, ExprToNegate),
    NegatingFun = fun(Row) -> -ValueToNegate(Row) end,
    case ValidityToNegate of
        true ->
            {[TypeToNegate],
             ValidityToNegate,
             NegatingFun};
        _ -> {[], ValidityToNegate, []}
    end;
compile_select_col_stateless(_, {Type, V})
  when Type == varchar; Type == boolean ->
    {[Type], true, fun(_) -> V end};
%% TODO why is this integer not sint64?
compile_select_col_stateless(_, {Type, V}) when Type == integer ->
    {[sint64], true, fun(_) -> V end};
%% TODO ditto float and double
compile_select_col_stateless(_, {Type, V}) when Type == float ->
    {[double], true, fun(_) -> V end};
compile_select_col_stateless(#ddl_v1{ fields = Fields }, {identifier, ColumnName}) ->
    {Index, Type} = col_index_and_type_of(Fields, to_column_name_binary(ColumnName)),
    {[Type], true, fun(Row) ->
                           lists:nth(Index, Row)
                   end};
compile_select_col_stateless(DDL, {Op, A, B}) ->
    {Ta, IsValida, Arg_a} = compile_select_col_stateless(DDL, A),
    {Tb, IsValidb, Arg_b} = compile_select_col_stateless(DDL, B),
    IsValid2 = merge_validation(IsValida, IsValidb),
    case IsValid2 of
        true ->
            [TypeA] = Ta,
            [TypeB] = Tb,
            {IsValid, NewType} = resolve_operator_type(Op, TypeA, TypeB),
            {[NewType], IsValid, compile_select_col_stateless2(Op, Arg_a, Arg_b)};
        _ ->
            {[], IsValid2, []}
    end.

%% TODO rewrite so that all the operators are just functions
%% then we can eliminate the double code for creating return types
%% and type checking and stuff 'cos this is messy
resolve_operator_type(Op, Type, Type) when Op =:= '+' orelse
                                           Op =:= '-' orelse
                                           Op =:= '*',
                                           Type =:= double orelse
                                           Type =:= sint64 -> {true, Type};
resolve_operator_type(Op, Type1, Type2) when Op =:= '+' orelse
                                             Op =:= '-' orelse
                                             Op =:= '*',
                                             Type1 =:= double andalso
                                             Type2 =:= sint64;
                                             Type1 =:= sint64 andalso
                                             Type2 =:= double -> {true, double};
resolve_operator_type('/', Type1, Type2) when Type1 =:= double andalso
                                              Type2 =:= double -> {true, double};
resolve_operator_type('/', Type1, Type2) when Type1 =:= sint64 andalso
                                              Type2 =:= sint64 -> {true, sint64};
resolve_operator_type('/', Type1, Type2) when Type1 =:= sint64 orelse
                                              Type1 =:= double;
                                              Type2 =:= sint64 orelse
                                              Type2 =:= double -> {true, double};
resolve_operator_type(Op, Type1, Type2) -> {{error, {invalid_type, Op, Type1, Type2}}, []}.

merge_validation(true,        IsValid)     -> IsValid;
merge_validation(IsValid,     true)        -> IsValid;
merge_validation({error, E1}, {error, E2}) -> {error, E1 ++ E2}.

%%
compile_select_col_stateless2('+', A, B) ->
    fun(Row) -> A(Row) + B(Row) end;
compile_select_col_stateless2('*', A, B) ->
    fun(Row) -> A(Row) * B(Row) end;
compile_select_col_stateless2('/', A, B) ->
    fun(Row) -> A(Row) / B(Row) end;
compile_select_col_stateless2('-', A, B) ->
    fun(Row) -> A(Row) - B(Row) end.

check_types(_FnName, {_, RetType}, _Type)
  %% functions such as COUNT taking an argument of any type have
  %% a special signature
  when not is_list(RetType) ->
    {true, [RetType]};
check_types(FnName, {_Arity, TypeSig}, [Type]) ->
    case lists:keyfind(Type, 1, TypeSig) of
        {Type, Ret} -> {true, [Ret]};
        false       -> {{error, [{fn_called_with_invalid_type, FnName, Type}]}, []}
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

expand_where(Where, #key_v1{ast = PAST}) ->
    GetMaxMinFun = fun({startkey, List}, {_S, E}) ->
                           {element(3, lists:last(List)), E};
                      ({endkey,   List}, {S, _E}) ->
                           {S, element(3, lists:last(List))};
                      (_, {S, E})  ->
                           {S, E}
                   end,
    {Min, Max} = lists:foldl(GetMaxMinFun, {"", ""}, Where),
    [{[QField], Q, U}] = [{X, Y, Z}
                          || #hash_fn_v1{mod = riak_ql_quanta,
                                         fn   = quantum,
                                         args = [#param_v1{name = X}, Y, Z]}
                                 <- PAST],
    EffMin = case proplists:get_value(start_inclusive, Where, true) of
                 true ->
                     Min;
                 _ ->
                     Min + 1
             end,
    EffMax = case proplists:get_value(end_inclusive, Where, false) of
                 true ->
                     Max + 1;
                 _ ->
                     Max
             end,
    {NoSubQueries, Boundaries} = riak_ql_quanta:quanta(EffMin, EffMax, Q, U),
    MaxSubQueries =
        app_helper:get_env(riak_kv, timeseries_query_max_quanta_span),
    if
        NoSubQueries == 1 ->
            [Where];
        NoSubQueries > 1 andalso NoSubQueries =< MaxSubQueries ->
            make_wheres(Where, QField, Min, Max, Boundaries);
        NoSubQueries > MaxSubQueries ->
            {error, {too_many_subqueries, NoSubQueries}}
    end.

make_wheres(Where, QField, Min, Max, Boundaries) ->
    {HeadOption, TailOption, NewWhere} = extract_options(Where),
    Starts = [Min | Boundaries],
    Ends   = Boundaries ++ [Max],
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

quantum_field_name(#ddl_v1{ partition_key = PK }) ->
    #key_v1{ ast = PartitionKeyAST } = PK,
    [_, _, Quantum] = PartitionKeyAST,
    #hash_fn_v1{args = [#param_v1{name = QFieldName} | _]} = Quantum,
    QFieldName.

check_if_timeseries(#ddl_v1{table = T, partition_key = PK, local_key = LK} = DDL,
                    [W]) ->
    try
        #key_v1{ast = PartitionKeyAST} = PK,
        LocalFields     = [X || #param_v1{name = X} <- LK#key_v1.ast],
        PartitionFields = [X || #param_v1{name = X} <- PartitionKeyAST],
        [QuantumFieldName] = quantum_field_name(DDL),
        StrippedW = strip(W, []),
        {StartW, EndW, Filter} = break_out_timeseries(StrippedW, LocalFields, [QuantumFieldName]),
        Mod = riak_ql_ddl:make_module_name(T),
        StartKey = rewrite(LK, StartW, Mod),
        EndKey = rewrite(LK, EndW, Mod),
        %% defaults on startkey and endkey are different
        IncStart = case includes(StartW, '>', Mod) of
                       true  -> [{start_inclusive, false}];
                       false -> []
                   end,
        IncEnd = case includes(EndW, '<', Mod) of
                     true  -> [];
                     false -> [{end_inclusive, true}]
                 end,
        case has_errors(StartKey, EndKey) of
            [] ->
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
check_if_timeseries(#ddl_v1{ }, []) ->
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
find_timestamp_bounds(QuantumField, LocalFields) when is_binary(QuantumField) ->
    find_timestamp_bounds2(QuantumField, LocalFields, [], {undefined, undefined}).

%%
find_timestamp_bounds2(_, [], OtherFilters, BoundsAcc) ->
    {lists:reverse(OtherFilters), BoundsAcc};
find_timestamp_bounds2(QuantumFieldName, [{or_, {_, QuantumFieldName, _}, _} | _], _, _) ->
    %% if this is an or state ment, lookahead at what is being tested, the quanta
    %% cannot be tested with an OR operator
    error({time_bounds_must_use_and_op, ?E_TIME_BOUNDS_MUST_USE_AND});
find_timestamp_bounds2(QuantumFieldName, [{Op, QuantumFieldName, _} = Filter | Tail], OtherFilters, BoundsAcc1) ->
    %% if there are already end bounds throw an error
    if
        Op == '>' orelse Op == '>=' ->
            find_timestamp_bounds2(QuantumFieldName, Tail, OtherFilters, acc_lower_bounds(Filter, BoundsAcc1));
        Op == '<' orelse Op == '<=' ->
            find_timestamp_bounds2(QuantumFieldName, Tail, OtherFilters, acc_upper_bounds(Filter, BoundsAcc1));
        Op == '=' orelse Op == '!=' ->
            find_timestamp_bounds2(QuantumFieldName, Tail, [Filter | OtherFilters], BoundsAcc1)
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
break_out_timeseries(Filters1, LocalFields1, [QuantumFields]) ->
    case find_timestamp_bounds(QuantumFields, Filters1) of
        {_, {undefined, undefined}} ->
            error({incomplete_where_clause, ?E_TSMSG_NO_BOUNDS_SPECIFIED});
        {_, {_, undefined}} ->
            error({incomplete_where_clause, ?E_TSMSG_NO_UPPER_BOUND});
        {_, {undefined, _}} ->
            error({incomplete_where_clause, ?E_TSMSG_NO_LOWER_BOUND});
        {_, {{_,_,{_,Starts}}, {_,_,{_,Ends}}}} when is_integer(Starts),
                                                     is_integer(Ends),
                                                     Starts > Ends ->
            error({lower_bound_must_be_less_than_upper_bound,
                   ?E_TSMSG_LOWER_BOUND_MUST_BE_LESS_THAN_UPPER_BOUND});
        {_, {{'>',_,{_,Starts}}, {'<',_,{_,Ends}}}} when is_integer(Starts),
                                                         is_integer(Ends),
                                                         Starts == Ends ->
            %% catch when the filter values for time bounds are equal but we're
            %% using greater than or less than so could never match, if >= or <=
            %% were used on either side then
            error({lower_and_upper_bounds_are_equal_when_no_equals_operator,
                   ?E_TSMSG_LOWER_AND_UPPER_BOUNDS_ARE_EQUAL_WHEN_NO_EQUALS_OPERATOR});
        {Filters2, {Starts, Ends}} ->
            %% remove the quanta from the local fields, this has alreadfy been
            %% removed from the fields
            [F1, F2, _] = LocalFields1,
            LocalFields2 = [F1,F2],
            %% create the keys by splitting the key filters and prepending it
            %% with the time bound.
            {Body, Filters3} = split_key_from_filters(LocalFields2, Filters2),
            {[Starts | Body], [Ends | Body], Filters3}
    end.

%% separate the key fields from the other filters
split_key_from_filters(LocalFields, Filters) ->
    lists:mapfoldl(fun split_key_from_filters2/2, Filters, LocalFields).

%%
split_key_from_filters2([FieldName], Filters) when is_binary(FieldName) ->
    take_key_field(FieldName, Filters, []).

%%
take_key_field(FieldName, [], Acc) ->
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

-ifdef(TEST).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

%%
%% Helper Fns for unit tests
%%

make_ddl(Table, Fields) when is_binary(Table) ->
    make_ddl(Table, Fields, #key_v1{}, #key_v1{}).

make_ddl(Table, Fields, PK) when is_binary(Table) ->
    make_ddl(Table, Fields, PK, #key_v1{}).

make_ddl(Table, Fields, #key_v1{} = PK, #key_v1{} = LK)
  when is_binary(Table) ->
    #ddl_v1{table         = Table,
            fields        = Fields,
            partition_key = PK,
            local_key     = LK}.

make_query(Table, Selections) ->
    make_query(Table, Selections, []).

make_query(Table, Selections, Where) ->
    ?SQL_SELECT{'FROM'   = Table,
                'SELECT' = Selections,
                'WHERE'  = Where}.

-define(MIN, 60 * 1000).
-define(NAME, "time").

is_query_valid(#ddl_v1{ table = Table } = DDL, Q) ->
    Mod = riak_ql_ddl:make_module_name(Table),
    riak_ql_ddl:is_query_valid(Mod, DDL, Q).

get_query(String) ->
    Lexed = riak_ql_lexer:get_tokens(String),
    {ok, _Q} = riak_ql_parser:parse(Lexed).

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
    {ok, DDL} = riak_ql_parser:parse(Lexed),
    {module, _Module} = riak_ql_ddl_compiler:compile_and_load_from_tmp(DDL),
    DDL.

get_standard_pk() ->
    #key_v1{ast = [
                   #param_v1{name = [<<"location">>]},
                   #param_v1{name = [<<"user">>]},
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
                   #param_v1{name = [<<"location">>]},
                   #param_v1{name = [<<"user">>]},
                   #param_v1{name = [<<"time">>]}
                  ]}.

%%
%% Unit tests
%%

%%
%% tests for adding type information and rewriting filters
%%

simple_filter_typing_test() ->
    #ddl_v1{table = T} = get_long_ddl(),
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
%% rewrite passing tests
%%
%% success here is because the where clause covers the entire local key
%% we have enough info to build a range scan
%%
simple_rewrite_test() ->
    #ddl_v1{table = T} = get_standard_ddl(),
    Mod = riak_ql_ddl:make_module_name(T),
    LK  = #key_v1{ast = [
                         #param_v1{name = [<<"geohash">>]},
                         #param_v1{name = [<<"time">>]}
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
    #ddl_v1{table = T} = get_standard_ddl(),
    Mod = riak_ql_ddl:make_module_name(T),
    LK  = #key_v1{ast = [
                         #param_v1{name = [<<"geohash">>]},
                         #param_v1{name = [<<"user">>]}
                        ]},
    W   = [
           {'=', <<"geohash">>, {"word", "yardle"}}
          ],
    ?assertEqual(
       {error, {missing_param, ?E_MISSING_PARAM_IN_WHERE_CLAUSE("user")}},
       rewrite(LK, W, Mod)
      ).

simple_rewrite_fail_2_test() ->
    #ddl_v1{table = T} = get_standard_ddl(),
    Mod = riak_ql_ddl:make_module_name(T),
    LK  = #key_v1{ast = [
                         #param_v1{name = [<<"geohash">>]},
                         #param_v1{name = [<<"user">>]}
                        ]},
    W   = [
           {'=', <<"user">>, {"word", "yardle"}}
          ],
    ?assertEqual(
       {error, {missing_param, ?E_MISSING_PARAM_IN_WHERE_CLAUSE("geohash")}},
       rewrite(LK, W, Mod)
      ).

simple_rewrite_fail_3_test() ->
    #ddl_v1{table = T} = get_standard_ddl(),
    Mod = riak_ql_ddl:make_module_name(T),
    LK  = #key_v1{ast = [
                         #param_v1{name = [<<"geohash">>]},
                         #param_v1{name = [<<"user">>]},
                         #param_v1{name = [<<"temperature">>]}
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
    Query = "select weather from GeoCheckin where time > 3000 and time < 5000 and user = 'user_1' and location = 'San Francisco'",
    {ok, Q} = get_query(Query),
    true = is_query_valid(DDL, Q),
    [Where1] =
        test_data_where_clause(<<"San Francisco">>, <<"user_1">>, [{3000, 5000}]),
    Where2 = Where1 ++ [{start_inclusive, false}],
    PK = get_standard_pk(),
    LK = get_standard_lk(),
    ?assertMatch(
       {ok, [?SQL_SELECT{ is_executable = true,
                          type          = timeseries,
                          'WHERE'       = Where2,
                          partition_key = PK,
                          local_key     = LK }]},
       compile(DDL, Q)
      ).

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
        test_data_where_clause(<<"Scotland">>, <<"user_1">>, [{3000, 5000}]),
    Where = [
             StartKey,
             EndKey,
             {filter, {'=', {field, <<"weather">>, varchar}, {const, <<"yankee">>}}},
             {start_inclusive, false}
            ],
    PK = get_standard_pk(),
    LK = get_standard_lk(),
    ?assertMatch(
       {ok, [?SQL_SELECT{ is_executable = true,
                          type          = timeseries,
                          'WHERE'       = Where,
                          partition_key = PK,
                          local_key     = LK }]},
       compile(DDL, Q)
      ).

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
    Where = [
             StartKey,
             EndKey,
             {filter,   {'=', {field, <<"weather">>, varchar}, {const, <<"yankee">>}}}
            ],
    PK = get_standard_pk(),
    LK = get_standard_lk(),
    ?assertMatch(
       {ok, [?SQL_SELECT{ is_executable = true,
                          type          = timeseries,
                          'WHERE'       = Where,
                          partition_key = PK,
                          local_key     = LK }]},
       compile(DDL, Q)
      ).

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
        test_data_where_clause(<<"Scotland">>, <<"user_1">>, [{3000, 5000}]),
    PK = get_standard_pk(),
    LK = get_standard_lk(),
    Where = [
             StartKey,
             EndKey,
             {filter, {'=', {field, <<"weather">>, varchar}, {const, <<"yankee">>}}},
             {start_inclusive, false},
             {end_inclusive,   true}
            ],
    ?assertMatch(
       {ok, [?SQL_SELECT{ is_executable = true,
                          type          = timeseries,
                          'WHERE'       = Where,
                          partition_key = PK,
                          local_key     = LK }]},
       compile(DDL, Q)
      ).

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
        test_data_where_clause(<<"Scotland">>, <<"user_1">>, [{3000, 5000}]),
    PK = get_standard_pk(),
    LK = get_standard_lk(),
    Where = [
             StartKey,
             EndKey,
             {filter,
              {and_,
               {'=', {field, <<"weather">>, varchar}, {const, <<"yankee">>}},
               {'=', {field, <<"temperature">>, varchar}, {const, <<"yelp">>}}
              }
             },
             {start_inclusive, false}
            ],
    ?assertMatch(
       {ok, [?SQL_SELECT{ is_executable = true,
                          type          = timeseries,
                          'WHERE'       = Where,
                          partition_key = PK,
                          local_key     = LK }]},
       compile(DDL, Q)
      ).

complex_with_4_field_filter_test() ->
    Query = "select weather from GeoCheckin where time > 3000 and time < 5000 and user = 'user_1' and location = 'Scotland' and extra = 1 and (weather = 'yankee' or (temperature = 'yelp' and geohash = 'erko'))",
    {ok, Q} = get_query(Query),
    DDL = get_long_ddl(),
    true = is_query_valid(DDL, Q),
    [[Start, End | _]] =
        test_data_where_clause(<<"Scotland">>, <<"user_1">>, [{3000, 5000}]),
    Where2 = [
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
              },
              {start_inclusive, false}
             ],
    PK = get_standard_pk(),
    LK = get_standard_lk(),
    ?assertMatch(
       {ok, [?SQL_SELECT{ is_executable = true,
                          type          = timeseries,
                          'WHERE'       = Where2,
                          partition_key = PK,
                          local_key     = LK }]},
       compile(DDL, Q)
      ).

complex_with_boolean_rewrite_filter_test() ->
    DDL = get_long_ddl(),
    {ok, Q} = get_query(
                "SELECT weather FROM GeoCheckin "
                "WHERE time > 3000 AND time < 5000 "
                "AND user = 'user_1' AND location = 'Scotland' "
                "AND (myboolean = False OR myboolean = tRue)"),
    true = is_query_valid(DDL, Q),
    [[StartKey, EndKey |_]] =
        test_data_where_clause(<<"Scotland">>, <<"user_1">>, [{3000, 5000}]),
    PK = get_standard_pk(),
    LK = get_standard_lk(),
    Where = [
             StartKey,
             EndKey,
             {filter,
              {or_,
               {'=', {field, <<"myboolean">>, boolean}, {const, false}},
               {'=', {field, <<"myboolean">>, boolean}, {const, true}}
              }
             },
             {start_inclusive, false}
            ],
    ?assertMatch(
       {ok, [?SQL_SELECT{ is_executable  = true,
                          type          = timeseries,
                          'WHERE'       = Where,
                          partition_key = PK,
                          local_key     = LK
                        }]},
       compile(DDL, Q)
      ).

%% got for 3 queries to get partition ordering problems flushed out
simple_spanning_boundary_test() ->
    DDL = get_standard_ddl(),
    {ok, Q} = get_query(
                "select weather from GeoCheckin where time >= 3000 and time < 31000 and user = 'user_1' and location = 'Scotland'"),
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
    Query = "select weather from GeoCheckin where time >= 14000 and time <= 15000 and user = 'user_1' and location = 'Scotland'",
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
    Query = "select weather from GeoCheckin where time >= 3000 and time < 30000 and user = 'user_1' and location = 'Scotland'",
    {ok, Q} = get_query(Query),
    true = is_query_valid(DDL, Q),
    %% now make the result - expecting 2 queries
    [Where1, Where2] =
        test_data_where_clause(<<"Scotland">>, <<"user_1">>, [{3000, 15000}, {15000, 30000}]),
    PK = get_standard_pk(),
    LK = get_standard_lk(),
    ?assertMatch(
       {ok, [?SQL_SELECT{ 'WHERE'       = Where1,
                          partition_key = PK,
                          local_key     = LK},
             ?SQL_SELECT{ 'WHERE'       = Where2,
                          partition_key = PK,
                          local_key     = LK
                        }]},
       compile(DDL, Q)
      ).

%%
%% test failures
%%

simplest_compile_once_only_fail_test() ->
    DDL = get_standard_ddl(),
    Query = "select weather from GeoCheckin where time >= 3000 and time < 5000 and user = 'user_1' and location = 'Scotland'",
    {ok, Q} = get_query(Query),
    true = is_query_valid(DDL, Q),
    %% now try and compile twice
    {ok, [Q2]} = compile(DDL, Q),
    Got = compile(DDL, Q2),
    ?assertEqual(
       {error, 'query is already compiled'},
       Got
      ).

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
    ?assertMatch(
       {ok, [?SQL_SELECT{
                'WHERE' = [
                           {startkey, [
                                       {<<"time_a">>, timestamp, 10},
                                       {<<"time_b">>, timestamp, 15},
                                       {<<"time_c">>, timestamp, 2999}
                                      ]},
                           {endkey, [
                                     {<<"time_a">>, timestamp, 10},
                                     {<<"time_b">>, timestamp, 15},
                                     {<<"time_c">>, timestamp, 5000}
                                    ]},
                           {filter, []},
                           {start_inclusive, false}]
               }]},
       compile(DDL, Q)
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
       {error, {lower_bound_must_be_less_than_upper_bound, ?E_TSMSG_LOWER_BOUND_MUST_BE_LESS_THAN_UPPER_BOUND}},
       compile(DDL, Q)
      ).

lower_bound_is_same_as_upper_bound_test() ->
    DDL = get_standard_ddl(),
    {ok, Q} = get_query(
                "SELECT weather FROM GeoCheckin "
                "WHERE time > 5000 AND time < 5000"
                "AND user = 'user_1' AND location = 'derby'"),
    ?assertEqual(
       {error, {lower_and_upper_bounds_are_equal_when_no_equals_operator, ?E_TSMSG_LOWER_AND_UPPER_BOUNDS_ARE_EQUAL_WHEN_NO_EQUALS_OPERATOR}},
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
    {ok, Q} = get_query("select * from test1 where time > 1 and time < 6 and user = '2' and location != '4'"),
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
    Sel = testing_compile_row_select(DDL,
                                     "SELECT geohash FROM GeoCheckin "
                                     "WHERE time > 1 AND time < 6 AND user = '2' AND location = '4'"),
    #riak_sel_clause_v1{clause = SelectSpec} = Sel,
    ?assertEqual(
       [<<"geodude">>],
       run_select(SelectSpec, ?ROW)
      ).

run_select_last_test() ->
    DDL = get_standard_ddl(),
    Sel = testing_compile_row_select(DDL,
                                     "SELECT temperature FROM GeoCheckin "
                                     "WHERE time > 1 AND time < 6 AND user = '2' AND location = '4'"),
    #riak_sel_clause_v1{clause = SelectSpec} = Sel,
    ?assertEqual(
       [12.2],
       run_select(SelectSpec, ?ROW)
      ).

run_select_all_individually_test() ->
    DDL = get_standard_ddl(),
    Sel = testing_compile_row_select(DDL,
                                     "SELECT geohash, location, user, time, weather, temperature FROM GeoCheckin "
                                     "WHERE time > 1 AND time < 6 AND user = '2' AND location = '4'"),
    #riak_sel_clause_v1{clause = SelectSpec} = Sel,
    ?assertEqual(
       ?ROW,
       run_select(SelectSpec, ?ROW)
      ).

run_select_some_test() ->
    DDL = get_standard_ddl(),
    Sel = testing_compile_row_select(DDL,
                                     "SELECT  location, weather FROM GeoCheckin "
                                     "WHERE time > 1 AND time < 6 AND user = '2' AND location = '4'"),
    #riak_sel_clause_v1{clause = SelectSpec} = Sel,
    ?assertEqual(
       [<<"derby">>, <<"hot">>],
       run_select(SelectSpec, ?ROW)
      ).

select_count_aggregation_test() ->
    DDL = get_standard_ddl(),
    Sel = testing_compile_row_select(DDL,
                                     "SELECT count(location) FROM GeoCheckin "
                                     "WHERE time > 1 AND time < 6 AND user = '2' AND location = '4'"),
    #riak_sel_clause_v1{clause = SelectSpec} = Sel,
    ?assertEqual(
       [1],
       run_select(SelectSpec, ?ROW, [0])
      ).


select_count_aggregation_2_test() ->
    DDL = get_standard_ddl(),
    Sel = testing_compile_row_select(DDL,
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
    SQL = "SELECT location from mytab WHERE myfamily = 'familyX' and myseries = 'seriesX' and time > 1 and time < 2",
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
    SQL = "SELECT count(location), avg(mydouble), avg(mysint) from mytab WHERE myfamily = 'familyX' and myseries = 'seriesX' and time > 1 and time < 2",
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
    SQL = "SELECT 1 + 2 - 3 /4 * 5 from mytab WHERE myfamily = 'familyX' and myseries = 'seriesX' and time > 1 and time < 2",
    {ok, Rec} = get_query(SQL),
    {ok, Sel} = compile_select_clause(get_sel_ddl(), Rec),
    ?assertMatch(
       #riak_sel_clause_v1{
          calc_type        = rows,
          col_return_types = [sint64],
          col_names        = [<<"((1+2)-((3/4)*5))">>] },
       Sel
      ).

basic_select_arith_2_test() ->
    SQL = "SELECT 1 + 2.0 - 3 /4 * 5 from mytab WHERE myfamily = 'familyX' and myseries = 'seriesX' and time > 1 and time < 2",
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
       #riak_sel_clause_v1{ initial_state = [0] },
       Select
      ).

function_2_initial_state_test() ->
    {ok, Rec} = get_query(
                  "SELECT SUM(mydouble), SUM(mydouble) FROM mytab WHERE myfamily = 'familyX' "
                  "AND myseries = 'seriesX' AND time > 1 AND time < 2"),
    {ok, Select} = compile_select_clause(get_sel_ddl(), Rec),
    ?assertMatch(
       #riak_sel_clause_v1{ initial_state = [0, 0] },
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

%% basic_select_window_agg_fn_arith_1_test() ->
%%     {ok, Rec} = get_query(
%%         "SELECT count(location) + 1 from mytab "
%%         "WHERE myfamily = 'familyX' "
%%         "AND myseries = 'seriesX' AND time > 1 AND time < 2"
%%     ),
%%     {ok, Selection} = compile_select_clause(get_sel_ddl(), Rec),
%%     ?assertMatch(
%%         #riak_sel_clause_v1{
%%             calc_type = aggregate,
%%             col_return_types = [sint64],
%%             col_names = [<<"COUNT(location)+1">>] },
%%         Selection
%%     ).

%% FIXME
%% basic_select_window_agg_fn_arith_2_test() ->
%%     DDL = get_sel_ddl(),
%%     SQL = "SELECT count(location + 1.0) from mytab WHERE myfamily = 'familyX' and myseries = 'seriesX' and time > 1 and time < 2",
%%     {ok, Rec} = get_query(SQL),
%%     {ok, Sel} = compile_select_clause(DDL, Rec),
%%     ?assertMatch(#riak_sel_clause_v1{calc_type        = aggregate,
%%                                      col_return_types = [
%%                                                          double
%%                                                         ],
%%                                      col_names        = [
%%                                                          <<"COUNT(location+1)">>
%%                                                         ]
%%                                     },
%%                  Sel).

-endif.
