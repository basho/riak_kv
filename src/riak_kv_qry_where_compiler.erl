%% -------------------------------------------------------------------
%%
%% riak_kv_qry_where_compiler: compile the where clause for
%% execution
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
-module(riak_kv_qry_where_compiler).

-export([compile_where_clause/3]).
-export([get_version/0]).

-include("riak_kv_ts.hrl").
-include("riak_kv_index.hrl").
-include("riak_kv_ts_error_msgs.hrl").

-type where_props() :: [{startkey, [term()]} |
                        {endkey, [term()]} |
                        {filter, [term()]} |
                        {start_inclusive, boolean()} |
                        {end_inclusive, boolean()}].

-export_type([where_props/0]).

get_version() -> 2.

%% adding the local key here is a bodge
%% should be a helper fun in the generated DDL module but I couldn't
%% write that up in time
compile_where_clause(#ddl_v1{} = DDL,
                     ?SQL_SELECT{is_executable = false,
                                 'WHERE'       = W,
                                 cover_context = Cover} = Q,
                     MaxSubQueries) ->
    {RealCover, WhereModifications} = unwrap_cover(Cover),
    case compile_where(DDL, W) of
        {error, E} -> {error, E};
        NewW ->
            expand_query(DDL, Q?SQL_SELECT{cover_context = RealCover},
                              update_where_for_cover(NewW, WhereModifications),
                              MaxSubQueries)
    end.

%% now break out the query on quantum boundaries
expand_query(#ddl_v1{local_key = LK, partition_key = PK},
             ?SQL_SELECT{} = Q1, Where1,
             MaxSubQueries) ->
    case expand_where(Where1, PK, MaxSubQueries) of
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

%%
-spec expand_where(riak_ql_ddl:filter(), #key_v1{}, integer()) ->
        [where_props()] | {error, any()}.
expand_where(Where, PartitionKey, MaxSubQueries) ->
    case find_quantum_field_index_in_key(PartitionKey) of
        {QField, QSize, QUnit, QIndex} ->
            hash_timestamp_to_quanta(QField, QSize, QUnit, QIndex, MaxSubQueries, Where);
        notfound ->
            [Where]
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
                                               fn  = quantum,
                                               args = [#param_v1{name = [X]}, Y, Z] }|_], Index) ->
    {X,Y,Z,Index};
find_quantum_field_index_in_key2([_|Tail], Index) ->
    find_quantum_field_index_in_key2(Tail, Index+1).

%%
hash_timestamp_to_quanta(QField, QSize, QUnit, QIndex, MaxSubQueries, Where) ->
    GetMaxMinFun = fun({startkey, List}, {_S, E}) ->
                           {element(3, lists:nth(QIndex, List)), E};
                      ({endkey,   List}, {S, _E}) ->
                           {S, element(3, lists:nth(QIndex, List))};
                      (_, {S, E})  ->
                           {S, E}
                   end,
    {Min, Max} = lists:foldl(GetMaxMinFun, {"", ""}, Where),
    EffMin = case proplists:get_value(start_inclusive, Where, true) of
                 true  -> Min;
                 false -> Min + 1
             end,
    EffMax = case proplists:get_value(end_inclusive, Where, false) of
                 true  -> Max + 1;
                 false -> Max
             end,
    {NoSubQueries, Boundaries} =
        riak_ql_quanta:quanta(EffMin, EffMax, QSize, QUnit),
    if
        NoSubQueries == 1 ->
            [Where];
        NoSubQueries > 1 andalso (MaxSubQueries == undefined orelse
                                  NoSubQueries =< MaxSubQueries) ->
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

check_if_timeseries(#ddl_v1{table = T, partition_key = PK, local_key = LK0} = DDL,
                    [W]) ->
    try
        #key_v1{ast = PartitionKeyAST} = PK,
        PartitionFields = [X || #param_v1{name = X} <- PartitionKeyAST],
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
find_timestamp_bounds(QuantumField, LocalFields) ->
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
break_out_timeseries(Filters1, PartitionFields1, no_quanta) ->
    {Body, Filters2} = split_key_from_filters(PartitionFields1, Filters1),
    {Body, Body, Filters2};
break_out_timeseries(Filters1, PartitionFields1, QuantumField) when is_binary(QuantumField) ->
    case find_timestamp_bounds(QuantumField, Filters1) of
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

%% Functions to assist with coverage chunks that redefine quanta ranges
unwrap_cover(undefined) ->
    {undefined, undefined};
unwrap_cover(Cover) ->
    {ok, {OpaqueContext, {FieldName, RangeTuple}}} =
        riak_kv_pb_coverage:checksum_binary_to_term(Cover),
    {riak_kv_pb_coverage:term_to_checksum_binary(OpaqueContext),
     {FieldName, RangeTuple}}.

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
-include_lib("riak_ql/include/riak_ql_ddl.hrl").

-define(UTIL, riak_kv_qry_compiler_test_util).


%%
%% tests for adding type information and rewriting filters
%%

simple_filter_typing_test() ->
    #ddl_v1{table = T} = ?UTIL:get_long_ddl(),
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
    #ddl_v1{table = T} = ?UTIL:get_standard_ddl(),
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
    #ddl_v1{table = T} = ?UTIL:get_standard_ddl(),
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
    #ddl_v1{table = T} = ?UTIL:get_standard_ddl(),
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
    #ddl_v1{table = T} = ?UTIL:get_standard_ddl(),
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

-endif.
