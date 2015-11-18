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
         compile/2
        ]).

-include_lib("riak_ql/include/riak_ql_ddl.hrl").
-include("riak_kv_index.hrl").
-include("riak_kv_ts_error_msgs.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(MAXSUBQ, 5).

-spec compile(#ddl_v1{}, #riak_sql_v1{}) ->
    [#riak_sql_v1{}] | {error, any()}.
compile(#ddl_v1{}, #riak_sql_v1{is_executable = true}) ->
    {error, 'query is already compiled'};
compile(#ddl_v1{}, #riak_sql_v1{'SELECT' = []}) ->
    {error, 'full table scan not implmented'};
compile(#ddl_v1{} = DDL, #riak_sql_v1{is_executable = false,
                                      type          = sql} = Q) ->
    comp2(DDL, Q).

%% adding the local key here is a bodge
%% should be a helper fun in the generated DDL module but I couldn't
%% write that up in time
comp2(#ddl_v1{} = DDL, #riak_sql_v1{is_executable = false,
                    'WHERE'       = W} = Q) ->
    case compile_where(DDL, W) of
        {error, E} -> {error, E};
        NewW       -> expand_query(DDL, Q, NewW)
    end.

%% now break out the query on quantum boundaries
expand_query(#ddl_v1{local_key = LK, partition_key = PK}, Q, NewW) ->
    case expand_where(NewW, PK) of
    {error, E} ->
        {error, E};
    NewWs ->
        [Q#riak_sql_v1{is_executable = true,
               type          = timeseries,
               'WHERE'       = X,
               local_key     = LK,
               partition_key = PK} || X <- NewWs]
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
    {NoSubQueries, Boundaries} = riak_ql_quanta:quanta(Min, Max, Q, U),
    if
        NoSubQueries =:= 1 ->
            [Where];
        1 < NoSubQueries andalso NoSubQueries =< ?MAXSUBQ ->
            _NewWs = make_wheres(Where, QField, Min, Max, Boundaries);
        ?MAXSUBQ < NoSubQueries ->
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
              [{and_, _LHS, _RHS} = W]) ->
    try    #key_v1{ast = PartitionKeyAST} = PK,
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
        error:{incomplete_where_clause, _} = E -> {error, E};
        error:{lower_bound_specified_more_than_once, _} = E -> {error, E};
        error:{upper_bound_specified_more_than_once, _} = E -> {error, E};
        error:Reason ->
            % if it is not a known error then return the stack trace for
            % debugging
            {error, {where_not_timeseries, Reason, erlang:get_stacktrace()}}
    end;
check_if_timeseries(_DLL, Where) ->
    % TODO return the SQL string
    {error, {where_not_supported, Where}}.

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

% find the upper and lower bound for the time
find_timestamp_bounds(QuantumField, LocalFields) when is_binary(QuantumField) ->
    find_timestamp_bounds2(QuantumField, LocalFields, [], {undefined, undefined}).

find_timestamp_bounds2(_, [], OtherFilters, BoundsAcc) ->
    {lists:reverse(OtherFilters), BoundsAcc};
find_timestamp_bounds2(QuantumFieldName, [{Op, QuantumFieldName, _} = Filter | Tail], OtherFilters, BoundsAcc1) ->
    % if there are already end bounds throw an error
    if
        Op == '>' orelse Op == '>=' ->
            find_timestamp_bounds2(QuantumFieldName, Tail, OtherFilters, acc_lower_bounds(Filter, BoundsAcc1));
        Op == '<' orelse Op == '<=' ->
            find_timestamp_bounds2(QuantumFieldName, Tail, OtherFilters, acc_upper_bounds(Filter, BoundsAcc1));
        Op == '=' orelse Op == '!=' ->
            find_timestamp_bounds2(QuantumFieldName, Tail, [Filter | OtherFilters], BoundsAcc1)
    end;
find_timestamp_bounds2(QuantumFieldName, [Filter | Tail], OtherFilters, BoundsAcc1) ->
    % this filter is not on the quantum
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
break_out_timeseries(Filters1, LocalFields, [QuantumFields]) ->
    case find_timestamp_bounds(QuantumFields, Filters1) of
        {_, {undefined, undefined}} ->
            error({incomplete_where_clause, ?E_TSMSG_NO_BOUNDS_SPECIFIED});
        {_, {_, undefined}} ->
            error({incomplete_where_clause, ?E_TSMSG_NO_UPPER_BOUND});
        {_, {undefined, _}} ->
            error({incomplete_where_clause, ?E_TSMSG_NO_LOWER_BOUND});
        {Filters2, {Starts, Ends}} ->
            {Filter, Body} = get_fields(LocalFields, Filters2, []),
            {[Starts | Body], [Ends | Body], Filter}
    end.

get_fields([], Ands, Acc) ->
    {Ands, lists:sort(Acc)};
get_fields([[H] | T], Ands, Acc) ->
    {NewAnds, Vals} = take(H, Ands, []),
    get_fields(T, NewAnds, Vals ++ Acc).

take(Key, Ands, Acc) ->
    case lists:keytake(Key, 2, Ands) of
        {value, Val, NewAnds} -> take(Key, NewAnds, [Val | Acc]);
        false                 -> {Ands, lists:sort(Acc)}
    end.

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
normalise(Val, boolean) ->
    case string:to_lower(binary_to_list(Val)) of
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

rewrite(#key_v1{ast = AST}, W, Mod) ->
    rew2(AST, W, Mod, []).


rew2([], [], _Mod, Acc) ->
   lists:reverse(Acc);
%% the rewrite should have consumed all the passed in values
rew2([], _W, _Mod, _Acc) ->
    {error, {invalid_rewrite, _W}};
rew2([#param_v1{name = [N]} | T], W, Mod, Acc) ->
    Type = Mod:get_field_type([N]),
    case lists:keytake(N, 2, W) of
        false                           ->
            {error, {missing_param, ?E_MISSING_PARAM_IN_WHERE_CLAUSE(N)}};
        {value, {_, _, {_, Val}}, NewW} ->
            rew2(T, NewW, Mod, [{N, Type, Val} | Acc])
    end.

-ifdef(TEST).
-compile(export_all).

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
    #riak_sql_v1{'FROM'   = Table,
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
    SQL = "CREATE TABLE GeoCheckin " ++
        "(geohash varchar not null, " ++
        "location varchar not null, " ++
        "user varchar not null, " ++
        "time timestamp not null, " ++
        "weather varchar not null, " ++
        "temperature varchar, " ++
        "PRIMARY KEY((location, user, quantum(time, 15, 's')), " ++
        "location, user, time))",
    get_ddl(SQL).

get_ddl(SQL) ->
    Lexed = riak_ql_lexer:get_tokens(SQL),
    {ok, DDL} = riak_ql_parser:parse(Lexed),
    {module, _Module} = riak_ql_ddl_compiler:make_helper_mod(DDL),
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
    % TODO only returns error info about the first missing param, temperature
    %      should also be in the error message.
    ?assertEqual(
        {error, {missing_param, ?E_MISSING_PARAM_IN_WHERE_CLAUSE("user")}},
        rewrite(LK, W, Mod)
    ).

%%
%% complete query passing tests
%%

simplest_test() ->
    DDL = get_standard_ddl(),
    Query = "select weather from GeoCheckin where time > 3000 and time < 5000 and user = \"user_1\" and location = \"San Francisco\"",
    {ok, Q} = get_query(Query),
    true = is_query_valid(DDL, Q),
    Got = compile(DDL, Q),
    Where = [
         {startkey,        [
                {<<"location">>, varchar, <<"San Francisco">>},
                {<<"user">>, varchar,    <<"user_1">>},
                {<<"time">>, timestamp, 3000}
                   ]},
         {endkey,          [
                {<<"location">>, varchar, <<"San Francisco">>},
                {<<"user">>, varchar,    <<"user_1">>},
                {<<"time">>, timestamp, 5000}
                   ]},
         {filter,          []},
         {start_inclusive, false}
        ],
    Expected = [Q#riak_sql_v1{is_executable = true,
                              type          = timeseries,
                              'WHERE'       = Where,
                              partition_key = get_standard_pk(),
                              local_key     = get_standard_lk()
                     }],
    ?assertEqual(Expected, Got).

simple_with_filter_1_test() ->
    DDL = get_standard_ddl(),
    Query = "select weather from GeoCheckin where time > 3000 and time < 5000 and user = \"user_1\" and location = \"Scotland\" and weather = \"yankee\"",
    {ok, Q} = get_query(Query),
    true = is_query_valid(DDL, Q),
    Got = compile(DDL, Q),
    Where = [
        {startkey,        [
            {<<"location">>, varchar, <<"Scotland">>},
            {<<"user">>, varchar,    <<"user_1">>},
            {<<"time">>, timestamp, 3000}
        ]},
        {endkey,          [
            {<<"location">>, varchar, <<"Scotland">>},
            {<<"user">>, varchar,    <<"user_1">>},
            {<<"time">>, timestamp, 5000}
        ]},
        {filter,          {'=', {field, <<"weather">>, varchar}, {const, <<"yankee">>}}},
        {start_inclusive, false}
        ],
    Expected = [Q#riak_sql_v1{is_executable = true,
                              type          = timeseries,
                              'WHERE'       = Where,
                              partition_key = get_standard_pk(),
                              local_key     = get_standard_lk()
                     }],
    ?assertEqual(Expected, Got).

simple_with_filter_2_test() ->
    DDL = get_standard_ddl(),
    Query = "select weather from GeoCheckin where time >= 3000 and time < 5000 and user = \"user_1\" and location = \"Scotland\" and weather = \"yankee\"",
    {ok, Q} = get_query(Query),
    true = is_query_valid(DDL, Q),
    Got = compile(DDL, Q),
    Where = [
        {startkey,        [
            {<<"location">>, varchar, <<"Scotland">>},
            {<<"user">>, varchar,    <<"user_1">>},
            {<<"time">>, timestamp, 3000}
        ]},
        {endkey,          [
            {<<"location">>, varchar, <<"Scotland">>},
            {<<"user">>, varchar,    <<"user_1">>},
            {<<"time">>, timestamp, 5000}
        ]},
        {filter,   {'=', {field, <<"weather">>, varchar}, {const, <<"yankee">>}}}
        ],
    Expected = [Q#riak_sql_v1{is_executable = true,
                              type          = timeseries,
                              'WHERE'       = Where,
                              partition_key = get_standard_pk(),
                              local_key     = get_standard_lk()
                     }],
    ?assertEqual(Expected, Got).

simple_with_filter_3_test() ->
    DDL = get_standard_ddl(),
    Query = "select weather from GeoCheckin where time > 3000 and time <= 5000 and user = \"user_1\" and location = \"Scotland\" and weather = \"yankee\"",
    {ok, Q} = get_query(Query),
    true = is_query_valid(DDL, Q),
    Got = compile(DDL, Q),
    Where = [
         {startkey,        [
             {<<"location">>, varchar, <<"Scotland">>},
             {<<"user">>, varchar,    <<"user_1">>},
             {<<"time">>, timestamp, 3000}
         ]},
        {endkey,          [
            {<<"location">>, varchar, <<"Scotland">>},
            {<<"user">>, varchar,    <<"user_1">>},
            {<<"time">>, timestamp, 5000}
        ]},
         {filter,          {'=', {field, <<"weather">>, varchar}, {const, <<"yankee">>}}},
         {start_inclusive, false},
         {end_inclusive,   true}
        ],
    Expected = [Q#riak_sql_v1{is_executable = true,
                              type          = timeseries,
                              'WHERE'       = Where,
                              partition_key = get_standard_pk(),
                              local_key     = get_standard_lk()
                     }],
    ?assertEqual(Expected, Got).

simple_with_2_field_filter_test() ->
    DDL = get_standard_ddl(),
    Query = "select weather from GeoCheckin where time > 3000 and time < 5000 and user = \"user_1\" and location = \"Scotland\" and weather = \"yankee\" and temperature = 'yelp'",
    {ok, Q} = get_query(Query),
    true = is_query_valid(DDL, Q),
    Got = compile(DDL, Q),
    Where = [
        {startkey,        [
            {<<"location">>, varchar, <<"Scotland">>},
            {<<"user">>, varchar,    <<"user_1">>},
            {<<"time">>, timestamp, 3000}
        ]},
        {endkey,          [
            {<<"location">>, varchar, <<"Scotland">>},
            {<<"user">>, varchar,    <<"user_1">>},
            {<<"time">>, timestamp, 5000}
        ]},
         {filter,
             {and_,
                {'=', {field, <<"weather">>,     varchar},
                 {const, <<"yankee">>}},
                {'=', {field, <<"temperature">>, varchar},
                 {const, <<"yelp">>}}
             }
         },
         {start_inclusive, false}
        ],
    Expected = [Q#riak_sql_v1{is_executable = true,
                              type          = timeseries,
                              'WHERE'       = Where,
                              partition_key = get_standard_pk(),
                              local_key     = get_standard_lk()
                     }],
    ?assertEqual(Expected, Got).

complex_with_4_field_filter_test() ->
    DDL = get_long_ddl(),
    Query = "select weather from GeoCheckin where time > 3000 and time < 5000 and user = \"user_1\" and location = \"Scotland\" and extra = 1 and (weather = 'yankee' or (temperature = 'yelp' and geohash = 'erko'))",
    {ok, Q} = get_query(Query),
    true = is_query_valid(DDL, Q),
    Got = compile(DDL, Q),
    Where = [
        {startkey,        [
            {<<"location">>, varchar, <<"Scotland">>},
            {<<"user">>, varchar,    <<"user_1">>},
            {<<"time">>, timestamp, 3000}
        ]},
        {endkey,          [
            {<<"location">>, varchar, <<"Scotland">>},
            {<<"user">>, varchar,    <<"user_1">>},
            {<<"time">>, timestamp, 5000}
        ]},
         {filter,
             {and_,
                {or_,
                 {'=', {field, <<"weather">>, varchar},
                  {const, <<"yankee">>}},
                 {and_,
                  {'=', {field, <<"geohash">>,     varchar},
                   {const, <<"erko">>}},
                  {'=', {field, <<"temperature">>, varchar},
                   {const, <<"yelp">>}}
                 }
                },
                {'=', {field, <<"extra">>, sint64},
                 {const, 1}}
                   }
         },
         {start_inclusive, false}
        ],
    Expected = [Q#riak_sql_v1{is_executable = true,
                              type          = timeseries,
                              'WHERE'       = Where,
                              partition_key = get_standard_pk(),
                              local_key     = get_standard_lk()
                     }],
    ?assertEqual(Expected, Got).

complex_with_boolean_rewrite_filter_test() ->
    DDL = get_long_ddl(),
    Query = "select weather from GeoCheckin where time > 3000 and time < 5000 and user = \"user_1\" and location = \"Scotland\" and (myboolean = False or myboolean = tRue)",
    {ok, Q} = get_query(Query),
    true = is_query_valid(DDL, Q),
    Got = compile(DDL, Q),
    Where = [
        {startkey,        [
            {<<"location">>, varchar, <<"Scotland">>},
            {<<"user">>, varchar,    <<"user_1">>},
            {<<"time">>, timestamp, 3000}
        ]},
        {endkey,          [
            {<<"location">>, varchar, <<"Scotland">>},
            {<<"user">>, varchar,    <<"user_1">>},
            {<<"time">>, timestamp, 5000}
        ]},
         {filter,
             {or_,
                 {'=', {field, <<"myboolean">>, boolean},
                  {const, false}},
                                 {'=', {field, <<"myboolean">>, boolean},
                                  {const, true}}
                   }
         },
         {start_inclusive, false}
        ],
    Expected = [Q#riak_sql_v1{is_executable = true,
                              type          = timeseries,
                              'WHERE'       = Where,
                              partition_key = get_standard_pk(),
                              local_key     = get_standard_lk()
                     }],
    ?assertEqual(Expected, Got).

%% got for 3 queries to get partition ordering problems flushed out
simple_spanning_boundary_test() ->
    DDL = get_standard_ddl(),
    Query = "select weather from GeoCheckin where time >= 3000 and time < 31000 and user = \"user_1\" and location = \"Scotland\"",
    {ok, Q} = get_query(Query),
    true = is_query_valid(DDL, Q),
    %% get basic query
    Got = compile(DDL, Q),
    %% now make the result - expecting 3 queries
    W1 = [
        {startkey,        [
            {<<"location">>, varchar, <<"Scotland">>},
            {<<"user">>, varchar,    <<"user_1">>},
            {<<"time">>, timestamp, 3000}
        ]},
        {endkey,          [
            {<<"location">>, varchar, <<"Scotland">>},
            {<<"user">>, varchar,    <<"user_1">>},
            {<<"time">>, timestamp, 15000}
        ]},
      {filter,          []}
     ],
    W2 = [
        {startkey,        [
            {<<"location">>, varchar, <<"Scotland">>},
            {<<"user">>, varchar,    <<"user_1">>},
            {<<"time">>, timestamp, 15000}
        ]},
        {endkey,          [
            {<<"location">>, varchar, <<"Scotland">>},
            {<<"user">>, varchar,    <<"user_1">>},
            {<<"time">>, timestamp, 30000}
        ]},
      {filter,          []}
     ],
    W3 = [
        {startkey,        [
            {<<"location">>, varchar, <<"Scotland">>},
            {<<"user">>, varchar,    <<"user_1">>},
            {<<"time">>, timestamp, 30000}
        ]},
        {endkey,          [
            {<<"location">>, varchar, <<"Scotland">>},
            {<<"user">>, varchar,    <<"user_1">>},
            {<<"time">>, timestamp, 31000}
        ]},
      {filter,          []}
     ],
    Expected = [
        Q#riak_sql_v1{is_executable = true,
                  type          = timeseries,
                  'WHERE'       = W1,
                  partition_key = get_standard_pk(),
                  local_key     = get_standard_lk()},
        Q#riak_sql_v1{is_executable = true,
                  type          = timeseries,
                  'WHERE'       = W2,
                  partition_key = get_standard_pk(),
                  local_key     = get_standard_lk()},
        Q#riak_sql_v1{is_executable = true,
                  type          = timeseries,
                  'WHERE'       = W3,
                  partition_key = get_standard_pk(),
                  local_key     = get_standard_lk()}
           ],
    ?assertEqual(Expected, Got).

%%
%% test failures
%%

no_where_clause_fail_test() ->
    DDL = get_standard_ddl(),
    Query = "select weather from GeoCheckin where time > 3000 and time < 5000 and user = \"user_1\"",
    {ok, Q} = get_query(Query),
    true = is_query_valid(DDL, Q),
    %% now replace the where clause
    Where = [],
    Q2 = Q#riak_sql_v1{'WHERE' = Where},
    ?assertEqual(
        {error, {where_not_supported, Where}},
        compile(DDL, Q2)
    ).

simplest_fail_test() ->
    DDL = get_standard_ddl(),
    Query = "select weather from GeoCheckin where time > 3000 and time < 5000 and user = \"user_1\"",
    {ok, Q} = get_query(Query),
    true = is_query_valid(DDL, Q),
    Where = [{xor_, {myop, "fakefield", 22}, {notherop, "real_gucci", atombomb}}],
    %% now replace the where clause
    Q2 = Q#riak_sql_v1{'WHERE' = Where},
    ?assertEqual(
        {error, {where_not_supported, Where}},
        compile(DDL, Q2)
    ).

simplest_compile_once_only_fail_test() ->
    DDL = get_standard_ddl(),
    Query = "select weather from GeoCheckin where time >= 3000 and time < 5000 and user = \"user_1\" and location = \"Scotland\"",
    {ok, Q} = get_query(Query),
    true = is_query_valid(DDL, Q),
    %% now try and compile twice
    [Q2] = compile(DDL, Q),
    Got = compile(DDL, Q2),
    Expected = {error, 'query is already compiled'},
    ?assertEqual(Expected, Got).

end_key_not_a_range_test() ->
    DDL = get_standard_ddl(),
    {ok, Q} = get_query(
        "SELECT weather FROM GeoCheckin "
        "WHERE time > 3000 AND time != 5000 "
        "AND user = \"user_1\" AND location = \"derby\""),
    ?assertEqual(
        {error, {incomplete_where_clause, ?E_TSMSG_NO_UPPER_BOUND}},
        compile(DDL, Q)
    ).

start_key_not_a_range_test() ->
    DDL = get_standard_ddl(),
    {ok, Q} = get_query(
        "SELECT weather FROM GeoCheckin "
        "WHERE time = 3000 AND time < 5000 "
        "AND user = \"user_1\" AND location = \"derby\""),
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
        "SELECT weather FROM GeoCheckin "
        "WHERE time_c > 2999 AND time_c < 5000 "
        "AND time_a = 10 AND time_b = 15"),
    ?assertMatch(
        [#riak_sql_v1{
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
        }],
        compile(DDL, Q)
    ).

duplicate_lower_bound_filter_not_allowed_test() ->
    DDL = get_standard_ddl(),
    {ok, Q} = get_query(
        "SELECT weather FROM GeoCheckin "
        "WHERE time > 3000 AND  time > 3001 AND time < 5000 "
        "AND user = \"user_1\" AND location = \"derby\""),
    ?assertEqual(
        {error, {lower_bound_specified_more_than_once, ?E_TSMSG_DUPLICATE_LOWER_BOUND}},
        compile(DDL, Q)
    ).

duplicate_upper_bound_filter_not_allowed_test() ->
    DDL = get_standard_ddl(),
    {ok, Q} = get_query(
        "SELECT weather FROM GeoCheckin "
        "WHERE time > 3000 AND time < 5000 AND time < 4999 "
        "AND user = \"user_1\" AND location = \"derby\""),
    ?assertEqual(
        {error, {upper_bound_specified_more_than_once, ?E_TSMSG_DUPLICATE_UPPER_BOUND}},
        compile(DDL, Q)
    ).

% FIXME getting an invalid rewrite error on this
% filter_on_quanta_field_test() ->
%     DDL = get_standard_ddl(),
%     {ok, Q} = get_query(
%         "SELECT weather FROM GeoCheckin "
%         "WHERE time > 3000 AND time < 5000 "
%         "AND time = 3002 AND user = \"user_1\" AND location = \"derby\""),
%     ?assertMatch(
%         [#riak_sql_v1{
%             'WHERE' = [
%                 {startkey, [
%                     {<<"time_a">>, timestamp, 10},
%                     {<<"time_b">>, timestamp, 15},
%                     {<<"time_c">>, timestamp, 2999}
%                 ]},
%                 {endkey, [
%                     {<<"time_a">>, timestamp, 10},
%                     {<<"time_b">>, timestamp, 15},
%                     {<<"time_c">>, timestamp, 5000}
%                 ]},
%                 {filter, []},
%                 {start_inclusive, false}]
%         }],
%         compile(DDL, Q)
%     ).

-endif.
