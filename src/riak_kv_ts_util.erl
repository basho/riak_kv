%% -------------------------------------------------------------------
%%
%% riak_kv_ts_util: supporting functions for timeseries code paths
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

%% @doc Various functions related to timeseries.

-module(riak_kv_ts_util).

-export([
         apply_timeseries_bucket_props/3,
         build_sql_record/3,
         encode_typeval_key/1,
         get_column_types/2,
         get_table_ddl/1,
         lk/1,
         lk_to_pk/3,
         make_ts_keys/3,
         maybe_parse_table_def/2,
         pk/1,
         queried_table/1,
         sql_record_to_tuple/1,
         sql_to_cover/6,
         table_to_bucket/1,
         validate_rows/2,
         row_to_key/3
        ]).
-export([explain_query/1, explain_query/2]).
-export([explain_query_print/1]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-compile(export_all).
-endif.

%% NOTE on table_to_bucket/1: Clients will work with table
%% names. Those names map to a bucket type/bucket name tuple in Riak,
%% with both the type name and the bucket name matching the table.
%%
%% Thus, as soon as code transitions from dealing with timeseries
%% concepts to Riak KV concepts, the table name must be converted to a
%% bucket tuple. This function is a convenient mechanism for doing so
%% and making that transition more obvious.

%%-include("riak_kv_wm_raw.hrl").
-include_lib("riak_ql/include/riak_ql_ddl.hrl").
-include("riak_kv_ts.hrl").

%% riak_ql_ddl:is_query_valid expects a tuple, not a SQL record
sql_record_to_tuple(?SQL_SELECT{'FROM'   = From,
                                'SELECT' = #riak_sel_clause_v1{clause=Select},
                                'WHERE'  = Where}) ->
    {From, Select, Where}.

build_sql_record(Command, SQL, Cover) ->
    try build_sql_record_int(Command, SQL, Cover) of
        Return -> Return
    catch
        throw:Atom ->
            {error, Atom}
    end.

%% Convert the proplist obtained from the QL parser
build_sql_record_int(select, SQL, Cover) ->
    T = proplists:get_value(tables, SQL),
    F = proplists:get_value(fields, SQL),
    L = proplists:get_value(limit, SQL),
    W = proplists:get_value(where, SQL),
    GroupBy = proplists:get_value(group_by, SQL),
    case is_binary(T) of
        true ->
            Mod = riak_ql_ddl:make_module_name(T),
            {ok,
             ?SQL_SELECT{'SELECT'   = #riak_sel_clause_v1{clause = F},
                         'FROM'     = T,
                         'WHERE'    = convert_where_timestamps(Mod, W),
                         'LIMIT'    = L,
                         helper_mod = Mod,
                         cover_context = Cover,
                         group_by = GroupBy }
            };
        false ->
            {error, <<"Must provide exactly one table name">>}
    end;
build_sql_record_int(describe, SQL, _Cover) ->
    D = proplists:get_value(identifier, SQL),
    {ok, #riak_sql_describe_v1{'DESCRIBE' = D}};
build_sql_record_int(insert, SQL, _Cover) ->
    T = proplists:get_value(table, SQL),
    case is_binary(T) of
        true ->
            Mod = riak_ql_ddl:make_module_name(T),
            %% If columns are not specified, all columns are implied
            F = riak_ql_ddl:insert_sql_columns(Mod, proplists:get_value(fields, SQL, [])),
            V = proplists:get_value(values, SQL),
            {ok, #riak_sql_insert_v1{'INSERT'   = T,
                                     fields     = F,
                                     values     = convert_insert_timestamps(Mod, F, V),
                                     helper_mod = Mod
                                    }};
        false ->
            {error, <<"Must provide exactly one table name">>}
    end;
build_sql_record_int(show_tables, _SQL, _Cover) ->
    {ok, #riak_sql_show_tables_v1{}}.

convert_where_timestamps(_Mod, []) ->
    [];
convert_where_timestamps(Mod, Where) ->
    [replace_ast_timestamps(Mod, hd(Where))].

replace_ast_timestamps(Mod, {Op, Item1, Item2}) when is_tuple(Item1) andalso is_tuple(Item2) ->
    {Op, replace_ast_timestamps(Mod, Item1), replace_ast_timestamps(Mod, Item2)};
replace_ast_timestamps(Mod, {Op, FieldName, {binary, Value}}) ->
    {Op, FieldName, maybe_convert_to_epoch(catch Mod:get_field_type([FieldName]), Value, Op)};
replace_ast_timestamps(_Mod, {Op, Item1, Item2}) ->
    {Op, Item1, Item2}.

do_epoch(String, CompleteFun, PostCompleteFun, Exponent) ->
    Normal = timestamp_to_normalized(String),
    case jam:is_valid(Normal) of
        false ->
            throw(<<"Invalid date/time string">>);
        true ->
            ok
    end,
    Epoch = case jam:is_complete(Normal) of
                true ->
                    jam:to_epoch(Normal, Exponent);
                false ->
                    PostCompleteFun(jam:to_epoch(CompleteFun(Normal), Exponent))
            end,
    case is_atom(Epoch) of
        true ->
            %% jam returns atoms if processing gives an error
            throw(Epoch);
        false ->
            Epoch
    end.

maybe_convert_to_epoch({'EXIT', _}, Value, _Op) ->
    {binary, Value};
maybe_convert_to_epoch(timestamp, Value, Op) when Op == '>'; Op == '<=' ->
    %% For strictly greater-than or less-than-equal-to, we increment
    %% any incomplete date/time strings by one "unit", where the unit
    %% is the least significant value provided by the user. "2016"
    %% becomes "2017". "2016-07-05T11" becomes "2016-07-05T12".
    %%
    %% The result is then decremented by 1ms to maintain the proper
    %% semantics. The alternative would be to convert the operator in
    %% the syntax tree (from > to >=, or from <= to <).
    CompleteFun = fun(DT) -> jam:expand(jam:increment(DT, 1), second) end,
    PostCompleteFun = fun(Epoch) -> Epoch - 1 end,
    {integer, do_epoch(Value, CompleteFun, PostCompleteFun, 3)};
maybe_convert_to_epoch(timestamp, Value, _Op) ->
    CompleteFun = fun(DT) -> jam:expand(DT, second) end,
    PostCompleteFun = fun(Epoch) -> Epoch end,
    {integer, do_epoch(Value, CompleteFun, PostCompleteFun, 3)};
maybe_convert_to_epoch(_Type, Value, _Op) ->
    {binary, Value}.

convert_insert_timestamps(Mod, Fields, Rows) ->
    lists:map(fun(Row) -> row_timestamp_translation(Mod, Fields, Row) end,
              Rows).

row_timestamp_translation(Mod, Fields, Row) ->
    Types = lists:map(fun({identifier, [Column]}) ->
                              catch Mod:get_field_type([Column])
                      end, Fields),
    TypeMap = lists:zip(Row, Types),

    CompleteFun = fun(DT) -> jam:expand(DT, second) end,
    PostCompleteFun = fun(Epoch) -> Epoch end,
    lists:map(fun({binary, String}=Value) ->
                      case lists:keyfind(Value, 1, TypeMap) of
                          {Value, timestamp} ->
                              {integer, do_epoch(String, CompleteFun,
                                                 PostCompleteFun, 3)};
                          _ ->
                              Value
                      end;
                 (Value) ->
                      Value
              end, Row).

get_default_timezone(Default) ->
    try
        riak_core_metadata:get({riak_core, timezone},
                               string, [{default, Default}])
            of
        Value ->
            Value
    catch
        _:_ -> Default
    end.

timestamp_to_normalized(Str) ->
    ProcessOptions =
        [
         {default_timezone, get_default_timezone("+00")}
        ],
    Normal = jam:normalize(
               jam:compile(
                 jam_iso8601:parse(Str), ProcessOptions)
              ),
    case Normal of
        undefined ->
            throw(<<"Invalid date/time string">>);
        _ ->
            Normal
    end.

%% Useful key extractors for functions (e.g., in get or delete code
%% paths) which are agnostic to whether they are dealing with TS or
%% non-TS data
pk({PK, _LK}) ->
    PK;
pk(NonTSKey) ->
    NonTSKey.

lk({_PK, LK}) ->
    LK;
lk(NonTSKey) ->
    NonTSKey.

%% Utility API to limit some of the confusion over tables vs buckets
table_to_bucket(Table) when is_binary(Table) ->
    {Table, Table}.


-spec queried_table(riak_kv_qry:sql_query_type_record() | ?DDL{}) -> binary().
%% Extract table name from various sql records.
queried_table(?DDL{table = Table}) -> Table;
queried_table(#riak_sql_describe_v1{'DESCRIBE' = Table}) -> Table;
queried_table(?SQL_SELECT{'FROM' = Table})               -> Table;
queried_table(#riak_sql_insert_v1{'INSERT' = Table})     -> Table;
queried_table(#riak_sql_show_tables_v1{})                -> <<>>.


-spec get_table_ddl(binary()) ->
                           {ok, module(), ?DDL{}} |
                           {error, term()}.
%% Check that Table is in good standing and ready for TS operations
%% (its bucket type has been activated and it has a DDL in its props)
get_table_ddl(Table) when is_binary(Table) ->
    case riak_core_bucket:get_bucket(table_to_bucket(Table)) of
        {error, _} = Error ->
            Error;
        [_|_] ->
            Mod = riak_ql_ddl:make_module_name(Table),
            case catch Mod:get_ddl() of
                {_, {undef, _}} ->
                    {error, missing_helper_module};
                DDL ->
                    {ok, Mod, DDL}
            end
    end.


%%
-spec apply_timeseries_bucket_props(DDL :: ?DDL{},
                                    DDLVersion :: riak_ql_component:component_version(),
                                    Props1 :: [proplists:property()]) ->
        {ok, Props2::[proplists:property()]}.
apply_timeseries_bucket_props(DDL, DDLVersion, Props1) ->
    Props2 = lists:keystore(
        <<"write_once">>, 1, Props1, {<<"write_once">>, true}),
    Props3 = lists:keystore(
        <<"ddl">>, 1, Props2, {<<"ddl">>, DDL}),
    Props4 = lists:keystore(
        <<"ddl_compiler_version">>, 1, Props3, {<<"ddl_compiler_version">>, DDLVersion}),
    {ok, Props4}.


-spec maybe_parse_table_def(BucketType :: binary(),
                            Props :: list(proplists:property())) ->
        {ok, Props2 :: [proplists:property()]} | {error, any()}.
%% Given bucket properties, transform the `<<"table_def">>' property if it
%% exists to riak_ql DDL and store it under the `<<"ddl">>' key, table_def
%% is removed.
maybe_parse_table_def(BucketType, Props) ->
    case lists:keytake(<<"table_def">>, 1, Props) of
        false ->
            {ok, Props};
        {value, {<<"table_def">>, TableDef}, PropsNoDef} ->
            case catch riak_ql_parser:ql_parse(
                         riak_ql_lexer:get_tokens(binary_to_list(TableDef))) of
                {ddl, DDL = ?DDL{}, WithProps} ->
                    ok = assert_type_and_table_name_same(BucketType, DDL),
                    ok = try_compile_ddl(DDL),
                    MergedProps = merge_props_with_preference(
                                    PropsNoDef, WithProps),
                    ok = assert_write_once_not_false(BucketType, MergedProps),
                    apply_timeseries_bucket_props(DDL, riak_ql_ddl_compiler:get_compiler_version(), MergedProps);
                {'EXIT', {Reason, _}} ->
                    % the lexer throws exceptions, the reason should always be a
                    % binary
                    {error, Reason};
                {error, {_LineNo, riak_ql_parser, Reason}} ->
                    % the parser returns errors, the reason should always be a
                    % binary
                    {error, Reason};
                {error, _} = E ->
                    E
            end
    end.

%% Time series must always use write_once so throw an error if the write_once
%% property is ever set to false. This prevents a user thinking they have
%% disabled write_once when it has been set to true internally.
assert_write_once_not_false(BucketType, Props) ->
    case lists:keyfind(<<"write_once">>, 1, Props) of
        {<<"write_once">>, false} ->
            throw({error,
                   {write_once,
                    flat_format(
                      "Time series bucket type ~s has write_once == false", [BucketType])}});
        _ ->
            ok
    end.

-spec merge_props_with_preference(proplists:proplist(), proplists:proplist()) ->
                                         proplists:proplist().
%% If same keys appear in RpbBucketProps as well as embedded in the
%% query ("CREATE TABLE ... WITH"), we merge the two proplists giving
%% preference to the latter.
merge_props_with_preference(PbProps, WithProps) ->
    lists:foldl(
      fun({K, _} = P, Acc) -> lists:keystore(K, 1, Acc, P) end,
      PbProps, WithProps).

%% Ensure table name in DDL and bucket type are the same.
assert_type_and_table_name_same(BucketType, ?DDL{table = BucketType}) ->
    ok;
assert_type_and_table_name_same(BucketType1, ?DDL{table = BucketType2}) ->
    throw({error,
           {table_name,
            flat_format(
              "Time series bucket type and table name do not match (~s != ~s)",
              [BucketType1, BucketType2])}}).

%% Attempt to compile the DDL but don't do anything with the output, this is
%% catch failures as early as possible. Also the error messages are easy to
%% return at this point.
try_compile_ddl(DDL) ->
    {_, AST} = riak_ql_ddl_compiler:compile(DDL),
    {ok, _, _} = compile:forms(AST),
    ok.


-spec make_ts_keys([riak_pb_ts_codec:ldbvalue()], ?DDL{}, module()) ->
                          {ok, {tuple(), tuple()}} |
                          {error, {bad_key_length, integer(), integer()}}.
%% Given a list of values (of appropriate types) and a DDL, produce a
%% partition and local key pair, which can be used in riak_client:get
%% to fetch TS objects.
make_ts_keys(CompoundKey, DDL = ?DDL{local_key = #key_v1{ast = LKAst},
                                     fields = Fields}, Mod) ->
    %% 1. use elements in Key to form a complete data record:
    KeyFields = [F || ?SQL_PARAM{name = [F]} <- LKAst],
    AllFields = [F || #riak_field_v1{name = F} <- Fields],
    Got = length(CompoundKey),
    Need = length(KeyFields),
    case {Got, Need} of
        {_N, _N} ->
            DummyObject = build_dummy_object_from_keyed_values(
                            lists:zip(KeyFields, CompoundKey), AllFields),
            %% 2. make the PK and LK
            PK  = encode_typeval_key(
                    riak_ql_ddl:get_partition_key(DDL, DummyObject, Mod)),
            LK  = encode_typeval_key(
                    riak_ql_ddl:get_local_key(DDL, DummyObject, Mod)),
            {ok, {PK, LK}};
       {G, N} ->
            {error, {bad_key_length, G, N}}
    end.

build_dummy_object_from_keyed_values(LK, AllFields) ->
    VoidRecord = [{F, void} || F <- AllFields],
    %% (void values will not be looked at in riak_ql_ddl:make_key;
    %% only LK-constituent fields matter)
    list_to_tuple(
      [proplists:get_value(K, LK)
       || {K, _} <- VoidRecord]).


-spec encode_typeval_key(list({term(), term()})) -> tuple().
%% Encode a time series key returned by riak_ql_ddl:get_partition_key/3,
%% riak_ql_ddl:get_local_key/3,
encode_typeval_key(TypeVals) ->
    list_to_tuple([Val || {_Type, Val} <- TypeVals]).


-spec lk_to_pk(tuple(), ?DDL{}, module()) -> tuple().
%% A simplified version of make_key/3 which only returns the PK.
lk_to_pk(LKVals, DDL = ?DDL{local_key = #key_v1{ast = LKAst},
                        fields = Fields}, Mod)
  when is_tuple(LKVals) andalso size(LKVals) == length(LKAst) ->
    KeyFields = [F || ?SQL_PARAM{name = [F]} <- LKAst],
    AllFields = [F || #riak_field_v1{name = F} <- Fields],
    DummyObject = build_dummy_object_from_keyed_values(
                    lists:zip(KeyFields, tuple_to_list(LKVals)), AllFields),
    encode_typeval_key(
      riak_ql_ddl:get_partition_key(DDL, DummyObject, Mod)).


%% Print the query explanation to the shell.
explain_query_print(QueryString) ->
    explain_query_print2(1, explain_query(QueryString)).

explain_query_print2(_, []) ->
    ok;
explain_query_print2(Index, [{Start, End, Filter}|Tail]) ->
    io:format("SUB QUERY ~p~n~s ~s~n~s~n",
        [Index,Start,filter_to_string(Filter),End]),
    explain_query_print2(Index+1, Tail).

%% Show some debug info about how a query is compiled into sub queries
%% and what key ranges are created.
explain_query(QueryString) ->
    {ok, ?SQL_SELECT{ 'FROM' = Table } = Select} =
        explain_compile_query(QueryString),
    {ok, _Mod, DDL} = get_table_ddl(Table),
    explain_query(DDL, Select).

%% Explain a query using the ddl and select records. The select can be a query
%% string.
%%
%% Have a flexible API because it is a debugging function.
explain_query(DDL, ?SQL_SELECT{} = Select) ->
    {ok, SubQueries} = riak_kv_qry_compiler:compile(DDL, Select, 10000),
    [explain_sub_query(SQ) || SQ <- SubQueries];
explain_query(DDL, QueryString) ->
    {ok, Select} = explain_compile_query(QueryString),
    explain_query(DDL, Select).

%%
explain_compile_query(QueryString) ->
    {ok, Q} = riak_ql_parser:parse(riak_ql_lexer:get_tokens(QueryString)),
    build_sql_record(select, Q, undefined).

%%
explain_sub_query(?SQL_SELECT{ 'WHERE' = SubQueryWhere }) ->
    {_, StartKey1} = lists:keyfind(startkey, 1, SubQueryWhere),
    {_, EndKey1} = lists:keyfind(endkey, 1, SubQueryWhere),
    {_, Filter} = lists:keyfind(filter, 1, SubQueryWhere),
    explain_query_keys(StartKey1, EndKey1, Filter).

%%
explain_query_keys(StartKey1, EndKey1, Filter) ->
    StartKey2 = [[key_element_to_string(V), $/] || {_,_,V} <- StartKey1],
    EndKey2 = [[key_element_to_string(V), $/] || {_,_,V} <- EndKey1],
    case lists:keyfind(start_inclusive, 1, StartKey1) of
        {start_inclusive,true} ->
            StartKey3 = [">= ", StartKey2];
        _ ->
            StartKey3 = [">  ", StartKey2]
    end,
    case lists:keyfind(end_inclusive, 1, EndKey1) of
        {end_inclusive,true} ->
            EndKey3 = ["<= ", EndKey2];
        _ ->
            EndKey3 = ["<  ", EndKey2]
    end,
    {StartKey3, EndKey3, Filter}.

%%
key_element_to_string(V) when is_binary(V) -> varchar_quotes(V);
key_element_to_string(V) when is_float(V) -> mochinum:digits(V);
key_element_to_string(V) -> io_lib:format("~p", [V]).

%%
filter_to_string([]) ->
    "NO FILTER";
filter_to_string(Filter) ->
    ["FILTER ", filter_to_string2(Filter)].

%%
filter_to_string2({const,V}) ->
    key_element_to_string(V);
filter_to_string2({field,V,_}) ->
    V;
filter_to_string2({Op, A, B}) ->
    [filter_to_string2(A), op_to_string(Op), filter_to_string2(B)].

%%
op_to_string(and_) -> " AND ";
op_to_string(or_) -> " OR ";
op_to_string(Op) -> " " ++ atom_to_list(Op) ++ " ".

%%
varchar_quotes(V) ->
    <<"'", V/binary, "'">>.

%% Give validate_rows/2 a DDL Module and a list of decoded rows,
%% and it will return a list of invalid rows indexes.
-spec validate_rows(module(), list(tuple())) -> [integer()].
validate_rows(Mod, Rows) ->
    ValidateFn =
        fun(Row, {N, Acc}) ->
                case Mod:validate_obj(Row) of
                    true -> {N + 1, Acc};
                    _    -> {N + 1, [N|Acc]}
                end
        end,
    {_, BadRowIdxs} = lists:foldl(ValidateFn, {1, []}, Rows),
    lists:reverse(BadRowIdxs).


-spec get_column_types(list(binary()), module()) -> [riak_pb_ts_codec:tscolumntype()].
get_column_types(ColumnNames, Mod) ->
    [Mod:get_field_type([N]) || N <- ColumnNames].

row_to_key(Row, DDL, Mod) ->
    encode_typeval_key(
      riak_ql_ddl:get_partition_key(DDL, Row, Mod)).

%% Result from riak_client:get_cover is a nested list of coverage plan
%% because KV coverage requests are designed that way, but in our case
%% all we want is the singleton head

%% If any of the results from get_cover are errors, we want that tuple
%% to be the sole return value
sql_to_cover(_Client, [], _Bucket, _Replace, _Unavail, Accum) ->
    lists:reverse(Accum);
sql_to_cover(Client, [SQL|Tail], Bucket, undefined, _Unavail, Accum) ->
    case Client:get_cover(riak_kv_qry_coverage_plan, Bucket, undefined,
                          {SQL, Bucket}) of
        {error, Error} ->
            {error, Error};
        [Cover] ->
            {Description, SubRange} = reverse_sql(SQL),
            Node = proplists:get_value(node, Cover),
            {IP, Port} = riak_kv_pb_coverage:node_to_pb_details(Node),

            %% As of 1.3, coverage chunks returned to the client for
            %% KV are a simple proplist, but for TS are a tuple with
            %% the first element the proplist, and the 2nd element a
            %% representation of the quantum to which this chunk
            %% applies.
            %%
            %% The fact that the structures are unnecessarily
            %% different results in some awkwardness when interpreting
            %% them. See `riak_client:replace_cover' for an example.
            %%
            %% As of 1.6 or post-merge equivalent (so that mixed
            %% clusters with 1.3 are not supported), the SubRange
            %% value should be added to the cover proplist instead of
            %% creating this clumsy tuple so that cover chunks are
            %% simple proplists for all types of coverage queries.
            %%
            %% See also `riak_kv_qry_compiler:unwrap_cover/1' for the
            %% code that will interpret the proplists properly as of
            %% 1.4 and later.
            %%
            %% The 2nd field in the tuple that is returned by
            %% `reverse_sql/1' is itself a tuple with two elements:
            %% the name of the quantum field and the relevant range
            %% for this chunk.
            %%
            %% So the code to implement with 1.6 would be roughly:
            %% Context = term_to_checksum_binary(Cover ++
            %%     [{ts_where_field, element(1, SubRange)},
            %%      {ts_where_range, element(2, SubRange)}]).
            %%
            %% (Real code would be pattern-matchy and generally cleaner.)

            Context = riak_kv_pb_coverage:term_to_checksum_binary({Cover, SubRange}),
            Entry = {{IP, Port}, Context, SubRange, Description},
            sql_to_cover(Client, Tail, Bucket, undefined, [], [Entry|Accum])
    end;
sql_to_cover(Client, [SQL|Tail], Bucket, Replace, Unavail, Accum) ->
    case Client:replace_cover(riak_kv_qry_coverage_plan, Bucket, undefined,
                              riak_kv_pb_coverage:checksum_binary_to_term(Replace),
                              lists:map(fun riak_kv_pb_coverage:checksum_binary_to_term/1,
                                        Unavail),
                              [{SQL, Bucket}]) of
        {error, Error} ->
            {error, Error};
        [Cover] ->
            {Description, SubRange} = reverse_sql(SQL),
            Node = proplists:get_value(node, Cover),
            {IP, Port} = riak_kv_pb_coverage:node_to_pb_details(Node),
            Context = riak_kv_pb_coverage:term_to_checksum_binary({Cover, SubRange}),
            Entry = {{IP, Port}, Context, SubRange, Description},
            sql_to_cover(Client, Tail, Bucket, Replace, Unavail, [Entry|Accum])
    end.



%% Generate a human-readable description of the target
%%     <<"<TABLE> / time > X and time < Y">>
%% Generate a start/end timestamp for future replacement in a query
reverse_sql(?SQL_SELECT{'FROM'  = Table,
                        'WHERE' = KeyProplist,
                        partition_key = PartitionKey}) ->
    QuantumField = identify_quantum_field(PartitionKey),
    RangeTuple = extract_time_boundaries(QuantumField, KeyProplist),
    Desc = derive_description(Table, QuantumField, RangeTuple),
    ReplacementValues = {QuantumField, RangeTuple},
    {Desc, ReplacementValues}.


derive_description(Table, Field, {{Start, StartInclusive}, {End, EndInclusive}}) ->
    StartOp = pick_operator(">", StartInclusive),
    EndOp = pick_operator("<", EndInclusive),
    unicode:characters_to_binary(
      flat_format("~ts / ~ts ~s ~B and ~ts ~s ~B",
                  [Table, Field, StartOp, Start,
                   Field, EndOp, End]), utf8).

pick_operator(LGT, true) ->
    LGT ++ "=";
pick_operator(LGT, false) ->
    LGT.

extract_time_boundaries(FieldName, WhereList) ->
    {FieldName, timestamp, Start} =
        lists:keyfind(FieldName, 1, proplists:get_value(startkey, WhereList, [])),
    {FieldName, timestamp, End} =
        lists:keyfind(FieldName, 1, proplists:get_value(endkey, WhereList, [])),
    StartInclusive = proplists:get_value(start_inclusive, WhereList, true),
    EndInclusive = proplists:get_value(end_inclusive, WhereList, false),
    {{Start, StartInclusive}, {End, EndInclusive}}.


%%%%%%%%%%%%
%% FRAGILE HORRIBLE BAD BAD BAD AST MANGLING
identify_quantum_field(#key_v1{ast = KeyList}) ->
    HashFn = find_hash_fn(KeyList),
    P_V1 = hd(HashFn#hash_fn_v1.args),
    hd(P_V1?SQL_PARAM.name).

find_hash_fn([]) ->
    throw(wtf);
find_hash_fn([#hash_fn_v1{}=Hash|_T]) ->
    Hash;
find_hash_fn([_H|T]) ->
    find_hash_fn(T).

%%%%%%%%%%%%


flat_format(Format, Args) ->
    lists:flatten(io_lib:format(Format, Args)).

%%%
%%% TESTS
%%%

-ifdef(TEST).

helper_compile_def_to_module(SQL) ->
    Lexed = riak_ql_lexer:get_tokens(SQL),
    {ok, {DDL, _Props}} = riak_ql_parser:parse(Lexed),
    {module, Mod} = riak_ql_ddl_compiler:compile_and_load_from_tmp(DDL),
    {DDL, Mod}.

% basic family/series/timestamp
make_ts_keys_1_test() ->
    {DDL, Mod} = helper_compile_def_to_module(
        "CREATE TABLE table1 ("
        "a SINT64 NOT NULL, "
        "b SINT64 NOT NULL, "
        "c TIMESTAMP NOT NULL, "
        "PRIMARY KEY((a, b, quantum(c, 15, 's')), a, b, c))"),
    ?assertEqual(
        {ok, {{1,2,0}, {1,2,3}}},
        make_ts_keys([1,2,3], DDL, Mod)
    ).

% a two element key, still using the table definition field order
make_ts_keys_2_test() ->
    {DDL, Mod} = helper_compile_def_to_module(
        "CREATE TABLE table1 ("
        "a SINT64 NOT NULL, "
        "b TIMESTAMP NOT NULL, "
        "c SINT64 NOT NULL, "
        "PRIMARY KEY((a, quantum(b, 15, 's')), a, b))"),
    ?assertEqual(
        {ok, {{1,0}, {1,2}}},
        make_ts_keys([1,2], DDL, Mod)
    ).

make_ts_keys_3_test() ->
    {DDL, Mod} = helper_compile_def_to_module(
        "CREATE TABLE table2 ("
        "a SINT64 NOT NULL, "
        "b SINT64 NOT NULL, "
        "c TIMESTAMP NOT NULL, "
        "d SINT64 NOT NULL, "
        "PRIMARY KEY  ((d,a,quantum(c, 1, 's')), d,a,c))"),
    ?assertEqual(
        {ok, {{10,20,0}, {10,20,1}}},
        make_ts_keys([10,20,1], DDL, Mod)
    ).

make_ts_keys_4_test() ->
    {DDL, Mod} = helper_compile_def_to_module(
        "CREATE TABLE table2 ("
        "ax SINT64 NOT NULL, "
        "a SINT64 NOT NULL, "
        "b SINT64 NOT NULL, "
        "c TIMESTAMP NOT NULL, "
        "d SINT64 NOT NULL, "
        "PRIMARY KEY  ((ax,a,quantum(c, 1, 's')), ax,a,c))"),
    ?assertEqual(
        {ok, {{10,20,0}, {10,20,1}}},
        make_ts_keys([10,20,1], DDL, Mod)
    ).


test_helper_validate_rows_mod() ->
    {ddl, DDL, []} =
        riak_ql_parser:ql_parse(
          riak_ql_lexer:get_tokens(
            "CREATE TABLE mytable ("
            "family VARCHAR NOT NULL,"
            "series VARCHAR NOT NULL,"
            "time TIMESTAMP NOT NULL,"
            "PRIMARY KEY ((family, series, quantum(time, 1, 'm')),"
            " family, series, time))")),
    riak_ql_ddl_compiler:compile_and_load_from_tmp(DDL).

validate_rows_empty_test() ->
    {module, Mod} = test_helper_validate_rows_mod(),
    ?assertEqual(
        [],
        validate_rows(Mod, [])
    ).

validate_rows_1_test() ->
    {module, Mod} = test_helper_validate_rows_mod(),
    ?assertEqual(
        [],
        validate_rows(Mod, [{<<"f">>, <<"s">>, 11}])
    ).

validate_rows_bad_1_test() ->
    {module, Mod} = test_helper_validate_rows_mod(),
    ?assertEqual(
        [1],
        validate_rows(Mod, [{}])
    ).

validate_rows_bad_2_test() ->
    {module, Mod} = test_helper_validate_rows_mod(),
    ?assertEqual(
        [1, 3, 4],
        validate_rows(Mod, [{}, {<<"f">>, <<"s">>, 11}, {a, <<"s">>, 12}, {"hithere"}])
    ).

bad_timestamp_select_test() ->
    BadFormat = "20151T10",
    BadQuery = lists:flatten(
                 io_lib:format("select * from table1 "
                               "where b > '~s' and "
                               " b < '20151013T20:00:00'", [BadFormat])),
    {_DDL, _Mod} = helper_compile_def_to_module(
        "CREATE TABLE table1 ("
        "a SINT64 NOT NULL, "
        "b TIMESTAMP NOT NULL, "
        "c SINT64 NOT NULL, "
        "PRIMARY KEY((a, quantum(b, 15, 's')), a, b))"),
    Lexed = riak_ql_lexer:get_tokens(BadQuery),
    {ok, Q} = riak_ql_parser:parse(Lexed),
    ?assertMatch({error, <<"Invalid date/time string">>},
                 build_sql_record(select, Q, undefined)).

good_timestamp_select_test() ->
    GoodQuery = "select * from table1 "
        " where b > '20151013T18:30' and "
        " b <= '20151013T19' ",
    {_DDL, _Mod} = helper_compile_def_to_module(
        "CREATE TABLE table1 ("
        "a SINT64 NOT NULL, "
        "b TIMESTAMP NOT NULL, "
        "c SINT64 NOT NULL, "
        "PRIMARY KEY((a, quantum(b, 15, 's')), a, b))"),
    Lexed = riak_ql_lexer:get_tokens(GoodQuery),
    {ok, Q} = riak_ql_parser:parse(Lexed),
    {ok, Select} = build_sql_record(select, Q, undefined),
    Where = Select?SQL_SELECT.'WHERE',
    %% Navigate the tree looking for integer values for b
    %% comparisons. Verify that we find both (hence `2' as our
    %% equality)
    ?assertEqual(2,
                 check_integer_timestamps(
                   hd(Where),
                   %% The first value, the lower bound, is
                   %% 20151013T18:31:00 (- 1ms) because of the
                   %% incomplete datetime string. The upper bound is
                   %% 20151013T20:00:00 (- 1ms) for the same reason.
                   1444761059999,
                   1444766399999)).

%% This is not a general-purpose tool, it is tuned specifically to the
%% needs of `good_timestamp_select_test'.
check_integer_timestamps({_Op, A, B}, Lower, Upper) when is_tuple(A), is_tuple(B) ->
    check_integer_timestamps(A, Lower, Upper) +
        check_integer_timestamps(B, Lower, Upper);
check_integer_timestamps({'<=', <<"b">>, Compare}, _Lower, Upper) ->
    ?assertEqual({integer, Upper}, Compare),
    1;
check_integer_timestamps({'>', <<"b">>, Compare}, Lower, _Upper) ->
    ?assertEqual({integer, Lower}, Compare),
    1;
check_integer_timestamps(_, _, _) ->
    0.

good_timestamp_insert_test() ->
    GoodInsert = "insert into table1 values "
        " (1, '2015', 2), "
        " (1, '2015-06', 2), "
        " (1, '2015-06-05', 2), "
        " (1, '2015-06-05 10', 2), "
        " (1, '2015-06-05 10:10:11', 2), "
        " (1, '2015-06-05 10:10:11Z', 2) ",
    {_DDL, _Mod} = helper_compile_def_to_module(
        "CREATE TABLE table1 ("
        "a SINT64 NOT NULL, "
        "b TIMESTAMP NOT NULL, "
        "c SINT64 NOT NULL, "
        "PRIMARY KEY((a, quantum(b, 15, 's')), a, b))"),
    Lexed = riak_ql_lexer:get_tokens(GoodInsert),
    {ok, Q} = riak_ql_parser:parse(Lexed),
    {ok, Insert} = build_sql_record(insert, Q, undefined),
    Values = Insert#riak_sql_insert_v1.values,

    %% Verify all values are now integers
    ?assertEqual([],
                 lists:filtermap(fun(Row) ->
                                         case lists:filter(fun({integer, _}) -> false;
                                                              (_) -> true end, Row) of
                                             [] ->
                                                 false;
                                             _ ->
                                                 true
                                         end
                                 end, Values)).

timestamp_parsing_test() ->
    BadFormat = "20151T10",
    BadQuery = lists:flatten(
                 io_lib:format("select * from table1 "
                               "where b > '~s' and "
                               " b < '201510T20:00:00'", [BadFormat])),
    {_DDL, Mod} = helper_compile_def_to_module(
        "CREATE TABLE table1 ("
        "a SINT64 NOT NULL, "
        "b TIMESTAMP NOT NULL, "
        "c SINT64 NOT NULL, "
        "PRIMARY KEY((a, quantum(b, 15, 's')), a, b))"),
    ?assertEqual(timestamp, Mod:get_field_type([<<"b">>])),
    Lexed = riak_ql_lexer:get_tokens(BadQuery),
    {ok, Q} = riak_ql_parser:parse(Lexed),
    ?assertMatch({error, <<"Invalid date/time string">>},
                 build_sql_record(select, Q, undefined)).

-endif.
