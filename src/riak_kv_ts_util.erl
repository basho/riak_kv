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
         check_table_feature_supported/2,
         is_table_supported/2,
         encode_typeval_key/1,
         explain_query/1, explain_query/2,
         get_column_types/2,
         get_table_ddl/1,
         lk/1,
         maybe_parse_table_def/2,
         pk/1,
         queried_table/1,
         rm_rf/1,
         sql_record_to_tuple/1,
         sql_to_cover/6,
         table_to_bucket/1,
         validate_rows/2,
         varchar_to_timestamp/1
        ]).

-type ts_service_req() ::
    {ok, riak_kv_ts_svc:ts_requests(), {PermSpec::string(), Table::binary()}}.

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

-spec sql_record_to_tuple(?SQL_SELECT{}) -> {binary(), [tuple(2) | tuple(3)], list(), [tuple(2)]}.
%% riak_ql_ddl:is_query_valid expects a tuple, not a SQL record
sql_record_to_tuple(?SQL_SELECT{'FROM'   = From,
                                'SELECT' = #riak_sel_clause_v1{clause=Select},
                                'WHERE'  = Where,
                                'ORDER BY' = OrderBy}) ->
    {From, Select, Where, OrderBy}.

-spec build_sql_record(riak_kv_qry:query_type(), proplists:proplist(), proplists:proplist()) ->
                              {ok, ?SQL_SELECT{}} | {error, any()}.
build_sql_record(Command, SQL, Options) ->
    try build_sql_record_int(Command, SQL, Options) of
        {ok, Record} ->
            {ok, Record};
        ER ->
            ER
    catch
        throw:Atom ->
            {error, Atom}
    end.


%% Convert the proplist obtained from the QL parser
build_sql_record_int(select, SQL, _Options = undefined) ->
    build_sql_record_int(select, SQL, []);
build_sql_record_int(select, SQL, Options) ->
    AllowQBufReuse = proplists:get_value(allow_qbuf_reuse, Options),
    Cover          = proplists:get_value(cover, Options),
    if not (Cover == undefined orelse is_binary(Cover)) ->
            {error, bad_coverage_context};
       el/=se ->
            T = proplists:get_value(tables, SQL),
            F = proplists:get_value(fields, SQL),
            W = proplists:get_value(where,  SQL),
            L = proplists:get_value(limit,  SQL),
            O = proplists:get_value(offset, SQL),
            OrderBy = proplists:get_value(order_by, SQL),
            GroupBy = proplists:get_value(group_by, SQL),
            case is_binary(T) of
                true ->
                    Mod = riak_ql_ddl:make_module_name(T),
                    {ok,
                     ?SQL_SELECT{'SELECT'   = #riak_sel_clause_v1{clause = F},
                                 'FROM'     = T,
                                 'WHERE'    = convert_where_timestamps(Mod, W),
                                 'LIMIT'    = L,
                                 'OFFSET'   = O,
                                 'ORDER BY' = OrderBy,
                                 helper_mod = Mod,
                                 cover_context = Cover,
                                 allow_qbuf_reuse = AllowQBufReuse,
                                 group_by = GroupBy }
                    };
                false ->
                    {error, <<"Must provide exactly one table name">>}
            end
    end;

build_sql_record_int(explain, SQL, Options) ->
    case build_sql_record_int(select, SQL, Options) of
        {ok, Select} ->
            {ok, #riak_sql_explain_query_v1{'EXPLAIN' = Select}};
        Error -> Error
    end;

build_sql_record_int(describe, SQL, _Options) ->
    D = proplists:get_value(identifier, SQL),
    {ok, #riak_sql_describe_v1{'DESCRIBE' = D}};

build_sql_record_int(insert, SQL, _Options) ->
    Table = proplists:get_value(table, SQL),
    case is_binary(Table) of
        true ->
            %% To support non- and partial specification of columns, the DDL
            %% Module, Fields specified in the query, and Values specified in
            %% the query are sent to the riak_ql_ddl module to flesh out the
            %% missing or partially specified column to value mappings.
            Mod = riak_ql_ddl:make_module_name(Table),
            FieldsInQuery = proplists:get_value(fields, SQL, []),
            ValuesInQuery = proplists:get_value(values, SQL),
            {Fields, Values} = riak_ql_ddl:insert_sql_columns(Mod, FieldsInQuery, ValuesInQuery),

            %% In order to convert timestamps given as strings, rather
            %% than doing this here, we pass Values as is and
            %% relegate the timestamp conversion to
            %% riak_kv_qry:do_insert. The conversion will take place
            %% after checking for too many values wrt the number of
            %% Fields. Otherwise, this check would need to be done
            %% twice as the timestamp conversion code needs to have
            %% the Types available (and check that length(Types) ==
            %% length(Values)).
            {ok, #riak_sql_insert_v1{'INSERT'   = Table,
                                     fields     = Fields,
                                     values     = Values,
                                     helper_mod = Mod
                                    }};
        false ->
            {error, <<"Must provide exactly one table name">>}
    end;
build_sql_record_int(show_tables, _SQL, _Options) ->
    {ok, #riak_sql_show_tables_v1{}};
build_sql_record_int(show_create_table, SQL, _Options) ->
    Table = proplists:get_value(identifier, SQL),
    {ok, #riak_sql_show_create_table_v1{'SHOW_CREATE_TABLE' = Table}}.

convert_where_timestamps(_Mod, []) ->
    [];
convert_where_timestamps(Mod, Where) ->
    [replace_ast_timestamps(Mod, hd(Where))].

replace_ast_timestamps(_Mod, {NullOp, Item1}) when NullOp =:= is_null orelse
                                                   NullOp =:= is_not_null ->
    {NullOp, Item1};
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

varchar_to_timestamp(String) ->
    CompleteFun = fun(DT) -> jam:expand(DT, second) end,
    PostCompleteFun = fun(Epoch) -> Epoch end,
    do_epoch(String, CompleteFun, PostCompleteFun, 3).


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
queried_table(#riak_sql_show_tables_v1{})                -> <<>>;
queried_table(
    #riak_sql_explain_query_v1{'EXPLAIN'=?SQL_SELECT{'FROM' = Table}}) ->
        Table;
queried_table(#riak_sql_show_create_table_v1{'SHOW_CREATE_TABLE' = Table}) ->
    Table.

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

-spec encode_typeval_key(list({term(), term()})) -> tuple().
%% Encode a time series key returned by riak_ql_ddl:get_partition_key/3,
%% riak_ql_ddl:get_local_key/3,
encode_typeval_key(TypeVals) ->
    list_to_tuple([Val || {_Type, Val} <- TypeVals]).

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
explain_query(DDL, ?SQL_SELECT{'FROM' = Table} = Select) ->
    Props = riak_core_bucket_type:get(Table),
    NVal = proplists:get_value(n_val, Props),
    case riak_kv_qry_compiler:compile(DDL, Select) of
        {ok, SubQueries} ->
            {_Total, Rows} =
                lists:foldl(
                    fun(SQ, {Idx, Acc}) ->
                        Row = explain_sub_query(Idx, NVal, SQ),
                        {Idx + 1, [Row | Acc]}
                    end,
                    {1,[]}, SubQueries),
            lists:reverse(Rows);
        Error ->
            Error
    end;
explain_query(DDL, QueryString) ->
    {ok, Select} = explain_compile_query(QueryString),
    explain_query(DDL, Select).

%%
explain_compile_query(QueryString) ->
    {ok, Q} = riak_ql_parser:parse(riak_ql_lexer:get_tokens(QueryString)),
    build_sql_record(select, Q, []).

%%
explain_sub_query(Index, NVal, ?SQL_SELECT{'FROM' = Table,
                                           'WHERE' = SubQueryWhere,
                                           helper_mod = HelperMod,
                                           partition_key = PartitionKey}) ->
    CoverKey = riak_kv_qry_coverage_plan:make_key(HelperMod, PartitionKey, SubQueryWhere),
    Coverage = format_coverage(Table, CoverKey, NVal),
    {_, StartKey1} = lists:keyfind(startkey, 1, SubQueryWhere),
    {_, EndKey1} = lists:keyfind(endkey, 1, SubQueryWhere),
    {_, Filter} = lists:keyfind(filter, 1, SubQueryWhere),
    StartKey2 = string:join([key_to_string(Key) || Key <- StartKey1], ", "),
    EndKey2 = string:join([key_to_string(Key) || Key <- EndKey1], ", "),
    StartInclusive = lists:keymember(start_inclusive, 1, StartKey1),
    EndInclusive = lists:keymember(end_inclusive, 1, EndKey1),
    [Index,
     list_to_binary(Coverage),
     list_to_binary(StartKey2),
     StartInclusive,
     list_to_binary(EndKey2),
     EndInclusive,
     list_to_binary(filter_to_string(Filter))].

%%
format_coverage(Table, CoverKey, NVal) ->
    Bucket = table_to_bucket(Table),
    DocIdx = riak_core_util:chash_key({Bucket, CoverKey}),
    Coverage = riak_core_apl:get_primary_apl(DocIdx, NVal, riak_kv),
    RingSize = riak_client:ring_size(),
    VNodes = lists:map(
        fun({{Key, Node}, _}) ->
            Exponent = trunc(math:log(RingSize)/math:log(2)),
            Number = Key bsr (160-Exponent),
            lists:flatten(io_lib:format("~s/~b", [Node, Number]))
        end, Coverage),
    string:join(lists:sort(VNodes), ", ").

%%
key_element_to_string(V) when is_binary(V) -> binary_to_list(V);
key_element_to_string(V) when is_float(V) -> mochinum:digits(V);
key_element_to_string(V) -> lists:flatten(io_lib:format("~p", [V])).

%%
%% This one quotes values which should be quoted for assignment
element_to_quoted_string(V) when is_binary(V) -> varchar_quotes(V);
element_to_quoted_string(V) when is_float(V) -> mochinum:digits(V);
element_to_quoted_string(V) -> lists:flatten(io_lib:format("~p", [V])).

%%
-spec key_to_string({binary(),any(),term()}) -> string().
key_to_string({Field, _Op, Value}) ->
    lists:flatten(io_lib:format("~s = ~s",
        [key_element_to_string(Field), element_to_quoted_string(Value)])).

%%
-spec filter_to_string([]| {const,any()} | {atom(),any(),any()}) ->
    string().
filter_to_string([]) ->
    [];
filter_to_string({const,V}) ->
    element_to_quoted_string(V);
filter_to_string({field,V,_}) ->
    key_element_to_string(V);
filter_to_string({Op, A, B}) ->
    lists:flatten(io_lib:format("(~s~s~s)",
        [filter_to_string(A), op_to_string(Op), filter_to_string(B)])).

%%
op_to_string(and_) -> " AND ";
op_to_string(or_) -> " OR ";
op_to_string(Op) -> " " ++ atom_to_list(Op) ++ " ".

%%
varchar_quotes(V) when is_binary(V) ->
    lists:flatten(io_lib:format("'~s'", [V])).

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


%% Given the current capability value of the ddl records supported in the
%% cluster and the decoded request, check if the table features are supported
%% in the cluster.
-spec check_table_feature_supported(DDLRecCap::atom(),
                                    DecodedReq::ts_service_req()) -> ts_service_req() | {error, string()}.
check_table_feature_supported(DDLRecCap, DecodedReq) ->
    case DecodedReq of
        {ok, {?DDL{} = DDL, _}, {_, Table}} ->
            %% CREATE TABLE requires a separate path because there is no helper
            %% module yet to check the min required ddl capability, so just
            %% work it out from the DDL itself.
            MinCap = riak_ql_ddl:get_minimum_capability(DDL),
            case is_ddl_version_supported(MinCap, DDLRecCap) of
                true ->
                    DecodedReq;
                false ->
                    {error, create_table_not_supported_message(Table)}
            end;
        {ok, _, {_, Table}} ->
            case is_table_supported(DDLRecCap, Table) of
                true ->
                    DecodedReq;
                false ->
                    {error, table_not_supported_message(Table)}
            end;
        _ ->
            DecodedReq
    end.

%%
is_table_supported(_, << >>) ->
    %% an empty binary for the table name means that the request is not for a
    %% specific table e.g. SHOW TABLES
    true;
is_table_supported(DDLRecCap, Table) when is_binary(Table) ->
    Mod = riak_ql_ddl:make_module_name(Table),
    MinCap = Mod:get_min_required_ddl_cap(),
    MinCap /= disabled andalso is_ddl_version_supported(MinCap, DDLRecCap).

%%
is_ddl_version_supported(MinCap, DDLRecCap) ->
    riak_ql_ddl:is_version_greater(DDLRecCap, MinCap) /= false.

%%
table_not_supported_message(Table) ->
    Reason = "Request was not executed. Table ~ts has features not supported "
             "by all nodes in the cluster.",
    lists:flatten(io_lib:format(Reason, [Table])).

%%
create_table_not_supported_message(Table) ->
    Reason = "Table was not ~ts was not created. It contains features which "
             "are not supported by all nodes in the cluster.",
    lists:flatten(io_lib:format(Reason, [Table])).

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

%% @doc Does os:cmd(flat_format("rm -rf '~s'", [Dir])).
rm_rf(Dir) ->
    rm_direntries(Dir, filelib:wildcard("*", flat_format("~s", [Dir]))).

rm_direntries(Dir, []) ->
    case file:del_dir(Dir) of
        {error, enoent} ->
            ok;
        OkOrOther ->
            OkOrOther
    end;
rm_direntries(Dir, [F|Rest]) ->
    Entry = filename:join(Dir, F),
    Res =
        case filelib:is_dir(Entry) of
            true ->
                rm_rf(Entry);
            false ->
                file:delete(Entry)
        end,
    case Res of
        ok ->
            rm_direntries(Dir, Rest);
        ER ->
            ER
    end.


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
                 build_sql_record(select, Q, [])).

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
    {ok, Select} = build_sql_record(select, Q, []),
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
                 build_sql_record(select, Q, [])).


rm_rf_test() ->
    Dir = test_server:temp_name("/tmp/"),
    os:cmd(flat_format("mkdir -p '~s'", [Dir])),
    os:cmd(flat_format("mkdir -p '~s/ke'", [Dir])),
    os:cmd(flat_format("mkdir -p '~s/mi'", [Dir])),
    os:cmd(flat_format("touch '~s/a'", [Dir])),
    os:cmd(flat_format("touch '~s/ke/b'", [Dir])),
    os:cmd(flat_format("touch '~s/d'", [Dir])),
    os:cmd(flat_format("ln -s '~s/d' '~s/A'", [Dir, Dir])),
    Exists1 = filelib:is_dir(Dir),
    ?assertEqual(Exists1, true),
    ok = rm_rf(Dir),
    Exists2 = filelib:is_dir(Dir),
    ?assertEqual(Exists2, false).



%%
helper_sql_to_module(SQL) ->
    Lexed = riak_ql_lexer:get_tokens(SQL),
    {ddl, DDL, _Props} = riak_ql_parser:ql_parse(Lexed),
    {module, _Module} = riak_ql_ddl_compiler:compile_and_load_from_tmp(DDL).

check_table_feature_supported_error_test() ->
    ?assertEqual(
        {error, myerror},
        check_table_feature_supported(v2, {error, myerror})
    ).

check_table_feature_supported_is_supported_v1_test() ->
    Table =
        "CREATE TABLE check_table_feature_supported_is_supported_v1_test ("
        " a varchar not null,"
        " b varchar not null,"
        " c timestamp not null,"
        " primary key ((a, b, quantum(c, 1, 'm')), a, b, c))",
    {module, _Mod} = helper_sql_to_module(Table),
    DecodedReq = {ok, req, {"perm", <<"check_table_feature_supported_is_supported_v1_test">>}},
    ?assertEqual(
       DecodedReq,
       check_table_feature_supported(v1, DecodedReq)
    ).

check_table_feature_supported_is_supported_v2_test() ->
    Table =
        "CREATE TABLE check_table_feature_supported_is_supported_v2_test ("
        " a varchar not null,"
        " b varchar not null,"
        " c timestamp not null,"
        " primary key ((a, b, quantum(c, 1, 'm')), a, b, c))",
    {module, _Mod} = helper_sql_to_module(Table),
    DecodedReq = {ok, req, {"perm", <<"check_table_feature_supported_is_supported_v2_test">>}},
    ?assertEqual(
       DecodedReq,
       check_table_feature_supported(v2, DecodedReq)
    ).

check_table_feature_supported_not_supported_test() ->
    Table =
        "CREATE TABLE check_table_feature_supported_not_supported_test ("
        " a varchar not null,"
        " b varchar not null,"
        " c timestamp not null,"
        " primary key ((a, b, quantum(c, 1, 'm')), a, b, c DESC))",
    {module, _Mod} = helper_sql_to_module(Table),
    DecodedReq = {ok, req, {"perm", <<"check_table_feature_supported_not_supported_test">>}},
    ?assertMatch(
        {error, _},
        check_table_feature_supported(v1, DecodedReq)
    ).

check_table_feature_supported_when_table_is_disabled_test() ->
    Table = <<"check_table_feature_supported_when_table_is_disabled_test">>,
    {module, _Mod} = riak_ql_ddl_compiler:compile_and_load_disabled_module_from_tmp(Table),
    DecodedReq = {ok, req, {"perm", Table}},
    ?assertMatch(
        {error, _},
        check_table_feature_supported(v2, DecodedReq)
    ).

-endif.
