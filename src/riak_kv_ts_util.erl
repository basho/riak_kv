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
         apply_timeseries_bucket_props/2,
         build_sql_record/3,
         compile_to_per_quantum_queries/2,
         delete_data/2, delete_data/3, delete_data/4, delete_data/5,
         encode_typeval_key/1,
         get_column_types/2,
         get_data/2, get_data/3, get_data/4,
         get_table_ddl/1,
         lk/1,
         make_ts_keys/3,
         maybe_parse_table_def/2,
         pk/1,
         put_data/2, put_data/3,
         queried_table/1,
         sql_record_to_tuple/1,
         sql_to_cover/4,
         table_to_bucket/1,
         validate_rows/2
        ]).

%% NOTE on table_to_bucket/1: Clients will work with table
%% names. Those names map to a bucket type/bucket name tuple in Riak,
%% with both the type name and the bucket name matching the table.
%%
%% Thus, as soon as code transitions from dealing with timeseries
%% concepts to Riak KV concepts, the table name must be converted to a
%% bucket tuple. This function is a convenient mechanism for doing so
%% and making that transition more obvious.

-include("riak_kv_wm_raw.hrl").
-include_lib("riak_ql/include/riak_ql_ddl.hrl").
-include("riak_kv_ts.hrl").

%% riak_ql_ddl:is_query_valid expects a tuple, not a SQL record
sql_record_to_tuple(?SQL_SELECT{'FROM'   = From,
                                'SELECT' = #riak_sel_clause_v1{clause=Select},
                                'WHERE'  = Where}) ->
    {From, Select, Where}.

%% Convert the proplist obtained from the QL parser
build_sql_record(select, SQL, Cover) ->
    T = proplists:get_value(tables, SQL),
    F = proplists:get_value(fields, SQL),
    L = proplists:get_value(limit, SQL),
    W = proplists:get_value(where, SQL),
    case is_binary(T) of
        true ->
            {ok,
             ?SQL_SELECT{'SELECT'   = #riak_sel_clause_v1{clause = F},
                         'FROM'     = T,
                         'WHERE'    = W,
                         'LIMIT'    = L,
                         helper_mod = riak_ql_ddl:make_module_name(T),
                         cover_context = Cover}
            };
        false ->
            {error, <<"Must provide exactly one table name">>}
    end;
build_sql_record(describe, SQL, _Cover) ->
    D = proplists:get_value(identifier, SQL),
    {ok, #riak_sql_describe_v1{'DESCRIBE' = D}}.


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


-spec queried_table(#riak_sql_describe_v1{} | ?SQL_SELECT{}) -> binary().
%% Extract table name from various sql records.
queried_table(#riak_sql_describe_v1{'DESCRIBE' = Table}) -> Table;
queried_table(?SQL_SELECT{'FROM' = Table})               -> Table.


-spec get_table_ddl(binary()) ->
                           {ok, module(), #ddl_v1{}} |
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
-spec apply_timeseries_bucket_props(DDL::#ddl_v1{},
                                    Props1::[proplists:property()]) ->
        {ok, Props2::[proplists:property()]}.
apply_timeseries_bucket_props(DDL, Props1) ->
    Props2 = lists:keystore(
        <<"write_once">>, 1, Props1, {<<"write_once">>, true}),
    {ok, [{<<"ddl">>, DDL} | Props2]}.


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
            case catch riak_ql_parser:parse(riak_ql_lexer:get_tokens(binary_to_list(TableDef))) of
                {ok, DDL} ->
                    ok = assert_type_and_table_name_same(BucketType, DDL),
                    ok = try_compile_ddl(DDL),
                    ok = assert_write_once_not_false(PropsNoDef),
                    apply_timeseries_bucket_props(DDL, PropsNoDef);
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
assert_write_once_not_false(Props) ->
    case lists:keyfind(<<"write_once">>, 1, Props) of
        {<<"write_once">>, false} ->
            throw({error,
                   {write_once,
                    "Error, the time series bucket type could not be created. "
                    "The write_once property must be true\n"}});
        _ ->
            ok
    end.

%% Ensure table name in DDL and bucket type are the same.
assert_type_and_table_name_same(BucketType, #ddl_v1{table = BucketType}) ->
    ok;
assert_type_and_table_name_same(BucketType1, #ddl_v1{table = BucketType2}) ->
    throw({error,
           {table_name,
            "The bucket type and table name must be the same\n"
            "    bucket type was: " ++ binary_to_list(BucketType1) ++ "\n"
            "     table name was: " ++ binary_to_list(BucketType2) ++ "\n"}}).

%% Attempt to compile the DDL but don't do anything with the output, this is
%% catch failures as early as possible. Also the error messages are easy to
%% return at this point.
try_compile_ddl(DDL) ->
    {_, AST} = riak_ql_ddl_compiler:compile(DDL),
    {ok, _, _} = compile:forms(AST),
    ok.


-spec make_ts_keys([riak_pb_ts_codec:ldbvalue()], #ddl_v1{}, module()) ->
                          {ok, {binary(), binary()}} |
                          {error, {bad_key_length, integer(), integer()}}.
%% Given a list of values (of appropriate types) and a DDL, produce a
%% partition and local key pair, which can be used in riak_client:get
%% to fetch TS objects.
make_ts_keys(CompoundKey, DDL = #ddl_v1{local_key = #key_v1{ast = LKParams},
                                        fields = Fields}, Mod) ->
    %% 1. use elements in Key to form a complete data record:
    KeyFields = [F || #param_v1{name = [F]} <- LKParams],
    Got = length(CompoundKey),
    Need = length(KeyFields),
    case {Got, Need} of
        {_N, _N} ->
            KeyAssigned = lists:zip(KeyFields, CompoundKey),
            VoidRecord = [{F, void} || #riak_field_v1{name = F} <- Fields],
            %% (void values will not be looked at in riak_ql_ddl:make_key;
            %% only LK-constituent fields matter)
            BareValues =
                list_to_tuple(
                  [proplists:get_value(K, KeyAssigned)
                   || {K, _} <- VoidRecord]),

            %% 2. make the PK and LK
            PK  = encode_typeval_key(
                    riak_ql_ddl:get_partition_key(DDL, BareValues, Mod)),
            LK  = encode_typeval_key(
                    riak_ql_ddl:get_local_key(DDL, BareValues, Mod)),
            {ok, {PK, LK}};
       {G, N} ->
            {error, {bad_key_length, G, N}}
    end.

-spec encode_typeval_key(list({term(), term()})) -> tuple().
%% Encode a time series key returned by riak_ql_ddl:get_partition_key/3,
%% riak_ql_ddl:get_local_key/3,
encode_typeval_key(TypeVals) ->
    list_to_tuple([Val || {_Type, Val} <- TypeVals]).


%% Give validate_rows/2 a DDL Module and a list of decoded rows,
%% and it will return a list of strings that represent the invalid rows indexes.
-spec validate_rows(module(), list(tuple())) -> list(string()).
validate_rows(Mod, Rows) ->
    ValidateFn = fun(X, {Acc, BadRowIdxs}) ->
        case Mod:validate_obj(X) of
            true -> {Acc+1, BadRowIdxs};
            _ -> {Acc+1, [integer_to_list(Acc) | BadRowIdxs]}
        end
    end,
    {_, BadRowIdxs} = lists:foldl(ValidateFn, {1, []}, Rows),
    lists:reverse(BadRowIdxs).


-spec put_data([[riak_pb_ts_codec:ldbvalue()]], binary()) -> integer().
%% return count of records we failed to put
put_data(Data, Table) ->
    put_data(Data, Table, riak_ql_ddl:make_module_name(Table)).

-spec put_data([[riak_pb_ts_codec:ldbvalue()]], binary(), module()) -> integer().
put_data(Data, Table, Mod) ->
    DDL = Mod:get_ddl(),
    Bucket = table_to_bucket(Table),
    BucketProps = riak_core_bucket:get_bucket(Bucket),
    NVal = proplists:get_value(n_val, BucketProps),

    PartitionedData = partition_data(Data, Bucket, BucketProps, DDL, Mod),
    PreflistData = add_preflists(PartitionedData, NVal,
                                 riak_core_node_watcher:nodes(riak_kv)),
    EncodeFn =
        fun(O) -> riak_object:to_binary(v1, O, msgpack) end,

    {ReqIds, FailReqs} =
        lists:foldl(
          fun({DocIdx, Preflist, Records}, {GlobalReqIds, GlobalErrorsCnt}) ->
                  case riak_kv_w1c_worker:validate_options(
                         NVal, Preflist, [], BucketProps) of
                      {ok, W, PW} ->
                          {Ids, Errs} =
                              lists:foldl(
                                fun(Record, {PartReqIds, PartErrors}) ->
                                        {RObj, LK} =
                                            build_object(Bucket, Mod, DDL,
                                                         Record, DocIdx),

                                        {ok, ReqId} =
                                            riak_kv_w1c_worker:async_put(
                                              RObj, W, PW, Bucket, NVal, LK,
                                              EncodeFn, Preflist),
                                        {[ReqId | PartReqIds], PartErrors}
                                end,
                                {[], 0}, Records),
                          {GlobalReqIds ++ Ids, GlobalErrorsCnt + Errs};
                      _Error ->
                          {GlobalReqIds, GlobalErrorsCnt + length(Records)}
                  end
          end,
          {[], 0}, PreflistData),
    Responses = riak_kv_w1c_worker:async_put_replies(ReqIds, []),
    length(
      lists:filter(
        fun({error, _}) -> true;
           (_) -> false
        end, Responses)) + FailReqs.


-spec partition_data(Data :: list(term()),
                     Bucket :: {binary(), binary()},
                     BucketProps :: proplists:proplist(),
                     DDL :: #ddl_v1{},
                     Mod :: module()) ->
                            list(tuple(chash:index(), list(term()))).
partition_data(Data, Bucket, BucketProps, DDL, Mod) ->
    PartitionTuples =
        [ { riak_core_util:chash_key({Bucket, row_to_key(R, DDL, Mod)},
                                     BucketProps), R } || R <- Data ],
    dict:to_list(
      lists:foldl(fun({Idx, R}, Dict) ->
                          dict:append(Idx, R, Dict)
                  end,
                  dict:new(),
                  PartitionTuples)).

row_to_key(Row, DDL, Mod) ->
    encode_typeval_key(
      riak_ql_ddl:get_partition_key(DDL, Row, Mod)).

add_preflists(PartitionedData, NVal, UpNodes) ->
    lists:map(fun({Idx, Rows}) -> {Idx,
                                   riak_core_apl:get_apl_ann(Idx, NVal, UpNodes),
                                   Rows} end,
              PartitionedData).

build_object(Bucket, Mod, DDL, Row, PK) ->
    Obj = Mod:add_column_info(Row),
    LK  = encode_typeval_key(
            riak_ql_ddl:get_local_key(DDL, Row, Mod)),

    RObj = riak_object:newts(
             Bucket, PK, Obj,
             dict:from_list([{?MD_DDL_VERSION, ?DDL_VERSION}])),
    {RObj, LK}.




-spec get_data([riak_pb_ts_codec:ldbvalue()], binary()) ->
                      {ok, {[binary()], [[riak_pb_ts_codec:ldbvalue()]]}} | {error, term()}.
get_data(Key, Table) ->
    get_data(Key, Table, undefined, []).

-spec get_data([riak_pb_ts_codec:ldbvalue()], binary(), module()) ->
                      {ok, {[binary()], [[riak_pb_ts_codec:ldbvalue()]]}} | {error, term()}.
get_data(Key, Table, Mod) ->
    get_data(Key, Table, Mod, []).

-spec get_data([riak_pb_ts_codec:ldbvalue()], binary(), module(), proplists:proplist()) ->
                      {ok, [{binary(), riak_pb_ts_codec:ldbvalue()}]} | {error, term()}.
get_data(Key, Table, Mod0, Options) ->
    Mod =
        case Mod0 of
            undefined ->
                riak_ql_ddl:make_module_name(Table);
            Mod0 ->
                Mod0
        end,
    DDL = Mod:get_ddl(),
    Result =
        case make_ts_keys(Key, DDL, Mod) of
            {ok, PKLK} ->
                riak_client:get(
                  table_to_bucket(Table), PKLK, Options,
                  {riak_client, [node(), undefined]});
            ErrorReason ->
                ErrorReason
        end,
    case Result of
        {ok, RObj} ->
            case riak_object:get_value(RObj) of
                [] ->
                    {error, notfound};
                Record ->
                    {ok, Record}
            end;
        ErrorReason2 ->
            ErrorReason2
    end.

-spec get_column_types(list(binary()), module()) -> [riak_pb_ts_codec:tscolumntype()].
get_column_types(ColumnNames, Mod) ->
    [Mod:get_field_type([N]) || N <- ColumnNames].


-spec delete_data([any()], riak_object:bucket()) ->
                         ok | {error, term()}.
delete_data(Key, Table) ->
    delete_data(Key, Table, undefined, [], undefined).

-spec delete_data([any()], riak_object:bucket(), module()) ->
                         ok | {error, term()}.
delete_data(Key, Table, Mod) ->
    delete_data(Key, Table, Mod, [], undefined).

-spec delete_data([any()], riak_object:bucket(), module(), proplists:proplist()) ->
                         ok | {error, term()}.
delete_data(Key, Table, Mod, Options) ->
    delete_data(Key, Table, Mod, Options, undefined).

-spec delete_data([any()], riak_object:bucket(), module(), proplists:proplist(),
                  undefined | vclock:vclock()) ->
                         ok | {error, term()}.
delete_data(Key, Table, Mod0, Options0, VClock0) ->
    Mod =
        case Mod0 of
            undefined ->
                riak_ql_ddl:make_module_name(Table);
            Mod0 ->
                Mod0
        end,
    %% Pass the {dw,all} option in to the delete FSM
    %% to make sure all tombstones are written by the
    %% async put before the reaping get runs otherwise
    %% if the default {dw,quorum} is used there is the
    %% possibility that the last tombstone put overlaps
    %% inside the KV vnode with the reaping get and
    %% prevents the tombstone removal.
    Options = lists:keystore(dw, 1, Options0, {dw, all}),
    DDL = Mod:get_ddl(),
    VClock =
        case VClock0 of
            undefined ->
                %% this will trigger a get in riak_kv_delete:delete to
                %% retrieve the actual vclock
                undefined;
            VClock0 ->
                %% else, clients may have it already (e.g., from an
                %% earlier riak_object:get), which will short-circuit
                %% to avoid a separate get
                riak_object:decode_vclock(VClock0)
        end,
    Result =
        case make_ts_keys(Key, DDL, Mod) of
            {ok, PKLK} ->
                riak_client:delete_vclock(
                  table_to_bucket(Table), PKLK, VClock, Options,
                  {riak_client, [node(), undefined]});
            ErrorReason ->
                ErrorReason
        end,
    Result.


%% Result from riak_client:get_cover is a nested list of coverage plan
%% because KV coverage requests are designed that way, but in our case
%% all we want is the singleton head

%% If any of the results from get_cover are errors, we want that tuple
%% to be the sole return value
sql_to_cover(_Client, [], _Bucket, Accum) ->
    lists:reverse(Accum);
sql_to_cover(Client, [SQL|Tail], Bucket, Accum) ->
    case Client:get_cover(riak_kv_qry_coverage_plan, Bucket, undefined,
                          {SQL, Bucket}) of
        {error, Error} ->
            {error, Error};
        [Cover] ->
            {Description, RangeReplacement} = reverse_sql(SQL),
            sql_to_cover(Client, Tail, Bucket, [{Cover, RangeReplacement,
                                                 Description}|Accum])
    end.

-spec compile_to_per_quantum_queries(module(), ?SQL_SELECT{}) ->
                                            {ok, [?SQL_SELECT{}]} | {error, any()}.
%% @doc Break up a query into a list of per-quantum queries
compile_to_per_quantum_queries(Mod, SQL) ->
    case catch Mod:get_ddl() of
        {_, {undef, _}} ->
            {error, no_helper_module};
        DDL ->
            case riak_ql_ddl:is_query_valid(
                   Mod, DDL, sql_record_to_tuple(SQL)) of
                true ->
                    riak_kv_qry_compiler:compile(DDL, SQL, undefined);
                {false, _Errors} ->
                    {error, invalid_query}
            end
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
    hd(P_V1#param_v1.name).

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
-include_lib("eunit/include/eunit.hrl").

helper_compile_def_to_module(SQL) ->
    Lexed = riak_ql_lexer:get_tokens(SQL),
    {ok, DDL} = riak_ql_parser:parse(Lexed),
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
% make_ts_keys_2_test() ->
%     {DDL, Mod} = helper_compile_def_to_module(
%         "CREATE TABLE table1 ("
%         "a SINT64 NOT NULL, "
%         "b TIMESTAMP NOT NULL, "
%         "c SINT64 NOT NULL, "
%         "PRIMARY KEY((a, quantum(b, 15, 's')), a, b))"),
%     ?assertEqual(
%         {ok, {{1,0}, {1,2}}},
%         make_ts_keys([1,2], DDL, Mod)
%     ).

% make_ts_keys_3_test() ->
%     {DDL, Mod} = helper_compile_def_to_module(
%         "CREATE TABLE table2 ("
%         "a SINT64 NOT NULL, "
%         "b SINT64 NOT NULL, "
%         "c TIMESTAMP NOT NULL, "
%         "d SINT64 NOT NULL, "
%         "PRIMARY KEY  ((d,a,quantum(c, 1, 's')), d,a,c))"),
%     ?assertEqual(
%         {ok, {{10,20,0}, {10,20,1}}},
%         make_ts_keys([10,20,1], DDL, Mod)
%     ).

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

-endif.
