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
         encode_typeval_key/1,
         get_table_ddl/1,
         lk/1,
         make_ts_keys/3,
         maybe_parse_table_def/2,
         pk/1,
         queried_table/1,
         table_to_bucket/1,
         build_sql_record/3,
         sql_record_to_tuple/1
        ]).

%% NOTE on table_to_bucket/1: Clients will work with table
%% names. Those names map to a bucket type/bucket name tuple in Riak,
%% with both the type name and the bucket name matching the table.
%%
%% Thus, as soon as code transitions from dealing with timeseries
%% concepts to Riak KV concepts, the table name must be converted to a
%% bucket tuple. This function is a convenient mechanism for doing so
%% and making that transition more obvious.

-include_lib("riak_ql/include/riak_ql_ddl.hrl").
-include("riak_kv_ts.hrl").

%% riak_ql_ddl:is_query_valid expects a tuple, not a SQL record
sql_record_to_tuple(?SQL_SELECT{'FROM'=From,
                                'SELECT'=#riak_sel_clause_v1{clause=Select},
                                'WHERE'=Where}) ->
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
