%% -------------------------------------------------------------------
%%
%% riak_kv_ts_util: supporting functions for timeseries code paths
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

%% @doc Various functions related to timeseries.

-module(riak_kv_ts_util).

-export([maybe_parse_table_def/2]).

-include_lib("riak_ql/include/riak_ql_ddl.hrl").

%% Given bucket properties, transform the `<<"table_def">>' property if it
%% exists to riak_ql DDL and store it under the `<<"ddl">>' key, table_def
%% is removed.
-spec maybe_parse_table_def(BucketType :: binary(),
                            Props :: list(proplists:property())) ->
        {ok, Props2 :: [proplists:property()]} | {error, any()}.
maybe_parse_table_def(BucketType, Props) ->
    case lists:keytake(<<"table_def">>, 1, Props) of
        false ->
            {ok, Props};
        {value, {<<"table_def">>, TableDef}, PropsNoDef} ->
            case riak_ql_parser:parse(riak_ql_lexer:get_tokens(binary_to_list(TableDef))) of
                {ok, DDL} ->
                    ok = assert_type_and_table_name_same(BucketType, DDL),
                    ok = try_compile_ddl(DDL),
                    ok = assert_write_once_not_false(PropsNoDef),
                    apply_timeseries_bucket_props(DDL, PropsNoDef);
                {error, _} = E ->
                    E
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

%% @doc Ensure table name in DDL and bucket type are the same.
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
