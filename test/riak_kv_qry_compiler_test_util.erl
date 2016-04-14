%% -------------------------------------------------------------------
%%
%% riak_kv_qry_compiler_test_util: shared functions for testing the
%% riak_kv_qry_compiler
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
-module(riak_kv_qry_compiler_test_util).

-export([
         is_query_valid/2,
         get_where_clauses/1,
         get_query/1,
         get_long_ddl/0,
         get_standard_ddl/0,
         get_ddl/1,
         get_standard_pk/0,
         get_standard_lk/0
        ]).
%%
%% Helper Fns for unit tests
%%

-include_lib("riak_ql/include/riak_ql_ddl.hrl").
-include("riak_kv_ts.hrl").

-define(MIN, 60 * 1000).
-define(NAME, "time").

is_query_valid(#ddl_v1{ table = Table } = DDL, Q) ->
    Mod = riak_ql_ddl:make_module_name(Table),
    riak_ql_ddl:is_query_valid(Mod, DDL, riak_kv_ts_util:sql_record_to_tuple(Q)).

get_where_clauses(SQLs) when is_list(SQLs) ->
    [W || ?SQL_SELECT{'WHERE' = W} <- SQLs].

get_query(String) ->
    Lexed = riak_ql_lexer:get_tokens(String),
    {ok, Q} = riak_ql_parser:parse(Lexed),
    riak_kv_ts_util:build_sql_record(select, Q, undefined).

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

