%% -------------------------------------------------------------------
%%
%% Store the state about what bucket type DDLs have been compiled.
%%
%% Copyright (c) 2015-2016 Basho Technologies, Inc.  All Rights Reserved.
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

-module(riak_kv_compile_tab).

-export([
         delete_dets/1,
         get_all_table_names/0,
         get_compiled_ddl_versions/1,
         get_ddl/2,
         insert/2,
         new/1,
         populate_v3_table/0
        ]).

-include_lib("riak_ql/include/riak_ql_ddl.hrl").

%% the table for TS 1.4 and below
-define(TABLE2, riak_kv_compile_tab_v2).
%% the table for TS 1.5
-define(TABLE3, riak_kv_compile_tab_v3).

%% previous versions of the compile table used a plain tuple, and it was
%% difficult to different versions of the row tuple in the table for fear
%% of mixing them up. A versioned row record means several versions can
%% co-exist in the same table, and be matched in isolation.
%%
%% This row still requires a new table because the dets key index is
%% defaulted to 1.  All values default to underscores for simple matching.
-record(row_v3, {
    table = '_' :: binary(),
    %% dets key, a composite of the table name and ddl version
    table_version = '_' :: {binary(), riak_ql_ddl:ddl_version()},
    ddl = '_' :: riak_ql_ddl:any_ddl()
 }).

%% do not repair the table when we're testing, because it is always a one-shot
%% and writes out ugly logs.
-ifdef(TEST).
-define(DETS_OPTIONS, [{type, set}]).
-else.
-define(DETS_OPTIONS, [{type, set}, {repair, force}]).
-endif.

%%
-spec new(file:name()) -> {ok, dets:tab_name()} | error.
new(Dir) ->
    {ok, _} = dets:open_file(?TABLE2, [{file, file_v2(Dir)} | ?DETS_OPTIONS]),
    {ok, _} = dets:open_file(?TABLE3, [{file, file_v3(Dir)}, {keypos, #row_v3.table_version } | ?DETS_OPTIONS]).

%% Useful testing tool
-spec delete_dets(file:name()) ->
    ok | {error, any()}.
delete_dets(FileDir) ->
    _ = dets:close(file_v3(FileDir)),
    _ = file:delete(file_v3(FileDir)).

%%
file_v2(Dir) ->
    filename:join(Dir, [?TABLE2, ".dets"]).

%%
file_v3(Dir) ->
    filename:join(Dir, [?TABLE3, ".dets"]).

%%
-spec insert(BucketType :: binary(), DDL :: term()) -> ok.
insert(BucketType, DDL) when is_binary(BucketType), is_tuple(DDL) ->
    lager:info("DDL DETS Update: ~p, ~p", [BucketType, DDL]),
    DDLVersion = riak_ql_ddl:ddl_record_version(element(1, DDL)),
    Row = #row_v3{
         table = BucketType
        ,table_version = {BucketType, DDLVersion}
        ,ddl = DDL},
    ok = dets:insert(?TABLE3, Row),
    ok = dets:sync(?TABLE3),
    case lists:keyfind(ddl_v1, 1, riak_ql_ddl:convert(v1, DDL)) of
        false -> ok;
        DDLV1 -> ok = insert_v2(BucketType, DDLV1)
    end.

%% insert into the v2 table so that the record is available
insert_v2(BucketType, #ddl_v1{} = DDL) ->
    %% the version is always 1 for the v2 table
    DDLVersion = 1,
    %% put compiling as the compile state in the old table so that
    %% it will always recompile the modules on a downgrade
    CompileState = compiling,
    V2Row = {BucketType, DDLVersion, DDL, self(), CompileState},
    dets:insert(?TABLE2, V2Row),
    log_compile_tab_v2_inserted(BucketType),
    ok = dets:sync(?TABLE2).

%%
-spec get_compiled_ddl_versions(BucketType :: binary()) ->
    riak_ql_component:component_version() | notfound.
get_compiled_ddl_versions(BucketType) when is_binary(BucketType) ->
    MatchRow = #row_v3{
        table = BucketType,
        table_version = '$1' },
    case dets:match(?TABLE3, MatchRow) of
        [] ->
            notfound;
        TableVersions ->
            Versions = lists:map(fun([{_,V}]) -> V end, TableVersions),
            lists:sort(
                fun(A,B) ->
                    riak_ql_ddl:is_version_greater(A,B) == true
                end, Versions)
    end.

%%
-spec get_ddl(BucketType::binary(), Version::riak_ql_ddl:ddl_version()) ->
        {ok, term()} | notfound.
get_ddl(BucketType, Version) when is_binary(BucketType), is_atom(Version) ->
    case dets:lookup(?TABLE3, {BucketType,Version}) of
        [#row_v3{ ddl = DDL }] ->
            {ok, DDL};
        [] ->
            notfound
    end.

%%
-spec get_all_table_names() -> [binary()].
get_all_table_names() ->
    Matches = dets:match(?TABLE3, #row_v3{ table = '$1' }),
    Tables = [Table || [Table] <- Matches],
    lists:usort(Tables).

%%
log_compile_tab_v2_inserted(BucketType) ->
    lager:info("DDL for table ~ts was stored, it can be used in Riak TS 1.4",
    [BucketType]).

%% ===================================================================
%% V2 to V3 compile tab upgrade
%% ===================================================================

%%
populate_v3_table() ->
    [ok = maybe_insert_into_v3(T) || T <- get_all_table_names_v2()],
    ok.

maybe_insert_into_v3(BucketType) ->
    case get_ddl(BucketType, v1) of
        {ok, _} ->
            ok;
        notfound ->
            log_v2_to_v3_ddl_migration(BucketType),
            {ok, DDLV2} = get_ddl_v2(BucketType),
            insert(BucketType, DDLV2)
    end.

log_v2_to_v3_ddl_migration(BucketType) ->
    lager:info("Moving table ~ts from compile tab v2 to v3 as part of upgrade.",
        [BucketType]).

-spec get_all_table_names_v2() -> [binary()].
get_all_table_names_v2() ->
    Matches = dets:match(?TABLE2, {'$1', '_', '_', '_', '_'}),
    Tables = [Table || [Table] <- Matches],
    lists:usort(Tables).

get_ddl_v2(BucketType) when is_binary(BucketType) ->
    case dets:lookup(?TABLE2, BucketType) of
        [{_,_,#ddl_v1{} = DDL,_,_}] ->
            {ok, DDL};
        [] ->
            notfound
    end.

%% ===================================================================
%% EUnit tests
%% ===================================================================

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-define(in_process(TestCode),
    Self = self(),
    Pid = spawn_monitor(
        fun() ->
            _ = riak_kv_compile_tab:delete_dets("."),
            _ = riak_kv_compile_tab:new("."),
            TestCode,
            Self ! test_ok
        end),
    receive
        test_ok -> ok;
        {'DOWN',_,_,Pid,normal} -> ok;
        {'DOWN',_,_,Pid,Error} -> error(Error)
    end
).

insert_test() ->
    ?in_process(
        begin
            DDLV2 = #ddl_v2{local_key = #key_v1{ }, partition_key = #key_v1{ }},
            ok = insert(<<"my_type">>, DDLV2),
            ?assertEqual(
                {ok, DDLV2},
                get_ddl(<<"my_type">>, v2)
            )
        end).

compiled_version_test() ->
    ?in_process(
        begin
            ok = insert(<<"my_type">>, #ddl_v1{local_key = #key_v1{ }}),
            ?assertEqual(
                [v1],
                get_compiled_ddl_versions(<<"my_type">>)
            )
        end).

get_ddl_test() ->
    ?in_process(
        begin
            ok = insert(<<"my_type">>, #ddl_v1{local_key = #key_v1{ }}),
            ?assertEqual(
                {ok, #ddl_v1{local_key = #key_v1{ }}},
                get_ddl(<<"my_type">>, v1)
            )
        end).
-endif.
