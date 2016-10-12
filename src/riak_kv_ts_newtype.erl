%% -------------------------------------------------------------------
%%
%% riak_kv_ts_newtype
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

-module(riak_kv_ts_newtype).
-behaviour(gen_server).

%% API.
-export([
         is_compiled/2,
         new_type/1,
         start_link/0,
         recompile_ddl/0,
         retrieve_ddl_from_metadata/1,
         verify_helper_modules/0]).

%% gen_server.
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).

-record(state, {
}).

-define(BUCKET_TYPE_PREFIX, {core, bucket_types}).

-include_lib("riak_ql/include/riak_ql_ddl.hrl").

%%%
%%% API.
%%%

-spec start_link() -> {ok, pid()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec new_type(binary()) -> ok.
new_type(BucketType) ->
    lager:info("Add new Time Series bucket type ~s", [BucketType]),
    gen_server:cast(?MODULE, {new_type, BucketType}).

%%
-spec is_compiled(binary(), riak_ql_ddl:any_ddl()) -> boolean().
is_compiled(BucketType, DDL) when is_binary(BucketType) ->
    DDLVersion = riak_ql_ddl:ddl_record_version(element(1,DDL)),
    %% if an operator changed his/her mind and submitted another definition for
    %% the DDL shortly after the first, it’s theoretically possible the first
    %% would be compiled on some and the second on others
    %%
    %% even if we're running an upgraded version of the DDL, the DDL argument
    %% is from the metadata and will still be the old version so find the
    %% original that was stored and compare that
    (beam_exists(BucketType) andalso
        riak_kv_compile_tab:get_ddl(BucketType, DDLVersion) == {ok, DDL}).

%%%
%%% gen_server.
%%%

init([]) ->
    process_flag(trap_exit, true),

    % do this outside of init so that we get crash messages output to crash.log
    % if it fails
    self() ! add_ddl_ebin_to_path,
    {ok, #state{}}.

handle_call(_Request, _From, State) ->
    {reply, ignored, State}.

handle_cast({new_type, BucketType}, State) ->
    ok = do_new_type(BucketType),
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(add_ddl_ebin_to_path, State) ->
    ok = riak_core_metadata_manager:swap_notification_handler(
        ?BUCKET_TYPE_PREFIX, riak_kv_metadata_store_listener, []),
    ok = add_ddl_ebin_to_path(),
    {noreply, State};
handle_info(_, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%
%%% Internal.
%%%

%% We rely on the claimant to not give us new DDLs after the bucket
%% type is activated, at least until we have a system in place for
%% managing DDL versioning
do_new_type(Table) ->
    case retrieve_ddl_from_metadata(Table) of
        undefined ->
            %% this bucket type name does not exist in the metadata!
            log_missing_ddl_metadata(Table);
        DDL ->
            CurrentVersion = riak_ql_ddl:current_version(),
            case riak_kv_compile_tab:get_ddl(Table, CurrentVersion) of
                notfound ->
                    try
                        ok = prepare_ddl_versions(Table, CurrentVersion, DDL),
                        actually_compile_ddl(Table)
                    catch
                        throw:unknown_ddl_version ->
                            log_unknown_ddl_version(Table, DDL),
                            ok
                    end;
                {ok, DDL} ->
                    log_new_type_is_duplicate(Table);
                {ok, _StoredDDL} ->
                    %% there is a different DDL for the same table/version
                    %% FIXME recompile anyway? That is the current behaviour
                    %% this cannot happen because of the is_compiled check
                    ok
            end
    end.

prepare_ddl_versions(Table, CurrentVersion, DDL) when is_binary(Table), is_atom(CurrentVersion) ->
    %% this is a new DDL
    case is_known_ddl_version(CurrentVersion, DDL) of
        false ->
            %% the version is unknown!  another node with a higher version has
            %% somehow not respected the capability and sent a version we
            %% cannot handle
            throw(unknown_ddl_version);
        true ->
            ok
    end,
    log_compiling_new_type(Table),
    %% conversions, if the DDL is greater, then we need to convert
    UpgradedDDLs = riak_ql_ddl:convert(CurrentVersion, DDL),
    % lager:info("DDLs ~p", [UpgradedDDLs]),
    [ok = riak_kv_compile_tab:insert(Table, DDLx) || DDLx <- UpgradedDDLs],
    ok.

%%
is_known_ddl_version(CurrentVersion, DDL) ->
    DDLVersion = riak_ql_ddl:ddl_record_version(element(1, DDL)),
    riak_ql_ddl:is_version_greater(CurrentVersion, DDLVersion) /= false.

%%
log_missing_ddl_metadata(Table) ->
    lager:info("No 'ddl' property in the metata for bucket type ~ts",
        [Table]).

%%
log_compiling_new_type(Table) ->
    lager:info("Compiling new DDL for bucket type ~ts with version ~p",
               [Table, riak_ql_ddl:current_version()]).

%%
log_new_type_is_duplicate(Table) ->
    lager:info("Not compiling DDL for table ~ts because it is unchanged",
        [Table]).

%%
log_unknown_ddl_version(Table, DDL) ->
    lager:error(
        "Unknown DDL version ~p for bucket type ~ts (current version is ~p)",
        [DDL, Table, riak_ql_ddl:current_version()]).

%%
-spec actually_compile_ddl(BucketType::binary()) -> pid().
actually_compile_ddl(BucketType) ->
    lager:info("Starting DDL compilation of ~ts", [BucketType]),
    Self = self(),
    Ref = make_ref(),
    Pid = proc_lib:spawn_link(
        fun() ->
            try
                TabResult = riak_kv_compile_tab:get_ddl(BucketType, riak_ql_ddl:current_version()),
                {ModuleName, AST} = compile_to_ast(TabResult, BucketType),
                {ok, ModuleName, Bin} = compile:forms(AST),
                ok = store_module(ddl_ebin_directory(), ModuleName, Bin),
                Self ! {compilation_complete, Ref}
            after
                %% this spawned process may crash before lager is properly
                %% started and never write a crash report, so do a cheeky wee
                %% sleep after a success has returned to the newtype gen_server
                %% so only the error case is blocked
                timer:sleep(100)
            end
        end),
    receive
        {compilation_complete, Ref} ->
            lager:info("Compilation of DDL ~ts complete and stored to disk", [BucketType]),
            ok;
        {'EXIT', Pid, normal} ->
            ok;
        {'EXIT', Pid, Error} ->
            lager:error("Error compiling DDL ~ts with error ~p", [BucketType, Error]),
            ok
    after
        30000 ->
            %% really allow a lot of time to complete this, because there is
            %% not much we can do if it fails
            lager:error("timeout on compiling table ~ts", [BucketType]),
            {error, timeout}
    end.

compile_to_ast(TabResult, BucketType) ->
    case TabResult of
        {ok, DDL} ->
            riak_ql_ddl_compiler:compile(DDL);
        _ ->
            lager:info("No DDL for table ~ts, requests on it will not be supported.", [BucketType]),
            riak_ql_ddl_compiler:compile_disabled_module(BucketType)
    end.

%%
store_module(Dir, Module, Bin) ->
    Filepath = beam_file_path(Dir, Module),
    ok = filelib:ensure_dir(Filepath),
    lager:info("STORING BEAM ~p to ~p", [Module, Filepath]),
    ok = file:write_file(Filepath, Bin).

%%
beam_file_path(BeamDir, Module) ->
    filename:join(BeamDir, [Module, ".beam"]).

%% Return the directory where compiled DDL module beams are stored
%% before bucket type activation.
ddl_ebin_directory() ->
   DataDir = app_helper:get_env(riak_core, platform_data_dir),
   filename:join(DataDir, ddl_ebin).

%% The returned DDL may not be the current DDL record, it could be
%% an old one.
retrieve_ddl_from_metadata(BucketType) when is_binary(BucketType) ->
    retrieve_ddl_2(riak_core_metadata:get(?BUCKET_TYPE_PREFIX, BucketType,
                                          [{allow_put, false}])).

%%
retrieve_ddl_2(undefined) ->
    undefined;
retrieve_ddl_2(Props) ->
    proplists:get_value(ddl, Props, undefined).

%%
add_ddl_ebin_to_path() ->
    Ebin_Path = ddl_ebin_directory(),
    ok = filelib:ensure_dir(filename:join(Ebin_Path, any)),
    % the code module ensures that there are no duplicates
    true = code:add_path(Ebin_Path),
    ok.

%% For each table
%%     Find the most recent version
%%     If the version is the most current one then continue
%%     Else upgrade it to the current version
%%     If the version is higher than the current version then continue (being downgraded, might not be able to check higher cos atoms)
recompile_ddl() ->
    Tables = riak_kv_compile_tab:get_all_table_names(),
    CurrentVersion = riak_ql_ddl:current_version(),
    [upgrade_ddl(T, CurrentVersion) || T <- Tables],
    ok.

%%
upgrade_ddl(Table, CurrentVersion) ->
    [HighestVersion|_] = riak_kv_compile_tab:get_compiled_ddl_versions(Table),
    case riak_ql_ddl:is_version_greater(CurrentVersion, HighestVersion) of
        equal ->
            %% the table is up to date, no need to upgrade
            ok;
        true ->
            %% upgrade, current version is greater than our persisted
            %% known versions
            {ok, DDL} = riak_kv_compile_tab:get_ddl(Table, HighestVersion),
            DDLs = riak_ql_ddl:convert(CurrentVersion, DDL),
            log_ddl_upgrade(DDL, DDLs),
            [ok = riak_kv_compile_tab:insert(Table, DDLx) || DDLx <- DDLs],
            DowngradedDDLs = riak_ql_ddl:convert(riak_ql_ddl:first_version(), DDL),
            [ok = insert_downgraded_ddl(Table, DDLx) || DDLx <- DowngradedDDLs],
            ok;
        false ->
            %% downgrade, the current version is lower than the latest
            %% persisted version, we have to hope there is a downgraded
            %% ddl in the compile tab
            ok
    end.

insert_downgraded_ddl(BucketType, {error, {cannot_downgrade, Version}}) ->
    log_cannot_downgrade_to_version(BucketType, Version);
insert_downgraded_ddl(BucketType, DDL) ->
    DDLVersion = riak_ql_ddl:ddl_record_version(element(1, DDL)),
    case riak_kv_compile_tab:get_ddl(BucketType, DDLVersion) of
        {ok,_} ->
            %% the DDL already exists, so don't overwrite it
            ok;
        notfound ->
            log_storing_downgraded_ddl(BucketType, DDLVersion),
            riak_kv_compile_tab:insert(BucketType, DDL)
    end.

%%
log_storing_downgraded_ddl(BucketType, DDLVersion) when is_atom(DDLVersion) ->
    lager:info("A DDL for table ~ts for DDL version ~p was stored, for use "
               "in the event of a downgrade", [BucketType, DDLVersion]).

%%
log_cannot_downgrade_to_version(BucketType, Version) ->
    lager:warn("Cannot downgrade table ~ts to version ~p, "
               "table will be disablded under this version",
                [BucketType, Version]).

%%
log_ddl_upgrade(DDL, DDLs) ->
    lager:info("UPGRADING ~p WITH ~p", [DDL, DDLs]).

%% For each table
%%     build the table name
%%     check if the beam is there for that module
%%     if not then build it
verify_helper_modules() ->
    [verify_helper_module(T) || T <- riak_kv_compile_tab:get_all_table_names()],
    ok.

verify_helper_module(Table) when is_binary(Table) ->
    case beam_exists(Table) of
        true ->
            lager:info("beam file for table ~ts exists", [Table]),
            ok;
        false ->
            lager:info("beam file for table ~ts must be recompiled", [Table]),
            actually_compile_ddl(Table)
    end.

%%
beam_exists(Table) when is_binary(Table) ->
    BeamDir = ddl_ebin_directory(),
    ModuleName = riak_ql_ddl:make_module_name(Table),
    filelib:is_file(beam_file_path(BeamDir, ModuleName)).


%% ===================================================================
%% EUnit tests
%% ===================================================================

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

is_known_ddl_version_v2_test() ->
    ?assertEqual(
        true,
        is_known_ddl_version(v2, #ddl_v2{})
    ).

is_known_ddl_version_v1_test() ->
    ?assertEqual(
        true,
        is_known_ddl_version(v2, #ddl_v1{})
    ).

-endif.
