%% -------------------------------------------------------------------
%%
%% riak_kv_ts_newtype 
%%
%% Copyright (c) 2015, 2016 Basho Technologies, Inc.  All Rights Reserved.
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
-export([start_link/0]).
-export([new_type/1]).
-export([retrieve_ddl_from_metadata/1]).

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

-type accepted_ddl_versions() :: #ddl_v1{} | #ddl_v2{}.

%%%
%%% API.
%%%

-spec start_link() -> {ok, pid()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec new_type(binary()) -> ok.
new_type(BucketType) ->
    gen_server:cast(?MODULE, {new_type, BucketType}).

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

handle_info({'EXIT', Pid, normal}, State) ->
    % success
    _ = riak_kv_compile_tab:update_state(Pid, compiled),
    {noreply, State};
handle_info({'EXIT', _, bucket_type_changed_mid_compile}, State) ->
    % this means that the process was interrupted while compiling by an update
    % to the metadata
    {noreply, State};
handle_info({'EXIT', Pid, _Error}, State) ->
    % compilation error, check
    _ = riak_kv_compile_tab:update_state(Pid, failed),
    {noreply, State};
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

-spec do_new_type(BucketType::binary()) -> ok.
%% We rely on the claimant to not give us new DDLs after the bucket
%% type is activated, at least until we have a system in place for
%% managing DDL versioning
do_new_type(BucketType) ->
    maybe_compile_ddl(
      BucketType,
      riak_ql_ddl:upgrade(
        retrieve_ddl_from_metadata(BucketType)),
      riak_kv_compile_tab:get_ddl(BucketType)).

-spec maybe_compile_ddl(BucketType::binary(),
                        accepted_ddl_versions(), accepted_ddl_versions()) ->
                               ok.
maybe_compile_ddl(_BucketType, NewDDL, NewDDL) ->
    %% Do nothing; we're seeing a CMD update but the DDL hasn't changed
    ok;
maybe_compile_ddl(BucketType, NewDDL, _OldDDL)
  when is_record(NewDDL, ddl_v1);
       %% ddl versions 1 and 2 are ok
       is_record(NewDDL, ddl_v2) ->
    ok = maybe_stop_current_compilation(BucketType),
    ok = start_compilation(BucketType, NewDDL);
maybe_compile_ddl(_BucketType, _NewDDL, _OldDDL) ->
    %% We don't know what to do with this new DDL, so stop
    ok.

-spec maybe_stop_current_compilation(BucketType::binary()) -> ok.
%%
maybe_stop_current_compilation(BucketType) ->
    case riak_kv_compile_tab:is_compiling(BucketType) of
        {true, CompilerPid} ->
            ok = stop_current_compilation(CompilerPid);
        false ->
            ok
    end.

-spec stop_current_compilation(pid()) -> ok.
%%
stop_current_compilation(CompilerPid) ->
    case is_process_alive(CompilerPid) of
        true ->
            exit(CompilerPid, bucket_type_changed_mid_compile),
            ok = flush_exit_message(CompilerPid);
        false ->
            ok
    end.

-spec flush_exit_message(pid()) -> ok.
%%
flush_exit_message(CompilerPid) ->
    receive
        {'EXIT', CompilerPid, _} -> ok
    after
        1000 -> ok
    end.

-spec start_compilation(binary(), accepted_ddl_versions()) -> ok.
%%
start_compilation(BucketType, DDL) ->
    Pid = proc_lib:spawn_link(
        fun() ->
            ok = compile_and_store(ddl_ebin_directory(), DDL)
        end),
    ok = riak_kv_compile_tab:insert(BucketType, DDL, Pid, compiling).

-spec compile_and_store(string(), accepted_ddl_versions()) -> ok.
%%
compile_and_store(BeamDir, DDL) ->
    case riak_ql_ddl_compiler:compile(DDL) of
        {error, _} = E ->
            E;
        {_, AST} ->
            {ok, ModuleName, Bin} = compile:forms(AST),
            ok = store_module(BeamDir, ModuleName, Bin)
    end.

-spec store_module(Dir::string(), module(), binary()) -> ok.
%%
store_module(Dir, Module, Bin) ->
    Filepath = beam_file_path(Dir, Module),
    ok = filelib:ensure_dir(Filepath),
    ok = file:write_file(Filepath, Bin).

-spec beam_file_path(Dir::string(), module()) -> file:name_all().
%%
beam_file_path(BeamDir, Module) ->
    filename:join(BeamDir, [Module, ".beam"]).

%% Return the directory where compiled DDL module beams are stored
%% before bucket type activation.
ddl_ebin_directory() ->
   DataDir = app_helper:get_env(riak_core, platform_data_dir),
   filename:join(DataDir, ddl_ebin).

%% Would be nice to have a function in riak_core_bucket_type or
%% similar to get either the prefix or the actual metadata instead
%% of including a riak_core header file for this prefix
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
