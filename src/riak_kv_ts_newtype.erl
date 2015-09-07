%% -------------------------------------------------------------------
%%
%% riak_kv_ts_newtype: 
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

-module(riak_kv_ts_newtype).
-behaviour(gen_server).

%% API.
-export([start_link/0]).
-export([new_type/1]).

%% gen_server.
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).

-record(state, {
}).

-define(new_metadata_appeared,new_metadata_appeared).
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
    gen_server:cast(?MODULE, {new_type, BucketType}).

%%% 
%%% gen_server.
%%%

init([]) ->
    process_flag(trap_exit, true),
    {ok, #state{}}.

handle_call(_Request, _From, State) ->
    {reply, ignored, State}.

handle_cast({new_type, Bucket_type}, State) ->
    ok = do_new_type(Bucket_type),
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'EXIT', _, normal}, State) ->
    % success
    {noreply, State};
handle_info({'EXIT', _, ?new_metadata_appeared}, State) ->
    % this means that the process was interrupted while compiling by an update
    % to the metadata
    {noreply, State};
handle_info({'EXIT', _, _Error}, State) ->
    % compilation error, check
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

%%
do_new_type(Bucket_type) ->
    case retrieve_ddl(Bucket_type) of
        undefined        ->
            ok; % TODO is this ok?
        already_compiled ->
            ok;
        #ddl_v1{} = DDL  ->
            ok = maybe_stop_current_compilation(Bucket_type),
            ok = start_compilation(Bucket_type, DDL)
    end.

%%
maybe_stop_current_compilation(Bucket_type) ->
    case riak_kv_compile_tab:is_compiling(Bucket_type) of
        {true, Compiler_pid} ->
            ok = stop_current_compilation(Compiler_pid);
        false ->
            ok
    end.

%%
stop_current_compilation(Compiler_pid) ->
    case is_process_alive(Compiler_pid) of
        true ->
            exit(Compiler_pid, ?new_metadata_appeared),
            ok = flush_exit_message(Compiler_pid);
        false ->
            ok
    end.

%%
flush_exit_message(Compiler_pid) ->
    receive
        {'EXIT', Compiler_pid, _} -> ok
    after
        1000 -> ok
    end.

%%
start_compilation(Bucket_type, DDL) ->
    Pid = proc_lib:spawn_link(
        fun() ->
            ok = compile_and_store(tmp_beam_directory(), Bucket_type, DDL)
        end),
    ok = riak_kv_compile_tab:insert(Bucket_type, DDL, Pid, compiling).

%%
compile_and_store(BeamDir, Bucket_type, DDL) ->
    case riak_ql_ddl_compiler:compile(DDL) of
        {error, _} = E ->
            E;
        {_, AST} ->
            {ok, Module_name, Bin} = compile:forms(AST),
            ok = store_module(BeamDir, Module_name, Bin),
            ok = put_ddl_module_name_in_metadata(Bucket_type, Module_name)
    end.

%%
store_module(Dir, Module, Bin) ->
    Filepath = beam_file_path(Dir, Module),
    ok = filelib:ensure_dir(Filepath),
    ok = file:write_file(Filepath, Bin).

%%
beam_file_path(BeamDir, Module) ->
    filename:join(BeamDir, [Module, ".beam"]).

%% Return the directory where compiled DDL module beams are stored
%% before bucket type activation.
tmp_beam_directory() ->
   Data_dir = app_helper:get_env(riak_core, platform_data_dir),
   filename:join(Data_dir, ddl_tmp).

%% Would be nice to have a function in riak_core_bucket_type or
%% similar to get either the prefix or the actual metadata instead
%% of including a riak_core header file for this prefix
retrieve_ddl(Bucket_type) ->
    retrieve_ddl_2(riak_core_metadata:get(?BUCKET_TYPE_PREFIX, Bucket_type)).

%%
retrieve_ddl_2(undefined) ->
    undefined;
retrieve_ddl_2(Proplist) ->
    case proplists:is_defined(ddl_module, Proplist) of
        true ->
            already_compiled;
        false ->
            proplists:get_value(ddl, Proplist)
    end.

%%
put_ddl_module_name_in_metadata(Bucket_type, Module_name) ->
    Metadata1 = riak_core_metadata:get(?BUCKET_TYPE_PREFIX, Bucket_type),
    Metadata2 = 
        lists:keystore(ddl_module, 1, Metadata1, {ddl_module, Module_name}),
    ok = riak_core_metadata:put(?BUCKET_TYPE_PREFIX, Bucket_type, Metadata2).