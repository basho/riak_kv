%% -------------------------------------------------------------------
%%
%% riak_kv_ts_compiler: Manages the DDL->AST->beam compilation
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

-module(riak_kv_ts_compiler).

-export([compile/2]).

%%
compile(DDL, BeamDir) ->
    case riak_ql_ddl_compiler:make_helper_mod(DDL) of
        {module, AST} ->
            {ok, Module, Bin} = compile:forms(AST),
            Filepath = beam_file_path(BeamDir, Module),
            ok = filelib:ensure_dir(Filepath),
            ok = file:write_file(Filepath, Bin),
            success;
        {error, _} = E ->
            E
    end.

%%
beam_file_path(BeamDir, Module) ->
    Filename = atom_to_list(Module) ++ ".beam",
    filename:join(BeamDir, Filename).