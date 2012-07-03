%% -------------------------------------------------------------------
%%
%% riak_test_util: utilities for test scripts
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc utilities for test scripts

-module(riak_kv_test_util).

-ifdef(TEST).

-export([call_unused_fsm_funs/1,
         stop_process/1,
         wait_for_pid/1,
         wait_for_children/1,
         common_setup/1,
         common_setup/2,
         common_cleanup/1,
         common_cleanup/2]).

-include_lib("eunit/include/eunit.hrl").

-define(SETUPTHUNK, fun(_) -> ok end).

%% Creates a setup function for tests that need Riak KV stood
%% up in an isolated fashion.
common_setup(T) when is_atom(T) ->
    common_setup(atom_to_list(T));
common_setup(TestName) ->
    common_setup(TestName, ?SETUPTHUNK).

common_setup(T, S) when is_atom(T) ->
    common_setup(atom_to_list(T), S);
common_setup(TestName, Setup) ->
    fun() -> setup(TestName, Setup) end.

%% Creates a cleanup function for tests that need Riak KV stood up in
%% an isolated fashion.
common_cleanup(T) when is_atom(T) ->
    common_cleanup(atom_to_list(T));
common_cleanup(TestName) ->
    common_cleanup(TestName, ?SETUPTHUNK).

common_cleanup(T, C) when is_atom(T) ->
    common_cleanup(atom_to_list(T), C);
common_cleanup(TestName, Cleanup) ->
    fun(X) -> cleanup(TestName, Cleanup, X) end.

%% Calls gen_fsm functions that might not have been touched by a test
call_unused_fsm_funs(Mod) ->
    Mod:handle_event(event, statename, state),
    Mod:handle_sync_event(event, from, stateneame, state),
    Mod:handle_info(info, statename, statedata),
    Mod:terminate(reason, statename, state),
    Mod:code_change(oldvsn, statename, state, extra).

%% Stop a running pid - unlink and exit(kill) the process
%%
stop_process(undefined) ->
    ok;
stop_process(RegName) when is_atom(RegName) ->
    stop_process(whereis(RegName));
stop_process(Pid) when is_pid(Pid) ->
    unlink(Pid),
    exit(Pid, shutdown),
    ok = wait_for_pid(Pid).

%% Wait for a pid to exit
wait_for_pid(Pid) ->
    Mref = erlang:monitor(process, Pid),
    receive
        {'DOWN',Mref,process,_,_} ->
            ok
    after
        5000 ->
            {error, didnotexit, Pid, erlang:process_info(Pid)}
    end.

%% Wait for children that were spawned with proc_lib.
%% They have an '$ancestors' entry in their dictionary
wait_for_children(PPid) ->
    F = fun(CPid) ->
                case process_info(CPid, initial_call) of
                    {initial_call, {proc_lib, init_p, 3}} ->
                        case process_info(CPid, dictionary) of
                            {dictionary, Dict} ->
                                case proplists:get_value('$ancestors', Dict) of
                                    undefined ->
                                        %% Process dictionary not updated yet
                                        true;
                                    Ancestors ->
                                        lists:member(PPid, Ancestors)
                                end;
                            undefined ->
                                %% No dictionary - should be one if proclib spawned it
                                true
                        end;
                    _ ->
                        %% Not in proc_lib
                        false
                end
        end,
    case lists:any(F, processes()) of
        true ->
            timer:sleep(1),
            wait_for_children(PPid);
        false ->
            ok
    end.

%% Performs setup for a test
setup(TestName, SetupFun) ->
    %% Cleanup in case a previous test did not
    cleanup(TestName, SetupFun, setup),

    %% Load application environments
    Deps = dep_apps(TestName, SetupFun),
    do_dep_apps(load, Deps),

    %% Start erlang node
    {ok, Hostname} = inet:gethostname(),
    TestNode = list_to_atom(TestName ++ "@" ++ Hostname),
    net_kernel:start([TestNode, longnames]),

    %% Start dependent applications
    do_dep_apps(start, Deps),

    %% Wait for KV to be ready
    riak_core:wait_for_application(riak_kv),
    riak_core:wait_for_service(riak_kv),
    ok.

cleanup(Test, CleanupFun, setup) ->
    %% Remove existing ring files so we have a fresh ring
    os:cmd("rm -rf " ++ Test ++ "/ring"),
    cleanup(Test, CleanupFun, ok);
cleanup(Test, CleanupFun, _) ->
    Deps = lists:reverse(dep_apps(Test, CleanupFun)),

    %% Stop the applications in reverse order
    do_dep_apps(stop, Deps),

    %% Cleanup potentially runaway processes
    catch exit(whereis(riak_kv_vnode_master), kill),
    catch exit(whereis(riak_sysmon_filter), kill),
    catch riak_core_stat_cache:stop(),

    %% Stop distributed Erlang
    net_kernel:stop(),

    %% Reset the riak_core vnode_modules
    application:set_env(riak_core, vnode_modules, []),

    %% Unload the dependent applications
    do_dep_apps(unload, Deps),
    ok.

dep_apps(Test, Extra) ->
    Silencer = fun(load) ->
                       %% Silence logging junk
                       application:set_env(kernel, error_logger, silent),
                       filelib:ensure_dir(Test ++ "/log/sasl.log"),
                       application:set_env(sasl, sasl_error_logger, {file, Test++"/log/sasl.log"}),
                       error_logger:tty(false);
                  (_) -> ok
               end,

    DefaultSetupFun =
        fun(load) ->
                %% Set some missing env vars that are normally part of
                %% release packaging. These can be overridden by the
                %% Extra fun.
                application:set_env(riak_core, ring_creation_size, 64),
                application:set_env(riak_core, ring_state_dir, Test ++ "/ring"),
                application:set_env(riak_core, platform_data_dir, Test ++ "/data"),
                application:set_env(lager, handlers, [{lager_file_backend,
                                                       [
                                                        {Test ++ "/log/debug.log", debug, 10485760, "$D0", 5}]}]),
                application:set_env(lager, crash_log, Test ++ "/log/crash.log");
           (_) -> ok
        end,

    [sasl, Silencer, crypto, public_key, ssl, riak_sysmon, os_mon,
     runtime_tools, erlang_js, inets, mochiweb, webmachine, luke,
     basho_stats, bitcask, compiler, syntax_tools, lager, folsom,
     riak_core, riak_pipe, riak_kv, DefaultSetupFun, Extra].


do_dep_apps(StartStop, Apps) ->
    lists:map(fun(A) when is_atom(A) -> application:StartStop(A);
                 (F)                 -> F(StartStop)
              end, Apps).

-endif. % TEST
