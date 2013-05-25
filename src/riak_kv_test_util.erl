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

%% @doc Creates a setup function for tests that need Riak KV stood
%% up in an isolated fashion.
%% @see setup/3
-spec common_setup(TestName::atom() | string()) -> fun().
common_setup(T) when is_atom(T) ->
    common_setup(atom_to_list(T));
common_setup(TestName) ->
    common_setup(TestName, ?SETUPTHUNK).

-spec common_setup(atom() | string(), SetupFun::fun((load|start|stop) -> any())) -> fun().
common_setup(T, S) when is_atom(T) ->
    common_setup(atom_to_list(T), S);
common_setup(TestName, Setup) ->
    fun() -> setup(TestName, Setup) end.

%% @doc Creates a cleanup function for tests that need Riak KV stood up in
%% an isolated fashion.
%% @see cleanup/3
-spec common_cleanup(TestName::atom() | string()) -> fun().
common_cleanup(T) when is_atom(T) ->
    common_cleanup(atom_to_list(T));
common_cleanup(TestName) ->
    common_cleanup(TestName, ?SETUPTHUNK).

-spec common_cleanup(TestName::atom() | string(), CleanupFun::fun((stop) -> any())) -> fun().
common_cleanup(T, C) when is_atom(T) ->
    common_cleanup(atom_to_list(T), C);
common_cleanup(TestName, Cleanup) ->
    fun(X) -> cleanup(TestName, Cleanup, X) end.

%% @doc Calls gen_fsm functions that might not have been touched by a
%% test
-spec call_unused_fsm_funs(module()) -> any().
call_unused_fsm_funs(Mod) ->
    Mod:handle_event(event, statename, state),
    Mod:handle_sync_event(event, from, stateneame, state),
    Mod:handle_info(info, statename, statedata),
    Mod:terminate(reason, statename, state),
    Mod:code_change(oldvsn, statename, state, extra).

%% @doc Stop a running pid - unlink and exit(kill) the process
stop_process(undefined) ->
    ok;
stop_process(RegName) when is_atom(RegName) ->
    stop_process(whereis(RegName));
stop_process(Pid) when is_pid(Pid) ->
    unlink(Pid),
    exit(Pid, shutdown),
    ok = wait_for_pid(Pid).

%% @doc Wait for a pid to exit
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

%% @doc Performs generic, riak_kv-specific and test-specific setup
%% when used within a test fixture. This includes cleaning up any
%% leaky state from previous tests (internally calling `cleanup/3'),
%% loading dependent applications, starting distributed Erlang,
%% starting dependent applications, and waiting for riak_kv to become
%% available.
%%
%% The given `SetupFun' will be called first with the argument `stop'
%% before other applications are stopped (to cleanup leaky test
%% state), `load' after all other applications are loaded, and then
%% `start' after all other applications are started. It is generally
%% good practice to use the same function in the `SetupFun' as the
%% `CleanupFun' given to `cleanup/3'.
%%
%% @see common_setup/2, dep_apps/2, do_dep_apps/2
-spec setup(TestName::string(), fun((load|start|stop) -> any())) -> ok.
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

%% @doc Performs generic, riak_kv-specific and test-specific cleanup
%% when used within a test fixture. This includes stopping dependent
%% applications, stopping distributed Erlang, and killing pernicious
%% processes. The given `CleanupFun' will be called with the argument
%% `stop' before other components are stopped.
%%
%% @see common_cleanup/2, dep_apps/2, do_dep_apps/2
-spec cleanup(Test::string(), CleanupFun::fun((stop) -> any()), SetupResult::setup | atom()) -> ok.
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
    ok.

%% @doc Calculates a list of dependent applications and functions that
%% can be passed to do_deps_apps/2 to perform the lifecycle phase on
%% them all at once. This ensures that applications start and stop in
%% the correct order and the test also has a chance to inject its own
%% setup and teardown code. Included in the sequence are two default
%% setup functions, one that silences SASL logging and redirects it to
%% a file, and one that configures some settings for riak_core and
%% lager.
%%
%% By passing the `Test' argument, the test's data and logging state
%% is also isolated to its own directory so as not to clobber other
%% tests.
%%
%% The `Extra' function takes an atom which represents the phase of
%% application lifecycle, one of `load', `start' or `stop'.
%%
%% @see common_setup/2, common_cleanup/2
-spec dep_apps(Test::string(), Extra::fun((load | start | stop) -> any())) -> [ atom() | fun() ].
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
                application:set_env(riak_core, handoff_port, 0), %% pick a random handoff port
                application:set_env(lager, handlers, [{lager_file_backend,
                                                       [
                                                        {Test ++ "/log/debug.log", debug, 10485760, "$D0", 5}]}]),
                application:set_env(lager, crash_log, Test ++ "/log/crash.log");
           (stop) -> ok;
           (_) -> ok
        end,

    [sasl, Silencer, crypto, public_key, ssl, riak_sysmon, os_mon,
     runtime_tools, erlang_js, inets, mochiweb, webmachine, sidejob,
     basho_stats, bitcask, compiler, syntax_tools, lager, folsom,
     riak_core, riak_pipe, riak_api, riak_kv, DefaultSetupFun, Extra].


%% @doc Runs the application-lifecycle phase across all of the given
%% applications and functions.
%% @see dep_apps/2
-spec do_dep_apps(load | start | stop, [ atom() | fun() ]) -> [ any() ].
do_dep_apps(StartStop, Apps) ->
    lists:map(fun(A) when is_atom(A) ->
                      case include_app_phase(StartStop, A) of
                          true -> application:StartStop(A);
                          _ -> ok
                      end;
                 (F)                 -> F(StartStop)
              end, Apps).

%% @doc Determines whether a given application should be modified in
%% the given phase. If this returns false, the application will not be
%% loaded, started, or stopped by `do_dep_apps/2'.
-spec include_app_phase(Phase::load | start | stop, Application::atom()) -> true | false.
include_app_phase(stop, crypto) -> false;
include_app_phase(start, folsom) -> false;
include_app_phase(_Phase, _App) -> true.


-endif. % TEST
