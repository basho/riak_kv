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
         wait_for_children/1]).
-include_lib("eunit/include/eunit.hrl").


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
            {error, didnotexit}
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

-endif. % TEST
