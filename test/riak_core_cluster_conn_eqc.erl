%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved.
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
%% ---------------------------------------------------------------------

%% @doc Quickcheck test module for `riak_core_cluster_conn'.

-module(riak_core_cluster_conn_eqc).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_fsm.hrl").
-include_lib("eunit/include/eunit.hrl").

%% eqc properties
-export([prop_cluster_conn_state_transition/0]).

%% States
-export([connecting/1,
         waiting_for_cluster_name/1,
         waiting_for_cluster_members/1,
         connected/1]).

%% eqc_fsm callbacks
-export([initial_state/0,
         initial_state_data/0,
         next_state_data/5,
         precondition/4,
         postcondition/5]).

%% Helpers
-export([test/0,
         test/1]).

-compile(export_all).

-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) ->
                              io:format(user, Str, Args) end, P)).

-define(TEST_ITERATIONS, 500).
-define(TEST_MODULE, riak_core_cluster_conn).

-define(P(EXPR), PPP = (EXPR), case PPP of true -> ok; _ -> io:format(user, "PPP ~p at line ~p\n", [PPP, ?LINE]) end, PPP).

-record(ccst_state, {current_state :: atom(),
                     previous_state :: atom()}).

%%====================================================================
%% Eunit tests
%%====================================================================

eqc_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     {spawn,
      [
       {timeout, 60, ?_assertEqual(true, eqc:quickcheck(eqc:testing_time(30, ?QC_OUT(prop_cluster_conn_state_transition()))))}
      ]
     }}.

setup() ->
    riak_repl_test_util:start_lager().

cleanup(Apps) ->
    riak_repl_test_util:stop_apps(Apps).

%% ====================================================================
%% EQC Properties
%% ====================================================================

%% This property is designed to exercise the state transitions of
%% `riak_core_cluster_conn'. The goal is to ensure that the reception
%% of expected and unexpected events results in the expected state
%% transition behavior. It is not intended to verify any of the side
%% effects that may occur in any particular state and those are
%% avoided by starting the fsm in `test' mode.
prop_cluster_conn_state_transition() ->
    ?FORALL(Cmds,
            commands(?MODULE),
            begin
                {ok, Pid} = ?TEST_MODULE:start_link("FARFARAWAY", test),
                {H, {_F, _S}, Res} =
                    run_commands(?MODULE, Cmds, [{fsm_pid, Pid}]),
                aggregate(zip(state_names(H), command_names(Cmds)),
                          ?WHENFAIL(
                             begin
                                 ?debugFmt("\nCmds: ~p~n",
                                           [zip(state_names(H),
                                                command_names(Cmds))]),
                                 ?debugFmt("\nResult: ~p~n", [Res]),
                                 ?debugFmt("\nHistory: ~p~n", [H])
                             end,
                             equals(ok, Res)))
            end).

%%====================================================================
%% eqc_fsm callbacks
%%====================================================================

initiating_connection(_S) ->
    [
     {history, {call, ?MODULE, poll_cluster, [{var, fsm_pid}]}},
     {history, {call, ?MODULE, garbage_event, [{var, fsm_pid}]}},
     {history, {call, ?TEST_MODULE, status, [{var, fsm_pid}]}},
     {connecting, {call, ?MODULE, send_timeout, [{var, fsm_pid}]}}
    ].

connecting(_S) ->
    [
     {history, {call, ?MODULE, poll_cluster, [{var, fsm_pid}]}},
     {history, {call, ?MODULE, garbage_event, [{var, fsm_pid}]}},
     {history, {call, ?TEST_MODULE, status, [{var, fsm_pid}]}},
     {waiting_for_cluster_name, {call, ?MODULE, connected_to_remote, [{var, fsm_pid}]}}
    ].

waiting_for_cluster_name(_S) ->
    [
     {history, {call, ?MODULE, poll_cluster, [{var, fsm_pid}]}},
     {history, {call, ?MODULE, garbage_event, [{var, fsm_pid}]}},
     {history, {call, ?TEST_MODULE, status, [{var, fsm_pid}]}},
     {waiting_for_cluster_members, {call, ?MODULE, cluster_name, [{var, fsm_pid}]}}
    ].

waiting_for_cluster_members(_S) ->
    [
     {history, {call, ?MODULE, poll_cluster, [{var, fsm_pid}]}},
     {history, {call, ?MODULE, garbage_event, [{var, fsm_pid}]}},
     {history, {call, ?TEST_MODULE, status, [{var, fsm_pid}]}},
     {connected, {call, ?MODULE, cluster_members, [{var, fsm_pid}]}}
    ].

connected(_S) ->
    [
     {history, {call, ?MODULE, garbage_event, [{var, fsm_pid}]}},
     {history, {call, ?TEST_MODULE, status, [{var, fsm_pid}]}},
     {waiting_for_cluster_name, {call, ?MODULE, poll_cluster, [{var, fsm_pid}]}},
     {stopped, {call, ?TEST_MODULE, stop, [{var, fsm_pid}]}}
    ].

stopped(_S) ->
    [].

initial_state() ->
    initiating_connection.

initial_state_data() ->
    #ccst_state{}.

next_state_data(_From, _To, S, _R, _C) ->
    S.

precondition(_From, _To, _S, _C) ->
    true.

postcondition(initiating_connection, connecting, _S, {call, ?MODULE, send_timeout, [Pid]}, _R) ->
    ?P(?MODULE:current_fsm_state(Pid) =:= connecting);
postcondition(connected, connected, _S ,{call, ?MODULE, status, _}, R) ->
    ExpectedStatus = {fake_socket,
                      ranch_tcp,
                      "overtherainbow",
                      [{clustername, "FARFARAWAY"}],
                      {1,0}},
    {_, status, Status} = R,
    ?P(Status =:= ExpectedStatus);
postcondition(State, State, _S ,{call, ?MODULE, status, [Pid]}, R) ->
    ?P(R =:= State andalso ?MODULE:current_fsm_state(Pid) =:= State);
postcondition(State, State, _S ,{call, ?MODULE, garbage_event, [Pid]}, _R) ->
    ?P(?MODULE:current_fsm_state(Pid) =:= State);
postcondition(connected, waiting_for_cluster_name, _S, {call, ?MODULE, poll_cluster, [Pid]}, _R) ->
    ?P(?MODULE:current_fsm_state(Pid) =:= waiting_for_cluster_name);
postcondition(State, State, _S, {call, ?MODULE, poll_cluster, [Pid]}, _R) ->
    ?P(?MODULE:current_fsm_state(Pid) =:= State);
postcondition(connecting, waiting_for_cluster_name, _S, {call, ?MODULE, connected_to_remote, [Pid]}, _R) ->
    ?P(?MODULE:current_fsm_state(Pid) =:= waiting_for_cluster_name);
postcondition(waiting_for_cluster_name, waiting_for_cluster_members, _S, {call, ?MODULE, cluster_name, [Pid]}, _R) ->
    ?P(?MODULE:current_fsm_state(Pid) =:= waiting_for_cluster_members);
postcondition(waiting_for_cluster_members, connected, _S, {call, ?MODULE, cluster_members, [Pid]}, _R) ->
    ?P(?MODULE:current_fsm_state(Pid) =:= connected);
postcondition(connected, stopped, _S ,{call, ?TEST_MODULE, stop, [Pid]}, _R) ->
    ?P(is_process_alive(Pid) =:= false);
%% Catch all
postcondition(_From, _To, _S , _C, _R) ->
    true.

%%====================================================================
%% Helpers
%%====================================================================

test() ->
    test(500).

test(Iterations) ->
    eqc:quickcheck(eqc:numtests(Iterations, prop_cluster_conn_state_transition())).

current_fsm_state(Pid) ->
    {CurrentState, _} = ?TEST_MODULE:current_state(Pid),
    CurrentState.

send_timeout(Pid) ->
    gen_fsm:send_event(Pid, timeout).

poll_cluster(Pid) ->
    gen_fsm:send_event(Pid, poll_cluster).

garbage_event(Pid) ->
    gen_fsm:send_event(Pid, slartibartfast).

connected_to_remote(Pid) ->
    Event = {connected_to_remote,
             fake_socket,
             ranch_tcp,
             "overtherainbow",
             [{clustername, "FARFARAWAY"}],
             {1,0}},
    gen_fsm:send_event(Pid, Event).

cluster_name(Pid) ->
    Event = {cluster_name, "FARFARAWAY"},
    gen_fsm:send_event(Pid, Event).

cluster_members(Pid) ->
    Event = {cluster_members,
             [{"fake-address-1", 1},
              {"fake-address-2", 2}
             ]},
    gen_fsm:send_event(Pid, Event).

-endif.
