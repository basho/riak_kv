%% -------------------------------------------------------------------
%%
%% ts_watch_eqc: Test DDL management
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

-module(ts_watch_eqc).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_component.hrl").

-compile(export_all).

-define(MDPREFIX, {core, bucket_types}).

-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).

%% -- State ------------------------------------------------------------------
-record(state,{
          fsm                  :: 'undefined'|pid(),
          bucket_type          :: 'undefined'|binary(),
          type_definition      :: 'undefined'|list(tuple()),

          %% The `next_state' value as represented in the model state
          %% has a very special purpose. It does *NOT* reflect the
          %% state as returned by the code being tested; instead, it
          %% is hard-coded to reflect the state we *THINK* the test
          %% will return so that during the test generation phase of
          %% EQC statem, we can use `weight/2' to intelligent derive
          %% one of a small set of new commands that are valid for
          %% this state.
          next_state           :: atom()
          %% TL;DR for next_state: DO NOT USE for postconditions, and
          %% for `_next' functions set this as an atom, not as a
          %% variable binding provided by EQC
         }).

-define(STATE_EDGES,
        [
         {waiting, [waiting_waiting, waiting_compiling, waiting_stop]},
         {compiling, [compiling_stop, compiling_compiled]},
         {compiled, [compiled_compiled, compiled_compiling, compiled_stop]},
         {stop, []}
        ]).

%% Dynamically determine weights from ?STATE_EDGES. Any valid state
%% transition gets 1, anything else 0.
weight(#state{bucket_type=undefined}, init) ->
    1;
weight(#state{bucket_type=undefined}, _Cmd) ->
    0;
weight(#state{next_state=FromState}, Cmd) ->
    case lists:member(Cmd, proplists:get_value(FromState, ?STATE_EDGES)) of
        true ->
            1;
        false ->
            0
    end.

%% @doc Returns the state in which each test case starts. (Unless a different
%%      initial state is supplied explicitly to, e.g. commands/2.)
-spec initial_state() -> eqc_statem:symbolic_state().
initial_state() ->
    #state{}.

%% -- Common pre-/post-conditions --------------------------------------------
%% @doc General command filter, checked before a command is generated.
%% init must be the first command, and can only be run once
-spec command_precondition_common(S, Cmd) -> boolean()
    when S    :: eqc_statem:symbolic_state(),
         Cmd  :: atom().
command_precondition_common(#state{next_state=undefined}, Cmd) ->
    Cmd == init;
%% This is redundant with `weight/2' but necessary for shrinking
command_precondition_common(#state{next_state=StateName}, Cmd) ->
    lists:member(Cmd, proplists:get_value(StateName, ?STATE_EDGES)).

%% -- Operations -------------------------------------------------------------

%% --- Operation: init ---
%% Inserting a new bucket type definition and launching the FSM is the
%% beginning of the process, both for Riak and for this test.

%% @doc init_args - Argument generator
-spec init_args(S :: eqc_statem:symbolic_state()) -> eqc_gen:gen([term()]).
init_args(_S) ->
    [?MDPREFIX, non_empty(binary())].

%% @doc init - The actual operation
init(?MDPREFIX, Type) ->
    %% We can't launch the FSM until we have a type and this will only
    %% be invoked as the first operation
    {ok, Pid} =
        riak_kv_ts_watch_fsm:start(riak_kv_ts_compiler,
                                   Type,
                                   self(),
                                   "beam-dir-does-not-matter",
                                   %% Long timeouts so they don't
                                   %% fire and mess up our state
                                   [{metadata_retry, 100000},
                                    {update_retry, 100000},
                                    {transition_timeout, 100000}]),
    Pid.

%% @doc init_next - Next state function
-spec init_next(S, Var, Args) -> NewS
    when S    :: eqc_statem:symbolic_state() | eqc_state:dynamic_state(),
         Var  :: eqc_statem:var() | term(),
         Args :: [term()],
         NewS :: eqc_statem:symbolic_state() | eqc_state:dynamic_state().
init_next(S, Pid, [_MDPrefix, Type]) ->
    S#state{bucket_type=Type, fsm=Pid, next_state=waiting}.



%% --- Operation: waiting_waiting ---

%% @doc waiting_waiting_args - Argument generator
-spec waiting_waiting_args(S :: eqc_statem:symbolic_state()) -> eqc_gen:gen([term()]).
waiting_waiting_args(#state{fsm=FSM}) ->
    [FSM, undefined].

%% @doc waiting_waiting - The actual operation
waiting_waiting(FSM, undefined) ->
    gen_fsm:sync_send_event(FSM, timeout).

%% @doc waiting_waiting_callouts - Callouts for waiting_waiting
-spec waiting_waiting_callouts(S, Args) -> eqc_gen:gen(eqc_component:callout())
    when S    :: eqc_statem:symbolic_state(),
         Args :: [term()].
waiting_waiting_callouts(#state{bucket_type=Type}, [_FSM, undefined]) ->
    ?SEQ([?CALLOUT(riak_core_metadata, get,
             [{core, bucket_types}, Type],
             undefined),
          ?CALLOUT(
             riak_core_metadata, get,
             [{core, bucket_types}, Type],
             undefined)]).

%% @doc waiting_waiting_post - Postcondition for waiting_waiting
-spec waiting_waiting_post(S, Args, Res) -> true | term()
    when S    :: eqc_state:dynamic_state(),
         Args :: [term()],
         Res  :: term().
waiting_waiting_post(_S, [_FSM, undefined], Next) ->
    Next == waiting.


%% --- Operation: waiting_compiling ---

%% @doc waiting_compiling_args - Argument generator
-spec waiting_compiling_args(S :: eqc_statem:symbolic_state()) -> eqc_gen:gen([term()]).
waiting_compiling_args(#state{fsm=FSM}) ->
    [FSM, [{a, b},{ddl, "code"}]].

%% @doc waiting_compiling - The actual operation
waiting_compiling(FSM, _Metadata) ->
    gen_fsm:sync_send_event(FSM, timeout).

%% @doc waiting_compiling_callouts - Callouts for waiting_compiling
-spec waiting_compiling_callouts(S, Args) -> eqc_gen:gen(eqc_component:callout())
    when S    :: eqc_statem:symbolic_state(),
         Args :: [term()].
waiting_compiling_callouts(#state{bucket_type=Type}, [_FSM, Metadata]) ->
    ?SEQ([?CALLOUT(riak_core_metadata, get,
             [{core, bucket_types}, Type],
             Metadata),
          ?CALLOUT(
             riak_core_metadata, get,
             [{core, bucket_types}, Type],
             Metadata)
         ]).

%% @doc waiting_compiling_next - Next state function
-spec waiting_compiling_next(S, Var, Args) -> NewS
    when S    :: eqc_statem:symbolic_state() | eqc_state:dynamic_state(),
         Var  :: eqc_statem:var() | term(),
         Args :: [term()],
         NewS :: eqc_statem:symbolic_state() | eqc_state:dynamic_state().
waiting_compiling_next(S, _NextStateName, [_FSM, Metadata]) ->
    S#state{next_state=compiling, type_definition=Metadata}.

%% @doc waiting_compiling_post - Postcondition for waiting_compiling
-spec waiting_compiling_post(S, Args, Res) -> true | term()
    when S    :: eqc_state:dynamic_state(),
         Args :: [term()],
         Res  :: term().
waiting_compiling_post(_S, [_FSM, _Metadata], Next) ->
    Next == compiling.


%% --- Operation: compiling_compiled ---
%% @doc compiling_compiled_args - Argument generator
-spec compiling_compiled_args(S :: eqc_statem:symbolic_state()) -> eqc_gen:gen([term()]).
compiling_compiled_args(#state{fsm=FSM}) ->
    [FSM].

%% @doc compiling_compiled - The actual operation
compiling_compiled(FSM) ->
    gen_fsm:sync_send_event(FSM, timeout).

%% @doc compiling_compiled_callouts - Callouts for compiling_compiled
-spec compiling_compiled_callouts(S, Args) -> eqc_gen:gen(eqc_component:callout())
    when S    :: eqc_statem:symbolic_state(),
         Args :: [term()].
compiling_compiled_callouts(
  #state{bucket_type=Type}, [_FSM]) ->
    ?SEQ([?CALLOUT(riak_kv_ts_compiler, compile,
                   ["code", Type, "beam-dir-does-not-matter"],
                   success)]).

%% @doc compiling_compiled_next - Next state function
-spec compiling_compiled_next(S, Var, Args) -> NewS
    when S    :: eqc_statem:symbolic_state() | eqc_state:dynamic_state(),
         Var  :: eqc_statem:var() | term(),
         Args :: [term()],
         NewS :: eqc_statem:symbolic_state() | eqc_state:dynamic_state().
compiling_compiled_next(S, _NextStateName, [_FSM]) ->
    S#state{next_state=compiled}.

%% @doc compiling_compiled_post - Postcondition for compiling_compiled
-spec compiling_compiled_post(S, Args, Res) -> true | term()
    when S    :: eqc_state:dynamic_state(),
         Args :: [term()],
         Res  :: term().
compiling_compiled_post(_S, [_FSM], Next) ->
    Next == compiled.



%% %% --- Operation: compiling_stop ---
%% %% XXX: until we sort through EQC and stopped FSMs
%% compiling_stop_pre(_) ->
%%     false.

%% %% @doc compiling_stop_args - Argument generator
%% -spec compiling_stop_args(S :: eqc_statem:symbolic_state()) -> eqc_gen:gen([term()]).
%% compiling_stop_args(_S) ->
%%     gen_fsm:sync_send_event(FSM, timeout).

%% %% @doc compiling_stop - The actual operation
%% compiling_stop() ->
%%     ok.

%% %% @doc compiling_stop_callouts - Callouts for compiling_stop
%% -spec compiling_stop_callouts(S, Args) -> eqc_gen:gen(eqc_component:callout())
%%     when S    :: eqc_statem:symbolic_state(),
%%          Args :: [term()].
%% compiling_stop_callouts(#state{bucket_type=Type}, [_FSM, Metadata]) ->
%%     ?SEQ([?CALLOUT(riak_kv_ts_compiler, compile,
%%                    [DDL, Type,
%%                     "beam-dir-does-not-matter"],
%%                    {fail, Reason})]).

%% %% @doc compiling_stop_post - Postcondition for compiling_stop
%% -spec compiling_stop_post(S, Args, Res) -> true | term()
%%     when S    :: eqc_state:dynamic_state(),
%%          Args :: [term()],
%%          Res  :: term().
%% compiling_stop_post(_S, [_FSM, _Metadata], Next) ->
%%     Next == stop.

%% %% @doc compiling_stop_next - Next state function
%% -spec compiling_stop_next(S, Var, Args) -> NewS
%%     when S    :: eqc_statem:symbolic_state() | eqc_state:dynamic_state(),
%%          Var  :: eqc_statem:var() | term(),
%%          Args :: [term()],
%%          NewS :: eqc_statem:symbolic_state() | eqc_state:dynamic_state().
%% compiling_stop_next(S, NextStateName, [_FSM, Metadata]) ->
%%     S#state{next_state=stop}.

%% --- Operation: waiting_stop ---

%% XXX For now, don't test this. Need to figure out how not to have
%% EQC throw a fit when the FSM shuts down.
waiting_stop_pre(_) ->
    false.

%% @doc waiting_stop_args - Argument generator
-spec waiting_stop_args(S :: eqc_statem:symbolic_state()) -> eqc_gen:gen([term()]).
waiting_stop_args(#state{fsm=FSM}) ->
    [FSM, [{a, b},{ddl, "code"},{active, true}]].

%% @doc waiting_stop - The actual operation
waiting_stop(FSM, _Metadata) ->
    gen_fsm:sync_send_event(FSM, timeout).

%% @doc waiting_stop_callouts - Callouts for waiting_stop
-spec waiting_stop_callouts(S, Args) -> eqc_gen:gen(eqc_component:callout())
    when S    :: eqc_statem:symbolic_state(),
         Args :: [term()].
waiting_stop_callouts(#state{bucket_type=Type}, [_FSM, Metadata]) ->
    ?SEQ([?CALLOUT(riak_core_metadata, get,
             [{core, bucket_types}, Type],
             Metadata)]).
          %% ?CALLOUT(
          %%    riak_core_metadata, get,
          %%    [{core, bucket_types}, Type],
          %%    Metadata)]).

%% @doc waiting_stop_next - Next state function
-spec waiting_stop_next(S, Var, Args) -> NewS
    when S    :: eqc_statem:symbolic_state() | eqc_state:dynamic_state(),
         Var  :: eqc_statem:var() | term(),
         Args :: [term()],
         NewS :: eqc_statem:symbolic_state() | eqc_state:dynamic_state().
waiting_stop_next(S, _NextStateName, [_FSM, Metadata]) ->
    S#state{next_state=stop, type_definition=Metadata}.

%% @doc waiting_stop_post - Postcondition for waiting_stop
-spec waiting_stop_post(S, Args, Res) -> true | term()
    when S    :: eqc_state:dynamic_state(),
         Args :: [term()],
         Res  :: term().
waiting_stop_post(_S, [_FSM, _Metadata], Next) ->
    Next == stop.


%% @doc Default generated property
-spec prop_ts_watch_eqc() -> eqc:property().
prop_ts_watch_eqc() ->
    ?SETUP(fun() ->
                   %% setup mocking here
                   eqc_mocking:start_mocking(api_spec()),
                   fun() -> ok end %% Teardown function
           end,
  ?FORALL(Cmds, commands(?MODULE),
  begin
      {H, S, Res} = run_commands(?MODULE,Cmds),
      %% XXX: I believe this to be entirely safe, but...
      kill_fsm(S#state.fsm),
      pretty_commands(?MODULE, Cmds, {H, S, Res},
                      measure(length, length(Cmds),
                              aggregate(command_names(Cmds),
                                        Res == ok)))
  end)).

kill_fsm(undefined) ->
    true;
kill_fsm(Pid) ->
    unlink(Pid),
    exit(Pid, kill).

%% -- API-spec ---------------------------------------------------------------
%% @doc API specification for mocked components
-spec api_spec() -> #api_spec{}.
api_spec() ->
    #api_spec {
       language = erlang,
       mocking = eqc_mocking,
       modules =
           [
            #api_module {
               name = riak_core_metadata,
               functions =
                   [
                    #api_fun{ name = get, arity = 2 }
                   ]
              },
            #api_module {
               name = riak_kv_ts_compiler,
               functions =
                   [
                    #api_fun{ name = compile, arity = 3 }
                   ]
              }
           ]
      }.
