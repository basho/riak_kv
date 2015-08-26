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
          state_name=waiting   :: atom(),
          bucket_type          :: binary(),
          type_definition      :: 'undefined'|list(tuple())
         }).

%% @doc Returns the state in which each test case starts. (Unless a different
%%      initial state is supplied explicitly to, e.g. commands/2.)
-spec initial_state() -> eqc_statem:symbolic_state().
initial_state() ->
    #state{}.

%% -- Common pre-/post-conditions --------------------------------------------
%% @doc General command filter, checked before a command is generated.
%% insert_type must be the first command, and can only be run once
-spec command_precondition_common(S, Cmd) -> boolean()
    when S    :: eqc_statem:symbolic_state(),
         Cmd  :: atom().
command_precondition_common(#state{bucket_type=undefined}, insert_type) ->
    true;
command_precondition_common(_S, insert_type) ->
    false;
command_precondition_common(#state{bucket_type=undefined}, _Cmd) ->
    false;
command_precondition_common(_S, _Cmd) ->
    true.

%% @doc General precondition, applied *before* specialized preconditions.
-spec precondition_common(S, Call) -> boolean()
    when S    :: eqc_statem:symbolic_state(),
         Call :: eqc_statem:call().
precondition_common(_S, _Call) ->
    true.

%% @doc General postcondition, applied *after* specialized postconditions.
%% -spec postcondition_common(S, Call, Res) -> true | term()
%%     when S    :: eqc_statem:dynamic_state(),
%%          Call :: eqc_statem:call(),
%%          Res  :: term().
%% postcondition_common(S, Call, Res) ->
%%     eq(Res, return_value(S, Call)). %% Check all return values

%% -- Operations -------------------------------------------------------------

%% --- Operation: insert_type ---
%% Inserting a new bucket type definition is the beginning of the
%% process, both for Riak and for this test.

%% @doc insert_type_pre/1 - Precondition for generation
-spec insert_type_pre(S :: eqc_statem:symbolic_state()) -> boolean().
insert_type_pre(#state{type_definition=Def}) ->
    Def == undefined.

%% @doc insert_type_args - Argument generator
-spec insert_type_args(S :: eqc_statem:symbolic_state()) -> eqc_gen:gen([term()]).
insert_type_args(_S) ->
    [?MDPREFIX, non_empty(binary())].

%% @doc insert_type_pre/2 - Precondition for insert_type
-spec insert_type_pre(S, Args) -> boolean()
    when S    :: eqc_statem:symbolic_state(),
         Args :: [term()].
insert_type_pre(_S, _Args) ->
    true.

%% @doc insert_type - The actual operation
insert_type(?MDPREFIX, Type) ->
    %% We can't launch the FSM until we have a type and this will only
    %% be invoked as the first operation
    {ok, Pid} =
        riak_kv_ts_watch_fsm:start_link(riak_kv_ts_compiler,
                                        Type,
                                        self(),
                                        "beam-dir-does-not-matter",
                                        %% Long timeouts so they don't
                                        %% fire and mess up our state
                                        [{metadata_retry, 1200000},
                                         {update_retry, 1200000},
                                         {compile_wait, 1200000}]),
    Pid.

%% @doc insert_type_callouts - Callouts for insert_type
-spec insert_type_callouts(S, Args) -> eqc_gen:gen(eqc_component:callout())
    when S    :: eqc_statem:symbolic_state(),
         Args :: [term()].
insert_type_callouts(_S, _Args) ->
    ?EMPTY.

%% @doc insert_type_next - Next state function
-spec insert_type_next(S, Var, Args) -> NewS
    when S    :: eqc_statem:symbolic_state() | eqc_state:dynamic_state(),
         Var  :: eqc_statem:var() | term(),
         Args :: [term()],
         NewS :: eqc_statem:symbolic_state() | eqc_state:dynamic_state().
insert_type_next(S, Pid, [_MDPrefix, Type]) ->
    S#state{bucket_type=Type, fsm=Pid}.

%% @doc insert_type_post - Postcondition for insert_type
-spec insert_type_post(S, Args, Res) -> true | term()
    when S    :: eqc_state:dynamic_state(),
         Args :: [term()],
         Res  :: term().
insert_type_post(_S, _Args, _Res) ->
    true.

%% %% @doc insert_type_return - Return value for insert_type
%% -spec insert_type_return(S, Args) -> term()
%%     when S    :: eqc_statem:symbolic_state(),
%%          Args :: [term()].
%% insert_type_return(#state{fsm=Pid}, _Args) ->
%%     {ok, _}.

%% --- ... more operations

%% --- Operation: wait ---
%% @doc wait_pre/1 - Precondition for generation
-spec wait_pre(S :: eqc_statem:symbolic_state()) -> boolean().
wait_pre(_S) ->
    true.

%% @doc wait_args - Argument generator

%% There are 4 possible cluster metadata states that the FSM could awake to:
%%   1) No bucket type definition has propagated to this node yet
%%   2) Bucket type definition exists, can proceed
%%   3) Bucket type definition exists, but no DDL attached
%%   4) Bucket type definition exists, but already active
%%
%% 3 and 4 don't make much sense, but are conceivable with the right
%% user actions and/or software bugs; we need to respond appropriately
%% However, testing those conditions can wait for now.
-spec wait_args(S :: eqc_statem:symbolic_state()) -> eqc_gen:gen([term()]).
wait_args(#state{fsm=FSM}) ->
    [FSM, elements([undefined, [{a, b},{ddl, "code"}]])].

%% @doc wait_pre/2 - Precondition for wait
-spec wait_pre(S, Args) -> boolean()
    when S    :: eqc_statem:symbolic_state(),
         Args :: [term()].
wait_pre(#state{state_name=FSMState}, [_FSM, _MD]) ->
    FSMState == waiting.

%% @doc wait - The actual operation
wait(FSM, _Metadata) ->
    gen_fsm:sync_send_event(FSM, timeout).


%% @doc wait_callers - Which modules are allowed to call this operation. Default: [anyone]
%% -spec wait_callers() -> [atom()].
%% wait_callers() ->
%%   [anyone].
%%

%% @doc wait_callouts - Callouts for wait
-spec wait_callouts(S, Args) -> eqc_gen:gen(eqc_component:callout())
    when S    :: eqc_statem:symbolic_state(),
         Args :: [term()].
wait_callouts(#state{bucket_type=Type}, [_FSM, undefined]) ->
    ?SEQ([?CALLOUT(riak_core_metadata, get,
             [{core, bucket_types}, Type],
             undefined),
          ?CALLOUT(
             riak_core_metadata, get,
             [{core, bucket_types}, Type],
             undefined)]);
wait_callouts(#state{bucket_type=Type}, [FSM, Metadata]) ->
    ?SEQ([?CALLOUT(riak_core_metadata, get,
             [{core, bucket_types}, Type],
             Metadata),
          ?CALLOUT(
             riak_core_metadata, get,
             [{core, bucket_types}, Type],
             Metadata),
          ?CALLOUT(
             riak_kv_ts_compiler, compile,
             [FSM, "code", Type, "beam-dir-does-not-matter"],
             spawn(fun() -> timer:sleep(10000) end)
            )
         ]).

%% @doc wait_next - Next state function
-spec wait_next(S, Var, Args) -> NewS
    when S    :: eqc_statem:symbolic_state() | eqc_state:dynamic_state(),
         Var  :: eqc_statem:var() | term(),
         Args :: [term()],
         NewS :: eqc_statem:symbolic_state() | eqc_state:dynamic_state().
%% stopping will be a viable result once we start testing
%% prematurely-activated bucket types
wait_next(S, Var, [_FSM, Metadata]) ->
    S#state{state_name=Var,type_definition=Metadata}.

%% @doc wait_post - Postcondition for wait
-spec wait_post(S, Args, Res) -> true | term()
    when S    :: eqc_state:dynamic_state(),
         Args :: [term()],
         Res  :: term().
wait_post(_S, [_FSM, undefined], Next) ->
    Next == waiting;
wait_post(_S, [_FSM, Metadata], Next) ->
    case {proplists:get_value(ddl, Metadata), Next} of
        {undefined, waiting} ->
            true;
        {_Code, compiling} ->
            true;
        _ ->
            false
    end.

%% @doc wait_return - Return value for wait
%% -spec wait_return(S, Args) -> term()
%%     when S    :: eqc_statem:symbolic_state(),
%%          Args :: [term()].
%% wait_return(_S, [_FSM, _Metadata]) ->
%%     ok.

%% @doc wait_blocking - Is the operation blocking in this State
%% -spec wait_blocking(S, Args) -> boolean()
%%     when S    :: eqc_statem:symbolic_state(),
%%          Args :: [term()].
%% wait_blocking(_S, [_FSM]) ->
%%   false.

%% @doc wait_features - Collects a list of features of this call with these arguments.
%% -spec wait_features(S, Args, Res) -> list(any())
%%     when S    :: eqc_statem:dynmic_state(),
%%          Args :: [term()],
%%          Res  :: term().
%% wait_features(_S, [_FSM], _Res) ->
%%   [].

%% @doc wait_adapt - How to adapt a call in this State
%% -spec wait_adapt(S, Args) -> boolean()
%%     when S    :: eqc_statem:symbolic_state(),
%%          Args :: [term()].
%% wait_adapt(_S, [_FSM]) ->
%%   false.

%% @doc wait_dynamicpre - Dynamic precondition for wait
%% -spec wait_dynamicpre(S, Args) -> boolean()
%%     when S    :: eqc_statem:symbolic_state(),
%%          Args :: [term()].
%% wait_dynamicpre(_S, [_FSM]) ->
%%   true.

%% --- Operation: compiling ---
%% @doc compiling_pre/1 - Precondition for generation
-spec compiling_pre(S :: eqc_statem:symbolic_state()) -> boolean().
compiling_pre(_S) ->
    true.

%% @doc compiling_args - Argument generator
-spec compiling_args(S :: eqc_statem:symbolic_state()) -> eqc_gen:gen([term()]).
compiling_args(_S) ->
    [].

%% @doc compiling_pre/2 - Precondition for compiling
-spec compiling_pre(S, Args) -> boolean()
    when S    :: eqc_statem:symbolic_state(),
         Args :: [term()].
compiling_pre(_S, []) ->
    true.

%% @doc compiling - The actual operation
compiling() ->
    ok.

%% @doc compiling_callers - Which modules are allowed to call this operation. Default: [anyone]
%% -spec compiling_callers() -> [atom()].
%% compiling_callers() ->
%%   [anyone].
%%

%% @doc compiling_callouts - Callouts for compiling
-spec compiling_callouts(S, Args) -> eqc_gen:gen(eqc_component:callout())
    when S    :: eqc_statem:symbolic_state(),
         Args :: [term()].
compiling_callouts(_S, []) ->
    ?EMPTY.

%% @doc compiling_next - Next state function
-spec compiling_next(S, Var, Args) -> NewS
    when S    :: eqc_statem:symbolic_state() | eqc_state:dynamic_state(),
         Var  :: eqc_statem:var() | term(),
         Args :: [term()],
         NewS :: eqc_statem:symbolic_state() | eqc_state:dynamic_state().
compiling_next(S, _Value, []) ->
    S.

%% @doc compiling_post - Postcondition for compiling
-spec compiling_post(S, Args, Res) -> true | term()
    when S    :: eqc_state:dynamic_state(),
         Args :: [term()],
         Res  :: term().
compiling_post(_S, [], _Res) ->
    true.

%% @doc compiling_return - Return value for compiling
-spec compiling_return(S, Args) -> term()
    when S    :: eqc_statem:symbolic_state(),
         Args :: [term()].
compiling_return(_S, []) ->
    ok.

%% @doc compiling_blocking - Is the operation blocking in this State
%% -spec compiling_blocking(S, Args) -> boolean()
%%     when S    :: eqc_statem:symbolic_state(),
%%          Args :: [term()].
%% compiling_blocking(_S, []) ->
%%   false.

%% @doc compiling_features - Collects a list of features of this call with these arguments.
%% -spec compiling_features(S, Args, Res) -> list(any())
%%     when S    :: eqc_statem:dynmic_state(),
%%          Args :: [term()],
%%          Res  :: term().
%% compiling_features(_S, [], _Res) ->
%%   [].

%% @doc compiling_adapt - How to adapt a call in this State
%% -spec compiling_adapt(S, Args) -> boolean()
%%     when S    :: eqc_statem:symbolic_state(),
%%          Args :: [term()].
%% compiling_adapt(_S, []) ->
%%   false.

%% @doc compiling_dynamicpre - Dynamic precondition for compiling
%% -spec compiling_dynamicpre(S, Args) -> boolean()
%%     when S    :: eqc_statem:symbolic_state(),
%%          Args :: [term()].
%% compiling_dynamicpre(_S, []) ->
%%   true.




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
      pretty_commands(?MODULE, Cmds, {H, S, Res},
                      measure(length, length(Cmds),
                              aggregate(command_names(Cmds),
                                        Res == ok)))
  end)).

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
                    #api_fun{ name = compile, arity = 4 }
                   ]
              }
           ]
      }.


%% Sample sequence to experiment with mocking
lang() ->
    ?SEQ([?EVENT(riak_core_metadata, get,
                 [{core, bucket_types}, <<"type">>],
                 [{ddl, "some code"}]),
          ?EVENT(riak_core_metadata, get,
                 [{core, bucket_types}, <<"type">>],
                 [{ddl, "some code"},{active, true}])
         ]).
