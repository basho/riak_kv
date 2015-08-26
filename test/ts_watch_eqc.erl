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
-record(state,{state_name=waiting :: atom(),
               bucket_type :: binary(),
               type_definition :: undefined|list(tuple())
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
-spec postcondition_common(S, Call, Res) -> true | term()
    when S    :: eqc_statem:dynamic_state(),
         Call :: eqc_statem:call(),
         Res  :: term().
postcondition_common(S, Call, Res) ->
    eq(Res, return_value(S, Call)). %% Check all return values

%% -- Operations -------------------------------------------------------------

%% --- Operation: insert_type ---
%% @doc insert_type_pre/1 - Precondition for generation
-spec insert_type_pre(S :: eqc_statem:symbolic_state()) -> boolean().
insert_type_pre(#state{type_definition=Def}) ->
    Def == undefined.

%% @doc insert_type_args - Argument generator
-spec insert_type_args(S :: eqc_statem:symbolic_state()) -> eqc_gen:gen([term()]).
insert_type_args(_S) ->
    %% We need two types of type definitions: with and without a ddl
    %% tuple. There should be at least one other tuple to distinguish
    %% from an unset type definition.
    [?MDPREFIX, non_empty(binary()),
     elements([[{a, b}], [{a, b},{ddl, "code"}]])].

%% @doc insert_type_pre/2 - Precondition for insert_type
-spec insert_type_pre(S, Args) -> boolean()
    when S    :: eqc_statem:symbolic_state(),
         Args :: [term()].
insert_type_pre(_S, _Args) ->
    true.

%% @doc insert_type - The actual operation
insert_type(?MDPREFIX, Type, _Def) ->
    %% We can't launch the FSM until we have a type
    riak_kv_ts_watch_fsm:start_link(riak_kv_ts_compiler,
                                    Type,
                                    self(),
                                    "beam-dir-does-not-matter",
                                    [{metadata_retry, 120000},
                                     {update_retry, 120000},
                                     {compile_wait, 120000}]),
    ok.

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
insert_type_next(S, _Var, [_MDPrefix, Type, Def]) ->
    S#state{type_definition=Def, bucket_type=Type}.

%% @doc insert_type_post - Postcondition for insert_type
-spec insert_type_post(S, Args, Res) -> true | term()
    when S    :: eqc_state:dynamic_state(),
         Args :: [term()],
         Res  :: term().
insert_type_post(_S, _Args, _Res) ->
    true.

%% @doc insert_type_return - Return value for insert_type
-spec insert_type_return(S, Args) -> term()
    when S    :: eqc_statem:symbolic_state(),
         Args :: [term()].
insert_type_return(_S, _Args) ->
    ok.

%% --- ... more operations

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
