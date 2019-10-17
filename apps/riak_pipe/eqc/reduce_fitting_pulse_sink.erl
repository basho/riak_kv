%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc Support module for reduce_fitting_pulse tests - implements a
%% pipe fsm-style sink.
-module(reduce_fitting_pulse_sink).

-compile(export_all).
%% compiler gets angry if behavior functions are not exported explicitly
-export([init/1,handle_event/3,handle_sync_event/4,terminate/3,
         handle_info/3,code_change/4]).

-ifdef(PULSE).
-include_lib("pulse/include/pulse.hrl").
%% have to transform the 'receive' of the work results
-compile({parse_transform, pulse_instrument}).
%% don't trasnform toplevel test functions
-compile({pulse_replace_module,[{gen_fsm,pulse_gen_fsm}]}).
-endif.

-include("include/riak_pipe.hrl").

-behaviour(gen_fsm).

-record(state, {monitor, owner, ref, builder, results=[], logs=[], waiter}).

start_link(Owner, Ref) ->
    gen_fsm:start_link(?MODULE, [Owner, Ref], []).

use_pipe(Sink, Ref, Pipe) ->
    gen_fsm:sync_send_event(Sink, {use_pipe, Ref, Pipe}, infinity).

all_results(Sink, Ref) ->
    gen_fsm:sync_send_event(Sink, {all_results, Ref}, infinity).

init([Owner, Ref]) ->
    Monitor = erlang:monitor(process, Owner),
    {ok,
     wait_pipe,
     #state{monitor=Monitor, owner=Owner, ref=Ref}}.

wait_pipe({use_pipe, Ref, #pipe{builder=Builder}},
          _From,
          #state{ref=Ref}=State) ->
    Monitor = erlang:monitor(process, Builder),
    {reply,
     ok,
     accumulating,
     State#state{builder={Monitor, Builder}}}.

accumulating(#pipe_result{ref=Ref}=R,
             #state{ref=Ref, results=Results}=State) ->
    {next_state, accumulating, State#state{results=[R|Results]}};
accumulating(#pipe_log{ref=Ref}=L,
             #state{ref=Ref, logs=Logs}=State) ->
    {next_state, accumulating, State#state{logs=[L|Logs]}};
accumulating(#pipe_eoi{ref=Ref},
             #state{ref=Ref, waiter=Waiter}=State) ->
    case Waiter of
        undefined ->
            {next_state, waiting, State};
        _ ->
            gen_fsm:reply(Waiter, State#state.results),
            {stop, normal, State}
    end.

accumulating({all_results, Ref}, From, #state{ref=Ref}=State) ->
    {next_state, accumulating, State#state{waiter=From}};
accumulating(Other, _From, State) ->
    case accumulating(Other, State) of
        {next_state, NextStateName, NextState} ->
            {reply, ok, NextStateName, NextState};
        {stop, Reason, State} ->
            {stop, Reason, ok, State};
        Response ->
            Response
    end.

waiting({all_results, Ref},
        _From,
        #state{ref=Ref, results=Results}=State) ->
    {stop, normal, Results, State}.

handle_info({'DOWN', Monitor, process, Owner, _Reason},
            _StateName,
            #state{monitor=Monitor, owner=Owner}=State) ->
    {stop, normal, State};
handle_info({'DOWN', Monitor, process, Builder, Reason},
            StateName,
            #state{builder={Monitor, Builder}}=State) ->
    case Reason of
        normal ->
            %% mimicking riak_kv_mrc_sink for the moment
            {next_state, StateName, State};
        _ ->
            %% also mimicking riak_kv_mrc_sink
            {stop, normal, State}
    end.

handle_event(_, _, State) ->
    {stop, unknown_message, State}.

handle_sync_event(_, _, _, State) ->
    {stop, unknown_message, State}.

code_change(_Old, StateName, State, _Extra) ->
    {ok, StateName, State}.

terminate(_Reason, _StateName, _State) ->
    ok.
