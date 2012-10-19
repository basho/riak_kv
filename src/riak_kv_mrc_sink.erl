%% -------------------------------------------------------------------
%%
%% riak_kv_mrc_sink: A simple process to act as a Pipe sink for
%% MapReduce queries
%%
%% Copyright (c) 2012 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc This FSM acts as a Riak Pipe sink, and dumbly accumulates
%% messages received from the pipe, until it is asked to send them to
%% its owner. The owner is whatever process started this FSM.

%% Messages are delivered to the owners as an erlang message that is a
%% `#kv_mrc_pipe{}' record. The `logs' field is a list of log messages
%% received, ordered oldest to youngest, each having the form
%% `{PhaseId, Message}'. The `results' field is an orddict keyed by
%% `PhaseId', with each value being a list of results received from
%% that phase, ordered oldest to youngest. The `ref' field is the
%% reference from the `#pipe{}' record. The `done' field is `true' if
%% the `eoi' message has been received, or `false' otherwise.

%% There should be three states: `which_pipe', `collect_output', and
%% `send_output'.

%% The FSM starts in `which_pipe', and waits there until it
%% is told which pipe to expect output from.

%% From `which_pipe', the FSM moves to `collect_output'. While in
%% `collect_output', the FSM simply collects `#pipe_log{}',
%% `#pipe_result{}', and `#pipe_eoi{}' messages.

%% If the FSM has received logs, results, or the eoi before it
%% receives a `next' event, it sends everything it has accumulated to
%% the owner, wrapped in a `#kv_mrc_sink{}' record, clears its buffers,
%% and returns to collecting pipe messages.

%% If the FSM has not received any logs, results, or the eoi before it
%% receives a `next' event, it enters the `send_ouput' state. As soon
%% as the FSM receives any log, result, or eoi message in the
%% `send_output' state, it sends that message to the owner process,
%% and then returns to the `collect_output' state.

%% The FSM only exits on its own in three cases. The first is when its
%% owner exits. The second is when the builder of the pipe for which
%% it is consuming messages exits abnormally. The third is after it
%% delivers the a `#kv_mrc_sink{}' in which it has marked
%% `done=true'.
-module(riak_kv_mrc_sink).

-export([
         start/1,
         start_link/1,
         use_pipe/2,
         next/1,
         stop/1,
         init/1,
         which_pipe/2, which_pipe/3,
         collect_output/2, collect_output/3,
         send_output/2, send_output/3,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4
        ]).

-behaviour(gen_fsm).

-include_lib("riak_pipe/include/riak_pipe.hrl").
-include("riak_kv_mrc_sink.hrl").

-record(state, {
          owner :: pid(),
          builder :: pid(),
          ref :: reference(),
          results=[] :: [{PhaseId::term(), Results::list()}],
          logs=[] :: list(),
          done=false :: boolean()
         }).

start(OwnerPid) ->
    riak_kv_mrc_sink_sup:start_sink(OwnerPid).

start_link(OwnerPid) ->
    gen_fsm:start_link(?MODULE, [OwnerPid], []).

use_pipe(Sink, Pipe) ->
    gen_fsm:sync_send_event(Sink, {use_pipe, Pipe}).

%% @doc Trigger the send of the next result/log/eoi batch received.
next(Sink) ->
    gen_fsm:send_event(Sink, next).

stop(Sink) ->
    riak_kv_mrc_sink_sup:terminate_sink(Sink).

%% gen_fsm exports

init([OwnerPid]) ->
    erlang:monitor(process, OwnerPid),
    {ok, which_pipe, #state{owner=OwnerPid}}.

%%% which_pipe: waiting to find out what pipe we're listening to

which_pipe(_, State) ->
    {next_state, which_pipe, State}.

which_pipe({use_pipe, #pipe{builder=Builder, sink=Sink}}, _From, State) ->
    erlang:monitor(process, Builder),
    {reply, ok, collect_output,
     State#state{builder=Builder, ref=Sink#fitting.ref}};
which_pipe(_, _, State) ->
    {next_state, which_pipe, State}.

%%% collect_output: buffering results and logs until asked for them

collect_output(next, State) ->
    case State#state.done of
        true ->
            NewState = send_to_owner(State),
            {stop, normal, NewState};
        false ->
            case has_output(State) of
                true ->
                    NewState = send_to_owner(State),
                    {next_state, collect_output, NewState};
                false ->
                    %% nothing to send yet, prepare to send as soon as
                    %% there is something
                    {next_state, send_output, State}
            end
    end;
collect_output(_, State) ->
    {next_state, collect_output, State}.
collect_output(_, _, State) ->
    {next_state, collect_output, State}.

%% send_output: waiting for output to send, after having been asked
%% for some while there wasn't any

%% all work for send_output is done in handle_info
send_output(_, State) ->
    {next_state, send_output, State}.
send_output(_, _, State) ->
    {next_state, send_output, State}.

handle_event(_, StateName, State) ->
    {next_state, StateName, State}.

handle_sync_event(_, _, StateName, State) ->
    {next_state, StateName, State}.

handle_info(#pipe_result{ref=Ref, from=PhaseId, result=Res},
            StateName, #state{ref=Ref, results=Acc}=State) ->
    NewAcc = add_result(PhaseId, Res, Acc),
    info_response(StateName, State#state{results=NewAcc});
handle_info(#pipe_log{ref=Ref, from=PhaseId, msg=Msg},
            StateName, #state{ref=Ref, logs=Acc}=State) ->
    info_response(StateName, State#state{logs=[{PhaseId, Msg}|Acc]});
handle_info(#pipe_eoi{ref=Ref},
            StateName, #state{ref=Ref}=State) ->
    info_response(StateName, State#state{done=true});
handle_info({'DOWN', _, process, Pid, Reason}, _,
            #state{owner=Pid}=State) ->
    %% exit as soon as the owner dies
    {stop, Reason, State};
handle_info({'DOWN', _, process, Pid, Reason}, _,
            #state{builder=Pid}=State) when Reason /= normal ->
    %% don't stop when the builder exits 'normal', because that's
    %% probably just the pipe shutting down normally - wait for the
    %% owner to ask for the last outputs
    {stop, Reason, State};
handle_info(_, StateName, State) ->
    {next_state, StateName, State}.

%% continue buffering, unless we've been waiting to reply; stop if we
%% were waiting to reply and we've received eoi
info_response(collect_output, State) ->
    {next_state, collect_output, State};
info_response(send_output, #state{done=Done}=State) ->
    NewState = send_to_owner(State),
    if Done -> {stop, normal, NewState};
       true -> {next_state, collect_output, NewState}
    end.

terminate(_, _, _) ->
    ok.

code_change(_, StateName, State, _) ->
    {ok, StateName, State}.

%% internal

has_output(#state{results=[], logs=[]}) ->
    false;
has_output(_) ->
    true.

%% also clears buffers
send_to_owner(#state{owner=Owner, ref=Ref,
                     results=Results, logs=Logs, done=Done}=State) ->
    Owner ! #kv_mrc_sink{ref=Ref,
                         results=finish_results(Results),
                         logs=lists:reverse(Logs),
                         done=Done},
    State#state{results=[], logs=[]}.

%% results are kept as lists in a proplist
add_result(PhaseId, Result, Acc) ->
    case lists:keytake(PhaseId, 1, Acc) of
        {value, {PhaseId, IAcc}, RAcc} ->
            [{PhaseId,[Result|IAcc]}|RAcc];
        false ->
            [{PhaseId,[Result]}|Acc]
    end.

%% transform the proplist buffers into orddicts time-ordered
finish_results(Results) ->
    [{I, lists:reverse(R)} || {I, R} <- lists:keysort(1, Results)].
