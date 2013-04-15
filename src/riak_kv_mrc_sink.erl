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

%% This FSM will speak both `raw' and `fsm' sink types (it
%% answers appropriately to each, without parameterization).

%% The FSM enforces a soft cap on the number of results and logs
%% accumulated when receiving `fsm' sink type messages. When the
%% number of results+logs that have been delivered exceeds the cap
%% between calls to {@link next/1}, the sink stops delivering result
%% acks to workers. The value of this cap can be specified by
%% including a `buffer' property in the `Options' parameter of {@link
%% start/2}, or by setting the `mrc_sink_buffer' environment variable
%% in the `riak_kv' application. If neither settings is specified, or
%% they are not specified as non-negative integers, the default
%% (currently 1000) is used.

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
         start/2,
         start_link/2,
         use_pipe/2,
         next/1,
         stop/1,
         merge_outputs/1,
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

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include_lib("riak_pipe/include/riak_pipe.hrl").
-include("riak_kv_mrc_sink.hrl").

-define(BUFFER_SIZE_DEFAULT, 1000).

-record(state, {
          owner :: pid(),
          builder :: pid(),
          ref :: reference(),
          results=[] :: [{PhaseId::term(), Results::list()}],
          delayed_acks=[] :: list(),
          logs=[] :: list(),
          done=false :: boolean(),
          buffer_max :: integer(),
          buffer_left :: integer()
         }).

start(OwnerPid, Options) ->
    riak_kv_mrc_sink_sup:start_sink(OwnerPid, Options).

start_link(OwnerPid, Options) ->
    gen_fsm:start_link(?MODULE, [OwnerPid, Options], []).

use_pipe(Sink, Pipe) ->
    gen_fsm:sync_send_event(Sink, {use_pipe, Pipe}).

%% @doc Trigger the send of the next result/log/eoi batch received.
next(Sink) ->
    gen_fsm:send_event(Sink, next).

stop(Sink) ->
    riak_kv_mrc_sink_sup:terminate_sink(Sink).

%% @doc Convenience: If outputs are collected as a list of orddicts,
%% with the first being the most recently received, merge them into
%% one orddict.
%%
%% That is, for one keep, our input should look like:
%%    [ [{0, [G,H,I]}], [{0, [D,E,F]}], [{0, [A,B,C]}] ]
%% And we want it to come out as:
%%    [{0, [A,B,C,D,E,F,G,H,I]}]
-spec merge_outputs([ [{integer(), list()}] ]) -> [{integer(), list()}].
merge_outputs(Acc) ->
    %% each orddict has its outputs in oldest->newest; since we're
    %% iterating from newest->oldest overall, we can just tack the
    %% next list onto the front of the accumulator
    DM = fun(_K, O, A) -> O++A end,
    lists:foldl(fun(O, A) -> orddict:merge(DM, O, A) end, [], Acc).

%% gen_fsm exports

init([OwnerPid, Options]) ->
    erlang:monitor(process, OwnerPid),
    Buffer = buffer_size(Options),
    {ok, which_pipe, #state{owner=OwnerPid,
                            buffer_max=Buffer,
                            buffer_left=Buffer}}.

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
collect_output(#pipe_result{ref=Ref, from=PhaseId, result=Res},
               #state{ref=Ref, results=Acc, buffer_left=Left}=State) ->
    NewAcc = add_result(PhaseId, Res, Acc),
    {next_state, collect_output,
     State#state{results=NewAcc, buffer_left=Left-1}};
collect_output(#pipe_log{ref=Ref, from=PhaseId, msg=Msg},
               #state{ref=Ref, logs=Acc, buffer_left=Left}=State) ->
    {next_state, collect_output,
     State#state{logs=[{PhaseId, Msg}|Acc], buffer_left=Left-1}};
collect_output(#pipe_eoi{ref=Ref}, #state{ref=Ref}=State) ->
    {next_state, collect_output, State#state{done=true}};
collect_output(_, State) ->
    {next_state, collect_output, State}.

collect_output(#pipe_result{ref=Ref, from=PhaseId, result=Res},
               From,
               #state{ref=Ref, results=Acc}=State) ->
    NewAcc = add_result(PhaseId, Res, Acc),
    maybe_ack(From, State#state{results=NewAcc});
collect_output(#pipe_log{ref=Ref, from=PhaseId, msg=Msg},
               From,
               #state{ref=Ref, logs=Acc}=State) ->
    maybe_ack(From, State#state{logs=[{PhaseId, Msg}|Acc]});
collect_output(#pipe_eoi{ref=Ref}, _From, #state{ref=Ref}=State) ->
    {reply, ok, collect_output, State#state{done=true}};
collect_output(_, _, State) ->
    {next_state, collect_output, State}.

maybe_ack(_From, #state{buffer_left=Left}=State) when Left > 0 ->
    %% there's room for more, tell the worker it can continue
    {reply, ok, collect_output, State#state{buffer_left=Left-1}};
maybe_ack(From, #state{buffer_left=Left, delayed_acks=Delayed}=State) ->
    %% there's no more room, hold up the worker
    %% not actually necessary to update buffer_left, but it could make
    %% for interesting stats
    {next_state, collect_output,
     State#state{buffer_left=Left-1, delayed_acks=[From|Delayed]}}.

%% send_output: waiting for output to send, after having been asked
%% for some while there wasn't any

send_output(#pipe_result{ref=Ref, from=PhaseId, result=Res},
            #state{ref=Ref, results=Acc}=State) ->
    NewAcc = add_result(PhaseId, Res, Acc),
    NewState = send_to_owner(State#state{results=NewAcc}),
    {next_state, collect_output, NewState};
send_output(#pipe_log{ref=Ref, from=PhaseId, msg=Msg},
            #state{ref=Ref, logs=Acc}=State) ->
    NewState = send_to_owner(State#state{logs=[{PhaseId, Msg}|Acc]}),
    {next_state, collect_output, NewState};
send_output(#pipe_eoi{ref=Ref}, #state{ref=Ref}=State) ->
    NewState = send_to_owner(State#state{done=true}),
    {stop, normal, NewState};
send_output(_, State) ->
    {next_state, send_output, State}.

send_output(#pipe_result{ref=Ref, from=PhaseId, result=Res},
            _From, #state{ref=Ref, results=Acc}=State) ->
    NewAcc = add_result(PhaseId, Res, Acc),
    NewState = send_to_owner(State#state{results=NewAcc}),
    {reply, ok, collect_output, NewState};
send_output(#pipe_log{ref=Ref, from=PhaseId, msg=Msg},
            _From, #state{ref=Ref, logs=Acc}=State) ->
    NewState = send_to_owner(State#state{logs=[{PhaseId, Msg}|Acc]}),
    {reply, ok, collect_output, NewState};
send_output(#pipe_eoi{ref=Ref}, _From, #state{ref=Ref}=State) ->
    NewState = send_to_owner(State#state{done=true}),
    {stop, normal, ok, NewState};
send_output(_, _, State) ->
    {next_state, send_output, State}.

handle_event(_, StateName, State) ->
    {next_state, StateName, State}.

handle_sync_event(_, _, StateName, State) ->
    {next_state, StateName, State}.

%% Clusters containing nodes running Riak version 1.2 and previous
%% will send raw results, regardless of sink type.  We can't block
%% these worker sending raw results, but we can still track these
%% additions, and block other workers because of them.
handle_info(#pipe_result{ref=Ref, from=PhaseId, result=Res},
            StateName,
            #state{ref=Ref, results=Acc, buffer_left=Left}=State) ->
    NewAcc = add_result(PhaseId, Res, Acc),
    info_response(StateName,
                  State#state{results=NewAcc, buffer_left=Left-1});
handle_info(#pipe_log{ref=Ref, from=PhaseId, msg=Msg},
            StateName,
            #state{ref=Ref, logs=Acc, buffer_left=Left}=State) ->
    info_response(StateName,
                  State#state{logs=[{PhaseId, Msg}|Acc],
                              buffer_left=Left-1});
handle_info(#pipe_eoi{ref=Ref},
            StateName, #state{ref=Ref}=State) ->
    info_response(StateName, State#state{done=true});
handle_info({'DOWN', _, process, Pid, _Reason}, _,
            #state{owner=Pid}=State) ->
    %% exit as soon as the owner dies
    {stop, normal, State};
handle_info({'DOWN', _, process, Pid, Reason}, _,
            #state{builder=Pid}=State) when Reason /= normal ->
    %% don't stop when the builder exits 'normal', because that's
    %% probably just the pipe shutting down normally - wait for the
    %% owner to ask for the last outputs
    {stop, normal, State};
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
                     results=Results, logs=Logs, done=Done,
                     buffer_max=Max, delayed_acks=Delayed}=State) ->
    Owner ! #kv_mrc_sink{ref=Ref,
                         results=finish_results(Results),
                         logs=lists:reverse(Logs),
                         done=Done},
    [ gen_fsm:reply(From, ok) || From <- Delayed ],
    State#state{results=[], logs=[],
                buffer_left=Max, delayed_acks=[]}.

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

%% choose buffer size, given Options, app env, default
-spec buffer_size(list()) -> non_neg_integer().
buffer_size(Options) ->
    case buffer_size_options(Options) of
        {ok, Size} -> Size;
        false ->
            case buffer_size_app_env() of
                {ok, Size} -> Size;
                false ->
                    ?BUFFER_SIZE_DEFAULT
            end
    end.

-spec buffer_size_options(list()) -> {ok, non_neg_integer()} | false.
buffer_size_options(Options) ->
    case lists:keyfind(buffer, 1, Options) of
        {buffer, Size} when is_integer(Size), Size >= 0 ->
            {ok, Size};
        _ ->
            false
    end.

-spec buffer_size_app_env() -> {ok, non_neg_integer()} | false.
buffer_size_app_env() ->
    case application:get_env(riak_kv, mrc_sink_buffer) of
        {ok, Size} when is_integer(Size), Size >= 0 ->
            {ok, Size};
        _ ->
            false
    end.

%% TEST

-ifdef(TEST).

buffer_size_test_() ->
    Tests = [ {"buffer option", 5, [{buffer, 5}], []},
              {"buffer app env", 5, [], [{mrc_sink_buffer, 5}]},
              {"buffer default", ?BUFFER_SIZE_DEFAULT, [], []} ],
    FillFuns = [ {"send_event", fun gen_fsm:send_event/2},
                 {"sync_send_event", fun gen_fsm:sync_send_event/2},
                 {"erlang:send", fun(S, R) -> S ! R, ok end} ],
    {foreach,
     fun() -> application:load(riak_kv) end,
     fun(_) -> application:unload(riak_kv) end,
     [buffer_size_test_helper(Name, FillFun, Size, Options, AppEnv)
      || {Name, Size, Options, AppEnv} <- Tests,
         FillFun <- FillFuns]}.

buffer_size_test_helper(Name, {FillName, FillFun}, Size, Options, AppEnv) ->
    {Name++" "++FillName,
     fun() ->
            application:load(riak_kv),
            [ application:set_env(riak_kv, K, V) || {K, V} <- AppEnv ],
            
            %% start up our sink
            {ok, Sink} = ?MODULE:start_link(self(), Options),
            Ref = make_ref(),
            Pipe = #pipe{builder=self(),
                         sink=#fitting{pid=Sink, ref=Ref}},
            ?MODULE:use_pipe(Sink, Pipe),

            %% fill its buffer
            [ ok = FillFun(
                     Sink,
                     #pipe_result{from=tester, ref=Ref, result=I})
              || I <- lists:seq(1, Size) ],
            
            %% ensure extra result will block
            {'EXIT',{timeout,{gen_fsm,sync_send_event,_}}} =
                (catch gen_fsm:sync_send_event(
                         Sink,
                         #pipe_result{from=tester, ref=Ref, result=Size+1},
                         1000)),
            
            %% now drain what's there
            ?MODULE:next(Sink),

            %% make sure that all results were received, including
            %% blocked one
            receive
                #kv_mrc_sink{ref=Ref, results=[{tester,R}]} ->
                    ?assertEqual(Size+1, length(R))
            end,
            %% make sure that the delayed ack was received
            receive
                {GenFsmRef, ok} when is_reference(GenFsmRef) ->
                    ok
            end
     end}.

-endif.
