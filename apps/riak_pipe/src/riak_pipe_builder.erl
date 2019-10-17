%%--------------------------------------------------------------------
%%
%% Copyright (c) 2011 Basho Technologies, Inc.
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
%%--------------------------------------------------------------------

%% @doc The builder starts and monitors the fitting processes.
%%
%%      This startup process is how each fitting learns about the
%%      fitting that follows it.  The builder is also the process that
%%      the client asks to find the head fitting.
-module(riak_pipe_builder).

-behaviour(gen_fsm_compat).

%% API
-export([start_link/2]).
-export([fitting_pids/1,
         pipeline/1,
         destroy/1]).

%% gen_fsm_compat callbacks
-export([init/1,
         wait_pipeline_shutdown/2,
         wait_pipeline_shutdown/3,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4]).

-include("riak_pipe.hrl").
-include("riak_pipe_debug.hrl").

-ifdef(PULSE).
-include_lib("pulse/include/pulse.hrl").
%% have to transform the 'receive' of the work results
-compile({parse_transform, pulse_instrument}).
%% don't trasnform toplevel test functions
-compile({pulse_replace_module,[{gen_fsm_compat,pulse_gen_fsm}]}).
-endif.

-record(state, {options :: riak_pipe:exec_opts(),
                pipe :: #pipe{},
                alive :: [{#fitting{}, reference()}], % monitor ref
                sinkmon :: reference()}). % monitor ref

-opaque state() :: #state{}.
-export_type([state/0]).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Start a builder to setup the pipeline described by `Spec'.
-spec start_link([riak_pipe:fitting_spec()], riak_pipe:exec_opts()) ->
         {ok, pid(), reference()} | ignore | {error, term()}.
start_link(Spec, Options) ->
    case gen_fsm_compat:start_link(?MODULE, [Spec, Options], []) of
        {ok, Pid} ->
            {sink, #fitting{ref=Ref}} = lists:keyfind(sink, 1, Options),
            {ok, Pid, Ref};
        Error ->
            Error
    end.

%% @doc Get the list of pids for fittings that this builder started.
%%      If the builder terminated before this call was made, the
%%      function returns the atom `gone'.
-spec fitting_pids(pid()) -> {ok, FittingPids::[pid()]} | gone.
fitting_pids(Builder) ->
    try
        {ok, gen_fsm_compat:sync_send_all_state_event(Builder, fittings)}
    catch exit:{noproc, _} ->
            gone
    end.

%% @doc Get the `#pipe{}' record describing the pipeline created by
%%      this builder.  This function will block until the builder has
%%      finished building the pipeline.
-spec pipeline(pid()) -> {ok, #pipe{}} | gone.
pipeline(BuilderPid) ->
    gen_fsm_compat:sync_send_event(BuilderPid, pipeline).

%% @doc Shutdown the pipeline built by this builder.
-spec destroy(pid()) -> ok.
destroy(BuilderPid) ->
    try
        gen_fsm_compat:sync_send_event(BuilderPid, destroy, infinity)
    catch exit:_Reason ->
            %% the builder exited before the call completed,
            %% since we were shutting it down anyway, this is ok
            ok
    end.

%%%===================================================================
%%% gen_fsm_compat callbacks
%%%===================================================================

%% @doc Initialize the builder fsm (gen_fsm_compat callback).
-spec init([ [riak_pipe:fitting_spec()] | riak_pipe:exec_opts() ]) ->
         {ok, wait_pipeline_shutdown, state()}.
init([Spec, Options]) ->
    {sink, #fitting{ref=Ref}=Sink} = lists:keyfind(sink, 1, Options),
    SinkMon = erlang:monitor(process, Sink#fitting.pid),
    Fittings = start_fittings(Spec, Options),
    NamedFittings = lists:zip(
                      [ N || #fitting_spec{name=N} <- Spec ],
                      [ F || {F, _R} <- Fittings ]),
    Pipe = #pipe{builder=self(),
                 fittings=NamedFittings,
                 sink=Sink},
    put(eunit, [{module, ?MODULE},
                {ref, Ref},
                {spec, Spec},
                {options, Options},
                {fittings, Fittings}]),
    {ok, wait_pipeline_shutdown,
     #state{options=Options,
            pipe=Pipe,
            alive=Fittings,
            sinkmon=SinkMon}}.

%% @doc All fittings have been started, and the builder is just
%%      monitoring the pipeline (and replying to clients looking
%%      for the head fitting).
-spec wait_pipeline_shutdown(term(), state()) ->
         {next_state, wait_pipeline_shutdown, state()}.
wait_pipeline_shutdown(_Event, State) ->
    {next_state, wait_pipeline_shutdown, State}.

%% @doc A client is asking for the fittings.  Respond.
-spec wait_pipeline_shutdown(pipeline | destroy, term(), state()) ->
         {reply,
          {ok, #pipe{}},
          wait_pipeline_shutdown,
          state()}
        |{stop, normal, ok, state()}.
wait_pipeline_shutdown(pipeline, _From, #state{pipe=Pipe}=State) ->
    %% everything is started - reply now
    {reply, {ok, Pipe}, wait_pipeline_shutdown, State};
wait_pipeline_shutdown(destroy, _From, State) ->
    %% client asked to shutdown this pipe immediately
    {stop, normal, ok, State};
wait_pipeline_shutdown(_, _, State) ->
    %% unknown message - reply {error, unknown} to get rid of it
    {reply, {error, unknown}, wait_pipeline_shutdown, State}.

%% @doc Unused.
-spec handle_event(term(), atom(), state()) ->
         {next_state, atom(), state()}.
handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

%% @doc The only sync event recognized in all states is `fittings',
%%      which retrieves a count of fittings waiting to be started,
%%      and pids for fittings already started.
-spec handle_sync_event(fittings, term(), atom(), state()) ->
         {reply,
          FittingPids::[pid()],
          StateName::atom(),
          state()}.
handle_sync_event(fittings, _From, StateName,
                  #state{alive=Alive}=State) ->
    Reply = [ Pid || {#fitting{pid=Pid},_Ref} <- Alive ],
    {reply, Reply, StateName, State};
handle_sync_event(_Event, _From, StateName, State) ->
    Reply = ok,
    {reply, Reply, StateName, State}.

%% @doc The only non-gen_fsm message this process expects are `'DOWN''
%%      messages from monitoring the fittings it has started.  When
%%      normal `'DOWN'' messages have been received from all monitored
%%      fittings, this gen_fsm stops with reason `normal'.  If an
%%      error `'DOWN'' message is received for any fitting, this
%%      process exits immediately, with an error reason.
-spec handle_info({'DOWN', reference(), process, pid(), term()},
                  atom(), state()) ->
         {next_state, atom(), state()}
       | {stop, term(), state()}.
handle_info({'DOWN', Ref, process, Pid, Reason}, StateName,
            #state{alive=Alive}=State) ->
    %% stages should exit normally in order,
    %% but messages may be delivered out-of-order
    case lists:keytake(Ref, 2, Alive) of
        {value, {#fitting{pid=Pid}, Ref}, Rest} ->
            %% one of our fittings died
            case Reason of
                normal -> ok;
                _ ->
                    lager:warning("~p: Fitting worker ~p died. Reason: ~p",
                                  [StateName, Pid, Reason])
            end,
            maybe_shutdown(Reason,
                           StateName,
                           State#state{alive=Rest});
        false ->
            case (State#state.sinkmon == Ref) andalso
                (((State#state.pipe)#pipe.sink)#fitting.pid == Pid) of
                true ->
                    %% the sink died - kill the pipe, since it has
                    %% nowhere to send its output

                    %% Exit normal here because an abnormal sink exit
                    %% should have generated its own error log, and a
                    %% normal sink exit should not generate spam.
                    {stop, normal, State};
               false ->
                    %% this wasn't meant for us - ignore
                    {next_state, StateName, State}
            end
    end;
handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

%% @doc Decide whether to shutdown, or continue waiting for `'DOWN''
%%      messages from other fittings.
-spec maybe_shutdown(term(), atom(), state()) ->
         {stop, normal, state()}
       | {stop, {fitting_exited_abnormally, term()}, state()}
       | {next_state, wait_pipeline_shutdown, state()}.
maybe_shutdown(normal, wait_pipeline_shutdown, #state{alive=[]}=S) ->
    %% all fittings stopped normally, and we were waiting for them
    {stop, normal, S};
maybe_shutdown(normal, wait_pipeline_shutdown, State) ->
    %% fittings are beginning to stop, but we're still waiting on some
    {next_state, wait_pipeline_shutdown, State};
maybe_shutdown(Reason, _StateName, State) ->
    %% some fitting exited abnormally
    %% (either non-normal status, or before we were ready)
    %% explode!
    {stop, {fitting_exited_abnormally, Reason}, State}.

%% @doc Terminate any fittings that are still alive.
-spec terminate(term(), atom(), state()) -> ok.
terminate(_Reason, _StateName, #state{alive=Alive}) ->
    %% this is a brutal kill of each fitting, just in case that fitting
    %% is otherwise swamped with stop/restart messages from its workers
    _ = [ _ = riak_pipe_fitting_sup:terminate_fitting(F) || {F,_R} <- Alive ],
    ok.

%% @doc Unused.
-spec code_change(term(), atom(), state(), term()) ->
         {ok, atom(), state()}.
code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @doc Start and monitor all of the fittings for this builder's
%%      pipeline.
-spec start_fittings([riak_pipe:fitting_spec()],
                     riak_pipe:exec_opts()) ->
           [{riak_pipe:fitting(), reference()}].
start_fittings(Spec, Options) ->
    [Tail|Rest] = lists:reverse(Spec),
    ClientOutput = client_output(Options),
    lists:foldl(fun(FitSpec, [{Output,_}|_]=Acc) ->
                        [start_fitting(FitSpec, Output, Options)|Acc]
                end,
                [start_fitting(Tail, ClientOutput, Options)],
                Rest).

%% @doc Start a new fitting, as specified by `Spec', sending its
%%      output to `Output'.
-spec start_fitting(riak_pipe:fitting_spec(),
                    riak_pipe:fitting(),
                    riak_pipe:exec_opts()) ->
         {riak_pipe:fitting(), reference()}.
start_fitting(Spec, Output, Options) ->
    ?DPF("Starting fitting for ~p", [Spec]),
    {ok, Pid, Fitting} = riak_pipe_fitting_sup:add_fitting(
                           self(), Spec, Output, Options),
    Ref = erlang:monitor(process, Pid),
    {Fitting, Ref}.

%% @doc Find the sink in the options passed.
-spec client_output(riak_pipe:exec_opts()) -> riak_pipe:fitting().
client_output(Options) ->
    proplists:get_value(sink, Options).
