%% -------------------------------------------------------------------
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
%% -------------------------------------------------------------------

%% @doc Basic worker process implementation, to be parameterized with
%%      fitting implementation module.
%%
%%      Modules that implement this behavior need to export at least
%%      three functions:
%%
%% ```
%% init(Partition :: riak_pipe_vnode:partition(),
%%      FittingDetails :: riak_pipe_fitting:details())
%%   -> {ok, ModuleState :: term()}
%% '''
%%
%%      The `init/2' function is called when the worker starts.  The
%%      module should do whatever it needs to get ready before
%%      processing inputs.  The module will probably also want to
%%      store the `Parition' and `FittingDetails' arguments in its
%%      state, as it will need them to send outputs later.  The
%%      `ModuleState' returned from this function will be passed to
%%      the `process/3' function later.
%%
%% ```
%% process(Input :: term(),
%%         LastInPreflist :: boolean(),
%%         ModuleState :: term())
%%   -> {ok, NewModuleState :: term()}
%%     |forward_preflist.
%% '''
%%
%%      The `process/3' function is called once for each input
%%      delivered to the worker.  The module should do whatever
%%      processing is needed, sending outputs if appropriate.  The
%%      `NewModuleState' returned from `process/3' will be passed back
%%      in on the next input.
%%
%%      `LastInPreflist' is an indicator as to whether this worker is
%%      the last in the partition preflist for this input.  When this
%%      parameter is `false', the function may return the atom
%%      `forward_preflist' to have the input sent to the next vnode in
%%      the prefence list.  When is parameter is `true', returning
%%      `forward_preflist' will cause an error trace message to be
%%      generated, with the reason `preflist_exhausted'.
%%
%% ```
%% done(ModuleState :: term()) -> ok.
%% '''
%%
%%      The `done/1' function is called when all inputs have been
%%      processed, and the end-of-inputs flag has been received from
%%      the fitting.  The module should clean up any resources it
%%      needs to clean up, and send any outputs it still needs to
%%      send.  When `done/1' returns, the worker will terminate.
%%
%%      There are also four optional functions that a worker behavior
%%      module can export:
%%
%% ```
%% validate_arg(Arg :: term()) -> ok | {error, Reason :: iolist()}.
%% '''
%%
%%      The `validate_arg/1' function is called before a pipeline is
%%      constructed.  If the behavior module exports this function,
%%      then it will be evaluated on the value of the `arg' field of a
%%      `#fitting_spec{}' record that points to this module.  If the
%%      argument is valid, this function should return the atom `ok'.
%%      If the argument is invalid, the function should return an
%%      error tuple, with the Reason being a printable iolist.
%%
%% ```
%% archive(ModuleState :: term()) -> {ok, Archive :: term()}.
%% '''
%%
%%      The `archive/1' function is called when the vnode that owns
%%      this worker is being handed off to another node.  The worker
%%      should produce some erlang term that represents its state.
%%      This `Archive' term will be passed to the `handoff/2' function
%%      of the module, by the worker running on the handoff target.
%%
%% ```
%% handoff(Archive :: term(),
%%         ModuleState :: term()) ->
%%    {ok, NewModuleState :: term()}.
%% '''
%%
%%      The `handoff/2' function is called when a vnode receives a
%%      handoff archive from another vnode.  The module should "merge"
%%      the `Archive' with its `ModuleState' (in whatever sense
%%      "merge" may mean for this fitting), and return the resulting
%%      `NewModuleState'.
%%
%% ```
%% no_input_run_reduce_once() -> boolean().
%% '''
%%
%% If present and returns `true', then in the case that a fitting has
%% no input (as measured by having zero workers), then a "fake" worker
%% will be started for the express purpose of running its computation
%% once and sending some output downstream.  Right now, the only
%% fitting that needs this feature is riak_kv_w_reduce.erl, which
%% needs the capability to run its reduce function once (with input of
%% an empty list) in order to maintain full compatibility with Riak
%% KV's Map/Reduce.
%%
%% For riak_kv_w_reduce.erl and any other pipe behavior callback
%% module where this function returns `true', the
%% `#fitting_details.options' property list will contain the property
%% `pipe_fitting_no_input' to indicate that the fitting has no input.
%%
-module(riak_pipe_vnode_worker).

-behaviour(gen_fsm_compat).

%% API
-export([start_link/3]).
-export([send_input/2,
         recurse_input/3,
         recurse_input/4,
         send_handoff/2,
         send_archive/1,
         send_output/3,
         send_output/4,
         send_output/5]).

%% gen_fsm_compat callbacks
-export([
         init/1,
         initial_input_request/2,
         wait_for_input/2,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4
        ]).

-include("riak_pipe.hrl").
-include("riak_pipe_log.hrl").

-record(state, {partition :: riak_pipe_vnode:partition(),
                details :: riak_pipe_fitting:details(),
                vnode :: pid(),
                modstate :: term()}).
-opaque state() :: #state{}.
-export_type([state/0]).

-type callback_state() :: term().

-callback init(riak_pipe_vnode:partition(),
               riak_pipe_fitting:details()) ->
    {ok, callback_state()}.
-callback process(term(), boolean(), callback_state()) ->
    {ok, callback_state()} |
    {forward_preflist, callback_state()} |
    {{error, term()}, callback_state()}.
-callback done(callback_state()) -> ok.

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Start a worker for the specified fitting+vnode.
-spec start_link(riak_pipe_vnode:partition(),
                 pid(),
                 riak_pipe_fitting:details()) ->
         {ok, pid()} | ignore | {error, term()}.
start_link(Partition, VnodePid, FittingDetails) ->
    gen_fsm_compat:start_link(?MODULE, [Partition, VnodePid, FittingDetails], []).

%% @doc Send input to the worker.  Note: this should only be called
%%      by the vnode that owns the worker, as the result of the worker
%%      asking for its next input.
-spec send_input(pid(), done | {term(), riak_core_apl:preflist()}) -> ok.
send_input(WorkerPid, Input) ->
    gen_fsm_compat:send_event(WorkerPid, {input, Input}).

%% @doc Ask the worker to merge handoff data from an archived worker.
%%      Note: this should only be called by the vnode that owns the
%%      worker, as the result of the worker asking for its next input
%%      when the vnode has received handoff data for the worker's
%%      fitting.
-spec send_handoff(pid(), Archive::term()) -> ok.
send_handoff(WorkerPid, Handoff) ->
    gen_fsm_compat:send_event(WorkerPid, {handoff, Handoff}).

%% @doc Ask the worker to archive itself.  The worker will send the
%%      archive data to the owning vnode when it has done so.  Once
%%      it has sent the archive, the worker shuts down normally.
-spec send_archive(pid()) -> ok.
send_archive(WorkerPid) ->
    gen_fsm_compat:send_event(WorkerPid, archive).

%% @equiv send_output(Output, FromPartition, Details, infinity)
send_output(Output, FromPartition, Details) ->
    send_output(Output, FromPartition, Details, infinity).

%% @doc Send output from the given fitting to the next output down the
%%      line. `FromPartition' is used in the case that the next
%%      fitting's partition function is `follow'.
-spec send_output(term(),
                  riak_pipe_vnode:partition(),
                  riak_pipe_fitting:details(),
                  riak_pipe_vnode:qtimeout()) ->
         ok | {error, term()}.
send_output(Output, FromPartition,
            #fitting_details{output=Fitting}=Details,
            Timeout) ->
    send_output(Output, FromPartition, Details, Fitting, Timeout).

%% @equiv recurse_input(Input, FromPartition, Details, noblock)
recurse_input(Input, FromPartition, Details) ->
    recurse_input(Input, FromPartition, Details, noblock).

%% @doc Send a new input from this fitting, to itself.  This can be
%%      used to write fittings that perform recursive calculation,
%%      where steps in the recursion might be done in parallel.
%%
%%      For example, when walking intermediate tree nodes, using
%%      `recurse_input/3' to send children to other vnodes, instead of
%%      processing them in the same worker, may be a useful strategy.
%%
%%      WARNING: Using recurse_input with a `Timeout' of `infinity' is
%%      discouraged, unless you can guarantee that the queues for a
%%      fitting will never be full.  Otherwise, it's possible to
%%      deadlock a fitting by blocking on enqueueing an input for a
%%      worker that is blocking on enqueueing an input for the sender
%%      (circular blocking).  Use `noblock' and handle timeout
%%      failures to prevent deadlock.
%%
%%      Internal details: This works because of the nature of the
%%      blocking enqueue operation.  It is guaranteed that as long as
%%      this worker is alive, the fitting for which it works will not
%%      receive all of its `done' messages.  So, the vnode that
%%      enqueues this input will still be able to ask the fitting for
%%      details, and the fitting will know that it has to wait on that
%%      vnode.
-spec recurse_input(term(),
                    riak_pipe_vnode:partition(),
                    riak_pipe_fitting:details(),
                    riak_pipe_vnode:qtimeout()) ->
         ok | {error, term()}.
recurse_input(Input, FromPartition, Details, Timeout) ->
    recurse_input(Input, FromPartition, Details, Timeout, []).

-spec recurse_input(term(),
                    riak_pipe_vnode:partition(),
                    riak_pipe_fitting:details(),
                    riak_pipe_vnode:qtimeout(),
                    riak_core_apl:preflist()) ->
         ok | {error, term()}.
recurse_input(Input, FromPartition,
              #fitting_details{fitting=Fitting}=Details,
              Timeout, UsedPreflist) ->
    send_output(Input, FromPartition, Details,
                Fitting, Timeout, UsedPreflist).

%% @doc Send output from the given fitting to a specific fitting.
%%      This is most often used to send output to the sink, but also
%%      happens to be the internal implementation of {@link
%%      send_output/3}.
-spec send_output(term(), riak_pipe_vnode:partition(),
                  riak_pipe_fitting:details(), riak_pipe:fitting(),
                  riak_pipe_vnode:qtimeout()) ->
         ok | {error, term()}.
send_output(Output, FromPartition, Details, FittingOverride, Timeout) ->
    send_output(Output, FromPartition, Details, FittingOverride, Timeout, []).

-spec send_output(term(), riak_pipe_vnode:partition(),
                  riak_pipe_fitting:details(), riak_pipe:fitting(),
                  riak_pipe_vnode:qtimeout(),
                  riak_core_apl:preflist()) ->
         ok | {error, term()}.
send_output(Output, FromPartition,
            #fitting_details{name=Name, options=Opts}=_Details,
            FittingOverride,
            Timeout, UsedPreflist) ->
    case FittingOverride#fitting.chashfun of
        sink ->
            riak_pipe_sink:result(Name, FittingOverride, Output, Opts);
        follow ->
            %% TODO: should 'follow' use the original preflist (in
            %%       case of failover)?
            riak_pipe_vnode:queue_work(
              FittingOverride, Output, Timeout, UsedPreflist,
              riak_pipe_vnode:hash_for_partition(FromPartition));
        _ ->
            riak_pipe_vnode:queue_work(
              FittingOverride, Output, Timeout, UsedPreflist)
    end.

%%%===================================================================
%%% gen_fsm_compat callbacks
%%%===================================================================

%% @doc Initialize the worker.  This function calls the implementing
%%      module's init function.  If that init function fails, the
%%      worker stops with an `{init_failed, Type, Error}' reason.
-spec init([riak_pipe_vnode:partition() | pid()
            | riak_pipe_fitting:details()]) ->
         {ok, initial_input_request, state(), 0}
       | {stop, {init_failed, term(), term()}}.
init([Partition, VnodePid, #fitting_details{module=Module}=FittingDetails]) ->
    try
        put(eunit, [{module, ?MODULE},
                    {partition, Partition},
                    {VnodePid, VnodePid},
                    {details, FittingDetails}]),
        {ok, ModState} = Module:init(Partition, FittingDetails),
        {ok, initial_input_request,
         #state{partition=Partition,
                details=FittingDetails,
                vnode=VnodePid,
                modstate=ModState},
         0}
    catch Type:Error ->
            {stop, {init_failed, Type, Error}}
    end.

%% @doc The worker has just started, and should request its first
%%      input from its owning vnode.  This is done after a zero
%%      timeout instead of in the init function to get around the
%%      deadlock that would result from having the worker wait for a
%%      message from the vnode, which is waiting for a response from
%%      this process.
-spec initial_input_request(timeout, state()) ->
         {next_state, wait_for_input, state()}.
initial_input_request(timeout, State) ->
    request_input(State),
    {next_state, wait_for_input, State}.

%% @doc The worker has requested its next input item, and is waiting
%%      for it.
%%
%%      If the input is `done', due to end-of-inputs from the fitting,
%%      then the implementing module's `done' function is evaluated,
%%      the the worker terminates normally.
%%
%%      If the input is any regular input, then the implementing
%%      module's `process' function is evaluated.  When it finishes,
%%      the next input is requested from the vnode.
%%
%%      If the input is a handoff from another vnode, the worker asks
%%      the implementing module to merge the archive, if the worker
%%      exports that functionality.
%%
%%      If the input is a request to archive, the worker asks the
%%      implementing module to archive itself, if the worker exports
%%      that functionality.  When the archiving process has finished,
%%      the worker terminates normally.
-spec wait_for_input({input, done | {term(), riak_core_apl:preflist()}}
                    |{handoff, term()}
                    |archive,
                     state()) ->
         {next_state, wait_for_input, state()}
       | {stop, normal, state()}.
wait_for_input({input, done}, State) ->
    ok = process_done(State),
    {stop, normal, State}; %%TODO: monitor
wait_for_input({input, {Input, UsedPreflist}}, State) ->
    NewState = process_input(Input, UsedPreflist, State),
    request_input(NewState),
    {next_state, wait_for_input, NewState};
wait_for_input({handoff, HandoffState}, State) ->
    %% receiving handoff from another vnode
    NewState = handoff(HandoffState, State),
    request_input(NewState),
    {next_state, wait_for_input, NewState};
wait_for_input(archive, State) ->
    %% sending handoff to another vnode
    Archive = archive(State),
    reply_archive(Archive, State),
    {stop, normal, State}.

%% @doc Unused.
-spec handle_event(term(), atom(), state()) ->
         {next_state, atom(), state()}.
handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

%% @doc Unused.
-spec handle_sync_event(term(), term(), atom(), state()) ->
         {reply, ok, atom(), state()}.
handle_sync_event(_Event, _From, StateName, State) ->
    Reply = ok,
    {reply, Reply, StateName, State}.

%% @doc Unused.
-spec handle_info(term(), atom(), state()) ->
         {next_state, atom(), state()}.
handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

%% @doc Unused.
-spec terminate(term(), atom(), state()) -> ok.
terminate(_Reason, _StateName, _State) ->
    ok.

%% @doc Unused.
-spec code_change(term(), atom(), state(), term()) ->
         {ok, atom(), state()}.
code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @doc Ask the vnode for this worker's next input.  The input will be
%%      sent as an event later.
-spec request_input(state()) -> ok.
request_input(#state{vnode=Vnode, details=Details}) ->
    riak_pipe_vnode:next_input(Vnode, Details#fitting_details.fitting).

%% @doc Process an input - call the implementing module's `process/3'
%%      function.
-spec process_input(term(), riak_core_apl:preflist(), state()) -> state().
process_input(Input, UsedPreflist,
              #state{details=FD, modstate=ModState}=State) ->
    Module = FD#fitting_details.module,
    NVal = case (FD#fitting_details.fitting)#fitting.nval of
               NValInt when is_integer(NValInt) -> NValInt;
               {NValMod, NValFun}               -> NValMod:NValFun(Input);
               %% 1.0.x compatibility
               NValFun                          ->
                   riak_pipe_fun:compat_apply(NValFun, [Input])
           end,
    try
        {Result, NewModState} = Module:process(Input,
                                               length(UsedPreflist) == NVal,
                                               ModState),
        case Result of
            ok ->
                ok;
            forward_preflist ->
                forward_preflist(Input, UsedPreflist, State);
            {error, RError} ->
                processing_error(
                  result, RError, FD, ModState, Module, State, Input)
        end,
        State#state{modstate=NewModState}
    catch Type:Error ->
            processing_error(Type, Error, FD, ModState, Module, State, Input),
            exit(processing_error)
    end.            

%% @private
processing_error(Type, Error, FD, ModState, Module, State, Input) ->
    Fields = record_info(fields, fitting_details),
    FieldPos = lists:zip(Fields, lists:seq(2, length(Fields)+1)),
    DsList = [{Field, element(Pos, FD)} || {Field, Pos} <- FieldPos],
    ?T_ERR(FD, [{module, Module},
                {partition, State#state.partition},
                {details, DsList},
                {type, Type},
                {error, Error},
                {input, Input},
                {modstate, ModState},
                {stack, erlang:get_stacktrace()}]).

%% @doc Process a done (end-of-inputs) message - call the implementing
%%      module's `done/1' function.
-spec process_done(state()) -> ok.
process_done(#state{details=FD, modstate=ModState}) ->
    Module = FD#fitting_details.module,
    Module:done(ModState).

%% @doc Process a handoff message - call the implementing module's
%%      `handoff/2' function, if exported.
-spec handoff(term(), state()) -> state().
handoff(HandoffArchive, #state{details=FD, modstate=ModState}=State) ->
    Module = FD#fitting_details.module,
    case lists:member({handoff, 2}, Module:module_info(exports)) of
        true ->
            {ok, NewModState} = Module:handoff(HandoffArchive, ModState),
            State#state{modstate=NewModState};
        false ->
            %% module doesn't bother handing off state
            State
    end.

%% @doc Process an archive request - call the implementing module's
%%      `archive/1' fucntion, if exported.  The atom `undefined' is if
%%      archive/1 is not exported.
-spec archive(state()) -> term().
archive(#state{details=FD, modstate=ModState}) ->
    Module = FD#fitting_details.module,
    case lists:member({archive, 1}, Module:module_info(exports)) of
        true ->
            {ok, Archive} = Module:archive(ModState),
            Archive;
        false ->
            %% module doesn't bother handif off state
            undefined
    end.

%% @doc Send the archive to the vnode after it has been generated.
-spec reply_archive(term(), state()) -> ok.
reply_archive(Archive, #state{vnode=Vnode, details=Details}) ->
    riak_pipe_vnode:reply_archive(Vnode,
                                  Details#fitting_details.fitting,
                                  Archive).

%% @doc Instead of processing this input here, forward it to the next
%%      vnode in its preflist.
%%
%%      If that forwarding fails, an error trace message will be sent
%%      under the filter `forward_preflist', listing the error and the
%%      input.
-spec forward_preflist(term(), riak_core_apl:preflist(), state()) -> ok.
forward_preflist(Input, UsedPreflist,
                 #state{partition=Partition,
                        details=FittingDetails}=State) ->
    case recurse_input(Input, Partition, FittingDetails,
                       noblock, UsedPreflist) of
        ok -> ok;
        {error, Error} ->
            processing_error(forward_preflist, Error, FittingDetails,
                             State#state.modstate,
                             FittingDetails#fitting_details.module,
                             State, Input)
    end.
