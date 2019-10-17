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

%% @doc The coordinator process that hold the details for the fitting.
%%      This process also manages the end-of-inputs synchronization
%%      for this stage of the pipeline.
-module(riak_pipe_fitting).

-behaviour(gen_fsm_compat).

%% API
-export([start_link/4]).
-export([eoi/1,
         get_details/2,
         worker_done/1,
         workers/1]).
-export([validate_fitting/1,
         format_name/1]).

%% gen_fsm_compat callbacks
-export([init/1,
         wait_upstream_eoi/2, wait_upstream_eoi/3,
         wait_workers_done/3,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4]).

-include("riak_pipe.hrl").
-include("riak_pipe_log.hrl").
-include("riak_pipe_debug.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-ifdef(PULSE).
-include_lib("pulse/include/pulse.hrl").
%% have to transform the 'receive' of the work results
-compile({parse_transform, pulse_instrument}).
%% don't trasnform toplevel test functions
-compile({pulse_replace_module,[{gen_fsm_compat,pulse_gen_fsm}]}).
-endif.

-record(worker, {partition :: riak_pipe_vnode:partition(),
                 pid :: pid(),
                 monitor :: reference()}).
-record(state, {builder :: pid(),
                details :: #fitting_details{},
                workers :: [#worker{}],
                ref :: reference()}). %% to avoid digging two levels

-opaque state() :: #state{}.

-export_type([state/0, details/0]).
-type details() :: #fitting_details{}.

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Start the coordinator, according to the `Spec' given.  The
%%      coordinator will register with `Builder' and will request its
%%      outputs to be processed under the `Output' fitting.
-spec start_link(pid(),
                 riak_pipe:fitting_spec(),
                 riak_pipe:fitting(),
                 riak_pipe:exec_opts()) ->
         {ok, pid(), riak_pipe:fitting()} | ignore | {error, term()}.
start_link(Builder, Spec, Output, Options) ->
    case gen_fsm_compat:start_link(?MODULE, [Builder, Spec, Output, Options], []) of
        {ok, Pid} ->
            {ok, Pid, fitting_record(Pid, Spec, Output)};
        Error ->
            Error
    end.

%% @doc Send an end-of-inputs message to the specified coordinator.
-spec eoi(riak_pipe:fitting()) -> ok.
eoi(#fitting{pid=Pid, ref=Ref, chashfun=C}) when C =/= sink ->
    gen_fsm_compat:send_event(Pid, {eoi, Ref}).

%% @doc Request the details about this fitting.  The ring partition
%%      index of the vnode requesting the details is included such
%%      that the coordinator can inform the vnode of end-of-inputs later.
%%      This function assumes that it is being called from the vnode
%%      process, so the `self()' can be used to give the coordinator
%%      a pid to monitor.
-spec get_details(riak_pipe:fitting(), riak_pipe_vnode:partition()) ->
         {ok, details()} | gone.
get_details(#fitting{pid=Pid, ref=Ref}, Partition) ->
    try
        gen_fsm_compat:sync_send_event(Pid, {get_details, Ref, Partition, self()})
    catch exit:_ ->
            %% catching all exit types here , since we don't care
            %% whether the coordinator was gone before we asked ('noproc')
            %% or if it went away before responding ('normal' or other
            %% exit reason)
            gone
    end.

%% @doc Tell the coordinator that this worker is done.  This function
%%      assumes that it is being called from the vnode process, so
%%      that `self()' can be used to inform the coordinator of which
%%      worker is done.
-spec worker_done(riak_pipe:fitting()) -> ok | gone.
worker_done(#fitting{pid=Pid, ref=Ref}) ->
    try
        gen_fsm_compat:sync_send_event(Pid, {done, Ref, self()})
    catch exit:_ ->
            %% catching all exit types here , since we don't care
            %% whether the coordinator was gone before we asked ('noproc')
            %% or if it went away before responding ('normal' or other
            %% exit reason)
            gone
    end.

%% @doc Get the list of ring partition indexes (vnodes) that are doing
%%      work for this coordinator.
-spec workers(pid()) -> {ok, [riak_pipe_vnode:partition()]} | gone.
workers(Fitting) ->
    try
        {ok, gen_fsm_compat:sync_send_all_state_event(Fitting, workers)}
    catch exit:_ ->
            %% catching all exit types here , since we don't care
            %% whether the coordinator was gone before we asked ('noproc')
            %% or if it went away before responding ('normal' or other
            %% exit reason)
            gone
    end.

%%%===================================================================
%%% gen_fsm_compat callbacks
%%%===================================================================

%% @doc Initialize the coordinator.  This function monitors the
%%      builder process, so it will tear down if the builder exits.
-spec init([pid() | riak_pipe:fitting_spec() | riak_pipe:fitting()
            | riak_pipe:exec_opts()]) ->
         {ok, wait_upstream_eoi, state()}.
init([Builder,
      #fitting_spec{name=Name, module=Module, arg=Arg, q_limit=QLimit}=Spec,
      Output,
      Options]) ->
    Fitting = fitting_record(self(), Spec, Output),
    Details = #fitting_details{fitting=Fitting,
                               name=Name,
                               module=Module,
                               arg=Arg,
                               output=Output,
                               options=Options,
                               q_limit=QLimit},

    ?T(Details, [], {fitting, init_started}),

    erlang:monitor(process, Builder),

    ?T(Details, [], {fitting, init_finished}),

    put(eunit, [{module, ?MODULE},
                {fitting, Fitting},
                {details, Details},
                {builder, Builder}]),
    {ok, wait_upstream_eoi,
     #state{builder=Builder, details=Details, workers=[],
            ref=Output#fitting.ref}}.

%% @doc The coordinator is just hanging out, serving details and waiting
%%      for end-of-inputs.
%%
%%      When it gets eoi, it forwards the signal to its workers, and
%%      then begins waiting for them to respond done.  If it has no
%%      workers when it receives end-of-inputs, the coordinator stops
%%      immediately.
-spec wait_upstream_eoi(eoi, state()) ->
         {stop, normal, state()}
       | {next_state, wait_workers_done, state()}.
wait_upstream_eoi({eoi, Ref},
                  #state{ref=Ref, workers=[], details=Details}=State) ->
    ?T(Details, [eoi], {fitting, receive_eoi}),
    %% No workers to stop
    try
        %% To assist some fittings, such as riak_kv_w_reduce, we need
        %% to fake spinning up a single worker and have it send its
        %% result downstream (which is done as a side-effect of
        %% calling wait_for_input()).
        #fitting_details{module=Module, options=Os0} = Details,
        true = Module:no_input_run_reduce_once(),
        Os = [pipe_fitting_no_input|Os0],
        {ok, WState1} = Module:init(0, Details#fitting_details{options=Os}),
        _ = Module:done(WState1)
    catch
        error:_ ->                              % undef or badmatch
            ok
    end,
    forward_eoi(State),
    {stop, normal, State};
wait_upstream_eoi({eoi, Ref},
                  #state{ref=Ref, workers=Workers, details=Details}=State) ->
    ?T(Details, [eoi], {fitting, receive_eoi}),
    _ = [ riak_pipe_vnode:eoi(Pid, Details#fitting_details.fitting)
          || #worker{pid=Pid} <- Workers ],
    {next_state, wait_workers_done, State};
wait_upstream_eoi(_, State) ->
    %% unknown message - ignore
    {next_state, wait_upstream_eoi, State}.


%% @doc The coordinator is just hanging out, serving details and waiting
%%      for end-of-inputs.
%%
%%      When it gets a request for the fitting details, it sets up
%%      a monitor for the working vnode, and responds with details.
%%
%%      The coordinator may receive a `done' message from a vnode before
%%      eoi has been sent, if handoff causes the worker to relocate.
%%      In this case, the coordinator simply demonitors the vnode, and
%%      removes it from its worker list.
-spec wait_upstream_eoi({get_details, riak_pipe_vnode:partition(), pid()},
                        term(), state()) ->
         {reply, {ok, details()}, wait_upstream_eoi, state()};
                       ({done, pid()}, term(), state()) ->
         {reply, ok, wait_upstream_eoi, state()}.
wait_upstream_eoi({get_details, Ref, Partition, Pid}=M, _From,
                  #state{ref=Ref}=State) ->
    ?T(State#state.details, [get_details], {fitting, M}),
    NewState = add_worker(Partition, Pid, State),
    {reply,
     {ok, State#state.details},
     wait_upstream_eoi,
     NewState};
wait_upstream_eoi({done, Ref, Pid}=M, _From, #state{ref=Ref}=State) ->
    %% handoff caused early done
    ?T(State#state.details, [done], {early_fitting, M}),
    case lists:keytake(Pid, #worker.pid, State#state.workers) of
        {value, Worker, Rest} ->
            erlang:demonitor(Worker#worker.monitor);
        false ->
            Rest = State#state.workers
    end,
    %% don't check for empty Rest like in wait_workers_done, though
    %% because we haven't seen eoi yet
    {reply, ok, wait_upstream_eoi, State#state{workers=Rest}};
wait_upstream_eoi(_, _, State) ->
    %% unknown message - reply {error, unknown} to get rid of it
    {reply, {error, unknown}, wait_upstream_eoi, State}.

%% @doc The coordinator has forwarded the end-of-inputs signal to all of
%%      the vnodes working for it, and is waiting for done responses.
%%
%%      When the coordinator receives a done response, it demonitors
%%      the vnode that sent it, and removes it from its worker list.
%%      If there are no more responses to wait for, the coordinator
%%      forwards the end-of-inputs signal to the coordinator for the
%%      next fitting in the pipe, and then shuts down normally.
%%
%%      If the coordinator receives a request for details from a vnode
%%      while in this state, it responds with the detail as usual,
%%      but also immediately sends end-of-inputs to that vnode.
-spec wait_workers_done({get_details, riak_pipe_vnode:partition(), pid()},
                        term(), state()) ->
         {reply, {ok, details()}, wait_workers_done, state()};
                       ({done, pid()}, term(), state()) ->
         {reply, ok, wait_workers_done, state()}
       | {stop, normal, ok, state()}.
wait_workers_done({get_details, Ref, Partition, Pid}=M, _From,
                  #state{ref=Ref}=State) ->
    %% handoff caused a late get_details
    ?T(State#state.details, [get_details], {late_fitting, M}),
    %% send details, and monitor as usual
    NewState = add_worker(Partition, Pid, State),
    %% also send eoi, to have worker immediately finish up
    Details = NewState#state.details,
    riak_pipe_vnode:eoi(Pid, Details#fitting_details.fitting),
    {reply,
     {ok, NewState#state.details},
     wait_workers_done,
     NewState};
wait_workers_done({done, Ref, Pid}=M, _From, #state{ref=Ref}=State) ->
    ?T(State#state.details, [done], {fitting, M}),
    case lists:keytake(Pid, #worker.pid, State#state.workers) of
        {value, Worker, Rest} ->
            erlang:demonitor(Worker#worker.monitor);
        false ->
            Rest = State#state.workers
    end,
    case Rest of
        [] ->
            forward_eoi(State),
            {stop, normal, ok, State#state{workers=[]}};
        _ ->
            {reply, ok, wait_workers_done, State#state{workers=Rest}}
    end;
wait_workers_done(_, _, State) ->
    %% unknown message - reply {error, unknown} to get rid of it
    {reply, {error, unknown}, wait_workers_done, State}.

%% @doc Unused.
-spec handle_event(term(), atom(), state()) ->
         {next_state, atom(), state()}.
handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

%% @doc The only sync event handled in all states is `workers', which
%%      retrieves a list of ring partition indexes that have requested
%%      the fitting details (i.e. that are doing work for this
%%      coordinator).
-spec handle_sync_event(workers, term(), atom(), state()) ->
         {reply, [riak_pipe_vnode:partition()], atom(), state()}.
handle_sync_event(workers, _From, StateName, #state{workers=Workers}=State) ->
    Partitions = [ P || #worker{partition=P} <- Workers ],
    {reply, Partitions, StateName, State};
handle_sync_event({test_crash, Fun},_,_,_) ->
    %% Only test-enabled client sends this.
    %% See riak_test's rt_pipe:crash_fitting/2 and pipe_verify_* tests
    Fun();
handle_sync_event(_Event, _From, StateName, State) ->
    Reply = ok,
    {reply, Reply, StateName, State}.

%% @doc The non-gen_fsm message that this process expects is 'DOWN'.
%%
%%      'DOWN' messages are received when monitored vnodes exit.  In
%%      that case, the vnode is removed from the worker list.  If that
%%      was also the last vnode we were waiting on a `done' message
%%      from, also forward `eoi' and shut down the coordinator.
-spec handle_info({'DOWN', reference(), term(), term(), term()},
                  atom(), state()) ->
         {next_state, atom(), state()}
        |{stop, normal, state()}.
handle_info({'DOWN', _Ref, process, Builder, _Reason},
            _StateName,
            #state{builder=Builder}=State) ->
    %% if the builder exits, stop immediately
    {stop, normal, State};
handle_info({'DOWN', Ref, _, _, _}, StateName, State) ->
    case lists:keytake(Ref, #worker.monitor, State#state.workers) of
        {value, Worker, Rest} ->
            ?T(State#state.details, [done, 'DOWN'],
               {vnode_failure, Worker#worker.partition}),
            %% check whether this coordinator was just waiting on a final
            %% 'done' and stop if so (because anything left in that
            %% vnode's worker queue is lost)
            case {StateName, Rest} of
                {wait_workers_done, []} ->
                    forward_eoi(State),
                    {stop, normal, State#state{workers=[]}};
                _ ->
                    {next_state, StateName, State#state{workers=Rest}}
            end;
        false ->
            %% looks like a misdirected down notification - ignore
            {next_state, StateName, State}
    end;
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

%% @doc Construct a #fitting{} record, given this coordinator's pid,
%%      its fitting spec, and output destination.
-spec fitting_record(pid(),
                     Spec::riak_pipe:fitting_spec(),
                     Output::riak_pipe:fitting()) ->
         riak_pipe:fitting().
fitting_record(Pid,
               #fitting_spec{chashfun=HashFun, nval=NVal},
               #fitting{ref=Ref}) ->
    #fitting{pid=Pid, ref=Ref, chashfun=HashFun, nval=NVal}.

%% @doc Send the end-of-inputs signal to the next coordinator.
-spec forward_eoi(state()) -> ok.
forward_eoi(#state{details=Details}) ->
    ?T(Details, [eoi], {fitting, send_eoi}),
    case Details#fitting_details.output of
        #fitting{chashfun=sink}=Sink ->
            riak_pipe_sink:eoi(Sink, Details#fitting_details.options);
        #fitting{}=Fitting ->
            riak_pipe_fitting:eoi(Fitting)
    end.

%% @doc Monitor the given vnode, and add it to our list of workers.
-spec add_worker(riak_pipe_vnode:partition(), pid(), state()) -> state().
add_worker(Partition, Pid, State) ->
    %% check if we're already monitoring this pid before setting up a
    %% new monitor (in case pid re-requests details)
    case worker_by_partpid(Partition, Pid, State) of
        {ok, _Worker} ->
            %% already monitoring
            State;
        none ->
            Ref = erlang:monitor(process, Pid),
            State#state{workers=[#worker{partition=Partition,
                                         pid=Pid,
                                         monitor=Ref}
                                 |State#state.workers]}
    end.

%% @doc Find a worker's entry in the worker list by its ring
%%      partition index and pid.
-spec worker_by_partpid(riak_pipe_vnode:partition(), pid(), state()) ->
         {ok, #worker{}} | none.
worker_by_partpid(Partition, Pid, #state{workers=Workers}) ->
    case [ W || #worker{partition=A, pid=I}=W <- Workers,
                A == Partition, I == Pid] of
        [#worker{}=Worker] -> {ok, Worker};
        []                 -> none
    end.

%% @doc Ensure that a fitting specification is valid.  This function
%%      will check that the module is an atom that names a valid
%%      module (see {@link riak_pipe_v:validate_module/2}), that the
%%      arg is valid for the module (see {@link validate_argument/2}),
%%      and that the partition function is of the proper form (see
%%      {@link validate_chashfun/1}).  It also checks that nval is
%%      undefined or a postive integer.
%%
%%      If all components are valid, the atom `ok' is returned.  If
%%      any piece is invalid, `{badarg, #fitting_spec.name, ErrorMsg}'
%%      is thrown.
-spec validate_fitting(riak_pipe:fitting_spec()) -> ok.
validate_fitting(#fitting_spec{name=Name,
                               module=Module,
                               arg=Arg,
                               chashfun=HashFun,
                               nval=NVal}) ->
    case riak_pipe_v:validate_module("module", Module) of
        ok -> ok;
        {error, ModError} ->
            lager:error(
              "Invalid module in fitting spec \"~s\": ~s",
              [format_name(Name), ModError]),
            throw({badarg, Name, ModError})
    end,
    case validate_argument(Module, Arg) of
        ok -> ok;
        {error, ArgError} ->
            lager:error(
              "Invalid module argument in fitting spec \"~s\": ~s",
              [format_name(Name), ArgError]),
            throw({badarg, Name, ArgError})
    end,
    case validate_chashfun(HashFun) of
        ok -> ok;
        {error, PFError} ->
            lager:error(
              "Invalid chashfun in fitting spec \"~s\": ~s",
              [format_name(Name), PFError]),
            throw({badarg, Name, PFError})
    end,
    case validate_nval(NVal) of
        ok -> ok;
        {error, NVError} ->
            lager:error(
              "Invalid nval in fitting spec \"~s\": ~s",
              [format_name(Name), NVError]),
            throw({badarg, Name, NVError})
    end;
validate_fitting(Other) ->
    lager:error(
      "Invalid fitting_spec given (expected fitting_spec record):~n~P",
      [Other, 3]),
    throw({badarg, undefined, "not a fitting_spec record"}).

%% @doc Validate initialization `Arg' for the given `Module' by calling
%%      `Module:validate_arg(Arg)', if it exists.  This function assumes
%%      that `Module' has already been validate.
-spec validate_argument(module(), term()) -> ok | {error, string()}.
validate_argument(Module, Arg) ->
    case lists:member({validate_arg, 1}, Module:module_info(exports)) of
        true ->
            try
                Module:validate_arg(Arg)
            catch Type:Error ->
                    {error, io_lib:format(
                              "failed to validate module argument: ~p:~p",
                              [Type, Error])}
            end;
        false ->
            ok %% don't force modules to validate their args
    end.

%% @doc Validate the consistent hashing function.  This must be the
%%      atom `follow', a static hash as a 160-bit binary, or a valid
%%      funtion of arity 1 (see {@link riak_pipe_v:validate_function/3}).
-spec validate_chashfun(follow | riak_pipe_vnode:chashfun()) ->
         ok | {error, string()}.
validate_chashfun(follow) ->
    ok;
validate_chashfun(Hash) when is_binary(Hash) ->
    case byte_size(Hash) of
        20 ->
            % consistent hashes are 160 bits
            ok;
        Other ->
            {error, io_lib:format(
                      "expected a 160-bit binary, found ~p bits", [Other])}
    end;
validate_chashfun(HashFun) ->
    riak_pipe_v:validate_function("chashfun", 1, HashFun).

%% @doc Validate the nval parameter.  This must either be a positive
%%      integer, or a function of arity 1 (that produces a positive
%%      integer).  The function may be specified anonymously or as a
%%      {Mod, Fun} tuple.
-spec validate_nval(term()) -> ok | {error, string()}.
validate_nval(NVal) when is_integer(NVal) ->
    if NVal > 0 -> ok;
       true ->
            {error, io_lib:format(
                      "expected a positive integer, found ~p", [NVal])}
    end;
validate_nval(NVal) when is_function(NVal) ->
    riak_pipe_v:validate_function("nval", 1, NVal);
validate_nval({Mod, Fun}) when is_atom(Mod), is_atom(Fun) ->
    riak_pipe_v:validate_function("nval", 1, {Mod, Fun});
validate_nval(NVal) ->
    {error, io_lib:format(
              "expected a positive integer,"
              " or a function or {Mod, Fun} of arity 1; not a ~p",
              [riak_pipe_v:type_of(NVal)])}.

%% @doc Coerce a fitting name into a printable string.
-spec format_name(term()) -> iolist().
format_name(Name) when is_binary(Name) ->
    Name;
format_name(Name) when is_list(Name) ->
    case is_iolist(Name) of
        true -> Name;
        false -> io_lib:format("~p", [Name])
    end;
format_name(Name) ->
    io_lib:format("~p", [Name]).

%% @doc Determine if a term is an iolist.
-spec is_iolist(term()) -> boolean().
is_iolist(Name) when is_list(Name) ->
    lists:all(fun is_iolist/1, Name);
is_iolist(Name) when is_binary(Name) ->
    true;
is_iolist(Name) when is_integer(Name), Name >= 0, Name =< 255 ->
    true;
is_iolist(_) ->
    false.

-ifdef(TEST).
validate_test_() ->
    [{"very bad fitting",
      %% undefined name because it's not a fitting_spec
      ?_assertMatch({badarg, undefined, _Msg},
                    (catch riak_pipe_fitting:validate_fitting(x)))},
     {"bad fitting module",
      ?_assertMatch({badarg, empty_pass, _Msg},
                    (catch riak_pipe_fitting:validate_fitting(
                             #fitting_spec{name=empty_pass,
                                           module=does_not_exist})))},
     {"bad fitting argument",
      ?_assertMatch({badarg, empty_pass, _Msg},
                    (catch riak_pipe_fitting:validate_fitting(
                             #fitting_spec{name=empty_pass,
                                           module=riak_pipe_w_reduce,
                                           arg=bogus_arg})))},
     {"good partfun",
      ?_assertEqual(ok,
                    (catch
                         riak_pipe_fitting:validate_fitting(
                           #fitting_spec{name=empty_pass,
                                         module=riak_pipe_w_pass,
                                         chashfun=follow})))},
     {"bad partfun",
      ?_assertMatch({badarg, empty_pass, _Msg},
                    (catch riak_pipe_fitting:validate_fitting(
                             #fitting_spec{name=empty_pass,
                                           module=riak_pipe_w_pass,
                                           chashfun=fun(_,_) -> 0 end})))},
     {"format_name binary",
      ?_assertEqual(<<"foo">>,
                    riak_pipe_fitting:format_name(<<"foo">>))},
     {"format_name string",
      ?_assertEqual("foo",
                    riak_pipe_fitting:format_name("foo"))},
     {"format_name atom",
      ?_assertEqual("[foo]",
                    lists:flatten(riak_pipe_fitting:format_name([foo])))}
     ].

-endif.
