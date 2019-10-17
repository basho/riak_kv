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

%% @doc Basic interface to riak_pipe.
%%
%%      Clients of riak_pipe are most likely to be interested in
%%      {@link exec/2}, {@link wait_first_fitting/1},
%%      {@link receive_result/1}, and {@link collect_results/1}.
%%
%%      Basic client usage should go something like this:
%% ```
%% % define the pipeline
%% PipelineSpec = [#fitting_spec{name="passer"
%%                               module=riak_pipe_w_pass}],
%%
%% % start things up
%% {ok, Pipe} = riak_pipe:exec(PipelineSpec, []),
%%
%% % send in some work
%% riak_pipe:queue_work(Pipe, "work item 1"),
%% riak_pipe:queue_work(Pipe, "work item 2"),
%% riak_pipe:queue_work(Pipe, "work item 3"),
%% riak_pipe:eoi(Pipe),
%%
%% % wait for results (alternatively use receive_result/1 repeatedly)
%% {ok, Results} = riak_pipe:collect_results(Pipe).
%% '''
%%
%%      Many examples are included in the source code, and exported
%%      as functions named `example'*.
%%
%%      The functions {@link result/3}, {@link eoi/1}, and {@link
%%      log/3} are used by workers and fittings to deliver messages to
%%      the sink.
-module(riak_pipe).

%% client API
-export([exec/2,
         receive_result/1,
         receive_result/2,
         collect_results/1,
         collect_results/2,
         queue_work/2,
         queue_work/3,
         eoi/1,
         destroy/1,
         status/1,
         active_pipelines/1
        ]).
%% examples
-export([example/0,
         example_start/0,
         example_send/1,
         example_receive/1,

         example_transform/0,
         generic_transform/4,
         example_reduce/0,
         example_tick/3,
         example_tick/4,
         zero_part/1]).

-include("riak_pipe.hrl").
-include("riak_pipe_debug.hrl").

-ifdef(namespaced_types).
-type riak_pipe_dict() :: dict:dict().
-else.
-type riak_pipe_dict() :: dict().
-endif.

-export_type([pipe/0,
              fitting/0,
              fitting_spec/0,
              exec_opts/0]).
-type pipe() :: #pipe{}.
-type fitting() :: #fitting{}.
-type fitting_spec() :: #fitting_spec{}.
-type exec_opts() :: [exec_option()].
-type exec_option() :: {sink, fitting()}
                     | {sink_type, riak_pipe_sink:sink_type()}
                     | {trace, list() | riak_pipe_log:trace_filter()}
                     | {log, sink | sasl | {sink, fitting()} | lager}.
-type stat() :: {atom(), term()}.

%% @doc Setup a pipeline.  This function starts up fitting/monitoring
%%      processes according the fitting specs given, returning a
%%      handle to the pipeline.  Inputs may then be sent to vnodes,
%%      tagged with that head fitting.
%%
%%      The pipeline is specified as an ordered list of
%%      `#fitting_spec{}' records.  Each record has the fields:
%%<dl><dt>
%%      `name'
%%</dt><dd>
%%      Any term. Will be used in logging, trace, and result messages.
%%</dd><dt>
%%      `module'
%%</dt><dd>
%%      Atom. The name of the module implementing the fitting.  This
%%      module must implement the `riak_pipe_vnode_worker' behavior.
%%</dd><dt>
%%      `arg'
%%</dt><dd>
%%      Any term. Will be available to the fitting-implementation
%%      module's initialization function.  This is a good way to
%%      parameterize general fittings.
%%</dd><dt>
%%     `chashfun'
%%</dt><dd>
%%      A function of arity 1.  The consistent-hashing function used
%%      to determine which vnode should receive an input.  This
%%      function will be evaluated as `Fun(Input)'.  The result of
%%      that evaluation should be a binary, 160 bits in length, which
%%      will be used to choose the working vnode from a
%%      `riak_core_ring'.  (Very similar to the `chash_keyfun' bucket
%%      property used in `riak_kv'.)
%%
%%      The default is `fun chash:key_of/1', which will distribute
%%      inputs according to the SHA-1 hash of the input.
%%</dd><dt>
%%      `nval'
%%</dt><dd>
%%      Either a positive integer, or a function of arity 1 that
%%      returns a positive integer.  This field determines the maximum
%%      number of vnodes that might be asked to handle the input.  If
%%      a worker is unable to process an input on a given vnode, it
%%      can ask to have the input sent to a different vnode.  Up to
%%      `nval' vnodes will be tried in this manner.
%%
%%      If `nval' is an integer, that static number is used for all
%%      inputs.  If `nval' is a function, the function is evaluated as
%%      `Fun(Input)' (much like `chashfun'), and is expected to return
%%      a positive integer.
%%</dd></dl>
%%
%%      Defined elements of the `Options' list are:
%%<dl><dt>
%%      `{sink, Sink}'
%%</dt><dd>
%%      If no `sink' option is provided, one will be created, such
%%      that the calling process will receive all messages sent to the
%%      sink (all output, logging, and trace messages).  If specified,
%%      `Sink' should be a `#fitting{}' record, filled with the pid of
%%      the process prepared to receive these messages.
%%</dd><dt>
%%      `{sink_type, Type}'
%%</dt><dd>
%%      Specifies the way in which messages are delivered to the
%%      sink. If `Type' is the atom `raw', messages are delivered as
%%      plain Erlang messages. If `Type' is the tuple `{fsm,
%%      Period, Timeout}', messages are delivered by calling {@link
%%      gen_fsm:send_event/2} `Period` times with the sink's pid and
%%      the result message, then calling {@link
%%      gen_fsm:sync_send_event/3} once with the sink's pid, the
%%      result message, and the specified timeout. If no `sink_type'
%%      option is provided, `Type' defaults to `raw'.
%%
%%      Some simple `fsm' period examples: `{fsm, 0, T}` will send
%%      every message synchronously, `{fsm, infinity, T}` will send
%%      every message asynchronously, `{fsm, 1, T}` will alternate
%%      every message, `{fsm, 10, T}` sends ten messages
%%      asynchronously then one synchronously. For every period except
%%      `infinity' the first message is always sent synchrounously.
%%</dd><dt>
%%      `{trace, TraceMatches}'
%%</dt><dd>
%%      If no `trace' option is provided, tracing will be disabled for
%%      this pipeline.  If specified, `TraceMatches' should be either
%%      the atom `all', in which case all trace messages will be
%%      delivered, or a list of trace tags to match, in which case
%%      only messages with matching tags will be delivered.
%%</dd><dt>
%%      `{log, LogTarget}'
%%</dt><dd>
%%      If no `log' option is provided, logging will be disabled for
%%      this pipeline.  If specified, `LogTarget' should be one of the
%%      following atoms:
%%   <dl><dt>
%%      `sink'
%%   </dt><dd>
%%      all log (and trace) messages will be delivered to the sink
%%   </dd><dt>
%%      `sasl'
%%   </dt><dd>
%%      all log (and trace) messages will be printed via
%%      `error_logger' to the SASL log
%%   </dd><dt>
%%      `lager'
%%   </dt><dd>
%%      all log (and trace) messages will be printed to the Riak
%%      node's log via the lager utility
%%   </dd></dl>
%%</dd></dl>
%%
%%      Other values are allowed, but ignored, in `Options'.  The
%%      value of `Options' is provided to all fitting modules during
%%      initialization, so it can be a good vector for global
%%      configuration of general fittings.
-spec exec([fitting_spec()], exec_opts()) ->
         {ok, Pipe::pipe()}.
exec(Spec, Options) ->
    lists:foreach(fun riak_pipe_fitting:validate_fitting/1, Spec),
    CorrectOptions = correct_trace(
                       validate_sink_type(
                         ensure_sink(Options))),
    riak_pipe_builder_sup:new_pipeline(Spec, CorrectOptions).

%% @doc Ensure that the `{sink, Sink}' exec/2 option is defined
%%      correctly, or define a fresh one pointing to the current
%%      process if the option is absent.
-spec ensure_sink(exec_opts()) -> exec_opts().
ensure_sink(Options) ->
    case lists:keyfind(sink, 1, Options) of
        {sink, #fitting{pid=Pid}=Sink} ->
            if is_pid(Pid) ->
                    HFSink = case Sink#fitting.chashfun of
                                 undefined ->
                                     Sink#fitting{chashfun=sink};
                                 _ ->
                                     Sink
                             end,
                    RHFSink = case HFSink#fitting.ref of
                                  undefined ->
                                      HFSink#fitting{ref=make_ref()};
                                  _ ->
                                      HFSink
                              end,
                    lists:keyreplace(sink, 1, Options, {sink, RHFSink});
               true ->
                    throw({invalid_sink, nopid})
            end;
        false ->
            Sink = #fitting{pid=self(), ref=make_ref(), chashfun=sink},
            [{sink, Sink}|Options];
        _ ->
            throw({invalid_sink, not_fitting})
    end.

%% @doc Make sure that the `sink_type' option is valid, if it is set.
-spec validate_sink_type(exec_opts()) -> exec_opts().
validate_sink_type(Options) ->
    case riak_pipe_sink:valid_sink_type(Options) of
        true ->
            Options;
        {false, Invalid} ->
            throw({invalid_sink_type, Invalid})
    end.

%% @doc Validate the trace option.  Converts `{trace, list()}' to
%%      `{trace, set()}' or `{trace, ordset()}' (as supported by the
%%      cluster) for easier comparison later.
-spec correct_trace(exec_opts()) -> exec_opts().
correct_trace(Options) ->
    case lists:keyfind(trace, 1, Options) of
        {trace, all} ->
            %% nothing to correct
            Options;
        {trace, Trace} ->
            %% convert trace list to [ord]set for comparison later
            case trace_set(Trace) of
                {ok, TraceSet} ->
                    lists:keyreplace(trace, 1, Options,
                                     {trace, TraceSet});
                false ->
                    throw({invalid_trace, "not list or set or ordset"})
            end;
        false ->
            %% nothing to correct
            Options
    end.

%% @doc Convert a trace list/set/ordset to a set or ordset, as
%% supported by the cluster.
trace_set(Trace) when is_list(Trace) ->
    case riak_core_capability:get({riak_pipe, trace_format}, sets) of
        ordsets ->
            %% post-1.2
            {ok, ordsets:from_list(Trace)};
        sets ->
            %% 1.2 and earlier
            {ok, sets:from_list(Trace)}
    end;
trace_set(Trace) ->
    case sets:is_set(Trace) of
        true ->
            trace_set(sets:to_list(Trace));
        false ->
            false
    end.

%% @doc Send an end-of-inputs message to the head of the pipe.
-spec eoi(Pipe::pipe()) -> ok.
eoi(#pipe{fittings=[{_,Head}|_]}) ->
    riak_pipe_fitting:eoi(Head).

%% @equiv queue_work(Pipe, Input, infinity)
queue_work(Pipe, Input) ->
    queue_work(Pipe, Input, infinity).

%% @doc Send inputs to the head of the pipe.
%%
%%      Note that `Timeout' options are only `infinity' and `noblock',
%%      not generic durations yet.
-spec queue_work(Pipe::pipe(),
                 Input::term(),
                 Timeout::riak_pipe_vnode:qtimeout())
         -> ok | {error, riak_pipe_vnode:qerror()}.
queue_work(#pipe{fittings=[{_,Head}|_]}, Input, Timeout)
  when Timeout =:= infinity; Timeout =:= noblock ->
    riak_pipe_vnode:queue_work(Head, Input, Timeout).

%% @doc Pull the next pipeline result out of the sink's mailbox.
%%      The `From' element of the `result' and `log' messages will
%%      be the name of the fitting that generated them, as specified
%%      in the `#fitting_spec{}' record used to start the pipeline.
%%      This function assumes that it is called in the sink's process.
%%      Passing the #fitting{} structure is only needed for reference
%%      to weed out misdirected messages from forgotten pipelines.
%%      A static timeout of five seconds is hard-coded (TODO).
-spec receive_result(pipe()) ->
         {result, {From::term(), Result::term()}}
       | {log, {From::term(), Message::term()}}
       | eoi
       | timeout.
receive_result(Pipe) ->
    receive_result(Pipe, 5000).

-spec receive_result(Pipe::pipe(), Timeout::integer() | 'infinity') ->
         {result, {From::term(), Result::term()}}
       | {log, {From::term(), Message::term()}}
       | eoi
       | timeout.
receive_result(#pipe{sink=#fitting{ref=Ref}}, Timeout) ->
    receive
        #pipe_result{ref=Ref, from=From, result=Result} ->
            {result, {From, Result}};
        #pipe_log{ref=Ref, from=From, msg=Msg} ->
            {log, {From, Msg}};
        #pipe_eoi{ref=Ref} ->
            eoi
    after Timeout ->
            timeout
    end.

%% @doc Receive all results and log messages, up to end-of-inputs
%%      (unless {@link receive_result} times out before the eoi
%%      arrives).
%%
%%      If end-of-inputs was the last message received, the first
%%      element of the returned tuple will be the atom `eoi'.  If the
%%      receive timed out before receiving end-of-inputs, the first
%%      element of the returned tuple will be the atom `timeout'.
%%
%%      The second element will be a list of all result messages
%%      received, while the third element will be a list of all log
%%      messages received.
%%
%%      This function assumes that it is called in the sink's process.
%%      Passing the #fitting{} structure is only needed for reference
%%      to weed out misdirected messages from forgotten pipelines.  A
%%      static inter-message timeout of five seconds is hard-coded
%%      (TODO).
-spec collect_results(pipe()) ->
          {eoi | timeout,
           Results::[{From::term(), Result::term()}],
           Logs::[{From::term(), Message::term()}]}.
collect_results(#pipe{}=Pipe) ->
    collect_results(Pipe, [], [], 5000).

-spec collect_results(pipe(), Timeout::integer() | 'infinity') ->
          {eoi | timeout,
           Results::[{From::term(), Result::term()}],
           Logs::[{From::term(), Message::term()}]}.
collect_results(#pipe{}=Pipe, Timeout) ->
    collect_results(Pipe, [], [], Timeout).

%% @doc Internal implementation of collect_results/1.  Just calls
%%      receive_result/1, and accumulates lists of result and log
%%      messages.
-spec collect_results(Pipe::pipe(),
                      ResultAcc::[{From::term(), Result::term()}],
                      LogAcc::[{From::term(), Result::term()}],
                      Timeout::integer() | 'infinity') ->
          {eoi | timeout,
           Results::[{From::term(), Result::term()}],
           Logs::[{From::term(), Message::term()}]}.
collect_results(Pipe, ResultAcc, LogAcc, Timeout) ->
    case receive_result(Pipe, Timeout) of
        {result, {From, Result}} ->
            collect_results(Pipe, [{From,Result}|ResultAcc], LogAcc, Timeout);
        {log, {From, Result}} ->
            collect_results(Pipe, ResultAcc, [{From,Result}|LogAcc], Timeout);
        End ->
            %% result order shouldn't matter,
            %% but it's useful to have logging output in time order
            {End, ResultAcc, lists:reverse(LogAcc)}
    end.

%% @doc Brutally kill a pipeline.  Use this when it is necessary to
%%      stop all parts of a pipeline as quickly as possible, instead
%%      of waiting for an `eoi' to propagate through.
-spec destroy(pipe()) -> ok.
destroy(#pipe{builder=Builder}) ->
    riak_pipe_builder:destroy(Builder).

%% @doc Get all active pipelines hosted on `Node'.  Pass the atom
%%      `global' instead of a node name to get all pipelines hosted on
%%      all nodes.
%%
%%      The return value for a Node is a list of `#pipe{}' records.
%%      When `global' is used, the return value is a list of `{Node,
%%      [#pipe{}]}' tuples.
-spec active_pipelines(node() | global) ->
         [#pipe{}] | error | [{node(), [#pipe{}] | error}].
active_pipelines(global) ->
    [ {Node, active_pipelines(Node)}
      || Node <- riak_core_node_watcher:nodes(riak_pipe) ];
active_pipelines(Node) when is_atom(Node) ->
    try rpc:call(Node, riak_pipe_builder_sup, pipelines, []) of
        {badrpc, _} ->
            error;
        Pipes ->
            Pipes
    catch
        _:_ ->
            error
    end.

%% @doc Retrieve details about the status of the workers in this
%%      pipeline.  The form of the return is a list with one entry per
%%      fitting in the pipe.  Each fitting's entry is a 2-tuple of the
%%      form `{FittingName, WorkerDetails}', where `FittingName' is
%%      the name that was given to the fitting in the call to {@link
%%      riak_pipe:exec/2}, and `WorkerDetails' is a list with one
%%      entry per worker.  Each worker entry is a proplist, of the
%%      form returned by {@link riak_pipe_vnode:status/1}, with two
%%      properties added: `node', the node on which the worker is
%%      running, and `partition', the index of the vnode that the
%%      worker belongs to.
-spec status(pipe())
         -> [{FittingName::term(),[PartitionStatus::[stat()]]}].
status(#pipe{fittings=Fittings}) ->
    %% get all fittings and their lists of workers
    FittingWorkers = [ fitting_workers(F) || {_, F} <- Fittings ],

    %% convert to a mapping of workers -> fittings they're performing
    %% this allows us to make one status call per vnode,
    %% instead of one per vnode per fitting
    WorkerFittings = invert_dict(fun(_K, V) -> V end,
                                 fun(K, _V) -> K end,
                                 dict:from_list(FittingWorkers)),

    %% grab all worker-fitting statuses at once
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    WorkerStatuses = dict:map(worker_status(Ring), WorkerFittings),

    %% regroup statuses by fittings
    PidNames = [ {Name, Pid} || {Name, #fitting{pid=Pid}} <- Fittings ],
    FittingStatus = invert_dict(fun(_K, V) ->
                                        {fitting, Pid} =
                                            lists:keyfind(fitting, 1, V),
                                        {Name, Pid} =
                                            lists:keyfind(Pid, 2, PidNames),
                                        Name
                                end,
                                fun(_K, V) -> V end,
                                WorkerStatuses),
    dict:to_list(FittingStatus).

%% @doc Given a dict mapping keys to lists of values, "invert" the
%%      dict to map the values to their keys.
%%
%%      That is, for each `{K, [V]}' in `Dict', append `{KeyFun(K,V),
%%      ValFun(K,V)} to dict.
%%
%%      For example:
%% ```
%% D0 = dict:from_list([{a, [1, 2, 3]}, {b, [2, 3, 4]}]),
%% D1 = invert_dict(fun(_K, V) -> V end,
%%                  fun(K, _V) -> K end,
%%                  D0),
%% [{1, [a]}, {2, [a, b]}, {3, [a, b]}, {4, [b]}] = dict:to_list(D1).
%% '''
-spec invert_dict(fun((term(), term()) -> term()),
                  fun((term(), term()) -> term()),
                  riak_pipe_dict()) -> riak_pipe_dict().
invert_dict(KeyFun, ValFun, Dict) ->
    dict:fold(
      fun(Key, Vals, DAcc) ->
              lists:foldl(fun(V, LAcc) ->
                                  dict:append(KeyFun(Key, V),
                                              ValFun(Key, V),
                                              LAcc)
                          end,
                          DAcc,
                          Vals)
      end,
      dict:new(),
      Dict).

%% @doc Get the list of vnodes working for a fitting.
-spec fitting_workers(#fitting{})
         -> {#fitting{}, [riak_pipe_vnode:partition()]}.
fitting_workers(#fitting{pid=Pid}=Fitting) ->
    case riak_pipe_fitting:workers(Pid) of
        {ok, Workers} ->
            {Fitting, Workers};
        gone ->
            {Fitting, []}
    end.

%% @doc Produce a function that can be handed a partition number and a
%%      list of fittings, and will return the status for those
%%      fittings on that partition.  The closure over the Ring is a
%%      way to map over a list without having to fetch the ring
%%      repeatedly.
-spec worker_status(riak_core_ring:riak_core_ring())
         -> fun( (riak_pipe_vnode:partition(), [#fitting{}])
                 -> [ [{atom(), term()}] ] ).
worker_status(Ring) ->
    fun(Partition, Fittings) ->
            %% lookup vnode pid
            Node = riak_core_ring:index_owner(Ring, Partition),
            try rpc:call(Node, riak_core_vnode_master, get_vnode_pid,
                               [Partition, riak_pipe_vnode]) of
                {badrpc, _} = Error ->
                    [{node, Node}, {partition, Partition}, {error, Error}];
                {ok, Vnode} ->
                    %% get status of each worker
                    {Partition, Workers} = riak_pipe_vnode:status(Vnode, Fittings),

                    %% add 'node' and 'partition' to status
                    [ [{node, Node}, {partition, Partition} | W]
                        || W <- Workers ]
            catch
                _:_ = Error ->
                    [{node, Node}, {partition, Partition}, {error, Error}]
            end
    end.

%% @doc An example run of a simple pipe.  Uses {@link example_start/0},
%%      {@link example_send/0}, and {@link example_receive/0} to send
%%      nonsense through a pipe.
%%
%%      If everything behaves correctly, this function should return
%% ```
%% {eoi, [{empty_pass, "hello"}], _LogMessages}.
%% '''
-spec example() -> {eoi | timeout, list(), list()}.
example() ->
    {ok, Pipe} = example_start(),
    example_send(Pipe),
    example_receive(Pipe).

%% @doc An example of starting a simple pipe.  Starts a pipe with one
%%      "pass" fitting.  Sink is pointed at the current process.
%%      Logging is pointed at the sink.  All tracing is enabled.
-spec example_start() -> {ok, Pipe::pipe()}.
example_start() ->
    riak_pipe:exec(
      [#fitting_spec{name=empty_pass,
                     module=riak_pipe_w_pass,
                     chashfun=fun(_) -> <<0:160/integer>> end}],
      [{log, sink},
       {trace, all}]).

%% @doc An example of sending data into a pipeline.  Queues the string
%%      `"hello"' for the fitting provided, then signals end-of-inputs
%%      to that fitting.
-spec example_send(pipe()) -> ok.
example_send(Pipe) ->
    ok = riak_pipe:queue_work(Pipe, "hello"),
    riak_pipe:eoi(Pipe).

%% @doc An example of receiving data from a pipeline.  Reads all
%%      results sent to the given sink.
-spec example_receive(pipe()) ->
         {eoi | timeout, list(), list()}.
example_receive(Pipe) ->
    collect_results(Pipe).

%% @doc Another example pipeline use.  This one sets up a simple
%%      "transform" fitting, which expects lists of numbers as
%%      input, and produces the sum of that list as output.
%%
%%      If everything behaves correctly, this function should return
%% ```
%% {eoi, [{"sum transform", 55}], []}.
%% '''
-spec example_transform() -> {eoi | timeout, list(), list()}.
example_transform() ->
    MsgFun = fun lists:sum/1,
    DriverFun = fun(Pipe) ->
                        ok = riak_pipe:queue_work(Pipe, lists:seq(1, 10)),
                        riak_pipe:eoi(Pipe),
                        ok
                end,
    generic_transform(MsgFun, DriverFun, [], 1).

generic_transform(MsgFun, DriverFun, ExecOpts, NumFittings) ->
    MsgFunThenSendFun = fun(Input, Partition, FittingDetails) ->
                                ok = riak_pipe_vnode_worker:send_output(
                                       MsgFun(Input),
                                       Partition,
                                       FittingDetails)
                        end,
    {ok, Pipe} =
        riak_pipe:exec(
          lists:duplicate(NumFittings,
                          #fitting_spec{name="generic transform",
                                        module=riak_pipe_w_xform,
                                        arg=MsgFunThenSendFun,
                                        chashfun={?MODULE, zero_part}}),
          ExecOpts),
    ok = DriverFun(Pipe),
    example_receive(Pipe).

%% @doc Another example pipeline use.  This one sets up a simple
%%      "reduce" fitting, which expects tuples of the form
%%      `{Key::term(), Value::number()}', and produces results of the
%%      same form, where the output value is the sum of all of the
%%      input values for a given key.
%%
%%      If everything behaves correctly, this function should return
%% ```
%% {eoi, [{"sum reduce", {a, [55]}}, {"sum reduce", {b, [155]}}], []}.
%% '''
example_reduce() ->
    SumFun = fun(_Key, Inputs, _Partition, _FittingDetails) ->
                     {ok, [lists:sum(Inputs)]}
             end,
    {ok, Pipe} =
        riak_pipe:exec(
          [#fitting_spec{name="sum reduce",
                         module=riak_pipe_w_reduce,
                         arg=SumFun,
                         chashfun=fun riak_pipe_w_reduce:chashfun/1}],
          []),
    [ok,ok,ok,ok,ok] =
        [ riak_pipe:queue_work(Pipe, {a, N})
          || N <- lists:seq(1, 5) ],
    [ok,ok,ok,ok,ok] =
        [ riak_pipe:queue_work(Pipe, {b, N})
          || N <- lists:seq(11, 15) ],
    [ok,ok,ok,ok,ok] =
        [ riak_pipe:queue_work(Pipe, {a, N})
          || N <- lists:seq(6, 10) ],
    [ok,ok,ok,ok,ok] =
        [ riak_pipe:queue_work(Pipe, {b, N})
          || N <- lists:seq(16, 20) ],
    riak_pipe:eoi(Pipe),
    example_receive(Pipe).

example_tick(TickLen, NumTicks, ChainLen) ->
    example_tick(TickLen, 1, NumTicks, ChainLen).

example_tick(TickLen, BatchSize, NumTicks, ChainLen) ->
    Specs = [#fitting_spec{name=list_to_atom("tick_pass" ++ integer_to_list(F_num)),
                           module=riak_pipe_w_pass,
                           chashfun={?MODULE, zero_part}}
             || F_num <- lists:seq(1, ChainLen)],
    {ok, Pipe} = riak_pipe:exec(Specs, [{log, sink},
                                        {trace, all}]),
    _ = [begin
             _ = [ok = riak_pipe:queue_work(Pipe, {tick, {TickSeq, X}, os:timestamp()})
                  || X <- lists:seq(1, BatchSize)],
             if TickSeq /= NumTicks -> timer:sleep(TickLen);
                true                -> ok
             end
         end || TickSeq <- lists:seq(1, NumTicks)],
    riak_pipe:eoi(Pipe),
    example_receive(Pipe).

%% @doc dummy chashfun for tests and examples
%%      sends everything to partition 0
zero_part(_) ->
    riak_pipe_vnode:hash_for_partition(0).
