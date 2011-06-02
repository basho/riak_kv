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

%% @doc A "reduce"-like fitting (in the MapReduce sense) for Riak KV
%%      MapReduce compatibility.  See riak_pipe_w_reduce.erl for more
%%      docs: this module is a stripped-down version of that one.
-module(riak_kv_w_reduce).
-behaviour(riak_pipe_vnode_worker).

-export([init/2,
         process/3,
         done/1,
         archive/1,
         handoff/2,
         validate_arg/1]).
-export([chashfun/1, reduce_compat/2]).

-include_lib("riak_pipe/include/riak_pipe.hrl").
-include_lib("riak_pipe/include/riak_pipe_log.hrl").

-record(state, {acc :: list(),
                delay :: integer(),
                delay_max :: integer(),
                p :: riak_pipe_vnode:partition(),
                fd :: riak_pipe_fitting:details()}).
-opaque state() :: #state{}.

%% @doc Setup creates an empty list accumulator and
%%      stashes away the `Partition' and `FittingDetails' for later.
-spec init(riak_pipe_vnode:partition(),
           riak_pipe_fitting:details()) ->
         {ok, state()}.
init(Partition, FittingDetails) ->
    DelayMax = calc_delay_max(FittingDetails),
    {ok, #state{acc=[], delay=0, delay_max = DelayMax,
                p=Partition, fd=FittingDetails}}.

%% @doc Process looks up the previous result for the `Key', and then
%%      evaluates the funtion on that with the new `Input'.
-spec process(term(), boolean(), state()) -> {ok, state()}.
process(Input, _Last,
        #state{acc=OldAcc, delay=Delay, delay_max=DelayMax}=State) ->
    InAcc = [Input|OldAcc],
    if Delay + 1 >= DelayMax ->
            OutAcc = reduce(InAcc, State, "reducing"),
            {ok, State#state{acc=OutAcc, delay=0}};
       true ->
            {ok, State#state{acc=InAcc, delay=Delay + 1}}
    end.

%% @doc Unless the aggregation function sends its own outputs, done/1
%%      is where all outputs are sent.
-spec done(state()) -> ok.
done(#state{acc=Acc0, delay=Delay, p=Partition, fd=FittingDetails} = S) ->
    Acc = if Delay == 0 ->
                  Acc0;
             true ->
                  reduce(Acc0, S, "done()")
          end,
    riak_pipe_vnode_worker:send_output(Acc, Partition, FittingDetails),
    ok.

%% @doc The archive is the accumulator.
-spec archive(state()) -> {ok, list()}.
archive(#state{acc=Acc}) ->
    %% just send state of reduce so far
    {ok, Acc}.

%% @doc The handoff merge is simply an accumulator list.  The reduce
%%      function is also re-evaluated for the key, such that {@link
%%      done/1} still has the correct value to send, even if no more
%%      inputs arrive.
-spec handoff(list(), state()) -> {ok, state()}.
handoff(HandoffAcc, #state{acc=Acc}=State) ->
    %% for each Acc, add to local accs;
    NewAcc = handoff_acc(HandoffAcc, Acc, State),
    {ok, State#state{acc=NewAcc}}.

-spec handoff_acc([term()], [term()], state()) -> [term()].
handoff_acc(HandoffAcc, LocalAcc, State) ->
    InAcc = HandoffAcc++LocalAcc,
    reduce(InAcc, State, "reducing handoff").

%% @doc Actually evaluate the aggregation function.
-spec reduce([term()], state(), string()) ->
         {ok, [term()]} | {error, {term(), term(), term()}}.
reduce(Inputs, #state{fd=FittingDetails}, ErrString) ->
    {rct, Fun, Arg} = FittingDetails#fitting_details.arg,
    try
        ?T(FittingDetails, [reduce], {reducing, length(Inputs)}),
        Outputs = Fun(Inputs, Arg),
        true = is_list(Outputs), %%TODO: nicer error
        ?T(FittingDetails, [reduce], {reduced, length(Outputs)}),
        Outputs
    catch Type:Error ->
            %%TODO: forward
            ?T(FittingDetails, [reduce], {reduce_error, Type, Error}),
            error_logger:error_msg(
              "~p:~p ~s:~n   ~P~n   ~P",
              [Type, Error, ErrString, Inputs, 15, erlang:get_stacktrace(), 15]),
            Inputs
    end.

%% @doc Check that the arg is a valid arity-4 function.  See {@link
%%      riak_pipe_v:validate_function/3}.
-spec validate_arg({rct, function(), term()}) -> ok | {error, iolist()}.

validate_arg({rct, Fun, _FunArg}) when is_function(Fun) ->
    validate_fun(Fun).

validate_fun(Fun) when is_function(Fun) ->
    riak_pipe_v:validate_function("arg", 2, Fun);
validate_fun(Fun) ->
    {error, io_lib:format("~p requires a function as argument, not a ~p",
                          [?MODULE, riak_pipe_v:type_of(Fun)])}.

%% @doc The preferred hashing function.  Chooses a partition based
%%      on the hash of the `Key'.
-spec chashfun({term(), term()}) -> riak_pipe_vnode:chash().
chashfun({Key,_}) ->
    chash:key_of(Key).

%% @doc Compatibility wrapper for an old-school Riak MR reduce function,
%%      which is an arity-2 function `fun(InputList, SpecificationArg)'.

reduce_compat({modfun, Module, Function}, PreviousIsReduceP) ->
    reduce_compat({qfun, erlang:make_fun(Module, Function, 2)}, 
                  PreviousIsReduceP);
reduce_compat({qfun, Fun}, true) ->
    %% Previous stage was reduce, so concatenate its output lists
    fun(Inputs, Arg) -> Fun(lists:append(Inputs), Arg) end;
reduce_compat({qfun, Fun}, false) ->
    Fun.

calc_delay_max(#fitting_details{arg = {rct, _ReduceFun, ReduceArg}}) ->
    Props = case ReduceArg of
                L when is_list(L) -> L;         % May or may not be a proplist
                _                 -> []
            end,
    AppMax = app_helper:get_env(riak_kv, mapred_reduce_phase_batch_size, 1),
    case proplists:get_value(reduce_phase_only_1, Props) of
        undefined ->
            proplists:get_value(reduce_phase_batch_size,
                                Props, AppMax);
        true ->
            an_atom_is_always_bigger_than_an_integer_so_make_1_huge_batch
    end.
