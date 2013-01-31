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

%% @doc A pipe fitting that applies a function to a list of inputs,
%% and sends the accumulated results downstream.  This module is
%% intended to be used as the emulation of 'reduce' phases in Riak KV
%% MapReduce.
%%
%% Upstream fittings should send each of their outputs separately.
%% This worker will assemble them into a list and apply the function
%% to that list.
%%
%% This fitting expects a 3-tuple of `{rct, Fun, Arg}'.  The `Fun'
%% should be a function expecting two arguments: `Inputs :: list()'
%% and `Arg'.  The fun should return a list as its result.  The
%% function {@link reduce_compat/1} should be used to transform the
%% usual MapReduce phase spec (`{modfun, ...}', '{jsanon, ...}', etc.)
%% into the variety of function expected here.
%%
%% The default behavior is to apply the reduce function to the first
%% 20 inputs, and then apply the Fun to that result with the next 20
%% inputs received cons'd on the front, and repeat this re-running
%% untill finished.  Two knobs exist to change this behavior.  The
%% first is `reduce_phase_batch_size'.  The property may be set by
%% specifying `Arg' as a proplist, and providing a positive integer.
%% For example, setting `Arg=[{reduce_phase_batch_size, 1}]', if the
%% inputs A, B, and C were received, evaluation would look something
%% like:
%% ```
%% X = Fun([A], Arg),
%% Y = Fun([B,X], Arg),
%% Z = Fun([C,Y], Arg)
%% '''
%%
%% Setting `Arg=[{reduce_phase_batch_size, 2}]'instead, with the same
%% inputs would cause evaulation to look more like:
%% ```
%% X = Fun([B,A], Arg),
%% Y = Fun([C,X], Arg)
%% '''
%% The default batch size allowed is controlled by the riak_kv
%% application environment variable `mapred_reduce_phase_batch_size'
%%
%% The other knob to control batching behavior is known as
%% `reduce_phase_only_1'.  If this option is set in the `Arg'
%% proplist, the reduce function will be evaluated at most once.  That
%% is, the example set of inputs from above would evaulate as:
%% ```
%% X = Fun([C,B,A], Arg)
%% '''
%%
%% To use `reduce_phase_only_1' and `reduce_phase_batch_size' over the
%% HTTP interface, specify a JSON structure as the function's
%% argument, as in:
%% ```
%% {...,"query":[...,{"reduce":{...,"arg":{"reduce_phase_batch_size":100}}}]}
%% '''
%% Or:
%% ```
%% {...,"query":[...,{"reduce":{...,"arg":{"reduce_phase_only1":true}}}]}
%% '''
%% The HTTP interface will translate that argument into a mochijson2
%% structure (e.g. `{struct, [{<<"reduce_phase_only_1">>, true}]}'),
%% which this fitting will understand.  This also provides a safe way
%% to pass these arguments when using a reduce phase implemented in
%% Javascript over the Protocol Buffer or native interfaces.
%% Mochijson2 conversion will fail on the bare proplist, but will
%% succeed at encoding this form.
%%
%% If no inputs are received before eoi, this fitting evaluated the
%% function once, with an empty list as `Inputs'.
%%
%% For Riak KV MapReduce reduce phase compatibility, a chashfun that
%% directs all inputs to the same partition should be used.  Multiple
%% workers will reduce only parts of the input set, and produce
%% multiple independent outputs, otherwise (note that this may be
%% desirable in a "pre-reduce" phase).
-module(riak_kv_w_reduce).
-behaviour(riak_pipe_vnode_worker).

-export([init/2,
         process/3,
         done/1,
         archive/1,
         handoff/2,
         validate_arg/1]).
-export([reduce_compat/1]).
-export([no_input_run_reduce_once/0]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include_lib("riak_pipe/include/riak_pipe.hrl").
-include_lib("riak_pipe/include/riak_pipe_log.hrl").

-include("riak_kv_js_pools.hrl").

-record(state, {acc :: list(),
                inacc :: list(),
                delay :: integer(),
                delay_max :: integer(),
                p :: riak_pipe_vnode:partition(),
                fd :: riak_pipe_fitting:details()}).
-opaque state() :: #state{}.

-define(DEFAULT_JS_RESERVE_ATTEMPTS, 10).

%% @doc Setup creates an empty list accumulator and
%%      stashes away the `Partition' and `FittingDetails' for later.
-spec init(riak_pipe_vnode:partition(),
           riak_pipe_fitting:details()) ->
         {ok, state()}.
init(Partition, #fitting_details{options=Options} = FittingDetails) ->
    DelayMax = calc_delay_max(FittingDetails),
    Acc = case proplists:get_value(pipe_fitting_no_input, Options) of
              true ->
                  %% AZ 479: Riak KV Map/Reduce compatibility: call reduce
                  %% function once when no input is received by fitting.
                  %% Note that the partition number given to us is bogus.
                  reduce([], #state{fd=FittingDetails},"riak_kv_w_reduce init");
              _ ->
                  []
          end,
    {ok, #state{acc=Acc, inacc=[], delay=0, delay_max = DelayMax,
                p=Partition, fd=FittingDetails}}.

%% @doc Evaluate the function if the batch is ready.
-spec process(term(), boolean(), state()) -> {ok, state()}.
process(Input, _Last, #state{inacc=OldInAcc, delay=Delay}=State) ->
    InAcc = [Input|OldInAcc],
    maybe_reduce(State#state{inacc=InAcc, delay=Delay+1}, "reducing").

%% @doc Reduce if the accumulated inputs trip the batch size trigger.
-spec maybe_reduce(state(), string()) -> {ok, state()}.
maybe_reduce(#state{acc=PrevAcc, inacc=InAcc,
                    delay=Delay, delay_max=DelayMax}=State,
             Message) ->
    if Delay >= DelayMax ->
            OutAcc = reduce(PrevAcc ++ lists:reverse(InAcc), State, Message),
            {ok, State#state{acc=OutAcc, inacc=[], delay=0}};
       true ->
            {ok, State}
    end.

%% @doc Reduce any unreduced inputs, and then send on the outputs.
-spec done(state()) -> ok.
done(#state{acc=Acc0, inacc=InAcc, delay=Delay, p=Partition, fd=FittingDetails} = S) ->
    Acc = if Delay == 0 ->
                  Acc0;
             true ->
                  reduce(Acc0 ++ lists:reverse(InAcc), S, "done()")
          end,
    [ riak_pipe_vnode_worker:send_output(O, Partition, FittingDetails)
      || O <- Acc ],
    ok.

%% @doc The archive is the accumulator.
-spec archive(state()) -> {ok, list()}.
archive(#state{acc=Acc, inacc=InAcc}) ->
    %% just send state of reduce so far
    {ok, Acc ++ lists:reverse(InAcc)}.

%% @doc Handoff simply concatenates the accumulators from the remote
%% worker with the accumulator from this worker, and then reduces if
%% the resulting accumulator crosses the batch size threshold.
-spec handoff(list(), state()) -> {ok, state()}.
handoff(HandoffAcc, #state{inacc=OldInAcc}=State) ->
    %% assume that inputs received by the vnode that was archived were
    %% meant to arrive before any inputs received here (because the
    %% typical handoff case is that this is a new node taking over)
    %%
    %% put all incoming unreduced inputs after all local inputs, and
    %% then put all reduced inputs reversed after that. this has the
    %% best chance of producing the correct order for a reduce phase
    %% that was sorting inputs
    %%   Example:  HandoffAcc = Acc ++ InAcc = [1,2,3] ++ [4,5,6]
    %%             OldInAcc = [9,8,7]
    %%             InAcc = [9,8,7] ++ [6,5,4] ++ [3,2,1]
    InAcc = OldInAcc ++ lists:reverse(HandoffAcc),
    maybe_reduce(State#state{inacc=InAcc, delay=length(InAcc)},
                 "reducing handoff").

%% @doc Actually evaluate the aggregation function.
-spec reduce([term()], state(), string()) -> list().
reduce(Inputs, #state{fd=FittingDetails}, ErrString) ->
    {rct, Fun, Arg} = FittingDetails#fitting_details.arg,
    ?T(FittingDetails, [reduce], {reducing, ErrString, length(Inputs)}),
    case Fun(Inputs, Arg) of
        Outputs when is_list(Outputs) ->
            ?T(FittingDetails, [reduce], {reduced, ErrString, length(Outputs)}),
            Outputs;
        _NonListOutputs ->
            exit(non_list_result)
    end.

%% @doc Check that the arg is a valid arity-2 function.  See {@link
%%      riak_pipe_v:validate_function/3}.
-spec validate_arg({rct, function(), term()}) -> ok | {error, iolist()}.
validate_arg({rct, Fun, _FunArg}) ->
    validate_fun(Fun).

validate_fun(Fun) when is_function(Fun) ->
    riak_pipe_v:validate_function("arg", 2, Fun);
validate_fun(Fun) ->
    {error, io_lib:format("~p requires a function as argument, not a ~p",
                          [?MODULE, riak_pipe_v:type_of(Fun)])}.

%% @doc Compatibility wrapper for an old-school Riak MR reduce function,
%%      which is an arity-2 function `fun(InputList, SpecificationArg)'.
-spec reduce_compat(riak_kv_mrc_pipe:reduce_query_fun()) -> fun().
reduce_compat({jsanon, {Bucket, Key}})
  when is_binary(Bucket), is_binary(Key) ->
    reduce_compat({qfun, js_runner({jsanon, stored_source(Bucket, Key)})});
reduce_compat({jsanon, Source})
  when is_binary(Source) ->
    reduce_compat({qfun, js_runner({jsanon, Source})});
reduce_compat({jsfun, Name})
  when is_binary(Name) ->
    reduce_compat({qfun, js_runner({jsfun, Name})});
reduce_compat({strfun, {Bucket, Key}})
  when is_binary(Bucket), is_binary(Key) ->
    reduce_compat({strfun, stored_source(Bucket, Key)});
reduce_compat({strfun, Source}) ->
    {allow_strfun, true} = {allow_strfun,
                            app_helper:get_env(riak_kv, allow_strfun)},
    {ok, Fun} = riak_kv_mrc_pipe:compile_string(Source),
    true = is_function(Fun, 2),
    reduce_compat({qfun, Fun});
reduce_compat({modfun, Module, Function}) ->
    reduce_compat({qfun, erlang:make_fun(Module, Function, 2)});
reduce_compat({qfun, Fun}) ->
    Fun.

%% @doc True; this fitting should be started and stopped, even if
%% no inputs were received (no normal workers were started).
no_input_run_reduce_once() ->
    true.

%% @doc Fetch source code for the reduce function stored in a Riak KV
%% object.
-spec stored_source(binary(), binary()) -> binary().
stored_source(Bucket, Key) ->
    {ok, C} = riak:local_client(),
    {ok, Object} = C:get(Bucket, Key, 1),
    riak_object:get_value(Object).

%% @doc Produce a function suitable for this fitting's `Arg' that will
%% evaluate the given piece of Javascript.
-spec js_runner({jsanon | jsfun, binary()}) ->
         fun( (list(), term()) -> list() ).
js_runner(JS) ->
    fun(Inputs, Arg) ->
            SafeArg = remove_batch_props(Arg),
            JSInputs = [riak_kv_mapred_json:jsonify_not_found(I)
                        || I <- Inputs],
            JSCall = {JS, [JSInputs, SafeArg]},
            case riak_kv_js_manager:blocking_dispatch(
                   ?JSPOOL_REDUCE, JSCall, ?DEFAULT_JS_RESERVE_ATTEMPTS) of
                {ok, Results0} when is_list(Results0) ->
                    [riak_kv_mapred_json:dejsonify_not_found(R)
                     || R <- Results0];
                {ok, NonlistResults} ->
                    NonlistResults; %% will blow up in reduce/3
                {error, Error} ->
                    exit(Error)
            end
    end.

%% @doc Remove reduce batch size knobs from the `Arg' list, so
%% mochijson2 doesn't blow up when trying to encode them.
remove_batch_props(Arg) when is_list(Arg) ->
    lists:filter(fun(reduce_phase_only_1)         -> false;
                    ({reduce_phase_only_1,_})     -> false;
                    ({reduce_phase_batch_size,_}) -> false;
                    (_)                           -> true
                 end,
                 Arg);
remove_batch_props(Arg) ->
    Arg.

%% @doc Determine what batch size should be used for this fitting.
%% Default is 20, but may be overridden by the `Arg' props
%% `reduce_phase_only_1' and `reduce_phase_batch_size', or the riak_kv
%% application environment variable `mapred_reduce_pahse_batch_size'.
%%
%% NOTE: An atom is used when the reduce should be run only once,
%% since atoms always compare greater than integers.
-spec calc_delay_max(riak_pipe_fitting:details()) ->
         integer() | atom().
calc_delay_max(#fitting_details{arg = {rct, _ReduceFun, ReduceArg}}) ->
    Props = case ReduceArg of
                L when is_list(L) -> L;         % May or may not be a proplist
                {struct, L} -> delay_props_from_json(L);
                _                 -> []
            end,
    AppMax = app_helper:get_env(riak_kv, mapred_reduce_phase_batch_size, 20),
    case proplists:get_value(reduce_phase_only_1, Props) of
        true ->
            an_atom_is_always_bigger_than_an_integer_so_make_1_huge_batch;
        _ ->
            proplists:get_value(reduce_phase_batch_size,
                                Props, AppMax)
    end.

%% @doc convert JSON struct properties with similar names to Erlang
%% atoms, since the HTTP interface has no way to send atoms natively
-spec delay_props_from_json(list()) -> [{atom(), term()}].
delay_props_from_json(JsonProps) ->
    Only1 = extract_json_prop(reduce_phase_only_1, JsonProps),
    Batch = extract_json_prop(reduce_phase_batch_size, JsonProps),
    Only1 ++ Batch.

extract_json_prop (Key, JsonProps) ->
    case lists:keyfind(atom_to_binary(Key, latin1), 1, JsonProps) of
        {_,Value} ->
            [{Key,Value}];
        false ->
            []
    end.

-ifdef(TEST).

%% This test should check that the reduce function is not called more
%% often than reduce_phase_batch_size or reduce_phase_only_1 request.
batch_size_during_handoff_test() ->
    Fun = fun riak_kv_mapreduce:reduce_count_inputs/2,

    ReduceEvery5 = [{reduce_phase_batch_size, 5}],
    AInputs = [a,b,c],
    {ok, StateUnreduced} =
        handoff_test_helper(Fun, ReduceEvery5, AInputs, []),
    %% handing off three unprocessed inputs to a fresh worker with
    %% zero unprocessed inputs should not immediately process the
    %% inputs if the batch size is greater than 3
    ?assertEqual(length(AInputs), StateUnreduced#state.delay),
    ?assertEqual(lists:reverse(AInputs), StateUnreduced#state.inacc),

    {ok, StateFinally} =
        test_helper({ok, StateUnreduced}, [e,f]),
    %% just two more inputs should still trigger the reduce
    ?assertEqual(0, StateFinally#state.delay),
    ?assertEqual([], StateFinally#state.inacc),
    
    BInputs = [e,f,g],
    {ok, StateReduced} =
        handoff_test_helper(Fun, ReduceEvery5, AInputs, BInputs),
    %% handing off three unprocessed inputs to a worker that has three
    %% more unprocessed inputs should immediately process the inputs
    %% if the batch size is less than or equal to 6
    ?assertEqual(0, StateReduced#state.delay),
    ?assertEqual([], StateReduced#state.inacc),
    
    ReduceOnce = [reduce_phase_only_1],
    {ok, StateNever} =
        handoff_test_helper(Fun, ReduceOnce, AInputs, BInputs),
    %% handing off unprocessed inputs when reduce_phase_only_1 is set
    %% should never immediately reduce them
    ?assertEqual(length(AInputs++BInputs), StateNever#state.delay),
    ?assertEqual(lists:reverse(AInputs++BInputs), StateNever#state.inacc).

%% Start reducer A and reducer B.  Feed AInputs to A and BInputs to B,
%% then archive A and handoff its data to B.  Returns B's resulting
%% state.
handoff_test_helper(Fun, Arg, AInputs, BInputs) ->
    {ok, StateA} = test_helper(Fun, Arg, AInputs),
    {ok, StateB} = test_helper(Fun, Arg, BInputs),
    {ok, Archive} = archive(StateA),
    handoff(Archive, StateB).

%% Initialize a reducer with the given fun and arg, then pass it the
%% list of inputs.
test_helper(Fun, Arg, Inputs) ->
    Fitting = #fitting{pid=self(),
                       ref=make_ref(),
                       chashfun=fun() -> <<0:160/integer>> end,
                       nval=1},
    Details = #fitting_details{fitting=Fitting,
                               name=batch_size_during_handoff_test,
                               module=?MODULE,
                               arg={rct, Fun, Arg},
                               output=Fitting,
                               options=[],
                               q_limit=64},
    test_helper(init(0, Details), Inputs).

%% Pass the list of inputs to the given reducer.
test_helper({ok, State}, Inputs) ->
    lists:foldl(
      fun(I, {ok, S}) -> process(I, true, S) end,
      {ok, State},
      Inputs).

-endif.
