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

%% @doc A "reduce"-like fitting (in the MapReduce sense).  Really more
%%      like a keyed list-fold.  This fitting expects inputs of the
%%      form `{Key, Value}'.  For each input, the fitting evaluates a
%%      function (its argument) on the `Value' and any previous result
%%      for that `Key', or `[]' (the empty list) if that `Key' has
%%      never been seen by this worker.  When `done' is finally
%%      received, the fitting sends each key-value pair it has
%%      evaluated as an input to the next fittin.
%%
%%      The intent is that a fitting might receive a stream of inputs
%%      like `{a, 1}, {b, 2}, {a 3}, {b, 4}' and send on results like
%%      `{a, 4}, {b, 6}' by using a simple "sum" function.
%%
%%      This function expects a function as its argument.  The function
%%      should be arity-4 and expect the arguments:
%%<dl><dt>
%%      `Key' :: term()
%%</dt><dd>
%%      Whatever aggregation key is necessary for the algorithm.
%%</dd><dt>
%%      `InAccumulator' :: [term()]
%%</dt><dd>
%%      A list composed of the new input, cons'd on the front of the
%%      result of the last evaluation for this `Key' (or the empty list
%%      if this is the first evaluation for the key).
%%</dd><dt>
%%      `Partition' :: riak_pipe_vnode:partition()
%%</dt><dd>
%%      The partition of the vnode on which this worker is running.
%%      (Useful for logging, or sending other output.)
%%</dd><dt>
%%      `FittingDetails' :: #fitting_details{}
%%</dt><dd>
%%      The details of this fitting.
%%      (Useful for logging, or sending other output.)
%%</dd></dl>
%%
%%      The function should return a tuple of the form `{ok,
%%      NewAccumulator}', where `NewAccumulator' is a list, onto which
%%      the next input will be cons'd.  For example, the function to
%%      sum values for a key, as described above might look like:
%% ```
%% fun(_Key, Inputs, _Partition, _FittingDetails) ->
%%    {ok, [lists:sum(Inputs)]}
%% end
%% '''
%%
%%      The preferred consistent-hash function for this fitting is
%%      {@link chashfun/1}.  It hashes the input `Key'.  Any other
%%      partition function should work, but beware that a function
%%      that sends values for the same `Key' to different partitions
%%      will result in fittings down the pipe receiving multiple
%%      results for the `Key'.
%%
%%      This fitting produces as its archive, the store of evaluation
%%      results for the keys it has seen.  To merge handoff values,
%%      the lists stored with each key are concatenated, and the
%%      reduce function is re-evaluated.
-module(riak_pipe_w_reduce).
-behaviour(riak_pipe_vnode_worker).

-export([init/2,
         process/3,
         done/1,
         archive/1,
         handoff/2,
         validate_arg/1]).
-export([chashfun/1]).

-include("riak_pipe.hrl").

-ifdef(namespaced_types).
-type riak_pipe_w_reduce_dict() :: dict:dict().
-else.
-type riak_pipe_w_reduce_dict() :: dict().
-endif.

-record(state, {accs :: riak_pipe_w_reduce_dict(),
                p :: riak_pipe_vnode:partition(),
                fd :: riak_pipe_fitting:details()}).
-opaque state() :: #state{}.
-export_type([state/0]).

%% @doc Setup creates the store for evaluation results (a dict()) and
%%      stashes away the `Partition' and `FittingDetails' for later.
-spec init(riak_pipe_vnode:partition(),
           riak_pipe_fitting:details()) ->
         {ok, state()}.
init(Partition, FittingDetails) ->
    {ok, #state{accs=dict:new(), p=Partition, fd=FittingDetails}}.

%% @doc Process looks up the previous result for the `Key', and then
%%      evaluates the funtion on that with the new `Input'.
-spec process({term(), term()}, boolean(), state()) -> {ok, state()}.
process({Key, Input}, _Last, #state{accs=Accs}=State) ->
    case dict:find(Key, Accs) of
        {ok, OldAcc} -> ok;
        error        -> OldAcc=[]
    end,
    InAcc = [Input|OldAcc],
    case reduce(Key, InAcc, State) of
        {ok, OutAcc} ->
            {ok, State#state{accs=dict:store(Key, OutAcc, Accs)}};
        {error, {Type, Error, Trace}} ->
            %%TODO: forward
            lager:error(
              "~p:~p reducing:~n   ~P~n   ~P",
              [Type, Error, InAcc, 2, Trace, 5]),
            {ok, State}
    end.

%% @doc Unless the aggregation function sends its own outputs, done/1
%%      is where all outputs are sent.
-spec done(state()) -> ok.
done(#state{accs=Accs, p=Partition, fd=FittingDetails}) ->
    _ = [ ok = riak_pipe_vnode_worker:send_output(A, Partition, FittingDetails)
          || A <- dict:to_list(Accs)],
    ok.

%% @doc The archive is just the store (dict()) of evaluation results.
-spec archive(state()) -> {ok, riak_pipe_w_reduce_dict()}.
archive(#state{accs=Accs}) ->
    %% just send state of reduce so far
    {ok, Accs}.

%% @doc The handoff merge is simple a dict:merge, where entries for
%%      the same key are concatenated.  The reduce function is also
%%      re-evaluated for the key, such that {@link done/1} still has
%%      the correct value to send, even if no more inputs arrive.
-spec handoff(riak_pipe_w_reduce_dict(), state()) -> {ok, state()}.
handoff(HandoffAccs, #state{accs=Accs}=State) ->
    %% for each Acc, add to local accs;
    NewAccs = dict:merge(fun(K, HA, A) ->
                                 handoff_acc(K, HA, A, State)
                         end,
                         HandoffAccs, Accs),
    {ok, State#state{accs=NewAccs}}.

%% @doc The dict:merge function for handoff.  Handles the reducing.
-spec handoff_acc(term(), [term()], [term()], state()) -> [term()].
handoff_acc(Key, HandoffAccs, LocalAccs, State) ->
    InAcc = HandoffAccs++LocalAccs,
    case reduce(Key, InAcc, State) of
        {ok, OutAcc} ->
            OutAcc;
        {error, {Type, Error, Trace}} ->
                lager:error(
                  "~p:~p reducing handoff:~n   ~P~n   ~P",
                  [Type, Error, InAcc, 2, Trace, 5]),
            LocalAccs %% don't completely barf
    end.

%% @doc Actually evaluate the aggregation function.
-spec reduce(term(), [term()], state()) ->
         {ok, [term()]} | {error, {term(), term(), term()}}.
reduce(Key, InAcc, #state{p=Partition, fd=FittingDetails}) ->
    Fun = FittingDetails#fitting_details.arg,
    try
        {ok, OutAcc} = Fun(Key, InAcc, Partition, FittingDetails),
        true = is_list(OutAcc), %%TODO: nicer error
        {ok, OutAcc}
    catch Type:Error ->
            {error, {Type, Error, erlang:get_stacktrace()}}
    end.

%% @doc Check that the arg is a valid arity-4 function.  See {@link
%%      riak_pipe_v:validate_function/3}.
-spec validate_arg(term()) -> ok | {error, iolist()}.
validate_arg(Fun) when is_function(Fun) ->
    riak_pipe_v:validate_function("arg", 4, Fun);
validate_arg(Fun) ->
    {error, io_lib:format("~p requires a function as argument, not a ~p",
                          [?MODULE, riak_pipe_v:type_of(Fun)])}.

%% @doc The preferred hashing function.  Chooses a partition based
%%      on the hash of the `Key'.
-spec chashfun({term(), term()}) -> riak_pipe_vnode:chash().
chashfun({Key,_}) ->
    chash:key_of(Key).
