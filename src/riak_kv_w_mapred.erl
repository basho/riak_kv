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
-module(riak_kv_w_mapred).
-behaviour(riak_pipe_vnode_worker).

-export([init/2,
         process/3,
         done/1,
         archive/1,
         handoff/2,
         validate_arg/1]).
-export([chashfun/1]).

-include_lib("riak_pipe/include/riak_pipe.hrl").

-record(state, {acc :: list(),
                p :: riak_pipe_vnode:partition(),
                fd :: riak_pipe_fitting:details()}).
-opaque state() :: #state{}.

%% @doc Setup creates an empty list accumulator and
%%      stashes away the `Partition' and `FittingDetails' for later.
-spec init(riak_pipe_vnode:partition(),
           riak_pipe_fitting:details()) ->
         {ok, state()}.
init(Partition, FittingDetails) ->
    {ok, #state{acc=[], p=Partition, fd=FittingDetails}}.

%% @doc Process looks up the previous result for the `Key', and then
%%      evaluates the funtion on that with the new `Input'.
-spec process(term(), boolean(), state()) -> {ok, state()}.
process(Input, _Last, #state{acc=OldAcc}=State) ->
    InAcc = [Input|OldAcc],
    case reduce(InAcc, State) of
        {ok, OutAcc} ->
            {ok, State#state{acc=OutAcc}};
        {error, {Type, Error, Trace}} ->
            %%TODO: forward
            error_logger:error_msg(
              "~p:~p reducing:~n   ~P~n   ~P",
              [Type, Error, InAcc, 2, Trace, 5]),
            {ok, State}
    end.

%% @doc Unless the aggregation function sends its own outputs, done/1
%%      is where all outputs are sent.
-spec done(state()) -> ok.
done(#state{acc=Acc, p=Partition, fd=FittingDetails}) ->
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
    case reduce(InAcc, State) of
        {ok, OutAcc} ->
            OutAcc;
        {error, {Type, Error, Trace}} ->
                error_logger:error_msg(
                  "~p:~p reducing handoff:~n   ~P~n   ~P",
                  [Type, Error, InAcc, 2, Trace, 5]),
            LocalAcc %% don't completely barf
    end.

%% @doc Actually evaluate the aggregation function.
-spec reduce([term()], state()) ->
         {ok, [term()]} | {error, {term(), term(), term()}}.
reduce(InAcc, #state{p=Partition, fd=FittingDetails}) ->
    Fun = FittingDetails#fitting_details.arg,
    try
        {ok, OutAcc} = Fun(bogus_key, InAcc, Partition, FittingDetails),
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
