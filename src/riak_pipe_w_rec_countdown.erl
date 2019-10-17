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

%% @doc Proof of concept for recursive input (fitting sending output
%%      to itself).  When this fitting receives an input, it passes
%%      that input to its output, and also passes `Input-1' to itself
%%      as input until the input is `0'.  Thus, sending `3' as the
%%      input to this fitting, would result in the outputs `3', `2',
%%      `1', and `0'.  That is:
%%
%%```
%% Spec = [#fitting_spec{name=counter,
%%                       module=riak_pipe_w_rec_countdown}],
%% {ok, Pipe} = riak_pipe:exec(Spec, []),
%% riak_pipe:queue_work(Pipe, 3),
%% riak_pipe:eoi(Pipe),
%% {eoi, Results, []} = riak_pipe:collect_results(Pipe).
%% [{counter,0},{counter,1},{counter,2},{counter,3}] = Results.
%%'''
%%
%%      This fitting should work with any consistent-hash function.
%%      It requires no archiving for handoff.
%%
%%      If the argument is the atom `testeoi', then the final
%%      recursive input (`0') will be sent three times, with no delay
%%      before the second case and a 1-second delay before the third.
%%      These two sends should test the behavior of vnode enqueueing
%%      while attempting to force a worker to `done'.  If all `eoi'
%%      handling is done properly, then `0' should appear three times
%%      in the result list.  The `testeoi' case should go like this:
%%
%%```
%% Spec = [#fitting_spec{name=counter,
%%                       module=riak_pipe_w_rec_countdown,
%%                       arg=testeoi}],
%% Options = [{trace,[restart]},{log,sink}],
%% {ok, Pipe} = riak_pipe:exec(Spec, Options),
%% riak_pipe:queue_work(Pipe, 3),
%% riak_pipe:eoi(Pipe),
%% {eoi, Results, Trace} = riak_pipe:collect_results(Pipe).
%% [{counter,0},{counter,0},{counter,0},
%%  {counter,1},{counter,2},{counter,3}] = Results.
%% [{counter,{trace,[restart],{vnode,{restart,_}}}}] = Trace.
%%'''
%%
%%      If `Results' contains less than three instances of
%%      `{counter,0}', then the test failed.  If `Trace' is empty, the
%%      done/eoi race was not triggered, and the test should be
%%      re-run.
%%
%% NOTE: This test code has been copied to the EUnit tests in riak_pipe.erl,
%%       into the basic_test_() collection.

-module(riak_pipe_w_rec_countdown).
-behaviour(riak_pipe_vnode_worker).

-export([init/2,
         process/3,
         done/1]).

-include("riak_pipe.hrl").
-include("riak_pipe_log.hrl").

-record(state, {p :: riak_pipe_vnode:partition(),
                fd :: riak_pipe_fitting:details()}).
-opaque state() :: #state{}.
-export_type([state/0]).

%% @doc Initialization just stows the partition and fitting details in
%%      the module's state, for sending outputs in {@link process/3}.
-spec init(riak_pipe_vnode:partition(),
           riak_pipe_fitting:details()) ->
        {ok, state()}.
init(Partition, FittingDetails) ->
    {ok, #state{p=Partition, fd=FittingDetails}}.

%% @doc Process sends `Input' directly to the next fitting, and also
%%      `Input-1' back to this fitting as new input.
-spec process(term(), boolean(), state()) -> {ok, state()}.
process(Input, _Last, #state{p=Partition, fd=FittingDetails}=State) ->
    ?T(FittingDetails, [], {input, Input, Partition}),
    ok = riak_pipe_vnode_worker:send_output(Input, Partition, FittingDetails),
    if Input =< 0 ->
            ok;
       Input == 1, FittingDetails#fitting_details.arg == testeoi ->
            ?T(FittingDetails, [], {zero1, Partition}),
            ok = riak_pipe_vnode_worker:recurse_input(
                   0, Partition, FittingDetails),
            ?T(FittingDetails, [], {zero2, Partition}),
            ok = riak_pipe_vnode_worker:recurse_input(
                   0, Partition, FittingDetails),
            timer:sleep(1000),
            ?T(FittingDetails, [], {zero3, Partition}),
            ok = riak_pipe_vnode_worker:recurse_input(
                   0, Partition, FittingDetails);
       true ->
            ?T(FittingDetails, [], {recinput, Input-1, Partition}),
            ok = riak_pipe_vnode_worker:recurse_input(
                   Input-1, Partition, FittingDetails)
    end,
    {ok, State}.

%% @doc Unused.
-spec done(state()) -> ok.
done(_State) ->
    ?T(_State#state.fd, [], {done, _State#state.p}),
    ok.
