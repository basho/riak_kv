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

%% @doc Send inputs to another fitting (often the sink) in addition to
%%      the output.
%%
%%      If the argument to this fitting is the atom `sink', every
%%      input will be sent to the sink.  If the argument is a
%%      `#fitting{}' record, every input will be sent to that fitting.
-module(riak_pipe_w_tee).
-behaviour(riak_pipe_vnode_worker).

-export([init/2,
         process/3,
         done/1,
         validate_arg/1]).

-include("riak_pipe.hrl").
-include("riak_pipe_log.hrl").

-record(state, {p :: riak_pipe_vnode:partition(),
                fd :: riak_pipe_fitting:details()}).
-opaque state() :: #state{}.
-export_type([state/0]).

%% @doc Init just stashes the `Partition' and `FittingDetails' for later.
-spec init(riak_pipe_vnode:partition(), riak_pipe_fitting:details()) ->
         {ok, state()}.
init(Partition, FittingDetails) ->
    {ok, #state{p=Partition, fd=FittingDetails}}.

%% @doc Processing an input involves sending it to both the fitting
%%      specified by the argument (possibly the sink), and to the
%%      output.
-spec process(term(), boolean(), state()) -> {ok, state()}.
process(Input, _Last, #state{p=Partition, fd=FittingDetails}=State) ->
    Tee = case FittingDetails#fitting_details.arg of
              sink ->
                  proplists:get_value(
                    sink, FittingDetails#fitting_details.options);
              #fitting{}=Fitting ->
                  Fitting
          end,
    ok = riak_pipe_vnode_worker:send_output(
           Input, Partition, FittingDetails, Tee, infinity),
    ok = riak_pipe_vnode_worker:send_output(Input, Partition, FittingDetails),
    {ok, State}.

%% @doc Unused.
-spec done(state()) -> ok.
done(_State) ->
    ok.

%% @doc Check that the fitting's argument is either the atom `sink' or
%%      a `#fitting{}' record.
-spec validate_arg(term()) -> ok | {error, iolist()}.
validate_arg(sink)   -> ok;
validate_arg(#fitting{}) -> ok;
validate_arg(Other) ->
    {error, io_lib:format("~p requires a fitting record,"
                          " or the atom 'sink'"
                          " as its argument, not a ~p",
                          [?MODULE, riak_pipe_v:type_of(Other)])}.
