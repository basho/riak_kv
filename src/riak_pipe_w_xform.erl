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

%% @doc Alter an input, and send the alteration as output.  For each
%%      input, the fitting evaluates a function (passed as the fitting
%%      argument).  That function should accept three parameters:
%%<dl><dt>
%%      `Input' :: term()
%%</dt><dd>
%%      Whatever the upstream fitting has sent.
%%</dd><dt>
%%      `Partition' :: riak_pipe_vnode:partition()
%%</dt><dd>
%%      The partition of the vnode on which this worker is running.
%%      Necessary for logging and sending output.
%%</dd><dt>
%%      `FittingDetails' :: #fitting_details{}
%%</dt><dd>
%%      The details for the fitting.  Also necessary for logging and
%%      sending output.
%%</dd></dl>
%%
%%      The function should use `Partition' and `FittingDetails' along
%%      with {@link riak_pipe_vnode_worker:send_output/3} to send
%%      whatever outputs it likes (this allows the fitting to be used
%%      as "filter" as well as "map").
-module(riak_pipe_w_xform).
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

%% @doc Process evaluates the fitting's argument function.
-spec process(term(), boolean(), state()) -> {ok, state()}.
process(Input, _Last, #state{p=Partition, fd=FittingDetails}=State) ->
    Fun = FittingDetails#fitting_details.arg,
    ok = riak_pipe_fun:compat_apply(
           Fun, [Input, Partition, FittingDetails]),
    {ok, State}.

%% @doc Unused.
-spec done(state()) -> ok.
done(_State) ->
    ok.

%% @doc Check that the argument is a valid function of arity 3.  See
%%      {@link riak_pipe_v_validate_function/3}.
-spec validate_arg(term()) -> ok | {error, iolist()}.
validate_arg(Fun) when is_function(Fun) ->
    riak_pipe_v:validate_function("arg", 3, Fun);
validate_arg(Fun) ->
    {error, io_lib:format("~p requires a function as argument, not a ~p",
                          [?MODULE, riak_pipe_v:type_of(Fun)])}.
