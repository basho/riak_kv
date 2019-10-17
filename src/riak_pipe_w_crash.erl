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

%% @doc It's what we do: crash.

-module(riak_pipe_w_crash).
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

%% name of the table reMEMbering restarts
-define(MEM, ?MODULE).

%% @doc Initialization just stows the partition and fitting details in
%%      the module's state, for sending outputs in {@link process/3}.
-spec init(riak_pipe_vnode:partition(),
           riak_pipe_fitting:details()) ->
        {ok, state()}.
init(Partition, FittingDetails) ->
    case FittingDetails#fitting_details.arg of
        init_exit ->
            exit(crash);
        init_badreturn ->
            crash;
        init_restartfail ->
            case is_restart(Partition, FittingDetails) of
                true ->
                    restart_crash;
                false ->
                    {ok, #state{p=Partition, fd=FittingDetails}}
            end;
        _ ->
            {ok, #state{p=Partition, fd=FittingDetails}}
    end.

is_restart(Partition, FittingDetails) ->
    Fitting = FittingDetails#fitting_details.fitting,
    %% set the fitting coordinator as the heir, such that the ets
    %% table survives when this worker exits, but gets cleaned
    %% up when the pipeline shuts down
    case (catch ets:new(?MEM, [set, {keypos, 1},
                               named_table, public,
                               {heir, Fitting#fitting.pid, ok}])) of
        {'EXIT',{badarg,_}} ->
            %% table was already created
            ok;
        ?MEM ->
            %% table is now created
            ok
    end,
    case ets:lookup(?MEM, Partition) of
        [] ->
            %% no record - not restart;
            %% make a record for the next start to check
            ets:insert(?MEM, {Partition, true}),
            false;
        [{Partition, true}] ->
            true
    end.

%% @doc Process just sends `Input' directly to the next fitting.  This
%%      function also generates two trace messages: `{processing,
%%      Input}' before sending the output, and `{processed, Input}' after
%%      the blocking output send has returned.  This can be useful for
%%      dropping in another pipeline to watching data move through it.
-spec process(term(), boolean(), state()) -> {ok, state()}.
process(Input, _Last, #state{p=Partition, fd=FittingDetails}=State) ->
    ?T(FittingDetails, [], {processing, Input}),
    case FittingDetails#fitting_details.arg of
        Input ->
            if Input == init_restartfail ->
                    %% "worker restart failure, input forwarding" test in
                    %% riak_pipe exploits this timer:sleep to test both
                    %% moments of forwarding: those items left in a
                    %% failed-restart worker's queue, as well as those items
                    %% sent to a worker that is already forwarding
                    timer:sleep(1000);
               true -> ok
            end,
            ?T(FittingDetails, [], {crashing, Input}),
            exit(process_input_crash);
        {recurse_done_pause, _} ->
            %% "restart after eoi" test in riak_pipe uses this
            %% behavior see done/1 for more details
            ok = case Input of
                     [_] -> ok;
                     [_|More] ->
                         timer:sleep(100),
                         riak_pipe_vnode_worker:recurse_input(
                           More, Partition, FittingDetails)
                 end,
            {ok, State};
        _Other ->
            ok = riak_pipe_vnode_worker:send_output(
                   Input, Partition, FittingDetails),
            ?T(FittingDetails, [], {processed, Input}),
            {ok, State}
    end.

%% @doc Unused.
-spec done(state()) -> ok.
done(#state{fd=FittingDetails}) ->
    case FittingDetails#fitting_details.arg of
        {recurse_done_pause, Time} ->
            %% "restart after eoi" test in riak_pipe exploits this
            %% sleep to get more input queued while the worker is
            %% shutting down
            timer:sleep(Time);
        _ ->
            ok
    end.
