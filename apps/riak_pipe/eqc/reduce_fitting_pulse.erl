%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc Test riak_pipe_fitting for no-input, reduce-once-anyway
%% behavior, possibly related to github issues 48 and 49.
-module(reduce_fitting_pulse).

-include("include/riak_pipe.hrl").
-include("include/riak_pipe_log.hrl").

-behaviour(riak_pipe_vnode_worker).
-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-endif.
-include_lib("eunit/include/eunit.hrl").

%% riak_pipe_worker behavior
-export([no_input_run_reduce_once/0,
         init/2,
         process/3,
         done/1]).

%% console debugging convenience
-compile(export_all).

-ifdef(PULSE).
-include_lib("pulse/include/pulse.hrl").
%% have to transform the 'receive' of the work results
-compile({parse_transform, pulse_instrument}).
%% don't trasnform toplevel test functions
-compile({pulse_skip,[{death_test_,0}]}).
-endif.

%%% Worker Definition This is a rough copy of important parts of
%% riak_kv_w_reduce. The important bit is that it defines
%% no_input_run_reduce_once/0 as true, so that the fitting process
%% will run these functions itself, instead of evaulating them in a
%% vnode worker.

no_input_run_reduce_once() ->
    true.

init(Partition, #fitting_details{}=Details) ->
    maybe_send_trace(Details),
    {ok, {Partition, Details}}.

process(_Input, _Last, State) ->
    %% we're only concerned with the zero-worker case, which is
    %% easiest to replicate with zero inputs
    throw(this_should_not_be_part_of_the_test),
    {ok, State}.

done(State) ->
    maybe_send_output(State),
    ok.

maybe_send_trace({_, #fitting_details{arg=Arg}=Details}) ->
    case lists:member(send_trace, Arg) of
        true ->
            ?T(Details, [maybe_send_trace], sending_trace);
        false ->
            ok
    end.

maybe_send_output({Partition, #fitting_details{arg=Arg}=Details}) ->
    case lists:member(send_output, Arg) of
        true ->
            riak_pipe_vnode_worker:send_output(
              an_output, Partition, Details);
        false ->
            ok
    end.

%% none of these tests make sense if PULSE is not used
-ifdef(PULSE).

%% @doc Nothing should ever cause the fitting to exit abnormally.
%% This is a bit of a wide net to cast, with more options than are
%% likely necessary to find edge cases. It was constructed while
%% searching for the unknown source of github issues 48 and 49.
%% Although only a small corner provoked failures, such coverage may
%% be useful when making changes in the future, so it's staying in.
prop_fitting_dies_normal() ->
    ?SETUP(fun() ->
                   pulse:start(),
                   fun() -> pulse:stop() end
           end,
           ?FORALL({Seed, Trace, Output, SinkType,
                    {Eoi, Destroy, AlsoDestroySink, ExitPoint}},
                   {pulse:seed(), bool(), bool(), oneof([fsm, raw]),
                    ?LET({Eoi, Destroy},
                         {bool(), bool()},
                         {Eoi, Destroy,
                          %% to mimick riak_kv HTTP&PB endpoints, we only
                          %% destroy the sink if we also destroy the pipe
                          oneof([Destroy, false]),
                          oneof([before_eoi]
                                %% can't exit after eoi if there's no eoi
                                ++ [ after_eoi || Eoi]
                                %% similarly for destroy
                                ++ [ after_destroy || Destroy]
                                %% and "never" exiting requires at least one of
                                %% eoi or destroy, or the test will hang
                                ++ [ never || Eoi or Destroy])})},

                   %% this is a gigantic table to collect, but useful to see anyway
                   collect({Trace, Output, SinkType,
                            Eoi, Destroy, AlsoDestroySink, ExitPoint},

                           begin
                               ExitReasons =
                               fitting_exit_reason(
                                 Seed, Trace, Output, SinkType,
                                 Eoi, Destroy, AlsoDestroySink, ExitPoint),
                               ?WHENFAIL(
                                  io:format(user, "Exit Reasons: ~p~n", [ExitReasons]),
                                  exit_reason_is_normalish(
                                    proplists:get_value(fitting, ExitReasons)) andalso
                                  proplists:get_value(client, ExitReasons) == ExitPoint)
                           end))).

exit_reason_is_normalish(normal) ->
    %% the fitting chose to exit on its own
    true;
exit_reason_is_normalish(shutdown) ->
    %% the fitting's supervisor shut it down
    true.

%% @doc This is a nice convenience to have when re-running failures
%% manually.
fitting_exit_reason(Seed, Trace, Output, SinkType,
                    Eoi, Destroy, AlsoDestroySink, ExitPoint) ->
    pulse:run_with_seed(
      fun() ->
              fitting_exit_reason(Trace, Output, SinkType,
                                  Eoi, Destroy, AlsoDestroySink, ExitPoint)
      end,
      Seed).

-endif. %% PULSE

%% PULSE is not *required* to run this code. The code is much more
%% interesting with PULSE enabled, but it may be useful to run without
%% PULSE to sanity check.

%% @doc The actual run.
fitting_exit_reason(Trace, Output, SinkType,
                    Eoi, Destroy, AlsoDestroySink, ExitPoint) ->
    Supervisors = [riak_pipe_builder_sup,
                   riak_pipe_fitting_sup,
                   reduce_fitting_pulse_sink_sup],
    [ {ok,_} = Sup:start_link() || Sup <- Supervisors ],

    %% we want the client linked to this process so that it dies if
    %% the test dies, but we don't want death to spread the other way
    erlang:process_flag(trap_exit, true),
    ClientRef = make_ref(),
    Self = self(),
    Client = spawn_link(
               fun() ->
                       pipe_client({ClientRef, Self}, Trace, Output, SinkType,
                                   Eoi, Destroy, AlsoDestroySink, ExitPoint)
               end),

    Pipe = receive
               {ClientRef, PipeDef} ->
                   PipeDef
           end,

    %% monitor before the client proceeds with the test, to
    %% ensure we get the actual exit reason, instead of a
    %% bogus noproc
    FittingMonitor = monitor(process, fitting_process(Pipe)),

    %% we're not checking the builder at the moment, but watching for
    %% this 'DOWN' prevents us from killing the builder supervisor
    %% before the builder exits
    BuilderMonitor = monitor(process, builder_process(Pipe)),

    %% for 'raw' sink type, Sink and Client are the same, but our
    %% checks should be fine with that
    SinkMonitor = monitor(process, sink_process(Pipe)),
    Client ! {ClientRef, ok},

    Reasons = receive_exits([{fitting, FittingMonitor},
                             {builder, BuilderMonitor},
                             {sink, SinkMonitor},
                             {client, Client}]),

    [ begin
          unlink(whereis(Sup)),
          exit(whereis(Sup), kill)
      end
      || Sup <- Supervisors ],
    Reasons.

fitting_process(#pipe{fittings=[{fake_reduce, #fitting{pid=Pid}}]}) ->
    Pid.

builder_process(#pipe{builder=Builder}) ->
    Builder.

sink_process(#pipe{sink=#fitting{pid=Pid}}) ->
    Pid.

receive_exits([]) ->
    [];
receive_exits(Waiting) ->
    {Tag, Reason} = receive
                        {'DOWN', Monitor, process, _Pid, R} ->
                            {Monitor, R};
                        {'EXIT', Pid, R} ->
                            {Pid, R}
                    end,
    case lists:keytake(Tag, 2, Waiting) of
        {value, {Name, Tag}, Rest} ->
            [{Name, Reason}|receive_exits(Rest)];
        false ->
            receive_exits(Waiting)
    end.

pipe_client({Ref, Test}, Trace, Output, SinkType,
            Eoi, Destroy, AlsoDestroySink, ExitPoint) ->
    Options = case SinkType of
                  fsm ->
                      %% mimicking riak_kv here, using a supervisor
                      %% for the sink instead of bare linking, in case
                      %% that makes a difference (though it doesn't
                      %% seem to)
                      {ok, S} = reduce_fitting_pulse_sink_sup:start_sink(
                                  self(), Ref),
                      [{sink, #fitting{pid=S, ref=Ref, chashfun=sink}},
                       {sink_type, {fsm, 1, infinity}}];
                  raw ->
                      [{sink, #fitting{pid=self(), ref=Ref, chashfun=sink}},
                       {sink_type, raw}]
              end,

    {ok, Pipe} = riak_pipe:exec(
                   [#fitting_spec{name=fake_reduce,
                                  module=?MODULE,
                                  arg=[send_trace || Trace]++
                                      [send_output || Output]}],
                   Options),

    case SinkType of
        fsm ->
            reduce_fitting_pulse_sink:use_pipe(
              sink_process(Pipe), Ref, Pipe);
        raw ->
            ok
    end,
    
    Test ! {Ref, Pipe},
    receive {Ref, ok} -> ok end,
    
    maybe_exit(before_eoi, ExitPoint),
    
    case Eoi of
        true ->
            riak_pipe:eoi(Pipe),
            maybe_exit(after_eoi, ExitPoint);
        false -> ok
    end,

    case Destroy of
        true ->
            riak_pipe:destroy(Pipe),
            case {AlsoDestroySink, SinkType} of
                {true, fsm} ->
                    reduce_fitting_pulse_sink_sup:terminate_sink(
                      sink_process(Pipe));
                _ ->
                    %% *this* process is the sink if we're not using fsm
                    %% (to mimick earlier Riak KV versions)
                    ok
            end,
            maybe_exit(after_destroy, ExitPoint);
        false ->
            case SinkType of
                fsm ->
                    %% we don't care if this call fails, just that it
                    %% doesn't return until the sink has finished its
                    %% work; mimicking riak_kv endpoints that don't
                    %% wait for results if they destroy
                    catch reduce_fitting_pulse_sink:all_results(
                            sink_process(Pipe), Ref);
                raw ->
                    %% *this* process is the sink if we're not using fsm
                    %% (to mimick earlier Riak KV versions)
                    receive #pipe_eoi{ref=Ref} -> ok end
            end
    end,

    %% we can't let the exit be 'normal', because the test process
    %% will never see it (it's linking not monitoring)
    exit(ExitPoint).

maybe_exit(Now, Now) ->    
    exit(Now);
maybe_exit(_Now, _NotNow) -> 
    ok.
