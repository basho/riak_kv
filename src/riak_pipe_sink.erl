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

%% @doc Methods for sending messages to the sink.
%%
%%      Sink messages are delivered as three record types:
%%      `#pipe_result{}', `#pipe_log{}', and `#pipe_eoi{}'.
-module(riak_pipe_sink).

-export([
         result/4,
         log/4,
         eoi/2,
         valid_sink_type/1
        ]).

-include("riak_pipe.hrl").

-ifdef(PULSE).
-include_lib("pulse/include/pulse.hrl").
%% have to transform the 'receive' of the work results
-compile({parse_transform, pulse_instrument}).
%% don't trasnform toplevel test functions
-compile({pulse_replace_module,[{gen_fsm_compat,pulse_gen_fsm}]}).
-endif.

-export_type([sink_type/0]).
-type sink_type() :: raw
                   | {fsm, Period::integer(), Timeout::timeout()}.

%% @doc Send a result to the sink (used by worker processes).  The
%%      result is delivered as a `#pipe_result{}' record in the sink
%%      process's mailbox.
-spec result(term(), Sink::riak_pipe:fitting(), term(),
             riak_pipe:exec_opts()) ->
         ok | {error, term()}.
result(From, #fitting{pid=Pid, ref=Ref, chashfun=sink}, Output, Opts) ->
    send_to_sink(Pid,
                 #pipe_result{ref=Ref, from=From, result=Output},
                 sink_type(Opts)).

%% @doc Send a log message to the sink (used by worker processes and
%%      fittings).  The message is delivered as a `#pipe_log{}' record
%%      in the sink process's mailbox.
-spec log(term(), Sink::riak_pipe:fitting(), term(), list()) ->
         ok | {error, term()}.
log(From, #fitting{pid=Pid, ref=Ref, chashfun=sink}, Msg, Opts) ->
    send_to_sink(Pid, #pipe_log{ref=Ref, from=From, msg=Msg},
                 sink_type(Opts)).

%% @doc Send an end-of-inputs message to the sink (used by fittings).
%%      The message is delivered as a `#pipe_eoi{}' record in the sink
%%      process's mailbox.
-spec eoi(Sink::riak_pipe:fitting(), list()) ->
         ok | {error, term()}.
eoi(#fitting{pid=Pid, ref=Ref, chashfun=sink}, Opts) ->
    send_to_sink(Pid, #pipe_eoi{ref=Ref},
                 sink_type(Opts)).

%% @doc Learn the type of sink we're dealing with from the execution
%% options.
-spec sink_type(riak_pipe:exec_opts()) -> sink_type().
sink_type(Opts) ->
    case lists:keyfind(sink_type, 1, Opts) of
        {_, Type} ->
            Type;
        false ->
            raw
    end.

%% @doc Validate the type of sink given in the execution
%% options. Returns `true' if the type is valid, or `{false, Type}' if
%% invalid, where `Type' is what was found.
-spec valid_sink_type(riak_pipe:exec_opts()) -> true | {false, term()}.
valid_sink_type(Opts) ->
    case lists:keyfind(sink_type, 1, Opts) of
        {_, {fsm, Period, Timeout}}
          when (is_integer(Period) orelse Period == infinity),
              (is_integer(Timeout) orelse Timeout == infinity) ->
            true;
        %% other types as needed (fsm_async, for example) can go here
        {_, raw} ->
            true;
        false ->
            true;
        Other ->
            {false, Other}
    end.

%% @doc Do the right kind of communication, given the sink type.
-spec send_to_sink(pid(),
                   #pipe_result{} | #pipe_log{} | #pipe_eoi{},
                   sink_type()) ->
         ok | {error, term()}.
send_to_sink(Pid, Msg, raw) ->
    Pid ! Msg,
    ok;
send_to_sink(Pid, Msg, {fsm, Period, Timeout}) ->
    case get(sink_sync) of
        undefined ->
            %% never sync for an 'infinity' Period, but always sync
            %% first send for any other Period, to prevent worker
            %% restart from overwhelming the sink
            send_to_sink_fsm(Pid, Msg, Timeout, Period /= infinity, 0);
        Count ->
            %% integer is never > than atom, so X is not > 'infinity'
            send_to_sink_fsm(Pid, Msg, Timeout, Count >= Period, Count)
    end.

send_to_sink_fsm(Pid, Msg, _Timeout, false, Count) ->
    gen_fsm_compat:send_event(Pid, Msg),
    put(sink_sync, Count+1),
    ok;
send_to_sink_fsm(Pid, Msg, Timeout, true, _Count) ->
    try
        gen_fsm_compat:sync_send_event(Pid, Msg, Timeout),
        put(sink_sync, 0),
        ok
    catch
        exit:{timeout,_} ->
            {error, timeout};
        exit:{_Reason,{gen_fsm_compat,sync_send_event,_}} ->
            %% we don't care why it died, just that it did ('noproc'
            %% and 'normal' have been seen; others could be possible)
            {error, sink_died};
        exit:{_Reason,{pulse_gen_fsm,sync_send_event,_}} ->
            %% the pulse parse transform won't catch just the atom 'gen_fsm'
            {error, sink_died}
    end.

