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

%% @doc Logging support for pipes.

-module(riak_pipe_log).

-export([log/2,
         trace/3]).

-include("riak_pipe.hrl").

-export_type([trace_filter/0]).

-ifdef(namespaced_types).
-type riak_pipe_log_set() :: sets:set().
-else.
-type riak_pipe_log_set() :: set().
-endif.

-type trace_filter() :: all | riak_pipe_log_set() | trace_compiled().
-type trace_compiled() :: ordsets:ordset(term()).

%% @doc Log the given message, if logging is enabled, to the specified
%%      log target.  Logging is enabled and directed via the `log'
%%      option passed to {@link riak_pipe:exec/2}.  If the option was
%%      set to `sink', log messages are sent to the sink.  If the
%%      option was set to `sasl', log messages are printed via
%%      `error_logger' to the SASL log.  If the option was set to
%%      `lager', log messages are printed via `lager' to the Riak
%%      node's log.  If no option was given, log messages are
%%      discarded.
-spec log(riak_pipe_fitting:details(), term()) -> ok.
log(#fitting_details{options=O, name=N}, Msg) ->
    case proplists:get_value(log, O) of
        undefined ->
            ok; %% no logging
        sink ->
            Sink = proplists:get_value(sink, O),
            riak_pipe_sink:log(N, Sink, Msg, O);
        {sink, Sink} ->
            riak_pipe_sink:log(N, Sink, Msg, O);
        lager ->
            lager:info(
              "~s: ~P",
              [riak_pipe_fitting:format_name(N), Msg, 9]);
        sasl ->
            error_logger:info_msg(
              "Pipe log -- ~s:~n~P",
              [riak_pipe_fitting:format_name(N), Msg, 9])
    end.

%% @doc Log a trace message.  If any of the `Types' given matches any
%%      of the types in the `trace' option that was passed to {@link
%%      riak_pipe:exec/2} (or if `trace' was set to `all'), the trace
%%      message will be sent to the log target (if logging is enabled;
%%      see {@link log/2}).  If no `trace' option was given, or no
%%      type matches, the message is discarded.
%%
%%      The `node()' and the name of the fitting will be added to the
%%      `Types' list - the calling function does not need to specify
%%      them.
-spec trace(riak_pipe_fitting:details(), [term()], term()) -> ok.
trace(#fitting_details{options=O, name=N}=FD, Types, Msg) ->
    TraceOn = case proplists:get_value(trace, O) of
                  all ->
                      {true, all};
                  undefined ->
                      false;
                  EnabledSet when is_list(EnabledSet) ->
                      %% ordsets (post 1.2)
                      find_enabled([N|Types], EnabledSet);
                  EnabledSet ->
                      %% sets (1.2 and earlier)
                      OS = ordsets:from_list(sets:to_list(EnabledSet)),
                      find_enabled([N|Types], OS)
              end,
    case TraceOn of
        {true, Traces} -> log(FD, {trace, Traces, Msg});
        false          -> ok
    end.

-spec find_enabled(list(), trace_compiled()) ->
         {true, list()} | false.
find_enabled(Types, Enabled) ->
    MatchSet = ordsets:from_list([node()|Types]),
    Intersection = ordsets:intersection(Enabled, MatchSet),
    case Intersection of
        [] -> false;
        _ -> {true, Intersection}
    end.
