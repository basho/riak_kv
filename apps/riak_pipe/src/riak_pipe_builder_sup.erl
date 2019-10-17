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

%% @doc Supervisor for the pipe builder processes.
%%
%%      This supervisor is mostly convenience for later investigation,
%%      to learn how many pipelines are active.  No restart strategy
%%      is used.
-module(riak_pipe_builder_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).
-export([new_pipeline/2,
         pipelines/0,
         builder_pids/0]).

%% Supervisor callbacks
-export([init/1]).

-include("riak_pipe.hrl").

-ifdef(PULSE).
-include_lib("pulse/include/pulse.hrl").
%% have to transform the 'receive' of the work results
-compile({parse_transform, pulse_instrument}).
%% don't trasnform toplevel test functions
-compile({pulse_replace_module,[{supervisor,pulse_supervisor}]}).
-endif.

-define(SERVER, ?MODULE).

%%%===================================================================
%%% API functions
%%%===================================================================

%% @doc Start the supervisor.  It will be registered under the atom
%%      `riak_pipe_builder_sup'.
-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%% @doc Start a new pipeline builder.  Starts the builder process
%%      under this supervisor.
-spec new_pipeline([#fitting_spec{}], riak_pipe:exec_opts()) ->
         {ok, Pipe::#pipe{}} | {error, Reason::term()}.
new_pipeline(Spec, Options) ->
    case supervisor:start_child(?MODULE, [Spec, Options]) of
        {ok, Pid, Ref} ->
            case riak_pipe_builder:pipeline(Pid) of
                {ok, #pipe{sink=#fitting{ref=Ref}}=Pipe} ->
                    riak_pipe_stat:update({create, Pid}),
                    {ok, Pipe};
                _ ->
                    riak_pipe_stat:update(create_error),
                    {error, startup_failure}
            end;
        Error ->
            riak_pipe_stat:update(create_error),
            Error
    end.

%% @doc Get the list of pipelines hosted on this node.
-spec pipelines() -> [#pipe{}].
pipelines() ->
    Children = builder_pids(),
    Responses = [ riak_pipe_builder:pipeline(BuilderPid)
                  || BuilderPid <- Children ],
    %% filter out gone responses
    [ P || {ok, #pipe{}=P} <- Responses ].

%% @doc Get information about the builders supervised here.
-spec builder_pids() -> [term()].
builder_pids() ->
    [ Pid || {_, Pid, _, _} <- supervisor:which_children(?SERVER) ].

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

%% @doc Initialize this supervisor.  This is a `simple_one_for_one',
%%      whose child spec is for starting `riak_pipe_builder' FSMs.
-spec init([]) -> {ok, {{supervisor:strategy(),
                         pos_integer(),
                         pos_integer()},
                        [ supervisor:child_spec() ]}}.
init([]) ->
    RestartStrategy = simple_one_for_one,
    MaxRestarts = 1000,
    MaxSecondsBetweenRestarts = 3600,

    SupFlags = {RestartStrategy, MaxRestarts, MaxSecondsBetweenRestarts},

    Restart = temporary,
    Shutdown = brutal_kill,
    Type = worker,

    Child = {undefined, {riak_pipe_builder, start_link, []},
             Restart, Shutdown, Type, [riak_pipe_builder]},

    {ok, {SupFlags, [Child]}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
