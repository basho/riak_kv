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

%% @doc Supervisor of fitting processes.
-module(riak_pipe_fitting_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).
-export([add_fitting/4,
         terminate_fitting/1]).

%% Supervisor callbacks
-export([init/1]).

-include("riak_pipe.hrl").
-include("riak_pipe_debug.hrl").

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
%%      `riak_pipe_fitting_sup'.
-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%% @doc Start a new fitting coordinator under this supervisor.
-spec add_fitting(pid(),
                  riak_pipe:fitting_spec(),
                  riak_pipe:fitting(),
                  riak_pipe:exec_opts()) ->
         {ok, pid(), riak_pipe:fitting()}.
add_fitting(Builder, Spec, Output, Options) ->
    ?DPF("Adding fitting for ~p", [Spec]),
    supervisor:start_child(?SERVER, [Builder, Spec, Output, Options]).

%% @doc Terminate a coordinator immediately.  Useful for tearing down
%% pipelines that may be otherwise swamped with messages from
%% restarting workers.
-spec terminate_fitting(riak_pipe:fitting()) -> ok | {error, term()}.
terminate_fitting(#fitting{pid=Pid}) ->
    supervisor:terminate_child(?SERVER, Pid).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

%% @doc Initialize this supervisor.  This is a `simple_one_for_one',
%%      whose child spec is for starting `riak_pipe_fitting' FSMs.
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
    Shutdown = 2000,
    Type = worker,

    Child = {undefined,
             {riak_pipe_fitting, start_link, []},
             Restart, Shutdown, Type, [riak_pipe_fitting]},

    {ok, {SupFlags, [Child]}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
