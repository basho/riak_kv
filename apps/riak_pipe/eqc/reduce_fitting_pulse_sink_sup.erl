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

%% @doc Support module for reduce_fitting_pulse tests - supervisor for
%% fsm-style sink, like Riak KV uses.
-module(reduce_fitting_pulse_sink_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).
-export([start_sink/2,
         terminate_sink/1]).

%% Supervisor callbacks
-export([init/1]).

-ifdef(PULSE).
-include_lib("pulse/include/pulse.hrl").
%% have to transform the 'receive' of the work results
-compile({parse_transform, pulse_instrument}).
%% don't trasnform toplevel test functions
-compile({pulse_replace_module,[{supervisor,pulse_supervisor}]}).
-endif.

%%%===================================================================
%%% API functions
%%%===================================================================

%% @doc Start the supervisor.
-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% @doc Start a new worker under the supervisor.
-spec start_sink(pid(), reference()) -> {ok, pid()}.
start_sink(Owner, Ref) ->
    supervisor:start_child(?MODULE, [Owner, Ref]).

%% @doc Stop a worker immediately
-spec terminate_sink(pid()) -> ok | {error, term()}.
terminate_sink(Sink) ->
    supervisor:terminate_child(?MODULE, Sink).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

%% @doc Initialize the supervisor.  This is a `simple_one_for_one',
%% whose child spec is for starting `riak_kv_mrc_sink' FSMs.
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

    AChild = {undefined, % no registered name
              {reduce_fitting_pulse_sink, start_link, []},
              Restart, Shutdown, Type, [reduce_fitting_pulse_sink]},

    {ok, {SupFlags, [AChild]}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
