%% -------------------------------------------------------------------
%%
%% riak_api_sup: supervise the Riak API services
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc supervise the Riak API services

-module(riak_api_sup).

-behaviour(supervisor).

-export([start_link/0,
         init/1]).

-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).
-define(CHILD(I, Type, Args), {I, {I, start_link, Args}, permanent, 5000, Type, [I]}).
-define(LNAME(IP, Port), lists:flatten(io_lib:format("pb://~p:~p", [IP, Port]))).
-define(PB_LISTENER(IP, Port), {?LNAME(IP, Port),
                                {riak_api_pb_listener, start_link, [IP, Port]},
                                permanent, 5000, worker, [riak_api_pb_listener]}).
%% @doc Starts the supervisor.
-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% @doc The init/1 supervisor callback, initializes the supervisor.
-spec init(list()) -> {ok,{{RestartStrategy,MaxR,MaxT},[ChildSpec]}} | ignore when
      RestartStrategy :: supervisor:strategy(),
      MaxR :: pos_integer(),
      MaxT :: pos_integer(),
      ChildSpec :: supervisor:child_spec().
init([]) ->
    Helper = ?CHILD(riak_api_pb_registration_helper, worker),
    Registrar = ?CHILD(riak_api_pb_registrar, worker),
    PBProcesses = pb_processes(riak_api_pb_listener:get_listeners()),
    WebProcesses = web_processes(riak_api_web:get_listeners()),
    NetworkProcesses = PBProcesses ++ WebProcesses,
    {ok, {{one_for_one, 10, 10}, [Helper, Registrar|NetworkProcesses]}}.

%% Generates child specs from the HTTP/HTTPS listener configuration.
%% @private
web_processes([]) ->
    lager:info("No HTTP/HTTPS listeners were configured, HTTP connections will be disabled."),
    [];
web_processes(Listeners) ->
    lists:flatten([ web_listener_spec(Scheme, Binding) ||
                      {Scheme, Binding} <- Listeners ]).

web_listener_spec(Scheme, Binding) ->
    riak_api_web:binding_config(Scheme, Binding).

%% Generates child specs from the PB listener configuration.
%% @private
pb_processes([]) ->
    lager:info("No PB listeners were configured, PB connections will be disabled."),
    [];
pb_processes(Listeners) ->
    [?CHILD(riak_api_pb_sup, supervisor)| pb_listener_specs(Listeners)].

pb_listener_specs(Pairs) ->
    [ ?PB_LISTENER(IP, Port) || {IP, Port} <- Pairs ].
