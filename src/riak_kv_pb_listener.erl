%% -------------------------------------------------------------------
%%
%% riak_kv_pb_listener: Listen for protocol buffer clients
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

%% @doc entry point for TCP-based protocol buffers service

-module(riak_kv_pb_listener).
-behavior(gen_nb_server).
-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).
-export([sock_opts/0, new_connection/2]).
-record(state, {portnum}).

start_link() ->
    PortNum = app_helper:get_env(riak_kv, pb_port),
    IpAddr = app_helper:get_env(riak_kv, pb_ip),
    gen_nb_server:start_link(?MODULE, IpAddr, PortNum, [PortNum]).

init([PortNum]) -> 
    {ok, #state{portnum=PortNum}}.

sock_opts() ->
    BackLog = app_helper:get_env(riak_kv, pb_backlog, 5),
    NoDelay = app_helper:get_env(riak_kv, disable_pb_nagle, true),
    [binary, {packet, 4}, {reuseaddr, true}, {backlog, BackLog}, {nodelay, NoDelay}].

handle_call(_Req, _From, State) -> 
    {reply, not_implemented, State}.

handle_cast(_Msg, State) -> {noreply, State}.

handle_info(_Info, State) -> {noreply, State}.

terminate(_Reason, _State) -> ok.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

new_connection(Socket, State) ->
    {ok, Pid} = riak_kv_pb_socket_sup:start_socket(),
    ok = gen_tcp:controlling_process(Socket, Pid),
    ok = riak_kv_pb_socket:set_socket(Pid, Socket),
    {ok, State}.

