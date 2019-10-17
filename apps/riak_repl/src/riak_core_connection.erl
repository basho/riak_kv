%% -------------------------------------------------------------------
%%
%% Riak Subprotocol Server Dispatch and Client Connections
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

%% @doc This module handles the protocol negotiation for new connections.
%% Basic operation is this:
%%
%% Connections are initally a raw tcp socket {packet, 4}.
%% <ol>
%% <li>Initally the client sends `term_to_binary({?CTRL_HELLO, ?CTRL_REV, MyCaps}).'</li>
%% <li>The server sends `term_to_binary({?CTRL_ACK, ?CTRL_REV, TheirCaps}).'</li>
%% <li>If the server and client agree on SSL, the session is upgraded.</li>
%% <li>The client sends `term_to_binary({Protocol, Version}).'</li>
%% <li>The server sends `term_to_binary({ok, {ProtoName, {CommonMajor, RemoteMinor, LocalMinor}}})',</li>
%% <li>after which we call ?MODULE:connect/6 and exit.</li>

-module(riak_core_connection).
-behavior(gen_fsm_compat).

-include("riak_core_connection.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(state, {
    transport, socket, protocol, protovers, socket_opts, mod, mod_args,
    cluster_name, local_capabilities, remote_capabilities, ip, port
}).

%% public API
-export([connect/2,
         sync_connect/2]).
-export([start_link/2, start_link/7, start/2, start/7]).
%% gen_fsm_compat
-export([init/1]).
-export([
    wait_for_capabilities/3, wait_for_capabilities/2,
    wait_for_protocol/3, wait_for_protocol/2
]).
-export([handle_sync_event/4, handle_event/3, handle_info/3]).
-export([terminate/3, code_change/4]).

%%TODO: move to riak_ring_core...symbolic cluster naming
-export([set_symbolic_clustername/2, set_symbolic_clustername/1,
         symbolic_clustername/1, symbolic_clustername/0]).

%% @doc Sets the symbolic, human readable, name of this cluster.
set_symbolic_clustername(Ring, ClusterName) ->
    {new_ring,
     riak_core_ring:update_meta(symbolic_clustername, ClusterName, Ring)}.

set_symbolic_clustername(ClusterName) when is_list(ClusterName) ->
    {error, "argument is not a string"};
set_symbolic_clustername(ClusterName) ->
    case riak_core_ring_manager:get_my_ring() of
        {ok, _Ring} ->
            riak_core_ring_manager:ring_trans(fun riak_core_connection:set_symbolic_clustername/2,
                                              ClusterName);
        {error, Reason} ->
            lager:error("Can't set symbolic clustername because: ~p", [Reason])
    end.

symbolic_clustername(Ring) ->
    case riak_core_ring:get_meta(symbolic_clustername, Ring) of
        {ok, Name} -> Name;
        undefined -> "undefined"
    end.

%% @doc Returns the symbolic, human readable, name of this cluster.
symbolic_clustername() ->
    case riak_core_ring_manager:get_my_ring() of
        {ok, Ring} ->
            symbolic_clustername(Ring);
        {error, Reason} ->
            lager:error("Can't read symbolic clustername because: ~p", [Reason]),
            "undefined"
    end.

%% Make async connection request. The connection manager is responsible for retry/backoff
%% and calls your module's functions on success or error (asynchrously):
%%   Module:connected(Socket, TransportModule, {IpAddress, Port}, {Proto, MyVer, RemoteVer}, Args)
%%   Module:connect_failed(Proto, {error, Reason}, Args)
%%       Reason could be 'protocol_version_not_supported"
%%
%%
%% You can set options on the tcp connection, e.g.
%% [{packet, 4}, {active, false}, {keepalive, true}, {nodelay, true}]
%%
%% ClientProtocol specifies the preferences of the client, in terms of what versions
%% of a protocol it speaks. The host will choose the highest common major version and
%% inform the client via the callback Module:connected() in the HostProtocol parameter.
%%
%% Note: that the connection will initially be setup with the `binary` option
%% because protocol negotiation requires binary. TODO: should we allow non-binary
%% options? Check that binary is not overwritten.
%%
%% connect returns the pid() of the asynchronous process that will attempt the connection.

-spec(connect(ip_addr(), clientspec()) -> pid()).
connect(IPPort, ClientSpec) ->
    start(IPPort, ClientSpec).

-spec sync_connect(ip_addr(), clientspec()) -> ok | {error, atom()}.
sync_connect(IPPort, ClientSpec) ->
    case start(IPPort, ClientSpec) of
        {ok, Pid} ->
            Mon = erlang:monitor(process, Pid),
            receive
                {'DOWN', Mon, process, Pid, normal} ->
                    ok;
                {'DOWN', Mon, process, Pid, Why} ->
                    case Why of
                        {error, _Wut} ->
                            Why;
                        _ ->
                            {error, Why}
                    end
            end;
        ignore ->
            {error, could_not_connect};
        Else ->
            lager:debug("start returned ~p", [Else]),
            Else
    end.

start_link({Ip, Port}, ClientSpec) ->
    {{Protocol, ProtoVers}, {TcpOptions, CallbackMod, CallbackArgs}} = ClientSpec,
    start_link(Ip, Port, Protocol, ProtoVers, TcpOptions, CallbackMod, CallbackArgs).

start_link(Ip, Port, Protocol, ProtoVers, SocketOptions, Mod, ModArgs) ->
    start_maybe_link(Ip, Port, Protocol, ProtoVers, SocketOptions, Mod, ModArgs, start_link).

start({IP, Port}, ClientSpec) ->
    {{Protocol, ProtoVers}, {TcpOptions, CallbackMod, CallbackArgs}} = ClientSpec,
    start(IP, Port, Protocol, ProtoVers, TcpOptions, CallbackMod, CallbackArgs).

start(Ip, Port, Protocol, ProtoVers, TcpOptions, CallbackMod, CallbackArgs) ->
    start_maybe_link(Ip, Port, Protocol, ProtoVers, TcpOptions, CallbackMod, CallbackArgs, start).

start_maybe_link(Ip, Port, Protocol, ProtoVers, SocketOptions, Mod, ModArgs, StartFunc) ->
    gen_fsm_compat:StartFunc(?MODULE, {Ip, Port, Protocol, ProtoVers, SocketOptions, Mod, ModArgs}, []).

%% gen_fsm_compat callbacks

init({IP, Port, Protocol, ProtoVers, SocketOptions, Mod, ModArgs}) ->
    case gen_tcp:connect(IP, Port, ?CONNECT_OPTIONS, ?CONNECTION_SETUP_TIMEOUT) of
        {ok, Socket} ->
            ok = ranch_tcp:setopts(Socket, ?CONNECT_OPTIONS),
            SSLEnabled = app_helper:get_env(riak_core, ssl_enabled, false),
            MyName = symbolic_clustername(),
            MyCaps = [{clustername, MyName}, {ssl_enabled, SSLEnabled}],
            Hello = term_to_binary({?CTRL_HELLO, ?CTRL_REV, MyCaps}),
            ok = ranch_tcp:send(Socket, Hello),
            ranch_tcp:setopts(Socket, [{active, once}]),
            State = #state{transport = ranch_tcp,
                           socket = Socket,
                           protocol = Protocol,
                           protovers = ProtoVers,
                           socket_opts = SocketOptions,
                           mod = Mod,
                           mod_args = ModArgs,
                           cluster_name = MyName,
                           local_capabilities = MyCaps,
                           ip = IP,
                           port = Port},
            {ok, wait_for_capabilities, State};
        Else ->
            lager:warning("Could not connect ~p:~p due to ~p", [IP, Port, Else]),
            ignore
    end.

wait_for_capabilities(_Req, _From, State) ->
    {reply, {error, invalid}, wait_for_capabilities, State}.

wait_for_capabilities(_Req, State) ->
    {next_state, wait_for_capabilities, State}.

wait_for_protocol(_Req, _From, State) ->
    {reply, {error, invalid}, wait_for_protocol, State}.

wait_for_protocol(_Req, State) ->
    {next_state, wait_for_protocol, State}.

handle_sync_event(_Req, _From, StateName, State) ->
    {reply, {error, invalid}, StateName, State}.

handle_event(_Req, StateName, State) ->
    {next_state, StateName, State}.

handle_info({_Transport, Socket, Data}, wait_for_capabilities, State = #state{socket = Socket}) ->
    case binary_to_term(Data) of
        % 'Tis better to check this a fail than hope the future is always
        % compatible.
        {?CTRL_ACK, ?CTRL_REV, TheirCaps} ->
            case try_ssl(Socket, ranch_tcp, State#state.local_capabilities, TheirCaps) of
                {error, _} = Error ->
                    {stop, Error, State};
                {NewTransport, NewSocket} ->
                    FullProto = {State#state.protocol, State#state.protovers},
                    NewTransport:send(NewSocket, erlang:term_to_binary(FullProto)),
                    NewTransport:setopts(NewSocket, [{active, once}]),
                    State2 = State#state{transport = NewTransport,
                                         socket = NewSocket,
                                         remote_capabilities = TheirCaps},
                    {next_state, wait_for_protocol, State2}
            end;
        Else ->
            lager:warning("Invalid response from remote server: ~p", [Else]),
            {stop, {invalid_response, Else}, State}
    end;

handle_info({_TransTag, Socket, Data}, wait_for_protocol, State = #state{socket = Socket}) ->
    case binary_to_term(Data) of
        {ok, {ProtoName, {CommonMajor, RemoteMinor, LocalMinor}}} ->
            #state{transport = Transport, mod = Module, mod_args = ModArgs} = State,
            IpPort = {State#state.ip, State#state.port},
            NegotiatedProto = {ProtoName, {CommonMajor, LocalMinor}, {CommonMajor, RemoteMinor}},
            _ = Transport:setopts(Socket, State#state.socket_opts),
            _ModStarted = Module:connected(Socket,
                                           Transport,
                                           IpPort,
                                           NegotiatedProto,
                                           ModArgs,
                                           State#state.remote_capabilities),
            {stop, normal, State};
        Else ->
            lager:warning("Invalid version returned: ~p", [Else]),
            {stop, Else, State}
    end;

handle_info({_LikelyClose, Socket}, _StateName, State = #state{socket = Socket}) ->
    lager:debug("Socket closed"),
    {stop, socket_closed, State}.

terminate(_Why, _StateName, _State) ->
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%% @private

try_ssl(Socket, Transport, MyCaps, TheirCaps) ->
    MySSL = proplists:get_value(ssl_enabled, TheirCaps, false),
    TheirSSL = proplists:get_value(ssl_enabled, MyCaps, false),
    MyName = proplists:get_value(clustername, MyCaps),
    TheirName = proplists:get_value(clustername, TheirCaps),
    case {MySSL, TheirSSL} of
        {true, false} ->
            lager:warning("~p requested SSL, but ~p doesn't support it",
                [MyName, TheirName]),
            {error, no_ssl};
        {false, true} ->
            lager:warning("~p requested SSL but ~p doesn't support it",
                [TheirName, MyName]),
            {error, no_ssl};
        {false, false} ->
            lager:info("~p and ~p agreed to not use SSL", [MyName, TheirName]),
            {Transport, Socket};
        {true, true} ->
            lager:info("~p and ~p agreed to use SSL", [MyName, TheirName]),
            case riak_core_ssl_util:upgrade_client_to_ssl(Socket, riak_core) of
                {ok, SSLSocket} ->
                    {ranch_ssl, SSLSocket};
                {error, Reason} ->
                    lager:error("Error negotiating SSL between ~p and ~p: ~p",
                        [MyName, TheirName, Reason]),
                    {error, Reason}
            end
      end.
