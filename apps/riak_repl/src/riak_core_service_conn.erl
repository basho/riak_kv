-module(riak_core_service_conn).
-behavior(gen_fsm_compat).

-include("riak_core_connection.hrl").

% external api; generally used by ranch
-export([start_link/4]).
% gen_fsm_compat
-export([init/1]).
-export([
    wait_for_hello/3, wait_for_hello/2,
    wait_for_protocol_versions/3, wait_for_protocol_versions/2
]).
-export([ handle_event/3, handle_sync_event/4, handle_info/3 ]).
-export([code_change/4, terminate/3]).

-record(state, {
    transport, transport_msgs, socket, remote_caps, init_args
}).

%% ===============
%% Public API
%% ===============

start_link(Listener, Socket, Transport, InArgs) ->
    lager:debug("Start_link dispatch_service"),
    % to avoid a race condition with ranch, we need to do a little
    % song and dance with the init and start up.
    % in short, the start_link doesn't return until the init is complete,
    % and the ranch_ack does a blocking call into the process that starts
    % the gen_*.
    % http://ninenines.eu/docs/en/ranch/HEAD/guide/protocols/#using_gen_server
    proc_lib:start_link(?MODULE, init, [{Listener, Socket, Transport, InArgs}]).

%% ===============
%% Init
%% ===============

init({Listener, Socket, Transport, InArgs}) ->
    ok = proc_lib:init_ack({ok, self()}),
    ok = ranch:accept_ack(Listener),
    ok = Transport:setopts(Socket, ?CONNECT_OPTIONS),
    ok = Transport:setopts(Socket, [{active, once}]),
    TransportMsgs = Transport:messages(),
    State = #state{transport = Transport, transport_msgs = TransportMsgs, socket = Socket, init_args = InArgs},
    gen_fsm_compat:enter_loop(?MODULE, [], wait_for_hello, State).

%% ===============
%% wait_for_hello
%% ===============

wait_for_hello(_Req, _From, State) ->
    {reply, {error, invalid}, wait_for_hello, State}.

wait_for_hello(_Req, State) ->
    {next_state, wait_for_hello, State}.

%% ===============
%% wait_for_protocol_versions
%% ===============

wait_for_protocol_versions(_Req, _From, State) ->
    {reply, {error, invalid}, wait_for_protocol_versions, State}.

wait_for_protocol_versions(_Req, State) ->
    {next_state, wait_for_protocol_versions, State}.

%% ===============
%% handle_event/handle_sync_event
%% ===============

handle_sync_event(_Req, _From, StateName, State) ->
    {reply, {error, invalid}, StateName, State}.

handle_event(_Req, StateName, State) ->
    {next_state, StateName, State}.

%% ===============
%% handle_info
%% ===============

handle_info({TransOk, Socket, Data}, wait_for_hello, State = #state{socket = Socket, transport_msgs = {TransOk, _, _}}) when is_binary(Data) ->
    case binary_to_term(Data) of
        {?CTRL_HELLO, _CTRL_REV, ThierCaps} ->
            SSLEnabled = app_helper:get_env(riak_core, ssl_enabled, false),
            MyName = riak_core_connection:symbolic_clustername(),
            MyCaps = [{clustername, MyName}, {ssl_enabled, SSLEnabled}],
            Ack = term_to_binary({?CTRL_ACK, ?CTRL_REV, MyCaps}),
            (State#state.transport):send(Socket, Ack),
            case try_ssl(Socket, State#state.transport, MyCaps, ThierCaps) of
                {error, _Reson} = Err ->
                    {stop, Err, State};
                {NewTransport, NewSocket} ->
                    NewTransport:setopts(NewSocket, [{active, once}]),
                    TransMsgs = NewTransport:messages(),
                    State2 = State#state{transport = NewTransport, transport_msgs = TransMsgs, socket = NewSocket, remote_caps = ThierCaps},
                    {next_state, wait_for_protocol_versions, State2}
            end;
        _Else ->
            lager:debug("Invalid hello: ~p", [Data]),
            {stop, {error, invalid_hello}, State}
    end;

handle_info({TransOk, Socket, Data}, wait_for_protocol_versions, State = #state{socket = Socket, transport_msgs = {TransOk, _, _}}) when is_binary(Data) ->
    case binary_to_term(Data) of
        {ClientProto, _Versions} = ClientVersions->
            Transport = State#state.transport,
            Services = gen_server:call(riak_core_service_manager, get_services),
            MyVersions = [Protocol || {_Key, {Protocol, _Strat}} <- Services],
            ChosenVer = choose_version(ClientVersions, MyVersions),
            case ChosenVer of
                {ok, {ClientProto, Major, ClientMinor, MyMinor}, Rest} ->
                    ok = Transport:send(Socket, term_to_binary({ok, {ClientProto, {Major, MyMinor, ClientMinor}}})),
                    Negotiated = {{ok, {ClientProto, {Major, MyMinor}, {Major, ClientMinor}}}, Rest},
                    lager:debug("What I'm sending to the start_negotiated_service: ~p", [Negotiated]),
                    start_negotiated_service(Socket, State#state.transport, Negotiated, State#state.remote_caps);
                {error, _} = ChosenError ->
                    Transport:send(Socket, term_to_binary(ChosenError)),
                    {stop, ChosenError, State};
                {error, ChosenError, _} ->
                    Transport:send(Socket, term_to_binary({error, ChosenError})),
                    {stop, {error, ChosenError}, State}
            end;
        _Else ->
            {stop, {error, invalid_protocol_response}, State}
    end;

handle_info({TransError, Socket, Error}, _StateName, State = #state{socket = Socket, transport_msgs = {_, _, TransError}}) ->
    lager:warning("Socket error: ~p", [Error]),
    {stop, Error, State};

handle_info({TransClosed, Socket}, _StateName, State = #state{socket = Socket, transport_msgs = {_, TransClosed, _}}) ->
    lager:debug("Socket closed"),
    {stop, normal, State}.

%% ===============
%% termiante
%% ===============

terminate(Why, StateName, _State) ->
    lager:debug("Exiting while in state ~s due to ~p", [StateName, Why]),
    ok.

%% ===============
%% Code change
%% ===============

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%% ===============
%% Interal
%% ===============

try_ssl(Socket, Transport, MyCaps, TheirCaps) ->
    MySSL = proplists:get_value(ssl_enabled, TheirCaps, false),
    TheirSSL = proplists:get_value(ssl_enabled, MyCaps, false),
    MyName = proplists:get_value(clustername, MyCaps),
    TheirName = proplists:get_value(clustername, TheirCaps),
    _Res = case {MySSL, TheirSSL} of
        {true, false} ->
            lager:warning("~p requested SSL, but ~p doesn't support it",
                [MyName, TheirName]),
            {error, no_ssl};
        {false, true} ->
            lager:warning("~p requested SSL but ~p doesn't support it",
                [TheirName, MyName]),
            {error, no_ssl};
        {true, true} ->
            lager:info("~p and ~p agreed to use SSL", [MyName, TheirName]),
            case riak_core_ssl_util:maybe_use_ssl(riak_core) of
                false ->
                    {ranch_tcp, Socket};
                _Config ->
                    case riak_core_ssl_util:upgrade_server_to_ssl(Socket, riak_core) of
                        {ok, S} ->
                            {ranch_ssl, S};
                        Other ->
                            Other
                    end
            end;
        {false, false} ->
            lager:info("~p and ~p agreed to not use SSL", [MyName, TheirName]),
            {Transport, Socket}
    end.

%% Note that the callee is responsible for taking ownership of the socket via
%% Transport:controlling_process(Socket, Pid),
start_negotiated_service(Socket, Transport,
                         {NegotiatedProtocols, {Options, Module, Function, Args}},
                         Props) ->
    %% Set requested Tcp socket options now that we've finished handshake phase
    lager:debug("Setting user options on service side; ~p", [Options]),
    lager:debug("negotiated protocols: ~p", [NegotiatedProtocols]),
    Transport:setopts(Socket, Options),

    %% call service body function for matching protocol. The callee should start
    %% a process or gen_server or such, and return `{ok, pid()}'.
    case Module:Function(Socket, Transport, NegotiatedProtocols, Args, Props) of
        {ok, Pid} ->
            {ok,{ClientProto,_Client,_Host}} = NegotiatedProtocols,
            gen_server:cast(riak_core_service_manager, {service_up_event, Pid, ClientProto}),
            {stop, normal, undefined};
        Error ->
            lager:debug("service dispatch of ~p:~p failed with ~p",
                             [Module, Function, Error]),
            lager:error("service dispatch of ~p:~p failed with ~p",
                        [Module, Function, Error]),
            {stop, Error, undefined}
    end.

choose_version({ClientProto,ClientVersions}=_CProtocol, HostProtocols) ->
    lager:debug("choose_version: client proto = ~p, HostProtocols = ~p",
                     [_CProtocol, HostProtocols]),
    %% first, see if the host supports the subprotocol
    case [H || {{HostProto,_Versions},_Rest}=H <- HostProtocols, ClientProto == HostProto] of
        [] ->
            %% oops! The host does not support this sub protocol type
            lager:error("Failed to find host support for protocol: ~p, HostProtocols = ~p", [ClientProto, HostProtocols]),
            lager:debug("choose_version: no common protocols"),
            {error,protocol_not_supported};
        [{{_HostProto,HostVersions},Rest}=_Matched | _DuplicatesIgnored] ->
            lager:debug("choose_version: unsorted = ~p clientversions = ~p",
                             [_Matched, ClientVersions]),
            CommonVers = [{CM,CN,HN} || {CM,CN} <- ClientVersions, {HM,HN} <- HostVersions, CM == HM],
            lager:debug("common versions = ~p", [CommonVers]),
            %% sort by major version, highest to lowest, and grab the top one.
            case lists:reverse(lists:keysort(1,CommonVers)) of
                [] ->
                    %% oops! No common major versions for Proto.
                    lager:debug("Failed to find a common major version for protocol: ~p",
                                     [ClientProto]),
                    lager:error("Failed to find a common major version for protocol: ~p", [ClientProto]),
                    {error,protocol_version_not_supported,Rest};
                [{Major,CN,HN}] ->
                    {ok, {ClientProto,Major,CN,HN},Rest};
                [{Major,CN,HN} | _] ->
                    {ok, {ClientProto,Major,CN,HN},Rest}
            end
    end.

