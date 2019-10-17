%% Riak Core Cluster Manager Connections to Remote Clusters
%% Copyright (c) 2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% @doc This is an instance of the core cluster-cluster connection.
%% Initially, it asks the riak_core_connection_mgr to establish a socket
%% connection and enters the connecting state. Once connected, this module
%% receives a ?MODULE:connected/6 callback.
%%
%% At any time, a riak_core_cluster_conn can be asked for its status.
%%
%% Once connected, it will issue a request for the name of the remote
%% cluster: `Transport:send(Socket, ?CTRL_ASK_NAME)'. Then enter the
%% wait_for_cluster_name. The next `{tcp, Socket, Data}' to arrive is the
%% remote cluster name.
%%
%% Next, it requests the remote cluster members, by sending <i>two</i>
%% separate packets. First, Transport:send(Socket, ?CTRL_ASK_MEMBERS), and
%% then as a separate packet,`Transport:send(Socket, term_to_binary({PeerIPStr, PeerPort}))'
%% where `PeerIPStr' is the remote's IP address as seen from here (using
%% Socket:peername/1).
%%
%% Once the list of remote IPs has been received it sends a cast to
%% `?CLUSTER_MANAGER_SERVER' with all its information and enters the stable %% state named connected. Once in this state, it can be asked to poll,
%% which restats the process from (1) above.

-module(riak_core_cluster_conn).

-behavior(gen_fsm_compat).

-include("riak_core_cluster.hrl").
-include("riak_core_connection.hrl").


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

%% Test API
-export([current_state/1]).

%% For testing, we need to have two different cluster manager services running
%% on the same node, which is normally not done. The remote cluster service is
%% the one we're testing, so use a different protocol for the client connection
%% during eunit testing, which will emulate a cluster manager from the test.
-define(REMOTE_CLUSTER_PROTO_ID, test_cluster_mgr).
-else.
-define(REMOTE_CLUSTER_PROTO_ID, ?CLUSTER_PROTO_ID).
-endif.

%% API
-export([start_link/1,
         start_link/2,
         status/1,
         status/2,
         connected/6,
         connect_failed/3,
         stop/1]).

%% gen_fsm_compat callbacks
-export([init/1,
         initiating_connection/2,
         initiating_connection/3,
         connecting/2,
         connecting/3,
         waiting_for_cluster_name/2,
         waiting_for_cluster_name/3,
         waiting_for_cluster_members/2,
         waiting_for_cluster_members/3,
         connected/2,
         connected/3,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4]).

-type remote() :: {cluster_by_name, clustername()} | {cluster_by_addr, ip_addr()}.
-type peer_address() :: {string(), pos_integer()}.
-type node_address() :: {atom(), peer_address()}.
-type ranch_transport_messages() :: {atom(), atom(), atom()}.
-record(state, {mode :: atom(),
                remote :: remote(),
                socket :: port() | undefined,
                name :: clustername() |undefined,
                previous_name="undefined" :: clustername(),
                members=[] :: [peer_address() | node_address()],
                connection_ref :: reference() | undefined,
                connection_timeout :: timeout(),
                transport :: atom() | undefined,
                address :: peer_address() | undefined,
                connection_props = [] :: proplists:proplist(),
                transport_msgs :: ranch_transport_messages() | undefined,
                proto_version :: {non_neg_integer(), non_neg_integer()} | undefined }).
-type state() :: #state{}.

%%%===================================================================
%%% API
%%%===================================================================

%% start a connection with a locator type, either {cluster_by_name, clustername()}
%% or {cluster_by_addr, ip_addr()}. This is asynchronous. If it dies, the connection
%% supervisior will restart it.
-spec start_link(remote()) -> {ok, pid()} | {error, term()}.
start_link(Remote) ->
    start_link(Remote, normal).

-spec start_link(remote(), atom()) -> {ok, pid()} | {error, term()}.
start_link(Remote, Mode) ->
    gen_fsm_compat:start_link(?MODULE, [Remote, Mode], []).

-spec stop(pid()) -> ok.
stop(Ref) ->
    gen_fsm_compat:sync_send_all_state_event(Ref, force_stop).

-spec status(pid()) -> term().
status(Ref) ->
    status(Ref, infinity).

-spec status(pid(), timeout()) -> term().
status(Ref, Timeout) ->
    gen_fsm_compat:sync_send_event(Ref, status, Timeout).

-spec connected(port(), atom(), ip_addr(), term(), term(), proplists:proplist()) -> ok.
connected(Socket,
          Transport,
          Addr,
          {?REMOTE_CLUSTER_PROTO_ID,
           _MyVer    ={CommonMajor,LocalMinor},
           _RemoteVer={CommonMajor,RemoteMinor}},
          {_Remote, Client},
          Props) ->
    %% give control over the socket to the `Client' process.
    %% tell client we're connected and to whom
    Transport:controlling_process(Socket, Client),
    gen_fsm_compat:send_event(Client,
                       {connected_to_remote, Socket, Transport, Addr, Props,
                        {CommonMajor, min(LocalMinor,RemoteMinor)}}).

-spec connect_failed({term(), term()}, {error, term()}, {_, atom() | pid() | port() | {atom(), _} | {via, _, _}}) -> ok.
connect_failed({_Proto, _Vers}, {error, _}=Error, {_Remote, Client}) ->
    %% increment stats for "client failed to connect"
    riak_repl_stats:client_connect_errors(),
    %% tell client we bombed and why
    gen_fsm_compat:send_event(Client, {connect_failed, Error}).

%%%===================================================================
%%% gen_fsm_compat callbacks
%%%===================================================================

-spec init([remote() | atom()]) -> {ok, initiating_connection, state(), timeout()}.
init([Remote, test]) ->
    _ = lager:debug("connecting to ~p", [Remote]),
    State = #state{connection_timeout=infinity,
                   mode=test,
                   remote=Remote},
    {ok, initiating_connection, State};
init([Remote, Mode]) ->
    _ = lager:debug("connecting to ~p", [Remote]),
    State = #state{connection_timeout=?INITIAL_CONNECTION_RESPONSE_TIMEOUT,
                   mode=Mode,
                   remote=Remote},
    {ok, initiating_connection, State, 0}.

%% Async message handling for the `initiating_connection' state
initiating_connection(timeout, State) ->
    UpdState = initiate_connection(State),
    {next_state, connecting, UpdState, ?CONNECTION_SETUP_TIMEOUT + 5000};
initiating_connection(_, State) ->
    {next_state, initiating_connection, State}.

%% Sync message handling for the `initiating_connection' state
initiating_connection(status, _From, State) ->
    {reply, {initiating_connection, State#state.remote}, initiating_connection, State};
initiating_connection(_, _From, State) ->
    {reply, ok, initiating_connection, State}.

%% Async message handling for the `connecting' state
connecting(timeout, State=#state{remote=Remote}) ->
    %% @TODO Original comment below says we want to die, but code was
    %% implemented to loop in the same function. Determine which is
    %% the better choice. Stopping for now.

    %% die with error once we've passed the timeout period that the
    %% core_connection module will expire. Go round and let the connection
    %% manager keep trying.
    _ = lager:debug("cluster_conn: client timed out waiting for ~p", [Remote]),
    {stop, giving_up, State};
connecting({connect_failed, Error}, State=#state{remote=Remote}) ->
    _ = lager:debug("ClusterManager Client: connect_failed to ~p because ~p."
                    " Will retry.",
                    [Remote, Error]),
    _ = lager:warning("ClusterManager Client: connect_failed to ~p because ~p."
                      " Will retry.",
                      [Remote, Error]),
    %% This is fatal! We are being supervised by conn_sup and if we
    %% die, it will restart us.
    {stop, Error, State};
connecting({connected_to_remote, Socket, Transport, Addr, Props, ProtoVersion}, State) ->
    RemoteName = proplists:get_value(clustername, Props),
    _ = lager:debug("Cluster Manager control channel client connected to"
                    " remote ~p at ~p named ~p",
                    [State#state.remote, Addr, RemoteName]),
    _ = lager:debug("Cluster Manager control channel client connected to"
                    " remote ~p at ~p named ~p",
                    [State#state.remote, Addr, RemoteName]),
    TransportMsgs = Transport:messages(),
    UpdState = State#state{socket=Socket,
                           transport=Transport,
                           address=Addr,
                           connection_props=Props,
                           transport_msgs = TransportMsgs,
                           proto_version=ProtoVersion
                          },
    _ = request_cluster_name(UpdState),
    {next_state, waiting_for_cluster_name, UpdState, ?CONNECTION_SETUP_TIMEOUT};
connecting(poll_cluster, State) ->
    %% cluster manager doesn't know we haven't connected yet.
    %% just ignore this while we're waiting to connect or fail
    {next_state, connecting, State};
connecting(Other, State=#state{remote=Remote}) ->
    _ = lager:error("cluster_conn: client got unexpected "
                    "msg from remote: ~p, ~p",
                    [Remote, Other]),
    {next_state, connecting, State}.

%% Sync message handling for the `connecting' state
connecting(status, _From, State) ->
    {reply, {connecting, State#state.remote}, connecting, State};
connecting(_, _From, _State) ->
    {reply, ok, connecting, _State}.

%% Async message handling for the `waiting_for_cluster_name' state
waiting_for_cluster_name({cluster_name, NewName}, State=#state{name=undefined}) ->
    UpdState = State#state{name=NewName, previous_name="undefined"},
    _ = request_member_ips(UpdState),
    {next_state, waiting_for_cluster_members, UpdState, ?CONNECTION_SETUP_TIMEOUT};
waiting_for_cluster_name({cluster_name, NewName}, State=#state{name=Name}) ->
    UpdState = State#state{name=NewName, previous_name=Name},
    _ = request_member_ips(UpdState),
    {next_state, waiting_for_cluster_members, UpdState, ?CONNECTION_SETUP_TIMEOUT};
waiting_for_cluster_name(_, _State) ->
    {next_state, waiting_for_cluster_name, _State}.

%% Sync message handling for the `waiting_for_cluster_name' state
waiting_for_cluster_name(status, _From, State) ->
    {reply, {waiting_for_cluster_name, State#state.remote}, waiting_for_cluster_name, State};
waiting_for_cluster_name(_, _From, _State) ->
    {reply, ok, waiting_for_cluster_name, _State}.

%% Async message handling for the `waiting_for_cluster_members' state
waiting_for_cluster_members({cluster_members, NewMembers}, State = #state{ proto_version={1,0} }) ->
    #state{address=Addr,
           name=Name,
           previous_name=PreviousName,
           members=OldMembers,
           remote=Remote} = State,
    %% this is 1.0 code. NewMembers is list of {IP,Port}

    SortedNew = ordsets:from_list(NewMembers),
    Members =
        NewMembers ++ lists:filter(fun(Mem) ->
                                           not ordsets:is_element(Mem, SortedNew)
                                   end,
                                   OldMembers),

    ClusterUpdatedMsg = {cluster_updated,
                         PreviousName,
                         Name,
                         Members,
                         Addr,
                         Remote},
    gen_server:cast(?CLUSTER_MANAGER_SERVER, ClusterUpdatedMsg),
    {next_state, connected, State#state{members=Members}};
waiting_for_cluster_members({all_cluster_members, NewMembers}, State) ->
    #state{address=Addr,
           name=Name,
           previous_name=PreviousName,
           members=OldMembers,
           remote=Remote} = State,

    %% this is 1.1+ code. Members is list of {node,{IP,Port}}

    Members =
        lists:foldl(fun(Elm={_Node,{_Ip,Port}}, Acc) when is_integer(Port) ->
                            [Elm|Acc];
                       ({Node,_}, Acc) ->
                            case lists:keyfind(Node, 1, OldMembers) of
                                Elm={Node,{_IP,Port}} when is_integer(Port) ->
                                    [Elm|Acc];
                                _ ->
                                    Acc
                            end
                    end,
                    [],
                    NewMembers ),

    ClusterUpdatedMsg = {cluster_updated,
                         PreviousName,
                         Name,
                         [Member || {_Node,Member} <- Members],
                         Addr,
                         Remote},
    gen_server:cast(?CLUSTER_MANAGER_SERVER, ClusterUpdatedMsg),
    {next_state, connected, State#state{members=Members}};
waiting_for_cluster_members(_, _State) ->
    {next_state, waiting_for_cluster_members, _State}.

%% Sync message handling for the `waiting_for_cluster_members' state
waiting_for_cluster_members(status, _From, State) ->
    {reply, {waiting_for_cluster_members, State#state.name}, waiting_for_cluster_members, State};
waiting_for_cluster_members(Other, _From, State) ->
    _ = lager:error("cluster_conn: client got unexpected "
                    "msg from remote: ~p, ~p",
                    [State#state.remote, Other]),
    {reply, ok, waiting_for_cluster_members, State}.

%% Async message handling for the `connected' state
connected(poll_cluster, State) ->
    _ = request_cluster_name(State),
    {next_state, waiting_for_cluster_name, State};
connected(_, State) ->
    {next_state, connected, State}.

%% Sync message handling for the `connected' state
connected(status, _From, State) ->
    #state{address=Addr,
           name=Name,
           members=Members,
           transport=Transport} = State,
    %% request for our connection status
    %% don't try talking to the remote cluster; we don't want to stall our status
    Status = {Addr, Transport, Name, Members},
    {reply, {self(), status, Status}, connected, State};
connected(_, _From, _State) ->
    {reply, ok, connected, _State}.

handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

handle_sync_event(current_state, _From, StateName, State) ->
    Reply = {StateName, State},
    {reply, Reply, StateName, State};
handle_sync_event(force_stop, _From, _StateName, State) ->
    ok = lager:debug("Stopping because I was asked nicely to."),
    {stop, normal, ok, State};
handle_sync_event(_Event, _From, StateName, State) ->
    Reply = ok,
    {reply, Reply, StateName, State}.

%% @doc Handle any non-fsm messages
handle_info({TransOK, Socket, Name},
            waiting_for_cluster_name,
            State=#state{socket=Socket, transport_msgs = {TransOK, _, _}}) ->
    gen_fsm_compat:send_event(self(), {cluster_name, binary_to_term(Name)}),
    Transport = State#state.transport,
    _ = Transport:setopts(Socket, [{active, once}]),
    {next_state, waiting_for_cluster_name, State};
handle_info({TransOK, Socket, Members},
            waiting_for_cluster_members,
            State=#state{socket=Socket, transport_msgs = {TransOK, _, _}, proto_version={1,0}}) ->
    Transport = State#state.transport,
    gen_fsm_compat:send_event(self(), {cluster_members, binary_to_term(Members)}),
    _ = Transport:setopts(Socket, [{active, once}]),
    {next_state, waiting_for_cluster_members, State};
handle_info({TransOK, Socket, Members},
            waiting_for_cluster_members,
            State=#state{socket=Socket, transport_msgs = {TransOK, _, _}}) ->
    Transport = State#state.transport,
    gen_fsm_compat:send_event(self(), {all_cluster_members, binary_to_term(Members)}),
    _ = Transport:setopts(Socket, [{active, once}]),
    {next_state, waiting_for_cluster_members, State};
handle_info({TransOK, Socket, Data},
            StateName,
            State=#state{address=Addr,
                         name=Name,
                         remote=Remote,
                         socket=Socket, transport_msgs = {TransOK, _, _}}) ->
    {cluster_members_changed, Members} = binary_to_term(Data),
    ClusterUpdMsg = {cluster_updated, Name, Name, Members, Addr, Remote},
    gen_server:cast(?CLUSTER_MANAGER_SERVER, ClusterUpdMsg),
    Transport = State#state.transport,
    _ = Transport:setopts(Socket, [{active, once}]),
    {next_state, StateName, State#state{members=Members}};
handle_info({TransError, Socket, Error},
            StateName,
            State=#state{remote=Remote,
                         socket=Socket,
                         transport_msgs = {_, _, TransError}}) ->
    _ = lager:error("cluster_conn: connection ~p failed in state ~s because ~p", [Remote, StateName, Error]),
    {stop, Error, State};
handle_info({TransClosed, Socket} = Msg,
            _StateName,
            State=#state{socket=Socket, transport_msgs = {_, TransClosed, _}}) ->
    % if the connection spuriously closes, it is more likley something is
    % wrong, like the remote node has gone down, than something is normal.
    % thus, we exit abnormally and let the supervisor restart a new us.
    ok = lager:debug("Stopping because it looks like the connect closed: ~p", [Msg]),
    {stop, connection_closed, State};
handle_info({_ClusterManager, poll_cluster}, StateName, State) ->
    gen_fsm_compat:send_event(self(), poll_cluster),
    {next_state, StateName, State};
handle_info(Msg, StateName, State) ->
    lager:warning("Unmatch message ~p", [Msg]),
    {next_state, StateName, State}.

terminate(Reason, StateName, _State) ->
    lager:debug("Exiting while in state ~s due to ~p", [StateName, Reason]),
    ok.

%% @doc this fsm has no special upgrade process
code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%===================================================================
%% Internal functions
%%%===================================================================

-spec request_cluster_name(state()) -> ok | {error, term()}.
request_cluster_name(#state{mode=test}) ->
    ok;
request_cluster_name(#state{socket=Socket, transport=Transport}) ->
    _ = Transport:setopts(Socket, [{active, once}]),
    Transport:send(Socket, ?CTRL_ASK_NAME).

-spec request_member_ips(state()) -> ok | {error, term()}.
request_member_ips(#state{mode=test}) ->
    ok;
request_member_ips(#state{socket=Socket, transport=Transport, proto_version={1,0}}) ->
    Transport:send(Socket, ?CTRL_ASK_MEMBERS),
    %% get the IP we think we've connected to
    {ok, {PeerIP, PeerPort}} = Transport:peername(Socket),
    %% make it a string
    PeerIPStr = inet_parse:ntoa(PeerIP),
    Transport:send(Socket, term_to_binary({PeerIPStr, PeerPort}));
request_member_ips(#state{socket=Socket, transport=Transport, proto_version={1,1}}) ->
    Transport:send(Socket, ?CTRL_ALL_MEMBERS),
    %% get the IP we think we've connected to
    {ok, {PeerIP, PeerPort}} = Transport:peername(Socket),
    %% make it a string
    PeerIPStr = inet_parse:ntoa(PeerIP),
    Transport:send(Socket, term_to_binary({PeerIPStr, PeerPort})).

initiate_connection(State=#state{mode=test}) ->
    State;
initiate_connection(State=#state{remote=Remote}) ->
    %% Dialyzer complains about this call because the spec for
    %% `riak_core_connection_mgr::connect/4' is incorrect.
    {ok, Ref} = riak_core_connection_mgr:connect(
                  Remote,
                  {{?REMOTE_CLUSTER_PROTO_ID, [{1,1},{1,0}]},
                   {?CTRL_OPTIONS, ?MODULE, {Remote, self()}}},
                  default),
    State#state{connection_ref=Ref}.

%% ===================================================================
%% Test API
%% ===================================================================

-ifdef(TEST).

%% @doc Get the current state of the fsm for testing inspection
-spec current_state(pid()) -> {atom(), #state{}} | {error, term()}.
current_state(Pid) ->
    gen_fsm_compat:sync_send_all_state_event(Pid, current_state).

-endif.
