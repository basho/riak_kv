%% Riak Core Cluster Manager
%%
%% Copyright (c) 2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% A cluster manager runs on every node. It registers a service via the 
%% riak_core_service_mgr with protocol 'cluster_mgr'. The service will either
%% answer queries (if it's the leader), or foward them to the leader (if it's
%% not the leader).
%%
%% Every cluster manager instance (one per node in the cluster) is told who the
%% leader is when there is a leader change. An outside agent is responsible for
%% determining which instance of cluster manager is the leader. For example,
%% the riak_repl2_leader server is probably a good place to do this from. Call
%% set_leader_node(node(), pid()).
%%
%% If I'm the leader, I answer local gen_server:call requests from non-leader
%% cluster managers. I also establish out-bound connections to any IP address
%% added via add_remote_cluster(ip_addr()), in order to resolve the name of the
%% remote cluster and to collect any additional member addresses of that
%% cluster. I keep a database of members per named cluster.
%%
%% If I am not the leader, I proxy all requests to the actual leader because I
%% probably don't have the latest inforamtion. I don't make outbound
%% connections either.
%%
%% The local cluster's members list is supplied by the members_fun in
%% register_member_fun() API call. The cluster manager will call the registered
%% function to get a list of the local cluster members; that function should
%% return a list of {IP,Port} tuples in order of the least "busy" to most
%% "busy". Busy is probably proportional to the number of connections it has for
%% replication or handoff. The cluster manager will then hand out the full list
%% to remote cluster managers when asked for its members, except that each time
%% it hands our the list, it will rotate the list so that the fist "least busy"
%% is moved to the end, and all others are pushed up the front of the list.
%% This helps balance the load when the local connection manager asks the
%% cluster manager for a list of IPs to connect for a single connection request.
%% Thus, successive calls from the connection manager will appear to round-robin
%% through the last known list of IPs from the remote cluster. The remote
%% clusters are occasionaly polled to get a fresh list, which will also help
%% balance the connection load on them.
%%
%% TODO:
%% 1. should the service side do push notifications to the client when nodes are added/deleted?


-module(riak_core_cluster_mgr).
-behaviour(gen_server).

-include("riak_core_cluster.hrl").
-include("riak_core_connection.hrl").
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(SERVER, ?CLUSTER_MANAGER_SERVER).
-define(MAX_CONS, 20).
-define(CLUSTER_POLLING_INTERVAL, 10 * 1000).
-define(GC_INTERVAL, infinity).
-define(PROXY_CALL_TIMEOUT, 30 * 1000).

%% State of a resolved remote cluster
-record(cluster, {name :: string(),     % obtained from the remote cluster by ask_name()
                  members :: [ip_addr()], % list of suspected ip addresses for cluster
                  last_conn :: erlang:timestamp() % last time we connected to the remote cluster
                 }).

%% remotes := orddict, key = ip_addr(), value = unresolved | clustername()

-record(state, {is_leader = false :: boolean(),                % true when the buck stops here
                leader_node = undefined :: undefined | node(),
                gc_interval = infinity,
                member_fun = fun(_Addr) -> [] end,             % return members of local cluster
                all_member_fun = fun(_Addr) -> [] end,             % return members of local cluster
                restore_targets_fun = fun() -> [] end,         % returns persisted cluster targets
                save_members_fun = fun(_C,_M) -> ok end,       % persists remote cluster members
                balancer_fun = fun(Addrs) -> Addrs end,        % registered balancer function
                clusters = orddict:new() :: orddict:orddict()  % resolved clusters by name
               }).

-export([start_link/0,
         start_link/4,
         set_leader/2,
         get_leader/0,
         get_is_leader/0,
         register_member_fun/1,
         register_all_member_fun/1,
         register_restore_cluster_targets_fun/1,
         register_save_cluster_members_fun/1,
         add_remote_cluster/1, remove_remote_cluster/1,
         get_known_clusters/0,
         get_connections/0,
         get_ipaddrs_of_cluster/1,
         set_gc_interval/1,
         stop/0,
         connect_to_clusters/0,
         shuffle_remote_ipaddrs/1
         ]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% internal functions
-export([%ctrlService/5, ctrlServiceProcess/5,
         round_robin_balancer/1, cluster_mgr_sites_fun/0,
         get_my_members/1, get_all_members/1]).

-export([ensure_valid_ip_addresses/1]).

%%%===================================================================
%%% API
%%%===================================================================
%% @doc start the Cluster Manager
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

start_link(DefaultLocator, DefaultAllLocator, DefaultSave, DefaultRestore) ->
    Args = [DefaultLocator, DefaultAllLocator, DefaultSave, DefaultRestore],
    Options = [],
    gen_server:start_link({local, ?SERVER}, ?MODULE, Args, Options).

%% @doc Tells us who the leader is. Called by riak_repl_leader whenever a
%% leadership election takes place.
set_leader(LeaderNode, _LeaderPid) ->
    gen_server:cast(?SERVER, {set_leader_node, LeaderNode}).

%% Reply with the current leader node.
get_leader() ->
    gen_server:call(?SERVER, leader_node, infinity).

%% Reply with the current leader node.
connect_to_clusters() ->
    gen_server:call(?SERVER, connect_to_clusters, infinity).

%% @doc True if the local manager is the leader.
get_is_leader() ->
    gen_server:call(?SERVER, get_is_leader, infinity).

%% @doc Register a function that will get called to get out local riak node
%% member's IP addrs. MemberFun(ip_addr()) -> [{IP,Port}] were IP is a string
-spec register_member_fun(MemberFun :: fun((ip_addr()) -> [{string(),pos_integer()}])) -> 'ok'.
register_member_fun(MemberFun) ->
    gen_server:cast(?SERVER, {register_member_fun, MemberFun}).

%% @doc Register a function that will get called to get out local riak node
%% member's IP addrs. MemberFun(ip_addr()) -> [{node(),{IP,Port}}] were IP is a string
-spec register_all_member_fun(MemberFun :: fun((ip_addr()) -> [{atom(),{string(),pos_integer()}}])) -> 'ok'.
register_all_member_fun(MemberFun) ->
    gen_server:cast(?SERVER, {register_all_member_fun, MemberFun}).

register_restore_cluster_targets_fun(ReadClusterFun) ->
    gen_server:cast(?SERVER, {register_restore_cluster_targets_fun, ReadClusterFun}).

register_save_cluster_members_fun(WriteClusterFun) ->
    gen_server:cast(?SERVER, {register_save_cluster_members_fun, WriteClusterFun}).

%% @doc Specify how to reach a remote cluster, its name is
%% retrieved by asking it via the control channel.
-spec(add_remote_cluster(ip_addr()) -> ok).
add_remote_cluster({IP,Port}) ->
    gen_server:cast(?SERVER, {add_remote_cluster, {IP,Port}}).

%% @doc Remove a remote cluster by name
-spec(remove_remote_cluster(ip_addr() | string()) -> ok).
remove_remote_cluster(Cluster) ->
    gen_server:cast(?SERVER, {remove_remote_cluster, Cluster}).

%% @doc Retrieve a list of known remote clusters that have been resolved (they responded).
-spec(get_known_clusters() -> {ok,[clustername()]} | term()).
get_known_clusters() ->
    gen_server:call(?SERVER, get_known_clusters, infinity).

%% @doc Retrieve a list of IP,Port tuples we are connected to or trying to connect to
get_connections() ->
    gen_server:call(?SERVER, get_connections, infinity).

get_my_members(MyAddr) ->
    gen_server:call(?SERVER, {get_my_members, MyAddr}, infinity).

get_all_members(MyAddr) ->
    gen_server:call(?SERVER, {get_all_members, MyAddr}, infinity).

%% @doc Return a list of the known IP addresses of all nodes in the remote cluster.
get_ipaddrs_of_cluster(ClusterName) ->
    case gen_server:call(?SERVER, {get_known_ipaddrs_of_cluster, {name,ClusterName}}, infinity) of
        {ok, Reply} ->
            shuffle_remote_ipaddrs(Reply);
        Reply ->
            Reply
    end.

%% @doc stops the local server.
-spec stop() -> 'ok'.
stop() ->
    gen_server:call(?SERVER, stop, infinity).

-spec set_gc_interval(Interval :: timeout()) -> 'ok'.
set_gc_interval(Interval) ->
    gen_server:cast(?SERVER, {set_gc_interval, Interval}).


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
init(Defaults) ->
    lager:debug("Cluster Manager: starting"),
    %% start our cluster_mgr service if not already started.
    case riak_core_service_mgr:is_registered(?CLUSTER_PROTO_ID) of
        false ->
            ServiceProto = {?CLUSTER_PROTO_ID, [{1,1}, {1,0}]},
            %ServiceSpec = {ServiceProto, {?CTRL_OPTIONS, ?MODULE, ctrlService, []}},
            ServiceSpec = {ServiceProto, {?CTRL_OPTIONS, riak_core_cluster_serv, start_link, []}},
            riak_core_service_mgr:sync_register_service(ServiceSpec, {round_robin,?MAX_CONS});
        true ->
            ok
    end,
    %% schedule a timer to poll remote clusters occasionaly
    erlang:send_after(?CLUSTER_POLLING_INTERVAL, self(), poll_clusters_timer),
    BalancerFun = fun(Addr) -> round_robin_balancer(Addr) end,
    MeNode = node(),
    State = register_defaults(Defaults, #state{
                is_leader = false,
                balancer_fun = BalancerFun}),

    %% Schedule a delayed connection to know clusters
    schedule_cluster_connections(),
    case riak_repl2_leader:leader_node() of
        undefined ->
            % there's an election in progress, so we can just hang on until
            % that finishes
            {ok, State};
        MeNode ->
            State2 = become_leader(State#state{leader_node = MeNode}, MeNode),
            {ok, State2};
        NotMeNode ->
            State2 = become_proxy(State#state{leader_node = NotMeNode}, NotMeNode),
            {ok, State2}
    end.

handle_call(get_is_leader, _From, State) ->
    {reply, State#state.is_leader, State};

handle_call({get_my_members, MyAddr}, _From, State) ->
    %% This doesn't need to call the leader.
    MemberFun = State#state.member_fun,
    MyMembers = [{string_of_ip(IP),Port} || {IP,Port} <- MemberFun(MyAddr), is_integer(Port)],
    {reply, MyMembers, State};

handle_call({get_all_members, MyAddr}, _From, State) ->
    %% This doesn't need to call the leader.
    AllMemberFun = State#state.all_member_fun,
    MyMembers = lists:map(fun({Node,{IP,Port}}) when is_integer(Port) ->
                                  {Node,{string_of_ip(IP),Port}};
                             ({Node,_}) ->
                                  {Node, unreachable}
                          end,
                          AllMemberFun(MyAddr)),
    {reply, MyMembers, State};

handle_call(leader_node, _From, State) ->
    {reply, State#state.leader_node, State};

handle_call(connect_to_clusters, _From, State) ->
    connect_to_persisted_clusters(State),
    {reply, ok, State};

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};

%% Reply with list of resolved cluster names.
%% If a leader has not been elected yet, return an empty list.
handle_call(get_known_clusters, _From, State) ->
    case State#state.is_leader of
        true ->
            Remotes = [Name || {Name,_C} <- orddict:to_list(State#state.clusters)],
            {reply, {ok, Remotes}, State};
        false ->
            NoLeaderResult = {ok, []},
            proxy_call(get_known_clusters, NoLeaderResult, State)
    end;

handle_call(get_connections, _From, State) ->
    case State#state.is_leader of
        true ->
            Conns = riak_core_cluster_conn_sup:connections(),
            {reply, {ok, Conns}, State};
        false ->
            NoLeaderResult = {ok, []},
            proxy_call(get_connections, NoLeaderResult, State)
    end;
    

%% Return possible IP addrs of nodes on the named remote cluster.
%% If a leader has not been elected yet, return an empty list.
%% This list will get rotated or randomized depending on the balancer
%% function installed. Every time we poll the remote cluster or it
%% pushes an update, the list will get reset to whatever the remote
%% thinks is the best order. The first call here will return the most
%% recently updated list and then it will rebalance and save for next time.
%% So, if no updates come from the remote, we'll just keep cycling through
%% the list of known members according to the balancer fun.
handle_call({get_known_ipaddrs_of_cluster, {name, ClusterName}}, _From, State) ->
    case State#state.is_leader of
        true ->
            %% Call a balancer function that will rotate or randomize
            %% the list. Return original members and save reblanced ones
            %% for next iteration.
            Members = members_of_cluster(ClusterName, State),
            BalancerFun = State#state.balancer_fun,
            RebalancedMembers = BalancerFun(Members),
            lager:debug("Rebalancer: ~p -> ~p", [Members, RebalancedMembers]),
            {reply, {ok, Members},
             State#state{clusters=add_ips_to_cluster(ClusterName, RebalancedMembers,
                                                     State#state.clusters)}};
        false ->
            NoLeaderResult = {ok, []},
            proxy_call({get_known_ipaddrs_of_cluster, {name, ClusterName}},
                       NoLeaderResult,
                       State)
    end.

handle_cast({set_leader_node, LeaderNode}, State) ->
    State2 = State#state{leader_node = LeaderNode},
    case node() of
        LeaderNode ->
            %% oh crap, it's me!
            {noreply, become_leader(State2, LeaderNode)};
        _ ->
            %% not me.
            {noreply, become_proxy(State2, LeaderNode)}
    end;

handle_cast({set_gc_interval, Interval}, State) ->
    schedule_gc_timer(Interval),
    State#state{gc_interval=Interval};

handle_cast({register_member_fun, Fun}, State) ->
    {noreply, State#state{member_fun=Fun}};

handle_cast({register_all_member_fun, Fun}, State) ->
    {noreply, State#state{all_member_fun=Fun}};

handle_cast({register_save_cluster_members_fun, Fun}, State) ->
    {noreply, State#state{save_members_fun=Fun}};

handle_cast({register_restore_cluster_targets_fun, Fun}, State) ->
    %% If we are already the leader, connect to known clusters after some delay.
    %% TODO: 5 seconds is arbitrary. It's enough time for the ring to be stable
    %% so that the call into the repl_ring handler won't crash. Fix this.
    erlang:send_after(5000, self(), connect_to_clusters),
    {noreply, State#state{restore_targets_fun=Fun}};

handle_cast({add_remote_cluster, {_IP,_Port} = Addr}, State) ->
    _ = case State#state.is_leader of
        false ->
            %% forward request to leader manager
            proxy_cast({add_remote_cluster, Addr}, State);
        true ->
            %% start a connection if one does not already exist
            Remote = {?CLUSTER_ADDR_LOCATOR_TYPE, Addr},
            ensure_remote_connection(Remote)
    end,
    {noreply, State};

%% remove a connection if one already exists, by name or by addr.
%% This is usefull if you accidentally add a bogus cluster address or
%% just want to disconnect from one.
handle_cast({remove_remote_cluster, Cluster}, State) ->
    State2 =
        case State#state.is_leader of
            false ->
                %% forward request to leader manager
                proxy_cast({remove_remote_cluster, Cluster}, State),
                State;
            true ->
                remove_remote(Cluster, State)
        end,
    {noreply, State2};

%% The client connection recived (or polled for) an update from the remote cluster.
handle_cast({cluster_updated, "undefined", NewName, Members, Addr,
             {cluster_by_addr, _CAddr}=Remote}, State) ->
    %% replace connection by address with connection by clustername if that would be safe.
    case is_ok_to_connect(NewName, Remote, true) of
        true ->
            {noreply, update_cluster_members("undefined", NewName, Members, Addr, Remote, State)};
        false ->
            %% connection to that cluster is denied
            {noreply, State}
    end;
handle_cast({cluster_updated, OldName, NewName, Members, Addr, Remote}, State) ->
    %% Remote cluster changed names or just connected by clustername. allow reconnect
    case is_ok_to_connect(NewName, Remote, false) of
        true ->
            {noreply, update_cluster_members(OldName, NewName, Members, Addr, Remote, State)};
        false ->
            %% connection to that cluster is denied
            {noreply, State}
    end;

handle_cast(_Unhandled, _State) ->
    lager:debug("Unhandled gen_server cast: ~p", [_Unhandled]),
    {error, unhandled}. %% this will crash the server

%% it is time to poll all clusters and get updated member lists
handle_info(poll_clusters_timer, State) when State#state.is_leader == true ->
    Connections = riak_core_cluster_conn_sup:connections(),
    _ = [Pid ! {self(), poll_cluster} || {_Remote, Pid} <- Connections],
    erlang:send_after(?CLUSTER_POLLING_INTERVAL, self(), poll_clusters_timer),
    {noreply, State};
handle_info(poll_clusters_timer, State) ->
    erlang:send_after(?CLUSTER_POLLING_INTERVAL, self(), poll_clusters_timer),
    {noreply, State};

%% Remove old clusters that no longer have any IP addresses associated with them.
%% They are probably old cluster names that no longer exist. If we don't have an IP,
%% then we can't connect to it anyhow.
handle_info(garbage_collection_timer, State) ->
    State1 = collect_garbage(State),
    schedule_gc_timer(State#state.gc_interval),
    {noreply, State1};

handle_info(connect_to_clusters, State) ->
    connect_to_persisted_clusters(State),
    {noreply, State};

handle_info(_Unhandled, State) ->
    lager:debug("Unhandled gen_server info: ~p", [_Unhandled]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Private
%%%===================================================================

%% Cause ourself to try and reconnect to known clusters at various intervals.
%% If a connection is already established, it won't create a new one.
schedule_cluster_connections() ->
    erlang:send_after(5000, self(), connect_to_clusters),
    erlang:send_after(15000, self(), connect_to_clusters),
    erlang:send_after(30000, self(), connect_to_clusters),
    erlang:send_after(60000, self(), connect_to_clusters).

register_defaults(Defaults, State) ->
    case Defaults of
        [] ->
            State;
        [MembersFun, AllMembersFun, SaveFun, RestoreFun] ->
            lager:debug("Registering default cluster manager functions."),
            State#state{member_fun=MembersFun,
                        all_member_fun=AllMembersFun,
                        save_members_fun=SaveFun,
                        restore_targets_fun=RestoreFun}
    end.

is_ok_to_connect(NewName, Remote, CheckConnected) ->
    NewRemote = {cluster_by_name, NewName},
    AlreadyConnected =
        case CheckConnected of
            true ->
                riak_core_cluster_conn_sup:is_connected(NewRemote);
            false ->
                false
        end,
    MyClusterName = riak_core_connection:symbolic_clustername(),
    case NewName of
        "undefined" ->
            %% Don't connect to clusters that haven't been named yet
            lager:warning("ClusterManager: dropping connection ~p to undefined clustername",
                          [Remote]),
            remove_remote_connection(Remote),
            false;
        MyClusterName ->
            %% We somehow got connected to a cluster that is named the same as
            %% us; could be ourself. Hard to tell. Drop it and log a warning.
            lager:warning("ClusterManager: dropping connection ~p to identically named cluster: ~p",
                          [Remote, NewName]),
            remove_remote_connection(Remote),
            false;
        _SomeName when AlreadyConnected == true ->
            %% We are already connected to that cluster
            lager:warning("ClusterManager: dropping connection ~p because already connected to ~p",
                          [Remote, NewName]),
            remove_remote_connection(Remote),
            false;
        _OtherName ->
            true
    end.

schedule_gc_timer(infinity) ->
    ok;
schedule_gc_timer(0) ->
    ok;
schedule_gc_timer(Interval) ->
    %% schedule a timer to garbage collect old cluster and endpoint data
    _ = erlang:send_after(Interval, self(), garbage_collection_timer),
    ok.

is_valid_ip(Addr) when is_list(Addr) ->
    %% a string. try and parse it.
    case inet_parse:address(Addr) of
        {ok,_} -> true;
        _ -> false
    end;
is_valid_ip(IP) when is_tuple(IP) ->
    %% maybe it's a tuple like {1.2.3.4}
    try
        _S = inet_parse:ntoa(IP),
        true
    catch
        _Err ->
            false
    end.

is_valid_member({IP, Port}) when is_integer(Port) -> is_valid_ip(IP);
is_valid_member(_Junk) -> false.

%% filter the list of "ip addresses" to ensure that only ones that appear
%% to be real addresses remain. Valid IPs look like: {"17.173.26.138",9085}, e.g.
ensure_valid_ip_addresses(Members) ->
    lists:filter(fun(Member) ->
                         case is_valid_member(Member) of
                             true -> true;
                             false ->
                                 lager:warning("Cluster Manager: ignoring bad remote IP address: ~p",
                                               [Member]),
                                 false
                         end
                 end,
                 Members).

save_cluster(NewName, OldMembers, ReturnedMembers, State) ->
    %% per issue #243, ensure that only reasonable IP addresses are persisted.
    Members = ensure_valid_ip_addresses(ReturnedMembers),
    %% persist clustername and ip members to ring so the locator will find it by cluster name
    case OldMembers == lists:sort(Members) of
        true ->
            ok;
        false ->
            case Members of
                [] ->
                    %% oh boo. All bad addresses? Don't overwrite what
                    %% we already know with [].
                    lager:warning("Cluster Manager: skipped update of ~p with all bad members: ~p",
                                  [NewName, Members]);
                _ ->
                    persist_members_to_ring(State, NewName, Members),
                    lager:info("Cluster Manager: updated ~p with members: ~p OldMembers ~p",
                               [NewName, Members, OldMembers])
            end
    end,
    %% clear out these IPs from other clusters
    Clusters1 = remove_ips_from_all_clusters(Members, State#state.clusters),
    %% add them back to the new cluster
    State#state{clusters=add_ips_to_cluster(NewName, Members, Clusters1)}.

%% Update ip member information for cluster "Name",
%% remove aliased connections, and try to ensure that IP addresses only
%% appear in one cluster.
update_cluster_members(_OldName, _NewName, [], _Addr, _Remote, State) ->
    lager:warning("Cluster Manager: got empty list of addresses for remote ~p", [_Remote]),
    State;
update_cluster_members(_OldName, NewName, Members, _Addr, {cluster_by_addr, _CAddr}=Remote, State) ->
    %% This was a connection by host:ip, replace with cluster connection
    State1 = save_cluster(NewName, [], Members, State),
    %% restart connection as a cluster_by_name
    remove_remote_connection(Remote),
    ensure_remote_connection({cluster_by_name, NewName}),
    State1;
update_cluster_members(OldName, NewName, Members, _Addr, {cluster_by_name, CName}, State)
  when CName =/= NewName ->
    %% Remote cluster changed names since last time we spoke to it
    lager:warning("Remote cluster changed its name from ~p to ~p", [OldName, NewName]),
    State1 = remove_remote(CName, State),
    ensure_remote_connection({cluster_by_name, NewName}),
    save_cluster(NewName, [], Members, State1);
update_cluster_members(OldName, NewName, Members, _Addr, _Remote, State) ->
    %% simple update of existing cluster
    OldMembers = lists:sort(members_of_cluster(OldName, State)),
    save_cluster(NewName, OldMembers, Members, State).

collect_garbage(State0) ->
    lager:debug("ClusterManager: GC - cleaning out old empty cluster connections."),
    %% remove clusters that have no member IP addrs from our view
    State1 = orddict:fold(fun(Name, Cluster, State) ->
                                  case Cluster#cluster.members of
                                      [] ->
                                          lager:debug("ClusterManager: GC - cluster ~p has no members.",
                                                    [Name]),
                                          remove_remote(Name, State);
                                      _ ->
                                          State
                                  end
                          end,
                          State0,
                          State0#state.clusters),
    State1.

%% Remove the given "remote" from all state and persisted ring and connections.
remove_remote(RemoteName, State) ->
    case RemoteName of
        {IP, Port} ->
            Remote = {?CLUSTER_ADDR_LOCATOR_TYPE, {IP, Port}},
            remove_remote_connection(Remote),
            State;
        ClusterName ->
            Remote = {?CLUSTER_NAME_LOCATOR_TYPE, ClusterName},
            remove_remote_connection(Remote),
            UpdatedClusters = orddict:erase(ClusterName, State#state.clusters),
            %% remove cluster from ring, which is done by saving an empty member list
            persist_members_to_ring(State, ClusterName, []),
            State#state{clusters = UpdatedClusters}
    end.

%% Simple Round Robin Balancer moves head to tail each time called.
round_robin_balancer([]) ->
    [];
round_robin_balancer([Addr|Addrs]) ->
    Addrs ++ [Addr].

%% Convert an inet:address to a string if needed.
string_of_ip(IP) when is_tuple(IP) ->    
    inet_parse:ntoa(IP);
string_of_ip(IP) ->
    IP.

members_of_cluster(ClusterName, State) ->
    case orddict:find(ClusterName, State#state.clusters) of
        error -> [];
        {ok,C} -> C#cluster.members
    end.

ensure_remote_connection({cluster_by_name, "undefined"}) ->
    ok;
ensure_remote_connection(Remote) ->
    %% add will make sure there is only one connection per remote
    _ = riak_core_cluster_conn_sup:add_remote_connection(Remote),
    ok.

%% Drop our connection to the remote cluster.
remove_remote_connection(Remote) ->
    case riak_core_cluster_conn_sup:is_connected(Remote) of
        true ->
            riak_core_cluster_conn_sup:remove_remote_connection(Remote),
            ok;
        _ ->
            ok
    end.

proxy_cast(_Cast, _State = #state{leader_node=Leader}) when Leader == undefined ->
    lager:debug("proxy_cast: leader is undefined. dropping cast: ~p", [_Cast]),
    ok;
proxy_cast(Cast, _State = #state{leader_node=Leader}) ->
    lager:debug("proxy_cast: casting to leader ~p: ~p", [Leader, Cast]),
    gen_server:cast({?SERVER, Leader}, Cast).

%% Make a proxy call to the leader. If there is no leader elected or the request fails,
%% it will return the NoLeaderResult supplied.
proxy_call(_Call, NoLeaderResult, State = #state{leader_node=Leader}) when Leader == undefined ->
    lager:debug("proxy_call: leader is undefined. dropping call: ~p", [_Call]),
    {reply, NoLeaderResult, State};
proxy_call(Call, NoLeaderResult, State = #state{leader_node=Leader}) ->
    lager:debug("proxy_call: call to leader ~p: ~p", [Leader, Call]),
    Reply = try gen_server:call({?SERVER, Leader}, Call, ?PROXY_CALL_TIMEOUT) of
                R -> R
            catch
                exit:{noproc, _} ->
                    NoLeaderResult;
                exit:{{nodedown, _}, _} ->
                    NoLeaderResult
            end,
    {reply, Reply, State}.

%% Remove given IP Addresses from all clusters. Returns revised clusters orddict.
remove_ips_from_all_clusters(Addrs, Clusters) ->
    orddict:map(fun(_Name,C) ->
                        Mbrs = lists:foldl(fun(Addr, Acc) -> lists:delete(Addr, Acc) end,
                                           C#cluster.members,
                                           Addrs),
                        C#cluster{members=Mbrs}
                end,
                Clusters).

%% Add Members to Name'd cluster. Returns revised clusters orddict.
add_ips_to_cluster(Name, RebalancedMembers, Clusters) ->
    orddict:store(Name,
                  #cluster{name = Name,
                           members = RebalancedMembers,
                           last_conn = os:timestamp()},
                  Clusters).

%% Setup a connection to all given cluster targets
connect_to_targets(Targets) ->
    lists:foreach(fun(Target) -> ensure_remote_connection(Target) end,
                  Targets).

%% start being a cluster manager leader
become_leader(State, LeaderNode) when State#state.is_leader == false ->
    lager:info("ClusterManager: ~p becoming the leader", [LeaderNode]),
    %% start leading and tell ourself to connect to known clusters in a bit.
    %% Wait enough time for the ring to be stable
    %% so that the call into the repl_ring handler won't crash.
    %% We can try several time delays because it's idempotent.
    erlang:send_after(5000, self(), connect_to_clusters),
    State#state{is_leader = true};
become_leader(State, LeaderNode) ->
    lager:debug("ClusterManager: ~p still the leader", [LeaderNode]),
    State.

%% stop being a cluster manager leader
become_proxy(State, LeaderNode) when State#state.is_leader == true ->
    lager:info("ClusterManager: ~p becoming a proxy to ~p", [node(), LeaderNode]),
    %% stop leading
    %% remove any outbound connections
    case riak_core_cluster_conn_sup:connections() of
        [] ->
            ok;
        Connections ->
            lager:debug("ClusterManager: proxy is removing connections to remote clusters:"),
            _ = [riak_core_cluster_conn_sup:remove_remote_connection(Remote)
             || {Remote, _Pid} <- Connections],
            ok
    end,
    State#state{is_leader = false};
become_proxy(State, LeaderNode) ->
    lager:debug("ClusterManager: ~p still a proxy to ~p", [node(), LeaderNode]),
    State.

persist_members_to_ring(State, ClusterName, Members) ->
    SaveFun = State#state.save_members_fun,
    SaveFun(ClusterName, Members).

%% Return a list of locators, in our case, we'll use cluster names
%% that were saved in the ring
cluster_mgr_sites_fun() ->
    %% get cluster names from cluster manager
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Clusters = riak_repl_ring:get_clusters(Ring),
    [{?CLUSTER_NAME_LOCATOR_TYPE, Name} || {Name, _Addrs} <- Clusters].    

%% @doc If the current leader, connect to all clusters that have been
%%      currently persisted in the ring.
connect_to_persisted_clusters(State) ->
    case State#state.is_leader of
        true ->
            Fun = State#state.restore_targets_fun,
            ClusterTargets = Fun(),
            lager:debug("Cluster Manager will connect to clusters: ~p", 
                        [ClusterTargets]),
            connect_to_targets(ClusterTargets);
        _ ->
            ok
    end.

shuffle_with_seed(List, Seed={_,_,_}) ->
    _ = rand:seed(exrop, Seed),
    [E || {E, _} <- lists:keysort(2, [{Elm, rand:uniform()} || Elm <- List])];
shuffle_with_seed(List, Seed) ->
    <<_:10,S1:50,S2:50,S3:50>> = crypto:hash(sha, term_to_binary(Seed)),
    shuffle_with_seed(List, {S1,S2,S3}).


shuffle_remote_ipaddrs([]) ->
  {ok, []};
shuffle_remote_ipaddrs(RemoteUnsorted) ->
    {ok, MyRing} = riak_core_ring_manager:get_my_ring(),
    SortedNodes = lists:sort(riak_core_ring:all_members(MyRing)),
    NodesTagged = lists:zip(lists:seq(1, length(SortedNodes)), SortedNodes),
    case lists:keyfind(node(), 2, NodesTagged) of
        {MyPos, _} ->
            OurClusterName = riak_core_connection:symbolic_clustername(),
            RemoteAddrs = shuffle_with_seed(lists:sort(RemoteUnsorted), [OurClusterName]),

            %% MyPos is the position if *this* node in the sorted list of
            %% all nodes in my ring.  Now choose the node at the corresponding
            %% index in RemoteAddrs as out "buddy"
            SplitPos = ((MyPos-1) rem length(RemoteAddrs)),
            case lists:split(SplitPos,RemoteAddrs) of
                {BeforeBuddy,[Buddy|AfterBuddy]} ->
                    {ok, [Buddy | shuffle_with_seed(AfterBuddy ++ BeforeBuddy, node())]}
            end;
        false ->
            {ok, shuffle_with_seed(lists:sort(RemoteUnsorted), node())}
    end.
