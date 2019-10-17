%% Riak EnterpriseDS
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
-module(riak_repl_app).
-author('Andy Gross <andy@basho.com>').
-behaviour(application).
-export([start/2,prep_stop/1,stop/1]).
% deprecated: used only when on a mixed version cluster where some
% versions < 1.3
-export([get_matching_address/2,
         cluster_mgr_member_fun/1,
         cluster_mgr_all_member_fun/1,
         cluster_mgr_write_cluster_members_to_ring/2,
         cluster_mgr_read_cluster_targets_from_ring/0]).

-include("riak_core_cluster.hrl").
-include("riak_core_connection.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-define(TRACE(_Stmt),ok).
-else.
-define(TRACE(_Stmt),ok).
-endif.

%% @spec start(Type :: term(), StartArgs :: term()) ->
%%          {ok,Pid} | ignore | {error,Error}
%% @doc The application:start callback for riak_repl.
%%      Arguments are ignored as all configuration is done via the erlenv file.
start(_Type, _StartArgs) ->
    riak_core_util:start_app_deps(riak_repl),

    %% Ensure that the KV service has fully loaded.
    riak_core:wait_for_service(riak_kv),

    IncarnationId = erlang:phash2({make_ref(), os:timestamp()}),
    application:set_env(riak_repl, incarnation, IncarnationId),
    ok = ensure_dirs(),

    riak_core:register([{bucket_fixup, riak_repl}]),

    %% Register our capabilities
    riak_core_capability:register({riak_repl, bloom_fold},
                                  [true, false], %% prefer to use bloom_fold in new code
                                  false),        %% the default is false for legacy code
    % If the realtime queue supports meta data on queued objects. This was a
    % feature introduced for cascading realtime.
    riak_core_capability:register({riak_repl, rtq_meta},
        [true, false], false),

    %% Register capability for the default bucket properties hash.
    riak_core_capability:register(
        {riak_repl, default_bucket_props_hash},
        [
            %% 2.0.6, 2.1.2
            [consistent, datatype, n_val, write_once],

            %% 2.0.5, 2.1.1 and earlier
            [consistent, datatype, n_val, allow_mult, last_write_wins]
        ],
        [consistent, datatype, n_val, allow_mult, last_write_wins]),

    %% skip Riak CS blocks
    case riak_repl_util:proxy_get_active() of
        true ->
            lager:info("REPL CS block skip enabled"),
            riak_core:register([{repl_helper, riak_repl_cs}]);
        false ->
            lager:info("REPL CS block skip disabled")
    end,

    ok = riak_api_pb_service:register(riak_repl_pb_get, 128, 129),

    %% Register our cluster_info app callback modules, with catch if
    %% the app is missing or packaging is broken.
    catch cluster_info:register_app(riak_repl_cinfo),

    %% Spin up supervisor
    case riak_repl_sup:start_link() of
        {ok, Pid} ->
            %% Register connection manager stats application with core
            riak_core:register(riak_conn_mgr_stats,
                               [{stat_mod, riak_core_connection_mgr_stats}]),

            %% register functions for cluster manager to find it's own
            %% nodes' ip addrs
            riak_core_cluster_mgr:register_member_fun(
                fun cluster_mgr_member_fun/1),

            %% register functions for cluster manager to find it's own
            %% nodes' ip addrs
            riak_core_cluster_mgr:register_all_member_fun(
                fun cluster_mgr_all_member_fun/1),

            %% cluster manager leader will follow repl leader
            riak_repl2_leader:register_notify_fun(
              fun riak_core_cluster_mgr:set_leader/2),

            %% fullsync co-ordincation will follow leader
            riak_repl2_leader:register_notify_fun(
                fun riak_repl2_fscoordinator_sup:set_leader/2),
            riak_repl2_leader:register_notify_fun(
              fun riak_repl2_pg_proxy_sup:set_leader/2),

            name_this_cluster(),

            riak_core:register(riak_repl, [{stat_mod, riak_repl_stats}]),
            ok = riak_core_ring_events:add_guarded_handler(
                    riak_repl_ring_handler, []),

            %% Add routes to webmachine
            _ = [ webmachine_router:add_route(R)
              || R <- lists:reverse(riak_repl_web:dispatch_table()) ],

            %% Now that we have registered the ring handler, we can
            %% register the cluster manager name locator function (which
            %% reads the ring).
            register_cluster_name_locator(),

            %% makes service manager start connection dispatcher
            ok = riak_repl2_rtsink_conn:sync_register_service(),
            ok = riak_repl2_fssink:sync_register_service(),
            ok = riak_repl2_fscoordinator_serv:sync_register_service(),
            ok = riak_repl2_pg_block_requester:sync_register_service(),

            %% Don't announce service is available until we've
            %% registered all listeners.
            riak_core_node_watcher:service_up(riak_repl, Pid),

            {ok, Pid};
        {error, Reason} ->
            {error, Reason}
    end.

%% @spec stop(State :: term()) -> ok
%% @doc The application:stop callback for riak_repl.
stop(_State) ->
    lager:info("Stopped application riak_repl"),
    ok.

ensure_dirs() ->
    {ok, DataRoot} = application:get_env(riak_repl, data_root),
    LogDir = filename:join(DataRoot, "logs"),
    case filelib:ensure_dir(filename:join(LogDir, "empty")) of
        ok ->
            application:set_env(riak_repl, log_dir, LogDir),
            ok;
        {error, Reason} ->
            Msg = io_lib:format("riak_repl couldn't create log dir ~p: ~p~n", [LogDir, Reason]),
            riak:stop(lists:flatten(Msg))
    end,
    {ok, Incarnation} = application:get_env(riak_repl, incarnation),
    WorkRoot = filename:join([DataRoot, "work"]),
    _ = prune_old_workdirs(WorkRoot),
    WorkDir = filename:join([WorkRoot, integer_to_list(Incarnation)]),
    case filelib:ensure_dir(filename:join([WorkDir, "empty"])) of
        ok ->
            application:set_env(riak_repl, work_dir, WorkDir),
            ok;
        {error, R} ->
            M = io_lib:format("riak_repl couldn't create work dir ~p: ~p~n", [WorkDir,R]),
            riak:stop(lists:flatten(M)),
            {error, R}
    end.

prune_old_workdirs(WorkRoot) ->
    case file:list_dir(WorkRoot) of
        {ok, SubDirs} ->
            DirPaths = [filename:join(WorkRoot, D) || D <- SubDirs],
            Cmds = [lists:flatten(io_lib:format("rm -rf ~s", [D])) || D <- DirPaths],
            _ = [os:cmd(Cmd) || Cmd <- Cmds],
            ok;
        _ ->
            ignore
    end.

%% Get the list of nodes of our ring
%% This list includes all up-nodes, that host the riak_kv service
cluster_mgr_member_fun({IP, Port}) ->
    lists_shuffle([ {XIP,XPort} || {_Node,{XIP,XPort}} <- cluster_mgr_members({IP, Port}, riak_core_node_watcher:nodes(riak_kv)),
                                 is_integer(XPort) ]).

%% this list includes *all* members of the ring (even those marked down).
%% returns a list [ { node(), {IP, Port} | unreachable }, ... ]
cluster_mgr_all_member_fun({IP, Port}) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    cluster_mgr_members({IP, Port}, riak_core_ring:all_members(Ring)).

cluster_mgr_members({IP, Port}, Nodes) ->

    %% find the subnet for the interface we connected to
    {ok, MyIPs} = inet:getifaddrs(),
    {ok, NormIP} = riak_repl_util:normalize_ip(IP),
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Map = riak_repl_ring:get_nat_map(Ring),
    %% apply the NAT map
    RealIP = riak_repl2_ip:maybe_apply_nat_map(NormIP, Port, Map),
    lager:debug("normIP is ~p, after nat map ~p", [NormIP, RealIP]),
    case riak_repl2_ip:determine_netmask(MyIPs, RealIP) of
        undefined ->
            lager:warning("Connected IP not present locally, must be NAT. Returning ~p",
                         [{IP,Port}]),
            %% might as well return the one IP we know will work
            [{node(), {IP, Port}}];
        CIDR ->
            ?TRACE(lager:notice("CIDR is ~p", [CIDR])),
            %AddressMask = riak_repl2_ip:mask_address(NormIP, MyMask),
            %?TRACE(lager:notice("address mask is ~p", [AddressMask])),
            {Results, BadNodes} = rpc:multicall(Nodes, riak_repl2_ip,
                get_matching_address, [RealIP, CIDR]),
            % when this code was written, a multicall will list the results
            % in the same order as the nodes where tried.
            %% Anya specific
            AddressMask = riak_repl2_ip:mask_address(RealIP, CIDR),
            Results2 = maybe_retry_ip_rpc(Results, Nodes, BadNodes, [RealIP,
                                                                     AddressMask]),
            case RealIP == NormIP of
                true ->
                    %% No nat, just return the results
                    Results2;
                false ->
                    %% NAT is in effect
                    NatRes = lists:foldl(fun({XNode, {XIP, XPort}}, Acc) ->
                            case riak_repl2_ip:apply_reverse_nat_map(XIP, XPort, Map) of
                                error ->
                                    %% there's no NAT configured for this IP!
                                    %% location_down is the closest thing we
                                    %% can reply with.
                                    lager:warning("There's no NAT mapping for"
                                        "~p:~b to an external IP (node: ~p)",
                                        [XIP, XPort, XNode]),
                                    Acc;
                                {ExternalIP, ExternalPort} ->
                                    [{XNode, {ExternalIP, ExternalPort}} | Acc];
                                ExternalIP ->
                                    [{XNode, {ExternalIP, XPort}}|Acc]
                            end
                    end, [], Results2),
                    Results3 = NatRes ++ [ {Node, unreachable} || Node <- BadNodes ],
                    lager:debug("nat: ~p -> ~p", [Results2, Results3]),
                    Results3
            end
    end.

maybe_retry_ip_rpc(Results, Nodes, BadNodes, Args) ->
    Nodes2 = Nodes -- BadNodes,
    Zipped = lists:zip(Results, Nodes2),
    MaybeRetry = fun
        ({{badrpc, {'EXIT', {undef, _StrackTrace}}}, Node}) ->
            RPCResult = riak_core_util:safe_rpc(Node, riak_repl_app, get_matching_address, Args),
            lager:debug("rpc to get_matching_address: ~p", [RPCResult]),
            {Node, RPCResult};
        ({Result, Node}) ->
            {Node, Result}
    end,
    lists:map(MaybeRetry, Zipped).

lists_shuffle([]) ->
    [];

lists_shuffle([E]) ->
    [E];

lists_shuffle(List) ->
    Max = length(List),
    Keyed = [{rand:uniform(Max), E} || E <- List],
    Sorted = lists:sort(Keyed),
    [N || {_, N} <- Sorted].

%% TODO: check the config for a name. Don't overwrite one a user has set via cmd-line
name_this_cluster() ->
    ClusterName = case riak_core_connection:symbolic_clustername() of
                      "undefined" ->
                          {ok, Ring} = riak_core_ring_manager:get_my_ring(),
                          lists:flatten(
                            io_lib:format("~p", [riak_core_ring:cluster_name(Ring)]));
                      Name ->
                          Name
                  end,
    riak_core_connection:set_symbolic_clustername(ClusterName).


%% Persist the named cluster and it's members to the repl ring metadata.
%% TODO: an empty Members list means "delete this cluster name"
cluster_mgr_write_cluster_members_to_ring(ClusterName, Members) ->
    lager:debug("Saving cluster to the ring: ~p of ~p", [ClusterName, Members]),
    riak_core_ring_manager:ring_trans(fun riak_repl_ring:set_clusterIpAddrs/2,
                                      {ClusterName, Members}).

%% Return a list of cluster targets by cluster name.
%% These will then be resolved by calls back to our registered
%% locator function, registered below.
cluster_mgr_read_cluster_targets_from_ring() ->
    %% get cluster names from cluster manager
    Ring = get_ring(),
    Clusters = riak_repl_ring:get_clusters(Ring),
    [{?CLUSTER_NAME_LOCATOR_TYPE, Name} || {Name, _Addrs} <- Clusters].

%% Register a locator for cluster names. MUST do this BEFORE we
%% register the save/restore functions because the restore function
%% is going to immediately try and locate functions if the cluster
%% manager is already the leader.
register_cluster_name_locator() ->
    Locator = fun(ClusterName, _Policy) ->
                      Ring = get_ring(),
                      Addrs = riak_repl_ring:get_clusterIpAddrs(Ring, ClusterName),
                      lager:debug("located members for cluster ~p: ~p", [ClusterName, Addrs]),
                      {ok,Addrs}
              end,
    ok = riak_core_connection_mgr:register_locator(?CLUSTER_NAME_LOCATOR_TYPE, Locator),
    %% Register functions to save/restore cluster names and their members
    riak_core_cluster_mgr:register_save_cluster_members_fun(
      fun cluster_mgr_write_cluster_members_to_ring/2),
    riak_core_cluster_mgr:register_restore_cluster_targets_fun(
      fun cluster_mgr_read_cluster_targets_from_ring/0).

get_ring() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    riak_repl_ring:ensure_config(Ring).

prep_stop(_State) ->
    %% TODO: this should only run with BNW

    try %% wrap with a try/catch - application carries on regardless,
        %% no error message or logging about the failure otherwise.

        %% mark the service down so other nodes don't try to migrate to this
        %% one while it's going down
        riak_core_node_watcher:service_down(riak_repl),

        %% remove the ring event handler
        riak_core:delete_guarded_event_handler(riak_core_ring_events,
            riak_repl_ring_handler, []),

        %% the repl bucket hook will check to see if the queue is running and deliver to
        %% another node if it's shutting down
        lager:info("Redirecting realtime replication traffic"),
        riak_repl2_rtq:shutdown(),

        lager:info("Stopping application riak_repl - marked service down.\n", []),

        case riak_repl_migration:start_link() of
            {ok, _Pid} ->
                lager:info("Started migration server"),
                riak_repl_migration:migrate_queue();
            {error, _} ->
                lager:error("Can't start replication migration server")
        end,
        %% stop it cleanly, don't just kill it
        riak_repl2_rtq:stop()
       catch
        Type:Reason ->
            lager:error("Stopping application riak_api - ~p:~p.\n", [Type, Reason])
       end,
       Stats = riak_repl_stats:get_stats(),
       SourceErrors = proplists:get_value(rt_source_errors, Stats, 0),
       SinkErrors = proplists:get_value(rt_sink_errors, Stats, 0),
       % Setting these to debug as I'm not sure they are entirely accurate
       lager:debug("There were ~p rt_source_errors upon shutdown",
                  [SourceErrors]),
       lager:debug("There were ~p rt_sink_errors upon shutdown",
                  [SinkErrors]),
    stopping.

%% This function is only here for nodes using a version < 1.3. Remove it in
%% future version
get_matching_address(IP, Mask) ->
    %% Riak 1.2.x incorrectly calculates the netmask before passing it to this
    %% function. This works with 1.2 nodes, that expect the wrong input, but
    %% the bug was fixed, so we need to undo that conversion here before
    %% calling get_matching_address.
    CIDR = unmask_address(list_to_binary(tuple_to_list(IP)), Mask, 32),
    riak_repl2_ip:get_matching_address(IP, CIDR).

%% this is kind of brute force-y, but I couldn't cook up a better solution
unmask_address(_, _, 0) ->
    %% should never happen in normal usage
    error;
unmask_address(IP, Mask, Size) ->
    case IP of
        <<Mask:Size, _/bitstring>> ->
            Size;
        _ ->
            unmask_address(IP, Mask, Size - 1)
    end.


%%%%%%%%%%%%%%%%
%% Unit Tests %%
%%%%%%%%%%%%%%%%

-ifdef(TEST).

-endif.
