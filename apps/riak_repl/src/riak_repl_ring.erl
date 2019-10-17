%% Riak EnterpriseDS
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
-module(riak_repl_ring).
-author('Andy Gross <andy@andygross.org>').
-include("riak_repl.hrl").

-export([ensure_config/1,
         initial_config/0,
         get_repl_config/1,
         set_repl_config/2,
         add_site/2,
         add_site_addr/3,
         del_site_addr/3,
         get_site/2,
         del_site/2,
         add_listener/2,
         add_nat_listener/2,
         get_listener/2,
         del_listener/2,
         del_nat_listener/2,
         get_nat_listener/2,
         set_clusterIpAddrs/2,
         get_clusterIpAddrs/2,
         get_clusters/1,
         rt_enable_trans/2,
         rt_disable_trans/2,
         rt_enabled/1,
         rt_start_trans/2,
         rt_stop_trans/2,
         rt_started/1,
         rt_cascades_trans/2,
         rt_cascades/1,
         fs_enable_trans/2,
         fs_disable_trans/2,
         fs_enabled/1,
         set_modes/2,
         get_modes/1,
         compose/2,
         multicompose/1,
         pg_enable_trans/2,
         pg_disable_trans/2,
         pg_enabled/1,
         add_nat_map/2,
         del_nat_map/2,
         get_nat_map/1
         ]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-spec ensure_config(ring()) -> ring().
%% @doc Ensure that Ring has replication config entry in the ring metadata dict.
ensure_config(Ring) ->
    % can't use get_repl_config becuase that's guarenteed to return a config.
    case riak_core_ring:get_meta(?MODULE, Ring) of
        undefined ->
            riak_core_ring:update_meta(?MODULE, initial_config(), Ring);
        _ ->
            Ring
    end.

-spec get_repl_config(ring()) -> riak_repl_dict()|undefined.
%% @doc Get the replication config dictionary from Ring.
get_repl_config(Ring) ->
    case riak_core_ring:get_meta(?MODULE, Ring) of
        {ok, RC} -> RC;
        undefined -> initial_config()
    end.

-spec set_repl_config(ring(), riak_repl_dict()) -> ring().
%% @doc Set the replication config dictionary in Ring.
set_repl_config(Ring, RC) ->
    riak_core_ring:update_meta(?MODULE, RC, Ring).

-spec add_site(ring(), #repl_site{}) -> ring().
%% @doc Add a replication site to the Ring.
add_site(Ring, Site=#repl_site{name=Name}) ->
    RC = get_repl_config(Ring),
    Sites = dict:fetch(sites, RC),
    NewSites = case lists:keysearch(Name, 2, Sites) of
        false ->
            [Site|Sites];
        {value, OldSite} ->
            NewSite = OldSite#repl_site{addrs=lists:usort(
                    OldSite#repl_site.addrs ++
                    Site#repl_site.addrs)},
            [NewSite|lists:delete(OldSite, Sites)]
    end,
    riak_core_ring:update_meta(
        ?MODULE,
        dict:store(sites, NewSites, RC),
        Ring).

-spec del_site(ring(), repl_sitename()) -> ring().
%% @doc Delete a replication site from the Ring.
del_site(Ring, SiteName) ->
    RC  = get_repl_config(Ring),
    Sites = dict:fetch(sites, RC),
    case lists:keysearch(SiteName, 2, Sites) of
        false ->
            Ring;
        {value, Site} ->
            NewSites = lists:delete(Site, Sites),
            riak_core_ring:update_meta(
              ?MODULE,
              dict:store(sites, NewSites, RC),
              Ring)
    end.

-spec get_site(ring(), repl_sitename()) -> #repl_site{}|undefined.
%% @doc Get a replication site record from the Ring.
get_site(Ring, SiteName) ->
    RC = get_repl_config(Ring),
    Sites  = dict:fetch(sites, RC),
    case lists:keysearch(SiteName, 2, Sites) of
        false -> undefined;
        {value, ReplSite} -> ReplSite
    end.

-spec add_site_addr(ring(), repl_sitename(), repl_addr()) -> ring().
%% @doc Add a site address to connect to.
add_site_addr(Ring, SiteName, {_IP, _Port}=Addr) ->
    case get_site(Ring, SiteName) of
        undefined ->
            Ring;
        #repl_site{addrs=Addrs}=Site ->
            case lists:member(Addr,Addrs) of
                false ->
                    Ring0 = del_site(Ring, SiteName),
                    add_site(Ring0, Site#repl_site{addrs=[Addr|Addrs]});
                true ->
                    Ring
            end
    end.

-spec del_site_addr(ring(), repl_sitename(), repl_addr()) -> ring().
%% @doc Delete a server address from the site
del_site_addr(Ring, SiteName, {_IP, _Port}=Addr) ->
    case get_site(Ring, SiteName) of
        undefined ->
            Ring;
        #repl_site{addrs=Addrs}=Site ->
            case lists:member(Addr,Addrs) of
                false ->
                    Ring;
                true ->
                    Ring0 = del_site(Ring, SiteName),
                    add_site(Ring0, Site#repl_site{addrs=lists:delete(Addr, Addrs)})
            end
    end.

-spec add_listener(ring(), #repl_listener{}) -> ring().
%% @doc Add a replication listener host/port to the Ring.
add_listener(Ring,Listener) ->
    RC = get_repl_config(Ring),
    Listeners = dict:fetch(listeners, RC),
    case lists:member(Listener, Listeners) of
        false ->
            NewListeners = [Listener|Listeners],
            riak_core_ring:update_meta(
              ?MODULE,
              dict:store(listeners, NewListeners, RC),
              Ring);
        true ->
            Ring
    end.

-spec add_nat_listener(ring(), #nat_listener{}) -> ring().
%% @doc Add a replication NAT listener host/port to the Ring.
add_nat_listener(Ring,NatListener) ->
    RC = get_repl_config(Ring),
    case dict:find(natlisteners,RC) of
        {ok, NatListeners} ->
            case lists:member(NatListener, NatListeners) of
                false ->
                    NewListeners = [NatListener|NatListeners],
                    riak_core_ring:update_meta(
                      ?MODULE,
                      dict:store(natlisteners, NewListeners, RC),
                      Ring);
                true ->
                    Ring
            end;
        error ->
            %% there are no natlisteners entries yet.
            NewListeners = [NatListener],
            riak_core_ring:update_meta(
              ?MODULE,
              dict:store(natlisteners, NewListeners, RC),
              Ring)
    end.

-spec del_listener(ring(), #repl_listener{}) -> ring().
%% @doc Delete a replication listener from the Ring.
del_listener(Ring,Listener) ->
    RC  = get_repl_config(Ring),
    Listeners = dict:fetch(listeners, RC),
    case lists:member(Listener, Listeners) of
        false -> Ring;
        true ->
            NatRing = del_nat_listener(Ring,Listener),
            NewListeners = lists:delete(Listener, Listeners),
            riak_core_ring:update_meta(
              ?MODULE,
              dict:store(listeners, NewListeners, get_repl_config(NatRing)), NatRing)
    end.

-spec del_nat_listener(ring(),#repl_listener{}) -> ring().
%% @doc Delete a nat_listener from the list of nat_listeners
del_nat_listener(Ring,Listener) ->
    RC  = get_repl_config(Ring),
    case get_nat_listener(Ring, Listener) of
        undefined ->  Ring;
        NatListener ->
            case dict:find(natlisteners, RC) of
                {ok, NatListeners} ->
                    NewNatListeners = lists:delete(NatListener, NatListeners),
                    riak_core_ring:update_meta(
                      ?MODULE,
                      dict:store(natlisteners, NewNatListeners, RC),
                      Ring);
                error -> io:format("Unknown listener~n")
            end
    end.

-spec get_listener(ring(), repl_addr()) -> #repl_listener{}|undefined.
%% @doc Fetch a replication host/port listener record from the Ring.
get_listener(Ring,{_IP,_Port}=ListenAddr) ->
    RC = get_repl_config(Ring),
    case dict:find(listeners, RC) of
        {ok, Listeners}  ->
            case lists:keysearch(ListenAddr, 3, Listeners) of
                false -> undefined;
                {value,Listener} -> Listener
            end;
        error -> undefined
    end.

-spec get_nat_listener(ring(), #repl_listener{}) -> #nat_listener{}|undefined.
%% @doc Fetch a replication nat host/port listener record from the Ring.
get_nat_listener(Ring,Listener) ->
    RC = get_repl_config(Ring),
    case dict:find(natlisteners, RC) of
        {ok, NatListeners} ->
            %% search for a natlistener using only nodename, ip + port,
            %% since nat uses nodename+ip+port+natip+natport as a key
            NatListenerMatches =
                [NatListener ||
                    NatListener <- NatListeners,
                    (NatListener#nat_listener.listen_addr == Listener#repl_listener.listen_addr
                     orelse NatListener#nat_listener.nat_addr == Listener#repl_listener.listen_addr),
                    NatListener#nat_listener.nodename == Listener#repl_listener.nodename],
            %% this will only return the first nat listener that matches
            %% the search criteria
            case NatListenerMatches of
                [NatListener|_] -> NatListener;
                [] -> undefined
            end;
        error -> undefined
    end.

%% set or replace the list of Addrs associated with ClusterName in the ring
set_clusterIpAddrs(Ring, {ClusterName, Addrs}) ->
    RC = get_repl_config(ensure_config(Ring)),
    OldClusters = get_list(clusters, Ring),
    %% replace Cluster in the list of Clusters
    Cluster = {ClusterName, Addrs},
    Clusters = case lists:keymember(ClusterName, 1, OldClusters) of
                   true ->
                       case Addrs of
                           [] ->
                               lists:keydelete(ClusterName, 1, OldClusters);
                           _Addrs ->
                               lists:keyreplace(ClusterName, 1, OldClusters, Cluster)
                       end;
                   _ when Addrs == [] ->
                       %% New cluster has no members, don't add it to the ring
                       OldClusters;
                   _ ->
                       [Cluster | OldClusters]
               end,
    %% replace Clusters in ring
    RC2 = dict:store(clusters, Clusters, RC),
    case RC == RC2 of
        true ->
            %% nothing changed
            {ignore, {not_changed, clustername}};
        false ->
            {new_ring, riak_core_ring:update_meta(
                    ?MODULE,
                    RC2,
                    Ring)}
    end.

%% get list of Addrs associated with ClusterName in the ring
get_clusterIpAddrs(Ring, ClusterName) ->
    Clusters = get_clusters(Ring),
    case lists:keyfind(ClusterName, 1, Clusters) of
        false -> [];
        {_Name,Addrs} -> Addrs
    end.

get_clusters(Ring) ->
    RC = get_repl_config(ensure_config(Ring)),
    case dict:find(clusters, RC) of
        {ok, Clusters} ->
            Clusters;
        error ->
            []
    end.


%% Enable proxy_get replication for a given remote
pg_enable_trans(Ring, Remote) ->
    add_list_trans(Remote, pg_enabled, Ring).

%% Disable proxy_get replication for a given remote
pg_disable_trans(Ring, Remote) ->
    del_list_trans(Remote, pg_enabled, Ring).

%% Get list of RT enabled remotes
pg_enabled(Ring) ->
    get_list(pg_enabled, Ring).

%% Enable replication for the remote (queue will start building)
rt_enable_trans(Ring, Remote) ->
    add_list_trans(Remote, rt_enabled, Ring).

%% Disable replication for the remote (queue will be cleaned up)
rt_disable_trans(Ring, Remote) ->
    del_list_trans(Remote, rt_enabled, Ring).

%% Get list of RT enabled remotes
rt_enabled(Ring) ->
    get_list(rt_enabled, Ring).

%% Start replication for the remote - make connection and send
rt_start_trans(Ring, Remote) ->
    add_list_trans(Remote, rt_started, Ring).

%% Stop replication for the remote - break connection and queue
rt_stop_trans(Ring, Remote) ->
    del_list_trans(Remote, rt_started, Ring).

%% Get list of RT started remotes
rt_started(Ring) ->
    get_list(rt_started, Ring).

rt_cascades_trans(Ring, Val) ->
    RC = get_repl_config(ensure_config(Ring)),
    OldVal = case dict:find(realtime_cascades, RC) of
        error ->
            % we want to trigger an update to get the config in the ring
            error;
        {ok, V} ->
            V
    end,
    case Val of
        OldVal ->
            {ignore, {no_change, OldVal}};
        _ when Val =:= always; Val =:= never ->
            {new_ring, riak_core_ring:update_meta(
                              ?MODULE,
                              dict:store(realtime_cascades, Val, RC),
                              Ring)};
        _ ->
            lager:warning("ignoring invalid cascading realtime setting: ~p; using old setting ~p", [Val, OldVal]),
            {ignore, {invalid_option, Val}}
    end.

%% Get status of cascading rt
rt_cascades(Ring) ->
    RC = get_repl_config(ensure_config(Ring)),
    % find because the ring may be from a time before cascading rt.
    case dict:find(realtime_cascades, RC) of
        error ->
            never;
        {ok, Val} ->
            Val
    end.

%% Enable replication for the remote (queue will start building)
fs_enable_trans(Ring, Remote) ->
    add_list_trans(Remote, fs_enabled, Ring).

%% Disable replication for the remote (queue will be cleaned up)
fs_disable_trans(Ring, Remote) ->
    del_list_trans(Remote, fs_enabled, Ring).

%% Get list of RT enabled remotes
fs_enabled(Ring) ->
    get_list(fs_enabled, Ring).

initial_config() ->
    dict:from_list(
      [{natlisteners, []},
       {listeners, []},
       {sites, []},
       {realtime_cascades, always},
       {version, ?REPL_VERSION}]
      ).

add_list_trans(Item, ListName, Ring) ->
    RC = get_repl_config(ensure_config(Ring)),
    List = case dict:find(ListName, RC) of
               {ok, List1} ->
                   List1;
               error ->
                   []
           end,
    case lists:member(Item, List) of
        false ->
            NewList = lists:usort([Item|List]),
            {new_ring, riak_core_ring:update_meta(
                         ?MODULE,
                         dict:store(ListName, NewList, RC),
                         Ring)};
        true ->
            {ignore, {already_present, Item}}
    end.

del_list_trans(Item, ListName, Ring) ->
    RC = get_repl_config(ensure_config(Ring)),
    List = case dict:find(ListName, RC) of
               {ok, List1} ->
                   List1;
               error ->
                   []
           end,
    case lists:member(Item, List) of
        true ->
            NewList = List -- [Item],
            {new_ring, riak_core_ring:update_meta(
                         ?MODULE,
                         dict:store(ListName, NewList, RC),
                         Ring)};
        false ->
            {ignore, {not_present, Item}}
    end.

%% Lookup the list name in the repl config, return empty list if not found
get_list(ListName, Ring) ->
    RC = get_repl_config(ensure_config(Ring)),
    case dict:find(ListName, RC) of
        {ok, List}  ->
            List;
        error ->
            []
    end.

%% set the "mode" for realtime repl behavior
%% possible values are 
%% v1, v2, v3
set_modes(Ring, NewModes) ->
    % it doesn't make sense to have a mode_repl13_migrating to represent
    % node shutdown repl migration hook since the mode is stored in the
    % repl ring
    ModeCheck = lists:all(fun (Mode) -> 
                            proplists:is_defined(Mode,
                            ?REPL_MODES) end,
                            NewModes)
                          andalso length(NewModes) > 0,
    case ModeCheck of
        true ->
            RC = get_repl_config(Ring),
            % just overwrite whatever was there before
            NewState = riak_core_ring:update_meta(
                ?MODULE,
                dict:store(repl_modes, NewModes, RC),
                Ring),
            %% force the bucket hooks to be reinstalled
            riak_core_ring_manager:force_update(),
            NewState;
        false ->
            lager:warning("Invalid replication modes specified: ~p", [NewModes]),
            Ring
    end.

get_modes(Ring) ->
    RC = get_repl_config(Ring),
    case dict:find(repl_modes, RC) of
        {ok, ReplModes} -> ReplModes;
        error ->
            %% default to mixed modes
            [mode_repl12, mode_repl13]
    end.

add_nat_map(Ring, Mapping) ->
    add_list_trans(Mapping, nat_map, Ring).

del_nat_map(Ring, Mapping) ->
    del_list_trans(Mapping, nat_map, Ring).

get_nat_map(Ring) ->
    get_list(nat_map, Ring).

%% Function composition
compose(F,G) -> fun(X) -> F(G(X)) end.
multicompose(Fs) ->
    lists:foldl(fun compose/2, fun(X) -> X end, Fs).

%% unit tests

-ifdef(TEST).

mock_ring() ->
    riak_core_ring:fresh(16, 'test@test').

ensure_config_test() ->
    Ring = ensure_config(mock_ring()),
    ?assertNot(undefined =:= riak_core_ring:get_meta(?MODULE, Ring)),
    Ring.

add_get_site_test() ->
    Ring0 = ensure_config_test(),
    Site = #repl_site{name="test"},
    Ring1 = add_site(Ring0, Site),
    Site = get_site(Ring1, "test"),
    Ring1.

add_site_addr_test() ->
    Ring0 = add_get_site_test(),
    Site = get_site(Ring0, "test"),
    ?assertEqual([], Site#repl_site.addrs),
    Ring1 = add_site_addr(Ring0, "test", {"127.0.0.1", 9010}),
    Site1 = get_site(Ring1, "test"),
    ?assertEqual([{"127.0.0.1", 9010}], Site1#repl_site.addrs),
    Ring1.

del_site_addr_test() ->
    Ring0 = add_site_addr_test(),
    Ring1 = del_site_addr(Ring0, "test", {"127.0.0.1", 9010}),
    Site1 = get_site(Ring1, "test"),
    ?assertEqual([], Site1#repl_site.addrs).

del_site_test() ->
    Ring0 = add_get_site_test(),
    Ring1 = del_site(Ring0, "test"),
    ?assertEqual(undefined, get_site(Ring1, "test")).

add_get_listener_test() ->
    Ring0 = ensure_config_test(),
    Listener = #repl_listener{nodename='test@test',
                              listen_addr={"127.0.0.1", 9010}},
    Ring1 = add_listener(Ring0, Listener),
    Listener = get_listener(Ring1, {"127.0.0.1", 9010}),
    Ring1.

del_listener_test() ->
    Ring0 = add_get_listener_test(),
    Ring1 = del_listener(Ring0, #repl_listener{nodename='test@test',
                                               listen_addr={"127.0.0.1", 9010}}),
    ?assertEqual(undefined, get_listener(Ring1, {"127.0.0.1", 9010})).

add_get_natlistener_test() ->
    Ring0 = ensure_config_test(),
    NodeName   = "test@test",
    ListenAddr = "127.0.0.1",
    ListenPort = 9010,
    NatAddr    = "10.11.12.13",
    NatPort    = 9011,
    NatListener = #nat_listener{nodename=NodeName,
                              listen_addr={ListenAddr, ListenPort},
                              nat_addr={NatAddr, NatPort}
                               },
    Ring1 = add_nat_listener(Ring0, NatListener),
    %% you can only get a nat_listener by using a repl_listener
    Listener = #repl_listener{nodename=NodeName,
                              listen_addr={ListenAddr, ListenPort}},
    ?assertNot(undefined == get_nat_listener(Ring1, Listener)),
    Ring1.

%% delete the nat listener using the internal ip address
del_natlistener_test() ->
    Ring0 = add_get_natlistener_test(),
    NodeName   = "test@test",
    ListenAddr = "127.0.0.1",
    ListenPort = 9010,
    Listener = #repl_listener{nodename=NodeName,
                              listen_addr={ListenAddr, ListenPort}},
    Ring1 = del_nat_listener(Ring0, Listener),
    %% functions in riak_repl_ring don't automatically add
    %% both regular and nat listeners. So, a regular listener shouldn't appear
    %% in this test
    ?assertEqual(undefined, get_listener(Ring1, {ListenAddr, ListenPort})),
    ?assertEqual(undefined, get_nat_listener(Ring1, Listener)).

%% delete the nat listener using the public ip address
del_natlistener_publicip_test() ->
    Ring0 = add_get_natlistener_test(),
    NodeName   = "test@test",
    ListenAddr = "10.11.12.13",
    ListenPort = 9011,
    Listener = #repl_listener{nodename=NodeName,
                              listen_addr={ListenAddr, ListenPort}},
    Ring1 = del_nat_listener(Ring0, Listener),
    %% functions in riak_repl_ring don't automatically add
    %% both regular and nat listeners. So, a regular listener shouldn't appear
    %% in this test
    ?assertEqual(undefined, get_listener(Ring1, {ListenAddr, ListenPort})),
    ?assertEqual(undefined, get_nat_listener(Ring1, Listener)).

get_all_listeners(Ring) ->
    RC = riak_repl_ring:get_repl_config(Ring),
    Listeners = dict:fetch(listeners, RC),
    NatListeners = dict:fetch(natlisteners, RC),
    {Listeners, NatListeners}.

add_del_private_and_publicip_nat_test() ->
    Ring0 = ensure_config_test(),
    NodeName   = "test@test",
    ListenAddr = "127.0.0.1",
    ListenPort = 9010,
    NatAddr    = "10.11.12.13",
    NatPort    = 9011,
    NatListener = #nat_listener{nodename=NodeName,
                                listen_addr={ListenAddr, ListenPort},
                                nat_addr={NatAddr, NatPort}
                               },
    %% you can only get a nat_listener by using a repl_listener
    Listener = #repl_listener{nodename=NodeName,
                              listen_addr={ListenAddr, ListenPort}},
    %% similar to riak_repl_console:add_nat_listener
    Ring1 = add_listener(Ring0, Listener),
    Ring2 = add_nat_listener(Ring1, NatListener),
    Ring3 = del_listener(Ring2, Listener),
    Ring4 = del_nat_listener(Ring3, Listener),
    {Listeners, NatListeners} = get_all_listeners(Ring4),
    ?assertEqual(0,length(Listeners)),
    ?assertEqual(0,length(NatListeners)),
    ?assertEqual(undefined, get_listener(Ring4, {ListenAddr, ListenPort})),
    ?assertEqual(undefined, get_nat_listener(Ring4, Listener)).

%% similar to test above, however, just delete the nat address
add_del_private_and_publicip_nat2_test() ->
    Ring0 = ensure_config_test(),
    NodeName   = "test@test",
    ListenAddr = "127.0.0.1",
    ListenPort = 9010,
    NatAddr    = "10.11.12.13",
    NatPort    = 9011,
    NatListener = #nat_listener{nodename=NodeName,
                                listen_addr={ListenAddr, ListenPort},
                                nat_addr={NatAddr, NatPort}
                               },
    %% you can only get a nat_listener by using a repl_listener
    Listener = #repl_listener{nodename=NodeName,
                              listen_addr={ListenAddr, ListenPort}},
    %% similar to riak_repl_console:add_nat_listener
    Ring1 = add_listener(Ring0, Listener),
    Ring2 = add_nat_listener(Ring1, NatListener),
    Ring3 = del_listener(Ring2, Listener),
    {Listeners, NatListeners} = get_all_listeners(Ring3),
    ?assertEqual(0,length(Listeners)),
    ?assertEqual(0,length(NatListeners)),
    ?assertEqual(undefined, get_listener(Ring3, {ListenAddr, ListenPort})),
    ?assertEqual(undefined, get_nat_listener(Ring3, Listener)).

%% similar to test above, however, just delete the nat address
%% This will "downgrade" the listener to a standard listener
add_del_private_and_publicip_nat3_test() ->
    Ring0 = ensure_config_test(),
    NodeName   = "test@test",
    ListenAddr = "127.0.0.1",
    ListenPort = 9010,
    NatAddr    = "10.11.12.13",
    NatPort    = 9011,
    NatListener = #nat_listener{nodename=NodeName,
                                listen_addr={ListenAddr, ListenPort},
                                nat_addr={NatAddr, NatPort}
                               },
    %% you can only get a nat_listener by using a repl_listener
    Listener = #repl_listener{nodename=NodeName,
                              listen_addr={ListenAddr, ListenPort}},
    %% similar to riak_repl_console:add_nat_listener
    Ring1 = add_listener(Ring0, Listener),
    Ring2 = add_nat_listener(Ring1, NatListener),
    Ring3 = del_nat_listener(Ring2, Listener),
    {Listeners, NatListeners} = get_all_listeners(Ring3),
    ?assertEqual(1,length(Listeners)),
    ?assertEqual(0,length(NatListeners)),
    ?assertNot(undefined == get_listener(Ring3, {ListenAddr, ListenPort})),
    ?assertEqual(undefined, get_nat_listener(Ring3, Listener)).

%% verify that adding a listener, and then a nat listener
%% with the same internal IP "upgrades" the current listener
verify_adding_nat_upgrades_test() ->
    Ring0 = riak_repl_ring:ensure_config(mock_ring()),
    NodeName   = "test@test",
    ListenAddr = "127.0.0.1",
    ListenPort = 9010,
    NatAddr    = "10.11.12.13",
    NatPort    = 9011,
    Listener = #repl_listener{nodename=NodeName,
                              listen_addr={ListenAddr, ListenPort}},
    NatListener = #nat_listener{nodename=NodeName,
                                listen_addr={ListenAddr, ListenPort},
                                nat_addr={NatAddr, NatPort}
                               },
    %% add a regular listener
    Ring1 = riak_repl_ring:add_listener(Ring0, Listener),
    %% then add a nat listener. The regular listener now becomes a NAT listener
    Ring2 = riak_repl_ring:add_nat_listener(Ring1, NatListener),
    {Listeners, NatListeners} = get_all_listeners(Ring2),
    ?assertEqual(1,length(Listeners)),
    ?assertEqual(1,length(NatListeners)),
    ?assertNot(undefined == riak_repl_ring:get_listener(Ring2, {ListenAddr, ListenPort})).

%% cascading rt defaults to always
realtime_cascades_defaults_always_test() ->
    Ring0 = riak_repl_ring:ensure_config(mock_ring()),
    ?assertEqual(always, riak_repl_ring:rt_cascades(Ring0)).

%% disable rt cascading
realtime_cascades_disable_test() ->
    Ring0 = riak_repl_ring:ensure_config(mock_ring()),
    TransRes = riak_repl_ring:rt_cascades_trans(Ring0, never),
    ?assertMatch({new_ring, _Ring}, TransRes),
    {new_ring, Ring1} = TransRes,
    ?assertEqual(never, riak_repl_ring:rt_cascades(Ring1)).

%% disable and re-enable
realtime_cascades_enabled_test() ->
    Ring0 = riak_repl_ring:ensure_config(mock_ring()),
    {new_ring, Ring1} = riak_repl_ring:rt_cascades_trans(Ring0, never),
    TransRes = riak_repl_ring:rt_cascades_trans(Ring1, always),
    ?assertMatch({new_ring, _Ring}, TransRes),
    {new_ring, Ring2} = TransRes,
    ?assertEqual(always, riak_repl_ring:rt_cascades(Ring2)).

%% attempt to set to something invalid
realtime_cascades_invalid_set_test() ->
    Ring0 = riak_repl_ring:ensure_config(mock_ring()),
    BadOpt = sometimes,
    ?assertMatch({ignore, {invalid_option, BadOpt}}, riak_repl_ring:rt_cascades_trans(Ring0, BadOpt)).

-endif.
