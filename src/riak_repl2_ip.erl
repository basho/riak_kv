%% Riak EnterpriseDS
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved.
-module(riak_repl2_ip).
-export([get_matching_address/2, determine_netmask/2, mask_address/2,
        maybe_apply_nat_map/3, apply_reverse_nat_map/3]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% @doc Given the result of `inet:getifaddrs()' and an IP a client has
%%      connected to, attempt to determine the appropriate subnet mask.  If
%%      the IP the client connected to cannot be found, undefined is returned.
determine_netmask(Ifaddrs, SeekIP) when is_list(SeekIP) ->
    {ok, NormIP} = riak_repl_util:normalize_ip(SeekIP),
    determine_netmask(Ifaddrs, NormIP);

determine_netmask([], _NormIP) ->
    undefined;

determine_netmask([{_If, Attrs} | Tail], NormIP) ->
    case lists_pos({addr, NormIP}, Attrs) of
        not_found ->
            determine_netmask(Tail, NormIP);
        N ->
            case lists:nth(N + 1, Attrs) of
                {netmask, {_, _, _, _} = NM} ->
                    cidr(list_to_binary(tuple_to_list(NM)),0);
                _ ->
                    determine_netmask(Tail, NormIP)
            end
    end.

lists_pos(Needle, Haystack) ->
    lists_pos(Needle, Haystack, 1).

lists_pos(_Needle, [], _N) ->
    not_found;

lists_pos(Needle, [Needle | _Haystack], N) ->
    N;

lists_pos(Needle, [_NotNeedle | Haystack], N) ->
    lists_pos(Needle, Haystack, N + 1).

%% count the number of 1s in netmask to get the CIDR
%% Maybe there's a better way....?
cidr(<<>>, Acc) ->
    Acc;
cidr(<<X:1/bits, Rest/bits>>, Acc) ->
    case X of
        <<1:1>> ->
            cidr(Rest, Acc + 1);
        _ ->
            cidr(Rest, Acc)
    end.

%% @doc Get the subnet mask as an integer, stolen from an old post on
%%      erlang-questions.
mask_address(Addr={_, _, _, _}, Maskbits) ->
    B = list_to_binary(tuple_to_list(Addr)),
    lager:debug("address as binary: ~w ~w", [B,Maskbits]),
    <<Subnet:Maskbits, _Host/bitstring>> = B,
    Subnet;
mask_address(_, _) ->
    %% presumably ipv6, don't have a function for that one yet
    undefined.

%% return RFC1918 mask for IP or false if not in RFC1918 range
rfc1918({10, _, _, _}) ->
    8;
rfc1918({192,168, _, _}) ->
    16;
rfc1918(IP={172, _, _, _}) ->
    %% this one is a /12, not so simple
    case mask_address({172, 16, 0, 0}, 12) == mask_address(IP, 12) of
        true ->
            12;
        false ->
            false
    end;
rfc1918(_) ->
    false.

%% true/false if IP is RFC1918
is_rfc1918(IP) ->
    case rfc1918(IP) of
        false ->
            false;
        _ ->
            true
    end.

%% @doc Find the right address to serve given the IP the node connected to.
%%      Ideally, it will choose an IP in the same subnet, but it will fall
%%      back to the 'closest' subnet (at least up to a class A). Then it will
%%      just try to find anything that matches the IP's RFC 1918 status (ie.
%%      public or private). Localhost will never be 'guessed', but it can be
%%      directly matched.
get_matching_address(IP, CIDR) ->
    {ok, MyIPs} = inet:getifaddrs(),
    get_matching_address(IP, CIDR, MyIPs).

get_matching_address(IP, CIDR, MyIPs) ->
    {RawListenIP, Port} = app_helper:get_env(riak_core, cluster_mgr),
    {ok, ListenIP} = riak_repl_util:normalize_ip(RawListenIP),
    case ListenIP of
        {0, 0, 0, 0} ->
            case rfc1918(IP) of
                false ->
                    %% search as low as a class A
                    find_best_ip(MyIPs, IP, Port, CIDR, 8);
                RFCCIDR ->
                    %% search as low as the bottom of the RFC1918 subnet
                    find_best_ip(MyIPs, IP, Port, CIDR, RFCCIDR)
            end;
        _ ->
            case is_rfc1918(IP) == is_rfc1918(ListenIP) of
                true ->
                    %% Both addresses are either internal or external.
                    %% We'll have to assume the user knows what they're
                    %% doing
                    lager:debug("returning specific listen IP ~p",
                            [ListenIP]),
                    {ListenIP, Port};
                false ->
                    %% we should never get here if things are configured right
                    lager:warning("NAT detected, do you need to define a"
                        " nat-map?"),
                    undefined
            end
    end.


find_best_ip(MyIPs, MyIP, Port, MyCIDR, MaxDepth) when MyCIDR < MaxDepth ->
    %% CIDR is now too small to meaningfully return a result
    %% blindly return *anything* that is close, I guess?
    lager:warning("Unable to find an approximate match for ~s/~b,"
        "trying to guess one.",
        [inet_parse:ntoa(MyIP), MyCIDR]),

    %% when guessing, only use non-loopback, "up" interfaces
    NonLoopbackIPs = riak_repl_util:non_loopback_interfaces(MyIPs),

    Res = lists:foldl(fun({_IF, Attrs}, Acc) ->
                V4Attrs = lists:filter(fun({addr, {_, _, _, _}}) ->
                            true;
                        ({netmask, {_, _, _, _}}) ->
                            true;
                        (_) ->
                            false
                    end, Attrs),
                case V4Attrs of
                    [] ->
                        lager:debug("no valid IPs for ~s", [_IF]),
                        Acc;
                    _ ->
                        lager:debug("IPs for ~s : ~p", [_IF, V4Attrs]),
                        IP = proplists:get_value(addr, V4Attrs),
                        case is_rfc1918(MyIP) == is_rfc1918(IP) of
                            true ->
                                lager:debug("wildly guessing that  ~p is close"
                                    "to ~p", [IP, MyIP]),
                                {IP, Port};
                            false ->
                                Acc
                        end
                end
        end, undefined, NonLoopbackIPs),
    case Res of
        undefined ->
            lager:warning("Unable to guess an appropriate local IP to match"
                " ~s/~b", [inet_parse:ntoa(MyIP), MyCIDR]),
            Res;
        {IP, _Port} ->
            lager:notice("Guessed ~s to match ~s/~b",
                [inet_parse:ntoa(IP), inet_parse:ntoa(MyIP), MyCIDR]),
            Res
    end;

find_best_ip(MyIPs, MyIP, Port, MyCIDR, MaxDepth) ->
    Res = lists:foldl(fun({_IF, Attrs}, Acc) ->
                V4Attrs = lists:filter(fun({addr, {_, _, _, _}}) ->
                            true;
                        ({netmask, {_, _, _, _}}) ->
                            true;
                        (_) ->
                            false
                    end, Attrs),
                case V4Attrs of
                    [] ->
                        lager:debug("no valid IPs for ~s", [_IF]),
                        Acc;
                    _ ->
                        lager:debug("IPs for ~s : ~p", [_IF, V4Attrs]),
                        IP = proplists:get_value(addr, V4Attrs),
                        case {mask_address(IP, MyCIDR),
                                mask_address(MyIP, MyCIDR)}  of
                            {Mask, Mask} ->
                                %% the 172.16/12 is a pain in the ass
                                case is_rfc1918(IP) == is_rfc1918(MyIP) of
                                    true ->
                                        lager:debug("matched IP ~p for ~p", [IP,
                                                MyIP]),
                                        {IP, Port};
                                    false ->
                                        Acc
                                end;
                            {_A, _B} ->
                                lager:debug("IP ~p with CIDR ~p masked as ~p",
                                        [IP, MyCIDR, _A]),
                                lager:debug("IP ~p with CIDR ~p masked as ~p",
                                        [MyIP, MyCIDR, _B]),
                                Acc
                        end
                end
        end, undefined, MyIPs),
    case Res of
        undefined ->
            %% Increase the search depth and retry, this will decrement the
            %% CIDR masks by one
            find_best_ip(MyIPs, MyIP, Port, MyCIDR - 1, MaxDepth);
        Res ->
            Res
    end.

%% Apply the relevant NAT-mapping rule, if any
maybe_apply_nat_map(IP, Port, Map0) ->
    Map = expand_hostnames(Map0),
    case lists:keyfind({IP, Port}, 1, Map) of
        false ->
            case lists:keyfind(IP, 1, Map) of
                {_, {InternalIP, _InternalPort}} ->
                    InternalIP;
                {_, InternalIP} ->
                    InternalIP;
                false ->
                    IP
            end;
        {_, {InternalIP, _InternalPort}} ->
            InternalIP;
        {_, InternalIP} ->
            InternalIP
    end.

%% Find the external IP for this interal IP
%% Should only be called when you know NAT is in effect
apply_reverse_nat_map(IP, Port, Map0) ->
    Map = expand_hostnames(Map0),
    case lists:keyfind({IP, Port}, 2, Map) of
        false ->
            case lists:keyfind(IP, 2, Map) of
                false ->
                    error;
                {Res, _} ->
                    Res
            end;
        {Res, _} ->
            %% this will either be the IP or an {IP, Port} tuple
            Res
    end.

expand_hostnames(Map) ->
    lists:reverse(lists:foldl(fun expand_hostnames/2, [],  Map)).

expand_hostnames({External, {Host, InternalPort}}, Acc) when is_list(Host) ->
    %% resolve it via /etc/hosts
    case inet_gethost_native:gethostbyname(Host) of
        {ok, {hostent, _Domain, _, inet, 4, Addresses}} ->
            lager:debug("locally resolved ~p to ~p", [Host, Addresses]),
            [{External, {Addr, InternalPort}} || Addr <- Addresses] ++ Acc;
        Res ->
            %% resolve it via the configured nameserver
            case inet_res:gethostbyname(Host) of
                {ok, {hostent, _Domain, _, inet, 4, Addresses}} ->
                    lager:debug("remotely resolved ~p to ~p", [Host, Addresses]),
                    [{External, {Addr, InternalPort}} || Addr <- Addresses] ++ Acc;
                Res2 ->
                    lager:warning("Failed to resolve ~p locally: ~p", [Host, Res]),
                    lager:warning("Failed to resolve ~p remotely: ~p", [Host, Res2]),
                    Acc
            end
    end;
expand_hostnames({External, Host}, Acc) when is_list(Host) ->
    %% resolve it via /etc/hosts
    case inet_gethost_native:gethostbyname(Host) of
        {ok, {hostent, _Domain, _, inet, 4, Addresses}} ->
            lager:debug("locally resolved ~p to ~p", [Host, Addresses]),
            [{External, Addr} || Addr <- Addresses] ++ Acc;
        Res ->
            %% resolve it via the configured nameserver
            case inet_res:gethostbyname(Host) of
                {ok, {hostent, _Domain, _, inet, 4, Addresses}} ->
                    lager:debug("remotely resolved ~p to ~p", [Host, Addresses]),
                    [{External, Addr} || Addr <- Addresses] ++ Acc;
                Res2 ->
                    lager:warning("Failed to resolve ~p locally: ~p", [Host, Res]),
                    lager:warning("Failed to resolve ~p remotely: ~p", [Host, Res2]),
                    Acc
            end
    end;
expand_hostnames(Any, Acc) ->
    [Any|Acc].

-ifdef(TEST).

make_ifaddrs(Interfaces) ->
    lists:ukeymerge(1, lists:usort(Interfaces), lists:usort([
                {"lo",
                    [{flags,[up,loopback,running]},
                        {hwaddr,[0,0,0,0,0,0]},
                        {addr,{127,0,0,1}},
                        {netmask,{255,0,0,0}},
                        {addr,{0,0,0,0,0,0,0,1}},
                        {netmask,{65535,65535,65535,65535,65535,65535,65535,
                                65535}}]}])).


get_matching_address_test_() ->
        error_logger:tty(false),
        {setup,
         fun() ->
                 application:set_env(riak_core, cluster_mgr, {{0, 0, 0, 0},
                                                              9090}),
                 ok
         end,
         fun(_) ->
                 application:unset_env(riak_core, cluster_mgr)
         end,
         [
          {timeout, 30,
           {"adjacent RFC 1918 IPs in subnet",
            fun() ->
                    Addrs = make_ifaddrs([{"eth0",
                                           [{flags,[up,broadcast,running,multicast]},
                                            {addr, {10, 0, 0, 99}},
                                            {netmask, {255, 0, 0, 0}}]}]),
                    Res = get_matching_address({10, 0, 0, 1}, 8, Addrs),
                    ?assertEqual({{10,0,0,99},9090}, Res)
            end}},
          {timeout, 30,
           {"RFC 1918 IPs in adjacent subnets",
            fun() ->
                    Addrs = make_ifaddrs([{"eth0",
                                           [{flags,[up,broadcast,running,multicast]},
                                            {addr, {10, 4, 0, 99}},
                                            {netmask, {255, 255, 255, 0}}]}]),
                    Res = get_matching_address({10, 0, 0, 1}, 24, Addrs),
                    ?assertEqual({{10,4,0,99},9090}, Res)
            end
           }},
          {timeout, 30,
           {"RFC 1918 IPs in different RFC 1918 blocks",
            fun() ->
                    Addrs = make_ifaddrs([{"eth0",
                                           [{flags,[up,broadcast,running,multicast]},
                                            {addr, {10, 4, 0, 99}},
                                            {netmask, {255, 0, 0, 0}}]}]),
                    Res = get_matching_address({192, 168, 0, 1}, 24, Addrs),
                    ?assertEqual({{10,4,0,99},9090}, Res)
            end
           }},
          {timeout, 30,
           {"adjacent public IPs in subnet",
            fun() ->
                    Addrs = make_ifaddrs([{"eth0",
                                           [{flags,[up,broadcast,running,multicast]},
                                            {addr, {8, 8, 8, 8}},
                                            {netmask, {255, 255, 255, 0}}]}]),
                    Res = get_matching_address({8, 8, 8, 1}, 24, Addrs),
                    ?assertEqual({{8,8,8,8},9090}, Res)
            end
           }},
          {timeout, 30,
           {"public IPs in adjacent subnets",
            fun() ->
                    Addrs = make_ifaddrs([{"eth0",
                                           [{flags,[up,broadcast,running,multicast]},
                                            {addr, {8, 0, 8, 8}},
                                            {netmask, {255, 255, 255, 0}}]}]),
                    Res = get_matching_address({8, 8, 8, 1}, 24, Addrs),
                    ?assertEqual({{8,0,8,8},9090}, Res)
            end
           }},
          {timeout, 30,
           {"public IPs in different /8s",
            fun() ->
                    Addrs = make_ifaddrs([{"eth0",
                                           [{flags,[up,broadcast,running,multicast]},
                                            {addr, {8, 0, 8, 8}},
                                            {netmask, {255, 255, 255, 0}}]}]),
                    Res = get_matching_address({64, 8, 8, 1}, 24, Addrs),
                    ?assertEqual({{8,0,8,8},9090}, Res)
            end
           }},
          {timeout, 30,
           {"connecting to localhost",
            fun() ->
                    Addrs = make_ifaddrs([{"eth0",
                                           [{flags,[up,broadcast,running,multicast]},
                                            {addr, {8, 0, 8, 8}},
                                            {netmask, {255, 255, 255, 0}}]}]),
                    Res = get_matching_address({127, 0, 0, 1}, 8, Addrs),
                    ?assertEqual({{127,0,0,1},9090}, Res)
            end
           }},
          {timeout, 30,
           {"RFC 1918 IPs when all we have are public ones",
            fun() ->
                    Addrs = make_ifaddrs([{"eth0",
                                           [{flags,[up,broadcast,running,multicast]},
                                            {addr, {172, 0, 8, 8}},
                                            {netmask, {255, 255, 255, 0}}]}]),
                    Res = get_matching_address({172, 16, 0, 1}, 24, Addrs),
                    ?assertEqual(undefined, Res)
            end
           }},
          {timeout, 30,
           {"public IPs when all we have are RFC1918 ones",
            fun() ->
                    Addrs = make_ifaddrs([{"eth0",
                                           [{flags,[up,broadcast,running,multicast]},
                                            {addr, {172, 16, 0, 1}},
                                            {netmask, {255, 255, 255, 0}}]}]),
                    Res = get_matching_address({172, 0, 8, 8}, 24, Addrs),
                    ?assertEqual(undefined, Res)
            end
           }},
          {timeout, 30,
           {"public IPs when all we have are IPv6 ones",
            fun() ->
                    Addrs = make_ifaddrs([{"eth0",
                                           [{flags,[up,broadcast,running,multicast]},
                                            {addr,
                                             {65152,0,0,0,29270,33279,65179,6921}},
                                            {netmask,
                                             {65535,65535,65535,65535,0,0,0,0}}]}]),
                    Res = get_matching_address({8, 8, 8, 1}, 8, Addrs),
                    ?assertEqual(undefined, Res)
            end
           }},
          {timeout, 30,
           {"public IPs in different subnets, prefer closest",
            fun() ->
                    Addrs = make_ifaddrs([{"eth0",
                                           [{flags,[up,broadcast,running,multicast]},
                                            {addr, {8, 0, 8, 8}},
                                            {netmask, {255, 255, 255, 0}}]},
                                          {"eth1",
                                           [{flags,[up,broadcast,running,multicast]},
                                            {addr, {64, 172, 243, 100}},
                                            {netmask, {255, 255, 255, 0}}]}
                                         ]),
                    Res = get_matching_address({64, 8, 8, 1}, 24, Addrs),
                    ?assertEqual({{64,172,243,100},9090}, Res)
            end
           }},
          {timeout, 30,
           {"listen IP is not 0.0.0.0, return statically configured IP if both public",
            fun() ->
                    application:set_env(riak_core, cluster_mgr, {{12, 24, 36, 8},
                                                                 9096}),
                    Addrs = make_ifaddrs([{"eth0",
                                           [{flags,[up,broadcast,running,multicast]},
                                            {addr, {8, 0, 8, 8}},
                                            {netmask, {255, 255, 255, 0}}]},
                                          {"eth1",
                                           [{flags,[up,broadcast,running,multicast]},
                                            {addr, {64, 172, 243, 100}},
                                            {netmask, {255, 255, 255, 0}}]}
                                         ]),
                    Res = get_matching_address({64, 8, 8, 1}, 24, Addrs),
                    ?assertEqual({{12,24,36,8},9096}, Res)
            end
           }},
          {timeout, 30,
           {"listen IP is not 0.0.0.0, return statically configured IP if both private",
            fun() ->
                    application:set_env(riak_core, cluster_mgr, {{192, 168, 1, 1},
                                                                 9096}),
                    Addrs = make_ifaddrs([{"eth0",
                                           [{flags,[up,broadcast,running,multicast]},
                                            {addr, {10, 0, 0, 1}},
                                            {netmask, {255, 255, 255, 0}}]},
                                          {"eth1",
                                           [{flags,[up,broadcast,running,multicast]},
                                            {addr, {64, 172, 243, 100}},
                                            {netmask, {255, 255, 255, 0}}]}
                                         ]),
                    Res = get_matching_address({10, 0, 0, 1}, 24, Addrs),
                    ?assertEqual({{192,168,1,1},9096}, Res)
            end
           }},
          {timeout, 30,
           {"listen IP is not 0.0.0.0, return undefined if both not public/private",
            fun() ->
                    application:set_env(riak_core, cluster_mgr, {{192, 168, 1, 1},
                                                                 9096}),
                    Addrs = make_ifaddrs([{"eth0",
                                           [{flags,[up,broadcast,running,multicast]},
                                            {addr, {10, 0, 0, 1}},
                                            {netmask, {255, 255, 255, 0}}]},
                                          {"eth1",
                                           [{flags,[up,broadcast,running,multicast]},
                                            {addr, {64, 172, 243, 100}},
                                            {netmask, {255, 255, 255, 0}}]}
                                         ]),
                    Res = get_matching_address({8, 8, 8, 8}, 24, Addrs),
                    ?assertEqual(undefined, Res)
            end
           }}
         ]}.

determine_netmask_test_() ->
    error_logger:tty(false),
    [
        {"simple case",
            fun() ->
                    Addrs = make_ifaddrs([{"eth0",
                                [{flags,[up,broadcast,running,multicast]},
                                 {addr, {10, 0, 0, 1}},
                                 {netmask, {255, 255, 255, 0}}]}]),
                    ?assertEqual(24, determine_netmask(Addrs,
                            {10, 0, 0, 1}))
            end
        },
        {"loopback",
            fun() ->
                    Addrs = make_ifaddrs([{"eth0",
                                [{flags,[up,broadcast,running,multicast]},
                                 {addr, {10, 0, 0, 1}},
                                    {netmask, {255, 255, 255, 0}}]}]),
                    ?assertEqual(8, determine_netmask(Addrs,
                            {127, 0, 0, 1}))
            end
        }

    ].

natmap_test_() ->
    error_logger:tty(false),
    [{timeout, 30,
        {"forward lookups work",
            fun() ->
                    Map = [
                        {{65, 172, 243, 10}, {10, 0, 0, 10}},
                        {{65, 172, 243, 11}, {10, 0, 0, 11}},
                        {{65, 172, 243, 12}, {10, 0, 0, 12}}
                    ],
                    ?assertEqual({10, 0, 0, 11},
                        maybe_apply_nat_map({65, 172, 243, 11}, 9080, Map)),
                    ?assertEqual({10, 0, 0, 10},
                        maybe_apply_nat_map({65, 172, 243, 10}, 9090, Map)),
                    ?assertEqual({10, 0, 0, 10},
                        maybe_apply_nat_map({10, 0, 0, 10}, 9090, Map)),
                    ok
            end
        }},
        {timeout, 30, {"forward lookups with ports work",
            fun() ->
                    Map = [
                        {{{65, 172, 243, 10}, 10080}, {10, 0, 0, 10}},
                        {{{65, 172, 243, 10}, 10081}, {10, 0, 0, 11}},
                        {{{65, 172, 243, 10}, 10082}, {10, 0, 0, 12}}
                    ],
                    ?assertEqual({10, 0, 0, 10},
                        maybe_apply_nat_map({65, 172, 243, 10}, 10080, Map)),
                    ?assertEqual({10, 0, 0, 11},
                        maybe_apply_nat_map({65, 172, 243, 10}, 10081, Map)),
                    ?assertEqual({10, 0, 0, 10},
                        maybe_apply_nat_map({10, 0, 0, 10}, 9090, Map)),
                    ok
            end
        }},
        {timeout, 30, {"reverse lookups work",
            fun() ->
                    Map = [
                        {{65, 172, 243, 10}, {10, 0, 0, 10}},
                        {{65, 172, 243, 11}, {10, 0, 0, 11}},
                        {{65, 172, 243, 12}, {10, 0, 0, 12}}
                    ],
                    ?assertEqual({65, 172, 243, 10},
                        apply_reverse_nat_map({10, 0, 0, 10}, 9080, Map)),
                    ?assertEqual({65, 172, 243, 11},
                        apply_reverse_nat_map({10, 0, 0, 11}, 9090, Map)),
                    ?assertEqual(error,
                        apply_reverse_nat_map({10, 0, 0, 20}, 9090, Map)),
                    ok
            end
        }},
        {timeout, 30, {"reverse lookups with ports work",
            fun() ->
                    Map = [
                        {{{65, 172, 243, 10}, 10080}, {10, 0, 0, 10}},
                        {{{65, 172, 243, 10}, 10081}, {10, 0, 0, 11}},
                        {{{65, 172, 243, 10}, 10082}, {10, 0, 0, 12}}
                    ],
                    ?assertEqual({{65, 172, 243, 10}, 10080},
                        apply_reverse_nat_map({10, 0, 0, 10}, 9080, Map)),
                    ?assertEqual({{65, 172, 243, 10}, 10081},
                        apply_reverse_nat_map({10, 0, 0, 11}, 9080, Map)),
                    ?assertEqual(error,
                        apply_reverse_nat_map({10, 0, 0, 20}, 9080, Map)),
                    ok
            end
        }},
        {timeout, 30, {"forward lookups with hostnames work",
            fun() ->
                    Map = [
                        {{65, 172, 243, 10}, "localhost"},
                        {{65, 172, 243, 11}, {10, 0, 0, 11}},
                        {{65, 172, 243, 12}, {10, 0, 0, 12}}
                    ],
                    ?assertEqual({127, 0, 0, 1},
                        maybe_apply_nat_map({65, 172, 243, 10}, 9080, Map)),
                    ok
            end
        }},
        {timeout, 30, {"reverse lookups with hostnames work",
            fun() ->
                    {ok, {hostent, "basho.com", _, inet, 4, Addresses}} = inet_res:gethostbyname("basho.com"),
                    Map = [
                        {{65, 172, 243, 10}, "basho.com"},
                        {{65, 172, 243, 11}, {10, 0, 0, 11}},
                        {{65, 172, 243, 12}, {10, 0, 0, 12}}
                    ],
                    ?assertEqual({65, 172, 243, 10},
                        apply_reverse_nat_map(hd(Addresses), 9080, Map)),
                    ok
            end
        }}
    ].

-endif.
