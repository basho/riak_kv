%% Riak EnterpriseDS
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
-module(riak_repl_listener_sup).
-author('Andy Gross <andy@basho.com>').
-include("riak_repl.hrl").
-export([start_listener/1, ensure_listeners/1, close_all_connections/0,
        server_pids/0]).

start_listener(Listener = #repl_listener{listen_addr={IP, Port}}) ->
    case riak_repl_util:valid_host_ip(IP) of
        true ->
            lager:info("Starting replication listener on ~s:~p",
                [IP, Port]),
            {ok, RawAddress} = inet_parse:address(IP),
            ranch:start_listener(Listener, 10, ranch_tcp,
                [{ip, RawAddress}, {port, Port}], riak_repl_tcp_server, []);
        _ ->
            lager:error("Cannot start replication listener "
                "on ~s:~p - invalid address.",
                [IP, Port])
    end.

ensure_listeners(Ring) ->
    ReplConfig = 
    case riak_repl_ring:get_repl_config(Ring) of
        undefined ->
            riak_repl_ring:initial_config();
        RC -> RC
    end,
    CurrentListeners = [L ||
        {{_, L}, Pid, _Type, _Modules} <- supervisor:which_children(ranch_sup),
        is_pid(Pid), is_record(L, repl_listener)],
    ConfiguredListeners = [Listener || Listener <- dict:fetch(listeners, ReplConfig),
        Listener#repl_listener.nodename == node()],
    ToStop = sets:to_list(
               sets:subtract(
                 sets:from_list(CurrentListeners), 
                 sets:from_list(ConfiguredListeners))),
    ToStart = sets:to_list(
               sets:subtract(
                 sets:from_list(ConfiguredListeners), 
                 sets:from_list(CurrentListeners))),
    _ = [start_listener(Listener) || Listener <- ToStart],
    lists:foreach(fun(Listener) ->
                {IP, Port} = Listener#repl_listener.listen_addr,
                lager:info("Stopping replication listener on ~s:~p",
                    [IP, Port]),
                ranch:stop_listener(Listener)
        end, ToStop),
    ok.

close_all_connections() ->
    [exit(P, kill) ||  P <-
        server_pids()].

server_pids() ->
    %%%%%%%%
    %% NOTE:
    %% This is needed because Ranch doesn't directly expose child PID's.
    %% However, digging into the Ranch supervision tree can cause problems in the
    %% future if Ranch is upgraded. Ideally, this code should be moved into
    %% Ranch. Please check for Ranch updates!
    %%%%%%%%
    [Pid2 ||
        {{ranch_listener_sup, _}, Pid, _Type, _Modules} <- supervisor:which_children(ranch_sup), is_pid(Pid),
        {ranch_conns_sup,Pid1,_,_} <- supervisor:which_children(Pid),
        {_,Pid2,_,_} <- supervisor:which_children(Pid1)].
