%% Riak EnterpriseDS
%% Copyright 2007-2009 Basho Technologies, Inc. All Rights Reserved.
-module(riak_repl_client_sup).
-author('Andy Gross <andy@basho.com>').
-behaviour(supervisor).
-include("riak_repl.hrl").
-export([start_link/0, init/1, stop/1]).
-export([start_site/1, stop_site/1, running_site_procs/0,
        running_site_procs_rpc/0, ensure_sites/1]).

start_site(SiteName) ->
    lager:info("Starting replication site ~p", [SiteName]),
    ChildSpec = make_site(SiteName),
    supervisor:start_child(?MODULE, ChildSpec).

stop_site(SiteName) ->
    lager:info("Stopping replication site ~p", [SiteName]),
    _ = supervisor:terminate_child(?MODULE, SiteName),
    _ = supervisor:delete_child(?MODULE, SiteName).

running_site_procs() ->
    [{SiteName, Pid} || {SiteName, Pid, _, _} <- supervisor:which_children(?MODULE)].

%% return the node along with the running sites for accounting
running_site_procs_rpc() ->
    {node(), catch(running_site_procs())}.

ensure_sites(Ring) ->
    ReplConfig = 
    case riak_repl_ring:get_repl_config(Ring) of
        undefined ->
            riak_repl_ring:initial_config();
        RC -> RC
    end,
    CurrentConfig = running_site_procs(),
    CurrentSites = lists:map(fun({SiteName, _Pid}) -> SiteName end,
        CurrentConfig),
    ConfiguredSites = [Site#repl_site.name || Site <- dict:fetch(sites, ReplConfig)],
    ToStop = sets:to_list(
               sets:subtract(
                 sets:from_list(CurrentSites), 
                 sets:from_list(ConfiguredSites))),
    ToStart = sets:to_list(
               sets:subtract(
                 sets:from_list(ConfiguredSites), 
                 sets:from_list(CurrentSites))),
    _ = [start_site(SiteName) || SiteName <- ToStart],
    _ = [stop_site(SiteName) || SiteName <- ToStop],
    ok.

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

stop(_S) -> ok.

%% @private
init([]) ->
    {ok, {{one_for_one, 100, 10}, []}}.

make_site(SiteName) ->
    {SiteName, {riak_repl_tcp_client, start_link, [SiteName]},
        permanent, brutal_kill, worker, [riak_repl_tcp_client]}.
