%% Riak EnterpriseDS
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
-module(riak_repl_ring_handler).
-author('Andy Gross <andy@basho.com>').

-behaviour(gen_event).

-include("riak_repl.hrl").

%% gen_event callbacks
-export([init/1, handle_event/2, handle_call/2,
         handle_info/2, terminate/2, code_change/3]).

-export([update_leader/1]).

-record(state, { ring :: tuple() }).


%% ===================================================================
%% gen_event Callbacks
%% ===================================================================

init([]) ->
    %% Give the leader the intial set of candidates
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    AllNodes = riak_core_ring:all_members(Ring),
    riak_repl2_leader:set_candidates(AllNodes, []),
    _ = rt_update_events(Ring),
    _ = pg_update_events(Ring),
    {ok, #state{ring=Ring}}.

handle_event({ring_update, Ring}, State=#state{ring=Ring}) ->
    %% Ring is unchanged from previous notification
    {ok, State};
handle_event({ring_update, NewRing}, State=#state{ring=OldRing}) ->
    %% Ring has changed.
    FinalRing = init_repl_config(OldRing, NewRing),
    update_leader(FinalRing),
    _ = rt_update_events(FinalRing),
    _ = pg_update_events(FinalRing),
    riak_repl_listener_sup:ensure_listeners(FinalRing),
    case riak_repl_leader:is_leader() of
        true ->
            riak_repl_leader:ensure_sites();
        _ ->
            ok
    end,
    %% Force the cluster manager to connect to the clusters when it
    %% learns about an event *after* an election has occurred.
    case riak_repl2_leader:is_leader() of
        true ->
            riak_core_cluster_mgr:connect_to_clusters();
        _ ->
            ok
    end,
    {ok, State#state{ring=FinalRing}};
handle_event(_Event, State) ->
    {ok, State}.


handle_call(_Request, State) ->
    {ok, ok, State}.


handle_info(_Info, State) ->
    {ok, State}.


terminate(_Reason, _State) ->
    ok.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%% ===================================================================
%% Internal functions
%% ===================================================================

%%
%% Initialize repl config structure as necessary
%%
init_repl_config(OldRing, NewRing) ->
    %% Check the repl config for changes. The ring is updated
    %% such that we are guaranteed to have an initialized data
    %% structure after this function returns. If necessary, we
    %% also push out the changed ring.
    OldRC = riak_repl_ring:get_repl_config(OldRing),
    NewRC = riak_repl_ring:get_repl_config(NewRing),
    case {OldRC, NewRC} of
        {undefined, undefined} ->
            update_ring(riak_repl_ring:initial_config());
        {_, undefined} ->
            update_ring(OldRC);
        _ ->
            NewRing
    end.


%%
%% Given a new repl config, update the system-local ring.
%%
update_ring(ReplConfig) ->
    lager:info("Repushing new REPL config!"),
    F = fun(Ring, _) ->
                {new_ring, riak_repl_ring:set_repl_config(Ring, ReplConfig)}
        end,
    {ok, FinalRing} = riak_core_ring_manager:ring_trans(F, undefined),
    FinalRing.


%%
%% Pass updated configuration settings to the leader
%%
update_leader(Ring) ->
    AllNodes = riak_core_ring:all_members(Ring),
    riak_repl2_leader:set_candidates(AllNodes, []),
    case riak_repl_ring:get_repl_config(Ring) of
        undefined ->
            ok;
        RC ->
            Listeners = listener_nodes(RC),
            NonListeners = ordsets:to_list(
                             ordsets:subtract(ordsets:from_list(AllNodes),
                                              ordsets:from_list(Listeners))),

            case {has_sites(RC), has_listeners(RC)} of
                {_, true} ->
                    Candidates=Listeners,
                    Workers=NonListeners;
                {true, false} ->
                    Candidates=AllNodes,
                    Workers=[];
                {false, false} ->
                    Candidates=[],
                    Workers=[]
            end,
            riak_repl_listener_sup:ensure_listeners(Ring),
            riak_repl_leader:set_candidates(Candidates, Workers)
    end.

has_sites(ReplConfig) ->
    dict:fetch(sites, ReplConfig) /= [].

has_listeners(ReplConfig) ->
    dict:fetch(listeners, ReplConfig) /= [].

listener_nodes(ReplConfig) ->
    Listeners = dict:fetch(listeners, ReplConfig),
    lists:usort([L#repl_listener.nodename || L <- Listeners]).

%% Run whenever the ring is changed or on startup.
%% Compare desired state of realtime repl to configured
rt_update_events(Ring) ->
    _ = riak_repl2_rt:ensure_rt(riak_repl_ring:rt_enabled(Ring),
                                riak_repl_ring:rt_started(Ring)),
    %% ensure_rt sets this
    RTEnabled = app_helper:get_env(riak_repl, rtenabled, false),

    RC = case riak_repl_ring:get_repl_config(Ring) of
        undefined ->
            riak_repl_ring:initial_config();
        R ->
            R
    end,

    LegacyEnabled = case {has_listeners(RC), has_sites(RC),
          app_helper:get_env(riak_repl, inverse_connection, false)} of
        {false, _, false} ->
            false; % No need to install hook if nobody is listening
        {true, _, false} ->
            true;
        {_, false, true} ->
            false; %% no sites
        {_, true, true} ->
            true
    end,

    case RTEnabled == LegacyEnabled of
        true ->
            ok;
        _ ->
            %% one of them must be true
            application:set_env(riak_repl, rtenabled, true)
    end,

    RTCascades = case dict:find(realtime_cascades, RC) of
        error ->
            always;
        {ok, RTCascadesFound} ->
            RTCascadesFound
    end,
    application:set_env(riak_repl, realtime_cascades, RTCascades),

    %% always 'install' the hook, the postcommit hooks will be toggled by
    %% the rtenabled environment variable
    riak_repl:install_hook().

pg_update_events(Ring) ->
    riak_repl2_pg:ensure_pg(riak_repl_ring:pg_enabled(Ring)).


