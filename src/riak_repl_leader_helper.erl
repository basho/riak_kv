%% Riak EnterpriseDS
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.

%%===================================================================
%%
%% Helper module for riak_repl_leader_helper used for holding
%% elections for the replication leader.  gen_leader is used
%% for elections but must use a static list of leader candidates.
%% To accomodate the dynamic nature of riak replication, the
%% registered name for the module is based on a hash of the configured
%% nodes in the system. This guarantees that the election will have 
%% the same candidates/workers. The main riak_repl_leader module restart
%% this helper when the replication configuration changes.
%%
%% The approach of naming the gen_leader process after the candidates
%% runs the risk of exhausting all available atoms on the node if there
%% are enough changes to the nodes in the ring.  In normal production
%% use this is unlikely and the extra atoms will go away when the node
%% is restarted.  It was the best option of a bad bunch (try and
%% create some kind of proxy process, change gen_leader to add the
%% candidate hash to each message).
%%===================================================================
-module(riak_repl_leader_helper).
-behaviour(gen_leader).
-export([start_link/4, leader_node/1, leader_node/2, refresh_leader/1]).
-export([init/1,elected/3,surrendered/3,handle_leader_call/4, 
         handle_leader_cast/3, from_leader/3, handle_call/4,
         handle_cast/3, handle_DOWN/3, handle_info/2, terminate/2,
         code_change/4]).

-define(LEADER_OPTS, [{vardir, VarDir}, {bcast_type, all}]).

-record(state, {leader_mod,   % module name of replication leader
                local_pid,    % pid of local replication leader gen_server
                elected_node, % node last elected
                elected_pid}).% pid or replication leader on elected node

%%%===================================================================
%%% API
%%%===================================================================

start_link(LeaderModule, LeaderPid, Candidates, Workers) ->
    %% Make a unique name for this list of candidates.  gen_leader protocol is designed
    %% to only handle static lists of candidate nodes.  Make sure this is true by
    %% basing the name of the server on the list of candidates.
    Hash = integer_to_list(erlang:phash2({Candidates, Workers})),
    CandidateHashStr = Hash,
    LeaderModuleStr = atom_to_list(LeaderModule),
    RegStr = LeaderModuleStr ++ "_" ++ CandidateHashStr,
    RegName = list_to_atom(RegStr),

    %% Make sure there is a unique directory for this election
    {ok, DataRootDir} = application:get_env(riak_repl, data_root),
    VarDir = filename:join(DataRootDir, RegStr),
    ok = filelib:ensure_dir(filename:join(VarDir, ".empty")),

    LOpts = [{vardir, VarDir},{workers, Workers}, {bcast_type, all}],
    gen_leader:start_link(RegName, Candidates, LOpts, ?MODULE, [LeaderModule, LeaderPid], []).

leader_node(HelperPid) ->
    leader_node(HelperPid, 5000).

leader_node(HelperPid, Timeout) ->
    gen_leader:call(HelperPid, leader_node, Timeout).

%% Asyncrhonously request a refresh of the leader... gen_leader can 
%% block until a candidate node comes up
refresh_leader(HelperPid) ->
    gen_leader:cast(HelperPid, refresh_leader).

%%%===================================================================
%%% gen_leader callbacks
%%%===================================================================

init([LeaderModule, LeaderPid]) ->
    {ok, #state{leader_mod=LeaderModule, local_pid=LeaderPid}}.

elected(State, _NewElection, _Node) ->
    NewState = State#state{elected_node=node(), elected_pid=State#state.local_pid},
    tell_who_leader_is(NewState),
    {ok, {i_am_leader, node(), State#state.local_pid}, NewState}.

surrendered(State, {i_am_leader, Leader, ElectedPid}, _NewElection) ->
    NewState = State#state{elected_node=Leader, elected_pid=ElectedPid},
    tell_who_leader_is(NewState),
    {ok, NewState}.

handle_leader_call(Msg, _From,  State, _E) ->
    {stop, {badmsg, Msg}, badmsg, State}.

handle_leader_cast(Msg, State, _Election) ->
    {stop, {badmsg, Msg}, State}.

from_leader({i_am_leader, _Leader, _ElectedPid}, State, _NewElection) ->
    %% sent as a result of the 2nd element of the elected/3 return tuple - ignored
    {ok, State}.

handle_call(leader_node, _From, State, E) ->
    {reply, gen_leader:leader_node(E), State}.

handle_cast(refresh_leader, State, _E) ->
    tell_who_leader_is(State),
    {noreply, State}.

handle_DOWN(Node, State, _Election) ->
    %% only seems to fire when non-leader candidate nodes go down.  not useful for
    %% replication purposes.
    lager:info("Replication candidate node ~p down", [Node]),
    {ok, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Election, _Extra) -> 
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% Tell who the main riak_repl_leader process who the leader is
tell_who_leader_is(State) ->
    LeaderModule = State#state.leader_mod,
    LeaderModule:set_leader(State#state.local_pid, 
                            State#state.elected_node,
                            State#state.elected_pid).
    
  
