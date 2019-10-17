%% Riak EnterpriseDS
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.

%%===================================================================
%% Replication leader - responsible for receiving objects to be replicated
%% by the postcommit hook.  If the node is acting as the leader all
%% objects will be sent to any connected server sockets, if not the
%% object will be sent to the node acting as leader.
%%
%% riak_repl_leader_helper is used to perform the elections and work
%% around the gen_leader limit of having a fixed list of candidates.
%%===================================================================

-module(riak_repl_leader).
-behaviour(gen_server).

-include("riak_repl.hrl").
-type overflow() :: non_neg_integer().
-type clients_per_node() :: non_neg_integer().

%% API
-export([start_link/0,
         set_candidates/2,
         leader_node/0,
         is_leader/0,
         postcommit/1,
         add_receiver_pid/1,
         rm_receiver_pid/1,
         receiver_pids/0]).
-export([set_leader/3]).
-export([ensure_sites/0]).
-export([helper_pid/0]).
-export([balance/2]).
-export([balance_clients/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% Server name has changed for 0.14.0 now that the gen_leader
%% portion has been broken out into riak_repl_leader_helper.
%% During rolling upgrades old gen_leader messages from pre-0.14
%% would be sent to the gen_server
-define(SERVER, riak_repl_leader_gs).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-export([prop_balance/0]).
-endif.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(state, {helper_pid,  % pid of riak_repl_leader_helper
                i_am_leader=false :: boolean(), % true if the leader
                leader_node=undefined :: undefined | node(), % current leader
                leader_mref=undefined :: undefined | reference(), % monitor
                candidates=[] :: [node()],      % candidate nodes for leader
                workers=[node()] :: [node()],   % workers
                receivers=[] :: [{reference(),pid(), drop | send}],
                check_tref :: timer:tref(),     % check mailbox timer
                elected_mbox_size = 0 :: integer(), % elected leader box size
                lastpoll = {0, 0, 0}
               }).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% Set the list of candidate nodes for replication leader
set_candidates(Candidates, Workers) ->
    gen_server:cast(?SERVER, {set_candidates, Candidates, Workers}).

%% Return the current leader node
leader_node() ->
    gen_server:call(?SERVER, leader_node, infinity).

%% Are we the leader?
is_leader() ->
    gen_server:call(?SERVER, is_leader, infinity).

%% Send the object to the leader
postcommit(Object) ->
    case erlang:process_info(whereis(?SERVER), message_queue_len) of
        {message_queue_len, X} when X < 20000 ->
            gen_server:cast(?SERVER, {repl, Object});
        _ ->
            ok
    end.

%% Add the pid of a riak_repl_tcp_sender process.  The pid is monitored
%% and removed from the list when it exits.
add_receiver_pid(Pid) when is_pid(Pid) ->
    gen_server:call(?SERVER, {add_receiver_pid, Pid}, infinity).

rm_receiver_pid(Pid) when is_pid(Pid) ->
    gen_server:call(?SERVER, {rm_receiver_pid, Pid}, infinity).

receiver_pids() ->
    gen_server:call(?SERVER, receiver_pids, infinity).

ensure_sites() ->
    gen_server:cast(?SERVER, ensure_sites).

%%%===================================================================
%%% Callback for riak_repl_leader_helper
%%%===================================================================

%% Called by riak_repl_leader_helper whenever a leadership election
%% takes place.
set_leader(LocalPid, LeaderNode, LeaderPid) ->
    gen_server:call(LocalPid, {set_leader_node, LeaderNode, LeaderPid}, infinity).

%%%===================================================================
%%% Unit test support for riak_repl_leader_helper
%%%===================================================================

helper_pid() ->
    gen_server:call(?SERVER, helper_pid, infinity).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    process_flag(trap_exit, true),
    erlang:send_after(0, self(), update_leader),
    Fn=fun(Services) ->
            case lists:member(riak_kv, Services) of
                true ->
                    %% repl isn't started yet, give it 5 seconds to do so.
                    %% This is particularly important for new candidate nodes
                    %% (ie. new nodes that have no listeners configured)
                    %% because no election changes are triggered.
                    spawn(fun() ->
                                timer:sleep(5000),
                                ensure_sites()
                        end);
                _ ->
                    ok
            end
    end,
    riak_core_node_watcher_events:add_sup_callback(Fn),
    {ok, TRef} = timer:send_interval(1000, check_mailbox),
    {ok, #state{check_tref = TRef}}.

handle_call({add_receiver_pid, Pid}, _From, State) when State#state.i_am_leader =:= true ->
    Mref = erlang:monitor(process, Pid),
    {reply, ok, State#state{receivers = [{Mref, Pid, send} | State#state.receivers]}};
handle_call({add_receiver_pid, _Pid}, _From, State) ->
    {reply, {error, not_leader}, State};

handle_call({rm_receiver_pid, Pid}, _From, State = #state{receivers=R0}) when State#state.i_am_leader =:= true ->
    Receivers = case lists:keyfind(Pid, 2, R0) of
        {MRef, Pid, _} ->
            erlang:demonitor(MRef),
            lists:keydelete(MRef, 1, R0);
        false ->
            R0
    end,
    {reply, ok, State#state{receivers = Receivers}};
handle_call({rm_receiver_pid, _Pid}, _From, State) ->
    {reply, {error, not_leader}, State};
handle_call(receiver_pids, _From, State) ->
    {reply, [Pid || {_Mref, Pid, send} <- State#state.receivers], State};

handle_call(leader_node, _From, State) ->
    {reply, State#state.leader_node, State};

handle_call(is_leader, _From, State) ->
    {reply, State#state.i_am_leader, State};

handle_call({set_leader_node, LeaderNode, LeaderPid}, _From, State) ->
    case node() of
        LeaderNode ->
            {reply, ok, become_leader(LeaderNode, State)};
        _ ->
            {reply, ok, new_leader(LeaderNode, LeaderPid, State)}
    end;

handle_call(helper_pid, _From, State) ->
    {reply, State#state.helper_pid, State}.

handle_cast({set_candidates, CandidatesIn, WorkersIn}, State) ->
    Candidates = lists:sort(CandidatesIn),
    Workers = lists:sort(WorkersIn),
    case {State#state.candidates, State#state.workers} of
        {Candidates, Workers} -> % no change to candidate list, leave helper alone
            {noreply, State};
        {_OldCandidates, _OldWorkers} ->
            UpdState1 = remonitor_leader(undefined, State),
            UpdState2 = UpdState1#state{candidates=Candidates,
                                        workers=Workers,
                                        leader_node=undefined},
            leader_change(State#state.i_am_leader, false),
            {noreply, restart_helper(UpdState2)}
    end;
handle_cast({repl, Msg}, State) when State#state.i_am_leader =:= true ->
    %% To simulate a slow elected repl leader, uncomment and/or change amount
    %% timer:sleep(1),
    case State#state.receivers of
        [] ->
            riak_repl_util:dropped_realtime_hook(Msg),
            riak_repl_stats:objects_dropped_no_clients(),
            {noreply, State};
        Receivers ->
            case timer:now_diff(os:timestamp(), State#state.lastpoll) of
                X when X > 1000000 ->
                    R2 = lists:map(fun({Mref, Pid, _}) ->
                                    S = case erlang:process_info(Pid,message_queue_len) of
                                        {message_queue_len, L} when L < 20000 ->
                                            send;
                                        _ ->
                                            drop
                                    end,
                                    {Mref, Pid, S}
                            end, Receivers),
                    _ = [P ! {repl, Msg} || {_Mref, P, send} <- R2],
                    riak_repl_stats:objects_sent(),
                    {noreply, State#state{receivers=R2,
                                          lastpoll=os:timestamp()}};
                _ ->
                    _ = [P ! {repl, Msg} || {_Mref, P, send} <- Receivers],
                    riak_repl_stats:objects_sent(),
                    {noreply, State}
            end
    end;
handle_cast({repl, Msg}, State) when State#state.leader_node =/= undefined ->
    MboxSize = State#state.elected_mbox_size,
    {MaybeDropSize, DefiniteDropSize} = get_drop_sizes(),
    SendProb = if MboxSize < MaybeDropSize ->
                       1.0;
                  MboxSize < DefiniteDropSize ->
                       (DefiniteDropSize - MboxSize) /
                            (DefiniteDropSize - MaybeDropSize);
                  true ->
                       0.0
               end,
    Rand = rand:uniform(),
    case SendProb == 1.0 orelse Rand < SendProb of
        true ->
            %% S = definitely send, s = send in middle probability range
            %% if SendProb == 1.0 -> io:format("S"); true -> io:format("s") end,
            gen_server:cast({?SERVER, State#state.leader_node}, {repl, Msg}),
            riak_repl_stats:objects_forwarded();
        false ->
            %% D = definitely drop
            %% io:format("D"),
            %% TODO: create a new stat rather than abusing this counter.
            riak_repl_util:dropped_realtime_hook(Msg),
            riak_repl_stats:objects_dropped_no_clients()
    end,
    {noreply, State};
handle_cast({repl, Msg}, State) ->
    %% No leader currently defined - cannot do anything
    riak_repl_util:dropped_realtime_hook(Msg),
    riak_repl_stats:objects_dropped_no_leader(),
    {noreply, State};
handle_cast(ensure_sites, State) ->
    %% use the leader refresh to trigger a set_leader which will call
    %% ensure_sites
    riak_repl_leader_helper:refresh_leader(State#state.helper_pid),
    {noreply, State}.

handle_info(update_leader, State) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    riak_repl_ring_handler:update_leader(Ring),
    {noreply, State};
handle_info({'DOWN', Mref, process, _Object, _Info}, % dead riak_repl_leader
            #state{leader_mref=Mref}=State) ->
    case State#state.helper_pid of
        undefined ->
            ok;
        Pid ->
            riak_repl_leader_helper:refresh_leader(Pid)
    end,
    {noreply, State#state{leader_node = undefined, leader_mref = undefined}};
handle_info({elected_mailbox_size, MboxSize}, State) ->
    %% io:format("\n~p\n", [MboxSize]),
    {noreply, State#state{elected_mbox_size = MboxSize}};
handle_info(check_mailbox, State) when State#state.i_am_leader =:= false,
                                       State#state.leader_node =/= undefined ->
    Parent = self(),
    spawn(fun() ->
                  try
                      %% The default timeouts are fine, failure is fine.
                      LeaderPid = rpc:call(State#state.leader_node,
                                           erlang, whereis, [?SERVER]),
                      {_, MboxSize} = rpc:call(State#state.leader_node,
                                               erlang, process_info,
                                               [LeaderPid, message_queue_len]),
                      Parent ! {elected_mailbox_size, MboxSize},
                      exit(normal)
                  catch
                      _:_ ->
                          exit(normal)
                  after
                      exit(normal)
                  end,

                  exit(normal)
          end),
    {noreply, State};
handle_info(check_mailbox, State) ->
    %% We're the leader, or we don't know who the leader is, so ignore.
    {noreply, State};
handle_info({'DOWN', Mref, process, _Object, _Info}, State) ->
    %% May be called here with a stale leader_mref, will not matter
    %% as it will not be in the State#state.receivers so will do nothing.
    UpdReceivers = lists:keydelete(Mref, 1, State#state.receivers),
    {noreply, State#state{receivers = UpdReceivers}};
handle_info({'EXIT', Pid, killed}, State=#state{helper_pid={killed,Pid}}) ->
    {noreply, maybe_start_helper(State)};
handle_info({'EXIT', Pid, Reason}, State=#state{helper_pid=Pid}) ->
    lager:warning(
      "Replication leader helper exited unexpectedly: ~p",
      [Reason]),
    {noreply, maybe_start_helper(State)}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

become_leader(Leader, State) ->
    case State#state.leader_node of
        Leader ->
            NewState = State,
            %% we can get here if a non-leader node goes down
            %% so we want to make sure any missing clients are started
            ensure_sites(Leader),
            lager:info("Re-elected as replication leader");
        _ ->
            riak_repl_stats:elections_elected(),
            riak_repl_stats:elections_leader_changed(),
            leader_change(State#state.i_am_leader, true),
            NewState1 = State#state{i_am_leader = true, leader_node = Leader},
            NewState = remonitor_leader(undefined, NewState1),
            ensure_sites(Leader),
            lager:info("Elected as replication leader")
    end,
    NewState.

new_leader(Leader, LeaderPid, State0) ->
    This = node(),
    State = State0#state{i_am_leader = false, leader_node = Leader},
    NewState = case State0#state.leader_node of
        This ->
            %% this node is surrendering leadership
            leader_change(State0#state.i_am_leader, false), % will close connections
            riak_repl_stats:elections_leader_changed(),
            lager:info("Replication leadership surrendered to ~p", [Leader]),
            %% reset the mailbox size to 0 until we poll it again
            State#state{elected_mbox_size = 0};
        Leader ->
            lager:info("Replication leader kept as ~p", [Leader]),
            State;
        _NewLeader ->
            riak_repl_stats:elections_leader_changed(),
            lager:info("Replication leader set to ~p", [Leader]),
            %% reset the mailbox size to 0 until we poll it again
            State#state{elected_mbox_size = 0}
    end,
    %% Set up a monitor on the new leader so we can make the helper
    %% check the elected node if it ever goes away.  This handles
    %% the case where all candidate nodes are down and only workers
    %% remain.
    remonitor_leader(LeaderPid, NewState).

remonitor_leader(LeaderPid, State) ->
    case State#state.leader_mref of
        undefined ->
            ok;
        OldMref ->
            erlang:demonitor(OldMref)
    end,
    case LeaderPid of
        undefined ->
            State#state{leader_mref = undefined};
        _ ->
            State#state{leader_mref = erlang:monitor(process, LeaderPid)}
    end.

%% Restart the helper
restart_helper(State) ->
    case State#state.helper_pid of
        undefined -> % no helper running, start one if needed
            maybe_start_helper(State);
        {killed, _OldPid} ->
            %% already been killed - waiting for exit
            State;
        OldPid ->
            %% Tell helper to stop - cannot use gen_server:call
            %% as may be blocked in an election.  The exit will
            %% be caught in handle_info and the helper will be restarted.
            exit(OldPid, kill),
            State#state{helper_pid = {killed, OldPid}}
    end.

maybe_start_helper(State) ->
    %% Start the helper if there are any candidates
    case State#state.candidates of
        [] ->
            Pid = undefined;
        Candidates ->
            {ok, Pid} = riak_repl_leader_helper:start_link(?MODULE, self(), Candidates,
                                                           State#state.workers)
    end,
    State#state{helper_pid = Pid}.

leader_change(A, A) ->
    %% nothing changed
    ok;
leader_change(false, true) ->
    %% we've become the leader, stop any local clients
    RunningSiteProcs = riak_repl_client_sup:running_site_procs(),
    _ = [riak_repl_client_sup:stop_site(SiteName) ||
        {SiteName, _Pid} <- RunningSiteProcs],
    ok;
leader_change(true, false) ->
    %% we've lost the leadership, close any local listeners
    case app_helper:get_env(riak_repl, inverse_connection) of
        true ->
            %% in the inverted case need to stop sites
            RunningSiteProcs = riak_repl_client_sup:running_site_procs(),
            _ = [riak_repl_client_sup:stop_site(SiteName) ||
                {SiteName, _Pid} <- RunningSiteProcs],
            ok;
        _ ->
            ok
    end,
    _ = riak_repl_listener_sup:close_all_connections(),
    ok.

%% Inspect the cluster and determine if we can balance clients between
%% non-leader nodes
ensure_sites(Leader) ->
    AliveNodes0 = riak_core_node_watcher:nodes(riak_kv) -- [Leader],
    InverseConnections = app_helper:get_env(riak_repl, inverse_connection),
    case AliveNodes0 of
        [] ->
            %% only node there is
            {ok, Ring} = riak_core_ring_manager:get_my_ring(),
            riak_repl_client_sup:ensure_sites(Ring);
        _ when InverseConnections == true ->
            %% inverse connections are incompatible with client balancing
            lager:info("Inverse connections enabled; clients will all be "
                "run on the leader"),
            {ok, Ring} = riak_core_ring_manager:get_my_ring(),
            riak_repl_client_sup:ensure_sites(Ring);
        _ ->
            {Results, DeadNodes} = rpc:multicall(AliveNodes0, riak_repl_client_sup,
                running_site_procs_rpc, []),
            case DeadNodes of
                [] ->
                    ok;
                _ ->
                    lager:warning("Some nodes failed to respond to replication"
                        "client querying ~p", [DeadNodes])
            end,

            {ok, Ring} = riak_core_ring_manager:get_my_ring(),
            ReplConfig =
            case riak_repl_ring:get_repl_config(Ring) of
                undefined ->
                    riak_repl_ring:initial_config();
                RC -> RC
            end,

            {AliveNodes, CurrentConfig} =
            lists:foldl(fun({badrpc, {'EXIT', _}}, Acc) ->
                        Acc;
                    ({_Node, {'EXIT', _}}, Acc) ->
                        Acc;
                    ({Node, Sites}, {N, C}) ->
                        {[Node|N], [{Node, Sites}|C]}
                end, {[], []}, Results),

            case AliveNodes of
                [] ->
                    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
                    riak_repl_client_sup:ensure_sites(Ring);
                _ ->
                    %% stop any local clients on the leader
                    RunningSiteProcs = riak_repl_client_sup:running_site_procs(),
                    _ = [riak_repl_client_sup:stop_site(SiteName) ||
                        {SiteName, _Pid} <- RunningSiteProcs],
                    ConfiguredSites = [Site#repl_site.name ||
                        Site <- dict:fetch(sites, ReplConfig)],
                    {ToStop, ToStart} = balance_clients(CurrentConfig,
                        ConfiguredSites),
                    _ = [riak_core_util:safe_rpc(Node, riak_repl_client_sup, stop_site, [Site])
                        || {Node, Site} <- ToStop],
                    _ = [riak_core_util:safe_rpc(Node, riak_repl_client_sup, start_site, [Site])
                        || {Node, Site} <- ToStart],
                    ok
            end
    end.

-type balance_acc() :: {repl_ns_pairs(),
                        [{node(), repl_sitenames()}],
                        overflow(),
                        clients_per_node()}.
-spec balance(repl_sitename(), balance_acc()) -> balance_acc().

balance(Site, {TS, [{Node, Sites}|Nodes], O, ClientsPerNode}) ->
    case length(Sites) of
        ClientsPerNode when O > 0 ->
            {[{Node, Site}|TS], Nodes, O - 1, ClientsPerNode};
        Count when Count < ClientsPerNode ->
            case Count + 1 of
                ClientsPerNode when O =< 0 ->
                    {[{Node, Site}|TS], Nodes, O, ClientsPerNode};
                _ ->
                    {[{Node, Site}|TS], [{Node, [Site|Sites]}|Nodes], O,
                        ClientsPerNode}
            end;
        _ ->
            %% this node is already full, try again with the next one
            balance(Site, {TS, Nodes, O, ClientsPerNode})
    end.


-spec clients_per_node(non_neg_integer(), non_neg_integer()) ->
                              {clients_per_node(), overflow()}.
clients_per_node(0, _) ->
    {0, 0};
clients_per_node(CSLen, CCLen) ->
    if CSLen > CCLen -> {CSLen div CCLen, CSLen rem CCLen};
        true -> {0, CSLen}
    end.

%% @doc Given the desired number of clients per node `ClientsPerNode'
%%      and the desired site configuration `ConfiguredSites' return a
%%      fun that will fold over all current active sites `{Node,
%%      Sites}' and determine which ones need to be stopped `Stop',
%%      which ones need to remain up `Remaining', which ones are
%%      already up `Up', and how much overflow is remaining `Over'.
-type sts_acc() :: {Stop::repl_ns_pairs(),
                    Remaining::[{node(), repl_sitenames()}],
                    Up::repl_sitenames(),
                    Over::non_neg_integer()}.
-type sts_fold() :: fun(({node(), repl_np_pairs()}, sts_acc()) -> sts_acc()).

-spec sites_to_stop(non_neg_integer(), repl_sitenames()) -> sts_fold().
sites_to_stop(ClientsPerNode, ConfiguredSites) ->
    fun({Node, Sites}, {Stop0, Remain0, Up0, Over}) ->
            %% figure out what needs to be stopped to satisfy the
            %% configuration
            SC = satisfy_config(Node, ConfiguredSites),
            {Stop, Remaining, Up} = lists:foldl(SC, {[], [], Up0}, Sites),

            %% Length of remaining sites for the node `Node'
            RemainingLen = length(Remaining),
            if RemainingLen =< ClientsPerNode ->
                    %% not overflowing
                    {Stop ++ Stop0, [{Node, Remaining}|Remain0], Up, Over};
               RemainingLen == (ClientsPerNode + 1) andalso Over > 0 ->
                    %% over by 1 and overflow is permitted
                    {Stop ++ Stop0, [{Node, Remaining}|Remain0], Up, Over-1};
               true ->
                    %% too many clients on this node
                    %% ToPrune::Site, NewOver::integer()
                    %% Take sites to remove from the front of `Remaining'
                    {ToPrune, NewOver} =
                        if Over > 0 ->
                                {lists:sublist(Remaining,
                                               length(Remaining) - ClientsPerNode - 1),
                                 Over - 1};
                           true ->
                                {lists:sublist(Remaining,
                                               length(Remaining) - ClientsPerNode),
                                 0}
                        end,
                    PrunedSites = [{Node, Site} || Site <- ToPrune],
                    {Stop ++ Stop0 ++ PrunedSites,
                     [{Node, (Remaining -- ToPrune)} | Remain0],
                     Up -- ToPrune, NewOver}
            end
    end.

%% @doc Given the desired configuration `ConfiguredSites' return a
%%      function to fold over the sites that are currently up.  For
%%      each active site `Site' decide if it should be stopped
%%      (`Stop') or remain up (`Remain').  The `Up' variable keeps
%%      track of `ConfiguredSites' that are already running on one of
%%      the nodes.  After a `Site' has been found to be up any
%%      subsequent instances will be added to `Stop'.
-type sc_acc() :: {Stop::repl_ns_pairs(), Remain::repl_sitenames(), Up::repl_sitenames()}.
-type sc_fold() :: fun((repl_np_pair(), sc_acc()) -> sc_acc()).

-spec satisfy_config(node(), repl_sitenames()) -> sc_fold().
satisfy_config(Node, ConfiguredSites) ->
    fun({Site, _Pid}, {Stop, Remain, Up}) ->
            case lists:member(Site, ConfiguredSites) of
                true ->
                    case lists:member(Site, Up) of
                        true ->
                            %% site is a duplicate
                            {[{Node, Site}|Stop], Remain, Up};
                        false ->
                            %% site is ok
                            {Stop, [Site|Remain], [Site|Up]}
                    end;
                false ->
                    %% site isn't configured anymore
                    {[{Node, Site}|Stop], Remain, Up}
            end
    end.

%% Effectively, this function maps M configured replication clients to N
%% nodes, where N > 1 and M >= 0. As nodes are added or removed from the
%% cluster clients will be shuffled around to try to balance the load on any
%% individual node.
-spec balance_clients([repl_node_sites()], repl_sitenames()) ->
                             {ToStop::repl_ns_pairs(),
                              ToStart::repl_ns_pairs()}.
balance_clients(CurrentConfig, ConfiguredSites) ->
    %% currentconfig is a list of {node, [site]} tuples and configuredsites is
    %% merely a list of sites that should be running

    %% how many clients should be running per node, and how many need to be
    %% running one extra.
    {ClientsPerNode, OverFlow} = clients_per_node(length(ConfiguredSites),
                                                  length(CurrentConfig)),

    %% figure out what sites need to be stopped because they're no longer
    %% configured or are duplicates
    SitesToStop = sites_to_stop(ClientsPerNode, ConfiguredSites),

    %% Note: `CurrentConfig1' isn't quite the same type as `CurrentConfig'
    {ToStop, CurrentConfig1, Up, RemOver} =
        lists:foldl(SitesToStop, {[], [], [], OverFlow}, CurrentConfig),

    NotStarted = lists:filter(is_down(Up), ConfiguredSites),

    {ToStart, _, _, _} = lists:foldl(fun ?MODULE:balance/2,
        {[], CurrentConfig1, RemOver, ClientsPerNode},
        NotStarted),
    {ToStop, ToStart}.

%% @private
%%
%% @doc Given the list of up sites `Up' return a predicate telling
%%      whether a site `Site' is down.
-spec is_down(repl_sitenames()) ->
                     fun((repl_sitename()) -> IsDown::boolean()).
is_down(Up) ->
    fun(Site) ->
            not lists:member(Site, Up)
    end.

get_drop_sizes() ->
    %% TODO: configurable?
    {10*1000, 20*1000}.
    %% {1, 200}.                                   % 2-node testing @ slow rates

-ifdef(TEST).

balance_clients_test() ->
    ?assertEqual({[], [{node1, site1}]}, balance_clients([{node1, []}], [site1])),
    ?assertEqual({[{node1, site3}, {node1, site2}], []}, balance_clients([{node1, [{site1, self()}, {site2,
                            self()}, {site3, self()}]}], [site1])),
    ?assertEqual({[{node2, site1}, {node1, site3}, {node1, site2}], []}, balance_clients([{node1, [{site1, self()}, {site2,
                            self()}, {site3, self()}]}, {node2, [{site1,
                            self()}]}], [site1])),
    ?assertEqual({[{node1, site3}], [{node2, site3}]}, balance_clients([{node1, [{site1, self()}, {site2,
                            self()}, {site3, self()}]}, {node2, []}], [site1, site2, site3])),
    ?assertEqual({[{node1, site3}], [{node2, site3}]}, balance_clients([{node2, []}, {node1, [{site1, self()}, {site2,
                            self()}, {site3, self()}]}], [site1, site2, site3])),
    ok.

-ifdef(EQC).

node_gen() ->
    elements([node1, node2, node3, node4, node5, node6]).

site() ->
    elements([site1, site2, site3, site4, site5, site6, site7, site8, site9,
            site10]).

site_pid() ->
    {site(), self()}.

site_config() ->
    {node_gen(), ?LET(Sites, list(site_pid()), lists:usort(Sites))}.

configured_sites() ->
    ?LET(CC, list(site()), lists:usort(CC)).

unique_config(Config) ->
    {_, Result} = lists:foldl(fun({Node, Sites}, {Seen, Output}) ->
                case lists:member(Node, Seen) of
                    true ->
                        {Seen, Output};
                    _ ->
                        {[Node|Seen], [{Node, Sites}|Output]}
                end
        end, {[], []}, Config),
    Result.

current_config() ->
    ?LET(Config, ?SUCHTHAT(C, list(site_config()), length(C) > 0), unique_config(Config)).

prop_balance() ->
    ?FORALL({CurrentConfig, ConfiguredSites}, {current_config(),
            configured_sites()},
        begin
                {ToStop, ToStart} = balance_clients(CurrentConfig, ConfiguredSites),
                FinalConfig = lists:foldl(fun({Node, SiteToStart}, Result) ->
                            {Node, Sites} = lists:keyfind(Node, 1, Result),
                            lists:keyreplace(Node, 1, Result,
                                {Node, [{SiteToStart, self()}|Sites]})
                    end, lists:foldl(fun({Node, SiteToStop}, Result) ->
                                {Node, Sites} = lists:keyfind(Node, 1, Result),
                                lists:keyreplace(Node, 1, Result,
                                    {Node, lists:keydelete(SiteToStop, 1, Sites)})
                        end, CurrentConfig, ToStop), ToStart),

            {ClientsPerNode, OverFlow} =
                clients_per_node(length(ConfiguredSites),
                                 length(CurrentConfig)),

                ?WHENFAIL(
                    ?debugFmt("CurrentConfig ~p, ConfiguredSites ~p,"
                        "ToStart ~p ToStop ~p, ClientsPerNode ~p, Overflow ~p,"
                        "FinalConfig ~p~n",
                        [CurrentConfig, ConfiguredSites, ToStart, ToStop,
                            ClientsPerNode, OverFlow, FinalConfig]),

                    %% check that we are only starting sites that are supposed
                    %% to start
                    lists:all(fun({_Node, Site}) ->
                                lists:member(Site, ConfiguredSites)
                        end, ToStart) andalso
                    %% check that we've balanced the # of sites across the
                    %% nodes
                    case lists:foldl(fun({_Node, Sites}, {Valid, Over}) ->
                                    ClientsPlusOne = ClientsPerNode + 1,
                                    case length(Sites) of
                                        ClientsPerNode ->
                                            {true andalso Valid, Over};
                                        ClientsPlusOne when Over > 0 ->
                                            {true andalso Valid, Over - 1};
                                        _ ->
                                            {false, Over}
                                    end
                            end, {true, OverFlow}, FinalConfig) of
                        {true, 0} ->
                            %% all the nodes have the right number of clients,
                            %% and the overflow is completely consumed
                            true;
                        _ ->
                            false
                    end andalso
                    %% TODO make sure we don't churn unnecessarily
                    true
                )
        end).

%% eunit wrapper
eqc_test_() ->
    {spawn,
     [{timeout, 60, ?_assert(eqc:quickcheck(eqc:testing_time(4, prop_balance())))}]}.

-endif.

-endif.
