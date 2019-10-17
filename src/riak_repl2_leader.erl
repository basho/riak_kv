%% Riak EnterpriseDS
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.

%%===================================================================
%% Replication Mark II Leader
%%
%% riak_repl_leader_helper is used to perform the elections and work
%% around the gen_leader limit of having a fixed list of candidates.
%%===================================================================

-module(riak_repl2_leader).
-behaviour(gen_server).

%% API
-export([start_link/0,
         set_candidates/2,
         leader_node/0,
         is_leader/0,
         register_notify_fun/1]).
-export([set_leader/3]).
-export([helper_pid/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, riak_repl2_leader_gs).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-endif.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(state, {helper_pid,  % pid of riak_repl_leader_helper
                i_am_leader=false :: boolean(), % true if the leader
                leader_node=undefined :: undefined | node(), % current leader
                leader_pid=undefined :: undefined | pid(), % current leader Pid
                leader_mref=undefined :: undefined | reference(), % monitor
                notify_funs=[],                 % registered notification callbacks
                candidates=[] :: [node()],      % candidate nodes for leader
                workers=[node()] :: [node()],   % workers
                check_tref :: timer:tref(),     % check mailbox timer
                elected_mbox_size :: undefined | non_neg_integer()  % elected leader box size
               }).
     
%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% Set the list of candidate nodes for replication leader
set_candidates(Candidates, Workers) ->
    All = Candidates ++ Workers,
    gen_server:cast(?SERVER, {set_candidates, All, []}).

%% register a callback for notification of leader changes
register_notify_fun(Fun) ->
    gen_server:cast(?SERVER, {register_notify_fun, Fun}).

%% Return the current leader node
leader_node() ->
    gen_server:call(?SERVER, leader_node, infinity).

%% Are we the leader?
is_leader() ->
    gen_server:call(?SERVER, is_leader, infinity).

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
    {ok, TRef} = timer:send_interval(1000, check_mailbox),
    {ok, #state{check_tref = TRef}}.

handle_call(leader_node, _From, State) ->
    {reply, State#state.leader_node, State};

handle_call(is_leader, _From, State) ->
    {reply, State#state.i_am_leader, State};

handle_call({set_leader_node, LeaderNode, LeaderPid}, _From, State) ->
    State1 = State#state{leader_pid = LeaderPid},
    Reply = case node() of
                LeaderNode ->
                    {reply, ok, become_leader(LeaderNode, State1)};
                _ ->
                    {reply, ok, new_leader(LeaderNode, LeaderPid, State1)}
            end,
    %% notify all registered parties of the current leader node.
    _ = [NotifyFun(LeaderNode, LeaderPid) || NotifyFun <- State#state.notify_funs],
    Reply;

handle_call(helper_pid, _From, State) ->
    {reply, State#state.helper_pid, State}.

handle_cast({register_notify_fun, Fun}, State) ->
    %% Notify the interested party immediately, in case leader election
    %% has already occured.
    Fun(State#state.leader_node, State#state.leader_pid),
    {noreply, State#state{notify_funs=State#state.notify_funs ++ [Fun]}};

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
            {noreply, restart_helper(UpdState2)}
    end.

handle_info(update_leader, State) ->
    %% {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    %% riak_repl_ring_handler:update_leader(Ring),
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
handle_info({'DOWN', _Mref, process, _Object, _Info}, State) ->
    %% May be called here with a stale leader_mref, will not matter.
    {noreply, State};
handle_info({'EXIT', Pid, killed}, State=#state{helper_pid={killed,Pid}}) ->
    {noreply, maybe_start_helper(State)};
handle_info({'EXIT', Pid, Reason}, State=#state{helper_pid=Pid}) ->
    lager:warning(
      "Replication leader Mark II helper exited unexpectedly: ~p",
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
            %% already the leader
            NewState = State,
            %% we can get here if a non-leader node goes down
            %% so we want to make sure any missing clients are started
            lager:debug("LeaderII: ~p re-elected as replication leader", [Leader]);
        _ ->
            %% newly the leader
            NewState1 = State#state{i_am_leader = true, leader_node = Leader},
            NewState = remonitor_leader(undefined, NewState1),
            lager:info("Leader2: ~p elected as replication leader", [Leader])
    end,
    NewState.

new_leader(Leader, LeaderPid, State) ->
    %% Set up a monitor on the new leader so we can make the helper
    %% check the elected node if it ever goes away.  This handles
    %% the case where all candidate nodes are down and only workers
    %% remain.
    NewState = State#state{i_am_leader = false, leader_node = Leader},
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
