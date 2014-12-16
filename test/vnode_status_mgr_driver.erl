-module(vnode_status_mgr_driver).
%% -------------------------------------------------------------------
%%
%% vnode_status_mgr_driver: A module that emulates the interaction
%% between `riak_kv_vnode' and `riak_kv_vnode_status_mgr' in order to
%% test the interaction of the two module without the myriad of
%% dependencies required to directly use the `riak_kv_vnode' module.
%%
%% Copyright (c) 2007-2014 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

-behaviour(gen_server).

-ifdef(TEST).
-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-endif.
-include_lib("eunit/include/eunit.hrl").
-endif.

-include_lib("eunit/include/eunit.hrl").

%% API
-export([start_link/1,
         status/1,
         increment_counter/2,
         clear_counter/1,
         stop/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-include("vnode_status_mgr_test.hrl").

-type init_args() :: {pid(), non_neg_integer()}.
-type vnodeid() :: binary().
-type counter_lease_error() :: {error, counter_lease_max_errors | counter_lease_timeout}.

-define(ELSE, true).

%%%===================================================================
%%% API
%%%===================================================================

-spec start_link(pos_integer()) -> {ok, pid()} | {error, term()}.
start_link(CounterLeaseSize) ->
    gen_server:start_link(?MODULE, [CounterLeaseSize], []).

status(Pid) ->
    gen_server:call(Pid, status).

increment_counter(Pid, Increments) ->
    gen_server:call(Pid, {increment_counter, Increments}).

clear_counter(Pid) ->
    gen_server:call(Pid, clear_counter).

stop(Pid) ->
    gen_server:call(Pid, stop).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

-spec init(Args :: init_args()) -> {ok, #state{}}.
init([CounterLeaseSize]) ->
    process_flag(trap_exit, true),
    {ok, StatusMgr} = riak_kv_vnode_status_mgr:start_link(self(), 0),
    {ok, {VId, CounterState}} = get_vnodeid_and_counter(StatusMgr,
                                                        CounterLeaseSize),

    {ok, #state{vnodeid=VId,
                status_mgr_pid=StatusMgr,
                counter=CounterState}}.

-spec handle_call(term(), {pid(), term()}, #state{}) ->
                         {reply, {ok, {VnodeId :: binary(),
                                       Counter :: non_neg_integer(),
                                       LeaseTo :: non_neg_integer()}},
                          #state{}}.
handle_call(status, _From, State) ->
    {reply, State, State};
handle_call({increment_counter, Increments}, _From, State) ->
    StatusMgrPid = State#state.status_mgr_pid,
    UpdState = update_counter(Increments, State),
    {ok, StatusMgrStatus} = riak_kv_vnode_status_mgr:status(StatusMgrPid),
    {reply, {State, StatusMgrStatus}, UpdState};
handle_call(clear_counter, _From, #state{status_mgr_pid=StatusMgr}=State) ->
    %% clear vnodeid first, if drop removes data but fails
    %% want to err on the side of creating a new vnodeid
    {ok, cleared} = clear_vnodeid(StatusMgr),
    UpdState = State#state{vnodeid=undefined},
    {ok, MgrStatus} = riak_kv_vnode_status_mgr:status(StatusMgr),
    {reply, {UpdState, MgrStatus}, UpdState};
handle_call(stop, _From, #state{status_mgr_pid=Pid}=State) ->
    riak_kv_vnode_status_mgr:stop(Pid),
    {stop, normal, ok, State}.

-spec handle_cast(term(), #state{}) -> {noreply, #state{}}.
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({counter_lease, {FromPid, VnodeId, NewLease}}, State=#state{status_mgr_pid=FromPid}) ->
    #state{counter=CounterState} = State,
    UpdCounterState = CounterState#counter_state{lease=NewLease, leasing=false},
    UpdState = State#state{counter=UpdCounterState, vnodeid=VnodeId},
    {noreply, UpdState};
handle_info({'EXIT', Pid, Reason}, State=#state{status_mgr_pid=Pid, counter=CntrState}) ->
    lager:error("Vnode status manager exit ~p", [Reason]),
    %% The status manager died, start a new one
    #counter_state{lease_size=LeaseSize, leasing=Leasing} = CntrState,
    {ok, NewPid} = riak_kv_vnode_status_mgr:start_link(self(), 0),

   if Leasing ->
            %% Crashed when getting a lease, try again
            ok = riak_kv_vnode_status_mgr:lease_counter(NewPid, LeaseSize);
       ?ELSE ->
            ok
    end,
    {noreply, State#state{status_mgr_pid=NewPid}};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec get_vnodeid_and_counter(pid(), non_neg_integer()) ->
                                     {ok, {vnodeid(), #counter_state{}}} |
                                     {error, Reason::term()}.
get_vnodeid_and_counter(StatusMgr, CounterLeaseSize) ->
    {ok, {VId, Counter, Lease}} = riak_kv_vnode_status_mgr:get_vnodeid_and_counter(StatusMgr, CounterLeaseSize),
    {ok, {VId, #counter_state{cnt=Counter, lease=Lease, lease_size=CounterLeaseSize}}}.

%% Clear the vnodeid - returns {ok, cleared}
clear_vnodeid(StatusMgr) ->
    riak_kv_vnode_status_mgr:clear_vnodeid(StatusMgr).

%% @private we can never use a counter that is greater or equal to the
%% one fysnced to disk. If the incremented counter is == to the
%% current Lease, we must block until the new Lease is stored. If the
%% current counter is 80% through the current lease, and we have not
%% asked for a new lease, ask for one.  If we're less than 80% through
%% the lease, do nothing.  If not yet blocking(equal) and we already
%% asked for a new lease, do nothing.
maybe_lease_counter(State=#state{counter=#counter_state{cnt=Lease, lease=Lease,
                                                        leasing=true}}) ->
    %% Block until we get a new lease, or crash the vnode
    {ok, NewState} = blocking_lease_counter(State),
    NewState;
maybe_lease_counter(State=#state{counter=#counter_state{leasing=true}}) ->
    %% not yet at the blocking stage, waiting on a lease
    State;
maybe_lease_counter(State) ->
    #state{status_mgr_pid=MgrPid, counter=CS=#counter_state{cnt=Cnt, lease=Lease, lease_size=LeaseSize}} = State,
    %% @TODO (rdb) configurable??
    %% has more than 80% of the lease been used?
    CS2 = if (Lease - Cnt) =< 0.2 * LeaseSize  ->
                  ok = riak_kv_vnode_status_mgr:lease_counter(MgrPid, LeaseSize),
                  CS#counter_state{leasing=true};
             ?ELSE ->
                  CS
          end,
    State#state{counter=CS2}.

%% @private by now, we have to be waiting for a lease, or an exit,
%% from the mgr. Block until we receive a lease. If we get an exit,
%% retry a number of times. If we get neither in a reasonable time,
%% return an error.
-spec blocking_lease_counter(#state{}) ->
                                    {ok, #state{}} |
                                    counter_lease_error().
blocking_lease_counter(State) ->
    blocking_lease_counter(State, {0, 20, 5000}).

-spec blocking_lease_counter(#state{}, {Errors :: non_neg_integer(),
                                        MaxErrors :: non_neg_integer(),
                                        TimeRemainingMillis :: non_neg_integer()}
                            ) ->
                                    {ok, #state{}} |
                                    counter_lease_error().
blocking_lease_counter(_State, {MaxErrs, MaxErrs, _MaxTime}) ->
    {error, counter_lease_max_errors};
blocking_lease_counter(State, {ErrCnt, MaxErrors, MaxTime}) ->
    #state{status_mgr_pid=Pid, counter=CounterState} = State,
    #counter_state{lease_size=LeaseSize} = CounterState,
    Start = os:timestamp(),
    receive
        {'EXIT', Pid, Reason} ->
            lager:error("Failed to lease counter ~p", [Reason]),
            {ok, NewPid} = riak_kv_vnode_status_mgr:start_link(self(), 0),
            ok = riak_kv_vnode_status_mgr:lease_counter(NewPid, LeaseSize),
            NewState = State#state{status_mgr_pid=NewPid},
            Elapsed = timer:now_diff(os:timestamp(), Start),
            blocking_lease_counter(NewState, {ErrCnt+1, MaxErrors, MaxTime - Elapsed});
        {counter_lease, {Pid, VnodeId, NewLease}} ->
            NewCS = CounterState#counter_state{lease=NewLease, leasing=false},
            {ok, State#state{counter=NewCS, vnodeid=VnodeId}}
    after
        MaxTime ->
            {error, counter_lease_timeout}
    end.

%% @private increment the per vnode coordinating put counter,
%% flushing/leasing if needed
-spec update_counter(pos_integer(), state()) -> state().
update_counter(Increments, State=#state{counter=CounterState}) ->
    #counter_state{cnt=Counter0} = CounterState,
    Counter = Counter0 +  Increments,
    maybe_lease_counter(State#state{counter=CounterState#counter_state{cnt=Counter}}).
