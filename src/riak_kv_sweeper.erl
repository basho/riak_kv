%% -------------------------------------------------------------------
%%
%% riak_kv_sweeper: Riak sweep scheduler
%%
%% Copyright (c) 2007-2015 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc
%% This module implements a gen_server process that manages and schedules sweeps.
%% Anyone can register a #sweep_participant{} with information about how
%% often it should run and what kind of fun it include
%%  (?DELETE_FUN, ?MODIFY_FUN or ?OBSERV_FUN)
%%
%% riak_kv_sweeper keep one sweep per index.
%% Once every tick  riak_kv_sweeper check if it's in the configured sweep_window
%% and find sweeps to run. It does this by comparing what the sweeps have swept
%% before with the requirments in #sweep_participant{}.
-module(riak_kv_sweeper).
-behaviour(gen_server).
-include("riak_kv_sweeper.hrl").

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% Default number of concurrent sweeps that are allowed to run.
-define(DEFAULT_SWEEP_TICK, timer:minutes(1)).
-define(ESTIMATE_EXPIRY, 24 * 3600). %% 1 day in s

%% ====================================================================
%% API functions
%% ====================================================================
-export([start_link/0,
         stop/0,
         add_sweep_participant/1,
         remove_sweep_participant/1,
         status/0,
         sweep/1,
         sweep_result/2,
         update_progress/2,
         update_started_sweep/3,
         stop_all_sweeps/0,
         disable_sweep_scheduling/0,
         enable_sweep_scheduling/0,
         get_run_interval/1,
         in_sweep_window/0]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


stop() ->
    gen_server:call(?MODULE, stop).


%% @doc Add callback module that will be asked to participate in sweeps.
-spec add_sweep_participant(#sweep_participant{}) -> ok.
add_sweep_participant(Participant) ->
    gen_server:call(?MODULE, {add_sweep_participant, Participant}).


%% @doc Remove participant callback module.
-spec remove_sweep_participant(atom()) -> true | false.
remove_sweep_participant(Module) ->
    gen_server:call(?MODULE, {remove_sweep_participant, Module}).


%% @doc Initiat a sweep without using scheduling. Can be used as fold replacment.
-spec sweep(non_neg_integer()) -> ok.
sweep(Index) ->
    gen_server:call(?MODULE, {sweep_request, Index}, infinity).


%% @doc Get information about participants and all sweeps.
-spec status() -> {[#sweep_participant{}], [#sweep{}]}.
status() ->
    gen_server:call(?MODULE, status).


%% @doc Stop all running sweeps
-spec stop_all_sweeps() ->  ok.
stop_all_sweeps() ->
    gen_server:call(?MODULE, stop_all_sweeps).


%% Stop scheduled sweeps and disable the scheduler from starting new sweeps
%% Only allow manual sweeps throu sweep/1.
-spec disable_sweep_scheduling() -> ok.
disable_sweep_scheduling() ->
    lager:info("Disable sweep scheduling"),
    stop_all_sweeps().


-spec enable_sweep_scheduling() ->  ok.
enable_sweep_scheduling() ->
    lager:info("Enable sweep scheduling"),
    application:set_env(riak_kv, sweeper_scheduler, true).


update_started_sweep(Index, ActiveParticipants, Estimate) ->
    gen_server:cast(?MODULE, {update_started_sweep, Index, ActiveParticipants, Estimate}).


%% @private used by the sweeping process to report results when done.
sweep_result(Index, Result) ->
    gen_server:cast(?MODULE, {sweep_result, Index, Result}).


% @private used by the sweeping process to report progress.
update_progress(Index, SweptKeys) ->
    gen_server:cast(?MODULE, {update_progress, Index, SweptKeys}).


in_sweep_window() ->
    riak_kv_sweeper_state:in_sweep_window().


get_run_interval(RunIntervalFun) when is_function(RunIntervalFun) ->
    riak_kv_sweeper_state:get_run_interval(RunIntervalFun).

%% ====================================================================
%% Behavioural functions
%% ====================================================================
init([]) ->
    process_flag(trap_exit, true),
    random:seed(erlang:now()),
    Ref = schedule_initial_sweep_tick(),
    State = riak_kv_sweeper_state:new(),
    State1 = State:update_timer_ref(Ref),
    {ok, State1}.


handle_call({add_sweep_participant, Participant}, _From, State) ->
    State1 = riak_kv_sweeper_state:add_sweep_participant(Participant, State),
    {reply, ok, State1};

handle_call({remove_sweep_participant, Module}, _From, State) ->
    {ok, Removed, State1} = riak_kv_sweeper_state:remove_sweep_participant(Module, State),
    {reply, Removed, State1};

handle_call({sweep_request, Index}, _From, State) ->
    State1 = riak_kv_sweeper_state:maybe_initiate_sweeps(State),
    State3 =
        case riak_kv_sweeper_state:sweep_request(Index, State1) of
            {ok, Index, State2} ->
                do_sweep(Index, State2);
            State2 ->
                State2
        end,
    {reply, ok, State3};

handle_call(status, _From, State) ->
    {ok, {Participants, Sweeps}, State1} = riak_kv_sweeper_state:status(State),
    {reply, {Participants , Sweeps}, State1};

handle_call(stop_all_sweeps, _From, State) ->
    {ok, Running, State1} = riak_kv_sweeper_state:stop_all_sweeps(State),
    {reply, Running, State1};

handle_call(stop, _From, State) ->
    {ok, _Running, State1} = riak_kv_sweeper_state:stop_all_sweeps(State),
    {stop, normal, ok, State1}.


handle_cast({update_started_sweep, Index, ActiveParticipants, Estimate}, State) ->
    State1 = riak_kv_sweeper_state:update_started_sweep(Index, ActiveParticipants, Estimate, State),
    {noreply, State1};

handle_cast({sweep_result, Index, Result}, State) ->
    State1 = riak_kv_sweeper_state:update_finished_sweep(Index, Result, State),
    {noreply, State1};

handle_cast({update_progress, Index, SweptKeys}, State) ->
    State1 = riak_kv_sweeper_state:update_progress(Index, SweptKeys, State),
    {noreply, State1};

handle_cast(_Msg, State) ->
    {noreply, State}.


handle_info({test_tick, Ref, From}, State) ->
    {noreply, State1} = handle_info(sweep_tick, State),
    From ! {ok, Ref},
    {noreply, State1};

handle_info(sweep_tick, State) ->
    Ref = schedule_sweep_tick(),
    State3 =
        case lists:member(riak_kv, riak_core_node_watcher:services(node())) of
            true ->
                State1 = riak_kv_sweeper_state:maybe_initiate_sweeps(State),
                case riak_kv_sweeper_state:maybe_schedule_sweep(State1) of
                    {ok, Index, State2} ->
                        do_sweep(Index, State2);
                    State2 ->
                        State2
                end;
            false ->
                State
        end,
    State4 = riak_kv_sweeper_state:update_timer_ref(Ref, State3),
    {noreply, State4};

handle_info(Msg, State) ->
    lager:error("riak_kv_sweeper received unexpected message ~p", [Msg]),
    {noreply, State}.


terminate(_, State) ->
    riak_kv_sweeper_state:persist_sweeps(State),
    ok.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


do_sweep(Index, State) ->
    %% Ask for estimate before we ask_participants since riak_kv_index_tree
    %% clears the trees if they are expired.
    AAEEnabled = riak_kv_entropy_manager:enabled(),
    Estimate = get_estimate_keys(Index, AAEEnabled, State),

    State2 =
        case riak_kv_sweeper_state:ask_participants(Index, State) of
            {ok, [], State1} ->
                State1;
            {ok, ActiveParticipants, State1} ->
                ?MODULE:update_started_sweep(Index, ActiveParticipants, Estimate),
                Workerpid = riak_kv_vnode:sweep({Index, node()},
                                                ActiveParticipants,
                                                Estimate),
                riak_kv_sweeper_state:start_sweep(Index, Workerpid, State1)
        end,
    State2.


get_estimate_keys(Index, AAEEnabled, State) ->
    {ok, OldEstimate, _NewState} = riak_kv_sweeper_state:get_estimate_keys(Index, State),
    maybe_estimate_keys(Index, AAEEnabled, OldEstimate).


%% We keep the estimate from previus sweep unless it's older then ?ESTIMATE_EXPIRY.
maybe_estimate_keys(Index, true, undefined) ->
    get_estimtate(Index);
maybe_estimate_keys(Index, true, {EstimatedNrKeys, TS}) ->
	EstimateOutdated = elapsed_secs(os:timestamp(), TS) > ?ESTIMATE_EXPIRY,
	case EstimateOutdated of
            true ->
                get_estimtate(Index);
            false ->
                EstimatedNrKeys
	end;
maybe_estimate_keys(_Index, false, {EstimatedNrKeys, _TS}) ->
    EstimatedNrKeys;
maybe_estimate_keys(_Index, false, _) ->
    false.


get_estimtate(Index) ->
    Pid = self(),
    %% riak_kv_index_hashtree release lock when the process die
    proc_lib:spawn(fun() ->
                Estimate =
                    case riak_kv_index_hashtree:get_lock(Index, estimate) of
                        ok ->
                            case riak_kv_index_hashtree:estimate_keys(Index) of
                                {ok, EstimatedNrKeys} ->
                                    EstimatedNrKeys;
                                Other ->
                                    lager:info("Failed to get estimate for ~p, got result"
                                               " ~p. Defaulting to 0...", [Index, Other]),
                                    0
                            end;
                        _ ->
                            lager:info("Failed to get lock for index ~p for estimate, "
                                       "defaulting to 0...", [Index]),
                            0
                    end,
                Pid ! {estimate, Estimate}
        end),
    wait_for_estimate().


wait_for_estimate() ->
    receive
        {estimate, Estimate} ->
            Estimate
    after 5000 ->
        0
    end.


schedule_initial_sweep_tick() ->
    InitialTick = trunc(get_tick() * random:uniform()),
    erlang:send_after(InitialTick, ?MODULE, sweep_tick).


schedule_sweep_tick() ->
    erlang:send_after(get_tick(), ?MODULE, sweep_tick).


get_tick() ->
    app_helper:get_env(riak_kv, sweep_tick, ?DEFAULT_SWEEP_TICK).


elapsed_secs(Now, Start) ->
    timer:now_diff(Now, Start) div 1000000.
