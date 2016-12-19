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
%% This module implements a gen_server process that manages and
%% schedules sweeps.  Anyone can register a #sweep_participant{} with
%% information about how often it should run and what kind of fun it
%% include
%%  (delete_fun, modify_fun or observ_fun) riak_kv_sweeper keep one
%% sweep per index.
%%
%% Once every tick riak_kv_sweeper check if it's in the configured
%% sweep_window and find sweeps to run. It does this by comparing what
%% the sweeps have swept before with the requirments in
%% #sweep_participant{}.
-module(riak_kv_sweeper).
-behaviour(gen_server).

-export([init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).

-export([start_link/0,
         stop/0,
         add_sweep_participant/1,
         remove_sweep_participant/1,
         status/0,
         sweep_tick/0,
         sweep/1,
         sweep_result/2,
         update_progress/2,
         update_started_sweep/3,
         enable_sweep_scheduling/0,
         stop_all_sweeps/0,
         get_run_interval/1,
         in_sweep_window/0,
         sleep_for_throttle/1
        ]).

-export_type([fun_type/0,
              index/0,
              run_interval_fun/0,
              participant_module/0]).

-include("riak_kv_sweeper.hrl").

-define(SERVER, ?MODULE).
-define(CHRONOS, riak_kv_chronos).

-define(DEFAULT_SWEEP_TICK, timer:minutes(1)).
-define(ESTIMATE_EXPIRY, 86400). %% 1 day in seconds

%% ====================================================================
%% Sweep module callback types and specifications
%% ====================================================================
-type index() :: non_neg_integer().

-type sweep_fun_acc() :: term().

-type fun_type() :: 'delete_fun'
                  | 'modify_fun'
                  | 'observe_fun'.

-type run_interval_fun() :: fun(() -> non_neg_integer()).

-type sweep_fun_return() :: {'ok', sweep_fun_acc()}
                          | {'deleted', sweep_fun_acc()}
                          | {'mutated',
                             riak_object:riak_object(),
                             sweep_fun_acc()}.

-type participant_module() :: 'riak_kv_delete'
                            | 'riak_kv_index_hashtree'
                            | 'riak_kv_object_ttl'.

-type sweep_fun() :: fun(({{riak_object:bucket(), riak_object:key()},
                           riak_object:riak_object()},
                          sweep_fun_acc(),
                          Options :: [{atom(), term()}]) ->
    sweep_fun_return()).

-callback participate_in_sweep(Index :: index(),
                               SweeperProc :: pid()) ->
    {'ok', sweep_fun(), sweep_fun_acc()} |
    'false'.

-callback successful_sweep(Index :: index(),
                           FinalAcc :: sweep_fun_acc()) -> _.

-callback failed_sweep(Index :: index(), Reason :: term()) -> _.

%% ====================================================================
%% API functions
%% ====================================================================
-spec start_link() -> {ok, pid()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec stop() -> ok.
stop() ->
    gen_server:call(?SERVER, stop).

%% @doc Add callback module that will be asked to participate in
%% sweeps.
-spec add_sweep_participant(#sweep_participant{}) -> ok.
add_sweep_participant(Participant) ->
    gen_server:call(?SERVER, {add_sweep_participant, Participant}).

%% @doc Remove participant callback module.
-spec remove_sweep_participant(participant_module()) -> true | false.
remove_sweep_participant(Module) ->
    gen_server:call(?SERVER, {remove_sweep_participant, Module}).

%% @doc Initiat a sweep without using scheduling. Can be used as fold
%% replacment.
-spec sweep(index()) -> ok | {error, term()}.
sweep(Index) ->
    gen_server:call(?SERVER, {sweep_request, Index}, infinity).

%% @doc Get information about participants and all sweeps.
-spec status() -> {[#sweep_participant{}], [#sweep{}]}.
status() ->
    gen_server:call(?SERVER, status).

-spec sweep_tick() -> ok.
sweep_tick() ->
    gen_server:call(?SERVER, sweep_tick).

-spec enable_sweep_scheduling() -> ok.
enable_sweep_scheduling() ->
    lager:info("Enable sweep scheduling"),
    application:set_env(riak_kv, sweeper_scheduler, true).

%% @doc Stop all sweeps and disable the scheduler from starting new
%% sweeps. Only allow manual sweeps throu sweep/1.  Returns the number
%% of running sweeps that were stopped.
-spec stop_all_sweeps() -> Running :: non_neg_integer().
stop_all_sweeps() ->
    application:set_env(riak_kv, sweeper_scheduler, false),
    gen_server:call(?SERVER, stop_all_sweeps).

-spec update_started_sweep(index(),
                           [#sweep_participant{}],
                           EstimatedKeys :: non_neg_integer()) -> ok.
update_started_sweep(Index, ActiveParticipants, Estimate) ->
    gen_server:cast(?SERVER,
                    {update_started_sweep,
                     Index, ActiveParticipants, Estimate}).

%% @private used by the sweeping process to report results when done.
-spec sweep_result(index(), riak_kv_sweeper_fold:sweep_result()) -> ok.
sweep_result(Index, Result) ->
    gen_server:cast(?SERVER, {sweep_result, Index, Result}).

% @private used by the sweeping process to report progress.
-spec update_progress(index(), SweptKeys :: non_neg_integer()) -> ok.
update_progress(Index, SweptKeys) ->
    gen_server:cast(?SERVER, {update_progress, Index, SweptKeys}).

-spec in_sweep_window() -> boolean().
in_sweep_window() ->
    riak_kv_sweeper_state:in_sweep_window().

-spec get_run_interval(run_interval_fun()) -> non_neg_integer().
get_run_interval(RunIntervalFun) when is_function(RunIntervalFun) ->
    riak_kv_sweeper_state:get_run_interval(RunIntervalFun).

%% Exported and used within the sweeper so that we can run traces and/or
%% mock this function for testing and debugging purposes.
-spec sleep_for_throttle(timeout()) -> ok.
sleep_for_throttle(Time) ->
    timer:sleep(Time).

%% ====================================================================
%% Behavioural functions
%% ====================================================================
init([]) ->
    process_flag(trap_exit, true),
    random:seed(erlang:now()),
    schedule_initial_sweep_tick(),
    State = riak_kv_sweeper_state:new(),
    {ok, State}.

handle_call({add_sweep_participant, Participant}, _From, State) ->
    State1 = riak_kv_sweeper_state:add_sweep_participant(State, Participant),
    {reply, ok, State1};

handle_call({remove_sweep_participant, Module}, _From, State) ->
    {ok, Removed, State1} = riak_kv_sweeper_state:remove_sweep_participant(State, Module),
    {reply, Removed, State1};

handle_call({sweep_request, Index}, _From, State) ->
    State1 = riak_kv_sweeper_state:update_sweep_specs(State),
    case riak_kv_sweeper_state:sweep_request(State1, Index) of
        {ok, Index, State2} ->
            case do_sweep(Index, State2) of
                {ok, State3} ->
                    {reply, ok, State3};
                {error, Reason, State3} ->
                    {reply, {error, Reason}, State3}
            end;
        State2 ->
            {reply, {error, sweep_request_failed}, State2}
    end;

handle_call(status, _From, State) ->
    {ok, {Participants, Sweeps}, State1} = riak_kv_sweeper_state:status(State),
    {reply, {Participants , Sweeps}, State1};

handle_call(sweep_tick, _From, State) ->
    schedule_sweep_tick(),
    State4 =
        case lists:member(riak_kv, riak_core_node_watcher:services(node())) of
            true ->
                State1 = riak_kv_sweeper_state:update_sweep_specs(State),
                case riak_kv_sweeper_state:maybe_schedule_sweep(State1) of
                    {ok, Index, State2} ->
                        case do_sweep(Index, State2) of
                            {ok, State3} ->
                                State3;
                            {error, _Reason, State3} ->
                                State3
                        end;
                    State2 ->
                        State2
                end;
            false ->
                State
        end,
    {reply, ok, State4};

handle_call(stop_all_sweeps, _From, State) ->
    {ok, Running, State1} = riak_kv_sweeper_state:stop_all_sweeps(State),
    {reply, Running, State1};

handle_call(stop, _From, State) ->
    {ok, _Running, State1} = riak_kv_sweeper_state:stop_all_sweeps(State),
    {stop, normal, ok, State1}.

handle_cast({update_started_sweep, Index, ActiveParticipants, Estimate}, State) ->
    State1 = riak_kv_sweeper_state:update_started_sweep(State, Index, ActiveParticipants, Estimate),
    {noreply, State1};

handle_cast({sweep_result, Index, Result}, State) ->
    State1 = riak_kv_sweeper_state:update_finished_sweep(State, Index, Result),
    {noreply, State1};

handle_cast({update_progress, Index, SweptKeys}, State) ->
    State1 = riak_kv_sweeper_state:update_progress(State, Index, SweptKeys),
    {noreply, State1};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(Msg, State) ->
    lager:error("riak_kv_sweeper received unexpected message ~p", [Msg]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ====================================================================
%% Internal Functions
%% ====================================================================
-spec do_sweep(index(), riak_kv_sweeper_state:state()) ->
    {ok, riak_kv_sweeper_state:state()} | {error, term(), riak_kv_sweeper_state:state()}.
do_sweep(Index, State) ->
    %% Ask for estimate before we call get_active_participants, since
    %% riak_kv_index_tree clears the trees if they are expired.
    AAEEnabled = riak_kv_entropy_manager:enabled(),
    EstimatedKeys = get_estimate_keys(Index, AAEEnabled, State),

    case riak_kv_sweeper_state:get_active_participants(State, Index) of
        {ok, [], State1} ->
            {ok, State1};
        {ok, ActiveParticipants, State1} ->
            ?MODULE:update_started_sweep(Index,
                                         ActiveParticipants,
                                         EstimatedKeys),
            Result = riak_kv_vnode:sweep({Index, node()},
                                         ActiveParticipants,
                                         EstimatedKeys),
            riak_kv_sweeper_state:handle_vnode_sweep_response(State1, Index, Result)
    end.

-spec get_estimate_keys(index(),
                        AAEEnabled :: boolean(),
                        riak_kv_sweeper_state:state()) ->
    EstimatedKeys :: non_neg_integer() | false.
get_estimate_keys(Index, AAEEnabled, State) ->
    {ok, OldEstimate, _NewState} =
        riak_kv_sweeper_state:get_estimate_keys(State, Index),
    maybe_estimate_keys(Index, AAEEnabled, OldEstimate).

%% We keep the estimate from previus sweep unless it's older then
%% ?ESTIMATE_EXPIRY.
-spec maybe_estimate_keys(index(),
                          AAEEnabled :: 'true',
                          'undefined' | {EstimatedKeys :: non_neg_integer(),
                                         TS :: erlang:timestamp()}) ->
   EstimatedKeys :: non_neg_integer() | false.
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

-spec get_estimtate(riak_kv_sweeper:index()) ->
    EstimatedKeys :: non_neg_integer().
get_estimtate(Index) ->
    case riak_kv_index_hashtree:get_lock(Index, estimate) of
        ok ->
            Result = case riak_kv_index_hashtree:estimate_keys(Index) of
                         {ok, EstimatedNrKeys} ->
                             EstimatedNrKeys;
                         Other ->
                             lager:info("Failed to get estimate for ~p, got result"
                                        " ~p. Defaulting to 0...", [Index, Other]),
                             0
                     end,
            %% If we wanted to be extra paranoid, we could wrap the above in a try
            %% block and put release_lock in an after block, but then that can mask
            %% exceptions in the try block if release_lock fails too...and this will
            %% be safe right now anyway, since we don't catch exceptions anywhere;
            %% the death of the process holding the lock triggers the lock's release,
            %% so if we raise an exception the sweeper will die and the tree will unlock.
            riak_kv_index_hashtree:release_lock(Index),
            Result;
        _ ->
            lager:info("Failed to get lock for index ~p for estimate, "
                       "defaulting to 0...", [Index]),
            0
    end.

-spec schedule_initial_sweep_tick() -> _.
schedule_initial_sweep_tick() ->
    InitialTick = trunc(get_tick() * random:uniform()),
    chronos:start_timer(?CHRONOS, sweep_tick, InitialTick, {?MODULE, sweep_tick, []}).

-spec schedule_sweep_tick() -> _.
schedule_sweep_tick() ->
    chronos:start_timer(?CHRONOS, sweep_tick, get_tick(), {?MODULE, sweep_tick, []}).

-spec get_tick() -> non_neg_integer().
get_tick() ->
    DEFAULT_SWEEP_TICK = ?DEFAULT_SWEEP_TICK,
    case app_helper:get_env(riak_kv, sweep_tick, ?DEFAULT_SWEEP_TICK) of
        DEFAULT_SWEEP_TICK ->
            ?DEFAULT_SWEEP_TICK;
        Other when is_integer(Other) ->
            Other
    end.

-spec elapsed_secs(erlang:timestamp(), erlang:timestamp()) -> Secs :: non_neg_integer().
elapsed_secs(Now, Start) ->
    timer:now_diff(Now, Start) div 1000000.
