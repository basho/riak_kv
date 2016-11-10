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
-ifdef(PULSE).
-compile({parse_transform, pulse_instrument}).
-endif.

-ifdef(TEST).
-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-endif.
-include_lib("eunit/include/eunit.hrl").
-endif.
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% Default number of concurrent sweeps that are allowed to run.
-define(DEFAULT_SWEEP_CONCURRENCY,1).
-define(DEFAULT_SWEEP_TICK, timer:minutes(1)).
-define(ESTIMATE_EXPIRY, 24 * 3600). %% 1 day in s

%% Throttle used when sweeping over K/V data: {Type, Limit, Wait}.
%% Type can be pace or obj_size.
%% Default: 1 MB limit / 100 ms wait
-define(DEFAULT_SWEEP_THROTTLE, {obj_size, 1000000, 100}).

%% Default value for how much faster the sweeper throttle should be during
%% sweep window.
-define(SWEEP_WINDOW_THROTTLE_DIV, 1).

-define(SWEEPS_FILE, "sweeps.dat").

-type scheduler_event() :: {request, Index :: non_neg_integer()} | tick.
-type scheduler_sweep_window() :: never | always | {non_neg_integer(), non_neg_integer()}.

-type shedulder_result() ::
        {ok, {tick, #sweep{}}} |
        {ok, {tick, none}} |
        {ok, {tick, disabled}} |
        {ok, {tick, sweep_window_never}} |
        {ok, {tick, concurrency_limit_reached}} |
        {ok, {tick, not_in_sweep_window}} |
        {ok, {request, #sweep{}}} |
        {ok, {request, {restart, #sweep{}}}} |
        {ok, {request, restarting}} |
        {ok, {request, queue}} |
        {ok, {request, not_index}}.

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

%% Exported only for testing
-export([sweep_file/0]).

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

%% ====================================================================
%% Behavioural functions
%% ====================================================================
-record(state, {sweep_participants = dict:new() :: dict(),
                sweeps             = dict:new() :: dict(),
                timer_ref                       :: reference()
               }).

init([]) ->
    process_flag(trap_exit, true),
    random:seed(erlang:now()),
    Ref = schedule_initial_sweep_tick(),
    State0 =
        case get_persistent_participants() of
            undefined ->
                #state{};
            SP ->
                #state{sweep_participants = SP}
        end,
    State1 = State0#state{timer_ref = Ref, sweeps = get_persistent_sweeps()},
    State2 = maybe_initiate_sweeps(State1),
    {ok, State2}.

handle_call({add_sweep_participant, Participant}, _From, #state{sweep_participants = SP} = State) ->
    SP1 = dict:store(Participant#sweep_participant.module, Participant, SP),
    persist_participants(SP1),
    {reply, ok, State#state{sweep_participants = SP1}};

handle_call({remove_sweep_participant, Module}, _From, #state{sweeps = Sweeps,
                                                              sweep_participants = SP} = State) ->
    Reply = dict:is_key(Module, SP),
    SP1 = dict:erase(Module, SP),
    persist_participants(SP1),
    disable_sweep_participant_in_running_sweep(Module, Sweeps),
    {reply, Reply, State#state{sweep_participants = SP1}};

handle_call({sweep_request, Index}, _From, State) ->
    State1 = maybe_initiate_sweeps(State),
    State2 = sweep_request(Index, State1),
    {reply, ok, State2};

handle_call(status, _From, State) ->
    Participants =
        [Participant ||
         {_Mod, Participant} <- dict:to_list(State#state.sweep_participants)],
    Sweeps = [Sweep || {_Index, Sweep} <- dict:to_list(State#state.sweeps)],
    {reply, {Participants , Sweeps}, State};

handle_call(stop_all_sweeps, _From, #state{timer_ref = Ref, sweeps = Sweeps} = State) ->
    erlang:cancel_timer(Ref),
    application:set_env(riak_kv, sweeper_scheduler, false),
    Running = [Sweep || Sweep <- get_running_sweeps(Sweeps)],
    [stop_sweep(Sweep) || Sweep <- Running],
    {reply, length(Running), State};

handle_call(stop, _From, #state{sweeps = Sweeps} = State) ->
    [stop_sweep(Sweep) || Sweep <- get_running_sweeps(Sweeps)],
    {stop, normal, ok, State}.

handle_cast({update_started_sweep, Index, ActiveParticipants, Estimate}, State) ->
    State1 = update_started_sweep(Index, ActiveParticipants, Estimate, State),
    {noreply, State1};

handle_cast({sweep_result, Index, Result}, State) ->
    {noreply, update_finished_sweep(Index, Result, State)};

handle_cast({update_progress, Index, SweptKeys}, State) ->
    {noreply, update_progress(Index, SweptKeys, State)};

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
                State1 = maybe_initiate_sweeps(State),
                State2 = maybe_schedule_sweep(State1),
                State2;
            false ->
                State
        end,
    {noreply, State3#state{timer_ref = Ref}};
handle_info(Msg, State) ->
    lager:error("riak_kv_sweeper received unexpected message ~p", [Msg]),
    {noreply, State}.

terminate(_, #state{sweeps = Sweeps}) ->
    persist_sweeps(Sweeps),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ====================================================================
%% Internal functions
%% ====================================================================

maybe_schedule_sweep(State) ->
    Enabled = scheduler_enabled(),
    ConcurrenyLimit = get_concurrency_limit(),
    SweepWindow = sweep_window(),
    Now = os:timestamp(),

    case schedule_sweep1(_Event = tick,
                         Enabled, SweepWindow, ConcurrenyLimit, Now, State) of
        {ok, {tick, #sweep{} = Sweep}} ->
            do_sweep(Sweep, State);
        {ok, {tick, _Other}} ->
            State
    end.



-spec schedule_sweep1(Event            :: scheduler_event(),
                      Enabled          :: boolean(),
                      SweepWindow      :: scheduler_sweep_window(),
                      ConcurrencyLimit :: non_neg_integer(),
                      Now              :: erlang:timestamp(),
                      State            :: #state{}) -> shedulder_result().

schedule_sweep1(tick, _Enabled = false, _SweepWindow, _ConcurrencyLimit, _Now, _State) ->
    {ok, {tick, disabled}};

schedule_sweep1(tick, _Enabled = true, _SweepWindow = never, _ConcurrencyLimit, _Now, _State) ->
    {ok, {tick, sweep_window_never}};

schedule_sweep1(Event = tick, Enabled = true, _SweepWindow = always, ConcurrencyLimit, Now, State) ->
    schedule_sweep1(Event, Enabled, {0, 23}, ConcurrencyLimit, Now, State);

schedule_sweep1(tick, _Enabled = true, SweepWindow, ConcurrencyLimit, Now, State) ->
    #state{sweep_participants = Participants,
           sweeps = Sweeps} = State,
    {_, {NowHour, _, _}} = calendar:now_to_local_time(Now),
    InSweepWindow = in_sweep_window(NowHour, SweepWindow),
    CLR = length(get_running_sweeps(Sweeps)) >= ConcurrencyLimit,

    case {CLR, InSweepWindow} of
        {true, _} ->
            {ok, {tick, concurrency_limit_reached}};
        {false, false} ->
            {ok, {tick, not_in_sweep_window}};
        {false, true} ->
            schedule_sweep2(Now, Participants, Sweeps)
    end;
schedule_sweep1({request, Index}, _Enabled, _SweepWindow, ConcurrencyLimit, _Now, State) ->
    #state{sweeps = Sweeps} = State,
    CLR = length(get_running_sweeps(Sweeps)) >= ConcurrencyLimit,
    case dict:find(Index, Sweeps) of
        {ok, #sweep{state = running} = Sweep} ->
            {ok, {request, {restart, Sweep}}};
        {ok, #sweep{state = restart}} ->
            {ok, {request, restarting}};
        {ok, #sweep{} = Sweep} ->
            case CLR of
                true ->
                    {ok, {request, queue}};
                false ->
                    {ok, {request, Sweep}}
            end;
        _ ->
            {ok, {request, not_index}}
    end.


-spec schedule_sweep2(Now          :: erlang:timestamp(),
                      Participants :: dict(),
                      Sweeps       :: dict()) -> {ok, {tick, none}}|
                                                 {ok, {tick, #sweep{}}}.
schedule_sweep2(Now, Participants, Sweeps) ->
    case get_never_runned_sweeps(Sweeps) of
        [] ->
            case get_queued_sweeps(Sweeps) of
                [] ->
                    case find_expired_participant(Now, Sweeps, Participants) of
                        [] ->
                            {ok, {tick, none}};
                        Sweep ->
                            {ok, {tick, Sweep}}
                    end;
                QueuedSweeps ->
                    {ok, {tick, hd(QueuedSweeps)}}
            end;
        NeverRunnedSweeps ->
            {ok, {tick, random_sweep(NeverRunnedSweeps)}}
    end.

random_sweep(Sweeps) ->
    Index = random:uniform(length(Sweeps)),
    lists:nth(Index, Sweeps).


sweep_request(Index, #state{sweeps = Sweeps} = State) ->
    SweepSchedulerEnabled = scheduler_enabled(),
    ConcurrenyLimit = get_concurrency_limit(),
    SweepWindow = sweep_window(),
    Now = os:timestamp(),

    case schedule_sweep1({request, Index}, SweepSchedulerEnabled, SweepWindow, ConcurrenyLimit, Now, State) of
        {ok, {request, queue}} ->
            queue_sweep(Index, State);
        {ok, {request, {restart, Sweep}}} ->
            stop_sweep(Sweep),
            Sweeps1 =
                dict:store(Index, Sweep#sweep{state = restart}, Sweeps),
            State#state{sweeps = Sweeps1};
        {ok, {request, restarting}} ->
            State;
        {ok, {request, #sweep{} = Sweep}} ->
            do_sweep(Sweep, State);
        {ok, {request, not_index}} ->
            State
    end.


queue_sweep(Index, #state{sweeps = Sweeps} = State) ->
    case dict:fetch(Index, Sweeps) of
        #sweep{queue_time = undefined} = Sweep ->
            Sweeps1 =
                dict:store(Index, Sweep#sweep{queue_time = os:timestamp()}, Sweeps),
            State#state{sweeps = Sweeps1};
        _ ->
            State
    end.

do_sweep(#sweep{index = Index}, State) ->
    do_sweep(Index, State);
do_sweep(Index, #state{sweep_participants = SweepParticipants, sweeps = Sweeps} = State) ->

    %% Ask for estimate before we ask_participants since riak_kv_index_tree
    %% clears the trees if they are expired.
    AAEEnabled = riak_kv_entropy_manager:enabled(),
    Estimate = get_estimate_keys(Index, AAEEnabled, Sweeps),
    case ask_participants(Index, SweepParticipants) of
        [] ->
            State;
        ActiveParticipants ->
            ?MODULE:update_started_sweep(Index, ActiveParticipants, Estimate),
            Workerpid = riak_kv_vnode:sweep({Index, node()},
                                            ActiveParticipants,
                                            Estimate),
            start_sweep(Index, Workerpid, State)
    end.

get_estimate_keys(Index, AAEEnabled, Sweeps) ->
	#sweep{estimated_keys = OldEstimate} = dict:fetch(Index, Sweeps),
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

disable_sweep_participant_in_running_sweep(Module, Sweeps) ->
    [disable_participant(Sweep, Module) ||
       #sweep{active_participants = ActiveP} = Sweep <- get_running_sweeps(Sweeps),
       lists:keymember(Module, #sweep_participant.module, ActiveP)].

disable_participant(Sweep, Module) ->
    riak_kv_sweeper_fold:send_to_sweep_worker({disable, Module}, Sweep).

stop_sweep(Sweep) ->
    riak_kv_sweeper_fold:send_to_sweep_worker(stop, Sweep).

in_sweep_window() ->
    {_, {Hour, _, _}} = calendar:local_time(),
    in_sweep_window(Hour, sweep_window()).

in_sweep_window(_NowHour, always) ->
    true;
in_sweep_window(_NowHour, never) ->
    false;
in_sweep_window(NowHour, {Start, End}) when Start =< End ->
    (NowHour >= Start) and (NowHour =< End);
in_sweep_window(NowHour, {Start, End}) when Start > End ->
    (NowHour >= Start) or (NowHour =< End).

sweep_window() ->
    case application:get_env(riak_kv, sweep_window) of
        {ok, always} ->
            always;
        {ok, never} ->
            never;
        {ok, {StartHour, EndHour}} when StartHour >= 0, StartHour =< 23,
                                        EndHour >= 0, EndHour =< 23 ->
            {StartHour, EndHour};
        Other ->
            error_logger:error_msg("Invalid riak_kv_sweep window specified: ~p. "
                                   "Defaulting to 'always'.\n", [Other]),
            always
    end.

-spec scheduler_enabled() -> boolean().
scheduler_enabled() ->
    case app_helper:get_env(riak_kv, sweeper_scheduler, true) of
        E when E == true; E == false ->
            E;
        _ ->
            false
    end.


get_concurrency_limit() ->
    app_helper:get_env(riak_kv, sweep_concurrency, ?DEFAULT_SWEEP_CONCURRENCY).

schedule_initial_sweep_tick() ->
    InitialTick = trunc(get_tick() * random:uniform()),
    erlang:send_after(InitialTick, ?MODULE, sweep_tick).

schedule_sweep_tick() ->
    erlang:send_after(get_tick(), ?MODULE, sweep_tick).

get_tick() ->
    app_helper:get_env(riak_kv, sweep_tick, ?DEFAULT_SWEEP_TICK).

%% @private
ask_participants(Index, Participants) ->
    Funs =
        [{Participant, Module:participate_in_sweep(Index, self())} ||
         {Module, Participant} <- dict:to_list(Participants)],

    %% Filter non active participants
    [Participant#sweep_participant{sweep_fun = Fun, acc = InitialAcc} ||
     {Participant, {ok, Fun, InitialAcc}} <- Funs].

update_finished_sweep(Index, Result, #state{sweeps = Sweeps} = State) ->
    case dict:find(Index, Sweeps) of
        {ok, Sweep} ->
            Sweep1 = store_result(Result, Sweep),
            finish_sweep(Sweep1,
                         State#state{sweeps = dict:store(Index, Sweep1, Sweeps)});
        _ ->
            State
    end.

store_result({SweptKeys, Result}, #sweep{results = OldResult} = Sweep) ->
    TimeStamp = os:timestamp(),
    UpdatedResults =
        lists:foldl(fun({Mod, Outcome}, Dict) ->
                            dict:store(Mod, {TimeStamp, Outcome}, Dict)
                    end, OldResult, Result),
    Sweep#sweep{swept_keys = SweptKeys,
                estimated_keys = {SweptKeys, TimeStamp},
                results = UpdatedResults,
                end_time = TimeStamp}.

update_progress(Index, SweptKeys, #state{sweeps = Sweeps} = State) ->
    case dict:find(Index, Sweeps) of
        {ok, Sweep} ->
            Sweep1 = Sweep#sweep{swept_keys = SweptKeys},
            State#state{sweeps = dict:store(Index, Sweep1, Sweeps)};
        _ ->
            State
    end.

find_expired_participant(Now, Sweeps, Participants) ->
    ExpiredMissingSweeps =
        [{expired_or_missing(Now, Sweep, Participants), Sweep} ||
         Sweep <- get_idle_sweeps(Sweeps)],
    case ExpiredMissingSweeps of
        [] ->
            [];
        _ ->
            MostExpiredMissingSweep =
                hd(lists:reverse(lists:keysort(1, ExpiredMissingSweeps))),
            case MostExpiredMissingSweep of
                %% Non of the sweeps have a expired or missing participant.
                {{0,0}, _} -> [];
                {_N, Sweep} -> Sweep
            end
    end.

expired_or_missing(Now, #sweep{results = Results}, Participants) ->
    ResultsList = dict:to_list(Results),
    Missing = missing(run_interval, Participants, ResultsList),
    Expired =
        [begin
             RunInterval = run_interval(Mod, Participants),
             expired(Now, TS, RunInterval)
         end ||
         {Mod, {TS, _Outcome}} <- ResultsList],
    MissingSum = lists:sum(Missing),
    ExpiredSum = lists:sum(Expired),
    {MissingSum, ExpiredSum}.

missing(Return, Participants, ResultList) ->
    [case Return of
         run_interval ->
             get_run_interval(RunInterval);
         module ->
             Module
     end ||
     {Module, #sweep_participant{run_interval = RunInterval}}
         <- dict:to_list(Participants), not lists:keymember(Module, 1, ResultList)].

expired(_Now, _TS, disabled) ->
    0;
expired(Now, TS, RunInterval) ->
    case elapsed_secs(Now, TS) - RunInterval of
        N when N < 0 ->
            0;
        N ->
            N
    end.

run_interval(Mod, Participants) ->
    case dict:find(Mod, Participants) of
        {ok, #sweep_participant{run_interval = RunInterval}} ->
            get_run_interval(RunInterval);
        _ ->
            %% Participant have been disabled since last run.
            %% TODO: should we remove inactive results?
            disabled
    end.

get_run_interval(RunIntervalFun) when is_function(RunIntervalFun) ->
    RunIntervalFun();
get_run_interval(RunInterval) ->
    RunInterval.

elapsed_secs(Now, Start) ->
    timer:now_diff(Now, Start) div 1000000.


finish_sweep(#sweep{state = restart, index = Index}, State) ->
    do_sweep(Index, State);
finish_sweep(#sweep{index = Index}, #state{sweeps = Sweeps} = State) ->
    Sweeps1 =
        dict:update(Index,
                fun(Sweep) ->
                        Sweep#sweep{state = idle, pid = undefined}
                end, Sweeps),
    State#state{sweeps = Sweeps1}.

start_sweep(Index, Pid, #state{ sweeps = Sweeps} = State) ->
    Sweeps1 =
        dict:update(Index,
                    fun(Sweep) ->
                            Sweep#sweep{state = running,
                                        pid = Pid,
                                        queue_time = undefined}
                    end, Sweeps),
    State#state{sweeps = Sweeps1}.

update_started_sweep(Index, ActiveParticipants, Estimate, State) ->
    Sweeps = State#state.sweeps,
    SweepParticipants = State#state.sweep_participants,
    TS = os:timestamp(),
    Sweeps1 =
        dict:update(Index,
                fun(Sweep) ->
                        %% We add information about participants that where asked and said no
                        %% So they will not be asked again until they expire.
                        Results = add_asked_to_results(Sweep#sweep.results, SweepParticipants),
                        Sweep#sweep{results = Results,
                                    estimated_keys = {Estimate, TS},
                                    active_participants = ActiveParticipants,
                                    start_time = TS,
                                    end_time = undefined}
                end, Sweeps),
    State#state{sweeps = Sweeps1}.

add_asked_to_results(Results, SweepParticipants) ->
    ResultList = dict:to_list(Results),
    MissingResults = missing(module, SweepParticipants, ResultList),
    TimeStamp = os:timestamp(),
    lists:foldl(fun(Mod, Dict) ->
                        dict:store(Mod, {TimeStamp, asked}, Dict)
                end, Results, MissingResults).

maybe_initiate_sweeps(#state{sweeps = Sweeps} = State) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Indices = riak_core_ring:my_indices(Ring),
    MissingIdx = [Idx || Idx <- Indices,
                         not dict:is_key(Idx, Sweeps)],
    Sweeps1 = add_sweeps(MissingIdx, Sweeps),

    NotOwnerIdx = [Index || {Index, _Sweep} <- dict:to_list(Sweeps),
                            not lists:member(Index, Indices)],
    Sweeps2 = remove_sweeps(NotOwnerIdx, Sweeps1),

    State#state{sweeps = Sweeps2}.

add_sweeps(MissingIdx, Sweeps) ->
    lists:foldl(fun(Idx, SweepsDict) ->
                        dict:store(Idx, #sweep{index = Idx}, SweepsDict)
                end, Sweeps, MissingIdx).

remove_sweeps(NotOwnerIdx, Sweeps) ->
    lists:foldl(fun(Idx, SweepsDict) ->
                        dict:erase(Idx, SweepsDict)
                end, Sweeps, NotOwnerIdx).

get_running_sweeps(Sweeps) ->
    [Sweep ||
      {_Index, #sweep{state = State} = Sweep} <- dict:to_list(Sweeps),
      State == running orelse State == restart].

get_queued_sweeps(Sweeps) ->
    QueuedSweeps =
        [Sweep ||
         {_Index, #sweep{queue_time = QueueTime} = Sweep} <- dict:to_list(Sweeps),
         not (QueueTime == undefined)],
    lists:keysort(#sweep.queue_time, QueuedSweeps).

get_idle_sweeps(Sweeps) ->
    [Sweep || {_Index, #sweep{state = idle} = Sweep} <- dict:to_list(Sweeps)].

get_never_runned_sweeps(Sweeps) ->
    [Sweep || {_Index, #sweep{state = idle, results = ResDict} = Sweep}
                  <- dict:to_list(Sweeps), dict:size(ResDict) == 0].

persist_participants(Participants) ->
    application:set_env(riak_kv, sweep_participants, Participants).

get_persistent_participants() ->
    app_helper:get_env(riak_kv, sweep_participants).

persist_sweeps(Sweeps) ->
    CleanedSweep =
        dict:map(fun(_Key, Sweep) ->
                     #sweep{index = Sweep#sweep.index,
                            start_time = Sweep#sweep.start_time,
                            end_time = Sweep#sweep.end_time,
                            results = Sweep#sweep.results
                            }
             end, Sweeps),
    file:write_file(sweep_file(?SWEEPS_FILE) , io_lib:fwrite("~p.\n",[CleanedSweep])).

get_persistent_sweeps() ->
    case file:consult(sweep_file(?SWEEPS_FILE)) of
        {ok, [Sweeps]} ->
            Sweeps;
        _ ->
            dict:new()
    end.

sweep_file() ->
    sweep_file(?SWEEPS_FILE).

sweep_file(File) ->
     PDD = app_helper:get_env(riak_core, platform_data_dir, "/tmp"),
     SweepDir = filename:join(PDD, ?MODULE),
     SweepFile = filename:join(SweepDir, File),
     ok = filelib:ensure_dir(SweepFile),
     SweepFile.


%% ====================================================================
%% Unit tests
%% ====================================================================
-ifdef(TEST).

sweeper_test_() ->
    {foreach, fun setup/0, fun cleanup/1,
     [
        fun test_initiate_sweeps/1,
        fun test_find_never_sweeped/1,
        fun test_find_missing_part/1
    ]}.

setup() ->
    MyRingPart = lists:seq(1, 10),
    Participants = [1,10,100],
    meck:new(riak_core_ring_manager),
    meck:expect(riak_core_ring_manager, get_my_ring, fun() -> {ok, ring} end),
    meck:new(riak_core_ring),
    meck:expect(riak_core_ring, my_indices,  fun(ring) -> MyRingPart  end),
    meck:new(riak_kv_vnode),
    meck:expect(riak_kv_vnode, sweep, fun(_, _, _) -> [] end),
    State = maybe_initiate_sweeps(#state{}),
    State1 = add_test_sweep_participant(State, Participants),
    {MyRingPart, Participants, State1}.

add_test_sweep_participant(StateIn, Participants) ->
    Fun = fun(N, State) ->
                  Participant = test_sweep_participant(N),
                  {reply, ok, StateOut} =
                      handle_call({add_sweep_participant, Participant}, nobody, State),
                  StateOut
          end,
    lists:foldl(Fun, StateIn, Participants).

test_sweep_participant(N) ->
    Module = get_module(N),
    meck:new(Module, [non_strict]),
    meck:expect(Module, participate_in_sweep, fun(_, _) -> {ok, sweepfun, acc} end),
    #sweep_participant{module = Module,
                       run_interval = N
                      }.
get_module(N) ->
    list_to_atom(integer_to_list(N)).

test_initiate_sweeps({MyRingPart, _Participants, State}) ->
    fun() ->
            ?assertEqual(length(MyRingPart), dict:size(State#state.sweeps))
    end.

test_find_never_sweeped({MyRingPart, Participants, State}) ->
    fun() ->
            %% One sweep will not be given any results
            %% so it will be returnd by get_never_sweeped
            [NoResult | Rest]  = MyRingPart,
            Result = [{get_module(Part), succ} ||Part <- Participants],
            State1 =
                lists:foldl(fun(Index, AccState) ->
                                    update_finished_sweep(Index, {0, Result}, AccState)
                            end, State, Rest),
            [NeverRunnedSweep] = get_never_runned_sweeps(State1#state.sweeps),
            ?assertEqual(NeverRunnedSweep#sweep.index, NoResult)
    end.

test_find_missing_part({MyRingPart, Participants, State}) ->
    fun() ->
            %% Give all but one index results from all participants
            %% The last Index will miss one result and would be prioritized
            [NotAllResult | Rest]  = MyRingPart,
            Result = [{get_module(Part), succ} || Part <- Participants],

            State1 =
                lists:foldl(fun(Index, AccState) ->
                                    update_finished_sweep(Index, {0, Result}, AccState)
                            end, State, Rest),

            Result2 = [{get_module(Part), succ} || Part <- tl(Participants)],
            State2 = update_finished_sweep(NotAllResult, {0, Result2}, State1),
            ?assertEqual([], get_never_runned_sweeps(State2#state.sweeps)),
            MissingPart =
                find_expired_participant(os:timestamp(), State2#state.sweeps, State2#state.sweep_participants),
            ?assertEqual(MissingPart#sweep.index, NotAllResult)
    end.

cleanup(_State) ->
    meck:unload().
-endif.

-ifdef(EQC).
gen_now() ->
    ?LET({IncMins, {Megas, Secs, Micros}}, {nat(), os:timestamp()},
         {Megas, Secs + (60*IncMins), Micros}).


gen_indices() ->
    ?LET(Max, nat(),
        lists:seq(0, Max)).


gen_sweep_participant_modules() ->
    Participants = [sp0, sp1, sp2, sp3, sp4, sp5, sp6, sp7, sp8, sp9],
    ?LET(N, nat(), lists:sublist(Participants, N)).


gen_sweep_participant(Module) ->
    #sweep_participant{
       description = 'EQC sweep participant',
       module = Module,
       fun_type = oneof([?DELETE_FUN, ?MODIFY_FUN, ?OBSERV_FUN]),
       sweep_fun = eqc_sweep_function,
       run_interval = oneof([60, 3600]),
       acc = [],
       options = [],
       errors = 0,
       fail_reason = undefined}.


gen_empty_sweep_results() ->
    dict:new().


gen_nonempty_sweep_results({Mega, Secs, Micro}) ->
    ?LET({N, Modules, SweepResult}, {nat(), gen_sweep_participant_modules(), oneof([succ, fail])},
         lists:foldl(fun(Module, D) ->
                             dict:store(Module, {{Mega, Secs - (N*60) , Micro}, SweepResult}, D)
                     end,
                     dict:new(), Modules)).


gen_sweep_neverrun(Index) ->
    ?LET(StartTime, os:timestamp(),
         #sweep{index = Index,
                state = idle,
                results = gen_empty_sweep_results(),
                active_participants = [],
                start_time = StartTime,
                end_time = undefined,
                queue_time = undefined}).


gen_sweep_running(Index) ->
    ?LET(StartTime, os:timestamp(),
         #sweep{index = Index,
                state = running,
                results = oneof([gen_empty_sweep_results(), gen_nonempty_sweep_results(StartTime)]),
                start_time = StartTime,
                end_time = undefined,
                queue_time = undefined}).


gen_sweep_restart(Index) ->
    ?LET(StartTime, os:timestamp(),
         #sweep{index = Index,
                state = restart,
                results = oneof([gen_empty_sweep_results(), gen_nonempty_sweep_results(StartTime)]),
                start_time = StartTime,
                end_time = undefined,
                queue_time = undefined}).


gen_sweep_queued(Index) ->
    ?LET(StartTime, os:timestamp(),
         #sweep{index = Index,
                state = idle,
                results = oneof([gen_empty_sweep_results(), gen_nonempty_sweep_results(StartTime)]),
                start_time = StartTime,
                end_time = undefined,
                queue_time = StartTime}).


gen_sweep_ended(Index) ->
        ?LET(StartTime, os:timestamp(),
         begin
         {Mega, Secs, Micros} = StartTime,
         #sweep{index = Index,
                state = idle,
                results = gen_nonempty_sweep_results(StartTime),
                start_time = {Mega, Secs - 180, Micros},
                end_time = StartTime,
                queue_time = undefined}
         end).


gen_sweep(Index) ->
    oneof([gen_sweep_neverrun(Index),
           gen_sweep_running(Index),
           gen_sweep_queued(Index),
           gen_sweep_ended(Index),
           gen_sweep_restart(Index)]).


gen_sweep_participants() ->
    ?LET(Modules, gen_sweep_participant_modules(),
         ?LET(SPs, [gen_sweep_participant(M) || M <- Modules],
              lists:foldl(
                fun(SP, D) ->
                        dict:store(SP#sweep_participant.module, SP, D)
                end,
                dict:new(), SPs))).


gen_sweeps() ->
    ?LET(Indices, gen_indices(),
         ?LET(Sweeps, [gen_sweep(I) || I <- Indices],
              lists:foldl(fun(Sweep, D) ->
                                  dict:store(Sweep#sweep.index, Sweep, D)
                          end,
                          dict:new(), Sweeps))).


gen_hour() ->
    choose(0, 23).


gen_sweep_window() ->
    oneof([always, never, {gen_hour(),
                           gen_hour()}]).

gen_concurrency_limit() ->
    oneof([0, 1, 4]).

gen_state() ->
    ?LET({SPs, Sweeps}, {gen_sweep_participants(), gen_sweeps()},
         #state{sweep_participants=SPs,
                sweeps=Sweeps}).

gen_enabled() ->
    frequency([{10, true}, {1, false}]).


is_sweep_index_in(Sweep, Sweeps) ->
    lists:member(Sweep#sweep.index,
                 [Index || #sweep{index = Index} <- Sweeps]).


prop_schedule_sweep_request() ->
    ?FORALL({Event, Enabled, SweepWindow, ConcurrencyLimit, Now, State},
            {{request, nat()},
             gen_enabled(),
             gen_sweep_window(),
             gen_concurrency_limit(),
             gen_now(),
             gen_state()},
            begin
                SPs = State#state.sweep_participants,
                Sweeps = State#state.sweeps,
                SPs = State#state.sweep_participants,
                case schedule_sweep1(Event, Enabled, SweepWindow, ConcurrencyLimit, Now, State) of
                    {ok, {request, not_index}} ->
                        {request, Index} = Event,
                        Indices = [I0 || {I0, _} <- dict:to_list(Sweeps)],
                        not lists:member(Index, Indices);
                    {ok, {request, restarting}} ->
                        {request, Index} = Event,
                        {ok, Sweep} = dict:find(Index, Sweeps),
                        restart == Sweep#sweep.state;
                    {ok, {request, {restart, Sweep}}} ->
                        running == Sweep#sweep.state;
                    {ok, {request, queue}} ->
                        get_running_sweeps(Sweeps) >= ConcurrencyLimit;
                    {ok, {request, Sweep}} ->
                        length(get_running_sweeps(Sweeps)) < ConcurrencyLimit andalso
                            Sweep#sweep.state == idle
                end
            end).

prop_schedule_sweep_tick() ->
        ?FORALL({Event, Enabled, SweepWindow, ConcurrencyLimit, Now, State},
            {tick,
             gen_enabled(),
             gen_sweep_window(),
             gen_concurrency_limit(),
             gen_now(),
             gen_state()},
            begin
                SPs = State#state.sweep_participants,
                Sweeps = State#state.sweeps,
                NeverRunSweeps = get_never_runned_sweeps(Sweeps),
                QueuedSweeps = get_queued_sweeps(Sweeps),
                ExpiredSweep = find_expired_participant(Now, Sweeps, SPs),
                SPs = State#state.sweep_participants,
                case schedule_sweep1(Event, Enabled, SweepWindow, ConcurrencyLimit, Now, State) of
                    {ok, {tick, none}} ->
                        length(NeverRunSweeps) == 0 andalso
                            length (QueuedSweeps) == 0 andalso
                            ExpiredSweep == [];
                    {ok, {tick, concurrency_limit_reached}} ->
                        length(get_running_sweeps(Sweeps)) >= ConcurrencyLimit;
                    {ok, {tick, disabled}} ->
                        Enabled == false;
                    {ok, {tick, sweep_window_never}} ->
                        SweepWindow == never;
                    {ok, {tick, not_in_sweep_window}} ->
                        {_, {NowHour, _, _}} = calendar:now_to_local_time(Now),
                        not in_sweep_window(NowHour, SweepWindow);
                    {ok, {tick, #sweep{} = Sweep}} ->
                        case {NeverRunSweeps, QueuedSweeps, ExpiredSweep} of
                            {[], [], []} ->
                                false ;
                            {[], [], ExpiredSweep} ->
                                is_sweep_index_in(Sweep, [ExpiredSweep]);
                            {[], QueuedSweeps, _} ->
                                is_sweep_index_in(Sweep, QueuedSweeps);
                            {NeverRunSweeps, [], _} ->
                                is_sweep_index_in(Sweep, NeverRunSweeps);
                            {NeverRunSweeps, QueuedSweeps, []} ->
                                is_sweep_index_in(Sweep, NeverRunSweeps);
                            {NeverRunSweeps, QueuedSweeps, ExpiredSweep} ->
                                is_sweep_index_in(Sweep, NeverRunSweeps)
                        end
                end
            end).


prop_schedule_sweep1_test_() ->
    {timeout, 30,
     [fun() -> ?assert(eqc:quickcheck(prop_schedule_sweep_request())) end,
      fun() -> ?assert(eqc:quickcheck(prop_schedule_sweep_tick())) end]}.


prop_in_window() ->
    ?FORALL({NowHour, WindowLen, StartTime}, {choose(0, 23), choose(0, 23), choose(0, 23)},
            begin
                EndTime = (StartTime + WindowLen) rem 24,

                %% Generate a set of all hours within this window
                WindowHours = [H rem 24 || H <- lists:seq(StartTime, StartTime + WindowLen)],

                %% If NowHour is in the set of windows hours, we expect our function
                %% to indicate that we are in the window
                ExpInWindow = lists:member(NowHour, WindowHours),
                ?assertEqual(ExpInWindow, in_sweep_window(NowHour, {StartTime, EndTime})),
                true
            end).

prop_in_window_test_() ->
    {timeout, 30,
     [fun() -> ?assert(eqc:quickcheck(prop_in_window())) end]}.


-endif.
