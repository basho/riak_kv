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

-define(MAX_SWEEP_CRASHES, 10).
%% Throttle used when sweeping over K/V data: {Type, Limit, Wait}.
%% Default: 1 MB limit / 100 ms wait
-define(DEFAULT_SWEEP_THROTTLE, {obj_size, 1000000, 100}).

%% ====================================================================
%% API functions
%% ====================================================================
-export([start_link/0,
         add_sweep_participant/1,
         remove_sweep_participant/1,
         status/0,
         sweep/1,
         sweep_result/2,
         update_started_sweep/3,
         stop_all_sweeps/0,
         disable_sweep_scheduling/0,
         enable_sweep_scheduling/0,
         get_run_interval/1,
         get_sweep_throttle/0,
         do_sweep/8]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

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
    gen_server:call(?MODULE, {sweep_request, Index}).

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
    application:set_env(riak_kv, sweeper_scheduler, false),
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
                sweeps             = dict:new() :: dict()
               }).

init([]) ->
    process_flag(trap_exit, true),
    random:seed(erlang:now()),
    schedule_initial_sweep_tick(),
    case get_persistent_participants() of
        undefined ->
            {ok, #state{}};
        SP ->
            {ok, #state{sweep_participants = SP}}
    end.

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
    State1 = sweep_request(Index, State),
    {reply, ok, State1};

handle_call(status, _From, State) ->
    State1 =
        case dict:size(State#state.sweeps) of
            0 ->
                maybe_initiate_sweeps(State);
            _ ->
                State
        end,
    Participants =
        [Participant ||
         {_Mod, Participant} <- dict:to_list(State1#state.sweep_participants)],
    Sweeps =   [Sweep || {_Index, Sweep} <- dict:to_list(State1#state.sweeps)],
    {reply, {Participants , Sweeps}, State1};

handle_call(stop_all_sweeps, _From, #state{sweeps = Sweeps} = State) ->
    [stop_sweep(Sweep) || Sweep <- get_running_sweeps(Sweeps)],
    {reply, ok, State}.

handle_cast({update_started_sweep, Index, ActiveParticipants, Estimate}, State) ->
    State1 = update_started_sweep(Index, ActiveParticipants, Estimate, State),
    {noreply, State1};

handle_cast({sweep_result, Index, Result}, State) ->
    {noreply, update_finished_sweep(Index, Result, State)};

handle_cast({update_progress, Index, SweptKeys}, State) ->
    {noreply, update_progress(Index, SweptKeys, State)};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(sweep_tick, State) ->
    schedule_sweep_tick(),
    case lists:member(riak_kv, riak_core_node_watcher:services(node())) of
        true ->
            State1 = maybe_initiate_sweeps(State),
            State2 = maybe_schedule_sweep(State1),
            {noreply, State2};
        false ->
            {noreply, State}
    end;
handle_info({estimate,_}, State) ->
    {noreply, State}.

%% terminate(shutdown, _State) ->
%%     delete_persistent_participants(),
%%     ok;
terminate(_, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ====================================================================
%% Internal functions
%% ====================================================================

maybe_schedule_sweep(#state{sweeps = Sweeps} = State) ->
    CLR = concurrency_limit_reached(Sweeps),
    InSweepWindow = in_sweep_window(),
    SweepSchedulerEnabled = app_helper:get_env(riak_kv, sweeper_scheduler, true),
    case InSweepWindow and not CLR and SweepSchedulerEnabled of
        true ->
            schedule_sweep(State);
        false ->
            State
    end.

schedule_sweep(#state{sweeps = Sweeps,
                      sweep_participants = Participants} = State) ->
    case get_never_runned_sweeps(Sweeps) of
        [] ->
            case get_queued_sweeps(Sweeps) of
                [] ->
                    case find_expired_participant(Sweeps, Participants) of
                        [] ->
                            State;
                        Sweep ->
                            do_sweep(Sweep, State)
                    end;
                QueuedSweeps ->
                    lager:info("QueuedSweeps  ~p", [QueuedSweeps]),
                    do_sweep(hd(QueuedSweeps), State)
            end;
        NeverRunnedSweeps ->
            do_sweep(hd(NeverRunnedSweeps), State)
    end.

sweep_request(Index, #state{sweeps = Sweeps} = State) ->
    case maybe_restart(Index, State) of
        false ->
            case concurrency_limit_reached(Sweeps) of
                true ->
                    queue_sweep(Index, State);
                false ->
                     do_sweep(Index, State)
            end;
        RestartState ->
            RestartState
    end.

maybe_restart(Index, #state{sweeps = Sweeps} = State) ->
    case dict:find(Index, Sweeps) of
        {ok, #sweep{state = running} = Sweep} ->
            stop_sweep(Sweep),
            %% Setup sweep for restart
            %% When the running sweep finish it will start a new
            Sweeps1 =
                dict:store(Index, Sweep#sweep{state = restart}, Sweeps),
            State#state{sweeps = Sweeps1};
        {ok, #sweep{state = restart}} ->
            %% Already restarting
            State;
        {ok, #sweep{}} ->
            false;
        _ ->
            %% New index since last tick
            sweep_request(Index, maybe_initiate_sweeps(State))
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
    spawn(fun() ->
                Estimate =
                    case riak_kv_index_hashtree:get_lock(Index, estimate) of
                        ok ->
                            case riak_kv_index_hashtree:estimate_keys(Index) of
                                {ok, EstimatedNrKeys} ->
                                    EstimatedNrKeys;
                                _ ->
                                    0
                            end;
                        _ ->
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
    send_to_sweep_worker({disable, Module}, Sweep).

stop_sweep(Sweep) ->
    send_to_sweep_worker(stop, Sweep).

send_to_sweep_worker(Msg, #sweep{index = Index, pid = Pid}) when is_pid(Pid)->
    Ref = make_ref(),
    Pid ! {Msg, self(), Ref},
    receive
        {ack, Ref} ->
            ok;
        {Response, Ref} ->
            Response
    after 4000 ->
        lager:error("No response from sweep proc index ~p ~p", [Index, Pid]),
        false
    end;
send_to_sweep_worker(Msg, #sweep{index = Index}) ->
    lager:info("no pid ~p to ~p " , [Msg, Index]),
    no_pid.

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

concurrency_limit_reached(Sweeps) ->
    length(get_running_sweeps(Sweeps)) >= get_concurrency_limit().

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

find_expired_participant(Sweeps, Participants) ->
    ExpiredMissingSweeps =
        [{expired_or_missing(Sweep, Participants), Sweep} ||
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

expired_or_missing(#sweep{results = Results}, Participants) ->
    Now = os:timestamp(),
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

format_result(#sa{swept_keys = SweptKeys, active_p = Succ, failed_p = Failed}) ->
    {SweptKeys,
     format_result(succ, Succ) ++ format_result(fail, Failed)}.
format_result(SuccFail, Results) ->
    [{Module, SuccFail} || #sweep_participant{module = Module} <- Results].

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

%% Used by riak_kv_vnode:sweep/3
do_sweep(ActiveParticipants, EstimatedKeys, Sender, Opts, Index, Mod, ModState, VnodeState) ->
    CompleteFoldReq = make_complete_fold_req(),
    InitialAcc = make_initial_acc(Index, ActiveParticipants, EstimatedKeys),
    case Mod:fold_objects(CompleteFoldReq, InitialAcc, Opts, ModState) of
        #sa{} = Acc ->
            inform_participants(Acc, Index),
            {reply, Acc, VnodeState};
        {async, Work} ->
            FinishFun =
                fun(Acc) ->
                        inform_participants(Acc, Index),
                        ?MODULE:sweep_result(Index, format_result(Acc))
                end,
            {async, {sweep, Work, FinishFun}, Sender, VnodeState};
        Reason ->
            failed_sweep(ActiveParticipants, Index, Reason),
            {reply, Reason, VnodeState}
    end.

inform_participants(#sa{active_p = Succ, failed_p = Failed}, Index) ->
    successfull_sweep(Succ, Index),
    failed_sweep(Failed, Index).

successfull_sweep(Succ, Index) ->
    [Module:successfull_sweep(Index, FinalAcc) ||
       #sweep_participant{module = Module, acc = FinalAcc} <- Succ].
failed_sweep(Failed, Index) ->
    [Module:failed_sweep(Index, Reason) ||
       #sweep_participant{module = Module, fail_reason = Reason } <- Failed].
failed_sweep(Failed, Index, Reason) ->
    [Module:failed_sweep(Index, Reason) ||
       #sweep_participant{module = Module} <- Failed].

%% @private
make_complete_fold_req() ->
    fun(Bucket, Key, RObjBin, #sa{index = Index, swept_keys = SweptKeys} = Acc) ->
            Acc1 = maybe_throttle_sweep(RObjBin, Acc),
            Acc2 =
                case SweptKeys rem 1000 of
                    0 ->
                        update_progress(Index, SweptKeys),
                        maybe_receive_request(Acc1);
                    _ ->
                        Acc1
                end,
            RObj = riak_object:from_binary(Bucket, Key, RObjBin),
            fold_funs({{Bucket, Key}, RObj}, Acc2#sa{swept_keys = SweptKeys + 1})
    end.

fold_funs(_, #sa{index = Index,
                 failed_p = FailedParticipants,
                 active_p = [],
                 succ_p = []} = SweepAcc) ->
    lager:info("No more participants in sweep of Index ~p Failed: ~p", [Index, FailedParticipants]),
    throw(SweepAcc);

%%% No active participants return all succ for next key to run
fold_funs(_, #sa{active_p = [],
                 succ_p = Succ} = SweepAcc) ->
    SweepAcc#sa{active_p = lists:reverse(Succ),
                succ_p = []};

%% Check if the sweep_participant have reached crash limit.
fold_funs(KeyObj, #sa{failed_p = Failed,
                      active_p = [#sweep_participant{errors = ?MAX_SWEEP_CRASHES,
                                                     module = Module}
                                      = Sweep | Rest]} = SweepAcc) ->
    lager:error("Sweeper fun ~p crashed too many times.", [Module]),
    fold_funs(KeyObj, SweepAcc#sa{active_p = Rest,
                                  failed_p = [Sweep#sweep_participant{fail_reason = too_many_crashes} | Failed]});

%% Key deleted nothing to do
fold_funs(deleted, #sa{active_p = [Sweep | ActiveRest],
                       succ_p = Succ} = SweepAcc) ->
    fold_funs(deleted, SweepAcc#sa{active_p = ActiveRest,
                                   succ_p = [Sweep | Succ]});

%% Main function: call fun with it's acc and aptionals
fold_funs({BKey, RObj}, #sa{active_p = [Sweep | ActiveRest],
                            succ_p = Succ,
                            modified_objects = ModObj} = SweepAcc) ->
    #sweep_participant{sweep_fun = Fun,
                       acc = Acc,
                       errors = Errors,
                       options = Options} = Sweep,
    {Opt, SweepAcc1} = maybe_add_opt_info({BKey, RObj}, SweepAcc, Options),

    try Fun({BKey, RObj}, Acc, Opt) of
        {deleted, NewAcc} ->
            riak_kv_vnode:local_reap(SweepAcc1#sa.index, BKey, RObj),
            fold_funs(deleted,
                      SweepAcc1#sa{active_p = ActiveRest,
                                   modified_objects = ModObj + 1,
                                   succ_p = [Sweep#sweep_participant{acc = NewAcc} | Succ]});
        {mutated, MutatedRObj, NewAcc} ->
            riak_kv_vnode:local_put(SweepAcc1#sa.index, MutatedRObj, [{hashtree_action, tombstone}]),
            fold_funs({BKey, MutatedRObj},
                      SweepAcc1#sa{active_p = ActiveRest,
                                   modified_objects = ModObj + 1,
                                   succ_p = [Sweep#sweep_participant{acc = NewAcc} | Succ]});
        {ok, NewAcc} ->
            fold_funs({BKey, RObj},
                      SweepAcc1#sa{active_p = ActiveRest,
                                   succ_p = [Sweep#sweep_participant{acc = NewAcc} | Succ]})
    catch C:T ->
              lager:error("Sweeper fun crashed ~p ~p Key: ~p", [{C, T}, Sweep, BKey]),
              fold_funs({BKey, RObj},
                        SweepAcc1#sa{active_p = ActiveRest,
                                     %% We keep the sweep in succ unil we have enough crashes.
                                     succ_p = [Sweep#sweep_participant{errors = Errors + 1} | Succ]})
    end.

maybe_add_opt_info({BKey, RObj}, SweepAcc, Options) ->
    lists:foldl(fun(Option, InfoSweepAcc) ->
                        add_opt_info({BKey, RObj}, Option, InfoSweepAcc)
                end, {[], SweepAcc}, Options).

add_opt_info({{Bucket, _Key}, _RObj}, bucket_props, {OptInfo, #sa{bucket_props = BucketPropsDict} = SweepAcc}) ->
    {BucketProps, BucketPropsDict1} = get_bucket_props(Bucket, BucketPropsDict),
    {[{bucket_props, BucketProps} | OptInfo], SweepAcc#sa{bucket_props = BucketPropsDict1}}.

get_bucket_props(Bucket, BucketPropsDict) ->
    case dict:find(Bucket, BucketPropsDict) of
        {ok, BucketProps} ->
            {BucketProps, BucketPropsDict};
        _ ->
            BucketProps = riak_core_bucket:get_bucket(Bucket),
            BucketPropsDict1 = dict:store(Bucket, BucketProps, BucketPropsDict),
            {BucketProps, BucketPropsDict1}
    end.

%% Throttle depending on swept keys.
maybe_throttle_sweep(_RObjBin, #sa{throttle = {pace, Limit, Wait},
                         swept_keys = SweepKeys} = SweepAcc) ->
    case SweepKeys rem Limit of
        0 ->
            NewThrottle = get_sweep_throttle(),
            %% We use receive after to throttle instead of sleep.
            %% This way we can respond on requests while throttling
            SweepAcc1 =
                maybe_receive_request(SweepAcc#sa{throttle = NewThrottle}, Wait),
            maybe_extra_throttle(SweepAcc1);
        _ ->
            maybe_extra_throttle(SweepAcc)
    end;

%% Throttle depending on total obj_size.
maybe_throttle_sweep(RObjBin, #sa{throttle = {obj_size, Limit, Wait},
                         total_obj_size = TotalObjSize} = SweepAcc) ->
    ObjSize = byte_size(RObjBin),
    TotalObjSize1 = ObjSize + TotalObjSize,
    case (Limit =/= 0) andalso (TotalObjSize1 > Limit) of
        true ->
            NewThrottle = get_sweep_throttle(),
            %% We use receive after to throttle instead of sleep.
            %% This way we can respond on requests while throttling
            SweepAcc1 =
                maybe_receive_request(SweepAcc#sa{throttle = NewThrottle}, Wait),
            maybe_extra_throttle(SweepAcc1#sa{total_obj_size = 0});
        _ ->
            maybe_extra_throttle(SweepAcc#sa{total_obj_size = TotalObjSize1})
    end.

%% Throttle depending on how many objects the sweep modify.
maybe_extra_throttle(#sa{throttle = Throttle,
                         modified_objects = ModObj} = SweepAcc) ->
    case Throttle of
        {pace, Limit, Wait} ->
            ok;
        {obj_size, _SizeLimit, Wait} ->
            Limit = 100
    end,
    %% +1 since some sweeps doesn't modify any objects
    case ModObj + 1 rem Limit of
        0 ->
            maybe_receive_request(SweepAcc, Wait);
        _ ->
            SweepAcc
    end.

get_sweep_throttle() ->
    app_helper:get_env(riak_kv, sweep_throttle, ?DEFAULT_SWEEP_THROTTLE).

maybe_receive_request(Acc) ->
    maybe_receive_request(Acc, 0).

maybe_receive_request(#sa{active_p = Active, failed_p = Fail } = Acc, Wait) ->
    receive
        {stop, From, Ref} ->
            Active1 =
                [ActiveSP#sweep_participant{fail_reason = sweep_stop } ||
                 #sweep_participant{} = ActiveSP <- Active],
            Acc1 = #sa{active_p = [],  failed_p = Active1 ++ Fail},
            From ! {ack, Ref},
            throw({stop_sweep, Acc1});
        {{disable, Module}, From, Ref} ->
            case lists:keytake(Module, #sweep_participant.module, Active) of
                {value, SP, Active1} ->
                    From ! {ack, Ref},
                    Acc#sa{active_p = Active1,
                           failed_p = [SP#sweep_participant{fail_reason = disable} | Fail]};
                _ ->
                    From ! {ack, Ref},
                    Acc
            end
    after Wait ->
        Acc
    end.

%% Make sweep accumulator with all ActiveParticipants
%% that will be called for each key
make_initial_acc(Index, ActiveParticipants, EstimatedNrKeys) ->
    SweepsParticipants =
        [AP ||
         %% Sort list depening on fun typ.
         AP <- lists:keysort(#sweep_participant.fun_type, ActiveParticipants)],
    #sa{index = Index, active_p = SweepsParticipants, estimated_keys = EstimatedNrKeys}.


%% ====================================================================
%% Unit tests
%% ====================================================================

-ifdef(TEST).

%% Basic sweep test. Check that callback get complete Acc when sweep finish
sweep_delete_test() ->
    setup_sweep(Keys = 100),
    DeleteRem = 3,
    {reply, Acc, _State} =
        do_sweep(delete_sweep(DeleteRem), 0, no_sender, [], no_index, fake_backend, [], []),
    ?assertEqual(Keys, Acc#sa.swept_keys),
    %% Verify acc return
    [{_Pid,{_,_,[_,N]},ok}] = meck:history(delete_callback_module),
    ?assertEqual(Keys div DeleteRem, N),
    meck:unload().

%% Verify that a sweep asking for bucket_props gets them
sweep_delete_bucket_props_test() ->
    setup_sweep(Keys = 100),
    {reply, Acc, _State} =
        do_sweep(delete_sweep_bucket_props(), 0, no_sender, [], no_index, fake_backend, [], []),

    ?assertEqual(Keys, dict:size(Acc#sa.bucket_props)),
    ?assertEqual(Keys, Acc#sa.swept_keys),
    meck:unload().

%% Delete 1/3 of the keys the rest gets seen by the observer sweep
sweep_observ_delete_test() ->
    setup_sweep(Keys = 100),
    DeleteRem = 3,
    Sweeps = delete_sweep(DeleteRem) ++ observ_sweep(),
    {reply, Acc, _State} = do_sweep(Sweeps, 0, no_sender, [], no_index, fake_backend, [], []),
    ?assertEqual(Keys, Acc#sa.swept_keys),
    %% Verify acc return
    [{_Pid,{_,_,[_,DeleteN]},ok}] = meck:history(delete_callback_module),
    [{_Pid,{_,_,[_,{0, ObservN}]},ok}] = meck:history(observ_callback_module),

    NrDeleted = Keys div DeleteRem,
    ?assertEqual(NrDeleted, DeleteN),
    ?assertEqual(Keys - NrDeleted, ObservN),
    ?assertEqual(Keys, Acc#sa.swept_keys),
    meck:unload().

%% Test including all types of sweeps. Delete 1/4 Modify 1/4 and
sweep_modify_observ_delete_test() ->
    setup_sweep(Keys = 100),
    DeleteRem = 4,
    ModifyRem = 2,
    Sweeps = delete_sweep(DeleteRem) ++ observ_sweep() ++ modify_sweep(ModifyRem),
    {reply, Acc, _State} = do_sweep(Sweeps, 0, no_sender, [], no_index, fake_backend, [], []),
    %% Verify acc return
    [{_Pid,{_,_,[_,DeleteN]},ok}] = meck:history(delete_callback_module),
    [{_Pid,{_,_,[_,ModifyN]},ok}] = meck:history(modify_callback_module),
    [{_Pid,{_,_,[_,{Mod, Org}]},ok}] = meck:history(observ_callback_module),

    %% Delete and modify should have touched the same number of object.
    NrDeletedModify = Keys div DeleteRem,
    ?assertEqual(NrDeletedModify, DeleteN),
    ?assertEqual(NrDeletedModify, ModifyN),

    ?assertEqual(Mod, ModifyN),
    ?assertEqual(Org, Keys - DeleteN - ModifyN),
    ?assertEqual(Keys, Acc#sa.swept_keys),
    meck:unload().

%% Verify that even if one sweep crashes for all objects the following participants still run
sweep_delete_crash_observ_test() ->
    setup_sweep(Keys = 100),
    Sweeps = delete_sweep_crash() ++ observ_sweep(),
    {reply, Acc, _State} = do_sweep(Sweeps, 0, no_sender, [], no_index, fake_backend, [], []),
    [{_Pid,{_,_,[_,DeleteN]},ok}] = meck:history(delete_callback_module),
    [{_Pid,{_,_,[_,ObservAcc]},ok}] = meck:history(observ_callback_module),

    %% check that the delete sweep failed but observer succeed
    ?assertEqual(too_many_crashes, DeleteN),
    ?assertEqual({0, Keys}, ObservAcc),
    ?assertEqual(Keys, Acc#sa.swept_keys),
    meck:unload().

make_keys(Nr) ->
    [integer_to_binary(N) || N <- lists:seq(1, Nr)].

setup_sweep(N) ->
    meck:new(riak_kv_vnode, []),
    meck:expect(riak_kv_vnode, local_put, fun(_, _, _) -> [] end),
    meck:expect(riak_kv_vnode, local_reap, fun(_, _, _) -> [] end),
    meck:new(riak_core_bucket),
    meck:expect(riak_core_bucket, get_bucket, fun(_) -> [] end),
    [meck_callback_modules(Module) ||
       Module <- [delete_callback_module, modify_callback_module, observ_callback_module]],
    meck:new(fake_backend, [non_strict]),
    Keys = make_keys(N),
    BackendFun =
        fun(CompleteFoldReq, InitialAcc, _, _) ->
                lists:foldl(fun(NBin, Acc) ->
                                    InitialObj = riak_object:to_binary(v1, riak_object:new(NBin, NBin, <<>>)),
                                    CompleteFoldReq(NBin, NBin, InitialObj, Acc)
                            end, InitialAcc, Keys)
        end,
    meck:expect(fake_backend, fold_objects, BackendFun).

meck_callback_modules(Module) ->
    meck:new(Module, [non_strict]),
    meck:expect(Module, failed_sweep, fun(_Index, _Reason) -> ok end),
    meck:expect(Module, successfull_sweep, fun(_Index, _Reason) -> ok end).

rem_keys(BKey, N, MatchReturn, DefaultReturn) ->
    case binary_to_integer(BKey) rem N of
        0 ->
            MatchReturn;
        _ ->
            DefaultReturn
    end.

modify_sweep(N) ->
    ModifyFun =
        fun({{_Bucket, BKey}, _RObj}, Acc, _Opt) ->
                rem_keys(BKey, N, {mutated, <<"mutated">>, Acc + 1}, {ok, Acc})
        end,
    [#sweep_participant{module = modify_callback_module,
                        sweep_fun = ModifyFun,
                        fun_type = ?MODIFY_FUN,
                        acc = 0
                       }].

observ_sweep() ->
    %% Keep track on nr of modified and original objects
    ObservFun = fun({{_, _BKey}, <<"mutated">>}, {Mut, Org}, _Opt) -> {ok, {Mut + 1, Org}};
                   ({{_, _BKey}, _RObj}, {Mut, Org}, _Opt) -> {ok, {Mut, Org + 1}} end,
    [#sweep_participant{module = observ_callback_module,
                       sweep_fun = ObservFun,
                       fun_type = ?OBSERV_FUN,
                        acc = {0,0}
                       }].

delete_sweep(N) ->
    DeleteFun =
        fun({{_, BKey}, _RObj}, Acc, _Opt) ->
                rem_keys(BKey, N, {deleted, Acc + 1}, {ok, Acc})
        end,
    [#sweep_participant{module = delete_callback_module,
                       sweep_fun = DeleteFun,
                       fun_type = ?DELETE_FUN,
                        acc = 0
                       }].
%% delete sweep participant that ask for bucket props
delete_sweep_bucket_props() ->
    %% Check that we receive bucket_props when we ask for it
    DeleteFun = fun({_BKey, _RObj}, Acc, [{bucket_props, _}]) -> {deleted, Acc} end,
    [#sweep_participant{module = delete_callback_module,
                       sweep_fun = DeleteFun,
                       fun_type = ?DELETE_FUN,
                       options = [bucket_props]}].
%% Crashing delete sweep participant
delete_sweep_crash() ->
    DeleteFun = fun({_BKey, RObj}, _Acc, _Opt) -> RObj = crash end,
    [#sweep_participant{module = delete_callback_module,
                        sweep_fun = DeleteFun,
                        fun_type = ?DELETE_FUN,
                        options = [bucket_props]}].


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
                find_expired_participant(State2#state.sweeps, State2#state.sweep_participants),
            ?assertEqual(MissingPart#sweep.index, NotAllResult)
    end.

cleanup(_State) ->
    meck:unload().
-endif.

-ifdef(EQC).

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
