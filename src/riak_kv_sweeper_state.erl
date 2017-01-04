%% -------------------------------------------------------------------
%%
%% riak_kv_sweeper: Riak sweep scheduler
%%
%% Copyright (c) 2007-2016 Basho Technologies, Inc.  All Rights Reserved.
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
-module(riak_kv_sweeper_state).

-export([add_sweep_participant/6,
         get_active_participants/2,
         get_estimate_keys/2,
         get_run_interval/1,
         update_sweep_specs/1,
         maybe_schedule_sweep/1,
         new/0,
         remove_sweep_participant/2,
         handle_vnode_sweep_response/3,
         status/1,
         format_sweep/3,
         format_active_sweeps/2,
         stop_all_sweeps/1,
         sweep_request/2,
         update_finished_sweep/3,
         update_progress/3,
         update_started_sweep/4,
         in_sweep_window/0]).

%% For testing/debug use only:
-export([sweep_state/1,
         sweep_index/1,
         sweep_queue_time/1
        ]).

-export_type([state/0, sweep/0]).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_mocking.hrl").
-endif.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(DEFAULT_SWEEP_CONCURRENCY,1).

-define(IS_HOUR(H), (is_integer(H) andalso H >= 0 andalso H =< 23)).

-record(sweep,
        {
         index,
         state = idle :: idle | running | restart,
         pid :: pid() | undefined,
         results = dict:new(),
         active_participants,  %% Active in current run
         start_time :: erlang:timestamp(),
         end_time :: erlang:timestamp(),
         queue_time :: erlang:timestamp(),
         estimated_keys :: {non_neg_integer(), erlang:timestamp()},
         swept_keys :: non_neg_integer() | undefined
        }).

-record(state, {sweep_participants = dict:new() :: dict(),
                sweeps             = dict:new() :: dict()
               }).

%% ====================================================================
%% Types
%% ====================================================================
-type state() :: #state{}.
-type sweep() :: #sweep{}.

-type index() :: riak_kv_sweeper:index().

-type scheduler_event() :: {'request', index()} | 'tick'.
-type scheduler_sweep_window() :: 'never'
                                | 'always'
                                | {non_neg_integer(), non_neg_integer()}.

-type shedulder_result() ::
        {'ok', {'tick', #sweep{}}} |
        {'ok', {'tick', 'none'}} |
        {'ok', {'tick', 'disabled'}} |
        {'ok', {'tick', 'sweep_window_never'}} |
        {'ok', {'tick', 'concurrency_limit_reached'}} |
        {'ok', {'tick', 'not_in_sweep_window'}} |
        {'ok', {'request', #sweep{}}} |
        {'ok', {'request', {'restart', #sweep{}}}} |
        {'ok', {'request', 'restarting'}} |
        {'ok', {'request', 'queue'}} |
        {'ok', {'request', 'not_index'}}.

%% ====================================================================
%% API functions
%% ====================================================================
-spec new() -> state().
new() ->
    State =
        case get_persistent_participants() of
            undefined ->
                #state{};
            SP ->
                #state{sweep_participants = SP}
        end,
    update_sweep_specs(State).

-spec add_sweep_participant(state(), string(), module(), riak_kv_sweeper:fun_type(),
                            riak_kv_sweeper:run_interval(), [atom()]) -> state().
add_sweep_participant(State, Description, Module, FunType, RunInterval, Options) ->
    Participant = riak_kv_sweeper_fold:participant(Description, Module, FunType,
                                                   RunInterval, Options),
    #state{sweep_participants = SP} = State,
    SP1 = dict:store(Module, Participant, SP),
    persist_participants(SP1),
    State#state{sweep_participants = SP1}.

-spec remove_sweep_participant(state(), riak_kv_sweeper:participant_module()) ->
    {'ok', boolean(), state()}.
remove_sweep_participant(State, Module) ->
    #state{sweeps = Sweeps,
           sweep_participants = SP} = State,
    Removed = dict:is_key(Module, SP),
    SP1 = dict:erase(Module, SP),
    persist_participants(SP1),
    disable_sweep_participant_in_running_sweep(Module, Sweeps),
    State1 = State#state{sweep_participants = SP1},
    {ok, Removed, State1}.

-spec update_sweep_specs(state()) -> state().
update_sweep_specs(State) ->
    #state{sweeps = Sweeps} = State,
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Indices = riak_core_ring:my_indices(Ring),
    MissingIdx = [Idx || Idx <- Indices,
                         not dict:is_key(Idx, Sweeps)],
    Sweeps1 = add_sweeps(MissingIdx, Sweeps),

    NotOwnerIdx = [Index || {Index, _Sweep} <- dict:to_list(Sweeps),
                            not lists:member(Index, Indices)],
    Sweeps2 = remove_sweeps(NotOwnerIdx, Sweeps1),

    State#state{sweeps = Sweeps2}.

-spec status(state()) -> {'ok',
                          {Participants :: [riak_kv_sweeper_fold:participant()],
                           Sweeps :: [#sweep{}]},
                          state()}.
status(#state{sweep_participants = Participants0} = State) ->
    Participants =
        [Participant ||
            {_Mod, Participant} <- dict:to_list(Participants0)],
    Sweeps = [Sweep || {_Index, Sweep} <- dict:to_list(State#state.sweeps)],
    SortedSweeps = lists:keysort(#sweep.index, Sweeps),
    {ok, {Participants, SortedSweeps}, State}.

-spec format_sweep(sweep(), [{integer(), riak_kv_sweeper_fold:participant()}],
                   erlang:timestamp()) -> [{string(), term()}].
format_sweep(Sweep, IndexedParticipants, Now) ->
    StartTime = Sweep#sweep.start_time,
    EndTime = Sweep#sweep.end_time,
    LastSweep = format_timestamp(Now, StartTime),
    Duration = format_timestamp(EndTime, StartTime),
    ResultsColumns = format_results(Now, Sweep#sweep.results, IndexedParticipants),
    [{"Index", Sweep#sweep.index},
     {"Last sweep", LastSweep},
     {"Duration", Duration}
    ] ++ ResultsColumns.

format_results(Now, Results, IndexedParticipants) ->
    lists:flatten([get_result(Index, Participant, Results, Now)
                     || {Index, Participant} <- IndexedParticipants]).

get_result(Index, Participant, Results, Now) ->
    Module = riak_kv_sweeper_fold:participant_module(Participant),
    case dict:find(Module, Results) of
        {ok, {TimeStamp, Outcome}} when Outcome == succ orelse Outcome == fail ->
            LastRun = format_timestamp(Now, TimeStamp),
            OutcomeString = string:to_upper(atom_to_list(Outcome));
        _ ->
            LastRun = "-",
            OutcomeString = "-"
    end,
    IndexString = integer_to_list(Index),
    [{"Last run " ++ IndexString, LastRun},
     {"Result "   ++ IndexString, OutcomeString}].

format_timestamp(_Now, undefined) ->
    "--";
format_timestamp(undefined, _) ->
    "--";
format_timestamp(Now, TS) ->
    riak_core_format:human_time_fmt("~.1f", timer:now_diff(Now, TS)).

format_active_sweeps(Sweeps, IndexedParticipants) ->
    Rows = [format_progress(Sweep, IndexedParticipants)
              || #sweep{state = State} = Sweep <- Sweeps, State == running],
    case Rows of
        [] ->
            [];
        Rows ->
            Header = io_lib:format("~s", ["Running sweeps:"]),
            [clique_status:text(Header),
             clique_status:table(Rows)]
    end.

format_progress(#sweep{index = Index,
                       active_participants = AcitvePart,
                       estimated_keys = {EstimatedKeys, _TS},
                       swept_keys = SweptKeys},
                IndexedParticipants) ->
    EstimatedKeysFormat = format_estimated_keys(EstimatedKeys),
    [{"Index", Index},
     {"Swept keys", SweptKeys}] ++
    EstimatedKeysFormat ++
    riak_kv_sweeper_fold:format_active_participants(AcitvePart, IndexedParticipants);

format_progress(_, _) ->
    "".

format_estimated_keys(EstimatedKeys) when is_integer(EstimatedKeys), EstimatedKeys > 0 ->
    [{"Estimated Keys", EstimatedKeys}];
format_estimated_keys(_EstimatedKeys) ->
    [].

-spec send_to_sweep_worker('stop' | {'disable', riak_kv_sweeper:participant_module()},
                           #sweep{}) -> 'ok' | 'no_pid'.
send_to_sweep_worker(Msg, #sweep{pid = Pid}) when is_pid(Pid) ->
    lager:debug("Send to sweep worker ~p: ~p", [Pid, Msg]),
    Pid ! Msg,
    ok;
send_to_sweep_worker(Msg, #sweep{index = Index}) ->
    lager:info("no pid ~p to ~p " , [Msg, Index]),
    no_pid.

-spec stop_all_sweeps(state()) -> {'ok', Running :: non_neg_integer(), state()}.
stop_all_sweeps(State) ->
    #state{sweeps = Sweeps} = State,
    Running = [Sweep || Sweep <- get_running_sweeps(Sweeps)],
    [stop_sweep(Sweep) || Sweep <- Running],
    {ok, length(Running), State}.

-spec get_active_participants(state(), index()) ->
    {'ok', Participants :: [riak_kv_sweeper_fold:participant()], state()}.
get_active_participants(State, Index) ->
    Participants = State#state.sweep_participants,
    ActiveParticipants = riak_kv_sweeper_fold:get_active_participants(Participants, Index),
    {ok, ActiveParticipants, State}.

-spec get_estimate_keys(state(), index()) ->
    {'ok', {EstimatedKeys :: non_neg_integer(), erlang:timestamp()}, state()}
  | {'ok', 'undefined', state()}.
get_estimate_keys(State, Index) ->
    #state{sweeps = Sweeps} = State,
    #sweep{estimated_keys = OldEstimate} = dict:fetch(Index, Sweeps),
    {ok, OldEstimate, State}.

-spec update_finished_sweep(state(),
                            index(),
                            [riak_kv_sweeper_fold:sweep_result()]) -> state().
update_finished_sweep(State, Index, Result) ->
    #state{sweeps = Sweeps} = State,
    case dict:find(Index, Sweeps) of
        {ok, Sweep} ->
            Sweep1 = store_result(Result, Sweep),
            finish_sweep(Sweep1, State#state{sweeps = dict:store(Index, Sweep1, Sweeps)});
        _ ->
            State
    end.

-spec update_started_sweep(state(),
                           index(),
                           [riak_kv_sweeper_fold:participant()],
                           EstimatedKeys :: non_neg_integer()) -> state() .
update_started_sweep(State, Index, ActiveParticipants, EstimatedKeys) ->
    Sweeps = State#state.sweeps,
    SweepParticipants = State#state.sweep_participants,
    TS = os:timestamp(),
    Sweeps1 =
        dict:update(Index,
                    fun(Sweep) ->
                            %% We add information about participants that
                            %% where asked and said no So they will not be
                            %% asked again until they expire.
                            Results = add_asked_to_results(Sweep#sweep.results,
                                                           SweepParticipants),
                            Sweep#sweep{results = Results,
                                        estimated_keys = {EstimatedKeys, TS},
                                        active_participants = ActiveParticipants,
                                        start_time = TS,
                                        end_time = undefined}
                    end, Sweeps),
    State#state{sweeps = Sweeps1}.

-spec update_progress(state(), index(), SweptKeys :: integer()) -> state().
update_progress(State, Index, SweptKeys) ->
    #state{sweeps = Sweeps} = State,
    case dict:find(Index, Sweeps) of
        {ok, Sweep} ->
            Sweep1 = Sweep#sweep{swept_keys = SweptKeys},
            State#state{sweeps = dict:store(Index, Sweep1, Sweeps)};
        _ ->
            State
    end.

-spec maybe_schedule_sweep(state()) -> {'ok', index(), state()} | state().
maybe_schedule_sweep(State) ->
    Enabled = scheduler_enabled(),
    ConcurrenyLimit = get_concurrency_limit(),
    SweepWindow = sweep_window(),
    Now = os:timestamp(),

    Result =
        case schedule_sweep(_Event = tick,
                            Enabled,
                            SweepWindow,
                            ConcurrenyLimit, Now, State) of
            {ok, {tick, #sweep{} = Sweep}} ->
                {Sweep, State};
            {ok, {tick, _Other}} ->
                State
        end,
    case Result of
        {#sweep{index=Index}, State1} ->
            {ok, Index, State1};
        #state{} = State1 ->
            State1
    end.

-spec sweep_request(state(), index()) -> {'ok', index(), state()} | state().
sweep_request(State, Index) ->
    #state{sweeps = Sweeps} = State,
    ConcurrenyLimit = get_concurrency_limit(),

    Result =
        case schedule_sweep({request, Index},
                            _Enabled = true,
                            _SweepWindow = always,
                            ConcurrenyLimit, {0,0,0}, State) of
            {ok, {request, queue}} ->
                queue_sweep(Index, State);
            {ok, {request, {restart, Sweep}}} ->
                stop_sweep(Sweep),
                Sweeps1 = dict:store(Index,
                                     Sweep#sweep{state = restart}, Sweeps),
                State#state{sweeps = Sweeps1};
            {ok, {request, restarting}} ->
                State;
            {ok, {request, #sweep{} = Sweep}} ->
                {Sweep, State};
            {ok, {request, not_index}} ->
                State
        end,
    case Result of
        {#sweep{index=Index}, State1} ->
            {ok, Index, State1};
        #state{} = State1 ->
            State1
    end.

-spec handle_vnode_sweep_response(state(), index(), pid() | term()) ->
    {ok, state()} | {error, term(), state()}.
handle_vnode_sweep_response(State, Index, WorkerPid) when is_pid(WorkerPid) ->
    #state{ sweeps = Sweeps} = State,
    Sweeps1 =
        dict:update(Index,
                    fun(Sweep) ->
                            Sweep#sweep{state = running,
                                        pid = WorkerPid,
                                        queue_time = undefined}
                    end, Sweeps),
    {ok, State#state{sweeps = Sweeps1}};
handle_vnode_sweep_response(State, Index, Error) ->
    lager:warning("got error response ~p from vnode when requsting sweep on index ~p",
                  [Error, Index]),
    {error, Error, State}.

%% =============================================================================
%% Internal Functions
%% =============================================================================
-spec finish_sweep(#sweep{}, #state{}) -> #state{}.
finish_sweep(#sweep{index = Index}, #state{sweeps = Sweeps} = State) ->
    Sweeps1 =
        dict:update(Index,
                fun(Sweep) ->
                        Sweep#sweep{state = idle, pid = undefined}
                end, Sweeps),
    State#state{sweeps = Sweeps1}.

-spec store_result({SweeptKeys :: non_neg_integer(),
                    Result :: [riak_kv_sweeper_fold:sweep_result()]},
                   #sweep{}) -> #sweep{}.
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

-spec add_asked_to_results(Results :: dict(),
                           SweepParticipants :: dict()) -> Results :: dict().
add_asked_to_results(Results, SweepParticipants) ->
    ResultList = dict:to_list(Results),
    MissingResults = riak_kv_sweeper_fold:missing(module, SweepParticipants, ResultList),
    TimeStamp = os:timestamp(),
    lists:foldl(fun(Mod, Dict) ->
                        dict:store(Mod, {TimeStamp, asked}, Dict)
                end, Results, MissingResults).

expired_counts(Now, ResultsList, Participants) ->
    [begin
         RunInterval = run_interval(Mod, Participants),
         expired(Now, TS, RunInterval)
     end ||
     {Mod, {TS, _Outcome}} <- ResultsList].

-spec get_run_interval(RunInterval :: non_neg_integer()
                       | riak_kv_sweeper:run_interval_fun()) ->
    RunInterval :: non_neg_integer().
get_run_interval(RunIntervalFun) when is_function(RunIntervalFun) ->
    RunIntervalFun();
get_run_interval(RunInterval) ->
    RunInterval.

-spec schedule_sweep(Event            :: scheduler_event(),
                     Enabled          :: boolean(),
                     SweepWindow      :: scheduler_sweep_window(),
                     ConcurrencyLimit :: non_neg_integer(),
                     Now              :: erlang:timestamp(),
                     State            :: #state{}) -> shedulder_result().

schedule_sweep(tick, _Enabled = false, _SweepWindow,
               _ConcurrencyLimit, _Now, _State) ->
    {ok, {tick, disabled}};

schedule_sweep(tick, _Enabled = true, _SweepWindow = never,
               _ConcurrencyLimit, _Now, _State) ->
    {ok, {tick, sweep_window_never}};

schedule_sweep(Event = tick, Enabled = true, _SweepWindow = always,
               ConcurrencyLimit, Now, State) ->
    schedule_sweep(Event, Enabled, {0, 23},
                   ConcurrencyLimit, Now, State);

schedule_sweep(tick, _Enabled = true, SweepWindow,
               ConcurrencyLimit, Now, State) ->
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

schedule_sweep({request, Index}, _Enabled, _SweepWindow,
               ConcurrencyLimit, _Now, State) ->
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

-spec in_sweep_window() -> boolean().
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

-spec schedule_sweep2(Now          :: erlang:timestamp(),
                      Participants :: dict(),
                      Sweeps       :: dict()) ->
    {'ok', {'tick', 'none'}}|
    {'ok', {'tick', #sweep{}}}.
schedule_sweep2(Now, Participants, Sweeps) ->
    case get_never_runned_sweeps(Sweeps) of
        [] ->
            case get_queued_sweeps(Sweeps) of
                [] ->
                    case find_expired_participant(Now, Sweeps, Participants) of
                        not_found ->
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

-spec get_idle_sweeps(dict()) -> [#sweep{}].
get_idle_sweeps(Sweeps) ->
    [Sweep || {_Index, #sweep{state = idle} = Sweep} <- dict:to_list(Sweeps)].

-spec get_never_runned_sweeps(dict()) -> [#sweep{}].
get_never_runned_sweeps(Sweeps) ->
    [Sweep || {_Index, #sweep{state = idle, results = ResDict} = Sweep}
                  <- dict:to_list(Sweeps), dict:size(ResDict) == 0].

-spec get_queued_sweeps(dict()) -> [#sweep{}].
get_queued_sweeps(Sweeps) ->
    QueuedSweeps =
        [Sweep ||
         {_Index, #sweep{queue_time = QueueTime} = Sweep} <- dict:to_list(Sweeps),
         not (QueueTime == undefined)],
    lists:keysort(#sweep.queue_time, QueuedSweeps).

-spec find_expired_participant(erlang:timestamp(), dict(), dict()) -> not_found | #sweep{}.
find_expired_participant(Now, Sweeps, Participants) ->
    ExpiredMissingSweeps =
        [{expired_or_missing(Now, Sweep, Participants), Sweep} ||
         Sweep <- get_idle_sweeps(Sweeps)],
    case ExpiredMissingSweeps of
        [] ->
            not_found;
        _ ->
            MostExpiredMissingSweep =
                hd(lists:reverse(lists:keysort(1, ExpiredMissingSweeps))),
            case MostExpiredMissingSweep of
                %% Non of the sweeps have a expired or missing participant.
                {{0,0}, _} -> not_found;
                {_N, Sweep} -> Sweep
            end
    end.

-spec expired_or_missing(erlang:timestamp(),
                         #sweep{},
                         dict()) ->
    {MissingSum :: non_neg_integer(),
     ExpiredSum :: non_neg_integer()}.
expired_or_missing(Now, #sweep{results = Results}, Participants) ->
    ResultsList = dict:to_list(Results),
    Missing = riak_kv_sweeper_fold:missing(run_interval, Participants, ResultsList),
    Expired = expired_counts(Now, ResultsList, Participants),
    MissingSum = lists:sum(Missing),
    ExpiredSum = lists:sum(Expired),
    {MissingSum, ExpiredSum}.

-spec expired(erlang:timestamp(),
              erlang:timestamp(),
              RunInterval :: non_neg_integer() | disabled) ->
    non_neg_integer().
expired(_Now, _TS, disabled) ->
    0;
expired(Now, TS, RunInterval) ->
    max(0, elapsed_secs(Now, TS) - RunInterval).

-spec run_interval(riak_kv_sweeper:participant_module(),
                   Participants :: dict()) ->
    RunInterval :: non_neg_integer() | 'disabled'.
run_interval(Mod, Participants) ->
    case dict:find(Mod, Participants) of
        {ok, P} ->
            RunInterval = riak_kv_sweeper_fold:participant_run_interval(P),
            get_run_interval(RunInterval);
        _ ->
            %% Participant have been disabled since last run.
            %% TODO: should we remove inactive results?
            disabled
    end.

-spec elapsed_secs(erlang:timestamp(), erlang:timestamp()) ->
    non_neg_integer().
elapsed_secs(Now, Start) ->
    timer:now_diff(Now, Start) div 1000000.

-spec random_sweep([#sweep{}]) -> #sweep{}.
random_sweep(Sweeps) ->
    Index = random:uniform(length(Sweeps)),
    lists:nth(Index, Sweeps).

-spec scheduler_enabled() -> boolean().
scheduler_enabled() ->
    case app_helper:get_env(riak_kv, sweeper_scheduler, true) of
        E when is_boolean(E) ->
            E;
        _ ->
            false
    end.

-spec sweep_window() ->
    'always'
    | 'never'
    | {StartHour :: non_neg_integer(), EndHour :: non_neg_integer()}.
sweep_window() ->
    case application:get_env(riak_kv, sweep_window) of
        {ok, always} ->
            always;
        {ok, never} ->
            never;
        {ok, {StartHour, EndHour}} when ?IS_HOUR(StartHour), ?IS_HOUR(EndHour) ->
            {StartHour, EndHour};
        Other ->
            lager:warning("Invalid riak_kv_sweep window specified: ~p. "
                          "Defaulting to 'never'.", [Other]),
            never
    end.

-spec get_concurrency_limit() -> integer().
get_concurrency_limit() ->
    case app_helper:get_env(riak_kv, sweep_concurrency, ?DEFAULT_SWEEP_CONCURRENCY) of
        Value when is_integer(Value) ->
            Value;
        Other ->
            lager:warning("Invalid value ~p for sweep_concurrency. "
                          "Defaulting to ~p.", [Other, ?DEFAULT_SWEEP_CONCURRENCY]),
            ?DEFAULT_SWEEP_CONCURRENCY
    end.

-spec queue_sweep(riak_kv_sweeper:index(), #state{}) -> #state{}.
queue_sweep(Index, #state{sweeps = Sweeps} = State) ->
    case dict:fetch(Index, Sweeps) of
        #sweep{queue_time = undefined} = Sweep ->
            Sweeps1 =
                dict:store(Index,
                           Sweep#sweep{queue_time = os:timestamp()},
                           Sweeps),
            State#state{sweeps = Sweeps1};
        _ ->
            State
    end.

-spec stop_sweep(#sweep{}) -> ok.
stop_sweep(Sweep) ->
    send_to_sweep_worker(stop, Sweep),
    ok.

-spec get_running_sweeps(dict()) -> [#sweep{}].
get_running_sweeps(Sweeps) ->
    [Sweep ||
      {_Index, #sweep{state = State} = Sweep} <- dict:to_list(Sweeps),
      State == running orelse State == restart].

-spec disable_sweep_participant_in_running_sweep(riak_kv_sweeper:participant_module(),
                                                 Sweeps :: dict()) -> ok.
disable_sweep_participant_in_running_sweep(Module, Sweeps) ->
    [disable_participant(Sweep, Module) ||
       #sweep{active_participants = ActiveP} = Sweep <- get_running_sweeps(Sweeps),
        riak_kv_sweeper_fold:is_participant_in_list(Module, ActiveP)],
    ok.

-spec disable_participant(#sweep{},
                          riak_kv_sweeper:participant_module()) -> ok.
disable_participant(Sweep, Module) ->
    send_to_sweep_worker({disable, Module}, Sweep),
    ok.

-spec persist_participants(Participants :: dict()) -> ok.
persist_participants(Participants) ->
    application:set_env(riak_kv, sweep_participants, Participants).

-spec add_sweeps([riak_kv_sweeper:index()],
                 Sweeps :: dict()) -> Sweeps :: dict().
add_sweeps(MissingIdx, Sweeps) ->
    lists:foldl(fun(Idx, SweepsDict) ->
                        dict:store(Idx, #sweep{index = Idx}, SweepsDict)
                end, Sweeps, MissingIdx).

-spec remove_sweeps([riak_kv_sweeper:index()], Sweeps :: dict()) -> Sweeps :: dict().
remove_sweeps(NotOwnerIdx, Sweeps) ->
    lists:foldl(fun(Idx, SweepsDict) ->
                        dict:erase(Idx, SweepsDict)
                end, Sweeps, NotOwnerIdx).

-spec get_persistent_participants() -> SweepParticipants :: dict() | undefined.
get_persistent_participants() ->
    app_helper:get_env(riak_kv, sweep_participants).

%% For testing/debug use only:
sweep_state(#sweep{state = State}) -> State.
sweep_index(#sweep{index = Index}) -> Index.
sweep_queue_time(#sweep{queue_time = QueueTime}) -> QueueTime.

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
    State = update_sweep_specs(#state{}),
    State1 = add_test_sweep_participant(State, Participants),
    {MyRingPart, Participants, State1}.

add_test_sweep_participant(StateIn, Participants) ->
    Fun = fun(N, State) ->
                  AddParticipantMsg = test_sweep_participant(N),
                  {reply, ok, StateOut} =
                      riak_kv_sweeper:handle_call(AddParticipantMsg, nobody, State),
                  StateOut
          end,
    lists:foldl(Fun, StateIn, Participants).

test_sweep_participant(N) ->
    Module = get_module(N),
    meck:new(Module, [non_strict]),
    meck:expect(Module, participate_in_sweep, fun(_, _) -> {ok, sweepfun, acc} end),
    {add_sweep_participant, "Dummy test participant", Module, observe_fun, N, []}.

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
                                    update_finished_sweep(AccState, Index, {0, Result})
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
                                    update_finished_sweep(AccState, Index, {0, Result})
                            end, State, Rest),

            Result2 = [{get_module(Part), succ} || Part <- tl(Participants)],
            State2 = update_finished_sweep(State1, NotAllResult, {0, Result2}),
            ?assertEqual([], get_never_runned_sweeps(State2#state.sweeps)),
            MissingPart = find_expired_participant(os:timestamp(), State2#state.sweeps,
                                                   State2#state.sweep_participants),
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
    riak_kv_sweeper_fold:participant("EQC sweep participant", Module,
                                     oneof([delete_fun, modify_fun, observe_fun]),
                                     oneof([60, 3600]), []).

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
                        Mod = riak_kv_sweeper_fold:participant_module(SP),
                        dict:store(Mod, SP, D)
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
                case schedule_sweep(Event, Enabled, SweepWindow, ConcurrencyLimit, Now, State) of
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
                case schedule_sweep(Event, Enabled, SweepWindow, ConcurrencyLimit, Now, State) of
                    {ok, {tick, none}} ->
                        length(NeverRunSweeps) == 0 andalso
                            length (QueuedSweeps) == 0 andalso
                            ExpiredSweep == not_found;
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
                            {[], [], not_found} ->
                                false ;
                            {[], [], ExpiredSweep} ->
                                is_sweep_index_in(Sweep, [ExpiredSweep]);
                            {[], QueuedSweeps, _} ->
                                is_sweep_index_in(Sweep, QueuedSweeps);
                            {NeverRunSweeps, [], _} ->
                                is_sweep_index_in(Sweep, NeverRunSweeps);
                            {NeverRunSweeps, QueuedSweeps, not_found} ->
                                is_sweep_index_in(Sweep, NeverRunSweeps);
                            {NeverRunSweeps, QueuedSweeps, ExpiredSweep} ->
                                is_sweep_index_in(Sweep, NeverRunSweeps)
                        end
                end
            end).

prop_schedule_sweep_test_() ->
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
