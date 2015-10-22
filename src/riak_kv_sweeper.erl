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
%% Once every tick the riak_kv_sweeper check if it's in the configured sweep_window
%% and find sweeps to run.
-module(riak_kv_sweeper).
-behaviour(gen_server).
-include("riak_kv_sweeper.hrl").
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% Default number of concurrent sweeps that are allowed to run.
-define(DEFAULS_SWEEP_CONCURRENCY,2).
-define(DEFAULT_SWEEP_TICK, 60 * 1000). %% 1/min



%% ====================================================================
%% API functions
%% ====================================================================
-export([start_link/0,
         add_sweep_participant/1,
         remove_sweep_participant/1,
         status/0,
         sweep/1,
         sweep_result/2,
         stop_all_sweeps/0,
         stop_sweeps/1]).

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
-spec sweep(atom()) -> ok.
sweep(Index) ->
    gen_server:call(?MODULE, {sweep, Index}).

%% @doc Get information about participants and all sweeps.
-spec status() -> {[#sweep_participant{}], [#sweep{}]}.
status() ->
    gen_server:call(?MODULE, status).

%% @doc Stop all running sweeps
-spec stop_all_sweeps() ->  true | false.
stop_all_sweeps() ->
    ok.
%% @doc Stop all sweeps with Module as a participant.
-spec stop_sweeps(atom()) -> true | false.
stop_sweeps(_Module) ->
    ok.

%% @private used by the sweeping process to report results when done.
sweep_result(Index, Result) ->
    gen_server:call(?MODULE, {sweep_result, Index, Result}).

%% ====================================================================
%% Behavioural functions
%% ====================================================================
-record(state, {sweep_participants = dict:new() :: dict:dict(),
                sweeps             = dict:new() :: dict:dict()
               }).

init([]) ->
    process_flag(trap_exit, true),
    schedule_sweep_tick(),
    {ok, #state{}}.

handle_call({add_sweep_participant, Participant}, _From, #state{sweep_participants = SP} = State) ->
    {reply, ok, State#state{sweep_participants = dict:store(Participant#sweep_participant.module, Participant, SP)}};

handle_call({remove_sweep_participant, Module}, _From, #state{sweep_participants = SP} = State) ->
    Reply = dict:is_key(Module, SP),
    SP1 = dict:erase(Module, SP),
    {reply, Reply, State#state{sweep_participants = SP1}};

handle_call({sweep, Index}, _From, State) ->
    State1 = do_sweep(Index, State),
    {reply, ok, State1};

handle_call(status, _From, State) ->
    {reply, {dict:to_list(State#state.sweep_participants), dict:to_list(State#state.sweeps)}, State};

handle_call(sweeps, _From, State) ->
    {reply, State#state.sweeps, State};

handle_call({sweep_result, Index, Result}, _From, State) ->
    {reply, ok, update_finished_sweep(Index, Result, State)}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'EXIT', Pid, Reason}, #state{sweeps = Sweeps} = State) ->
    SweepList = get_running_sweeps(Sweeps),
    case {lists:keyfind(Pid, #sweep.pid,SweepList), Reason} of
        {false, _} ->
            lager:error("Unknown proc ~p ~p", [Pid, Reason]),
            {noreply, State};
        {Sweep, normal} ->
            {noreply, State#state{sweeps = finish_sweep(Sweeps, Sweep)}};
        {Sweep, Reason} ->
            lager:error("Sweep crashed ~p ~p", [Sweep, Reason]),
            {noreply, State#state{sweeps = finish_sweep(Sweeps, Sweep)}}
    end;

handle_info(sweep_tick, State) ->
    schedule_sweep_tick(),
    State1 = maybe_initiate_sweeps(State),
    State2 = maybe_sweep(State1),
    {noreply, State2}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ====================================================================
%% Internal functions
%% ====================================================================

maybe_sweep(#state{sweeps = Sweeps } = State) ->
    case in_sweep_window() andalso not concurrency_limit_reached(Sweeps) of
        true ->
            maybe_schedule_sweep(State);
        false ->
            State
    end.

maybe_schedule_sweep(#state{sweeps = Sweeps,
                            sweep_participants = _Participant} = State) ->
    case get_never_runned_sweeps(Sweeps) of
        [] ->
            case find_oldes_sweep(Sweeps) of
%%             case find_expired_participant(Sweeps, Participant) of
                [] ->
                    State;
                Index ->
                    do_sweep(Index, State)
            end;
        Indices ->
            do_sweep(hd(Indices), State)
    end.

do_sweep(#sweep{index = Index}, State) ->
    do_sweep(Index, State);
do_sweep(Index, #state{sweep_participants = SP, sweeps = Sweeps} = State) ->
    case ask_participants(Index, SP) of
        [] ->
            State;
        ActiveParticipants ->
            Pid =
                spawn_link(fun() ->
                                   Result = riak_kv_vnode:sweep({Index, node()}, ActiveParticipants),
                                   lager:info("Sweep_result: ~p" ,[Result]),
                                   ?MODULE:sweep_result(Index, format_result(Result))
                           end),
            State#state{sweeps = start_sweep(Sweeps, Index, Pid, ActiveParticipants)}
    end.


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
    app_helper:get_env(riak_kv, sweep_concurrency, ?DEFAULS_SWEEP_CONCURRENCY).

-spec schedule_sweep_tick() -> ok.
schedule_sweep_tick() ->
    Tick = app_helper:get_env(riak_kv, sweep_tick, ?DEFAULT_SWEEP_TICK),
    erlang:send_after(Tick, ?MODULE, sweep_tick),
    ok.

%% @private
ask_participants(Index, Participants) ->
    Funs =
        [{Participant, (Participant#sweep_participant.module):participate_in_sweep(Index, self())} ||
         {_Module, Participant} <- dict:to_list(Participants)],
    
    %% Filter non active participants
    [Participant#sweep_participant{sweep_fun = Fun, initial_acc = InitialAcc} ||
      {Participant, {ok, Fun, InitialAcc}} <- Funs].

update_finished_sweep(Index, Result, #state{sweeps = Sweeps} = State) ->
    Sweep = dict:fetch(Index, Sweeps),
    Sweep1 = store_result(Result, Sweep),
    State#state{sweeps = dict:store(Index, Sweep1, Sweeps)}.

store_result(Result, #sweep{results = OldResult} = Sweep) ->
    TimeStamp = os:timestamp(),
    lager:info("~p ~p ", [OldResult, Result]),
    UpdatedResults =
        lists:foldl(fun({Mod, OutCome}, Dict) ->
                            dict:store(Mod, {TimeStamp, OutCome}, Dict)
                    end, OldResult, Result),
    Sweep#sweep{results = UpdatedResults, end_time = os:timestamp()}.

find_oldes_sweep(Sweeps) ->
    OldestSweep = 
        hd(lists:keysort(#sweep.start_time, get_idle_sweeps(Sweeps))),
    OldestSweep#sweep.index.

%%
%% find_expired_participant(Sweeps, Participants) ->
%%     ExpiredMissingSweeps = 
%%         [{expired_or_missing(Sweep, Participants), Sweep} || Sweep <- get_idle_sweeps(Sweeps)],
%%     MaxExpiredMissingSweep = hd(lists:reverse(lists:keysort(1, ExpiredMissingSweeps))),
%%     case MaxExpiredMissingSweep of
%%         {0, _} -> [];
%%         {_N, Sweep} -> Sweep
%%     end.
%% 
%% expired_or_missing(#sweep{results = Result}, Participants) ->
%%     Now = os:timestamp(),
%%     ResultList = dict:to_list(Result),
%%     Missing =
%%         [ Module || {Module, _}
%%                         <- dict:to_list(Participants), not lists:keymember(Module, 1, ResultList)],
%%     Expired =
%%         [ Mod || {Mod, {TS, _Outcome}} 
%%                      <- ResultList, expired(Now, TS,  Mod, Participants)],
%%     lager:info("Missing: ~p Expired: ~p", [Missing, Expired]),
%%     length(Missing) + length(Expired).
%% 
%% expired(Now, TS,  Mod, Participants) ->
%%     lager:info("expired ~p ~p", [elapsed_secs(Now, TS), get_run_interval(Mod, Participants)]),
%%     elapsed_secs(Now, TS) >= get_run_interval(Mod, Participants).
%% 
%% get_run_interval(Mod, Participants) ->
%%     #sweep_participant{run_interval = RunInterval} = dict:fetch(Mod, Participants),
%%     RunInterval.
%%
%% elapsed_secs(Now, Start) ->
%%     timer:now_diff(Now, Start) / 1000000.

format_result(#sa{active_p = Succ, failed_p = Failed}) ->
    format_result(succ, Succ) ++ format_result(fail, Failed).
format_result(SuccFail, Results) ->
    [{Module, SuccFail} || {Module, _Fun, _Acc, _Errors} <- Results].

finish_sweep(Sweeps, #sweep{index = Index}) ->
    dict:update(Index,
                fun(Sweep) ->
                        Sweep#sweep{state = idle, pid = undefind}
                end, Sweeps).

start_sweep(Sweeps, Index, Pid, ActiveParticipants) ->
    TS = os:timestamp(),
    dict:update(Index,
                fun(Sweep) ->
                        Sweep#sweep{state = running,
                                    pid = Pid,
                                    active_participants = ActiveParticipants,
                                    start_time = TS}
                end, Sweeps).

maybe_initiate_sweeps(#state{sweeps = Sweeps} = State) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Indices = riak_core_ring:my_indices(Ring),
    MissingIdx = [Idx || Idx <- Indices,
                         not dict:is_key(Idx, Sweeps)],
    Sweeps1 = add_sweeps(MissingIdx, Sweeps),
    State#state{sweeps = Sweeps1}.

add_sweeps(MissingIdx, Sweeps) ->
    lists:foldl(fun(Idx, SweepsDict) ->
                        dict:store(Idx, #sweep{index = Idx}, SweepsDict)
                end, Sweeps, MissingIdx).

get_running_sweeps(Sweeps) ->
    [Sweep || {_Index, #sweep{state = running} = Sweep} <- dict:to_list(Sweeps)].

get_idle_sweeps(Sweeps) ->
    [Sweep || {_Index, #sweep{state = idle} = Sweep} <- dict:to_list(Sweeps)].

get_never_runned_sweeps(Sweeps) ->
    [Sweep || {_Index, #sweep{state = idle, results = ResDict} = Sweep} <- dict:to_list(Sweeps), dict:is_empty(ResDict)].
