%% -------------------------------------------------------------------
%%
%% riak_kv_sweeper_fold: Riak sweep scheduler callbacks
%%
%% Copyright (c) 2016 Basho Technologies, Inc.  All Rights Reserved.
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
-module(riak_kv_sweeper_fold).

-export([get_active_participants/2,
         do_sweep/8
        ]).

-export([get_sweep_throttle/0,
         missing/3
        ]).

-export([participant/5,
         participant_module/1,
         participant_run_interval/1,
         is_participant_in_list/2,
         format_active_participants/2,
         format_sweep_participant/2
        ]).

-export_type([sweep_result/0,
              participant/0
             ]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(MAX_SWEEP_CRASHES, 10).
%% Throttle used when sweeping over K/V data: {Type, Limit, Wait}.
%% Type can be pace or obj_size.Default: 1 MB limit / 100 ms wait
-define(DEFAULT_SWEEP_THROTTLE, {obj_size, 1000000, 100}).
%% Default value for how much faster the sweeper throttle should be
%% during sweep window.
-define(SWEEP_WINDOW_THROTTLE_DIV, 1).

-record(sweep_participant,
        {
         description :: string(),   %% Human readeble description of the user.
         module :: atom(),          %% module where the sweep call back lives.
         fun_type :: riak_kv_sweeper:fun_type(), %% delete_fun | modify_fun | observe_fun
         sweep_fun :: fun(),        %%
         run_interval :: integer() | fun(), %% Defines how often participant wants to run.
         acc :: any(),
         options = [] :: list(),    %% optional values that will be added during sweep
         errors = 0 :: integer(),
         fail_reason
        }).

%% Sweep accumulator
-record(sa,
        {index,
         bucket_props = dict:new(),
         active_p,
         failed_p = [],
         succ_p = [],
         estimated_keys = 0  :: non_neg_integer(),
         swept_keys = 0  :: non_neg_integer(),
         num_mutated = 0 :: non_neg_integer(),
         num_deleted = 0 :: non_neg_integer(),
         throttle = riak_kv_sweeper_fold:get_sweep_throttle(),
         total_obj_size = 0,

         %% Stats counters to track and report metrics about a sweep
         stat_mutated_objects_counter = 0 :: non_neg_integer(),
         stat_deleted_objects_counter = 0 :: non_neg_integer(),
         stat_swept_keys_counter = 0 :: non_neg_integer(),
         stat_obj_size_counter = 0 :: non_neg_integer()
        }).

%% Short-lived accumulator used for applying participant funs
-record(apply_acc, {
          obj :: deleted | riak_object:riak_object(),
          failed = [] :: [#sweep_participant{}],
          successful = [] :: [#sweep_participant{}],
          num_mutated = 0 :: non_neg_integer(),
          num_deleted = 0 :: non_neg_integer(),
          bucket_props :: dict()
}).

%% ====================================================================
%% Types
%% ====================================================================
-type opaque() :: any(). % Opaque types should not be inspected

-type participant() :: #sweep_participant{}.

-type sweep_result() :: {SweptKeys :: non_neg_integer(),
                         [{riak_kv_sweeper:participant_module(),
                           'succ' | 'fail'}]}.

%% ====================================================================
%% API And callbacks
%% ====================================================================
-spec get_active_participants(dict(), riak_kv_sweeper:index()) -> [participant()].
get_active_participants(Participants, Index) ->
    Funs = [{Participant, Module:participate_in_sweep(Index, self())} ||
            {Module, Participant} <- dict:to_list(Participants)],

    %% Filter non-active participants
    [Participant#sweep_participant{sweep_fun = Fun, acc = InitialAcc} ||
     {Participant, {ok, Fun, InitialAcc}} <- Funs].

-spec do_sweep([ActiveParticipants :: #sweep_participant{}],
               EstimatedKeys       :: non_neg_integer(),
               Sender              :: opaque(),
               Opts                :: [],
               Index               :: riak_kv_sweeper:index(),
               Mod                 :: atom(),
               ModState            :: opaque(),
               VnodeState          :: opaque()) ->
    {reply, Acc :: #sa{}, VnodeState :: opaque()}
  | {reply, Reason :: opaque(), VnodeState :: opaque()}
  | {async, {sweep,
             Work :: opaque(),
             FinishFun :: fun((#sa{}) -> #sa{})},
     Sender :: opaque(),
     VnodeState :: opaque()}.

do_sweep(ActiveParticipants, EstimatedKeys, Sender, Opts, Index,
         Mod, ModState, VnodeState) ->
    InitialAcc = make_initial_acc(Index, ActiveParticipants, EstimatedKeys),
    case Mod:fold_objects(fun fold_req_fun/4, InitialAcc, Opts, ModState) of
        {ok, #sa{} = Acc} ->
            FinishFun = finish_sweep_fun(Index),
            {reply, FinishFun(Acc), VnodeState};
        {async, Work} ->
            FinishFun = finish_sweep_fun( Index),
            {async, {sweep, Work, FinishFun}, Sender, VnodeState};
        Reason ->
            failed_sweep(ActiveParticipants, Index, Reason),
            {reply, Reason, VnodeState}
    end.

-spec finish_sweep_fun(Index :: non_neg_integer()) -> fun((#sa{})-> #sa{}).
finish_sweep_fun(Index) ->
    fun(Acc) ->
            inform_participants(Acc, Index),
            riak_kv_sweeper:sweep_result(Index, format_result(Acc)),
            stat_send(Index, {final, Acc})
    end.

-spec is_receive_request(SweptKeys :: integer()) -> boolean().
is_receive_request(SweptKeys) ->
    (SweptKeys rem 1000) == 0.

-spec fold_req_fun(riak_object:bucket(), riak_object:key(), RObjBin :: binary(),
                   #sa{}) -> #sa{} | no_return().
fold_req_fun(_Bucket, _Key, _RObjBin,
             #sa{active_p = [], failed_p = Failed} = Acc) when Failed =/= [] ->
    lager:error("No more participants in sweep of Index ~p Failed: ~p",
                [Acc#sa.index, Failed]),
    throw({stop_sweep, Acc});

fold_req_fun(Bucket, Key, RObjBin,
             #sa{index = Index, swept_keys = SweptKeys} = Acc) ->
    Acc1 = maybe_throttle_sweep(RObjBin, Acc),
    Acc3 =
        case is_receive_request(SweptKeys) of
            true ->
                riak_kv_sweeper:update_progress(Index, SweptKeys),
                Acc2 = maybe_receive_request(Acc1),
                stat_send(Index, {in_progress, Acc2});
            false ->
                Acc1
        end,
    RObj = riak_object:from_binary(Bucket, Key, RObjBin),
    {NumMutated, NumDeleted, Acc4} =
        apply_sweep_participant_funs({{Bucket, Key}, RObj},
                                     Acc3#sa{swept_keys = SweptKeys + 1}),
    update_stat_counters(NumMutated, NumDeleted, byte_size(RObjBin), Acc4).

-spec make_initial_acc(riak_kv_sweeper:index(),
                       [#sweep_participant{}],
                       EstimatedKeys :: non_neg_integer()) -> #sa{}.
make_initial_acc(Index, ActiveParticipants, EstiamatedKeys) ->
    #sa{index = Index,
        active_p = sort_participants(ActiveParticipants),
        estimated_keys = EstiamatedKeys}.

-spec sort_participants([#sweep_participant{}]) ->
    [#sweep_participant{}].
sort_participants(Participants) ->
    lists:sort(fun compare_participants/2, Participants).

-spec compare_participants(#sweep_participant{},
                           #sweep_participant{}) -> boolean().
compare_participants(A, B) ->
    TypeA = A#sweep_participant.fun_type,
    TypeB = B#sweep_participant.fun_type,
    fun_type_rank(TypeA) =< fun_type_rank(TypeB).

-spec fun_type_rank(riak_kv_sweeper:fun_type()) -> 1 | 2 | 3.
fun_type_rank(delete_fun) -> 1;
fun_type_rank(modify_fun) -> 2;
fun_type_rank(observe_fun) -> 3.

-spec inform_participants(#sa{}, riak_kv_sweeper:index()) -> ok.
inform_participants(#sa{active_p = Succ, failed_p = Failed}, Index) ->
    successful_sweep(Succ, Index),
    failed_sweep(Failed, Index).

-spec successful_sweep([#sweep_participant{}], riak_kv_sweeper:index()) -> ok.
successful_sweep(Succ, Index) ->
    SuccessFun = fun(#sweep_participant{module = Module, acc = FinalAcc}) ->
                         Module:successful_sweep(Index, FinalAcc)
                 end,
    lists:foreach(SuccessFun, Succ).

-spec failed_sweep([#sweep_participant{}], riak_kv_sweeper:index()) -> ok.
failed_sweep(Failed, Index) ->
    FailFun = fun(#sweep_participant{module = Module, fail_reason = Reason, acc = Acc}) ->
                      Module:failed_sweep(Index, Acc, Reason)
              end,
    lists:foreach(FailFun, Failed).

failed_sweep(Failed, Index, Reason) ->
    FailFun = fun(#sweep_participant{module = Module, acc = Acc}) ->
                      Module:failed_sweep(Index, Acc, Reason)
              end,
    lists:foreach(FailFun, Failed).

-spec format_result(#sa{}) -> sweep_result().
format_result(#sa{swept_keys = SweptKeys,
                  active_p = Succ, failed_p = Failed}) ->
    {SweptKeys,
     format_result(succ, Succ) ++ format_result(fail, Failed)}.

-spec format_result('succ'|'fail', [#sweep_participant{}]) ->
    [{riak_kv_sweeper:participant_module(), 'succ' | 'fail'}].
format_result(SuccFail, Results) ->
    [{Module, SuccFail} || #sweep_participant{module = Module} <- Results].

%% Throttle depending on swept keys.
-spec maybe_throttle_sweep(binary(), #sa{}) -> #sa{}.
maybe_throttle_sweep(_RObjBin, #sa{throttle = {pace, Limit, Wait}, swept_keys = SweepKeys} = Acc)
  when SweepKeys rem Limit =:= 0 ->
    Acc1 = Acc#sa{throttle = get_sweep_throttle()},
    riak_kv_sweeper:sleep_for_throttle(Wait),
    maybe_extra_throttle(Acc1);
maybe_throttle_sweep(_RObjBin, #sa{throttle = {pace, _, _}} = Acc) ->
    maybe_extra_throttle(Acc);
maybe_throttle_sweep(RObjBin, #sa{throttle = {obj_size, Limit, _}} = Acc) when Limit =:= 0 ->
    ObjSize = byte_size(RObjBin),
    TotalObjSize1 = ObjSize + Acc#sa.total_obj_size,
    maybe_extra_throttle(Acc#sa{total_obj_size = TotalObjSize1});
%% Throttle depending on total obj_size.
maybe_throttle_sweep(RObjBin, #sa{throttle = {obj_size, _, _}} = Acc0) ->
    #sa{throttle = {obj_size, Limit, Wait},
        total_obj_size = TotalObjSize} = Acc0,
    ObjSize = byte_size(RObjBin),
    TotalObjSize1 = ObjSize + TotalObjSize,
    case TotalObjSize1 > Limit of
        true ->
            Acc1 = Acc0#sa{throttle = get_sweep_throttle()},
            riak_kv_sweeper:sleep_for_throttle(Wait),
            maybe_extra_throttle(Acc1#sa{total_obj_size = 0});
        false ->
            maybe_extra_throttle(Acc0#sa{total_obj_size = TotalObjSize1})
    end.

%% Throttle depending on how many objects the sweep modify.
-spec maybe_extra_throttle(#sa{}) -> #sa{}.
maybe_extra_throttle(Acc) ->
    #sa{throttle = Throttle,
        num_mutated = NumMutated,
        num_deleted = NumDeleted} = Acc,
    {Limit, Wait} = get_throttle_params(Throttle),

    %% +1 since some sweeps doesn't modify any objects
    case (NumMutated + NumDeleted + 1) rem Limit of
        0 ->
            riak_kv_sweeper:sleep_for_throttle(Wait);
        _ ->
            ok
    end,
    Acc.

get_throttle_params({pace, Limit, Wait}) ->
    {Limit, Wait};
get_throttle_params({obj_size, _Limit, Wait}) ->
    {100, Wait}.

%% Throttle depending on how many objects the sweep modify.
-spec get_sweep_throttle() -> {'obj_size'|'pace',
                               Limit :: non_neg_integer(),
                               Sleep :: non_neg_integer()}.
get_sweep_throttle() ->
    {Type, Limit, Sleep} = app_helper:get_env(riak_kv, sweep_throttle, ?DEFAULT_SWEEP_THROTTLE),
    AdjustedSleep = adjust_sleep(Sleep),
    {Type, Limit, AdjustedSleep}.

adjust_sleep(Sleep) ->
    case riak_kv_sweeper:in_sweep_window() of
        true ->
            Div = app_helper:get_env(riak_kv, sweep_window_throttle_div,
                                     ?SWEEP_WINDOW_THROTTLE_DIV),
            Sleep div Div;
        false ->
            Sleep
    end.

-spec maybe_receive_request(#sa{}) -> #sa{}.
maybe_receive_request(#sa{active_p = Active, failed_p = Fail } = Acc) ->
    receive
        stop ->
            Active1 = set_fail_reason_for_participants(sweep_stop, Active),
            Acc#sa{active_p = [], failed_p = Active1 ++ Fail};
        {disable, Module} ->
            maybe_disable_participant_for_module(Module, Acc)
    after 0 ->
        Acc
    end.

set_fail_reason_for_participants(Reason, Participants) ->
    [SP#sweep_participant{fail_reason = Reason} || SP <- Participants].

maybe_disable_participant_for_module(Module, Acc) ->
    #sa{active_p = Active, failed_p = Failed} = Acc,
    case lists:keytake(Module, #sweep_participant.module, Active) of
        {value, SP, Active1} ->
            Acc#sa{active_p = Active1,
                   failed_p = [SP#sweep_participant{fail_reason = disable} | Failed]};
        _ ->
            Acc
    end.

-spec apply_sweep_participant_funs({{riak_object:bucket(),
                                     riak_object:key()},
                                    riak_object:riak_object()}, #sa{}) ->
    {NumMutated :: non_neg_integer(), NumDeleted :: non_neg_integer(), #sa{}}.
apply_sweep_participant_funs({BKey, RObj}, SweepAcc) ->
    ActiveParticipants = SweepAcc#sa.active_p,
    FailedParticipants = SweepAcc#sa.failed_p,
    Index = SweepAcc#sa.index,
    BucketProps = SweepAcc#sa.bucket_props,

    InitialAcc = #apply_acc{
                    obj = RObj,
                    failed = FailedParticipants,
                    bucket_props = BucketProps},

    FoldFun = fun(E, Acc) -> run_participant_fun(E, Index, BKey, Acc) end,
    Results = lists:foldl(FoldFun, InitialAcc, ActiveParticipants),

    #apply_acc{
       failed = Failed,
       successful = Successful,
       num_mutated = NumMutated,
       num_deleted = NumDeleted,
       bucket_props = NewBucketProps
      } = Results,

    SweepAcc1 = update_modified_objs_count(SweepAcc, NumMutated, NumDeleted),
    SweepAcc2 = SweepAcc1#sa{bucket_props = NewBucketProps},
    SweepAcc3 = update_sa_with_participant_results(SweepAcc2, Failed, Successful),
    {NumMutated, NumDeleted, SweepAcc3}.



%% Check if the sweep_participant have reached crash limit.
-spec run_participant_fun(#sweep_participant{},
                          riak_kv_sweeper:index(),
                          {riak_object:bucket(), riak_object:key()},
                          #apply_acc{}) -> #apply_acc{}.
run_participant_fun(SP = #sweep_participant{errors = ?MAX_SWEEP_CRASHES,
                                            module = Module},
                    _Index, _BKey, Acc) ->
    lager:error("Sweeper fun ~p crashed too many times.", [Module]),
    PrevFailed = Acc#apply_acc.failed,
    FailedSP = SP#sweep_participant{fail_reason = too_many_crashes},
    Acc#apply_acc{failed = [FailedSP | PrevFailed]};

run_participant_fun(SP, _Index, _BKey, Acc = #apply_acc{obj = deleted}) ->
    PrevSuccessful = Acc#apply_acc.successful,
    Acc#apply_acc{successful = [SP | PrevSuccessful]};

run_participant_fun(SP, Index, BKey, Acc) ->
    Fun = SP#sweep_participant.sweep_fun,
    ParticipantAcc = SP#sweep_participant.acc,
    Errors = SP#sweep_participant.errors,
    Options = SP#sweep_participant.options,

    RObj = Acc#apply_acc.obj,
    Successful = Acc#apply_acc.successful,
    NumMutated = Acc#apply_acc.num_mutated,
    NumDeleted = Acc#apply_acc.num_deleted,

    {Bucket, _Key} = BKey,
    {OptValues, NewBucketProps} =
        maybe_add_bucket_info(Bucket, Acc#apply_acc.bucket_props, Options),

    try Fun({BKey, RObj}, ParticipantAcc, OptValues) of
        Result ->
            {NewRObj, Mutated, Deleted, NewParticipantAcc} =
                do_participant_side_effects(Result, Index, BKey, RObj),
            NewSucc = [SP#sweep_participant{acc = NewParticipantAcc} | Successful],
            Acc#apply_acc{
              obj = NewRObj,
              successful = NewSucc,
              num_mutated = NumMutated + Mutated,
              num_deleted = NumDeleted + Deleted,
              bucket_props = NewBucketProps
             }
    catch C:T ->
            lager:error("Sweeper fun crashed ~p ~p Key: ~p Obj: ~p", [{C, T}, SP, BKey, RObj]),
            %% We keep the sweep in succ unil we have enough crashes
            NewSucc = [SP#sweep_participant{errors = Errors + 1} | Successful],
            Acc#apply_acc{successful = NewSucc}
    end.

-spec do_participant_side_effects({'mutated', riak_object:riak_object(), #sa{}}
                                  | {'ok' | 'deleted', #sa{}},
                                  riak_kv_sweeper:index(),
                                  {riak_object:bucket(), riak_object:key()},
                                  riak_object:riak_object()) ->
    {'deleted'
     | riak_object:riak_object(),
     Modified :: non_neg_integer(),
     Deleted :: non_neg_integer(),
     #sa{}}.
do_participant_side_effects({deleted, NewAcc},
                            Index, BKey, RObj) ->
    riak_kv_vnode:local_reap(Index, BKey, RObj),
    {deleted, 0, 1, NewAcc};
do_participant_side_effects({mutated, MutatedRObj, NewAcc},
                            Index, _BKey, _RObj) ->
    riak_kv_vnode:local_put(Index,
                            MutatedRObj,
                            [{hashtree_action, tombstone}]),
    {MutatedRObj, 1, 0, NewAcc};
do_participant_side_effects({ok, NewAcc}, _Index, _BKey, RObj) ->
    {RObj, 0, 0, NewAcc}.

-spec update_modified_objs_count(#sa{},
                                 NumMutated :: non_neg_integer(),
                                 NumDeleted :: non_neg_integer()) -> #sa{}.
update_modified_objs_count(SweepAcc, NumMutated, NumDeleted) ->
    Mutated0 = SweepAcc#sa.num_mutated,
    Deleted0 = SweepAcc#sa.num_deleted,
    SweepAcc#sa{num_mutated = Mutated0 + NumMutated,
                num_deleted = Deleted0 + NumDeleted}.

-spec update_sa_with_participant_results(#sa{},
                                         [#sweep_participant{}],
                                         [#sweep_participant{}]) -> #sa{}.
update_sa_with_participant_results(SweepAcc, Failed, Successful) ->
    SweepAcc#sa{active_p = lists:reverse(Successful),
                failed_p = Failed,
                succ_p = []}.

-spec maybe_add_bucket_info(riak_object:bucket(),
                            BucketProps :: dict(),
                            Options :: [term()]) ->
    {[term()], BucketProps :: dict()}.
maybe_add_bucket_info(Bucket, BucketPropsDict, Options) ->
    case lists:member(bucket_props, Options) of
        true ->
            {BucketProps, BucketPropsDict1} = get_bucket_props(Bucket, BucketPropsDict),
            {[{bucket_props, BucketProps}], BucketPropsDict1};
        false ->
            {[], BucketPropsDict}
    end.

-spec get_bucket_props(riak_object:bucket(),
                       BucketProps :: dict()) ->
    {BucketProps :: [{atom(), term()}], BucketPropsDict :: dict()}.
get_bucket_props(Bucket, BucketPropsDict) ->
    case dict:find(Bucket, BucketPropsDict) of
        {ok, BucketProps} ->
            {BucketProps, BucketPropsDict};
        _ ->
            BucketProps = riak_core_bucket:get_bucket(Bucket),
            BucketPropsDict1 = dict:store(Bucket, BucketProps, BucketPropsDict),
            {BucketProps, BucketPropsDict1}
    end.

-spec update_stat_counters(NumMutated :: non_neg_integer(),
                           NumDeleted :: non_neg_integer(),
                           BinSize :: non_neg_integer(),
                           #sa{}) -> #sa{}.
update_stat_counters(NumMutated, NumDeleted, RObjBinSize, Acc) ->
    StatSweptKeysCounter = Acc#sa.stat_swept_keys_counter,
    StatObjSizeCounter = Acc#sa.stat_obj_size_counter,
    StatNumMutated = Acc#sa.stat_mutated_objects_counter,
    StatNumDeleted = Acc#sa.stat_deleted_objects_counter,

    Acc#sa{stat_mutated_objects_counter = StatNumMutated + NumMutated,
           stat_deleted_objects_counter = StatNumDeleted + NumDeleted,
           stat_obj_size_counter = StatObjSizeCounter + RObjBinSize,
           stat_swept_keys_counter = StatSweptKeysCounter + 1}.

-spec stat_send(riak_kv_sweeper:index(),
                {'final'| 'in_progress', #sa{}}) -> #sa{}.
stat_send(Index, {final, Acc}) ->
    FailedFun = fun(#sweep_participant{module = Module}) ->
                        riak_kv_stat:update({sweeper, Index, failed, Module, 1})
                end,
    SuccessFun = fun(#sweep_participant{module = Module}) ->
                         riak_kv_stat:update({sweeper, Index, successful, Module, 1})
                 end,
    lists:foreach(FailedFun, Acc#sa.failed_p),
    lists:foreach(SuccessFun, Acc#sa.active_p),
    stat_send(Index, {in_progress, Acc});

stat_send(Index, {in_progress, Acc}) ->
    riak_kv_stat:update({sweeper, Index, keys, Acc#sa.stat_swept_keys_counter}),
    riak_kv_stat:update({sweeper, Index, bytes, Acc#sa.stat_obj_size_counter}),
    riak_kv_stat:update({sweeper, Index, deleted, Acc#sa.stat_deleted_objects_counter}),
    riak_kv_stat:update({sweeper, Index, mutated, Acc#sa.stat_mutated_objects_counter}),

    Acc#sa{stat_deleted_objects_counter = 0,
           stat_mutated_objects_counter = 0,
           stat_swept_keys_counter = 0,
           stat_obj_size_counter = 0}.

%% Participant record constructors/accessors/mutators:
-spec participant(string(), module(), riak_kv_sweeper:fun_type(),
                  riak_kv_sweeper:run_interval(), [atom()]) -> participant().
participant(Description, Module, FunType, RunInterval, Options) ->
    #sweep_participant{
       description = Description,
       module = Module,
       fun_type = FunType,
       run_interval = RunInterval,
       options = Options
      }.

-spec participant_module(participant()) -> module().
participant_module(Participant) -> Participant#sweep_participant.module.

-spec participant_run_interval(participant()) -> riak_kv_sweeper:run_interval().
participant_run_interval(Participant) -> Participant#sweep_participant.run_interval.

-spec is_participant_in_list(module(), [participant()]) -> boolean().
is_participant_in_list(Module, Participants) ->
    lists:keymember(Module, #sweep_participant.module, Participants).

-spec format_active_participants([participant()], [{pos_integer(), participant()}]) ->
    [{string(), boolean()}].
format_active_participants(AcitvePart, IndexedParticipants) ->
    [begin
         IndexString = integer_to_list(Index),
         {"Active " ++ IndexString,
          lists:keymember(Mod, #sweep_participant.module, AcitvePart)}
     end
     || {Index, #sweep_participant{module = Mod}} <- IndexedParticipants].

-spec format_sweep_participant(riak_kv_sweeper:index(), participant()) -> [{string(), term()}].
format_sweep_participant(Index, #sweep_participant{module = Mod,
                                                   description = Description,
                                                   run_interval = Interval}) ->
    IntervalValue = riak_kv_sweeper:get_run_interval(Interval),
    IntervalString = format_interval(IntervalValue * 1000000),
    [{"ID", Index},
     {"Module" ,atom_to_list(Mod)},
     {"Description", Description},
     {"Interval",  IntervalString}].

format_interval(Interval) ->
    riak_core_format:human_time_fmt("~.1f", Interval).

-spec missing('run_interval' | 'module', dict(),
              [{riak_kv_sweeper:participant_module(),
                {erlang:timestamp(),
                 'succ'| 'fail'}}]) ->
     [non_neg_integer()] | [riak_kv_sweeper:participant_module()].
missing(Return, Participants, ResultList) ->
    [case Return of
         run_interval ->
             riak_kv_sweeper_state:get_run_interval(RunInterval);
         module ->
             Module
     end ||
     {Module, #sweep_participant{run_interval = RunInterval}}
         <- dict:to_list(Participants),
        not lists:keymember(Module, 1, ResultList)].

-ifdef(TEST).

setup_sweep(N) ->
    %% Shouldn't be necessary to run this in setup, but some tests may not clean up
    %% after themselves. This also helps work around a bug in meck where mocking a
    %% module that has previously run eunit tests can cause the eunit test process
    %% to be killed. (See https://github.com/eproxus/meck/issues/133 for more info.)
    meck:unload(),
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
                SA = lists:foldl(fun(NBin, Acc) ->
                                         InitialObj = riak_object:new(NBin, NBin, <<>>),
                                         ObjBin = riak_object:to_binary(v1, InitialObj),
                                         CompleteFoldReq(NBin, NBin, ObjBin, Acc)
                                 end, InitialAcc, Keys),
                {ok, SA}
        end,
    meck:expect(fake_backend, fold_objects, BackendFun),
    meck:new(riak_kv_stat),
    meck:expect(riak_kv_stat, update, fun(_) -> ok end).

observ_sweep() ->
    %% Keep track on nr of modified and original objects
    ObservFun = fun({{_, _BKey}, <<"mutated">>}, {Mut, Org}, _Opt) -> {ok, {Mut + 1, Org}};
                   ({{_, _BKey}, _RObj}, {Mut, Org}, _Opt) -> {ok, {Mut, Org + 1}} end,
    [#sweep_participant{module = observ_callback_module,
                       sweep_fun = ObservFun,
                       fun_type = observe_fun,
                        acc = {0,0}
                       }].

delete_sweep(N) ->
    DeleteFun =
        fun({{_, BKey}, _RObj}, Acc, _Opt) ->
                rem_keys(BKey, N, {deleted, Acc + 1}, {ok, Acc})
        end,
    [#sweep_participant{module = delete_callback_module,
                       sweep_fun = DeleteFun,
                       fun_type = delete_fun,
                        acc = 0
                       }].

make_keys(Nr) ->
    [integer_to_binary(N) || N <- lists:seq(1, Nr)].

meck_callback_modules(Module) ->
    meck:new(Module, [non_strict]),
    meck:expect(Module, failed_sweep, fun(_Index, _Acc, _Reason) -> ok end),
    meck:expect(Module, successful_sweep, fun(_Index, _Reason) -> ok end).

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
                        fun_type = modify_fun,
                        acc = 0
                       }].

%% delete sweep participant that ask for bucket props
delete_sweep_bucket_props() ->
    %% Check that we receive bucket_props when we ask for it
    DeleteFun = fun({_BKey, _RObj}, Acc, [{bucket_props, _}]) -> {deleted, Acc} end,
    [#sweep_participant{module = delete_callback_module,
                       sweep_fun = DeleteFun,
                       fun_type = delete_fun,
                       options = [bucket_props]}].
%% Crashing delete sweep participant
delete_sweep_crash() ->
    DeleteFun = fun({_BKey, RObj}, _Acc, _Opt) -> RObj = crash end,
    [#sweep_participant{module = delete_callback_module,
                        sweep_fun = DeleteFun,
                        fun_type = delete_fun,
                        options = [bucket_props]}].

sweep_fold_test_() ->
    {timeout, 30, [
                   fun test_sweep_delete/0,
                   fun test_sweep_delete_bucket_props/0,
                   fun test_sweep_observ_delete/0,
                   fun test_sweep_modify_observ_delete/0,
                   fun test_sweep_delete_crash_observ/0,
                   fun test_make_initial_sweep_participant/0
                  ]}.

%% Basic sweep test. Check that callback get complete Acc when sweep finish
test_sweep_delete() ->
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
test_sweep_delete_bucket_props() ->
    setup_sweep(Keys = 100),
    {reply, Acc, _State} =
        do_sweep(delete_sweep_bucket_props(), 0, no_sender, [], no_index, fake_backend, [], []),

    ?assertEqual(Keys, dict:size(Acc#sa.bucket_props)),
    ?assertEqual(Keys, Acc#sa.swept_keys),
    meck:unload().

%% Delete 1/3 of the keys the rest gets seen by the observer sweep
test_sweep_observ_delete() ->
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
test_sweep_modify_observ_delete() ->
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
test_sweep_delete_crash_observ() ->
    setup_sweep(Keys = 100),
    Sweeps = delete_sweep_crash() ++ observ_sweep(),
    {reply, Acc, _State} = do_sweep(Sweeps, 0, no_sender, [], no_index, fake_backend, [], []),
    [{_Pid,{_,_,[_,_,FailReason]},ok}] = meck:history(delete_callback_module),
    [{_Pid,{_,_,[_,ObservAcc]},ok}] = meck:history(observ_callback_module),

    %% check that the delete sweep failed but observer succeed
    ?assertEqual(too_many_crashes, FailReason),
    ?assertEqual({0, Keys}, ObservAcc),
    ?assertEqual(Keys, Acc#sa.swept_keys),
    meck:unload().

test_make_initial_sweep_participant() ->
    SP0s = [#sweep_participant{module = 3, fun_type = observe_fun},
            #sweep_participant{module = 2, fun_type = modify_fun},
            #sweep_participant{module = 1, fun_type = delete_fun}],
    #sa{active_p = SPs} = make_initial_acc(1, SP0s, 100),
    [1, 2, 3] = [SP#sweep_participant.module || SP <- SPs],
    ok.

-endif.
