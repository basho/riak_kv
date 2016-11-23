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

-include("riak_kv_sweeper.hrl").
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([do_sweep/8]).
-export([get_sweep_throttle/0]).
-export([send_to_sweep_worker/2]).

-define(MAX_SWEEP_CRASHES, 10).
%% Throttle used when sweeping over K/V data: {Type, Limit, Wait}.
%% Type can be pace or obj_size.
%% Default: 1 MB limit / 100 ms wait
-define(DEFAULT_SWEEP_THROTTLE, {obj_size, 1000000, 100}).

%% Default value for how much faster the sweeper throttle should be during
%% sweep window.
-define(SWEEP_WINDOW_THROTTLE_DIV, 1).

do_sweep(ActiveParticipants, EstimatedKeys, Sender, Opts, Index, Mod, ModState, VnodeState) ->
    CompleteFoldReq = make_complete_fold_req(),
    InitialAcc = make_initial_acc(Index, ActiveParticipants, EstimatedKeys),
    case Mod:fold_objects(CompleteFoldReq, InitialAcc, Opts, ModState) of
        {ok, #sa{} = Acc} ->
            inform_participants(Acc, Index),
            riak_kv_sweeper:sweep_result(Index, format_result(Acc)),
            {reply, Acc, VnodeState};
        {async, Work} ->
            FinishFun =
                fun(Acc) ->
                        inform_participants(Acc, Index),
                        riak_kv_sweeper:sweep_result(Index, format_result(Acc))
                end,
            {async, {sweep, Work, FinishFun}, Sender, VnodeState};
        Reason ->
            failed_sweep(ActiveParticipants, Index, Reason),
            {reply, Reason, VnodeState}
    end.

make_complete_fold_req() ->
    fun(Bucket, Key, RObjBin, #sa{index = Index, swept_keys = SweptKeys} = Acc) ->
            Acc1 = maybe_throttle_sweep(RObjBin, Acc),
            Acc2 =
                case SweptKeys rem 1000 of
                    0 ->
                        riak_kv_sweeper:update_progress(Index, SweptKeys),
                        maybe_receive_request(Acc1);
                    _ ->
                        Acc1
                end,
            RObj = riak_object:from_binary(Bucket, Key, RObjBin),
            fold_funs({{Bucket, Key}, RObj}, Acc2#sa{swept_keys = SweptKeys + 1})
    end.

make_initial_acc(Index, ActiveParticipants, EstimatedNrKeys) ->
    SweepsParticipants = sort_participants(ActiveParticipants),
    #sa{index = Index, active_p = SweepsParticipants, estimated_keys = EstimatedNrKeys}.

sort_participants(Participants) ->
    lists:sort(fun compare_participants/2, Participants).
compare_participants(A, B) ->
    TypeA = A#sweep_participant.fun_type,
    TypeB = B#sweep_participant.fun_type,
    fun_type_rank(TypeA) =< fun_type_rank(TypeB).
fun_type_rank(delete_fun) -> 1;
fun_type_rank(modify_fun) -> 2;
fun_type_rank(observe_fun) -> 3.

inform_participants(#sa{active_p = Succ, failed_p = Failed}, Index) ->
    successful_sweep(Succ, Index),
    failed_sweep(Failed, Index).

successful_sweep(Succ, Index) ->
    [Module:successful_sweep(Index, FinalAcc) ||
       #sweep_participant{module = Module, acc = FinalAcc} <- Succ].

failed_sweep(Failed, Index) ->
    [Module:failed_sweep(Index, Reason) ||
       #sweep_participant{module = Module, fail_reason = Reason } <- Failed].

failed_sweep(Failed, Index, Reason) ->
    [Module:failed_sweep(Index, Reason) ||
       #sweep_participant{module = Module} <- Failed].

format_result(#sa{swept_keys = SweptKeys, active_p = Succ, failed_p = Failed}) ->
    {SweptKeys,
     format_result(succ, Succ) ++ format_result(fail, Failed)}.

format_result(SuccFail, Results) ->
    [{Module, SuccFail} || #sweep_participant{module = Module} <- Results].

%% Throttle depending on swept keys.
maybe_throttle_sweep(_RObjBin, #sa{throttle = {pace, Limit, Wait},
                         swept_keys = SweepKeys,
                         throttle_total_wait_msecs = ThrottleTotalWait} = SweepAcc) ->
    case SweepKeys rem Limit of
        0 ->
            NewThrottle = get_sweep_throttle(),
            %% We use receive after to throttle instead of sleep.
            %% This way we can respond on requests while throttling
            SweepAcc0 = SweepAcc#sa{throttle = NewThrottle,
                                    throttle_total_wait_msecs = ThrottleTotalWait + Wait},
            SweepAcc1 =
                maybe_receive_request(SweepAcc0, Wait),
            maybe_extra_throttle(SweepAcc1);
        _ ->
            maybe_extra_throttle(SweepAcc)
    end;

%% Throttle depending on total obj_size.
maybe_throttle_sweep(RObjBin, #sa{throttle = {obj_size, Limit, Wait},
                         total_obj_size = TotalObjSize,
                         throttle_total_wait_msecs = ThrottleTotalWait} = SweepAcc) ->
    ObjSize = byte_size(RObjBin),
    TotalObjSize1 = ObjSize + TotalObjSize,
    case (Limit =/= 0) andalso (TotalObjSize1 > Limit) of
        true ->
            NewThrottle = get_sweep_throttle(),
            %% We use receive after to throttle instead of sleep.
            %% This way we can respond on requests while throttling
            SweepAcc0 = SweepAcc#sa{throttle = NewThrottle,
                                    throttle_total_wait_msecs = ThrottleTotalWait + Wait},
            SweepAcc1 =
                maybe_receive_request(SweepAcc0, Wait),
            maybe_extra_throttle(SweepAcc1#sa{total_obj_size = 0});
        _ ->
            maybe_extra_throttle(SweepAcc#sa{total_obj_size = TotalObjSize1})
    end.

%% Throttle depending on how many objects the sweep modify.
maybe_extra_throttle(#sa{throttle = Throttle,
                         throttle_total_wait_msecs = ThrottleTotalWait,
                         modified_objects = ModObj} = SweepAcc) ->
    {Limit, Wait} =
        case Throttle of
            {pace, Limit0, Wait0} ->
                {Limit0, Wait0};
            {obj_size, _Limit0, Wait0} ->
                {100, Wait0}
        end,
    %% +1 since some sweeps doesn't modify any objects
    case (ModObj + 1) rem Limit of
        0 ->
            SweepAcc1 = SweepAcc#sa{throttle_total_wait_msecs = ThrottleTotalWait + Wait},
            maybe_receive_request(SweepAcc1, Wait);
        _ ->
            SweepAcc
    end.

%% Throttle depending on how many objects the sweep modify.
get_sweep_throttle() ->
    {Type, Limit, Sleep} =
        app_helper:get_env(riak_kv, sweep_throttle, ?DEFAULT_SWEEP_THROTTLE),
    case riak_kv_sweeper:in_sweep_window() of
        true ->
            Div = app_helper:get_env(riak_kv, sweep_window_throttle_div,
                                     ?SWEEP_WINDOW_THROTTLE_DIV),
            {Type, Limit, Sleep div Div};
        false ->
            {Type, Limit, Sleep}
    end.

send_to_sweep_worker(Msg, #sweep{pid = Pid}) when is_pid(Pid) ->
    lager:debug("Send to sweep worker ~p: ~p", [Pid, Msg]),
    Pid ! Msg;

send_to_sweep_worker(Msg, #sweep{index = Index}) ->
    lager:info("no pid ~p to ~p " , [Msg, Index]),
    no_pid.

maybe_receive_request(Acc) ->
    maybe_receive_request(Acc, 0).

maybe_receive_request(#sa{active_p = Active, failed_p = Fail } = Acc, Wait) ->
    receive
        stop ->
            Active1 =
                [ActiveSP#sweep_participant{fail_reason = sweep_stop } ||
                 #sweep_participant{} = ActiveSP <- Active],
            Acc1 = #sa{active_p = [],  failed_p = Active1 ++ Fail},
            throw({stop_sweep, Acc1});
        {disable, Module} ->
            case lists:keytake(Module, #sweep_participant.module, Active) of
                {value, SP, Active1} ->
                    Acc#sa{active_p = Active1,
                           failed_p = [SP#sweep_participant{fail_reason = disable} | Fail]};
                _ ->
                    Acc
            end
    after Wait ->
        Acc
    end.

fold_funs(_, #sa{index = Index,
                 failed_p = FailedParticipants,
                 active_p = [],
                 succ_p = []} = SweepAcc) ->
    lager:info("No more participants in sweep of Index ~p Failed: ~p", [Index, FailedParticipants]),

    throw({stop_sweep, SweepAcc});

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

-ifdef(TEST).

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
                SA = lists:foldl(fun(NBin, Acc) ->
                                         InitialObj = riak_object:new(NBin, NBin, <<>>),
                                         ObjBin = riak_object:to_binary(v1, InitialObj),
                                         CompleteFoldReq(NBin, NBin, ObjBin, Acc)
                                 end, InitialAcc, Keys),
                {ok, SA}
        end,
    meck:expect(fake_backend, fold_objects, BackendFun).

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
    meck:expect(Module, failed_sweep, fun(_Index, _Reason) -> ok end),
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

-endif.
