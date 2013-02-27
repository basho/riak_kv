-module(get_fsm_eqc).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("riak_kv_vnode.hrl").

-compile(export_all).
-define(DEFAULT_BUCKET_PROPS,
        [{allow_mult, false},
         {chash_keyfun, {riak_core_util, chash_std_keyfun}},
         {basic_quorum, true},
         {notfound_ok, false},
         {r, quorum},
         {pr, 0}]).
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).

-record(state, {n,
                r,
                real_r,
                pr,
                real_pr,
                num_primaries,
                preflist,
                history,
                objects,
                deleted,
                options,
                notfound_is_ok,
                basic_quorum,
                deletedvclock,
                exp_result,
                num_oks = 0,
                del_oks = 0,
                num_errs = 0}).


%%====================================================================
%% eunit test
%%====================================================================

eqc_test_() ->
    {spawn,
     [{setup,
       fun setup/0,
       fun cleanup/1,
       [%% Run the quickcheck tests
        {timeout, 60000, % do not trust the docs - timeout is in msec
         ?_assertEqual(true, eqc:quickcheck(eqc:testing_time(50, ?QC_OUT(prop_basic_get()))))}
       ]
      }
     ]
    }.

setup() ->
    %% Shut logging up - too noisy.
    application:load(sasl),
    application:set_env(sasl, sasl_error_logger, {file, "get_fsm_eqc_sasl.log"}),
    error_logger:tty(false),
    error_logger:logfile({open, "get_fsm_eqc.log"}),

    %% Start up mock servers and dependencies
    fsm_eqc_util:start_mock_servers(),
    fsm_eqc_util:start_fake_rng(?MODULE),
    ok.

cleanup(_) ->
    fsm_eqc_util:cleanup_mock_servers(),
    ok.

%% Call unused callback functions to clear them in the coverage
%% checker so the real code stands out.
coverage_test() ->
    riak_kv_test_util:call_unused_fsm_funs(riak_kv_get_fsm).

%%====================================================================
%% Shell helpers
%%====================================================================

prepare() ->
    fsm_eqc_util:start_mock_servers().

test() ->
    test(100).

test(N) ->
    fsm_eqc_util:start_mock_servers(),
    fsm_eqc_util:start_fake_rng(?MODULE),
    try quickcheck(numtests(N, prop_basic_get()))
    after
        fsm_eqc_util:cleanup_mock_servers(),
        exit(whereis(?MODULE), kill)
    end.

check() ->
    fsm_eqc_util:start_mock_servers(),
    fsm_eqc_util:start_fake_rng(?MODULE),
    try check(prop_basic_get(), current_counterexample())
    after
        fsm_eqc_util:cleanup_mock_servers(),
        exit(whereis(?MODULE), kill)
    end.

%%====================================================================
%% Generators
%%====================================================================

merge_heads([]) ->
    [];
merge_heads([Lin|Lins]) ->
    merge_heads(Lin, merge_heads(Lins)).

merge_heads(Lin, Lins) ->
    Subsumes = fun(Lin0, Lin1) ->
            is_descendant(Lin0, Lin1) orelse
            Lin0 == Lin1
        end,
    case lists:any(fun(Lin1) -> Subsumes(Lin1, Lin) end, Lins) of
        true  -> Lins;
        false ->
            [Lin|lists:filter(fun(Lin1) -> not Subsumes(Lin, Lin1) end, Lins)]
    end.

is_descendant(Lin, Lin) ->
    false;
is_descendant(current, _) ->
    true;
is_descendant(_, ancestor) ->
    true;
is_descendant(_, _) ->
    false.

is_sibling(Lin, Lin) ->
    false;
is_sibling(Lin1, Lin2) ->
    not is_descendant(Lin1, Lin2) andalso
    not is_descendant(Lin2, Lin1).


vnodegetresps() ->
    fsm_eqc_util:not_empty(fsm_eqc_util:longer_list(2, vnodegetresp())).

vnodegetresp() ->
    {fsm_eqc_util:partval(), nodestatus()}.


nodestatus() ->
    ?SHRINK(frequency([{9, primary},
                       {1, fallback}]),
            [primary]).

r_seed() ->
    frequency([{10, quorum},
               {10, default},
               {10, missing},
               {10, one},
               {10, all},
               {10, fsm_eqc_util:largenat()},
               { 1, garbage}]).

detail() -> frequency([{1, timing},
                       {1, vnodes},
                       {1, not_a_detail}]).

details() -> frequency([{10, true}, %% All details requested
                        {10, list(detail())},
                        { 1, false}]).

bool_prop(Name) ->
    frequency([{4, {Name, true}},
               {1, Name},
               {5, {Name, false}}]).

option() ->
    frequency([{1, {details, details()}},
               {1, bool_prop(notfound_ok)},
               {1, bool_prop(deletedvclock)},
               {1, bool_prop(basic_quorum)}]).

options() ->
    list(option()).

rr_abort_opts() ->
    % {SoftCap, HardCap, ActualRunning, RandomRollResult}
    {choose(1000, 2000), choose(3000, 4000), choose(0, 4999), choose(0, 100)}.

prop_basic_get() ->
    ?FORALL({RSeed,PRSeed,Options0, Objects,ReqId,VGetResps,RRAbort},
            {r_seed(), r_seed(), options(),
             fsm_eqc_util:riak_objects(), noshrink(largeint()),
             vnodegetresps(),rr_abort_opts()},
    begin
        N = length(VGetResps),
        {R, RealR} = r_val(N, 1000000, RSeed),
        {PR, RealPR} = pr_val(N, 1000000, PRSeed),
        PL2 = make_preflist2(VGetResps, 1, []),
        PartVals = make_partvals(VGetResps, []),
        fsm_eqc_vnode:set_data(Objects, PartVals),
        BucketProps = [{n_val, N}
                       |?DEFAULT_BUCKET_PROPS],

        %% Needed for riak_kv_util get_default_rw_val
        application:set_env(riak_core,
                            default_bucket_props,
                            BucketProps),

        [{_,Object}|_] = Objects,

        Options = fsm_eqc_util:make_options([{r, R}, {pr, PR}], [{timeout, 200} | Options0]),

        {SoftCap, HardCap, Actual, Roll} = RRAbort,
        application:set_env(riak_kv, read_repair_soft, SoftCap),
        application:set_env(riak_kv, read_repair_max, HardCap),
        FolsomKey = {riak_kv, node, gets, fsm, active},
        % don't really care if the key doesn't exist when we delete it.
        catch folsom_metrics:delete_metric(FolsomKey),
        folsom_metrics:new_counter(FolsomKey),
        folsom_metrics:notify({FolsomKey, {inc, Actual}}),
        fsm_eqc_util:set_fake_rng(?MODULE, Roll),

        {ok, GetPid} = riak_kv_get_fsm:test_link({raw, ReqId, self()},
                            riak_object:bucket(Object),
                            riak_object:key(Object),
                            Options,
                            [{starttime, riak_core_util:moment()},
                             {n, N},
                             {bucket_props, BucketProps},
                             {preflist2, PL2}]),

        process_flag(trap_exit, true),
        ok = riak_kv_test_util:wait_for_pid(GetPid),
        Res = fsm_eqc_util:wait_for_req_id(ReqId, GetPid),
        process_flag(trap_exit, false),

        History = fsm_eqc_vnode:get_history(),
        RepairHistory = fsm_eqc_vnode:get_put_history(),
        Ok         = length([ ok || {_, {ok, _}} <- History ]),
        NotFound   = length([ ok || {_, notfound} <- History ]),
        NoReply    = length([ ok || {_, timeout}  <- History ]),
        Partitions = [ P || {P, _} <- History ],
        Deleted    = [ {Lin, case riak_kv_util:obj_not_deleted(Obj) == undefined of
                                 true  -> deleted;
                                 false -> present
                             end
                       }
                        || {Lin, Obj} <- Objects ],
        NotFoundIsOk = proplists:get_value(notfound_ok, Options, false),
        BasicQuorum = proplists:get_value(basic_quorum, Options, true),
        DeletedVClock = proplists:get_value(deletedvclock, Options, false),
        NumPrimaries = length([xx || {_,primary} <- PL2]),
        State = expect(#state{n = N, r = R, real_r = RealR, pr = PR, real_pr = RealPR,
                              history = History, preflist=PL2,
                              objects = Objects, deleted = Deleted, options = Options,
                              notfound_is_ok = NotFoundIsOk, basic_quorum = BasicQuorum,
                              num_primaries = NumPrimaries, deletedvclock = DeletedVClock}),
        ExpResult = State#state.exp_result,
        ExpectedN = case ExpResult of
                        {error, {n_val_violation, _}} ->
                            0;
                        {error, {r_val_violation, _}} ->
                            0;
                        {error, {pr_val_violation, _}} ->
                            0;
                        {error, {pr_val_unsatisfied, _, Y}} ->
                            case Y >= NumPrimaries of
                                true ->
                                    %% if this preflist can never satisfy PR,
                                    %% the put fsm will bail early
                                    0;
                                false ->
                                    length([xx || {_, Resp} <- VGetResps, Resp /= timeout])
                            end;
                        _ ->
                            length([xx || {_, Resp} <- VGetResps, Resp /= timeout])
                    end,

        %% A perfect preference list has all owner partitions available
        PerfectPreflist = lists:all(fun({{_Idx,_Node},primary}) -> true;
                                       ({{_Idx,_Node},fallback}) -> false
                                    end, PL2),


        {RetResult, RetInfo} = case Res of
                                   timeout ->
                                       {Res, undefined};
                                   {ok, _RetObj} ->
                                       {Res, undefined};
                                   {error, _Reason} ->
                                       {Res, undefined};
                                   {ok, RetObj, Info0} ->
                                       {{ok, RetObj}, Info0};
                                   {error, Reason, Info0} ->
                                       {{error, Reason}, Info0}
                               end,
        ?WHENFAIL(
            begin
                io:format("Res: ~p\n", [Res]),
                io:format("Repair: ~p~nHistory: ~p~n",
                          [RepairHistory, History]),
                io:format("SoftCap: ~p~nHardCap: ~p~nActual: ~p~nRoll: ~p~n",
                          [SoftCap, HardCap, Actual, Roll]),
                io:format("RetResult: ~p~nExpResult: ~p~nDeleted objects: ~p~n",
                          [RetResult, ExpResult, Deleted]),
                io:format("N: ~p  R: ~p  RealR: ~p  PR: ~p  RealPR: ~p~n"
                          "Options: ~p~nVGetResps: ~p~nPL2: ~p~n",
                          [N, R, RealR, PR, RealPR, Options, VGetResps, PL2]),
                io:format("Ok: ~p~nNotFound: ~p~nNoReply: ~p~n",
                          [Ok, NotFound, NoReply])
            end,
            conjunction(
                [{result, equals(RetResult, ExpResult)},
                 {details, check_details(RetInfo, State)},
                 {n_value, equals(length(History), ExpectedN)},
                 {repair, check_repair(Objects, RepairHistory, History, RRAbort)},
                 {delete,  check_delete(Objects, RepairHistory, History, PerfectPreflist)},
                 {distinct, all_distinct(Partitions)},
                 {told_monitor, fsm_eqc_util:is_get_put_last_cast(get, GetPid)}
                ]))
    end).


%% make preflist2 from the vnode responses.
%% [{notfound|{ok, lineage()}, PrimaryFallback, Response]
make_preflist2([], _Index, PL2) ->
    lists:reverse(PL2);
make_preflist2([{_PartVal, PrimaryFallback} | Rest], Index, PL2) ->
    make_preflist2(Rest, Index + 1,
                   [{{Index, whereis(fsm_eqc_vnode)}, PrimaryFallback} | PL2]).

%% Make responses
make_partvals([], PartVals) ->
    lists:reverse(PartVals);
make_partvals([{PartVal, _PrimaryFallback} | Rest], PartVals) ->
    make_partvals(Rest, [PartVal | PartVals]).


%% Work out R given a seed.
%% Generate a value from 0..N+1
r_val(_N, _Min, garbage) ->
    {garbage, 1000000};
r_val(N, Min, Seed) when is_number(Seed) ->
    Val = Seed rem N + 2,
    {Val, erlang:min(Min, Val)};
r_val(N, Min, Seed) ->
    Val = case Seed of
              one -> 1;
              all -> N;
              X when X == quorum; X == missing; X == default ->
                  (N div 2) + 1;
              _ -> Seed
          end,
    {Seed, erlang:min(Min, Val)}.

pr_val(_N, _Min, garbage) ->
    {garbage, 1000000};
pr_val(N, Min, Seed) when is_number(Seed) ->
    Val = Seed rem N + 2,
    {Val, erlang:min(Min, Val)};
pr_val(N, Min, Seed) ->
    Val = case Seed of
              one -> 1;
              all -> N;
              quorum -> (N div 2) + 1;
              X when X == missing; X == default -> 0;
              _ -> Seed
          end,
    {Seed, erlang:min(Min, Val)}.


do_repair(_Heads, notfound) ->
    true;
do_repair(Heads, {ok, Lineage}) ->
    lists:any(fun(Head) ->
                is_descendant(Head, Lineage) orelse
                is_sibling(Head, Lineage)
              end, Heads);
do_repair(_Heads, _V) ->
    false.

expected_repairs(_H, {_Soft,Hard,Actual,_Roll}) when Actual >= Hard ->
    [];
expected_repairs(H, {Soft, Hard, Actual, Roll}) when Soft < Actual, Actual < Hard ->
    CapAdjusted = Hard - Soft,
    ActualAdjusted = Actual - Soft,
    Percent = ActualAdjusted / CapAdjusted * 100,
    if
      Roll < Percent ->
        [];
      true ->
        expected_repairs(H, true)
    end;
expected_repairs(H,_RRAbort) ->
    case [ Lineage || {_, {ok, Lineage}} <- H ] of
        []   -> [];
        Lins ->
            Heads = merge_heads(Lins),
            [ Part || {Part, V} <- H,
                      do_repair(Heads, V) ]
    end.

check_details(Info, State = #state{options = Options}) ->
    case proplists:get_value(details, Options, false) of
        false ->
            equals(undefined, Info);
        [] ->
            equals(undefined, Info);
        _ ->
            equals(check_info(Info, State), true)
    end.

check_info([], _State) ->
    true;
check_info([{not_a_detail, unknown_detail} | Rest], State) ->
    check_info(Rest, State);
check_info([{response_usecs, _} | Rest], State) ->
    check_info(Rest, State);
check_info([{stages, _} | Rest], State) ->
    check_info(Rest, State);
check_info([{vnode_oks, VnodeOks} | Rest], State = #state{num_oks = NumOks}) ->
    %% How many Ok's in first RealR responses received by FSM.
    case NumOks of
        VnodeOks ->
            check_info(Rest, State);
        Expected ->
            {vnode_oks, VnodeOks, expected, Expected}
    end;
check_info([{vnode_errors, _Errors} | Rest], State) ->
    %% The first RealR errors from the vnode history
    check_info(Rest, State).

%% Check the read repairs - no need to worry about delete objects, deletes can only
%% happen when all read repairs are complete.

check_repair(Objects, RepairH, H, RRAbort) ->
    Actual = [ Part || {Part, ?KV_PUT_REQ{}} <- RepairH ],
    Heads  = merge_heads([ Lineage || {_, {ok, Lineage}} <- H ]),
    RepairObject  = (catch build_merged_object(Heads, Objects)),
    Expected =expected_repairs(H,RRAbort),
    RepairObjects = [ Obj || {_Idx, ?KV_PUT_REQ{object=Obj}} <- RepairH ],
    conjunction(
        [{puts, equals(lists:sort(Expected), lists:sort(Actual))},
         {sanity, equals(length(RepairObjects), length(Actual))},
         {right_object,
            ?WHENFAIL(io:format("RepairObject: ~p~n", [RepairObject]),
                lists:all(fun(Obj) -> Obj =:= RepairObject end,
                          RepairObjects))}
        ]).


check_delete(Objects, RepairH, H, PerfectPreflist) ->
    Deletes  = [ Part || {Part, ?KV_DELETE_REQ{}} <- RepairH ],

    %% Should get deleted if all vnodes returned the same object
    %% and a perfect preflist and the object is deleted
    RetLins = [Lineage || {_Idx, {ok, Lineage}} <- H],
    URetLins = lists:usort(RetLins),
    Expected = case PerfectPreflist andalso
                   length(RetLins) == length(H) andalso
                   length(URetLins) == 1 andalso
                   riak_kv_util:is_x_deleted(proplists:get_value(hd(URetLins), Objects)) of
                   true ->
                       [P || {P, _} <- H];  %% send deletes to all nodes
                   false ->
                       []
               end,
    ?WHENFAIL(io:format("Objects: ~p\nExpected: ~p\nDeletes: ~p\nH: ~p\n",
                        [Objects, Expected, Deletes, H]),
              equals(lists:sort(Expected), lists:sort(Deletes))).

all_distinct(Xs) ->
    equals(lists:sort(Xs),lists:usort(Xs)).

build_merged_object([], _Objects) ->
    undefined;
build_merged_object(Heads, Objects) ->
    Lineage = fsm_eqc_util:merge(Heads),
    Object  = proplists:get_value(Lineage, Objects),
    Vclock  = vclock:merge(
                [ riak_object:vclock(proplists:get_value(Head, Objects))
                    || Head <- Heads ]),
   riak_object:set_vclock(Object, Vclock).

expect(State = #state{r = R}) when R =:= garbage ->
    State#state{exp_result = {error, {r_val_violation, garbage}}};
expect(State = #state{n = N, real_r = RealR}) when RealR > N ->
    State#state{exp_result = {error, {n_val_violation, N}}};
expect(State = #state{pr = PR}) when PR =:= garbage ->
    State#state{exp_result = {error, {pr_val_violation, garbage}}};
expect(State = #state{n = N, real_pr = RealPR}) when RealPR > N ->
    State#state{exp_result = {error, {n_val_violation, N}}};
expect(State = #state{real_pr = RealPR, num_primaries = NumPrimaries}) when RealPR > NumPrimaries ->
    State#state{exp_result = {error, {pr_val_unsatisfied, RealPR, NumPrimaries}}};
expect(State = #state{history = History, objects = Objects,
                      deletedvclock = DeletedVClock, deleted = _Deleted}) ->
    State1 = expect(History, State, 0, 0, 0, 0, 0, []),
    case State1#state.exp_result of
        {ok, Heads} ->
            Object = build_merged_object(Heads, Objects),
            case riak_kv_util:obj_not_deleted(Object) of
                undefined when DeletedVClock ->
                    State1#state{exp_result = {error, {deleted,
                                 riak_object:vclock(Object)}}};
                undefined -> State1#state{exp_result = {error, notfound}};
                Obj       -> State1#state{exp_result = {ok, Obj}}
            end;
        Err ->
            State1#state{exp_result = {error, Err}}
    end.

%% decide on error message - if only got notfound messages, return notfound
%% otherwise let caller know R value was not met.
notfound_or_error(NotFound, 0, 0, _Oks, _R, _PR, _PROks) when NotFound > 0 ->
    notfound;
notfound_or_error(_NotFound, _NumNotDeleted, _Err, Oks, R, PR, PROks) ->
    %% PR trumps R, if PR >= R
    if Oks >= R, PROks < PR ->
            {pr_val_unsatisfied, PR, PROks};
        R > PR ->
            {r_val_unsatisfied, erlang:max(R, PR), Oks};
        true ->
            {pr_val_unsatisfied, PR, PROks}
    end.

expect(H, State = #state{n = N, real_r = R, real_pr = PR, deleted = Deleted, notfound_is_ok = NotFoundIsOk,
                         basic_quorum = BasicQuorum, preflist=Preflist},
       NotFounds, Oks, PROks, DelOks, Errs, Heads) ->
    FailR = erlang:max(PR, R),
    FailThreshold =
    case BasicQuorum of
        true ->
            erlang:min((N div 2)+1, % basic quorum, or
                (N-FailR+1)); % cannot ever get R 'ok' replies
        _ElseFalse ->
            N - FailR + 1 % cannot ever get R 'ok' replies
    end,
    Pending = N - (NotFounds + Oks + Errs),
    if  Oks >= R andalso PROks >= PR -> % we made quorum
            ExpResult = case Heads of
                            [] ->
                                notfound;
                            _ ->
                                {ok, Heads}
                        end,
            State#state{exp_result = ExpResult, num_oks = Oks, num_errs = Errs};
        (BasicQuorum andalso (NotFounds + Errs)*2 > N) orelse % basic quorum
            Pending + Oks < R ->            % no way we'll make quorum
            %% Adjust counts to make deleted objects count towards notfound.
            State#state{exp_result = notfound_or_error(NotFounds + DelOks, Oks - DelOks,
                                                       Errs, Oks, R, PR, PROks),
                        num_oks = Oks, del_oks = DelOks, num_errs = Errs};
        (PROks < PR andalso (NotFounds + Errs >= FailThreshold)) ->
            State#state{exp_result = notfound_or_error(NotFounds + DelOks, Oks - DelOks,
                                                       Errs, Oks, R, PR, PROks),
                        num_oks = Oks, del_oks = DelOks, num_errs = Errs};
        true ->
            case H of
                [] ->
                    case PROks < PR andalso (Oks + Errs + NotFounds) == N of
                        true ->
                            State#state{exp_result = notfound_or_error(NotFounds + DelOks, Oks - DelOks,
                                    Errs, Oks, R, PR, PROks),
                                num_oks = Oks, del_oks = DelOks, num_errs = Errs};
                        false ->
                            %% not enough responses, must be a timeout
                            State#state{exp_result = timeout, num_oks = Oks, num_errs = Errs}
                    end;
                [{_Idx, timeout}|Rest] ->
                    expect(Rest, State, NotFounds, Oks, PROks, DelOks, Errs, Heads);
                [{Idx, notfound}|Rest] ->
                    PRInc = case lists:keyfind(Idx, 1, [{Index, Primacy} ||
                                {{Index, _Pid}, Primacy} <- Preflist]) of
                        {Idx, primary} ->
                            1;
                        _ ->
                            0
                    end,
                    case NotFoundIsOk of
                        true ->
                            expect(Rest, State, NotFounds, Oks + 1, PROks +
                                PRInc, DelOks, Errs, Heads);
                        false ->
                            expect(Rest, State, NotFounds + 1, Oks, PROks, DelOks, Errs, Heads)
                    end;
                [{_Idx, error}|Rest] ->
                    expect(Rest, State, NotFounds, Oks, PROks, DelOks, Errs + 1, Heads);
                [{Idx, {ok,Lineage}}|Rest] ->
                    PRInc = case lists:keyfind(Idx, 1, [{Index, Primacy} ||
                                {{Index, _Pid}, Primacy} <- Preflist]) of
                        {Idx, primary} ->
                            1;
                        _ ->
                            0
                    end,
                    IncDelOks = length([xx || {Lineage1, deleted} <- Deleted, Lineage1 == Lineage]),
                    expect(Rest, State, NotFounds, Oks + 1, PROks + PRInc, DelOks + IncDelOks, Errs,
                           merge_heads(Lineage, Heads))
            end
    end.



-endif. % EQC
