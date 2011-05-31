-module(get_fsm_qc).

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
         ?_assertEqual(true, quickcheck(numtests(500, ?QC_OUT(prop_basic_get()))))}
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
    quickcheck(numtests(N, prop_basic_get())).

check() ->
    check(prop_basic_get(), current_counterexample()).

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

prop_basic_get() ->
    ?FORALL({RSeed,PRSeed,Options0, Objects,ReqId,VGetResps},
            {r_seed(), r_seed(), options(),
             fsm_eqc_util:riak_objects(), noshrink(largeint()),
             vnodegetresps()},
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
                              history = History,
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
                        {error, {pr_val_unsatisfied, _, _}} ->
                            0;
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
                 {repair, check_repair(Objects, RepairHistory, History)},
                 {delete,  check_delete(Objects, RepairHistory, History, PerfectPreflist)},
                 {distinct, all_distinct(Partitions)}
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

expected_repairs(H) ->
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
check_info([{duration, _} | Rest], State) ->
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

check_repair(Objects, RepairH, H) ->
    Actual = [ Part || {Part, ?KV_PUT_REQ{}} <- RepairH ],
    Heads  = merge_heads([ Lineage || {_, {ok, Lineage}} <- H ]),
    RepairObject  = (catch build_merged_object(Heads, Objects)),
    Expected =expected_repairs(H),
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
    H = [ V || {_, V} <- History ],
    State1 = expect(H, State, 0, 0 , 0, 0, []),
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
notfound_or_error(NotFound, 0, 0, _Oks, _R) when NotFound > 0 ->
    notfound;
notfound_or_error(_NotFound, _NumNotDeleted, _Err, Oks, R) ->
    {r_val_unsatisfied, R, Oks}.

expect(H, State = #state{n = N, real_r = R, deleted = Deleted, notfound_is_ok = NotFoundIsOk,
                         basic_quorum = BasicQuorum},
       NotFounds, Oks, DelOks, Errs, Heads) ->
    Pending = N - (NotFounds + Oks + Errs),
    if  Oks >= R ->                     % we made quorum
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
                                                       Errs, Oks, R),
                        num_oks = Oks, del_oks = DelOks, num_errs = Errs};
        true ->
            case H of
                [] ->
                    State#state{exp_result = timeout, num_oks = Oks, num_errs = Errs};
                [timeout|Rest] ->
                    expect(Rest, State, NotFounds, Oks, DelOks, Errs, Heads);
                [notfound|Rest] ->
                    case NotFoundIsOk of
                        true ->
                            expect(Rest, State, NotFounds, Oks + 1, DelOks, Errs, Heads);
                        false ->
                            expect(Rest, State, NotFounds + 1, Oks, DelOks, Errs, Heads)
                    end;
                [error|Rest] ->
                    expect(Rest, State, NotFounds, Oks, DelOks, Errs + 1, Heads);
                [{ok,Lineage}|Rest] ->
                    IncDelOks = length([xx || {Lineage1, deleted} <- Deleted, Lineage1 == Lineage]),
                    expect(Rest, State, NotFounds, Oks + 1, DelOks + IncDelOks, Errs,
                           merge_heads(Lineage, Heads))
            end
    end.

    
    
-endif. % EQC
