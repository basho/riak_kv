-module(get_fsm_qc).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("riak_kv_vnode.hrl").

-compile(export_all).
-define(DEFAULT_BUCKET_PROPS,
        [{allow_mult, false},
         {chash_keyfun, {riak_core_util, chash_std_keyfun}}]).
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).

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

all_distinct(Xs) ->
    equals(lists:sort(Xs),lists:usort(Xs)).


ring(Partitions) ->
    riak_core_ring:fresh(Partitions, node()).


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

prop_len() ->
    ?FORALL({R, Ps}, {choose(1, 10), fsm_eqc_util:partvals()},
        collect({R, length(Ps)}, true)
    ).

r_seed() ->
    frequency([{50, quorum},
               {10, one},
               {10, all},
               {10, fsm_eqc_util:largenat()},
               { 1, garbage}]).

detail() -> frequency([{1, timing},
                       {1, not_a_detail}]).
    
details() -> frequency([{10, true}, %% All details requested
                        {10, list(detail())},
                        { 1, false}]).
    
option() -> 
    frequency([{1, {details, details()}}]).

options() ->
    list(option()).

prop_basic_get() ->
    ?FORALL({RSeed,Options0, Objects,ReqId,VGetResps},
            {r_seed(), options(),
             fsm_eqc_util:riak_objects(), noshrink(largeint()),
             vnodegetresps()},
    begin
        N = length(VGetResps),
        {R, RealR} = r_val(N, 1000000, RSeed),
        PL2 = make_preflist2(VGetResps, 1, []),
        PartVals = make_partvals(VGetResps, []),
        fsm_eqc_vnode:set_data(Objects, PartVals),
        ok = gen_server:call(riak_kv_vnode_master,
                             {set_data, Objects, PartVals}),
        
        BucketProps = [{n_val, N}
                       |?DEFAULT_BUCKET_PROPS],

        %% The app set_env will probably go away.
        application:set_env(riak_core,
                            default_bucket_props,
                            BucketProps),
        
        [{_,Object}|_] = Objects,
        
        Options = [{r, R}, {timeout, 200} | Options0],

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
        % Give read repairs and deletes a chance to go through
        %timer:sleep(5),
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
        H          = [ V || {_, V} <- History ],
        Expected  = expect(Objects, Deleted, H, N, R, RealR),
        ExpectedN = case Expected of
                        {error, {n_val_violation, _}} ->
                            0;
                        {error, {r_val_violation, _}} ->
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
                io:format("Repair: ~p~nHistory: ~p~n",
                          [RepairHistory, History]),
                io:format("Result: ~p~nExpected: ~p~nDeleted objects: ~p~n",
                          [Res, Expected, Deleted]),
                io:format("N: ~p~nR: ~p~nRealR: ~p~nOptions: ~p~nVGetResps: ~p~n",
                          [N, R, RealR, Options, VGetResps]),
                io:format("H: ~p~nOk: ~p~nNotFound: ~p~nNoReply: ~p~n",
                          [H, Ok, NotFound, NoReply])
            end,
            conjunction(
                [{result, equals(RetResult, Expected)},
                 {details, check_details(RetInfo, Options)},
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
              quorum -> (N div 2) + 1;
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

check_details(Details, Options) ->
    case proplists:get_value(details, Options, false) of
        false ->
            equals(undefined, Details);
        [] ->
            equals(undefined, Details);
        _ ->
            Details /= undefined
    end.

check_repair(Objects, RepairH, H) ->
    Actual = [ Part || {Part, ?KV_PUT_REQ{}} <- RepairH ],
    Heads  = merge_heads([ Lineage || {_, {ok, Lineage}} <- H ]),
    
    AllDeleted = lists:all(fun({_, {ok, Lineage}}) ->
                                Obj1 = proplists:get_value(Lineage, Objects),
                                riak_kv_util:is_x_deleted(Obj1);
                              (_) -> true
                           end, H),
    Expected = case AllDeleted of
            false -> expected_repairs(H);
            true  -> []  %% we don't expect read repair if everyone has a tombstone
        end,

    RepairObject  = (catch build_merged_object(Heads, Objects)),
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

    %% Used to have a check for for node() - no longer easy
    %% with new core vnode code.  Not sure it is necessary.
    AllDeleted = lists:all(fun({_Idx, {ok, Lineage}}) ->
                                Obj = proplists:get_value(Lineage, Objects),
                                Rc = riak_kv_util:is_x_deleted(Obj),
                                Rc;
                              ({_Idx, notfound}) -> true;
                              ({_, error})    -> false;
                              ({_, timeout})  -> false
                           end, H),

    HasOk = lists:any(fun({_, {ok, _}}) -> true;
                         (_) -> false
                      end, H),

    Expected = case AllDeleted andalso HasOk andalso PerfectPreflist of
        true  -> [ P || {P, _} <- H ];  %% send deletes to notfound nodes as well
        false -> []
    end,
    ?WHENFAIL(io:format("Objects: ~p\nExpected: ~p\nDeletes: ~p\nAllDeleted: ~p\nHasOk: ~p\nH: ~p\n",
                        [Objects, Expected, Deletes, AllDeleted, HasOk, H]),
              equals(lists:sort(Expected), lists:sort(Deletes))).
   

build_merged_object([], _Objects) ->
    undefined;
build_merged_object(Heads, Objects) ->
    Lineage = fsm_eqc_util:merge(Heads),
    Object  = proplists:get_value(Lineage, Objects),
    Vclock  = vclock:merge(
                [ riak_object:vclock(proplists:get_value(Head, Objects))
                    || Head <- Heads ]),
   riak_object:set_vclock(Object, Vclock).

expect(_Object, _Deleted, _History, _N, R, _RealR) when R =:= garbage ->
    {error, {r_val_violation, garbage}};
expect(_Object, _Deleted, _History, N, _R, RealR) when RealR > N ->
    {error, {n_val_violation, N}};
expect(Objects, Deleted, History,N, _R, RealR) ->
    case expect(Deleted, History,N,RealR,0,0,0,0,[]) of
        {ok, Heads} ->
            case riak_kv_util:obj_not_deleted(build_merged_object(Heads, Objects)) of
                undefined -> {error, notfound};
                Obj       -> {ok, Obj}
            end;
        Err ->
            {error, Err}
    end.

%% decide on error message - if only got notfound messages, return notfound
%% otherwise let caller know R value was not met.
notfound_or_error(NotFound, 0, 0, _R) when NotFound > 0 ->
    notfound;
notfound_or_error(_NotFound, Oks, _Err, R) ->
    {r_val_unsatisfied, R, Oks}.

expect(D, H, N, R, NotFounds, Oks, DelOks, Errs, Heads) ->
    Pending = N - (NotFounds + Oks + Errs),
    if  Oks >= R ->                     % we made quorum
            {ok, Heads};
        (NotFounds + Errs)*2 > N orelse % basic quorum
        Pending + Oks < R ->            % no way we'll make quorum
            %% Adjust counts to make deleted objects count towards notfound.
            notfound_or_error(NotFounds + DelOks, Oks - DelOks, Errs, R);
        true ->
            case H of
                [] ->
                    timeout;
                [timeout|Rest] ->
                    expect(D, Rest, N, R, NotFounds, Oks, DelOks, Errs, Heads);
                [notfound|Rest] ->
                    expect(D, Rest, N, R, NotFounds + 1, Oks, DelOks, Errs, Heads);
                [error|Rest] ->
                    expect(D, Rest, N, R, NotFounds, Oks, DelOks, Errs + 1, Heads);
                [{ok,Lineage}|Rest] ->
                    IncDelOks = length([xx || {Lineage1, deleted} <- D, Lineage1 == Lineage]),
                    expect(D, Rest, N, R, NotFounds, Oks + 1, DelOks + IncDelOks, Errs,
                           merge_heads(Lineage, Heads))
            end
    end.

    
    
-endif. % EQC
