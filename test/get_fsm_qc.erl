-module(get_fsm_qc).

-ifdef(EQC).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("riak_kv_vnode.hrl").

-compile(export_all).
-define(RING_KEY, riak_ring).
-define(DEFAULT_BUCKET_PROPS,
        [{allow_mult, false},
         {chash_keyfun, {riak_core_util, chash_std_keyfun}}]).

%% Generators

n(Max) ->
    choose(1, Max).


all_distinct(Xs) ->
    equals(lists:sort(Xs),lists:usort(Xs)).


ring(Partitions) ->
    riak_core_ring:fresh(Partitions, node()).



    


%%
%%         ancestor
%%       /     |    \
%%  brother   sister otherbrother
%%       \     |    /
%%         current
%%    
lineage() ->
    elements([current, ancestor, brother, sister, otherbrother]).
 
merge(ancestor, Lineage) -> Lineage;
merge(Lineage, ancestor) -> Lineage;
merge(_, current)        -> current;
merge(current, _)        -> current;
merge(otherbrother, _)   -> otherbrother;
merge(_, otherbrother)   -> otherbrother;
merge(sister, _)         -> sister;
merge(_, sister)         -> sister;
merge(brother, _)        -> brother;
merge(_, brother)        -> brother.

merge([Lin]) ->
    Lin;
merge([Lin|Lins]) ->
    merge(Lin, merge(Lins)).

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



prop_len() ->
    ?FORALL({R, Ps}, {choose(1, 10), fsm_eqc_util:partvals()},
        collect({R, length(Ps)}, true)
    ).

prop_basic_get() ->
    ?FORALL({RSeed,NQdiff,Objects,ReqId,PartVals,NodeStatus0},
            {fsm_eqc_util:largenat(),choose(0,4096),
             fsm_eqc_util:riak_objects(), noshrink(largeint()),
             fsm_eqc_util:partvals(),fsm_eqc_util:some_up_node_status(10)},
    begin
        N = length(PartVals),
        R = (RSeed rem N) + 1,
        Q = fsm_eqc_util:make_power_of_two(N + NQdiff),
        NodeStatus = fsm_eqc_util:cycle(Q, NodeStatus0),
        Ring = fsm_eqc_util:reassign_nodes(NodeStatus,
                                           riak_core_ring:fresh(Q, node())),
                              
        

        ok = gen_server:call(riak_kv_vnode_master,
                         {set_data, Objects, PartVals}),

        mochiglobal:put(?RING_KEY, Ring),

        application:set_env(riak_core,
                            default_bucket_props,
                            [{n_val, N}
                             |?DEFAULT_BUCKET_PROPS]),
    
        [{_,Object}|_] = Objects,
    
        {ok, GetPid} = riak_kv_get_fsm:start(ReqId,
                            riak_object:bucket(Object),
                            riak_object:key(Object),
                            R,
                            200,
                            self()),

        ok = riak_kv_test_util:wait_for_pid(GetPid),
        % Give read repairs and deletes a chance to go through
        timer:sleep(5),
        Res = fsm_eqc_util:wait_for_req_id(ReqId),
        History = get_fsm_qc_vnode_master:get_history(),
        RepairHistory = get_fsm_qc_vnode_master:get_put_history(),
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
        ExpectedN  = lists:min([N, length([xx || up <- NodeStatus])]),
        Expected   = expect(Objects, H, N, R),
        %% A perfect preference list has all owner partitions available
        DocIdx = riak_core_util:chash_key({riak_object:bucket(Object),
                                           riak_object:key(Object)}),
        Preflist = lists:sublist(riak_core_ring:preflist(DocIdx, Ring), N),
        PerfectPreflist = lists:all(fun({_Idx,Node}) -> Node =:= node() end, Preflist),
        ?WHENFAIL(
            begin
                io:format("Ring: ~p~nRepair: ~p~nHistory: ~p~n",
                          [Ring, RepairHistory, History]),
                io:format("Result: ~p~nExpected: ~p~nNode status: ~p~nDeleted objects: ~p~n",
                          [Res, Expected, NodeStatus, Deleted]),
                io:format("N: ~p~nR: ~p~nQ: ~p~n",
                          [N, R, Q]),
                io:format("H: ~p~nOk: ~p~nNotFound: ~p~nNoReply: ~p~n",
                          [H, Ok, NotFound, NoReply])
            end,
            conjunction(
                [{result, Res =:= Expected},
                 {n_value, equals(length(History), ExpectedN)},
                 {repair, check_repair(Objects, RepairHistory, History)},
                 {delete,  check_delete(Objects, RepairHistory, History, PerfectPreflist)},
                 {distinct, all_distinct(Partitions)}
                ]))
    end).


test() ->
    test(100).

test(N) ->
    quickcheck(numtests(N, prop_basic_get())).

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
                                %% io:format("AllDeleted Lineage=~p Obj=~p Rc=~p\n", [Lineage, Obj, Rc]),
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
   

build_merged_object(Heads, Objects) ->
    Lineage = merge(Heads),
    Object  = proplists:get_value(Lineage, Objects),
    Vclock  = vclock:merge(
                [ riak_object:vclock(proplists:get_value(Head, Objects))
                    || Head <- Heads ]),
   riak_object:set_vclock(Object, Vclock).

expect(Objects,History,N,R) ->
    case expect(History,N,R,0,0,0,[]) of
        {ok, Heads} ->
            case riak_kv_util:obj_not_deleted(build_merged_object(Heads, Objects)) of
                undefined -> {error, notfound};
                Obj       -> {ok, Obj}
            end;
        Err ->
            {error, Err}
    end.

notfound_or_error(0, Err) ->
    lists:duplicate(Err, error);
notfound_or_error(_NotFound, _Err) ->
    notfound.

expect(H, N, R, NotFounds, Oks, Errs, Heads) ->
    Pending = N - (NotFounds + Oks + Errs),
    if  Oks >= R ->                     % we made quorum
            {ok, Heads};
        (NotFounds + Errs)*2 > N orelse % basic quorum
        Pending + Oks < R ->            % no way we'll make quorum
            notfound_or_error(NotFounds, Errs);
        true ->
            case H of
                [] ->
                    timeout;
                [timeout|Rest] ->
                    expect(Rest, N, R, NotFounds, Oks, Errs, Heads);
                [notfound|Rest] ->
                    expect(Rest, N, R, NotFounds + 1, Oks, Errs, Heads);
                [error|Rest] ->
                    expect(Rest, N, R, NotFounds, Oks, Errs + 1, Heads);
                [{ok,Lineage}|Rest] ->
                    expect(Rest, N, R, NotFounds, Oks + 1, Errs,
                           merge_heads(Lineage, Heads))
            end
    end.

    
eqc_test_() ->
    {spawn,
    {timeout, 20, ?_test(
        begin
            fsm_eqc_util:start_mock_servers(),
            ?assert(test(30))
        end)
    }}.

%% Call unused callback functions to clear them in the coverage
%% checker so the real code stands out.
coverage_test() ->
    riak_kv_test_util:call_unused_fsm_funs(riak_kv_get_fsm).
    
-endif. % EQC
