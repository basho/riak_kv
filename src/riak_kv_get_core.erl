%% -------------------------------------------------------------------
%%
%% riak_kv_get_core: Riak get logic
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
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
-module(riak_kv_get_core).
-export([init/10, update_init/2, head_merge/1,
            add_result/4, update_result/5, result_shortcode/1,
            enough/1, response/1, has_all_results/1, final_action/1, info/1]).
-export_type([getcore/0, result/0, reply/0, final_action/0]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type result() :: {ok, riak_object:riak_object()} |
                  {error, notfound} | % for dialyzer
                  {error, any()}.
-type reply() :: {ok, riak_object:riak_object()} |
                 {error, notfound} |
                 {error, any()} |
                 {fetch, list()}.
-type repair_reason() :: notfound | outofdate.
-type final_action() :: 
    nop |
    {read_repair,
        [{non_neg_integer(), repair_reason()}],
        riak_object:riak_object()} |
    {delete_repair,
        [{non_neg_integer(), repair_reason()}],
        riak_object:riak_object()} |
    delete.
-type idxresult() :: {non_neg_integer(), result()}.
-type idx_type() :: [{non_neg_integer, 'primary' | 'fallback'}].

-record(getcore, {n :: pos_integer(),
                  r :: pos_integer(),
                  pr :: pos_integer(),
                  ur :: non_neg_integer(), % updated reads
                  fail_threshold :: pos_integer(),
                  notfound_ok :: boolean(),
                  allow_mult :: boolean(),
                  deletedvclock :: boolean(),
                  %% NOTE: throughout this module it is expected these
                  %% results in the reverse order to which they are
                  %% received, fastest last at the end of the list
                  results = [] :: [idxresult()],
                  merged ::
                    {notfound, undefined} |
                        {tombstone, riak_object:riak_object()} |
                        {ok, riak_object:riak_object()} |
                        undefined,
                  num_ok = 0 :: non_neg_integer(),
                  num_pok = 0 :: non_neg_integer(),
                  num_notfound = 0 :: non_neg_integer(),
                  num_deleted = 0 :: non_neg_integer(),
                  num_fail = 0 :: non_neg_integer(),
                  num_upd = 0 :: non_neg_integer(),
                  idx_type :: idx_type(),
                  head_merge = false :: boolean(),
                  expected_fetchclock = false :: boolean()|vclock:vclock(),
                  node_confirms = 0 :: non_neg_integer(),
                  confirmed_nodes = []}).
-opaque getcore() :: #getcore{}.

%% ====================================================================
%% Public API
%% ====================================================================

%% Initialize a get and return an opaque get core context
-spec init(N::pos_integer(), R::pos_integer(), PR::pos_integer(),
           FailThreshold::pos_integer(), NotFoundOK::boolean(),
           AllowMult::boolean(), DeletedVClock::boolean(),
           IdxType::idx_type(),
           ExpClock::false|vclock:vclock(),
           NodeConfirms::non_neg_integer()) -> getcore().
init(N, R, PR, FailThreshold, NotFoundOk, AllowMult,
        DeletedVClock, IdxType, ExpClock, NodeConfirms) ->
    #getcore{n = N,
             r = case ExpClock of false -> R; _ -> N end,
             pr = PR,
             ur = 0,
             fail_threshold = FailThreshold,
             notfound_ok = NotFoundOk,
             allow_mult = AllowMult,
             deletedvclock = DeletedVClock,
             idx_type = IdxType,
             expected_fetchclock = ExpClock,
             node_confirms = NodeConfirms}.

%% Re-initialise a get to a restricted number of vnodes (that must all respond)
-spec update_init(N::pos_integer(), getcore()) -> getcore().
update_init(N, PrevGetCore) ->
    PrevGetCore#getcore{ur = N,
                        head_merge = true}.

%% Convert the get so that it is expecting to potentially receive the
%% responses to head requests (though for backwards compatibility these may
%% actually still be get responses)
-spec head_merge(getcore()) -> getcore().
head_merge(GetCore) ->
    GetCore#getcore{head_merge = true}.

%% Add a result for a vnode index NOTE: in other code downstream
%% (merge_heads for example) the reverse of the order in which results
%% arrive is assumed to be preserved in the get core
%% datastructure. i.e. first arriving at the end of the list, latest
%% arrival at the head.
-spec add_result(non_neg_integer(), result(), node(), getcore()) -> getcore().
add_result(Idx, {ok, RObj}, Node, GetCore0) ->
    GetCore = 
        case GetCore0#getcore.expected_fetchclock of
            false ->
                GetCore0;
            true ->
                GetCore0;
            ExpectedClock ->
                case vclock:descends(riak_object:vclock(RObj),
                                        ExpectedClock) of
                    true ->
                        GetCore0#getcore{expected_fetchclock = true};
                    false ->
                        GetCore0
                end
        end,
    {Dels, Result} = 
        case riak_kv_util:is_x_deleted(RObj) of
            true ->  {1, {ok, riak_object:spoof_getdeletedobject(RObj)}};
            false -> {0, {ok, RObj}}
        end,
    num_pr(GetCore#getcore{
            results = [{Idx, Result}|GetCore#getcore.results],
            merged = undefined,
            num_ok = GetCore#getcore.num_ok + 1,
            num_deleted = GetCore#getcore.num_deleted + Dels,
            confirmed_nodes =
                lists:usort([Node|GetCore#getcore.confirmed_nodes])},
            Idx);
add_result(Idx, {error, notfound} = Result, Node, GetCore) ->
    case GetCore#getcore.notfound_ok of
        true ->
            num_pr(GetCore#getcore{
                    results = [{Idx, Result}|GetCore#getcore.results],
                    merged = undefined,
                    num_ok = GetCore#getcore.num_ok + 1,
                    confirmed_nodes =
                        lists:usort([Node|GetCore#getcore.confirmed_nodes])},
                    Idx);
        _ ->
            GetCore#getcore{
                results = [{Idx, Result}|GetCore#getcore.results],
                merged = undefined,
                num_notfound = GetCore#getcore.num_notfound + 1}
    end;
add_result(Idx, {error, _Reason} = Result, _Node, GetCore) ->
    GetCore#getcore{
        results = [{Idx, Result}|GetCore#getcore.results],
        merged = undefined,
        num_fail = GetCore#getcore.num_fail + 1}.

%% Replace a result for a vnode index i.e. when the result had
%% previously been as a result of a HEAD, and there is now a result
%% from a GET. It will add any results which were not from the list of
%% updated indexes with add_result/3
-spec update_result(non_neg_integer(),
                    result(),
                    list(),
                    node(),
                    getcore()) -> getcore().
update_result(Idx, Result, IdxList, Node, GetCore) ->
    case lists:member(Idx, IdxList) of
        true ->
            % This results should always be OK
            UpdResults = lists:keyreplace(Idx,
                                            1,
                                            GetCore#getcore.results,
                                            {Idx, Result}),
            GetCore#getcore{results = UpdResults,
                                merged = undefined,
                                num_upd = GetCore#getcore.num_upd + 1};
        false ->
            % This is expected, sent n head requests originally, enough was
            % reached at r/pr - so if have a follow on GET may receive n-r
            % delayed HEADs while waiting
            % Add them to the result set - the result set will still be used
            % for read repair.  Will also detect if the last read was actually
            % a more upto date object
            add_result(Idx, Result, Node, GetCore)
    end.


result_shortcode({ok, _RObj})       -> 1;
result_shortcode({error, notfound}) -> 0;
result_shortcode(_)                 -> -1.

%% Check if enough results have been added to respond
-spec enough(getcore()) -> boolean().
%% Found expected clock
enough(#getcore{expected_fetchclock = true}) ->
    true;
%% Met quorum
enough(#getcore{r = R, ur = UR, pr= PR,
                    num_ok = NumOK, num_pok = NumPOK,
                    num_upd = NumUPD,
                    node_confirms = RequiredConfirms, confirmed_nodes = Nodes})
        when NumOK >= R andalso
                NumPOK >= PR andalso
                NumUPD >= UR andalso
                length(Nodes) >= RequiredConfirms ->
    true;
%% Too many failures
enough(#getcore{fail_threshold = FailThreshold, num_notfound = NumNotFound,
            num_fail = NumFail})
        when NumNotFound + NumFail >= FailThreshold ->
    true;
%% Got all N responses, and no updated reads outstanding - not waiting on
%% anything.
%% In this case there has been a failure to satisfy PR or node_confirms
%% but enough is known, so can return true to prompt error via response
%% rather than sit waiting for a timeout.
enough(#getcore{n = N, ur = UR, num_ok = NumOK, num_notfound = NumNotFound,
            num_fail = NumFail})
        when NumOK + NumNotFound + NumFail >= N andalso UR == 0 ->
    true;
%% Awaiting outstanding responses
enough(_) ->
    false.

%% Get success/fail response once enough results received
-spec response(getcore()) -> {reply(), getcore()}.
%% Met quorum for a standard get request/response
response(
    #getcore{
        node_confirms = RqdConfirms,
        confirmed_nodes = Nodes,
        expected_fetchclock = ExpClock} = GetCore)
            when (length(Nodes) < RqdConfirms andalso ExpClock =/= true) ->
    check_overload(
        {error, {insufficient_nodes, length(Nodes), need, RqdConfirms}},
        GetCore);
    %% Insufficient nodes confirmed
response(#getcore{r = R, num_ok = NumOK, pr= PR, num_pok = NumPOK,
                    expected_fetchclock = ExpClock, head_merge = HM} = GetCore)
        when 
            ((NumOK >= R andalso NumPOK >= PR)
                orelse ExpClock == true)
            andalso HM == false ->
    #getcore{results = Results, allow_mult=AllowMult,
        deletedvclock = DeletedVClock} = GetCore,
    {ObjState, MObj} = Merged = merge(Results, AllowMult),
    Reply = case ObjState of
        ok ->
            Merged; % {ok, MObj}
        tombstone when DeletedVClock ->
            {error, {deleted, riak_object:vclock(MObj), MObj}};
        _ -> % tombstone or notfound or expired
            {error, notfound}
    end,
    {Reply, GetCore#getcore{merged = Merged}};
%% Met quorum, but the request had asked only for head responses
response(#getcore{r = R, num_ok = NumOK, pr= PR, num_pok = NumPOK,
                    expected_fetchclock = ExpClock} = GetCore)
        when (NumOK >= R andalso NumPOK >= PR) orelse ExpClock == true ->
    #getcore{results = Results, allow_mult=AllowMult,
        deletedvclock = DeletedVClock} = GetCore,
    Merged = merge_heads(Results, AllowMult),
    case Merged of
        {ok, _MergedObj} ->
            {Merged, GetCore#getcore{merged = Merged}}; % {ok, MObj}
        {tombstone, MObj} when DeletedVClock ->
            {{error, {deleted, riak_object:vclock(MObj), MObj}},
                GetCore#getcore{merged = Merged}};
        {fetch, IdxList} ->
            % A list of vnode indexes to be fetched from using a GET request
            {{fetch, IdxList}, GetCore};
        _ -> % tombstone or notfound or expired
            {{error, notfound}, GetCore#getcore{merged = Merged}}
    end;
%% everything was either a tombstone or a notfound
response(#getcore{num_notfound = NumNotFound, num_ok = NumOK,
        num_deleted = NumDel, num_fail = NumFail} = GetCore)
        when NumNotFound + NumDel > 0, NumOK - NumDel == 0, NumFail == 0  ->
    {{error, notfound}, GetCore};
%% We've satisfied R, but not PR
response(#getcore{r = R, pr = PR, num_ok = NumR, num_pok = NumPR} = GetCore)
      when PR > 0, NumPR < PR, NumR >= R ->
    check_overload({error, {pr_val_unsatisfied, PR,  NumPR}}, GetCore);
%% PR and/or R are unsatisfied, but PR is more restrictive
response(#getcore{r = R, num_pok = NumPR, pr = PR} = GetCore) when PR >= R ->
    check_overload({error, {pr_val_unsatisfied, PR,  NumPR}}, GetCore);
%% PR and/or R are unsatisfied, but R is more restrictive
response(#getcore{r = R, num_ok = NumR} = GetCore) ->
    check_overload({error, {r_val_unsatisfied, R,  NumR}}, GetCore).

%% Check for vnode overload
check_overload(Response, GetCore = #getcore{results=Results}) ->
    case [x || {_,{error, overload}} <- Results] of
        [] ->
            {Response, GetCore};
        _->
            {{error, overload}, GetCore}
    end.

%% Check if all expected results have been added
-spec has_all_results(getcore()) -> boolean().
has_all_results(#getcore{n = N, num_ok = NOk,
                         num_fail = NFail, num_notfound = NNF}) ->
    NOk + NFail + NNF >= N.


-spec isnot_head(tuple()) -> boolean().
%% Is a result an object head
isnot_head({ok, Robj}) ->
    not riak_object:is_head(Robj);
isnot_head(_Result) ->
    false.

%% Decide on any post-response actions
%% nop - do nothing
%% {readrepair, Indices, MObj} - send read repairs iff any vnode has ancestor data
%%                               (including tombstones)
%% delete - issue deletes if all vnodes returned tombstones.  This needs to be
%%          supplemented with a check that the vnodes were all primaries.
%%
-spec final_action(getcore()) -> {final_action(), getcore()}.
final_action(GetCore = #getcore{n = N, merged = Merged0, results = Results,
                                allow_mult = AllowMult}) ->
    PredFun = fun({_Idx, Res}) -> isnot_head(Res) end,
    {FilteredResults, _ResultHEADs} = lists:partition(PredFun, Results),
    Merged = 
        case Merged0 of
            undefined ->
                % We will only repair from a fetched object (not a head_only
                % object).  It is possible that we may have, when n > r, 
                % received a better HEAD response after r has been fulfilled, 
                % and after the GET response was received. This will not now 
                % reliably be read repaired.  We only read repair a superior 
                % object discovered up to 'enough' and the receipt of the GET
                % response.
                merge(FilteredResults, AllowMult);
            _ ->
                Merged0
            end,
    {ObjState, MObj} = Merged,

    ReadRepairs =
        case ObjState of
            notfound ->
                [];
            _ -> % ok or tombstone
                %% Any object that is strictly descended by
                %% the merge result must be read-repaired,
                %% this ensures even tombstones get repaired
                %% so reap will work. We join the list of
                %% dominated (in need of repair) indexes and
                %% the list of not_found (in need of repair)
                %% indexes.
                [{Idx, outofdate} || {Idx, {ok, RObj}} <- Results,
                        riak_object:strict_descendant(MObj, RObj)] ++
                    [{Idx, notfound} || {Idx, {error, notfound}} <- Results]
        end,
    AllResults = length([xx || {_Idx, {ok, _RObj}} <- Results]) == N,
    Action =
        case ReadRepairs of
            [] when ObjState == tombstone, AllResults ->
                delete;
            [] ->
               nop;
            _ when ObjState == tombstone, AllResults ->
                {delete_repair, ReadRepairs, MObj};
            _ ->
                {read_repair, ReadRepairs, MObj}
        end,
    {Action, GetCore#getcore{merged = Merged}}.


%% Return request info
-spec info(undefined | getcore()) -> [{vnode_oks, non_neg_integer()} |
                                      {vnode_errors, [any()]}].

info(undefined) ->
    []; % make uninitialized case easier
info(#getcore{num_ok = NumOks, num_fail = NumFail, results = Results}) ->
    Oks = [{vnode_oks, NumOks}],
    case NumFail of
        0 ->
            Oks;
        _ ->
            Errors = [Reason || {_Idx, {error, Reason}} <- Results,
                                Reason /= undefined],
            [{vnode_errors, Errors} | Oks]
    end.

%% ====================================================================
%% Internal functions
%% ====================================================================
merge(Replies, AllowMult) ->
    RObjs = [RObj || {_I, {ok, RObj}} <- Replies],
    case RObjs of
        [] ->
            {notfound, undefined};
        _ ->
            Merged = riak_object:reconcile(RObjs, AllowMult), % include tombstones
            case riak_kv_util:is_x_deleted(Merged) of
                true ->
                    {tombstone, Merged};
                _ ->
                    {ok, Merged}
            end
    end.

%% @private - replaces the merge method when an initial HEAD request is used
%% not a GET request.  The results returned will be either normal objects (if
%% a backend not supporting HEAD was called, or the operation was an UPDATE),
%% or body-less objects.
%%
-spec merge_heads(list(result()), boolean()) ->
        {notfound, undefined}|
        {tombstone, riak_object:riak_object()}|{ok, riak_object:riak_object()}|
        {fetch, list(non_neg_integer())}.
merge_heads(Replies, AllowMult) ->
    % Replies should be a list of [{Idx, {ok, RObj}]
    IdxObjs = [{I, {ok, RObj}} || {I, {ok, RObj}} <- Replies],
    % Those that don't pattern match will be not_found
    case IdxObjs of
        [] ->
            {notfound, undefined};
        _ ->
            {BestReplies, FetchIdxObjL} = riak_object:find_bestobject(IdxObjs),
            case FetchIdxObjL of
                [] ->
                    merge(BestReplies, AllowMult);
                IdxL ->
                    {fetch, lists:map(fun({Idx, _Rsp}) ->  Idx end, IdxL)}
            end
    end.




%% @private Checks IdxType to see if Idx is a primary.
%% If the Idx is not in the IdxType the world must be
%% resizing (ring expanding). In that case, Idx is
%% assumed to be a primary, since only primaries forward.
is_primary_response(Idx, IdxType) ->
    case lists:keyfind(Idx, 1, IdxType) of
        false -> true;
        {Idx, Status} -> Status == primary
    end.


%% @private Increment PR, if appropriate
num_pr(GetCore = #getcore{num_pok=NumPOK, idx_type=IdxType}, Idx) ->
    case is_primary_response(Idx, IdxType) of
        true ->
            GetCore#getcore{num_pok=NumPOK+1};
        false ->
            GetCore
    end.

%% @private Print a warning if objects are not equal. Only called on case of no read-repair
%% This situation could happen with pre 2.1 vclocks in very rare cases. Fixing the object
%% requires the user to rewrite the object in 2.1+ of Riak. Logic is enabled when capabilities
%% returns a version(all nodes at least 2.2) and the entropy_manager is not yet version 0
% maybe_log_old_vclock(Results) ->
%     case riak_core_capability:get({riak_kv, object_hash_version}, legacy) of
%         legacy ->
%             ok;
%         0 ->
%             Version = riak_kv_entropy_manager:get_version(),
%             case [RObj || {_Idx, {ok, RObj}} <- Results] of
%                 [] ->
%                     ok;
%                 [_] ->
%                     ok;
%                 _ when Version == 0 ->
%                     ok;
%                 [R1|Rest] ->
%                     case [RObj || RObj <- Rest, not riak_object:equal(R1, RObj)] of
%                         [] ->
%                             ok;
%                         _ ->
%                             object:warning("Bucket: ~p Key: ~p should be rewritten to guarantee
%                               compatability with AAE version 0",
%                                 [riak_object:bucket(R1),riak_object:key(R1)])
%                     end
%             end;
%         _ ->
%             ok
%     end.

-ifdef(TEST).

update_test() ->
    B = <<"buckets are binaries">>,
    K = <<"keys are binaries">>,
    V = <<"Some value">>,
    InObject = riak_object:new(B, K, V,
                                dict:from_list([{<<"X-Riak-Val-Encoding">>, 2},
                                {<<"X-Foo_MetaData">>, "Foo"}])),
    Obj3 = riak_object:convert_object_to_headonly(B, K, InObject),

    GC0 = #getcore{n= 3, r = 2, pr=0,
                    fail_threshold = 1, num_ok = 2, num_pok = 0,
                    num_notfound = 0, num_deleted = 0, num_fail = 0,
                    idx_type = [],
                    results = [{1, {ok, fake_head1}}, {2, {ok, fake_head2}}]},
    GC1 = update_init(1, GC0),
    GC2 = update_result(3, {ok, Obj3}, [2], node(), GC1),
    ?assertMatch(3, GC2#getcore.num_ok),
    ?assertMatch(2, GC2#getcore.r),
    ?assertMatch(1, GC2#getcore.ur),
    ?assertMatch(0, GC2#getcore.num_upd),
    ?assertMatch(3, length(GC2#getcore.results)),
    GC3 = update_result(2, {ok, fake_get2}, [2], node(), GC2),
    ?assertMatch(3, GC3#getcore.num_ok),
    ?assertMatch(2, GC3#getcore.r),
    ?assertMatch(1, GC3#getcore.ur),
    ?assertMatch(1, GC3#getcore.num_upd),
    ?assertMatch(3, length(GC3#getcore.results)),
    ?assertMatch([{3, {ok, Obj3}},
                        {1, {ok, fake_head1}},
                        {2, {ok, fake_get2}}],
                    GC3#getcore.results).

increment_vclock(Object, ClientId) ->
    NewClock = vclock:increment(ClientId, riak_object:vclock(Object)),
    riak_object:set_vclock(Object, NewClock).

enough_expectedclock_test() ->
    B = <<"B">>,
    K = <<"K">>,
    V = <<"V">>,
    V0 = <<"V0">>,
    V1 = <<"V1">>,
    V2 = <<"V2">>,
    V3 = <<"V3">>,

    Obj0 = 
        increment_vclock(
            riak_object:new(B, K, V,
                                dict:from_list([{<<"X-Riak-Val-Encoding">>, 2},
                                {<<"X-Foo_MetaData">>, "Foo"}])),
            a),
    Obj1 =
        increment_vclock(
            riak_object:apply_updates(riak_object:update_value(Obj0, V0)),
            a),
    
    Obj2 =
        increment_vclock(
            riak_object:apply_updates(riak_object:update_value(Obj1, V1)),
            b),
    
    Obj3 =
        increment_vclock(
            riak_object:apply_updates(riak_object:update_value(Obj2, V2)),
            c),
    
    Obj4 =
        increment_vclock(
            riak_object:apply_updates(riak_object:update_value(Obj3, V3)),
            a),

    
    ExpectedClock = riak_object:vclock(Obj3),

    GC0 = #getcore{n= 3, r = 3, pr=0, ur=0,
                    fail_threshold = 1, num_ok = 0, num_pok = 0,
                    num_notfound = 0, num_deleted = 0, num_fail = 0,
                    idx_type = [],
                    results = [],
                    allow_mult = true,
                    expected_fetchclock = ExpectedClock},
    GC1 = add_result(3, {ok, Obj1}, node(), GC0),
    ?assertEqual(false, enough(GC1)),
    ?assertEqual({{error, {r_val_unsatisfied, 3, 1}}, GC1}, response(GC1)),
    
    GC2A = add_result(1, {ok, Obj3}, node(), GC1),
    ?assertEqual(true, enough(GC2A)),
    ?assertEqual({ok, Obj3}, element(1, response(GC2A))),
    
    GC2B = add_result(1, {ok, Obj4}, node(), GC1),
    ?assertEqual(true, enough(GC2B)),
    ?assertEqual({ok, Obj4}, element(1, response(GC2B))),
    
    GC2C = add_result(1, {ok, Obj2}, node(), GC1),
    ?assertEqual(false, enough(GC2C)),
    ?assertEqual({{error, {r_val_unsatisfied, 3, 2}}, GC2C}, response(GC2C)),

    GC3C = add_result(2, {ok, Obj1}, node(), GC2C),
    io:format("GC32 ~w~n", [GC3C]),
    ?assertEqual(true, enough(GC3C)),
    ?assertEqual({ok, Obj2}, element(1, response(GC3C))).


%% simple sanity tests
enough_test_() ->
    [
        {"Checking R",
            fun() ->
                    %% cannot meet R
                    ?assertEqual(false, enough(#getcore{n= 3,
                                r = 3, pr=0, ur=0,
                                fail_threshold = 1, num_ok = 0, num_pok = 0,
                                num_notfound = 0, num_deleted = 0,
                                num_fail = 0})),
                    ?assertEqual(false, enough(#getcore{n= 3,
                                r = 3, pr=0, ur=0,
                                fail_threshold = 1, num_ok = 1, num_pok = 0,
                                num_notfound = 0, num_deleted = 0,
                                num_fail = 0})),
                    ?assertEqual(false, enough(#getcore{n= 3,
                                r = 3, pr=0, ur=0,
                                fail_threshold = 1, num_ok = 2, num_pok = 0,
                                num_notfound = 0, num_deleted = 0,
                                num_fail = 0})),
                    %% met R
                    ?assertEqual(true, enough(#getcore{n= 3,
                                r = 3, pr=0, ur=0,
                                fail_threshold = 1, num_ok = 3, num_pok = 0,
                                num_notfound = 0, num_deleted = 0,
                                num_fail = 0})),
                    %% met R - missing updated
                    ?assertEqual(false, enough(#getcore{n= 3,
                                r = 3, pr=0, ur=1,
                                fail_threshold = 1, num_ok = 3, num_pok = 0,
                                num_notfound = 0, num_deleted = 0, num_upd=0,
                                num_fail = 0})),
                    ?assertEqual(true, enough(#getcore{n= 3,
                                r = 3, pr=0, ur=1,
                                fail_threshold = 1, num_ok = 3, num_pok = 0,
                                num_notfound = 0, num_deleted = 0, num_upd=1,
                                num_fail = 0})),
                    %% too many failures
                    ?assertEqual(true, enough(#getcore{n= 3,
                                r = 3, pr=0, ur=0,
                                fail_threshold = 1, num_ok = 2, num_pok = 0,
                                num_notfound = 0, num_deleted = 0,
                                num_fail = 1})),
                    %% Seen expected clock
                    ?assertEqual(true, enough(#getcore{n= 3,
                                r = 3, pr=0, ur=1,
                                fail_threshold = 1, num_ok = 3, num_pok = 0,
                                num_notfound = 0, num_deleted = 0, num_upd=0,
                                num_fail = 0, expected_fetchclock = true})),
                    %% Not seen expected clock
                    ?assertEqual(false, enough(#getcore{n= 3,
                                r = 3, pr=0, ur=1,
                                fail_threshold = 1, num_ok = 3, num_pok = 0,
                                num_notfound = 0, num_deleted = 0, num_upd=0,
                                num_fail = 0, expected_fetchclock = []})),
                    ok
            end},
        {"Checking PR",
            fun() ->
                    %% cannot meet PR
                    ?assertEqual(false, enough(#getcore{n= 3,
                                r = 0, pr=3, ur=0,
                                fail_threshold = 1, num_ok = 1, num_pok = 1,
                                num_notfound = 0, num_deleted = 0,
                                num_fail = 0})),
                    ?assertEqual(false, enough(#getcore{n= 3,
                                r = 0, pr=3, ur=0,
                                fail_threshold = 1, num_ok = 2, num_pok = 2,
                                num_notfound = 0, num_deleted = 0,
                                num_fail = 0})),
                    %% met PR
                    ?assertEqual(true, enough(#getcore{n= 3,
                                r = 0, pr=3, ur=0,
                                fail_threshold = 1, num_ok = 3, num_pok = 3,
                                num_notfound = 0, num_deleted = 0,
                                num_fail = 0})),
                    %% met R but not PR
                    ?assertEqual(true, enough(#getcore{n= 3,
                                r = 0, pr=3, ur=0,
                                fail_threshold = 3, num_ok = 3, num_pok = 2,
                                num_notfound = 0, num_deleted = 0,
                                num_fail = 0})),
                    ok
            end}
    ].

response_test_() ->
    {setup,
     fun() ->
             meck:new(riak_core_bucket),
             meck:expect(riak_core_bucket, get_bucket,
                         fun(_) -> [] end),
             ok
     end,
     fun(_) ->
             meck:unload(riak_core_bucket)
     end,
     [
        {"Requirements met",
            fun() ->
                    RObj = riak_object:new(<<"foo">>, <<"bar">>, <<"baz">>),
                    ?assertMatch({{ok, RObj}, _},
                        response(#getcore{n= 3, r = 3, pr=0,
                                fail_threshold = 1, num_ok = 3, num_pok = 0,
                                allow_mult = false,
                                num_notfound = 0, num_deleted = 0,
                                num_fail = 0,
                                results= [
                                    {1, {ok, RObj}},
                                    {2, {ok, RObj}},
                                    {3, {ok, RObj}}]})),
                    ok
            end},
        {"R unsatisfied",
            fun() ->
                    RObj = riak_object:new(<<"foo">>, <<"bar">>, <<"baz">>),
                    ?assertMatch({{error, {r_val_unsatisfied, 3, 2}}, _},
                        response(#getcore{n= 3, r = 3, pr=0,
                                fail_threshold = 1, num_ok = 2, num_pok = 2,
                                allow_mult = false,
                                num_notfound = 0, num_deleted = 0,
                                num_fail = 1,
                                results= [
                                    {1, {ok, RObj}},
                                    {3, {ok, RObj}}]})),
                    ok
            end},
        {"PR unsatisfied",
            fun() ->
                    RObj = riak_object:new(<<"foo">>, <<"bar">>, <<"baz">>),
                    ?assertMatch({{error, {pr_val_unsatisfied, 3, 2}}, _},
                        response(#getcore{n= 3, r = 0, pr=3,
                                fail_threshold = 1, num_ok = 3, num_pok = 2,
                                allow_mult = false,
                                num_notfound = 0, num_deleted = 0,
                                num_fail = 0,
                                results= [
                                    {1, {ok, RObj}},
                                    {2, {ok, RObj}},
                                    {4, {ok, RObj}}]})), %% from a fallback
                    ok
            end},
        {"PR unsatisfied but expected clock found",
            fun() ->
                    RObj = riak_object:new(<<"foo">>, <<"bar">>, <<"baz">>),
                    ?assertMatch({{ok, RObj}, _},
                        response(#getcore{n= 3, r = 0, pr=3,
                                fail_threshold = 1, num_ok = 3, num_pok = 2,
                                allow_mult = false,
                                num_notfound = 0, num_deleted = 0,
                                num_fail = 0,
                                expected_fetchclock = true,
                                results= [
                                    {1, {ok, RObj}},
                                    {2, {ok, RObj}},
                                    {4, {ok, RObj}}]})), %% from a fallback
                    ok
            end},
        {"R & PR unsatisfied, PR >= R",
            fun() ->
                    RObj = riak_object:new(<<"foo">>, <<"bar">>, <<"baz">>),
                    ?assertMatch({{error, {pr_val_unsatisfied, 3, 1}}, _},
                        response(#getcore{n= 3, r = 2, pr=3,
                                fail_threshold = 1, num_ok = 1, num_pok = 1,
                                allow_mult = false,
                                num_notfound = 0, num_deleted = 0,
                                num_fail = 2,
                                results= [
                                    {1, {ok, RObj}},
                                    {2, {error, foo}},
                                    {3, {error, foo}}]})),
                    ok
            end},
        {"R & PR unsatisfied, R > PR",
            fun() ->
                    RObj = riak_object:new(<<"foo">>, <<"bar">>, <<"baz">>),
                    ?assertMatch({{error, {r_val_unsatisfied, 3, 1}}, _},
                        response(#getcore{n= 3, r = 3, pr=2,
                                fail_threshold = 1, num_ok = 1, num_pok = 1,
                                allow_mult = false,
                                num_notfound = 0, num_deleted = 0,
                                num_fail = 2,
                                results= [
                                    {1, {ok, RObj}},
                                    {2, {error, foo}},
                                    {3, {error, foo}}]})),
                    ok
            end},

        {"All results notfound/tombstone",
            fun() ->
                    RObj = riak_object:new(<<"foo">>, <<"bar">>, <<"baz">>,
                        dict:from_list([{<<"X-Riak-Deleted">>, true}])),
                    ?assertMatch({{error, notfound}, _},
                        response(#getcore{n= 3, r = 3, pr=0,
                                fail_threshold = 1, num_ok = 1, num_pok = 0,
                                allow_mult = false,
                                num_notfound = 2, num_deleted = 1,
                                num_fail = 0,
                                results= [
                                    {1, {ok, RObj}},
                                    {2, {error, notfound}},
                                    {3, {error, notfound}}]})),
                    ok
            end},
        {"Confirms not met",
            fun() ->
                    RObj = riak_object:new(<<"foo">>, <<"bar">>, <<"baz">>),
                    ?assertMatch({{error,
                                    {insufficient_nodes, 1, need, 2}}, _},
                        response(#getcore{n= 3, r = 3, pr=0,
                                fail_threshold = 1, num_ok = 3, num_pok = 0,
                                allow_mult = false,
                                num_notfound = 0, num_deleted = 0,
                                num_fail = 0,
                                node_confirms = 2,
                                confirmed_nodes = [node()],
                                results= [
                                    {1, {ok, RObj}},
                                    {2, {ok, RObj}},
                                    {3, {ok, RObj}}]})),
                    ok
            end}
    ]}.
-endif.
