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
-export([init/8, update_init/2, head_merge/1,
            add_result/3, update_result/4, result_shortcode/1,
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
                 {error, any()}.
-type repair_reason() :: notfound | outofdate.
-type final_action() :: nop |
                        {read_repair, [{non_neg_integer() | repair_reason()}], riak_object:riak_object()} |
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
                  results = [] :: [idxresult()],
                  merged :: {notfound | tombstone | ok,
                             riak_object:riak_object() | undefined},
                  num_ok = 0 :: non_neg_integer(),
                  num_pok = 0 :: non_neg_integer(),
                  num_notfound = 0 :: non_neg_integer(),
                  num_deleted = 0 :: non_neg_integer(),
                  num_fail = 0 :: non_neg_integer(),
                  num_upd = 0 :: non_neg_integer(),
                  idx_type :: idx_type(),
                  head_merge = false :: boolean()}).
-opaque getcore() :: #getcore{}.

%% ====================================================================
%% Public API
%% ====================================================================

%% Initialize a get and return an opaque get core context
-spec init(N::pos_integer(), R::pos_integer(), PR::pos_integer(),
           FailThreshold::pos_integer(), NotFoundOK::boolean(),
           AllowMult::boolean(), DeletedVClock::boolean(),
           IdxType::idx_type()) -> getcore().
init(N, R, PR, FailThreshold, NotFoundOk, AllowMult, DeletedVClock, IdxType) ->
    #getcore{n = N,
             r = R,
             pr = PR,
             ur = 0,
             fail_threshold = FailThreshold,
             notfound_ok = NotFoundOk,
             allow_mult = AllowMult,
             deletedvclock = DeletedVClock,
             idx_type = IdxType}.

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

%% Add a result for a vnode index
-spec add_result(non_neg_integer(), result(), getcore()) -> getcore().
add_result(Idx, {ok, RObj} = Result, GetCore) ->
    Dels = case riak_kv_util:is_x_deleted(RObj) of
        true ->  1;
        false -> 0
    end,
    num_pr(GetCore#getcore{
            results = [{Idx, Result}|GetCore#getcore.results],
            merged = undefined,
            num_ok = GetCore#getcore.num_ok + 1,
            num_deleted = GetCore#getcore.num_deleted + Dels}, Idx);
add_result(Idx, {error, notfound} = Result, GetCore) ->
    case GetCore#getcore.notfound_ok of
        true ->
            num_pr(GetCore#getcore{
                    results = [{Idx, Result}|GetCore#getcore.results],
                    merged = undefined,
                    num_ok = GetCore#getcore.num_ok + 1}, Idx);
        _ ->
            GetCore#getcore{
                results = [{Idx, Result}|GetCore#getcore.results],
                merged = undefined,
                num_notfound = GetCore#getcore.num_notfound + 1}
    end;
add_result(Idx, {error, _Reason} = Result, GetCore) ->
    GetCore#getcore{
        results = [{Idx, Result}|GetCore#getcore.results],
        merged = undefined,
        num_fail = GetCore#getcore.num_fail + 1}.

%% Replace a result for a vnode index i.e. when the result had previously
%% been as a result of a HEAD, and there is now a result from a GET
%% Ignore any results which were not from the list of updated indexes
-spec update_result(non_neg_integer(),
                    result(),
                    list(),
                    getcore()) -> getcore().
update_result(Idx, Result, IdxList, GetCore) ->
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
            add_result(Idx, Result, GetCore)
    end.


result_shortcode({ok, _RObj})       -> 1;
result_shortcode({error, notfound}) -> 0;
result_shortcode(_)                 -> -1.

%% Check if enough results have been added to respond
-spec enough(getcore()) -> boolean().
%% Met quorum
enough(#getcore{r = R, ur = UR, pr= PR,
                    num_ok = NumOK, num_pok = NumPOK,
                    num_upd = NumUPD})
        when NumOK >= R andalso NumPOK >= PR andalso NumUPD >= UR ->
    true;
%% too many failures
enough(#getcore{fail_threshold = FailThreshold, num_notfound = NumNotFound,
            num_fail = NumFail})
        when NumNotFound + NumFail >= FailThreshold ->
    true;
%% Got all N responses, but unable to satisfy PR
enough(#getcore{n = N, ur = UR, num_ok = NumOK, num_notfound = NumNotFound,
            num_fail = NumFail})
        when NumOK + NumNotFound + NumFail >= N andalso UR == 0 ->
    true;
enough(_) ->
    false.

%% Get success/fail response once enough results received
-spec response(getcore()) -> {reply(), getcore()}.
%% Met quorum for a standard get request/response
response(#getcore{r = R, num_ok = NumOK, pr= PR, num_pok = NumPOK, head_merge = HM} = GetCore)
        when NumOK >= R andalso NumPOK >= PR andalso HM == false ->
    #getcore{results = Results, allow_mult=AllowMult,
        deletedvclock = DeletedVClock} = GetCore,
    {ObjState, MObj} = Merged = merge(Results, AllowMult),
    Reply = case ObjState of
        ok ->
            Merged; % {ok, MObj}
        tombstone when DeletedVClock ->
            {error, {deleted, riak_object:vclock(MObj)}};
        _ -> % tombstone or notfound or expired
            {error, notfound}
    end,
    {Reply, GetCore#getcore{merged = Merged}};
%% Met quorum, but the request had asked only for head responses
response(#getcore{r = R, num_ok = NumOK, pr= PR, num_pok = NumPOK} = GetCore)
        when NumOK >= R andalso NumPOK >= PR ->
    #getcore{results = Results, allow_mult=AllowMult,
        deletedvclock = DeletedVClock} = GetCore,
    Merged = head_merge(Results, AllowMult),
    case Merged of
        {ok, _MergedObj} ->
            {Merged, GetCore#getcore{merged = Merged}}; % {ok, MObj}
        {tombstone, MObj} when DeletedVClock ->
            {{error, {deleted, riak_object:vclock(MObj)}},
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
    Merged = case Merged0 of
                 undefined ->
                     merge(Results, AllowMult);
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
    Action =
        case ReadRepairs of
           [] when ObjState == tombstone ->
               %% Allow delete if merge object is deleted,
               %% there are no read repairs pending and
               %% a value was received from all vnodes
               case riak_kv_util:is_x_deleted(MObj) andalso
                   length([xx || {_Idx, {ok, _RObj}} <- Results]) == N of
                   true ->
                       delete;
                   _ ->
                       % maybe_log_old_vclock(Results),
                       nop
               end;
           [] when ObjState == notfound ->
               nop;
           [] ->
               % maybe_log_old_vclock(Results),
               nop;
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

head_merge(Replies, AllowMult) ->
    % Replies should be a list of [{Idx, {ok, RObj}]
    IdxObjs = [{I, {ok, RObj}} || {I, {ok, RObj}} <- Replies],
    % Those that don't pattern match will be not_found
    case IdxObjs of
        [] ->
            {notfound, undefined};
        _ ->
            {BestReplies, FetchIdxObjL} = riak_object:find_bestobject(IdxObjs),
            FoldFun =
                fun({Idx, {ok, Obj}}, Acc) ->
                    case riak_kv_util:is_x_deleted(Obj) of
                        true ->
                            Acc;
                        false ->
                            [Idx|Acc]
                    end
                end,
            case lists:foldr(FoldFun, [], FetchIdxObjL) of
                [] ->
                    merge(BestReplies, AllowMult);
                IdxL ->
                    {fetch, IdxL}
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
    GC2 = update_result(3, {ok, Obj3}, [2], GC1),
    ?assertMatch(3, GC2#getcore.num_ok),
    ?assertMatch(2, GC2#getcore.r),
    ?assertMatch(1, GC2#getcore.ur),
    ?assertMatch(0, GC2#getcore.num_upd),
    ?assertMatch(3, length(GC2#getcore.results)),
    GC3 = update_result(2, {ok, fake_get2}, [2], GC2),
    ?assertMatch(3, GC3#getcore.num_ok),
    ?assertMatch(2, GC3#getcore.r),
    ?assertMatch(1, GC3#getcore.ur),
    ?assertMatch(1, GC3#getcore.num_upd),
    ?assertMatch(3, length(GC3#getcore.results)),
    ?assertMatch([{3, {ok, Obj3}},
                        {1, {ok, fake_head1}},
                        {2, {ok, fake_get2}}],
                    GC3#getcore.results).

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
            end}
    ]}.
-endif.
