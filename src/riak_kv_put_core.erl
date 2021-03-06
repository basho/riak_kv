%% -------------------------------------------------------------------
%%
%% riak_kv_put_core: Riak put logic
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
-module(riak_kv_put_core).
-export([init/8, add_result/2, enough/1, response/1,
         final/1, result_shortcode/1, result_idx/1]).
-export_type([putcore/0, result/0, reply/0]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type vput_result() :: any().

-type result() :: w |
                  {dw, undefined} |
                  {dw, riak_object:riak_object()} |
                  {error, any()}.

-type reply() :: ok | 
                 {ok, riak_object:riak_object()} |
                 {error, notfound} |
                 {error, any()}.
-type idxresult() :: {non_neg_integer(), result()}.
-type idx_type() :: [{non_neg_integer(), 'primary' | 'fallback', node()}].
-record(putcore, {n :: pos_integer(),
                  w :: non_neg_integer(),
                  dw :: non_neg_integer(),
                  pw :: non_neg_integer(),
                  node_confirms :: non_neg_integer(),
                  pw_fail_threshold :: pos_integer(),
                  node_confirms_fail_threshold :: pos_integer(),
                  dw_fail_threshold :: pos_integer(),
                  returnbody :: boolean(),
                  allowmult :: boolean(),
                  results = [] :: [idxresult()],
                  final_obj :: undefined | riak_object:riak_object(),
                  num_w = 0 :: non_neg_integer(),
                  num_dw = 0 :: non_neg_integer(),
                  num_pw = 0 :: non_neg_integer(),
                  num_node_confirms = 0 :: non_neg_integer(),
                  num_fail = 0 :: non_neg_integer(),
                  idx_type :: idx_type() %% mapping of idx -> primary | fallback
                 }).
-opaque putcore() :: #putcore{}.

%% ====================================================================
%% Public API
%% ====================================================================

%% Initialize a put and return an opaque put core context
-spec init(N::pos_integer(), W::non_neg_integer(),
           PW::non_neg_integer(), NodeConfirms::non_neg_integer(),
           DW::non_neg_integer(),
           AllowMult::boolean(), ReturnBody::boolean(),
           IDXType::idx_type()) -> putcore().
init(N, W, PW, NodeConfirms, DW, AllowMult, ReturnBody, IdxType) ->
    #putcore{n = N, w = W, pw = PW, dw = DW, node_confirms = NodeConfirms,
             pw_fail_threshold = calculate_fail_threshold(N, PW),
             node_confirms_fail_threshold = calculate_fail_threshold(N, NodeConfirms),
             dw_fail_threshold = calculate_fail_threshold(N, DW),
             allowmult = AllowMult,
             returnbody = ReturnBody,
             idx_type = IdxType}.

%% @priv
-spec calculate_fail_threshold(pos_integer(), non_neg_integer()) -> non_neg_integer().
calculate_fail_threshold(N, Q) ->
    N-Q+1.

%% Add a result from the vnode
-spec add_result(vput_result(), putcore()) -> putcore().
add_result({w, Idx, _ReqId}, PutCore = #putcore{results = Results,
                                                num_w = NumW}) ->
    PutCore#putcore{results = [{Idx, w} | Results],
                    num_w = NumW + 1};
add_result({dw, Idx, _ReqId}, PutCore = #putcore{results = Results,
                                                 num_dw = NumDW}) ->
    num_node_confirms(num_pw(PutCore#putcore{results = [{Idx, {dw, undefined}} | Results],
                    num_dw = NumDW + 1}, Idx));
add_result({dw, Idx, ResObj, _ReqId}, PutCore = #putcore{results = Results,
                                                         num_dw = NumDW}) ->
    num_node_confirms(num_pw(PutCore#putcore{results = [{Idx, {dw, ResObj}} | Results],
                    num_dw = NumDW + 1}, Idx));
add_result({fail, Idx, _ReqId}, PutCore = #putcore{results = Results,
                                                   num_fail = NumFail}) ->
    PutCore#putcore{results = [{Idx, {error, undefined}} | Results],
                    num_fail = NumFail + 1};
add_result(_Other, PutCore = #putcore{num_fail = NumFail}) ->
    %% Treat unrecognized messages as failures - no index to store them against
    PutCore#putcore{num_fail = NumFail + 1}.

%% Check if enough results have been added to respond
-spec enough(putcore()) -> boolean().
%% The perfect world, all the quorum restrictions have been met.
enough(#putcore{w = W, num_w = NumW, dw = DW, num_dw = NumDW, pw = PW, num_pw = NumPW, node_confirms = NodeConfirms, num_node_confirms = NumNodeConfirms}) when
      NumW >= W, NumDW >= DW, NumPW >= PW, NumNodeConfirms >= NodeConfirms ->
    true;
%% Enough failures that we can't meet the PW restriction
enough(#putcore{ num_fail = NumFail, pw_fail_threshold = PWFailThreshold}) when
      NumFail >= PWFailThreshold ->
    true;
%% Enough failures that we can't meet the NodeConfirms restriction
enough(#putcore{ num_fail = NumFail, node_confirms_fail_threshold = NodeConfirmsFailThreshold}) when
      NumFail >= NodeConfirmsFailThreshold ->
    true;
%% Enough failures that we can't meet the DW restriction
enough(#putcore{ num_fail = NumFail, dw_fail_threshold = DWFailThreshold}) when
      NumFail >= DWFailThreshold ->
    true;
%% We've received all DW responses but can't satisfy PW
enough(#putcore{n = N, num_dw = NumDW, num_fail = NumFail, pw = PW, num_pw = NumPW}) when
      NumDW + NumFail >= N, NumPW < PW ->
    true;
%% We've received all DW responses but can't satisfy NodeConfirms
enough(#putcore{n = N, num_dw = NumDW, num_fail = NumFail, node_confirms = NodeConfirms, num_node_confirms = NumNodeConfirms}) when
      NumDW + NumFail >= N, NumNodeConfirms < NodeConfirms ->
    true;
enough(_PutCore) ->
    false.

%% Get success/fail response once enough results received
-spec response(putcore()) -> {reply(), putcore()}.
%% Perfect world - all quora met
response(PutCore = #putcore{w = W, num_w = NumW, dw = DW, num_dw = NumDW, pw = PW, num_pw = NumPW, node_confirms = NodeConfirms, num_node_confirms = NumNodeConfirms}) when
      NumW >= W, NumDW >= DW, NumPW >= PW, NumNodeConfirms >= NodeConfirms ->
    maybe_return_body(PutCore);
%% Everything is ok, except we didn't meet PW
response(PutCore = #putcore{w = W, num_w = NumW, dw = DW, num_dw = NumDW, pw = PW, num_pw = NumPW}) when
      NumW >= W, NumDW >= DW, NumPW < PW ->
    check_overload({error, {pw_val_unsatisfied, PW, NumPW}}, PutCore);
%% Didn't make PW, and PW >= DW
response(PutCore = #putcore{n = N, num_fail = NumFail, dw = DW, pw=PW, num_pw = NumPW}) when
      NumFail > N - PW, PW >= DW ->
    check_overload({error, {pw_val_unsatisfied, PW, NumPW}}, PutCore);
%% Everything is ok, except we didn't meet NodeConfirms
response(PutCore = #putcore{w = W, num_w = NumW, dw = DW, num_dw = NumDW, node_confirms = NodeConfirms, num_node_confirms = NumNodeConfirms}) when
      NumW >= W, NumDW >= DW, NumNodeConfirms < NodeConfirms ->
    check_overload({error, {node_confirms_val_unsatisfied, NodeConfirms, NumNodeConfirms}}, PutCore);
%% Didn't make NodeConfirms, and NodeConfirms >= DW
response(PutCore = #putcore{n = N, num_fail = NumFail, dw = DW, node_confirms=NodeConfirms, num_node_confirms = NumNodeConfirms}) when
      NumFail > N - NodeConfirms, NodeConfirms >= DW ->
    check_overload({error, {node_confirms_val_unsatisfied, NodeConfirms, NumNodeConfirms}}, PutCore);
%% Didn't make DW and DW > PW
response(PutCore = #putcore{n = N, num_fail = NumFail, dw = DW, num_dw = NumDW}) when
      NumFail > N - DW ->
    check_overload({error, {dw_val_unsatisfied, DW, NumDW}}, PutCore).

%% Check for vnode overload
check_overload(Response, PutCore = #putcore{results=Results}) ->
    case [x || {_,{error, overload}} <- Results] of
        [] ->
            {Response, PutCore};
        _->
            {{error, overload}, PutCore}
    end.

%% Get final value - if returnbody did not need the result it allows delaying
%% running reconcile until after the client reply is sent.
-spec final(putcore()) -> {riak_object:riak_object()|undefined, putcore()}.
final(PutCore = #putcore{final_obj = FinalObj, 
                         results = Results, allowmult = AllowMult}) ->
    case FinalObj of
        undefined ->
            RObjs = [RObj || {_Idx, {dw, RObj}} <- Results, RObj /= undefined],
            ReplyObj = case RObjs of
                           [] ->
                               undefined;
                           _ ->
                               riak_object:reconcile(RObjs, AllowMult)
                       end,
            {ReplyObj, PutCore#putcore{final_obj = ReplyObj}};
        _ ->
            {FinalObj, PutCore}
    end.

result_shortcode({w, _, _})     -> 1;
result_shortcode({dw, _, _})    -> 2;
result_shortcode({dw, _, _, _}) -> 2;
result_shortcode({fail, _, _})  -> -1;
result_shortcode(_)             -> -2.

result_idx({_, Idx, _})    -> Idx;
result_idx({_, Idx, _, _}) -> Idx;
result_idx(_)              -> -1.

%% ====================================================================
%% Internal functions
%% ====================================================================
maybe_return_body(PutCore = #putcore{returnbody = false}) ->
    {ok, PutCore};
maybe_return_body(PutCore = #putcore{returnbody = true}) ->
    {ReplyObj, UpdPutCore} = final(PutCore),
    {{ok, ReplyObj}, UpdPutCore}.

%% @private Checks IdxType to see if Idx is a primary.
%% If the Idx is not in the IdxType the world must be
%% resizing (ring expanding). In that case, Idx is
%% assumed to be a primary, since only primaries forward.
is_primary_response(Idx, IdxType) ->
    case lists:keyfind(Idx, 1, IdxType) of
        false -> true;
        {Idx, Status, _Node} -> Status == primary
    end.

%% @private Increment PW, if appropriate
num_pw(PutCore = #putcore{num_pw=NumPW, idx_type=IdxType}, Idx) ->
    case is_primary_response(Idx, IdxType) of
        true ->
            PutCore#putcore{num_pw=NumPW+1};
        false ->
            PutCore
    end.

%% @private Calculate number of physically diverse partitions in results
-spec count_diverse_nodes(IDXType::idx_type(), [idxresult()]) -> non_neg_integer().
count_diverse_nodes(IdxType, Results) ->
    DWrites = [Part || {Part, {dw, _}} <- Results ],
    count_physically_diverse(IdxType, DWrites, []).

-spec count_physically_diverse(IDXType::idx_type(), [non_neg_integer()], [node()]) -> non_neg_integer().
count_physically_diverse(_IdxType, [], NodeAcc) ->
    UniqueNodes = lists:usort(NodeAcc),
    length(UniqueNodes);
count_physically_diverse(IdxType, [DWPart | DWRest], NodeAcc) ->
    {DWPart, _Type, Node} = lists:keyfind(DWPart, 1, IdxType),
    count_physically_diverse(IdxType, DWRest, [Node | NodeAcc]).

%% @private Return number of physically diverse partitions in results
-spec num_node_confirms(putcore()) -> putcore().
num_node_confirms(PutCore = #putcore{idx_type=IdxType, results=Results}) ->
    Cdn = count_diverse_nodes(IdxType, Results),
    PutCore#putcore{num_node_confirms=Cdn}.

-ifdef(TEST).
%% simple sanity tests
diversity_test_() ->
    [
        {"Diversity",
            fun() ->
                    ?assertEqual(1, count_diverse_nodes(
                                 [{1,primary,node1},
                                  {2,primary,node2},
                                  {3,fallback,node3}],
                                 [{1, {dw, undefined}}])),
                    ?assertEqual(2, count_diverse_nodes(
                                 [{1,primary,node1},
                                  {2,primary,node2},
                                  {3,fallback,node3}],
                                 [{1, {dw, undefined}},
                                  {3, {dw, undefined}}])),
                    ?assertEqual(1, count_diverse_nodes(
                                 [{1,primary,node1},
                                  {2,primary,node2},
                                  {3,fallback,node3}],
                                 [{3, {dw, undefined}},
                                  {3, {dw, undefined}},
                                  {1, w}])),
                ok
        end}
    ].

enough_test_() ->
    [
        {"Checking W",
            fun() ->
                    %% you can never fail W directly...
                    ?assertEqual(false, enough(#putcore{n=3, w=3, dw=0, pw=0, node_confirms=0,
                                dw_fail_threshold=4, pw_fail_threshold=4, num_w=1,
                                num_dw=0, num_pw=0, num_fail=0})),
                    ?assertEqual(false, enough(#putcore{n=3, w=3, dw=0, pw=0, node_confirms=0,
                                dw_fail_threshold=4, pw_fail_threshold=4, num_w=2,
                                num_dw=0, num_pw=0, num_fail=0})),
                    %% got enough Ws
                    ?assertEqual(true, enough(#putcore{n=3, w=3, dw=0, pw=0, node_confirms=0,
                                dw_fail_threshold=4, pw_fail_threshold=4, num_w=3,
                                num_dw=0, num_pw=0, num_fail=0})),
                ok
        end},
        {"Checking DW",
            fun() ->
                    ?assertEqual(false, enough(#putcore{n=3, w=0, dw=3, pw=0, node_confirms=0,
                                dw_fail_threshold=1, pw_fail_threshold=4, num_w=3,
                                num_dw=1, num_pw=0, num_fail=0})),
                    ?assertEqual(false, enough(#putcore{n=3, w=0, dw=3, pw=0, node_confirms=0,
                                dw_fail_threshold=1, pw_fail_threshold=4, num_w=3,
                                num_dw=2, num_pw=0, num_fail=0})),
                    %% got enough DWs
                    ?assertEqual(true, enough(#putcore{n=3, w=0, dw=3, pw=0, node_confirms=0,
                                dw_fail_threshold=1, pw_fail_threshold=4, num_w=3,
                                num_dw=3, num_pw=0, num_fail=0})),
                    %% exceeded failure threshold
                    ?assertEqual(true, enough(#putcore{n=3, w=0, dw=3, pw=0, node_confirms=0,
                                dw_fail_threshold=1, pw_fail_threshold=4, num_w=3,
                                num_dw=2, num_pw=0, num_fail=1})),
                ok
        end},
        {"Checking PW",
            fun() ->
                    ?assertEqual(false, enough(#putcore{n=3, w=0, dw=0, pw=3, node_confirms=0,
                                dw_fail_threshold=4, pw_fail_threshold=1, num_w=3,
                                num_dw=1, num_pw=1, num_fail=0})),
                    ?assertEqual(false, enough(#putcore{n=3, w=0, dw=0, pw=3, node_confirms=0,
                                dw_fail_threshold=4, pw_fail_threshold=1, num_w=3,
                                num_dw=2, num_pw=2, num_fail=0})),
                    %% got enough PWs
                    ?assertEqual(true, enough(#putcore{n=3, w=0, dw=0, pw=3, node_confirms=0,
                                dw_fail_threshold=4, pw_fail_threshold=1, num_w=3,
                                num_dw=3, num_pw=3, num_fail=0})),
                    %% exceeded failure threshold
                    ?assertEqual(true, enough(#putcore{n=3, w=0, dw=0, pw=3, node_confirms=0,
                                dw_fail_threshold=4, pw_fail_threshold=1, num_w=3,
                                num_dw=2, num_pw=2, num_fail=1})),
                    %% can never satisfy PW
                    ?assertEqual(true, enough(#putcore{n=3, w=0, dw=0, pw=3, node_confirms=0,
                                dw_fail_threshold=4, pw_fail_threshold=1, num_w=3,
                                num_dw=3, num_pw=2, num_fail=0})),

                ok
        end},
        {"Checking NodeConfirms",
            fun() ->
                    ?assertEqual(false, enough(#putcore{n=3, w=0, dw=0, pw=0, node_confirms=3,
                                dw_fail_threshold=4, node_confirms_fail_threshold=1, num_w=3,
                                num_dw=1, num_node_confirms=1, num_fail=0})),
                    ?assertEqual(false, enough(#putcore{n=3, w=0, dw=0, pw=0, node_confirms=3,
                                dw_fail_threshold=4, node_confirms_fail_threshold=1, num_w=3,
                                num_dw=2, num_node_confirms=2, num_fail=0})),
                    %% got enough NodeConfirmss
                    ?assertEqual(true, enough(#putcore{n=3, w=0, dw=0, pw=0, node_confirms=3,
                                dw_fail_threshold=4, node_confirms_fail_threshold=1, num_w=3,
                                num_dw=3, num_node_confirms=3, num_fail=0})),
                    %% exceeded failure threshold
                    ?assertEqual(true, enough(#putcore{n=3, w=0, dw=0, pw=0, node_confirms=3,
                                dw_fail_threshold=4, node_confirms_fail_threshold=1, num_w=3,
                                num_dw=2, num_node_confirms=2, num_fail=1})),
                    %% can never satisfy NodeConfirms
                    ?assertEqual(true, enough(#putcore{n=3, w=0, dw=0, pw=0, node_confirms=3,
                                dw_fail_threshold=4, node_confirms_fail_threshold=1, num_w=3,
                                num_dw=3, num_node_confirms=2, num_fail=0})),

                ok
        end}
    ].

response_test_() ->
    [
        {"Requirements met",
            fun() ->
                    ?assertMatch({ok, _},
                        response(#putcore{n=3, w=1, dw=3, pw=2, node_confirms=0,
                                dw_fail_threshold=1, pw_fail_threshold=2, num_w=3,
                                num_dw=3, num_pw=2, num_fail=0,
                                returnbody=false})),
                    ok
            end},
        {"DW val unsatisfied",
            fun() ->
                    ?assertMatch({{error, {dw_val_unsatisfied, 3, 2}}, _},
                        response(#putcore{n=3, w=0, dw=3, pw=0, node_confirms=0,
                                dw_fail_threshold=1, pw_fail_threshold=4, num_w=3,
                                num_dw=2, num_pw=0, num_fail=1})),
                    %% can never satify PW or DW and PW >= DW
                    ?assertMatch({{error, {dw_val_unsatisfied, 3, 2}}, _},
                        response(#putcore{n=3, w=0, dw=3, pw=2,
                                dw_fail_threshold=1, pw_fail_threshold=2, num_w=3,
                                num_dw=2, num_pw=1, num_fail=1})),
                    ok
            end},
        {"PW val unsatisfied",
            fun() ->
                    ?assertMatch({{error, {pw_val_unsatisfied, 3, 2}}, _},
                        response(#putcore{n=3, w=0, dw=0, pw=3, node_confirms=0,
                                dw_fail_threshold=4, pw_fail_threshold=1, num_w=3,
                                num_dw=2, num_pw=2, num_fail=1})),
                    ?assertMatch({{error, {pw_val_unsatisfied, 3, 1}}, _},
                        response(#putcore{n=3, w=0, dw=0, pw=3, node_confirms=0,
                                dw_fail_threshold=4, pw_fail_threshold=1, num_w=3,
                                num_dw=3, num_pw=1, num_fail=0})),
                    %% can never satify PW or DW and PW >= DW
                    ?assertMatch({{error, {pw_val_unsatisfied, 3, 2}}, _},
                        response(#putcore{n=3, w=0, dw=3, pw=3, node_confirms=0,
                                dw_fail_threshold=1, pw_fail_threshold=1, num_w=3,
                                num_dw=2, num_pw=2, num_fail=1})),
                    ok
            end},
        {"NodeConfirms val unsatisfied",
            fun() ->
                    ?assertMatch({{error, {node_confirms_val_unsatisfied, 3, 2}}, _},
                        response(#putcore{n=3, w=0, dw=0, pw=0, node_confirms=3,
                                dw_fail_threshold=4, node_confirms_fail_threshold=1, num_w=3,
                                num_dw=2, num_node_confirms=2, num_fail=1})),
                    ?assertMatch({{error, {node_confirms_val_unsatisfied, 3, 1}}, _},
                        response(#putcore{n=3, w=0, dw=0, pw=0, node_confirms=3,
                                dw_fail_threshold=4, node_confirms_fail_threshold=1, num_w=3,
                                num_dw=3, num_node_confirms=1, num_fail=0})),
                    %% can never satify PW or DW and PW >= DW
                    ?assertMatch({{error, {node_confirms_val_unsatisfied, 3, 2}}, _},
                        response(#putcore{n=3, w=0, dw=3, pw=0, node_confirms=3,
                                dw_fail_threshold=1, node_confirms_fail_threshold=1, num_w=3,
                                num_dw=2, num_node_confirms=2, num_fail=1})),
                    ok
            end}

    ].

-endif.
