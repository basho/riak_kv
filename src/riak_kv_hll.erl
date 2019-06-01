%% -*- coding: utf-8 -*-
%% -------------------------------------------------------------------
%%
%% riak_kv_hll: Datatype to approximate/estimate number of distinct
%%              elements in a set. Best to read "HyperLogLog in
%%              Practice: Algorithmic Engineering of a State of the
%%              Art Cardinality Estimation Algorithm" from Google, of
%%              which many of the libs reference.
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

-module(riak_kv_hll).

-behaviour(riak_dt).

-include("riak_kv_types.hrl").

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-export([within_error_check/3, gen_op/0, update_expected/3, eqc_state_value/1,
        prop_hll_converge/0]).
-define(NUMTESTS, 1000).
-endif.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([new/0, value/1, value/2]).
-export([update/3, update/4, merge/2, equal/2]).
-export([to_binary/1, from_binary/1]).
-export([to_binary/2]).
-export([stats/1, stat/2]).
-export([parent_clock/2]).
-export([to_version/2]).

%% Additional APIs for hyperloglog types
-export([new/1, reduce/2, precision/1, check_precision_and_reduce/2]).

-export_type([hllset/0, card/0, precision/0]).

-define(HYPER_DEFAULT_BACKEND, hyper_binary).
-define(STATS, [bytes, precision]).

-define(DT_HLLSET_TAG, 86).
-define(TAG, ?DT_HLLSET_TAG).
-define(V1_VERS, 1).

%% Error macros pulled from riak_dt_tags.hrl
-define(UNSUPPORTED_VERSION(Vers), {error, unsupported_version, Vers}).
-define(INVALID_BINARY, {error, invalid_binary}).
-define(UNSUPPORTED_VERSION, {error, unsupported_version,
                              Vers :: pos_integer()}).
-define(OPS, [add, add_all]).

-record(hyper, {p :: precision(),
                registers :: {module(), registers()}}).

-opaque hllset() :: #hyper{}.

-type hllset_op() :: {add|add_all, term()|list(term())} |
                     {update, [hllset_op()]}.

-type precision() :: hyper:precision().
-type registers() :: hyper:registers().
-type card() :: number().
-type actor() :: riak_dt:actor().
-type stat() :: bytes | precision.

-spec new() -> hllset().
new() ->
    new(?HYPER_DEFAULT_PRECISION).

-spec new(precision()|riak_kv_bucket:props()) -> hllset().
new(Precision) when is_integer(Precision), Precision > 3
                    andalso Precision < 17 ->
    hyper:new(Precision, ?HYPER_DEFAULT_BACKEND);
new(BProps) when is_list(BProps) ->
    Precision = proplists:get_value(hll_precision, BProps,
                                    ?HYPER_DEFAULT_PRECISION),
    new(Precision);

%% @NOTE: If precision is not between 4 and 16, inclusive, give
%% a bad precision error for clarity and handling of it via
%% application logic. Hyper's original error is not helpful for
%% any understanding.
new(_) ->
    error({bad_precision, "Precision for HLL datatype must be between 4"
           " and 16, inclusive"}).

-spec reduce(hllset(), precision()) -> hllset().
reduce(HllSet, NewPrecision) when is_integer(NewPrecision), NewPrecision > 3
                                  andalso NewPrecision < 17 ->
    OldPrecision = precision(HllSet),
    case NewPrecision =< OldPrecision of
        true -> hyper:reduce_precision(NewPrecision, HllSet);
        false -> error({bad_precision,
                       lists:flatten(
                         io_lib:format("New precision, ~p, must be less than"
                                       " or equal to previous precision, ~p"
                                       " when reducing",
                                       [NewPrecision, OldPrecision]))})
    end;

%% @see new/1 w/o guard
reduce(_, _) ->
    error({bad_precision, "Precision for HLL datatype must be between 4"
           " and 16, inclusive"}).

-spec precision(hllset()) -> precision().
precision(HllSet) ->
    hyper:precision(HllSet).

-spec value(hllset()) -> card().
value(HllSet) ->
    round(hyper:card(HllSet)).
value(precise, HllSet) ->
    value(HllSet).

-spec update(hllset_op(), actor(), hllset()) -> {ok, hllset()} |
                                               {error, term()}.
update({add, Elem}, _Actor, HllSet) ->
    {ok, hyper:insert(Elem, HllSet)};
update({add_all, Elems}, _Actor, HllSet) ->
    {ok, hyper:insert_many(Elems, HllSet)};
update({update, Ops}, Actor, HllSet) ->
    apply_ops(Ops, Actor, HllSet).

-spec update(hllset_op(), actor(), hllset(), riak_dt:context())
            -> {ok, hllset()}.
update(Op, Actor, HllSet, _Ctx) ->
    update(Op, Actor, HllSet).

-spec parent_clock(riak_dt_vclock:vclock(), hllset()) -> hllset().
parent_clock(_Clock, HllSet) ->
    HllSet.

-spec merge(hllset(), hllset()) -> hllset().
merge(HllSet1, HllSet2) ->
    hyper:union(HllSet1, HllSet2).

-spec equal(hllset(), hllset()) -> boolean().
equal(HllSet1, HllSet2) ->
    hyper:to_json(HllSet1) == hyper:to_json(HllSet2).

-spec to_binary(hllset()) -> binary().
to_binary(HllSet) ->
    <<?TAG:8/integer, ?V1_VERS:8/integer, (riak_dt:to_binary(HllSet))/binary>>.

-spec to_binary(Vers :: pos_integer(), hllset()) -> {ok, binary()} |
                                                   ?UNSUPPORTED_VERSION.
to_binary(?V1_VERS, HllSet) ->
    B = to_binary(hyper:compact(HllSet)),
    {ok, B};
to_binary(Vers, _HllSet) ->
    ?UNSUPPORTED_VERSION(Vers).

-spec from_binary(binary()) -> {ok, hllset()} | ?UNSUPPORTED_VERSION |
                              ?INVALID_BINARY.
from_binary(<<?TAG:8/integer, ?V1_VERS:8/integer, Bin/binary>>) ->
    {ok, riak_dt:from_binary(Bin)};
from_binary(<<?TAG:8/integer, Vers:8/integer, _Bin/binary>>) ->
    ?UNSUPPORTED_VERSION(Vers);
from_binary(_B) ->
    ?INVALID_BINARY.

-spec to_version(Vers :: pos_integer(), hllset()) -> hllset().
to_version(_Version, HllSet) ->
    HllSet.

-spec stats(hllset()) -> [{stat(), number()}].
stats(HllSet) ->
    [{S, stat(S, HllSet)} || S <- ?STATS].

-spec stat(stat(), hllset()) -> number() | undefined.
%% spec is more precisely byte() 1..255 or precision() 4..16 - however dialyzer
%% does not recognise this as being more precise than number() - hence number()
%% used in the actual spec
stat(bytes, HllSet) ->
    hyper:bytes(HllSet);
stat(precision, HllSet) ->
    precision(HllSet);
stat(_, _) -> undefined.

-spec apply_ops([hllset_op()], actor(), hllset()) ->
                       {ok, hllset()} | {error, term()}.
apply_ops([], _Actor, HllSet) ->
    {ok, HllSet};
apply_ops([{Verb, _V}=Op | Rest], Actor, HllSet0) when Verb == add_all;
                                                       Verb == add ->
    case update(Op, Actor, HllSet0) of
        {ok, HllSet1} ->
            apply_ops(Rest, Actor, HllSet1);
        Error ->
            Error
    end;
apply_ops([Op | _Rest], _, _) ->
    Msg = io_lib:format("Error applying operation ~p."
                        " Can only apply operations with ~p.\n", [Op, ?OPS]),
    {error, Msg}.

%% @doc Function for reducing precision of an hll data structure. Uses the
%%      precision setting from bucket props and reduces if new precision
%%      is lower then than the previous setting. Otherwise, return the
%%      original. We're guarantted to get correct values (between 4 and 16)
%%      due to bucket-validation code.
-spec check_precision_and_reduce(riak_kv_bucket:props(), {_, _, hllset()})
                                -> hllset().
check_precision_and_reduce(BProps, {_, _, #hyper{p=P0}=HllSet})
  when is_list(BProps) ->
    P1 = proplists:get_value(precision, BProps, P0),
    case P1 < P0 of
        true ->
            reduce(HllSet, P1);
        false ->
            HllSet
    end;
check_precision_and_reduce(_, {_, _, HllSet}) ->
    HllSet.

%% ===================================================================
%% EUnit tests
%% ===================================================================

-ifdef(EQC).

bin_int() ->
    ?LET(BinInt, int(), integer_to_binary(BinInt)).

uvector(0, _Gen) ->
   [];
uvector(N, Gen) ->
   ?LET(Values,uvector(N-1,Gen),
        ?LET(Value,?SUCHTHAT(V,Gen, not lists:member(V,Values)),
             [Value|Values])).

%% @doc EQC generator of unique vectors... HLL++ preforms better at larger
%%      cardinalities since we're eqc'ing a check within some probabilistic
%%      range
gen_op() ->
    oneof([{add, bin_int()},
           ?LAZY({add_all, non_empty(uvector(100, bin_int()))})]).

update_expected(ID, {add, Elem}, {Cnt0, Dict}) ->
    Adds = dict:fetch(ID, Dict),
    Cnt = case sets:is_element(Elem, Adds) of
              true -> Cnt0;
              false -> Cnt0+1
          end,
    {Cnt, dict:store(ID, sets:add_element(Elem, Adds), Dict)};
update_expected(ID, {add_all, Elems}, State) ->
    lists:foldl(fun(Elem, S) ->
                        update_expected(ID, {add, Elem}, S) end,
                State,
                Elems);
update_expected(ID, {merge, SourceID}, {Cnt, Dict}) ->
    Adds1 = dict:fetch(ID, Dict),
    Adds2 = dict:fetch(SourceID, Dict),
    {Cnt, dict:store(ID, sets:union(Adds1, Adds2), Dict)};
update_expected(ID, create, {Cnt, Dict}) ->
    {Cnt, dict:store(ID, sets:new(), Dict)}.

eqc_state_value({_Cnt, Dict}) ->
    S = dict:fold(fun(_K, V, Acc) ->
                          sets:union(V, Acc) end,
                  sets:new(),
                  Dict),
    sets:size(S).

%% @doc Standard Error is σ ≈ 1.04/√m, where m is the # of registers.
%% Deviations are related to margin of error away from the actual cardinality
%% of percentils.
%% σ = 65%, 2σ=95%, 3σ=99%
margin_of_error(P, Deviations) ->
    M = trunc(math:pow(2, P)),
    Sigma = 1.04 / math:sqrt(M),
    Sigma*Deviations.

%% @doc Check if Estimated Card from HllSet is within an acceptable
%%      margin of error determined by m-registers and 3 deviations of
%%      the standard error. Use a window of +1 to account for rounding
%%      and extremely small cardinalities.
within_error_check(Card, HllSet, HllVal) ->
    case Card > 0 of
        true ->
            Precision = riak_kv_hll:precision(HllSet),
            MarginOfError = margin_of_error(Precision, 3),
            RelativeError = (abs(Card-HllVal)/Card),
            %% Is the relative error within the margin of error times
            %% the estimation *(andalso)* is the value difference less than
            %% the actual cardinality times the margin of error
            BoundCheck1 = RelativeError =< (MarginOfError * HllVal)+1,
            BoundCheck2 = abs(HllVal-Card) =< (Card*MarginOfError)+1,
            BoundCheck1 andalso BoundCheck2;
        _ -> trunc(HllVal) == Card
    end.

prop_hll_converge() ->
    crdt_statem_eqc:prop_converge({0, dict:new()},?MODULE).

-endif.

-ifdef(TEST).
unique_merge_test() ->
    {ok, A1} = update({add, <<"bar">>}, 1, new()),
    {ok, B1} = update({add, <<"baz">>}, 2, new()),
    C1 = merge(A1, B1),
    %% non-unique check
    {ok, A2} = update({add, <<"bar">>}, 1, new()),
    {ok, B2} = update({add, <<"bar">>}, 2, new()),
    C2 = merge(A2, B2),
    ?assertEqual(2, value(C1)),
    ?assertEqual(1, value(C2)).

mult_add_ops_test() ->
    {ok, A1} = update({update, [{add, <<"bar">>}]}, a, new()),
    {ok, B1} = update({update, [{add, <<"baz">>}]}, b, new()),
    {ok, B2} = update({update, [{add_all, [<<"foo">>, <<"sum">>]},
                                {add, <<"super">>}]}, b, B1),
    {error, _Err} = update({update, [{addy, <<"baz">>}]}, c, new()),
    ?assertEqual(1, value(A1)),
    ?assertEqual(1, value(B1)),
    ?assertEqual(4, value(B2)).

stat_test() ->
    HLLS0 = new(),
    {ok, HLLS1} = update({add_all, [<<"a">>, <<"b1">>, <<"c23">>, <<"d234">>]},
                         1,
                         HLLS0),
    ?assertEqual([{bytes, 48}, {precision, 14}], stats(HLLS0)),
    ?assertEqual([{bytes, 208}, {precision, 14}], stats(HLLS1)),
    ?assertEqual(208, stat(bytes, HLLS1)),
    ?assertEqual(14, stat(precision, HLLS1)),
    ?assertEqual(undefined, stat(actor_count, HLLS1)).

precision_new_test_() ->
    {setup,
     fun() ->
             meck:new(riak_core_bucket),
             ok
     end,
     fun(_) ->
             meck:unload(riak_core_bucket)
     end,
     [
      ?_test(begin
                 HLL0 = new(),
                 ?assertEqual(?HYPER_DEFAULT_PRECISION, precision(HLL0)),
                 HLL1 = new(12),
                 ?assertEqual(12, precision(HLL1)),
                 ?assertError({bad_precision, _}, new(1)),
                 ?assertError({bad_precision, _}, reduce(HLL1, 2)),
                 ?assertError({bad_precision, _}, reduce(HLL1, 14))
             end),
      ?_test(begin
                 meck:expect(riak_core_bucket, get_bucket,
                             fun(_Bucket) -> [{datatype, hll},
                                             {hll_precision, 10}]
                             end),
                 Bucket0 = {<<"hllhellz">>, <<"crdt">>},
                 BPropsBucket0 = riak_core_bucket:get_bucket(Bucket0),
                 HLL3 = new(BPropsBucket0),
                 ?assertEqual(10, precision(HLL3))
             end),
      ?_test(begin
                 meck:expect(riak_core_bucket, get_bucket,
                             fun(_Bucket) -> [{datatype, hll}]
                             end),
                 Bucket1 = {<<"hllz">>, <<"crdt">>},
                 BPropsBucket1 = riak_core_bucket:get_bucket(Bucket1),
                 HLL4 = new(BPropsBucket1),
                 ?assertEqual(?HYPER_DEFAULT_PRECISION, precision(HLL4))
             end),
      ?_test(begin
                 meck:expect(riak_core_bucket, get_bucket,
                             fun(_Bucket) -> [{datatype, hll}]
                             end),
                 Bucket2 = {<<"lemonade">>, <<"crdt">>},
                 BPropsBucket2 = riak_core_bucket:get_bucket(Bucket2),
                 HLL5 = new(BPropsBucket2),
                 ?assertEqual(?HYPER_DEFAULT_PRECISION, precision(HLL5))
             end)]}.
-endif.
