%% -------------------------------------------------------------------
%%
%% kv_counter_eqc: Quickcheck test for riak_kv_counter
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved.
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

-module(kv_counter_eqc).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").

-include("../src/riak_kv_wm_raw.hrl").

-compile(export_all).

-define(BUCKET, <<"b">>).
-define(KEY, <<"k">>).
-define(NUMTESTS, 500).
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) ->
                              io:format(user, Str, Args) end, P)).

%%====================================================================
%% eunit test
%%====================================================================

counter_value_test_() ->
    {timeout, 60000, % do not trust the docs - timeout is in msec
      ?_assertEqual(true, quickcheck(numtests(?NUMTESTS, ?QC_OUT(prop_value()))))}.

counter_merge_test_() ->
    {timeout, 60000, % do not trust the docs - timeout is in msec
       ?_assertEqual(true, quickcheck(numtests(?NUMTESTS, ?QC_OUT(prop_merge()))))}.

counter_update_test_() ->
    {timeout, 60000, % do not trust the docs - timeout is in msec
       ?_assertEqual(true, quickcheck(numtests(?NUMTESTS, ?QC_OUT(prop_update()))))}.

%%====================================================================
%% Shell helpers
%%====================================================================

test() ->
    test(100).

test(N) ->
    quickcheck(numtests(N, prop_value())).

test_merge() ->
    test_merge(100).

test_merge(N) ->
        quickcheck(numtests(N, prop_merge())).

test_update() ->
    test_update(100).

test_update(N) ->
    quickcheck(numtests(N, prop_update())).

%%====================================================================
%% properties
%%====================================================================

prop_value() ->
    ?FORALL(RObj, riak_object(),
            equals(sumthem(RObj), riak_kv_counter:value(RObj))).

prop_merge() ->
    ?FORALL({RObj, IndexSpecs}, {riak_object(), index_specs()},
            begin
                Merged = riak_kv_counter:merge(RObj, IndexSpecs),
                FExpectedCounters = fun(NumGeneratedCounters) ->
                                            case NumGeneratedCounters of
                                                0 -> 0;
                                                _ -> 1
                                            end
                                    end,
                MergeSeed = undefined,

                ?WHENFAIL(
                   begin
                       io:format("Gen ~p\n", [RObj]),
                       io:format("Merged ~p\n", [Merged]),
                       io:format("Index Specs ~p~n", [IndexSpecs])
                   end,

                   conjunction([
                                {value, equals(sumthem(RObj), riak_kv_counter:value(Merged))},
                                {verify_merge, verify_merge(RObj, Merged, IndexSpecs, FExpectedCounters, MergeSeed)}
                               ]))
            end).

prop_update() ->
    ?FORALL({RObj, IndexSpecs, Actor, Amt},
            {riak_object(), index_specs(), noshrink(binary(4)), int()},
            begin
                Updated = riak_kv_counter:update(RObj, IndexSpecs, Actor, Amt),
                FExpectedCounters = fun(NumGeneratedCounters) ->
                                            case {NumGeneratedCounters, Amt} of
                                                {0, 0} -> 0;
                                                _ -> 1
                                            end
                                    end,
                MergeSeed = case Amt of
                                0 -> undefined;
                                _ -> riak_kv_pncounter:new(Actor, Amt)
                            end,

                ?WHENFAIL(
                   begin
                       io:format("Gen ~p~n", [RObj]),
                       io:format("Updated ~p~n", [Updated]),
                       io:format("Index Specs ~p~n", [IndexSpecs]),
                       io:format("Amt ~p~n", [Amt])
                   end,
                   conjunction([
                                {counter_value, equals(sumthem(RObj) + Amt,
                                                       riak_kv_counter:value(Updated))},
                                {verify_merge, verify_merge(RObj, Updated, IndexSpecs, FExpectedCounters, MergeSeed)}
                               ]))
            end).

%%====================================================================
%% Helpers
%%====================================================================

%% Update and Merge are the same, except for the
%% end value of the counter. Reuse the common properties.
verify_merge(Generated, PostAction, IndexSpecs, FExpectedCounters, MergeSeed) ->
    NumGeneratedCounters = num_counters(Generated),
    ExpectedCounters = FExpectedCounters(NumGeneratedCounters),
    ExpectedCounter = merge_object(Generated, MergeSeed),
    NumMergedCounters =  num_counters(PostAction),
    {MergedMeta, MergedCounter} = single_counter(PostAction),
    ExpectedIndexMeta = expected_indexes(IndexSpecs, ExpectedCounters),
    ExpectedSiblings = non_counter_siblings(Generated),
    ActualSiblings = non_counter_siblings(PostAction),
    conjunction([{number_of_counters,
                   equals(ExpectedCounters, NumMergedCounters)},
                  {counter_structure,
                   counters_equal(ExpectedCounter, MergedCounter)},
                  {siblings, equals(ExpectedSiblings, ActualSiblings)},
                  {index_meta, equals(ExpectedIndexMeta, sorted_index_meta(MergedMeta))}]).

sorted_index_meta(undefined) ->
    undefined;
sorted_index_meta(Meta) ->
    case dict:find(?MD_INDEX, Meta) of
        error ->
            undefined;
        {ok, Val} ->
            lists:sort(Val)
    end.

expected_indexes(_IndexSpecs, 0) ->
    undefined;
expected_indexes(IndexSpecs, _) ->
    %% Use fold just to differentiate the code
    %% under test and the testing code
    case lists:foldl(fun({add, Index, Val}, Acc) ->
                             [{Index, Val} | Acc];
                        (_, Acc) ->
                             Acc
                     end,
                     [],
                     IndexSpecs) of
        [] ->
            undefined;
        Indexes -> lists:sort(Indexes)
    end.

%% safe wrap of riak_kv_pncounter:equal/2
counters_equal(undefined, undefined) ->
    true;
counters_equal(_C1, undefined) ->
    false;
counters_equal(undefined, _C2) ->
    false;
counters_equal(C1, C2) ->
    riak_kv_pncounter:equal(C1, C2).


%% Extract a single {meta, counter} value
single_counter(Merged) ->
    Contents = riak_object:get_contents(Merged),
    case [begin
              {riak_kv_pncounter, Counter} = Val,
              {Meta, Counter}
          end || {Meta, Val} <- Contents,
                is_tuple(Val),
                riak_kv_pncounter =:= element(1, Val)] of
        [Single] ->
            Single;
        _Many -> {undefined, undefined}
    end.

non_counter_siblings(RObj) ->
    Contents = riak_object:get_contents(RObj),
    {_Counters, NonCounters} = lists:partition(fun({_Md, {riak_kv_pncounter, _C}}) ->
                                                      true;
                                                 ({_MD, _Val}) ->
                                                      false
                                              end,
                                              Contents),
    lists:sort(NonCounters).


num_counters(RObj) ->
    Values = riak_object:get_values(RObj),
    length([ok || Val <- Values,
                  is_tuple(Val),
                  riak_kv_pncounter =:= element(1, Val)]).

merge_object(RObj, Seed) ->
    Values = riak_object:get_values(RObj),
    lists:foldl(fun({riak_kv_pncounter, Counter}, undefined) ->
                        Counter;
                    ({riak_kv_pncounter, Counter}, Mergedest) ->
                        riak_kv_pncounter:merge(Counter, Mergedest);
                   (_Bin, Mergedest) ->
                        Mergedest end,
                Seed,
                Values).

%% Somewhat duplicates the logic under test
%% but is a different implementation, at least
sumthem(RObj) ->
    Merged = merge_object(RObj, riak_kv_pncounter:new()),
    riak_kv_pncounter:value(Merged).

%%====================================================================
%% Generators
%%====================================================================
riak_object() ->
    ?LET({Contents, VClock},
         {contents(), fsm_eqc_util:vclock()},
         riak_object:set_contents(
           riak_object:set_vclock(
             riak_object:new(?BUCKET, ?KEY, <<>>),
             VClock),
           Contents)).

contents() ->
    list(content()).

content() ->
    {metadata(), value()}.

metadata() ->
    %% generate a dict of metadata
    ?LET(Meta, metadatas(), dict:from_list(Meta)).

metadatas() ->
    list(metadatum()).

metadatum() ->
    %% doesn't need to be realistic,
    %% just present
    {binary(), binary()}.

value() ->
    oneof([binary(), pncounter()]).

gcounter() ->
    list(clock()).

pncounter() ->
    {riak_kv_pncounter, {gcounter(), gcounter()}}.

clock() ->
    {int(), nat()}.

index_specs() ->
    list(index_spec()).

index_spec() ->
    {index_op(), binary(), index_value()}.

index_op() ->
    oneof([add, remove]).

index_value() ->
    oneof([binary(), int()]).

-endif. % EQC

