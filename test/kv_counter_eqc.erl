%% -------------------------------------------------------------------
%%
%% kv_counter_eqc: Quickcheck test for riak_kv_counter
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
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

%%-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("eunit/include/eunit.hrl").

-include("../src/riak_kv_wm_raw.hrl").

-compile(export_all).

-define(BUCKET, <<"b">>).
-define(KEY, <<"k">>).
-define(NUMTESTS, 1000).
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) ->
                              io:format(user, Str, Args) end, P)).

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

prop_value() ->
    %% given any riak_object,
    %% value will return the value
    %% of merged PN-Counter payloads (zero if no counters)
    ?FORALL(RObj, riak_object(),
            collect(num_counters(RObj),
                    equals(sumthem(RObj), riak_kv_counter:value(RObj)))).

prop_merge() ->
    %% given any riak_object
    %% merge will return a
    %% riak_object where all counter siblings
    %% are squashed to a single value
    %% and all counter metadata is squashed to
    %% a single dict of indexes
    %% all other siblings must be maintained, untouched.
    ?FORALL({RObj, IndexSpecs}, {riak_object(), index_specs()},
            begin
                Merged = riak_kv_counter:merge(RObj, IndexSpecs),
                NumGeneratedCounters = num_counters(RObj),
                NumMergedCounters =  num_counters(Merged),
                ExpectedCounters = case NumGeneratedCounters of
                                       0 -> 0;
                                       _ -> 1
                                   end,
                %% Check that the structure of the merged counter is correct
                ExpectedCounter = merge_object(RObj, undefined),
                {MergedMeta, MergedCounter} = single_counter(Merged),
                %% Check that the meta is correct
                ExpectedIndexMeta = expected_indexes(IndexSpecs, NumGeneratedCounters),
                %% Check that non-sibling values and meta are untouched
                ExpectedSiblings = non_counter_siblings(RObj),
                ActualSiblings = non_counter_siblings(Merged),
                ?WHENFAIL(
                   begin
                       io:format("Gen ~p\n", [RObj]),
                       io:format("Merged ~p\n", [Merged]),
                       io:format("Index Specs ~p~n", [IndexSpecs])
                   end,
                   collect(NumGeneratedCounters,
                           conjunction([
                                        {value, equals(sumthem(RObj), riak_kv_counter:value(Merged))},
                                        {number_of_counters,
                                         equals(ExpectedCounters, NumMergedCounters)},
                                       {counter_structure,
                                        counters_equal(ExpectedCounter, MergedCounter)},
                                        {siblings, equals(lists:sort(ExpectedSiblings), lists:sort(ActualSiblings))},
                                        {index_meta, equals(ExpectedIndexMeta, sorted_index_meta(MergedMeta))}
                                       ])))
            end).

prop_update() ->
    %% given any riak object, will update the counter value of that object.
    %% If any counter values are present they will be merged and incremented
    %% if not, then a new counter is created, and incremented
    %% all non-counter values and meta are left alone
    %% only counter index meta is preserved, again using indexspecs.
    ?FORALL({RObj, IndexSpecs, Actor, Amt},
            {riak_object(), index_specs(), noshrink(binary(4)), int()},
            begin
                Updated = riak_kv_counter:update(RObj, IndexSpecs, Actor, Amt),

                NumMergedCounters =  num_counters(Updated),
                ExpectedCounters = case {NumMergedCounters, Amt} of
                                       {0, 0} -> 0;
                                       _ -> 1
                                   end,
                %% Check that the structure of the merged counter is correct
                CounterSeed = case Amt of
                                  0 -> undefined;
                                  _ -> riak_kv_pncounter:new(Actor, Amt)
                              end,
                ExpectedCounter = merge_object(RObj, CounterSeed),
                {MergedMeta, MergedCounter} = single_counter(Updated),
                %% Check that the meta is correct
                ExpectedIndexMeta = expected_indexes(IndexSpecs, ExpectedCounters),
                %% Check that non-sibling values and meta are untouched
                ExpectedSiblings = non_counter_siblings(RObj),
                ActualSiblings = non_counter_siblings(Updated),

                ?WHENFAIL(
                   begin
                       io:format("Gen ~p~n", [RObj]),
                       io:format("Updated ~p~n", [Updated]),
                       io:format("Index Specs ~p~n", [IndexSpecs]),
                       io:format("Amt ~p~n", [Amt]),
                       io:format("Merged counter ~p Expected counter ~p", [ExpectedCounter, MergedCounter])
                   end,
                   collect(Amt,
                           conjunction([
                                       {counter_value, equals(sumthem(RObj) + Amt,
                                                              riak_kv_counter:value(Updated))},
                                         {number_of_counters,
                                         equals(ExpectedCounters, NumMergedCounters)},
                                       {counter_structure,
                                        counters_equal(ExpectedCounter, MergedCounter)},
                                        {siblings, equals(lists:sort(ExpectedSiblings), lists:sort(ActualSiblings))},
                                        {index_meta, equals(ExpectedIndexMeta, sorted_index_meta(MergedMeta))}
                                       ])
                          ))
            end).

%% Both update and merge
%% share most of their properties
%% so reuses them
verify_merge(Generated, PostAction, IndexSpecs) ->
    NumGeneratedCounters = num_counters(Generated),
    NumPostActionCounters =  num_counters(PostAction),
    ExpectedCounters = case NumGeneratedCounters of
                           0 -> 0;
                           _ -> 1
                       end,
    %% Check that the structure of the merged counter is correct
    ExpectedCounter = merge_object(Generated, undefined),
    {PostActionMeta, PostActionCounter} = single_counter(PostAction),
    %% Check that the meta is correct
    ExpectedIndexMeta = expected_indexes(IndexSpecs, NumGeneratedCounters),
    %% Check that non-sibling values and meta are untouched
    ExpectedSiblings = non_counter_siblings(Generated),
    ActualSiblings = non_counter_siblings(PostAction),
    conjunction([{number_of_counters,
                  equals(ExpectedCounters, NumPostActionCounters)},
                 {counter_structure,
                  counters_equal(ExpectedCounter, PostActionCounter)},
                 {siblings, equals(lists:sort(ExpectedSiblings), lists:sort(ActualSiblings))},
                 {index_meta, equals(ExpectedIndexMeta, sorted_index_meta(PostActionMeta))}
                ]).

%%====================================================================
%% Helpers
%%====================================================================
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

%% Extract a single {meta , counter} value
single_counter(Merged) ->
    Contents = riak_object:get_contents(Merged),
    case [begin
              {riak_kv_pncounter, Counter} = Val,
              {Meta, Counter}
          end|| {Meta, Val} <- Contents,
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
    NonCounters.

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

%%-endif. % EQC
