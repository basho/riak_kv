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

prop_value() ->
    %% given any riak_object,
    %% value will return the value
    %% of merged PN-Counter payloads (zero if no counters)
    ?FORALL(RObj, riak_object(),
            collect(num_counters(riak_object:get_values(RObj)),
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
                NumCounters = num_counters(riak_object:get_values(RObj)),
                NumMergedCounters =  num_counters(riak_object:get_values(Merged)),
                ExpectedCounters = case NumCounters of
                                       0 -> 0;
                                       _ -> 1
                                   end,
                %% Check the structure of the merged counter is correct
                ExpectedCounter = merge_object(RObj, undefined),
                MergedCounter = single_counter(Merged),
                %% Check that the meta is correct
                %% Check that non-sibling values and meta are untouched
                ExpectedSiblings = non_counter_siblings(RObj),
                ActualSiblings = non_counter_siblings(Merged),
                ?WHENFAIL(
                   begin
                       io:format("Gen ~p\n", [RObj]),
                       io:format("Merged ~p\n", [Merged])
                   end,
                   collect(NumCounters,
                           conjunction([{number_of_counters,
                                         equals(ExpectedCounters, NumMergedCounters)},
                                       {counter_structure,
                                        counters_equal(ExpectedCounter, MergedCounter)},
                                        {siblings, equals(lists:sort(ExpectedSiblings), lists:sort(ActualSiblings))}
                                       ])))
            end).

%%====================================================================
%% Helpers
%%====================================================================
counters_equal(undefined, undefined) ->
    true;
counters_equal(_C1, undefined) ->
    false;
counters_equal(undefined, _C2) ->
    false;
counters_equal(C1, C2) ->
    riak_kv_pncounter:equal(C1, C2).

single_counter(Merged) ->
    Values = riak_object:get_values(Merged),
    case [begin
              {riak_kv_pncounter, Counter} = Val,
              Counter
          end|| Val <- Values,
                is_tuple(Val),
                riak_kv_pncounter =:= element(1, Val)] of
        [Single] ->
            Single;
        _Many -> undefined
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

num_counters(Values) ->
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
    %% doesn't need to be realistic, even
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
