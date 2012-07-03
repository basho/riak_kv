%% -------------------------------------------------------------------
%%
%% Copyright (c) 2011 Basho Technologies, Inc.
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

-module(mapred_test).

-include_lib("eunit/include/eunit.hrl").
-compile(export_all).

setup() ->
    riak_kv_test_util:common_setup(?MODULE, fun configure/1).

cleanup() ->
    riak_kv_test_util:common_cleanup(?MODULE, fun configure/1).

configure(load) ->
    KVSettings = [{storage_backend, riak_kv_memory_backend},
                  {test, true},
                  {vnode_vclocks, true},
                  {pb_ip, "0.0.0.0"},
                  {pb_port, 48087}, % arbitrary #
                  {map_js_vm_count, 4},
                  {reduce_js_vm_count, 3}],
    CoreSettings = [{handoff_ip, "0.0.0.0"},
                     {handoff_port, 9183},
                     {ring_creation_size, 16}],
    [ application:set_env(riak_core, K, V) || {K,V} <- CoreSettings ],
    [ application:set_env(riak_kv, K, V) || {K,V} <- KVSettings ],
    ok;
configure(unload) ->
    application:set_env(riak_api, services, dict:new());
configure(start) ->
    riak_core:wait_for_service(riak_pipe);
configure(_) ->
    ok.

inputs_gen_seq(Pipe, Max, _Timeout) ->
    [riak_pipe:queue_work(Pipe, X) || X <- lists:seq(1, Max)],
    riak_pipe:eoi(Pipe),
    ok.

inputs_gen_bkeys_1(Pipe, {Bucket, Start, End}, _Timeout) ->
    BKeys = [{Bucket, list_to_binary("bar"++integer_to_list(X))} ||
                 X <- lists:seq(Start, End)],
    [riak_pipe:queue_work(Pipe, BK) || BK <- BKeys],
    riak_pipe:eoi(Pipe),
    ok.

compat_basic1_test_() ->
    IntsBucket = <<"foonum">>,
    ReduceSumFun = fun(Inputs, _) -> [lists:sum(Inputs)] end,
    LinkBucket = <<"link bucket">>,
    LinkKey = <<"yo">>,

    {setup,
     setup(),
     cleanup(),
     fun(_) ->
         [
          ?_test(
             %% The data created by this step is used by all/most of the
             %% following tests.
             begin
                 ok = riak_kv_mrc_pipe:example_setup(),
                 {ok, C} = riak:local_client(),
                 Obj = riak_object:new(LinkBucket, LinkKey, <<"link val">>),
                 MD = dict:store(<<"Links">>,
                                 [{{LinkBucket, <<"nokey-1">>}, <<"link 1">>},
                                  {{LinkBucket, <<"nokey-2">>}, <<"link 2">>}],
                                 dict:new()),
                 ok = C:put(riak_object:update_metadata(Obj, MD))
             end
            ),
          ?_test(
             %% Empty query
             begin
                 %% This will trigger a traversal of IntsBucket, but
                 %% because the query is empty, the MapReduce will
                 %% traverse the bucket and send BKeys down the pipe.
                 %% AFAICT, the original Riak MapReduce will crash with
                 %% luke_flow errors if the query list is empty.  This
                 %% new implementation will pass the BKeys as-is.
                 {ok, BKeys} =
                     riak_kv_mrc_pipe:mapred(IntsBucket, []),
                 5 = length(BKeys),
                 {IntsBucket, <<"bar1">>} = hd(lists:sort(BKeys))
             end),
          ?_test(
             %% AZ 479: Reduce with zero inputs -> call reduce once w/empty list
             begin
                 Spec = [{reduce, {qfun, ReduceSumFun}, none, true}],
                 {ok, [0]} = riak_kv_mrc_pipe:mapred([], Spec)
             end),
          ?_test(
             %% Basic compatibility: keep both stages
             begin
                 Spec = 
                     [{map, {modfun, riak_kv_mapreduce, map_object_value},
                       none, true},
                      {reduce, {qfun, ReduceSumFun},
                       none, true}],
                 {ok, [MapRs, [15]]} =
                     riak_kv_mrc_pipe:mapred(IntsBucket, Spec),
                 5 = length(MapRs)
             end),
          ?_test(
             %% Basic compat: keep neither stages -> no output
             begin
                 Spec = 
                     [{map, {modfun, riak_kv_mapreduce, map_object_value},
                       none, false},
                      {reduce, {qfun, ReduceSumFun},
                       none, false}],
                 %% "Crazy" semantics: if only 1 keeper stage, then
                 %% return List instead of [List].
                 {ok, []} = riak_kv_mrc_pipe:mapred(IntsBucket, Spec)
             end),
          ?_test(
             %% Basic compat: keep first stage only, want 'crazy' result",
             begin
                 Spec = 
                     [{map, {modfun, riak_kv_mapreduce, map_object_value},
                       none, true},
                      {reduce, {qfun, ReduceSumFun},
                       none, false}],
                 %% "Crazy" semantics: if only 1 keeper stage, then
                 %% return List instead of [List].
                 {ok, MapRs} = riak_kv_mrc_pipe:mapred(IntsBucket, Spec),
                 5 = length(MapRs)
             end),
          ?_test(
             %% Basic compat: keep second stage only, want 'crazy' result
             begin
                 Spec = 
                     [{map, {modfun, riak_kv_mapreduce, map_object_value},
                       none, false},
                      {reduce, {qfun, ReduceSumFun},
                       none, true}],
                 %% "Crazy" semantics: if only 1 keeper stage, then
                 %% return List instead of [List].
                 {ok, [15]} = riak_kv_mrc_pipe:mapred(IntsBucket, Spec)
             end),
          ?_test(
             %% Explicit rereduce
             begin
                 Spec = 
                     [{map, {modfun, riak_kv_mapreduce, map_object_value},
                       none, true}] ++
                     lists:duplicate(
                       5, {reduce, {qfun, ReduceSumFun}, none, true}),
                 {ok, [_, [15],[15],[15],[15],[15]]} =
                     riak_kv_mrc_pipe:mapred(IntsBucket, Spec)
             end),
          ?_test(
             %% Make certain that {error, not_found} goes down the pipe
             %% from a map phase.
             begin
                 Inputs = [{<<"no-such-bucket">>, <<"no-such-key!">>}],
                 Spec = 
                     [{map, {modfun, riak_kv_mapreduce, map_object_value},
                       {struct,[{<<"sub">>,[<<"0">>]}]}, false},
                      {reduce, {modfun, riak_kv_mapreduce,
                                reduce_string_to_integer},none,true}],
                 {ok, [0]} =
                     riak_kv_mrc_pipe:mapred(Inputs, Spec)
             end),
          ?_test(
             %% Basic link phase
             begin
                 %% Inputs = [{LinkBucket, LinkKey}],
                 Inputs = LinkBucket,
                 Spec = [{link, '_', <<"link 1">>, true}],
                 {ok, [ [LinkBucket, <<"nokey-1">>, <<"link 1">>] ]} =
                     riak_kv_mrc_pipe:mapred(Inputs, Spec)
             end),
          ?_test(
             %% Link phase + notfound
             begin
                 Inputs = [{<<"no">>, K} || K <- [<<"no1">>, <<"no2">>]],
                 Spec = [{link, '_', '_', true}],
                 {ok, []} =
                     riak_kv_mrc_pipe:mapred(Inputs, Spec)
             end),
          ?_test(
             %% KeyData
             begin
                 UnMap = fun(O, undefined, _) ->
                                 [{riak_object:bucket(O),
                                   riak_object:key(O)}];
                            (O, KeyData, _) ->
                                 [{{riak_object:bucket(O),
                                    riak_object:key(O)},
                                   KeyData}]
                         end,
                 Normalize = fun({{B,K},D}) -> {{B,K},D};
                                ({B,K})     -> {B,K};
                                ([B,K])     -> {B,K};
                                ([B,K,D])   -> {{B,K},D}
                             end,
                 Spec =
                     [{map, {qfun, UnMap}, none, true}],
                 Inputs = [{IntsBucket, <<"bar1">>},
                           {{IntsBucket, <<"bar2">>}, <<"keydata works">>},
                           [IntsBucket, <<"bar3">>],
                           [IntsBucket, <<"bar4">>, <<"keydata still works">>]],
                 {ok, Results} =
                     riak_kv_mrc_pipe:mapred(Inputs, Spec),
                 SortedNormal = lists:sort([ Normalize(I) || I <- Inputs ]),
                 ?assertEqual(SortedNormal, lists:sort(Results))
             end),
          ?_test(
             %% Key Filters
             begin
                 %% filter sould match only "bar4" key
                 Inputs = {IntsBucket, [[<<"ends_with">>, <<"r4">>]]},
                 Spec = [{map, {modfun, riak_kv_mapreduce, map_object_value},
                          none, true}],
                 {ok, [4]} = riak_kv_mrc_pipe:mapred(Inputs, Spec)
             end),
          ?_test(
             %% modfun for inputs generator
             begin
                 Inputs = {modfun, ?MODULE, inputs_gen_seq, 6},
                 Spec = [{reduce, {qfun, ReduceSumFun},none,true}],
                 {ok, [21]} = riak_kv_mrc_pipe:mapred(Inputs, Spec)
             end),
          ?_test(
             %% modfun for inputs generator: make BKeys for conventional phases
             begin
                 Inputs = {modfun, ?MODULE, inputs_gen_bkeys_1,
                           {IntsBucket, 1, 5}},
                 Spec = [{map, {modfun, riak_kv_mapreduce, map_object_value},
                          none, false},
                         {reduce, {modfun, riak_kv_mapreduce,
                                   reduce_string_to_integer},none,false},
                         {reduce, {qfun, ReduceSumFun},none,true}],
                 {ok, [15]} = riak_kv_mrc_pipe:mapred(Inputs, Spec)
             end)
          ]
     end}.

compat_buffer_and_prereduce_test_() ->
    IntsBucket = <<"foonum">>,
    NumInts = 1000,
    ReduceSumFun = fun(Inputs, _) -> [lists:sum(Inputs)] end,

    {setup,
     setup(),
     cleanup(),
     fun(_) ->
         [
          ?_test(
             %% The data created by this step is used by all/most of the
             %% following tests.
             ok = riak_kv_mrc_pipe:example_setup(NumInts)
            ),
          ?_test(
             %% Verify that example_setup/1 did what it was supposed to.
             begin
                 Spec = 
                     [{map, {modfun, riak_kv_mapreduce, map_object_value},
                       none, true},
                      {reduce, {qfun, ReduceSumFun},
                       none, true}],
                 {ok, [MapRs, [500500]]} =
                     riak_kv_mrc_pipe:mapred(IntsBucket, Spec),
                 NumInts = length(MapRs)
             end),
          ?_test(
             %% Test the {reduce_phase_batch_size, int()} option
             begin
                 Spec = 
                     [{map, {modfun, riak_kv_mapreduce, map_object_value},
                       none, true},
                      {reduce, {qfun, ReduceSumFun},
                       [{reduce_phase_batch_size, 10}], true}],
                 {ok, [MapRs, [500500]]} =
                     riak_kv_mrc_pipe:mapred(IntsBucket, Spec),
                 NumInts = length(MapRs)
             end),
          ?_test(
             %% Test degenerate {reduce_phase_batch_size, 0} option
             begin
                 Spec = 
                     [{map, {modfun, riak_kv_mapreduce, map_object_value},
                       none, true},
                      {reduce, {qfun, ReduceSumFun},
                       [{reduce_phase_batch_size, 0}], true}],
                 {ok, [MapRs, [500500]]} =
                     riak_kv_mrc_pipe:mapred(IntsBucket, Spec),
                 NumInts = length(MapRs)
             end),
          ?_test(
             %% Test degenerate reduce_phase_only_1 option
             begin
                 Spec = 
                     [{map, {modfun, riak_kv_mapreduce, map_object_value},
                       none, true},
                      {reduce, {qfun, ReduceSumFun},
                       [reduce_phase_only_1], true}],
                 {ok, [MapRs, [500500]]} =
                     riak_kv_mrc_pipe:mapred(IntsBucket, Spec),
                 NumInts = length(MapRs)
             end),
          ?_test(
             %% Prereduce+reduce_phase_only_1 (combined happily!)
             %% and then reduce batch size = 7.
             begin
                 Spec = 
                     [{map, {modfun, riak_kv_mapreduce, map_object_value},
                       [do_prereduce, reduce_phase_only_1], true},
                      {reduce, {qfun, ReduceSumFun},
                       [{reduce_phase_batch_size, 7}], true}],
                 {ok, [MapRs, [500500]]} =
                     riak_kv_mrc_pipe:mapred(IntsBucket, Spec),
                 NumInts = length(MapRs)
             end)
         ]
     end}.

compat_javascript_test_() ->
    IntsBucket = <<"foonum">>,
    NumInts = 5,
    JSBucket = <<"jsfuns">>,
    NotFoundBkey = {<<"does not">>, <<"exit">>},

    {setup,
     setup(),
     cleanup(),
     fun(_) ->
         [
          ?_test(
             %% The data created by this step is used by all/most of the
             %% following tests.
             ok = riak_kv_mrc_pipe:example_setup(NumInts)
            ),
          ?_test(
             begin
                 %% map & reduce with jsanon-Source
                 Spec =
                     [{map,
                       {jsanon, <<"function(v) {
                                      return [v.values[0].data];
                                   }">>},
                       <<>>, true},
                      {reduce,
                       {jsanon, <<"function(v) {
                                      Sum = function(A, B) { return A+B; };
                                      return [ v.reduce(Sum) ];
                                   }">>},
                       <<>>, true}],
                 {ok, [MapRs, [15]]} =
                     riak_kv_mrc_pipe:mapred(IntsBucket, Spec),
                 5 = length(MapRs)
             end),
          ?_test(
             begin
                 %% map & reduce with jsanon-Bucket/Key
                 {ok, C} = riak:local_client(),
                 ok = C:put(riak_object:new(
                              JSBucket, <<"map">>,
                              <<"function(v) {
                                    return [v.values[0].data];
                                 }">>),
                            1),
                 ok = C:put(riak_object:new(
                              JSBucket, <<"reduce">>,
                              <<"function(v) {
                                    Sum = function(A, B) { return A+B; };
                                    return [ v.reduce(Sum) ];
                                 }">>),
                            1),
                 Spec =
                     [{map,
                       {jsanon, {JSBucket, <<"map">>}},
                       <<>>, true},
                      {reduce,
                       {jsanon, {JSBucket, <<"reduce">>}},
                       <<>>, true}],
                 {ok, [MapRs, [15]]} =
                     riak_kv_mrc_pipe:mapred(IntsBucket, Spec),
                 5 = length(MapRs)
             end),
          ?_test(
             begin
                 %% map & reduce with jsfun
                 Spec =
                     [{map,
                       {jsfun, <<"Riak.mapValues">>},
                       <<>>, true},
                      {reduce,
                       {jsfun, <<"Riak.reduceSum">>},
                       <<>>, true}],
                 {ok, [MapRs, [15]]} =
                     riak_kv_mrc_pipe:mapred(IntsBucket, Spec),
                 5 = length(MapRs)
             end),
          ?_test(
             begin
                 %% objects not found for JS map turn into
                 %% {not_found, {Bucket, Key}, KeyData} tuples
                 Spec =
                     [{map, {jsfun, <<"Riak.mapValues">>}, <<>>, true},
                      {reduce,
                       {jsanon, <<"function(v) {
                                      F = function(O) {
                                             if ((O[\"not_found\"] &&
                                                  O.not_found[\"bucket\"]) ||
                                                 O[\"mapred_test_pass\"])
                                                return {mapred_test_pass:1};
                                             else
                                                return O;
                                          }
                                      return v.map(F);
                                   }">>},
                       <<>>, true}],
                 {ok, [[{not_found,
                         NotFoundBkey,
                         undefined}],
                       [{struct,[{<<"mapred_test_pass">>,1}]}]]} =
                     riak_kv_mrc_pipe:mapred([NotFoundBkey], Spec)
             end),
          ?_test(
             %% KeyData
             begin
                 UnMap = <<"function(O, KD) {
                               R = {b:O.bucket, k:O.key};
                               if (KD != \"undefined\")
                                  R.d = KD;
                               return [R];
                            }">>,
                 Normalize = fun({{B,K},D}) -> {struct, [{<<"b">>, B},
                                                         {<<"k">>, K},
                                                         {<<"d">>, D}]};
                                ({B,K})     -> {struct, [{<<"b">>, B},
                                                         {<<"k">>, K}]};
                                ([B,K])     -> {struct, [{<<"b">>, B},
                                                         {<<"k">>, K}]};
                                ([B,K,D])   -> {struct, [{<<"b">>, B},
                                                         {<<"k">>, K},
                                                         {<<"d">>, D}]}
                             end,
                 Spec =
                     [{map, {jsanon, UnMap}, none, true}],
                 Inputs = [{IntsBucket, <<"bar1">>},
                           {{IntsBucket, <<"bar2">>}, <<"keydata works">>},
                           [IntsBucket, <<"bar3">>],
                           [IntsBucket, <<"bar4">>, <<"keydata still works">>]],
                 {ok, Results} =
                     riak_kv_mrc_pipe:mapred(Inputs, Spec),
                 SortedNormal = lists:sort([ Normalize(I) || I <- Inputs ]),
                 ?assertEqual(SortedNormal, lists:sort(Results))
             end)
          ]
     end}.

wait_until_dead(Pid) when is_pid(Pid) ->
    Ref = monitor(process, Pid),
    receive
        {'DOWN', Ref, process, _Obj, Info} ->
            Info
    after 10*1000 ->
            exit({timeout_waiting_for, Pid})
    end;
wait_until_dead(_) ->
    ok.
