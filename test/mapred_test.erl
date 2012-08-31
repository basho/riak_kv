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
-include_lib("riak_pipe/include/riak_pipe.hrl").
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
                  {pb_port, 0}, % arbitrary #
                  {map_js_vm_count, 4},
                  {reduce_js_vm_count, 3}],
    CoreSettings = [{handoff_ip, "0.0.0.0"},
                     {handoff_port, 0},
                     {ring_creation_size, 16}],
    [ application:set_env(riak_core, K, V) || {K,V} <- CoreSettings ],
    [ application:set_env(riak_kv, K, V) || {K,V} <- KVSettings ],
    ok;

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

dead_pipe_test_() ->
    {setup,
     setup(),
     cleanup(),
     fun(_) ->
         [
          ?_test(
             %% Verify that sending inputs to a pipe that has already
             %% stopped raises an error (synchronous send)
             begin
                 Spec = 
                     [{map, {modfun, riak_kv_mapreduce, map_object_value},
                       none, true}],
                 {{ok, Pipe}, _NumKeeps} =
                     riak_kv_mrc_pipe:mapred_stream(Spec),
                 riak_pipe:destroy(Pipe),
                 {error, Reason} = riak_kv_mrc_pipe:send_inputs(
                                     Pipe, [{<<"foo">>, <<"bar">>}]),
                 %% Each vnode should have received the input, but
                 %% being unable to find the fitting process, returned
                 %% `worker_startup_failed` (and probably also printed
                 %% "fitting was gone before startup")
                 ?assert(lists:member(worker_startup_failed, Reason))
             end),
          ?_test(
             %% Verify that sending inputs to a pipe that has already
             %% stopped raises an error (async send)
             begin
                 Spec = 
                     [{map, {modfun, riak_kv_mapreduce, map_object_value},
                       none, true}],
                 {{ok, Pipe}, _NumKeeps} =
                     riak_kv_mrc_pipe:mapred_stream(Spec),
                 riak_pipe:destroy(Pipe),
                 %% this is a hack to make sure that the async sender
                 %% doesn't die immediately upon linking to the
                 %% already-dead builder
                 PipeB = Pipe#pipe{builder=spawn(fake_builder(self()))},
                 {Sender, SenderRef} =
                     riak_kv_mrc_pipe:send_inputs_async(
                       PipeB, [{<<"foo">>, <<"bar">>}]),
                 receive
                     {'DOWN', SenderRef, process, Sender, Error} ->
                         {error, Reason} = Error
                 end,
                 %% let the fake builder shut down now
                 PipeB#pipe.builder ! test_over,
                 %% Each vnode should have received the input, but
                 %% being unable to find the fitting process, returned
                 %% `worker_startup_failed` (and probably also printed
                 %% "fitting was gone before startup")
                 ?assert(lists:member(worker_startup_failed, Reason))
             end)
         ]
     end}.

fake_builder(TestProc) ->
    fun() ->
            Ref = erlang:monitor(process, TestProc),
            receive
                test_over ->
                    ok;
                {'DOWN',Ref,process,TestProc,_} ->
                    ok
            end
    end.

notfound_failover_test_() ->
    IntsBucket = <<"foonum">>,
    NumInts = 5,

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
             %% check the condition that used to bring down a pipe in
             %% https://github.com/basho/riak_kv/issues/290
             %% this version checks it with an actual not-found
             begin
                 QLimit = 3,
                 WaitRef = make_ref(),
                 Spec =
                     [{map,
                       {modfun, riak_kv_mapreduce, map_object_value},
                       <<"include_keydata">>, false},
                      {reduce,
                       {modfun, ?MODULE, reduce_wait_for_signal},
                       [{reduce_phase_batch_size, 1},
                        {wait, {self(), WaitRef}}],
                       true}],
                 PipeSpec = riak_kv_mrc_pipe:mapred_plan(Spec),
                 %% make it easier to fill
                 SmallPipeSpec = [ S#fitting_spec{q_limit=QLimit}
                                   || S <- PipeSpec ],
                 {ok, Pipe} = riak_pipe:exec(SmallPipeSpec,
                                             [{log, sink},
                                              {trace, [error, queue_full]}]),
                 ExistingKey = {IntsBucket, <<"bar1">>},
                 ChashFun = (hd(SmallPipeSpec))#fitting_spec.chashfun,
                 MissingKey = find_adjacent_key(ChashFun, ExistingKey),
                 %% get main workers spun up
                 ok = riak_pipe:queue_work(Pipe, ExistingKey),
                 receive {waiting, WaitRef, ReducePid} -> ok end,

                 %% reduce is now blocking, fill its queue
                 [ ok = riak_pipe:queue_work(Pipe, ExistingKey)
                   || _ <- lists:seq(1, QLimit) ],

                 {NValMod,NValFun} = (hd(SmallPipeSpec))#fitting_spec.nval,
                 NVal = NValMod:NValFun(ExistingKey),

                 %% each of N paths through the primary preflist
                 [ fill_map_queue(Pipe, QLimit, ExistingKey)
                   || _ <- lists:seq(1, NVal) ],

                 %% check get queue actually full
                 ExpectedTOs = lists:duplicate(NVal, timeout),
                 {error, ExpectedTOs} =
                     riak_pipe:queue_work(Pipe, ExistingKey, noblock),

                 %% now inject a missing key that would need to
                 %% failover to the full queue
                 ok = riak_pipe:queue_work(Pipe, {MissingKey, test_passing}),
                 %% and watch for it to block in the reduce queue
                 %% *this* is when pre-patched code would fail:
                 %% we'll receive an [error] trace from the kvget fitting's
                 %% failure to forward the bkey along its preflist
                 ok = consume_queue_full(Pipe, 1),

                 %% let the pipe finish
                 riak_pipe:eoi(Pipe),
                 ReducePid ! {continue, WaitRef},

                 {eoi, Results, Logs} = riak_pipe:collect_results(Pipe),
                 %% the object does not exist, but we told the map
                 %% phase to send on its keydata - check for it
                 ?assert(lists:member({1, test_passing}, Results)),
                 %% just to be a little extra cautious, check for
                 %% other errors
                 ?assertEqual([], [E || {_,{trace,[error],_}}=E <- Logs])
             end),
          ?_test(
             %% check the condition that used to bring down a pipe in
             %% https://github.com/basho/riak_kv/issues/290
             %% this version checks with an object that is missing a replica
             begin
                 QLimit = 3,
                 WaitRef = make_ref(),
                 Spec =
                     [{map,
                       {modfun, riak_kv_mapreduce, map_object_value},
                       none, false},
                      {reduce,
                       {modfun, ?MODULE, reduce_wait_for_signal},
                       [{reduce_phase_batch_size, 1},
                        {wait, {self(), WaitRef}}],
                       true}],
                 PipeSpec = riak_kv_mrc_pipe:mapred_plan(Spec),
                 %% make it easier to fill
                 SmallPipeSpec = [ S#fitting_spec{q_limit=QLimit}
                                   || S <- PipeSpec ],
                 {ok, Pipe} = riak_pipe:exec(SmallPipeSpec,
                                             [{log, sink},
                                              {trace, [error, queue_full]}]),
                 ExistingKey = {IntsBucket, <<"bar1">>},
                 ChashFun = (hd(SmallPipeSpec))#fitting_spec.chashfun,
                 {MissingBucket, MissingKey} =
                     find_adjacent_key(ChashFun, ExistingKey),

                 %% create a value for the "missing" key
                 {ok, C} = riak:local_client(),
                 ok = C:put(riak_object:new(MissingBucket, MissingKey,
                                            test_passing),
                            3),
                 %% and now kill the first replica;
                 %% this will make the vnode local to the kvget pipe
                 %% fitting return an error (because it's the memory
                 %% backend), so it will have to look at another kv vnode
                 [{{PrimaryIndex, _},_}] =
                     riak_core_apl:get_primary_apl(
                       ChashFun({MissingBucket, MissingKey}), 1, riak_kv),
                 {ok, VnodePid} = riak_core_vnode_manager:get_vnode_pid(
                                    PrimaryIndex, riak_kv_vnode),
                 exit(VnodePid, kill),
                 
                 %% get main workers spun up
                 ok = riak_pipe:queue_work(Pipe, ExistingKey),
                 receive {waiting, WaitRef, ReducePid} -> ok end,

                 %% reduce is now blocking, fill its queue
                 [ ok = riak_pipe:queue_work(Pipe, ExistingKey)
                   || _ <- lists:seq(1, QLimit) ],

                 {NValMod,NValFun} = (hd(SmallPipeSpec))#fitting_spec.nval,
                 NVal = NValMod:NValFun(ExistingKey),

                 %% each of N paths through the primary preflist
                 [ fill_map_queue(Pipe, QLimit, ExistingKey)
                   || _ <- lists:seq(1, NVal) ],

                 %% check get queue actually full
                 ExpectedTOs = lists:duplicate(NVal, timeout),
                 {error, ExpectedTOs} =
                     riak_pipe:queue_work(Pipe, ExistingKey, noblock),

                 %% now inject a missing key that would need to
                 %% failover to the full queue
                 ok = riak_pipe:queue_work(Pipe, {MissingBucket, MissingKey}),
                 %% and watch for it to block in the reduce queue
                 %% *this* is when pre-patched code would fail:
                 %% we'll receive an [error] trace from the kvget fitting's
                 %% failure to forward the bkey along its preflist
                 ok = consume_queue_full(Pipe, 1),

                 %% let the pipe finish
                 riak_pipe:eoi(Pipe),
                 ReducePid ! {continue, WaitRef},

                 {eoi, Results, Logs} = riak_pipe:collect_results(Pipe),
                 %% the object does not exist, but we told the map
                 %% phase to send on its keydata - check for it
                 ?assert(lists:member({1, test_passing}, Results)),
                 %% just to be a little extra cautious, check for
                 %% other errors
                 ?assertEqual([], [E || {_,{trace,[error],_}}=E <- Logs])
             end)
         ]
     end}.

fill_map_queue(Pipe, QLimit, ExistingKey) ->
    %% give the map worker one more to block on
    ok = riak_pipe:queue_work(Pipe, ExistingKey, noblock),
    consume_queue_full(Pipe, 1),
    %% map is now blocking, fill its queue
    [ ok = riak_pipe:queue_work(Pipe, ExistingKey, noblock)
      || _ <- lists:seq(1, QLimit) ],
    %% give the get worker one more to block on
    ok = riak_pipe:queue_work(Pipe, ExistingKey, noblock),
    consume_queue_full(Pipe, {xform_map, 0}),
    %% get is now blocking, fill its queue
    [ ok = riak_pipe:queue_work(Pipe, ExistingKey, noblock)
      || _ <- lists:seq(1, QLimit) ],
    ok.

find_adjacent_key({Mod, Fun}, ExistingKey) ->
    [ExistingHead|_] = riak_core_apl:get_primary_apl(
                         Mod:Fun(ExistingKey), 2, riak_kv),
    [K|_] = lists:dropwhile(
              fun(N) ->
                      K = {<<"foonum_missing">>,
                           list_to_binary(integer_to_list(N))},
                      [_,Second] = riak_core_apl:get_primary_apl(
                                     Mod:Fun(K), 2, riak_kv),
                      Second /= ExistingHead
              end,
              lists:seq(1, 1000)),
    {<<"foonum_missing">>, list_to_binary(integer_to_list(K))}.

consume_queue_full(Pipe, FittingName) ->
    {log, {FittingName, {trace, [queue_full], _}}} =
        riak_pipe:receive_result(Pipe, 5000),
    ok.

reduce_wait_for_signal(Inputs, Args) ->
    case get(waited) of
        true ->
            Inputs;
        _ ->
            {TestProc, WaitRef} = proplists:get_value(wait, Args),
            TestProc ! {waiting, WaitRef, self()},
            receive {continue, WaitRef} -> ok end,
            put(waited, true),
            Inputs
    end.

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
