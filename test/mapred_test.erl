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

dep_apps() ->
    DelMe = "./EUnit-SASL.log",
    DataDir = "./EUnit-datadir",
    os:cmd("mkdir " ++ DataDir),
    KillDamnFilterProc = fun() ->
                                 timer:sleep(5),
                                 catch exit(whereis(riak_sysmon_filter), kill),
                                 timer:sleep(5)
                         end,                                 
    Core_Settings = [{handoff_ip, "0.0.0.0"},
                     {handoff_port, 9183},
                     {ring_creation_size, 16},
                     {ring_state_dir, DataDir}],
    KV_Settings = [{storage_backend, riak_kv_ets_backend},
                   {pb_ip, "0.0.0.0"},
                   {pb_port, 48087}], % arbitrary #
    [fun(start) ->
             _ = application:stop(sasl),
             _ = application:load(sasl),
             put(old_sasl_l, app_helper:get_env(sasl, sasl_error_logger)),
             ok = application:set_env(sasl, sasl_error_logger, {file, DelMe}),
             ok = application:start(sasl),
             error_logger:tty(false);
        (stop) ->
             ok = application:stop(sasl),
             ok = application:set_env(sasl, sasl_error_logger, erase(old_sasl_l));
        (fullstop) ->
             _ = application:stop(sasl)
     end,
     %% public_key and ssl are not needed here but started by others so
     %% stop them when we're done.
     crypto, public_key, ssl,
     fun(start) ->
             ok = application:start(riak_sysmon);
        (stop) ->
             ok = application:stop(riak_sysmon),
             KillDamnFilterProc();
        (fullstop) ->
             _ = application:stop(riak_sysmon),
             KillDamnFilterProc()
     end,
     webmachine,
     fun(start) ->
             _ = application:load(riak_core),
             [begin
                  put({?MODULE,AppKey}, app_helper:get_env(riak_core, AppKey)),
                  ok = application:set_env(riak_core, AppKey, Val)
              end || {AppKey, Val} <- Core_Settings],
             ok = application:start(riak_core);
        (stop) ->
             ok = application:stop(riak_core),
             [ok = application:set_env(riak_core, AppKey, get({?MODULE, AppKey}))
              || {AppKey, _Val} <- Core_Settings];
        (fullstop) ->
             _ = application:stop(riak_core)
     end,
     riak_pipe,
     luke,
     erlang_js,
     mochiweb,
     os_mon,
     fun(start) ->
             net_kernel:start([mapred_test@localhost]),
             timer:sleep(50),
             Ring = riak_core_ring:fresh(16, node()),
             riak_core_ring_manager:set_ring_global(Ring),
             _ = application:load(riak_kv),
             [begin
                  put({?MODULE,AppKey}, app_helper:get_env(riak_kv, AppKey)),
                  ok = application:set_env(riak_kv, AppKey, Val)
              end || {AppKey, Val} <- KV_Settings],
             ok = application:start(riak_kv);
        (stop) ->
             ok = application:stop(riak_kv),
             net_kernel:stop(),
             [ok = application:set_env(riak_kv, AppKey, get({?MODULE, AppKey}))
              || {AppKey, _Val} <- KV_Settings];
        (fullstop) ->
             _ = application:stop(riak_kv)
     end].

do_dep_apps(fullstop) ->
    lists:map(fun(A) when is_atom(A) -> _ = application:stop(A);
                 (F)                 -> F(fullstop)
              end, lists:reverse(dep_apps()));
do_dep_apps(StartStop) ->
    Apps = if StartStop == start -> dep_apps();
              StartStop == stop  -> lists:reverse(dep_apps())
           end,
    lists:map(fun(A) when is_atom(A) -> ok = application:StartStop(A);
                 (F)                 -> F(StartStop)
              end, Apps).

prepare_runtime() ->
     fun() ->
             do_dep_apps(fullstop),
             timer:sleep(5),
             do_dep_apps(start),
             timer:sleep(5),
             [foo1, foo2]
     end.

teardown_runtime() ->
     fun(_PrepareThingie) ->
             do_dep_apps(stop),
             timer:sleep(5)
     end.    

setup_demo_test_() ->
    {foreach,
     prepare_runtime(),
     teardown_runtime(),
     [
      fun(_) ->
              {"Setup demo test",
               fun() ->
                       Num = 5,
                       {ok, C} = riak:local_client(),
                       [ok = C:put(riak_object:new(
                                     <<"foonum">>,
                                     list_to_binary("bar"++integer_to_list(X)),
                                     X)) 
                        || X <- lists:seq(1, Num)],
                       [{ok, _} = C:get(<<"foonum">>,
                                      list_to_binary("bar"++integer_to_list(X)))
                        || X <- lists:seq(1, Num)],
                       ok
               end}
      end
     ]
    }.

compat_basic1_test_() ->
    IntsBucket = <<"foonum">>,
    ReduceSumFun = fun(Inputs, _) -> [lists:sum(Inputs)] end,

    {setup,
     prepare_runtime(),
     teardown_runtime(),
     fun(_) ->
         [
          ?_test(
             %% The data created by this step is used by all/most of the
             %% following tests.
             ok = riak_kv_mrc_pipe:example_setup()
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
             %% Basic compat: keep neither stages -> force keep last stage
             begin
                 Spec = 
                     [{map, {modfun, riak_kv_mapreduce, map_object_value},
                       none, false},
                      {reduce, {qfun, ReduceSumFun},
                       none, false}],
                 %% "Crazy" semantics: if only 1 keeper stage, then
                 %% return List instead of [List].
                 {ok, [15]} = riak_kv_mrc_pipe:mapred(IntsBucket, Spec)
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
                       none, true},
                      {reduce, {qfun, ReduceSumFun}, none, true},
                      {reduce, {qfun, ReduceSumFun}, none, true}],
                 {ok, _, [15], [15]} =
                     riak_kv_mrc_pipe:mapred(IntsBucket, Spec)
             end)
          ]
     end}.
