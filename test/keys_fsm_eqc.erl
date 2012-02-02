%% -------------------------------------------------------------------
%%
%% keys_fsm_eqc: Quickcheck testing for the key listing fsm.
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
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

-module(keys_fsm_eqc).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("riak_kv_vnode.hrl").

-import(fsm_eqc_util, [non_blank_string/0]).

-compile(export_all).

-define(TEST_ITERATIONS, 50).
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).

%%====================================================================
%% eunit test
%%====================================================================

eqc_test_() ->
    {spawn,
     [{setup,
       fun setup/0,
       fun cleanup/1,
       [%% Run the quickcheck tests
        {timeout, 60000, % timeout is in msec
         ?_assertEqual(true, quickcheck(numtests(?TEST_ITERATIONS, ?QC_OUT(prop_basic_listkeys()))))}
       ]
      }
     ]
    }.

setup() ->
    %% Shut logging up - too noisy.
    application:load(sasl),
    application:set_env(sasl, sasl_error_logger, {file, "keys_fsm_eqc_sasl.log"}),
    error_logger:tty(false),
    error_logger:logfile({open, "keys_fsm_eqc.log"}),

    %% Cleanup in case a previous test did not
    cleanup(setup),
    %% Pause the make sure everything is cleaned up
    timer:sleep(2000),

    %% Start erlang node
    TestNode = list_to_atom("testnode" ++ integer_to_list(element(3, now()))),
    net_kernel:start([TestNode, shortnames]),
    do_dep_apps(start, dep_apps()),

    %% Create a fresh ring for the test
    riak_core_ring_manager:ring_trans(
      fun(R0, _Args) ->
              R1 = riak_core_ring:add_member(node(), R0, node()),
              R2 = lists:foldl(fun({I, _OldNode}, RAcc) ->
                                       riak_core_ring:transfer_node(I, node(), RAcc)
                               end, R1, riak_core_ring:all_owners(R0)),
              {new_ring, R2}
      end, undefined),
    ok.

cleanup(_) ->
    do_dep_apps(stop, lists:reverse(dep_apps())),
    catch exit(whereis(riak_kv_vnode_master), kill), %% Leaks occasionally
    catch exit(whereis(riak_sysmon_filter), kill), %% Leaks occasionally
    net_kernel:stop(),
    %% Reset the riak_core vnode_modules
    application:set_env(riak_core, vnode_modules, []),
    ok.

%% Call unused callback functions to clear them in the coverage
%% checker so the real code stands out.
coverage_test() ->
    riak_kv_test_util:call_unused_fsm_funs(riak_core_coverage_fsm).

%% ====================================================================
%% eqc property
%% ====================================================================

prop_basic_listkeys() ->
    ?FORALL({ReqId, Bucket, KeyFilter, NVal, ObjectCount, Timeout, ClientType},
            {g_reqid(), g_bucket(), g_key_filter(), g_n_val(), g_object_count(), g_timeout(), g_client_type()},
        ?TRAPEXIT(
           begin
               {ok, Client} = riak:local_client(),
               {ok, Buckets} = Client:list_buckets(),
               case lists:member(Bucket, Buckets) of
                   true -> % bucket has already been used
                       %% Delete the existing keys in the bucket
                       {ok, OldKeys} = Client:list_keys(Bucket),
                       [Client:delete(Bucket, OldKey) || OldKey <- OldKeys];
                   false ->
                       ok
               end,
               %% Set bucket properties
               BucketProps = riak_core_bucket:get_bucket(Bucket),
               NewBucketProps = orddict:store(n_val, NVal, BucketProps),
               riak_core_bucket:set_bucket(Bucket, NewBucketProps),
               %% Create objects in bucket
               GeneratedKeys = [list_to_binary(integer_to_list(X)) || X <- lists:seq(1, ObjectCount)],
               [ok = Client:put(riak_object:new(Bucket, Key, <<"val">>)) || Key <- GeneratedKeys],

               %% Set the expected output based on if a
               %% key filter is being used or not.
               case KeyFilter of
                   none ->
                       ExpectedKeys = GeneratedKeys;
                   _ ->
                       ExpectedKeyFilter =
                           fun(K, Acc) ->
                                   case KeyFilter(K) of
                                       true ->
                                           [K | Acc];
                                       false ->
                                           Acc
                                   end
                           end,
                       ExpectedKeys = lists:foldl(ExpectedKeyFilter, [], GeneratedKeys)
               end,
               %% Call start_link
               Keys = start_link(ReqId, Bucket, KeyFilter, Timeout, ClientType),
              ?WHENFAIL(
                  begin
                      io:format("Bucket: ~p n_val: ~p ObjectCount: ~p KeyFilter: ~p~n", [Bucket, NVal, ObjectCount, KeyFilter]),
                      io:format("Expected Key Count: ~p Actual Key Count: ~p~n",
                                [length(ExpectedKeys), length(Keys)]),
                      io:format("Expected Keys: ~p~nActual Keys: ~p~n",
                                [ExpectedKeys, lists:sort(Keys)])
                  end,
                  conjunction(
                    [
                     {results, equals(lists:sort(Keys), lists:sort(ExpectedKeys))}
                    ]))

           end
          )).

%%====================================================================
%% Wrappers
%%====================================================================

start_link(ReqId, Bucket, Filter, Timeout, ClientType) ->
    Sink = spawn(?MODULE, data_sink, [ReqId, [], false]),
    From = {raw, ReqId, Sink},
    {ok, _FsmPid} = riak_core_coverage_fsm:start_link(riak_kv_keys_fsm, From, [Bucket, Filter, Timeout, ClientType]),
    wait_for_replies(Sink, ReqId).

%%====================================================================
%% Generators
%%====================================================================

g_bucket() ->
    non_blank_string().

g_key_filter() ->
    %% Create a key filter function.
    %% There will always be at least 10 keys
    %% due to the lower bound of object count
    %% generator.
    MatchKeys = [list_to_binary(integer_to_list(X)) || X <- lists:seq(1,10)],
    KeyFilter =
        fun(X) ->
                lists:member(X, MatchKeys)
        end,
    frequency([{5, none}, {2, KeyFilter}]).

g_client_type() ->
    %% TODO: Incorporate mapred type
    plain.

g_n_val() ->
    choose(1,5).

g_object_count() ->
    choose(10, 2000).

g_reqid() ->
    ?LET(X, noshrink(largeint()), abs(X)).

g_timeout() ->
    choose(10000, 60000).

%%====================================================================
%% Helpers
%%====================================================================

prepare() ->
    application:load(sasl),
    error_logger:delete_report_handler(sasl_report_tty_h),
    error_logger:delete_report_handler(error_logger_tty_h),

    TestNode = list_to_atom("testnode" ++ integer_to_list(element(3, now())) ++
                                "@localhost"),
    {ok, _} = net_kernel:start([TestNode, longnames]),
    do_dep_apps(start, dep_apps()),
    ok.

test() ->
    test(100).

test(N) ->
    quickcheck(numtests(N, prop_basic_listkeys())).

check() ->
    check(prop_basic_listkeys(), current_counterexample()).

dep_apps() ->
    SetupFun =
        fun(start) ->
            %% Set some missing env vars that are normally
            %% part of release packaging.
            application:set_env(riak_core, ring_creation_size, 64),
            application:set_env(riak_kv, storage_backend, riak_kv_memory_backend),
            application:set_env(riak_kv, vnode_vclocks, true),
            application:set_env(riak_kv, delete_mode, immediate),
            application:set_env(riak_kv, legacy_keylisting, false),

            %% Start riak_kv
            timer:sleep(500);
           (stop) ->
            ok
        end,
    XX = fun(_) -> error_logger:info_msg("Registered: ~w\n", [lists:sort(registered())]) end,
    [sasl, crypto, riak_sysmon, webmachine, XX, os_mon,
     lager, riak_core, XX, luke, erlang_js,
     inets, mochiweb, riak_pipe, SetupFun, riak_kv, SetupFun].

do_dep_apps(StartStop, Apps) ->
    lists:map(fun(A) when is_atom(A) -> application:StartStop(A);
                 (F)                 -> F(StartStop)
              end, Apps).

data_sink(ReqId, KeyList, Done) ->
    receive
        {ReqId, {keys, Keys}} ->
            data_sink(ReqId, KeyList++Keys, false);
        {ReqId, done} ->
            data_sink(ReqId, KeyList, true);
        {ReqId, Error} ->
            ?debugFmt("Error occurred: ~p~n", [Error]),
            data_sink(ReqId, [], true);
        {keys, From, ReqId} ->
            From ! {ok, ReqId, KeyList};
        {'done?', From, ReqId} ->
            From ! {ok, ReqId, Done},
            data_sink(ReqId, KeyList, Done);
        Other ->
            ?debugFmt("Unexpected msg: ~p~n", [Other]),
            data_sink(ReqId, KeyList, Done)
    end.

wait_for_replies(Sink, ReqId) ->
    S = self(),
    Sink ! {'done?', S, ReqId},
    receive
        {ok, ReqId, true} ->
            Sink ! {keys, S, ReqId},
            receive
                {ok, ReqId, Keys} ->
                    Keys;
                {ok, ORef, _} ->
                    ?debugFmt("Received keys for older run: ~p~n", [ORef])
            end;
        {ok, ReqId, false} ->
            timer:sleep(100),
            wait_for_replies(Sink, ReqId);
        {ok, ORef, _} ->
            ?debugFmt("Received keys for older run: ~p~n", [ORef])
    end.

 -endif. % EQC

