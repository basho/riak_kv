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
       riak_kv_test_util:common_setup(?MODULE, fun configure/1),
       riak_kv_test_util:common_cleanup(?MODULE, fun configure/1),
       [%% Run the quickcheck tests
        {timeout, 60000, % timeout is in msec
         ?_assertEqual(true, quickcheck(numtests(?TEST_ITERATIONS, ?QC_OUT(prop_basic_listkeys()))))}
       ]
      }
     ]
    }.

%% Call unused callback functions to clear them in the coverage
%% checker so the real code stands out.
coverage_test() ->
    riak_kv_test_util:call_unused_fsm_funs(riak_core_coverage_fsm).

%% ====================================================================
%% eqc property
%% ====================================================================

prop_basic_listkeys() ->
    ?FORALL({ReqId, Bucket, KeyFilter, NVal, ObjectCount, Timeout},
            {g_reqid(), g_bucket(), g_key_filter(), g_n_val(), g_object_count(), g_timeout()},
            ?TRAPEXIT(
               begin
                   riak_kv_memory_backend:reset(),
                   {ok, Client} = riak:local_client(),
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
                   Keys = start_link(ReqId, Bucket, KeyFilter, Timeout),
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

start_link(ReqId, Bucket, Filter, Timeout) ->
    Sink = spawn(?MODULE, data_sink, [ReqId, [], false]),
    From = {raw, ReqId, Sink},
    {ok, _FsmPid} = riak_core_coverage_fsm:start_link(riak_kv_keys_fsm, From, [Bucket, Filter, Timeout]),
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

configure(load) ->
    application:set_env(riak_kv, storage_backend, riak_kv_memory_backend),
    application:set_env(riak_kv, test, true),
    application:set_env(riak_kv, vnode_vclocks, true),
    Out = application:set_env(riak_kv, delete_mode, immediate),
    Out;
configure(_) ->
    ok.

test() ->
    test(100).

test(N) ->
    quickcheck(numtests(N, prop_basic_listkeys())).

check() ->
    check(prop_basic_listkeys(), current_counterexample()).

data_sink(ReqId, KeyList, Done) ->
    receive
        {ReqId, From={_Pid,_Ref}, {keys, Keys}} ->
            riak_kv_keys_fsm:ack_keys(From),
            data_sink(ReqId, KeyList++Keys, false);
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
