%% -------------------------------------------------------------------
%%
%% Copyright (c) 2011 Basho Technologies, Inc.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License. You may obtain
%% a copy of the License at
%%
%% http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied. See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

%% Test various cluster membership behaviors.
%%
%% This test is designed to run against running Riak nodes, and will be
%% skipped if the following environment variables are not set:
%% (example values corresponding to standard dev release follow)
%%   RIAK_TEST_NODE_1="dev1@127.0.0.1"
%%   RIAK_TEST_NODE_2="dev2@127.0.0.1"
%%   RIAK_TEST_NODE_3="dev3@127.0.0.1"
%%   RIAK_TEST_COOKIE="riak"
%%   RIAK_EUNIT_NODE="eunit@127.0.0.1"

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-module(cluster_test).
-compile(export_all).

cluster_test_() ->
    Envs = ["RIAK_TEST_NODE_1", "RIAK_TEST_NODE_2", "RIAK_TEST_NODE_3",
            "RIAK_TEST_COOKIE", "RIAK_EUNIT_NODE"],
    Vals = [os:getenv(Env) || Env <- Envs],
    case lists:member(false, Vals) of
        true ->
            ?debugFmt("Skipping cluster_test~n", []),
            [];
        false ->
            [Node1V, Node2V, Node3V, CookieV, ENodeV] = Vals,
            Node1 = list_to_atom(Node1V),
            Node2 = list_to_atom(Node2V),
            Node3 = list_to_atom(Node3V),
            Cookie = list_to_atom(CookieV),
            ENode = list_to_atom(ENodeV),

            {spawn, [{setup,
                      fun() ->
                              net_kernel:start([ENode]),
                              erlang:set_cookie(node(), Cookie),
                              {Node1, Node2, Node3}
                      end,
                      fun(_) ->
                              ok
                      end,
                      fun(Args) ->
                              [{timeout, 1000000, ?_test(read_case(Args))}
                              ]
                      end
                     }]}
    end.

read_case({Node1, Node2, _Node3}) ->
    rpc:call(Node1, application, set_env, [riak_core, ring_creation_size, 8]),
    rpc:call(Node2, application, set_env, [riak_core, ring_creation_size, 8]),

    application:set_env(riak_core, ring_creation_size, 4),

    Ring1 = rpc:call(Node1, riak_core_ring, fresh, []),
    Ring2 = rpc:call(Node2, riak_core_ring, fresh, []),
    %% Ring3 = rpc:call(Node3, riak_core_ring, fresh, []),

    rpc:call(Node1, riak_core_ring_manager, set_my_ring, [Ring1]),
    rpc:call(Node2, riak_core_ring_manager, set_my_ring, [Ring2]),
    %% rpc:call(Node3, riak_core_ring_manager, set_my_ring, [Ring3]),

    %% Restart vnodes
    rpc:call(Node1, supervisor, terminate_child, [riak_core_sup, riak_core_vnode_sup]),
    rpc:call(Node1, supervisor, restart_child, [riak_core_sup, riak_core_vnode_sup]),
    rpc:call(Node2, supervisor, terminate_child, [riak_core_sup, riak_core_vnode_sup]),
    rpc:call(Node2, supervisor, restart_child, [riak_core_sup, riak_core_vnode_sup]),

    %% Load data
    ?debugFmt("Loading data~n", []),
    {ok, RC} = riak:client_connect(Node1),
    Data1M = list_to_binary(lists:duplicate(4096,lists:seq(0,255))),
    Count = 1024,
    [RC:put(riak_object:new(<<"cluster_test">>, term_to_binary(N), Data1M))
     || N <- lists:seq(0,Count)],

    ?debugFmt("Joining nodes~n", []),
    rpc:call(Node2, riak_kv_console, join, [[Node1]]),
    %%rpc:call(Node2, riak_core_jc, join, [Node1]),

    ?debugFmt("Sleeping~n", []),
    timer:sleep(65000),

    ?debugFmt("Reading~n", []),
    %% Read
    read(RC, Count, 60000000),
    ok.

read(RC, Size, Duration) ->
    read(RC, Size, Duration, erlang:now()).
read(RC, Size, Duration, T1) ->
    case timer:now_diff(erlang:now(), T1) > Duration of
        true ->
            ok;
        false ->
            lists:foreach(
              fun(N) ->
                      case RC:get(<<"cluster_test">>, term_to_binary(N), 1, 300) of
                          {error, notfound} ->
                              ?debugFmt("Not found~n", []),
                              throw(failed);
                          _ ->
                              ok
                      end
              end, lists:seq(0,Size)),
            read(RC, Size, Duration, T1)
    end.

-endif.
