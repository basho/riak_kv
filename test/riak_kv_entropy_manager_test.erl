%% -------------------------------------------------------------------
%%
%% Copyright (c) 2012 Basho Technologies, Inc.  All Rights Reserved.
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

-module(riak_kv_entropy_manager_test).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-define(TM, riak_kv_entropy_manager).

set_aae_throttle_test() ->
    try
        _ = ?TM:set_aae_throttle(-4),
        error(u)
    catch error:function_clause ->
            ok;
          error:u ->
            error(unexpected_success);
          _X:_Y ->
            error(wrong_exception)
    end,
    [begin ?TM:set_aae_throttle(V), V = ?TM:get_aae_throttle() end ||
        V <- [5,6]].

set_aae_throttle_limits_test() ->
    {error, _} = ?TM:set_aae_throttle_limits([]),
    {error, _} = ?TM:set_aae_throttle_limits([{5,7}]),
    {error, _} = ?TM:set_aae_throttle_limits([{-1,x}]),
    {error, _} = ?TM:set_aae_throttle_limits([{-1,0}, {x,7}]),
    ok = ?TM:set_aae_throttle_limits([{-1,0}, {100, 500}, {100, 500},
                                      {100, 500}, {100, 500}, {100, 500}]).

%% Drat, EUnit + Meck won't work if this test is inside the
%% riak_kv_entropy_manager.erl module.

side_effects_test_() ->
    BigWait = 100,
    LittleWait = 10,
    {setup,
     fun() ->
             meck:new(?TM, [passthrough]),
             ?TM:set_aae_throttle_limits([{-1, 0},
                                          {30, LittleWait}, {50, BigWait}])
     end,
     fun(_) ->
             ?TM:set_aae_throttle_kill(false),
             meck:unload(?TM),
             ?TM:set_aae_throttle(0)
     end,
     [
      ?_test(
         begin
             State0 = ?TM:make_state(),
             meck:expect(?TM, multicall, fun(_,_,_,_,_) -> {[], [nd]} end),
             State1 = ?TM:query_and_set_aae_throttle(State0),
             BigWait = ?TM:get_last_throttle(State1),

             meck:expect(?TM, multicall, fun(_,_,_,_,_) -> {[{0,nd}], [nd]} end),
             State2 = ?TM:query_and_set_aae_throttle(State1),
             BigWait = ?TM:get_last_throttle(State2),

             meck:expect(?TM, multicall, fun(_,_,_,_,_) -> {[{666,nd}], []} end),
             State3 = ?TM:query_and_set_aae_throttle(State2),
             BigWait = ?TM:get_last_throttle(State3),

             BadRPC_result = {badrpc,{'EXIT',{undef,[{riak_kv_entropy_manager,multicall,[x,x,x],[y,y,y]},{rpc,'-handle_call_call/6-fun-0-',5,[stack,trace,here]}]}}},
             meck:expect(?TM, multicall, fun(_,_,_,_,_) -> { [BadRPC_result], []} end),
             State4 = ?TM:query_and_set_aae_throttle(State3),
             BigWait = ?TM:get_last_throttle(State4),

             ?TM:set_aae_throttle_kill(true),
             meck:expect(?TM, multicall, fun(_,_,_,_,_) -> { [BadRPC_result], []} end),
             State4b = ?TM:query_and_set_aae_throttle(State4),
             0 = ?TM:get_last_throttle(State4b),
             ?TM:set_aae_throttle_kill(false),

             meck:expect(?TM, multicall, fun(_,_,_,_,_) -> {[{31,nd}], []} end),
             State5 = ?TM:query_and_set_aae_throttle(State4b),
             LittleWait = ?TM:get_last_throttle(State5),

             meck:expect(?TM, multicall, fun(_,_,_,_,_) -> {[{2,nd}], []} end),
             State6 = ?TM:query_and_set_aae_throttle(State5),
             0 = ?TM:get_last_throttle(State6)
         end)
      ]
    }.

-endif.
