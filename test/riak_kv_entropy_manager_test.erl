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
    ?assertError(invalid_throttle_limits, ?TM:set_aae_throttle_limits([])),
    ?assertError(invalid_throttle_limits, ?TM:set_aae_throttle_limits([{5,7}])),
    ?assertError(invalid_throttle_limits, ?TM:set_aae_throttle_limits([{-1,x}])),
    ?assertError(invalid_throttle_limits, ?TM:set_aae_throttle_limits([{-1,0}, {x,7}])),
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
             ?TM:enable_aae_throttle(),
             erase(inner_iters),
             meck:unload(?TM),
             ?TM:set_aae_throttle(0)
     end,
     [
      ?_test(
         begin
             State0 = ?TM:make_state(),

             ok = verify_mailbox_is_empty(),
             put(inner_iters, 1),

             BadRPC_result = {badrpc,{'EXIT',{undef,[{riak_kv_entropy_manager,multicall,[x,x,x],[y,y,y]},{rpc,'-handle_call_call/6-fun-0-',5,[stack,trace,here]}]}}},
             Tests1 = [{fun(_,_,_,_,_) -> {[], [nd]} end, BigWait},
                       {fun(_,_,_,_,_) -> {[{0,nd}], [nd]} end, BigWait},
                       {fun(_,_,_,_,_) -> {[{666,nd}], []} end, BigWait},
                       {fun(_,_,_,_,_) -> { [BadRPC_result], []} end, BigWait}],
             Eval = fun({Fun, ExpectThrottle}, St1) ->
                            meck:expect(?TM, multicall, Fun),
                            St3 = lists:foldl(
                                  fun(_, Stx) ->
                                          ?TM:query_and_set_aae_throttle(Stx)
                                  end, St1, lists:seq(1, get(inner_iters))),
                            {Throttle, St4} = ?TM:get_last_throttle(St3),
                            ?assertEqual(ExpectThrottle, Throttle),
                            St4
                    end,

             State10 = lists:foldl(Eval, State0, Tests1),
             ok = verify_mailbox_is_empty(),

             %% Put kill switch test in the middle, to try to catch
             %% problems after the switch is turned off again.
             Tests2 = [{fun(_,_,_,_,_) -> { [BadRPC_result], []} end, 0}],
             ?TM:disable_aae_throttle(),
             State20 = lists:foldl(Eval, State10, Tests2),
             ok = verify_mailbox_is_empty(),
             ?TM:enable_aae_throttle(),

             Tests3 = [{fun(_,_,_,_,_) -> {[{31,nd}], []} end, LittleWait},
                       {fun(_,_,_,_,_) -> {[{2,nd}], []} end, 0}],
             State30 = lists:foldl(Eval, State20, Tests3),
             ok = verify_mailbox_is_empty(),

             put(inner_iters, 10),
             Tests4 = [{fun(_,_,_,_,_) -> timer:sleep(1000), {[{2,nd}], []} end, 0}],
             _State40 = lists:foldl(Eval, State30, Tests4),
             ok = verify_mailbox_is_empty(),
             ok
         end)
      ]
    }.

verify_mailbox_is_empty() ->
    receive
        X ->
            error({mailbox_not_empty, got, X})
    after 0 ->
            ok
    end.

-endif.
