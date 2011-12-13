%% -------------------------------------------------------------------
%%
%% lk: Helper functions for list keys
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
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
-module(lk).

-export([fsm/1]).

fsm(Bucket) ->
    ReqId = random:uniform(10000),
    Start = erlang:now(),
    riak_kv_keys_fsm:start_link(ReqId, Bucket, 60000, plain, 0.0001, self()),
    {ok, Count} = gather_fsm_results(ReqId, 0),
    End = erlang:now(),
    Ms = erlang:round(timer:now_diff(End, Start) / 1000),
    io:format("Found ~p keys in ~pms.~n", [Count, Ms]).

gather_fsm_results(ReqId, Count) ->
    receive
        {ReqId, From, {keys, Keys}} ->
            riak_kv_keys_fsm:ack_keys(From),
            gather_fsm_results(ReqId, Count + length(Keys));
        {ReqId, {keys, Keys}} ->
            gather_fsm_results(ReqId, Count + length(Keys));
        {ReqId, done} ->
            {ok, Count}
    after 120000 ->
            {error, timeout}
    end.
