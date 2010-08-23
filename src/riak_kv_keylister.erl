%% -------------------------------------------------------------------
%%
%% riak_kv_keylister: Manage streaming keys for a bucket from a
%%                    cluster node
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

-module(riak_kv_keylister).

-behaviour(gen_fsm).

%% API
-export([start_link/3,
         list_keys/2]).

%% States
-export([waiting/2]).

%% gen_fsm callbacks
-export([init/1, state_name/2, state_name/3, handle_event/3,
         handle_sync_event/4, handle_info/3, terminate/3, code_change/4]).

-record(state, {reqid,
                caller,
                bucket,
                bloom}).

list_keys(ListerPid, VNode) ->
    gen_fsm:send_event(ListerPid, {lk, VNode}).

start_link(ReqId, Caller, Bucket) ->
    gen_fsm:start_link(?MODULE, [ReqId, Caller, Bucket], []).

init([ReqId, Caller, Bucket]) ->
    erlang:monitor(process, Caller),
    {ok, Bloom} = ebloom:new(10000000, 0.0001, crypto:rand_uniform(1, 5000)),
    {ok, waiting, #state{reqid=ReqId, caller=Caller, bloom=Bloom, bucket=Bucket}}.

waiting({lk, VNode}, #state{reqid=ReqId, bucket=Bucket}=State) ->
    riak_kv_vnode:list_keys(VNode, ReqId, self(), Bucket),
    {next_state, waiting, State}.

state_name(_Event, State) ->
    {next_state, waiting, State}.

state_name(_Event, _From, State) ->
    {reply, ignored, state_name, State}.

handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

handle_sync_event(_Event, _From, StateName, State) ->
    {reply, ignored, StateName, State}.

handle_info({ReqId, {kl, Idx, Keys0}}, waiting, #state{reqid=ReqId, bloom=Bloom,
                                                       caller=Caller}=State) ->
    F = fun(Key, Acc) ->
                case ebloom:contains(Bloom, Key) of
                    true ->
                        Acc;
                    false ->
                        ebloom:insert(Bloom, Key),
                        [Key|Acc]
                end end,
    case lists:foldl(F, [], Keys0) of
        [] ->
            ok;
        Keys ->
            gen_fsm:send_event(Caller, {ReqId, {kl, Idx, Keys}})
    end,
    {next_state, waiting, State};
handle_info({ReqId, Idx, done}, waiting, #state{reqid=ReqId, caller=Caller}=State) ->
    gen_fsm:send_event(Caller, {ReqId, Idx, done}),
    {next_state, waiting, State};
handle_info({'DOWN', _MRef, _Type, Caller, _Info}, waiting, #state{caller=Caller}=State) ->
    {stop, normal, State};
handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

terminate(_Reason, _StateName, #state{bloom=Bloom}) ->
    ebloom:clear(Bloom),
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%% Internal functions
