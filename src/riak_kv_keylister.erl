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
         start_link/5,
         list_keys/1,
         list_keys/2,
         augment_vnodes/2,
         update_vnodes/2
        ]).

%% States
-export([waiting/2]).

%% gen_fsm callbacks
-export([init/1, 
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4]).

-define (DEFAULT_TIMEOUT, 60000).

-record(state, {reqid,
                caller,
                bucket,               
                filter,
                vnodes}).

%% ===================================================================
%% Public API
%% ===================================================================

start_link(ReqId, Caller, Bucket) ->
    start_link(ReqId, Caller, Bucket, [], ?DEFAULT_TIMEOUT).

start_link(ReqId, Caller, Bucket, VNodes, Timeout) ->
    gen_fsm:start_link(?MODULE, [ReqId, Caller, Bucket, VNodes], [{timeout, Timeout}]).

list_keys(ListerPid) ->
    gen_fsm:send_event(ListerPid, start).
    
list_keys(ListerPid, VNode) ->    
    gen_fsm:send_event(ListerPid, {listkeys, VNode}).

augment_vnodes(ListerPid, VNodes) ->
    gen_fsm:send_event(ListerPid, {augment_vnodes, VNodes}).

update_vnodes(ListerPid, VNodes) ->
    gen_fsm:send_event(ListerPid, {update_vnodes, VNodes}).

%% ===================================================================
%% gen_fsm callbacks
%% ===================================================================

init([ReqId, Caller, Inputs, VNodes]) ->
    erlang:monitor(process, Caller),
    {Bucket, Filter} = build_filter(Inputs),    
    {ok, waiting, #state{reqid=ReqId, caller=Caller, bucket=Bucket,
                         filter=Filter, vnodes=VNodes}}.

waiting(start, #state{reqid=ReqId, bucket=Bucket, vnodes=VNodes}=State) ->
    riak_kv_vnode:list_keys(VNodes, ReqId, self(), Bucket),
    {next_state, waiting, State};

waiting({listkeys, VNode}, #state{reqid=ReqId, bucket=Bucket}=State) ->
    riak_kv_vnode:list_keys(VNode, ReqId, self(), Bucket),
    {next_state, waiting, State};

waiting({augment_vnodes, NewVNodes}, #state{vnodes=VNodes}=State) ->
    {next_state, waiting, State#state{vnodes=VNodes++NewVNodes}};

waiting({update_vnodes, VNodes}, State) ->
    {next_state, waiting, State#state{vnodes=VNodes}}.


handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

handle_sync_event(_Event, _From, StateName, State) ->
    {reply, ignored, StateName, State}.

handle_info({ReqId, {kl, Idx, Keys}}, waiting, #state{reqid=ReqId,
                                                       filter=list_buckets, caller=Caller}=State) ->    
    %% Skip the fold if listing buckets
    gen_fsm:send_event(Caller, {ReqId, {kl, {Idx, node()}, lists:usort(Keys)}}),
    {next_state, waiting, State};    
handle_info({ReqId, {kl, Idx, Keys0}}, waiting, #state{reqid=ReqId,
                                                       filter=Filter, caller=Caller}=State) ->    
    F = fun(Key, Acc) ->
                case is_function(Filter) of
                    true ->
                        case Filter(Key) of
                            true ->
                                [Key|Acc];
                            false ->
                                Acc
                        end;
                    false ->
                        [Key|Acc]
                end end,
    case lists:foldl(F, [], Keys0) of
        [] ->
            ok;
        Keys ->
            gen_fsm:send_event(Caller, {ReqId, {kl, {Idx, node()}, Keys}})
    end,
    {next_state, waiting, State};
handle_info({ReqId, Idx, done}, waiting, #state{reqid=ReqId, caller=Caller}=State) ->
    gen_fsm:send_event(Caller, {ReqId, {Idx, node()}, done}),
    {next_state, waiting, State};
handle_info({'DOWN', _MRef, _Type, Caller, _Info}, waiting, #state{caller=Caller}=State) ->
    {stop, normal, State};
handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

terminate(_Reason, _StateName, _State) ->
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%% ====================================================================
%% Internal functions
%% ====================================================================

build_filter('_') ->
    {'_', list_buckets};
build_filter(Bucket) when is_binary(Bucket) ->
    {Bucket, []};
build_filter({filter, Bucket, Fun}) when is_function(Fun) ->
    %% this is the representation used by riak_client:filter_keys
    {Bucket, Fun};
build_filter({Bucket, Filters}) ->
    FilterFun = riak_kv_mapred_filters:compose(Filters),
    {Bucket, FilterFun}.
