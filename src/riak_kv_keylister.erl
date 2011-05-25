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
         start_link/6,
         list_keys/1,
         list_keys/2,
         update_vnodes/3
        ]).

%% gen_fsm callbacks
-export([init/1,
         waiting/2,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4]).

-define (DEFAULT_TIMEOUT, 60000).

-record(state, {reqid,
                caller,
                bucket,
                filter_vnodes,
                key_filter,
                preflist_fun,
                vnodes}).

%% ===================================================================
%% Public API
%% ===================================================================

start_link(ReqId, Caller, Bucket) ->
    start_link(ReqId, Caller, Bucket, [], undefined, ?DEFAULT_TIMEOUT).

start_link(ReqId, Caller, Bucket, VNodes, Timeout) ->
    start_link(ReqId, Caller, Bucket, VNodes, undefined, Timeout).

start_link(ReqId, Caller, Bucket, VNodes, FilterVNodes, Timeout) ->
    gen_fsm:start_link(?MODULE, [ReqId, Caller, Bucket, VNodes, FilterVNodes], [{timeout, Timeout}]).

list_keys(ListerPid) ->
    gen_fsm:send_event(ListerPid, start).

list_keys(ListerPid, VNode) ->
    gen_fsm:send_event(ListerPid, {listkeys, VNode}).

update_vnodes(ListerPid, VNodes, FilterVNodes) ->
    gen_fsm:send_event(ListerPid, {update_vnodes, VNodes, FilterVNodes}).

%% ===================================================================
%% gen_fsm callbacks
%% ===================================================================

init([ReqId, Caller, Inputs, VNodes, FilterVNodes]) ->
    erlang:monitor(process, Caller),
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    {Bucket, KeyFilter} = build_key_filter(Inputs),
    PrefListFun = fun(X) -> get_first_preflist({Bucket, X}, Ring) end,
    {ok, waiting, #state{bucket=Bucket,
                         caller=Caller,
                         filter_vnodes=FilterVNodes,
                         key_filter=KeyFilter,
                         preflist_fun=PrefListFun,
                         reqid=ReqId,
                         vnodes=VNodes}}.

waiting(start, #state{reqid=ReqId, bucket=Bucket, vnodes=VNodes}=State) ->
    riak_kv_vnode:list_keys(VNodes, ReqId, self(), Bucket),
    {next_state, waiting, State};

waiting({listkeys, VNode}, #state{reqid=ReqId, bucket=Bucket}=State) ->
    riak_kv_vnode:list_keys(VNode, ReqId, self(), Bucket),
    {next_state, waiting, State};

waiting({update_vnodes, VNodes, FilterVNodes}, State) ->
    {next_state, waiting, State#state{filter_vnodes=FilterVNodes,
                                      vnodes=VNodes}}.


handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

handle_sync_event(_Event, _From, StateName, State) ->
    {reply, ignored, StateName, State}.

handle_info({ReqId, {kl, Idx, Keys}}, waiting, #state{caller=Caller,
                                                      key_filter=list_buckets,
                                                      reqid=ReqId}=State) ->
    %% Skip the fold if listing buckets
    gen_fsm:send_event(Caller, {ReqId, {kl, {Idx, node()}, lists:usort(Keys)}}),
    {next_state, waiting, State};
handle_info({ReqId, {kl, Idx, Keys}}, waiting, #state{caller=Caller,
                                                      filter_vnodes=undefined,
                                                      key_filter=none,
                                                      reqid=ReqId}=State) ->
    %% No need to filter so just return the keys
    gen_fsm:send_event(Caller, {ReqId, {kl, {Idx, node()}, Keys}}),
    {next_state, waiting, State};
handle_info({ReqId, {kl, Idx, Keys0}}, waiting, #state{caller=Caller,
                                                       filter_vnodes=undefined,
                                                       key_filter=KeyFilter,
                                                       reqid=ReqId}=State) ->
    %% Key filtering fun!
    FilterFun = fun(Key, Acc) ->
                        case KeyFilter(Key) of
                            true ->
                                [Key|Acc];
                            false ->
                                Acc
                        end
                end,
    %% Fold over the keys to perform the filtering
    case lists:foldl(FilterFun, [], Keys0) of
        [] ->
            ok;
        Keys ->
            gen_fsm:send_event(Caller, {ReqId, {kl, {Idx, node()}, Keys}})
    end,
    {next_state, waiting, State};
handle_info({ReqId, {kl, Idx, Keys0}}, waiting, #state{caller=Caller,
                                                       filter_vnodes=FilterVNodes,
                                                       key_filter=none,
                                                       preflist_fun=PrefListFun,
                                                       reqid=ReqId}=State) ->
    %% Create the VNode filtering fun
    case proplists:get_value(Idx, FilterVNodes) of
        undefined ->
            VNodeFilter = fun(_) -> true end;
        KeySpaceIndexes ->
            VNodeFilter = fun(X) ->
                                  {PrefListIndex, _} = PrefListFun(X),
                                  lists:member(PrefListIndex, KeySpaceIndexes)
                          end
    end,
    %% Use the key and vnode filters to get the correct keys
    FilterFun = fun(Key, Acc) ->
                        case VNodeFilter(Key) of
                            true ->
                                [Key|Acc];
                            false ->
                                Acc
                        end
                end,
    %% Fold over the keys to perform the filtering
    case lists:foldl(FilterFun, [], Keys0) of
        [] ->
            ok;
        Keys ->
            gen_fsm:send_event(Caller, {ReqId, {kl, {Idx, node()}, Keys}})
    end,
    {next_state, waiting, State};
handle_info({ReqId, {kl, Idx, Keys0}}, waiting, #state{caller=Caller,
                                                       filter_vnodes=FilterVNodes,
                                                       key_filter=KeyFilter,
                                                       preflist_fun=PrefListFun,
                                                       reqid=ReqId}=State) ->
    %% Create the VNode filtering fun
    case proplists:get_value(Idx, FilterVNodes) of
        undefined ->
            VNodeFilter = fun(_) -> true end;
        KeySpaceIndexes ->
            VNodeFilter = fun(X) ->
                                  FirstPrefList = PrefListFun(X),
                                  lists:member(FirstPrefList, KeySpaceIndexes)
                          end
    end,
    %% Use the key and vnode filters to get the correct keys
    FilterFun = fun(Key, Acc) ->
                        case KeyFilter(Key) andalso VNodeFilter(Key) of
                            true ->
                                [Key|Acc];
                            false ->
                                Acc
                        end
                end,
    %% Fold over the keys to perform the filtering
    case lists:foldl(FilterFun, [], Keys0) of
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

%% @private
build_key_filter('_') ->
    {'_', list_buckets};
build_key_filter(Bucket) when is_binary(Bucket) ->
    {Bucket, none};
build_key_filter({filter, Bucket, Fun}) when is_function(Fun) ->
    %% this is the representation used by riak_client:filter_keys
    {Bucket, Fun};
build_key_filter({Bucket, Filters}) ->
    FilterFun = riak_kv_mapred_filters:compose(Filters),
    {Bucket, FilterFun}.

%% @private
get_first_preflist({Bucket, Key}, Ring) ->
    %% Get the chash key for the bucket-key pair and
    %% use that to determine the preference list to
    %% use in filtering the keys from this VNode.
    ChashKey = riak_core_util:chash_key({Bucket, Key}),
    hd(riak_core_ring:preflist(ChashKey, Ring)).
