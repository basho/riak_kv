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
                bucket,
                caller,
                filters,
                inputs,
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

update_vnodes(ListerPid, VNodes, FilterVNodes) ->
    gen_fsm:send_event(ListerPid, {update_vnodes, VNodes, FilterVNodes}).

%% ===================================================================
%% gen_fsm callbacks
%% ===================================================================

init([ReqId, Caller, Inputs, VNodes, FilterVNodes]) ->
    erlang:monitor(process, Caller),
    {Bucket, Filters} = build_filters(Inputs, VNodes, FilterVNodes),
    {ok, waiting, #state{bucket=Bucket,
                         caller=Caller,
                         filters=Filters,
                         inputs=Inputs,
                         reqid=ReqId,
                         vnodes=VNodes}}.

waiting(start, #state{reqid=ReqId, bucket=Bucket, vnodes=VNodes}=State) ->
    riak_kv_vnode:list_keys(VNodes, ReqId, self(), Bucket),
    {next_state, waiting, State};

waiting({update_vnodes, VNodes, FilterVNodes}, #state{inputs=Inputs}=State) ->
    %% Update the vnodes and build new filters
    {_, Filters} = build_filters(Inputs, VNodes, FilterVNodes),
    {next_state, waiting, State#state{filters=Filters,
                                      vnodes=VNodes}}.

handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

handle_sync_event(_Event, _From, StateName, State) ->
    {reply, ignored, StateName, State}.

handle_info({ReqId, {kl, Idx, Keys}}, waiting, #state{caller=Caller,
                                                      filters=list_buckets,
                                                      reqid=ReqId}=State) ->
    %% Skip the fold if listing buckets
    gen_fsm:send_event(Caller, {ReqId, {kl, {Idx, node()}, lists:usort(Keys)}}),
    {next_state, waiting, State};
handle_info({ReqId, {kl, Idx, Keys0}}, waiting, #state{caller=Caller,
                                                       filters=Filters,
                                                       reqid=ReqId}=State) ->
    Filter = proplists:get_value(Idx, Filters),
    case Filter of
        undefined ->
            %% No need to filter, just return the keys
            gen_fsm:send_event(Caller, {ReqId, {kl, {Idx, node()}, Keys0}});
        _ ->
            case lists:foldl(Filter, [], Keys0) of
                [] ->
                    ok;
                Keys ->
                    gen_fsm:send_event(Caller, {ReqId, {kl, {Idx, node()}, Keys}})
            end
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
build_filters(Inputs, VNodes, FilterVNodes) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    {Bucket, KeyFilter} = build_key_filter(Inputs),

    %% Handling the filtering here is awkward.
    %% Hopefully it will soon be moved to the vnode.
    case KeyFilter of
        %% TODO: Refactor once coverage code is in riak_core.
        list_buckets ->
            Filters = list_buckets;
        _ ->
            %% TODO: Change this to use the upcoming addition to
            %% riak_core_ring that will allow finding the index
            %% responsible for a bkey pair without working out the
            %% entire preflist.
            PrefListFun = fun(X) -> get_first_preflist({Bucket, X}, Ring) end,

            %% Create a function to check which VNodes should be filtered.
            VNodeFilterCheck =
                fun({Index, _}, Filters) ->
                        case proplists:get_value(Index, FilterVNodes) of
                            undefined ->  % No VNode-level filtering required
                                case KeyFilter of
                                    none -> % No key filtering required
                                        Filters;
                                    _ -> % Create the function to do key filtering
                                        Filter = fun(Key, Acc) ->
                                                         case KeyFilter(Key) of
                                                             true ->
                                                                 [Key|Acc];
                                                             false ->
                                                                 Acc
                                                         end
                                                 end,
                                        [{Index, Filter} | Filters]
                                end;
                            KeySpaceIndexes -> % VNode-level filtering is required
                                VNodeFilter = fun(X) ->
                                                      {PrefListIndex, _} = PrefListFun(X),
                                                      lists:member(PrefListIndex, KeySpaceIndexes)
                                              end,
                                case KeyFilter of
                                    none -> % no key filtering
                                        Filter = fun(Key, Acc) ->
                                                         case VNodeFilter(Key) of
                                                             true ->
                                                                 [Key|Acc];
                                                             false ->
                                                                 Acc
                                                         end
                                                 end;
                                    _ -> % key filtering also required
                                        Filter = fun(Key, Acc) ->
                                                         case KeyFilter(Key) andalso VNodeFilter(Key) of
                                                             true ->
                                                                 [Key|Acc];
                                                             false ->
                                                                 Acc
                                                         end
                                                 end
                                end,
                                [{Index, Filter} | Filters]
                        end
                end,
                Filters = lists:foldl(VNodeFilterCheck, [], VNodes)
    end,
    {Bucket, Filters}.

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

