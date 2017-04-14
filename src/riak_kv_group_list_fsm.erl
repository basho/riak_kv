%% -------------------------------------------------------------------
%%
%% riak_kv_group_keys_fsm: list keys grouped by params, e.g. prefix
%%
%% Copyright (c) 2017 Basho Technologies, Inc.  All Rights Reserved.
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

-module(riak_kv_group_list_fsm).

-behaviour(riak_core_coverage_fsm).

%-include_lib("riak_kv_vnode.hrl").

-export([init/2,
         process_results/2,
         finish/2]).

-type req_id() :: non_neg_integer().
-type from() :: {raw, req_id(), pid()}.

-record(state, {
          from :: from(),
          bucket :: riak_object:bucket(),
          entries_acc = ordsets:new() :: ordsets:ordset(any()),
          metadatas_acc = [] :: list(),
          common_prefixes_acc = [] :: list(),
          group_params :: any()
         }).

%%
%% riak_core_coverage_fsm callbacks
%%

init(From, [Bucket, GroupParams, Timeout]) ->
    Req = req(Bucket, GroupParams),
    NVal = get_nval(Bucket),
    {
        Req, all, NVal, 1, riak_kv, riak_kv_vnode_master, Timeout,
        #state{from=From, bucket=Bucket, group_params = GroupParams}
    }.

process_results({From, Bucket, Entries}, StateData) ->
    NewState = process_entries(Bucket, Entries, StateData),
    _ = riak_kv_vnode:ack_keys(From),
    {ok, NewState};
process_results({error, Reason}, _State) ->
    {error, Reason};
process_results({Bucket, Entries}, StateData) ->
    NewState = process_entries(Bucket, Entries, StateData),
    {ok, NewState};
process_results(done, StateData) ->
    {done, StateData}.

finish({error, _}=Error,
       StateData=#state{from={raw, ReqId, ClientPid}}) ->
    riak_kv_stat:update(list_group_fsm_finish_error),
    %% Notify the requesting client that an error
    %% occurred or the timeout has elapsed.
    ClientPid ! {ReqId, Error},
    {stop, normal, StateData};
finish(clean,
       StateData=#state{from={raw, ReqId, ClientPid}}) ->
    Response = collate_list_group_results(StateData),
    riak_kv_stat:update({list_group_fsm_finish_count,
                         length(riak_kv_group_list_response:get_metadatas(Response)) +
                         length(riak_kv_group_list_response:get_common_prefixes(Response))}),
    ClientPid ! {ReqId, done, Response},
    {stop, normal, StateData}.

%%
%% Internal functions
%%

-spec req(binary(), term()) -> term().
req(Bucket, GroupParams) ->
    riak_kv_requests:new_list_group_request(Bucket, GroupParams).

get_nval(Bucket) ->
    BucketProps = riak_core_bucket:get_bucket(Bucket),
    proplists:get_value(n_val, BucketProps).


process_entries(_Bucket, Entries, State) ->
    lists:foldl(fun process_entry/2, State, Entries).

%% In the heads: we should only have results from TargetBucket by now; crash if we don't.
%% We are using an ordset to both ensure we have no duplicate Keys or CommonPrefixes, and to ensure
%% the correct utf8 lexicographic sort-order. In the CommonPrefix case, we double the CommonPrefix
%% so that the tuple-size doesn't interfere with the interleaving of Keys and CommonPrefixes in the
%% sort order even though we throw it away later in partition entry.
process_entry({{TargetBucket, Key}, Metadata = {metadata, _M}}, State = #state{bucket=TargetBucket}) ->
    State#state{entries_acc = ordsets:add_element({Key, Metadata}, State#state.entries_acc)};
process_entry({{TargetBucket, _}, {common_prefix, CommonPrefix}}, State = #state{bucket=TargetBucket})->
    State#state{entries_acc = ordsets:add_element({CommonPrefix, CommonPrefix}, State#state.entries_acc)}.

collate_list_group_results(State0 = #state{entries_acc = Entries0, group_params = GroupParams}) ->
    MaxKeys = riak_kv_group_list:get_max_keys(GroupParams),
    Entries = ordsets:to_list(Entries0),
    TruncatedEntries = lists:sublist(Entries, MaxKeys),
    NextContinuationToken = riak_kv_continuation:make_token(Entries,
                                                            MaxKeys,
                                                            fun({Key, _}) -> Key end),
    State = lists:foldr(fun partition_entry/2, State0, TruncatedEntries),
    riak_kv_group_list_response:new_response(State#state.metadatas_acc,
                                             State#state.common_prefixes_acc,
                                             NextContinuationToken).

partition_entry({Key, {metadata, Metadata}},
                State = #state{metadatas_acc = MetadatasAcc}) ->
    State#state{metadatas_acc = [{Key, Metadata} | MetadatasAcc]};
partition_entry({CommonPrefix, _CommonPrefix},
                State = #state{common_prefixes_acc = CommonPrefixesAcc}) ->
    State#state{common_prefixes_acc = [CommonPrefix | CommonPrefixesAcc]}.
