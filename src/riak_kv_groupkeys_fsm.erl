%% -------------------------------------------------------------------
%%
%% riak_kv_groupkeys_fsm: list keys grouped by params, e.g. prefix
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

-module(riak_kv_groupkeys_fsm).

-behaviour(riak_core_coverage_fsm).

%-include_lib("riak_kv_vnode.hrl").

-export([ack_keys/1]).
-export([init/2,
         process_results/2,
         finish/2]).

-type req_id() :: non_neg_integer().
-type from() :: {raw, req_id(), pid()}.

-record(state, {from :: from()}).


%%
%% public API
%%
-spec ack_keys(From::{pid(), reference()}) -> term().
ack_keys({Pid, Ref}) ->
    Pid ! {Ref, ok}.

%%
%% riak_core_coverage_fsm callbacks
%%

init(From, [Bucket, GroupParams, Timeout]) ->
    Req = req(Bucket, GroupParams),
    NVal = get_nval(Bucket),
    {
        Req, all, NVal, 1, riak_kv, riak_kv_vnode_master, Timeout,
        #state{from=From}
    }.

process_results({From, Bucket, Keys},
                StateData=#state{from={raw, ReqId, ClientPid}}) ->
    process_keys(Bucket, Keys, ReqId, ClientPid),
    _ = riak_kv_vnode:ack_keys(From),
    {ok, StateData};
process_results({error, Reason}, _State) ->
    {error, Reason};
process_results({Bucket, Keys},
                StateData=#state{from={raw, ReqId, ClientPid}}) ->
    process_keys(Bucket, Keys, ReqId, ClientPid),
    {ok, StateData};
process_results(done, StateData) ->
    {done, StateData}.

finish({error, _}=Error,
       StateData=#state{from={raw, ReqId, ClientPid}}) ->
    %% Notify the requesting client that an error
    %% occurred or the timeout has elapsed.
    ClientPid ! {ReqId, Error},
    {stop, normal, StateData};
finish(clean,
       StateData=#state{from={raw, ReqId, ClientPid}}) ->
    ClientPid ! {ReqId, done},
    {stop, normal, StateData}.

%%
%% Internal functions
%%

-spec req(binary(), term()) -> term().
req(Bucket, GroupParams) ->
    riak_kv_requests:new_listgroupkeys_request(Bucket, GroupParams).

get_nval(Bucket) ->
    BucketProps = riak_core_bucket:get_bucket(Bucket),
    proplists:get_value(n_val, BucketProps).

process_keys(_Bucket, Keys, ReqId, ClientPid) ->
    ClientPid ! {ReqId, {keys, Keys}}.
