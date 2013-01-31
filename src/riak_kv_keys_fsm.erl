%% -------------------------------------------------------------------
%%
%% riak_keys_fsm: listing of bucket keys
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc The keys fsm manages the listing of bucket keys.
%%
%%      The keys fsm creates a plan to achieve coverage
%%      of all keys from the cluster using the minimum
%%      possible number of VNodes, sends key listing
%%      commands to each of those VNodes, and compiles the
%%      responses.
%%
%%      The number of VNodes required for full
%%      coverage is based on the number
%%      of partitions, the number of available physical
%%      nodes, and the bucket n_val.

-module(riak_kv_keys_fsm).

-behaviour(riak_core_coverage_fsm).

-include_lib("riak_kv_vnode.hrl").

-export([init/2,
         process_results/2,
         finish/2]).
-export([use_ack_backpressure/0,
         req/2,
         ack_keys/1]).

-type from() :: {atom(), req_id(), pid()}.
-type req_id() :: non_neg_integer().

-record(state, {from :: from()}).

-include("riak_kv_dtrace.hrl").

%% @doc Returns `true' if the new ack-based backpressure listkeys
%% protocol should be used.  This decision is based on the
%% `listkeys_backpressure' setting in `riak_kv''s application
%% environment.
-spec use_ack_backpressure() -> boolean().
use_ack_backpressure() ->
    riak_core_capability:get({riak_kv, listkeys_backpressure}, false) == true.

%% @doc Construct the correct listkeys command record.
-spec req(binary(), term()) -> term().
req(Bucket, ItemFilter) ->
    case use_ack_backpressure() of
        true ->
            ?KV_LISTKEYS_REQ{bucket=Bucket,
                             item_filter=ItemFilter};
        false ->
            #riak_kv_listkeys_req_v3{bucket=Bucket,
                                     item_filter=ItemFilter}
    end.

%% @doc Return a tuple containing the ModFun to call per vnode,
%% the number of primary preflist vnodes the operation
%% should cover, the service to use to check for available nodes,
%% and the registered name to use to access the vnode master process.
init(From={_, _, ClientPid}, [Bucket, ItemFilter, Timeout]) ->
    riak_core_dtrace:put_tag(io_lib:format("~p", [Bucket])),
    ClientNode = atom_to_list(node(ClientPid)),
    PidStr = pid_to_list(ClientPid),
    FilterX = if ItemFilter == none -> 0;
                 true               -> 1
              end,
    %% "other" is a legacy term from when MapReduce used this FSM (in
    %% which case, the string "mapred" would appear
    ?DTRACE(?C_KEYS_INIT, [2, FilterX],
            [<<"other">>, ClientNode, PidStr]),
    %% Get the bucket n_val for use in creating a coverage plan
    BucketProps = riak_core_bucket:get_bucket(Bucket),
    NVal = proplists:get_value(n_val, BucketProps),
    %% Construct the key listing request
    Req = req(Bucket, ItemFilter),
    {Req, all, NVal, 1, riak_kv, riak_kv_vnode_master, Timeout,
     #state{from=From}}.

process_results({From, Bucket, Keys},
                StateData=#state{from={raw, ReqId, ClientPid}}) ->
    %% TODO: have caller give us the Idx number.
    ?DTRACE(?C_KEYS_PROCESS_RESULTS, [length(Keys)], []),
    process_keys(Bucket, Keys, ReqId, ClientPid),
    riak_kv_vnode:ack_keys(From), % tell that vnode we're ready for more
    {ok, StateData};
process_results({Bucket, Keys},
                StateData=#state{from={raw, ReqId, ClientPid}}) ->
    ?DTRACE(?C_KEYS_PROCESS_RESULTS, [length(Keys)], []),
    process_keys(Bucket, Keys, ReqId, ClientPid),
    {ok, StateData};
process_results(done, StateData) ->
    {done, StateData};
process_results({error, Reason}, _State) ->
    ?DTRACE(?C_KEYS_PROCESS_RESULTS, [-1], []),
    {error, Reason}.

finish({error, Error},
       StateData=#state{from={raw, ReqId, ClientPid}}) ->
    ?DTRACE(?C_KEYS_FINISH, [-1], []),
    %% Notify the requesting client that an error
    %% occurred or the timeout has elapsed.
    ClientPid ! {ReqId, Error},
    {stop, normal, StateData};
finish(clean,
       StateData=#state{from={raw, ReqId, ClientPid}}) ->
    ClientPid ! {ReqId, done},
    ?DTRACE(?C_KEYS_FINISH, [0], []),
    {stop, normal, StateData}.

%% ===================================================================
%% Internal functions
%% ===================================================================

%% @doc If a listkeys request sends a result of `{ReqId, From, {keys,
%% Items}}', that means it wants acknowledgement of those items before
%% it will send more.  Call this function with that `From' to trigger
%% the next batch.
-spec ack_keys(From::{pid(), reference()}) -> term().
ack_keys({Pid, Ref}) ->
    Pid ! {Ref, ok}.

process_keys(_Bucket, Keys, ReqId, ClientPid) ->
    case use_ack_backpressure() of
        true ->
            Monitor = erlang:monitor(process, ClientPid),
            ClientPid ! {ReqId, {self(), Monitor}, {keys, Keys}},
            receive
                {Monitor, ok} ->
                    erlang:demonitor(Monitor, [flush]);
                {'DOWN', Monitor, process, _Pid, _Reason} ->
                    exit(self(), normal)
            end;
        false ->
            ClientPid ! {ReqId, {keys, Keys}}
    end.
