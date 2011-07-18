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
         process_results/3,
         finish/2]).

-type from() :: {raw, req_id(), pid()}.
-type req_id() :: non_neg_integer().

-record(state, {client_type :: plain | mapred,
                from :: from()}).

%% @doc Return a tuple containing the ModFun to call per vnode,
%% the number of primary preflist vnodes the operation
%% should cover, the service to use to check for available nodes,
%% and the registered name to use to access the vnode master process.
init(From={raw, ReqId, ClientPid}, [Bucket, ItemFilter, Timeout, ClientType]) ->
    case ClientType of
        %% Link to the mapred job so we die if the job dies
        mapred ->
            link(ClientPid);
        _ ->
            ok
    end,
    %% Get the bucket n_val for use in creating a coverage plan
    BucketProps = riak_core_bucket:get_bucket(Bucket),
    NVal = proplists:get_value(n_val, BucketProps),
    %% Construct the key listing request
    Req = ?KV_LISTKEYS_REQ{bucket=Bucket,
                           item_filter=ItemFilter},
    Sender = {fsm, ReqId, self()},
    {Req, Sender, all, NVal, 1, riak_kv, riak_kv_vnode_master, Timeout,
     #state{client_type=ClientType, from=From}}.

process_results({results, VNode, {Bucket, Keys}},
                CoverageVNodes,
                StateData=#state{client_type=ClientType,
                                 from={raw, ReqId, ClientPid}}) ->
    case lists:member(VNode, CoverageVNodes) of
        true ->
             % Received an expected response from a Vnode
            case ClientType of
                mapred ->
                    try
                        luke_flow:add_inputs(ClientPid, [{Bucket, Key} || Key <- Keys])
                    catch _:_ ->
                            exit(self(), normal)
                    end;
                plain -> ClientPid ! {ReqId, {keys, Keys}}
            end;
        false -> % Ignore a response from a VNode that
                 % is not part of the coverage plan
            ignore
    end,
    StateData;
process_results({final_results, VNode, {Bucket, Keys}},
                CoverageVNodes,
                StateData=#state{client_type=ClientType,
                                 from={raw, ReqId, ClientPid}}) ->
    case lists:member(VNode, CoverageVNodes) of
        true -> % Received an expected response from a Vnode
            case ClientType of
                mapred ->
                    try
                        luke_flow:add_inputs(ClientPid, [{Bucket, Key} || Key <- Keys])
                    catch _:_ ->
                            exit(self(), normal)
                    end;
                plain -> ClientPid ! {ReqId, {keys, Keys}}
            end,
            %% Inform the coverage fsm that all results
            %% are in for this vnode.
            {done, VNode, StateData};
        false -> % Ignore a response from a VNode that
                 % is not part of the coverage plan
            StateData
    end.

finish({error, Error},
       StateData=#state{from={raw, ReqId, ClientPid},
                        client_type=ClientType}) ->
    case ClientType of
        mapred ->
            %% An error occurred or the timeout interval elapsed
            %% so all we can do now is die so that the rest of the
            %% MapReduce processes will also die and be cleaned up.
            exit(Error);
        plain ->
            %% Notify the requesting client that an error
            %% occurred or the timeout has elapsed.
            ClientPid ! {ReqId, Error}
    end,
    {stop, normal, StateData};
finish(clean,
       StateData=#state{from={raw, ReqId, ClientPid},
                        client_type=ClientType}) ->
    case ClientType of
        mapred ->
            luke_flow:finish_inputs(ClientPid);
        plain ->
            ClientPid ! {ReqId, done}
    end,
    {stop, normal, StateData}.
