%% -------------------------------------------------------------------
%%
%% riak_buckets_fsm: listing of buckets
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

%% @doc The buckets fsm manages the listing of buckets.

-module(riak_kv_buckets_fsm).

-behaviour(riak_core_coverage_fsm).

-include_lib("riak_kv_vnode.hrl").

-export([init/2,
         process_results/3,
         finish/2]).

-type from() :: {raw, req_id(), pid()}.
-type req_id() :: non_neg_integer().

-record(state, {buckets=[] :: [binary()],
                client_type :: plain | mapred,
                from :: from()}).

%% @doc Return a tuple containing the ModFun to call per vnode,
%% the number of primary preflist vnodes the operation
%% should cover, the service to use to check for available nodes,
%% and the registered name to use to access the vnode master process.
init(From={raw, ReqId, ClientPid}, [ItemFilter, Timeout, ClientType]) ->
    case ClientType of
        %% Link to the mapred job so we die if the job dies
        mapred ->
            link(ClientPid);
        _ ->
            ok
    end,
    %% Construct the bucket listing request
    Req = ?KV_LISTBUCKETS_REQ{item_filter=ItemFilter},
    Sender = {fsm, ReqId, self()},
    {Req, Sender, allup, 1, 1, riak_kv, riak_kv_vnode_master, Timeout,
     #state{client_type=ClientType, from=From}}.

process_results({results, VNode, Buckets},
                CoverageVNodes,
                StateData=#state{buckets=BucketAcc}) ->
    case lists:member(VNode, CoverageVNodes) of
        true ->
                                                % Received an expected response from a Vnode
            StateData#state{buckets=(Buckets ++ BucketAcc)};
        false -> % Ignore a response from a VNode that
                                                % is not part of the coverage plan
            StateData
    end.

finish({error, Error},
       StateData=#state{client_type=ClientType,
                        from={raw, ReqId, ClientPid}}) ->
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
       StateData=#state{buckets=Buckets,
                        client_type=ClientType,
                        from={raw, ReqId, ClientPid}}) ->
    case ClientType of
        mapred ->
            luke_flow:add_inputs(Buckets),
            luke_flow:finish_inputs(ClientPid);
        plain ->
            ClientPid ! {ReqId, {buckets, lists:usort(lists:flatten(Buckets))}}
    end,
    {stop, normal, StateData}.
