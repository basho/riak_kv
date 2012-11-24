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
         process_results/2,
         finish/2]).

-type from() :: {atom(), req_id(), pid()}.
-type req_id() :: non_neg_integer().

-record(state, {buckets=sets:new() :: [term()],
                from :: from()}).

-include("riak_kv_dtrace.hrl").

%% @doc Return a tuple containing the ModFun to call per vnode,
%% the number of primary preflist vnodes the operation
%% should cover, the service to use to check for available nodes,
%% and the registered name to use to access the vnode master process.
init(From={_, _, ClientPid}, [ItemFilter, Timeout]) ->
    ClientNode = atom_to_list(node(ClientPid)),
    PidStr = pid_to_list(ClientPid),
    FilterX = if ItemFilter == none -> 0;
                 true               -> 1
              end,
    %% "other" is a legacy term from when MapReduce used this FSM (in
    %% which case, the string "mapred" would appear
    ?DTRACE(?C_BUCKETS_INIT, [2, FilterX],
            [<<"other">>, ClientNode, PidStr]),
    %% Construct the bucket listing request
    Req = ?KV_LISTBUCKETS_REQ{item_filter=ItemFilter},
    {Req, allup, 1, 1, riak_kv, riak_kv_vnode_master, Timeout,
     #state{from=From}}.

process_results(done, StateData) ->
    {done, StateData};
process_results(Buckets,
                StateData=#state{buckets=BucketAcc}) ->
    ?DTRACE(?C_BUCKETS_PROCESS_RESULTS, [length(Buckets)], []),
    {ok, StateData#state{buckets=sets:union(sets:from_list(Buckets),
                                               BucketAcc)}};
process_results({error, Reason}, _State) ->
    ?DTRACE(?C_BUCKETS_PROCESS_RESULTS, [-1], []),
    {error, Reason}.

finish({error, Error},
       StateData=#state{from={raw, ReqId, ClientPid}}) ->
    ?DTRACE(?C_BUCKETS_FINISH, [-1], []),
    %% Notify the requesting client that an error
    %% occurred or the timeout has elapsed.
    ClientPid ! {ReqId, Error},
    {stop, normal, StateData};
finish(clean,
       StateData=#state{buckets=Buckets,
                        from={raw, ReqId, ClientPid}}) ->
    ClientPid ! {ReqId, {buckets, sets:to_list(Buckets)}},
    ?DTRACE(?C_BUCKETS_FINISH, [0], []),
    {stop, normal, StateData}.
