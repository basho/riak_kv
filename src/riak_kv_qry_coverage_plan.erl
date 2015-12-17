%% -------------------------------------------------------------------
%%
%% riak_kv_qry_coverage: generate the coverage for a hashed query
%%
%%
%% Copyright (c) 2015 Basho Technologies, Inc.  All Rights Reserved.
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
-module(riak_kv_qry_coverage_plan).

-export([
         create_plan/5,
         create_plan/6
        ]).

-include_lib("riak_ql/include/riak_ql_ddl.hrl").

%%
create_plan({Query, Bucket}, NVal, PVC, ReqId, NodeCheckService) ->
    create_plan({Query, Bucket}, NVal, PVC, ReqId, NodeCheckService, undefined).

create_plan({Query, Bucket}, NVal, PVC, ReqId, NodeCheckService, Request) when not is_integer(ReqId) ->
    %% ReqId generation code stolen from `riak_client'. Unclear why
    %% riak_kv_qry_worker sends us a tuple (qid) as a request id but
    %% that's a battle for another day
    create_plan({Query, Bucket}, NVal, PVC,
                erlang:phash2({self(), os:timestamp()}), NodeCheckService, Request);
%% PVC is generally 1 anyway, no support for it yet
create_plan({Query, Bucket}, NVal, _PVC, ReqId, NodeCheckService, _Request) ->
    {ok, CHBin} = riak_core_ring_manager:get_chash_bin(),
    RingSize = chashbin:num_partitions(CHBin),
    UnavailableVNodes =
        riak_core_coverage_plan:identify_unavailable_vnodes(CHBin, RingSize,
                                                            NodeCheckService),

    Key = make_key(Query),
    case hash_for_nodes(NVal, Bucket, Key) of
        [] ->
            {error, no_primaries_available};
        VNodes when is_list(VNodes) ->
            AvailVNodes = filter_down_vnodes(VNodes, UnavailableVNodes, RingSize),
            case AvailVNodes of
                [] ->
                    {error, no_primaries_available};
                _ ->
                    AvailCount = length(AvailVNodes),
                    Offset = ReqId rem AvailCount,
                    NoFilters = [],
                    Which =
                        riak_core_coverage_plan:add_offset(0, Offset, AvailCount),
                    {[lists:nth(Which+1, AvailVNodes)], NoFilters}
            end
    end.

%% VNodes is a list of tuples:
%%  [{91343852333181432387730302044767688728495783936, 'dev1@127.0.0.1'}]
%% Unavail is a list of quite different tuples:
%%  [{vnode_id, 3, 'dev1@127.0.0.1'}]
%% where 3 is a vnode ID that maps to a longer index (value depends on ring size)
%%
%% Anything in the unavailable list must be removed from VNodes.
filter_down_vnodes(VNodes, Unavail, RingSize) ->
    RingIndexInc = chash:ring_increment(RingSize),
    VNodes -- lists:map(fun({vnode_id, Id, Node}) -> {Id * RingIndexInc, Node} end,
                        Unavail).

%% This is fugly because the physical format of the startkey
%% which is neede by eleveldb is being used by the query
%% planner which should only know about a more logical format
make_key(#riak_sql_v1{helper_mod    = Mod,
                      partition_key = PartitionKey,
                      'WHERE'       = Where}) ->
    {startkey, StartKey} = proplists:lookup(startkey, Where),
    StartKey2 = [{Field, Val} || {Field, _Type, Val} <- StartKey],
    Key = riak_ql_ddl:make_key(Mod, PartitionKey, StartKey2),
    riak_kv_ts_util:encode_typeval_key(Key).

%%
hash_for_nodes(NVal,
               {_BucketType, _BucketName}=Bucket, Key) ->
    DocIdx = riak_core_util:chash_key({Bucket, Key}),
    % Fallbacks cannot be used for query results because they may have partial
    % or empty results and the riak_kv_qry_worker currently uses the first
    % result from a sub query, the primary must be used to get correct results
    Perfs = riak_core_apl:get_primary_apl(DocIdx, NVal, riak_kv),
    {VNodes, _} = lists:unzip(Perfs),
    VNodes.
