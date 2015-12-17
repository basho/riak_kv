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
         create_plan/6
        ]).

-include_lib("riak_ql/include/riak_ql_ddl.hrl").

%%
create_plan({Query, Bucket}, NVal, _PVC, _ReqId, _NodeCheckService, _Request) ->
    Key = make_key(Query),
    case hash_for_nodes(NVal, Bucket, Key) of
        [] ->
            {error, no_primaries_available};
        VNodes when is_list(VNodes) ->
            NoFilters = [],
            {VNodes, NoFilters}
    end.

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
