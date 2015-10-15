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
create_plan(_VNodeSelector, NVal, _PVC, _ReqId, _NodeCheckService, Request) ->
    Query = riak_local_index:get_query_from_req(Request),
    Key2 = make_key(Query),
    Bucket = riak_kv_util:get_bucket_from_req(Request),
    VNodes = hash_for_nodes(NVal, Bucket, Key2),
    NoFilters = [],
    _CoveragePlan = {VNodes, NoFilters}.

%% This is fugly because the physical format of the startkey
%% which is neede by eleveldb is being used by the query
%% planner which should only know about a more logical format
make_key(#riak_sql_v1{helper_mod    = Mod,
                      partition_key = PartitionKey,
                      'WHERE'       = Where}) ->
    {startkey, StartKey} = proplists:lookup(startkey, Where),
    StartKey2 = [{Field, Val} || {Field, _Type, Val} <- StartKey],
    Key = riak_ql_ddl:make_key(Mod, PartitionKey, StartKey2),
    eleveldb_ts:encode_key(Key).

%%
hash_for_nodes(NVal,
               {_BucketType, _BucketName}=Bucket, Key) when is_binary(Key) ->
    DocIdx = riak_core_util:chash_key({Bucket, Key}),
    UpNodes = riak_core_node_watcher:nodes(riak_kv),
    Perfs = riak_core_apl:get_apl_ann(DocIdx, NVal, UpNodes),
    {VNodes, _} = lists:unzip(Perfs),
    VNodes.
