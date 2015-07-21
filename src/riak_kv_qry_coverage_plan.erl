%% -------------------------------------------------------------------
%%
%% riak_kv_qry_coverage: generate the coverage for a hashed query
%%
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
-module(riak_kv_qry_coverage_plan).

-export([
	 create_plan/6
	]).
create_plan(VNodeSelector, NVal, PVC, ReqId, NodeCheckService, Request) ->
    io:format("in riak_kv_qry_coverage_plan with~n:" ++
    		  "- VNodeSelector:   ~p~n" ++
		  "- NVal:            ~p~n" ++
		  "- PVC              ~p~n" ++
		  "- ReqId            ~p~n" ++
		  "- NodeCheckService ~p~n" ++
		  "- Request          ~p~n",
    	      [VNodeSelector, NVal, PVC, ReqId, NodeCheckService,
	       Request]),
    BucketName = <<"TsBucket3">>, % riak_kv_util:get_bucket_from_req(Request),
    Query = riak_local_index:get_query_from_req(Request),
    Key = riak_local_index:get_key_from_li_query(Query),
    DocIdx = riak_core_util:chash_key({BucketName, Key}),
    BucketProps = riak_core_bucket:get_bucket(BucketName),
    UpNodes = riak_core_node_watcher:nodes(riak_kv),
    Perfs = riak_core_apl:get_apl_ann(DocIdx, NVal, UpNodes),
    {VNodes, _} = lists:unzip(Perfs),
    NoFilters = [],
    io:format("More stuff:~n" ++
		  "- BucketName:  ~p~n- Query:       ~p~n" ++
		  "- Key:         ~p~n- DocIdx:      ~p~n" ++
		  "- BucketProps: ~p~n- UpNodes:     ~p~n" ++
		  "- Perfs:       ~p~n- VNodes:      ~p~n",
	      [BucketName, Query, Key, DocIdx, BucketProps, UpNodes,
	       Perfs, VNodes]),
    _CoveragePlan = {VNodes, NoFilters}.
