%% -------------------------------------------------------------------
%%
%% riak_kv_qry_coverage_fsm: generate the coverage for a hashed query
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
create_plan(_VNodeSelector, _NVal, _PVC, _ReqId, _NodeCheckService, _Request) ->
    %% gg:format("in riak_kv_qry_coverage_plan with~n:" ++
    %%		  "- ~p~n- ~p~n- ~p~n- ~p~n- ~p~n- ~p~n",
    %%	      [VNodeSelector, NVal, PVC, ReqId, NodeCheckService, Request]),
    exit_with_error.
