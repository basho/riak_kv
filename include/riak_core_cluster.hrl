%% -------------------------------------------------------------------
%%
%% Riak Core Cluster Manager
%%
%% Copyright (c) 2013 Basho Technologies, Inc.  All Rights Reserved.
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
-define(CLUSTER_MANAGER_SERVER, riak_core_cluster_manager).
-define(CLUSTER_MGR_SERVICE_ADDR, {"0.0.0.0", 9085}).
-define(CM_CALL_TIMEOUT, 2000).
-define(CLUSTER_NAME_LOCATOR_TYPE, cluster_by_name).
-define(CLUSTER_ADDR_LOCATOR_TYPE, cluster_by_addr).

-define(CLUSTER_PROTO_ID, cluster_mgr).
-type(clustername() :: string()).
