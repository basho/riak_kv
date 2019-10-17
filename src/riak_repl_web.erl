%% -------------------------------------------------------------------
%%
%% riak_repl_web: setup Riak's REPL HTTP interface
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
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
-module(riak_repl_web).
-export([dispatch_table/0]).

dispatch_table() ->
    Props = props(),
    [
     {["riak-repl", "stats"],  riak_repl_wm_stats, []},

     {["rtq", bucket_type, bucket, key],
      riak_repl_wm_rtenqueue, Props},

     {["rtq", bucket, key],
      riak_repl_wm_rtenqueue, Props}
     ].

props() ->
    [{riak, local}, {bucket_type, <<"default">>}].

