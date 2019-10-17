%% -------------------------------------------------------------------
%%
%% riak_dt_gset_test: trivial assertive tests to illustrate module behavior
%%
%% Copyright (c) 2007-2014 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
-module(riak_dt_gset_tests).

-include_lib("eunit/include/eunit.hrl").
-import(riak_dt_gset, [update/3]).

-define(ACTOR_VAL, undefined).
-define(SINGLE_VAL, <<"binarytemple">>).
-define(FRANK_BOOTH, [<<"frank">>, <<"booth">>]).
-define(BOOTH_FRANK, [<<"booth">>, <<"frank">>]).

update_add_test() ->
  N = riak_dt_gset:new(),
  ?assertEqual({ok, [?SINGLE_VAL]}, update({add, ?SINGLE_VAL}, ?ACTOR_VAL, N))
.

update_add_all_test() ->
  ?assertEqual({ok, ?BOOTH_FRANK}, update({add_all, ?FRANK_BOOTH}, ?ACTOR_VAL, riak_dt_gset:new())),
  ?assertNotEqual({ok, ?FRANK_BOOTH}, update({add_all, ?FRANK_BOOTH}, ?ACTOR_VAL, riak_dt_gset:new()))
.
