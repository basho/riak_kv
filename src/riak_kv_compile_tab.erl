%% -------------------------------------------------------------------
%%
%% riak_kv_compile_tab: Store the state about what bucket type DDLs
%%                      have been compiled.
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

%% TODO use dets

-module(riak_kv_compile_tab).

-export([is_compiling/1]).
-export([new/0]).
-export([insert/4]).

-define(TABLE, ?MODULE).

%%
new() ->
	ets:new(?TABLE, [public, named_table]).

%%
insert(Bucket_type, DDL, Pid, State) when State == compiling orelse
                                          State == compiled orelse
                                          State == failed ->
	ets:insert(?TABLE, {Bucket_type, DDL, Pid, State}),
	ok.

%%
-spec is_compiling(Bucket_type :: binary()) ->
	{true, pid()} | false.
is_compiling(Bucket_type) ->
	case ets:lookup(?TABLE, Bucket_type) of
		{_,_,Pid,compiling} ->
			{true, Pid};
		_ ->
			false
	end.