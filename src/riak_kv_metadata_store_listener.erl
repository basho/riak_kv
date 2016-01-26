%% -------------------------------------------------------------------
%%
%% gen_event listener behaviour that receives `metadata_stored'
%% events and starts compilation of the DDL if it exists in the
%% metadata.
%%
%% Copyright (c) 2015, 2016 Basho Technologies, Inc.  All Rights Reserved.
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

-module(riak_kv_metadata_store_listener).

-behaviour(gen_event).

%%% ------------------------------------------------------------------
%%% gen_event Function Exports
%%% ------------------------------------------------------------------

-export([code_change/3]).
-export([handle_call/2]).
-export([handle_event/2]).
-export([handle_info/2]).
-export([init/1]).
-export([terminate/2]).

-record(state, {}).

%%% ------------------------------------------------------------------
%%% gen_event Function Definitions
%%% ------------------------------------------------------------------

init(_) ->
    {ok, #state{}}.


handle_event({metadata_stored, BucketType}, State) ->
	% catch here because if the handler throws an exception then it is removed
	% from the listener list.
	(catch riak_kv_ts_newtype:new_type(BucketType)),
    {ok, State};
handle_event(_Event, State) ->
    {ok, State}.

handle_call({is_type_compiled, [BucketType, _DDL]}, State) ->
    IsCompiled = riak_kv_compile_tab:get_state(BucketType) == compiled,
    %% andalso riak_kv_compile_tab:get_ddl(BucketType)) == DDL,
    %% This reqiest comes from a riak_ts-1.1 node: ignore the DDL
    %%
    %% Because the AST for ddl_v1 and _v2 will differ (ddl_v2 record
    %% contains an additional 'properties' field) while effectively
    %% representing the same schema, comparing the DDL bodies is not
    %% necessary (and the blobs will be different between versions).
    {ok, IsCompiled, State};
handle_call({is_type_compiled, [BucketType]}, State) ->
    IsCompiled = riak_kv_compile_tab:get_state(BucketType) == compiled,
    {ok, IsCompiled, State};
handle_call(_Request, State) ->
    Reply = ok,
    {ok, Reply, State}.

handle_info(_Info, State) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
