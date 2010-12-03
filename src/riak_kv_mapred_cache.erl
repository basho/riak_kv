%% -------------------------------------------------------------------
%%
%% riak_kv_mapred_cache: gen_server to manage starting up and ejecting
%%                       old data from the MapReduce cache
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
-module(riak_kv_mapred_cache).

-behaviour(gen_server).

%% API
-export([start_link/0,
         clear/0,
         cache_ref/0,
         eject/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {lru}).

clear() ->
    gen_server:call(?SERVER, clear, infinity).

eject(BKey) ->
    gen_server:cast(?SERVER, {eject, BKey}).

cache_ref() ->
    gen_server:call(?SERVER, cache_ref, infinity).

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
    CacheSize = app_helper:get_env(riak_kv, map_cache_size, 5000),
    {ok, #state{lru=riak_kv_lru:new(CacheSize)}}.

handle_call(clear, _From, #state{lru=LRU}=State) ->
    riak_kv_lru:clear(LRU),
    {reply, ok, State};
handle_call(cache_ref, _From, #state{lru=LRU}=State) ->
    {reply, {ok, LRU}, State};
handle_call(_Request, _From, State) ->
    {reply, ignore, State}.

handle_cast({eject, BKey}, #state{lru=LRU}=State) ->
    riak_kv_lru:clear_bkey(LRU, BKey),
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Internal functions
