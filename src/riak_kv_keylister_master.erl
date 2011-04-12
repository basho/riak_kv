%% -------------------------------------------------------------------
%%
%% riak_kv_keylister_master: Starts keylister processes on demand
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
-module(riak_kv_keylister_master).

-behaviour(gen_server).

%% API
-export([start_link/0,
         start_keylist/3,
         start_keylist/4]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(DEFAULT_TIMEOUT, 5000).

-record(state, {}).

start_keylist(Node, ReqId, Bucket) ->
    start_keylist(Node, ReqId, Bucket, ?DEFAULT_TIMEOUT).

start_keylist(Node, ReqId, Bucket, Timeout) ->
    try
        case gen_server:call({?SERVER, Node}, {start_kl, ReqId, self(), Bucket}, Timeout) of
            {ok, Pid} ->
                {ok, Pid};
            Error ->
                Error
        end
    catch
        exit:{timeout, _} ->
            {error, timeout}
    end.


start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
    {ok, #state{}}.

handle_call({start_kl, ReqId, Caller, Bucket}, _From, State) ->
    Reply = riak_kv_keylister_sup:new_lister(ReqId, Caller, Bucket),
    {reply, Reply, State};

handle_call(_Request, _From, State) ->
    {reply, ignore, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
