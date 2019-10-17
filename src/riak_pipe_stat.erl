%% -------------------------------------------------------------------
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
%%
%% @doc Collector for various pipe stats.
-module(riak_pipe_stat).

-behaviour(gen_server).

%% API
-export([start_link /0, register_stats/0,
         get_stats/0,
         update/1,
         stats/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(APP, riak_pipe).
-define(PFX, riak_core_stat:prefix()).

-type stat_type() :: counter | spiral.
-type stat_options() :: [tuple()].
-type stat_aliases() :: [{exometer:datapoint(), atom()}].

%% -------------------------------------------------------------------
%% API
%% -------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

register_stats() ->
    riak_core_stat:register_stats(?APP, stats()).

%% @doc Return current aggregation of all stats.
-spec get_stats() -> proplists:proplist().
get_stats() ->
    riak_core_stat:get_stats(?APP).

update(Arg) ->
    gen_server:cast(?SERVER, {update, Arg}).

%% gen_server

init([]) ->
    register_stats(),
    {ok, ok}.

handle_call(_Req, _From, State) ->
    {reply, ok, State}.

handle_cast({update, {create, Pid}}, State) ->
    erlang:monitor(process, Pid),
    do_update(create),
    {noreply, State};
handle_cast({update, Arg}, State) ->
    do_update(Arg),
    {noreply, State};
handle_cast(_Req, State) ->
    {noreply, State}.

handle_info({'DOWN', _Ref, process, _Pid, _Reason}, State) ->
    do_update(destroy),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% @doc Update the given `Stat'.
-spec do_update(term()) -> ok.
do_update(create) ->
    ok = exometer:update([?PFX, ?APP, pipeline, create], 1),
    exometer:update([?PFX, ?APP, pipeline, active], 1);
do_update(create_error) ->
    exometer:update([?PFX, ?APP, pipeline, create, error], 1);
do_update(destroy) ->
    exometer:update([?PFX, ?APP, pipeline, active], -1).

%% -------------------------------------------------------------------
%% Private
%% -------------------------------------------------------------------
-spec stats() -> [{riak_core_stat_q:path(), stat_type(), stat_options(),
		   stat_aliases()}].
stats() ->
    [
     {[pipeline, create], spiral, [], [{count, pipeline_create_count},
                                       {one, pipeline_create_one}]},
     {[pipeline, create, error], spiral, [], [{count, pipeline_create_error_count},
                                              {one, pipeline_create_error_one}]},
     {[pipeline, active], counter, [], [{value, pipeline_active}]}
    ].
