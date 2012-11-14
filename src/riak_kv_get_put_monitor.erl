%% -------------------------------------------------------------------
%%
%% riak_kv_get_put_monitor:  Oberserve and stat log get/put fsm's
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

%% @doc Monitor reporting processes and notify folsom about live and error
%% exits. It is up to the fsm to report it's existence to the monitor. If this
%% module exits unexpectadly, the stats in folsom are not lost; a clean exit
%% will reset the stats.
%%
%% The stats tracked are:
%% * put_fsm_in_progress: a counter of the number of put fsm's current running.
%% * get_fsm_in_progress: as above, but for get fsm's.
%% * put_fsm_errors_minute: a spiral metric of the put fsm's that have exited 
%% for a reason other than shutdown or normal.
%% * get_fsm_errors_minute: as above, but for get fsm's.
%% * put_fsm_errors_since_start: a count of all put fsm's that have exited for
%% a reason other than shutdown or normal since the folsom stat was created.
%% * get_fsm_errors_since_start: as above, but for get fsm's.

-module(riak_kv_get_put_monitor).
-behaviour(gen_server).
-define(SERVER, ?MODULE).

-record(state, {monitor_list = []}).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/0, get_fsm_spawned/1, put_fsm_spawned/1, stop/0]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

%% @doc Starts the monitor service.
-spec start_link() -> {'ok', pid()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% @doc Stops the service with reason 'normal'
-spec stop() -> 'ok'.
stop() ->
    gen_server:cast(?SERVER, stop).

%% @doc Begin monitoring the passed pid and tag its stats for a get fsm.
-spec get_fsm_spawned(Pid :: pid()) -> 'ok'.
get_fsm_spawned(Pid) ->
    gen_server:cast(?SERVER, {get_fsm_spawned, Pid}).

%% @doc Begin monitoring the passed pid and tag its stats for a put fsm.
-spec put_fsm_spawned(Pid :: pid()) -> 'ok'.
put_fsm_spawned(Pid) ->
    gen_server:cast(?SERVER, {put_fsm_spawned, Pid}).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

%% @private
init([]) ->
    folsom_metrics:new_counter(put_fsm_in_progress),
    folsom_metrics:new_counter(get_fsm_in_progress),
    folsom_metrics:new_spiral(put_fsm_errors_minute),
    folsom_metrics:new_spiral(get_fsm_errors_minute),
    folsom_metrics:new_counter(put_fsm_errors_since_start),
    folsom_metrics:new_counter(get_fsm_errors_since_start),
    {ok, #state{}}.

%% @private
handle_call(dump_state, _From, State) ->
    {reply, State, State};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

%% @private
handle_cast({get_fsm_spawned, Pid}, State) ->
    #state{monitor_list = List} = State,
    List2 = insert_pid(Pid, get, List),
    folsom_metrics:notify({get_fsm_in_progress, {inc, 1}}),
    {noreply, State#state{monitor_list = List2}};

handle_cast({put_fsm_spawned, Pid}, State) ->
    #state{monitor_list = List} = State,
    List2 = insert_pid(Pid, put, List),
    folsom_metrics:notify({put_fsm_in_progress, {inc, 1}}),
    {noreply, State#state{monitor_list = List2}};

handle_cast(stop, State) ->
    {stop, normal, State};

handle_cast(_Msg, State) ->
    {noreply, State}.

%% @private
handle_info({'DOWN', MonRef, process, Pid, Cause}, State) ->
    #state{monitor_list = MonList} = State,
    case orddict_get_erase(MonRef, MonList) of
        undefined ->
            % meh, likely a late message
            {noreply, State};
        {{Pid, Type}, MonList2} ->
            tell_folsom_about_exit(Type, Cause),
            {noreply, State#state{monitor_list = MonList2}}
    end;

handle_info(_Info, State) ->
    {noreply, State}.

%% @private
terminate(_Reason, _State) ->
	Counters = [put_fsm_in_progress, get_fsm_in_progress, put_fsm_errors_minute,
				get_fsm_errors_minute, put_fsm_errors_since_start,
				get_fsm_errors_since_start],
	[folsom_metrics:delete_metric(Counter) || Counter <- Counters],
    ok.

%% @private
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

insert_pid(Pid, Type, List) ->
    MonRef = erlang:monitor(process, Pid),
    orddict:store(MonRef, {Pid, Type}, List).

tell_folsom_about_exit(put, Cause) when Cause == normal; Cause == shutdown ->
    folsom_metrics:notify({put_fsm_in_progress, {dec, 1}});
tell_folsom_about_exit(get, Cause) when Cause == normal; Cause == shutdown ->
    folsom_metrics:notify({get_fsm_in_progress, {dec, 1}});
% for the abnormal cases, we not only decrmemt the in progess count (like a
    % normal exit) but increment the errors count as well.
tell_folsom_about_exit(put, _Cause) ->
    tell_folsom_about_exit(put, normal),
    folsom_metrics:notify({put_fsm_errors_since_start, {inc, 1}}),
    folsom_metrics:notify({put_fsm_errors_minute, 1});
tell_folsom_about_exit(get, _Cause) ->
    tell_folsom_about_exit(get, normal),
    folsom_metrics:notify({get_fsm_errors_since_start, {inc, 1}}),
    folsom_metrics:notify({get_fsm_errors_minute, 1}).

orddict_get_erase(Key, Orddict) ->
    case orddict:find(Key, Orddict) of
        undefined ->
            undefined;
        {ok, Value} ->
            Orddict2 = orddict:erase(Key, Orddict),
            {Value, Orddict2}
    end.
