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
%% The stat names are all 4 element tuples, the first two elements being
%% 'riak_kv', then the local node. The third is either 'get' or 'put' depending
%% on if the fsm reporting in was for a get or a put action.  The final element
%% is either 'in_progess' or 'errors'.
%%
%% The 'in_progess' is a simple counter reporting the number of get or put fsm's
%% currently alive.  'errors' is a spiral for all fsm's that exited with a
%% reason other than normal or shutdown.
-module(riak_kv_get_put_monitor).
-behaviour(gen_server).
-define(SERVER, ?MODULE).
-define(STATTYPES, [
    {new_counter, puts, in_progess},
    {new_counter, gets, in_progess},
    {new_spiral, puts, errors},
    {new_spiral, gets, errors}
]).
-define(COUNTER(FsmType, DataPoint), {riak_kv, node, FsmType, DataPoint}).
-define(COUNTERS, [?COUNTER(FsmType, DataPoint) ||
    {_, FsmType, DataPoint} <- ?STATTYPES]).

-type metric() :: {'riak_kv', 'node', 'puts' | 'gets', 'in_progess' | 'errors'}.
-type spiral_value() :: [{'counter', non_neg_integer()} | {'one', non_neg_integer()}].

-record(state, {monitor_list = []}).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/0, get_fsm_spawned/1, put_fsm_spawned/1,
    all_stats/0, stop/0, get_in_progress/0]).

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

%% @doc Get a proplist of all the stats tracked thus far.
-spec all_stats() -> [{metric(), non_neg_integer() | spiral_value()}].
all_stats() ->
    [{Key, folsom_metrics:get_metric_value(Key)} ||
        Key <- ?COUNTERS].

%% Returns the last count for the get fms's in progress.
-spec get_in_progress() -> non_neg_integer().
get_in_progress() ->
    folsom_metrics:get_metric_value(?COUNTER(gets, in_progress)).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

%% @private
init([]) ->
    [begin
        folsom_metrics:Func(?COUNTER(FsmType, DataPoint))
    end || {Func, FsmType, DataPoint} <- ?STATTYPES],
    {ok, #state{}}.

%% @private
handle_call(dump_state, _From, State) ->
    {reply, State, State};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

%% @private
handle_cast({get_fsm_spawned, Pid}, State) ->
    #state{monitor_list = List} = State,
    List2 = insert_pid(Pid, gets, List),
    tell_folsom_about_spawn(gets),
    {noreply, State#state{monitor_list = List2}};

handle_cast({put_fsm_spawned, Pid}, State) ->
    #state{monitor_list = List} = State,
    List2 = insert_pid(Pid, puts, List),
    tell_folsom_about_spawn(puts),
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
    [folsom_metrics:delete_metric(Counter) || Counter <- ?COUNTERS],
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

tell_folsom_about_spawn(Type) ->
    folsom_metrics:notify({?COUNTER(Type, in_progess), {inc, 1}}).

tell_folsom_about_exit(Type, Cause) when Cause == normal; Cause == shutdown ->
    folsom_metrics:notify({?COUNTER(Type, in_progess), {dec, 1}});
% for the abnormal cases, we not only decrmemt the in progess count (like a
    % normal exit) but increment the errors count as well.
tell_folsom_about_exit(Type, _Cause) ->
    tell_folsom_about_exit(Type, normal),
    folsom_metrics:notify({?COUNTER(Type, errors), 1}).

orddict_get_erase(Key, Orddict) ->
    case orddict:find(Key, Orddict) of
        undefined ->
            undefined;
        {ok, Value} ->
            Orddict2 = orddict:erase(Key, Orddict),
            {Value, Orddict2}
    end.
