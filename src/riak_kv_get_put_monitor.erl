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

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

stop() ->
    gen_server:cast(?SERVER, stop).

get_fsm_spawned(Pid) ->
    gen_server:cast(?SERVER, {get_fsm_spawned, Pid}).

put_fsm_spawned(Pid) ->
    gen_server:cast(?SERVER, {put_fsm_spawned, Pid}).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init([]) ->
    folsom_metrics:new_counter(put_fsm_in_progress),
    folsom_metrics:new_counter(get_fsm_in_progress),
    folsom_metrics:new_spiral(put_fsm_errors_minute),
    folsom_metrics:new_spiral(get_fsm_errors_minute),
    folsom_metrics:new_counter(put_fsm_errors_since_start),
    folsom_metrics:new_counter(get_fsm_errors_since_start),
    {ok, #state{}}.

handle_call(dump_state, _From, State) ->
    {reply, State, State};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

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

terminate(_Reason, _State) ->
	Counters = [put_fsm_in_progress, get_fsm_in_progress, put_fsm_errors_minute,
				get_fsm_errors_minute, put_fsm_errors_since_start,
				get_fsm_errors_since_start],
	[folsom_metrics:delete_metric(Counter) || Counter <- Counters],
    ok.

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
    folsom_metrics:notify({put_fsm_errors, 1});
tell_folsom_about_exit(get, _Cause) ->
    tell_folsom_about_exit(get, normal),
    folsom_metrics:notify({get_fsm_errors, 1}).

orddict_get_erase(Key, Orddict) ->
    case orddict:find(Key, Orddict) of
        undefined ->
            undefined;
        {ok, Value} ->
            Orddict2 = orddict:erase(Key, Orddict),
            {Value, Orddict2}
    end.
