-module(riak_kv_get_put_monitor).
-behaviour(gen_server).
-define(SERVER, ?MODULE).

-record(state, {gets = [], puts = []}).

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
    folsom_metrics:new_histogram(put_fsm_errors, slide, 60),
    folsom_metrics:new_histogram(get_fsm_errors, slide, 60),
    {ok, #state{}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({get_fsm_spawned, Pid}, State) ->
    #state{gets = GetList} = State,
    GetList2 = insert_pid(Pid, GetList),
    folsom_metrics:notify({get_fsm_in_progress, {inc, 1}}),
    {noreply, State#state{gets = GetList2}};

handle_cast({put_fsm_spawned, Pid}, State) ->
    #state{puts = PutList} = State,
    PutList2 = insert_pid(Pid, PutList),
    folsom_metrics:notify({put_fsm_in_process, {inc, 1}}),
    {noreply, State#state{puts = PutList2}};

handle_cast(stop, State) ->
    {stop, normal, State};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

insert_pid(Pid, List) ->
    MonRef = erlang:monitor(process, Pid),
    orddict:store(MonRef, Pid, List).
