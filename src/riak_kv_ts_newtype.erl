%% -------------------------------------------------------------------
%%
%% riak_kv_ts_newtype: Supervises local DDL compilation
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

%% XXX: would like to leverage supervisor:which_children on start to
%% build the transitions data structure in case of a restart

-module(riak_kv_ts_newtype).

-behaviour(gen_server).

%% API
-export([start_link/2, new_type/1, get_status/1, dump_status/1]).

%% Worker API
-export([notify/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {
          compilation_supervisor   :: module(),
          compiler_mod             :: module(),
          transitions = []         :: list(tuple())
         }).

%% The transitions list is a list of tuples:
%%    {type name, [transition info], worker process|undefined}
%%
%% List of transitions is most recent at the head
%%
%% Each transition info item is a tuple: {state, timestamp}
%%
%% States:

%% notfound is a generated status if a client invokes get_status or
%% dump_status on a type we haven't been notified of

%%   {start_failed, Reason}
%%   worker_started (worker launched, says nothing about compile started)
%%   compile_started (from worker)
%%   {compile_failed, Reason} (from worker)
%%   {worker_crashed, Reason}
%%   compile_success (from worker)
%%   activated
%%   activated_no_compile (worker awoke from waiting to discover type
%%                         already active)
%%   deleted (type vanished before worker could finish its job)

%% Those transition states map to the following status codes as
%% reported to anyone who asks:
%%    running  | worker_started, compile_started
%%    {failed, Reason}   | start_failed, compile_failed, worker_crashed
%%    compiled | compile_success, activated
%%    {error, <status>} where <status> is any of the others

%%%===================================================================
%%% API
%%%===================================================================
%% @doc Alert us to a new timeseries bucket type. Call
%% asynchronously.
-spec new_type(binary()) -> ok.
new_type(BucketType) ->
    gen_server:cast(?MODULE, {new_type, BucketType}).

get_status(BucketType) ->
    gen_server:call(?MODULE, {status, BucketType}).

dump_status(BucketType) ->
    gen_server:call(?MODULE, {dump, BucketType}).

notify(Type, Status) ->
    gen_server:cast(?MODULE, {notify, Type, Status}).

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(CompilationSup, CompilerMod) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE,
                          [CompilationSup, CompilerMod], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([CompilationSup, CompilerMod]) ->
    {ok, #state{compilation_supervisor=CompilationSup,
                compiler_mod=CompilerMod}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call({status, Type}, _From, #state{transitions=Transitions}=State) ->
    {reply, get_status(Type, Transitions), State};
handle_call({dump, Type}, _From, #state{transitions=Transitions}=State) ->
    {reply, dump_status(Type, Transitions), State};
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast({new_type, Type}, State) ->
    UpdatedTransitions = launch_worker(Type, State),
    {noreply, State#state{transitions=UpdatedTransitions}};
handle_cast({notify, Type, Status}, #state{transitions=Transitions}=State) ->
    UpdatedTransitions = worker_update(Status, Type, Transitions),
    {noreply, State#state{transitions=UpdatedTransitions}}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info({'DOWN', _MonitorRef, process, _Pid, normal}, State) ->
    %% We don't particularly care about normal exits, don't even need
    %% (probably?) to clean them out of the transitions list
    {noreply, State};
handle_info({'DOWN', _MonitorRef, process, _Pid, restarting}, State) ->
    %% This is our own kill; someone sent us a new_type message for a
    %% type which was already under "construction". We updated the
    %% transitions list already, no need to do anything further.
    {noreply, State};
handle_info({'DOWN', _MonitorRef, process, Pid, Reason},
            #state{transitions=Transitions}=State) ->
    %% Any other death reason, we should capture in our transitions list
    {noreply, State#state{transitions=crashed(Pid, Reason, Transitions)}};
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

get_status(Type, Transitions) ->
    latest_status(lists:keyfind(Type, 1, Transitions)).

dump_status(Type, Transitions) ->
    all_status(lists:keyfind(Type, 1, Transitions)).

latest_status(false) ->
    notfound;
latest_status({_Type, [H|_T], _Pid}) ->
    status_to_external(H).

all_status(false) ->
    notfound;
all_status({_Type, List, _Pid}) ->
    List.

status_to_external(worker_started) ->
    running;
status_to_external(compile_started) ->
    running;
status_to_external({_Error, Reason}) ->
    {failed, Reason};
status_to_external(compile_success) ->
    compiled;
status_to_external(activated) ->
    compiled.

beam_directory() ->
    %% XXX: Hack alert: on my devrel environment, the first item in
    %% code:get_path/0 is basho-patches. Should do something smarter
    hd(code:get_path()).

%%     update_type(map_update(Status), {Pid}, Type, Transitions).

%% Functions to update the transitions list
crashed(Pid, Reason, Transitions) ->
    {Type, _, Pid} = lists:keyfind(Pid, 3, Transitions),
    update_type({crashed, Reason}, Type, Transitions).

restarting(Type, Transitions) ->
    lists:foreach(fun({MyType, _, Pid}) when MyType == Type -> exit(Pid, restarting);
                     ({_, _, _}) -> ok
                  end,
                  Transitions),
    Transitions.

launching(Pid, Type, Transitions) ->
    update_type(worker_started, Pid, Type, Transitions).

failed_start(Reason, Type, Transitions) ->
    update_type({start_failed, Reason}, Type, Transitions).

worker_update(Status, Type, Transitions) ->
    update_type(map_update(Status), Type, Transitions).

%% Map worker status messages to the transition states we keep
%% internally
map_update(compiling) ->
    compile_started;
map_update({failed, Reason}) ->
    {compile_failed, Reason};
map_update(success) ->
    compile_success;
map_update(type_already_active) ->
    activated_no_compile;
map_update(type_deleted) ->
    deleted;
map_update(Other) ->
    Other.

launch_worker(Type, #state{compilation_supervisor=Sup,
                           compiler_mod=CompilerMod,
                           transitions=Transitions0}) ->

    %% Because it seems to make sense at the moment, if we receive
    %% duplicate calls of `new_type' with the same bucket type, we'll
    %% assume something changed with the DDL and start over with a new
    %% worker.
    %%
    %% XXX: Consider the need for code on the compiler/watcher side to
    %% deal with cleanup.
    Transitions1 = restarting(Type, Transitions0),

    %% Launch a child
    case supervisor:start_child(Sup,
                                [CompilerMod, Type,
                                 ?MODULE, beam_directory()]) of
        {ok, Pid} ->
            monitor(process, Pid),
            Transitions2 = launching(Pid, Type, Transitions1);
        {error, Reason} ->
            lager:error("Cannot launch new TS compiler watcher: ~p",
                        [Reason]),
            Transitions2 = failed_start(Reason, Type, Transitions1)
    end,
    Transitions2.

%%%%%
%% Remaining functions are for updating the bucket type in our
%% transitions list

%% update_type/3 is for a simple status update
update_type(Status, Type, Transitions) ->
    Update = update_transition(Status, Type,
                               lists:keyfind(Type, 1, Transitions)),
    lists:keyreplace(Type, 1, Transitions, Update).

%% update_type/4 is for a simple status update + pid replacement
update_type(Status, NewPid, Type, Transitions) ->
    Update = update_transition(Status, NewPid, Type,
                               lists:keyfind(Type, 1, Transitions)),
    lists:keyreplace(Type, 1, Transitions, Update).


update_transition(Status, Type, false) ->
    {Type, [{Status, erlang:timestamp()}], undefined};
update_transition(Status, Type, {Type, TList, Pid}) ->
    {Type, [{Status, erlang:timestamp()}] ++ TList, Pid}.

update_transition(Status, NewPid, Type, false) ->
    {Type, [{Status, erlang:timestamp()}], NewPid};
update_transition(Status, NewPid, Type, {Type, TList, _Pid}) ->
    {Type, [{Status, erlang:timestamp()}] ++ TList, NewPid}.
