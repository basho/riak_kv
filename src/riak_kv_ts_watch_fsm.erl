%% -------------------------------------------------------------------
%%
%% riak_kv_ts_watch_fsm: Polls for completion of DDL activities for bucket
%% type activation
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

-module(riak_kv_ts_watch_fsm).

-behaviour(gen_fsm).

%% API
-export([start_link/4, start_link/5,
         waiting/2, compiling/2, compiled/2, failed/2]).

%% gen_fsm callbacks
-export([init/1, handle_event/3,
         handle_sync_event/4, handle_info/3, terminate/3, code_change/4]).

-define(SERVER, ?MODULE).
-define(BUCKET_TYPE_PREFIX, {core, bucket_types}).

%% Can be overridden via `start_link/5' for testing
-define(DEFAULT_TIMEOUTS,
        [
         %%%% Retries
         %% Cluster metadata to propagate
         {metadata_retry, 2000},
         %% Bucket type updates (activation, ddl changes)
         {update_retry, 6000},

         %%%% Single-use timeouts
         %% DDL compilation
         {compile_wait, 30000}
        ]).

-record(state, {
          compiler_mod :: atom(),
          bucket_type :: binary(),
          supervisor :: pid(),
          ddl = undefined :: term(),
          compiler = undefined :: 'undefined' | pid(),
          beam_dir :: file:filename(), %% Path to location to store beams
          beam_file :: file:filename(), %% Full path to compiled beam when done

          %% See DEFAULT_TIMEOUTS for definition
          metadata_retry :: pos_integer(),
          update_retry :: pos_integer(),
          compile_wait :: pos_integer()
         }).

-define(EQC, true). %% XXX

-ifdef(EQC).
-export([waiting/3, compiling/3, compiled/3, failed/3]).

%% For testing with `gen_fsm:sync_send_event'
-define(STATE_TEST(StateName),
        StateName(Event, From, State) ->
               %% The tuple can be 3 or 4-arity, so we can't pattern match
               Tuple = StateName(Event, State),
               {First, Second} = {element(1, Tuple), element(2, Tuple)},
               case First of
                   stop ->
                       gen_fsm:reply(From, stopping);
                   next_state ->
                       gen_fsm:reply(From, Second)
               end,
               Tuple
).

?STATE_TEST(waiting).
?STATE_TEST(compiling).
?STATE_TEST(compiled).
?STATE_TEST(failed).

-endif. %% EQC



%%%===================================================================
%%% API
%%%===================================================================

start_link(Compiler, Type, Sup, Dir) ->
    start_link(Compiler, Type, Sup, Dir, []).

start_link(Compiler, Type, Sup, Dir, Timeouts) ->
    gen_fsm:start_link(?MODULE, [Compiler, Type, Sup, Dir, Timeouts], []).

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm is started using gen_fsm:start/[3,4] or
%% gen_fsm:start_link/[3,4], this function is called by the new
%% process to initialize.
%%
%% @spec init(Args) -> {ok, StateName, State} |
%%                     {ok, StateName, State, Timeout} |
%%                     ignore |
%%                     {stop, StopReason}
%% @end
%%--------------------------------------------------------------------
init([Compiler, Type, Sup, Dir, Timeouts]) ->
    %% Make sure all timeouts are available, with timeouts from any
    %% caller-supplied list preferred
    AllTimeouts = lists:ukeymerge(1, lists:sort(Timeouts), lists:sort(?DEFAULT_TIMEOUTS)),
    MetadataRetry = proplists:get_value(metadata_retry, AllTimeouts),
    UpdateRetry = proplists:get_value(update_retry, AllTimeouts),
    CompileWait = proplists:get_value(compile_wait, AllTimeouts),
    {ok, waiting, #state{bucket_type=Type, compiler_mod=Compiler,
                         supervisor=Sup, beam_dir=Dir,
                         metadata_retry=MetadataRetry,
                         update_retry=UpdateRetry,
                         compile_wait=CompileWait
                        }, MetadataRetry}.

waiting(_Event, #state{supervisor=Sup, bucket_type=Type}=State) ->
    %% Expecting timeout, but something else could notify us that the
    %% metadata for our bucket type has been pushed to this node.

    %% Make sure the bucket type isn't already active
    case check_activated(Type) of
        true ->
            notify_supervisor(Sup, {activated, Type}),
            {stop, {activated, Type}, State};
        false ->
            try_new_build(retrieve_ddl(Type), State)
    end.

compiling(timeout, #state{compiler=Pid, supervisor=Sup,
                          update_retry=Timeout}=State) ->
    %% If we got a timeout but the process is dead, either it safely
    %% terminated and has sent us a completion event we haven't
    %% received yet, or the process died and we haven't received the
    %% EXIT message yet. In either case, if it's dead, we just stay in
    %% compiling state until the appropriate message arrives (and hope
    %% it does actually arrive, should think about a retry count).

    %% Thus the only situation we care about here is if the
    %% compilation mechanism has taken too long and the process is
    %% alive; we have to consider this a failure.
    case is_process_alive(Pid) of
        true ->
            Reason = {failed, compile_timeout},
            exit(Pid, Reason),
            notify_supervisor(Sup, Reason),
            {next_state, failed, State#state{compiler=undefined}, Timeout};
        false ->
            {next_state, compiled, State, Timeout}
    end;
compiling({done, FullPath}=Success, #state{supervisor=Sup,update_retry=Timeout}=State) ->
    notify_supervisor(Sup, Success),
    {next_state, compiled, State#state{compiler=undefined, beam_file=FullPath}, Timeout};
compiling({failed, _Reason}=Error, #state{supervisor=Sup,update_retry=Timeout}=State) ->
    notify_supervisor(Sup, Error),
    {next_state, failed, State#state{compiler=undefined}, Timeout}.

compiled(timeout, #state{supervisor=Sup, bucket_type=Type, ddl=OldDDL}=State) ->
    case check_activated(Type) of
        true ->
            notify_supervisor(Sup, {activated, Type}),
            {stop, {activated, Type}, State};
        false ->
            try_updated_build(OldDDL, retrieve_ddl(Type), compiled, State)
    end.

failed(timeout, #state{bucket_type=Type, ddl=OldDDL}=State) ->
    try_updated_build(OldDDL, retrieve_ddl(Type), failed, State).

notify_supervisor(_Supervisor, _Status) ->
    ok.
%%    gen_server:cast(Supervisor, Status).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm receives an event sent using
%% gen_fsm:send_all_state_event/2, this function is called to handle
%% the event.
%%
%% @spec handle_event(Event, StateName, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState}
%% @end
%%--------------------------------------------------------------------
handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm receives an event sent using
%% gen_fsm:sync_send_all_state_event/[2,3], this function is called
%% to handle the event.
%%
%% @spec handle_sync_event(Event, From, StateName, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {reply, Reply, NextStateName, NextState} |
%%                   {reply, Reply, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState} |
%%                   {stop, Reason, Reply, NewState}
%% @end
%%--------------------------------------------------------------------
handle_sync_event(_Event, _From, StateName, State) ->
    Reply = ok,
    {reply, Reply, StateName, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_fsm when it receives any
%% message other than a synchronous or asynchronous event
%% (or a system message).
%%
%% @spec handle_info(Info,StateName,State)->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState}
%% @end
%%--------------------------------------------------------------------
handle_info({'EXIT', Pid, Reason}, compiling, #state{supervisor=Sup,
                                                     compiler=Pid}=State) ->
    notify_supervisor(Sup, {failed, Reason}),
    {stop, Reason, State#state{compiler=undefined}};
handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_fsm when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_fsm terminates with
%% Reason. The return value is ignored.
%%
%% @spec terminate(Reason, StateName, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _StateName, #state{compiler=undefined}) ->
    ok;
terminate(Reason, _StateName, #state{compiler=Pid}) ->
    %% Might be reached if node were shutting down before compilation
    %% finished?
    exit(Pid, Reason).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, StateName, State, Extra) ->
%%                   {ok, StateName, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% In state compiled, we need to watch for the activation of the type
%% so we can close up shop
check_activated(BucketType) ->
    extract_activated(riak_core_metadata:get(?BUCKET_TYPE_PREFIX, BucketType)).

extract_activated(undefined) ->
    false;
extract_activated(Proplist) ->
    proplists:get_value(active, Proplist, false).


%% In states waiting, compiled, failed we need to watch for DDL
%% changes in cluster metadata. Will return the DDL term or undefined.
retrieve_ddl(BucketType) ->
    %% Would be nice to have a function in riak_core_bucket_type or
    %% similar to get either the prefix or the actual metadata instead
    %% of including a riak_core header file for this prefix
    extract_ddl(riak_core_metadata:get(?BUCKET_TYPE_PREFIX, BucketType)).

%% Helper function for `retrieve_ddl'
extract_ddl(undefined) ->
    undefined;
extract_ddl(Proplist) ->
    proplists:get_value(ddl, Proplist).

spawn_build(Mod, DDL, Type, Dir) ->
    spawn_link(Mod, compile, [self(), DDL, Type, Dir]).

%% Function is called only from state waiting.  Arguable whether this
%% function should advance us to failed if there is no DDL defined in
%% the bucket type, but the net effect is largely the same. The only
%% difference between advancing to failed and returning to waiting is
%% whether the supervisor is notified about the "failure"
try_new_build(undefined, #state{metadata_retry=Timeout}=State) ->
    {next_state, waiting, State, Timeout};
try_new_build(DDL, #state{bucket_type=Type,beam_dir=Dir,
                          compiler_mod=Mod,compile_wait=Timeout}=State) ->
    Compiler = spawn_build(Mod, DDL, Type, Dir),
    {next_state, compiling, State#state{compiler=Compiler, ddl=DDL}, Timeout}.

%% Called from states compiled and failed
try_updated_build(_LastDDL, _LastDDL, CurrentStateName,
                  #state{update_retry=Timeout}=State) ->
    {next_state, CurrentStateName, State, Timeout};
try_updated_build(_LastDDL, undefined, _CurrentStateName,
                  #state{bucket_type=Type, supervisor=Sup}=State) ->
    %% The only way for the new DDL to be undefined is if the type has
    %% been removed from cluster metadata
    notify_supervisor(Sup, {removed_type, Type}),
    {stop, {removed_type, Type}, State};
try_updated_build(_LastDDL, NewDDL, _StateName,
                  #state{bucket_type=Type,beam_dir=Dir,
                         compiler_mod=Mod,compile_wait=Timeout}=State) ->
    Compiler = spawn_build(Mod, NewDDL, Type, Dir),
    {next_state, compiling, State#state{compiler=Compiler, ddl=NewDDL}, Timeout}.
