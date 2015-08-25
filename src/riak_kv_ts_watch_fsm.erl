%% -------------------------------------------------------------------
%%
%% riak_kv_ts_watcher: Polls for completion of DDL activities for bucket
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

-include_lib("riak_core/include/riak_core_bucket_type.hrl").

%% API
-export([start_link/3, waiting/2, compiling/2]).

%% gen_fsm callbacks
-export([init/1, handle_event/3,
         handle_sync_event/4, handle_info/3, terminate/3, code_change/4]).

-define(SERVER, ?MODULE).
-define(CMD_RETRY, 2000). %% How long to wait for CMD to propagate
-define(COMPILE_WAIT, 30000). %% How long to wait for the compilation mechanism to succeed before giving up

-record(state, {
          bucket_type :: binary(),
          supervisor :: pid(),
          compiler = undefined :: 'undefined' | pid(),
          beam_dir :: file:filename()
         }).


%%%===================================================================
%%% API
%%%===================================================================

start_link(Type, Sup, Dir) ->
    gen_fsm:start_link(?MODULE, [Type, Sup, Dir], []).

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
init([Type, Sup, Dir]) ->
    {ok, waiting, #state{bucket_type=Type,
                         supervisor=Sup, beam_dir=Dir}, 1000}.

cmd_exists(BucketType) ->
    %% Would be nice to have a function in riak_core_bucket_type or
    %% similar to get either the prefix or the actual metadata instead
    %% of including a riak_core header file for this prefix
    riak_core_metadata:get(?BUCKET_TYPE_PREFIX, BucketType).

waiting(_Event, #state{bucket_type=Type}=State) ->
    %% Typically timeout, but something else could notify us that the
    %% metadata for our bucket type has been pushed to this node
    check_for_metadata(cmd_exists(Type), State).

check_for_metadata(undefined, State) ->
    {next_state, waiting, State, ?CMD_RETRY};
check_for_metadata(Proplist, #state{beam_dir=Dir, bucket_type=Type}=State) ->
    {NextState, Compiler} = spawn_build(proplists:get_value(ddl, Proplist), Type, Dir),
    {next_state, NextState, State#state{compiler=Compiler}, ?COMPILE_WAIT}.
%% Technically the above is a bit silly if spawn_build returned a
%% failure; what purpose does the timeout serve? Not silly enough to
%% introduce an almost entirely superfluous case statement, however.



%% Must return a tuple with the next state atom, which can be `failed'
%% if the DDL isn't actually present in the bucket type definition or
%% `compiling' if the build process is spawned, and the spawned
%% process in the latter case
spawn_build(undefined, _Type, _Dir) ->
    {failed, undefined};
spawn_build(DDL, Type, Dir) ->
    {compiling,
     spawn_link(riak_kv_ts_compiler, compile, [self(), DDL, Type, Dir])}.

compiling(timeout, #state{compiler=Pid, supervisor=Sup}=State) ->
    %% If the process is dead, it's possible that it has safely
    %% terminated and has sent us a completion event we haven't
    %% received yet. So, the only situation we care about here is if
    %% the compilation mechanism has taken too long and the process is
    %% alive; we have to consider this a failure
    case is_process_alive(Pid) of
        true ->
            Reason = {failed, compile_timeout},
            exit(Pid, Reason),
            notify_supervisor(Sup, Reason),
            {stop, Reason, State#state{compiler=undefined}};
        false ->
            %% Assume a race condition we can ignore. Maybe.
            ok
    end;
%% Can be success or failure; we don't care which. Tuple with result
%% code and whatever useful message the compilation process can
%% provide
compiling(Completion, #state{supervisor=Sup}=State) ->
    notify_supervisor(Sup, Completion),
    {stop, Completion, State#state{compiler=undefined}}.

%% This is used strictly when we're ready to stop
notify_supervisor(Supervisor, Status) ->
    gen_server:cast(Supervisor, Status).


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
    %% This should never be reached
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
