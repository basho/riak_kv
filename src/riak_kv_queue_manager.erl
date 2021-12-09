%% -------------------------------------------------------------------
%%
%% riak_kv_queue_manager: A behaviour for a worker managing a queue
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

%% @doc A behaviour for a worker managing a queue, where the queue will
%% overflow to disk

-module(riak_kv_queue_manager).
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-behaviour(gen_server).

-export([init/1,
        handle_call/3,
        handle_cast/2,
        handle_info/2,
        terminate/2,
        code_change/3,
        format_status/2]).

-export([start_link/2,
            start_job/3,
            request/2,
            stats/1,
            immediate_action/2,
            override_redo/2,
            clear_queue/1,
            stop_job/1]).

-define(REDO_PRIORITY, 1).
-define(REQUEST_PRIORITY, 2).
-define(PRIORITY_LIST, [?REQUEST_PRIORITY, ?REDO_PRIORITY]).
-define(LOG_TICK, 60000).
-define(REQUEST_BATCHSIZE, 50).
-define(REDO_BATCHSIZE, 1).

-type action_fun() :: fun((term(), boolean()) -> boolean()).

-callback start_link() -> {ok, pid()}.

-callback start_job(pos_integer()) -> {ok, pid()}.

-callback get_limits() ->
    {non_neg_integer(), pos_integer(), pos_integer()}.

-callback action(term(), boolean()) -> boolean().

-callback redo() -> boolean().

-record(state,  {
            callback_mod :: atom(),
            queue = riak_kv_overflow_queue:overflowq(),
            attempts = 0 :: non_neg_integer(),
            aborts = 0 :: non_neg_integer(),
            job_id :: non_neg_integer(), % can be 0 for named eraser
            pending_close = false :: boolean(),
            redo = true :: boolean(),
            action_fun = none :: action_fun() | none,
            file_path :: string(),
            redo_timeout :: non_neg_integer()
}).


%%%============================================================================
%%% API
%%%============================================================================


-spec start_link(atom(), string()) -> {ok, pid()}.
start_link(Module, RootPath) ->
    gen_server:start_link(
        {local, Module}, ?MODULE, [0, Module, RootPath], []).

%% @doc
%% To be used when starting a reaper for a specific workload
-spec start_job(pos_integer(), atom(), string()) -> {ok, pid()}.
start_job(JobID, Module, RootPath) ->
    gen_server:start_link(
        ?MODULE, [JobID, Module, RootPath], []).

-spec request(pid()|module(), term()) -> ok.
request(Pid, Reference) ->
    gen_server:cast(Pid, {request, Reference, ?REQUEST_PRIORITY}).

-spec stats(pid()|module()) ->
    list({atom(), non_neg_integer()|riak_kv_overflow_queue:queue_stats()}).
stats(Pid) ->
    gen_server:call(Pid, stats, infinity).

-spec immediate_action(pid()|module(), term()) -> boolean().
immediate_action(Pid, Reference) ->
    gen_server:call(Pid, {immediate_action, Reference}, infinity).

-spec override_redo(pid()|module(), boolean()) -> ok.
override_redo(Pid, Bool) ->
    gen_server:call(Pid, {override_redo, Bool}, infinity).

-spec clear_queue(pid()|module()) -> ok.
clear_queue(Pid) ->
    gen_server:call(Pid, clear_queue, infinity).

-spec stop_job(pid()) -> ok.
stop_job(Pid) ->
    gen_server:call(Pid, stop_job, infinity).


%%%============================================================================
%%% gen_server callbacks
%%%============================================================================


init([JobID, Module, FilePath]) ->
    erlang:send_after(?LOG_TICK, self(), log_queue),
    {OverflowQueue, RedoTimeout} = create_queue(FilePath, Module),
    {ok,
        #state{callback_mod = Module,
                job_id = JobID,
                redo = Module:redo(),
                redo_timeout = RedoTimeout,
                file_path = FilePath,
                queue = OverflowQueue},
        0}.

handle_call(stats, _From, State) ->
    QStats = riak_kv_overflow_queue:stats(State#state.queue),
    AStats = [{attempts, State#state.attempts}, {aborts, State#state.aborts}],
    {reply, QStats ++ AStats, State, 0};
handle_call({override_redo, Redo}, _From, State) ->
    {reply, ok, State#state{redo = Redo}, 0};
handle_call({override_action, ActionFun}, _From, State) ->
    %% Used only in tests to change the action function, and hence override
    %% the callback module's default action function
    {reply, ok, State#state{action_fun = ActionFun}, 0};
handle_call(clear_queue, _From, State) ->
    riak_kv_overflow_queue:close(State#state.file_path, State#state.queue),
    {OverflowQueue, RedoTimeout} =
        create_queue(State#state.file_path, State#state.callback_mod),
    S0 = State#state{queue = OverflowQueue, redo_timeout = RedoTimeout},
    {reply, ok, S0, 0};
handle_call(stop_job, _From, State) ->
    {reply, ok, State#state{pending_close = true}, 0};
handle_call({immediate_action, Reference}, _From, State) ->
    Mod = State#state.callback_mod,
    {reply, Mod:action(Reference), State, 0}.


handle_cast({request, Reference, Priority}, State) ->
    UpdOverflowQueue =
        riak_kv_overflow_queue:addto_queue(Priority,
                                            Reference,
                                            State#state.queue),
    {noreply, State#state{queue = UpdOverflowQueue}, 0}.


handle_info(timeout, State) ->
    Mod = State#state.callback_mod,
    ActionFun = 
        case State#state.action_fun of
            none ->
                fun(Ref) -> Mod:action(Ref, State#state.redo) end;
            OverrideActionFun ->
                fun(Ref) -> OverrideActionFun(Ref, State#state.redo) end
        end,
    RequeueFun = 
        fun(Ref) ->
            gen_server:cast(self(), {request, Ref, ?REDO_PRIORITY})
        end,
    PBL = [{?REQUEST_PRIORITY, ?REQUEST_BATCHSIZE},
            {?REDO_PRIORITY, ?REDO_BATCHSIZE}],
    {Priority, Attempts, Aborts, UpdQueue} =
        riak_kv_overflow_queue:work(
            PBL, ActionFun, RequeueFun, State#state.queue),
    UpdState =
        State#state{attempts = State#state.attempts + Attempts,
                    aborts = State#state.aborts + Aborts,
                    queue = UpdQueue},
    case {Priority, State#state.pending_close, Aborts} of
        {none, true, _} ->
            {stop, normal, UpdState};
        {none, false, _} ->
            {noreply, UpdState};
        {?REDO_PRIORITY, _, A} when A > 0 ->
            {noreply, UpdState, State#state.redo_timeout};
        _ ->
            {noreply, UpdState, 0}
    end;
handle_info(log_queue, State) ->
    UpdQueue =
        riak_kv_overflow_queue:log(
            State#state.callback_mod,
            State#state.job_id,
            State#state.attempts,
            State#state.aborts,
            State#state.queue),
    erlang:send_after(?LOG_TICK, self(), log_queue),
    {noreply, State#state{attempts = 0, aborts = 0, queue = UpdQueue}, 0}.

format_status(normal, [_PDict, S]) ->
    S;
format_status(terminate, [_PDict, S]) ->
    S#state{queue = riak_kv_overflow_queue:format_state(S#state.queue)}.

terminate(_Reason, State) ->
    riak_kv_overflow_queue:close(State#state.file_path, State#state.queue),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%============================================================================
%%% Internal Functions
%%%============================================================================

-spec create_queue(string(), module()) ->
    {riak_kv_overflow_queue:overflowq(), non_neg_integer()}.
create_queue(RootPath, Module)->
    {RedoTimeout, QueueLimit, OverflowLimit} = Module:get_limits(),
    OverflowQueue =
        riak_kv_overflow_queue:new(
            ?PRIORITY_LIST, RootPath, QueueLimit, OverflowLimit),
    {OverflowQueue, RedoTimeout}.
    

%%%============================================================================
%%% Unit tests
%%%============================================================================


-ifdef(TEST).

format_status_test() ->
    RootPath = riak_kv_test_util:get_test_dir("reaper_format_status/"),
    {ok, P} = start_job(1, riak_kv_reaper, RootPath),
    {status, P, {module, gen_server}, SItemL} = sys:get_status(P),
    S = lists:keyfind(state, 1, lists:nth(5, SItemL)),
    MQ = riak_kv_overflow_queue:get_mqueue(S#state.queue),
    ?assertNotMatch(not_logged, MQ),
    ST = format_status(terminate, [dict:new(), S]),
    MQT = riak_kv_overflow_queue:get_mqueue(ST#state.queue),
    ?assertMatch(not_logged, MQT),
    ok = stop_job(P).

-endif.