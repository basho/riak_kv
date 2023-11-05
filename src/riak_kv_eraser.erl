%% -------------------------------------------------------------------
%%
%% riak_kv_eraser: Process for queueing and applying delete requests
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

%% @doc Queue up and act on delete requests, such as delete requests prompted
%% by aae_folds


-module(riak_kv_eraser).
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-behaviour(gen_server).

-export([init/1,
        handle_call/3,
        handle_cast/2,
        handle_info/2,
        terminate/2,
        code_change/3]).

-export([start_link/0,
            start_job/1,
            request_delete/1,
            request_delete/2,
            delete_stats/1,
            override_redo/1,
            clear_queue/1,
            stop_job/1]).

-define(QUEUE_LIMIT, 100000).
-define(LOG_TICK, 60000).
-define(REDO_TIMEOUT, 2000).
-define(MAX_BATCH_SIZE, 100).
-define(DELETE_TIMEOUT, 10000).

-record(state,  {
            delete_queue = riak_core_priority_queue:new() :: pqueue(),
            pqueue_length = {0, 0} :: queue_length(),
            delete_attempts = 0 :: non_neg_integer(),
            delete_aborts = 0 :: non_neg_integer(),
            job_id :: non_neg_integer(), % can be 0 for named eraser
            pending_close = false :: boolean(),
            last_tick_time = os:timestamp() :: erlang:timestamp(),
            erase_fun :: erase_fun(),
            redo_deletes = false :: boolean(),
            redo_timeout :: non_neg_integer()
}).

-type priority() :: 1..2.
-type squeue() :: {queue, [any()], [any()]}.
-type pqueue() ::  squeue() | {pqueue, [{priority(), squeue()}]}.
-type queue_length() :: {non_neg_integer(), non_neg_integer()}.
-type delete_reference() ::
    {{riak_object:bucket(), riak_object:key()}, vclock:vclock()}.
-type job_id() :: pos_integer().

-type delete_stats() :: {non_neg_integer(), non_neg_integer(), queue_length()}.

-type eraser_state() :: #state{}.
-type erase_fun() :: fun((delete_reference(), boolean()) -> boolean()).


-export_type([delete_reference/0, job_id/0]).

%%%============================================================================
%%% API
%%%============================================================================

start_link() ->
    DeleteMode = app_helper:get_env(riak_kv, delete_mode, 3000),
    gen_server:start_link({local, ?MODULE}, ?MODULE,
                            [0, fun erase/2, DeleteMode], []).

%% @doc
%% To be used when starting a eraser for a specific workload
-spec start_job(job_id()) -> {ok, pid()}.
start_job(JobID) ->
    DeleteMode = app_helper:get_env(riak_kv, delete_mode, 3000),
    gen_server:start_link(?MODULE, [JobID, fun erase/2, DeleteMode], []).

-spec request_delete(delete_reference()) -> ok.
request_delete(DeleteReference) ->
    request_delete(?MODULE, DeleteReference, 2).

-spec request_delete(pid(), delete_reference()) -> ok.
request_delete(Pid, DeleteReference) ->
    request_delete(Pid, DeleteReference, 2).

%% @doc
%% Priority of Deletes is by default 2. 1 is reserved for redo of deletes
-spec request_delete(pid()|atom(), delete_reference(), priority()) -> ok.
request_delete(Pid, DelReference, Priority)
                                        when Priority == 1; Priority == 2 ->
    gen_server:cast(Pid, {request_delete, DelReference, Priority}).

-spec delete_stats(pid()) -> delete_stats().
delete_stats(Pid) ->
    gen_server:call(Pid, delete_stats, infinity).

%% @doc
%% If delete_mode is not keep, but it is still preferred to attempt deletes
%% during unavailbaility of primaries then use override_redo to force this.
-spec override_redo(boolean()) -> ok.
override_redo(Redo) ->
    gen_server:call(?MODULE, {override_redo, Redo}, infinity).

-spec clear_queue(pid()|riak_kv_eraser) -> ok.
clear_queue(Eraser) ->
    gen_server:call(Eraser, clear_queue, infinity).

%% @doc
%% Stop the job once the queue is empty
-spec stop_job(pid()) -> ok.
stop_job(Pid) ->
    gen_server:call(Pid, stop_job).

%%%============================================================================
%%% gen_server callbacks
%%%============================================================================

init([JobID, DelFun, DeleteMode]) ->
    erlang:send_after(?LOG_TICK, self(), log_queue),
    Redo =
        case DeleteMode of
            keep ->
                %% Tombstones are kept, so a delete attempted during failure
                %% will eventually happen and not be resurrected.  So if
                %% primaries are not available no need to defer deletion to
                %% avoid resurrection of tombstones.
                false;
            _ ->
                true
        end,
    RedoTimeout = app_helper:get_env(riak_kv, eraser_redo_timeout, ?REDO_TIMEOUT),
    {ok, #state{job_id = JobID, erase_fun = DelFun, redo_deletes = Redo,
                redo_timeout = RedoTimeout}, 0}.

handle_call(Msg, _From, State) ->
    {reply, R, S0} = handle_sync_message(Msg, State),
    {reply, R, S0, 0}.

handle_cast(Msg, State) ->
    {noreply, S0} = handle_async_message(Msg, State),
    {noreply, S0, 0}.

handle_info(Msg, State) ->
    case handle_async_message(Msg, State) of
        {override_timeout, none, R} ->
            R;
        {override_timeout, T0, {noreply, S0}} ->
            {noreply, S0, T0};
        {noreply, S0} ->
            {noreply, S0, 0}
    end.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%============================================================================
%%% Actual Callbacks
%%%============================================================================

%% Handle a call message - with a reply.  The handle_call function will enforce
%% the addition of an immediate timeout
-spec handle_sync_message(any(), eraser_state()) ->
        {reply, any(), eraser_state()}.
handle_sync_message(delete_stats, State) ->
    Stats =
        {State#state.delete_attempts,
            State#state.delete_aborts,
            State#state.pqueue_length},
    {reply, Stats, State};
handle_sync_message({override_redo, Redo}, State) ->
    {reply, ok, State#state{redo_deletes = Redo}};
handle_sync_message(clear_queue, State) ->
    {reply,
        ok,
        State#state{delete_queue = riak_core_priority_queue:new(),
                    pqueue_length = {0, 0}}};
handle_sync_message(stop_job, State) ->
    {reply, ok, State#state{pending_close = true}}.

%% Handle a cast or info message - with a reply.  The handle_cast function
%% will expet a {noreply, State} response and will override the return with
%% an immediate timeout.  the handle_info may also receive a reply with an
%% overrride to the timeout.
-spec handle_async_message(any(), eraser_state()) ->
        {noreply, eraser_state()}|
        {override_timeout, none|pos_integer(), tuple()}.
handle_async_message({request_delete, DelReference, Priority}, State) ->

    UpdQL =
        setelement(Priority,
                    State#state.pqueue_length,
                    element(Priority, State#state.pqueue_length) + 1),
    UpdQ =
        riak_core_priority_queue:in(DelReference,
                                    Priority,
                                    State#state.delete_queue),
    {noreply, State#state{pqueue_length = UpdQL, delete_queue = UpdQ}};
handle_async_message(timeout, State) ->
    DelFun = State#state.erase_fun,
    case State#state.pqueue_length of
        {0, 0} ->
            case State#state.pending_close of
                true ->
                    {override_timeout, none, {stop, normal, State}};
                false ->
                    %% No timeout here, as no work to do
                    {override_timeout, none, {noreply, State}}
            end;
        {RedoQL, 0} ->
            %% Work a single item on the redo queue, and no immediate timeout
            %% if it aborts
            case riak_core_priority_queue:out(State#state.delete_queue) of
                {{value, DelRef}, Q0} ->
                    case DelFun(DelRef, State#state.redo_deletes) of
                        true ->
                            AT0 = State#state.delete_attempts + 1,
                            QL0 = {RedoQL - 1, 0},
                            {noreply,
                                State#state{delete_queue = Q0,
                                            delete_attempts = AT0,
                                            pqueue_length = QL0}};
                        false ->
                            AB0 = State#state.delete_aborts + 1,
                            Q1 = riak_core_priority_queue:in(DelRef, 1, Q0),
                            {override_timeout,
                                 State#state.redo_timeout,
                                {noreply,
                                    State#state{delete_queue = Q1,
                                                delete_aborts = AB0}}}
                    end;
                {empty, Q0} ->
                    %% This shoudn't happen - must have miscalulated
                    %% queue lengths
                    {noreply,
                        State#state{delete_queue = Q0,
                                    pqueue_length = {0, 0}}}
            end;
        {RedoQL, DeleteQL} ->
            %% Work a batch of items from the erase queue before yielding
            BatchSize = min(DeleteQL, ?MAX_BATCH_SIZE),
            BatchFun =
                fun(_X, {Q, AT, AB}) ->
                    case riak_core_priority_queue:out(Q) of
                        {{value, DelRef}, Q0} ->
                            case DelFun(DelRef, State#state.redo_deletes) of
                                true ->
                                    {Q0, AT + 1, AB};
                                false ->
                                    ok = request_delete(self(), DelRef, 1),
                                    {Q0, AT, AB + 1}
                            end;
                        {empty, Q0} ->
                            %% This shouldn't happen
                            {Q0, AT, AB}
                    end
                end,
            {UpdQ, AT0, AB0} =
                lists:foldl(BatchFun,
                            {State#state.delete_queue,
                                State#state.delete_attempts,
                                State#state.delete_aborts},
                            lists:seq(1, BatchSize)),
            {noreply,
                State#state{delete_queue = UpdQ,
                            delete_attempts = AT0,
                            delete_aborts = AB0,
                            pqueue_length = {RedoQL, DeleteQL - BatchSize}}}
    end;
handle_async_message(log_queue, State) ->
    case app_helper:get_env(riak_kv, queue_manager_log_suppress_zero_stats, false) of
        true when State#state.pqueue_length =:= {0,0},
                  State#state.delete_attempts =:= 0,
                  State#state.delete_aborts =:= 0 ->
            nop;
        _ ->
            lager:info("Reaper Job ~w has queue lengths ~w "
                       "reap_attempts=~w reap_aborts=~w",
                       [State#state.job_id,
                        State#state.pqueue_length,
                        State#state.delete_attempts,
                        State#state.delete_aborts])
        end,
    erlang:send_after(?LOG_TICK, self(), log_queue),
    {noreply, State#state{delete_attempts = 0, delete_aborts = 0}}.


%%%============================================================================
%%% Internal functions
%%%============================================================================

%% @doc
%% Try and delete the key.  If Redo is true, this should only be attempted if
%% all primaries are up
-spec erase(delete_reference(), boolean()) -> boolean().
erase({{Bucket, Key}, VectorClock}, true) ->
    BucketProps = riak_core_bucket:get_bucket(Bucket),
    DocIdx = riak_core_util:chash_key({Bucket, Key}, BucketProps),
    {n_val, N} = lists:keyfind(n_val, 1, BucketProps),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, N, riak_kv),
    case length(PrefList) of
        N ->
            riak_kv_delete:delete(eraser,
                                    Bucket, Key,
                                    [], ?DELETE_TIMEOUT, undefined, eraser,
                                    VectorClock),
            true;
        _ ->
            false
    end;
erase({{Bucket, Key}, VectorClock}, false) ->
    riak_kv_delete:delete(eraser,
                            Bucket, Key,
                            [], ?DELETE_TIMEOUT, undefined, eraser,
                            VectorClock),
    true.


%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).

start_test(JobID, DelFun) ->
    gen_server:start_link(?MODULE, [JobID, DelFun, immediate], []).


test_1inNdeletefun(N) ->
    fun(DelRef, _Redo) ->
        case erlang:phash2({DelRef, os:timestamp()}) rem N of
            0 -> false;
            _ -> true
        end
    end.

test_100delete(_DelRef, _Redo) ->
    true.

standard_eraser_test_() ->
    {timeout, 30, fun standard_eraser_tester/0}.

failure_eraser_test_() ->
    {timeout, 60, fun somefail_eraser_tester/0}.

standard_eraser_tester() ->
    NumberOfRefs = 1000,
    {ok, P} = start_test(1, fun test_100delete/2),
    B = {<<"type1">>, <<"B1">>},
    RefList =
        lists:map(fun(X) ->
                        {{B, term_to_binary(X)}, vclock:fresh(test, X)}
                    end,
                    lists:seq(1, NumberOfRefs)),
    spawn(fun() ->
                lists:foreach(fun(R) -> request_delete(P, R, 2) end, RefList)
            end),
    WaitFun =
        fun(Sleep, Done) ->
            case Done of
                false ->
                    timer:sleep(Sleep),
                    {AT, AB, L} = delete_stats(P),
                    case AT of
                        NumberOfRefs ->
                            ?assertMatch(0, AB),
                            ?assertMatch({0, 0}, L),
                            true;
                        _ ->
                            false
                    end;
                true ->
                    true
            end
        end,
    ?assertMatch(true, lists:foldl(WaitFun, false, lists:seq(101, 200))),
    ok = stop_job(P),
    timer:sleep(100),
    ?assertMatch(false, is_process_alive(P)).

somefail_eraser_tester() ->
    somefail_eraser_tester(4),
    somefail_eraser_tester(16),
    somefail_eraser_tester(64).


somefail_eraser_tester(N) ->
    NumberOfRefs = 1000,
    {ok, P} = start_test(1, test_1inNdeletefun(N)),
    B = {<<"type1">>, <<"B1">>},
    RefList =
        lists:map(fun(X) ->
                        {{B, term_to_binary(X)}, vclock:fresh(test, X)}
                    end,
                    lists:seq(1, NumberOfRefs)),
    spawn(fun() ->
                lists:foreach(fun(R) -> request_delete(P, R, 2) end, RefList)
            end),
    WaitFun =
        fun(Sleep, Done) ->
            case Done of
                false ->
                    timer:sleep(Sleep),
                    {AT, AB, {RedoQL, DelQL}} = delete_stats(P),
                    case (AT + AB >= NumberOfRefs) of
                        true ->
                            ?assertMatch(true, AB > 0),
                            ?assertMatch(true, AT > 0),
                            ?assertMatch(true, AT > AB),
                            ?assertMatch(true, RedoQL >= 0),
                            ?assertMatch(NumberOfRefs, AT + RedoQL),
                            ?assertMatch(0, DelQL),
                            true;
                        false ->
                            false
                    end;
                true ->
                    true
            end
        end,
    ?assertMatch(true, lists:foldl(WaitFun, false, lists:seq(101, 500))),
    ok = clear_queue(P),
    ok = stop_job(P),
    timer:sleep(100),
    ?assertMatch(false, is_process_alive(P)).


-endif.
