%% -------------------------------------------------------------------
%%
%% riak_kv_reaper: Process for queueing and applying reap requests
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

%% @doc Queue any reap request originating from this node.  The process will
%% reap each tombstone one by one, waiting or the reap attempt to be
%% acknowledged from each vnode - so as to act as a natural throttle on reap
%% workloads.
%% Each node should have a singleton reaper initiated at startup.  Should
%% additional reap capacity be required, then reap jobs could start their own
%% reapers.


-module(riak_kv_reaper).
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
            request_reap/2,
            request_reap/3,
            direct_reap/1,
            reap_stats/1,
            clear_queue/1,
            stop_job/1]).

-define(QUEUE_LIMIT, 100000).
-define(LOG_TICK, 60000).
-define(REDO_TIMEOUT, 2000).
-define(MAX_BATCH_SIZE, 100).

-record(state,  {
            reap_queue = riak_core_priority_queue:new() :: pqueue(),
            pqueue_length = {0, 0} :: queue_length(),
            reap_attempts = 0 :: non_neg_integer(),
            reap_aborts = 0 :: non_neg_integer(),
            job_id :: non_neg_integer(), % can be 0 for named reaper
            pending_close = false :: boolean(),
            last_tick_time = os:timestamp() :: erlang:timestamp(),
            reap_fun :: reap_fun()
}).

-type priority() :: 1..2.
-type squeue() :: {queue, [any()], [any()]}.
-type pqueue() ::  squeue() | {pqueue, [{priority(), squeue()}]}.
-type queue_length() :: {non_neg_integer(), non_neg_integer()}.
-type reap_reference() ::
    {{riak_object:bucket(), riak_object:key()}, non_neg_integer()}.
-type job_id() :: pos_integer().

-type reap_stats() :: {non_neg_integer(), non_neg_integer(), queue_length()}.

-type reap_fun() :: fun((reap_reference()) -> boolean()).

-export_type([reap_reference/0, job_id/0]).

%%%============================================================================
%%% API
%%%============================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [0, fun reap/1], []).

-spec start_job(job_id()) -> {ok, pid()}.
%% @doc
%% To be used when starting a reaper for a specific workload
start_job(JobID) ->
    gen_server:start_link(?MODULE, [JobID, fun reap/1], []).

-spec request_reap(reap_reference(), priority()) -> ok.
request_reap(ReapReference, Priority) when Priority == 1; Priority == 2 ->
    gen_server:cast(?MODULE, {request_reap, ReapReference, Priority}).

-spec request_reap(pid(), reap_reference(), priority()) -> ok.
request_reap(Pid, ReapReference, Priority) when Priority == 1; Priority == 2 ->
    gen_server:cast(Pid, {request_reap, ReapReference, Priority}).

-spec reap_stats(pid()) -> reap_stats().
reap_stats(Pid) ->
    gen_server:call(Pid, reap_stats, infinity).

-spec direct_reap(reap_reference()) -> boolean().
direct_reap(ReapReference) ->
    gen_server:call(?MODULE, {direct_reap, ReapReference}).

-spec clear_queue(pid()|riak_kv_reaper) -> ok.
clear_queue(Reaper) ->
    gen_server:call(Reaper, clear_queue, infinity).

%% @doc
%% Stop the job once the queue is empty
-spec stop_job(pid()) -> ok.
stop_job(Pid) ->
    gen_server:call(Pid, stop_job, 5000).

%%%============================================================================
%%% gen_server callbacks
%%%============================================================================

init([JobID, ReapFun]) ->
    erlang:send_after(?LOG_TICK, self(), log_queue),
    {ok, #state{job_id = JobID, reap_fun = ReapFun}, 0}.

handle_call(reap_stats, _From, State) ->
    Stats =
        {State#state.reap_attempts,
            State#state.reap_aborts,
            State#state.pqueue_length},
    {reply, Stats, State, 0};
handle_call({direct_reap, ReapReference}, _From, State) ->
    {reply, reap(ReapReference), State, 0};
handle_call(clear_queue, _From, State) ->
    {reply,
        ok,
        State#state{reap_queue = riak_core_priority_queue:new(),
                    pqueue_length = {0, 0}},
        0};
handle_call(stop_job, _From, State) ->
    {reply, ok, State#state{pending_close = true}, 0}.

handle_cast({request_reap, ReapReference, Priority}, State) ->

    UpdQL =
        setelement(Priority,
                    State#state.pqueue_length,
                    element(Priority, State#state.pqueue_length) + 1),
    UpdQ =
        riak_core_priority_queue:in(ReapReference,
                                    Priority,
                                    State#state.reap_queue),
    {noreply, State#state{pqueue_length = UpdQL, reap_queue = UpdQ}, 0}.

handle_info(timeout, State) ->
    ReapFun = State#state.reap_fun,
    case State#state.pqueue_length of
        {0, 0} ->
            case State#state.pending_close of
                true ->
                    {stop, normal, State};
                false ->
                    %% No timeout here, as no work to do
                    {noreply, State}
            end;
        {RedoQL, 0} ->
            %% Work a single item on the redo queue, and no immediate timeout
            %% if it aborts
            case riak_core_priority_queue:out(State#state.reap_queue) of
                {{value, ReapRef}, Q0} ->
                    case ReapFun(ReapRef) of
                        true ->
                            AT0 = State#state.reap_attempts + 1,
                            QL0 = {RedoQL - 1, 0},
                            {noreply,
                                State#state{reap_queue = Q0, 
                                            reap_attempts = AT0,
                                            pqueue_length = QL0},
                                0};
                        false ->
                            AB0 = State#state.reap_aborts + 1,
                            Q1 = riak_core_priority_queue:in(ReapRef, 1, Q0),
                            {noreply,
                                State#state{reap_queue = Q1, 
                                            reap_aborts = AB0},
                                ?REDO_TIMEOUT}
                    end;
                {empty, Q0} ->
                    %% This shoudn't happen - must have miscalulated
                    %% queue lengths
                    {noreply,
                        State#state{reap_queue = Q0,
                                    pqueue_length = {0, 0}},
                        0}
            end;
        {RedoQL, ReapQL} ->
            %% Work a batch of items from the reap queue before yielding
            BatchSize = min(ReapQL, ?MAX_BATCH_SIZE),
            BatchFun =
                fun(_X, {Q, AT, AB}) ->
                    case riak_core_priority_queue:out(Q) of
                        {{value, ReapRef}, Q0} ->
                            case ReapFun(ReapRef) of
                                true ->
                                    {Q0, AT + 1, AB};
                                false ->
                                    ok = request_reap(self(), ReapRef, 1),
                                    {Q0, AT, AB + 1}
                            end;
                        {empty, Q0} ->
                            %% This shouldn't happen
                            {Q0, AT, AB}
                    end
                end,
            {UpdQ, AT0, AB0} =
                lists:foldl(BatchFun,
                            {State#state.reap_queue,
                                State#state.reap_attempts,
                                State#state.reap_aborts},
                            lists:seq(1, BatchSize)),
            {noreply,
                State#state{reap_queue = UpdQ,
                            reap_attempts = AT0,
                            reap_aborts = AB0,
                            pqueue_length = {RedoQL, ReapQL - BatchSize}},
                0}
    end;
handle_info(log_queue, State) ->
    lager:info("Reaper Job ~w has queue lengths ~w " ++
                    " reap_attempts=~w reap_aborts=~w ",
                [State#state.job_id,
                    State#state.pqueue_length,
                    State#state.reap_attempts,
                    State#state.reap_aborts]),
    erlang:send_after(?LOG_TICK, self(), log_queue),
    {noreply, State#state{reap_attempts = 0, reap_aborts = 0}, 0}.


terminate(normal, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%============================================================================
%%% Internal functions
%%%============================================================================

%% @doc
%% If all primaries are up try and reap the tombstone.  The reap may fail, but
%% we will not redo - redo is only to handle the failire related to unavailable
%% primaries
-spec reap(reap_reference()) -> boolean().
reap({{Bucket, Key}, DeleteHash}) ->
    BucketProps = riak_core_bucket:get_bucket(Bucket),
    DocIdx = riak_core_util:chash_key({Bucket, Key}, BucketProps),
    {n_val, N} = lists:keyfind(n_val, 1, BucketProps),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, N, riak_kv),
    case length(PrefList) of
        N ->
            PL0 = lists:map(fun({Target, primary}) -> Target end, PrefList),
            ok = riak_kv_vnode:reap(PL0, {Bucket, Key}, DeleteHash),
            true;
        _ ->
            false
    end.


%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).

start_test(JobID, ReapFun) ->
    gen_server:start_link(?MODULE, [JobID, ReapFun], []).


test_1inNreapfun(N) ->
    fun(ReapRef) ->
        case erlang:phash2({ReapRef, os:timestamp()}) rem N of
            0 -> false;
            _ -> true
        end
    end.

test_100reap(_ReapRef) ->
    true.

standard_reaper_test_() ->
    {timeout, 30, fun standard_reaper_tester/0}.

failure_reaper_test_() ->
    {timeout, 60, fun somefail_reaper_tester/0}.

standard_reaper_tester() ->
    NumberOfRefs = 1000,
    {ok, P} = start_test(1, fun test_100reap/1),
    B = {<<"type1">>, <<"B1">>},
    RefList =
        lists:map(fun(X) -> {{B, term_to_binary(X)}, erlang:phash2(X)} end,
                    lists:seq(1, NumberOfRefs)),
    spawn(fun() ->
                lists:foreach(fun(R) -> request_reap(P, R, 2) end, RefList)
            end),
    WaitFun = 
        fun(Sleep, Done) ->
            case Done of
                false ->
                    timer:sleep(Sleep),
                    {AT, AB, L} = reap_stats(P),
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
    lists:foldl(WaitFun, false, lists:seq(101, 200)),
    ok = stop_job(P),
    timer:sleep(100),
    ?assertMatch(false, is_process_alive(P)).

somefail_reaper_tester() ->
    somefail_reaper_tester(4),
    somefail_reaper_tester(16),
    somefail_reaper_tester(64).


somefail_reaper_tester(N) ->
    NumberOfRefs = 1000,
    {ok, P} = start_test(1, test_1inNreapfun(N)),
    B = {<<"type1">>, <<"B1">>},
    RefList =
        lists:map(fun(X) -> {{B, term_to_binary(X)}, erlang:phash2(X)} end,
                    lists:seq(1, NumberOfRefs)),
    spawn(fun() ->
                lists:foreach(fun(R) -> request_reap(P, R, 2) end, RefList)
            end),
    WaitFun = 
        fun(Sleep, Done) ->
            case Done of
                false ->
                    timer:sleep(Sleep),
                    {AT, AB, {RedoQL, ReapQL}} = reap_stats(P),
                    case (AT + AB >= NumberOfRefs) of
                        true ->
                            ?assertMatch(true, AB > 0),
                            ?assertMatch(true, AT > 0),
                            ?assertMatch(true, AT > AB),
                            ?assertMatch(true, RedoQL >= 0),
                            ?assertMatch(NumberOfRefs, AT + RedoQL),
                            ?assertMatch(0, ReapQL),
                            true;
                        false ->
                            false
                    end;
                true ->
                    true
            end
        end,
    lists:foldl(WaitFun, false, lists:seq(101, 500)),
    ok = clear_queue(P),
    ok = stop_job(P),
    timer:sleep(100),
    ?assertMatch(false, is_process_alive(P)).


-endif.