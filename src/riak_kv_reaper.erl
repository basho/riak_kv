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
-export([start_link/1]).
-endif.

-behaviour(riak_kv_queue_manager).

-define(QUEUE_LIMIT, 100000).
-define(OVERFLOW_LIMIT, 10000000).
-define(REDO_TIMEOUT, 2000).

-export([start_link/0,
            start_job/1,
            request_reap/1,
            request_reap/2,
            direct_reap/1,
            reap_stats/0,
            reap_stats/1,
            clear_queue/0,
            clear_queue/1,
            stop_job/1]).

-export([action/2,
            get_limits/0,
            redo/0]).

-type reap_reference() ::
    {{riak_object:bucket(), riak_object:key()}, non_neg_integer()}.
-type job_id() :: pos_integer().

-export_type([reap_reference/0, job_id/0]).

%%%============================================================================
%%% API
%%%============================================================================

-spec start_link() -> {ok, pid()}.
start_link() ->
    start_link(app_helper:get_env(riak_kv, reaper_dataroot)).

start_link(FilePath) ->
    riak_kv_queue_manager:start_link(?MODULE, FilePath).

-spec start_job(job_id()) -> {ok, pid()}.
%% @doc
%% To be used when starting a reaper for a specific workload
start_job(JobID) ->
    start_job(JobID, app_helper:get_env(riak_kv, reaper_dataroot)).

start_job(JobID, FilePath) ->
   riak_kv_queue_manager:start_job(JobID, ?MODULE, FilePath).

-spec request_reap(reap_reference()) -> ok.
request_reap(ReapReference) ->
    request_reap(?MODULE, ReapReference).

-spec request_reap(pid()|module(), reap_reference()) -> ok.
request_reap(Pid, ReapReference) ->
    riak_kv_queue_manager:request(Pid, ReapReference).

-spec reap_stats() ->
    list({atom(), non_neg_integer()|riak_kv_overflow_queue:queue_stats()}).
reap_stats() -> reap_stats(?MODULE).

-spec reap_stats(pid()|module()) -> 
    list({atom(), non_neg_integer()|riak_kv_overflow_queue:queue_stats()}).
reap_stats(Pid) ->
    riak_kv_queue_manager:stats(Pid).

-spec direct_reap(reap_reference()) -> boolean().
direct_reap(ReapReference) ->
    riak_kv_queue_manager:immediate_action(?MODULE, ReapReference).

-spec clear_queue() -> ok.
clear_queue() -> clear_queue(?MODULE).

-spec clear_queue(pid()|module()) -> ok.
clear_queue(Reaper) ->
   riak_kv_queue_manager:clear_queue(Reaper).

%% @doc
%% Stop the job once the queue is empty
-spec stop_job(pid()) -> ok.
stop_job(Pid) ->
    riak_kv_queue_manager:stop_job(Pid).

%%%============================================================================
%%% Callback functions
%%%============================================================================

-spec get_limits() -> {pos_integer(), pos_integer(), pos_integer()}.
get_limits() ->
    RedoTimeout =
        app_helper:get_env(riak_kv, reaper_redo_timeout, ?REDO_TIMEOUT),
    QueueLimit =
        app_helper:get_env(riak_kv, reaper_queue_limit, ?QUEUE_LIMIT),
    OverflowLimit =
        app_helper:get_env(riak_kv, reaper_overflow_limit, ?OVERFLOW_LIMIT),
    {RedoTimeout, QueueLimit, OverflowLimit}.

%% @doc
%% If all primaries are up try and reap the tombstone.  The reap may fail, but
%% we will not redo - redo is only to handle the failure related to unavailable
%% primaries
-spec action(reap_reference(), boolean()) -> boolean().
action({{Bucket, Key}, DeleteHash}, Redo) ->
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
            if Redo -> false; true -> true end
    end.

-spec redo() -> boolean().
redo() -> true.

%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).


test_1inNreapfun(N) ->
    fun(ReapRef, _Bool) ->
        case erlang:phash2({ReapRef, os:timestamp()}) rem N of
            0 -> false;
            _ -> true
        end
    end.

test_100reap(_ReapRef, _Bool) ->
    true.


standard_reaper_test_() ->
    {timeout, 30, fun standard_reaper_tester/0}.

failure_reaper_test_() ->
    {timeout, 60, fun somefail_reaper_tester/0}.

standard_reaper_tester() ->
    NumberOfRefs = 1000,
    {ok, P} = start_job(1, riak_kv_test_util:get_test_dir("std_reaper")),
    ok = gen_server:call(P, {override_action, fun test_100reap/2}),
    B = {<<"type1">>, <<"B1">>},
    RefList =
        lists:map(fun(X) -> {{B, term_to_binary(X)}, erlang:phash2(X)} end,
                    lists:seq(1, NumberOfRefs)),
    spawn(fun() ->
                lists:foreach(fun(R) -> request_reap(P, R) end, RefList)
            end),
    WaitFun =
        fun(Sleep, Done) ->
            case Done of
                false ->
                    timer:sleep(Sleep),
                    [{mqueue_lengths,[{1,RedoQL},{2,ReapQL}]},
                        {overflow_lengths,[{2,0},{1,0}]},
                        {overflow_discards,[{2,0},{1,0}]},
                        {attempts,AT},
                        {aborts,AB}] = reap_stats(P),
                    case AT of
                        NumberOfRefs ->
                            ?assertMatch(0, AB),
                            ?assertMatch({0, 0}, {RedoQL, ReapQL}),
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
    {ok, P} = start_job(1, riak_kv_test_util:get_test_dir("err_reaper")),
    ok = gen_server:call(P, {override_action, test_1inNreapfun(N)}),
    B = {<<"type1">>, <<"B1">>},
    RefList =
        lists:map(fun(X) -> {{B, term_to_binary(X)}, erlang:phash2(X)} end,
                    lists:seq(1, NumberOfRefs)),
    spawn(fun() ->
                lists:foreach(fun(R) -> request_reap(P, R) end, RefList)
            end),
    WaitFun =
        fun(Sleep, Done) ->
            case Done of
                false ->
                    timer:sleep(Sleep),
                    [{mqueue_lengths,[{1,RedoQL},{2,ReapQL}]},
                        {overflow_lengths,[{2,0},{1,0}]},
                        {overflow_discards,[{2,0},{1,0}]},
                        {attempts,AT},
                        {aborts,AB}] = reap_stats(P),
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
