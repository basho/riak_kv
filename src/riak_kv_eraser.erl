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
-export([start_link/1]).
-endif.

-behaviour(riak_kv_queue_manager).

-define(QUEUE_LIMIT, 100000).
-define(OVERFLOW_LIMIT, 10000000).
-define(REDO_TIMEOUT, 2000).
-define(DELETE_TIMEOUT, 10000).

-export([start_link/0,
            start_job/1,
            request_delete/1,
            request_delete/2,
            delete_stats/0,
            delete_stats/1,
            override_redo/1,
            clear_queue/0,
            clear_queue/1,
            stop_job/1]).

-export([action/2,
            get_limits/0,
            redo/0]).

-type delete_reference() ::
    {{riak_object:bucket(), riak_object:key()}, vclock:vclock()}.

-type job_id() :: pos_integer().

-export_type([delete_reference/0, job_id/0]).

-include_lib("kernel/include/logger.hrl").

%%%============================================================================
%%% API
%%%============================================================================

-spec start_link() -> {ok, pid()}.
start_link() ->
    start_link(app_helper:get_env(riak_kv, eraser_dataroot)).

start_link(FilePath) ->
    riak_kv_queue_manager:start_link(?MODULE, FilePath).

-spec start_job(job_id()) -> {ok, pid()}.
%% @doc
%% To be used when starting a reaper for a specific workload
start_job(JobID) ->
    start_job(JobID, app_helper:get_env(riak_kv, eraser_dataroot)).

start_job(JobID, FilePath) ->
   riak_kv_queue_manager:start_job(JobID, ?MODULE, FilePath).

-spec request_delete(delete_reference()) -> ok.
request_delete(DeleteReference) ->
    request_delete(?MODULE, DeleteReference).

-spec request_delete(pid()|module(), delete_reference()) -> ok.
request_delete(Pid, DeleteReference) ->
    riak_kv_queue_manager:request(Pid, DeleteReference).

-spec delete_stats() ->
    list({atom(), non_neg_integer()|riak_kv_overflow_queue:queue_stats()}).
delete_stats() -> delete_stats(?MODULE).

-spec delete_stats(pid()|module()) ->
    list({atom(), non_neg_integer()|riak_kv_overflow_queue:queue_stats()}).
delete_stats(Pid) ->
    riak_kv_queue_manager:stats(Pid).

%% @doc
%% If delete_mode is not keep, but it is still preferred to attempt deletes
%% during unavailbaility of primaries then use override_redo to force this.
-spec override_redo(boolean()) -> ok.
override_redo(Redo) ->
    riak_kv_queue_manager:override_redo(?MODULE, Redo).

-spec clear_queue() -> ok.
clear_queue() -> clear_queue(?MODULE).

-spec clear_queue(pid()|module()) -> ok.
clear_queue(Pid) ->
    riak_kv_queue_manager:clear_queue(Pid).

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
        app_helper:get_env(riak_kv, eraser_redo_timeout, ?REDO_TIMEOUT),
    QueueLimit =
        app_helper:get_env(riak_kv, eraser_queue_limit, ?QUEUE_LIMIT),
    OverflowLimit =
        app_helper:get_env(riak_kv, eraser_overflow_limit, ?OVERFLOW_LIMIT),
    {RedoTimeout, QueueLimit, OverflowLimit}.

%% @doc
%% Try and delete the key.  If Redo is true, this should only be attempted if
%% all primaries are up
-spec action(delete_reference(), boolean()) -> boolean().
action({{Bucket, Key}, VectorClock}, true) ->
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
action({{Bucket, Key}, VectorClock}, false) ->
    riak_kv_delete:delete(eraser,
                            Bucket, Key,
                            [], ?DELETE_TIMEOUT, undefined, eraser,
                            VectorClock),
    true.

redo() ->
    case app_helper:get_env(riak_kv, delete_mode, 3000) of
        keep ->
            %% Tombstones are kept, so a delete attempted during failure
            %% will eventually happen and not be resurrected.  So if
            %% primaries are not available no need to defer deletion to
            %% avoid resurrection of tombstones.
            false;
        _ ->
            true
    end.


%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).

test_100delete(_DeleteRef, _Bool) ->
    true.

standard_eraser_test_() ->
    {timeout, 30, fun standard_eraser_tester/0}.

standard_eraser_tester() ->
    NumberOfRefs = 1000,
    {ok, Pid} = start_link(riak_kv_test_util:get_test_dir("std_eraser")),
    ?assert(is_process_alive(Pid)),
    ok = gen_server:call(Pid, {override_action, fun test_100delete/2}),
    B = {<<"type1">>, <<"B1">>},
    RefList =
        lists:map(fun(X) -> {{B, term_to_binary(X)}, erlang:phash2(X)} end,
                    lists:seq(1, NumberOfRefs)),
    spawn(fun() ->
                lists:foreach(fun(R) ->
                                request_delete(?MODULE, R)
                            end,
                            RefList)
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
                        {aborts,AB}] = delete_stats(?MODULE),
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
    ok = stop_job(?MODULE),
    timer:sleep(100),
    ?assertMatch(false, is_process_alive(Pid)).

-endif.
