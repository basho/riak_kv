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

%% @doc Queue any read request originating from this node.  This is intended
%% for background read requests to trigger read repair. 
%% Each node should have a singleton reader initiated at startup.  Should
%% additional reap capacity be required, then reap jobs could start their own
%% reapers.


-module(riak_kv_reader).
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-export([start_link/1]).
-endif.

-behaviour(riak_kv_queue_manager).

-define(QUEUE_LIMIT, 100000).
-define(OVERFLOW_LIMIT, 10000000).
-define(REDO_TIMEOUT, 2000).
-define(OVERLOAD_PAUSE_MS, 10000).

-export([start_link/0,
            start_job/1,
            request_read/1,
            request_read/2,
            read_stats/0,
            read_stats/1,
            clear_queue/0,
            clear_queue/1,
            stop_job/1]).

-export([action/2,
            get_limits/0,
            redo/0]).

-type read_reference() :: {riak_object:bucket(), riak_object:key()}.
-type job_id() :: pos_integer().

-export_type([read_reference/0, job_id/0]).

%%%============================================================================
%%% API
%%%============================================================================

-spec start_link() -> {ok, pid()}.
start_link() ->
    start_link(app_helper:get_env(riak_kv, reader_dataroot)).

start_link(FilePath) ->
    riak_kv_queue_manager:start_link(?MODULE, FilePath).

-spec start_job(job_id()) -> {ok, pid()}.
%% @doc
%% To be used when starting a reaper for a specific workload
start_job(JobID) ->
    start_job(JobID, app_helper:get_env(riak_kv, reader_dataroot)).

start_job(JobID, FilePath) ->
   riak_kv_queue_manager:start_job(JobID, ?MODULE, FilePath).

-spec request_read(read_reference()) -> ok.
request_read(ReadReference) ->
    request_read(?MODULE, ReadReference).

-spec request_read(pid()|module(), read_reference()) -> ok.
request_read(Pid, ReadReference) ->
    riak_kv_queue_manager:request(Pid, ReadReference).

-spec read_stats() ->
    list({atom(), non_neg_integer()|riak_kv_overflow_queue:queue_stats()}).
read_stats() -> read_stats(?MODULE).

-spec read_stats(pid()|module()) -> 
    list({atom(), non_neg_integer()|riak_kv_overflow_queue:queue_stats()}).
read_stats(Pid) ->
    riak_kv_queue_manager:stats(Pid).

-spec clear_queue() -> ok.
clear_queue() -> clear_queue(?MODULE).

-spec clear_queue(pid()|module()) -> ok.
clear_queue(Reader) ->
   riak_kv_queue_manager:clear_queue(Reader).

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
        app_helper:get_env(riak_kv, reader_redo_timeout, ?REDO_TIMEOUT),
    QueueLimit =
        app_helper:get_env(riak_kv, reader_queue_limit, ?QUEUE_LIMIT),
    OverflowLimit =
        app_helper:get_env(riak_kv, reader_overflow_limit, ?OVERFLOW_LIMIT),
    {RedoTimeout, QueueLimit, OverflowLimit}.

%% @doc
%% Attempt to read the object
-spec action(read_reference(), boolean()) -> boolean().
action({B, K}, _Redo) ->
    {ok, C} = riak:local_client(),
    case riak_kv_util:consistent_object(B) of
        true ->
            _ = riak_kv_exchange_fsm:repair_consistent({B, K});
        false ->
            _ = riak_client:get(B, K, C)
    end,
    true.

-spec redo() -> boolean().
redo() -> true.
