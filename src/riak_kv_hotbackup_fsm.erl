%% -------------------------------------------------------------------
%%
%% riak_hotbackup_fsm: Manages backups across all vnodes
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

%% @doc The AAE fold FSM allows for coverage folds acrosss Tictac AAE 
%% Controllers

-module(riak_kv_hotbackup_fsm).

-behaviour(riak_core_coverage_fsm).

-include_lib("riak_kv_vnode.hrl").

-export([init/2,
         process_results/2,
         finish/2]).
-export([complete/0, not_supported/0, bad_request/0]).

-type from() :: {atom(), req_id(), pid()}.
-type req_id() :: non_neg_integer().
-type backup_path() :: string().
-type coverage() :: {pos_integer(), pos_integer()}.
    % Coverage represents a pair of integers of default_nval and the n_val for
    % the backup coverage plan.  If all buckets have an n_val of 3, and the
    % request is to backup just enough to allow for rebuild *assuming* no vnode
    % is incomplete then {3, 1} would perform a minimal backup.
    % It is assumes that most backups will be of the form (N, N} (e.g. {3, 3}),
    % so that all vnodes are prompted to backup.
    % If there are multiple n_vals used in the cluster, choosing a
    % default_nval that is greater than the lowest n_val used may have
    % unpredictable results (if the plan n_val is also less than the default
    % n_val)
-type timeout() :: pos_integer().
    % timeout measured in ms
-type result() :: complete|not_supported|bad_request.
-type result_score() :: {result(), non_neg_integer()}.

-type inbound_api() :: list(backup_path()|coverage()|timeout()).


-record(state, {from :: from(),
                acc = [] :: list(result_score()),
                start_time = os:timestamp() :: erlang:timestamp()}).


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-spec init(from(), inbound_api()) -> tuple().
%% @doc 
%% Return a tuple containing the ModFun to call per vnode, the number of 
%% primary preflist vnodes the operation should cover, the service to use to 
%% check for available nodes,and the registered name to use to access the 
%% vnode master process.
init(From={_, _, _}, [BackupPath, CoverageN, Timeout]) ->
    lager:info("Hot Backup prompted with coverage ~w to path ~s",
                [CoverageN, BackupPath]),
    {TargetNVal, PlanNVal} = CoverageN,
    Req = riak_kv_requests:new_hotbackup_request(BackupPath),
    {Req, all, TargetNVal, PlanNVal, 
        riak_kv, riak_kv_vnode_master, 
        Timeout, 
        #state{from = From,
                acc = [{complete, 0}, {not_supported, 0}, {bad_request, 0}]}}.
        

process_results({error, Reason}, _State) ->
    lager:warning("Failure to process fold results due to ~w", [Reason]),
    {error, Reason};
process_results(Result, State=#state{acc=Acc}) ->
    % Results are received as a one-off for each vnode in this case, and so 
    % once results are merged work is always done.
    {Result, RC0} = lists:keyfind(Result, 1, Acc),
    UpdAcc = lists:keyreplace(Result, 1, Acc, {Result, RC0 + 1}),
    {done, State#state{acc = UpdAcc}}.

%% Once the coverage FSM has received done for all vnodes (as an output from
%% process_results), then it will call finish(clean, State) and so the results
%% can be sent to the client, and the FSM can be stopped. 
finish({error, Error}, State=#state{from={raw, ReqId, ClientPid}}) ->
    % Notify the requesting client that an error
    % occurred or the timeout has elapsed.
    lager:warning("Failure to finish process fold due to ~w", [Error]),
    ClientPid ! {ReqId, {error, Error}},
    {stop, normal, State};
finish(clean, State=#state{from={raw, ReqId, ClientPid}, acc=Acc}) ->
    % The client doesn't expect results in increments only the final result, 
    % so no need for a seperate send of a 'done' message
    BackupDuration = timer:now_diff(os:timestamp(), State#state.start_time),
    {_NS, BC0} = lists:keyfind(not_supported, 1, Acc),
    {_BR, BC1} = lists:keyfind(bad_request, 1, Acc),
    lager:info("Finished backup with result=~w and duration=~w seconds", 
                [Acc, BackupDuration/1000000]),
    Result = BC0 + BC1 == 0,
    ClientPid ! {ReqId, {results, Result}},
    {stop, normal, State}.


%% ===================================================================
%% External functions
%% ===================================================================

-spec complete() -> complete.
complete() -> complete.

-spec not_supported() -> not_supported.
not_supported() -> not_supported.

-spec bad_request() -> bad_request.
bad_request() -> bad_request.


%% ===================================================================
%% Internal functions
%% ===================================================================

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).


-endif.

