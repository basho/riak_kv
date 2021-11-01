%% -------------------------------------------------------------------
%%
%% riak_index_fsm: Manage secondary index queries.
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc The index fsm manages the execution of secondary index queries.
%%
%%      The index fsm creates a plan to achieve coverage
%%      of the cluster using the minimum
%%      possible number of VNodes, sends index query
%%      commands to each of those VNodes, and compiles the
%%      responses.
%%
%%      The number of VNodes required for full
%%      coverage is based on the number
%%      of partitions, the number of available physical
%%      nodes, and the bucket n_val.

-module(riak_kv_index_fsm).

-behaviour(riak_core_coverage_fsm).

-include_lib("riak_kv_vnode.hrl").

-define(SLOW_TIME, application:get_env(riak_kv, index_fsm_slow_timems, 200)).
-define(FAST_TIME, application:get_env(riak_kv, index_fsm_fast_timems, 10)).

-export([init/2,
         plan/2,
         process_results/3,
         process_results/2,
         finish/2]).
-export([use_ack_backpressure/0,
         req/3]).

-type from() :: {atom(), req_id(), pid()}.
-type req_id() :: non_neg_integer().

-ifdef(namespaced_types).
-type riak_kv_index_fsm_dict() :: dict:dict().
-else.
-type riak_kv_index_fsm_dict() :: dict().
-endif.

-record(timings, 
            {start_time = os:timestamp() :: os:timestamp(),
                max = 0 :: non_neg_integer(),
                min = infinity :: non_neg_integer()|infinity,
                count = 0 :: non_neg_integer(),
                sum = 0 :: non_neg_integer(),
                slow_count = 0 :: non_neg_integer(),
                fast_count = 0 :: non_neg_integer(),
                slow_time = ?SLOW_TIME,
                fast_time = ?FAST_TIME}).
-type timings() :: #timings{}.


-record(state, {from :: from(),
                pagination_sort :: boolean(),
                merge_sort_buffer = undefined :: sms:sms() | undefined,
                max_results :: all | pos_integer(),
                results_per_vnode = dict:new() :: riak_kv_index_fsm_dict(),
                timings = #timings{} :: timings(),
                bucket :: riak_object:buckey() | undefined,
                results_sent = 0 :: non_neg_integer()}).


%% @doc Returns `true' if the new ack-based backpressure index
%% protocol should be used.  This decision is based on the
%% `index_backpressure' setting in `riak_kv''s application
%% environment.
-spec use_ack_backpressure() -> boolean().
use_ack_backpressure() ->
    riak_core_capability:get({riak_kv, index_backpressure}, false) == true.

%% @doc Construct the correct index command record.
-spec req(binary(), term(), term()) -> term().
req(Bucket, ItemFilter, Query) ->
    riak_kv_requests:new_index_request(Bucket, ItemFilter, Query, use_ack_backpressure()).

%% @doc Return a tuple containing the ModFun to call per vnode,
%% the number of primary preflist vnodes the operation
%% should cover, the service to use to check for available nodes,
%% and the registered name to use to access the vnode master process.
init(From={_, _, _}, [Bucket, ItemFilter, Query, Timeout]) ->
    %% http://erlang.org/doc/reference_manual/expressions.html#id77404
    %% atom() > number()
    init(From, [Bucket, ItemFilter, Query, Timeout, all]);
init(From={_, _, _}, [Bucket, ItemFilter, Query, Timeout, MaxResults]) ->
    init(From, [Bucket, ItemFilter, Query, Timeout, MaxResults, undefined]);
init(From={_, _, _}, [Bucket, ItemFilter, Query, Timeout, MaxResults, PgSort0]) ->
    %% Get the bucket n_val for use in creating a coverage plan
    BucketProps = riak_core_bucket:get_bucket(Bucket),
    NVal = proplists:get_value(n_val, BucketProps),
    Paginating = is_integer(MaxResults) andalso MaxResults > 0, 
    PgSort = case {Paginating, PgSort0} of
        {true, _} ->
            true;
        {_, undefined} ->
            app_helper:get_env(riak_kv, secondary_index_sort_default, false);
        _ ->
            PgSort0
    end,
    %% Construct the key listing request
    Req = req(Bucket, ItemFilter, Query),
    {Req, all, NVal, 1, riak_kv, riak_kv_vnode_master, Timeout,
     #state{from=From,
            max_results=MaxResults,
            pagination_sort=PgSort,
            bucket=Bucket}}.

plan(CoverageVNodes, State = #state{pagination_sort=true}) ->
    {ok, State#state{merge_sort_buffer=sms:new(CoverageVNodes)}};
plan(_CoverageVNodes, State) ->
    {ok, State}.

process_results(_VNode, {error, Reason}, _State) ->
    {error, Reason};
process_results(_VNode, {From, _Bucket, _Results}, State=#state{max_results=X, results_sent=Y})  when Y >= X ->
    riak_kv_vnode:stop_fold(From),
    {done, State};
process_results(VNode, {From, Bucket, Results}, State) ->
    case process_results(VNode, {Bucket, Results}, State) of
        {ok, State2 = #state{pagination_sort=true}} ->
            #state{results_per_vnode=PerNode, max_results=MaxResults} = State2,
            VNodeCount = dict:fetch(VNode, PerNode),
            case VNodeCount < MaxResults of
                true ->
                    _ = riak_kv_vnode:ack_keys(From),
                    {ok, State2};
                false ->
                    riak_kv_vnode:stop_fold(From),
                    {done, State2}
            end;
        {ok, State2} ->
            _ = riak_kv_vnode:ack_keys(From),
            {ok, State2};
        {done, State2} ->
            riak_kv_vnode:stop_fold(From),
            {done, State2}
    end;
process_results(VNode, {_Bucket, Results}, State = #state{pagination_sort=true}) ->
    #state{merge_sort_buffer=MergeSortBuffer, results_per_vnode=PerNode,
           from={raw, ReqId, ClientPid}, results_sent=ResultsSent, max_results=MaxResults} = State,
    %% add new results to buffer
    {ToSend, NewBuff} = update_buffer(VNode, Results, MergeSortBuffer),
    NumResults = length(Results),
    NewPerNode = dict:update(VNode, fun(C) -> C + NumResults end, NumResults, PerNode),
    LenToSend = length(ToSend),
    {Response, ResultsLen, ResultsToSend} = get_results_to_send(LenToSend, ToSend, ResultsSent, MaxResults),
    send_results(ClientPid, ReqId, ResultsToSend),
    {Response, State#state{merge_sort_buffer=NewBuff,
                           results_per_vnode=NewPerNode,
                           results_sent=ResultsSent+ResultsLen}};
process_results(VNode, done, State = #state{pagination_sort=true}) ->
    %% tell the sms buffer about the done vnode
    #state{merge_sort_buffer=MergeSortBuffer} = State,
    BufferWithNewResults = sms:add_results(VNode, done, MergeSortBuffer),
    UpdTimings = update_timings(State#state.timings),
    {done,
        State#state{merge_sort_buffer=BufferWithNewResults,
                    timings=UpdTimings}};
process_results(_VNode, {_Bucket, Results}, State) ->
    #state{from={raw, ReqId, ClientPid}} = State,
    send_results(ClientPid, ReqId, Results),
    ResultsSent = length(Results) + State#state.results_sent,
    UpdTimings = update_timings(State#state.timings),
    {ok, State#state{timings = UpdTimings, results_sent = ResultsSent}};
process_results(_VNode, done, State) ->
    {done, State}.

%% @private Update the buffer with results and process it
update_buffer(VNode, Results, Buffer) ->
    BufferWithNewResults = sms:add_results(VNode, lists:reverse(Results), Buffer),
    sms:sms(BufferWithNewResults).

%% @private Get the subset of `ToSend' that we can send without violating `MaxResults'
get_results_to_send(LenToSend, ToSend, ResultsSent, MaxResults) when (ResultsSent + LenToSend) >= MaxResults ->
    ResultsLen = MaxResults - ResultsSent,
    ResultsToSend = lists:sublist(ToSend, ResultsLen),
    {done, ResultsLen, ResultsToSend};
get_results_to_send(LenToSend, ToSend, _, _) ->
    {ok, LenToSend, ToSend}.

%% @private send results, but only if there are some
send_results(_ClientPid, _ReqId, []) ->
    ok;
send_results(ClientPid, ReqId, ResultsToSend) ->
    ClientPid ! {ReqId, {results, ResultsToSend}}.


%% Legacy, unsorted 2i, should remove?
process_results({error, Reason}, _State) ->
    {error, Reason};
process_results({From, Bucket, Results},
                StateData=#state{from={raw, ReqId, ClientPid}}) ->
    process_query_results(Bucket, Results, ReqId, ClientPid),
    _ = riak_kv_vnode:ack_keys(From), % tell that vnode we're ready for more
    {ok, StateData};
process_results({Bucket, Results},
                StateData=#state{from={raw, ReqId, ClientPid}}) ->
    process_query_results(Bucket, Results, ReqId, ClientPid),
    {ok, StateData};
process_results(done, StateData) ->
    {done, StateData}.

finish({error, Error}, State=#state{from={raw, ReqId, ClientPid}}) ->
    %% Notify the requesting client that an error
    %% occurred or the timeout has elapsed.
    ClientPid ! {ReqId, {error, Error}},
    {stop, normal, State};
finish(clean,
       State=#state{from={raw, ReqId, ClientPid},
                    merge_sort_buffer=undefined}) ->
    ClientPid ! {ReqId, done},
    log_timings(State#state.timings,
                State#state.bucket,
                State#state.results_sent),
    {stop, normal, State};
finish(clean,
       State=#state{from={raw, ReqId, ClientPid},
                    merge_sort_buffer=MergeSortBuffer,
                    results_sent=ResultsSent,
                    max_results=MaxResults}) ->
    LastResults = sms:done(MergeSortBuffer),
    TotalResults = length(LastResults) + ResultsSent,
    DownTheWire =
        case TotalResults > MaxResults of
            true ->
                lists:sublist(LastResults, MaxResults - ResultsSent);
            false ->
                LastResults
        end,
    ClientPid ! {ReqId, {results, DownTheWire}},
    ClientPid ! {ReqId, done},
    log_timings(State#state.timings,
                State#state.bucket,
                min(MaxResults, TotalResults)),
    {stop, normal, State}.

%% ===================================================================
%% Internal functions
%% ===================================================================

process_query_results(_Bucket, Results, ReqId, ClientPid) ->
    ClientPid ! {ReqId, {results, Results}}.

-spec update_timings(timings()) -> timings().
update_timings(Timings) ->
    MS = timer:now_diff(os:timestamp(), Timings#timings.start_time) div 1000,
    SlowCount =
        case MS > Timings#timings.slow_time of
            true ->
                Timings#timings.slow_count + 1;
            false ->
                Timings#timings.slow_count
        end,
    FastCount = 
        case MS < Timings#timings.fast_time of
            true ->
                Timings#timings.fast_count + 1;
            false ->
                Timings#timings.fast_count
        end,
    Timings#timings{
        max = max(Timings#timings.max, MS),
        min = min(Timings#timings.min, MS),
        count = Timings#timings.count + 1,
        sum = Timings#timings.sum + MS,
        slow_count = SlowCount,
        fast_count = FastCount 
    }.

-spec log_timings(timings(), riak_object:bucket(), non_neg_integer()) -> ok.
log_timings(Timings, Bucket, ResultCount) ->
    Duration = timer:now_diff(os:timestamp(), Timings#timings.start_time),
    ok = riak_kv_stat:update({index_fsm_time, Duration, ResultCount}),
    log_timings(Timings,
                Bucket,
                ResultCount,
                application:get_env(riak_kv, log_index_fsm, false)).

log_timings(_Timings, _Bucket, _ResultCount, false) ->
    ok;
log_timings(Timings, Bucket, ResultCount, true) ->
    lager:info("Index query on bucket=~p " ++
                "max_vnodeq=~w min_vnodeq=~w sum_vnodeq=~w count_vnodeq=~w " ++
                "slow_count_vnodeq=~w fast_count_vnodeq=~w result_count=~w",
                [Bucket,
                    Timings#timings.max, Timings#timings.min,
                    Timings#timings.sum, Timings#timings.count,
                    Timings#timings.slow_count, Timings#timings.fast_count,
                    ResultCount]).