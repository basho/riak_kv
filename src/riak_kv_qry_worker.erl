%%%-------------------------------------------------------------------
%%%
%%% riak_kv_qry_worker: Riak SQL per-query workers
%%%
%%% Copyright (C) 2015 Basho Technologies, Inc. All rights reserved
%%%
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
%%%
%%%-------------------------------------------------------------------

%% @doc Under the queue manager accepting raw parsed and lexed queries
%%      from the user, workers take individual queries and communicate
%%      with eleveldb backend to execute the queries (with
%%      sub-queries), and hold the results until fetched back to the
%%      user.

-module(riak_kv_qry_worker).

-behaviour(gen_server).

%% OTP API
-export([start_link/1]).

%% gen_server callbacks
-export([
         init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3
        ]).

-include_lib("riak_ql/include/riak_ql_ddl.hrl").

-define(NO_SIDEEFFECTS, []).
-define(NO_MAX_RESULTS, no_max_results).
-define(NO_PG_SORT, undefined).

-record(state, {
          name                                :: atom(),
          qry           = none                :: none | ?SQL_SELECT{},
          qid           = undefined           :: undefined | {node(), non_neg_integer()},
          sub_qrys      = []                  :: [integer()],
          status        = void                :: void | accumulating_chunks,
          receiver_pid                        :: pid(),
          result        = []                  :: [{non_neg_integer(), list()}] | [{binary(), term()}],
          run_sub_qs_fn = fun run_sub_qs_fn/1 :: fun()
         }).

%%%===================================================================
%%% OTP API
%%%===================================================================
-spec start_link(RegisteredName::atom()) -> {ok, pid()} | ignore | {error, term()}.
start_link(RegisteredName) ->
    gen_server:start_link({local, RegisteredName}, ?MODULE, [RegisteredName], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

-spec init([RegisteredName::atom()]) -> {ok, #state{}}.
%% @private
init([RegisteredName]) ->
    pop_next_query(),
    {ok, new_state(RegisteredName)}.

handle_call(_, _, State) ->
    {reply, {error, not_handled}, State}.

-spec handle_cast(term(), #state{}) -> {noreply, #state{}}.
%% @private
handle_cast(Msg, State) ->
    lager:info("Not handling cast message ~p", [Msg]),
    {noreply, State}.

%% @private
-spec handle_info(term(), #state{}) -> {noreply, #state{}}.
handle_info(pop_next_query, State1) ->
    QueryWork = riak_kv_qry_queue:blocking_pop(),
    {ok, State2} = execute_query(QueryWork, State1),
    {noreply, State2};
handle_info({{_, QId}, done}, #state{ qid = QId } = State) ->
    {noreply, subqueries_done(QId, State)};
handle_info({{SubQId, QId}, {results, Chunk}}, #state{ qid = QId } = State) ->
    {noreply, add_subquery_result(SubQId, Chunk, State)};
handle_info({{SubQId, QId}, {error, Reason} = Error},
            State = #state{receiver_pid = ReceiverPid,
                           qid    = QId,
                           result = IndexedChunks}) ->
    lager:warning("Error ~p while collecting on QId ~p (~p);"
                  " dropping ~b chunks of data accumulated so far",
                  [Reason, QId, SubQId, length(IndexedChunks)]),
    ReceiverPid ! Error,
    pop_next_query(),
    {noreply, new_state(State#state.name)};

handle_info({{_SubQId, QId1}, _}, State = #state{qid = QId2}) when QId1 =/= QId2 ->
    %% catches late results or errors such getting results for invalid QIds.
    lager:debug("Bad query id ~p (expected ~p)", [QId1, QId2]),
    {noreply, State}.

-spec terminate(term(), #state{}) -> term().
%% @private
terminate(_Reason, _State) ->
    ok.

-spec code_change(term() | {down, term()}, #state{}, term()) -> {ok, #state{}}.
%% @private
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec new_state(RegisteredName::atom()) -> #state{}.
new_state(RegisteredName) ->
    #state{name = RegisteredName}.

run_sub_qs_fn([]) ->
    ok;
run_sub_qs_fn([{{qry, Q}, {qid, QId}} | T]) ->
    Table = Q?SQL_SELECT.'FROM',
    Bucket = riak_kv_pb_timeseries:table_to_bucket(Table),
    %% fix these up too
    Timeout = {timeout, 10000},
    Me = self(),
    CoverageFn = {colocated, riak_kv_qry_coverage_plan},
    Opts = [Bucket, none, Q, Timeout, all, undefined, CoverageFn],
    {ok, _PID} = riak_kv_index_fsm_sup:start_index_fsm(node(), [{raw, QId, Me}, Opts]),
    run_sub_qs_fn(T).

decode_results(KVList) ->
    [extract_riak_object(V) || {_, V} <- KVList].

extract_riak_object(V) when is_binary(V) ->
    %% don't care about bkey
    RObj = riak_object:from_binary(<<>>, <<>>, V),
    case riak_object:get_value(RObj) of
        <<>> ->
            %% record was deleted
            [];
        FullRecord ->
            [CellValue || {_, CellValue} <- FullRecord]
    end.

%% Send a message to this process to get the next query.
pop_next_query() ->
    self() ! pop_next_query.

%%
execute_query({query, ReceiverPid, QId, [Qry|_] = SubQueries, _},
              #state{ run_sub_qs_fn = RunSubQs } = State) ->
    Indices = lists:seq(1, length(SubQueries)),
    ZQueries = lists:zip(Indices, SubQueries),
    %% all subqueries have the same select clause
    ?SQL_SELECT{'SELECT' = Sel} = Qry,
    #riak_sel_clause_v1{initial_state = InitialState} = Sel,
    SubQs = [{{qry, Q}, {qid, {I, QId}}} || {I, Q} <- ZQueries],
    ok = RunSubQs(SubQs),
    {ok, State#state{qid          = QId,
                     receiver_pid = ReceiverPid,
                     qry          = Qry,
                     sub_qrys     = Indices,
                     result       = InitialState }}.

%%
add_subquery_result(SubQId, Chunk,
                    #state{qry      = Qry,
                           result   = QueryResult1,
                           sub_qrys = SubQs} = State) ->
    ?SQL_SELECT{'SELECT' = Sel} = Qry,
    #riak_sel_clause_v1{calc_type  = CalcType,
                        clause     = SelClause} = Sel,
    case lists:member(SubQId, SubQs) of
        true ->
            DecodedChunk = decode_results(lists:flatten(Chunk)),
            case CalcType of
                rows ->
                    IndexedChunks = [riak_kv_qry_compiler:run_select(SelClause, Row) 
                                     || Row <- DecodedChunk],
                    QueryResult2 = [{SubQId, IndexedChunks} | QueryResult1];
                aggregate ->
                    QueryResult2 = 
                      lists:foldl(
                          fun(E, Acc) ->
                              riak_kv_qry_compiler:run_select(SelClause, E, Acc)
                          end, QueryResult1, DecodedChunk)
            end,
            NSubQ = lists:delete(SubQId, SubQs),
            State#state{status   = accumulating_chunks,
                        result   = QueryResult2,
                        sub_qrys = NSubQ};
        false ->
            %% discard; Don't touch state as it may have already 'finished'.
            State
    end.

subqueries_done(QId,
                #state{qid          = QId,
                       receiver_pid = ReceiverPid,
                       result       = QueryResult1,
                       sub_qrys     = SubQQ,
                       qry          = ?SQL_SELECT{'SELECT' = Sel }} = State) ->
    case SubQQ of
        [] ->
            QueryResult2 = prepare_final_results(Sel, QueryResult1),
            %   send the results to the waiting client process
            ReceiverPid ! {ok, QueryResult2},
            pop_next_query(),
            % clean the state of query specfic data, ready for the next one
            new_state(State#state.name);
        _ ->
            % more sub queries are left to run
            State
    end.

-spec prepare_final_results(#riak_sel_clause_v1{}, [{non_neg_integer(), list()}]) ->
                                   {[riak_pb_ts_codec:tscolumnname()],
                                    [riak_pb_ts_codec:tscolumntype()],
                                    [[riak_pb_ts_codec:ldbvalue()]]}.
prepare_final_results(#riak_sel_clause_v1{calc_type = rows} = Select,
                      IndexedChunks) ->
    %% sort by index, to reassemble according to coverage plan
    {_, [R2]} = lists:unzip(lists:sort(IndexedChunks)),
    prepare_final_results2(Select, R2);
prepare_final_results(#riak_sel_clause_v1{ calc_type = aggregate,
                                           finalisers = FinaliserFns } = Select,
                      Aggregate1) ->
    Aggregate2 = finalise_aggregate(Aggregate1, FinaliserFns, []),
    prepare_final_results2(Select, [Aggregate2]).

%%
prepare_final_results2(#riak_sel_clause_v1{ col_return_types = ColTypes,
                                            col_names = ColNames}, Rows) ->
    {ColNames, ColTypes, Rows}.

%%
finalise_aggregate([], _, Acc) ->
    lists:reverse(Acc);
finalise_aggregate([Cell | Row], [CellFn | Fns], Acc) ->
    FinalisedCell = CellFn(Cell),
    finalise_aggregate(Row, Fns, [FinalisedCell | Acc]).

%%%===================================================================
%%% Unit tests
%%%===================================================================

-ifdef(TEST).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

prepare_final_results_test() ->
    Rows = [[12, <<"windy">>], [13, <<"windy">>]],
    IndexedChunks = [{1, Rows}],
    ?assertEqual(
        {[<<"a">>, <<"b">>], [sint64, varchar], Rows},
        prepare_final_results(
            #riak_sel_clause_v1{
                col_names = [<<"a">>, <<"b">>],
                col_return_types = [sint64, varchar],
                calc_type = rows }, IndexedChunks)
    ).

-endif.
