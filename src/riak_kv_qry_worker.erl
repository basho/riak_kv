%%%-------------------------------------------------------------------
%%%
%%% riak_kv_qry_worker: Riak SQL per-query workers
%%%
%%% Copyright (C) 2016 Basho Technologies, Inc. All rights reserved
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

-include("riak_kv_ts.hrl").

-define(NO_SIDEEFFECTS, []).
-define(NO_MAX_RESULTS, no_max_results).
-define(NO_PG_SORT, undefined).

%% accumulators for different types of query results.
-type rows_acc()      :: [{non_neg_integer(), list()}].
-type aggregate_acc() :: [{binary(), term()}].
-type group_by_acc()  :: {group_by, InitialState::term(), Groups::dict()}.

-record(state, {
          qry           = none                :: none | ?SQL_SELECT{},
          qid           = undefined           :: undefined | {node(), non_neg_integer()},
          sub_qrys      = []                  :: [integer()],
          receiver_pid                        :: pid(),
          result        = []                  :: rows_acc() | aggregate_acc() | group_by_acc(),
          run_sub_qs_fn = fun run_sub_qs_fn/1 :: fun()
         }).

%%%===================================================================
%%% OTP API
%%%===================================================================
-spec start_link(RegisteredName::atom()) -> {ok, pid()} | ignore | {error, term()}.
start_link(RegisteredName) ->
    gen_server:start_link({local, RegisteredName}, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

-spec init([RegisteredName::atom()]) -> {ok, #state{}}.
%% @private
init([]) ->
    pop_next_query(),
    {ok, new_state()}.

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
            #state{receiver_pid = ReceiverPid,
                   qid    = QId,
                   result = IndexedChunks}) ->
    lager:warning("Error ~p while collecting on QId ~p (~p);"
                  " dropping ~b chunks of data accumulated so far",
                  [Reason, QId, SubQId, length(IndexedChunks)]),
    ReceiverPid ! Error,
    pop_next_query(),
    {noreply, new_state()};

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

-spec new_state() -> #state{}.
new_state() ->
    #state{ }.

run_sub_qs_fn([]) ->
    ok;
run_sub_qs_fn([{{qry, ?SQL_SELECT{cover_context = undefined} = Q1}, {qid, QId}} | T]) ->
    Table = Q1?SQL_SELECT.'FROM',
    Bucket = riak_kv_ts_util:table_to_bucket(Table),
    %% fix these up too
    Timeout = {timeout, 10000},
    Me = self(),
    KeyConvFn = make_key_conversion_fun(Table),
    Q2 = convert_query_to_cluster_version(Q1),
    Opts = [Bucket, none, Q2, Timeout, all, undefined, {Q2, Bucket}, riak_kv_qry_coverage_plan, KeyConvFn],
    {ok, _PID} = riak_kv_index_fsm_sup:start_index_fsm(node(), [{raw, QId, Me}, Opts]),
    run_sub_qs_fn(T);
%% if cover_context in the SQL record is *not* undefined, we've been
%% given a mini coverage plan to map to a single vnode/quantum
run_sub_qs_fn([{{qry, Q1}, {qid, QId}} | T]) ->
    Table = Q1?SQL_SELECT.'FROM',
    {ok, CoverProps} =
        riak_kv_pb_coverage:checksum_binary_to_term(Q1?SQL_SELECT.cover_context),
    CoverageFn = riak_client:vnode_target(CoverProps),
    Bucket = riak_kv_ts_util:table_to_bucket(Table),
    %% fix these up too
    Timeout = {timeout, 10000},
    Me = self(),
    KeyConvFn = make_key_conversion_fun(Table),
    Q2 = convert_query_to_cluster_version(Q1),
    Opts = [Bucket, none, Q2, Timeout, all, undefined, CoverageFn, riak_kv_qry_coverage_plan, KeyConvFn],
    {ok, _PID} = riak_kv_index_fsm_sup:start_index_fsm(node(), [{raw, QId, Me}, Opts]),
    run_sub_qs_fn(T).

make_key_conversion_fun(Table) ->
    Mod = riak_ql_ddl:make_module_name(Table),
    DDL = Mod:get_ddl(),
    fun(Key) when is_binary(Key) ->
            riak_kv_ts_util:lk_to_pk(sext:decode(Key), DDL, Mod);
       (Key) ->
            lager:error("Key conversion function "
                        "encountered a non-binary object key: ~p", [Key]),
            Key
    end.

%% Convert the sql select record to a version that is safe to pass around the
%% cluster. Do not treat the result as a record in the local node, just as a
%% tuple.
convert_query_to_cluster_version(Query) ->
    Version = riak_core_capability:get({riak_kv, sql_select_version}),
    riak_kv_select:convert(Version, Query).

decode_results([]) ->
    [];
decode_results([{_,V}|Tail]) when is_binary(V) ->
    RObj = riak_object:from_binary(<<>>, <<>>, V),
    case riak_object:get_value(RObj) of
        <<>> ->
            %% record was deleted
            decode_results(Tail);
        FullRecord ->
            Values = [CellValue || {_, CellValue} <- FullRecord],
            [Values | decode_results(Tail)]
    end.

%% Send a message to this process to get the next query.
pop_next_query() ->
    self() ! pop_next_query.

%%
execute_query({query, ReceiverPid, QId, [Qry1|_] = SubQueries, _},
              #state{ run_sub_qs_fn = RunSubQs } = State) ->
    Indices = lists:seq(1, length(SubQueries)),
    ZQueries = lists:zip(Indices, SubQueries),
    %% all subqueries have the same select clause
    ?SQL_SELECT{'SELECT' = Sel} = Qry1,
    #riak_sel_clause_v1{initial_state = InitialState} = Sel,
    %% The initial state from the query compiler is used as a template for each
    %% new group row. Group by is a special case because the select is
    %% a typical aggregate, but we need a dict to store it for each group.
    InitialResult =
        case sql_select_calc_type(Qry1) of
            group_by ->
                {group_by, InitialState, dict:new()};
            _ ->
                InitialState
        end,
    SubQs = [{{qry, Q}, {qid, {I, QId}}} || {I, Q} <- ZQueries],
    ok = RunSubQs(SubQs),
    {ok, State#state{qid          = QId,
                     receiver_pid = ReceiverPid,
                     qry          = Qry1,
                     sub_qrys     = Indices,
                     result       = InitialResult }}.

%%
add_subquery_result(SubQId, Chunk, #state{ sub_qrys = SubQs} = State) ->
    case lists:member(SubQId, SubQs) of
        true ->
            try
                QueryResult2 = run_select_on_chunk(SubQId, Chunk, State),
                NSubQ = lists:delete(SubQId, SubQs),
                State#state{result   = QueryResult2,
                            sub_qrys = NSubQ}
            catch
              error:divide_by_zero ->
                  cancel_error_query(divide_by_zero, State)
            end;
        false ->
            %% discard; Don't touch state as it may have already 'finished'.
            State
    end.

%%
run_select_on_chunk(SubQId, Chunk, #state{ qry = Query,
                                           result = QueryResult1 }) ->
    DecodedChunk = decode_results(lists:flatten(Chunk)),
    SelClause = sql_select_clause(Query),
    case sql_select_calc_type(Query) of
        rows ->
            run_select_on_rows_chunk(SubQId, SelClause, DecodedChunk, QueryResult1);
        aggregate ->
            run_select_on_aggregate_chunk(SelClause, DecodedChunk, QueryResult1);
        group_by ->
            run_select_on_group(Query, SelClause, DecodedChunk, QueryResult1)
    end.

%%
run_select_on_group(Query, SelClause, Chunk, QueryResult1) ->
    lists:foldl(
        fun(Row, Acc) ->
            run_select_on_group_row(Query, SelClause, Row, Acc)
        end, QueryResult1, Chunk).

%%
run_select_on_group_row(Query, SelClause, Row, QueryResult1) ->
    {group_by, InitialGroupState, Dict1} = QueryResult1,
    Key = select_group(Query, Row),
    Aggregate1 =
        case dict:find(Key, Dict1) of
            error ->
                InitialGroupState;
            {ok, AggregateX} ->
                AggregateX
        end,
    Aggregate2 = riak_kv_qry_compiler:run_select(SelClause, Row, Aggregate1),
    Dict2 = dict:store(Key, Aggregate2, Dict1),
    {group_by, InitialGroupState, Dict2}.

%%
select_group(Query, Row) ->
    GroupByFields = sql_select_group_by(Query),
    [lists:nth(N, Row) || {N,_} <- GroupByFields].

%% Run the selection clause on results that accumulate rows
run_select_on_rows_chunk(SubQId, SelClause, DecodedChunk, QueryResult1) ->
    IndexedChunks =
        [riak_kv_qry_compiler:run_select(SelClause, Row) || Row <- DecodedChunk],
    [{SubQId, IndexedChunks} | QueryResult1].

%%
run_select_on_aggregate_chunk(SelClause, DecodedChunk, QueryResult1) ->
    lists:foldl(
        fun(E, Acc) ->
            riak_kv_qry_compiler:run_select(SelClause, E, Acc)
        end, QueryResult1, DecodedChunk).

%%
-spec cancel_error_query(Error::any(), State1::#state{}) ->
        State2::#state{}.
cancel_error_query(Error, #state{ receiver_pid = ReceiverPid}) ->
    ReceiverPid ! {error, Error},
    pop_next_query(),
    new_state().

%%
subqueries_done(QId, #state{qid          = QId,
                            receiver_pid = ReceiverPid,
                            sub_qrys     = SubQQ} = State) ->
    case SubQQ of
        [] ->
            QueryResult2 = prepare_final_results(State),
            %   send the results to the waiting client process
            ReceiverPid ! {ok, QueryResult2},
            pop_next_query(),
            % clean the state of query specfic data, ready for the next one
            new_state();
        _ ->
            % more sub queries are left to run
            State
    end.

-spec prepare_final_results(#state{}) ->
                                   {[riak_pb_ts_codec:tscolumnname()],
                                    [riak_pb_ts_codec:tscolumntype()],
                                    [[riak_pb_ts_codec:ldbvalue()]]}.
prepare_final_results(#state{
        result = IndexedChunks,
        qry = ?SQL_SELECT{'SELECT' = #riak_sel_clause_v1{calc_type = rows} = Select }}) ->
    %% sort by index, to reassemble according to coverage plan
    {_, R2} = lists:unzip(lists:sort(IndexedChunks)),
    prepare_final_results2(Select, lists:append(R2));
prepare_final_results(#state{
        result = Aggregate1,
        qry = ?SQL_SELECT{'SELECT' = #riak_sel_clause_v1{calc_type = aggregate} = Select }} = State) ->
    try
        Aggregate2 = riak_kv_qry_compiler:finalise_aggregate(Select, Aggregate1),
        prepare_final_results2(Select, [Aggregate2])
    catch
        error:divide_by_zero ->
            cancel_error_query(divide_by_zero, State)
    end;
prepare_final_results(#state{
        result = {group_by, _, Dict},
        qry = ?SQL_SELECT{'SELECT' = #riak_sel_clause_v1{calc_type = group_by} = Select }} = State) ->
    try
        FinaliseFn =
            fun(_,V,Acc) ->
                [riak_kv_qry_compiler:finalise_aggregate(Select, V) | Acc]
            end,
        GroupedRows = dict:fold(FinaliseFn, [], Dict),
        prepare_final_results2(Select, GroupedRows)
    catch
        error:divide_by_zero ->
            cancel_error_query(divide_by_zero, State)
    end.

%%
prepare_final_results2(#riak_sel_clause_v1{col_return_types = ColTypes,
                                           col_names = ColNames}, Rows) ->
    {ColNames, ColTypes, Rows}.

%% Return the `calc_type' from a query.
sql_select_calc_type(?SQL_SELECT{'SELECT' = #riak_sel_clause_v1{calc_type = Type}}) ->
    Type.

%% Return the selection clause from a query
sql_select_clause(?SQL_SELECT{'SELECT' = #riak_sel_clause_v1{clause = Clause}}) ->
    Clause.

sql_select_group_by(?SQL_SELECT{ group_by = GroupBy }) ->
    GroupBy.

%%%===================================================================
%%% Unit tests
%%%===================================================================

-ifdef(TEST).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

prepare_final_results_test() ->
    Rows = [[12, <<"windy">>], [13, <<"windy">>]],
    ?assertEqual(
        {[<<"a">>, <<"b">>], [sint64, varchar], Rows},
        prepare_final_results(
            #state{
                qry =
                    ?SQL_SELECT{
                        'SELECT' = #riak_sel_clause_v1{
                            col_names = [<<"a">>, <<"b">>],
                            col_return_types = [sint64, varchar],
                            calc_type = rows
                         }
                    },
                result = [{1, Rows}]})
    ).

-endif.
