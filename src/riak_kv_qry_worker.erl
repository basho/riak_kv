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

-ifdef(TEST).
-export([
         runner_TEST/1
        ]).
-endif.

-include_lib("riak_ql/include/riak_ql_ddl.hrl").

-define(NO_SIDEEFFECTS, []).
-define(NO_MAX_RESULTS, no_max_results).
-define(NO_PG_SORT, undefined).

-record(state, {
          name                 :: atom(),
          ddl                  :: undefined | #ddl_v1{},
          qry      = none      :: none | #riak_sql_v1{},
          qid      = undefined :: undefined | {node(), non_neg_integer()},
          sub_qrys  = []       :: [integer()],
          status    = void     :: void | accumulating_chunks,
          receiver_pid         :: pid(),
          result    = []       :: [{non_neg_integer(), list()}] | [{binary(), term()}]
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

-spec handle_call(term(), {pid(), term()}, #state{}) ->
                         {reply, Reply::ok | {error, atom()} | list(), #state{}}.
%% @private
handle_call(Request, _, State) ->
    case handle_req(Request, State) of
        {{error, _} = Error, _SEs, NewState} ->
            {reply, Error, NewState};
        {Reply, SEs, NewState} ->
            ok = handle_side_effects(SEs),
            {reply, Reply, NewState}
    end.


-spec handle_cast(term(), #state{}) -> {noreply, #state{}}.
%% @private
handle_cast(Msg, State) ->
    lager:info("Not handling cast message ~p", [Msg]),
    {noreply, State}.

%% @private
-spec handle_info(term(), #state{}) -> {noreply, #state{}}.
handle_info(pop_next_query, State1) ->
    {query, ReceivePid, QId, Qry, DDL} = riak_kv_qry_queue:blocking_pop(),
    Request = {execute, {QId, Qry, DDL}},
    case handle_req(Request, State1) of
        {{error, _} = Error, _, State2} ->
            ReceivePid ! Error,
            {noreply, new_state(State2#state.name)};
        {ok, SEs, State2} ->
            ok = handle_side_effects(SEs),
            {noreply, State2#state{ receiver_pid = ReceivePid }}
    end;

handle_info({{SubQId, QId}, done},
            State = #state{qid       = QId,
                           receiver_pid = ReceiverPid,
                           result    = IndexedChunks,
                           sub_qrys  = SubQQ}) ->
    lager:debug("Received done on QId ~p (~p); SubQQ: ~p", [QId, SubQId, SubQQ]),
    case SubQQ of
        [] ->
            lager:debug("Done collecting on QId ~p (~p): ~p", [QId, SubQId, IndexedChunks]),
            %% sort by index, to reassemble according to coverage plan
            {_, R2} = lists:unzip(lists:sort(IndexedChunks)),
            Results = lists:append(R2),
            % send the results to the waiting client process
            ReceiverPid ! {ok, Results},
            pop_next_query(),
            %% drop indexes, serialize
            {noreply, new_state(State#state.name)};
        _MoreSubQueriesNotDone ->
            {noreply, State}
    end;

handle_info({{SubQId, QId}, {results, Chunk}},
            State = #state{qid      = QId,
                           qry      = Qry,
                           result   = IndexedChunks,
                           sub_qrys = SubQs}) ->
    #riak_sql_v1{'SELECT' = SelectSpec} = Qry,
    NewS = case lists:member(SubQId, SubQs) of
               true ->
                   Decoded = decode_results(lists:flatten(Chunk), SelectSpec),
                   lager:debug("Got chunk on QId ~p (~p); SubQQ: ~p", [QId, SubQId, SubQs]),
                   NSubQ = lists:delete(SubQId, SubQs),
                   State#state{status   = accumulating_chunks,
                               result   = [{SubQId, Decoded} | IndexedChunks],
                               sub_qrys = NSubQ};
               false ->
                   %% discard;
                   %% Don't touch state as it may have already 'finished'.
                   State
           end,
    {noreply, NewS};

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

-spec handle_req({atom(), term()}, #state{}) ->
                        {ok | {ok | error, term()},
                         list(), #state{}}.
handle_req({execute, {QId, [Qry|_] = SubQueries, DDL}}, State = #state{status = void}) ->
    %% TODO make this run with multiple sub-queries
    %% limit sub-queries and throw error
    Indices = lists:seq(1, length(SubQueries)),
    ZQueries = lists:zip(Indices, SubQueries),
    SEs = [{run_sub_query, {qry, Q}, {qid, {I, QId}}} || {I, Q} <- ZQueries],
    {ok, SEs, State#state{qid = QId, qry = Qry, ddl = DDL, sub_qrys = Indices}};

handle_req({execute, {QId, _, _}}, State = #state{status = Status})
  when Status =/= void ->
    lager:error("Qry queue manager should have cleared the status before assigning new query ~p", [QId]),
    {{error, mismanagement}, [], State};

handle_req(_Request, State) ->
    {ok, ?NO_SIDEEFFECTS, State}.

handle_side_effects([]) ->
    ok;
handle_side_effects([{run_sub_query, {qry, Q}, {qid, QId}} | T]) ->
    Table = Q#riak_sql_v1.'FROM',
    Bucket = riak_kv_pb_timeseries:table_to_bucket(Table),
    %% fix these up too
    Timeout = {timeout, 10000},
    Me = self(),
    CoverageFn = {colocated, riak_kv_qry_coverage_plan},
    {ok, _PID} = riak_kv_index_fsm_sup:start_index_fsm(node(), [{raw, QId, Me}, [Bucket, none, Q, Timeout, all, undefined, CoverageFn]]),
    handle_side_effects(T);
handle_side_effects([H | T]) ->
    lager:warning("in riak_kv_qry:handle_side_effects not handling ~p", [H]),
    handle_side_effects(T).

decode_results(KVList, SelectSpec) ->
    lists:append(
      [extract_riak_object(SelectSpec, V) || {_, V} <- KVList]).

extract_riak_object({plain_row_select, SelectSpec}, V) when is_binary(V) ->
    % don't care about bkey
    RObj = riak_object:from_binary(<<>>, <<>>, V),
    case riak_object:get_value(RObj) of
        <<>> ->
            %% record was deleted
            [];
        FullRecord ->
            filter_columns(lists:flatten(SelectSpec), FullRecord)
    end.

%% Pull out the values we're interested in based on the select,
%% statement, e.g. select user, geoloc returns only user and geoloc columns.
-spec filter_columns(SelectSpec::[binary()],
                     ColValues::[{Field::binary(), Value::binary()}]) ->
        ColValues2::[{Field::binary(), Value::binary()}].
filter_columns([<<"*">>], ColValues) ->
    ColValues;
filter_columns(SelectSpec, ColValues) ->
    [Col || {Field, _} = Col <- ColValues, lists:member(Field, SelectSpec)].

%% Send a message to this process to get the next query.
pop_next_query() ->
    self() ! pop_next_query.

%%%===================================================================
%%% Unit tests
%%%===================================================================
-ifdef(TEST).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

%%
%% Test runner
%%

-define(NO_OUTPUTS, []).

runner_TEST(Tests) -> io:format("running tests ~p~n", [Tests]),
                      test_r2(Tests, #state{}, 1, [], [], []).

test_r2([], _State, _LineNo, SideEffects, Replies, Errs) ->
    {lists:reverse(SideEffects), lists:reverse(Replies), lists:reverse(Errs)};
test_r2([H | T], State, LineNo, SideEffects, Replies, Errs) ->
    io:format("Before ~p~n- SideEffects is ~p~n- Replies is ~p~n- Errors is ~p~n", [H, SideEffects, Replies, Errs]),
    {NewSt, NewSEs, NewRs, NewE} = run(H, State, LineNo, SideEffects, Replies, Errs),
    io:format("After ~p~n- NewSEs is ~p~n- NewRs is ~p~n- NewE is ~p~n", [H, NewSEs, NewRs, NewE]),
    test_r2(T, NewSt, LineNo + 1, NewSEs, NewRs, NewE).

run({run, {init, Arg}}, _State, _LNo, SideEffects, Replies, Errs) ->
    {ok, NewState} = init(Arg),
    {NewState, SideEffects, Replies, Errs};
run({clear, side_effects}, State, _LNo, _SideEffects, Replies, Errs) ->
    {State, [], Replies, Errs};
run({clear, replies}, State, _LNo, SideEffects, _Replies, Errs) ->
    {State, SideEffects, [], Errs};
run({dump, state}, State, LNo, SideEffects, Replies, Errs) ->
    io:format("On Line No ~p State is~n- ~p~n", [LNo, State]),
    {State, SideEffects, Replies, Errs};
run({dump, Replies}, State, LNo, SideEffects, Replies, Errs) ->
    io:format("On Line No ~p Replies is~n- ~p~n", [LNo, Replies]),
    {State, SideEffects, Replies, Errs};
run({dump, errors}, State, LNo, SideEffects, Replies, Errs) ->
    io:format("On Line No ~p Errs is~n- ~p~n", [LNo, Errs]),
    {State, SideEffects, Replies, Errs};
run({msg, Msg}, State, _LNo, SideEffects, Replies, Errs) ->
    {Reply, SEs, NewState} = handle_req(Msg, State),
    NewSEs = case SEs of
                 [] -> SideEffects;
                 _  -> lists:flatten(SEs, SideEffects)
             end,
    {NewState, NewSEs, [Reply | Replies], Errs};
run({side_effect, G}, State, LNo, SEs, Replies, Errs) ->
    {NewSE, NewErrs}
        = case SEs of
              []      -> Err = {error, {line_no, LNo}, {side_effect, {expected, []}, {got, G}}},
                         {[], [Err | Errs]};
              [G | T] -> {T, Errs};
              [E | T] -> Err = {error, {line_no, LNo}, {side_effect, {expected, E}, {got, G}}},
                         {T, [Err | Errs]}
          end,
    {State, NewSE, Replies, NewErrs};
run({reply, G}, State, LNo, SideEffects, Replies, Errs) ->
    {NewRs, NewErrs}
        = case Replies of
              []      -> Err = {error, {line_no, LNo}, {reply, {expected, []}, {got, G}}},
                         {[], [Err | Errs]};
              [G | T] -> {T, Errs};
              [E | T] -> Err = {error, {line_no, LNo}, {reply, {expected, E}, {got, G}}},
                         {T, [Err | Errs]}
          end,
    {State, SideEffects, NewRs, NewErrs};
run(H, State, LNo, SideEffects, Replies, Errs) ->
    Err = {error, {line_no, LNo}, {unknown_test_state, H}},
    {State, SideEffects, Replies, [Err | Errs]}.

-define(MAX_Q_LEN, 5).

simple_init_test() ->
    Tests = [
             {run, {init, [fms1]}}
           ],
    Results = runner_TEST(Tests),
    ?assertEqual({[], [], []}, Results).

-endif.
