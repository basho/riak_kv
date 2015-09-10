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

%% Developer API
-export([
         execute/2,
         fetch/2
        ]).

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

-include("riak_kv_qry_queue.hrl").
%% TODO sort out inclusion policy
%%-include_lib("riak_ql/include/riak_ql_sql.hrl").
-include_lib("riak_ql/include/riak_ql_ddl.hrl").

-define(SERVER, ?MODULE).
-define(NO_SIDEEFFECTS, []).
-define(NO_MAX_RESULTS, no_max_results).
-define(NO_PG_SORT, undefined).

-record(state, {
          name       :: qry_fsm_name(),
          ddl        :: undefined | #ddl_v1{},
          qry = none :: none | #riak_sql_v1{},
          qid = undefined :: undefined | {node(), non_neg_integer()},
          status = void   :: void | accumulating | complete,
          result = []     :: [binary()]
         }).

%%%===================================================================
%%% OTP API
%%%===================================================================

-spec start_link(qry_fsm_name()) -> {ok, pid()} | ignore | {error, term()}.
start_link(Name) ->
    gen_server:start_link({local, Name}, ?MODULE, [Name], []).


%%%===================================================================
%%% API
%%%===================================================================

-spec execute(qry_fsm_name(), {qry(), #ddl_v1{}, #ddl_v1{}}) -> ok | {error, atom()}.
execute(FSMName, {QId, Qry, DDL}) ->
    gen_server:call(FSMName, {execute, {QId, Qry, DDL}}).

-spec fetch(qry_fsm_name(), query_id()) -> list() | {error, atom()}.
fetch(FSMName, QId) ->
    gen_server:call(FSMName, {fetch, QId}).


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

-spec init([qry_fsm_name()]) -> {ok, #state{}}.
%% @private
init([Name]) ->
    {ok, #state{name = Name}}.


-spec handle_call(term(), {pid(), term()}, #state{}) ->
                         {reply, Reply::ok | {error, atom()} | list(), #state{}}.
%% @private
handle_call(Request, _From, State) ->
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


-spec handle_info(term(), #state{}) -> {noreply, #state{}}.
%% @private
handle_info({QId, done},
            State = #state{qid = QId,
                           status = QStatus})
  when QStatus =/= complete ->
    {noreply, State#state{status = complete}};

handle_info({QId, {results, [Chunk]}},
            State = #state{qid = QId, qry = Qry,
                           status = QStatus,
                           result = Accumulated})
  when QStatus =:= void; QStatus =:= accumulating ->
    #riak_sql_v1{'SELECT' = SelectSpec} = Qry,
    Decoded =
        decode_results(
          Chunk, SelectSpec),
    {noreply, State#state{status = accumulating,
                          result = [Decoded | Accumulated]}};

%% what if some late chunks arrive?
handle_info({QId, {results, _}},
            State = #state{qid = QId,
                           status = complete}) ->
    lager:warning("Discarding late chunk on qid ~p", [QId]),
    {noreply, State};

%% other error conditions
handle_info({QId1, _}, State = #state{qid = QId2}) ->
    lager:warning("Bad query id ~p (expected ~p)", [QId1, QId2]),
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

-spec handle_req({atom(), term()}, #state{}) ->
                        {ok | {ok | error, term()},
                         list(), #state{}}.
handle_req({fetch, QId}, State = #state{qid = QId2})
  when QId =/= QId2 ->
    {{error, bad_query_id}, [], State};

handle_req({execute, {QId, Qry, DDL}}, State = #state{status = void}) ->
    %% TODO make this run with multiple sub-queries
    Queries = riak_kv_qry_compiler:compile(DDL, Qry),
    SEs = [{run_sub_query, {qry, X}, {qid, QId}} || X <- Queries],
    {ok, SEs, State#state{qid = QId, qry = Qry, ddl = DDL,
                          status = accumulating,
                          result = []}};

handle_req({execute, {QId, _, _}}, State = #state{status = Status})
  when Status =/= void ->
    lager:error("Qry queue manager should have cleared the status before assigning new query ~p", [QId]),
    {{error, mismanagement}, [], State};

handle_req({fetch, QId}, State = #state{qid = QId,
                                        status = accumulating}) ->
    {{error, in_progress}, [], State};

handle_req({fetch, QId}, State = #state{qid = QId,
                                        result = Result}) ->
    {{ok, Result}, [], State#state{qid = undefined,
                                   status = void,
                                   result = []}};

handle_req(_Request, State) ->
    {ok, ?NO_SIDEEFFECTS, State}.


handle_side_effects([]) ->
    ok;
handle_side_effects([{run_sub_query, {qry, Q}, {qid, QId}} | T]) ->
    Bucket = Q#riak_sql_v1.'FROM',
    %% fix these up too
    Timeout = {timeout, 10000},
    Me = self(),
    CoverageFn = {colocated, riak_kv_qry_coverage_plan},
    {ok, _PID} = riak_kv_index_fsm_sup:start_index_fsm(node(), [{raw, QId, Me}, [Bucket, none, Q, Timeout, CoverageFn]]),
    handle_side_effects(T);
handle_side_effects([H | T]) ->
    io:format("in riak_kv_qry:handle_side_effects not handling ~p~n", [H]),
    handle_side_effects(T).


decode_results(ListOfBins, SelectSpec) ->
    decode_results(ListOfBins, SelectSpec, []).
decode_results([], _SelectSpec, Acc) ->
    %% lists:reverse(Acc);  %% recall that ListOfBins is in fact
    %% reversed (as it was accumulated from chunks in handle_info)
    lists:flatten(Acc);
decode_results([BList|Rest], SelectSpec, Acc) ->
    Records = extract(BList, SelectSpec, []),
    decode_results(
      Rest, SelectSpec, [Records | Acc]).

extract(<<>>, _SelectSpec, Acc) ->
    Acc;
extract(Batch, SelectSpec, Acc) ->
    {_Key, B1} = eleveldb:parse_string(Batch),
    { Val, B2} = eleveldb:parse_string(B1),
    FullRecord =
        eleveldb_ts:decode_record(
          riak_object:get_value(
            riak_object:from_binary(
              %% don't care about bkey
              <<>>, <<>>, Val))),
    Filtered =
        filter_columns(
          SelectSpec, FullRecord),
    extract(B2, SelectSpec, [Filtered | Acc]).


filter_columns(SelectSpec, KVList) ->
    %% TODO: deal with operators and combinators
    OnlyColumns = lists:foldl(
                    fun([C], Acc) -> [binary_to_list(C)|Acc];
                       (_, Acc) -> Acc
                    end,
                    [], SelectSpec),
    lists:filter(
      fun({Field, _Val}) -> lists:member(Field, OnlyColumns) end,
      KVList).


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
