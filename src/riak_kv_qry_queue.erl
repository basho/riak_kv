%%%-------------------------------------------------------------------
%%%
%%% riak_kv_qry_queue.erl: Riak SQL worker pool and query queue manager
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

%% @doc Manager of workers handling individual queries for Riak SQL.

-module(riak_kv_qry_queue).

-behaviour(gen_server).

%% User API
-export([
         put_on_queue/2,
         fetch/1
        ]).

%% OTP API
-export([
         start_link/1
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
-include_lib("riak_ql/include/riak_ql_ddl.hrl").  %% for #ddl_v1{}

-define(SERVER, ?MODULE).

-define(NO_SIDEEFFECTS, []).

-record(fsm, {
          name                 :: qry_fsm_name(),
          qry      = none      :: none | {any(), qry()},
          status   = available :: qry_status()
         }).

-record(state, {
          fsms           = [],
          inflight_qrys  = [] :: [{query_id(), qry()}],
          queued_qrys    = [] :: [{query_id(), qry()}],
          available_fsms = [] :: [qry_fsm_name()],
          results        = [],
          timestamp      = timestamp() :: timestamp(),
          next_query_id  = 1,
          max_q_len      = 0
         }).

%%%===================================================================
%%% API
%%%===================================================================

-spec put_on_queue(qry(), #ddl_v1{}) -> ok | {error, atom()}.
%% @doc Enqueue a prepared query for execution.  The query should be
%%      compatible with the DDL supplied.
put_on_queue(Qry, DDL) ->
    gen_server:call(?MODULE, {put_on_queue, Qry, DDL}).

-spec fetch(query_id()) -> list() | {error, atom()}.
%% @doc Fetch the results of execution of a previously submitted
%%      query.
fetch(QId) ->
    gen_server:call(?MODULE, {fetch, QId}).


%%%===================================================================
%%% OTP API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link({MaxQueue, Names}) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [MaxQueue, Names], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}.
%% @end
%%--------------------------------------------------------------------
init([MaxQ, Names]) when is_integer(MaxQ) andalso MaxQ > 0,
                         is_list(Names) ->
    FSMs = [#fsm{name = X} || X <- Names],
    {ok, #state{fsms           = FSMs,
                available_fsms = Names,
                max_q_len      = MaxQ}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call(Request, _From, State) ->
    {Reply, SEs, NewState} = handle_req(Request, State),
    ok = handle_side_effects(SEs),
    {reply, Reply, NewState}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
    {noreply, State}.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
timestamp() ->
    {MegaSeconds,Seconds,MilliSeconds}=os:timestamp(),
    (MegaSeconds * 1000000000000) + (Seconds * 1000000) + MilliSeconds.

is_overloaded(#state{queued_qrys = Q,
                     max_q_len   = Max}) when length(Q) >= Max -> true;
is_overloaded(#state{queued_qrys = Q,
                     max_q_len   = Max}) when length(Q) <  Max -> false.

handle_req({put_on_queue, Qry, DDL}, State) ->
    #state{fsms           = FSMs,
           available_fsms = Avl,
           queued_qrys    = Q,
           next_query_id  = Id}  = State,
    QId = {node(), Id},
    Reply = {qid, QId},
    %% naive case of short queues where we append the queued query to the end of the Q
    case Avl of
        []      -> case is_overloaded(State) of
                       false ->
                           NewS = State#state{queued_qrys   = Q ++ [{QId, Qry}],
                                              next_query_id = Id + 1},
                           {Reply, ?NO_SIDEEFFECTS, NewS};
                       true  ->
                           Over = {overloaded,
                                   {max_queue_length, State#state.max_q_len}},
                           {Over, ?NO_SIDEEFFECTS, State}
                   end;
        [H | T] -> F = #fsm{name   = H,
                            status = in_progress,
                            qry    = {QId, Qry}},
                   NewFSMs = lists:keyreplace(H, 2, FSMs, F),
                   NewAvl = T,
                   Disp = {execute, {{fsm, H}, {QId, Qry, DDL}}},
                   NewS = State#state{fsms           = NewFSMs,
                                      available_fsms = NewAvl,
                                      next_query_id  = Id + 1},
                   {Reply, [Disp], NewS}
    end;
handle_req({fetch, QId}, State = #state{fsms = FSMs,
                                        available_fsms = Avl}) ->
    case [FSM || FSM = #fsm{qry = {Qi, _}} <- FSMs, QId == Qi] of
        [FSM = #fsm{name = Name}] ->
            %% it's not feasible to put a call to riak_kv_qry:fetch/1
            %% alongside the :execute/2, because it's not so much
            %% about side effects as retrieving result and returning
            %% a value.
            case riak_kv_qry_worker:fetch(Name, QId) of
                {error, in_progress} = NotOurError ->
                    NotOurError;
                WorkerDone ->
                    NewFSM = FSM#fsm{status = available, qry = none},
                    NewFSMs = lists:keyreplace(FSM, 2, FSMs, NewFSM),  %% treating a noble record as lowly tuple?
                    NewAvl = [Name | Avl],
                    {WorkerDone, ?NO_SIDEEFFECTS, State#state{fsms           = NewFSMs,
                                                              available_fsms = NewAvl}}
            end;
        [] ->
            {{error, bad_qid}, ?NO_SIDEEFFECTS, State}
    end;
handle_req(get_active_qrys, State) ->
    #state{fsms = FSMs} = State,
    AQs = [X || #fsm{qry = {X, _}, status = in_progress} <- FSMs],
    Reply = {active_qrys, AQs},
    {Reply, ?NO_SIDEEFFECTS, State};
handle_req(get_queued_qrys, State) ->
    #state{queued_qrys = Queue} = State,
    Qs = [X || {X, _Y} <- Queue],
    Reply = {queued_qrys, Qs},
    {Reply, ?NO_SIDEEFFECTS, State};
handle_req(Request, State) ->
    io:format("Not handling ~p~n", [Request]),
    {ok, ?NO_SIDEEFFECTS, State}.

handle_side_effects([]) ->
    ok;
handle_side_effects([{execute, {{fsm, FSM}, {QId, Qry, DDL}}} | T]) ->
    ok = riak_kv_qry_worker:execute(FSM, {QId, Qry, DDL}),
    handle_side_effects(T);
handle_side_effects([H | T]) ->
    io:format("riak_kv_qry_queue:handling side_effect ~p~n", [H]),
    handle_side_effects(T).

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
    % io:format("Before ~p~n- SideEffects is ~p~n- Replies is ~p~n- Errors is ~p~n", [H, SideEffects, Replies, Errs]),
    {NewSt, NewSEs, NewRs, NewE} = run(H, State, LineNo, SideEffects, Replies, Errs),
    % io:format("After ~p~n- NewSEs is ~p~n- NewRs is ~p~n- NewE is ~p~n", [H, NewSEs, NewRs, NewE]),
    test_r2(T, NewSt, LineNo + 1, NewSEs, NewRs, NewE).

run({run, {init, {MaxQ, Names}}}, _State, _LNo, SideEffects, Replies, Errs) ->
    {ok, NewState} = init([MaxQ, Names]),
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
    io:format("after running ~p reply is ~p NewSEs is ~p~n", [Msg, Reply, NewSEs]),
    {NewState, NewSEs, [Reply | Replies], Errs};
run({side_effect, G}, State, LNo, SEs, Replies, Errs) ->
    io:format("in side effects G is ~p~n-SEs is ~p~n", [G, SEs]),
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
    io:format("in replies G is ~p~n-Replies is ~p~n", [G, Replies]),
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
             {run,   {init, {?MAX_Q_LEN, [fsm1, fsm2]}}},
             {msg,   get_active_qrys},
             {reply, {active_qrys, []}},
             {msg,   get_queued_qrys},
             {reply, {queued_qrys, []}}
           ],
    Results = runner_TEST(Tests),
    ?assertEqual({[], [], []}, Results).

simple_queue_test() ->
    Tests = [
             {run,         {init, {?MAX_Q_LEN, [fsm1, fsm2]}}},
             {msg,         {put_on_queue, a_query}},
             {reply,       {qid, {node(), 1}}},
             {side_effect, {execute, {{fsm, fsm1}, {{node(), 1}, a_query}}}},
             {msg,         get_active_qrys},
             {reply,       {active_qrys, [{node(), 1}]}}
           ],
    {SideEffects, Replies, Errors} = runner_TEST(Tests),
    ?assertEqual({[], [], []}, {SideEffects, Replies, Errors}).

simple_queue_2_test() ->
    Tests = [
             {run,   {init, {?MAX_Q_LEN, [fsm1, fsm2]}}},
             {msg,   {put_on_queue, a_query}},
             {msg,   {put_on_queue, a_query}},
             {msg,   {put_on_queue, a_query}},
             {msg,   {put_on_queue, a_query}},
             {clear, replies},
             {clear, side_effects},
             {msg,   get_active_qrys},
             {reply, {active_qrys, [
                                    {node(), 1},
                                    {node(), 2}
                                   ]}},
             {msg,   get_queued_qrys},
             {reply, {queued_qrys, [
                                    {node(), 3},
                                    {node(), 4}
                                   ]}}
            ],
    {SideEffects, Replies, Errors} = runner_TEST(Tests),
    ?assertEqual({[], [], []}, {SideEffects, Replies, Errors}).

-define(SHORT_Q, 1).

simple_overload_test() ->
    Tests = [
             {run,   {init, {?SHORT_Q, [fsm1, fsm2]}}},
             {msg,   {put_on_queue,  a_query}},
             {msg,   {put_on_queue,  a_query}},
             {msg,   {put_on_queue,  a_query}},
             {clear, replies},
             {clear, side_effects},
             {msg,   {put_on_queue,  a_query}},
             {reply, {overloaded, {max_queue_length, ?SHORT_Q}}}
            ],
    {SideEffects, Replies, Errors} = runner_TEST(Tests),
    ?assertEqual({[], [], []}, {SideEffects, Replies, Errors}).

-endif.
