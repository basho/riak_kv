%%%-------------------------------------------------------------------
%%%
%%% riak_kv_qry: executes the sub-queries
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
-module(riak_kv_qry).

-behaviour(gen_server).

%% OTP API
-export([start_link/1]).

%% Developer API
-export([
	 execute/2
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

%% Debuggin, yo
-export([
	 get_ddl_FIXUP/0
	]).

-ifdef(TEST).
-export([
	 runner_TEST/1
	]).
-endif.

-include_lib("riak_ql/include/riak_ql_sql.hrl").

-define(SERVER, ?MODULE).
-define(NO_SIDEEFFECTS, []).
-define(NO_MAX_RESULTS, no_max_results).
-define(NO_PG_SORT, undefined).

-record(state, {
	  name       :: atom(),
	  qry = none :: none | #riak_sql_v1{}
	 }).

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
start_link(Name) ->
    gen_server:start_link({local, Name}, ?MODULE, [Name], []).

%%%===================================================================
%%% API
%%%===================================================================
execute(FSM, {QId, Qry}) ->
    ok = gen_server:call(FSM, {execute, {QId, Qry}}).

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
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([Name]) ->
    {ok, #state{name = Name}}.

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
handle_cast(Msg, State) ->
    io:format("in handle cast for riak_kv_qry~n- ~p~n", [Msg]),
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
handle_info(Info, State) ->
    io:format("in handle info for riak_kv_qry~n- ~p~n", [Info]),
    {noreply, State}.

%%--------------------------------------------------------------------
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
handle_req({execute, {QId, Qry}}, State) ->
    %% TODO make this run with multiple sub-queries
    %% TODO fix this up properly
    {ok, DDL} = get_ddl_FIXUP(),
    Queries = riak_kv_qry_compiler:compile(DDL, Qry),
    SEs = [{run_sub_query, {qry, X}, {qid, QId}} || X <- Queries],
    {ok, SEs, State};
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

get_ddl_FIXUP() ->
    SQL = "CREATE TABLE GeoCheckin " ++
	"(geohash varchar not null, " ++ 
	"user varchar not null, " ++
	"time timestamp not null, " ++ 
	"weather varchar not null, " ++ 
	"temperature varchar, " ++ 
	"PRIMARY KEY((quantum(time, 15, s)), time, user))", 
    Lexed = riak_ql_lexer:get_tokens(SQL),
    {ok, _DDL} = riak_ql_parser:parse(Lexed).

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
