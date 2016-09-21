%%%-------------------------------------------------------------------
%%%
%%% riak_kv_qry_queue.erl: Riak SQL worker pool and query queue manager
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

%% @doc Manager of workers handling individual queries for Riak SQL.

-module(riak_kv_qry_queue).

-behaviour(gen_server).

%% User API
-export([
         blocking_pop/0,
         put_on_queue/4
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

-include("riak_kv_index.hrl").
-include("riak_kv_ts.hrl").

-define(SERVER, ?MODULE).

-type qry_fsm_name() :: atom().
-type query_id()     :: {node(), integer()}.
-type qry()          :: ?SQL_SELECT{}.

-record(state, {
          max_q_len ::integer(),
          queued_qrys = queue:new(),
          reply_fn = fun gen_server:reply/2,
          waiting_workers = queue:new()
         }).

%%%===================================================================
%%% API
%%%===================================================================

-spec put_on_queue(pid(), [qry()], ?DDL{}, riak_kv_qry_buffers:qbuf_ref() | undefined) ->
        {ok, query_id()} | {error, term()}.
%% @doc Enqueue a prepared query for execution.  The query should be
%%      compatible with the DDL supplied.
put_on_queue(ReceivePid, Subqueries, DDL, QBufRef) when is_pid(ReceivePid) ->
    %% worker needs DDL to perform column filtering
    gen_server:call(?SERVER, {push_query, ReceivePid, Subqueries, DDL, QBufRef}).

%% Pop a query from the queue, this function will not return until a queue is
%% read to be executed.
-spec blocking_pop() ->
        {query, ReceivePid::pid(), QId::any(), [Qry::any()], DDL::any(), riak_kv_qry_buffers:qbuf_ref() | undefined}.
blocking_pop() ->
    gen_server:call(?SERVER, blocking_pop, infinity).

%%%===================================================================
%%% OTP API
%%%===================================================================

start_link(MaxQueryLength) when is_integer(MaxQueryLength) ->
    gen_server:start_link({local, ?SERVER}, ?SERVER, [MaxQueryLength], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

-spec init([non_neg_integer() | qry_fsm_name()]) -> {ok, #state{}}.
%% @private
init([MaxQueryLength]) ->
    {ok, #state{ max_q_len = MaxQueryLength }}.

-spec handle_call(term(), {pid(), term()}, #state{}) ->
                         {reply, ok | {error, atom()} | list(), #state{}}.
%% @private
handle_call(blocking_pop, From, State) ->
    do_blocking_pop(From, State);
handle_call({push_query, ReceivePid, Subqueries, DDL, QBufRef}, _, State) ->
    QId = {node(), make_ref()},
    QueryItem = {query, ReceivePid, QId, Subqueries, DDL, QBufRef},
    do_push_query(QueryItem, State).

-spec handle_cast(term(), #state{}) -> {noreply, #state{}}.
%% @private
handle_cast(_Msg, State) ->
    {noreply, State}.


-spec handle_info(term(), #state{}) -> {noreply, #state{}}.
%% @private
handle_info(_Info, State) ->
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

%%
do_blocking_pop(From,
                #state{ queued_qrys = Queue1,
                        waiting_workers = WaitingWorkers } = State1) ->
    case queue:out(Queue1) of
        {empty, Queue2} ->
            % if there are no queued queries then push the worker onto the
            % waiting worker queue, we'll reply to it when a query gets pushed
            State2 = State1#state{
                queued_qrys = Queue2,
                waiting_workers = queue:in(From, WaitingWorkers) },
            {noreply, State2};
        {{value, Item}, Queue2} ->
            % we have a query already queued so reply with that
            {reply, Item, State1#state{ queued_qrys = Queue2 }}
    end.

%%
do_push_query(QueryItem,
              #state{ max_q_len = MaxQLen,
                      queued_qrys = Queue1,
                      reply_fn = ReplyFn,
                      waiting_workers = WaitingWorkers1 } = State1) ->
    case queue:out(WaitingWorkers1) of
        {empty, WaitingWorkers2} ->
            % there are no awaiting workers so try and queue the query if
            % there is space
            case queue:len(Queue1) >= MaxQLen of
                true  ->
                    % no space, return overload
                    {reply, {error, overload}, State1};
                false ->
                    % the query queue has space so push it
                    State2 =
                        State1#state{ queued_qrys = queue:in(QueryItem, Queue1),
                                      waiting_workers = WaitingWorkers2 },
                    {reply, ok, State2}
            end;
        {{value, Worker}, WaitingWorkers2} ->
            % there is a waiting worker so send it straight on
            _ = ReplyFn(Worker, QueryItem),
            {reply, ok, State1#state{ waiting_workers = WaitingWorkers2 }}
    end.

%%%===================================================================
%%% Unit tests
%%%===================================================================

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-define(Q(Items), queue:from_list(lists:flatten([Items]))).

% if there is no queued queries, queue the worker
do_blocking_pop_1_test() ->
    ?assertEqual(
        {noreply, #state{ waiting_workers = ?Q(test_from) }},
        do_blocking_pop(test_from, #state{ })
    ).

% if there is a queued query, return it immediately
do_blocking_pop_2_test() ->
    ?assertEqual(
        {reply, test_query, #state{ }},
        do_blocking_pop(test_from, #state{ queued_qrys = ?Q(test_query) })
    ).

% reply to a waiting worker with the pushed query
do_push_query_1_test() ->
    Ref = make_ref(),
    State = #state{
        reply_fn = fun(test_from, test_query) -> put(Ref, Ref) end
    },
    ?assertEqual(
        do_push_query(
            test_query,
            State#state{ waiting_workers = ?Q(test_from) }),
        {reply, ok, State}
    ),
    % test that the reply function was called
    ?assertEqual(
        Ref,
        get(Ref)
    ).

% no worker is waiting so queue the query
do_push_query_2_test() ->
    ?assertEqual(
        do_push_query(
            test_query,
            #state{}),
        {reply, ok, #state{ queued_qrys = ?Q(test_query) }}
    ).

% no worker is waiting so queue the query with other queries queued
do_push_query_3_test() ->
    {reply, ok, State} =
        do_push_query(
            test_query_3,
            #state{ max_q_len = 3, queued_qrys = ?Q([test_query_2,test_query_1]) }),
    ?assertEqual(
        [test_query_1,test_query_2,test_query_3],
        lists:sort(queue:to_list(State#state.queued_qrys))
    ).

% no worker is waiting but the queue is full, so overload!
do_push_query_4_test() ->
    State = #state{ max_q_len = 1, queued_qrys = ?Q([test_query_1]) },
    ?assertEqual(
        {reply, {error, overload}, State},
        do_push_query(test_query_3, State)
    ).

-endif.
