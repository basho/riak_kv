%%-------------------------------------------------------------------
%%
%% riak_kv_qry_buffers: Riak SQL query result disk-based temp storage
%%                     (aka 'query buffers')
%%
%% Copyright (C) 2016 Basho Technologies, Inc. All rights reserved
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
%%-------------------------------------------------------------------

%% @doc SELECT queries with a LIMIT clause are persisted on disk, in
%%      order to (a) effectively enable sorting of big amounts of data
%%      and (b) support paging, whereby subsequent, separate queries
%%      can extract a subrange ("SELECT * FROM T LIMIT 10" followed by
%%      "SELECT * FROM T LIMIT 10 OFFSET 10").
%%
%%      Disk-backed temporary storage is implemented as per-query
%%      instance of leveldb (a "query buffer").  Once qry_worker has
%%      finished collecting data from vnodes, the resulting set of
%%      chunks is stored in a single leveldb table.  Exported
%%      functions are provided to extract certain subranges from it
%%      and thus to execute any subsequent queries.
%%
%%      Queries are matched by identical SELECT, FROM, WHERE, GROUP BY
%%      and ORDER BY expressions.  A hashing function on these query
%%      parts is provided for query identification and quick
%%      comparisons.

-module(riak_kv_qry_buffers).

-behaviour(gen_server).

-export([start_link/1,
         init/1,
         handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3
        ]).

-export([
         batch_put/2,           %% emulate INSERT (new Chunk collected by worker for a query Ref)
         delete_qbuf/1,         %% drop a table by Ref
         fetch_limit/3,         %% emulate SELECT
         get_or_create_qbuf/5,  %% new Query arrives, with some Options
         get_qbuf_expiry/1,
         get_max_query_data_size/0,
         set_max_query_data_size/1,
         set_qbuf_expiry/2,
         set_ready_waiting_process/2,  %% notify a process when all chunks are here
         kill_all_qbufs/0,
         shutdown/0,

         %% utility functions
         limit_to_scalar/1,
         offset_to_scalar/1
        ]).

-type qbuf_ref() :: binary().
-type qbuf_option() :: {expiry_time, Seconds::non_neg_integer()} |
                       {atom(), term()}.
-type qbuf_options() :: [qbuf_option()].
-type watermark_status() :: underfull | limited_capacity.  %% overengineering much?
-type data_row() :: [riak_pb_ts_codec:ldbvalue()].

-export_type([qbuf_ref/0, qbuf_options/0, watermark_status/0, data_row/0]).

-include("riak_kv_ts.hrl").

-define(SERVER, ?MODULE).
-define(TIMER_TICK_MSEC, 1000).

%%%===================================================================
%%% API
%%%===================================================================

-spec get_or_create_qbuf(?SQL_SELECT{}, non_neg_integer(),
                         #riak_sel_clause_v1{}, [riak_kv_qry_compiler:sorter()],
                         proplists:proplist()) ->
                                {ok, {new|existing, qbuf_ref()}} |
                                {error, query_non_pageable|total_qbuf_size_limit_reached}.
%% @doc (Maybe create and) return a query buffer ref, set up for
%%      receiving chunks of data from qry_worker.  Options can contain
%%      `expire_msec` property, which will override the standard
%%      expiry time from State.
get_or_create_qbuf(SQL, NSubqueries, CompiledSelect, CompiledOrderBy, Options) ->
    gen_server:call(?SERVER, {get_or_create_qbuf, SQL, NSubqueries,
                              CompiledSelect, CompiledOrderBy, Options}).

-spec delete_qbuf(qbuf_ref()) -> ok.
%% @doc Dispose of this query buffer (do nothing if it does not exist).
delete_qbuf(QBufRef) ->
    gen_server:call(?SERVER, {delete_qbuf, QBufRef}).

-spec batch_put(qbuf_ref(), [data_row()]) ->
                       ok | {error, bad_qbuf_ref|overfull}.
%% @doc Emulate a batch put.
batch_put(QBufRef, Data) ->
    gen_server:call(?SERVER, {batch_put, QBufRef, Data}).

-spec set_ready_waiting_process(qbuf_ref(), function()) -> ok | {error, bad_qbuf_ref}.
%% @doc Set a process to notify on qbuf completion
set_ready_waiting_process(QBufRef, SelfNotifierFun) ->
    gen_server:call(?SERVER, {set_ready_waiting_process, QBufRef, SelfNotifierFun}).

-spec kill_all_qbufs() -> ok.
%% @doc Kill all query buffers.
kill_all_qbufs() ->
    gen_server:call(?SERVER, kill_all_qbufs).

-spec shutdown() -> ok.
%% @doc Kill all query buffers, delete our instance's root dir.
shutdown() ->
    gen_server:call(?SERVER, shutdown).

-spec fetch_limit(qbuf_ref(), unlimited | pos_integer(), non_neg_integer()) ->
                    {ok, riak_kv_qry:query_tabular_result()} |
                    {error, bad_qbuf_ref|bad_sql|qbuf_not_ready}.
%% @doc Emulate SELECT.
fetch_limit(QBufRef, Limit, Offset) ->
    gen_server:call(?SERVER, {fetch_limit, QBufRef, Limit, Offset}).

-spec get_qbuf_expiry(qbuf_ref()) ->
                    {ok, pos_integer()} | {error, bad_qbuf_ref}.
%% @doc Get this query buffer expiry period.
get_qbuf_expiry(QBufRef) ->
    gen_server:call(?SERVER, {get_qbuf_expiry, QBufRef}).

-spec set_qbuf_expiry(qbuf_ref(), pos_integer()) ->
                    ok | {error, bad_qbuf_ref}.
%% @doc Set this query buffer expiry period.
set_qbuf_expiry(QBufRef, NewExpiry) ->
    gen_server:call(?SERVER, {set_qbuf_expiry, QBufRef, NewExpiry}).

-spec get_max_query_data_size() -> non_neg_integer().
%% @doc Get the max query buffer size
get_max_query_data_size() ->
    gen_server:call(?SERVER, get_max_query_data_size).

-spec set_max_query_data_size(non_neg_integer()) -> ok.
%% @doc Get the max query buffer size
set_max_query_data_size(Value) ->
    gen_server:call(?SERVER, {set_max_query_data_size, Value}).


%% Utility functions that don't need or use the gen_server

limit_to_scalar([]) -> unlimited;
limit_to_scalar([A]) when is_integer(A) -> A.

offset_to_scalar([]) -> 0;
offset_to_scalar([A]) when is_integer(A), A >= 0 -> A.


-record(qbuf, {
          %% original SELECT query this buffer holds the data for
          orig_qry :: ?SQL_SELECT{},
          %% a DDL for it
          ddl :: ?DDL{},
          %% table the original query selected data from
          mother_table :: binary(),

          %% this qbuf expiry period (set to a default value from
          %% State; can be overridden)
          expire_msec :: non_neg_integer(),

          %% leveldb handle for the temp storage
          ldb_ref :: eleveldb:db_ref(),

          %% received chunks count so far
          chunks_got = 0 :: integer(),
          %% total chunks needed (== number of subqueries)
          chunks_need :: integer(),

          %% flipped to true when chunks_need == chunks_got
          is_ready = false :: boolean(),

          %% total records stored (when complete)
          total_records = 0 :: non_neg_integer(),

          %% %% iterator cache (need a working iterator support in eleveldb)
          %% iter_cache = [] :: [cached_iter()],

          %% total size on disk, for reporting
          size = 0 :: non_neg_integer(),

          %% last added chunk (when not complete) or queried
          last_accessed = 0 :: erlang:timestamp(),

          %% a proplist for all your options
          options = [] :: qbuf_options(),

          %% precomputed key field positions
          key_field_positions :: [non_neg_integer()],

          %% a fun to notify some process waiting for this qbuf ready
          %% status
          ready_waiting_notifier :: function() | undefined
         }).

-record(state, {
          status :: init_in_progress | {init_failed, Reason::term()} | ready | stopped,
          qbufs = [] :: [{qbuf_ref(), #qbuf{}}],
          total_size = 0 :: non_neg_integer(),
          %% no new queries; accumulation allowed
          soft_watermark :: non_neg_integer(),
          %% drop some tables now
          hard_watermark :: non_neg_integer(),
          %% drop incomplete query buffer after this long since last add_chunk
          incomplete_qbuf_release_msec :: non_neg_integer(),
          %% drop complete query buffers after this long since serving last query
          qbuf_expire_msec :: non_neg_integer(),
          %% max query size
          max_query_data_size :: non_neg_integer(),
          %% dir containing qbuf ldb files
          root_path :: string()
         }).


-spec start_link(list()) -> {ok, pid()} | ignore | {error, term()}.
start_link(Args) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Args, []).


-spec init([string() | integer()]) -> {ok, #state{}}.
init([RootPath, MaxRetSize,
      SoftWatermark, HardWatermark,
      QBufExpireMsec, IncompleteQBufReleaseMsec]) ->
    {ok, OurRootPath} = make_unique_dir_name(RootPath, _Attempts = 5),
    spawn(fun() -> prepare_qbuf_dir(OurRootPath) end),
    State =
        #state{status                       = init_in_progress,
               root_path                    = OurRootPath,
               max_query_data_size          = MaxRetSize,
               soft_watermark               = SoftWatermark,
               hard_watermark               = HardWatermark,
               qbuf_expire_msec             = QBufExpireMsec,
               incomplete_qbuf_release_msec = IncompleteQBufReleaseMsec
              },
    spawn_link(fun() -> schedule_tick() end),
    {ok, State}.

make_unique_dir_name(RootPath, Attempt) ->
    UniquePart = [crypto:rand_uniform($a, $z) || _ <- [x,x,x,i,x,x,x,i]],
    FullPath = filename:join(RootPath, UniquePart),
    case filelib:is_dir(FullPath) of
        true when Attempt > 0 ->
            make_unique_dir_name(RootPath, Attempt - 1);
        true ->
            lager:error("Could not find a unique name for a new dir in ~s", [RootPath]),
            %% something has gone terribly wrong with this system if
            %% there are five dirs already existing with names
            %% matching five successive 8-char sequences
            {error, temp_dir_create_fail};
        false ->
            {ok, FullPath}
    end.

prepare_qbuf_dir(RootPath) ->
    %% don't bother recovering any leftover tables
    case riak_kv_ts_util:rm_rf(RootPath) of
        ok ->
            gen_server:cast(?SERVER, {prepare_qbuf_dir, ok});
        {error, Reason1} ->
            lager:warning("Found old data in qbuf dir \"~s\" could not be removed: ~p", [RootPath, Reason1]),
            gen_server:cast(?SERVER, {prepare_qbuf_dir, ok})
            %% eleveldb:open may fail, users beware
    end,
    case filelib:ensure_dir(RootPath) of
        ok ->
            gen_server:cast(?SERVER, {prepare_qbuf_dir, ok});
        {error, Reason2} ->
            lager:warning("Could not create qbuf dir \"~s\": ~p", [RootPath, Reason2]),
            gen_server:cast(?SERVER, {prepare_qbuf_dir, {fail, Reason2}})
    end.

schedule_tick() ->
    gen_server:cast(?SERVER, tick),
    timer:sleep(?TIMER_TICK_MSEC),
    schedule_tick().

-spec handle_call(term(), pid() | {pid(), term()}, #state{}) -> {reply, term(), #state{}}.
handle_call(_Req, _From, State = #state{status = stopped}) ->
    {reply, {error, stopped}, State};
handle_call(_Req, _From, State = #state{status = init_in_progress}) ->
    {reply, {error, not_ready}, State};
handle_call(_Req, _From, State = #state{status = {init_failed, _Reason}}) ->
    %% don't report Reason: init_failed should be enough for smart
    %% clients to look for it in the logs
    {reply, {error, init_failed}, State};

handle_call({get_or_create_qbuf, SQL, NSubqueries, CompiledSelect, CompiledOrderBy, Options}, _From, State) ->
    do_get_or_create_qbuf(SQL, NSubqueries, CompiledSelect, CompiledOrderBy, Options, State);

handle_call({delete_qbuf, QBufRef}, _From, State) ->
    do_delete_qbuf(QBufRef, State);

handle_call({batch_put, QBufRef, Data}, _From, State) ->
    do_batch_put(QBufRef, Data, State);

handle_call({set_ready_waiting_process, QBufRef, SelfNotifierFun}, _From, State) ->
    do_set_ready_waiting_process(QBufRef, SelfNotifierFun, State);

handle_call(kill_all_qbufs, _From, State) ->
    do_kill_all_qbufs(State);

handle_call(shutdown, _From, State) ->
    do_shutdown(State);

handle_call({fetch_limit, QBufRef, Limit, Offset}, _From, State) ->
    do_fetch_limit(QBufRef, Limit, Offset, State);

handle_call({get_qbuf_expiry, QBufRef}, _From, State) ->
    do_get_qbuf_expiry(QBufRef, State);

handle_call({set_qbuf_expiry, QBufRef, NewExpiry}, _From, State) ->
    do_set_qbuf_expiry(QBufRef, NewExpiry, State);

handle_call(get_max_query_data_size, _From, State) ->
    do_get_max_query_data_size(State);

handle_call({set_max_query_data_size, Value}, _From, State) ->
    do_set_max_query_data_size(Value, State).


-spec handle_cast(term(), #state{}) -> {noreply, #state{}}.
handle_cast({prepare_qbuf_dir, ok}, State) ->
    {noreply, State#state{status = ready}};
handle_cast({prepare_qbuf_dir, {error, Reason}}, State) ->
    {noreply, State#state{status = {init_failed, Reason}}};
handle_cast(tick, State) ->
    do_reap_expired_qbufs(State);
handle_cast(_Msg, State) ->
    lager:warning("Not handling cast message ~p", [_Msg]),
    {noreply, State}.


-spec handle_info(term(), #state{}) -> {noreply, #state{}}.
handle_info({streaming_end, _Ref}, State) ->
    %% ignore streaming_end messages when they arrive to indicate
    %% eleveldb:fold has reached an end of range
    {noreply, State};
handle_info(_Msg, State) ->
    lager:warning("Not handling info message ~p", [_Msg]),
    {noreply, State}.


-spec terminate(term(), #state{}) -> term().
terminate(_Reason, State = #state{root_path = RootPath}) ->
    _ = kill_all_qbufs(State),
    _ = file:del_dir(RootPath),
    ok.


-spec code_change(term() | {down, term()}, #state{}, term()) -> {ok, #state{}}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%===================================================================
%%% do_thing functions
%%%===================================================================

do_get_or_create_qbuf(SQL = ?SQL_SELECT{'FROM' = OrigTable},
                      NSubqueries, CompiledSelect, CompiledOrderBy,
                      Options,
                      #state{qbufs            = QBufs0,
                             soft_watermark   = SoftWMark,
                             root_path        = RootPath,
                             total_size       = TotalSize,
                             qbuf_expire_msec = DefaultQBufExpireMsec} = State0) ->
    case get_qref(SQL, QBufs0) of
        {ok, {new, QBufRef}} ->
            case TotalSize > SoftWMark of
                true ->
                    {reply, {error, total_qbuf_size_limit_reached}, State0};
                false ->
                    DDL = ?DDL{table = Table} =
                        sql_to_ddl(OrigTable, CompiledSelect, CompiledOrderBy),
                    lager:debug("creating new query buffer ~p (ref ~p) for ~p", [Table, QBufRef, SQL]),
                    case riak_kv_qry_buffers_ldb:new_table(Table, RootPath) of
                        {ok, LdbRef} ->
                            QBuf = #qbuf{orig_qry      = SQL,
                                         ddl           = DDL,
                                         mother_table  = riak_kv_ts_util:queried_table(SQL),
                                         chunks_need   = NSubqueries,
                                         ldb_ref       = LdbRef,
                                         expire_msec   = proplists:get_value(
                                                           expiry_msec, Options, DefaultQBufExpireMsec),
                                         last_accessed = os:timestamp(),
                                         options       = Options,
                                         key_field_positions = get_lk_field_positions(DDL)},
                            QBufs = QBufs0 ++ [{QBufRef, QBuf}],
                            State = State0#state{qbufs = QBufs,
                                                 total_size = compute_total_qbuf_size(QBufs)},
                            {reply, {ok, {new, QBufRef}}, State};
                        {error, Reason} ->
                            {reply, {error, Reason}, State0}
                    end
            end
    end.


do_delete_qbuf(QBufRef, #state{qbufs = QBufs0,
                               root_path = RootPath} = State0) ->
    case get_qbuf_record(QBufRef, QBufs0) of
        false ->
            {reply, {error, bad_qbuf_ref}, State0};
        #qbuf{is_ready = false} ->
            {reply, {error, qbuf_not_ready}, State0};
        #qbuf{ldb_ref = LdbRef,
              ddl = ?DDL{table = Table}} ->
            ok = riak_kv_qry_buffers_ldb:delete_table(Table, LdbRef, RootPath),
            {reply, ok, State0#state{qbufs = lists:keydelete(QBufRef, 1, QBufs0)}}
    end.


do_batch_put(QBufRef, Data, #state{qbufs          = QBufs0,
                                   total_size     = TotalSize0,
                                   hard_watermark = HardWatermark} = State0) ->
    case get_qbuf_record(QBufRef, QBufs0) of
        false ->
            {reply, {error, bad_qbuf_ref}, State0};
        #qbuf{is_ready = true} ->
            {reply, {error, qbuf_already_finished}, State0};
        QBuf0 ->
            case maybe_add_chunk(
                   QBuf0, Data, TotalSize0, HardWatermark) of
                {ok, #qbuf{is_ready = IsReady,
                           ready_waiting_notifier = SelfNotifierFun} = QBuf} ->
                    State9 = State0#state{total_size = TotalSize0 + QBuf#qbuf.size,
                                          qbufs = lists:keyreplace(
                                                    QBufRef, 1, QBufs0, {QBufRef, QBuf})},
                    maybe_inform_waiting_process(
                      IsReady, SelfNotifierFun, QBufRef),
                    {reply, ok, State9};
                {error, Reason} ->
                    {reply, {error, Reason}, State0}
            end
    end.

-spec maybe_add_chunk(#qbuf{}, [data_row()],
                      non_neg_integer(), non_neg_integer()) ->
                             {ok, #qbuf{}, non_neg_integer()} | {error, total_qbuf_size_limit_reached | riak_kv_qry_buffers_ldb:errors()}.
maybe_add_chunk(#qbuf{ldb_ref       = LdbRef,
                      orig_qry      = ?SQL_SELECT{'ORDER BY' = OrderBy},
                      chunks_got    = ChunksGot0,
                      chunks_need   = ChunksNeed,
                      size          = Size,
                      total_records = TotalRecords0,
                      key_field_positions = KeyFieldPositions} = QBuf0,
                Data,
                TotalSize, HardWatermark) ->
    ChunkSize = compute_chunk_size(Data),
    case TotalSize + ChunkSize > HardWatermark of
        true ->
            {error, total_qbuf_size_limit_reached};
        false ->
            %% ChunkId will be used to construct a new and unique key
            %% for each record. Ideally, this should be the serial
            %% number of the subquery.
            ChunkId = ChunksGot0,
            ChunksGot = ChunksGot0 + 1,
            lager:debug("adding chunk ~b of ~b", [ChunksGot, ChunksNeed]),
            OrdByFieldQualifiers = lists:map(fun get_ordby_field_qualifiers/1, OrderBy),
            case riak_kv_qry_buffers_ldb:add_rows(LdbRef, Data, ChunkId,
                                                  KeyFieldPositions,
                                                  OrdByFieldQualifiers) of
                ok ->
                    IsReady = (ChunksNeed == ChunksGot),
                    QBuf = QBuf0#qbuf{size          = Size + ChunkSize,
                                      chunks_got    = ChunksGot,
                                      total_records = TotalRecords0 + length(Data),
                                      is_ready      = IsReady,
                                      last_accessed = os:timestamp()},
                    {ok, QBuf};
                {error, _} = ErrorReason ->
                    ErrorReason
            end
    end.

get_ordby_field_qualifiers({_, AscDesc, Nulls}) ->
    {AscDesc, Nulls}.


maybe_inform_waiting_process(true, SelfNotifierFun, QBufRef) when is_function(SelfNotifierFun) ->
    SelfNotifierFun({qbuf_ready, QBufRef});
maybe_inform_waiting_process(_IsReady, _SelfNotifierFun, _QBufRef) ->
    nop.


do_set_ready_waiting_process(QBufRef, SelfNotifierFun, #state{qbufs = QBufs0} = State0) ->
    case get_qbuf_record(QBufRef, QBufs0) of
        false ->
            {reply, {error, bad_qbuf_ref}, State0};
        #qbuf{is_ready = true} ->
            %% qbuf is already ready: don't wait for the next
            %% batch_put to turn round and notify the process (because
            %% there will be no more batch puts)
            SelfNotifierFun({qbuf_ready, QBufRef}),
            {reply, ok, State0};
        QBuf0 ->
            QBuf9 = QBuf0#qbuf{ready_waiting_notifier = SelfNotifierFun},
            State9 = State0#state{qbufs = lists:keyreplace(
                                            QBufRef, 1, QBufs0, {QBufRef, QBuf9})},
            {reply, ok, State9}
    end.


do_kill_all_qbufs(State0) ->
    State9 = kill_all_qbufs(State0),
    {reply, ok, State9}.


%% same as terminate/2, to be called from riak_kv_app:prep_stop
do_shutdown(State = #state{root_path = RootPath}) ->
    _ = kill_all_qbufs(State),
    _ = file:del_dir(RootPath),
    {reply, ok, State#state{status = stopped}}.


do_fetch_limit(QBufRef,
               Limit, Offset,
               #state{qbufs = QBufs0} = State0) ->
    case get_qbuf_record(QBufRef, QBufs0) of
        false ->
            {reply, {error, bad_qbuf_ref}, State0};
        #qbuf{is_ready = false} ->
            {reply, {error, qbuf_not_ready}, State0};
        #qbuf{ldb_ref = LdbRef,
              orig_qry = OrigQry,
              ddl = ?DDL{fields = QBufFields,
                         table = Table}} ->
            case riak_kv_qry_buffers_ldb:fetch_rows(LdbRef, Offset, Limit) of
                {ok, Rows} ->
                    lager:debug("fetched ~p rows from ~p for ~p", [length(Rows), Table, OrigQry]),
                    ColNames = [Name || #riak_field_v1{name = Name} <- QBufFields],
                    ColTypes = [Type || #riak_field_v1{type = Type} <- QBufFields],
                    State9 = touch_qbuf(QBufRef, State0),
                    {reply, {ok, {ColNames, ColTypes, Rows}}, State9}
                %% {error, Reason} ->
                %%     {reply, {error, Reason}, State0}
            end
    end.

do_get_qbuf_expiry(QBufRef, #state{qbufs = QBufs} = State) ->
    case get_qbuf_record(QBufRef, QBufs) of
        false ->
            {reply, {error, bad_qbuf_ref}, State};
        #qbuf{expire_msec = ExpiryMsec} ->
            {reply, {ok, ExpiryMsec}, State}
    end.

do_set_qbuf_expiry(QBufRef, NewExpiry, #state{qbufs = QBufs0} = State0) ->
    case get_qbuf_record(QBufRef, QBufs0) of
        false ->
            {reply, {error, bad_qbuf_ref}, State0};
        QBuf0 ->
            QBuf9 = QBuf0#qbuf{expire_msec = NewExpiry},
            State9 = State0#state{qbufs = lists:keyreplace(
                                            QBufRef, 1, QBufs0, {QBufRef, QBuf9})},
            {reply, ok, State9}
    end.


do_get_max_query_data_size(#state{max_query_data_size = Value} = State) ->
    {reply, Value, State}.

do_set_max_query_data_size(Value, State0) ->
    State9 = State0#state{max_query_data_size = Value},
    {reply, ok, State9}.


do_reap_expired_qbufs(#state{qbufs = QBufs0,
                             root_path = RootPath,
                             incomplete_qbuf_release_msec = IncompleteQbufReleaseMsec} = State) ->
    Now = os:timestamp(),
    QBufs9 =
        lists:filter(
          fun({_QBufRef, #qbuf{is_ready = true,
                               ldb_ref = LdbRef,
                               ddl = ?DDL{table = Table},
                               expire_msec = ExpireMsec,  %% qbuf-specific, possibly overriden
                               last_accessed = LastAccessed}}) ->
                  ExpiresOn = advance_timestamp(LastAccessed, ExpireMsec),
                  case ExpiresOn < Now of
                      true ->
                          ok = kill_ldb(RootPath, Table, LdbRef),
                          lager:debug("Reaped expired qbuf ~p", [Table]),
                          false;
                      false ->
                          true
                  end;
             ({_QBufRef, #qbuf{is_ready = false,
                               ldb_ref = LdbRef,
                               ddl = ?DDL{table = Table},
                               last_accessed = LastAccessed}}) ->
                  ExpiresOn = advance_timestamp(LastAccessed, IncompleteQbufReleaseMsec),
                  case ExpiresOn < Now of
                      true ->
                          ok = kill_ldb(RootPath, Table, LdbRef),
                          lager:info("Reaped incompletely filled qbuf ~p", [Table]),
                          false;
                      false ->
                          true
                  end
          end,
          QBufs0),
    TotalSize =
        lists:foldl(
          fun({_QBufRef, #qbuf{size = Size}}, Acc) -> Acc + Size end,
          0, QBufs9),
    lager:debug("TotalSize now ~p", [TotalSize]),
    {noreply, State#state{qbufs = QBufs9,
                          total_size = TotalSize}}.


%%%===================================================================
%%% other internal functions
%%%===================================================================

%% query properties

%% @private Create a DDL to accommodate the SELECT. Note that the
%% original query comes here not compiled and therefore, has fields
%% appearing as `{identifier, Field}` rather than `Field` (and has no
%% types).
sql_to_ddl(Table, CompiledSelect, CompiledOrderBy) ->
    ?DDL{table         = make_qbuf_id(Table, CompiledSelect, CompiledOrderBy),
         fields        = make_fields_from_select(CompiledSelect),
         partition_key = none,
         %% this ensures the right natural order in the newly created
         %% eleveldb
         local_key     = make_lk_from_orderby(CompiledOrderBy)}.

make_qbuf_id(From, Select, OrderBy) ->
    %% In order to emulate erlang:now/0 behaviour for generating
    %% unique timestamps, we can use the fact that the call chains
    %% eventually involving this function are serialized:
    timer:sleep(1),
    list_to_binary(
      fmt("~s_~s_~s__~s", [From, join_fields(Select), join_fields(OrderBy), tstamp()])).

make_fields_from_select(#riak_sel_clause_v1{col_return_types = ColReturnTypes,
                                            col_names = ColNames}) ->
    WithPositions = lists:zip3(lists:seq(1, length(ColNames)), ColNames, ColReturnTypes),
    [#riak_field_v1{name     = ColName,
                    position = Pos,
                    type     = ColReturnType,
                    optional = false} || {Pos, ColName, ColReturnType} <- WithPositions].

%% when Descending keys by @atill gets merged, we will be able to
%% store OrderingDirection in ?SQL_PARAM where it belongs. Until then,
%% we keep the ordering qualifiers in a makeshift #qbuf{} field outside DDL.
make_lk_from_orderby(OrderBy) ->
    #key_v1{ast = [?SQL_PARAM{name = [Field]} || {Field, _AscDesc, _Nulls} <- OrderBy]}.

get_lk_field_positions(?DDL{fields = Fields, local_key = #key_v1{ast = LKAST}}) ->
    AsProplist =
        [{Name, Pos} || #riak_field_v1{name = Name, position = Pos} <- Fields],
    [proplists:get_value(Name, AsProplist) || ?SQL_PARAM{name = [Name]} <- LKAST].

join_fields(#riak_sel_clause_v1{col_names = CC}) ->
    join_fields(CC);
join_fields(CC) ->
    iolist_to_binary(
      string:join(
        lists:map(
          fun({F, AscDesc, Nulls}) ->
                  fmt("~s.~c~c", [F, qualifier_char(AscDesc), qualifier_char(Nulls)]);
             (F) ->
                  fmt("~s", [F])
          end,
          CC),
        "+")).

qualifier_char(asc)   -> $a;
qualifier_char(desc)  -> $d;
qualifier_char(nulls_first) -> $f;
qualifier_char(nulls_last)  -> $l.


tstamp() ->
    {_, S, M} = os:timestamp(),
    fmt("~10..0b~10..0b", [S, M]).


%% data ops
compute_chunk_size(Data) ->
    erlang:external_size(Data).


%% buffer list maintenance

get_qref(_SQL, _QBufs) ->
    AlwaysUniqueId = term_to_binary(make_ref()),
    {ok, {new, AlwaysUniqueId}}.

kill_all_qbufs(State0 = #state{qbufs = QBufs,
                               root_path = RootPath}) ->
    [lager:debug("cleaning up ~b buffer(s)", [length(QBufs)]) || QBufs /= []],
    lists:foreach(
      fun({_QBufRef, #qbuf{ldb_ref = LdbRef,
                           ddl = ?DDL{table = Table}}}) ->
              kill_ldb(RootPath, Table, LdbRef)
      end,
      QBufs),
    State0#state{qbufs = [],
                 total_size = 0}.

touch_qbuf(QBufRef, State0 = #state{qbufs = QBufs0}) ->
    QBuf0 = get_qbuf_record(QBufRef, QBufs0),
    QBuf9 = QBuf0#qbuf{last_accessed = os:timestamp()},
    State0#state{qbufs = lists:keyreplace(
                           QBufRef, 1, QBufs0, {QBufRef, QBuf9})}.

kill_ldb(RootPath, Table, LdbRef) ->
    ok = riak_kv_qry_buffers_ldb:delete_table(Table, LdbRef, RootPath),
    ok.


compute_total_qbuf_size(QBufs) ->
    lists:foldl(
      fun({_Ref, #qbuf{size = Size}}, Acc) -> Acc + Size end,
      0, QBufs).

get_qbuf_record(Ref, QBufs) ->
    case lists:keyfind(Ref, 1, QBufs) of
        false ->
            false;
        {Ref, QBuf} ->
            QBuf
    end.


advance_timestamp({Mega0, Sec0, Micro0}, Msec) ->
    Micro1 = Micro0 + Msec * 1000,
    Micro9 = Micro1 rem 1000000,
    Sec1 = Sec0 + (Micro1 div 1000000),
    Sec9 = Sec1 rem 1000000,
    Mega9 = Mega0 + (Sec1 div 1000000),
    {Mega9, Sec9, Micro9}.


fmt(F, A) ->
    lists:flatten(io_lib:format(F, A)).
