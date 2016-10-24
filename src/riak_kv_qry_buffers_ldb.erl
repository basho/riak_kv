%%-------------------------------------------------------------------
%%
%% riak_kv_qry_buffers_ldb: Riak SQL query result disk-based temp storage
%%                          (leveldb backend ops)
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

%% @doc leveldb operations for Riak TS query buffers

-module(riak_kv_qry_buffers_ldb).

-export([new_table/2,
         delete_table/3,
         add_rows/5,
         fetch_rows/3]).


%% leveldb instance parameters
-define(LDB_WRITE_BUFFER_SIZE, 10*1024*1024).  %% 10 M should be enough for everybody


-type errors() :: ldb_put_failed.
-export_type([errors/0]).

-spec new_table(binary(), string()) -> {ok, eleveldb:db_ref()} | {error, term()}.
new_table(Table, Root) ->
    Path = filename:join(Root, binary_to_list(Table)),
    _ = filelib:ensure_dir(Path),
    %% Important settings are is_internal_db and write_buffer_size;
    %% others are set here provisionally, or keps at default values
    Options = [{create_if_missing, true},
               {error_if_exists, true},
               {write_buffer_size, ?LDB_WRITE_BUFFER_SIZE},
               {verify_compactions, false},
               {compression, false},
               %% this prevents leveldb from autoexpiring records:
               {is_internal_db, true}
              ],
    case eleveldb:open(Path, Options) of
        {ok, LdbRef} ->
            lager:info("new LdbRef ~p in ~p", [LdbRef, Path]),
            {ok, LdbRef};
        {error, {Atom, _Message} = LdbError} ->
            lager:warning("qbuf eleveldb:open(~s) failed: ~p", [Path, LdbError]),
            riak_kv_ts_util:rm_rf(Path),
            {error, Atom}
    end.

-spec delete_table(binary(), eleveldb:db_ref(), string()) -> ok.
delete_table(Table, LdbRef, Root) ->
    ok = eleveldb:close(LdbRef),
    riak_kv_ts_util:rm_rf(filename:join(Root, Table)),
    ok.


-spec add_rows(eleveldb:db_ref(), [riak_kv_qry_buffers:data_row()], integer(),
               [pos_integer()], [{asc|desc, nulls_first|nulls_last}]) ->
                      ok | {error, ldb_put_failed}.
add_rows(LdbRef, Rows, ChunkId,
         KeyFieldPositions,
         OrdByFieldQualifiers) ->
    %% 0. The new key is composed from fields appearing in the ORDER
    %%    BY clause, and may therefore not work out to be unique. We
    %%    now index the rows in the chunk to preserve the original
    %%    order (because imposing any other order is even worse)
    RowsIndexed = lists:zip(Rows, lists:seq(1, length(Rows))),
    try
        lists:foreach(
          fun({Row, Idx}) ->
                  %% a. Form a new key from ORDER BY fields
                  KeyRaw = [lists:nth(Pos, Row) || Pos <- KeyFieldPositions],
                  %% b. Negate values in columns marked as DESC in ORDER BY clause
                  KeyOrd = [maybe_negate(F, Dir, NullsGroup)
                            || {F, {Dir, NullsGroup}} <- lists:zip(KeyRaw, OrdByFieldQualifiers)],
                  %% c. Combine with chunk id and row idx to ensure uniqueness, and encode.
                  KeyEnc = sext:encode({KeyOrd, ChunkId, Idx}),
                  %% d. Encode the record (don't bother constructing a
                  %%    riak_object with metadata, vclocks):
                  RowEnc = sext:encode(Row),
                  ok = eleveldb:put(LdbRef, KeyEnc, RowEnc, [{sync, true}])
          end,
          RowsIndexed)
    catch
        error:badmatch ->
            {error, ldb_put_failed}
    end.

maybe_negate([], desc, nulls_first) ->
    0;     %% sort before entupled value
maybe_negate([], asc, nulls_last) ->
    <<>>;  %% sort after entupled value
maybe_negate([], desc, nulls_last) ->
    <<>>;
maybe_negate([], asc, nulls_first) ->
    0;
maybe_negate(F, asc, _) ->
    {F};
maybe_negate(F, desc, _) when is_number(F) ->
    {-F};
maybe_negate(F, desc, _) when is_binary(F) ->
    {[<<bnot X>> || <<X>> <= F]};
maybe_negate(F, desc, _) when is_boolean(F) ->
    {not F}.



-spec fetch_rows(eleveldb:db_ref(), undefined|non_neg_integer(), undefined|pos_integer()) ->
                        {ok, [riak_kv_qry_buffers:data_row()]} | {error, term()}.
fetch_rows(LdbRef, undefined, Limit) ->
    fetch_rows(LdbRef, 0, Limit);
fetch_rows(LdbRef, Offset, LimitOrUndef) ->
    %% Because the database is read-only, the plan is to keep a cache
    %% of iterators for faster seeking (to the nearest stored),
    %% ideally also enable eleveldb to do the folding from stored
    %% iterators rather than from 'first'.
    FetchLimitFn =
        fun(_KV, {Off, Lim, Pos, Acc}) when Pos < Off ->
                {Off, Lim, Pos + 1, Acc};
           %% Fetching K (let alone V) while "seeking" to our Offset
           %% is wasteful.  We need to properly implement sensible
           %% iterator support in eleveldb (iterator_move et al
           %% currently effectively dereferences the argument
           %% iterator), before we can think up an iterator cache in
           %% qbuf state.  For now, we have to trundle to Position
           %% every time we serve a query.
           ({_K, V}, {Off, Lim, Pos, Acc}) when Lim == undefined orelse
                                                Pos < Off + Lim ->
                {Off, Lim, Pos + 1, [V | Acc]};
           (_KV, {_, _, _, Acc}) ->
                throw({break, Acc})
        end,
    FoldRes =
        try eleveldb:fold(
              LdbRef, FetchLimitFn,
              {Offset, LimitOrUndef, 0, []},
              [{fold_method, streaming}]) of
            {_, _, _N, Acc} ->
                {ok, Acc}
            %% {error, _Reason} = ER ->
            %%     ER
        catch
            {break, Acc} ->
                {ok, Acc}
        end,
    case FoldRes of
        {ok, Fetched} ->
            Decoded =
                [sext:decode(Row) || Row <- lists:reverse(Fetched)],
            {ok, Decoded}
        %% {error, Reason} ->
        %%     {error, Reason}
    end.
