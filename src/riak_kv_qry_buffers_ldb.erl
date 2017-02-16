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
         add_rows/2,
         fetch_rows/2]).


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
            lager:debug("new LdbRef ~p in ~p", [LdbRef, Path]),
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


-spec add_rows(eleveldb:db_ref(), [riak_kv_qry_buffers:data_row()]) ->
                      ok | {error, ldb_put_failed}.
add_rows(LdbRef, Rows) ->
    try
        lists:foreach(
          fun({K, V}) ->
                  ok = eleveldb:put(LdbRef, sext:encode(K), sext:encode(V), [{sync, false}])
          end,
          Rows)
    catch
        error:badmatch ->
            {error, ldb_put_failed}
    end.


-spec fetch_rows(eleveldb:db_ref(), [{Offset::non_neg_integer(),
                                      Limit::unlimited|pos_integer()}]) ->
                        {ok, [riak_kv_qry_buffers:data_row()]} | {error, term()}.
fetch_rows(LdbRef, SortedSpecs) ->
    FetchLimitFn =
        fun(_KV, {[{Off, _Lim}|_] = CurSegment, Pos, Acc}) when Pos < Off ->
                {CurSegment, Pos + 1, Acc};

           ({_K, V}, {[{Off, Lim}|RestSegment] = CurSegment, Pos, Acc})
              when Lim == unlimited orelse Pos < Off + Lim ->
                NextSegment =
                    if Lim == unlimited ->
                            CurSegment;
                       Pos + 1 < Off + Lim ->
                            CurSegment;
                       el/=se ->
                            RestSegment
                    end,
                {NextSegment, Pos + 1, [V | Acc]};

           (_KV, {[], _Pos, Acc}) ->
                throw({break, Acc})
        end,
    {ok, Fetched} =
        try eleveldb:fold(
              LdbRef, FetchLimitFn,
              {SortedSpecs, 0, []},
              [{fold_method, streaming}]) of
            {_, _, Acc} ->
                {ok, Acc}
        catch
            {break, Acc} ->
                {ok, Acc}
        end,
    Decoded =
        [sext:decode(Row) || Row <- lists:reverse(Fetched)],
    {ok, Decoded}.
