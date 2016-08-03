%% -------------------------------------------------------------------
%%
%% riak_kv_ts_api: supporting functions for timeseries code paths
%%
%% Copyright (c) 2016 Basho Technologies, Inc.  All Rights Reserved.
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
%% -------------------------------------------------------------------

%% @doc Internal API for TS calls: single-key get and delete, batch
%%      put, coverage and query

-module(riak_kv_ts_api).

-export([
         api_call_from_sql_type/1,
         api_call_to_perm/1,
         api_calls/0,
         put_data/2, put_data/3,
         get_data/2, get_data/3, get_data/4,
         delete_data/2, delete_data/3, delete_data/4, delete_data/5,
         query/2,
         compile_to_per_quantum_queries/2  %% coverage
         %% To reassemble the broken-up queries into coverage entries
         %% for returning to pb or http clients (each needing to
         %% convert and repackage entry details in their own way),
         %% respective callbacks in riak_kv_{pb,wm}_timeseries will
         %% use riak_kv_ts_util:sql_to_cover/4.
        ]).

-include_lib("riak_ql/include/riak_ql_ddl.hrl").
-include("riak_kv_wm_raw.hrl").
-include("riak_kv_ts.hrl").

%% external API calls enumerated
-type query_api_call() :: create_table | query_select | describe_table | query_insert | query_explain | show_tables.
-type api_call() :: get | put | delete | list_keys | coverage | query_api_call().
-export_type([query_api_call/0, api_call/0]).

-spec api_call_from_sql_type(riak_kv_qry:query_type()) -> query_api_call().
api_call_from_sql_type(ddl)         -> create_table;
api_call_from_sql_type(select)      -> query_select;
api_call_from_sql_type(describe)    -> describe_table;
api_call_from_sql_type(show_tables) -> show_tables;
api_call_from_sql_type(insert)      -> query_insert;
api_call_from_sql_type(explain)     -> query_explain.

-spec api_call_to_perm(api_call()) -> string().
api_call_to_perm(get) ->
    "riak_ts.get";
api_call_to_perm(put) ->
    "riak_ts.put";
api_call_to_perm(delete) ->
    "riak_ts.delete";
api_call_to_perm(list_keys) ->
    "riak_ts.list_keys";
api_call_to_perm(coverage) ->
    "riak_ts.coverage";
api_call_to_perm(create_table) ->
    "riak_ts.create_table";
api_call_to_perm(query_select) ->
    "riak_ts.query_select";
api_call_to_perm(query_explain) ->
    "riak_ts.query_explain";
api_call_to_perm(describe_table) ->
    "riak_ts.describe_table";
%% INSERT query is a put, so let's call it that
api_call_to_perm(query_insert) ->
    api_call_to_perm(put);
api_call_to_perm(show_tables) ->
    "riak_ts.show_tables".

%%
-spec api_calls() -> [api_call()].
api_calls() ->
    [create_table, query_select, describe_table, query_insert,
     show_tables, get, put, delete, list_keys, coverage].


-spec query(string() | riak_kv_qry:sql_query_type_record(), ?DDL{}) ->
                   {ok, riak_kv_qry:query_tabular_result()} |
                   {error, term()}.
query(QueryStringOrSQL, DDL) ->
    riak_kv_qry:submit(QueryStringOrSQL, DDL).


-spec put_data([[riak_pb_ts_codec:ldbvalue()]], binary()) ->
                      ok | {error, {some_failed, integer()}} | {error, term()}.
put_data(Data, Table) ->
    put_data(Data, Table, riak_ql_ddl:make_module_name(Table)).

-spec put_data([[riak_pb_ts_codec:ldbvalue()]], binary(), module()) ->
                      ok | {error, {some_failed, integer()}} | {error, term()}.
put_data(Data, Table, Mod) ->
    DDL = Mod:get_ddl(),
    Bucket = riak_kv_ts_util:table_to_bucket(Table),
    case riak_core_bucket:get_bucket(Bucket) of
        {error, Reason} ->
            %% happens when, for example, the table has not been
            %% activated (Reason == no_type)
            {error, Reason};
        BucketProps ->
            case put_data_to_partitions(Data, Bucket, BucketProps, DDL, Mod) of
                0 ->
                    ok;
                NErrors ->
                    {error, {some_failed, NErrors}}
            end
    end.

put_data_to_partitions(Data, Bucket, BucketProps, DDL, Mod) ->
    DDL = Mod:get_ddl(),
    BucketProps = riak_core_bucket:get_bucket(Bucket),
    NVal = proplists:get_value(n_val, BucketProps),

    PartitionedData = partition_data(Data, Bucket, BucketProps, DDL, Mod),
    PreflistData = add_preflists(PartitionedData, NVal,
                                 riak_core_node_watcher:nodes(riak_kv)),

    SendFullBatches = riak_core_capability:get({riak_kv, w1c_batch_vnode}, false),
    %% Default to 1MB for a max batch size to not overwhelm disterl
    CappedBatchSize = app_helper:get_env(riak_kv, timeseries_max_batch_size,
                                         1024 * 1024),
    EncodeFn =
        fun(O) -> riak_object:to_binary(v1, O, msgpack) end,

    {ReqIds, FailReqs} =
        lists:foldl(
          fun({DocIdx, Preflist, Records}, {GlobalReqIds, GlobalErrorsCnt}) ->
                  case riak_kv_w1c_worker:validate_options(
                         NVal, Preflist, [], BucketProps) of
                      {ok, W, PW} ->
                          DataForVnode = pick_batch_option(SendFullBatches,
                                                           CappedBatchSize,
                                                           Records,
                                                           termsize(hd(Records)),
                                                           length(Records)),
                          Ids =
                              invoke_async_put(fun(Record) ->
                                                       build_object(Bucket, Mod, DDL,
                                                                    Record, DocIdx)
                                               end,
                                               fun(RObj, LK) ->
                                                       riak_kv_w1c_worker:async_put(
                                                         RObj, W, PW, Bucket, NVal, {DocIdx, LK},
                                                         EncodeFn, Preflist)
                                               end,
                                               fun(RObjs) ->
                                                       riak_kv_w1c_worker:ts_batch_put(
                                                         RObjs, W, PW, Bucket, NVal,
                                                         EncodeFn, DocIdx, Preflist)
                                               end,
                                               DataForVnode),
                          {GlobalReqIds ++ Ids, GlobalErrorsCnt};
                      _Error ->
                          {GlobalReqIds, GlobalErrorsCnt + length(Records)}
                  end
          end,
          {[], 0}, PreflistData),
    Responses = riak_kv_w1c_worker:async_put_replies(ReqIds, []),
    length(lists:filter(fun({error, _}) -> true;
                           (_) -> false
                        end, Responses)) + FailReqs.



-spec partition_data(Data :: list(term()),
                     Bucket :: {binary(), binary()},
                     BucketProps :: proplists:proplist(),
                     DDL :: ?DDL{},
                     Mod :: module()) ->
                            list(tuple(chash:index(), list(term()))).
partition_data(Data, Bucket, BucketProps, DDL, Mod) ->
    PartitionTuples =
        [ { riak_core_util:chash_key({Bucket, riak_kv_ts_util:row_to_key(R, DDL, Mod)},
                                     BucketProps), R } || R <- Data ],
    dict:to_list(
      lists:foldl(fun({Idx, R}, Dict) ->
                          dict:append(Idx, R, Dict)
                  end,
                  dict:new(),
                  PartitionTuples)).

add_preflists(PartitionedData, NVal, UpNodes) ->
    lists:map(fun({Idx, Rows}) -> {Idx,
                                   riak_core_apl:get_apl_ann(Idx, NVal, UpNodes),
                                   Rows} end,
              PartitionedData).

build_object(Bucket, Mod, DDL, Row, PK) ->
    Obj = Mod:add_column_info(Row),
    LK  = riak_kv_ts_util:encode_typeval_key(
            riak_ql_ddl:get_local_key(DDL, Row, Mod)),

    RObj = riak_object:newts(
             Bucket, PK, Obj,
             dict:from_list([{?MD_DDL_VERSION, ?DDL_VERSION}])),
    {LK, RObj}.

%%%%%%%%
%% Utility functions for batch delivery of records
termsize(Term) ->
    size(term_to_binary(Term)).

pick_batch_option(_, _, Records, _, 1) ->
    {individual, Records};
pick_batch_option(true, MaxBatch, Records, SampleSize, _NumRecs) ->
    {batches, create_batches(Records,
                             estimated_row_count(SampleSize, MaxBatch))};
pick_batch_option(false, _, Records, _, _) ->
    {individual, Records}.

estimated_row_count(SampleRowSize, MaxBatchSize) ->
    %% Assume some rows will be larger, so introduce a fudge factor of
    %% roughly 10 percent.
    RowSizeFudged = (SampleRowSize * 10) div 9,
    MaxBatchSize div RowSizeFudged.

create_batches(Rows, MaxSize) ->
    create_batches(Rows, MaxSize, []).

create_batches([], _MaxSize, Accum) ->
    Accum;
create_batches(Rows, MaxSize, Accum) when length(Rows) < MaxSize ->
    [Rows|Accum];
create_batches(Rows, MaxSize, Accum) ->
    {First, Rest} = lists:split(MaxSize, Rows),
    create_batches(Rest, MaxSize, [First|Accum]).

%% Returns a tuple with a list of request IDs and an error tally
invoke_async_put(BuildRObjFun, AsyncPutFun, _BatchPutFun, {individual, Records}) ->
    lists:map(fun(Record) ->
                      {LK, RObj} = BuildRObjFun(Record),
                      {ok, ReqId} = AsyncPutFun(RObj, LK),
                      ReqId
                end,
              Records);
invoke_async_put(BuildRObjFun, _AsyncPutFun, BatchPutFun, {batches, Batches}) ->
    lists:map(fun(Batch) ->
                      RObjs = lists:map(BuildRObjFun, Batch),
                      {ok, ReqId} = BatchPutFun(RObjs),
                      ReqId
                end,
              Batches).
%%%%%%%%



-spec get_data([riak_pb_ts_codec:ldbvalue()], binary()) ->
                      {ok, {[binary()], [[riak_pb_ts_codec:ldbvalue()]]}} | {error, term()}.
get_data(Key, Table) ->
    get_data(Key, Table, undefined, []).

-spec get_data([riak_pb_ts_codec:ldbvalue()], binary(), module()) ->
                      {ok, {[binary()], [[riak_pb_ts_codec:ldbvalue()]]}} | {error, term()}.
get_data(Key, Table, Mod) ->
    get_data(Key, Table, Mod, []).

-spec get_data([riak_pb_ts_codec:ldbvalue()], binary(), module(), proplists:proplist()) ->
                      {ok, [{binary(), riak_pb_ts_codec:ldbvalue()}]} | {error, term()}.
get_data(Key, Table, Mod0, Options) ->
    Mod =
        case Mod0 of
            undefined ->
                riak_ql_ddl:make_module_name(Table);
            Mod0 ->
                Mod0
        end,
    DDL = Mod:get_ddl(),
    Result =
        case riak_kv_ts_util:make_ts_keys(Key, DDL, Mod) of
            {ok, PKLK} ->
                riak_client:get(
                  riak_kv_ts_util:table_to_bucket(Table), PKLK, Options,
                  {riak_client, [node(), undefined]});
            ErrorReason ->
                ErrorReason
        end,
    case Result of
        {ok, RObj} ->
            case riak_object:get_value(RObj) of
                [] ->
                    {error, notfound};
                Record ->
                    {ok, Record}
            end;
        ErrorReason2 ->
            ErrorReason2
    end.


-spec delete_data([any()], riak_object:bucket()) ->
                         ok | {error, term()}.
delete_data(Key, Table) ->
    delete_data(Key, Table, undefined, [], undefined).

-spec delete_data([any()], riak_object:bucket(), module()) ->
                         ok | {error, term()}.
delete_data(Key, Table, Mod) ->
    delete_data(Key, Table, Mod, [], undefined).

-spec delete_data([any()], riak_object:bucket(), module(), proplists:proplist()) ->
                         ok | {error, term()}.
delete_data(Key, Table, Mod, Options) ->
    delete_data(Key, Table, Mod, Options, undefined).

-spec delete_data([any()], riak_object:bucket(), module(), proplists:proplist(),
                  undefined | vclock:vclock()) ->
                         ok | {error, term()}.
delete_data(Key, Table, Mod0, Options0, VClock0) ->
    Mod =
        case Mod0 of
            undefined ->
                riak_ql_ddl:make_module_name(Table);
            Mod0 ->
                Mod0
        end,
    %% Pass the {dw,all} option in to the delete FSM
    %% to make sure all tombstones are written by the
    %% async put before the reaping get runs otherwise
    %% if the default {dw,quorum} is used there is the
    %% possibility that the last tombstone put overlaps
    %% inside the KV vnode with the reaping get and
    %% prevents the tombstone removal.
    Options = lists:keystore(dw, 1, Options0, {dw, all}),
    DDL = Mod:get_ddl(),
    VClock =
        case VClock0 of
            undefined ->
                %% this will trigger a get in riak_kv_delete:delete to
                %% retrieve the actual vclock
                undefined;
            VClock0 ->
                %% else, clients may have it already (e.g., from an
                %% earlier riak_object:get), which will short-circuit
                %% to avoid a separate get
                riak_object:decode_vclock(VClock0)
        end,
    Result =
        case riak_kv_ts_util:make_ts_keys(Key, DDL, Mod) of
            {ok, PKLK} ->
                riak_client:delete_vclock(
                  riak_kv_ts_util:table_to_bucket(Table), PKLK, VClock, Options,
                  {riak_client, [node(), undefined]});
            ErrorReason ->
                ErrorReason
        end,
    Result.




-spec compile_to_per_quantum_queries(module(), ?SQL_SELECT{}) ->
                                            {ok, [?SQL_SELECT{}]} | {error, any()}.
%% @doc Break up a query into a list of per-quantum queries
compile_to_per_quantum_queries(Mod, SQL) ->
    case catch Mod:get_ddl() of
        {_, {undef, _}} ->
            {error, no_helper_module};
        DDL ->
            case riak_ql_ddl:is_query_valid(
                   Mod, DDL, riak_kv_ts_util:sql_record_to_tuple(SQL)) of
                true ->
                    riak_kv_qry_compiler:compile(DDL, SQL, undefined);
                {false, _Errors} ->
                    {error, invalid_query}
            end
    end.
