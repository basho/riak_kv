%% -------------------------------------------------------------------
%%
%% riak_kv_ts_svc.erl: Riak TS PB/TTB message handler services common
%%                     code
%%
%% Copyright (c) 2016 Basho Technologies, Inc.
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
%% @doc Common code for callbacks for TS TCP messages [codes 90..104]

-module(riak_kv_ts_svc).

-include_lib("riak_pb/include/riak_kv_pb.hrl").
-include_lib("riak_pb/include/riak_ts_pb.hrl").

-include("riak_kv_ts.hrl").
-include("riak_kv_ts_svc.hrl").
-include("riak_kv_wm_raw.hrl").

-export([decode_query_common/2,
         process/2,
         process_stream/3]).

-type ts_requests() :: #tsputreq{} | #tsdelreq{} | #tsgetreq{} |
                       #tslistkeysreq{} | #tsqueryreq{}.
-type ts_responses() :: #tsputresp{} | #tsdelresp{} | #tsgetresp{} |
                        #tslistkeysresp{} | #tsqueryresp{} | #rpberrorresp{}.
-type ts_query_types() :: ?DDL{} | ?SQL_SELECT{} | #riak_sql_describe_v1{} |
                          #riak_sql_insert_v1{}.

-type process_retval() :: {reply, RpbOrTsMessage::tuple(), #state{}}.

decode_query_common(Q, Cover) ->
    %% convert error returns to ok's, this means it will be passed into
    %% process which will not process it and return the error.
    case catch decode_query(Q, Cover) of
        {ok, DecodedQuery} ->
            PermAndTarget = decode_query_permissions(DecodedQuery),
            {ok, DecodedQuery, PermAndTarget};
        {error, Error} ->
            {ok, make_decoder_error_response(Error)};
        {'EXIT', {Error, _}} ->
            {ok, make_decoder_error_response(Error)}
    end.

-spec decode_query(Query::#tsinterpolation{}, Cover::term()) ->
    {error, _} | {ok, ts_query_types()}.
decode_query(#tsinterpolation{ base = BaseQuery }, Cover) ->
    Lexed = riak_ql_lexer:get_tokens(binary_to_list(BaseQuery)),
    case riak_ql_parser:ql_parse(Lexed) of
        {select, SQL} ->
            riak_kv_ts_util:build_sql_record(select, SQL, Cover);
        {describe, SQL} ->
            riak_kv_ts_util:build_sql_record(describe, SQL, Cover);
        {insert, SQL} ->
            riak_kv_ts_util:build_sql_record(insert, SQL, Cover);
        {ddl, DDL, WithProperties} ->
            {ok, {DDL, WithProperties}};
        Other ->
            Other
    end.

-spec decode_query_permissions(ts_query_types()) ->
                                      {string(), binary()}.
decode_query_permissions({?DDL{table = NewBucketType}, _WithProps}) ->
    {"riak_kv.ts_create_table", NewBucketType};
decode_query_permissions(?SQL_SELECT{'FROM' = Table}) ->
    {"riak_kv.ts_query", Table};
decode_query_permissions(#riak_sql_describe_v1{'DESCRIBE' = Table}) ->
    {"riak_kv.ts_describe", Table};
decode_query_permissions(#riak_sql_insert_v1{'INSERT' = Table}) ->
    {"riak_kv.ts_insert", Table}.


-spec process(atom() | ts_requests() | ts_query_types(), #state{}) ->
                     {reply, ts_responses(), #state{}}.
process(#rpberrorresp{} = Error, State) ->
    {reply, Error, State};

process(M = #tsputreq{table = Table}, State) ->
    check_table_and_call(Table, fun sub_tsputreq/4, M, State);

process(M = #tsgetreq{table = Table}, State) ->
    check_table_and_call(Table, fun sub_tsgetreq/4, M, State);

process(M = #tsdelreq{table = Table}, State) ->
    check_table_and_call(Table, fun sub_tsdelreq/4, M, State);

process(M = #tslistkeysreq{table = Table}, State) ->
    check_table_and_call(Table, fun sub_tslistkeysreq/4, M, State);

%% No support yet for replacing coverage components; we'll ignore any
%% value provided for replace_cover
process(M = #tscoveragereq{table = Table}, State) ->
    check_table_and_call(Table, fun sub_tscoveragereq/4, M, State);

%% this is tsqueryreq, subdivided per query type in its SQL
process({DDL = ?DDL{}, WithProperties}, State) ->
    %% the only one that doesn't require an activated table
    create_table({DDL, WithProperties}, State);

process(M = ?SQL_SELECT{'FROM' = Table}, State) ->
    check_table_and_call(Table, fun sub_tsqueryreq/4, M, State);

process(M = #riak_sql_describe_v1{'DESCRIBE' = Table}, State) ->
    check_table_and_call(Table, fun sub_tsqueryreq/4, M, State);

process(M = #riak_sql_insert_v1{'INSERT' = Table}, State) ->
    check_table_and_call(Table, fun sub_tsqueryreq/4, M, State).

%% There is no two-tuple variants of process_stream for tslistkeysresp
%% as TS list_keys senders always use backpressure.
process_stream({ReqId, done}, ReqId,
               State = #state{req = #tslistkeysreq{}, req_ctx = ReqId}) ->
    {done, #tslistkeysresp{done = true}, State};

process_stream({ReqId, From, {keys, []}}, ReqId,
               State = #state{req = #tslistkeysreq{}, req_ctx = ReqId}) ->
    riak_kv_keys_fsm:ack_keys(From),
    {ignore, State};

process_stream({ReqId, From, {keys, CompoundKeys}}, ReqId,
               State = #state{req = #tslistkeysreq{},
                              req_ctx = ReqId,
                              column_info = ColumnInfo}) ->
    riak_kv_keys_fsm:ack_keys(From),
    Keys = riak_pb_ts_codec:encode_rows(
             ColumnInfo, [tuple_to_list(sext:decode(A))
                          || A <- CompoundKeys, A /= []]),
    {reply, #tslistkeysresp{keys = Keys, done = false}, State};

process_stream({ReqId, {error, Error}}, ReqId,
               #state{req = #tslistkeysreq{}, req_ctx = ReqId}) ->
    {error, {format, Error}, #state{}};
process_stream({ReqId, Error}, ReqId,
               #state{req = #tslistkeysreq{}, req_ctx = ReqId}) ->
    {error, {format, Error}, #state{}}.


%% ---------------------------------
%% create_table, the only function for which we don't do
%% check_table_and_call

-spec create_table({?DDL{}, proplists:proplist()}, #state{}) ->
                          {reply, #tsqueryresp{} | #rpberrorresp{}, #state{}}.
create_table({DDL = ?DDL{table = Table}, WithProps}, State) ->
    {ok, Props1} = riak_kv_ts_util:apply_timeseries_bucket_props(DDL, WithProps),
    case catch [riak_kv_wm_utils:erlify_bucket_prop(P) || P <- Props1] of
        {bad_linkfun_modfun, {M, F}} ->
            {reply, table_create_fail_response(
                      Table, flat_format(
                               "Invalid link mod or fun in bucket type properties: ~p:~p\n", [M, F])),
             State};
        {bad_linkfun_bkey, {B, K}} ->
            {reply, table_create_fail_response(
                      Table, flat_format(
                               "Malformed bucket/key for anon link fun in bucket type properties: ~p/~p\n", [B, K])),
             State};
        {bad_chash_keyfun, {M, F}} ->
            {reply, table_create_fail_response(
                      Table, flat_format(
                               "Invalid chash mod or fun in bucket type properties: ~p:~p\n", [M, F])),
             State};
        Props2 ->
            case riak_core_bucket_type:create(Table, Props2) of
                ok ->
                    wait_until_active(Table, State, ?TABLE_ACTIVATE_WAIT);
                {error, Reason} ->
                    {reply, table_create_fail_response(Table, Reason), State}
            end
    end.

wait_until_active(Table, State, 0) ->
    {reply, table_activate_fail_response(Table), State};
wait_until_active(Table, State, Seconds) ->
    case riak_core_bucket_type:activate(Table) of
        ok ->
            {reply, #tsqueryresp{}, State};
        {error, not_ready} ->
            timer:sleep(1000),
            wait_until_active(Table, State, Seconds - 1);
        {error, undefined} ->
            %% this is inconceivable because create(Table) has
            %% just succeeded, so it's here mostly to pacify
            %% the dialyzer (and of course, for the odd chance
            %% of Erlang imps crashing nodes between create
            %% and activate calls)
            {reply, table_created_missing_response(Table), State}
    end.


%% ---------------------------------------------------
%% functions called from check_table_and_call, one per ts* request
%% ---------------------------------------------------

%%
%% INSERT statements, called from check_table_and_call.
%%
-spec make_insert_response(module(), #riak_sql_insert_v1{}) -> tsqueryresp | #rpberrorresp{}.
make_insert_response(Mod, #riak_sql_insert_v1{'INSERT' = Table, fields = Fields, values = Values}) ->
    case lookup_field_positions(Mod, Fields) of
    {ok, Positions} ->
        Empty = make_empty_row(Mod),
        case xlate_insert_to_putdata(Values, Positions, Empty) of
            {error, ValueReason} ->
                make_rpberrresp(?E_BAD_QUERY, ValueReason);
            {ok, Data} ->
                insert_putreqs(Mod, Table, Data)
            end;
    {error, FieldReason} ->
        make_rpberrresp(?E_BAD_QUERY, FieldReason)
    end.

insert_putreqs(Mod, Table, Data) ->
    case catch validate_rows(Mod, Data) of
        [] ->
            case put_data(Data, Table, Mod) of
                0 ->
                    tsqueryresp;
                ErrorCount ->
                    failed_put_response(ErrorCount)
            end;
        BadRowIdxs when is_list(BadRowIdxs) ->
            validate_rows_error_response(BadRowIdxs)
    end.

%%
%% Return an all-null empty row ready to be populated by the values
%%
-spec make_empty_row(module()) -> tuple(undefined).
make_empty_row(Mod) ->
    Positions = Mod:get_field_positions(),
    list_to_tuple(lists:duplicate(length(Positions), undefined)).

%%
%% Lookup the index of the field names selected to insert.
%%
%% This *requires* that once schema changes take place the DDL fields are left in order.
%%
-spec lookup_field_positions(module(), [riak_ql_ddl:field_identifier()]) ->
                           {ok, [pos_integer()]} | {error, string()}.
lookup_field_positions(Mod, FieldIdentifiers) ->
    case lists:foldl(
           fun({identifier, FieldName}, {Good, Bad}) ->
                   case Mod:is_field_valid(FieldName) of
               false ->
                   {Good, [FieldName | Bad]};
               true ->
                   {[Mod:get_field_position(FieldName) | Good], Bad}
                   end
           end, {[], []}, FieldIdentifiers)
    of
        {Positions, []} ->
            {ok, lists:reverse(Positions)};
        {_, Errors} ->
            {error, flat_format("undefined fields: ~s",
                                [string:join(lists:reverse(Errors), ", ")])}
    end.

%%
%% Map the list of values from statement order into the correct place in the tuple.
%% If there are less values given than the field list the NULL will carry through
%% and the general validation rules should pick that up.
%% If there are too many values given for the fields it returns an error.
%%
-spec xlate_insert_to_putdata([[riak_ql_ddl:data_value()]], [pos_integer()], tuple(undefined)) ->
                              {ok, [tuple()]} | {error, string()}.
xlate_insert_to_putdata(Values, Positions, Empty) ->
    ConvFn = fun(RowVals, {Good, Bad, RowNum}) ->
                 case make_insert_row(RowVals, Positions, Empty) of
                     {ok, Row} ->
                         {[Row | Good], Bad, RowNum + 1};
                     {error, _Reason} ->
                         {Good, [integer_to_list(RowNum) | Bad], RowNum + 1}
                 end
             end,
    Converted = lists:foldl(ConvFn, {[], [], 1}, Values),
    case Converted of
        {PutData, [], _} ->
            {ok, lists:reverse(PutData)};
        {_, Errors, _} ->
            {error, flat_format("too many values in row index(es) ~s",
                                [string:join(lists:reverse(Errors), ", ")])}
    end.

-spec make_insert_row([] | [riak_ql_ddl:data_value()], [] | [pos_integer()], tuple()) ->
                      {ok, tuple()} | {error, string()}.
make_insert_row([], _Positions, Row) when is_tuple(Row) ->
    %% Out of entries in the value - row is populated with default values
    %% so if we run out of data for implicit/explicit fieldnames can just return
    {ok, Row};
make_insert_row(_, [], Row) when is_tuple(Row) ->
    %% Too many values for the field
    {error, "too many values"};
%% Make sure the types match
make_insert_row([{_Type, Val} | Values], [Pos | Positions], Row) when is_tuple(Row) ->
    make_insert_row(Values, Positions, setelement(Pos, Row, Val)).


%% put
%% NB: since this method deals with PB and TTB messages, the message must be fully
%% decoded before sub_tsqueryreq is called
sub_tsputreq(Mod, _DDL, #tsputreq{table = Table, rows = Rows},
             State) ->
    case catch validate_rows(Mod, Rows) of
        [] ->
            case put_data(Rows, Table, Mod) of
                0 ->
                    {reply, #tsputresp{}, State};
                ErrorCount ->
                    {reply, failed_put_response(ErrorCount), State}
            end;
        BadRowIdxs when is_list(BadRowIdxs) ->
            {reply, validate_rows_error_response(BadRowIdxs), State}
    end.

%% Give validate_rows/2 a DDL Module and a list of decoded rows,
%% and it will return a list of strings that represent the invalid rows indexes.
-spec validate_rows(module(), list(tuple())) -> list(string()).
validate_rows(Mod, Rows) ->
    ValidateFn = fun(X, {Acc, BadRowIdxs}) ->
        case Mod:validate_obj(X) of
            true -> {Acc+1, BadRowIdxs};
            _ -> {Acc+1, [integer_to_list(Acc) | BadRowIdxs]}
        end
    end,
    {_, BadRowIdxs} = lists:foldl(ValidateFn, {1, []}, Rows),
    lists:reverse(BadRowIdxs).


-spec put_data([riak_pb_ts_codec:tsrow()], binary(), module()) -> integer().
%% return count of records we failed to put
put_data(Data, Table, Mod) when is_binary(Table) ->
    DDL = Mod:get_ddl(),
    Bucket = riak_kv_ts_util:table_to_bucket(Table),
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
                                                         RObj, W, PW, Bucket, NVal, LK,
                                                         EncodeFn, Preflist)
                                               end,
                                               fun(RObjs) ->
                                                       riak_kv_w1c_worker:ts_batch_put(
                                                         RObjs, W, PW, Bucket, NVal,
                                                         EncodeFn, Preflist)
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
        [ { riak_core_util:chash_key({Bucket, row_to_key(R, DDL, Mod)},
                                     BucketProps), R } || R <- Data ],
    dict:to_list(
      lists:foldl(fun({Idx, R}, Dict) ->
                          dict:append(Idx, R, Dict)
                  end,
                  dict:new(),
                  PartitionTuples)).

row_to_key(Row, DDL, Mod) ->
    riak_kv_ts_util:encode_typeval_key(
      riak_ql_ddl:get_partition_key(DDL, Row, Mod)).

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
%%%%%%%%

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


%% get
%% NB: since this method deals with PB and TTB messages, the message must be fully
%% decoded before sub_tsqueryreq is called
sub_tsgetreq(Mod, DDL, #tsgetreq{table = Table,
                                 key    = CompoundKey,
                                 timeout = Timeout},
             State) ->
    Options =
        if Timeout == undefined -> [];
           true -> [{timeout, Timeout}]
        end,

    Result =
        case riak_kv_ts_util:make_ts_keys(CompoundKey, DDL, Mod) of
            {ok, PKLK} ->
                riak_client:get(
                  riak_kv_ts_util:table_to_bucket(Table), PKLK, Options,
                  {riak_client, [node(), undefined]});
            ErrorReason ->
                ErrorReason
        end,
    case Result of
        {ok, RObj} ->
            Record = riak_object:get_value(RObj),
            {ColumnNames, Row} = lists:unzip(Record),
            %% the columns stored in riak_object are just
            %% names; we need names with types, so:
            ColumnTypes = get_column_types(ColumnNames, Mod),
            {reply, {tsgetresp, {ColumnNames, ColumnTypes, [Row]}}, State};
        {error, {bad_key_length, Got, Need}} ->
            {reply, key_element_count_mismatch(Got, Need), State};
        {error, notfound} ->
            {reply, make_rpberrresp(?E_NOTFOUND, "notfound"), State};
        {error, Reason} ->
            {reply, make_rpberrresp(?E_GET, to_string(Reason)), State}
    end.


%% delete
sub_tsdelreq(Mod, DDL, #tsdelreq{table = Table,
                                 key    = PbCompoundKey,
                                 vclock  = PbVClock,
                                 timeout  = Timeout},
             State) ->
    %% Pass the {dw,all} option in to the delete FSM
    %% to make sure all tombstones are written by the
    %% async put before the reaping get runs otherwise
    %% if the default {dw,quorum} is used there is the
    %% possibility that the last tombstone put overlaps
    %% inside the KV vnode with the reaping get and
    %% prevents the tombstone removal.
    Options =
        if Timeout == undefined -> [{dw, all}];
           true -> [{timeout, Timeout}, {dw, all}]
        end,
    VClock =
        case PbVClock of
            undefined ->
                %% this will trigger a get in riak_kv_delete:delete to
                %% retrieve the actual vclock
                undefined;
            PbVClock ->
                %% else, clients may have it already (e.g., from an
                %% earlier riak_object:get), which will short-circuit
                %% to avoid a separate get
                riak_object:decode_vclock(PbVClock)
        end,

    CompoundKey = riak_pb_ts_codec:decode_cells(PbCompoundKey),

    Result =
        case riak_kv_ts_util:make_ts_keys(CompoundKey, DDL, Mod) of
            {ok, PKLK} ->
                riak_client:delete_vclock(
                  riak_kv_ts_util:table_to_bucket(Table), PKLK, VClock, Options,
                  {riak_client, [node(), undefined]});
            ErrorReason ->
                ErrorReason
        end,
    case Result of
        ok ->
            {reply, tsdelresp, State};
        {error, {bad_key_length, Got, Need}} ->
            {reply, key_element_count_mismatch(Got, Need), State};
        {error, notfound} ->
            {reply, make_rpberrresp(?E_NOTFOUND, "notfound"), State};
        {error, Reason} ->
            {reply, failed_delete_response(Reason), State}
    end.


%% -----------
%% listkeys
%% -----------

sub_tslistkeysreq(Mod, DDL, #tslistkeysreq{table = Table,
                                           timeout = Timeout} = Req,
                  State) ->
    Result =
        riak_client:stream_list_keys(
          riak_kv_ts_util:table_to_bucket(Table), Timeout,
          {riak_client, [node(), undefined]}),
    case Result of
        {ok, ReqId} ->
            ColumnInfo =
                [Mod:get_field_type(N)
                 || #param_v1{name = N} <- DDL?DDL.local_key#key_v1.ast],
            {reply, {stream, ReqId}, State#state{req = Req, req_ctx = ReqId,
                                                 column_info = ColumnInfo}};
        {error, Reason} ->
            {reply, failed_listkeys_response(Reason), State}
    end.


%% -----------
%% coverage
%% -----------

sub_tscoveragereq(Mod, _DDL, #tscoveragereq{table = Table,
                                            query = Q},
                  State) ->
    case compile(Mod, catch decode_query(Q, tscoveragereq)) of
        {error, #rpberrorresp{} = Error} ->
            {reply, Error, State};
        {error, _} ->
            {reply, make_rpberrresp(
                      ?E_BAD_QUERY, "Failed to compile query"),
             State};
        SQL ->
            %% SQL is a list of queries (1 per quantum)
            Bucket = riak_kv_ts_util:table_to_bucket(Table),
            Client = {riak_client, [node(), undefined]},
            convert_cover_list(sql_to_cover(Client, SQL, Bucket, []), State)
    end.

%% Copied and modified from riak_kv_pb_coverage:convert_list. Would
%% be nice to collapse them back together, probably with a closure,
%% but time and effort.
convert_cover_list({error, Error}, State) ->
    {error, Error, State};
convert_cover_list(Results, State) ->
    %% Pull hostnames & ports
    %% Wrap each element of this list into a rpbcoverageentry
    Resp = #tscoverageresp{
              entries =
                  [begin
                       Node = proplists:get_value(node, Cover),
                       {IP, Port} = riak_kv_pb_coverage:node_to_pb_details(Node),
                       #tscoverageentry{
                          cover_context = riak_kv_pb_coverage:term_to_checksum_binary(
                                            {Cover, Range}),
                          ip = IP, port = Port,
                          range = assemble_ts_range(Range, SQLtext)
                         }
                   end || {Cover, Range, SQLtext} <- Results]
             },
    {reply, Resp, State}.

assemble_ts_range({FieldName, {{StartVal, StartIncl}, {EndVal, EndIncl}}}, Text) ->
    #tsrange{
       field_name = FieldName,
       lower_bound = StartVal,
       lower_bound_inclusive = StartIncl,
       upper_bound = EndVal,
       upper_bound_inclusive = EndIncl,
       desc = Text
      }.


%% Result from riak_client:get_cover is a nested list of coverage plan
%% because KV coverage requests are designed that way, but in our case
%% all we want is the singleton head

%% If any of the results from get_cover are errors, we want that tuple
%% to be the sole return value
sql_to_cover(_Client, [], _Bucket, Accum) ->
    lists:reverse(Accum);
sql_to_cover(Client, [SQL|Tail], Bucket, Accum) ->
    case Client:get_cover(riak_kv_qry_coverage_plan, Bucket, undefined,
                          {SQL, Bucket}) of
        {error, Error} ->
            {error, Error};
        [Cover] ->
            {Description, RangeReplacement} = reverse_sql(SQL),
            sql_to_cover(Client, Tail, Bucket, [{Cover, RangeReplacement,
                                                 Description}|Accum])
    end.

%% Generate a human-readable description of the target
%%     <<"<TABLE> / time > X and time < Y">>
%% Generate a start/end timestamp for future replacement in a query
reverse_sql(?SQL_SELECT{'FROM'  = Table,
                        'WHERE' = KeyProplist,
                        partition_key = PartitionKey}) ->
    QuantumField = identify_quantum_field(PartitionKey),
    RangeTuple = extract_time_boundaries(QuantumField, KeyProplist),
    Desc = derive_description(Table, QuantumField, RangeTuple),
    ReplacementValues = {QuantumField, RangeTuple},
    {Desc, ReplacementValues}.


derive_description(Table, Field, {{Start, StartInclusive}, {End, EndInclusive}}) ->
    StartOp = pick_operator(">", StartInclusive),
    EndOp = pick_operator("<", EndInclusive),
    unicode:characters_to_binary(
      flat_format("~ts / ~ts ~s ~B and ~ts ~s ~B",
                  [Table, Field, StartOp, Start,
                   Field, EndOp, End]), utf8).

pick_operator(LGT, true) ->
    LGT ++ "=";
pick_operator(LGT, false) ->
    LGT.

extract_time_boundaries(FieldName, WhereList) ->
    {FieldName, timestamp, Start} =
        lists:keyfind(FieldName, 1, proplists:get_value(startkey, WhereList, [])),
    {FieldName, timestamp, End} =
        lists:keyfind(FieldName, 1, proplists:get_value(endkey, WhereList, [])),
    StartInclusive = proplists:get_value(start_inclusive, WhereList, true),
    EndInclusive = proplists:get_value(end_inclusive, WhereList, false),
    {{Start, StartInclusive}, {End, EndInclusive}}.


%%%%%%%%%%%%
%% FRAGILE HORRIBLE BAD BAD BAD AST MANGLING
identify_quantum_field(#key_v1{ast = KeyList}) ->
    HashFn = find_hash_fn(KeyList),
    P_V1 = hd(HashFn#hash_fn_v1.args),
    hd(P_V1#param_v1.name).

find_hash_fn([]) ->
    throw(wtf);
find_hash_fn([#hash_fn_v1{}=Hash|_T]) ->
    Hash;
find_hash_fn([_H|T]) ->
    find_hash_fn(T).

%%%%%%%%%%%%


compile(_Mod, {error, Err}) ->
    {error, make_decoder_error_response(Err)};
compile(_Mod, {'EXIT', {Err, _}}) ->
    {error, make_decoder_error_response(Err)};
compile(Mod, {ok, ?SQL_SELECT{}=SQL}) ->
    case (catch Mod:get_ddl()) of
        {_, {undef, _}} ->
            {error, no_helper_module};
        DDL ->
            case riak_ql_ddl:is_query_valid(Mod, DDL,
                                            riak_kv_ts_util:sql_record_to_tuple(SQL)) of
                true ->
                    case riak_kv_qry_compiler:compile(DDL, SQL, undefined) of
                        {error,_} = Error ->
                            Error;
                        {ok, Queries} ->
                            Queries
                    end;
                {false, _Errors} ->
                    {error, invalid_query}
            end
    end.

%% query
%% NB: since this method deals with PB and TTB messages, the message must be fully
%% decoded before sub_tsqueryreq is called
-spec sub_tsqueryreq(module(), #ddl_v1{},
                     ?SQL_SELECT{} | #riak_sql_describe_v1{} | #riak_sql_insert_v1{},
                     #state{}) ->
                     {reply, tsqueryresp | #rpberrorresp{}, #state{}}.
sub_tsqueryreq(Mod, DDL, SQL, State) ->
    case riak_kv_qry:submit(SQL, DDL) of
        {ok, Data}  ->
            {reply, make_tsquery_resp(Mod, SQL, Data), State};

        %% parser messages have a tuple for Reason:
        {error, {E, Reason}} when is_atom(E), is_binary(Reason) ->
            ErrorMessage = flat_format("~p: ~s", [E, Reason]),
            {reply, make_rpberrresp(?E_SUBMIT, ErrorMessage), State};

        %% the following timeouts are known and distinguished:
        {error, qry_worker_timeout} ->
            %% the eleveldb process didn't send us any response after
            %% 10 sec (hardcoded in riak_kv_qry), and probably died
            {reply, make_rpberrresp(?E_TIMEOUT, "no response from backend"), State};
        {error, backend_timeout} ->
            %% the eleveldb process did manage to send us a timeout
            %% response
            {reply, make_rpberrresp(?E_TIMEOUT, "backend timeout"), State};

        {error, Reason} ->
            {reply, make_rpberrresp(?E_SUBMIT, to_string(Reason)), State}
    end.

make_tsquery_resp(_Mod, ?SQL_SELECT{}, Data) ->
    make_tsqueryresp(Data);
make_tsquery_resp(_Mod, #riak_sql_describe_v1{}, Data) ->
    make_describe_response(Data);
make_tsquery_resp(Mod, SQL = #riak_sql_insert_v1{}, _Data) ->
    make_insert_response(Mod, SQL).

%% ---------------------------------------------------
%% local functions
%% ---------------------------------------------------

-spec check_table_and_call(Table::binary(),
                           WorkItem::fun((module(), ?DDL{},
                                          OrigMessage::tuple(), #state{}) ->
                                                process_retval()),
                           OrigMessage::tuple(),
                           #state{}) ->
                                  process_retval().
%% Check that Table is good wrt TS operations and call a specified
%% function with its Mod and DDL; generate an appropriate
%% #rpberrorresp{} if a corresponding bucket type has not been
%% actvated or Table has no DDL (not a TS bucket). Otherwise,
%% transparently call the WorkItem function.
check_table_and_call(Table, Fun, TsMessage, State) ->
    case riak_kv_ts_util:get_table_ddl(Table) of
        {ok, Mod, DDL} ->
            Fun(Mod, DDL, TsMessage, State);
        {error, missing_helper_module} ->
            BucketProps = riak_core_bucket:get_bucket(
                            riak_kv_ts_util:table_to_bucket(Table)),
            {reply, missing_helper_module(Table, BucketProps), State};
        {error, _} ->
            {reply, table_not_activated_response(Table),
             State}
    end.



%%
-spec make_rpberrresp(integer(), string()) -> #rpberrorresp{}.
make_rpberrresp(Code, Message) ->
    #rpberrorresp{errcode = Code,
                  errmsg = lists:flatten(Message)}.

%%
-spec missing_helper_module(Table::binary(),
                            BucketProps::{error, any()} | [proplists:property()])
                           -> #rpberrorresp{}.
missing_helper_module(Table, {error, _}) ->
    missing_type_response(Table);
missing_helper_module(Table, BucketProps)
  when is_binary(Table), is_list(BucketProps) ->
    case lists:keymember(ddl, 1, BucketProps) of
        true  -> missing_table_module_response(Table);
        false -> not_timeseries_type_response(Table)
    end.

%%
-spec missing_type_response(Table::binary()) -> #rpberrorresp{}.
missing_type_response(Table) ->
    make_rpberrresp(
      ?E_MISSING_TYPE,
      flat_format("Time Series table ~s does not exist.", [Table])).

%%
-spec not_timeseries_type_response(Table::binary()) -> #rpberrorresp{}.
not_timeseries_type_response(Table) ->
    make_rpberrresp(
      ?E_NOT_TS_TYPE,
      flat_format("Attempt Time Series operation on non Time Series table ~s.", [Table])).

-spec missing_table_module_response(Table::binary()) -> #rpberrorresp{}.
missing_table_module_response(Table) ->
    make_rpberrresp(
      ?E_MISSING_TS_MODULE,
      flat_format("The compiled module for Time Series table ~s cannot be loaded.", [Table])).

-spec key_element_count_mismatch(Got::integer(), Need::integer()) -> #rpberrorresp{}.
key_element_count_mismatch(Got, Need) ->
    make_rpberrresp(
      ?E_BAD_KEY_LENGTH,
      flat_format("Key element count mismatch (key has ~b elements but ~b supplied).", [Need, Got])).

-spec validate_rows_error_response([string()]) ->#rpberrorresp{}.
validate_rows_error_response(BadRowIdxs) ->
    BadRowsString = string:join(BadRowIdxs,", "),
    make_rpberrresp(
      ?E_IRREG,
      flat_format("Invalid data found at row index(es) ~s", [BadRowsString])).

failed_put_response(ErrorCount) ->
    make_rpberrresp(
      ?E_PUT,
      flat_format("Failed to put ~b record(s)", [ErrorCount])).

failed_delete_response(Reason) ->
    make_rpberrresp(
      ?E_DELETE,
      flat_format("Failed to delete record: ~p", [Reason])).

failed_listkeys_response(Reason) ->
    make_rpberrresp(
      ?E_LISTKEYS,
      flat_format("Failed to list keys: ~p", [Reason])).

table_create_fail_response(Table, Reason) ->
    make_rpberrresp(
      ?E_CREATE, flat_format("Failed to create table ~s: ~p", [Table, Reason])).

table_activate_fail_response(Table) ->
    make_rpberrresp(
      ?E_ACTIVATE,
      flat_format("Failed to activate table ~s", [Table])).

table_not_activated_response(Table) ->
    make_rpberrresp(
      ?E_TABLE_INACTIVE,
      flat_format("~ts is not an active table.", [Table])).

table_created_missing_response(Table) ->
    make_rpberrresp(
      ?E_CREATED_GHOST,
      flat_format("Table ~s has been created but found missing", [Table])).

to_string(X) ->
    flat_format("~p", [X]).

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

%% helpers to make various responses

-spec make_tsqueryresp([] | {[riak_pb_ts_codec:tscolumnname()],
                             [riak_pb_ts_codec:tscolumntype()],
                             [[riak_pb_ts_codec:ldbvalue()]]}) -> {tsqueryresp, {}}.
make_tsqueryresp({_, _, []}) ->
    {tsqueryresp, {[], [], []}};
make_tsqueryresp({ColumnNames, ColumnTypes, Rows}) ->
    {tsqueryresp, {ColumnNames, ColumnTypes, Rows}}.

-spec make_describe_response([[term()]]) -> {tsqueryresp, {}}.
make_describe_response(Rows) ->
    ColumnNames = [<<"Column">>, <<"Type">>, <<"Is Null">>, <<"Primary Key">>, <<"Local Key">>],
    ColumnTypes = [   varchar,     varchar,     boolean,        sint64,             sint64    ],
    {tsqueryresp, {ColumnNames, ColumnTypes, Rows}}.

-spec get_column_types(list(binary()), module()) -> [riak_pb_ts_codec:tscolumntype()].
get_column_types(ColumnNames, Mod) ->
    [Mod:get_field_type([N]) || N <- ColumnNames].

make_decoder_error_response({LineNo, riak_ql_parser, Msg}) when is_integer(LineNo) ->
    make_rpberrresp(?E_PARSE_ERROR, flat_format("~ts", [Msg]));
make_decoder_error_response({Token, riak_ql_parser, _}) when is_binary(Token) ->
    make_rpberrresp(?E_PARSE_ERROR, flat_format("Unexpected token '~s'", [Token]));
make_decoder_error_response({Token, riak_ql_parser, _}) ->
    make_rpberrresp(?E_PARSE_ERROR, flat_format("Unexpected token '~p'", [Token]));
make_decoder_error_response(Error) ->
    Error.

flat_format(Format, Args) ->
    lists:flatten(io_lib:format(Format, Args)).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

missing_helper_module_missing_type_test() ->
    ?assertMatch(
        #rpberrorresp{errcode = ?E_MISSING_TYPE },
        missing_helper_module(<<"mytype">>, {error, any})
    ).

missing_helper_module_not_ts_type_test() ->
    ?assertMatch(
        #rpberrorresp{errcode = ?E_NOT_TS_TYPE },
        missing_helper_module(<<"mytype">>, []) % no ddl property
    ).

%% if the bucket properties exist and they contain a ddl property then
%% the bucket properties are in the correct state but the module is still
%% missing.
missing_helper_module_test() ->
    ?assertMatch(
        #rpberrorresp{errcode = ?E_MISSING_TS_MODULE },
        missing_helper_module(<<"mytype">>, [{ddl, ?DDL{}}])
    ).

test_helper_validate_rows_mod() ->
    {ddl, DDL, []} =
        riak_ql_parser:ql_parse(
          riak_ql_lexer:get_tokens(
            "CREATE TABLE mytable ("
            "family VARCHAR NOT NULL,"
            "series VARCHAR NOT NULL,"
            "time TIMESTAMP NOT NULL,"
            "PRIMARY KEY ((family, series, quantum(time, 1, 'm')),"
            " family, series, time))")),
    riak_ql_ddl_compiler:compile_and_load_from_tmp(DDL).

validate_rows_empty_test() ->
    {module, Mod} = test_helper_validate_rows_mod(),
    ?assertEqual(
        [],
        validate_rows(Mod, [])
    ).

validate_rows_1_test() ->
    {module, Mod} = test_helper_validate_rows_mod(),
    ?assertEqual(
        [],
        validate_rows(Mod, [{<<"f">>, <<"s">>, 11}])
    ).

validate_rows_bad_1_test() ->
    {module, Mod} = test_helper_validate_rows_mod(),
    ?assertEqual(
        ["1"],
        validate_rows(Mod, [{}])
    ).

validate_rows_bad_2_test() ->
    {module, Mod} = test_helper_validate_rows_mod(),
    ?assertEqual(
        ["1", "3", "4"],
        validate_rows(Mod, [{}, {<<"f">>, <<"s">>, 11}, {a, <<"s">>, 12}, "hithere"])
    ).

validate_rows_error_response_1_test() ->
    Msg = "Invalid data found at row index(es) ",
    ?assertEqual(
        #rpberrorresp{errcode = ?E_IRREG,
                      errmsg = Msg ++ "1" },
        validate_rows_error_response(["1"])
    ).

validate_rows_error_response_2_test() ->
    Msg = "Invalid data found at row index(es) ",
    ?assertEqual(
        #rpberrorresp{errcode = ?E_IRREG,
                      errmsg = Msg ++ "1, 2, 3" },
        validate_rows_error_response(["1", "2", "3"])
    ).

batch_1_test() ->
    ?assertEqual(lists:reverse([[1, 2, 3, 4], [5, 6, 7, 8], [9]]),
                 create_batches([1, 2, 3, 4, 5, 6, 7, 8, 9], 4)).

batch_2_test() ->
    ?assertEqual(lists:reverse([[1, 2, 3, 4], [5, 6, 7, 8], [9, 10]]),
                 create_batches([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 4)).

batch_3_test() ->
    ?assertEqual(lists:reverse([[1, 2, 3], [4, 5, 6], [7, 8, 9]]),
                 create_batches([1, 2, 3, 4, 5, 6, 7, 8, 9], 3)).

batch_undersized1_test() ->
    ?assertEqual([[1, 2, 3, 4, 5, 6]],
                 create_batches([1, 2, 3, 4, 5, 6], 6)).

batch_undersized2_test() ->
    ?assertEqual([[1, 2, 3, 4, 5, 6]],
                 create_batches([1, 2, 3, 4, 5, 6], 7)).

batch_almost_undersized_test() ->
    ?assertEqual(lists:reverse([[1, 2, 3, 4, 5], [6]]),
                 create_batches([1, 2, 3, 4, 5, 6], 5)).

validate_make_insert_row_basic_test() ->
    Data = [{integer,4}, {binary,<<"bamboozle">>}, {float, 3.14}],
    Positions = [3, 1, 2],
    Row = {undefined, undefined, undefined},
    Result = make_insert_row(Data, Positions, Row),
    ?assertEqual(
        {ok, {<<"bamboozle">>, 3.14, 4}},
        Result
    ).

validate_make_insert_row_too_many_test() ->
    Data = [{integer,4}, {binary,<<"bamboozle">>}, {float, 3.14}, {integer, 8}],
    Positions = [3, 1, 2],
    Row = {undefined, undefined, undefined},
    Result = make_insert_row(Data, Positions, Row),
    ?assertEqual(
        {error, "too many values"},
        Result
    ).


validate_xlate_insert_to_putdata_ok_test() ->
    Empty = list_to_tuple(lists:duplicate(5, undefined)),
    Values = [[{integer, 4}, {binary, <<"babs">>}, {float, 5.67}, {binary, <<"bingo">>}],
              [{integer, 8}, {binary, <<"scat">>}, {float, 7.65}, {binary, <<"yolo!">>}]],
    Positions = [5, 3, 1, 2, 4],
    Result = xlate_insert_to_putdata(Values, Positions, Empty),
    ?assertEqual(
        {ok,[{5.67,<<"bingo">>,<<"babs">>,undefined,4},
             {7.65,<<"yolo!">>,<<"scat">>,undefined,8}]},
        Result
    ).

validate_xlate_insert_to_putdata_too_many_values_test() ->
    Empty = list_to_tuple(lists:duplicate(5, undefined)),
    Values = [[{integer, 4}, {binary, <<"babs">>}, {float, 5.67}, {binary, <<"bingo">>}, {integer, 7}],
           [{integer, 8}, {binary, <<"scat">>}, {float, 7.65}, {binary, <<"yolo!">>}]],
    Positions = [3, 1, 2, 4],
    Result = xlate_insert_to_putdata(Values, Positions, Empty),
    ?assertEqual(
        {error,"too many values in row index(es) 1"},
        Result
    ).

-endif.
