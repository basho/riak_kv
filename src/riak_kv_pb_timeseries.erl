%% -------------------------------------------------------------------
%%
%% riak_kv_pb_timeseries.erl: Riak TS protobuf callbacks
%%
%% Copyright (c) 2015, 2016 Basho Technologies, Inc.
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
%% @doc Callbacks for TS protobuf messages [codes 90..104]

-module(riak_kv_pb_timeseries).

-include_lib("riak_pb/include/riak_kv_pb.hrl").
-include_lib("riak_pb/include/riak_ts_pb.hrl").
-include_lib("riak_ql/include/riak_ql_ddl.hrl").

-include("riak_kv_wm_raw.hrl").

-behaviour(riak_api_pb_service).

-export([init/0,
         decode/2,
         encode/1,
         process/2,
         process_stream/3]).

%% per RIAK-1437, error codes assigned to TS are in the 1000-1500 range
-define(E_SUBMIT,            1001).
-define(E_FETCH,             1002).
-define(E_IRREG,             1003).
-define(E_PUT,               1004).
-define(E_NOCREATE,          1005).   %% unused
-define(E_NOT_TS_TYPE,       1006).
-define(E_MISSING_TYPE,      1007).
-define(E_MISSING_TS_MODULE, 1008).
-define(E_DELETE,            1009).
-define(E_GET,               1010).
-define(E_BAD_KEY_LENGTH,    1011).
-define(E_LISTKEYS,          1012).
-define(E_TIMEOUT,           1013).
-define(E_CREATE,            1014).
-define(E_CREATED_INACTIVE,  1015).
-define(E_CREATED_GHOST,     1016).
-define(E_ACTIVATE,          1017).
-define(E_BAD_QUERY,         1018).
-define(E_TABLE_INACTIVE,    1019).

-define(FETCH_RETRIES, 10).  %% TODO make it configurable in tsqueryreq
-define(TABLE_ACTIVATE_WAIT, 30). %% ditto

-record(state, {
          req,
          req_ctx,
          column_info
         }).

-type ts_requests() :: #tsputreq{} | #tsttbputreq{} |
                       #tsdelreq{} | #tsgetreq{} | #tslistkeysreq{} | #tsqueryreq{}.
-type ts_responses() :: #tsputresp{} |
                        #tsdelresp{} | #tsgetresp{} | #tslistkeysresp{} | #tsqueryresp{} |
                        #rpberrorresp{}.
-type ts_query_types() :: #ddl_v1{} | ?SQL_SELECT{} | #riak_sql_describe_v1{}.

-type process_retval() :: {reply, RpbOrTsMessage::tuple(), #state{}}.

-spec init() -> any().
init() ->
    #state{}.

-spec decode(integer(), binary()) ->
                    {ok, ts_requests(), {PermSpec::string(), Table::binary()}} |
                    {error, _}.
decode(Code, Bin) ->
    Msg = riak_pb_codec:decode(Code, Bin),
    case Msg of
        #tsqueryreq{query = Q, cover_context = Cover} ->
            case catch decode_query(Q, Cover) of
                {ok, DecodedQuery} ->
                    PermAndTarget = decode_query_permissions(DecodedQuery),
                    {ok, DecodedQuery, PermAndTarget};
                {error, Error} ->
                    {error, decoder_parse_error_resp(Error)};
                {'EXIT', {Error, _}} ->
                    {error, decoder_parse_error_resp(Error)}
            end;
        #tsgetreq{table = Table}->
            {ok, Msg, {"riak_kv.ts_get", Table}};
        #tsputreq{table = Table} ->
            {ok, Msg, {"riak_kv.ts_put", Table}};
        #tsttbputreq{table = Table} ->
            {ok, Msg, {"riak_kv.ts_put", Table}};
        #tsdelreq{table = Table} ->
            {ok, Msg, {"riak_kv.ts_del", Table}};
        #tslistkeysreq{table = Table} ->
            {ok, Msg, {"riak_kv.ts_listkeys", Table}};
        #tscoveragereq{table = Table} ->
            {ok, Msg, {"riak_kv.ts_cover", Table}}
    end.

-spec decode_query(Query::#tsinterpolation{}) ->
    {error, _} | {ok, ts_query_types()}.
decode_query(SQL) ->
    decode_query(SQL, undefined).

-spec decode_query(Query::#tsinterpolation{}, term()) ->
    {error, _} | {ok, ts_query_types()}.
decode_query(#tsinterpolation{ base = BaseQuery }, Cover) ->
    Lexed = riak_ql_lexer:get_tokens(binary_to_list(BaseQuery)),
    case riak_ql_parser:parse(Lexed) of
        {ok, ?SQL_SELECT{} = SQL} ->
            {ok, SQL?SQL_SELECT{cover_context = Cover}};
        Other ->
            Other
    end.

-spec decode_query_permissions(ts_query_types()) ->
                                      {string(), binary()}.
decode_query_permissions(#ddl_v1{table = NewBucketType}) ->
    {"riak_kv.ts_create_table", NewBucketType};
decode_query_permissions(?SQL_SELECT{'FROM' = Table}) ->
    {"riak_kv.ts_query", Table};
decode_query_permissions(#riak_sql_describe_v1{'DESCRIBE' = Table}) ->
    {"riak_kv.ts_describe", Table}.



-spec encode(tuple()) -> {ok, iolist()}.
encode(Message) ->
    {ok, riak_pb_codec:encode(Message)}.


-spec process(atom() | ts_requests() | ts_query_types(), #state{}) ->
                     {reply, ts_responses(), #state{}}.
process(M = #tsputreq{table = Table}, State) ->
    check_table_and_call(Table, fun sub_tsputreq/4, M, State);

process(M = #tsttbputreq{table = Table}, State) ->
    check_table_and_call(Table, fun sub_tsttbputreq/4, M, State);

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
process(DDL = #ddl_v1{}, State) ->
    %% the only one that doesn't require an activated table
    create_table(DDL, State);

process(M = ?SQL_SELECT{'FROM' = Table}, State) ->
    check_table_and_call(Table, fun sub_tsqueryreq/4, M, State);

process(M = #riak_sql_describe_v1{'DESCRIBE' = Table}, State) ->
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

-spec create_table(#ddl_v1{}, #state{}) ->
                          {reply, #tsqueryresp{} | #rpberrorresp{}, #state{}}.
create_table(DDL = #ddl_v1{table = Table}, State) ->
    {ok, Props1} = riak_kv_ts_util:apply_timeseries_bucket_props(DDL, []),
    Props2 = [riak_kv_wm_utils:erlify_bucket_prop(P) || P <- Props1],
    case riak_core_bucket_type:create(Table, Props2) of
        ok ->
            wait_until_active(Table, State, ?TABLE_ACTIVATE_WAIT);
        {error, Reason} ->
            {reply, table_create_fail_response(Table, Reason), State}
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

%% -----------
%% put
%% -----------

sub_tsputreq(Mod, _DDL, #tsputreq{table = Table, rows = Rows},
             State) ->
    Data = riak_pb_ts_codec:decode_rows(Rows),
    sub_putreq_common(Mod, Table, Data, State).

sub_tsttbputreq(Mod, _DDL, #tsttbputreq{table = Table, rows = Data},
               State) ->
    sub_putreq_common(Mod, Table, Data, State).

sub_putreq_common(Mod, Table, Data, State) ->
    case catch validate_rows(Mod, Data) of
        [] ->
            case put_data(Data, Table, Mod) of
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
put_data(Data, Table, Mod) ->
    DDL = Mod:get_ddl(),
    Bucket = riak_kv_ts_util:table_to_bucket(Table),
    BucketProps = riak_core_bucket:get_bucket(Bucket),
    NVal = proplists:get_value(n_val, BucketProps),

    PartitionedData = partition_data(Data, Bucket, BucketProps, DDL, Mod),
    PreflistData = add_preflists(PartitionedData, NVal,
                                 riak_core_node_watcher:nodes(riak_kv)),

    EncodeFn =
        fun(O) -> riak_object:to_binary(v1, O, msgpack) end,

    {ReqIds, FailReqs} =
        lists:foldl(
          fun({DocIdx, Preflist, Records}, {GlobalReqIds, GlobalErrorsCnt}) ->
                  case riak_kv_w1c_worker:validate_options(
                         NVal, Preflist, [], BucketProps) of
                      {ok, W, PW} ->
                          {Ids, Errs} =
                              lists:foldl(
                                fun(Record, {PartReqIds, PartErrors}) ->
                                        {RObj, LK} =
                                            build_object(Bucket, Mod, DDL,
                                                         Record, DocIdx),

                                        {ok, ReqId} =
                                            riak_kv_w1c_worker:async_put(
                                              RObj, W, PW, Bucket, NVal, LK,
                                              EncodeFn, Preflist),
                                        {[ReqId | PartReqIds], PartErrors}
                                end,
                                {[], 0}, Records),
                          {GlobalReqIds ++ Ids, GlobalErrorsCnt + Errs};
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
                     DDL :: #ddl_v1{},
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
    {RObj, LK}.


%% -----------
%% get and delete
%% -----------

sub_tsgetreq(Mod, DDL, #tsgetreq{table = Table,
                                 key    = PbCompoundKey,
                                 timeout = Timeout},
             State) ->
    Options =
        if Timeout == undefined -> [];
           true -> [{timeout, Timeout}]
        end,

    CompoundKey = riak_pb_ts_codec:decode_cells(PbCompoundKey),

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
            Rows = riak_pb_ts_codec:encode_rows(ColumnTypes, [Row]),
            {reply, #tsgetresp{columns = make_tscolumndescription_list(
                                           ColumnNames, ColumnTypes),
                               rows = Rows}, State};
        {error, {bad_key_length, Got, Need}} ->
            {reply, key_element_count_mismatch(Got, Need), State};
        {error, notfound} ->
            {reply, tsgetresp, State};
        {error, Reason} ->
            {reply, make_rpberrresp(?E_GET, to_string(Reason)), State}
    end.


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
            {reply, tsdelresp, State};
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
                 || #param_v1{name = N} <- DDL#ddl_v1.local_key#key_v1.ast],
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
    SQL = compile(Mod, catch decode_query(Q)),
    Client = {riak_client, [node(), undefined]},

    case SQL of
        {error, _Error} ->
            {reply, make_rpberrresp(
                      ?E_BAD_QUERY, "Failed to compile query"),
             State};
        _ ->
            %% SQL is a list of queries (1 per quantum)
            Bucket = riak_kv_ts_util:table_to_bucket(Table),
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
    {error, decoder_parse_error_resp(Err)};
compile(_Mod, {'EXIT', {Err, _}}) ->
    {error, decoder_parse_error_resp(Err)};
compile(Mod, {ok, SQL}) ->
    case (catch Mod:get_ddl()) of
        {_, {undef, _}} ->
            {error, no_helper_module};
        DDL ->
            case riak_ql_ddl:is_query_valid(Mod, DDL, SQL) of
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
%%

sub_tsqueryreq(_Mod, DDL, SQL, State) ->
    case riak_kv_qry:submit(SQL, DDL) of
        {ok, Data} when element(1, SQL) =:= ?SQL_SELECT_RECORD_NAME ->
            {reply, make_tsqueryresp(Data), State};
        {ok, Data} when element(1, SQL) =:= riak_sql_describe_v1 ->
            {reply, make_describe_response(Data), State};

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


%% ---------------------------------------------------
%% local functions
%% ---------------------------------------------------

-spec check_table_and_call(Table::binary(),
                           WorkItem::fun((module(), #ddl_v1{},
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
        {error, {inappropriate_bucket_state, InappropriateState}} ->
            {reply, table_not_activated_response(
                      Table, InappropriateState),
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

table_not_activated_response(Table, BadState) ->
    make_rpberrresp(
      ?E_TABLE_INACTIVE,
      flat_format("Table ~ts has not been activated (is in state '~s')", [Table, BadState])).

table_created_missing_response(Table) ->
    make_rpberrresp(
      ?E_CREATED_GHOST,
      flat_format("Table ~s has been created but found missing", [Table])).

to_string(X) ->
    flat_format("~p", [X]).



%% helpers to make various error responses

-spec make_tsqueryresp([] | {[riak_pb_ts_codec:tscolumnname()],
                             [riak_pb_ts_codec:tscolumntype()],
                             [[riak_pb_ts_codec:ldbvalue()]]}) -> #tsqueryresp{}.
make_tsqueryresp({_, _, []}) ->
    #tsqueryresp{columns = [], rows = []};
make_tsqueryresp({ColumnNames, ColumnTypes, JustRows}) ->
    #tsqueryresp{columns = make_tscolumndescription_list(ColumnNames, ColumnTypes),
                 rows = riak_pb_ts_codec:encode_rows(ColumnTypes, JustRows)}.

-spec make_describe_response([[term()]]) -> #tsqueryresp{}.
make_describe_response(DescribeTableRows) ->
    ColumnNames = [<<"Column">>, <<"Type">>, <<"Is Null">>, <<"Primary Key">>, <<"Local Key">>],
    ColumnTypes = [   varchar,     varchar,     boolean,        sint64,             sint64    ],
    #tsqueryresp{columns = make_tscolumndescription_list(ColumnNames, ColumnTypes),
                 rows = riak_pb_ts_codec:encode_rows(ColumnTypes, DescribeTableRows)}.

-spec get_column_types(list(binary()), module()) -> [riak_pb_ts_codec:tscolumntype()].
get_column_types(ColumnNames, Mod) ->
    [Mod:get_field_type([N]) || N <- ColumnNames].

-spec make_tscolumndescription_list([binary()], [riak_pb_ts_codec:tscolumntype()]) ->
                                           [#tscolumndescription{}].
make_tscolumndescription_list(ColumnNames, ColumnTypes) ->
    [#tscolumndescription{name = Name, type = riak_pb_ts_codec:encode_field_type(Type)}
     || {Name, Type} <- lists:zip(ColumnNames, ColumnTypes)].



decoder_parse_error_resp({LineNo, riak_ql_parser, Msg}) when is_integer(LineNo) ->
    flat_format("~ts", [Msg]);
decoder_parse_error_resp({Token, riak_ql_parser, _}) ->
    flat_format("Unexpected token '~p'", [Token]);
decoder_parse_error_resp(Error) ->
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
        missing_helper_module(<<"mytype">>, [{ddl, #ddl_v1{}}])
    ).

test_helper_validate_rows_mod() ->
    riak_ql_ddl_compiler:compile_and_load_from_tmp(
        riak_ql_parser:parse(riak_ql_lexer:get_tokens(
            "CREATE TABLE mytable ("
            "family VARCHAR NOT NULL,"
            "series VARCHAR NOT NULL,"
            "time TIMESTAMP NOT NULL,"
            "PRIMARY KEY ((family, series, quantum(time, 1, 'm')), family, series, time))"))).

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

-endif.
