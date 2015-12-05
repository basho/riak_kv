%% -------------------------------------------------------------------
%%
%% riak_kv_pb_timeseries.erl: Riak TS protobuf callbacks
%%
%% Copyright (c) 2015 Basho Technologies, Inc.
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
%% @doc Callbacks for TS protobuf messages [codes 90..99]

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

%% NOTE: Clients will work with table names. Those names map to a
%% bucket type/bucket name tuple in Riak, with both the type name and
%% the bucket name matching the table.
%%
%% Thus, as soon as code transitions from dealing with timeseries
%% concepts to Riak KV concepts, the table name must be converted to a
%% bucket tuple. This function is a convenient mechanism for doing so
%% and making that transition more obvious.
-export([table_to_bucket/1]).
%% more utility functions for where TS and non-TS keys are dealt with
%% equally.
-export([pk/1, lk/1]).

-record(state, {}).

%% per RIAK-1437, error codes assigned to TS are in the 1000-1500 range
-define(E_SUBMIT,            1001).
-define(E_FETCH,             1002).
-define(E_IRREG,             1003).
-define(E_PUT,               1004).
-define(E_NOCREATE,          1005).
-define(E_NOT_TS_TYPE,       1006).
-define(E_MISSING_TYPE,      1007).
-define(E_MISSING_TS_MODULE, 1008).
-define(E_DELETE,            1009).
-define(E_GET,               1010).
-define(E_BAD_KEY_LENGTH,    1011).
-define(E_LISTKEYS,          1012).
-define(E_TIMEOUT,           1013).

-define(FETCH_RETRIES, 10).  %% TODO make it configurable in tsqueryreq


-spec init() -> any().
init() ->
    #state{}.


-spec decode(integer(), binary()) ->
    {ok, #tsputreq{} | #tsdelreq{} | #tsgetreq{} | #tslistkeysreq{}
       | #ddl_v1{} | #riak_sql_v1{},
     {PermSpec::string(), Table::binary()}} |
    {error, _}.
decode(Code, Bin) ->
    Msg = riak_pb_codec:decode(Code, Bin),
    case Msg of
        #tsqueryreq{query = Q}->
            case catch decode_query(Q) of
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
        #tsdelreq{table = Table} ->
            {ok, Msg, {"riak_kv.ts_del", Table}};
        #tslistkeysreq{table = Table} ->
            {ok, Msg, {"riak_kv.ts_listkeys", Table}}
    end.


-spec encode(tuple()) -> {ok, iolist()}.
encode(Message) ->
    {ok, riak_pb_codec:encode(Message)}.


-spec process(atom() | #tsputreq{} | #tsdelreq{} | #tsgetreq{} | #tslistkeysreq{}
              | #ddl_v1{} | #riak_sql_v1{}, #state{}) ->
                     {reply, #tsqueryresp{} | #rpberrorresp{}, #state{}}.
process(#tsputreq{rows = []}, State) ->
    {reply, #tsputresp{}, State};
process(#tsputreq{table = Table, columns = _Columns, rows = Rows}, State) ->
    Mod = riak_ql_ddl:make_module_name(Table),
    Data = riak_pb_ts_codec:decode_rows(Rows),
    %% validate only the first row as we trust the client to send us
    %% perfectly uniform data wrt types and order
    case (catch Mod:validate_obj(hd(Data))) of
        true ->
            %% however, prevent bad data to crash us
            try
                case put_data(Data, Table, Mod) of
                    0 ->
                        {reply, #tsputresp{}, State};
                    ErrorCount ->
                        EPutMessage = flat_format("Failed to put ~b record(s)", [ErrorCount]),
                        {reply, make_rpberrresp(?E_PUT, EPutMessage), State}
                end
            catch
                Class:Exception ->
                    lager:error("error: ~p:~p~n~p", [Class,Exception,erlang:get_stacktrace()]),
                    Error = make_rpberrresp(?E_IRREG, to_string({Class, Exception})),
                    {reply, Error, State}
            end;
        false ->
            {reply, make_rpberrresp(?E_IRREG, "Invalid data"), State};
        {_, {undef, _}} ->
            BucketProps = riak_core_bucket:get_bucket(table_to_bucket(Table)),
            {reply, missing_helper_module(Table, BucketProps), State}
    end;


process(#tsgetreq{table = Table, key = PbCompoundKey,
                  timeout = Timeout},
        State) ->
    Options =
        if Timeout == undefined -> [];
           true -> [{timeout, Timeout}]
        end,

    CompoundKey = riak_pb_ts_codec:decode_cells(PbCompoundKey),

    Mod = riak_ql_ddl:make_module_name(Table),
    case catch Mod:get_ddl() of
        {_, {undef, _}} ->
            Props = riak_core_bucket:get_bucket(Table),
            {reply, missing_helper_module(Table, Props), State};
        DDL ->
            Result =
                case make_ts_keys(CompoundKey, DDL, Mod) of
                    {ok, PKLK} ->
                        riak_client:get(
                          {Table, Table}, PKLK, Options, {riak_client, [node(), undefined]});
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
            end
    end;


process(#tsdelreq{table = Table, key = PbCompoundKey,
                  vclock = PbVClock, timeout = Timeout},
        State) ->
    Options =
        if Timeout == undefined -> [];
           true -> [{timeout, Timeout}]
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

    Mod = riak_ql_ddl:make_module_name(Table),
    case catch Mod:get_ddl() of
        {_, {undef, _}} ->
            Props = riak_core_bucket:get_bucket(Table),
            {reply, missing_helper_module(Table, Props), State};
        DDL ->
            Result =
                case make_ts_keys(CompoundKey, DDL, Mod) of
                    {ok, PKLK} ->
                        riak_client:delete_vclock(
                          {Table, Table}, PKLK, VClock, Options,
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
                    {reply, make_rpberrresp(
                              ?E_DELETE, flat_format("Failed to delete record: ~p", [Reason])),
                     State}
            end
    end;


process(#tslistkeysreq{table = Table, timeout = Timeout}, State) ->
    Mod = riak_ql_ddl:make_module_name(Table),
    case catch Mod:get_ddl() of
        {_, {undef, _}} ->
            Props = riak_core_bucket:get_bucket(Table),
            {reply, missing_helper_module(Table, Props), State};
        _DDL ->
            Filter = none,
            Result = riak_client:list_keys(
                       {Table, Table}, Filter, Timeout,
                       {riak_client, [node(), undefined]}),
            case Result of
                {ok, CompoundKeys} ->
                    Keys = riak_pb_ts_codec:encode_rows_non_strict(
                             [tuple_to_list(sext:decode(A)) || A <- CompoundKeys]),
                    {reply, #tslistkeysresp{keys = Keys, done = true}, State};
                {error, Reason} ->
                    {reply, make_rpberrresp(
                              ?E_LISTKEYS, flat_format("Failed to list keys: ~p", [Reason])),
                     State}
            end
    end;


process(#ddl_v1{}, State) ->
    {reply, make_rpberrresp(?E_NOCREATE,
                            "CREATE TABLE not supported via client interface;"
                            " use riak-admin command instead"),
     State};

process(SQL = #riak_sql_v1{'FROM' = Table}, State) ->
    Mod = riak_ql_ddl:make_module_name(Table),
    case (catch Mod:get_ddl()) of
        {_, {undef, _}} ->
            BucketProps = riak_core_bucket:get_bucket(table_to_bucket(Table)),
            {reply, missing_helper_module(Table, BucketProps), State};
        DDL ->
            submit_query(DDL, Mod, SQL, State)
    end.

%%
submit_query(DDL, Mod, SQL, State) ->
    case riak_kv_qry:submit(SQL, DDL) of
        {ok, Data} ->
            {reply, make_tsqueryresp(Data, Mod), State};
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


%% TODO: implement
process_stream(_, _, State)->
    {ignore, State}.

-spec decode_query(Query::#tsinterpolation{}) ->
    {error, _} | {ok, #ddl_v1{} | #riak_sql_v1{}}.
decode_query(#tsinterpolation{ base = BaseQuery }) ->
    Lexed = riak_ql_lexer:get_tokens(binary_to_list(BaseQuery)),
    riak_ql_parser:parse(Lexed).

decoder_parse_error_resp({LineNo, riak_ql_parser, Msg}) when is_integer(LineNo) ->
    flat_format("~ts", [Msg]);
decoder_parse_error_resp({Token, riak_ql_parser, _}) ->
    flat_format("Unexpected token '~p'", [Token]);
decoder_parse_error_resp(Error) ->
    Error.

flat_format(Format, Args) ->
    lists:flatten(io_lib:format(Format, Args)).

%% ---------------------------------------------------
%% local functions
%% ---------------------------------------------------

-spec make_rpberrresp(integer(), string()) -> #rpberrorresp{}.
make_rpberrresp(Code, Message) ->
    #rpberrorresp{errcode = Code,
                  errmsg = lists:flatten(Message)}.


-spec decode_query_permissions(#ddl_v1{} | #riak_sql_v1{}) -> {string(), binary()}.
decode_query_permissions(#ddl_v1{table = NewBucketType}) ->
    {"riak_kv.ts_create_table", NewBucketType};
decode_query_permissions(#riak_sql_v1{'FROM' = Table}) ->
    {"riak_kv.ts_query", Table}.

%%
-spec missing_helper_module(Table::binary(),
                            BucketProps::{error,any()} | [proplists:property()]) -> #rpberrorresp{}.
missing_helper_module(Table, {error, _}) ->
    missing_type_response(Table);
missing_helper_module(Table, BucketProps) when is_binary(Table), is_list(BucketProps) ->
    case lists:keymember(ddl, 1, BucketProps) of
        true  -> missing_table_module_response(Table);
        false -> not_timeseries_type_response(Table)
    end.

%%
-spec missing_type_response(BucketType::binary()) -> #rpberrorresp{}.
missing_type_response(BucketType) ->
    make_rpberrresp(
        ?E_MISSING_TYPE,
        flat_format("Bucket type ~s is missing.", [BucketType])).

%%
-spec not_timeseries_type_response(BucketType::binary()) -> #rpberrorresp{}.
not_timeseries_type_response(BucketType) ->
    make_rpberrresp(
        ?E_NOT_TS_TYPE,
        flat_format("Attempt Time Series operation on non Time Series bucket type ~s.", [BucketType])).

-spec missing_table_module_response(BucketType::binary()) -> #rpberrorresp{}.
missing_table_module_response(BucketType) ->
    make_rpberrresp(
        ?E_MISSING_TS_MODULE,
        flat_format("The compiled module for Time Series bucket ~s cannot be loaded.", [BucketType])).

-spec key_element_count_mismatch(Got::integer(), Need::integer()) -> #rpberrorresp{}.
key_element_count_mismatch(Got, Need) ->
    make_rpberrresp(
      ?E_BAD_KEY_LENGTH,
      flat_format("Key element count mismatch (key has ~b elements but ~b supplied).", [Need, Got])).

to_string(X) ->
    flat_format("~p", [X]).


%% ---------------------------------------------------
% functions supporting INSERT

row_to_key(Row, DDL, Mod) ->
    list_to_tuple([V || {_T, V} <- riak_ql_ddl:get_partition_key(DDL, Row, Mod)]).

-spec partition_data(Data :: list(term()),
                     Bucket :: {binary(), binary()},
                     BucketProps :: proplists:proplist(),
                     DDL :: #ddl_v1{},
                     Mod :: module()) ->
                            list(tuple(non_neg_integer(), list(term()))).
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

add_preflists(PartitionedData, NVal, UpNodes) ->
    lists:map(fun({Idx, Rows}) -> {Idx,
                                   riak_core_apl:get_apl_ann(Idx, NVal, UpNodes),
                                   Rows} end,
              PartitionedData).

build_object(Bucket, Mod, DDL, Row, PK) ->
    Obj = Mod:add_column_info(Row),
    LK  = list_to_tuple([V || {_T,V} <- riak_ql_ddl:get_local_key(DDL, Row, Mod)]),

    RObj = riak_object:newts(Bucket, PK, Obj,
                             dict:from_list([{?MD_DDL_VERSION, ?DDL_VERSION}])),
    {RObj, LK}.


-spec put_data([riak_pb_ts_codec:tsrow()], binary(), module()) -> integer().
%% @ignore return count of records we failed to put
put_data(Data, Table, Mod) ->
    DDL = Mod:get_ddl(),

    Bucket = table_to_bucket(Table),
    BucketProps = riak_core_bucket:get_bucket(Bucket),
    NVal = proplists:get_value(n_val, BucketProps),

    PartitionedData = partition_data(Data, Bucket, BucketProps, DDL, Mod),
    PreflistData = add_preflists(PartitionedData, NVal,
                                 riak_core_node_watcher:nodes(riak_kv)),

    EncodeFn =
        fun(O) -> riak_object:to_binary(v1, O, msgpack) end,

    {ReqIds, FailReqs} = lists:foldl(
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

-spec make_ts_keys([riak_pb_ts_codec:ldbvalue()], #ddl_v1{}, module()) ->
                          {ok, {binary(), binary()}} | {error, {bad_key_length, integer(), integer()}}.
make_ts_keys(CompoundKey, DDL = #ddl_v1{local_key = #key_v1{ast = LKParams},
                                        fields = Fields}, Mod) ->
    %% 1. use elements in Key to form a complete data record:
    KeyFields = [F || #param_v1{name = [F]} <- LKParams],
    Got = length(CompoundKey),
    Need = length(KeyFields),
    case {Got, Need} of
        {_N, _N} ->
            KeyAssigned = lists:zip(KeyFields, CompoundKey),
            VoidRecord = [{F, void} || #riak_field_v1{name = F} <- Fields],
            %% (void values will not be looked at in riak_ql_ddl:make_key;
            %% only LK-constituent fields matter)
            BareValues =
                list_to_tuple(
                  [proplists:get_value(K, KeyAssigned)
                   || {K, _} <- VoidRecord, lists:member(K, KeyFields)]),

            %% 2. make the PK and LK
            PK  = list_to_tuple([V || {_T, V} <- riak_ql_ddl:get_partition_key(DDL, BareValues, Mod)]),
            LK  = list_to_tuple([V || {_T, V} <- riak_ql_ddl:get_local_key(DDL, BareValues, Mod)]),
            {ok, {PK, LK}};
       {G, N} ->
            {error, {bad_key_length, G, N}}
    end.


%% ---------------------------------------------------
% functions supporting SELECT

-spec make_tsqueryresp([{binary(), term()}], module()) -> #tsqueryresp{}.
make_tsqueryresp([], _Module) ->
    #tsqueryresp{columns = [], rows = []};
make_tsqueryresp(FetchedRows, Mod) ->
    %% as returned by fetch, we have in Rows a sequence of KV pairs,
    %% making records concatenated in a flat list
    ColumnNames = get_column_names(FetchedRows),
    ColumnTypes = get_column_types(ColumnNames, Mod),
    Records = assemble_records(FetchedRows, length(ColumnNames)),
    JustRows = lists:map(
                 fun(Rec) -> [C || {_K, C} <- Rec] end,
                 Records),
    #tsqueryresp{columns = make_tscolumndescription_list(ColumnNames, ColumnTypes),
                 rows = riak_pb_ts_codec:encode_rows(ColumnTypes, JustRows)}.

get_column_names([{C1, _} | MoreRecords]) ->
    {RestOfColumns, _DiscardedValues} =
        lists:unzip(
          lists:takewhile(
            fun({Cn, _}) -> C1 /= Cn end,
            MoreRecords)),
    [C1|RestOfColumns].

-spec get_column_types(list(binary()), module()) -> list(riak_pb_ts_codec:tscolumntype()).
get_column_types(ColumnNames, Mod) ->
    [Mod:get_field_type([ColumnName]) || ColumnName <- ColumnNames].

-spec make_tscolumndescription_list(list(binary()), list(riak_pb_ts_codec:tscolumntype())) ->
                                           list(#tscolumndescription{}).
make_tscolumndescription_list(ColumnNames, ColumnTypes) ->
  [#tscolumndescription{name = Name, type = riak_pb_ts_codec:encode_field_type(Type)}
    || {Name, Type} <- lists:zip(ColumnNames, ColumnTypes)].

assemble_records(Rows, RecordSize) ->
    assemble_records_(Rows, RecordSize, []).
%% should we protect against incomplete records?
assemble_records_([], _, Acc) ->
    lists:reverse(Acc);
assemble_records_(RR, RSize, Acc) ->
    Remaining = lists:nthtail(RSize, RR),
    assemble_records_(
      Remaining, RSize, [lists:sublist(RR, RSize) | Acc]).


%% ---------------------------------------------------
% functions supporting list_keys

%% Utility API to limit some of the confusion over tables vs buckets
table_to_bucket(Table) ->
    {Table, Table}.

%% Useful key extractors for functions (e.g., in get or delete code
%% paths) which are agnostic to whether they are dealing with TS or
%% non-TS data
pk({PK, _LK}) ->
    PK;
pk(NonTSKey) ->
    NonTSKey.

lk({_PK, LK}) ->
    LK;
lk(NonTSKey) ->
    NonTSKey.



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

-endif.
