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
-define(E_PARSE_ERROR,       1020).
-define(E_NOTFOUND,          1021).

-define(FETCH_RETRIES, 10).  %% TODO make it configurable in tsqueryreq
-define(TABLE_ACTIVATE_WAIT, 30). %% ditto

-export([decode_query_common/2,
         process/2,
         process_stream/3]).

-type ts_requests() :: #tsputreq{} | #tsdelreq{} | #tsgetreq{} |
                       #tslistkeysreq{} | #tsqueryreq{}.
-type ts_responses() :: #tsputresp{} | #tsdelresp{} | #tsgetresp{} |
                        #tslistkeysresp{} | #tsqueryresp{} |
                        #rpberrorresp{}.
-type ts_get_response() :: {tsgetresp, {list(binary()), list(atom()), list(list(term()))}}.
-type ts_query_response() :: {tsqueryresp, {list(binary()), list(atom()), list(list(term()))}}.
-type ts_query_responses() :: #tsqueryresp{} | ts_query_response().
-type ts_query_types() :: ?DDL{} | riak_kv_qry:sql_query_type_record().
-export_type([ts_requests/0, ts_responses/0,
              ts_query_response/0, ts_query_responses/0,
              ts_query_types/0]).

decode_query_common(Q, Cover) ->
    %% convert error returns to ok's, this means it will be passed into
    %% process which will not process it and return the error.
    case decode_query(Q, Cover) of
        {ok, DecodedQuery} ->
            PermAndTarget = decode_query_permissions(DecodedQuery),
            {ok, DecodedQuery, PermAndTarget};
        {error, Error} ->
            {ok, make_decoder_error_response(Error)}
    end.

-spec decode_query(Query::#tsinterpolation{}, Cover::term()) ->
    {error, _} | {ok, ts_query_types()}.
decode_query(#tsinterpolation{base = BaseQuery}, Cover) ->
    case catch riak_ql_parser:ql_parse(
                 riak_ql_lexer:get_tokens(  %% yecc can throw nasty 'EXIT' exceptions
                   binary_to_list(BaseQuery))) of
        {ddl, DDL, WithProperties} ->
            {ok, {DDL, WithProperties}};
        {SqlType, SQL} when SqlType == select;
                            SqlType == describe;
                            SqlType == insert ->
            riak_kv_ts_util:build_sql_record(SqlType, SQL, Cover);
        {error, {_LineNo, riak_ql_parser, Msg}} when is_integer(_LineNo) ->
            {error, Msg};
        {error, {Token, riak_ql_parser, _}} ->
            {error, flat_format("Unexpected token: '~s'", [Token])};
        {'EXIT', {Reason, _StackTrace}} ->
            {error, flat_format("~p", [Reason])};
        Other ->
            Other
    end.

-spec decode_query_permissions({?DDL{}, proplists:proplist()} | ts_query_types()) ->
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
                     {reply, ts_query_responses(), #state{}} |
                     {reply, ts_get_response(), #state{}} |
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
    {ok, Props1} = riak_kv_ts_util:apply_timeseries_bucket_props(
                     DDL, riak_ql_ddl_compiler:get_compiler_version(), WithProps),
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
            {reply, make_tsqueryresp(
                      riak_kv_qry:empty_result()), State};
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

%% NB: since this method deals with PB and TTB messages, the message must be fully
%% decoded before sub_tsqueryreq is called
sub_tsputreq(Mod, _DDL, #tsputreq{table = Table, rows = Rows},
             State) ->
    case riak_kv_ts_util:validate_rows(Mod, Rows) of
        [] ->
            case riak_kv_ts_api:put_data(Rows, Table, Mod) of
                ok ->
                    {reply, #tsputresp{}, State};
                {error, {some_failed, ErrorCount}} ->
                    {reply, failed_put_response(ErrorCount), State};
                {error, no_type} ->
                    {reply, table_not_activated_response(Table), State};
                {error, OtherReason} ->
                    {reply, make_rpberrresp(?E_PUT, to_string(OtherReason)), State}
            end;
        BadRowIdxs when is_list(BadRowIdxs) ->
            {reply, validate_rows_error_response(BadRowIdxs), State}
    end.


%% -----------
%% get
%% -----------

%% NB: since this method deals with PB and TTB messages, the message must be fully
%% decoded before sub_tsqueryreq is called
sub_tsgetreq(Mod, _DDL, #tsgetreq{table = Table,
                                  key    = CompoundKey,
                                  timeout = Timeout},
             State) ->
    Options =
        if Timeout == undefined -> [];
           true -> [{timeout, Timeout}]
        end,
    %%CompoundKey = riak_pb_ts_codec:decode_cells(PbCompoundKey),
    %% decoding is done per wire protocol (ttb or pb), see riak_kv_ts.erl
    Mod = riak_ql_ddl:make_module_name(Table),
    case riak_kv_ts_api:get_data(
           CompoundKey, Table, Mod, Options) of
        {ok, Record} ->
            {ColumnNames, Row} = lists:unzip(Record),
            %% the columns stored in riak_object are just
            %% names; we need names with types, so:
            ColumnTypes = riak_kv_ts_util:get_column_types(ColumnNames, Mod),
            {reply, make_tsgetresp(ColumnNames, ColumnTypes, [Row]), State};
        {error, no_type} ->
            {reply, table_not_activated_response(Table), State};
        {error, {bad_key_length, Got, Need}} ->
            {reply, key_element_count_mismatch(Got, Need), State};
        {error, notfound} ->
            {reply, make_rpberrresp(?E_NOTFOUND, "notfound"), State};
        {error, Reason} ->
            {reply, make_rpberrresp(?E_GET, to_string(Reason)), State}
    end.


%% -----------
%% delete
%% -----------

sub_tsdelreq(Mod, _DDL, #tsdelreq{table = Table,
                                  key    = PbCompoundKey,
                                  vclock  = VClock,
                                  timeout  = Timeout},
             State) ->
    Options =
        if Timeout == undefined -> [];
           true -> [{timeout, Timeout}]
        end,
    CompoundKey = riak_pb_ts_codec:decode_cells(PbCompoundKey),
    Mod = riak_ql_ddl:make_module_name(Table),
    case riak_kv_ts_api:delete_data(
           CompoundKey, Table, Mod, Options, VClock) of
        ok ->
            {reply, tsdelresp, State};
        {error, no_type} ->
            {reply, table_not_activated_response(Table), State};
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
    Client = {riak_client, [node(), undefined]},
    %% all we need from decode_query is to compile the query,
    %% but also to check permissions
    case decode_query(Q, undefined) of
        {ok, SQL} ->
            case riak_kv_ts_api:compile_to_per_quantum_queries(Mod, SQL) of
                {ok, Compiled} ->
                    Bucket = riak_kv_ts_util:table_to_bucket(Table),
                    convert_cover_list(
                      riak_kv_ts_util:sql_to_cover(Client, Compiled, Bucket, []), State);
                {error, Reason} ->
                    make_rpberrresp(
                      ?E_BAD_QUERY, flat_format("Failed to compile query: ~p", [Reason]))
            end;
        {error, Reason} ->
            {reply, make_rpberrresp(
                      ?E_BAD_QUERY, flat_format("Failed to parse query: ~p", [Reason])),
             State}
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


%% ----------
%% query
%% ----------

%% NB: since this method deals with PB and TTB messages, the message must be fully
%% decoded before sub_tsqueryreq is called
-spec sub_tsqueryreq(module(), ?DDL{}, riak_kv_qry:sql_query_type_record(), #state{}) ->
                     {reply, ts_query_responses() | #rpberrorresp{}, #state{}}.
sub_tsqueryreq(_Mod, DDL = ?DDL{table = Table}, SQL, State) ->
    case riak_kv_ts_api:query(SQL, DDL) of
        {ok, Data = {_ColNames, _ColTypes, _Rows}} ->
            {reply, make_tsqueryresp(Data), State};

        %% the following timeouts are known and distinguished:
        {error, no_type} ->
            {reply, table_not_activated_response(DDL#ddl_v1.table), State};
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
        {error, no_type} ->
            {reply, table_not_activated_response(Table), State};
        {error, missing_helper_module} ->
            BucketProps = riak_core_bucket:get_bucket(
                            riak_kv_ts_util:table_to_bucket(Table)),
            {reply, missing_helper_module(Table, BucketProps), State}
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


%% helpers to make various responses

-spec make_tscolumndescription_list([binary()], [riak_pb_ts_codec:tscolumntype()]) ->
                                           [#tscolumndescription{}].
make_tscolumndescription_list(ColumnNames, ColumnTypes) ->
    [#tscolumndescription{name = Name, type = riak_pb_ts_codec:encode_field_type(Type)}
     || {Name, Type} <- lists:zip(ColumnNames, ColumnTypes)].

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
        riak_kv_ts_util:validate_rows(Mod, [])
    ).

validate_rows_1_test() ->
    {module, Mod} = test_helper_validate_rows_mod(),
    ?assertEqual(
        [],
        riak_kv_ts_util:validate_rows(Mod, [{<<"f">>, <<"s">>, 11}])
    ).

validate_rows_bad_1_test() ->
    {module, Mod} = test_helper_validate_rows_mod(),
    ?assertEqual(
        ["1"],
        riak_kv_ts_util:validate_rows(Mod, [{}])
    ).

validate_rows_bad_2_test() ->
    {module, Mod} = test_helper_validate_rows_mod(),
    ?assertEqual(
        ["1", "3", "4"],
        riak_kv_ts_util:validate_rows(Mod, [{}, {<<"f">>, <<"s">>, 11}, {a, <<"s">>, 12}, {"hithere"}])
    ).

validate_rows_error_response_1_test() ->
    Msg = "Invalid data at row index(es) ",
    ?assertEqual(
        #rpberrorresp{errcode = ?E_IRREG,
                      errmsg = Msg ++ "1" },
        validate_rows_error_response(["1"])
    ).

validate_rows_error_response_2_test() ->
    Msg = "Invalid data at row index(es) ",
    ?assertEqual(
        #rpberrorresp{errcode = ?E_IRREG,
                      errmsg = Msg ++ "1, 2, 3" },
        validate_rows_error_response(["1", "2", "3"])
    ).

-endif.
