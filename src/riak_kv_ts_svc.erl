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
                        #tscoverageresp{} |
                        #rpberrorresp{}.
-type ts_get_response() :: {tsgetresp, {list(binary()), list(atom()), list(list(term()))}}.
-type ts_query_response() :: {tsqueryresp, {list(binary()), list(atom()), list(list(term()))}}.
-type ts_query_responses() :: #tsqueryresp{} | ts_query_response().
-type ts_query_types() :: ?DDL{} | riak_kv_qry:sql_query_type_record().
-export_type([ts_requests/0, ts_responses/0,
              ts_query_response/0, ts_query_responses/0,
              ts_query_types/0]).

decode_query_common(Q, Cover) ->
    case decode_query(Q, Cover) of
        {QueryType, {ok, Query}} ->
            {ok, Query, decode_query_permissions(QueryType, Query)};
        {_QueryType, {error, Error}} ->
            {ok, make_decoder_error_response(Error)};
        {error, Error} ->
            %% convert error returns to ok's, this means it will be passed into
            %% process which will not process it and return the error.
            {ok, make_decoder_error_response(Error)}
    end.

-spec decode_query(Query::#tsinterpolation{}, Cover::term()) ->
    {error, _} | {ok, ts_query_types()}.
decode_query(#tsinterpolation{base = BaseQuery}, Cover) ->
    case catch riak_ql_parser:ql_parse(
                 riak_ql_lexer:get_tokens(  %% yecc can throw nasty 'EXIT' exceptions
                   binary_to_list(BaseQuery))) of
        {ddl, DDL, WithProperties} ->
            {ddl, {ok, {DDL, WithProperties}}};
        {QryType, SQL} when QryType /= error,
                            QryType /= 'EXIT' ->
            {QryType, riak_kv_ts_util:build_sql_record(QryType, SQL, Cover)};
        {'EXIT', {Reason, _StackTrace}} ->
            {error, {lexer_error, flat_format("~s", [Reason])}};
        {error, Other} ->
            {error, Other}
    end.

decode_query_permissions(QryType, {DDL = ?DDL{}, _WithProps}) ->
    decode_query_permissions(QryType, DDL);
decode_query_permissions(QryType, Qry) ->
    {riak_kv_ts_api:api_call_from_sql_type(QryType),
     riak_kv_ts_util:queried_table(Qry)}.


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
                          {reply, tsqueryresp | #rpberrorresp{}, #state{}}.
create_table({DDL = ?DDL{table = Table}, WithProps}, State) ->
    {ok, Props1} = riak_kv_ts_util:apply_timeseries_bucket_props(
                     DDL, riak_ql_ddl_compiler:get_compiler_version(), WithProps),
    case catch [riak_kv_wm_utils:erlify_bucket_prop(P) || P <- Props1] of
        {bad_linkfun_modfun, {M, F}} ->
            {reply, make_table_create_fail_resp(
                      Table, flat_format(
                               "Invalid link mod or fun in bucket type properties: ~p:~p\n", [M, F])),
             State};
        {bad_linkfun_bkey, {B, K}} ->
            {reply, make_table_create_fail_resp(
                      Table, flat_format(
                               "Malformed bucket/key for anon link fun in bucket type properties: ~p/~p\n", [B, K])),
             State};
        {bad_chash_keyfun, {M, F}} ->
            {reply, make_table_create_fail_resp(
                      Table, flat_format(
                               "Invalid chash mod or fun in bucket type properties: ~p:~p\n", [M, F])),
             State};
        Props2 ->
            case riak_core_bucket_type:create(Table, Props2) of
                ok ->
                    wait_until_active(Table, State, ?TABLE_ACTIVATE_WAIT);
                {error, Reason} ->
                    {reply, make_table_create_fail_resp(Table, Reason), State}
            end
    end.

wait_until_active(Table, State, 0) ->
    {reply, make_table_activate_fail_resp(Table), State};
wait_until_active(Table, State, Seconds) ->
    case riak_core_bucket_type:activate(Table) of
        ok ->
            {reply, tsqueryresp, State};
        {error, not_ready} ->
            timer:sleep(1000),
            wait_until_active(Table, State, Seconds - 1);
        {error, undefined} ->
            %% this is inconceivable because create(Table) has
            %% just succeeded, so it's here mostly to pacify
            %% the dialyzer (and of course, for the odd chance
            %% of Erlang imps crashing nodes between create
            %% and activate calls)
            {reply, make_table_created_missing_resp(Table), State}
    end.


%% ---------------------------------------------------
%% functions called from check_table_and_call, one per ts* request
%% ---------------------------------------------------


%% -----------
%% put
%% -----------

%% NB: since this method deals with PB and TTB messages, the message must be fully
%% decoded before sub_tsputreq is called
sub_tsputreq(Mod, _DDL, #tsputreq{table = Table, rows = Rows},
             State) ->
    case riak_kv_ts_util:validate_rows(Mod, Rows) of
        [] ->
            case riak_kv_ts_api:put_data(Rows, Table, Mod) of
                ok ->
                    {reply, #tsputresp{}, State};
                {error, {some_failed, ErrorCount}} ->
                    {reply, make_failed_put_resp(ErrorCount), State};
                {error, no_type} ->
                    {reply, make_table_not_activated_resp(Table), State};
                {error, OtherReason} ->
                    {reply, make_rpberrresp(?E_PUT, to_string(OtherReason)), State}
            end;
        BadRowIdxs ->
            {reply, make_validate_rows_error_resp(BadRowIdxs), State}
    end.


%% -----------
%% get
%% -----------

%% NB: since this method deals with PB and TTB messages, the message must be fully
%% decoded before sub_tsgetreq is called
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
            {reply, make_tsgetresp(ColumnNames, ColumnTypes, [list_to_tuple(Row)]), State};
        {error, no_type} ->
            {reply, make_table_not_activated_resp(Table), State};
        {error, {bad_key_length, Got, Need}} ->
            {reply, make_key_element_count_mismatch_resp(Got, Need), State};
        {error, notfound} ->
            {reply, make_tsgetresp([], [], []), State};
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
            {reply, make_table_not_activated_resp(Table), State};
        {error, {bad_key_length, Got, Need}} ->
            {reply, make_key_element_count_mismatch_resp(Got, Need), State};
        {error, notfound} ->
            {reply, make_rpberrresp(?E_NOTFOUND, "notfound"), State};
        {error, Reason} ->
            {reply, make_failed_delete_resp(Reason), State}
    end.


%% -----------
%% listkeys
%% -----------

sub_tslistkeysreq(Mod, DDL, #tslistkeysreq{table = Table,
                                           timeout = Timeout} = Req,
                  State) ->

    %% Construct a function to convert from TS local key to TS
    %% partition key.
    %%
    %% This is needed because coverage filter functions must check the
    %% hash of the partition key, not the local key, when folding over
    %% keys in the backend.

    KeyConvFn = 
        fun(Key) when is_binary(Key) ->
                riak_kv_ts_util:row_to_key(sext:decode(Key), DDL, Mod);
           (Key) ->
                %% Key read from leveldb should always be binary.
                %% This clause is just to keep dialyzer quiet
                %% (otherwise dialyzer will complain about no local
                %% return, since we have no way to spec the type
                %% of Key for an anonymous function).
                %%
                %% Nonetheless, we log an error in case this branch is
                %% ever exercised

                lager:error("Key conversion function " ++ 
                                "(from riak_kv_ts_svc:sub_tslistkeysreq/4) " ++
                                "encountered a non-binary object key: ~p~n", [Key]),
                Key
        end,

    Result =
        riak_client:stream_list_keys(
          riak_kv_ts_util:table_to_bucket(Table), Timeout, KeyConvFn,
          {riak_client, [node(), undefined]}),
    case Result of
        {ok, ReqId} ->
            ColumnInfo =
                [Mod:get_field_type(N)
                 || #param_v1{name = N} <- DDL?DDL.local_key#key_v1.ast],
            {reply, {stream, ReqId}, State#state{req = Req, req_ctx = ReqId,
                                                 column_info = ColumnInfo}};
        {error, Reason} ->
            {reply, make_failed_listkeys_resp(Reason), State}
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
        {_QryType, {ok, SQL}} ->
            case riak_kv_ts_api:compile_to_per_quantum_queries(Mod, SQL) of
                {ok, Compiled} ->
                    Bucket = riak_kv_ts_util:table_to_bucket(Table),
                    {reply,
                     {tscoverageresp, riak_kv_ts_util:sql_to_cover(Client, Compiled, Bucket, [])},
                     State};
                {error, Reason} ->
                    {reply, make_rpberrresp(
                      ?E_BAD_QUERY, flat_format("Failed to compile query: ~p", [Reason])), State}
            end;
        {error, Reason} ->
            {reply, make_decoder_error_response(Reason), State}
    end.


%% ----------
%% query
%% ----------

%% NB: since this method deals with PB and TTB messages, the message must be fully
%% decoded before sub_tsqueryreq is called
-spec sub_tsqueryreq(module(), ?DDL{}, riak_kv_qry:sql_query_type_record(), #state{}) ->
                     {reply, ts_query_responses() | #rpberrorresp{}, #state{}}.
sub_tsqueryreq(_Mod, DDL = ?DDL{table = Table}, SQL, State) ->
    case riak_kv_ts_api:query(SQL, DDL) of
        {ok, {ColNames, ColTypes, LdbNativeRows}} ->
            Rows = [list_to_tuple(R) || R <- LdbNativeRows],
            {reply, make_tsqueryresp({ColNames, ColTypes, Rows}), State};

        %% the following timeouts are known and distinguished:
        {error, no_type} ->
            {reply, make_table_not_activated_resp(Table), State};
        {error, qry_worker_timeout} ->
            %% the eleveldb process didn't send us any response after
            %% 10 sec (hardcoded in riak_kv_qry), and probably died
            {reply, make_rpberrresp(?E_TIMEOUT, "no response from backend"), State};
        {error, backend_timeout} ->
            %% the eleveldb process did manage to send us a timeout
            %% response
            {reply, make_rpberrresp(?E_TIMEOUT, "backend timeout"), State};

        %% this one comes from riak_kv_qry_worker
        {error, divide_by_zero} ->
            {reply, make_rpberrresp(?E_SUBMIT, "Divide by zero"), State};

        %% from riak_kv_qry
        {error, {invalid_data, BadRowIdxs}} ->
            {reply, make_validate_rows_error_resp(BadRowIdxs), State};
        {error, {too_many_insert_values, BadRowIdxs}} ->
            {reply, make_too_many_insert_values_resp(BadRowIdxs), State};
        {error, {undefined_fields, BadFields}} ->
            {reply, make_undefined_field_in_insert_resp(BadFields), State};

        %% these come from riak_kv_qry_compiler, even though the query is a valid SQL.
        {error, {_DDLCompilerErrType, DDLCompilerErrDesc}} when is_atom(_DDLCompilerErrType) ->
            {reply, make_rpberrresp(?E_SUBMIT, DDLCompilerErrDesc), State};

        {error, Reason} ->
            {reply, make_rpberrresp(?E_SUBMIT, Reason), State}
    end.


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
            {reply, make_table_not_activated_resp(Table), State};
        {error, missing_helper_module} ->
            BucketProps = riak_core_bucket:get_bucket(
                            riak_kv_ts_util:table_to_bucket(Table)),
            {reply, make_missing_helper_module_resp(Table, BucketProps), State}
    end.



%%
-spec make_rpberrresp(integer(), string()) -> #rpberrorresp{}.
make_rpberrresp(Code, Message) ->
    #rpberrorresp{errcode = Code,
                  errmsg = iolist_to_binary(Message)}.

%%
-spec make_missing_helper_module_resp(Table::binary(),
                            BucketProps::{error, any()} | [proplists:property()])
                           -> #rpberrorresp{}.
make_missing_helper_module_resp(Table, {error, _}) ->
    make_missing_type_resp(Table);
make_missing_helper_module_resp(Table, BucketProps)
  when is_binary(Table), is_list(BucketProps) ->
    case lists:keymember(ddl, 1, BucketProps) of
        true  -> make_missing_table_module_resp(Table);
        false -> make_nonts_type_resp(Table)
    end.

%%
-spec make_missing_type_resp(Table::binary()) -> #rpberrorresp{}.
make_missing_type_resp(Table) ->
    make_rpberrresp(
      ?E_MISSING_TYPE,
      flat_format("Time Series table ~s does not exist", [Table])).

%%
-spec make_nonts_type_resp(Table::binary()) -> #rpberrorresp{}.
make_nonts_type_resp(Table) ->
    make_rpberrresp(
      ?E_NOT_TS_TYPE,
      flat_format("Attempt Time Series operation on non Time Series table ~s", [Table])).

-spec make_missing_table_module_resp(Table::binary()) -> #rpberrorresp{}.
make_missing_table_module_resp(Table) ->
    make_rpberrresp(
      ?E_MISSING_TS_MODULE,
      flat_format("The compiled module for Time Series table ~s cannot be loaded", [Table])).

-spec make_key_element_count_mismatch_resp(Got::integer(), Need::integer()) -> #rpberrorresp{}.
make_key_element_count_mismatch_resp(Got, Need) ->
    make_rpberrresp(
      ?E_BAD_KEY_LENGTH,
      flat_format("Key element count mismatch (key has ~b elements but ~b supplied)", [Need, Got])).

-spec make_validate_rows_error_resp([integer()]) -> #rpberrorresp{}.
make_validate_rows_error_resp(BadRowIdxs) ->
    BadRowsString = string:join([integer_to_list(I) || I <- BadRowIdxs],", "),
    make_rpberrresp(
      ?E_IRREG,
      flat_format("Invalid data found at row index(es) ~s", [BadRowsString])).

make_too_many_insert_values_resp(BadRowIdxs) ->
    BadRowsString = string:join([integer_to_list(I) || I <- BadRowIdxs],", "),
    make_rpberrresp(
      ?E_BAD_QUERY,
      flat_format("too many values in row index(es) ~s", [BadRowsString])).

make_undefined_field_in_insert_resp(BadFields) ->
    make_rpberrresp(
      ?E_BAD_QUERY,
      flat_format("undefined fields: ~s", [string:join(BadFields, ", ")])).

make_failed_put_resp(ErrorCount) ->
    make_rpberrresp(
      ?E_PUT,
      flat_format("Failed to put ~b record(s)", [ErrorCount])).

make_failed_delete_resp(Reason) ->
    make_rpberrresp(
      ?E_DELETE,
      flat_format("Failed to delete record: ~p", [Reason])).

make_failed_listkeys_resp(Reason) ->
    make_rpberrresp(
      ?E_LISTKEYS,
      flat_format("Failed to list keys: ~p", [Reason])).

make_table_create_fail_resp(Table, Reason) ->
    make_rpberrresp(
      ?E_CREATE, flat_format("Failed to create table ~s: ~p", [Table, Reason])).

make_table_activate_fail_resp(Table) ->
    make_rpberrresp(
      ?E_ACTIVATE,
      flat_format("Failed to activate table ~s", [Table])).

make_table_not_activated_resp(Table) ->
    make_rpberrresp(
      ?E_TABLE_INACTIVE,
      flat_format("~ts is not an active table", [Table])).

make_table_created_missing_resp(Table) ->
    make_rpberrresp(
      ?E_CREATED_GHOST,
      flat_format("Table ~s has been created but found missing", [Table])).

to_string(X) ->
    flat_format("~p", [X]).


%% helpers to make various responses

make_tsgetresp(ColumnNames, ColumnTypes, Rows) ->
    {tsgetresp, {ColumnNames, ColumnTypes, Rows}}.

make_tsqueryresp({_ColumnNames, _ColumnTypes, []}) ->
    %% tests (and probably docs, too) expect an empty result set to be
    %% returned as {[], []} (with column names omitted). Before the
    %% TTB merge, the columns info was dropped in
    %% riak_kv_pb_ts:encode_response({reply, {tsqueryresp, Data}, _}).
    %% Now, because the TTB encoder is dumb, leaving the special case
    %% treatment in the PB-specific riak_kv_pb_ts lets empty query
    %% results going through TTB still have the columns info, which is
    %% not what the caller might expect. We ensure uniform {[], []}
    %% for bot PB and TTB by preparing this term in advance, here.
    {tsqueryresp, {[], [], []}};
make_tsqueryresp(Data = {_ColumnNames, _ColumnTypes, _Rows}) ->
    {tsqueryresp, Data}.


make_decoder_error_response({lexer_error, Msg}) ->
    make_rpberrresp(?E_PARSE_ERROR, flat_format("~s", [Msg]));
make_decoder_error_response({LineNo, riak_ql_parser, Msg}) when is_integer(LineNo) ->
    make_rpberrresp(?E_PARSE_ERROR, flat_format("~ts", [Msg]));
make_decoder_error_response({Token, riak_ql_parser, _}) when is_binary(Token) ->
    make_rpberrresp(?E_PARSE_ERROR, flat_format("Unexpected token '~s'", [Token]));
make_decoder_error_response({Token, riak_ql_parser, _}) ->
    make_rpberrresp(?E_PARSE_ERROR, flat_format("Unexpected token '~p'", [Token]));
make_decoder_error_response(Error) ->
    make_rpberrresp(?E_PARSE_ERROR, flat_format("~p", [Error])).

flat_format(Format, Args) ->
    lists:flatten(io_lib:format(Format, Args)).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

missing_helper_module_missing_type_test() ->
    ?assertMatch(
        #rpberrorresp{errcode = ?E_MISSING_TYPE },
        make_missing_helper_module_resp(<<"mytype">>, {error, any})
    ).

missing_helper_module_not_ts_type_test() ->
    ?assertMatch(
        #rpberrorresp{errcode = ?E_NOT_TS_TYPE },
        make_missing_helper_module_resp(<<"mytype">>, []) % no ddl property
    ).

%% if the bucket properties exist and they contain a ddl property then
%% the bucket properties are in the correct state but the module is still
%% missing.
missing_helper_module_test() ->
    ?assertMatch(
        #rpberrorresp{errcode = ?E_MISSING_TS_MODULE },
        make_missing_helper_module_resp(<<"mytype">>, [{ddl, ?DDL{}}])
    ).

-endif.
