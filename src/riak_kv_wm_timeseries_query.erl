%% -------------------------------------------------------------------
%%
%% riak_kv_wm_timeseries_query: Webmachine resource for riak TS query call.
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

%% @doc Resource for Riak TS operations over HTTP.
%%
%% ```
%% POST   /ts/v1/query?query="query string"   execute SQL query
%% '''
%%
%% Response is a JSON containing data rows with column headers.
%%

-module(riak_kv_wm_timeseries_query).

%% webmachine resource exports
-export([
         init/1,
         service_available/2,
         is_authorized/2,
         malformed_request/2,
         forbidden/2,
         allowed_methods/2,
         resource_exists/2,
         post_is_create/2,
         process_post/2,
         content_types_accepted/2,
         content_types_provided/2,
         encodings_provided/2,
         produce_doc_body/2
        ]).

%% exports for create table
-export([make_table_create_fail_resp/2,
         make_table_activate_error_timeout_resp/1,
         make_table_created_missing_resp/1]).

-include_lib("webmachine/include/webmachine.hrl").
-include_lib("riak_ql/include/riak_ql_ddl.hrl").
-include("riak_kv_wm_raw.hrl").
-include("riak_kv_ts.hrl").

-record(ctx,
        {
          api_version :: undefined | integer(),
          table       :: undefined | binary(),
          mod         :: undefined | module(),
          method      :: atom(),
          timeout     :: undefined | integer(), %% passed-in timeout value in ms
          security,     %% security context
          sql_type    :: undefined | riak_kv_qry:query_type(),
          compiled_query :: undefined | ?DDL{} | riak_kv_qry:sql_query_type_record(),
          with_props     :: undefined | proplists:proplist(),
          result         :: undefined | ok | {Headers::[binary()], Rows::[ts_rec()]}
         }).

-define(DEFAULT_TIMEOUT, 60000).
-define(TABLE_ACTIVATE_WAIT, 30).   %% wait until table's bucket type is activated

-type cb_rv_spec(T) :: {T, #wm_reqdata{}, #ctx{}}.
-type halt() :: {'halt', 200..599} | {'error' , term()}.
-type ts_rec() :: [riak_pb_ts_codec:ldbvalue()].


-spec init(proplists:proplist()) -> {ok, #ctx{}}.
init(_Props) ->
    {ok, #ctx{}}.

-spec service_available(#wm_reqdata{}, #ctx{}) -> cb_rv_spec(boolean()).
%% @doc Determine whether or not a connection to Riak
%%      can be established.
service_available(RD, Ctx) ->
    ApiVersion = riak_kv_wm_ts_util:extract_api_version(RD),
    case {riak_kv_wm_ts_util:is_supported_api_version(ApiVersion),
          init:get_status()} of
        {true, {started, _}} ->
            %% always available because no client connection is required
            {true, RD, Ctx};
        {false, {started, _}} ->
            riak_kv_wm_ts_util:handle_error({unsupported_version, ApiVersion}, RD, Ctx);
        {_, {InternalStatus, _}} ->
            riak_kv_wm_ts_util:handle_error({not_ready, InternalStatus}, RD, Ctx)
    end.


-spec allowed_methods(#wm_reqdata{}, #ctx{}) -> cb_rv_spec([atom()]).
allowed_methods(RD, Ctx) ->
    {['POST'], RD, Ctx}.

-spec malformed_request(#wm_reqdata{}, #ctx{}) -> cb_rv_spec(boolean()).
malformed_request(RD, Ctx) ->
    try
        {SqlType, SQL, WithProps} = query_from_request(RD),
        Table = riak_kv_ts_util:queried_table(SQL),
        Mod = riak_ql_ddl:make_module_name(Table),
        {false, RD, Ctx#ctx{sql_type = SqlType,
                            compiled_query = SQL,
                            with_props = WithProps,
                            table = Table,
                            mod = Mod}}
    catch
        throw:Condition ->
            riak_kv_wm_ts_util:handle_error(Condition, RD, Ctx)
    end.

-spec is_authorized(#wm_reqdata{}, #ctx{}) -> cb_rv_spec(boolean()|string()|halt()).
is_authorized(RD, #ctx{sql_type = SqlType, table = Table} = Ctx) ->
    Call = riak_kv_ts_api:api_call_from_sql_type(SqlType),
    case riak_kv_wm_ts_util:authorize(Call, Table, RD) of
        ok ->
            {true, RD, Ctx};
        {error, ErrorMsg} ->
            riak_kv_wm_ts_util:handle_error({not_permitted, Table, ErrorMsg}, RD, Ctx);
        insecure ->
            riak_kv_wm_ts_util:handle_error(insecure_connection, RD, Ctx)
    end.

-spec forbidden(#wm_reqdata{}, #ctx{}) -> cb_rv_spec(boolean()).
forbidden(RD, Ctx) ->
    case riak_kv_wm_utils:is_forbidden(RD) of
        true ->
            {true, RD, Ctx};
        false ->
            %% depends on query type, we will check this later; pass
            %% for now
            {false, RD, Ctx}
    end.

-spec content_types_provided(#wm_reqdata{}, #ctx{}) ->
                                    cb_rv_spec([{ContentType::string(), Producer::atom()}]).
content_types_provided(RD, Ctx) ->
    {[{"application/json", produce_doc_body}], RD, Ctx}.


-spec encodings_provided(#wm_reqdata{}, #ctx{}) ->
                                cb_rv_spec([{Encoding::string(), Producer::function()}]).
encodings_provided(RD, Ctx) ->
    {riak_kv_wm_utils:default_encodings(), RD, Ctx}.


-spec content_types_accepted(#wm_reqdata{}, #ctx{}) ->
                                    cb_rv_spec([ContentType::string()]).
content_types_accepted(RD, Ctx) ->
    {["text/plain"], RD, Ctx}.


-spec resource_exists(#wm_reqdata{}, #ctx{}) -> cb_rv_spec(boolean()|halt()).
resource_exists(RD, #ctx{sql_type = ddl,
                         mod = Mod,
                         table = Table} = Ctx) ->
    case riak_kv_wm_ts_util:table_module_exists(Mod) of
        false ->
            {true, RD, Ctx};
        true ->
            riak_kv_wm_ts_util:handle_error({table_exists, Table}, RD, Ctx)
    end;
resource_exists(RD, #ctx{sql_type = Type,
                         table = <<>>} = Ctx) when Type /= ddl ->
    % SHOW TABLES has table = <<>>
    {true, RD, Ctx};
resource_exists(RD, #ctx{sql_type = Type,
                         mod = Mod,
                         table = Table} = Ctx) when Type /= ddl ->
    case riak_kv_wm_ts_util:table_module_exists(Mod) of
        true ->
            {true, RD, Ctx};
        false ->
            riak_kv_wm_ts_util:handle_error({no_such_table, Table}, RD, Ctx)
    end.

-spec post_is_create(#wm_reqdata{}, #ctx{}) -> cb_rv_spec(boolean()).
post_is_create(RD, Ctx) ->
    {false, RD, Ctx}.

-spec process_post(#wm_reqdata{}, #ctx{}) -> cb_rv_spec(boolean()).
process_post(RD, #ctx{sql_type = ddl, compiled_query = SQL, with_props = WithProps} = Ctx) ->
    case create_table(SQL, WithProps) of
        ok ->
            Result = [{success, true}],  %% represents ok
            Json = to_json(Result),
            {true, wrq:append_to_response_body(Json, RD), Ctx};
        {error, Reason} ->
            riak_kv_wm_ts_util:handle_error(Reason, RD, Ctx)
    end;
process_post(RD, #ctx{table = <<>>} = Ctx) ->
    % SHOW TABLES has table = <<>>
    process_post_(RD, Ctx, ?DDL{});
process_post(RD, #ctx{mod = Mod} = Ctx) ->
    DDL = Mod:get_ddl(), %% might be faster to store this earlier on
    process_post_(RD, Ctx, DDL).

-spec process_post_(#wm_reqdata{}, #ctx{},
                    ?DDL{}) -> cb_rv_spec(boolean()).
process_post_(RD, #ctx{sql_type = QueryType,
                       compiled_query = SQL,
                       table = Table} = Ctx, DDL) ->
    case riak_kv_ts_api:query(SQL, DDL) of
        {ok, Data} ->
            {ColumnNames, ColumnTypes, Rows} = Data,
            Json = to_json({ColumnNames, ColumnTypes, Rows}),
            {true, wrq:append_to_response_body(Json, RD), Ctx};
        %% the following timeouts are known and distinguished:
        {error, qry_worker_timeout} ->
            %% the eleveldb process didn't send us any response after
            %% 10 sec (hardcoded in riak_kv_qry), and probably died
            riak_kv_wm_ts_util:handle_error(query_worker_timeout, RD, Ctx);
        {error, backend_timeout} ->
            %% the eleveldb process did manage to send us a timeout
            %% response
            riak_kv_wm_ts_util:handle_error(backend_timeout, RD, Ctx);
        {error, invalid_coverage_context_checksum} ->
            riak_kv_wm_ts_util:handle_error({parameter_error, "Query coverage context fails checksum"}, RD, Ctx);
        {error, bad_coverage_context} ->
            riak_kv_wm_ts_util:handle_error({parameter_error, "Bad coverage context"}, RD, Ctx);
        {error, Reason} ->
            riak_kv_wm_ts_util:handle_error({query_exec_error, QueryType, Table, Reason}, RD, Ctx)
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Helper functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
query_from_request(RD) ->
    QueryStr = query_string_from_request(RD),
    compile_query(QueryStr).

query_string_from_request(RD) ->
    case wrq:req_body(RD) of
        undefined ->
            throw(no_query_in_body);
        Str ->
            binary_to_list(Str)
    end.

compile_query(QueryStr) ->
    case catch riak_ql_parser:ql_parse(
                 riak_ql_lexer:get_tokens(QueryStr)) of
        %% parser messages have a tuple for Reason:
        {error, {_LineNo, riak_ql_parser, Msg}} when is_integer(_LineNo) ->
            throw({query_parse_error, Msg});
        {error, {Token, riak_ql_parser, _}} ->
            throw({query_parse_error, io_lib:format("Unexpected token: '~s'", [Token])});
        {'EXIT', {Reason, _StackTrace}} ->  %% these come from deep in the lexer
            throw({query_parse_error, Reason});
        {error, Reason} ->
            throw({query_compile_error, Reason});
        {ddl, _DDL, _Props} = Res ->
            Res;
        {Type, Compiled} ->
            {ok, SQL} = riak_kv_ts_util:build_sql_record(
                          Type, Compiled, []),
            {Type, SQL, undefined}
    end.


create_table(DDL = ?DDL{table = Table}, WithProps) ->
    case riak_kv_ts_api:create_table(?MODULE, DDL, WithProps) of
        ok -> ok;
        {error, Reason} -> {error, {table_create_fail, Table, Reason}}
    end.

flat_format(Format, Args) ->
    lists:flatten(io_lib:format(Format, Args)).

make_table_create_fail_resp(Table, Reason) ->
    flat_format("Failed to create table ~ts: ~p", [Table, Reason]).

make_table_activate_error_timeout_resp(Table) ->
    flat_format("Timed out while attempting to activate table ~ts", [Table]).

make_table_created_missing_resp(Table) ->
    flat_format("Table ~ts has been created but found missing", [Table]).

-spec produce_doc_body(#wm_reqdata{}, #ctx{}) -> cb_rv_spec(iolist()).
produce_doc_body(RD, Ctx = #ctx{result = {Columns, Rows}}) ->
    {mochijson2:encode(
       {struct, [{<<"columns">>, Columns},
                 {<<"rows">>, Rows}]}),
     RD, Ctx}.

to_json({Columns, Types, Rows}) ->
    mochijson2:encode(
      {struct, [{<<"columns">>, Columns},
                {<<"rows">>, rows_to_json_compat(Types, Rows)}]});
to_json(Other) ->
    mochijson2:encode(Other).

rows_to_json_compat(Types, Rows) ->
    lists:map(fun(R) -> row_to_json_compat(Types, R) end,
              Rows).

row_to_json_compat(Types, Row) ->
    lists:map(fun riak_kv_wm_timeseries:value_to_json_compat/1,
              lists:zip(Types, Row)).

%% log(Format, Args) ->
%%     lager:log(info, self(), Format, Args).
