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
%% GET/POST   /ts/v1/query   execute SQL query
%% '''
%%
%% Request body is expected to be a JSON containing key and/or value(s).
%% Response is a JSON containing data rows with column headers.
%%

-module(riak_kv_wm_timeseries_query).

%% webmachine resource exports
-export([
         init/1,
         service_available/2,
         is_authorized/2,
         forbidden/2,
         allowed_methods/2,
         process_post/2,
         malformed_request/2,
         content_types_accepted/2,
         resource_exists/2,
         content_types_provided/2,
         encodings_provided/2,
         produce_doc_body/2,
         accept_doc_body/2
        ]).

-include("riak_kv_wm_raw.hrl").
-include_lib("webmachine/include/webmachine.hrl").
-include_lib("riak_ql/include/riak_ql_ddl.hrl").

-record(ctx, {api_version,
              method  :: atom(),
              prefix,       %% string() - prefix for resource uris
              timeout,      %% integer() - passed-in timeout value in ms
              security,     %% security context
              client,       %% riak_client() - the store client
              riak,         %% local | {node(), atom()} - params for riak client
              table          :: undefined | binary(),
              cover_context  :: undefined | binary(),
              query          :: undefined | string(),
              compiled_query :: undefined | #ddl_v1{} | #riak_sql_describe_v1{} | ?SQL_SELECT{},
              result         :: undefined | ok | {Headers::[binary()], Rows::[ts_rec()]} |
                                [{entry, proplists:proplist()}]
             }).

-define(DEFAULT_TIMEOUT, 60000).
-define(TABLE_ACTIVATE_WAIT, 30).   %% wait until table's bucket type is activated

-define(CB_RV_SPEC, {boolean(), #wm_reqdata{}, #ctx{}}).
-type ts_rec() :: [riak_pb_ts_codec:ldbvalue()].


-spec init(proplists:proplist()) -> {ok, #ctx{}}.
%% @doc Initialize this resource.  This function extracts the
%%      'prefix' and 'riak' properties from the dispatch args.
init(Props) ->
    {ok, #ctx{prefix = proplists:get_value(prefix, Props),
              riak = proplists:get_value(riak, Props)}}.

-spec service_available(#wm_reqdata{}, #ctx{}) ->
    {boolean(), #wm_reqdata{}, #ctx{}}.
%% @doc Determine whether or not a connection to Riak
%%      can be established.  This function also takes this
%%      opportunity to extract the 'bucket' and 'key' path
%%      bindings from the dispatch, as well as any vtag
%%      query parameter.
service_available(RD, Ctx = #ctx{riak = RiakProps}) ->
    case riak_kv_wm_utils:get_riak_client(
           RiakProps, riak_kv_wm_utils:get_client_id(RD)) of
        {ok, C} ->
            {true, RD,
             Ctx#ctx{api_version = wrq:path_info(api_version, RD),
                     method = wrq:method(RD),
                     client = C,
                     table =
                         case wrq:path_info(table, RD) of
                             undefined -> undefined;
                             B -> list_to_binary(riak_kv_wm_utils:maybe_decode_uri(RD, B))
                         end
                    }};
        Error ->
            {false, wrq:set_resp_body(
                      flat_format("Unable to connect to Riak: ~p", [Error]),
                      wrq:set_resp_header(?HEAD_CTYPE, "text/plain", RD)),
             Ctx}
    end.


is_authorized(ReqData, Ctx) ->
    case riak_api_web_security:is_authorized(ReqData) of
        false ->
            {"Basic realm=\"Riak\"", ReqData, Ctx};
        {true, SecContext} ->
            {true, ReqData, Ctx#ctx{security = SecContext}};
        insecure ->
            %% XXX 301 may be more appropriate here, but since the http and
            %% https port are different and configurable, it is hard to figure
            %% out the redirect URL to serve.
            {{halt, 426},
             wrq:append_to_resp_body(
               <<"Security is enabled and "
                 "Riak does not accept credentials over HTTP. Try HTTPS instead.">>, ReqData),
             Ctx}
    end.


-spec forbidden(#wm_reqdata{}, #ctx{}) -> ?CB_RV_SPEC.
forbidden(RD, Ctx) ->
    case riak_kv_wm_utils:is_forbidden(RD) of
        true ->
            {true, RD, Ctx};
        false ->
            %% plug in early, and just do what it takes to do the job
            {false, RD, Ctx}
    end.
%% Because webmachine chooses to (not) call certain callbacks
%% depending on request method used, sometimes accept_doc_body is not
%% called at all, and we arrive at produce_doc_body empty-handed.
%% This is the case when curl is executed with -G and --data.


-spec allowed_methods(#wm_reqdata{}, #ctx{}) ->
    {[atom()], #wm_reqdata{}, #ctx{}}.
allowed_methods(RD, Ctx) ->
    {['GET', 'POST'], RD, Ctx}.


-spec malformed_request(#wm_reqdata{}, #ctx{}) -> ?CB_RV_SPEC.
malformed_request(RD, Ctx) ->
    %% this is plugged because requests are validated against
    %% effective query contained in the body (and hence, we need
    %% accept_doc_body to parse and extract things out of JSON first)
    {false, RD, Ctx}.


-spec preexec(#wm_reqdata{}, #ctx{}) -> ?CB_RV_SPEC.
%% * extract query from request body or, failing that, from
%%   POST k=v items, try to compile it;
%% * check API version;
%% * validate query type against HTTP method;
%% * check permissions on the query type.
preexec(RD, Ctx) ->
    case validate_request(RD, Ctx) of
        {true, RD1, Ctx1} ->
            case check_permissions(RD1, Ctx1) of
                {false, RD2, Ctx2} ->
                    call_api_function(RD2, Ctx2);
                FalseWithDetails ->
                    FalseWithDetails
            end;
        FalseWithDetails ->
            FalseWithDetails
    end.

-spec validate_request(#wm_reqdata{}, #ctx{}) -> ?CB_RV_SPEC.
validate_request(RD, Ctx) ->
    case wrq:path_info(api_version, RD) of
        "v1" ->
            validate_request_v1(RD, Ctx);
        BadVersion ->
            handle_error({unsupported_version, BadVersion}, RD, Ctx)
    end.

-spec validate_request_v1(#wm_reqdata{}, #ctx{}) -> ?CB_RV_SPEC.
validate_request_v1(RD, Ctx = #ctx{method = Method}) ->
    Json = extract_json(RD),
    case {Method, string:tokens(wrq:path(RD), "/"),
          extract_query(Json), extract_cover_context(Json)} of
        {Method, ["ts", "v1", "query"],
         Query, CoverContext}
          when (Method == 'GET' orelse Method == 'POST')
               andalso is_list(Query) ->
            case riak_ql_parser:parse(
                   riak_ql_lexer:get_tokens(Query)) of
                {ok, CompiledQry} ->
                    valid_params(
                      RD, Ctx#ctx{api_version = "v1",
                                  query = Query, cover_context = CoverContext,
                                  compiled_query = CompiledQry});
                {error, Reason} ->
                    handle_error({query_parse_error, Reason}, RD, Ctx)
            end;
        _Invalid ->
            handle_error({malformed_request, Method}, RD, Ctx)
    end.


-spec valid_params(#wm_reqdata{}, #ctx{}) -> ?CB_RV_SPEC.
valid_params(RD, Ctx) ->
    %% no params currently for query
    {true, RD, Ctx}.

%% This is a special case for curl -G.  `curl -G host --data $data`
%% will send the $data in URL instead of in the body, so we try to
%% look for it in req_qs.
extract_json(RD) ->
    case proplists:get_value("json", RD#wm_reqdata.req_qs) of
        undefined ->
            %% if it was a PUT or POST, data is in body
            binary_to_list(wrq:req_body(RD));
        BodyInPost ->
            BodyInPost
    end.

-spec extract_query(binary()) -> term().
extract_query(Json) ->
    try mochijson2:decode(Json) of
        {struct, Decoded} when is_list(Decoded) ->
            validate_ts_query(
              proplists:get_value(<<"query">>, Decoded))
    catch
        _:_ ->
            undefined
    end.

-spec extract_cover_context(binary()) -> term().
extract_cover_context(Json) ->
    try mochijson2:decode(Json) of
        {struct, Decoded} when is_list(Decoded) ->
            validate_ts_cover_context(
              proplists:get_value(<<"coverage_context">>, Decoded))
    catch
        _:_ ->
            undefined
    end.


validate_ts_query(Q) when is_binary(Q) ->
    binary_to_list(Q);
validate_ts_query(_) ->
    undefined.

validate_ts_cover_context(C) when is_binary(C) ->
    C;
validate_ts_cover_context(_) ->
    undefined.


-spec check_permissions(#wm_reqdata{}, #ctx{}) -> ?CB_RV_SPEC.
check_permissions(RD, Ctx = #ctx{security = undefined}) ->
    {false, RD, Ctx};
check_permissions(RD, Ctx = #ctx{security = Security,
                                 compiled_query = CompiledQry}) ->
    case riak_core_security:check_permission(
           decode_query_permissions(CompiledQry), Security) of
        {false, Error, _} ->
            handle_error(
              {not_permitted, unicode:characters_to_binary(Error, utf8, utf8)}, RD, Ctx);
        _ ->
            {false, RD, Ctx}
    end.

decode_query_permissions(#ddl_v1{table = NewBucketType}) ->
    {"riak_kv.ts_create_table", NewBucketType};
decode_query_permissions(?SQL_SELECT{'FROM' = Table}) ->
    {"riak_kv.ts_query", Table};
decode_query_permissions(#riak_sql_describe_v1{'DESCRIBE' = Table}) ->
    {"riak_kv.ts_describe", Table}.


-spec content_types_provided(#wm_reqdata{}, #ctx{}) ->
                                    {[{ContentType::string(), Producer::atom()}],
                                     #wm_reqdata{}, #ctx{}}.
content_types_provided(RD, Ctx) ->
    {[{"application/json", produce_doc_body}], RD, Ctx}.


-spec encodings_provided(#wm_reqdata{}, #ctx{}) ->
                                {[{Encoding::string(), Producer::function()}],
                                 #wm_reqdata{}, #ctx{}}.
encodings_provided(RD, Ctx) ->
    {riak_kv_wm_utils:default_encodings(), RD, Ctx}.


-spec content_types_accepted(#wm_reqdata{}, #ctx{}) ->
                                    {[{ContentType::string(), Acceptor::atom()}],
                                     #wm_reqdata{}, #ctx{}}.
content_types_accepted(RD, Ctx) ->
    {[{"application/json", accept_doc_body}], RD, Ctx}.


-spec resource_exists(#wm_reqdata{}, #ctx{}) ->
                             {boolean(), #wm_reqdata{}, #ctx{}}.
resource_exists(RD0, Ctx0) ->
    case preexec(RD0, Ctx0) of
        {true, RD, Ctx} ->
            call_api_function(RD, Ctx);
        FalseWithDetails ->
            FalseWithDetails
    end.

-spec process_post(#wm_reqdata{}, #ctx{}) -> ?CB_RV_SPEC.
%% @doc Pass through requests to allow POST to function
%%      as PUT for clients that do not support PUT.
process_post(RD, Ctx) ->
    accept_doc_body(RD, Ctx).

-spec accept_doc_body(#wm_reqdata{}, #ctx{}) -> ?CB_RV_SPEC.
accept_doc_body(RD0, Ctx0) ->
    case preexec(RD0, Ctx0) of
        {true, RD, Ctx} ->
            call_api_function(RD, Ctx);
        FalseWithDetails ->
            FalseWithDetails
    end.

-spec call_api_function(#wm_reqdata{}, #ctx{}) -> ?CB_RV_SPEC.
call_api_function(RD, Ctx = #ctx{result = Result})
  when Result /= undefined ->
    lager:debug("Function already executed", []),
    {true, RD, Ctx};
call_api_function(RD, Ctx = #ctx{method = Method,
                                 compiled_query = CompiledQry,
                                 cover_context = CoverCtx}) ->
    case CompiledQry of
        SQL = ?SQL_SELECT{} when Method == 'GET' ->
            %% inject coverage context
            process_query(SQL?SQL_SELECT{cover_context = CoverCtx}, RD, Ctx);
        Other when (is_record(Other, ddl_v1)               andalso Method == 'POST') orelse
                   (is_record(Other, riak_sql_describe_v1) andalso Method ==  'GET') ->
            process_query(Other, RD, Ctx);
        _Other ->
            handle_error({inappropriate_sql_for_method, Method}, RD, Ctx)
    end.


process_query(DDL = #ddl_v1{table = Table}, RD, Ctx) ->
    {ok, Props1} = riak_kv_ts_util:apply_timeseries_bucket_props(DDL, []),
    Props2 = [riak_kv_wm_utils:erlify_bucket_prop(P) || P <- Props1],
    %% TODO: let's not bother collecting user properties from (say)
    %% sidecar object in body JSON: when #ddl_v2 work is merged, we
    %% will have a way to collect those bespoke table properties from
    %% WITH clause.
    case riak_core_bucket_type:create(Table, Props2) of
        ok ->
            wait_until_active(Table, RD, Ctx, ?TABLE_ACTIVATE_WAIT);
        {error, Reason} ->
            handle_error({table_create_fail, Table, Reason}, RD, Ctx)
    end;

process_query(SQL = ?SQL_SELECT{'FROM' = Table}, RD, Ctx0 = #ctx{}) ->
    Mod = riak_ql_ddl:make_module_name(Table),
    case catch Mod:get_ddl() of
        {_, {undef, _}} ->
            handle_error({no_such_table, Table}, RD, Ctx0);
        DDL ->
            case riak_kv_qry:submit(SQL, DDL) of
                {ok, Data} ->
                    {ColumnNames, _ColumnTypes, Rows} = Data,
                    Ctx = Ctx0#ctx{result = {ColumnNames, Rows}},
                    prepare_data_in_body(RD, Ctx);
                %% the following timeouts are known and distinguished:
                {error, qry_worker_timeout} ->
                    %% the eleveldb process didn't send us any response after
                    %% 10 sec (hardcoded in riak_kv_qry), and probably died
                    handle_error(query_worker_timeout, RD, Ctx0);
                {error, backend_timeout} ->
                    %% the eleveldb process did manage to send us a timeout
                    %% response
                    handle_error(backend_timeout, RD, Ctx0);

                {error, Reason} ->
                    handle_error({query_exec_error, Reason}, RD, Ctx0)
            end
    end;

process_query(SQL = #riak_sql_describe_v1{'DESCRIBE' = Table}, RD, Ctx0 = #ctx{}) ->
    Mod = riak_ql_ddl:make_module_name(Table),
    case catch Mod:get_ddl() of
        {_, {undef, _}} ->
            handle_error({no_such_table, Table}, RD, Ctx0);
        DDL ->
            case riak_kv_qry:submit(SQL, DDL) of
                {ok, Data} ->
                    ColumnNames = [<<"Column">>, <<"Type">>, <<"Is Null">>,
                                   <<"Primary Key">>, <<"Local Key">>],
                    Ctx = Ctx0#ctx{result = {ColumnNames, Data}},
                    prepare_data_in_body(RD, Ctx);
                {error, Reason} ->
                    handle_error({query_exec_error, Reason}, RD, Ctx0)
            end
    end.


wait_until_active(Table, RD, Ctx, 0) ->
    handle_error({table_activate_fail, Table}, RD, Ctx);
wait_until_active(Table, RD, Ctx, Seconds) ->
    case riak_core_bucket_type:activate(Table) of
        ok ->
            prepare_data_in_body(RD, Ctx#ctx{result = {[],[]}});
            %% a way for CREATE TABLE queries to return 'ok' on success
        {error, not_ready} ->
            timer:sleep(1000),
            wait_until_active(Table, RD, Ctx, Seconds - 1);
        {error, undefined} ->
            %% this is inconceivable because create(Table) has
            %% just succeeded, so it's here mostly to pacify
            %% the dialyzer (and of course, for the odd chance
            %% of Erlang imps crashing nodes between create
            %% and activate calls)
            handle_error({table_created_missing, Table}, RD, Ctx)
    end.

prepare_data_in_body(RD0, Ctx0) ->
    {Json, RD1, Ctx1} = produce_doc_body(RD0, Ctx0),
    {true, wrq:append_to_response_body(Json, RD1), Ctx1}.


-spec produce_doc_body(#wm_reqdata{}, #ctx{}) -> ?CB_RV_SPEC.
%% @doc Extract the value of the document, and place it in the
%%      response body of the request.
produce_doc_body(RD, Ctx = #ctx{result = {Columns, Rows}}) ->
    {mochijson2:encode(
       {struct, [{<<"columns">>, Columns},
                 {<<"rows">>, Rows}]}),
     RD, Ctx}.


error_out(Type, Fmt, Args, RD, Ctx) ->
    {Type,
     wrq:set_resp_header(
       "Content-Type", "text/plain", wrq:append_to_response_body(
                                       flat_format(Fmt, Args), RD)),
     Ctx}.

-spec handle_error(atom()|tuple(), #wm_reqdata{}, #ctx{}) -> {tuple(), #wm_reqdata{}, #ctx{}}.
handle_error(Error, RD, Ctx) ->
    case Error of
        {unsupported_version, BadVersion} ->
            error_out({halt, 412},
                      "Unsupported API version ~s", [BadVersion], RD, Ctx);
        {not_permitted, Table} ->
            error_out({halt, 401},
                      "Access to table ~s not allowed", [Table], RD, Ctx);
        {malformed_request, Method} ->
            error_out({halt, 400},
                      "Malformed ~s request", [Method], RD, Ctx);
        {no_such_table, Table} ->
            error_out({halt, 404},
                      "Table \"~ts\" does not exist", [Table], RD, Ctx);
        {query_parse_error, Detailed} ->
            error_out({halt, 400},
                      "Malformed query: ~ts", [Detailed], RD, Ctx);
        {table_create_fail, Table, Reason} ->
            error_out({halt, 500},
                      "Failed to create table \"~ts\": ~p", [Table, Reason], RD, Ctx);
        query_worker_timeout ->
            error_out({halt, 503},
                      "Query worker timeout", [], RD, Ctx);
        backend_timeout ->
            error_out({halt, 503},
                      "Storage backend timeout", [], RD, Ctx);
        {query_exec_error, Detailed} ->
            error_out({halt, 400},
                      "Query execution failed: ~ts", [Detailed], RD, Ctx);
        {table_activate_fail, Table} ->
            error_out({halt, 500},
                      "Failed to activate bucket type for table \"~ts\"", [Table], RD, Ctx);
        {table_created_missing, Table} ->
            error_out({halt, 500},
                      "Bucket type for table \"~ts\" disappeared suddenly before activation", [Table], RD, Ctx);
        {inappropriate_sql_for_method, Method} ->
            error_out({halt, 400},
                      "Inappropriate method ~s for SQL query type", [Method], RD, Ctx)
    end.

flat_format(Format, Args) ->
    lists:flatten(io_lib:format(Format, Args)).
