%% -------------------------------------------------------------------
%%
%% riak_kv_wm_timeseris: Webmachine resource for riak TS operations.
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
%%      Copied with heavy modifications from riak_kv_wm_object.erl.
%%
%% ```
%% GET    /ts/v1/table/Table          single-key get
%% DELETE /ts/v1/table/Table          single-key delete
%% PUT    /ts/v1/table/Table          batch put
%% GET    /ts/v1/table/Table/keys     list_keys
%% GET    /ts/v1/coverage             coverage for a query
%% GET/POST   /ts/v1/query            execute SQL query
%% '''
%%
%% Request body is expected to be a JSON containing key and/or value(s).
%% Response is a JSON containing data rows with column headers.
%%

-module(riak_kv_wm_timeseries).

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
              api_call :: undefined|get|put|delete|list_keys|query|coverage,
              table    :: undefined | binary(),
              cover_context :: undefined | binary(),
              %% data in/out: the following fields are either
              %% extracted from the JSON that came in the request body
              %% in case of a PUT, or filled out by retrieved values
              %% for shipping (as JSON) in response body
              key     :: undefined |  ts_rec(),  %% parsed out of JSON that came in the body
              data    :: undefined | [ts_rec()], %% ditto
              query   :: undefined | string(),
              result  :: undefined | ok | {Headers::[binary()], Rows::[ts_rec()]}
                                   | [{entry, proplists:proplist()}],
              error   :: undefined | {Errcode::integer(), Errmsg::binary()}
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
            %%preexec(RD, Ctx)
            %%validate_request(RD, Ctx)
            %% plug in early, and just do what it takes to do the job
            {false, RD, Ctx}
    end.
%% Because webmachine chooses to (not) call certain callbacks
%% depending on request method used, sometimes accept_doc_body is not
%% called at all, and we arrive at produce_doc_body empty-handed.
%% This is the case when curl is executed with -X GET and --data.


-spec allowed_methods(#wm_reqdata{}, #ctx{}) ->
    {[atom()], #wm_reqdata{}, #ctx{}}.
%% @doc Get the list of methods this resource supports.
allowed_methods(RD, Ctx) ->
    {['GET', 'POST', 'PUT', 'DELETE'], RD, Ctx}.


-spec malformed_request(#wm_reqdata{}, #ctx{}) -> ?CB_RV_SPEC.
%% @doc Determine whether query parameters, request headers,
%%      and request body are badly-formed.
malformed_request(RD, Ctx) ->
    %% this is plugged because requests are validated against
    %% effective parameters contained in the body (and hence, we need
    %% accept_doc_body to parse and extract things out of JSON in the
    %% body)
    {false, RD, Ctx}.


-spec preexec(#wm_reqdata{}, #ctx{}) -> ?CB_RV_SPEC.
%% * collect any parameters from request body or, failing that, from
%%   POST k=v items;
%% * check API version;
%% * validate those parameters against URL and method;
%% * determine which api call to do, and check permissions on that;
preexec(RD, Ctx = #ctx{api_call = Call})
  when Call /= undefined ->
    %% been here, figured and executed api call, stored results for
    %% shipping to client
    {true, RD, Ctx};
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
          extract_key(Json), extract_data(Json),
          extract_query(Json), extract_cover_context(Json)} of
        %% single-key get
        {'GET',
         ["ts", "v1", "tables", Table],
         Key, undefined, undefined, undefined}
          when is_list(Table), Key /= undefined ->
            valid_params(
              RD, Ctx#ctx{api_version = "v1", api_call = get,
                          table = list_to_binary(Table), key = Key});
        %% single-key delete
        {'DELETE',
         ["ts", "v1", "tables", Table],
         Key, undefined, undefined, undefined}
          when is_list(Table), Key /= undefined ->
            valid_params(
              RD, Ctx#ctx{api_version = "v1", api_call = delete,
                          table = list_to_binary(Table), key = Key});
        %% batch put
        {'PUT',
         ["ts", "v1", "tables", Table],
         undefined, Data, undefined, undefined}
          when is_list(Table), Data /= undefined ->
            valid_params(
              RD, Ctx#ctx{api_version = "v1", api_call = put,
                          table = list_to_binary(Table), data = Data});
        %% list_keys
        {'GET',
         ["ts", "v1", "tables", Table, "keys"],
         undefined, undefined, undefined, undefined}
          when is_list(Table) ->
            valid_params(
              RD, Ctx#ctx{api_version = "v1", api_call = list_keys,
                          table = list_to_binary(Table)});
        %% coverage
        {'GET',
         ["ts", "v1", "coverage"],
         undefined, undefined, Query, undefined}
          when is_list(Query) ->
            valid_params(
              RD, Ctx#ctx{api_version = "v1", api_call = coverage,
                          query = Query});
        %% query
        {Method,
         ["ts", "v1", "query"],
         undefined, undefined, Query, CoverContext}
          when (Method == 'GET' orelse Method == 'POST' orelse Method == 'PUT')
               andalso is_list(Query) ->
            valid_params(
              RD, Ctx#ctx{api_version = "v1", api_call = query,
                          query = Query, cover_context = CoverContext});
        _Invalid ->
            handle_error({malformed_request, Method}, RD, Ctx)
    end.


-spec valid_params(#wm_reqdata{}, #ctx{}) -> ?CB_RV_SPEC.
valid_params(RD, Ctx) ->
    case wrq:get_qs_value("timeout", none, RD) of
        none ->
            {true, RD, Ctx};
        TimeoutStr ->
            try
                Timeout = list_to_integer(TimeoutStr),
                {true, RD, Ctx#ctx{timeout = Timeout}}
            catch
                _:_ ->
                    handle_error({bad_parameter, "timeout"}, RD, Ctx)
            end
    end.

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

-spec extract_key(binary()) -> term().
extract_key(Json) ->
    case catch mochijson2:decode(Json) of
        {struct, [{<<"key">>, Key}]} ->
            %% key alone (it's a get or delete)
            validate_ts_record(Key);
        Decoded when is_list(Decoded) ->
            %% key and data (it's a put)
            validate_ts_record(
              proplists:get_value(<<"key">>, Decoded));
        _ ->
            undefined
    end.

%% because, techically, key and data are 'arguments', we check they
%% are well-formed, too.
-spec extract_data(binary()) -> term().
extract_data(Json) ->
    case catch mochijson2:decode(Json) of
        {struct, Decoded} when is_list(Decoded) ->
            %% key and data (it's a put)
            validate_ts_records(
              proplists:get_value(<<"data">>, Decoded));
        _ ->
            undefined
    end.

-spec extract_query(binary()) -> term().
extract_query(Json) ->
    case catch mochijson2:decode(Json) of
        {struct, Decoded} when is_list(Decoded) ->
            validate_ts_query(
              proplists:get_value(<<"query">>, Decoded));
        _ ->
            undefined
    end.

-spec extract_cover_context(binary()) -> term().
extract_cover_context(Json) ->
    case catch mochijson2:decode(Json) of
        Decoded when is_list(Decoded) ->
            validate_ts_cover_context(
              proplists:get_value(<<"coverage_context">>, Decoded));
        _ ->
            undefined
    end.


validate_ts_record(undefined) ->
    undefined;
validate_ts_record(R) when is_list(R) ->
    case lists:all(
           %% check that all list elements are TS types
           fun(X) -> is_integer(X) orelse is_float(X) orelse is_binary(X) end,
           R) of
        true ->
            R;
        false ->
            undefined
    end;
validate_ts_record(_) ->
    undefined.

validate_ts_records(undefined) ->
    undefined;
validate_ts_records(RR) when is_list(RR) ->
    case lists:all(fun(R) -> validate_ts_record(R) /= undefined end, RR) of
        true ->
            RR;
        false ->
            undefined
    end;
validate_ts_records(_) ->
    undefined.

validate_ts_query(Q) when is_binary(Q) ->
    binary_to_list(Q);
validate_ts_query(_) ->
    undefined.

validate_ts_cover_context(C) when is_binary(C) ->
    C;
validate_ts_cover_context(_) ->
    undefined.


-spec check_permissions(#wm_reqdata{}, #ctx{}) -> ?CB_RV_SPEC.
%% We have to defer checking permission until we have figured which
%% api call it is, which is done in validate_request, which also needs
%% body, which happens to not be available in Ctx when webmachine
%% would normally call a forbidden callback. I *may* be missing
%% something, but given the extent we have bent the REST rules here,
%% checking permissions at a stage later than webmachine would have
%% done is not a big deal.
check_permissions(RD, Ctx = #ctx{security = undefined}) ->
    validate_resource(RD, Ctx);
check_permissions(RD, Ctx = #ctx{table = undefined}) ->
    {false, RD, Ctx};
check_permissions(RD, Ctx = #ctx{security = Security,
                                 api_call = Call,
                                 table = Table}) ->
    case riak_core_security:check_permission(
           {api_call_to_ts_perm(Call), {Table, Table}}, Security) of
        {false, Error, _} ->
            handle_error(
              {not_permitted, unicode:characters_to_binary(Error, utf8, utf8)}, RD, Ctx);
        _ ->
            validate_resource(RD, Ctx)
    end.

api_call_to_ts_perm(get) ->
    "riak_ts.get";
api_call_to_ts_perm(put) ->
    "riak_ts.put";
api_call_to_ts_perm(delete) ->
    "riak_ts.delete";
api_call_to_ts_perm(query) ->
    "riak_ts.query".

-spec validate_resource(#wm_reqdata{}, #ctx{}) -> ?CB_RV_SPEC.
validate_resource(RD, Ctx = #ctx{api_call = Call})
  when Call == query;
       Call == coverage ->
    %% there is always a resource for queries
    {false, RD, Ctx};
validate_resource(RD, Ctx = #ctx{table = Table}) ->
    %% Ensure the bucket type exists, otherwise 404 early.
    case riak_kv_wm_utils:bucket_type_exists(Table) of
        true ->
            {true, RD, Ctx};
        false ->
            handle_error({no_such_table, Table}, RD, Ctx)
    end.


-spec content_types_provided(#wm_reqdata{}, #ctx{}) ->
                                    {[{ContentType::string(), Producer::atom()}],
                                     #wm_reqdata{}, #ctx{}}.
%% @doc List the content types available for representing this resource.
content_types_provided(RD, Ctx) ->
    {[{"application/json", produce_doc_body}], RD, Ctx}.


-spec encodings_provided(#wm_reqdata{}, #ctx{}) ->
                                {[{Encoding::string(), Producer::function()}],
                                 #wm_reqdata{}, #ctx{}}.
%% @doc List the encodings available for representing this resource.
%%      "identity" and "gzip" are available.
encodings_provided(RD, Ctx) ->
    %% identity and gzip
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
call_api_function(RD, Ctx = #ctx{api_call = put,
                                 table = Table, data = Data}) ->
    Mod = riak_ql_ddl:make_module_name(Table),
    %% convert records to tuples, just for put
    Records = [list_to_tuple(R) || R <- Data],
    case catch riak_kv_ts_util:validate_rows(Mod, Records) of
        {_, {undef, _}} ->
            handle_error({no_such_table, Table}, RD, Ctx);
        [] ->
            case riak_kv_ts_util:put_data(Records, Table, Mod) of
                ok ->
                    prepare_data_in_body(RD, Ctx#ctx{result = ok});
                {error, {some_failed, ErrorCount}} ->
                    handle_error({failed_some_puts, ErrorCount, Table}, RD, Ctx);
                {error, no_ctype} ->
                    handle_error({table_activate_fail, Table}, RD, Ctx)
            end;
        BadRowIdxs when is_list(BadRowIdxs) ->
            handle_error({invalid_data, BadRowIdxs}, RD, Ctx)
    end;

call_api_function(RD, Ctx0 = #ctx{api_call = get,
                                  table = Table, key = Key,
                                  timeout = Timeout}) ->
    Options =
        if Timeout == undefined -> [];
           true -> [{timeout, Timeout}]
        end,
    Mod = riak_ql_ddl:make_module_name(Table),
    case catch riak_kv_ts_util:get_data(Key, Table, Mod, Options) of
        {_, {undef, _}} ->
            handle_error({no_such_table, Table}, RD, Ctx0);
        {ok, Record} ->
            {ColumnNames, Row} = lists:unzip(Record),
            %% ColumnTypes = riak_kv_ts_util:get_column_types(ColumnNames, Mod),
            %% We don't need column types here as well (for the PB interface, we
            %% needed them in order to properly construct tscells)
            DataOut = {ColumnNames, [Row]},
            %% all results (from get as well as query) are returned in
            %% a uniform 'tabular' form, hence the [] around Row
            Ctx = Ctx0#ctx{result = DataOut},
            prepare_data_in_body(RD, Ctx);
        {error, notfound} ->
            Ctx = Ctx0#ctx{result = {[], []}},
            prepare_data_in_body(RD, Ctx);
        {error, {bad_key_length, Got, Need}} ->
            handle_error({key_element_count_mismatch, Got, Need}, RD, Ctx0);
        {error, Reason} ->
            handle_error({riak_error, Reason}, RD, Ctx0)
    end;

call_api_function(RD, Ctx = #ctx{api_call = delete,
                                 table = Table, key = Key,
                                 timeout = Timeout}) ->
    Options =
        if Timeout == undefined -> [];
           true -> [{timeout, Timeout}]
        end,
    Mod = riak_ql_ddl:make_module_name(Table),
    case catch riak_kv_ts_util:delete_data(Key, Table, Mod, Options) of
        {_, {undef, _}} ->
            handle_error({no_such_table, Table}, RD, Ctx);
        ok ->
            prepare_data_in_body(RD, Ctx#ctx{result = ok});
        {error, {bad_key_length, Got, Need}} ->
            handle_error({key_element_count_mismatch, Got, Need}, RD, Ctx);
        {error, notfound} ->
            handle_error(notfound, RD, Ctx);
        {error, Reason} ->
            handle_error({riak_error, Reason}, RD, Ctx)
    end;

call_api_function(RD, Ctx = #ctx{api_call = query,
                                 method = Method,
                                 query = Query, cover_context = CoverCtx}) ->
    Lexed = riak_ql_lexer:get_tokens(Query),
    case riak_ql_parser:parse(Lexed) of
        {ok, SQL = ?SQL_SELECT{}} when Method == 'GET' ->
            %% inject coverage context
            process_query(SQL?SQL_SELECT{cover_context = CoverCtx}, RD, Ctx);
        {ok, Other}
          when (is_record(Other, ddl_v1)               andalso Method == 'POST') orelse
               (is_record(Other, riak_sql_describe_v1) andalso Method ==  'GET') ->
            process_query(Other, RD, Ctx);
        {ok, _MethodMismatch} ->
            handle_error({inappropriate_sql_for_method, Method}, RD, Ctx);
        {error, Reason} ->
            handle_error({query_parse_error, Reason}, RD, Ctx)
    end;

call_api_function(RD, Ctx = #ctx{api_call = list_keys}) ->
    %% the streaming function for this is set up in produce_doc_body
    produce_doc_body(RD, Ctx);

call_api_function(RD, Ctx = #ctx{api_call = coverage,
                                 query = Query,
                                 client = Client}) ->
    Lexed = riak_ql_lexer:get_tokens(Query),
    case riak_ql_parser:parse(Lexed) of
        {ok, SQL = ?SQL_SELECT{'FROM' = Table}} ->
            Mod = riak_ql_ddl:make_module_name(Table),
            case riak_kv_ts_util:compile_to_per_quantum_queries(Mod, SQL) of
                {ok, Compiled} ->
                    Bucket = riak_kv_ts_util:table_to_bucket(Table),
                    Results =
                        [begin
                             Node = proplists:get_value(node, Cover),
                             {IP, Port} = riak_kv_pb_coverage:node_to_pb_details(Node),
                             {entry,
                              [
                               {cover_context,
                                riak_kv_pb_coverage:term_to_checksum_binary({Cover, Range})},
                               {ip, IP},
                               {port, Port},
                               {range,
                                [
                                 {field_name, FieldName},
                                 {lower_bound, StartVal},
                                 {lower_bound_inclusive, StartIncl},
                                 {upper_bound, EndVal},
                                 {upper_bound_inclusive, EndIncl},
                                 {desc, SQLtext}
                                ]}
                              ]}
                         end || {Cover,
                                 Range = {FieldName, {{StartVal, StartIncl}, {EndVal, EndIncl}}},
                                 SQLtext}
                                    <- riak_kv_ts_util:sql_to_cover(Client, Compiled, Bucket, [])],
                    prepare_data_in_body(RD, Ctx#ctx{result = Results});
                {error, _Reason} ->
                    handle_error(query_compile_fail, RD, Ctx)
            end;
        {ok, _NonSelectQuery} ->
            handle_error(inappropriate_sql_for_coverage, RD, Ctx);
        {error, Reason} ->
            handle_error({query_parse_error, Reason}, RD, Ctx)
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
            prepare_data_in_body(RD, Ctx#ctx{result = {[], []}});
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
produce_doc_body(RD, Ctx = #ctx{result = ok}) ->
    {<<"ok">>, RD, Ctx};
produce_doc_body(RD, Ctx = #ctx{api_call = list_keys,
                                table = Table,
                                client = Client, timeout = Timeout}) ->
    F = fun() ->
                {ok, ReqId} = Client:stream_list_keys(
                                {Table, Table}, Timeout),
                stream_keys(ReqId)
        end,
    {{stream, {[], F}}, RD, Ctx};
produce_doc_body(RD, Ctx = #ctx{api_call = Call,
                                result = {Columns, Rows}})
  when Call == get;
       Call == query ->
    {mochijson2:encode(
       {struct, [{<<"columns">>, Columns},
                 {<<"rows">>, Rows}]}),
     RD, Ctx};
produce_doc_body(RD, Ctx = #ctx{api_call = Call,
                                result = CoverageDetails})
  when Call == coverage ->
    SafeCoverageDetails =
        [{entry, armor_entry(E)} || {entry, E} <- CoverageDetails],
    {mochijson2:encode(
       {struct, [{<<"coverage">>, SafeCoverageDetails}]}),
     RD, Ctx}.

armor_entry(EE) ->
    lists:map(
      fun({cover_context, Bin}) ->
              %% prevent list to be read and converted by mochijson2
              %% as utf8 binary
              {cover_context, binary_to_list(Bin)};
         (X) -> X
      end, EE).

%% copied here from riak_kv_wm_keylist.erl
stream_keys(ReqId) ->
    receive
        %% skip empty shipments
        {ReqId, {keys, []}} ->
            stream_keys(ReqId);
        {ReqId, From, {keys, []}} ->
            _ = riak_kv_keys_fsm:ack_keys(From),
            stream_keys(ReqId);
        {ReqId, From, {keys, Keys}} ->
            _ = riak_kv_keys_fsm:ack_keys(From),
            {ts_keys_to_json(Keys), fun() -> stream_keys(ReqId) end};
        {ReqId, {keys, Keys}} ->
            {ts_keys_to_json(Keys), fun() -> stream_keys(ReqId) end};
        {ReqId, done} ->
            {mochijson2:encode({struct, [{<<"keys">>, []}]}), done};
        {ReqId, {error, timeout}} ->
            {mochijson2:encode({struct, [{error, timeout}]}), done}
    end.

ts_keys_to_json(Keys) ->
    KeysTerm = [tuple_to_list(sext:decode(A))
                || A <- Keys, A /= []],
    mochijson2:encode({struct, [{<<"keys">>, KeysTerm}]}).


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
        {malformed_request, Method} ->
            error_out({halt, 400},
                      "Malformed ~s request", [Method], RD, Ctx);
        {bad_parameter, Param} ->
            error_out({halt, 400},
                      "Bad value for parameter \"~s\"", [Param], RD, Ctx);
        {no_such_table, Table} ->
            error_out({halt, 404},
                      "Table \"~ts\" does not exist", [Table], RD, Ctx);
        {failed_some_puts, NoOfFailures, Table} ->
            error_out({halt, 400},
                      "Failed to put ~b records to table \"~ts\"", [NoOfFailures, Table], RD, Ctx);
        {invalid_data, BadRowIdxs} ->
            error_out({halt, 400},
                      "Invalid record #~s", [hd(BadRowIdxs)], RD, Ctx);
        {key_element_count_mismatch, Got, Need} ->
            error_out({halt, 400},
                      "Incorrect number of elements (~b) for key of length ~b", [Need, Got], RD, Ctx);
        notfound ->
            error_out({halt, 404},
                      "Key not found", [], RD, Ctx);
        {riak_error, Detailed} ->
            error_out({halt, 500},
                      "Internal riak error: ~p", [Detailed], RD, Ctx);
        {query_parse_error, Detailed} ->
            error_out({halt, 400},
                      "Malformed query: ~ts", [Detailed], RD, Ctx);
        inappropriate_sql_for_coverage ->
            error_out({halt, 400},
                      "Inappropriate query for coverage request", [], RD, Ctx);
        query_compile_fail ->
            error_out({halt, 400},
                      "Failed to compile query for coverage request", [], RD, Ctx);
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
                      "Inappropriate method ~s for SQL query type", [Method], RD, Ctx);
        OutOfTheBlue ->
            error_out({halt, 418},
                      "Phantom error: ~p", [OutOfTheBlue], RD, Ctx)
    end.

flat_format(Format, Args) ->
    lists:flatten(io_lib:format(Format, Args)).
