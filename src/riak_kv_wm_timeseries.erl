%% -------------------------------------------------------------------
%%
%% riak_kv_wm_timeseries: Webmachine resource for riak TS operations.
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
%% GET     /ts/v1/table/Table/keys/K1/V1/...  single-key get
%% DELETE  /ts/v1/table/Table/keys/K1/V1/...  single-key delete
%% POST    /ts/v1/table/Table/keys            singe-key or batch put depending
%%                                            on the body
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
         delete_resource/2,
         content_types_provided/2,
         encodings_provided/2,
         produce_doc_body/2,
         accept_doc_body/2
        ]).

-include_lib("webmachine/include/webmachine.hrl").
-include_lib("riak_ql/include/riak_ql_ddl.hrl").
-include("riak_kv_wm_raw.hrl").
-include("riak_kv_ts.hrl").

-record(ctx, {api_version,
              method  :: atom(),
              prefix,       %% string() - prefix for resource uris
              timeout,      %% integer() - passed-in timeout value in ms
              security,     %% security context
              client,       %% riak_client() - the store client
              riak,         %% local | {node(), atom()} - params for riak client
              api_call :: undefined|get|put|delete,
              table    :: undefined | binary(),
              mod      :: undefined | module(),
              ddl      :: undefined | #ddl_v1{},
              %% data in/out: the following fields are either
              %% extracted from the JSON/path elements that came in
              %% the request body in case of a PUT, or filled out by
              %% retrieved values for shipping (as JSON) in response
              %% body
              key     :: undefined |  ts_rec(),  %% parsed out of JSON that came in the body
              data    :: undefined | [ts_rec()], %% ditto
              result  :: undefined | ok | {Headers::[binary()], Rows::[ts_rec()]}
             }).

-define(DEFAULT_TIMEOUT, 60000).
-define(TABLE_ACTIVATE_WAIT, 30).   %% wait until table's bucket type is activated

-define(CB_RV_SPEC, {boolean()|atom()|tuple(), #wm_reqdata{}, #ctx{}}).
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
%%      opportunity to extract the 'table' and 'key' path
%%      bindings from the dispatch.
service_available(RD, Ctx = #ctx{riak = RiakProps}) ->
    case riak_kv_wm_utils:get_riak_client(
           RiakProps, riak_kv_wm_utils:get_client_id(RD)) of
        {ok, C} ->
            {true, RD,
             Ctx#ctx{api_version = wrq:path_info(api_version, RD),
                     method = wrq:method(RD),
                     client = C,
                     table = utf8_to_binary(
                               mochiweb_util:unquote(
                                 wrq:path_info(table, RD)))
                    }};
        {error, Reason} ->
            handle_error({riak_client_error, Reason}, RD, Ctx)
    end.


is_authorized(RD, Ctx) ->
    case riak_api_web_security:is_authorized(RD) of
        false ->
            {"Basic realm=\"Riak\"", RD, Ctx};
        {true, SecContext} ->
            {true, RD, Ctx#ctx{security = SecContext}};
        insecure ->
            handle_error(insecure_connection, RD, Ctx)
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


-spec allowed_methods(#wm_reqdata{}, #ctx{}) ->
    {[atom()], #wm_reqdata{}, #ctx{}}.
%% @doc Get the list of methods this resource supports.
allowed_methods(RD, Ctx) ->
    {['GET', 'POST', 'DELETE'], RD, Ctx}.


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
                {true, RD2, Ctx2} ->
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

-spec validate_request_v1(#wm_reqdata{}, #ctx{}) ->
                                 ?CB_RV_SPEC.
validate_request_v1(RD, Ctx = #ctx{method = 'POST'}) ->
    Json = extract_json(RD),
    case extract_data(Json) of
        Data when Data /= undefined ->
            valid_params(
              RD, Ctx#ctx{api_version = "v1", api_call = put,
                          data = Data});
        _Invalid ->
            handle_error({malformed_request, 'POST'}, RD, Ctx)
    end;

validate_request_v1(RD, Ctx = #ctx{method = 'GET', table = Table,
                                   mod = Mod, ddl = DDL}) ->
    KeysInUrl = lists:map(fun mochiweb_util:unquote/1, wrq:path_tokens(RD)),
    case path_elements_to_key(Table, KeysInUrl, Mod, DDL) of
        {ok, Key} ->
            valid_params(
              RD, Ctx#ctx{api_version = "v1", api_call = get,
                          key = Key});
        {error, Reason} ->
            handle_error(Reason, RD, Ctx)
    end;

validate_request_v1(RD, Ctx = #ctx{method = 'DELETE', table = Table,
                                  mod = Mod, ddl = DDL}) ->
    KeysInUrl = lists:map(fun mochiweb_util:unquote/1, wrq:path_tokens(RD)),
    case path_elements_to_key(Table, KeysInUrl, Mod, DDL) of
        {ok, Key} ->
            valid_params(
              RD, Ctx#ctx{api_version = "v1", api_call = delete,
                          key = Key});
        {error, Reason} ->
            handle_error(Reason, RD, Ctx)
    end.

extract_json(RD) ->
    case proplists:get_value("json", RD#wm_reqdata.req_qs) of
        undefined ->
            %% if it was a PUT or POST, data is in body
            binary_to_list(wrq:req_body(RD));
        BodyInPost ->
            BodyInPost
    end.

%% because, techically, key and data are 'arguments', we check they
%% are well-formed, too.
-spec extract_data(binary()) -> term().
extract_data(Json) ->
    try mochijson2:decode(Json) of
        {struct, Decoded} when is_list(Decoded) ->
            %% (columns and) data for put
            validate_ts_records(
              proplists:get_value(<<"data">>, Decoded))
    catch
        _:_ ->
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


%% extract keys from path elements in the URL (.../K1/V1/K2/V2 ->
%% [{K1, V1}, {K2, V2}]), check with Table's DDL to make sure keys are
%% correct and values are of (convertible to) appropriate types, and
%% return the KV list
-spec path_elements_to_key(binary(), [string()], module(), #ddl_v1{}) ->
                                  {ok, [{string(), riak_pb_ts_codec:ldbvalue()}]} |
                                  {error, atom()|tuple()}.
path_elements_to_key(Table, PEList, Mod,
                     #ddl_v1{local_key = #key_v1{ast = LK}}) ->
    try
        TableKeyLength = length(LK),
        if TableKeyLength * 2 == length(PEList) ->
                %% values with field names:  "f1/v1/f2/v2/f3/v3"
                %% 1. check that supplied key fields exist and values
                %% supplied are convertible to their types
                FVList =
                    [convert_fv(Table, Mod, K, V)
                     || {K, V} <- empair(PEList, [])],
                %% 2. possibly reorder field-value pairs to match the LK order
                OrderedKeyValues =
                    ensure_lk_order_and_strip(LK, FVList),
                {ok, OrderedKeyValues};
           TableKeyLength == length(PEList) ->
                %% bare values: "v1/v2/v3"
                %% 1. retrieve field values from the DDL
                Fields = [F || #param_v1{name = F} <- LK],
                FVList =
                    [convert_fv(Table, Mod, K, V)
                     || {K, V} <- lists:zip(Fields, PEList)],
                {_, OrderedKeyValues} =
                    lists:unzip(FVList),
                {ok, OrderedKeyValues};
           el/=se ->
                {error, url_unpaired_keys}
        end
    catch
        throw:ConvertFailed ->
            {error, ConvertFailed}
    end.

empair([], Q) -> lists:reverse(Q);
empair([K, V | T], Q) -> empair(T, [{K, V}|Q]).

convert_fv(Table, Mod, FieldRaw, V) ->
    Field = [list_to_binary(X) || X <- string:tokens(FieldRaw, ".")],
    case Mod:is_field_valid(Field) of
        true ->
            try
                convert_field(Table, Field, Mod:get_field_type(Field), V)
            catch
                error:badarg ->
                    %% rethrow with key, for more informative reporting
                    throw({url_key_bad_value, Table, Field});
            false ->
                throw({url_key_bad_key, Table, Field})
        end
    end.

convert_field(_T, F, varchar, V) ->
    {F, list_to_binary(V)};
convert_field(_T, F, sint64, V) ->
    {F, list_to_integer(V)};
convert_field(_T, F, double, V) ->
    %% list_to_float("42") will fail, so
    try
        {F, list_to_float(V)}
    catch
        error:badarg ->
            {F, float(list_to_integer(V))}
    end;
convert_field(T, F, timestamp, V) ->
    case list_to_integer(V) of
        BadValue when BadValue < 1 ->
            throw({url_key_bad_value, T, F});
        GoodValue ->
            {F, GoodValue}
    end.


ensure_lk_order_and_strip(LK, FVList) ->
    [proplists:get_value(F, FVList)
     || #param_v1{name = F} <- LK].

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

-spec check_permissions(#wm_reqdata{}, #ctx{}) -> {term(), #wm_reqdata{}, #ctx{}}.
check_permissions(RD, Ctx = #ctx{security = undefined}) ->
    {true, RD, Ctx};
check_permissions(RD, Ctx = #ctx{security = Security,
                                 api_call = Call,
                                 table = Table}) ->
    case riak_core_security:check_permission(
           {riak_kv_ts_util:api_call_to_perm(Call), Table}, Security) of
        {false, Error, _} ->
            handle_error(
              {not_permitted, utf8_to_binary(Error)}, RD, Ctx);
        _ ->
            {true, RD, Ctx}
    end.


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
resource_exists(RD, Ctx = #ctx{table = Table}) ->
    Mod = riak_ql_ddl:make_module_name(Table),
    try
        DDL = Mod:get_ddl(),
        {true, RD, Ctx#ctx{mod = Mod, ddl = DDL}}
    catch
        error:undef ->
            handle_error({no_such_table, Table}, RD, Ctx)
    end.

-spec process_post(#wm_reqdata{}, #ctx{}) -> ?CB_RV_SPEC.
%% @doc Pass through requests to allow POST to function
%%      as PUT for clients that do not support PUT.
process_post(RD, Ctx) ->
    accept_doc_body(RD, Ctx).

-spec delete_resource(#wm_reqdata{}, #ctx{}) -> ?CB_RV_SPEC.
%% same for DELETE
delete_resource(RD, Ctx) ->
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
    case riak_kv_ts_util:validate_rows(Mod, Records) of
        [] ->
            case riak_kv_ts_api:put_data(Records, Table, Mod) of
                ok ->
                    prepare_data_in_body(RD, Ctx#ctx{result = ok});
                {error, {some_failed, ErrorCount}} ->
                    handle_error({failed_some_puts, ErrorCount, Table}, RD, Ctx);
                {error, no_ctype} ->
                    handle_error({no_such_table, Table}, RD, Ctx)
            end;
        BadRowIdxs when is_list(BadRowIdxs) ->
            handle_error({invalid_data, BadRowIdxs}, RD, Ctx)
    end;

call_api_function(RD, Ctx0 = #ctx{api_call = get,
                                  table = Table, key = Key, mod = Mod,
                                  timeout = Timeout}) ->
    Options =
        if Timeout == undefined -> [];
           true -> [{timeout, Timeout}]
        end,
    case riak_kv_ts_api:get_data(Key, Table, Mod, Options) of
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
            handle_error(notfound, RD, Ctx0);
        {error, {bad_key_length, Got, Need}} ->
            handle_error({key_element_count_mismatch, Got, Need}, RD, Ctx0);
        {error, Reason} ->
            handle_error({riak_error, Reason}, RD, Ctx0)
    end;

call_api_function(RD, Ctx = #ctx{api_call = delete,
                                 table = Table, key = Key,
                                 mod = Mod,
                                 timeout = Timeout}) ->
    Options =
        if Timeout == undefined -> [];
           true -> [{timeout, Timeout}]
        end,
    case riak_kv_ts_api:delete_data(Key, Table, Mod, Options) of
        ok ->
            prepare_data_in_body(RD, Ctx#ctx{result = ok});
        {error, {bad_key_length, Got, Need}} ->
            handle_error({key_element_count_mismatch, Got, Need}, RD, Ctx);
        {error, notfound} ->
            handle_error(notfound, RD, Ctx);
        {error, Reason} ->
            handle_error({riak_error, Reason}, RD, Ctx)
    end.


prepare_data_in_body(RD0, Ctx0) ->
    {Json, RD1, Ctx1} = produce_doc_body(RD0, Ctx0),
    {true, wrq:append_to_response_body(Json, RD1), Ctx1}.


-spec produce_doc_body(#wm_reqdata{}, #ctx{}) -> ?CB_RV_SPEC.
produce_doc_body(RD0, Ctx0 = #ctx{result = undefined}) ->
    case preexec(RD0, Ctx0) of
        {true, RD1, Ctx1} ->
            case call_api_function(RD1, Ctx1) of
                {true, RD2, Ctx2} ->
                    produce_doc_body(RD2, Ctx2);
                FalseWithDetails ->
                    FalseWithDetails
            end;
        FalseWithDetails ->
            FalseWithDetails
    end;
produce_doc_body(RD, Ctx = #ctx{result = ok}) ->
    {<<"ok">>, RD, Ctx};
produce_doc_body(RD, Ctx = #ctx{api_call = get,
                                result = {Columns, Rows}}) ->
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
        {riak_client_error, Reason} ->
            error_out(false,
                      "Unable to connect to Riak: ~p", [Reason], RD, Ctx);
        insecure_connection ->
            error_out({halt, 426},
                      "Security is enabled and Riak does not"
                      " accept credentials over HTTP. Try HTTPS instead.", [], RD, Ctx);
        {unsupported_version, BadVersion} ->
            error_out({halt, 412},
                      "Unsupported API version ~s", [BadVersion], RD, Ctx);
        {not_permitted, Table} ->
            error_out({halt, 401},
                      "Access to table ~ts not allowed", [Table], RD, Ctx);
        {malformed_request, Method} ->
            error_out({halt, 400},
                      "Malformed ~s request", [Method], RD, Ctx);
        {url_key_bad_method, Method} ->
            error_out({halt, 400},
                      "Inappropriate ~s request", [Method], RD, Ctx);
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
        {url_key_bad_key, Table, Key} ->
            error_out({halt, 400},
                      "Table \"~ts\" has no field named \"~s\"", [Table, Key], RD, Ctx);
        {url_key_bad_value, Table, Key} ->
            error_out({halt, 400},
                      "Bad value for field \"~s\" in table \"~ts\"", [Key, Table], RD, Ctx);
        url_unpaired_keys ->
            error_out({halt, 400},
                      "Unpaired field/value for key spec in URL", [], RD, Ctx);
        notfound ->
            error_out({halt, 404},
                      "Key not found", [], RD, Ctx);
        {riak_error, Detailed} ->
            error_out({halt, 500},
                      "Internal riak error: ~p", [Detailed], RD, Ctx)
    end.

flat_format(Format, Args) ->
    lists:flatten(io_lib:format(Format, Args)).

utf8_to_binary(S) ->
    unicode:characters_to_binary(S, utf8, utf8).
