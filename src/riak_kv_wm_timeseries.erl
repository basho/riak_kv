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
%% This resource is responsible for everything under
%% ```
%% ts/v1/tables/Table/keys
%% ```
%% Specific operations supported:
%% ```
%% GET     /ts/v1/tables/Table/keys/K1/V1/...  single-key get
%% DELETE  /ts/v1/tables/Table/keys/K1/V1/...  single-key delete
%% POST    /ts/v1/tables/Table/keys            singe-key or batch put depending
%%                                             on the body
%% '''
%%
%% Request body is expected to be a JSON containing a struct or structs for the
%% POST. GET and DELETE have no body.
%%
%% Response is a JSON containing full records or {"success": true} for POST and
%% DELETE.
%%

-module(riak_kv_wm_timeseries).

%% webmachine resource exports
-export([
         init/1,
         service_available/2,
         allowed_methods/2,
         malformed_request/2,
         is_authorized/2,
         forbidden/2,
         content_types_provided/2,
         content_types_accepted/2,
         encodings_provided/2,
         post_is_create/2,
         process_post/2,
         delete_resource/2,
         resource_exists/2
        ]).

%% webmachine body-producing functions
-export([to_json/2]).

-include_lib("webmachine/include/webmachine.hrl").
-include_lib("riak_ql/include/riak_ql_ddl.hrl").
-include("riak_kv_wm_raw.hrl").

-record(ctx,
        {
          api_version :: undefined | integer(),
          api_call    :: undefined | get | put | delete,
          table       :: undefined | binary(),
          mod         :: undefined | module(),
          key         :: undefined | ts_rec(),
          object,
          timeout     :: undefined | integer(),
          options = [],  %% for the call towards riak.
          prefix,
          riak
        }).

-define(DEFAULT_TIMEOUT, 60000).

-type cb_rv_spec(T) :: {T, #wm_reqdata{}, #ctx{}}.
-type halt() :: {'halt', 200..599} | {'error' , term()}.
-type ts_rec() :: [riak_pb_ts_codec:ldbvalue()].

-spec init(proplists:proplist()) -> {ok, #ctx{}}.
%% @doc Initialize this resource.  This function extracts the
%%      'prefix' and 'riak' properties from the dispatch args.
%%      (But how exactly are those properties used?)
init(Props) ->
    {ok, #ctx{prefix = proplists:get_value(prefix, Props),
              riak = proplists:get_value(riak, Props)}}.

-spec service_available(#wm_reqdata{}, #ctx{}) -> cb_rv_spec(boolean() | halt()).
%% @doc Determine whether or not a connection to Riak
%%      can be established.
%%      Convert the table name from the part of the URL.
service_available(RD, Ctx) ->
    ApiVersion = riak_kv_wm_ts_util:extract_api_version(RD),
    case {riak_kv_wm_ts_util:is_supported_api_version(ApiVersion),
          init:get_status()} of
        {true, {started, _}} ->
            Table = riak_kv_wm_ts_util:table_from_request(RD),
            Mod = riak_ql_ddl:make_module_name(Table),
            {true, RD, Ctx#ctx{api_version = ApiVersion,
                               table = Table, mod = Mod}};
        {false, {started, _}} ->
            riak_kv_wm_ts_util:handle_error({unsupported_version, ApiVersion}, RD, Ctx);
        {_, {InternalStatus, _}} ->
            riak_kv_wm_ts_util:handle_error({not_ready, InternalStatus}, RD, Ctx)
    end.

is_authorized(RD, #ctx{table = Table} = Ctx) ->
    Call = api_call(wrq:path_tokens(RD), wrq:method(RD)),
    case riak_kv_wm_ts_util:authorize(Call, Table, RD) of
        ok ->
            {true, RD, Ctx#ctx{api_call = Call}};
        {error, ErrorMsg} ->
            riak_kv_wm_ts_util:handle_error({not_permitted, Table, ErrorMsg}, RD, Ctx);
        insecure ->
            riak_kv_wm_ts_util:handle_error(insecure_connection, RD, Ctx)
    end.

-spec forbidden(#wm_reqdata{}, #ctx{}) -> cb_rv_spec(boolean()).
forbidden(RD, Ctx) ->
    Result = riak_kv_wm_utils:is_forbidden(RD),
    {Result, RD, Ctx}.

-spec allowed_methods(#wm_reqdata{}, #ctx{}) -> cb_rv_spec([atom()]).
allowed_methods(RD, Ctx) ->
    allowed_methods(wrq:path_tokens(RD), RD, Ctx).

allowed_methods([], RD, Ctx) ->
    {['POST'], RD, Ctx};
allowed_methods(_KeyInURL, RD, Ctx) ->
    {['GET', 'DELETE'], RD, Ctx}.

-spec malformed_request(#wm_reqdata{}, #ctx{}) -> cb_rv_spec(boolean()).
malformed_request(RD, Ctx) ->
    try
        Ctx2 = extract_params(wrq:req_qs(RD), Ctx),
        %% NOTE: if the supplied JSON body is wrong a malformed requset
        %% may be issued later. It will indeed, only it will be generated
        %% manually, via handle_error, and not detected by webmachine from
        %% malformed_request reporting it directly.
        {false, RD, Ctx2}
    catch
        throw:ParamError ->
            riak_kv_wm_ts_util:handle_error(ParamError, RD, Ctx)
    end.

-spec content_types_provided(#wm_reqdata{}, #ctx{}) -> cb_rv_spec([{string(), atom()}]).
content_types_provided(RD, Ctx) ->
    {[{"application/json", to_json}],
     RD, Ctx}.

-spec content_types_accepted(#wm_reqdata{}, #ctx{}) -> cb_rv_spec([{string(), atom()}]).
content_types_accepted(RD, Ctx) ->
    content_types_accepted(wrq:path_tokens(RD), RD, Ctx).

content_types_accepted([], RD, Ctx) ->
    %% the JSON in the POST will be handled by process_post,
    %% so this handler will never be called.
    {[{"application/json", undefined}], RD, Ctx};
content_types_accepted(_, RD, Ctx) ->
    {[], RD, Ctx}.

-spec resource_exists(#wm_reqdata{}, #ctx{}) -> cb_rv_spec(boolean() | halt()).
resource_exists(RD, #ctx{table = Table, mod = Mod} = Ctx) ->
    case riak_kv_wm_ts_util:table_module_exists(Mod) of
        true ->
            resource_exists(wrq:path_tokens(RD), wrq:method(RD), RD, Ctx);
        false ->
            riak_kv_wm_ts_util:handle_error({no_such_table, Table}, RD, Ctx)
    end.

resource_exists([], 'POST', RD, Ctx) ->
    {true, RD, Ctx};
resource_exists(Path, 'GET', RD,
                #ctx{table = Table,
                     mod = Mod,
                     options = Options} = Ctx) ->
    %% Would be nice if something cheaper than using get_data existed to check
    %% if a key is present.
    case validate_key(Path, Ctx) of
        {ok, Key} ->
            case riak_kv_ts_api:get_data(Key, Table, Mod, Options) of
                {ok, Record} ->
                    {true, RD, Ctx#ctx{object = Record,
                                       key = Key}};
                {error, notfound} ->
                    riak_kv_wm_ts_util:handle_error(notfound, RD, Ctx);
                {error, InternalReason} ->
                    riak_kv_wm_ts_util:handle_error({riak_error, InternalReason}, RD, Ctx)
            end;
        {error, Reason} ->
            riak_kv_wm_ts_util:handle_error(Reason, RD, Ctx)
    end;
resource_exists(Path, 'DELETE', RD, Ctx) ->
    %% Since reading the object is expensive we will assume for now that the
    %% object exists for a delete, but if it turns out that it does not then the
    %% processing of the delete will return 404 at that point.
    case validate_key(Path, Ctx) of
        {ok, Key} ->
            {true, RD, Ctx#ctx{key = Key}};
        {error, Reason} ->
            riak_kv_wm_ts_util:handle_error(Reason, RD, Ctx)
    end.

-spec encodings_provided(#wm_reqdata{}, #ctx{}) ->
                                cb_rv_spec([{Encoding::string(), Producer::function()}]).
encodings_provided(RD, Ctx) ->
    {riak_kv_wm_utils:default_encodings(), RD, Ctx}.

-spec post_is_create(#wm_reqdata{}, #ctx{}) -> cb_rv_spec(boolean()).
post_is_create(RD, Ctx) ->
    {false, RD, Ctx}.

-spec process_post(#wm_reqdata{}, #ctx{}) ->  cb_rv_spec(boolean()).
process_post(RD, #ctx{mod = Mod,
                      table = Table} = Ctx) ->
    try extract_data(RD, Mod, Table) of
        Records ->
            case riak_kv_ts_util:validate_rows(Mod, Records) of
                [] ->
                    case riak_kv_ts_api:put_data(Records, Table, Mod) of
                        ok ->
                            Json = result_to_json(ok),
                            Resp = riak_kv_wm_ts_util:set_json_response(Json, RD),
                            {true, Resp, Ctx};
                        {error, {some_failed, ErrorCount}} ->
                            riak_kv_wm_ts_util:handle_error({failed_some_puts, ErrorCount, Table}, RD, Ctx)
                    end;
                BadRowIdxs when is_list(BadRowIdxs) ->
                    riak_kv_wm_ts_util:handle_error(
                      {invalid_data, string:join([integer_to_list(I) || I <- BadRowIdxs],", ")},
                      RD, Ctx)
            end
    catch
        throw:Reason ->
            riak_kv_wm_ts_util:handle_error(Reason, RD, Ctx)
    end.

-spec delete_resource(#wm_reqdata{}, #ctx{}) ->  cb_rv_spec(boolean()|halt()).
delete_resource(RD, #ctx{table = Table,
                         mod = Mod,
                         key = Key,
                         options = Options} = Ctx) ->
    case riak_kv_ts_api:delete_data(Key, Table, Mod, Options) of
        ok ->
            Json = result_to_json(ok),
            Resp = riak_kv_wm_ts_util:set_json_response(Json, RD),
            {true, Resp, Ctx};
        {error, notfound} ->
            riak_kv_wm_ts_util:handle_error(notfound, RD, Ctx);
        {error, Reason} ->
            riak_kv_wm_ts_util:handle_error({riak_error, Reason}, RD, Ctx)
     end.

-spec to_json(#wm_reqdata{}, #ctx{}) ->  cb_rv_spec(iolist()|halt()).
to_json(RD, #ctx{api_call = get, object = Object} = Ctx) ->
    try
        Json = mochijson2:encode(Object),
        {Json, RD, Ctx}
    catch
        _:Reason ->
            riak_kv_wm_ts_util:handle_error({riak_error, Reason}, RD, Ctx)
    end.

-spec extract_params([{string(), string()}], #ctx{}) -> #ctx{} .
%% @doc right now we only allow a timeout parameter or nothing.
extract_params([], Ctx) ->
    Ctx;
extract_params([{"timeout", TimeoutStr} | Rest],
               Ctx0 = #ctx{options = Options0}) ->
    case catch list_to_integer(TimeoutStr) of
        Timeout when is_integer(Timeout), Timeout >= 0 ->
            Options = lists:keystore(timeout, 1, Options0, {timeout, Timeout}),
            extract_params(Rest,
                           Ctx0#ctx{timeout = Timeout,
                                    options = Options});
        _ ->
            Reason = io_lib:format("Bad timeout value: ~s", [TimeoutStr]),
            throw({parameter_error, Reason})
    end;
extract_params([{UnknownParam, _}|_], _Ctx) ->
    throw({parameter_error, io_lib:format("Unknown parameter: ~s", [UnknownParam])}).

validate_key(Path, #ctx{table = Table, mod = Mod}) ->
    UnquotedPath = lists:map(fun mochiweb_util:unquote/1, Path),
    path_elements_to_key(UnquotedPath, Table, Mod, Mod:get_ddl()).

path_elements_to_key(PEList, Table, Mod,
                     ?DDL{local_key = #key_v1{ast = LK}}) ->
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
                case ensure_lk_order_and_strip(LK, FVList) of
                    %% this will extract only LK-constituent fields, and catch
                    %% the case of the right number of fields, of which some
                    %% are not in LK
                    OrderedKeyValues when length(OrderedKeyValues) == TableKeyLength ->
                        {ok, OrderedKeyValues};
                    _WrongNumberOfLKFields ->
                        {error, url_has_not_all_lk_fields}
                end;
           el/=se ->
                {error, {url_malformed_key_path, length(PEList), TableKeyLength, Table}}
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
                    throw({bad_value, Table, Field})
            end;
        false ->
            throw({bad_field, Table, Field})
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
    %% exclude fields not in LK
    [V || V <- [proplists:get_value(F, FVList) || ?SQL_PARAM{name = F} <- LK],
          V /= undefined].


extract_data(RD, Mod, Table) ->
    try
        Json = binary_to_list(wrq:req_body(RD)),
        Batch = ensure_batch(
                  mochijson2:decode(Json)),
        FieldTypes = ddl_fields_and_types(Mod),
        [json_struct_to_obj(Rec, FieldTypes) || {struct, Rec} <- Batch]
    catch
        error:_JsonParserError ->
            throw(invalid_json);
        throw:{Kind, WhichFieldOrType} ->
            %% our own custom errors caught in process_post:
            %% inject Table and rethrow
            throw({Kind, Table, WhichFieldOrType})
    end.

ensure_batch({struct, SingleRecord}) ->
    [{struct, SingleRecord}];
ensure_batch(Batch) when is_list(Batch) ->
    Batch.

json_struct_to_obj(FieldValueList, Fields) ->
    List = [extract_field_value(Field, FieldValueList)
            || Field <- Fields],
    list_to_tuple(List).

extract_field_value({Name, Type}, FVList) ->
    case proplists:get_value(Name, FVList) of
        undefined ->
            throw({missing_field, Name});
        Value ->
            check_field_value(Name, Type, Value)
    end.

%% @todo: might be better if the DDL helper module had a
%% valid_field_value(Field, Value) -> boolean() function.
check_field_value(_Name, varchar, V) when is_binary(V)         -> V;
check_field_value(_Name, sint64, V) when is_integer(V)         -> V;
check_field_value(_Name, double, V) when is_number(V)          -> V;
check_field_value(_Name, timestamp, V) when is_integer(V), V>0 -> V;
check_field_value(_Name, boolean, V) when is_boolean(V)        -> V;
check_field_value(Name, Type, _V) ->
    throw({bad_value, {Name, Type}}).



%% @todo: this should be in the DDL helper module, so that the records don't
%% leak out of riak_ql.
ddl_fields_and_types(Mod) ->
    ?DDL{fields = Fields} = Mod:get_ddl(),
    [ {Name, Type} || #riak_field_v1{name=Name, type=Type} <- Fields ].

%% @private
api_call([]       , 'POST')   -> put;
api_call(_KeyInURL, 'GET')    -> get;
api_call(_KeyInURL, 'DELETE') -> delete.

%% @private
result_to_json(ok) ->
    mochijson2:encode([{success, true}]).
