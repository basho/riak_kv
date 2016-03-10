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
%% ts/v1/table/Table/keys
%% ```
%% Specific operations supported:
%% ```
%% GET     /ts/v1/table/Table/keys/K1/V1/...  single-key get
%% DELETE  /ts/v1/table/Table/keys/K1/V1/...  single-key delete
%% POST    /ts/v1/table/Table/keys            singe-key or batch put depending
%%                                            on the body
%% '''
%%
%% Request body is expected to be a JSON containing a struct or structs for the
%% POST. GET and DELETE have no body.
%%
%% Response is a JSON containing full records.
%%

-module(riak_kv_wm_timeseries).

%% webmachine resource exports
-export([init/1,
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
         resource_exists/2]).

%% webmachine body-producing functions
-export([to_json/2]).

-include_lib("webmachine/include/webmachine.hrl").
-include_lib("riak_ql/include/riak_ql_ddl.hrl").
-include("riak_kv_wm_raw.hrl").
-include("riak_kv_ts.hrl").

-record(ctx,
        {api_call    :: 'undefined' | 'get' | 'put' | 'delete',
         table       :: 'undefined' | binary(),
         mod         :: 'undefined' | module(),
         key         :: 'undefined' | ts_rec(),
         object,
         timeout :: 'undefined' | integer(),
         options,  %% for the call towards riak.
         prefix,
         riak}).

-define(DEFAULT_TIMEOUT, 60000).

-type cb_rv_spec(T) :: {T, #wm_reqdata{}, #ctx{}}.
-type halt() :: {'halt', 200..599} | {'error' , term()}.
-type ts_rec() :: [riak_pb_ts_codec:ldbvalue()].

-spec init(proplists:proplist()) -> {ok, #ctx{}}.
%% @doc Initialize this resource.  This function extracts the
%%      'prefix' and 'riak' properties from the dispatch args.
init(Props) ->
    {ok, #ctx{prefix = proplists:get_value(prefix, Props),
              riak = proplists:get_value(riak, Props)}}.
    %% {{trace, "/tmp"}, #ctx{prefix = proplists:get_value(prefix, Props),
    %%                        riak = proplists:get_value(riak, Props)}}.
%% wmtrace_resource:add_dispatch_rule("wmtrace", "/tmp").

-spec service_available(#wm_reqdata{}, #ctx{}) -> cb_rv_spec(boolean() | halt()).
%% @doc Determine whether or not a connection to Riak
%%      can be established.
%%      Convert the table name from the part of the URL.
service_available(RD, #ctx{riak = RiakProps}=Ctx) ->
    case riak_kv_wm_utils:get_riak_client(
           RiakProps, riak_kv_wm_utils:get_client_id(RD)) of
        {ok, _C} ->
            Table = table(RD),
            Mod = riak_ql_ddl:make_module_name(Table),
            {true, RD, Ctx#ctx{table=Table, mod=Mod}};
        {error, Reason} ->
            ErrorMsg = flat_format("Unable to connect to Riak: ~p", [Reason]),
            Resp = set_text_resp_header(ErrorMsg, RD),
            {false, Resp, Ctx}
    end.

is_authorized(RD, #ctx{table=Table}=Ctx) ->
    Call = api_call(wrq:path_tokens(RD), wrq:method(RD)),
    case riak_api_web_security:is_authorized(RD) of
        false ->
            {"Basic realm=\"Riak\"", RD, Ctx};
        {true, undefined} -> %% @todo: why is this returned during testing?
            {true, RD, Ctx#ctx{api_call=Call}};
        {true, SecContext} ->
            case riak_core_security:check_permission(
                   {riak_kv_ts_util:api_call_to_perm(Call), Table}, SecContext) of
                 {false, Error, _} ->
                    {utf8_to_binary(Error), RD, Ctx};
                _ ->
                    {true, RD, Ctx#ctx{api_call=Call}}
            end;
        insecure ->
            ErrorMsg = "Security is enabled and Riak does not" ++
                " accept credentials over HTTP. Try HTTPS instead.",
            Resp = set_text_resp_header(ErrorMsg, RD),
            {{halt, 426}, Resp, Ctx}
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
        malformed_request(wrq:path_tokens(RD), RD, Ctx2)
    catch
        throw:ParameterError ->
            Resp = set_error_message("parameter error: ~p", [ParameterError], RD),
            {true, Resp, Ctx}
    end.

malformed_request([], RD, Ctx) ->
    %% NOTE: if the supplied JSON body is wrong a malformed requset may be
    %% issued later.
    %% @todo: should the validation of the JSON happen here???
    {false, RD, Ctx};
malformed_request(KeyInUrl, RD, Ctx) when length(KeyInUrl) rem 2 == 0 ->
    {false, RD, Ctx};
malformed_request(_, RD, Ctx) ->
    {true, RD, Ctx}.

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
resource_exists(RD, #ctx{mod=Mod} = Ctx) ->
    case table_module_exists(Mod) of
        true ->
            resource_exists(wrq:path_tokens(RD), wrq:method(RD), RD, Ctx);
        false ->
            Resp = set_error_message("table ~p not created", [Mod], RD),
            {false, Resp, Ctx}
    end.

resource_exists([], 'POST', RD, Ctx) ->
    {true, RD, Ctx};
resource_exists(Path, 'GET', RD,
                #ctx{table=Table,
                     mod=Mod,
                     options=Options}=Ctx) ->
    %% Would be nice if something cheaper than using get_data existed to check
    %% if a key is present.
    try
        Key = validate_key(Path, Mod),
        case riak_kv_ts_api:get_data(Key, Table, Mod, Options) of
            {ok, Record} ->
                {true, RD, Ctx#ctx{object=Record,
                                   key=Key}};
            {error, notfound} ->
                {{halt, 404}, RD, Ctx};
            {error, InternalReason} ->
                InternalResp = set_error_message("Internal error: ~p", [InternalReason], RD),
                {{halt, 500}, InternalResp, Ctx}
        end
    catch
        _:Reason ->
            Resp = set_error_message("lookup on ~p failed due to ~p",
                                     [Path, Reason],
                                     RD),
            {false, Resp, Ctx}
    end;
resource_exists(Path, 'DELETE', RD, #ctx{mod=Mod}=Ctx) ->
    %% Since reading the object is expensive we will assume for now that the
    %% object exists for a delete, but if it turns out that it does not then the
    %% processing of the delete will return 404 at that point.
    try
        Key = validate_key(Path, Mod),
        {true, RD, Ctx#ctx{key=Key}}
    catch
        _:Reason ->
            Resp = set_error_message("lookup on ~p failed due to ~p",
                                     [Path, Reason],
                                     RD),
            {false, Resp, Ctx}
    end.

-spec encodings_provided(#wm_reqdata{}, #ctx{}) ->
                                cb_rv_spec([{Encoding::string(), Producer::function()}]).
encodings_provided(RD, Ctx) ->
    {riak_kv_wm_utils:default_encodings(), RD, Ctx}.

-spec post_is_create(#wm_reqdata{}, #ctx{}) -> cb_rv_spec(boolean()).
post_is_create(RD, Ctx) ->
    {false, RD, Ctx}.

-spec process_post(#wm_reqdata{}, #ctx{}) ->  cb_rv_spec(boolean()).
process_post(RD, #ctx{mod=Mod,
                     table=Table}=Ctx) ->
    try extract_data(RD, Mod) of
        Records ->
            case riak_kv_ts_util:validate_rows(Mod, Records) of
                [] ->
                    case riak_kv_ts_api:put_data(Records, Table, Mod) of
                        ok ->
                            Json = result_to_json(ok),
                            Resp = set_json_response(Json, RD),
                            {true, Resp, Ctx};
                        {error, {some_failed, ErrorCount}} ->
                            Resp = set_error_message("failed some puts ~p ~p",
                                                     [ErrorCount, Table],
                                                     RD),
                            {{halt, 400}, Resp, Ctx}
                    end;
                BadRowIdxs when is_list(BadRowIdxs) ->
                    Resp = set_error_message("invalid data: ~p",
                                             [BadRowIdxs],
                                             RD),
                    {{halt, 400}, Resp, Ctx}
            end
    catch
        throw:{data_problem,Reason} ->
            Resp = set_error_message("wrong body: ~p", [Reason], RD),
            {{halt, 400}, Resp, Ctx}
    end.

-spec delete_resource(#wm_reqdata{}, #ctx{}) ->  cb_rv_spec(boolean()|halt()).
delete_resource(RD,  #ctx{table=Table,
                          mod=Mod,
                          key=Key,
                          options=Options}=Ctx) ->
     try riak_kv_ts_api:delete_data(Key, Table, Mod, Options) of
        ok ->
             Json = result_to_json(ok),
             Resp = set_json_response(Json, RD),
             {true, Resp, Ctx};
        {error, notfound} ->
             {{halt, 404}, RD, Ctx}
    catch
        _:Reason ->
            lager:log(info, self(), "delete_resource failed: ~p", Reason),
            Resp = set_error_message("Internal error: ~p", [Reason], RD),
            {{halt, 500}, Resp, Ctx}
    end.

-spec to_json(#wm_reqdata{}, #ctx{}) ->  cb_rv_spec(iolist()|halt()).
to_json(RD, #ctx{api_call=get, object=Object}=Ctx) ->
    try
        Json = mochijson2:encode(Object),
        {Json, RD, Ctx}
    catch
        _:Reason ->
            Resp = set_error_message("object error ~p", [Reason], RD),
            {{halt, 500}, Resp, Ctx}
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% helper functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% @todo: this should be in riak_ql_ddl and should probably check deeper.
-spec table_module_exists(module()) -> boolean().
table_module_exists(Mod) ->
    try Mod:get_ddl() of
        #ddl_v1{} ->
            true
    catch
        _:_ ->
            false
    end.

-spec extract_params([{string(), string()}], #ctx{}) -> #ctx{} .
%% @doc right now we only allow a timeout parameter or nothing.
extract_params([], Ctx) ->
    Ctx#ctx{options=[]};
extract_params([{"timeout", TimeoutStr}], Ctx) ->
    try
        Timeout = list_to_integer(TimeoutStr),
        Ctx#ctx{timeout = Timeout,
                options = [{timeout, Timeout}]}
    catch
        _:_ ->
            throw(flat_format("timeout not an integer value: ~s", [TimeoutStr]))
    end;
extract_params(Params, _Ctx) ->
    throw(flat_format("incorrect paramters: ~p", [Params])).

validate_key(Path, Mod) ->
    UnquotedPath = lists:map(fun mochiweb_util:unquote/1, Path),
    path_elements(Mod, UnquotedPath).

%% extract keys from path elements in the URL (.../K1/V1/K2/V2/... ->
%% [V1, V2, ...]), check with Table's DDL to make sure keys are
%% correct and values are of (convertible to) appropriate types, and
%% return the KV list
%% @private
-spec path_elements(module(), [string()]) ->
                       [riak_pb_ts_codec:ldbvalue()].
path_elements(Mod, Path) ->
    KeyTypes = local_key_fields_and_types(Mod),
    match_path(Path, KeyTypes).

local_key_fields_and_types(Mod) ->
    LK = local_key(Mod),
    Types = [Mod:get_field_type([F]) || F <- LK ],
    LKStr = [ binary_to_list(F) || F <- LK ],
    lists:zip(LKStr, Types).

match_path([], []) ->
    [];
match_path([F,V|Path], [{F, Type}|KeyTypes]) ->
    [convert_field_value(Type, V)|match_path(Path, KeyTypes)];
match_path(Path, _KeyTypes) ->
    throw(io_lib:format("incorrect path ~p", [Path])).

%% @private
convert_field_value(varchar, V) ->
    list_to_binary(V);
convert_field_value(sint64, V) ->
    list_to_integer(V);
convert_field_value(double, V) ->
    try
        list_to_float(V)
    catch
        error:badarg ->
            float(list_to_integer(V))
    end;
convert_field_value(timestamp, V) ->
    case list_to_integer(V) of
        GoodValue when GoodValue > 0 ->
            GoodValue;
        _ ->
            throw(url_key_bad_value)
    end.

extract_data(RD, Mod) ->
    try
        JsonStr = binary_to_list(wrq:req_body(RD)),
        Json = mochijson2:decode(JsonStr),
        DDLFieldTypes = ddl_fields_and_types(Mod),
        extract_records(Json, DDLFieldTypes)
    catch
        Error:Reason ->
            lager:log(info, self(), "extract_data: ~p:~p", [Error, Reason]),
            throw({data_problem, Reason})
    end.

extract_records({struct, _}=Struct, Fields) ->
    [json_struct_to_obj(Struct, Fields)];
extract_records(Structs, Fields) when is_list(Structs) ->
    [json_struct_to_obj(S, Fields) || S <- Structs].

json_struct_to_obj({struct, FieldValueList}, Fields) ->
    List = [ extract_field_value(Field, FieldValueList)
             || Field <- Fields],
    list_to_tuple(List).

extract_field_value({Name, Type}, FVList) ->
    case proplists:get_value(Name, FVList) of
        undefined ->
            throw({data_problem, {missing_field, Name}});
        Value ->
           check_field_value(Type, Value)
    end.

local_key(Mod) ->
    ddl_local_key(Mod:get_ddl()).

%% this should be in the DDL helper module.
-spec ddl_local_key(#ddl_v1{}) -> [binary()].
ddl_local_key(#ddl_v1{local_key=LK}) ->
    #key_v1{ast=Ast} = LK,
    [ param_name(P) || P <- Ast].

param_name(#param_v1{name=[Name]}) ->
    Name.

%% @todo: might be better if the DDL helper module had a
%% valid_field_value(Field, Value) -> boolean() function.
check_field_value(varchar, V) when is_binary(V)         -> V;
check_field_value(sint64, V) when is_integer(V)         -> V;
check_field_value(double, V) when is_number(V)          -> V;
check_field_value(timestamp, V) when is_integer(V), V>0 -> V;
check_field_value(boolean, V) when is_boolean(V)        -> V;
check_field_value(Type, V) ->
    throw({data_problem, {wrong_type, Type, V}}).



%% @todo: this should be in the DDL helper module, so that the records don't
%% leak out of riak_ql.
ddl_fields_and_types(Mod) ->
    #ddl_v1{fields=Fields} = Mod:get_ddl(),
    [ {Name, Type} || #riak_field_v1{name=Name, type=Type} <- Fields ].


%% @private
table(RD) ->
    utf8_to_binary(
      mochiweb_util:unquote(
        wrq:path_info(table, RD))).

%% @private
api_call([]       , 'POST')   -> put;
api_call(_KeyInURL, 'GET')    -> get;
api_call(_KeyInURL, 'DELETE') -> delete.

%% @private
result_to_json(ok) ->
    mochijson2:encode([{success, true}]).

%% move to util module.
utf8_to_binary(S) ->
    unicode:characters_to_binary(S, utf8, utf8).

flat_format(Format, Args) ->
    lists:flatten(io_lib:format(Format, Args)).

set_text_resp_header(IoList, RD) ->
       wrq:set_resp_header(
       "Content-Type", "text/plain", wrq:append_to_response_body(IoList,RD)).

set_error_message(Format, Args, RD) ->
    set_text_resp_header(flat_format(Format, Args), RD).

set_json_response(Json, RD) ->
     wrq:set_resp_header("Content-Type", "application/json",
                         wrq:append_to_response_body(Json, RD)).
