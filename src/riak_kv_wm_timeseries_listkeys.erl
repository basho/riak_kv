%% -------------------------------------------------------------------
%%
%% riak_kv_wm_timeseries_listkeys: Webmachine resource for riak TS
%%                                  streaming operations.
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
%% GET    /ts/v1/table/Table/keys     list_keys
%% '''
%%
%% Request body is expected to be a JSON containing key and/or value(s).
%% Response is a JSON containing data rows with column headers.
%%

-module(riak_kv_wm_timeseries_listkeys).

%% webmachine resource exports
-export([
         init/1,
         service_available/2,
         allowed_methods/2,
         is_authorized/2,
         forbidden/2,
         malformed_request/2,
         resource_exists/2,
         content_types_provided/2,
         encodings_provided/2,
         produce_doc_body/2
        ]).

-include("riak_kv_wm_raw.hrl").
-include_lib("webmachine/include/webmachine.hrl").

-record(ctx, {riak,
              security,
              client,
              table    :: undefined | binary(),
              mod :: module()
             }).

-define(CB_RV_SPEC, {boolean()|atom()|tuple(), #wm_reqdata{}, #ctx{}}).

-define(DEFAULT_TIMEOUT, 60000).

-spec init(proplists:proplist()) -> {ok, #ctx{}}.
%% @doc Initialize this resource.  This function extracts the
%%      'prefix' and 'riak' properties from the dispatch args.
init(Props) ->
    {ok, #ctx{riak = proplists:get_value(riak, Props)}}.

-spec service_available(#wm_reqdata{}, #ctx{}) ->
    {boolean(), #wm_reqdata{}, #ctx{}}.
%% @doc Determine whether or not a connection to Riak
%%      can be established.
service_available(RD, Ctx = #ctx{riak = RiakProps}) ->
    case riak_kv_wm_utils:get_riak_client(
           RiakProps, riak_kv_wm_utils:get_client_id(RD)) of
        {ok, C} ->
            Table = riak_kv_wm_ts_util:table_from_request(RD),
            Mod = riak_ql_ddl:make_module_name(Table),
            {true, RD,
             Ctx#ctx{client = C,
                     table = Table,
                     mod = Mod}};
        {error, Reason} ->
            handle_error({riak_client_error, Reason}, RD, Ctx)
    end.


is_authorized(RD, #ctx{table=Table}=Ctx) ->
    case riak_kv_wm_ts_util:authorize(listkeys, Table, RD) of
        ok ->
            {true, RD, Ctx};
        {error, ErrorMsg} ->
            {ErrorMsg, RD, Ctx};
        {insecure, Halt, Resp} ->
            {Halt, Resp, Ctx}
    end.



-spec forbidden(#wm_reqdata{}, #ctx{}) -> ?CB_RV_SPEC.
forbidden(RD, Ctx) ->
    case riak_kv_wm_utils:is_forbidden(RD) of
        true ->
            {true, RD, Ctx};
        false ->
            case check_permissions(RD, Ctx) of
                {true, RD1, Ctx1} ->
                    {false, RD1, Ctx1};
                ErrorAlreadyReported ->
                    ErrorAlreadyReported
            end
    end.

-spec check_permissions(#wm_reqdata{}, #ctx{}) -> ?CB_RV_SPEC.
check_permissions(RD, Ctx = #ctx{security = undefined}) ->
    {true, RD, Ctx};
check_permissions(RD, Ctx = #ctx{security = Security,
                                 table = Table}) ->
    case riak_core_security:check_permission(
           {riak_kv_ts_util:api_call_to_perm(listkeys), Table}, Security) of
        {false, Error, _} ->
            handle_error(
              {not_permitted, utf8_to_binary(Error)}, RD, Ctx);
        _ ->
            {true, RD, Ctx}
    end.


-spec malformed_request(#wm_reqdata{}, #ctx{}) -> ?CB_RV_SPEC.
malformed_request(RD, Ctx) ->
    {false, RD, Ctx}.
%% malformed_request(RD, Ctx = #ctx{api_version = "v1"}) ->
%%     {false, RD, Ctx};
%% malformed_request(RD, Ctx = #ctx{api_version = UnsupportedVersion}) ->
%%     handle_error({unsupported_version, UnsupportedVersion}, RD, Ctx).


-spec allowed_methods(#wm_reqdata{}, #ctx{}) ->
    {[atom()], #wm_reqdata{}, #ctx{}}.
%% @doc Get the list of methods this resource supports.
allowed_methods(RD, Ctx) ->
    {['GET'], RD, Ctx}.


-spec resource_exists(#wm_reqdata{}, #ctx{}) ->
                             {boolean(), #wm_reqdata{}, #ctx{}}.
resource_exists(RD, #ctx{table = Table} = Ctx) ->
    Mod = riak_ql_ddl:make_module_name(Table),
    case catch Mod:get_ddl() of
        {_, {undef, _}} ->
            handle_error({no_such_table, Table}, RD, Ctx);
        _ ->
            {true, RD, Ctx}
    end.


-spec encodings_provided(#wm_reqdata{}, #ctx{}) ->
                                {[{Encoding::string(), Producer::function()}],
                                 #wm_reqdata{}, #ctx{}}.
%% @doc List the encodings available for representing this resource.
%%      "identity" and "gzip" are available.
encodings_provided(RD, Ctx) ->
    {riak_kv_wm_utils:default_encodings(), RD, Ctx}.

-spec content_types_provided(#wm_reqdata{}, #ctx{}) ->
                                    {[{ContentType::string(), Producer::atom()}],
                                     #wm_reqdata{}, #ctx{}}.
%% @doc List the content types available for representing this resource.
content_types_provided(RD, Ctx) ->
    {[{"application/json", produce_doc_body}], RD, Ctx}.


produce_doc_body(RD, Ctx = #ctx{table = Table,
                                client = Client}) ->
    F = fun() ->
                {ok, ReqId} = riak_client:stream_list_keys(
                                {Table, Table}, undefined, Client),
                stream_keys(ReqId)
        end,
    {{stream, {<<>>, F}}, RD, Ctx}.

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
            {<<>>, done};
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
        {no_such_table, Table} ->
            error_out({halt, 404},
                      "Table \"~ts\" does not exist", [Table], RD, Ctx)
    end.

flat_format(Format, Args) ->
    lists:flatten(io_lib:format(Format, Args)).

utf8_to_binary(S) ->
    unicode:characters_to_binary(S, utf8, utf8).
