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
         resource_exists/2,
         content_types_provided/2,
         encodings_provided/2,
         produce_doc_body/2
        ]).

-include("riak_kv_wm_raw.hrl").
-include_lib("webmachine/include/webmachine.hrl").

-record(ctx, {api_version,
              riak,
              security,
              client,
              table    :: undefined | binary()
             }).

-define(CB_RV_SPEC, {boolean(), #wm_reqdata{}, #ctx{}}).

-define(DEFAULT_TIMEOUT, 60000).

-spec init(proplists:proplist()) -> {ok, #ctx{}}.
%% @doc Initialize this resource.  This function extracts the
%%      'prefix' and 'riak' properties from the dispatch args.
init(Props) ->
    {ok, #ctx{api_version = proplists:get_value(api_version, Props),
              riak = proplists:get_value(riak, Props),
              table = proplists:get_value(table, Props)}}.

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
            {false, RD, Ctx}
    end.

-spec allowed_methods(#wm_reqdata{}, #ctx{}) ->
    {[atom()], #wm_reqdata{}, #ctx{}}.
%% @doc Get the list of methods this resource supports.
allowed_methods(RD, Ctx) ->
    {['GET'], RD, Ctx}.


-spec resource_exists(#wm_reqdata{}, #ctx{}) ->
                             {boolean(), #wm_reqdata{}, #ctx{}}.
resource_exists(RD, #ctx{table = Table} = Ctx) ->
    {riak_kv_wm_utils:bucket_type_exists(Table), RD, Ctx}.


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

flat_format(Format, Args) ->
    lists:flatten(io_lib:format(Format, Args)).
