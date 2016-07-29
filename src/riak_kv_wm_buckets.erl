%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2016 Basho Technologies, Inc.
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

%% @doc Webmachine resource for listing Riak buckets over HTTP.
%%
%% URLs that begin with `/types' are necessary for the new bucket
%% types implementation in Riak 2.0, those that begin with `/buckets'
%% are for the default bucket type, and `/riak' is an old URL style,
%% also only works for the default bucket type.
%%
%% It is possible to reconfigure the `/riak' prefix but that seems to
%% be rarely if ever used.
%%
%% ```
%% GET /types/Type/buckets?buckets=true
%% GET /types/Type/buckets?buckets=stream
%% GET /buckets?buckets=true
%% GET /buckets?buckets=stream
%% GET /riak?buckets=true
%% '''
%%   Get information about available buckets. Note that generating the
%%   bucket list is expensive, so we require the `buckets=true' or
%%   `buckets=stream' argument.
%%

-module(riak_kv_wm_buckets).

%% webmachine resource exports
-export([
         init/1,
         service_available/2,
         is_authorized/2,
         forbidden/2,
         resource_exists/2,
         content_types_provided/2,
         encodings_provided/2,
         produce_bucket_list/2,
         malformed_request/2
        ]).

-record(ctx, {
          bucket_type,  %% binary() - bucket type (from uri)
          api_version,  %% integer() - Determine which version of the API to use.
          client,       %% riak_client() - the store client
          prefix,       %% string() - prefix for resource uris
          riak,         %% local | {node(), atom()} - params for riak client
          method,       %% atom() - HTTP method for the request
          timeout,      %% integer() - list buckets timeout
          security     %% security context
         }).
-type context() :: #ctx{}.

-include_lib("webmachine/include/webmachine.hrl").
-include("riak_kv_wm_raw.hrl").

-define(DEFAULT_TIMEOUT, 5 * 60000).

-spec init(proplists:proplist()) -> {ok, context()}.
%% @doc Initialize this resource.  This function extracts the
%%      'prefix' and 'riak' properties from the dispatch args.
init(Props) ->
    {ok, #ctx{
       api_version=proplists:get_value(api_version, Props),
       prefix=proplists:get_value(prefix, Props),
       riak=proplists:get_value(riak, Props),
       bucket_type=proplists:get_value(bucket_type, Props)
      }}.


-spec service_available(#wm_reqdata{}, context()) ->
          {boolean(), #wm_reqdata{}, context()}.
%% @doc Determine whether or not a connection to Riak
%%      can be established.  This function also takes this
%%      opportunity to extract the 'bucket' and 'key' path
%%      bindings from the dispatch, as well as any vtag
%%      query parameter.
service_available(RD, Ctx0=#ctx{riak=RiakProps}) ->
    lager:info("list_buckets job submitted via HTTP from ~p", [wrq:peer(RD)]),
    Ctx = riak_kv_wm_utils:ensure_bucket_type(RD, Ctx0, #ctx.bucket_type),
    case riak_kv_wm_utils:get_riak_client(RiakProps, riak_kv_wm_utils:get_client_id(RD)) of
        {ok, C} ->
            {true,
             RD,
             Ctx#ctx{
               method=wrq:method(RD),
               client=C
              }};
        Error ->
            {false,
             wrq:set_resp_body(
               io_lib:format("Unable to connect to Riak: ~p~n", [Error]),
               wrq:set_resp_header(?HEAD_CTYPE, "text/plain", RD)),
             Ctx}
    end.

is_authorized(ReqData, Ctx) ->
    case riak_api_web_security:is_authorized(ReqData) of
        false ->
            {"Basic realm=\"Riak\"", ReqData, Ctx};
        {true, SecContext} ->
            {true, ReqData, Ctx#ctx{security=SecContext}};
        insecure ->
            %% XXX 301 may be more appropriate here, but since the http and
            %% https port are different and configurable, it is hard to figure
            %% out the redirect URL to serve.
            {{halt, 426}, wrq:append_to_resp_body(<<"Security is enabled and "
                    "Riak does not accept credentials over HTTP. Try HTTPS "
                    "instead.">>, ReqData), Ctx}
    end.

forbidden(RD, Ctx = #ctx{security=undefined}) ->
    {riak_kv_wm_utils:is_forbidden(RD, list_buckets), RD, Ctx};
forbidden(RD, Ctx) ->
    case riak_kv_wm_utils:is_forbidden(RD, list_buckets) of
        true ->
            {true, RD, Ctx};
        false ->
            Res = riak_core_security:check_permission({"riak_kv.list_buckets",
                                                       Ctx#ctx.bucket_type},
                                                      Ctx#ctx.security),
            case Res of
                {false, Error, _} ->
                    RD1 = wrq:set_resp_header("Content-Type", "text/plain", RD),
                    {true, wrq:append_to_resp_body(unicode:characters_to_binary(Error, utf8, utf8), RD1), Ctx};
                {true, _} ->
                    {false, RD, Ctx}
            end
    end.

-spec content_types_provided(#wm_reqdata{}, context()) ->
    {[{ContentType::string(), Producer::atom()}], #wm_reqdata{}, context()}.
%% @doc List the content types available for representing this resource.
%%      "application/json" is the content-type for bucket lists.
content_types_provided(RD, Ctx) ->
    {[{"application/json", produce_bucket_list}], RD, Ctx}.


-spec encodings_provided(#wm_reqdata{}, context()) ->
    {[{Encoding::string(), Producer::function()}], #wm_reqdata{}, context()}.
%% @doc List the encodings available for representing this resource.
%%      "identity" and "gzip" are available for bucket lists.
encodings_provided(RD, Ctx) ->
    {riak_kv_wm_utils:default_encodings(), RD, Ctx}.

malformed_request(RD, Ctx) ->
    malformed_timeout_param(RD, Ctx).

-spec malformed_timeout_param(#wm_reqdata{}, context()) ->
    {boolean(), #wm_reqdata{}, context()}.
%% @doc Check that the timeout parameter is are a
%%      string-encoded integer.  Store the integer value
%%      in context() if so.
malformed_timeout_param(RD, Ctx) ->
    case wrq:get_qs_value("timeout", none, RD) of
        none ->
            {false, RD, Ctx};
        TimeoutStr ->
            try
                Timeout = list_to_integer(TimeoutStr),
                {false, RD, Ctx#ctx{timeout=Timeout}}
            catch
                _:_ ->
                    {true,
                     wrq:append_to_resp_body(io_lib:format("Bad timeout "
                                                           "value ~p~n",
                                                           [TimeoutStr]),
                                             wrq:set_resp_header(?HEAD_CTYPE,
                                                                 "text/plain", RD)),
                     Ctx}
            end
    end.

resource_exists(RD, #ctx{bucket_type=BType}=Ctx) ->
    {riak_kv_wm_utils:bucket_type_exists(BType), RD, Ctx}.

-spec produce_bucket_list(#wm_reqdata{}, context()) ->
    {binary(), #wm_reqdata{}, context()}.
%% @doc Produce the JSON response to a bucket-level GET.
%%      Includes a list of known buckets if the "buckets=true" query
%%      param is specified.
produce_bucket_list(RD, #ctx{client=Client,
                             timeout=Timeout0,
                             bucket_type=BType}=Ctx) ->
    Timeout =
        case Timeout0 of
            undefined -> ?DEFAULT_TIMEOUT;
            Set -> Set
        end,
    case wrq:get_qs_value(?Q_BUCKETS, RD) of
        ?Q_TRUE ->
            %% Get the buckets.
            {ok, Buckets} = Client:list_buckets(none, Timeout, BType),
            {mochijson2:encode({struct, [{?JSON_BUCKETS, Buckets}]}),
             RD, Ctx};
        ?Q_STREAM ->
            F = fun() ->
                        {ok, ReqId} = Client:stream_list_buckets(none, Timeout, BType),
                        stream_buckets(ReqId)
                end,
            {{stream, {[], F}}, RD, Ctx};
        _ ->
            {mochijson2:encode({struct, [{?JSON_BUCKETS, []}]})}
        end.

stream_buckets(ReqId) ->
    receive
        {ReqId, done} ->
                {mochijson2:encode({struct,
                                    [{<<"buckets">>, []}]}), done};
        {ReqId, _From, {buckets_stream, Buckets}} ->
            {mochijson2:encode({struct, [{<<"buckets">>, Buckets}]}),
             fun() -> stream_buckets(ReqId) end};
        {ReqId, {buckets_stream, Buckets}} ->
            {mochijson2:encode({struct, [{<<"buckets">>, Buckets}]}),
             fun() -> stream_buckets(ReqId) end};
        {ReqId, {error, timeout}} -> {mochijson2:encode({struct, [{error, timeout}]}), done}
    end.
