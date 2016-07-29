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

%% @doc Webmachine resource for listing bucket keys over HTTP.
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
%% GET /types/Type/buckets/Bucket/keys?keys=true|stream
%% GET /buckets/Bucket/keys?keys=true|stream
%% GET /riak/Bucket?keys=true|stream'''
%%
%%   Get the keys for a bucket. This is an expensive operation.
%%
%%   Keys are returned in JSON form: `{"keys":[Key1,Key2,...]}'
%%
%%   If the `keys' param is set to `true', then keys are sent back in
%%   a single JSON structure. If set to "stream" then keys are
%%   streamed in multiple JSON snippets. Otherwise, no keys are sent.
%%
%%   If the `allow_props_param' context setting is `true', then
%%   the user can also specify a `props=true' to include props in the
%%   JSON response. This provides backward compatibility with the
%%   old HTTP API.
%%

-module(riak_kv_wm_keylist).

%% webmachine resource exports
-export([
         init/1,
         service_available/2,
         is_authorized/2,
         forbidden/2,
         content_types_provided/2,
         encodings_provided/2,
         resource_exists/2,
         produce_bucket_body/2,
         malformed_request/2
        ]).

-record(ctx, {api_version,  %% integer() - Determine which version of the API to use.
              bucket_type,  %% binary() - Bucket type (from uri)
              bucket,       %% binary() - Bucket name (from uri)
              client,       %% riak_client() - the store client
              prefix,       %% string() - prefix for resource uris
              riak,         %% local | {node(), atom()} - params for riak client
              allow_props_param, %% true if the user can also list props. (legacy API)
              timeout,      %% integer() - list keys timeout
              security,     %% security context
              is_enabled :: boolean() %% Does the config for this node allow list_keys?
             }).
-type context() :: #ctx{}.

-include_lib("webmachine/include/webmachine.hrl").
-include("riak_kv_wm_raw.hrl").

-spec init(proplists:proplist()) -> {ok, context()}.
%% @doc Initialize this resource.  This function extracts the
%%      'prefix' and 'riak' properties from the dispatch args.
init(Props) ->
    {ok, #ctx{api_version=proplists:get_value(api_version, Props),
              prefix=proplists:get_value(prefix, Props),
              riak=proplists:get_value(riak, Props),
              allow_props_param=proplists:get_value(allow_props_param, Props),
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
    lager:info("list_keys job submitted via HTTP from ~p", [wrq:peer(RD)]),
    Ctx = riak_kv_wm_utils:ensure_bucket_type(RD, Ctx0, #ctx.bucket_type),
    case riak_kv_wm_utils:get_riak_client(RiakProps, riak_kv_wm_utils:get_client_id(RD)) of
        {ok, C} ->
            {true,
             RD,
             Ctx#ctx{
               client=C,
               bucket=case wrq:path_info(bucket, RD) of
                         undefined -> undefined;
                         B -> list_to_binary(riak_kv_wm_utils:maybe_decode_uri(RD, B))
                      end
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
    {riak_kv_wm_utils:is_forbidden(RD, list_keys), RD, Ctx};
forbidden(RD, Ctx) ->
    case riak_kv_wm_utils:is_forbidden(RD, list_keys) of
        true ->
            {true, RD, Ctx};
        false ->
            Res = riak_core_security:check_permission({"riak_kv.list_keys",
                                                       {Ctx#ctx.bucket_type,
                                                        Ctx#ctx.bucket}},
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
%%      "application/json" is the content-type for listing keys.
content_types_provided(RD, Ctx) ->
    %% bucket-level: JSON description only
    {[{"application/json", produce_bucket_body}], RD, Ctx}.

-spec encodings_provided(#wm_reqdata{}, context()) ->
    {[{Encoding::string(), Producer::function()}], #wm_reqdata{}, context()}.
%% @doc List the encodings available for representing this resource.
%%      "identity" and "gzip" are available for listing keys.
encodings_provided(RD, Ctx) ->
    %% identity and gzip for top-level and bucket-level requests
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

resource_exists(RD, Ctx) ->
    {riak_kv_wm_utils:bucket_type_exists(Ctx#ctx.bucket_type), RD, Ctx}.

-spec produce_bucket_body(#wm_reqdata{}, context()) ->
    {binary(), #wm_reqdata{}, context()}.
%% @doc Produce the JSON response to a bucket-level GET.
%%      Includes the keys of the documents in the bucket unless the
%%      "keys=false" query param is specified. If "keys=stream" query param
%%      is specified, keys will be streamed back to the client in JSON chunks
%%      like so: {"keys":[Key1, Key2,...]}.
produce_bucket_body(RD, #ctx{client=Client,
                             bucket=Bucket0,
                             bucket_type=Type,
                             timeout=Timeout,
                             allow_props_param=AllowProps}=Ctx) ->
    Bucket = riak_kv_wm_utils:maybe_bucket_type(Type, Bucket0),
    IncludeBucketProps = (AllowProps == true)
        andalso (wrq:get_qs_value(?Q_PROPS, RD) /= ?Q_FALSE),

    BucketPropsJson =
        case IncludeBucketProps of
            true ->
                [riak_kv_wm_props:get_bucket_props_json(Client, Bucket)];
            false ->
                []
        end,

    case wrq:get_qs_value(?Q_KEYS, RD) of
        ?Q_STREAM ->
            %% Start streaming the keys...
            F = fun() ->
                        {ok, ReqId} = Client:stream_list_keys(Bucket,
                                                              Timeout),
                        stream_keys(ReqId)
                end,

            %% For API Version 1, send back the BucketPropsJson first
            %% (if defined) or an empty resultset. For API Version 2,
            %% use an empty list, which doesn't send an resultset.
            FirstResult =
                case Ctx#ctx.api_version of
                    1 ->
                        mochijson2:encode({struct, BucketPropsJson});
                    Two when Two >= 2 ->
                        []
                end,
            {{stream, {FirstResult, F}}, RD, Ctx};

        ?Q_TRUE ->
            %% Get the JSON response...
            case Client:list_keys(Bucket, Timeout) of
                {ok, KeyList} ->
                    JsonKeys = mochijson2:encode({struct, BucketPropsJson ++
                                                      [{?Q_KEYS, KeyList}]}),
                    {JsonKeys, RD, Ctx};
                {error, Reason} ->
                    {mochijson2:encode({struct, [{error, Reason}]}), RD, Ctx}
            end;
        _ ->
            JsonProps = mochijson2:encode({struct, BucketPropsJson}),
            {JsonProps, RD, Ctx}
    end.

stream_keys(ReqId) ->
    receive
        {ReqId, From, {keys, Keys}} ->
            _ = riak_kv_keys_fsm:ack_keys(From),
            {mochijson2:encode({struct, [{<<"keys">>, Keys}]}), fun() -> stream_keys(ReqId) end};
        {ReqId, {keys, Keys}} ->
            {mochijson2:encode({struct, [{<<"keys">>, Keys}]}), fun() -> stream_keys(ReqId) end};
        {ReqId, done} -> {mochijson2:encode({struct, [{<<"keys">>, []}]}), done};
        {ReqId, {error, timeout}} -> {mochijson2:encode({struct, [{error, timeout}]}), done}
    end.
