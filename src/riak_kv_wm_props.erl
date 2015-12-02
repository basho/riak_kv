%% -------------------------------------------------------------------
%%
%% riak_kv_wm_props: Webmachine resource for listing bucket properties.
%%
%% Copyright (c) 2007-2015 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc Resource for serving Riak bucket properties over HTTP.
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
%% GET /types/Type/buckets/Bucket/props
%% GET /buckets/Bucket/props
%% GET /riak/Bucket'''
%%
%%   Get information about the named Bucket, in JSON form:
%%     `{"props":{Prop1:Val1,Prop2:Val2,...}}'
%%
%%   Each bucket property will be included in the `props' object.
%%   `linkfun' and `chash_keyfun' properties will be encoded as
%%   JSON objects of the form:
%% ```
%%     {"mod":ModuleName,
%%      "fun":FunctionName}'''
%%
%%   Where ModuleName and FunctionName are each strings representing
%%   a module and function.
%%
%% ```
%% PUT /types/Type/buckets/Bucket/props
%% PUT /buckets/Bucket/props
%% PUT /riak/Bucket'''
%%
%%   Modify bucket properties.
%%
%%   Content-type must be `application/json', and the body must have
%%   the form:
%%     `{"props":{Prop:Val}}'
%%
%%   Where the `props' object takes the same form as returned from
%%   a GET of the same resource.
%%
%% ```
%% DELETE /types/Type/buckets/Bucket/props
%% DELETE /buckets/Bucket/props'''
%%
%%   Reset bucket properties back to the default settings

-module(riak_kv_wm_props).

%% webmachine resource exports
-export([
         init/1,
         service_available/2,
         is_authorized/2,
         forbidden/2,
         allowed_methods/2,
         malformed_request/2,
         content_types_provided/2,
         encodings_provided/2,
         content_types_accepted/2,
         resource_exists/2,
         produce_bucket_body/2,
         accept_bucket_body/2,
         get_bucket_props_json/2,
         delete_resource/2
        ]).

-record(ctx, {bucket_type,  %% binary() - Bucket type (from uri)
              bucket,       %% binary() - Bucket name (from uri)
              client,       %% riak_client() - the store client
              prefix,       %% string() - prefix for resource uris
              riak,         %% local | {node(), atom()} - params for riak client
              bucketprops,  %% proplist() - properties of the bucket
              method,       %% atom() - HTTP method for the request
              api_version,  %% non_neg_integer() - old or new http api
              security     %% security context
             }).
-type context() :: #ctx{}.

-include_lib("webmachine/include/webmachine.hrl").
-include("riak_kv_wm_raw.hrl").

-spec init(proplists:proplist()) -> {ok, context()}.
%% @doc Initialize this resource.  This function extracts the
%%      'prefix' and 'riak' properties from the dispatch args.
init(Props) ->
    {ok, #ctx{
       prefix=proplists:get_value(prefix, Props),
       riak=proplists:get_value(riak, Props),
       api_version=proplists:get_value(api_version,Props),
       bucket_type=proplists:get_value(bucket_type, Props)
      }}.

-spec service_available(#wm_reqdata{}, context()) ->
    {boolean(), #wm_reqdata{}, context()}.
%% @doc Determine whether or not a connection to Riak can be
%%      established.  This function also takes this opportunity to extract
%%      the 'bucket' bindings from the dispatch.
service_available(RD, Ctx0=#ctx{riak=RiakProps}) ->
    Ctx = riak_kv_wm_utils:ensure_bucket_type(RD, Ctx0, #ctx.bucket_type),
    case riak_kv_wm_utils:get_riak_client(RiakProps, riak_kv_wm_utils:get_client_id(RD)) of
        {ok, C} ->
            {true,
             RD,
             Ctx#ctx{
               method=wrq:method(RD),
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
    {riak_kv_wm_utils:is_forbidden(RD), RD, Ctx};
forbidden(RD, Ctx=#ctx{security=Security}) ->
    case riak_kv_wm_utils:is_forbidden(RD) of
        true ->
            {true, RD, Ctx};
        false ->
            Perm = case Ctx#ctx.method of
                'PUT' ->
                    "riak_core.set_bucket";
                'GET' ->
                    "riak_core.get_bucket";
                'HEAD' ->
                    "riak_core.get_bucket";
		'DELETE' ->
                    "riak_core.set_bucket"
            end,

            Res = riak_core_security:check_permission({Perm,
                                                           {Ctx#ctx.bucket_type,
                                                            Ctx#ctx.bucket}},
                                                           Security),
            case Res of
                {false, Error, _} ->
                    RD1 = wrq:set_resp_header("Content-Type", "text/plain", RD),
                    {true, wrq:append_to_resp_body(unicode:characters_to_binary(Error, utf8, utf8), RD1), Ctx};
                {true, _} ->
                    {false, RD, Ctx}
            end
    end.

-spec allowed_methods(#wm_reqdata{}, context()) ->
    {[atom()], #wm_reqdata{}, context()}.
%% @doc Get the list of methods this resource supports.
%%      Properties allows HEAD, GET, and PUT.
allowed_methods(RD, Ctx) when Ctx#ctx.api_version =:= 1 ->
    {['HEAD', 'GET', 'PUT'], RD, Ctx};
allowed_methods(RD, Ctx) when Ctx#ctx.api_version =:= 2;
                              Ctx#ctx.api_version =:= 3 ->
    {['HEAD', 'GET', 'PUT', 'DELETE'], RD, Ctx}.

-spec malformed_request(#wm_reqdata{}, context()) ->
    {boolean(), #wm_reqdata{}, context()}.
%% @doc Determine whether query parameters, request headers,
%%      and request body are badly-formed.
%%      Body format is checked to be valid JSON, including
%%      a "props" object for a bucket-PUT.
malformed_request(RD, Ctx) when Ctx#ctx.method =:= 'PUT' ->
    malformed_bucket_put(RD, Ctx);
malformed_request(RD, Ctx) ->
    {false, RD, Ctx}.

-spec malformed_bucket_put(#wm_reqdata{}, context()) ->
    {boolean(), #wm_reqdata{}, context()}.
%% @doc Check the JSON format of a bucket-level PUT.
%%      Must be a valid JSON object, containing a "props" object.
malformed_bucket_put(RD, Ctx) ->
    case catch mochijson2:decode(wrq:req_body(RD)) of
        {struct, Fields} ->
            case proplists:get_value(?JSON_PROPS, Fields) of
                {struct, Props} ->
                    {false, RD, Ctx#ctx{bucketprops=Props}};
                _ ->
                    {true, bucket_format_message(RD), Ctx}
            end;
        _ ->
            {true, bucket_format_message(RD), Ctx}
    end.

-spec bucket_format_message(#wm_reqdata{}) -> #wm_reqdata{}.
%% @doc Put an error about the format of the bucket-PUT body
%%      in the response body of the #wm_reqdata{}.
bucket_format_message(RD) ->
    wrq:append_to_resp_body(
      ["bucket PUT must be a JSON object of the form:\n",
       "{\"",?JSON_PROPS,"\":{...bucket properties...}}"],
      wrq:set_resp_header(?HEAD_CTYPE, "text/plain", RD)).


resource_exists(RD, Ctx) ->
    {riak_kv_wm_utils:bucket_type_exists(Ctx#ctx.bucket_type), RD, Ctx}.

-spec content_types_provided(#wm_reqdata{}, context()) ->
    {[{ContentType::string(), Producer::atom()}], #wm_reqdata{}, context()}.
%% @doc List the content types available for representing this resource.
%%      "application/json" is the content-type for props requests.
content_types_provided(RD, Ctx) ->
    {[{"application/json", produce_bucket_body}], RD, Ctx}.

-spec encodings_provided(#wm_reqdata{}, context()) ->
    {[{Encoding::string(), Producer::function()}], #wm_reqdata{}, context()}.
%% @doc List the encodings available for representing this resource.
%%      "identity" and "gzip" are available for props requests.
encodings_provided(RD, Ctx) ->
    {riak_kv_wm_utils:default_encodings(), RD, Ctx}.

-spec content_types_accepted(#wm_reqdata{}, context()) ->
    {[{ContentType::string(), Acceptor::atom()}],
     #wm_reqdata{}, context()}.
%% @doc Get the list of content types this resource will accept.
%%      "application/json" is the only type accepted for props PUT.
content_types_accepted(RD, Ctx) ->
    {[{"application/json", accept_bucket_body}], RD, Ctx}.

-spec produce_bucket_body(#wm_reqdata{}, context()) ->
    {binary(), #wm_reqdata{}, context()}.
%% @doc Produce the bucket properties as JSON.
produce_bucket_body(RD, Ctx) ->
    Client = Ctx#ctx.client,
    Bucket = riak_kv_wm_utils:maybe_bucket_type(Ctx#ctx.bucket_type, Ctx#ctx.bucket),
    JsonProps1 = get_bucket_props_json(Client, Bucket),
    JsonProps2 = {struct, [JsonProps1]},
    JsonProps3 = mochijson2:encode(JsonProps2),
    {JsonProps3, RD, Ctx}.

get_bucket_props_json(Client, Bucket) ->
    Props1 = Client:get_bucket(Bucket),
    Props2 = lists:map(fun riak_kv_wm_utils:jsonify_bucket_prop/1, Props1),
    {?JSON_PROPS, {struct, Props2}}.

-spec accept_bucket_body(#wm_reqdata{}, context()) ->
        {true, #wm_reqdata{}, context()}.
%% @doc Modify the bucket properties according to the body of the
%%      bucket-level PUT request.
accept_bucket_body(RD, Ctx=#ctx{bucket_type=T, bucketprops=Props1}) ->
    case catch riak_kv_ts_util:maybe_parse_table_def(T, Props1) of
        {ok, Props2} ->
            set_bucket(RD, Ctx#ctx{ bucketprops = Props2 });
        {error, Details} when is_list(Details) ->
            error_response([{<<"error">>, list_to_binary(Details)}], RD, Ctx);
        {error, Details} ->
            error_response([{<<"error">>, Details}], RD, Ctx)
    end.

set_bucket(RD, Ctx=#ctx{bucket_type=T, bucket=B, client=C, bucketprops=Props1}) ->
    Props2 = lists:map(fun riak_kv_wm_utils:erlify_bucket_prop/1, Props1),
    case C:set_bucket({T,B}, Props2) of
        ok ->
            {true, RD, Ctx};
        {error, Details} ->
            error_response(Details, RD, Ctx)
    end.

error_response(Details, RD, Ctx) ->
    JSON = mochijson2:encode(Details),
    RD2 = wrq:append_to_resp_body(JSON, RD),
    {{halt, 400}, RD2, Ctx}.

-spec delete_resource(#wm_reqdata{}, context()) ->
    {boolean(), #wm_reqdata{}, context()}.
%% @doc Reset the bucket properties back to the default values
delete_resource(RD, Ctx=#ctx{bucket_type=T, bucket=B, client=C}) ->
    C:reset_bucket({T,B}),
    {true, RD, Ctx}.
