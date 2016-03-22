%% -------------------------------------------------------------------
%%
%% riak_kv_wm_bucket_type: Webmachine resource for bucket type properties
%%
%% Copyright (c) 2013 Basho Technologies, Inc.  All Rights Reserved.
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
%% Available operations:
%%
%% `GET /types/Type/props'
%%
%%  Get information about the named Type, in JSON form:
%%     `{"props":{Prop1:Val1,Prop2:Val2,...}}'
%%
%%   Each bucket-type property will be included in the "props" object.
%%
%%   `linkfun' and `chash_keyfun' properties will be encoded as
%%   JSON objects of the form:
%%
%% ```
%%     {"mod":ModuleName,
%%      "fun":FunctionName}'''
%%
%%   Where ModuleName and FunctionName are each strings representing
%%   a module and function.
%%
%% `POST|PUT /types/Type/props'
%%
%%   Modify bucket-type properties.
%%
%%   Content-type must be `application/json', and the body must have
%%   the form:
%%
%%     `{"props":{Prop:Val}}'
%%
%%   Where the "props" object takes the same form as returned from
%%   a GET of the same resource.

-module(riak_kv_wm_bucket_type).

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
         produce_bucket_type_body/2,
         accept_bucket_type_body/2
        ]).

-record(ctx, {bucket_type,  %% binary() - Bucket type (from uri)
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
            {true, RD, Ctx#ctx{method=wrq:method(RD), client=C}};
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
                    "riak_core.set_bucket_type";
                'GET' ->
                    "riak_core.get_bucket_type";
                'HEAD' ->
                    "riak_core.get_bucket_type"
            end,

            Res = riak_core_security:check_permission({Perm, Ctx#ctx.bucket_type}, Security),
            case Res of
                {false, Error, _} ->
                    RD1 = wrq:set_resp_header("Content-Type", "text/plain", RD),
                    {true, wrq:append_to_resp_body(unicode:characters_to_binary(Error, utf8, utf8), RD1), Ctx};
                {true, _} ->
                    forbidden_check_bucket_type(RD, Ctx)
            end
    end.

%% @doc Detects whether the requested bucket-type exists.
forbidden_check_bucket_type(RD, #ctx{method=M}=Ctx) when M =:= 'PUT' ->
    %% If the bucket type doesn't exist, we want to bail early so that
    %% users cannot PUT bucket types that don't exist.
    case riak_kv_wm_utils:bucket_type_exists(Ctx#ctx.bucket_type) of
        true ->
            {false, RD, Ctx};
        false ->
            {{halt, 404},
             wrq:set_resp_body(
               io_lib:format("Cannot modify unknown bucket type: ~p", [Ctx#ctx.bucket_type]),
               wrq:set_resp_header(?HEAD_CTYPE, "text/plain", RD)),
             Ctx}
    end;
forbidden_check_bucket_type(RD, Ctx) ->
    {false, RD, Ctx}.


-spec allowed_methods(#wm_reqdata{}, context()) ->
    {[atom()], #wm_reqdata{}, context()}.
%% @doc Get the list of methods this resource supports.
%%      Properties allows HEAD, GET, and PUT.
allowed_methods(RD, Ctx) when Ctx#ctx.api_version =:= 3 ->
    {['HEAD', 'GET', 'PUT'], RD, Ctx}.

-spec malformed_request(#wm_reqdata{}, context()) ->
    {boolean(), #wm_reqdata{}, context()}.
%% @doc Determine whether query parameters, request headers,
%%      and request body are badly-formed.
%%      Body format is checked to be valid JSON, including
%%      a "props" object for a bucket-PUT.
malformed_request(RD, Ctx) when Ctx#ctx.method =:= 'PUT' ->
    malformed_props(RD, Ctx);
malformed_request(RD, Ctx) ->
    {false, RD, Ctx}.

-spec malformed_props(#wm_reqdata{}, context()) ->
    {boolean(), #wm_reqdata{}, context()}.
%% @doc Check the JSON format of a bucket-level PUT.
%%      Must be a valid JSON object, containing a "props" object.
malformed_props(RD, Ctx) ->
    case catch mochijson2:decode(wrq:req_body(RD)) of
        {struct, Fields} ->
            case proplists:get_value(?JSON_PROPS, Fields) of
                {struct, Props} ->
                    {false, RD, Ctx#ctx{bucketprops=Props}};
                _ ->
                    {true, props_format_message(RD), Ctx}
            end;
        _ ->
            {true, props_format_message(RD), Ctx}
    end.

-spec props_format_message(#wm_reqdata{}) -> #wm_reqdata{}.
%% @doc Put an error about the format of the bucket-PUT body
%%      in the response body of the #wm_reqdata{}.
props_format_message(RD) ->
    wrq:append_to_resp_body(
      ["bucket type PUT must be a JSON object of the form:\n",
       "{\"",?JSON_PROPS,"\":{...bucket properties...}}"],
      wrq:set_resp_header(?HEAD_CTYPE, "text/plain", RD)).


resource_exists(RD, Ctx) ->
    {riak_kv_wm_utils:bucket_type_exists(Ctx#ctx.bucket_type), RD, Ctx}.

-spec content_types_provided(#wm_reqdata{}, context()) ->
    {[{ContentType::string(), Producer::atom()}], #wm_reqdata{}, context()}.
%% @doc List the content types available for representing this resource.
%%      "application/json" is the content-type for props requests.
content_types_provided(RD, Ctx) ->
    {[{"application/json", produce_bucket_type_body}], RD, Ctx}.

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
    {[{"application/json", accept_bucket_type_body}], RD, Ctx}.

-spec produce_bucket_type_body(#wm_reqdata{}, context()) ->
    {binary(), #wm_reqdata{}, context()}.
%% @doc Produce the bucket properties as JSON.
produce_bucket_type_body(RD, Ctx) ->
    Props = riak_core_bucket_type:get(Ctx#ctx.bucket_type),
    JsonProps = mochijson2:encode(
                  {struct,
                   [
                    {?JSON_PROPS,
                     [ riak_kv_wm_utils:jsonify_bucket_prop(P) || P <- Props ]}
                   ]}),
    {JsonProps, RD, Ctx}.

-spec accept_bucket_type_body(#wm_reqdata{}, context()) ->
    {true, #wm_reqdata{}, context()}.
%% @doc Modify the bucket properties according to the body of the
%%      bucket-level PUT request.
accept_bucket_type_body(RD, Ctx=#ctx{bucket_type=T, bucketprops=Props}) ->
    try lists:map(fun riak_kv_wm_utils:erlify_bucket_prop/1, Props) of
        ErlProps ->
            case riak_core_bucket_type:update(T, ErlProps) of
                ok ->
                    {true, RD, Ctx};
                {error, Details} ->
                    JSON = mochijson2:encode(Details),
                    RD2 = wrq:append_to_resp_body(JSON, RD),
                    {{halt, 400}, RD2, Ctx}
            end
    catch
        throw:Details ->
            error_out({halt, 400}, "Bad bucket type properties: ~p", [Details], RD, Ctx)
    end.

error_out(Type, Fmt, Args, RD, Ctx) ->
    {Type,
     wrq:set_resp_header(
       "Content-Type", "text/plain",
       wrq:append_to_response_body(
         lists:flatten(io_lib:format(Fmt, Args)), RD)),
     Ctx}.
