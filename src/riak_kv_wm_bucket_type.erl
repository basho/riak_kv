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
%% GET /types/Type/props
%%   Get information about the named Type, in JSON form:
%%     {"props":{Prop1:Val1,Prop2:Val2,...}}
%%   Each bucket-type property will be included in the "props" object.
%%   "linkfun" and "chash_keyfun" properties will be encoded as
%%   JSON objects of the form:
%%     {"mod":ModuleName,
%%      "fun":FunctionName}
%%   Where ModuleName and FunctionName are each strings representing
%%   a module and function.

-module(riak_kv_wm_bucket_type).

%% webmachine resource exports
-export([
         init/1,
         service_available/2,
         is_authorized/2,
         forbidden/2,
         allowed_methods/2,
         content_types_provided/2,
         encodings_provided/2,
         resource_exists/2,
         produce_bucket_type_body/2
        ]).

%% @type context() = term()
-record(ctx, {bucket_type,  %% binary() - Bucket type (from uri)
              bucketprops,  %% proplist() - properties of the bucket
              api_version,  %% non_neg_integer() - old or new http api
              security     %% security context
             }).

-include_lib("webmachine/include/webmachine.hrl").
-include("riak_kv_wm_raw.hrl").

%% @spec init(proplist()) -> {ok, context()}
%% @doc Initialize this resource.  This function extracts the
%%      'prefix' and 'riak' properties from the dispatch args.
init(Props) ->
    {ok, #ctx{
            api_version=proplists:get_value(api_version,Props),
            bucket_type=proplists:get_value(bucket_type, Props)
           }}.

%% @spec service_available(reqdata(), context()) ->
%%          {boolean(), reqdata(), context()}
%% @doc Determine whether or not a connection to Riak can be
%%      established.  This function also takes this opportunity to extract
%%      the 'bucket' bindings from the dispatch.
service_available(RD, Ctx0=#ctx{riak=RiakProps}) ->
    Ctx = riak_kv_wm_utils:ensure_bucket_type(RD, Ctx0, #ctx.bucket_type),
    {true, RD, Ctx}.

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
            Res = riak_core_security:check_permission({"riak_core.get_bucket_type", Ctx#ctx.bucket_type}, Security),
            case Res of
                {false, Error, _} ->
                    RD1 = wrq:set_resp_header("Content-Type", "text/plain", RD),
                    {true, wrq:append_to_resp_body(list_to_binary(Error), RD1), Ctx};
                {true, _} ->
                    {false, RD, Ctx}
            end
    end.

%% @spec allowed_methods(reqdata(), context()) ->
%%          {[method()], reqdata(), context()}
%% @doc Get the list of methods this resource supports.
%%      Properties allows HEAD, GET, and PUT.
allowed_methods(RD, Ctx) when Ctx#ctx.api_version =:= 3 ->
    {['HEAD', 'GET'], RD, Ctx}.

%% @spec resource_exists(reqdata(), context()) ->
%%       {boolean(), reqdata(), context()}.
%% @doc Whether the resource exists
resource_exists(RD, Ctx) ->
    {riak_kv_wm_utils:bucket_type_exists(Ctx#ctx.bucket_type), RD, Ctx}.

%% @spec content_types_provided(reqdata(), context()) ->
%%          {[{ContentType::string(), Producer::atom()}], reqdata(), context()}
%% @doc List the content types available for representing this resource.
%%      "application/json" is the content-type for props requests.
content_types_provided(RD, Ctx) ->
    {[{"application/json", produce_bucket_type_body}], RD, Ctx}.

%% @spec encodings_provided(reqdata(), context()) ->
%%          {[{Encoding::string(), Producer::function()}], reqdata(), context()}
%% @doc List the encodings available for representing this resource.
%%      "identity" and "gzip" are available for props requests.
encodings_provided(RD, Ctx) ->
    {riak_kv_wm_utils:default_encodings(), RD, Ctx}.

%% @spec produce_bucket_body(reqdata(), context()) -> {binary(), reqdata(), context()}
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
