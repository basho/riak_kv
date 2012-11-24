%% -------------------------------------------------------------------
%%
%% raw_http_resource: Webmachine resource for listing bucket properties.
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
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
%% GET /buckets/Bucket/props (NEW)
%% GET /Prefix/Bucket (OLD)
%%   Get information about the named Bucket, in JSON form:
%%     {"props":{Prop1:Val1,Prop2:Val2,...}}
%%   Each bucket property will be included in the "props" object.
%%   "linkfun" and "chash_keyfun" properties will be encoded as
%%   JSON objects of the form:
%%     {"mod":ModuleName,
%%      "fun":FunctionName}
%%   Where ModuleName and FunctionName are each strings representing
%%   a module and function.
%%
%% GET /buckets/Bucket/props
%% PUT /Prefix/Bucket (OLD)
%%   Modify bucket properties.
%%   Content-type must be application/json, and the body must have
%%   the form:
%%     {"props":{Prop:Val}}
%%   Where the "props" object takes the same form as returned from
%%   a GET of the same resource.
%% 
%% DELETE /buckets/Bucket/props
%%   Reset bucket properties back to the default settings
%%   not supported by the OLD API

-module(riak_kv_wm_props).

%% webmachine resource exports
-export([
         init/1,
         service_available/2,
         forbidden/2,
         allowed_methods/2,
         malformed_request/2,
         content_types_provided/2,
         encodings_provided/2,
         content_types_accepted/2,
         produce_bucket_body/2,
         accept_bucket_body/2,
         get_bucket_props_json/2,
         delete_resource/2
        ]).

%% @type context() = term()
-record(ctx, {bucket,       %% binary() - Bucket name (from uri)
              client,       %% riak_client() - the store client
              prefix,       %% string() - prefix for resource uris
              riak,         %% local | {node(), atom()} - params for riak client
              bucketprops,  %% proplist() - properties of the bucket
              method,       %% atom() - HTTP method for the request
              api_version   %% non_neg_integer() - old or new http api
             }).

-include_lib("webmachine/include/webmachine.hrl").
-include("riak_kv_wm_raw.hrl").

%% @spec init(proplist()) -> {ok, context()}
%% @doc Initialize this resource.  This function extracts the
%%      'prefix' and 'riak' properties from the dispatch args.
init(Props) ->
    {ok, #ctx{
       prefix=proplists:get_value(prefix, Props),
       riak=proplists:get_value(riak, Props),
       api_version=proplists:get_value(api_version,Props)
      }}.

%% @spec service_available(reqdata(), context()) ->
%%          {boolean(), reqdata(), context()}
%% @doc Determine whether or not a connection to Riak can be
%%      established.  This function also takes this opportunity to extract
%%      the 'bucket' bindings from the dispatch.
service_available(RD, Ctx=#ctx{riak=RiakProps}) ->
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

forbidden(RD, Ctx) ->
    {riak_kv_wm_utils:is_forbidden(RD), RD, Ctx}.

%% @spec allowed_methods(reqdata(), context()) ->
%%          {[method()], reqdata(), context()}
%% @doc Get the list of methods this resource supports.
%%      Properties allows HEAD, GET, and PUT.
allowed_methods(RD, Ctx) when Ctx#ctx.api_version =:= 1 ->
    {['HEAD', 'GET', 'PUT'], RD, Ctx};
allowed_methods(RD, Ctx) when Ctx#ctx.api_version =:= 2 ->
    {['HEAD', 'GET', 'PUT', 'DELETE'], RD, Ctx}.

%% @spec malformed_request(reqdata(), context()) ->
%%          {boolean(), reqdata(), context()}
%% @doc Determine whether query parameters, request headers,
%%      and request body are badly-formed.
%%      Body format is checked to be valid JSON, including
%%      a "props" object for a bucket-PUT.
malformed_request(RD, Ctx) when Ctx#ctx.method =:= 'PUT' ->
    malformed_bucket_put(RD, Ctx);
malformed_request(RD, Ctx) ->
    {false, RD, Ctx}.

%% @spec malformed_bucket_put(reqdata(), context()) ->
%%          {boolean(), reqdata(), context()}
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

%% @spec bucket_format_message(reqdata()) -> reqdata()
%% @doc Put an error about the format of the bucket-PUT body
%%      in the response body of the reqdata().
bucket_format_message(RD) ->
    wrq:append_to_resp_body(
      ["bucket PUT must be a JSON object of the form:\n",
       "{\"",?JSON_PROPS,"\":{...bucket properties...}}"],
      wrq:set_resp_header(?HEAD_CTYPE, "text/plain", RD)).


%% @spec content_types_provided(reqdata(), context()) ->
%%          {[{ContentType::string(), Producer::atom()}], reqdata(), context()}
%% @doc List the content types available for representing this resource.
%%      "application/json" is the content-type for props requests.
content_types_provided(RD, Ctx) ->
    {[{"application/json", produce_bucket_body}], RD, Ctx}.

%% @spec encodings_provided(reqdata(), context()) ->
%%          {[{Encoding::string(), Producer::function()}], reqdata(), context()}
%% @doc List the encodings available for representing this resource.
%%      "identity" and "gzip" are available for props requests.
encodings_provided(RD, Ctx) ->
    {riak_kv_wm_utils:default_encodings(), RD, Ctx}.

%% @spec content_types_accepted(reqdata(), context()) ->
%%          {[{ContentType::string(), Acceptor::atom()}],
%%           reqdata(), context()}
%% @doc Get the list of content types this resource will accept.
%%      "application/json" is the only type accepted for props PUT.
content_types_accepted(RD, Ctx) ->
    {[{"application/json", accept_bucket_body}], RD, Ctx}.

%% @spec produce_bucket_body(reqdata(), context()) -> {binary(), reqdata(), context()}
%% @doc Produce the bucket properties as JSON.
produce_bucket_body(RD, Ctx) ->
    Client = Ctx#ctx.client,
    Bucket = Ctx#ctx.bucket,
    JsonProps1 = get_bucket_props_json(Client, Bucket),
    JsonProps2 = {struct, [JsonProps1]},
    JsonProps3 = mochijson2:encode(JsonProps2),
    {JsonProps3, RD, Ctx}.

get_bucket_props_json(Client, Bucket) ->
    Props1 = Client:get_bucket(Bucket),
    Props2 = lists:map(fun jsonify_bucket_prop/1, Props1),
    {?JSON_PROPS, {struct, Props2}}.

%% @spec accept_bucket_body(reqdata(), context()) -> {true, reqdata(), context()}
%% @doc Modify the bucket properties according to the body of the
%%      bucket-level PUT request.
accept_bucket_body(RD, Ctx=#ctx{bucket=B, client=C, bucketprops=Props}) ->
    ErlProps = lists:map(fun erlify_bucket_prop/1, Props),
    case C:set_bucket(B, ErlProps) of
        ok ->
            {true, RD, Ctx};
        {error, Details} ->
            JSON = mochijson2:encode(Details),
            RD2 = wrq:append_to_resp_body(JSON, RD),
            {{halt, 400}, RD2, Ctx}
    end.

%% @spec delete_resource(reqdata(), context()) -> {boolean, reqdata(), context()}
%% @doc Reset the bucket properties back to the default values
delete_resource(RD, Ctx=#ctx{bucket=B, client=C}) ->
    C:reset_bucket(B),
    {true, RD, Ctx}.

%% @spec jsonify_bucket_prop({Property::atom(), erlpropvalue()}) ->
%%           {Property::binary(), jsonpropvalue()}
%% @type erlpropvalue() = integer()|string()|boolean()|
%%                        {modfun, atom(), atom()}|{atom(), atom()}
%% @type jsonpropvalue() = integer()|string()|boolean()|{struct,[jsonmodfun()]}
%% @type jsonmodfun() = {mod_binary(), binary()}|{fun_binary(), binary()}
%% @doc Convert erlang bucket properties to JSON bucket properties.
%%      Property names are converted from atoms to binaries.
%%      Integer, string, and boolean property values are left as integer,
%%      string, or boolean JSON values.
%%      {modfun, Module, Function} or {Module, Function} values of the
%%      linkfun and chash_keyfun properties are converted to JSON objects
%%      of the form:
%%        {"mod":ModuleNameAsString,
%%         "fun":FunctionNameAsString}
jsonify_bucket_prop({linkfun, {modfun, Module, Function}}) ->
    {?JSON_LINKFUN, {struct, [{?JSON_MOD,
                               list_to_binary(atom_to_list(Module))},
                              {?JSON_FUN,
                               list_to_binary(atom_to_list(Function))}]}};
jsonify_bucket_prop({linkfun, {qfun, _}}) ->
    {?JSON_LINKFUN, <<"qfun">>};
jsonify_bucket_prop({linkfun, {jsfun, Name}}) ->
    {?JSON_LINKFUN, {struct, [{?JSON_JSFUN, Name}]}};
jsonify_bucket_prop({linkfun, {jsanon, {Bucket, Key}}}) ->
    {?JSON_LINKFUN, {struct, [{?JSON_JSANON,
                               {struct, [{?JSON_JSBUCKET, Bucket},
                                         {?JSON_JSKEY, Key}]}}]}};
jsonify_bucket_prop({linkfun, {jsanon, Source}}) ->
    {?JSON_LINKFUN, {struct, [{?JSON_JSANON, Source}]}};
jsonify_bucket_prop({chash_keyfun, {Module, Function}}) ->
    {?JSON_CHASH, {struct, [{?JSON_MOD,
                             list_to_binary(atom_to_list(Module))},
                            {?JSON_FUN,
                             list_to_binary(atom_to_list(Function))}]}};
%% TODO Remove Legacy extractor prop in future version
jsonify_bucket_prop({rs_extractfun, {modfun, M, F}}) ->
    {?JSON_EXTRACT_LEGACY, {struct, [{?JSON_MOD,
                                      list_to_binary(atom_to_list(M))},
                                     {?JSON_FUN,
                                      list_to_binary(atom_to_list(F))}]}};
jsonify_bucket_prop({rs_extractfun, {{modfun, M, F}, Arg}}) ->
    {?JSON_EXTRACT_LEGACY, {struct, [{?JSON_MOD,
                                      list_to_binary(atom_to_list(M))},
                                     {?JSON_FUN,
                                      list_to_binary(atom_to_list(F))},
                                     {?JSON_ARG, Arg}]}};
jsonify_bucket_prop({search_extractor, {struct, _}=S}) ->
    {?JSON_EXTRACT, S};
jsonify_bucket_prop({search_extractor, {M, F}}) ->
    {?JSON_EXTRACT, {struct, [{?JSON_MOD,
                             list_to_binary(atom_to_list(M))},
                              {?JSON_FUN,
                               list_to_binary(atom_to_list(F))}]}};
jsonify_bucket_prop({search_extractor, {M, F, Arg}}) ->
    {?JSON_EXTRACT, {struct, [{?JSON_MOD,
                               list_to_binary(atom_to_list(M))},
                              {?JSON_FUN,
                               list_to_binary(atom_to_list(F))},
                              {?JSON_ARG, Arg}]}};

jsonify_bucket_prop({Prop, Value}) ->
    {list_to_binary(atom_to_list(Prop)), Value}.

%% @spec erlify_bucket_prop({Property::binary(), jsonpropvalue()}) ->
%%          {Property::atom(), erlpropvalue()}
%% @doc The reverse of jsonify_bucket_prop/1.  Converts JSON representation
%%      of bucket properties to their Erlang form.
erlify_bucket_prop({?JSON_LINKFUN, {struct, Props}}) ->
    case {proplists:get_value(?JSON_MOD, Props),
          proplists:get_value(?JSON_FUN, Props)} of
        {Mod, Fun} when is_binary(Mod), is_binary(Fun) ->
            {linkfun, {modfun,
                       list_to_existing_atom(binary_to_list(Mod)),
                       list_to_existing_atom(binary_to_list(Fun))}};
        {undefined, undefined} ->
            case proplists:get_value(?JSON_JSFUN, Props) of
                Name when is_binary(Name) ->
                    {linkfun, {jsfun, Name}};
                undefined ->
                    case proplists:get_value(?JSON_JSANON, Props) of
                        {struct, Bkey} ->
                            Bucket = proplists:get_value(?JSON_JSBUCKET, Bkey),
                            Key = proplists:get_value(?JSON_JSKEY, Bkey),
                            %% bomb if malformed
                            true = is_binary(Bucket) andalso is_binary(Key),
                            {linkfun, {jsanon, {Bucket, Key}}};
                        Source when is_binary(Source) ->
                            {linkfun, {jsanon, Source}}
                    end
            end
    end;
erlify_bucket_prop({?JSON_CHASH, {struct, Props}}) ->
    {chash_keyfun, {list_to_existing_atom(
                      binary_to_list(
                        proplists:get_value(?JSON_MOD, Props))),
                    list_to_existing_atom(
                      binary_to_list(
                        proplists:get_value(?JSON_FUN, Props)))}};
erlify_bucket_prop({Prop, Value}) ->
    {list_to_existing_atom(binary_to_list(Prop)), Value}.
