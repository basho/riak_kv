%% --------------------------------------------------------------------------
%%
%% riak_kv_wm_preflist - Webmachine resource for getting bucket/key active
%%                       preflist.
%%
%%
%% Copyright (c) 2015 Basho Technologies, Inc.
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
%% --------------------------------------------------------------------------

%% @doc Resource for getting Riak bucket/key active preflist over HTTP.
%%
%% Available operations:
%%
%% GET /types/Type/buckets/Bucket/keys/Key/preflist
%% GET /buckets/Bucket/keys/Key/preflist
%%   Get information about the active preflist for a particular buckey/key
%%   combo.

-module(riak_kv_wm_preflist).

%% webmachine resource exports
-export([
         init/1,
         is_authorized/2,
         forbidden/2,
         allowed_methods/2,
         resource_exists/2,
         content_types_provided/2,
         encodings_provided/2,
         produce_preflist_body/2,
         malformed_request/2,
         service_available/2
        ]).

-record(ctx, {
          client,     %% :: riak_client()             %% the store client
          riak,       %% local | {node(), atom()}     %% params for riak client
          bucket_type :: binary(),                    %% bucket type (from uri)
          bucket      :: binary(),                    %% Bucket name (from uri)
          key         :: binary(),                    %% Key (from uri)
          security    :: riak_core_security:context() %% security context
         }).

-type context() :: #ctx{}.

-include_lib("webmachine/include/webmachine.hrl").
-include("riak_kv_wm_raw.hrl").

-spec init(proplists:proplist()) -> {ok, context()}.
%% @doc Initialize this resource.  This function extracts the
%%      'riak' properties from the dispatch args.
init(Props) ->
    {ok, #ctx{
            riak=proplists:get_value(riak, Props),
            bucket_type=proplists:get_value(bucket_type, Props)}}.

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

-spec service_available(#wm_reqdata{}, context()) ->
    {boolean(), #wm_reqdata{}, context()}.
%% @doc Determine whether or not a connection to Riak
%%      can be established. Also, extract query params.
service_available(RD, Ctx0=#ctx{riak=RiakProps}) ->
    Ctx = riak_kv_wm_utils:ensure_bucket_type(RD, Ctx0, #ctx.bucket_type),
    case riak_kv_wm_utils:get_riak_client(RiakProps,
                                          riak_kv_wm_utils:get_client_id(RD)) of
        {ok, C} ->
            {true, RD, Ctx#ctx { 
                         bucket=case wrq:path_info(bucket, RD) of
                             undefined -> undefined;
                             B -> list_to_binary(riak_kv_wm_utils:maybe_decode_uri(RD, B))
                         end,
                         key=case wrq:path_info(key, RD) of
                             undefined -> undefined;
                             K -> list_to_binary(riak_kv_wm_utils:maybe_decode_uri(RD, K))
                         end,
                         client=C }};
        Error ->
            {false,
             wrq:set_resp_body(
               io_lib:format("Unable to connect to Riak: ~p~n", [Error]),
               wrq:set_resp_header(?HEAD_CTYPE, "text/plain", RD)),
             Ctx}
    end.


forbidden(RD, Ctx = #ctx{security=undefined}) ->
    {riak_kv_wm_utils:is_forbidden(RD), RD, Ctx};
forbidden(RD, Ctx) ->
    case riak_kv_wm_utils:is_forbidden(RD) of
        true ->
            {true, RD, Ctx};
        false ->
            Res = riak_core_security:check_permission({"riak_kv.get_preflist",
                                                       {Ctx#ctx.bucket_type, Ctx#ctx.bucket}},
                                                      Ctx#ctx.security),
            case Res of
                {false, Error, _} ->
                    RD1 = wrq:set_resp_header("Content-Type", "text/plain", RD),
                    {true, wrq:append_to_resp_body(
                             unicode:characters_to_binary(
                               Error, utf8, utf8), RD1), Ctx};
                {true, _} ->
                    {false, RD, Ctx}
            end
    end.

-spec allowed_methods(#wm_reqdata{}, context()) ->
    {[atom()], #wm_reqdata{}, context()}.
%% @doc Get the list of methods this resource supports.
%%      Properties allows, GET.
allowed_methods(RD, Ctx) ->
    {['HEAD', 'GET'], RD, Ctx}.

-spec content_types_provided(#wm_reqdata{}, context()) ->
    {[{ContentType::string(), Producer::atom()}], #wm_reqdata{}, context()}.
%% @doc List the content types available for representing this resource.
%%      "application/json" is the content-type for listing keys.
content_types_provided(RD, Ctx) ->
    %% bucket-level: JSON description only
    {[{"application/json", produce_preflist_body}], RD, Ctx}.

-spec encodings_provided(#wm_reqdata{}, context()) ->
    {[{Encoding::string(), Producer::function()}], #wm_reqdata{}, context()}.
%% @doc List the encodings available for representing this resource.
%%      "identity" and "gzip" are available for a preflist request.
encodings_provided(RD, Ctx) ->
    %% identity and gzip for top-level and bucket-level requests
    {riak_kv_wm_utils:default_encodings(), RD, Ctx}.

resource_exists(RD, #ctx{bucket_type=BType}=Ctx) ->
    {riak_kv_wm_utils:bucket_type_exists(BType), RD, Ctx}.

-spec malformed_request(#wm_reqdata{}, context()) ->
    {boolean(), #wm_reqdata{}, context()}.
malformed_request(RD, Ctx) ->
    {false, RD, Ctx}.

-spec produce_preflist_body(#wm_reqdata{}, context()) ->
                                   {mochijson2:json_object(),
                                    #wm_reqdata{},
                                    context()}.
%% @doc Produce bucket/key annotated preflist as JSON.
produce_preflist_body(RD, #ctx{bucket=Bucket0,
                               bucket_type=BType,
                               key=Key}=Ctx) ->
    Bucket = riak_kv_wm_utils:maybe_bucket_type(BType, Bucket0),
    Preflist = riak_core_apl:get_apl_ann_with_pnum({Bucket, Key}),
    %% Encode
    Json = mochijson2:encode({struct,
                              [{<<"preflist">>,
                                lists:flatten(jsonify_preflist(Preflist))}]}),
    {Json, RD, Ctx}.

%% Private

-spec jsonify_preflist(riak_core_apl:preflist_with_pnum_ann()) -> list().
%% @doc Jsonify active preflist to json.
jsonify_preflist(Preflist) ->
    [jsonify_preflist_results(PartitionNumber, Node, T) ||
        {{PartitionNumber, Node}, T} <- Preflist].

-spec jsonify_preflist_results(non_neg_integer(), node(), primary|fallback) ->
                            [{struct, list()}].
jsonify_preflist_results(PartitionNumber, Node, Ann) ->
    [{struct,
      [{<<"partition">>, PartitionNumber},
       {<<"node">>, atom_to_binary(Node, utf8)},
       {<<"primary">>, Ann =:= primary}]}].
