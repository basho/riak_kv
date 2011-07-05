%% -------------------------------------------------------------------
%%
%% riak_kv_wm_index - Webmachine resource for running index queries.
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc Resource for listing Riak buckets over HTTP.
%%
%% Available operations:
%%
%% GET /buckets/bucket/indexes/index/op/arg1...
%%   Run an index lookup, return the results as JSON.

-module(riak_kv_wm_index).

%% webmachine resource exports
-export([
         init/1,
         service_available/2,
         malformed_request/2,
         content_types_provided/2,
         encodings_provided/2,
         produce_index_results/2
        ]).

%% @type context() = term()
-record(ctx, {
          client,       %% riak_client() - the store client
          riak,         %% local | {node(), atom()} - params for riak client
          bucket,       %% The bucket to query.
          index_query   %% The query..
         }).

-include_lib("webmachine/include/webmachine.hrl").
-include("riak_kv_wm_raw.hrl").

%% @spec init(proplist()) -> {ok, context()}
%% @doc Initialize this resource.  
init(Props) ->
    {ok, #ctx{
       riak=proplists:get_value(riak, Props)
      }}.


%% @spec service_available(reqdata(), context()) ->
%%          {boolean(), reqdata(), context()}
%% @doc Determine whether or not a connection to Riak
%%      can be established. Also, extract query params.
service_available(RD, Ctx=#ctx{riak=RiakProps}) ->
    case riak_kv_wm_utils:get_riak_client(RiakProps, riak_kv_wm_utils:get_client_id(RD)) of
        {ok, C} ->
            {true, RD, Ctx#ctx { client=C }};
        Error ->
            {false,
             wrq:set_resp_body(
               io_lib:format("Unable to connect to Riak: ~p~n", [Error]),
               wrq:set_resp_header(?HEAD_CTYPE, "text/plain", RD)),
             Ctx}
    end.

%% @spec malformed_request(reqdata(), context()) ->
%%          {boolean(), reqdata(), context()}
%% @doc Determine whether query parameters are badly-formed. 
%%      Specifically, we check that the index operation is of
%%      a known type.
malformed_request(RD, Ctx) ->
    %% Pull the params...
    Bucket = list_to_binary(riak_kv_wm_utils:maybe_decode_uri(RD, wrq:path_info(bucket, RD))),
    IndexName = list_to_binary(riak_kv_wm_utils:maybe_decode_uri(RD, wrq:path_info(index, RD))),
    IndexOp = wrq:path_info(op, RD),
    Args1 = wrq:path_tokens(RD),
    io:format("[~s:~p] DEBUG - Args1: ~p~n", [?MODULE, ?LINE, Args1]),
    Args2 = [list_to_binary(riak_kv_wm_utils:maybe_decode_uri(RD, X)) || X <- Args1],
        
    case to_index_query(IndexOp, IndexName, Args2) of
        {ok, Query} ->
            io:format("[~s:~p] DEBUG - Query: ~p~n", [?MODULE, ?LINE, Query]),
            %% Request is valid.
            NewCtx = Ctx#ctx{ 
                       bucket = Bucket, 
                       index_query = Query
                      },
            {false, RD, NewCtx};
        {error, Reason} ->
            {true,
             wrq:set_resp_body(
               io_lib:format("Invalid query: ~p~n", [Reason]),
               wrq:set_resp_header(?HEAD_CTYPE, "text/plain", RD)),
             Ctx}
    end.
            


%% @spec content_types_provided(reqdata(), context()) ->
%%          {[{ContentType::string(), Producer::atom()}], reqdata(), context()}
%% @doc List the content types available for representing this resource.
%%      "application/json" is the content-type for bucket lists.
content_types_provided(RD, Ctx) ->
    {[{"application/json", produce_index_results}], RD, Ctx}.


%% @spec encodings_provided(reqdata(), context()) ->
%%          {[{Encoding::string(), Producer::function()}], reqdata(), context()}
%% @doc List the encodings available for representing this resource.
%%      "identity" and "gzip" are available for bucket lists.
encodings_provided(RD, Ctx) ->
    {riak_kv_wm_utils:default_encodings(), RD, Ctx}.


%% @spec produce_index_results(reqdata(), context()) -> {binary(), reqdata(), context()}
%% @doc Produce the JSON response to an index lookup.
produce_index_results(RD, Ctx) ->
    %% Extract vars...
    Client = Ctx#ctx.client,
    Bucket = Ctx#ctx.bucket,
    Query = Ctx#ctx.index_query,

    %% Do the index lookup...
    {ok, Results} = Client:get_index(Bucket, Query),

    %% JSONify the results...
    JsonKeys1 = {struct, [{?Q_KEYS, Results}]},
    JsonKeys2 = mochijson2:encode(JsonKeys1),
    {JsonKeys2, RD, Ctx}.


%% @private
%% @spec to_index_op_query(binary(), binary(), [binary()]) -> 
%%         {ok, [{atom(), binary(), list(binary())}]} | {error, Reasons}.
%% @doc Given an IndexOp, IndexName, and Args, construct and return a
%%      valid query, or a list of errors if the query is malformed.
to_index_query(IndexOp, IndexName, Args) ->
    io:format("[~s:~p] DEBUG - IndexOp: ~p~n", [?MODULE, ?LINE, IndexOp]),
    io:format("[~s:~p] DEBUG - IndexName: ~p~n", [?MODULE, ?LINE, IndexName]),
    %% Validate the index operation...
    case list_to_index_op(IndexOp) of
        {IndexOp1, NumArgs} when length(Args) == NumArgs->
            %% Validate the number of arguments..
            case riak_index:parse_fields([{IndexName, X} || X <- Args]) of
                {ok, _} -> 
                    {ok, [{IndexOp1, IndexName, Args}]};
                {error, FailureReasons} ->
                    {error, FailureReasons}
            end;                                
        {_IndexOp1, NumArgs} when length(Args) < NumArgs ->
            {error, {too_few_arguments, Args}};
        {_IndexOp1, NumArgs} when length(Args) > NumArgs ->
            {error, {too_many_arguments, Args}};
        undefined ->
            {error, {unknown_operation, IndexOp}}
    end.

%% @private
%% @spec list_to_index_op(binary()) -> atom().
%% @doc Convert a binary into an atom representing an index
%%      operation. If the operation is known, then return undefined.
list_to_index_op("eq") -> {eq, 1};
list_to_index_op("lt") -> {lt, 1};
list_to_index_op("gt") -> {gt, 1};
list_to_index_op("lte") -> {lte, 1};
list_to_index_op("gte") -> {gte, 1};
list_to_index_op("range") -> {range, 2};
list_to_index_op(_) -> undefined.
