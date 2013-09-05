%% -------------------------------------------------------------------
%%
%% riak_kv_wm_index - Webmachine resource for running index queries.
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

%% @doc Resource for running queries on secondary indexes.
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
         forbidden/2,
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
          index_query,   %% The query..
          max_results :: all | pos_integer(), %% maximum number of 2i results to return, the page size.
          return_terms = false :: boolean(), %% should the index values be returned
          timeout :: non_neg_integer() | undefined | infinity
         }).

-define(ALL_2I_RESULTS, all).

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

forbidden(RD, Ctx) ->
    {riak_kv_wm_utils:is_forbidden(RD), RD, Ctx}.

%% @spec malformed_request(reqdata(), context()) ->
%%          {boolean(), reqdata(), context()}
%% @doc Determine whether query parameters are badly-formed.
%%      Specifically, we check that the index operation is of
%%      a known type.
malformed_request(RD, Ctx) ->
    %% Pull the params...
    Bucket = list_to_binary(riak_kv_wm_utils:maybe_decode_uri(RD, wrq:path_info(bucket, RD))),
    IndexField = list_to_binary(riak_kv_wm_utils:maybe_decode_uri(RD, wrq:path_info(field, RD))),
    Args1 = wrq:path_tokens(RD),
    Args2 = [list_to_binary(riak_kv_wm_utils:maybe_decode_uri(RD, X)) || X <- Args1],
    ReturnTerms0 = wrq:get_qs_value(?Q_2I_RETURNTERMS, "false", RD),
    ReturnTerms = normalize_boolean(string:to_lower(ReturnTerms0)),
    MaxResults0 = wrq:get_qs_value(?Q_2I_MAX_RESULTS, ?ALL_2I_RESULTS, RD),
    Continuation = wrq:get_qs_value(?Q_2I_CONTINUATION, undefined, RD),
    Timeout0 =  wrq:get_qs_value("timeout", undefined, RD),

    case {ReturnTerms, validate_timeout(Timeout0), validate_max(MaxResults0), riak_index:to_index_query(IndexField, Args2, Continuation)} of
        {malformed, _, _, _} ->
             {true,
             wrq:set_resp_body(io_lib:format("Invalid ~p. ~p is not a boolean",
                                             [?Q_2I_RETURNTERMS, ReturnTerms0]),
                               wrq:set_resp_header(?HEAD_CTYPE, "text/plain", RD)),
             Ctx};
        {_, {true, Timeout}, {true, MaxResults}, {ok, Query}} ->
            %% Request is valid.
            ReturnTerms1 = riak_index:return_terms(ReturnTerms, Query),
            NewCtx = Ctx#ctx{
                       bucket = Bucket,
                       index_query = Query,
                       max_results = MaxResults,
                       return_terms = ReturnTerms1,
                       timeout=Timeout
                      },
            {false, RD, NewCtx};
        {_, _, _, {error, Reason}} ->
            {true,
             wrq:set_resp_body(
               io_lib:format("Invalid query: ~p~n", [Reason]),
               wrq:set_resp_header(?HEAD_CTYPE, "text/plain", RD)),
             Ctx};
        {_, _, {false, BadVal}, _} ->
            {true,
             wrq:set_resp_body(io_lib:format("Invalid ~p. ~p is not a positive integer",
                                             [?Q_2I_MAX_RESULTS, BadVal]),
                               wrq:set_resp_header(?HEAD_CTYPE, "text/plain", RD)),
             Ctx};
        {_, {error, Input}, _, _} ->
            {true, wrq:append_to_resp_body(io_lib:format("Bad timeout "
                                                           "value ~p. Must be a non-negative integer~n",
                                                           [Input]),
                                             wrq:set_resp_header(?HEAD_CTYPE,
                                                                 "text/plain", RD)), Ctx}
    end.

validate_timeout(undefined) ->
    {true, undefined};
validate_timeout(Str) ->
    try
        list_to_integer(Str) of
        Int when Int >= 0 ->
            {true, Int};
        Neg ->
            {error, Neg}
    catch
        _:_ ->
            {error, Str}
    end.

validate_max(all) ->
    {true, all};
validate_max(N) when is_list(N) ->
    try
        list_to_integer(N) of
        Max when Max > 0  ->
            {true, Max};
        LessThanZero ->
            {false, LessThanZero}
    catch _:_ ->
            {false, N}
    end.

normalize_boolean("false") ->
    false;
normalize_boolean("true") ->
    true;
normalize_boolean(_) ->
    malformed.

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
    case wrq:get_qs_value("stream", "false", RD) of
        "true" ->
            handle_streaming_index_query(RD, Ctx);
        _ ->
            handle_all_in_memory_index_query(RD, Ctx)
    end.

handle_streaming_index_query(RD, Ctx) ->
    Client = Ctx#ctx.client,
    Bucket = Ctx#ctx.bucket,
    Query = Ctx#ctx.index_query,
    MaxResults = Ctx#ctx.max_results,
    ReturnTerms = Ctx#ctx.return_terms,
    Timeout = Ctx#ctx.timeout,

    %% Create a new multipart/mixed boundary
    Boundary = riak_core_util:unique_id_62(),
    CTypeRD = wrq:set_resp_header(
                "Content-Type",
                "multipart/mixed;boundary="++Boundary,
                RD),

    Opts = riak_index:add_timeout_opt(Timeout, [{max_results, MaxResults}]),

    {ok, ReqID, FSMPid} =  Client:stream_get_index(Bucket, Query, Opts),
    StreamFun = index_stream_helper(ReqID, FSMPid, Boundary, ReturnTerms, MaxResults, proplists:get_value(timeout, Opts), undefined, 0),
    {{stream, {<<>>, StreamFun}}, CTypeRD, Ctx}.

index_stream_helper(ReqID, FSMPid, Boundary, ReturnTerms, MaxResults, Timeout, LastResult, Count) ->
    fun() ->
            receive
                {ReqID, done} ->
                    Final = case make_continuation(MaxResults, [LastResult], Count) of
                                undefined -> ["\r\n--", Boundary, "--\r\n"];
                                Continuation ->
                                    Json = mochijson2:encode(mochify_continuation(Continuation)),
                                    ["\r\n--", Boundary, "\r\n",
                                     "Content-Type: application/json\r\n\r\n",
                                     Json,
                                     "\r\n--", Boundary, "--\r\n"]
                            end,
                    {iolist_to_binary(Final), done};
                {ReqID, {results, []}} ->
                    {<<>>, index_stream_helper(ReqID, FSMPid, Boundary, ReturnTerms, MaxResults, Timeout, LastResult, Count)};
                {ReqID, {results, Results}} ->
                    %% JSONify the results
                    JsonResults = encode_results(ReturnTerms, Results),
                    Body = ["\r\n--", Boundary, "\r\n",
                            "Content-Type: application/json\r\n\r\n",
                            JsonResults],
                    LastResult1 = last_result(Results),
                    Count1 = Count + length(Results),
                    {iolist_to_binary(Body),
                     index_stream_helper(ReqID, FSMPid, Boundary, ReturnTerms, MaxResults, Timeout, LastResult1, Count1)};
                {ReqID, Error} ->
                    stream_error(Error, Boundary)
            after Timeout ->
                    whack_index_fsm(ReqID, FSMPid),
                    stream_error({error, timeout}, Boundary)
            end
    end.

whack_index_fsm(ReqID, Pid) ->
    wait_for_death(Pid),
    clear_index_fsm_msgs(ReqID).

wait_for_death(Pid) ->
    Ref = erlang:monitor(process, Pid),
    exit(Pid, kill),
    receive
        {'DOWN', Ref, process, Pid, _Info} ->
            ok
    end.

clear_index_fsm_msgs(ReqID) ->
    receive
        {ReqID, _} ->
            clear_index_fsm_msgs(ReqID)
    after
        0 ->
            ok
    end.

stream_error(Error, Boundary) ->
    lager:error("Error in index wm: ~p", [Error]),
    ErrorJson = encode_error(Error),
    Body = ["\r\n--", Boundary, "\r\n",
            "Content-Type: application/json\r\n\r\n",
            ErrorJson,
            "\r\n--", Boundary, "--\r\n"],
    {iolist_to_binary(Body), done}.

encode_error({error, E}) ->
    encode_error(E);
encode_error(Error) when is_atom(Error); is_binary(Error) ->
    mochijson2:encode({struct, [{error, Error}]});
encode_error(Error) ->
    E = io_lib:format("~p",[Error]),
    mochijson2:encode({struct, [{error, erlang:iolist_to_binary(E)}]}).

handle_all_in_memory_index_query(RD, Ctx) ->
    Client = Ctx#ctx.client,
    Bucket = Ctx#ctx.bucket,
    Query = Ctx#ctx.index_query,
    MaxResults = Ctx#ctx.max_results,
    ReturnTerms = Ctx#ctx.return_terms,
    Timeout = Ctx#ctx.timeout,

    Opts = riak_index:add_timeout_opt(Timeout, [{max_results, MaxResults}]),

    %% Do the index lookup...
    case Client:get_index(Bucket, Query, Opts) of
        {ok, Results} ->
            Continuation = make_continuation(MaxResults, Results, length(Results)),
            JsonResults = encode_results(ReturnTerms, Results, Continuation),
            {JsonResults, RD, Ctx};
        {error, timeout} ->
            {{halt, 503},
             wrq:set_resp_header("Content-Type", "text/plain",
                                 wrq:append_to_response_body(
                                   io_lib:format("request timed out~n",[]),
                                   RD)),
             Ctx};
        {error, Reason} ->
            {{error, Reason}, RD, Ctx}
    end.

encode_results(ReturnTerms, Results) ->
    encode_results(ReturnTerms, Results, undefined).

encode_results(true, Results, Continuation) ->
    JsonKeys2 = {struct, [{?Q_RESULTS, [{struct, [{Val, Key}]} || {Val, Key} <- Results]}] ++
                     mochify_continuation(Continuation)},
    mochijson2:encode(JsonKeys2);
encode_results(false, Results, Continuation) ->
    JustTheKeys = filter_values(Results),
    JsonKeys1 = {struct, [{?Q_KEYS, JustTheKeys}] ++ mochify_continuation(Continuation)},
    mochijson2:encode(JsonKeys1).

mochify_continuation(undefined) ->
    [];
mochify_continuation(Continuation) ->
    [{?Q_2I_CONTINUATION, Continuation}].

filter_values([]) ->
    [];
filter_values([{_, _} | _T]=Results) ->
    [K || {_V, K} <- Results];
filter_values(Results) ->
    Results.

%% @doc Like `lists:last/1' but doesn't choke on an empty list
-spec last_result([] | list()) -> term() | undefined.
last_result([]) ->
    undefined;
last_result(L) ->
    lists:last(L).

%% @Doc if this is a paginated query make a continuation
-spec make_continuation(Max::non_neg_integer() | undefined,
                        list(),
                        ResultCount::non_neg_integer()) -> binary() | undefined.
make_continuation(MaxResults, Results, MaxResults) ->
    riak_index:make_continuation(Results);
make_continuation(_, _, _)  ->
    undefined.
