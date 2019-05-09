%% -------------------------------------------------------------------
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

%% @doc Webmachine resource for running aae fold queries.
%%
%% Available operations (NOTE: within square brackets means optional)
%%
%% ```
%% GET /cachedtrees/nvals/NVal/root
%% GET /cachedtrees/nvals/NVal/branch?filter
%% GET /cachedtrees/nvals/NVal/keysclocks?filter
%% GET /rangetrees/[types/Type/]buckets/Bucket/trees/Size?filter
%% GET /rangetrees/[types/Type/]buckets/Bucket/keysclocks?filter
%% GET /siblings/[types/Type/]buckets/Bucket/counts/Cnt?filter
%% GET /objectsizes/[types/Type/]buckets/Bucket/sizes/Size?filter
%% GET /objectstats/[types/Type/]buckets/Bucket?filter
%% '''
%% @TODO Filter contains key ranges, date ranges, has_fun, segment
%% filter now (doc below)
%%
%%   Run an AAE Fold on the underlying parallel or native AAE store
%%
%% The contents of the `filter' URL parameter varies depending on
%% URL. The `filter' is a set either a list of integers (for
%% cachedtrees branch and keysclocks) meaninb branches for
%% cachedtrees/branch and segments for cachedtrees/keysclocks OR
%% filter is set of Key->Value pairs encoded as JSON with the
%% following possible values:


%% <ul>
%%   <li><tt>key_range=Range</tt><br />
%%         an object with two keys start=Binary, and end=Binary. Where
%%         each is a key. Used to limit all of rangetrees, siblings,
%%         objectsizes and objectstats queries. If this element is
%%         absent from the filter then ALL keys will be queried
%%   </li>
%%   <li><tt>date_range=Range</tt><br /> an object with two keys
%%         start=Integer, and end=Integer. Where each is a 32bit unix
%%         seconds since the epoch timestamp. Used to limit all of
%%         rangetrees, siblings, objectsizes and objectstats
%%         queries. If this element is absent from the filter then ALL
%%         lastmodified will be queried
%%   </li>
%%   <li><tt>segment_filter=Object</tt><br />
%%         rangetrees only
%%         An object with two keys, segments=array(Integer) an array
%%         of segment IDs to query. And tree_size= [xxsmall | xsmall |
%%         small | medium | large | xlarge] not the tree size for this
%%         query, but the tree size from which the segments in
%%         `segments' where requested. This means that in the case of
%%         rangetrees queries you may request a tree of size X but
%%         with a filter on segments that were originally fetched with
%%         a tree of size Y. If absent then `all' segments will be queried.
%%   </li>
%%   <li><tt>hash_iv=Integer</tt><br />
%%          rangetrees trees query only
%%          an integer that is an initialisation vector for the hash
%%          method for the trees. Useful for avoiding hash collision,
%%          this IV will be used to initialise the hash function that
%%          will be used to hash version vectors that go into creating
%%          the merkle tree. If absent then the default hash fun is used.
%%   </li>
%% </ul>

-module(riak_kv_wm_aaefold).

%% webmachine resource exports
-export([
         init/1,
         service_available/2,
         malformed_request/2,
         content_types_provided/2,
         encodings_provided/2,
         resource_exists/2,
         produce_fold_results/2
        ]).


-ifdef(TEST).
-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-export([prop_not_malformed/0]).
-define(NUMTESTS, 1000).
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).
-endif.
-include_lib("eunit/include/eunit.hrl").
-endif.

-include_lib("webmachine/include/webmachine.hrl").
-include("riak_kv_wm_raw.hrl").

-record(ctx, {
              client,       %% riak_client() - the store client
              riak,         %% local | {node(), atom()} - params for riak client
              bucket_type,  %% Bucket type (from uri)
              bucket,       %% The bucket to query (if relevant)
              query   %% The query..
             }).

%% used for all the different query types that take a filter, only
%% some values used for some queries.
-record(filter, {
                 key_range = all :: {binary(), binary()} | all,
                 date_range = all :: {pos_integer(), pos_integer()} | all,
                 hash_method = pre_hash :: {rehash, non_neg_integer()} | pre_hash,
                 segment_filter = all :: {segments, list(pos_integer()), leveled_tictac:tree_size()} | all
                }).

-type context() :: #ctx{}.
-type filter() :: #filter{}.

-define(SEG_FILT, <<"segment_filter">>).
-define(KEY_RANGE, <<"key_range">>).
-define(DATE_RANGE, <<"date_range">>).
-define(HASH_IV, <<"hash_iv">>).

-define(FILTER_FIELDS, [?SEG_FILT, ?KEY_RANGE, ?DATE_RANGE, ?HASH_IV]).


%% @doc Initialize this resource.
-spec init(proplists:proplist()) -> {ok, context()}.
init(Props) ->
    {ok, #ctx{
       riak=proplists:get_value(riak, Props),
       bucket_type=proplists:get_value(bucket_type, Props)
      }}.

%% @doc Determine whether or not a connection to Riak
%%      can be established. Also, extract query params.
-spec service_available(#wm_reqdata{}, context()) ->
    {boolean(), #wm_reqdata{}, context()}.
service_available(RD, Ctx0=#ctx{riak=RiakProps}) ->
    Ctx = riak_kv_wm_utils:ensure_bucket_type(RD, Ctx0, #ctx.bucket_type),
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

%% @doc Determine whether query parameters are badly-formed.
%%      Specifically, we check that the aaefold operation is of
%%      a known type.
-spec malformed_request(#wm_reqdata{}, context()) ->
    {boolean(), #wm_reqdata{}, context()}.
malformed_request(RD, Ctx) ->
    %% determine type of query, and check for each type
    PathTokens = path_tokens(RD),

    FoldType = riak_kv_wm_utils:maybe_decode_uri(RD, hd(PathTokens)),

    case FoldType of
        "cachedtrees" -> malformed_cached_tree_request(RD, Ctx);
        "rangetrees" -> malformed_range_tree_request(RD, Ctx);
        "siblings" -> malformed_find_keys_request({sibling_count, count}, RD, Ctx);
        "objectsizes" -> malformed_find_keys_request({object_size, size}, RD, Ctx);
        "objectstats" -> malformed_object_stats_request(RD, Ctx)
    end.

%% @private check that we can parse out a valid cached tree aae fold
%% query, if so, store it in the ctx for execution
-spec malformed_cached_tree_request(#wm_reqdata{}, context()) ->
                                    {boolean(), #wm_reqdata{}, context()}.
malformed_cached_tree_request(RD, Ctx) ->
    case validate_nval(wrq:path_info(nval, RD)) of
        {invalid, Reason} ->
            malformed_response("Cached tree request invalid nval ~p",
                               [Reason],
                               RD, Ctx);
        {valid, NVal} ->
            malformed_cached_tree_request(NVal, RD, Ctx)
    end.

%% @private check that we have what we need in the request for a
%% cached tree query
-spec malformed_cached_tree_request(pos_integer(), #wm_reqdata{}, context()) ->
                                    {boolean(), #wm_reqdata{}, context()}.
malformed_cached_tree_request(NVal, RD, Ctx) ->
    QueryType = riak_kv_wm_utils:maybe_decode_uri(RD, lists:last(path_tokens(RD))),
    case  QueryType of
        "root" ->
            %% root query, has an nval, all good
            {false, RD,
             Ctx#ctx{
               query = {merge_root_nval, NVal}
              }};
        Q when Q == "branch";
               Q == "keysclocks" ->
            Filter0 = wrq:get_qs_value(?Q_AAEFOLD_FILTER, undefined, RD),
            malformed_cached_tree_request_filter(list_to_existing_atom(Q), Filter0, NVal, RD, Ctx);
        Other ->
            malformed_response("unkown cached aae tree query ~p",
                               [Other],
                               RD, Ctx)
    end.

%% @private check that the request provides a valid filter for cached
%% tree queries that need it
-spec malformed_cached_tree_request_filter(branch | keysclocks,
                                           Filter::any(),
                                           pos_integer(),
                                           #wm_reqdata{},
                                           context()) ->
                                                  {boolean(), #wm_reqdata{}, context()}.
malformed_cached_tree_request_filter(QType, undefined, _NVal, RD, Ctx) ->
    malformed_response("Filter query param required for ~p aae fold",
                       [QType],
                       RD,
                       Ctx);
malformed_cached_tree_request_filter(QType, Filter0, NVal, RD, Ctx) ->
    case validate_cached_tree_filter(Filter0) of
        {invalid, Reason} ->
            malformed_response("Invalid branch | segment fiter ~p",
                               [Reason],
                               RD,
                               Ctx);
        {valid, Filter} ->
            Query =
                case QType of
                    branch ->
                        {merge_branch_nval, NVal, Filter};
                    keysclocks ->
                        {fetch_clocks_nval, NVal, Filter}
                end,
            {false, RD,
             Ctx#ctx{
               query = Query
              }}
    end.

%% @private validate the request for range tree query and populate
%% context with query (if valid.)
-spec malformed_range_tree_request(#wm_reqdata{}, context()) ->
                                          {boolean(), #wm_reqdata{}, context()}.
malformed_range_tree_request(RD, Ctx) ->
    case wrq:path_info(bucket, RD) of
        undefined ->
            malformed_response("Bucket required", [], RD, Ctx);
        Bucket0 ->
            Bucket = erlang:list_to_binary(
                       riak_kv_wm_utils:maybe_decode_uri(RD, Bucket0)
                      ),
            TreeSize = wrq:path_info(size, RD),
            malformed_range_tree_request(TreeSize, RD, Ctx#ctx{bucket=Bucket})
    end.

%% @private decide on query type and parse out filter
-spec malformed_range_tree_request(undefined | string(), #wm_reqdata{}, context()) ->
                                          {boolean(),  #wm_reqdata{}, context()}.
malformed_range_tree_request(undefined=_TreeSize, RD, Ctx) ->
    %% no tree size, last token _MUST_ be "keysclocks" or it's invalid
    QueryType = riak_kv_wm_utils:maybe_decode_uri(RD, lists:last(path_tokens(RD))),
    case QueryType of
        "keysclocks" ->
            Filter0 = wrq:get_qs_value(?Q_AAEFOLD_FILTER, undefined, RD),
            malformed_range_tree_keysclocks_request(Filter0, RD, Ctx);
        Other ->
            malformed_response("Invalid rangetree aae query ~p",
                               [Other],
                               RD,
                               Ctx)
    end;
malformed_range_tree_request(TreeSize0, RD, Ctx) ->
    case validate_treesize(TreeSize0) of
        {invalid, Reason} ->
            malformed_response("Invalid treesize ~p", [Reason], RD, Ctx);
        {valid, TreeSize} ->
            Filter0 = wrq:get_qs_value(?Q_AAEFOLD_FILTER, undefined, RD),
            malformed_range_tree_request(Filter0, TreeSize, RD, Ctx)
    end.

%% @private finally parse out the query filter and validate it
-spec malformed_range_tree_request(undefined | string(),
                                   leveled_tictac:tree_size(),
                                   #wm_reqdata{},
                                   context()) ->
                                          {boolean(), #wm_reqdata{}, context()}.
malformed_range_tree_request(Filter0, TreeSize, RD, Ctx) ->
    case validate_range_filter(Filter0) of
        {invalid, Reason} ->
            malformed_response("Invalid range filter ~p",
                               [Reason],
                               RD,
                               Ctx);
        {valid, Filter} ->
            QBucket = query_bucket(Ctx),
             Query = {merge_tree_range,
                      QBucket,
                      Filter#filter.key_range,
                      TreeSize,
                      Filter#filter.segment_filter,
                      Filter#filter.date_range,
                      Filter#filter.hash_method},
            {false, RD,
             Ctx#ctx{query= Query}}
    end.

%% @private finally, parse out query filter and add query to context
-spec malformed_range_tree_keysclocks_request(undefined | string(),
                                              #wm_reqdata{},
                                              context()) ->
                                                     {boolean(), #wm_reqdata{}, context()}.
malformed_range_tree_keysclocks_request(Filter0, RD, Ctx) ->
    case validate_range_filter(Filter0) of
        {invalid, Reason} ->
            malformed_response("Invalid range filter ~p",
                               [Reason],
                               RD,
                               Ctx);
        {valid, Filter} ->
            QBucket = query_bucket(Ctx),
             Query = {fetch_clocks_range,
                      QBucket,
                      Filter#filter.key_range,
                      Filter#filter.segment_filter,
                      Filter#filter.date_range},
            {false, RD,
             Ctx#ctx{query= Query}}
    end.

%% @private validate and parse the find keys queries
-spec malformed_find_keys_request({sibling_count, count} | {object_size, size},
                                  #wm_reqdata{},
                                  context()) ->
                                         {boolean(), #wm_reqdata{}, context()}.
malformed_find_keys_request({QType, UrlArg}, RD, Ctx) ->
    case wrq:path_info(bucket, RD) of
        undefined ->
            malformed_response("Bucket required", [], RD, Ctx);
        Bucket0 ->
            Bucket = erlang:list_to_binary(
                       riak_kv_wm_utils:maybe_decode_uri(RD, Bucket0)
                      ),
            QArg = validate_int(wrq:path_info(UrlArg, RD)),
            malformed_find_keys_request({QType, UrlArg}, QArg, RD, Ctx#ctx{bucket=Bucket})
    end.

-spec malformed_find_keys_request({sibling_count, count} | {object_size, size},
                                  {valid, pos_integer()} | {invalid, any()},
                                  #wm_reqdata{},
                                  context()) ->
                                         {boolean(), #wm_reqdata{}, context()}.
malformed_find_keys_request({QType, UrlArg}, {invalid, Reason}, RD, Ctx) ->
    malformed_response("~p requires integer for ~p. Value ~p invalid",
                       [QType, UrlArg, Reason],
                       RD,
                       Ctx);
malformed_find_keys_request({QType, _UrlArg}, {valid, QArg}, RD, Ctx) ->
    Filter0 = wrq:get_qs_value(?Q_AAEFOLD_FILTER, undefined, RD),
    case validate_range_filter(Filter0) of
        {invalid, Reason} ->
            malformed_response("Invalid range filter ~p",
                               [Reason],
                               RD,
                               Ctx);
        {valid, Filter} ->
            QBucket = query_bucket(Ctx),
            Query = {
                     find_keys,
                     QBucket,
                     Filter#filter.key_range,
                     Filter#filter.date_range,
                     {QType, QArg}
                    },
            {false, RD,
             Ctx#ctx{query = Query}}
    end.

%% @private validate and populate the object stats query
-spec malformed_object_stats_request(#wm_reqdata{}, context()) ->
                                            {boolean(), #wm_reqdata{}, context()}.
malformed_object_stats_request(RD, Ctx) ->
    case wrq:path_info(bucket, RD) of
        undefined ->
            malformed_response("Bucket required", [], RD, Ctx);
        Bucket0 ->
            Bucket = erlang:list_to_binary(
                       riak_kv_wm_utils:maybe_decode_uri(RD, Bucket0)
                      ),
            Ctx2 = Ctx#ctx{bucket=Bucket},
            Filter0 = wrq:get_qs_value(?Q_AAEFOLD_FILTER, undefined, RD),
            case validate_range_filter(Filter0) of
                {invalid, Reason} ->
                    malformed_response("Invalid range filter ~p",
                                       [Reason],
                                       RD,
                                       Ctx2);
                {valid, Filter} ->
                    QBucket = query_bucket(Ctx2),
                    Query = {
                             object_stats,
                             QBucket,
                             Filter#filter.key_range,
                             Filter#filter.date_range
                            },
                    {false, RD,
                     Ctx2#ctx{query= Query}}
            end
    end.

%% @private since we use it so often, wrap it up
-spec malformed_response(string(), list(any()), #wm_reqdata{}, context()) ->
                                {true, #wm_reqdata{}, context()}.
malformed_response(MessageFmt, FmtArgs, RD, Ctx) ->
    {true,
     wrq:set_resp_body(io_lib:format(MessageFmt,
                                     FmtArgs),
                       wrq:set_resp_header(?HEAD_CTYPE, "text/plain", RD)),
    Ctx}.

%% @private buckets, are they typed, are they not?
-spec query_bucket(context()) -> riak_object:bucket().
query_bucket(Ctx) ->
    riak_kv_wm_utils:maybe_bucket_type(Ctx#ctx.bucket_type, Ctx#ctx.bucket).

-spec validate_nval(undefined | string()) ->
                           {invalid, Reason::any()} |
                           {valid, pos_integer()}.
validate_nval(NVal) ->
    validate_int(NVal).

-spec validate_int(undefined | string()) ->
                           {invalid, Reason::any()} |
                           {valid, pos_integer()}.
validate_int(undefined) ->
    {invalid, undefined};
validate_int(String) ->
    try
        list_to_integer(String) of
        N when N > 0 ->
            {valid, N};
        Neg ->
            {invalid, Neg}
    catch _:_ ->
            {invalid, String}
    end.

-spec validate_cached_tree_filter(string()) -> {valid, list(non_neg_integer())} |
                                               {invalid, Reason::any()}.
validate_cached_tree_filter(String) ->
    try mochijson2:decode(base64:decode(String)) of
        Filter when is_list(Filter) ->
            %% TODO: should check all are integer?
            {valid, Filter};
        Other ->
            {invalid, Other}
    catch _:_ ->
            {invalid, String}
    end.


-spec validate_range_filter(undefined | string()) ->
                                   {valid, filter()} |
                                   {invalid, Reason::any()}.
validate_range_filter(undefined) ->
    {valid, #filter{}};
validate_range_filter(String) ->
    try mochijson2:decode(base64:decode(String)) of
        {struct, Filter} ->
            validate_range_filter(Filter, ?FILTER_FIELDS, #filter{});
        Other ->
            {invalid, Other}
    catch _:_ ->
            {invalid, String}
    end.

-spec validate_range_filter(list(), list(), filter()) ->
                                   {invalid, Reason::term()} |
                                   {valid, filter()}.
validate_range_filter(_FilterJson, []=_Fields, Filter) ->
    {valid, Filter};
validate_range_filter(FilterJson, [Field | Fields], Filter0) ->
    FieldVal = proplists:get_value(Field, FilterJson),
    case validate_filter_field(Field, FieldVal, Filter0) of
        {valid, Filter} ->
            validate_range_filter(FilterJson, Fields, Filter);
        Other ->
            Other
    end.

-spec validate_filter_field(binary(), any(), filter()) ->
                                   {valid, filter()} |
                                   {invalid, Reason::any()}.
validate_filter_field(?SEG_FILT, {struct, SegFiltJson}, Filter) ->
    case validate_treesize(proplists:get_value(<<"tree_size">>, SegFiltJson)) of
        {valid, TreeSize} ->
            case validate_segment_list(proplists:get_value(<<"segments">>, SegFiltJson)) of
                {valid, Segments} ->
                    {valid, Filter#filter{segment_filter= {segments, Segments, TreeSize}}};
                {invalid, Reason} ->
                    {invalid, Reason}
            end;
        {invalid, ITS} ->
            {invalid, ITS}
    end;
validate_filter_field(?SEG_FILT, <<"all">>, Filter) ->
    {valid, Filter};
validate_filter_field(?SEG_FILT, undefined, Filter) ->
    {valid, Filter};
validate_filter_field(?SEG_FILT, Other, _Filter) ->
    {invalid, {?SEG_FILT, Other}};
validate_filter_field(?KEY_RANGE, {struct, KeyRangeJson}, Filter) ->
    case {proplists:get_value(<<"start">>, KeyRangeJson),
          proplists:get_value(<<"end">>, KeyRangeJson)} of
        {Start, End} when is_binary(Start), is_binary(End) ->
            {valid, Filter#filter{key_range= {Start, End}}};
        Other ->
            {invalid, {?KEY_RANGE, Other}}
    end;
validate_filter_field(?KEY_RANGE, <<"all">>, Filter) ->
    {valid, Filter};
validate_filter_field(?KEY_RANGE, undefined, Filter) ->
    {valid, Filter};
validate_filter_field(?KEY_RANGE, Other, _Filter) ->
    {invalid, {?KEY_RANGE, Other}};
validate_filter_field(?DATE_RANGE, {struct, DateRangeJson}, Filter) ->
    case {proplists:get_value(<<"start">>, DateRangeJson),
          proplists:get_value(<<"end">>, DateRangeJson)} of
        {Start, End} when is_integer(Start),
                          is_integer(End),
                          Start >= 0,
                          End >= 0 ->
            {valid, Filter#filter{date_range= {Start, End}}};
        Other ->
            {invalid, {?DATE_RANGE, Other}}
    end;
validate_filter_field(?DATE_RANGE, <<"all">>, Filter) ->
    {valid, Filter};
validate_filter_field(?DATE_RANGE, undefined, Filter) ->
    {valid, Filter};
validate_filter_field(?DATE_RANGE, Other, _Filter) ->
    {invalid, {?DATE_RANGE, Other}};
validate_filter_field(?HASH_IV, IV, Filter) when is_integer(IV) andalso IV > -1 ->
    {valid, Filter#filter{hash_method={rehash, IV}}};
validate_filter_field(?HASH_IV, undefined, Filter) ->
    {valid, Filter};
validate_filter_field(?HASH_IV, Other, _Filter) ->
    {invalid, {?HASH_IV, Other}}.

-spec validate_segment_list(any()) ->
                                   {valid, list()} |
                                   {invalid, Reason::any()}.
validate_segment_list(undefined) ->
    {invalid, undefined};
validate_segment_list(SegList) when is_list(SegList) ->
    %% @TODO should check contents??
    {valid, SegList};
validate_segment_list(Other) ->
    {invalid, Other}.

-spec validate_treesize(undefined | binary() | string()) ->
                               {valid, leveled_tictac:tree_size()} |
                               {invalid, Reason::any()}.
validate_treesize(undefined) ->
    {invalid, undefined};
validate_treesize(TreeSizeBin) when is_binary(TreeSizeBin) ->
    validate_treesize(binary_to_list(TreeSizeBin));
validate_treesize(TreeSizeStr) ->
    %% if it's a valid leveled_tictac:tree_size/0 there will be an
    %% existing atom!
    try list_to_existing_atom(TreeSizeStr) of
        TreeSize ->
            case leveled_tictac:valid_size(TreeSize) of
                true ->
                    {valid, TreeSize};
                false ->
                    {invalid, TreeSize}
            end
    catch _:_ ->
            {invalid, TreeSizeStr}
    end.

-spec content_types_provided(#wm_reqdata{}, context()) ->
    {[{ContentType::string(), Producer::atom()}], #wm_reqdata{}, context()}.
%% @doc List the content types available for representing this resource.
%%      "application/json" is the content-type for bucket lists.
content_types_provided(RD, Ctx) ->
    {[{"application/json", produce_fold_results}], RD, Ctx}.


-spec encodings_provided(#wm_reqdata{}, context()) ->
    {[{Encoding::string(), Producer::function()}], #wm_reqdata{}, context()}.
%% @doc List the encodings available for representing this resource.
%%      "identity" and "gzip" are available for bucket lists.
encodings_provided(RD, Ctx) ->
    {riak_kv_wm_utils:default_encodings(), RD, Ctx}.


resource_exists(RD, #ctx{bucket_type=BType}=Ctx) ->
    {riak_kv_wm_utils:bucket_type_exists(BType), RD, Ctx}.

-spec produce_fold_results(#wm_reqdata{}, context()) ->
    {any(), #wm_reqdata{}, context()}.
%% @doc Produce the JSON response to an aae fold
produce_fold_results(RD, Ctx) ->
    Client = Ctx#ctx.client,
    Query = Ctx#ctx.query,
    case Client:aae_fold(Query) of
        {ok, Results} ->
            QueryName = element(1, Query),
            JsonResults = riak_kv_clusteraae_fsm:json_encode_results(QueryName, Results),
            {JsonResults, RD, Ctx};
        {error, timeout} ->
            {{halt, 503},
             wrq:set_resp_header("Content-Type", "text/plain",
                                 wrq:append_to_response_body(
                                   io_lib:format("request timed out~n",
                                                 []),
                                   RD)),
             Ctx};
        {error, Reason} ->
            {{error, Reason}, RD, Ctx}
    end.

-spec path_tokens(#wm_reqdata{}) -> list(string()).
path_tokens(RD) ->
    string:tokens(wrq:path(RD), "/").

-ifdef(TEST).
-ifdef(EQC).

%% run the eqc property as an eunit test
malformed_request_test_() ->
    {setup,
     fun setup_mocks/0,
     fun teardown_mocks/1,
     {timeout, 120, ?_assertEqual(true, eqc:quickcheck(eqc:testing_time(50, ?QC_OUT(prop_not_malformed()))))}
    }.

setup_mocks() ->
    meck:new(wrq).

teardown_mocks(_) ->
    (catch meck:unload(wrq)).

%% @private happy-path testing for the query types. NOTE: only happy
%% path
prop_not_malformed() ->
    ?FORALL({Query, Filter}, gen_query(),
            begin
                MockReqData = setup_reqdata(Query, Filter),
                ExpectedQuery = element(1, Query),
                QueryName = element(1, ExpectedQuery),
                Ctx = #ctx{bucket_type= <<"default">>},
                {Res, ResRD, ResCtx} = ?MODULE:malformed_request(MockReqData, Ctx),
                aggregate([QueryName],
                          conjunction([{non_malformed, equals(Res, false)},
                                       {query_equal, equals(ResCtx#ctx.query, ExpectedQuery)},
                                       {no_resp, equals([], ResRD)}]))
            end).

%%%
%% query generetors
%%%

%% Pick one of the query types supported by aaefold.  NOTE a `query'
%% here is two tuple. The first element is a 3-tuple of
%% expected-query, the tuple we expect to be passed to aae-fold. The
%% second is the path to return from the wrq mock, the 3rd the
%% path-info to return from the wrq mock. We return these from the
%% generator as they depend on generated parameters.  The second
%% element of the returned 2-tuple is a proplist that can be
%% mochijson2 encoded. This JSON is what the WM resource works on to
%% generate the query. It also depends on the generated params. The
%% property then only checks that given the params in JSON the
%% resource produces a query that matches the generated query.
gen_query() ->
    oneof([gen_cached(),
           gen_find_keys(),
           gen_object_stats(),
           gen_merge_tree_range(),
           gen_fetch_clocks_range()]).

%% generate a fetch_clocks_range query
gen_fetch_clocks_range() ->
    ?LET(Bucket, gen_bucket(), gen_fetch_clocks_range(Bucket)).

gen_fetch_clocks_range(Bucket) ->
    ?LET({KeyRange, DateRange, SegmentFilter},
         {gen_keyrange(),
          gen_daterange(),
          gen_segment_filter()},
         begin
             {BucketPath, BucketPathInfo} = bucket_path(Bucket),
             {{{fetch_clocks_range, Bucket,
                KeyRange, SegmentFilter, DateRange},
               ["rangetrees"] ++ BucketPath ++  ["keysclocks"],
               BucketPathInfo},
              gen_range_filter([
                                {?KEY_RANGE, KeyRange},
                                {?DATE_RANGE, DateRange},
                                {?SEG_FILT, SegmentFilter}
                               ])
             }
         end).

gen_merge_tree_range() ->
    ?LET(Bucket, gen_bucket(), gen_merge_tree_range(Bucket)).

gen_merge_tree_range(Bucket) ->
    ?LET({TreeSize, KeyRange, DateRange, SegmentFilter, HashMethod},
         {gen_treesize(),
          gen_keyrange(),
          gen_daterange(),
          gen_segment_filter(),
          gen_hash_method()},
         begin
             {BucketPath, BucketPathInfo} = bucket_path(Bucket),
             {{{merge_tree_range, Bucket,
                KeyRange, TreeSize, SegmentFilter, DateRange, HashMethod},
               ["rangetrees"] ++ BucketPath ++  ["trees", TreeSize],
               [{size, atom_to_list(TreeSize)}] ++ BucketPathInfo},
              gen_range_filter([
                                {?KEY_RANGE, KeyRange},
                                {?DATE_RANGE, DateRange},
                                {?SEG_FILT, SegmentFilter},
                                {?HASH_IV, HashMethod}
                               ])
             }
         end).

gen_object_stats() ->
    ?LET(Bucket, gen_bucket(), gen_object_stats(Bucket)).

gen_object_stats(Bucket) ->
    ?LET({KeyRange, DateRange},
         {gen_keyrange(),
          gen_daterange()},
         begin
             {BucketPath, BucketPathInfo} = bucket_path(Bucket),
             {{{object_stats, Bucket, KeyRange, DateRange},
               ["objectstats"] ++ BucketPath,
               BucketPathInfo
              },
              gen_range_filter([
                                {?KEY_RANGE, KeyRange},
                                {?DATE_RANGE, DateRange}
                               ])
             }
         end).

gen_find_keys() ->
    ?LET({CntOrSize, Bucket}, {choose(1, 100), gen_bucket()},
         oneof([gen_find_siblings(CntOrSize, Bucket),
                gen_find_objsize(CntOrSize, Bucket)])).

gen_find_objsize(Size, Bucket) ->
    gen_find_keys(Size, Bucket, object_size, "objectsizes", "sizes", size).

gen_find_siblings(Cnt, Bucket) ->
    gen_find_keys(Cnt, Bucket, sibling_count, "siblings", "counts", count).

gen_find_keys(CntOrSize, Bucket, QTag, PathPrefix, PathElem, PathInfoTag) ->
    CntOrSizeStr = integer_to_list(CntOrSize),
    ?LET({KeyRange, DateRange},
         {gen_keyrange(),
          gen_daterange()},
         begin
             {BucketPath, BucketPathInfo} = bucket_path(Bucket),
             {{{find_keys, Bucket, KeyRange, DateRange, {QTag, CntOrSize}},
               [PathPrefix] ++ BucketPath ++ [PathElem, CntOrSizeStr],
               [{PathInfoTag, CntOrSizeStr} | BucketPathInfo]
              },
              gen_range_filter([
                                {?KEY_RANGE, KeyRange},
                                {?DATE_RANGE, DateRange}
                               ])
             }
         end).

gen_cached() ->
    ?LET(NVal, choose(2, 5), gen_cached(NVal)).

gen_cached(NVal) ->
    oneof([gen_cached_root(NVal),
           gen_cached_branch(NVal),
           gen_cached_keysclocks(NVal)
          ]).

gen_cached_root(NVal) ->
    NValStr = integer_to_list(NVal),
    {
     {{merge_root_nval, NVal},
      ["cachedtrees", "nvals", NValStr, "root"], %% path tokens
      [{nval, NValStr}] %% path info mappings
     },
     undefined}.

gen_cached_branch(NVal) ->
    NValStr = integer_to_list(NVal),
    ?LET(Branches, gen_branch_filter(),
         {
          {{merge_branch_nval, NVal, Branches},
           ["cachedtrees", "nvals", NValStr, "branch"], %% path tokens
           [{nval, NValStr}] %% path info mappings
          },
          Branches}).

gen_cached_keysclocks(NVal) ->
    NValStr = integer_to_list(NVal),
    %% NOTE: branches and segments are just lists of ints
    ?LET(Segs, gen_seg_filter(),
         {
          {{fetch_clocks_nval, NVal, Segs},
           ["cachedtrees", "nvals", NValStr, "keysclocks"], %% path tokens
           [{nval, NValStr}] %% path info mappings
          },
         Segs}).

%%%
%% param generetors
%%%

gen_segment_filter() ->
    ?LET({Segments, TreeSize}, {gen_seg_filter(), gen_treesize()},
         oneof([{segments, Segments, TreeSize}, all])).

gen_treesize() ->
    oneof([xxsmall, xsmall, small, medium, large, xlarge]).

gen_hash_method() ->
    ?LET(IV, nat(),
         oneof([pre_hash, {rehash, IV}])).

gen_range_filter(Props) ->
    Filt = lists:foldl(fun gen_filter_element/2, [], Props),
    {struct, Filt}.

gen_filter_element({Range, all}, Acc) when Range == ?KEY_RANGE;
                                       Range == ?DATE_RANGE;
                                       Range == ?SEG_FILT ->
    Acc;
gen_filter_element({Range, {Start, End}}, Acc) when Range == ?KEY_RANGE;
                                                Range == ?DATE_RANGE ->
    [{Range, {struct, [{<<"start">>, Start},
                       {<<"end">>, End}]}} | Acc];
gen_filter_element({?SEG_FILT, {segments, Segs, TreeSize}}, Acc) ->
    [{?SEG_FILT, {struct, [{<<"tree_size">>, atom_to_list(TreeSize)},
                           {<<"segments">>, Segs}]}} | Acc];
gen_filter_element({?HASH_IV, pre_hash}, Acc) ->
    Acc;
gen_filter_element({?HASH_IV, {rehash, IV}}, Acc) when is_integer(IV) ->
    [{?HASH_IV, IV} | Acc].

bucket_path(Bucket) ->
    BuckStr = binary_to_list(Bucket),
    {["buckets", BuckStr],
     [{bucket, BuckStr}]}.

gen_bucket() ->
    oneof([<<"bucket1">>, <<"bucket2">>]).

gen_keyrange() ->
    oneof([all, {<<"key1">>, <<"key99">>}]).


gen_daterange() ->
    oneof([{1543357393, 1543417393},
           all]).

gen_seg_filter() ->
    non_empty(list(nat())).

gen_branch_filter() ->
    non_empty(list(nat())).

%%%%
%% helpers
%%%%

encode_filter(undefined) ->
    undefined;
encode_filter(all) ->
    undefined;
encode_filter({struct, []}) ->
    undefined;
encode_filter(Filter) ->
    base64:encode(list_to_binary(
                    lists:flatten(mochijson2:encode(Filter)
                                 ))).

to_string(Atom) when is_atom(Atom) ->
    atom_to_list(Atom);
to_string(Int) when is_integer(Int)  ->
    integer_to_list(Int);
to_string(Bin) when is_binary(Bin) ->
    binary_to_list(Bin);
to_string(Str) when is_list(Str) ->
    Str.

setup_reqdata({_Q, PathTokens0, PathInfo}, Filter) ->
    %% we're changing the expectations per-run
    (catch teardown_mocks(ok)),
    setup_mocks(),
    PathTokens = [to_string(PathToken) || PathToken <- PathTokens0],
    Path = "/" ++ string:join(PathTokens, "/"),
    meck:expect(wrq, path, fun(_) ->
                                          Path
                                  end),
    meck:expect(wrq, path_info, fun(Name, _) ->
                                        proplists:get_value(Name, PathInfo)
                                end),
    EncodedFilter = encode_filter(Filter),
    meck:expect(wrq, get_qs_value, fun(?Q_AAEFOLD_FILTER, undefined, _) ->
                                          EncodedFilter
                                   end),
    %% for riak_kv_web_utils
    meck:expect(wrq, get_req_header, fun("X-Riak-URL-Encoding", _RD) ->
                                             "off"
                                     end),
    %% for malformed, just return message
    meck:expect(wrq, set_resp_header, fun(Header, Val, RD) ->
                                              [{Header, Val} | RD]
                                      end),
    meck:expect(wrq, set_resp_body, fun(Body, RD) ->
                                            [{body, lists:flatten(Body)} | RD]
                                    end),
    %% the mock req data is a list to accumulate resp-headers
    [].

-endif.

-endif.
