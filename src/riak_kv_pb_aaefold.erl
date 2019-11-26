%% -------------------------------------------------------------------
%%
%% Copyright (c) Martin Sumner
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

%% @doc <p>The AAE Fold PB service for Riak KV. This covers the
%% following request messages:</p>
%%
%% <pre>
%%  210 - RpbAaeFoldMergeRootNValReq
%%  211 - RpbAaeFoldMergeBranchNValReq
%%  212 - RpbAaeFoldFetchClocksNValReq
%%  213 - RpbAaeFoldMergeTreesRangeReq
%%  214 - RpbAaeFoldFetchClocksRangeReq
%%  215 - RpbAaeFoldFindKeysReq
%%  216 - RpbAaeFoldObjectStatsReq
%% </pre>
%%
%% <p>This service produces the following responses:</p>
%%
%% <pre>
%%  220 - RpbAaeFoldTreeResp
%%  221 - RpbAaeFoldKeyClockResp
%%  222 - RpbAaeFoldKeyIntResp
%% </pre>
%% @end

-module(riak_kv_pb_aaefold).

-include_lib("riak_pb/include/riak_kv_pb.hrl").

-behaviour(riak_api_pb_service).

-export([init/0,
         decode/2,
         encode/1,
         process/2,
         process_stream/3]).

-record(state, {client :: riak_client:riak_client()}).

%% @doc init/0 callback. Returns the service internal start
%% state.
-spec init() -> any().
init() ->
    {ok, C} = riak:local_client(),
    #state{client=C}.

%% @doc decode/2 callback. Decodes an incoming message.
decode(Code, Bin) ->
    {ok, riak_pb_codec:decode(Code, Bin)}.

%% @doc encode/1 callback. Encodes an outgoing response message.
encode(Message) ->
    {ok, riak_pb_codec:encode(Message)}.


%% @doc process/2 callback. Handles an incoming request message.
process(#rpbaaefoldmergerootnvalreq{n_val = N}, State) ->
    Query  = {merge_root_nval, N},
    process_query(Query, State);
process(#rpbaaefoldmergebranchnvalreq{n_val = N, id_filter = BF}, State) ->
    Query = {merge_branch_nval, N, BF},
    process_query(Query, State);
process(#rpbaaefoldfetchclocksnvalreq{n_val = N, id_filter = SF}, State) ->
    Query = {fetch_clocks_nval, N, SF},
    process_query(Query, State);
process(#rpbaaefoldmergetreesrangereq{type = T,
                                        bucket = B, 
                                        key_range = IsKR,
                                        start_key = SK,
                                        end_key = EK,
                                        tree_size = TS,
                                        segment_filter = IsSF,
                                        id_filter = SFL,
                                        filter_tree_size = FTS,
                                        modified_range = IsModR,
                                        last_mod_start = LMS,
                                        last_mod_end = LME,
                                        use_prehash = IsPH, 
                                        init_vector = IV}, State) ->
    KR = case IsKR of true -> {SK, EK}; false -> all end,
    SF = case IsSF of true -> {segments, SFL, FTS}; false -> all end,
    MR = case IsModR of true -> {date, LMS, LME}; false -> all end,
    HM = case IsPH of true -> pre_hash; false -> {rehash, IV} end,
    Query = {merge_tree_range, maybe_bucket_type(T, B), KR, TS, SF, MR, HM},
    process_query(Query, State);
process(#rpbaaefoldfetchclocksrangereq{type = T,
                                        bucket = B, 
                                        key_range = IsKR,
                                        start_key = SK,
                                        end_key = EK,
                                        segment_filter = IsSF,
                                        id_filter = SFL,
                                        filter_tree_size = FTS,
                                        modified_range = IsModR,
                                        last_mod_start = LMS,
                                        last_mod_end = LME}, State) ->
    KR = case IsKR of true -> {SK, EK}; false -> all end,
    SF = case IsSF of true -> {segments, SFL, FTS}; false -> all end,
    MR = case IsModR of true -> {date, LMS, LME}; false -> all end,
    Query = {fetch_clocks_range, maybe_bucket_type(T, B), KR, SF, MR},
    process_query(Query, State);
process(#rpbaaefoldreplkeysreq{type = T,
                                bucket = B, 
                                key_range = IsKR,
                                start_key = SK,
                                end_key = EK,
                                modified_range = IsModR,
                                last_mod_start = LMS,
                                last_mod_end = LME,
                                queuename = QN}, State) ->
    KR = case IsKR of true -> {SK, EK}; false -> all end,
    MR = case IsModR of true -> {date, LMS, LME}; false -> all end,
    QueueName = binary_to_atom(QN, utf8),
    Query = {repl_keys_range, maybe_bucket_type(T, B), KR, MR, QueueName},
    process_query(Query, State);
process(#rpbaaefoldfindkeysreq{type = T,
                                bucket = B, 
                                key_range = IsKR,
                                start_key = SK,
                                end_key = EK,
                                modified_range = IsModR,
                                last_mod_start = LMS,
                                last_mod_end = LME,
                                finder = FT,
                                find_limit = FL}, State) ->
    KR = case IsKR of true -> {SK, EK}; false -> all end,
    MR = case IsModR of true -> {date, LMS, LME}; false -> all end,
    Query = {find_keys, maybe_bucket_type(T, B), KR, MR, {FT, FL}},
    process_query(Query, State);
process(#rpbaaefoldobjectstatsreq{type = T,
                                    bucket = B, 
                                    key_range = IsKR,
                                    start_key = SK,
                                    end_key = EK,
                                    modified_range = IsModR,
                                    last_mod_start = LMS,
                                    last_mod_end = LME}, State) ->
                                    KR = case IsKR of true -> {SK, EK}; false -> all end,
    MR = case IsModR of true -> {date, LMS, LME}; false -> all end,
    Query = {object_stats, maybe_bucket_type(T, B), KR, MR},
    process_query(Query, State).



process_query(Query, State) ->
    case riak_client:aae_fold(Query, State#state.client) of
        {ok, Results} ->
            QT = element(1, Query),
            Response =
                riak_kv_clusteraae_fsm:pb_encode_results(QT, Query, Results),
            {reply, Response, State};
        {error, Reason} ->
            {error, {format, Reason}, State}
    end.

process_stream(_,_,State) ->
    {ignore, State}.

%% Construct a {Type, Bucket} tuple, if not working with the default bucket
maybe_bucket_type(undefined, B) ->
    B;
maybe_bucket_type(<<"default">>, B) ->
    B;
maybe_bucket_type(T, B) ->
    {T, B}.
