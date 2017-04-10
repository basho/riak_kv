%% -------------------------------------------------------------------
%%
%% Copyright (c) 2017 Basho Technologies, Inc.  All Rights Reserved.
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

-module(riak_kv_group_list_response).

-export([new_response/2,
         new_response/3,
         get_metadatas/1,
         get_common_prefixes/1,
         is_truncated/1,
         get_next_continuation_token/1
        ]).

-export_type([response/0]).

-record(response, {
          metadatas :: list(),
          common_prefixes :: list(binary()),
          is_truncated :: boolean(),
          next_continuation_token :: riak_kv_continuation:token()
         }).

-opaque response() :: #response{}.

-spec new_response(Metadatas::list(), CommonPrefixes::list(binary())) ->
    response().
new_response(Metadatas, CommonPrefixes) ->
    new_response(Metadatas, CommonPrefixes, undefined).

-spec new_response(Metadatas::list(),
                   CommonPrefixes::list(binary()),
                   NextContinuationToken::riak_kv_continuation:token()) ->
    response().
new_response(Metadatas, CommonPrefixes, NextContinuationToken) ->
    #response {
       metadatas = Metadatas,
       common_prefixes = CommonPrefixes,
       next_continuation_token = NextContinuationToken,
       is_truncated = NextContinuationToken /= undefined
      }.

-spec get_metadatas(response()) -> list().
get_metadatas(#response{metadatas = Metadatas}) -> Metadatas.

-spec get_common_prefixes(response()) -> list().
get_common_prefixes(#response{common_prefixes = CommonPrefixes}) -> CommonPrefixes.

-spec get_next_continuation_token(response()) -> riak_kv_continuation:token().
get_next_continuation_token(#response{next_continuation_token = NextContinuationToken}) ->
    NextContinuationToken.

-spec is_truncated(response()) -> boolean().
is_truncated(#response{is_truncated = IsTruncated}) -> IsTruncated.
