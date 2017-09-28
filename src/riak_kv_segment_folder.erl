%% -------------------------------------------------------------------
%%
%% riak_kv_tictacfolder: Module to be used in AAE MapFold.
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

%% @doc MapFold Module for TicTacTree AAE
%%
%%      Module for returning a list of keys and hashes for a passed in list 
%%      of segments.  The list of segments should be pipe-delimited integers
%%      and the tree size must be specified correctly


-module(riak_kv_segment_folder).

-export([generate_filter/1,
            generate_acc/1,
            generate_objectfold/2,
            generate_mergefun/1,
            state_needs/1,
            encode_results/2
            ]).

-define(NEEDS, [async_fold, snap_prefold, fold_heads]).

-define(DELIM_TOKEN, "|").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.


state_needs(_Opts) ->
    ?NEEDS.

generate_filter(Opts) ->
    TreeSize = list_to_atom(proplists:get_value(tree_size, Opts, "small")),
    ConcatenatedSegmentList = proplists:get_value(segment_list, Opts, ""),
    SegmentList = lists:map(fun list_to_integer/1, 
                            string:tokens(ConcatenatedSegmentList, 
                                            ?DELIM_TOKEN)),
    fun(K) ->
        lists:member(
            leveled_tictac:get_segment(erlang:phash2(K), TreeSize), 
            SegmentList)
    end.

generate_acc(_Opts) ->
    [].

generate_objectfold(_Opts, FilterFun) ->
    fun(_B, K, PO, Acc) ->
        case FilterFun(K) of 
            true ->
                [{K, riak_object:hash(PO, 0)}|Acc];
            false ->
                Acc
        end
    end.

generate_mergefun(_Opts) ->
    fun lists:append/2.

encode_results(KeyHashList, http) ->
    mochijson2:encode({struct, [{<<"deltas">>, KeyHashList}]}).

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

segfilter_test() ->
    % Simple test to check FilterFun

    K1 = "Key1",
    K2 = "Key2",
    K3 = "Key3",
    S1 = leveled_tictac:get_segment(erlang:phash2(K1), small),
    S2 = leveled_tictac:get_segment(erlang:phash2(K2), small),
    S3 = leveled_tictac:get_segment(erlang:phash2(K3), small),
    ?assertMatch(false, S3 == S2),
    ?assertMatch(false, S3 == S1),

    SegList = integer_to_list(S1) ++ "|" ++ integer_to_list(S2),
    Opts = [{tree_size, "small"}, {segment_list, SegList}],
    Filter = generate_filter(Opts),

    ?assertMatch(true, Filter(K1)),
    ?assertMatch(true, Filter(K2)),
    ?assertMatch(false, Filter(K3)).

checkjsonreversal_test() ->
    % Check what goes into Json comes back out the same way
    % A trivial test - but proves no mysterious structs appearing
    KeyHashList =  [{<<"Key1">>, erlang:phash2("{a,1}")}, 
                    {<<"Key2">>, erlang:phash2("{a,2}")}],
    Json = encode_results(KeyHashList, http),
    {struct, [{<<"deltas">>, Reverse}]} = mochijson2:decode(Json),
    io:format("Reverse ~w~n", [Reverse]),
    ?assertMatch({struct, KeyHashList}, Reverse).


-endif.
