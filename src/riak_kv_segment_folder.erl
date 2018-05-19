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

-export([valid_options/0,
            generate_queryoptions/1,
            generate_acc/1,
            generate_objectfold/1,
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

valid_options() ->
    [{tree_size, 
            % The size of the TicTac tree used when coming up with the 
            % segment list
                fun list_to_atom/1, 
                fun leveled_tictac:valid_size/1, 
                small},
        {return_clocks,
            % Should {keys, clocks} be returned not just keys
                fun list_to_atom/1,
                fun is_boolean/1,
                false},
        {check_presence,
            % Should the value store be checked (where the backend has a 
            % value store)
                fun list_to_atom/1,
                fun is_boolean/1,
                false},
        {segment_accelerate,
            % Should the query be accelerated by the backend filtering on
            % the segment list
                fun list_to_atom/1,
                fun is_boolean/1,
                false},
        {segment_list,
            % A concatenated list of segment integers stored as a string
                fun(ConcatenatedSegmentList) ->
                    SplitList = 
                        string:tokens(ConcatenatedSegmentList, ?DELIM_TOKEN),
                    lists:map(fun list_to_integer/1, SplitList)
                end,
                fun is_list/1,
                []}].

generate_queryoptions(Opts) ->
    Opts0 =
        case proplists:get_bool(segment_accelerate, Opts) of
            true ->
                [lists:keyfind(segment_list, 1, Opts)];
            false ->
                []
        end,
    Opts1 = 
        [lists:keyfind(check_presence, 1, Opts)],
    Opts0 ++ Opts1.

generate_acc(_Opts) ->
    [].

generate_objectfold(Opts) ->
    FilterFun = generate_filter(Opts),
    {return_clocks, ReturnClocks} = lists:keyfind(return_clocks, 1, Opts),
    fun(B, K, PO, Acc) ->
        case {FilterFun(B, K), ReturnClocks} of 
            {true, true} ->
                % Get the clock to return
                {VC, _Sz, _SC} = riak_object:summary_from_binary(PO),
                [{B, K, VC}|Acc];
            {true, false} ->
                [K|Acc];
            {false, _} ->
                Acc
        end
    end.

generate_mergefun(_Opts) ->
    fun lists:append/2.

encode_results(KeyList, http) ->
    mochijson2:encode({struct, [{<<"deltas">>, KeyList}]}).

%% ===================================================================
%% Internal Functions
%% ===================================================================

generate_filter(Opts) ->
    {tree_size, TreeSize} = lists:keyfind(tree_size, 1, Opts),
    {segment_list, SegmentList} = lists:keyfind(segment_list, 1, Opts),
    fun(B, K) ->
        HashK = leveled_tictac:keyto_segment32(<<B/binary, K/binary>>),
        lists:member(
            leveled_tictac:get_segment(HashK, TreeSize), 
            SegmentList)
    end.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

segfilter_test() ->
    % Simple test to check FilterFun
    B = <<"Bucket">>,
    K1 = <<"Key1">>,
    K2 = <<"Key2">>,
    K3 = <<"Key3">>,
    HashFun = 
        fun(K) -> leveled_tictac:keyto_segment32(<<B/binary, K/binary>>) end,
    S1 = leveled_tictac:get_segment(HashFun(K1), small),
    S2 = leveled_tictac:get_segment(HashFun(K2), small),
    S3 = leveled_tictac:get_segment(HashFun(K3), small),
    ?assertMatch(false, S3 == S2),
    ?assertMatch(false, S3 == S1),

    SegList = 
        list_to_binary(integer_to_list(S1) ++ "|" ++ integer_to_list(S2)),
    TreeSize = <<"small">>,
    Opts = 
        riak_kv_mapfold_fsm:generate_options(valid_options(), 
                                                [{segment_list, SegList}, 
                                                {tree_size, TreeSize}]),

    Filter = generate_filter(Opts),
    

    ?assertMatch(true, Filter(B, K1)),
    ?assertMatch(true, Filter(B, K2)),
    ?assertMatch(false, Filter(B, K3)).

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