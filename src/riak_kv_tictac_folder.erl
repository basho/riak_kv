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
%%      Module for computing a tictac aae tree within a fold


-module(riak_kv_tictac_folder).

-export([generate_filter/1,
            generate_acc/1,
            generate_objectfold/2,
            generate_mergefun/1,
            state_needs/1,
            encode_results/2
            ]).

-define(NEEDS, [async_fold, snap_prefold, fold_heads]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.


state_needs(_Opts) ->
    ?NEEDS.

generate_filter(_Opts) ->
    none.

generate_acc(Opts) ->
    TreeSize = 
        list_to_atom(
            binary_to_list(
                proplists:get_value(tree_size, Opts, "small"))),
    leveled_tictac:new_tree(tictac_folder, TreeSize).

generate_objectfold(_Opts, none) ->
    fun(B, K, PO, Acc) ->
        ExtractFun = 
            fun(Key, Obj) ->
                RiakObj = riak_object:from_binary(B, Key, Obj),
                {Key, lists:sort(riak_object:vclock(RiakObj))}
            end,
        leveled_tictac:add_kv(Acc, K, PO, ExtractFun, false)
    end.

generate_mergefun(_Opts) ->
    fun leveled_tictac:merge_trees/2.

encode_results(Tree, http) ->
    ExportedTree = leveled_tictac:export_tree(Tree),
    JsonKeys1 = {struct, [{<<"tree">>, ExportedTree}]},
    mochijson2:encode(JsonKeys1).

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

json_encode_tictac_empty_test() ->
    Tree = leveled_tictac:new_tree(tictac_folder_test, xlarge),
    JsonTree = encode_results(Tree, http),
    {struct, [{<<"tree">>, ExportedTree}]} = mochijson2:decode(JsonTree),
    ReverseTree = leveled_tictac:import_tree(ExportedTree),
    ?assertMatch([], leveled_tictac:find_dirtyleaves(Tree, ReverseTree)).

json_encode_tictac_withentries_test() ->
    Tree = leveled_tictac:new_tree(tictac_folder_test, xxsmall),
    ExtractFun = fun(K, V) -> {K, V} end,
    FoldFun = 
        fun({Key, Value}, AccTree) ->
            leveled_tictac:add_kv(AccTree, Key, Value, ExtractFun, false)
        end,
    KVList = [{<<"key1">>, <<"value1">>}, 
                {<<"key2">>, <<"value2">>}, 
                {<<"key3">>, <<"value3">>}],
    Tree0 = lists:foldl(FoldFun, Tree, KVList),
    JsonTree = encode_results(Tree0, http),
    {struct, [{<<"tree">>, ExportedTree}]} = mochijson2:decode(JsonTree),
    ReverseTree = leveled_tictac:import_tree(ExportedTree),
    ?assertMatch([], leveled_tictac:find_dirtyleaves(Tree0, ReverseTree)).

-endif.
