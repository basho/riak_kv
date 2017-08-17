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
            generate_objectfold/1,
            generate_mergefun/1,
            state_needs/1,
            encode_results/2
            ]).

-define(NEEDS, [async_fold, snap_prefold]).


state_needs(_Opts) ->
    ?NEEDS.

generate_filter(_FilterList) ->
    none.

generate_acc(Opts) ->
    TreeSize = proplists:get_value(tree_size, Opts, small),
    leveled_tictac:new_tree(tictac_folder, TreeSize).

generate_objectfold(_Opts) ->
    fun(_B, K, PO, Acc) ->
        HashFun = 
            fun(_Key, Obj) ->
                riak_object:hash(Obj, 0)
            end,
        leveled_tictac:add_kv(Acc, K, PO, HashFun)
    end.

generate_mergefun(_Opts) ->
    fun leveled_tictac:merge_trees/2.

encode_results(Tree, http) ->
    ExportedTree = leveled_tictac:export_tree(Tree),
    JsonKeys1 = {struct, [tree, ExportedTree]},
    mochijson2:encode(JsonKeys1).