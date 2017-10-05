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

-export([valid_options/0,
            generate_queryoptions/1,
            generate_acc/1,
            generate_objectfold/1,
            generate_mergefun/1,
            state_needs/1,
            encode_results/2
            ]).

-define(NEEDS, [async_fold, snap_prefold, fold_heads]).
-define(DEFAULT_TREESIZE, small).
-define(DEFAULT_CHECKPRESENCE, false).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.


state_needs(_Opts) ->
    ?NEEDS.

valid_options() ->
    [{tree_size, 
        % The size of the TicTac tree to be used in the fold
        % Larger tictac trees will require more network I/O, but lead to
        % stricter segment list queries
                fun list_to_atom/1, 
                fun leveled_tictac:valid_size/1, 
                small},
        {check_presence, 
            % Should a presence check be performed in the fold when the 
            % backend supports fold_heads. Presence checks can have a 
            % significant impact on fold performance
                fun list_to_atom/1, 
                fun is_boolean/1, 
                false},
        {exportable, 
            % Should the resultant TicTac tree use a hash algorithm which is 
            % replicable in another system (i.e. uses a hash algorithm which 
            % is not Erlang specific)
                fun list_to_atom/1,
                fun is_boolean/1,
                false}].

generate_queryoptions(Opts) ->
    [lists:keyfind(check_presence, 1, Opts)].

generate_acc(Opts) ->
    {tree_size, TreeSize} = lists:keyfind(tree_size, 1, Opts),
    {0, leveled_tictac:new_tree(tictac_folder, TreeSize)}.

generate_objectfold(Opts) ->
    {exportable, Exportable} = lists:keyfind(exportable, 1, Opts),
    fun(_B, K, PO, {Count, TreeAcc}) ->
        ExtractFun = 
            fun(Key, Obj) ->
                {VC, _Sz, _SC} = riak_object:summary_from_binary(Obj),
                {Key, lists:sort(VC)}
            end,
        {Count + 1, 
            leveled_tictac:add_kv(TreeAcc, K, PO, ExtractFun, Exportable)}
    end.

generate_mergefun(_Opts) ->
    fun({C0, T0},{C1, T1}) ->
        {C0 + C1, leveled_tictac:merge_trees(T0,T1)}
    end.

encode_results({Count, Tree}, http) ->
    ExportedTree = leveled_tictac:export_tree(Tree),
    JsonKeys1 = {struct, [{<<"tree">>, ExportedTree},
                            {<<"count">>, integer_to_list(Count)}]},
    mochijson2:encode(JsonKeys1).


%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

json_encode_tictac_empty_test() ->
    Tree = leveled_tictac:new_tree(tictac_folder_test, xlarge),
    JsonTree = encode_results({0, Tree}, http),
    {struct, [{<<"tree">>, ExportedTree}, {<<"count">>, "0"}]} 
        = mochijson2:decode(JsonTree),
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
    JsonTree = encode_results({0, Tree0}, http),
    {struct, [{<<"tree">>, ExportedTree}, {<<"count">>, "0"}]} 
        = mochijson2:decode(JsonTree),
    ReverseTree = leveled_tictac:import_tree(ExportedTree),
    ?assertMatch([], leveled_tictac:find_dirtyleaves(Tree0, ReverseTree)).

generate_optionless_acc_test() ->
    Empty = <<0:8192/integer>>,
    {InitCount, InitTree} = generate_acc([{tree_size, small}]),
    ?assertMatch(Empty, leveled_tictac:fetch_root(InitTree)),
    ?assertMatch(0, InitCount).

-endif.
