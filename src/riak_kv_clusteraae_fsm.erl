%% -------------------------------------------------------------------
%%
%% riak_clusteraae_fsm: Manage folds over tictacaae controllers
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

%% @doc The AAE fold FSM allows for coverage folds acrosss Tictac AAE 
%% Controllers

-module(riak_kv_clusteraae_fsm).

-behaviour(riak_core_coverage_fsm).

-include_lib("riak_kv_vnode.hrl").

-export([init/2,
         process_results/2,
         finish/2]).

-export([json_encode_results/2]).

-define(TREE_SIZE, 4096).
    % TODO: Need to sort what to do about changing the tree size.  This needs
    % to align with the default tree size (large) defined in 
    % kv_index_tictcatree.  
    % There is some stuff done as part of previous work to manipulate segment
    % lists so that dirty segments that have outputted from one tree size can
    % be still found with a different tree size
-define(EMPTY, <<>>).
-define(MAX_SEGMENT_FILTER, 256).
-define(MAX_BRANCH_FILTER,, 256).

-define(NVAL_QUERIES, 
            [merge_root_nval, merge_branch_nval, fetch_clocks_nval]).
-define(RANGE_QUERIES, 
            [merge_tree_range, fetch_clocks_range, find_keys, object_stats]).
-define(LIST_ACCUMULATE_QUERIES,
            [fetch_clocks_nval, fetch_clocks_range, find_keys]).


-type from() :: {atom(), req_id(), pid()}.
-type req_id() :: non_neg_integer().

% Building blocks for supported aae fold query definitions
-type segment_filter() :: list(integer()).
    % TODO:
    % Segment filter is based on a large tree_size - so if a smaller tree size
    % is used, then the segment list must be expanded out
-type branch_filter() :: list(integer()).
-type key_range() :: {riak_object:key(), riak_object:key()}|all.
-type bucket() :: riak_object:bucket().
-type query_types() :: 
    merge_root_nval|merge_branch_nval|fetch_clocks_nval|
    merge_tree_range|fetch_clocks_range|find_keys|object_stats.
-type query_definition() ::
    % Use of these folds depends on the Tictac AAE being enabled in either
    % native mode, or in parallel mode with key_order being used.  

    % N-val AAE (using cached trees)
    {merge_root_nval, integer()}|
        % Merge the roots of cached Tictac trees for the given n-val to give
        % a single root for the cluster.  This should be a fast, low-overhead
        % operation
    {merge_branch_nval, integer(), branch_filter()}|
        % Merge a selection of branches of cached Tictac trees for the given
        % n-val to give a combined view of those branches across the cluster.
        % This should be a fast, low-overhead operation
    {fetch_clocks_nval, integer(), segment_filter()}|
        % Scan over all the keys for a given n_val in the tictac AAE key store
        % (which for native stores will be the actual key store), skipping 
        % those blocks of the store not containing keys in the segment filter,
        % returning a list of keys and clocks for that n_val within the
        % cluster.  This is a background operation, but will have lower 
        % overheads than traditional store folds, subject to the size of the
        % segment filter being small - ideally o(10) or smaller
    
    % Range-based AAE (requiring folds over native/parallel AAE key stores)
    {merge_tree_range, bucket(), key_range(), small|medium|large}|
        % Provide the values for a subset of AAE tree branches for the given
        % key range.  This will be a background operation, and the cost of
        % the operation will be in-proportion to the number of keys in the
        % range.
        %
        % `all` may be used as an alternative to a branch_filter to return
        % all branches. Unlike consulting the cached trees with the queries
        % merge_root_nval|merge_branch_nval, the size of the accumulator tends
        % not to be relevant to the order of magnitude of the cost of the
        % query, and so there is not generally a benefit in asking (and
        % re-asking) for subsets of the tree rather than the whole tree
        %
        % Result is a list of tuples [{branchID, binary()}], where the binary
        % is a binary of concatenated hashes for all the segments in the
        % branch.
    {fetch_clocks_range, bucket(), key_range(), segment_filter()}|
        % Return the keys and clocks for a given list of segment IDs.  The
        % cost of the operation will be increased as both the size of the
        % segment_filter and the number of keys in the range increase.
        % The function returns a list of [{Key, Clock}] tuples

    % Operational support functions
    {find_keys, bucket(), key_range(), 
        {sibling_count, pos_integer()}|{object_size, pos_integer()}}|
        % Find all the objects in the key range that have more than the given 
        % count of siblings, or are bigger than the given object size.  This 
        % uses the AAE keystore, and will only discover siblings that have been 
        % generated and stored within a vnode (which should eventually be all 
        % siblings given AAE is enabled and if allow_mult is true). If finding
        % keys by size, then the size is the pre-calculated size stored in the
        % aae key store as metadata.
        %
        % The query returns a list of [{Key, SiblingCount}] tuples or 
        % [{Key, ObjectSize}] tuples depending on the filter requested.  The 
        % cost of this operation will increase with the size of the range
    {object_stats, bucket(), key_range()}.
        % Returns:
        % - the total count of objects in the key range
        % - the accumulated total size of all objects in the range
        % - a list [{Magnitude, ObjectCount}] tuples where Magnitude represents
        % the order of magnitude of the size of the object (e.g. 1KB is objects 
        % from 100 bytes to 1KB, 10KB is objects from 1KB to 10KB etc)
        % - a list of [{SiblingCount, ObjectCount}] tuples where Sibling Count
        % is the number of siblings the object has.
        % - sample portion - (n_val * sample_size) / ring_size
        % e.g.
        % [{total_count, 1000}, 
        %   {total_size, 1000000}, 
        %   {sizes, [{1000, 800}, {10000, 180}, {100000, 20}]}, 
        %   {siblings, [{1, 1000}]}]
-type inbound_api() :: list(query_definition()|integer()).

-export_type([query_definition/0]).

-record(state, {from :: from(),
                acc,
                query_type :: query_types(),
                start_time :: tuple()}).


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-spec init(from(), inbound_api()) -> tuple().
%% @doc 
%% Return a tuple containing the ModFun to call per vnode, the number of 
%% primary preflist vnodes the operation should cover, the service to use to 
%% check for available nodes,and the registered name to use to access the 
%% vnode master process.
init(From={_, _, _}, [Query, Timeout]) ->
    % Get the bucket n_val for use in creating a coverage plan
    QueryType = element(1, Query),
    NVal = 
        case {lists:member(QueryType, ?NVAL_QUERIES), 
                lists:member(QueryType, ?RANGE_QUERIES)} of
            {true, false} ->
                element(2, Query);
            {false, true} ->
                BucketProps = riak_core_bucket:get_bucket(element(2, Query)),
                proplists:get_value(n_val, BucketProps)
        end,
    
    true = safe_query(Query),

    InitAcc =
        case lists:member(QueryType, ?LIST_ACCUMULATE_QUERIES) of
            true ->
                [];
            false ->
                case QueryType of
                    merge_root_nval ->
                        ?EMPTY;
                    merge_branch_nval ->
                        lists:map(fun(X) -> {X, ?EMPTY} end, 
                                    element(3, Query));
                    merge_tree_range ->
                        TreeSize = element(4, Query),
                        leveled_tictac:new_tree(range_tree, TreeSize);
                    object_stats ->
                        [{total_count, 0}, 
                            {total_size, 0},
                            {sizes, []},
                            {siblings, []}]
                end
        end,
    
    Req = ?KV_AAEFOLD_REQ{qry = Query, init_acc = InitAcc, n_val = NVal}, 

    State = #state{from = From, 
                    acc = InitAcc, 
                    start_time = os:timestamp(),
                    query_type = QueryType},
    lager:info("AAE fold prompted of type=~w", [QueryType]),
    {Req, all, NVal, 1, 
        riak_kv, riak_kv_vnode_master, 
        Timeout, 
        State}.
        

process_results({error, Reason}, _State) ->
    lager:warning("Failure to process fold results due to ~w", [Reason]),
    {error, Reason};
process_results(Results, State) ->
    % Results are received as a one-off for each vnode in this case, and so 
    % once results are merged work is always done.
    Acc = State#state.acc,
    QueryType = State#state.query_type,
    UpdAcc = 
        case lists:member(QueryType, ?LIST_ACCUMULATE_QUERIES) of
            true ->
                lists:umerge(Acc, lists:reverse(Results));
            false ->
                case QueryType of
                    merge_root_nval ->
                        aae_exchange:merge_root(Results, Acc);
                    merge_branch_nval ->
                        aae_exchange:merge_branches(Results, Acc);
                    merge_tree_range ->
                        leveled_tictac:merge_trees(Results, Acc);
                    object_stats ->
                        [{total_count, R_TC}, 
                            {total_size, R_TS},
                            {sizes, R_SzL},
                            {siblings, R_SbL}] = Results,
                        [{total_count, A_TC}, 
                            {total_size, A_TS},
                            {sizes, A_SzL},
                            {siblings, A_SbL}] = Acc,
                        [{total_count, R_TC + A_TC}, 
                            {total_size, R_TS + A_TS},
                            {sizes, merge_countinlists(A_SzL, R_SzL)},
                            {siblings, merge_countinlists(A_SbL, R_SbL)}]
                end
        end,

    {done, State#state{acc = UpdAcc}}.

%% Once the coverage FSM has received done for all vnodes (as an output from
%% process_results), then it will call finish(clean, State) and so the results
%% can be sent to the client, and the FSM can be stopped. 
finish({error, Error}, State=#state{from={raw, ReqId, ClientPid}}) ->
    % Notify the requesting client that an error
    % occurred or the timeout has elapsed.
    lager:warning("Failure to finish process fold due to ~w", [Error]),
    ClientPid ! {ReqId, {error, Error}},
    {stop, normal, State};
finish(clean, State=#state{from={raw, ReqId, ClientPid}}) ->
    % The client doesn't expect results in increments only the final result, 
    % so no need for a seperate send of a 'done' message
    QueryDuration = timer:now_diff(os:timestamp(), State#state.start_time),
    lager:info("Finished aaefold of type=~w with fold_time=~w seconds", 
                [State#state.query_type, QueryDuration/1000000]),
    ClientPid ! {ReqId, {results, State#state.acc}},
    {stop, normal, State}.


%% ===================================================================
%% External functions
%% ===================================================================

-spec json_encode_results(query_types(), any()) -> iolist().
%% @doc
%% Encode the results of a query in JSON
%% Expected this will be called from the webmachine module that needs to
%% generate the response
json_encode_results(merge_tree_range, Tree) ->
    ExportedTree = leveled_tictac:export_tree(Tree),
    JsonKeys1 = {struct, [{<<"tree">>, ExportedTree}]},
    mochijson2:encode(JsonKeys1).

%% ===================================================================
%% Internal functions
%% ===================================================================

-spec merge_countinlists(list({integer(), integer()}), 
                            list({integer(), integer()})) 
                                            -> list({integer(), integer()}).
%% @doc
%% Take two lists with {IntegerId, Count} tuples and return a list where the
%% counts have been summed across the lists - even where one list is missing
%% an integer id
merge_countinlists(ResultList, AccList) ->
    MapFun =
        fun({Idx, AccCount}) ->
            case lists:keyfind(Idx, 1, ResultList) of
                false ->
                    {Idx, AccCount};
                {Idx, VnodeCount} ->
                    {Idx, AccCount + VnodeCount}
            end
        end,
    AccList0 = lists:map(MapFun, AccList),
    lists:ukeymerge(1, 
                    lists:ukeysort(1, AccList0),
                    lists:ukeysort(1, ResultList)).


-spec safe_query(query_definition()) -> boolean().
%% @doc
%% Some queries may have a significant impact on the cluster.  In particular
%% asking for too mnay branch IDs or segment IDs.
safe_query({fetch_clocks_range, _B, _KR, SegList}) 
                                when length(SegList) > ?MAX_SEGMENT_FILTER ->
    false;
safe_query({fetch_clocks_nval, _N, SegList}) 
                                when length(SegList) > ?MAX_SEGMENT_FILTER ->
    false;
safe_query(_Query) ->
    true.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

merge_countinlists_test() ->
    L0 = [{1, 23}, {4, 36}, {3, 17}, {8, 12}],
    L1 = [{7, 11}, {8, 15}, {1, 15}],
    Merged = [{1, 38}, {3, 17}, {4, 36}, {7, 11}, {8,27}],
    ?assertMatch(Merged, merge_countinlists(L0, L1)),
    ?assertMatch(Merged, merge_countinlists(L1, L0)).

json_encode_tictac_empty_test() ->
    Tree = leveled_tictac:new_tree(tictac_folder_test, large),
    JsonTree = json_encode_results(merge_tree_range, Tree),
    {struct, [{<<"tree">>, ExportedTree}]} = mochijson2:decode(JsonTree),
    ReverseTree = leveled_tictac:import_tree(ExportedTree),
    ?assertMatch([], leveled_tictac:find_dirtyleaves(Tree, ReverseTree)).

json_encode_tictac_withentries_test() ->
    encode_results_ofsize(small),
    encode_results_ofsize(large).

encode_results_ofsize(TreeSize) ->
    Tree = leveled_tictac:new_tree(tictac_folder_test, TreeSize),
    ExtractFun = fun(K, V) -> {K, V} end,
    FoldFun = 
        fun({Key, Value}, AccTree) ->
            leveled_tictac:add_kv(AccTree, Key, Value, ExtractFun)
        end,
    KVList = [{<<"key1">>, <<"value1">>}, 
                {<<"key2">>, <<"value2">>}, 
                {<<"key3">>, <<"value3">>}],
    Tree0 = lists:foldl(FoldFun, Tree, KVList),
    JsonTree = json_encode_results(merge_tree_range, Tree0),
    {struct, [{<<"tree">>, ExportedTree}]} = mochijson2:decode(JsonTree),
    ReverseTree = leveled_tictac:import_tree(ExportedTree),
    ?assertMatch([], leveled_tictac:find_dirtyleaves(Tree0, ReverseTree)).

-endif.

