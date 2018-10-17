%% -------------------------------------------------------------------
%%
%% riak_aaefold_fsm: Manage folds over tictacaae controllers
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
%%
%% There are multiple types of AAE folds
%%



-module(riak_kv_aaefold_fsm).

-behaviour(riak_core_coverage_fsm).

-include_lib("riak_kv_vnode.hrl").

-export([init/2,
         plan/2,
         process_results/2,
         finish/2,
         decode_options/1]).

-ifdef(TEST).
-export([generate_options/2]).
-endif.

-define(UNDEFINED_INPUT, <<"undefined">>).
-define(NVAL_QUERIES, [merge_root_nval, merge_branch_nval, fetch_clocks_nval]).
-define(RANGE_QUERIES, [merge_branch_range, fetch_clocks_range,
                        find_siblings, object_stats]).

-type from() :: {atom(), req_id(), pid()}.
-type req_id() :: non_neg_integer().

% Building blocks for supported aae fold query definitions
-type segment_filter() :: list(integer()).
-type branch_filter() :: list(integer()).
-type key_range() :: {riak_object:key(), riak_object:key()}|all.
-type sample_size() :: integer()|all.
-type excess_size_limit() :: integer()|infinity.
-type bucket() :: riak_object:bucket().
-type query_definition() ::
    % Use of these folds depends on the Tictac AAE being enabled in either
    % native mode, or in parallel mode with key_order bieng used.  

    % N-val AAE (using cached trees)
    {merge_root_nval, integer()}|
        % Merge the roots of cached Tictac trees for the given n-val to give
        % a single root for the cluster.  This should be a fast, low-overhead
        % operation
    {merge_branch_nval, integer(), branch_filter()}|
        % Merge a slection of branches of cached Tictac trees for the given
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
    {merge_branch_range, bucket(), key_range(), branch_filter()|all}|
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

    % Operational support functions
    {find_keys, bucket(), key_range(), 
        {sibling_count, pos_integer()}|{object_size, pos_integer()}|
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
    {object_stats, bucket(), key_range(), sample_size()}.
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
        %   {siblings, [{1, 1000}],
        %   {sample_portion, 100.0}]
        %
        % The sample_size determines how many vnodes in the coverage plan 
        % should be consulted.  Setting sample_size to all does a full coverage
        % query to give cluster-wide stats.  



-record(state, {from :: from(),
                acc,
                fold_reference,
                sample_size = all :: all|integer(),
                start_time :: tuple()}).


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.


-spec init(from(), 
            [query_definition(),
            integer()]).

%% @doc 
%% Return a tuple containing the ModFun to call per vnode, the number of 
%% primary preflist vnodes the operation should cover, the service to use to 
%% check for available nodes,and the registered name to use to access the 
%% vnode master process.
init(From={_, _, _}, [Query, Timeout]) ->
    % Get the bucket n_val for use in creating a coverage plan
    
    {Req0, State0}.
                


%% @doc
%% Need to do something about recognising the sample case in plan/2
%% However, maybe cannot do in callback as we don't know about which of the
%% coverage vnodes are filtered (so the sample may be 1, 2 or n partitions).
%% This probably therefore needs to happen in the behaviour.  Perhaps return
%% something of other than 'ok'.
plan(_CoverageVnodes, State = #state{sample=true}) ->
    % filter to a single partition, or perhaps to a single vnode that isn't 
    % filtered
    {filter, State}; 
plan(_CoverageVnodes, State) ->
    {ok, State}.


process_results({error, Reason}, _State) ->
    lager:warning("Failure to process fold results due to ~w", [Reason]),
    {error, Reason};
process_results(Results, State) ->
    % Results are received as a one-off for each vnode in this case, and so 
    % once results are merged work is always done.
    Acc = State#state.acc,
    MergeFun = State#state.merge_fun,
    UpdAcc = MergeFun(Acc, Results),
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
    lager:info("Finished mapfold in ~w seconds using module ~w", 
                [QueryDuration/1000000, State#state.fold_module]),
    ClientPid ! {ReqId, {results, State#state.acc}},
    {stop, normal, State}.

%% ===================================================================
%% Internal functions
%% ===================================================================


%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

-endif.

