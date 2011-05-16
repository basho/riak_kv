%% -------------------------------------------------------------------
%%
%% riak_keys_fsm: listing of bucket keys
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc The keys fsm manages the listing of bucket keys.
%%
%%      The keys fsm creates a plan to achieve coverage
%%      of all keys from the cluster using the minimum
%%      possible number of VNodes, sends key listing
%%      commands to each of those VNodes, and compiles the
%%      responses.
%%
%%      The number of VNodes required for full
%%      coverage is based on the number
%%      of partitions, the number of available physical
%%      nodes, and the bucket n_val.

-module(riak_kv_keys_fsm).

-behaviour(gen_fsm).

%% API
-export([start_link/4,
         start_link/5,
         start_link/6]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
%% Test API
-export([test_link/7, test_link/4]).
-endif.

%% gen_fsm callbacks
-export([init/1,
         initialize/2,
         waiting_kl/2,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4]).

-include_lib("riak_kv_vnode.hrl").

-type req_id() :: non_neg_integer().
-type input() :: riak_object:bucket() |
                 {riak_object:bucket(), fun()} |
                 {filter, riak_object:bucket(), [mfa()]}.
-type from() :: {raw, req_id(), pid()}.

-record(state, {bucket :: riak_object:bucket(),
                client_type :: atom(),
                from :: from(),
                input :: input(),
                n_val :: pos_integer(),
                pref_list_positions :: dict(),
                pref_list_remainders :: dict(),
                required_responses :: pos_integer(),
                response_count=0 :: non_neg_integer(),
                ring :: riak_core_ring:riak_core_ring(),
                timeout :: timeout(),
                upnodes :: [node()]
               }).

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Start a riak_kv_keys_fsm.
%% @deprecated Only in place for backwards compatibility.
%% Please use start_link/5.
start_link(ReqId, Input, Timeout, ClientType, _ErrorTolerance, From) ->
    start_link({raw, ReqId, From}, Input, Timeout, ClientType).

%% @doc Start a riak_kv_keys_fsm.
-spec start_link(req_id(), input(), timeout(), atom(), from()) ->
                        {ok, pid()} | ignore | {error, term()}.
start_link(ReqId, Input, Timeout, ClientType, From) ->
    start_link({raw, ReqId, From}, Input, Timeout, ClientType).

%% @doc Start a riak_kv_keys_fsm.
-spec start_link(from(), input(), timeout(), atom()) ->
                        {ok, pid()} | ignore | {error, term()}.
start_link(From, Bucket, Timeout, ClientType) ->
    gen_fsm:start_link(?MODULE,
                  [From, Bucket, Timeout, ClientType], []).

%% ===================================================================
%% Test API
%% ===================================================================

-ifdef(TEST).
%% Create an keys FSM for testing.  StateProps must include
%% starttime - start time in gregorian seconds
%% n - N-value for request (is grabbed from bucket props in prepare)
%% bucket_props - bucket properties
%% preflist2 - [{{Idx,Node},primary|fallback}] preference list
%%
test_link(ReqId, Input, _Key, R, Timeout, From, StateProps) ->
    test_link({raw, ReqId, From}, Input, [{r, R}, {timeout, Timeout}], StateProps).

test_link(From, Input, _Options, StateProps) ->
    Timeout = 60000,
    ClientType = plain,
    gen_fsm:start_link(?MODULE, {test, [From, Input, Timeout, ClientType], StateProps}, []).

-endif.

%% ====================================================================
%% gen_fsm callbacks
%% ====================================================================

%% @private
init([From={raw, _, ClientPid}, Input, Timeout, ClientType]) ->
    process_flag(trap_exit, true),
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    case Input of
        {filter, B, _} ->
            Bucket = B;
        {B, _} ->
            Bucket = B;
        _ ->
            Bucket = Input
    end,
    StateData = #state{client_type=ClientType, timeout=Timeout,
                       from=From, input=Input, bucket=Bucket, ring=Ring},
    case ClientType of
        %% Link to the mapred job so we die if the job dies
        mapred ->
            link(ClientPid);
        _ ->
            ok
    end,
    {ok, initialize, StateData, 0};
init({test, Args, StateProps}) ->
    %% Call normal init
    {ok, initialize, StateData, 0} = init(Args),

    %% Then tweak the state record with entries provided by StateProps
    Fields = record_info(fields, state),
    FieldPos = lists:zip(Fields, lists:seq(2, length(Fields)+1)),
    F = fun({Field, Value}, State0) ->
                Pos = proplists:get_value(Field, FieldPos),
                setelement(Pos, State0, Value)
        end,
    TestStateData = lists:foldl(F, StateData, StateProps),

    %% Enter into the execute state, skipping any code that relies on the
    %% state of the rest of the system
    {ok, waiting_kl, TestStateData, 0}.

%% @private
initialize(timeout, StateData0=#state{bucket=Bucket,
                                      from={_, ReqId, _},
                                      input=Input,
                                      ring=Ring,
                                      timeout=Timeout}) ->
    BucketProps = riak_core_bucket:get_bucket(Bucket, Ring),
    NVal = proplists:get_value(n_val, BucketProps),
    %% Rotate the list of preflists to the right by 1 so
    %% that the preference list that starts with index 0
    %% is at the head of the list.
    AllPrefLists = right_rotate(riak_core_ring:all_preflists(Ring, NVal), 1),
    %% Determine the number of physical nodes
    NodeCount = length(riak_core_ring:all_members(Ring)),
    %% Determine the minimum number of nodes
    %% required for full coverage.
    case NVal >= NodeCount of
        true ->
            NodeQuorum = 1;
        false ->
            PartitionCount = length(AllPrefLists),
            case NodeCount rem NVal of
                0 ->
                    %% The period of the node sequence is
                    %% evenly divisible by the period of the
                    %% bucket n_val sequence so the minimum number
                    %% of nodes is the node count divided by the
                    %% bucket n_val.
                    NodeQuorum = NodeCount div NVal;
                _ ->
                    LCM = lcm(NVal, NodeCount),
                    case LCM =< PartitionCount of
                        true ->
                            %% Use the least common multipler of the
                            %% bucket n_val and the node count to determine the
                            %% node quorum.
                            NodeQuorum = LCM div NVal;
                        false ->
                            %% The least common multiplier of the bucket n_val
                            %% the node count is greater than the partition count
                            %% so the node quorum is the number of partitions
                            %% divided by the bucket n_val plus one to account
                            %% for the remainder.
                            NodeQuorum = (PartitionCount div NVal) + 1
                    end
            end
    end,
    %% Determine the riak_kv nodes that are available
    UpNodes = riak_core_node_watcher:nodes(riak_kv),
    case plan_and_execute(ReqId, Input, AllPrefLists, UpNodes, [], NodeCount, NVal, NodeQuorum, Timeout, 0) of
        {ok, RequiredResponseCount, PrefListPositions} ->
            StateData = StateData0#state{n_val=NVal,
                                         pref_list_positions=PrefListPositions,
                                         required_responses=RequiredResponseCount,
                                         upnodes=UpNodes},
            {next_state, waiting_kl, StateData, Timeout};
        {error, Reason} ->
            finish({error, Reason}, StateData0)
    end.

%% @private
waiting_kl({ReqId, {kl, VNode, Keys}},
           StateData=#state{bucket=Bucket,
                            client_type=ClientType,
                            from=From={raw, ReqId, _},
                            n_val=NVal,
                            pref_list_positions=PrefListPositions,
                            ring=Ring,
                            timeout=Timeout,
                            upnodes=UpNodes}) ->
    %% Look up the position in the preference list of the VNode
    %% that the keys are expected to be reported from.
    PrefListPosition = dict:fetch(VNode, PrefListPositions),
    process_keys(VNode, Keys, Bucket, ClientType, Ring, NVal, UpNodes, PrefListPosition, From),
    {next_state, waiting_kl, StateData, Timeout};
waiting_kl({ReqId, _VNode, done}, StateData0=#state{from={raw, ReqId, _},
                                                    required_responses=RequiredResponses,
                                                    response_count=ResponseCount,
                                                    timeout=Timeout}) ->
    ResponseCount1 = ResponseCount + 1,
    StateData = StateData0#state{response_count=ResponseCount1},
    case ResponseCount1 >= RequiredResponses of
        true -> finish(clean, StateData);
        false -> {next_state, waiting_kl, StateData, Timeout}
    end;
waiting_kl(timeout, StateData) ->
    finish({error, timeout}, StateData).

%% @private
handle_event(_Event, _StateName, StateData) ->
    {stop,badmsg,StateData}.

%% @private
handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop,badmsg,StateData}.

%% @private
handle_info({'EXIT', Pid, Reason}, _StateName, #state{from={raw,_,Pid}}=StateData) ->
    {stop,Reason,StateData};
handle_info({_ReqId, {ok, _Pid}}, StateName, StateData=#state{timeout=Timeout}) ->
    %% Received a message from a key lister node that
    %% did not start up within the timeout. Just ignore
    %% the message and move on.
    {next_state, StateName, StateData, Timeout};
handle_info(_Info, _StateName, StateData) ->
    {stop,badmsg,StateData}.

%% @private
terminate(Reason, _StateName, _State) ->
    Reason.

%% @private
code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%% ====================================================================
%% Internal functions
%% ====================================================================

%% @private
finish({error, Error}, StateData=#state{from={raw, ReqId, ClientPid}, client_type=ClientType}) ->
    case ClientType of
        mapred ->
            %% No nodes are available for key listing so all
            %% we can do now is die so that the rest of the
            %% MapReduce processes will also die and be cleaned up.
            exit(Error);
        plain ->
            %%Notify the requesting client that the key
            %% listing is complete or that no nodes are
            %% available to fulfil the request.
            ClientPid ! {ReqId, Error}
    end,
    {stop,normal,StateData};
finish(clean, StateData=#state{from={raw, ReqId, ClientPid}, client_type=ClientType}) ->
    case ClientType of
        mapred ->
            luke_flow:finish_inputs(ClientPid);
        plain ->
            ClientPid ! {ReqId, done}
    end,
    {stop,normal,StateData}.

%% @private
plan_and_execute(ReqId, Input, PrefLists, UpNodes, KeyListers,
                 NodeCount, NVal, NodeQuorum, Timeout, Offset) ->
    case node_quorum_satisfied(NodeQuorum, UpNodes) of
        true ->
            %% Generate a coverage plan
            CoveragePlanResult =
                create_coverage_plan(PrefLists, UpNodes, NodeCount, NVal, NodeQuorum, Offset),
            case CoveragePlanResult of
                {error, _} ->
                    %% Failed to create a coverage plan so return the error
                    CoveragePlanResult;
                {NodeIndexes, PrefListPositions} ->
                    %% If successful start processes and execute
                    RequiredResponseCount = dict:size(PrefListPositions),
                    {AggregateKeyListers, ErrorNodes} =
                        start_keylisters(ReqId, Input, KeyListers, NodeIndexes, Timeout),
                    case ErrorNodes of
                        [] ->
                            %% Got a valid coverage plan so instruct the keylister
                            %% processes to begin listing keys for their list of Vnodes.
                            [riak_kv_keylister:list_keys(Pid) || {_, Pid} <- AggregateKeyListers],
                            {ok, RequiredResponseCount, PrefListPositions};
                        _ ->
                            plan_and_execute(ReqId, Input, PrefLists, UpNodes--ErrorNodes,
                                             AggregateKeyListers, NodeCount, NVal, NodeQuorum,
                                             Timeout, Offset)
                    end
            end;
        false ->
            {error, insufficient_nodes_available}
    end.

%% @private
node_quorum_satisfied(NodeQuorum, UpNodes) ->
    if
        UpNodes == [] ->
            false;
        length(UpNodes) < NodeQuorum ->
            false;
        true ->
            true
    end.

%% @private
create_coverage_plan(_PrefLists, _UpNodes, NodeCount, NVal, NodeQuorum, Offset) when Offset > NVal; NodeCount < NodeQuorum ->
    {error, cannot_achieve_coverage};
create_coverage_plan(PrefLists, UpNodes, NodeCount, NVal, _NodeQuorum, Offset) ->
    %% Rotate the list of preference lists
    RotatedPrefLists = left_rotate(PrefLists, Offset),
    UpNodeCount = length(UpNodes),
    %% Determine the preference list positions to use
    %% when filtering key listing responses from VNodes.
    case (UpNodeCount < NodeCount) andalso (UpNodeCount > NVal) of
        true ->
            %% Try to use a subset of preference list positions
            %% to create a minimal list of preference lists.
            DefaultPositions = lists:seq(1, NVal - (NodeCount - UpNodeCount));
        false ->
            DefaultPositions = all
    end,
    %% Determine the minimal list of preference lists
    %% required for full coverage.
    case (UpNodeCount < NodeCount) andalso (NVal > NodeCount) of
        true ->
            MinimalPrefLists = get_minimal_preflists(RotatedPrefLists, UpNodeCount);
        false ->
            MinimalPrefLists = get_minimal_preflists(RotatedPrefLists, NVal)
    end,
    %% Assemble the data structures required for
    %% executing the coverage operation.
    {NodeIndexes, PrefListPositions} = assemble_coverage_structures(MinimalPrefLists, NVal, DefaultPositions),
    case coverage_plan_valid(NodeIndexes, UpNodes) of
        true ->
            {NodeIndexes, PrefListPositions};
        false ->
            %% The current plan will not work so try
            %% to create another one.
            create_coverage_plan(PrefLists, UpNodes, NodeCount, NVal, _NodeQuorum, Offset+1)
    end.

%% @private
get_minimal_preflists(PrefLists, N) ->
    PrefListsCount = length(PrefLists),
    %% Get the count of VNodes remaining after
    %% dividing by N. This will be used in
    %% calculating the set of keys to retain
    %% from the final VNode we request keys from.
    RemainderVNodeCount = PrefListsCount rem N,
    %% Minimize the number of VNodes that we
    %% need to request keys from by selecting every Nth
    %% preference list. If the the number of partitions
    %% is not evenly divisible by N then
    %% a final preference list is selected to ensure complete
    %% coverage of all keys. In this case the keys from
    %% a VNode in this final preference list are filtered
    %% to ensure that duplicates are not introduced.
    PartitionFun = fun({A, _B}) ->
                           (A rem N == 0 andalso (PrefListsCount - A) >= N)
                               orelse ((PrefListsCount - A) == RemainderVNodeCount)
                   end,
    %% IndexVNodeTuples will contain a list of tuples where the
    %% first member is the ring index value between 0 and
    %% the number of ring partitions - 1 and the second
    %% member is a VNode tuple.
    {IndexVNodeTuples, _} = lists:partition(PartitionFun,
                               lists:zip(lists:seq(0, (PrefListsCount-1)), PrefLists)),
    %% Reverse the IndexVNodeTuples list and separate the ring
    %% indexes from the corresponding VNode tuples.
    {_, MinimalPrefLists} = lists:unzip(lists:reverse(IndexVNodeTuples)),
    MinimalPrefLists.

%% @private
assemble_coverage_structures([HeadPrefList | RestPrefLists]=PrefLists, N, DefaultPositions) ->
    %% Get the count of VNodes remaining after
    %% dividing by N. This will be used in
    %% calculating the set of keys to retain
    %% from the final VNode we request keys from.
    RemainderVNodeCount = length(PrefLists) rem N,
    case RemainderVNodeCount of
        0 ->
            assemble_coverage_structures(PrefLists, [], dict:new(), DefaultPositions);
        _ ->
            assemble_coverage_structures([{RemainderVNodeCount, N, HeadPrefList} | RestPrefLists], [], dict:new(), DefaultPositions)
    end.

%% @private
assemble_coverage_structures([], VNodes, PrefListPositions, _) ->
    %% NodeIndexes is a dictionary where the keys are
    %% nodes and the values are lists of VNode indexes.
    %% This is used to determine which nodes to start keylister
    %% processes on and to help minimize the inter-node
    %% communication required to complete the key listing.
    %%
    %% PrefListPositions is dictionary where the keys are
    %% VNodes and the values are either the atom all or
    %% a list of integers representing positions in the
    %% preference list.
    NodeIndexes = group_indexes_by_node(VNodes, []),
    {NodeIndexes, PrefListPositions};
assemble_coverage_structures([{RemainderVNodeCount, NVal, [{Index, Node} | _RestPrefList]} | RestPrefLists], VNodes, PrefListPositions, _DefaultPositions) ->
    VNodes1 = [{Index, Node} | VNodes],
    PositionList = lists:reverse(lists:seq(NVal, NVal-RemainderVNodeCount+1, -1)),
    PrefListPositions1 = dict:store({Index, Node}, PositionList, PrefListPositions),
    assemble_coverage_structures(RestPrefLists, VNodes1, PrefListPositions1, _DefaultPositions);
assemble_coverage_structures([[{Index, Node} | _RestPrefList] | RestPrefLists], VNodes, PrefListPositions, DefaultPositions) ->
    VNodes1 = [{Index, Node} | VNodes],
    PrefListPositions1 = dict:store({Index, Node}, DefaultPositions, PrefListPositions),
    assemble_coverage_structures(RestPrefLists, VNodes1, PrefListPositions1, DefaultPositions).

%% @private
coverage_plan_valid(NodeIndexes, UpNodes) ->
    PotentialNodes = proplists:get_keys(NodeIndexes),
    case PotentialNodes -- UpNodes of
        [] ->
            true;
        _ ->
            false
    end.

%% @private
group_indexes_by_node([], NodeIndexes) ->
    NodeIndexes;
group_indexes_by_node([{Index, Node} | OtherVNodes], NodeIndexes) ->
    %% Check if there is an entry for Node in NodeIndexes
    case proplists:get_value(Node, NodeIndexes) of
        undefined ->
            %% This is the first vnode for this physical node
            %% so add an entry for the node in NodeIndexList
            NodeIndexes1 = [{Node, [Index]} | NodeIndexes];
        Indexes ->
            %% An entry for the physical node is already present
            %% so just update the index information to the value
            %% for the node.
            NodeIndexes1 = [{Node, [Index | Indexes]} | proplists:delete(Node, NodeIndexes)]
    end,
    group_indexes_by_node(OtherVNodes, NodeIndexes1).

%% @private
start_keylisters(ReqId, Input, KeyListers, NodeIndexes, Timeout) ->
    %% Fold over the node indexes list to start
    %% keylister processes on each node and accumulate
    %% the successes and errors.
    StartListerFunc = fun({Node, Indexes}, {Successes, Errors}) ->
                              VNodes = [{Index, Node} || Index <- Indexes],
                              case proplists:get_value(Node, Successes) of
                                  undefined ->
                                      try
                                          case riak_kv_keylister_sup:start_keylister(Node, [ReqId, self(), Input, VNodes, Timeout]) of
                                              {error, Error} ->
                                                  error_logger:warning_msg("Unable to start a keylister process on ~p. Reason: ~p~n", [Node, Error]),
                                                  {Successes, [Node | Errors]};
                                              {ok, Pid} ->
                                                  {[{Node, Pid} | Successes], Errors}
                                          end
                                      catch
                                          _:ThrowReason ->
                                              error_logger:warning_msg("Unable to start a keylister process on ~p. Reason: ~p~n", [Node, ThrowReason]),
                                              {Successes, [Node | Errors]}
                                      end;
                                  KeyListerPid ->
                                      %% A keylister process is already running on the node
                                      %% so just update the VNodes it should list keys for.
                                      riak_kv_keylister:update_vnodes(KeyListerPid, VNodes),
                                      {Successes, Errors}
                              end
                      end,
    lists:foldl(StartListerFunc, {KeyListers, []}, NodeIndexes).

%% @private
process_keys(VNode, Keys, Bucket, ClientType, Ring, NVal, UpNodes, PrefListPosition, From) ->
    process_keys(VNode, Keys, Bucket, ClientType, Ring, NVal, UpNodes, PrefListPosition, From, []).

%% @private
process_keys(_, [], Bucket, ClientType, _, _, _, _, {raw,ReqId,ClientPid}, Acc) ->
    case ClientType of
        mapred ->
            try
                luke_flow:add_inputs(ClientPid, [{Bucket,K} || K <- Acc])
            catch _:_ ->
                    exit(self(), normal)
            end;
        plain -> ClientPid ! {ReqId, {keys, Acc}}
    end;
process_keys(_VNode, [K|Rest], _Bucket, _ClientType, _Ring, _NVal, _UpNodes, all, _From, Acc) ->
    process_keys(_VNode, Rest, _Bucket, _ClientType, _Ring, _NVal, _UpNodes, all, _From, [K|Acc]);
process_keys(VNode, [K|Rest], Bucket, ClientType, Ring, NVal, UpNodes, PrefListPosition, From, Acc) ->
    %% Get the chash key for the bucket-key pair and
    %% use that to determine the preference list to
    %% use in filtering the keys from this VNode.
    ChashKey = riak_core_util:chash_key({Bucket, K}),
    PrefList = riak_core_apl:get_apl_ann(ChashKey, NVal, Ring, UpNodes),
    case check_pref_list_positions(PrefListPosition, VNode, PrefList) of
        true ->
            process_keys(VNode, Rest, Bucket, ClientType, Ring, NVal, UpNodes, PrefListPosition, From, [K|Acc]);
        false ->
            process_keys(VNode, Rest, Bucket, ClientType, Ring, NVal, UpNodes, PrefListPosition, From, Acc)
    end.

%% @private
check_pref_list_positions([], _, _) ->
    false;
check_pref_list_positions([Position | RestPositions], VNode, PrefList) ->
    case lists:nth(Position, PrefList) of
        {VNode, primary} ->
            true;
        _ ->
            check_pref_list_positions(RestPositions, VNode, PrefList)
    end.

%% @private
%% @doc Rotate a list to the left.
left_rotate(List, 0) ->
    List;
left_rotate([Head | Rest], Rotations) ->
    RotatedList = lists:reverse([Head | lists:reverse(Rest)]),
    left_rotate(RotatedList, Rotations-1).

%% @private
%% @doc Rotate a list to the right.
right_rotate(List, 0) ->
    List;
right_rotate(List, Rotations) ->
    lists:reverse(left_rotate(lists:reverse(List), Rotations)).

%% @private
%% @doc Return the greatest common
%% denominator of two integers.
gcd(A, 0) ->
    A;
gcd(A, B) -> gcd(B, A rem B).

%% @private
%% @doc Return the least common
%% multiple of two integers.
lcm(A, B) ->
    (A * B) div gcd(A, B).
