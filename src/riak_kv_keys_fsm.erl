%% -------------------------------------------------------------------
%%
%% riak_keys_fsm: listing of bucket keys
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc listing of bucket keys

-module(riak_kv_keys_fsm).
-behaviour(gen_fsm).
-include_lib("riak_kv_vnode.hrl").
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-export([test_link/7, test_link/4]).
-endif.
-export([start_link/6]).
-export([init/1, handle_event/3, handle_sync_event/4,
         handle_info/3, terminate/3, code_change/4]).
-export([initialize/2,waiting_kl/2]).

-type req_id() :: non_neg_integer().

-record(state, {from :: {raw, req_id(), pid()},
                client_type :: atom(),
                pls :: [list()],
                bucket :: riak_object:bucket(),
                input,
                timeout :: pos_integer(),
                ring :: riak_core_ring:riak_core_ring(),
                node_indexes :: [{atom(), list()}],
                pref_list_positions :: dict(),
                pref_list_remainders :: dict(),
                upnodes :: [node()],
                response_count=0 :: non_neg_integer(),
                required_responses :: pos_integer(),
                n_val :: pos_integer()
               }).

%% ===================================================================
%% Public API
%% ===================================================================

start_link(ReqId,Bucket,Timeout,ClientType,ErrorTolerance,From) ->
    start_link({raw, ReqId, From}, Bucket, Timeout, ClientType, ErrorTolerance).

start_link(From,Bucket,Timeout,ClientType,ErrorTolerance) ->
    gen_fsm:start_link(?MODULE,
                  [From,Bucket,Timeout,ClientType,ErrorTolerance], []).

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
test_link(ReqId,Bucket,_Key,R,Timeout,From,StateProps) ->
    test_link({raw, ReqId, From}, Bucket, [{r, R}, {timeout, Timeout}], StateProps).

test_link(From, Bucket, _Options, StateProps) ->
    ErrorTolerance = 0.00003,
    Timeout = 60000,
    ClientType = plain,
    gen_fsm:start_link(?MODULE, {test, [From, Bucket, Timeout, ClientType, ErrorTolerance], StateProps}, []).

-endif.

%% ====================================================================
%% gen_fsm callbacks
%% ====================================================================

%% @private
init([From={raw, _, ClientPid}, Input, Timeout, ClientType, _ErrorTolerance]) ->
    process_flag(trap_exit, true),
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Bucket = case Input of
                 {B, _} ->
                     B;
                 _ ->
                     Input
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
initialize(timeout, StateData0=#state{input=Input, bucket=Bucket, ring=Ring, from={_, ReqId, _}, timeout=Timeout}) ->
    BucketProps = riak_core_bucket:get_bucket(Bucket, Ring),
    NVal = proplists:get_value(n_val, BucketProps),
    AllPrefLists = riak_core_ring:all_preflists(Ring, NVal),
    %% Determine the number of physical nodes
    NodeCount = length(riak_core_ring:all_members(Ring)),
    %% Determine the minimum number of nodes
    %% required for full coverage.
    case NodeCount >= NVal of
        true ->
            NodeQuorum = 1;
        false ->
            NodeQuorum = NVal - (NVal - NodeCount)
    end,
    %% Determine the riak_kv nodes that are available
    UpNodes = riak_core_node_watcher:nodes(riak_kv),
    case node_quorum_satisfied(NodeQuorum, UpNodes) of
        true ->
            %% Generate an coverage plan
            CoveragePlanResult = create_coverage_plan(AllPrefLists, UpNodes, NodeCount, NVal, NodeQuorum, 0),
            case CoveragePlanResult of
                {error, _} ->                    
                    %% Failed to create a coverage plan so return the error
                    finish(CoveragePlanResult, StateData0);
                {NodeIndexes, PrefListPositions} ->
                    %% If successful start processes and execute
                    RequiredResponseCount = dict:size(PrefListPositions),
                    start_keylisters(ReqId, Input, NodeIndexes, PrefListPositions, Timeout),
                    StateData = StateData0#state{
                                  pref_list_positions=PrefListPositions,
                                  upnodes=UpNodes,
                                  required_responses=RequiredResponseCount,
                                  n_val=NVal},
                    {next_state, waiting_kl, StateData, Timeout}                    
            end;
        false ->
            finish({error, insufficient_nodes_available}, StateData0)
    end.

waiting_kl({ReqId, {kl, VNode, Keys}},
           StateData=#state{from=From={raw, ReqId, _},
                            timeout=Timeout,
                            upnodes=UpNodes,
                            pref_list_positions=PrefListPositions,
                            n_val=NVal,
                            ring=Ring,
                            bucket=Bucket,
                            client_type=ClientType}) ->
    %% Look up the position in the preference list of the VNode
    %% that the keys are expected to be reported from.
    PrefListPosition = dict:fetch(VNode, PrefListPositions),
    process_keys(VNode, Keys, Bucket, ClientType, Ring, NVal, UpNodes, PrefListPosition, From),
    {next_state, waiting_kl, StateData, Timeout};
waiting_kl({ReqId, _VNode, done}, StateData0=#state{pls=_PLS,
                                                   from={raw, ReqId, _},
                                                   response_count=ResponseCount,
                                                   required_responses=RequiredResponses,
                                                   timeout=Timeout}) ->
    ResponseCount1 = ResponseCount + 1,
    StateData = StateData0#state{response_count=ResponseCount1},
    case ResponseCount1 >= RequiredResponses of
        true -> finish(clean, StateData);
        false -> {next_state, waiting_kl, StateData, Timeout}
    end;
waiting_kl(timeout, StateData) ->
    finish({error, timeout}, StateData).


%% ====================================================================
%% Internal functions
%% ====================================================================

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

node_quorum_satisfied(NodeQuorum, UpNodes) ->
    if 
        UpNodes == [] ->
            false;
        length(UpNodes) < NodeQuorum ->
            false;
        true ->
            true
    end.

create_coverage_plan(_AllPrefLists, _UpNodes, _NodeCount, NVal, _NodeQuorum, Offset) when Offset > NVal ->
    {error, cannot_achieve_coverage};
create_coverage_plan(AllPrefLists, UpNodes, _NodeCount, NVal, _NodeQuorum, Offset) ->
    %% Rotate the list of preference lists
    RotatedPrefLists = left_rotate(AllPrefLists, Offset),
    %% Determine the minimal list of preference lists
    %% required for full coverage.
    MinimalPrefLists = get_minimal_preflists(RotatedPrefLists, NVal),
    %% Assemble the data structures required for
    %% executing the coverage operation.
    %%
    %% NodeIndexes is a dictionary where the keys are
    %% nodes and the values are lists of VNode indexes.
    %%
    %% PrefListPositions is dictionary where the keys are
    %% VNodes and the values are either the atom all or
    %% a list of integers representing positions in the
    %% preference list.    
    {NodeIndexes, PrefListPositions} = assemble_coverage_structures(MinimalPrefLists, NVal),
    case coverage_plan_valid(NodeIndexes, UpNodes) of
        true ->
            {NodeIndexes, PrefListPositions};
        false ->
            %% TODO: Recurse and try another plan
            %% create_coverage_plan(AllPrefLists, UpNodes, NodeCount, NVal, NodeQuorum, Offset+1),
            {NodeIndexes, PrefListPositions}
    end.

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
    %% is not evenly divisible by the bucket n-val then
    %% a final preference list is selected to ensure complete
    %% coverage of all keys. In this case the keys from
    %% a VNode in this final preference list are filtered
    %% to ensure that duplicates are not introduced.
    PartitionFun = fun({A, _B}) ->
                           (A rem N == 0 andalso (PrefListsCount - A) > N)
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

left_rotate(List, 0) ->
    List;
left_rotate([Head | Rest], Rotations) ->
    RotatedList = lists:reverse([Head | lists:reverse(Rest)]),
    left_rotate(RotatedList, Rotations-1).

assemble_coverage_structures([HeadPrefList | RestPrefLists]=PrefLists, N) ->
    %% Get the count of VNodes remaining after
    %% dividing by N. This will be used in 
    %% calculating the set of keys to retain
    %% from the final VNode we request keys from.
    RemainderVNodeCount = length(PrefLists) rem N,
    case RemainderVNodeCount of
        0 ->
            assemble_coverage_structures(PrefLists, [], dict:new());
        _ ->
            assemble_coverage_structures([{RemainderVNodeCount, N, HeadPrefList} | RestPrefLists], [], dict:new())
    end.

assemble_coverage_structures([], VNodes, PrefListPositions) ->
    %% Create a proplist where the keys are nodes and
    %% the values are lists of VNode indexes. This is
    %% used to determine which nodes to start keylister
    %% processes on and to help minimize the inter-node
    %% communication required to complete the key listing.
    NodeIndexes = group_indexes_by_node(VNodes, []),
    {NodeIndexes, PrefListPositions};
assemble_coverage_structures([{RemainderVNodeCount, NVal, [{Index, Node} | _RestPrefList]} | RestPrefLists], VNodes, PrefListPositions) ->
    VNodes1 = [{Index, Node} | VNodes],
    PositionList = lists:reverse(lists:seq(NVal, NVal-RemainderVNodeCount+1, -1)),
    PrefListPositions1 = dict:store({Index, Node}, PositionList, PrefListPositions),
    assemble_coverage_structures(RestPrefLists, VNodes1, PrefListPositions1);
assemble_coverage_structures([[{Index, Node} | _RestPrefList] | RestPrefLists], VNodes, PrefListPositions) ->
    VNodes1 = [{Index, Node} | VNodes],
    PrefListPositions1 = dict:store({Index, Node}, all, PrefListPositions),
    assemble_coverage_structures(RestPrefLists, VNodes1, PrefListPositions1).

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
start_keylisters(ReqId, Input, NodeIndexes, _PrefListPositions, Timeout) ->
    %% Fold over the node indexes list to start
    %% keylister processes on each node and accumulate
    %% the successes and errors.
    StartListerFunc = fun({Node, Indexes}, {Successes, Errors}) ->
                              try
                                  case start_keylister(ReqId, Input, Node, Indexes, Timeout) of
                                      {error, Error} ->
                                          error_logger:warning_msg("Unable to start a keylister process on ~p. Reason: ~p~n", [Node, Error]),
                                          {Successes, [Node | Errors]};
                                      {ok, Pid} ->
                                          riak_kv_keylister:list_keys(Pid),
                                          {[{Node, Pid} | Successes], Errors}
                                  end
                              catch
                                  _:ThrowReason ->
                                      error_logger:warning_msg("Unable to start a keylister process on ~p. Reason: ~p~n", [Node, ThrowReason]),
                                      {Successes, [Node | Errors]}
                              end
                      end,
    {_KeyListerNodes, ErrorNodes} = lists:foldl(StartListerFunc, {[], []}, NodeIndexes),
    case ErrorNodes of
        [] ->
            %% All keylister processes started successfully
            ok;
        _ ->
            %% One or more keylister processes failed to start.
            %% Update the NodexIndexes list for the nodes that
            %% had errors and send the key listing request to
            %% the next entry in the preference list or return
            %% an error if all preference list entries have
            %% been exhausted.

            %% Retry the key listing on a different VNode from
            %% the preference list for each VNode on each node
            %% that had an error.
            %% ErrorHandlingResult = handle_keylister_errors(ErrorNodes, ReqId, Input, NodeIndexes, PrefListPositions, PrefListRemainders, Timeout),
            %% case ErrorHandlingResult of
            %%     {error, _} ->
            %%         ErrorHandlingResult;
            %%     {ok, PrefListPositions1, PrefListRemainders1} ->
            %%         {ok, PrefListPositions1, PrefListRemainders1}
            %% end
            fuck
    end.

%% @private
start_keylister(ReqId, Input, Node, Indexes, Timeout) ->
    VNodes = [{Index, Node} || Index <- Indexes],
    riak_kv_keylister_sup:start_keylister(Node, [ReqId, self(), Input, VNodes, Timeout]).

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
