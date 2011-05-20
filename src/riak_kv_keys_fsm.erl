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
-type bucket() :: binary().
-type input() :: bucket() |
                 {bucket(), fun()} |
                 {filter, bucket(), [mfa()]}.
-type from() :: {raw, req_id(), pid()}.

-record(state, {bucket :: bucket(),
                client_type :: atom(),
                from :: from(),
                input :: input(),
                required_responses :: pos_integer(),
                response_count=0 :: non_neg_integer(),
                timeout :: timeout()
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
-spec start_link(req_id(), input(), timeout(), atom(), pid()) ->
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
    case Input of
        {filter, B, _} ->
            Bucket = B;
        {B, _} ->
            Bucket = B;
        _ ->
            Bucket = Input
    end,
    StateData = #state{client_type=ClientType, timeout=Timeout,
                       from=From, input=Input, bucket=Bucket},
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
                                      timeout=Timeout}) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    BucketProps = riak_core_bucket:get_bucket(Bucket, Ring),
    NVal = proplists:get_value(n_val, BucketProps),
    VNodes = riak_core_ring:all_owners(Ring),
    %% Create a proplist with ring positions as
    %% keys and VNodes as the values.
    VNodePositions = lists:zip(lists:seq(0, length(VNodes)-1), VNodes),
    case plan_and_execute(ReqId, Input, VNodePositions, NVal, length(VNodes), Timeout, [], []) of
        {ok, RequiredResponseCount, _PrefListPositions} ->
            StateData = StateData0#state{required_responses=RequiredResponseCount},
            {next_state, waiting_kl, StateData, Timeout};
        {error, Reason} ->
            finish({error, Reason}, StateData0)
    end.

%% @private
waiting_kl({ReqId, {kl, _VNode, Keys}},
           StateData=#state{bucket=Bucket,
                            client_type=ClientType,
                            from=From={raw, ReqId, _},
                            timeout=Timeout}) ->
    process_keys(Keys, Bucket, ClientType, From),
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
            %% An error occurred or the timeout interval elapsed
            %% so all we can do now is die so that the rest of the
            %% MapReduce processes will also die and be cleaned up.
            exit(Error);
        plain ->
            %% Notify the requesting client that an error
            %% occurred or the timeout has elapsed.
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
plan_and_execute(ReqId, Input, VNodePositions, NVal, PartitionCount, Timeout, KeyListers, DownNodes) ->
    %% Generate a coverage plan
    CoveragePlanResult =
        create_coverage_plan(VNodePositions, NVal, PartitionCount, DownNodes),
    case CoveragePlanResult of
        {error, _} ->
            %% Failed to create a coverage plan so return the error
            CoveragePlanResult;
        {NodeIndexes, PrefListPositions, RequiredResponseCount} ->
            %% If successful start processes and execute
            {AggregateKeyListers, ErrorNodes} =
                start_keylisters(ReqId, Input, KeyListers, NodeIndexes, PrefListPositions, Timeout),
            case ErrorNodes of
                [] ->
                    %% Got a valid coverage plan so instruct the keylister
                    %% processes to begin listing keys for their list of Vnodes.
                    [riak_kv_keylister:list_keys(Pid) || {_, Pid} <- AggregateKeyListers],
                    {ok, RequiredResponseCount, PrefListPositions};
                _ ->
                    plan_and_execute(ReqId, Input, VNodePositions,
                                     NVal, PartitionCount,
                                     Timeout, AggregateKeyListers, ErrorNodes)
            end
    end.

%% @private
create_coverage_plan(VNodePositions, NVal, PartitionCount, DownNodes) ->
    PartitionCount = length(VNodePositions),
    %% AvailableVNodes = VNodes -- DownVNodes
    %% Make list of which partitions each VNode covers
    DownVNodeFilter = fun(X) ->
                              %% Determine the node for the
                              %% VNode at this ring position.
                              {_, Node} = proplists:get_value(X, VNodePositions),
                              not lists:member(Node, DownNodes)
                      end,
    KeySpace = lists:seq(0, PartitionCount - 1),
    AvailableKeySpace = lists:filter(DownVNodeFilter, KeySpace),
    Available = [{Vnode, n_keyspaces(Vnode, NVal, PartitionCount)} || Vnode <- AvailableKeySpace],
    CoverageResult = find_coverage(ordsets:from_list(KeySpace), Available, NVal, []),
    case CoverageResult of
        {ok, CoveragePlan} ->
            %% Assemble the data structures required for
            %% executing the coverage operation.
            assemble_coverage_structures(CoveragePlan, NVal, PartitionCount, VNodePositions);
       {insufficient_vnodes_available, _}  ->
            {error, insufficient_vnodes_available}
    end.

%% @private
%% @doc Find the N key spaces for a VNode
n_keyspaces(Vnode, N, Q) ->
     ordsets:from_list([X rem Q || X <- lists:seq(Q + Vnode, Q + Vnode + (N-1))]).

%% @private
%% @doc Find minimal set of covering vnodes
find_coverage([], _, _, Coverage) ->
    {ok, lists:sort(Coverage)};
find_coverage(KeySpace, [], _, Coverage) ->
    {insufficient_vnodes_available, KeySpace, lists:sort(Coverage)};
find_coverage(KeySpace, Available, NVal, Coverage) ->
    case next_vnode(KeySpace, NVal, Available) of
        {0, _} -> % out of vnodes
            find_coverage(KeySpace, [], NVal, Coverage);
        {_NumCovered, Vnode} ->
            {value, {Vnode, Covers}, UpdAvailable} = lists:keytake(Vnode, 1, Available),
            UpdCoverage = [{Vnode, ordsets:intersection(KeySpace, Covers)} | Coverage],
            UpdKeySpace = ordsets:subtract(KeySpace, Covers),
            find_coverage(UpdKeySpace, UpdAvailable, NVal, UpdCoverage)
    end.

%% @private
%% @doc Find the next vnode that covers the most of the
%% remaining keyspace. Use VNode id as tie breaker.
next_vnode(KeySpace, NVal, Available) ->
    CoverCount = [{covers(KeySpace, CoversKeys), VNode} || {VNode, CoversKeys} <- Available],
    case length(KeySpace) >= NVal of
        true ->
            hd(lists:sort(fun compare_next_vnode/2, CoverCount));
        false ->
            hd(lists:sort(fun compare_final_vnode/2, CoverCount))
    end.

%% @private
compare_next_vnode({CA,VA}, {CB, VB}) ->
    if
        CA > CB -> %% Descending sort on coverage
            true;
        CA < CB ->
            false;
        true ->
            VA < VB %% If equal coverage choose the lower node.
    end.

%% @private
compare_final_vnode({CA,VA}, {CB, VB}) ->
    if
        CA > CB -> %% Descending sort on coverage
            true;
        CA < CB ->
            false;
        true ->
            VA > VB %% If equal coverage choose the upper node.
    end.

%% @private
%% @doc Count how many of CoversKeys appear in KeySpace
covers(KeySpace, CoversKeys) ->
    ordsets:size(ordsets:intersection(KeySpace, CoversKeys)).

%% @private
assemble_coverage_structures(CoveragePlan, NVal, PartitionCount, VNodeIndex) ->
    assemble_coverage_structures(CoveragePlan, NVal, PartitionCount, VNodeIndex, [], []).

%% @private
assemble_coverage_structures([], _, _, _, CoverageVNodes, PrefListPositions) ->
    %% NodeIndexes is a proplist where the keys are
    %% nodes and the values are lists of VNode indexes.
    %% This is used to determine which nodes to start keylister
    %% processes on and to help minimize the inter-node
    %% communication required to complete the key listing.
    %%
    %% PrefListPositions is a proplist where the keys are
    %% nodes and the values are tuples with a VNode index
    %% as the first tuple member and a list of preference
    %% list positions for the VNode as the second tuple member.
    NodeIndexes = group_indexes_by_node(CoverageVNodes, []),
    {NodeIndexes, PrefListPositions, length(CoverageVNodes)};
assemble_coverage_structures([{RingPosition, RingPositionList} | RestCoveragePlan], NVal, PartitionCount, VNodeIndex, CoverageVNodes, PrefListPositions) ->
    %% Lookup the VNode index and node
    {Index, Node} = proplists:get_value(RingPosition, VNodeIndex),
    CoverageVNodes1 = [{Index, Node} | CoverageVNodes],
    case length(RingPositionList) == NVal of
        true ->
            PrefListPositions1 = PrefListPositions;
        false ->
            PositionList = lists:reverse([NVal - (((X - RingPosition) + PartitionCount) rem PartitionCount) || X <- RingPositionList]),
            case proplists:get_value(Node, PrefListPositions) of
                undefined ->
                    PrefListPositions1 = [{Node, [{Index, PositionList}]} | PrefListPositions];
                Positions ->
                    PrefListPositions1 = [{Node, [{Index, PositionList} | Positions]} | proplists:delete(Node, PrefListPositions)]
            end            
    end,
    assemble_coverage_structures(RestCoveragePlan, NVal, PartitionCount, VNodeIndex, CoverageVNodes1, PrefListPositions1).

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
start_keylisters(ReqId, Input, KeyListers, NodeIndexes, PrefListPositions, Timeout) ->
    %% Fold over the node indexes list to start
    %% keylister processes on each node and accumulate
    %% the successes and errors.
    StartListerFunc = fun({Node, Indexes}, {Successes, Errors}) ->
                              VNodes = [{Index, Node} || Index <- Indexes],
                              FilterVNodes = proplists:get_value(Node, PrefListPositions),
                              case proplists:get_value(Node, Successes) of
                                  undefined ->
                                      try
                                          case riak_kv_keylister_sup:start_keylister(Node, [ReqId, self(), Input, VNodes, FilterVNodes, Timeout]) of
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
process_keys(Keys, Bucket, ClientType, {raw, ReqId, ClientPid}) ->
    case ClientType of
        mapred ->
            try
                luke_flow:add_inputs(ClientPid, [{Bucket,K} || K <- Keys])
            catch _:_ ->
                    exit(self(), normal)
            end;
        plain -> ClientPid ! {ReqId, {keys, Keys}}
    end.
