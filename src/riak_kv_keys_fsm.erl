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

-compile(export_all).
%% API
-export([start_link/4,
         start_link/5,
         start_link/6]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
%% Test API
-export([test_link/6, test_link/4]).
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
-define(RINGTOP, trunc(math:pow(2,160)-1)).  % SHA-1 space

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
start_link(From, Input, Timeout, ClientType) ->
    gen_fsm:start_link(?MODULE,
                  [From, Input, Timeout, ClientType], []).

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
test_link(ReqId, Input, R, Timeout, From, StateProps) ->
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
    PartitionCount = riak_core_ring:num_partitions(Ring),
    %% Get the list of all nodes and the list of available
    %% nodes so we can have a list of unavailable nodes
    %% while creating a coverage plan.
    Nodes = riak_core_ring:all_members(Ring),
    UpNodes = riak_core_node_watcher:nodes(riak_kv),
    case plan_and_execute(ReqId, Input, NVal, PartitionCount, Ring, Timeout, [], Nodes -- UpNodes) of
        {ok, RequiredResponseCount} ->
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
handle_info({'EXIT', _Pid, Reason}, _StateName, StateData) ->
    finish({error, {node_failure, Reason}}, StateData);
handle_info({_ReqId, {ok, _Pid}}, StateName, StateData=#state{timeout=Timeout}) ->
    %% Received a message from a key lister node that
    %% did not start up within the timeout. Just ignore
    %% the message and move on.
    {next_state, StateName, StateData, Timeout};
handle_info(_Info, _StateName, StateData) ->
    {stop, badmsg, StateData}.

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
plan_and_execute(ReqId, Input, NVal, PartitionCount, Ring, Timeout, KeyListers, DownNodes) ->
    %% Get a list of the VNodes owned by any unavailble nodes
    DownVNodes = [Index || {Index, Node} <- riak_core_ring:all_owners(Ring), lists:member(Node, DownNodes)],
    %% Calculate an offset based on the request id to offer
    %% the possibility of different sets of VNodes being
    %% used even when all nodes are available.
    Offset = ReqId rem NVal,
    %% Generate a coverage plan
    CoveragePlanResult =
        create_coverage_plan(NVal, PartitionCount, Ring, Offset, DownVNodes),
    case CoveragePlanResult of
        {error, _} ->
            %% Failed to create a coverage plan so return the error
            CoveragePlanResult;
        {NodeIndexes, Filters, RequiredResponseCount} ->
            %% If successful start processes and execute
            {AggregateKeyListers, ErrorNodes} =
                start_keylisters(ReqId, Input, KeyListers, NodeIndexes, Filters, Timeout),
            case ErrorNodes of
                [] ->
                    %% Got a valid coverage plan so instruct the keylister
                    %% processes to begin listing keys for their list of Vnodes.
                    [riak_kv_keylister:list_keys(Pid) || {_, Pid} <- AggregateKeyListers],
                    {ok, RequiredResponseCount};
                _ ->
                    plan_and_execute(ReqId, Input,
                                     NVal, PartitionCount, Ring,
                                     Timeout, AggregateKeyListers, DownNodes ++ ErrorNodes)
            end
    end.

%% @private
create_coverage_plan(NVal, PartitionCount, Ring, Offset, DownVNodes) ->
    RingIndexInc = ?RINGTOP div PartitionCount,
    AllKeySpaces = lists:seq(0, PartitionCount - 1),
    UnavailableKeySpaces = [(DownVNode div RingIndexInc) || DownVNode <- DownVNodes],
    %% The offset value serves as a tiebreaker in the
    %% compare_next_vnode function and is used to distribute
    %% work to different sets of VNodes.
    AvailableKeySpaces = [{((VNode+Offset) rem PartitionCount), VNode, n_keyspaces(VNode, NVal, PartitionCount)}
                          || VNode <- (AllKeySpaces -- UnavailableKeySpaces)],
    CoverageResult = find_coverage(ordsets:from_list(AllKeySpaces), AvailableKeySpaces, []),
    case CoverageResult of
        {ok, CoveragePlan} ->
            %% Assemble the data structures required for
            %% executing the coverage operation.
            CoverageVNodeFun = fun({Position, KeySpaces}, Acc) ->
                                       %% Calculate the VNode index using the
                                       %% ring position and the increment of
                                       %% ring index values.
                                       VNodeIndex = (Position rem PartitionCount) * RingIndexInc,
                                       Node = riak_core_ring:index_owner(Ring, VNodeIndex),
                                       CoverageVNode = {VNodeIndex, Node},
                                       case length(KeySpaces) < NVal of
                                           true ->
                                               %% Get the VNode index of each keyspace to
                                               %% use to filter results from this VNode.
                                               KeySpaceIndexes = [(((KeySpaceIndex+1) rem PartitionCount) * RingIndexInc)
                                                                  || KeySpaceIndex <- KeySpaces],
                                               Acc1 = orddict:append(Node, {VNodeIndex, KeySpaceIndexes}, Acc),
                                               {CoverageVNode, Acc1};
                                           false ->
                                               {CoverageVNode, Acc}
                                       end
                               end,
            {CoverageVNodes, FilterVNodes} = lists:mapfoldl(CoverageVNodeFun, [], CoveragePlan),
            NodeIndexes = group_indexes_by_node(CoverageVNodes, []),
            {NodeIndexes, FilterVNodes, length(CoverageVNodes)};
       {insufficient_vnodes_available, _KeySpace, _Coverage}  ->
            {error, insufficient_vnodes_available}
    end.

%% @private
%% @doc Find the N key spaces for a VNode
n_keyspaces(VNode, N, PartitionCount) ->
     ordsets:from_list([X rem PartitionCount || X <- lists:seq(PartitionCount + VNode - N, PartitionCount + VNode - 1)]).

%% @private
%% @doc Find a minimal set of covering VNodes
find_coverage([], _, Coverage) ->
    {ok, lists:sort(Coverage)};
find_coverage(KeySpace, [], Coverage) ->
    {insufficient_vnodes_available, KeySpace, lists:sort(Coverage)};
find_coverage(KeySpace, Available, Coverage) ->
    Res = next_vnode(KeySpace, Available),
        case Res of
        {0, _, _} -> % out of vnodes
            find_coverage(KeySpace, [], Coverage);
        {_NumCovered, VNode, _} ->
            {value, {_, VNode, Covers}, UpdAvailable} = lists:keytake(VNode, 2, Available),
            UpdCoverage = [{VNode, ordsets:intersection(KeySpace, Covers)} | Coverage],
            UpdKeySpace = ordsets:subtract(KeySpace, Covers),
            find_coverage(UpdKeySpace, UpdAvailable, UpdCoverage)
    end.

%% @private
%% @doc Find the next vnode that covers the most of the
%% remaining keyspace. Use VNode id as tie breaker.
next_vnode(KeySpace, Available) ->
    CoverCount = [{covers(KeySpace, CoversKeys), VNode, TieBreaker} || {TieBreaker, VNode, CoversKeys} <- Available],
    hd(lists:sort(fun compare_next_vnode/2, CoverCount)).

%% @private
%% There is a potential optimization here once
%% the partition claim logic has been changed
%% so that physical nodes claim partitions at
%% regular intervals around the ring.
%% The optimization is for the case
%% when the partition count is not evenly divisible
%% by the n_val and when the coverage counts of the
%% two arguments are equal and a tiebreaker is
%% required to determine the sort order. In this
%% case, choosing the lower node for the final
%% vnode to complete coverage will result
%% in an extra physical node being involved
%% in the coverage plan so the optimization is
%% to choose the upper node to minimize the number
%% of physical nodes.
compare_next_vnode({CA, _VA, TBA}, {CB, _VB, TBB}) ->
    if
        CA > CB -> %% Descending sort on coverage
            true;
        CA < CB ->
            false;
        true ->
            TBA < TBB %% If equal coverage choose the lower node.
    end.

%% @private
%% @doc Count how many of CoversKeys appear in KeySpace
covers(KeySpace, CoversKeys) ->
    ordsets:size(ordsets:intersection(KeySpace, CoversKeys)).

%% @private
group_indexes_by_node([], NodeIndexes) ->
    NodeIndexes;
group_indexes_by_node([{Index, Node} | OtherVNodes], NodeIndexes) ->
    NodeIndexes1 = orddict:append(Node, Index, NodeIndexes),
    group_indexes_by_node(OtherVNodes, NodeIndexes1).

%% @private
start_keylisters(ReqId, Input, KeyListers, NodeIndexes, Filters, Timeout) ->
    case KeyListers of
        [] ->
            UpdatedKeyListers = KeyListers;
        _ ->
            %% Check for existing keylister processes whose
            %% node does not have an entry in NodeIndexes.
            %% This would happen if a coverage plan fails
            %% for some reason and the subsequent plan does
            %% not include any VNodes from a node where the
            %% previous plan did.
            KeyListerCleanup =
                fun({Node, Pid}=KeyLister, Acc) ->
                        case proplists:is_defined(Node, NodeIndexes) of
                            true ->
                                [KeyLister | Acc];
                            false ->
                                riak_kv_keylister:update_vnodes(Pid, [], []),
                                Acc
                        end
                end,
            UpdatedKeyListers = lists:foldl(KeyListerCleanup, [], KeyListers)
    end,
    %% Fold over the node indexes list to start
    %% keylister processes on each node and accumulate
    %% the successes and errors.
    StartListerFunc = fun({Node, Indexes}, {Successes, Errors}) ->
                              VNodes = [{Index, Node} || Index <- Indexes],
                              FilterVNodes = proplists:get_value(Node, Filters),
                              case proplists:get_value(Node, Successes) of
                                  undefined ->
                                      try
                                          case riak_kv_keylister_sup:start_keylister(Node, [ReqId, self(), Input, VNodes, FilterVNodes, Timeout]) of
                                              {error, Error} ->
                                                  lager:warning("Unable to start a keylister process on ~p: ~p", [Node, Error]),
                                                  {Successes, [Node | Errors]};
                                              {ok, Pid} ->
                                                  erlang:link(Pid),
                                                  {[{Node, Pid} | Successes], Errors}
                                          end
                                      catch
                                          _:ThrowReason ->
                                              lager:warning("Unable to start a keylister process on ~p: ~p", [Node, ThrowReason]),
                                              {Successes, [Node | Errors]}
                                      end;
                                  KeyListerPid ->
                                      %% A keylister process is already running on the node
                                      %% so just update the VNodes it should list keys for.
                                      riak_kv_keylister:update_vnodes(KeyListerPid, VNodes, FilterVNodes),
                                      {Successes, Errors}
                              end
                      end,
    lists:foldl(StartListerFunc, {UpdatedKeyListers, []}, NodeIndexes).

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
