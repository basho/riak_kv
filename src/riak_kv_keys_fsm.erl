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
-export([test_link/7, test_link/5]).
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
                node_indexes :: dict(),
                upnodes :: [node()],
                responses :: non_neg_integer(),
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
test_link(ReqId,Bucket,Key,R,Timeout,From,StateProps) ->
    test_link({raw, ReqId, From}, Bucket, [{r, R}, {timeout, Timeout}], StateProps).

test_link(From, Bucket, Options, StateProps) ->
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
    N = proplists:get_value(n_val,BucketProps),
    PLS0 = riak_core_ring:all_preflists(Ring,N),
    {LA1, LA2} = lists:partition(fun({A, _B}) ->
                                       A rem N == 0 orelse A rem (N + 1) == 0
                               end,
                               lists:zip(lists:seq(0, (length(PLS0)-1)), PLS0)),
    {_, PLS} = lists:unzip(LA1 ++ LA2),
    %% The call to reduce_pls minimizes the set of VNodes that
    %% need to perform key listings to ensure full coverage of 
    %% all of the bucket's keys. It returns a 2-tuple. The first
    %% element is a dictionary with unique riak nodes as the
    %% keys and the lists of VNodes that particular nodes should
    %% list the keys for as the values. The second element is the
    %% the number of unique responses from VNodes required before
    %% the key listing fsm can transition to the finish state.
    {NodeIndexDict, RequiredResponses} = reduce_pls(PLS),
    %% The call to start_keylisters will start keylister
    %% processes on each node that has a key in NodeIndexDict.
    %% The keylister process is given a list of VNodes that it 
    %% should list the keys for and those VNodes lists are the values 
    %% stored in NodeIndexDict.
    NodeIndexDict1 = start_keylisters(ReqId, Input, NodeIndexDict, Timeout),
    UpNodes = riak_core_node_watcher:nodes(riak_kv),
    StateData = StateData0#state{pls=PLS, 
                                 node_indexes = NodeIndexDict1,
                                 upnodes=UpNodes,
                                 responses=0,
                                 required_responses=RequiredResponses,
                                 n_val=N},
    {next_state, waiting_kl, StateData, Timeout}.
    

waiting_kl({ReqId, {kl, Index, Keys}},
           StateData=#state{from=From={raw, ReqId, _},
                             timeout=Timeout,
                             upnodes=UpNodes,
                             n_val=NVal,
                             ring=Ring,
                             bucket=Bucket,
                             client_type=ClientType}) ->
    process_keys(Index, Keys, Bucket, ClientType, Ring, NVal, UpNodes, From),
    {next_state, waiting_kl, StateData, Timeout};
waiting_kl({ReqId, _Index, done}, StateData0=#state{pls=_PLS,
                                                   from={raw, ReqId, _},
                                                   responses=Responses,
                                                   required_responses=RequiredResponses,
                                                   timeout=Timeout}) ->
    Responses1 = Responses + 1,
    StateData = StateData0#state{responses=Responses1},
    case Responses1 >= RequiredResponses of
        true -> 
            finish(StateData);
        false -> {next_state, waiting_kl, StateData, Timeout}
    end;
waiting_kl(timeout, StateData) ->
    finish(StateData).

finish(StateData=#state{from={raw, ReqId, ClientPid}, client_type=ClientType}) ->
    case ClientType of
        mapred ->
            luke_flow:finish_inputs(ClientPid);
        plain -> 
            ClientPid ! {ReqId, done}
    end,
    {stop,normal,StateData}.

%% ====================================================================
%% Internal functions
%% ====================================================================

reduce_pls([[{Index, Node} | RestPrefList] | RestPrefLists]) ->
    VNodeSet = sets:new(),
    VNodeSet1 = sets:add_element({Index, Node}, VNodeSet),
    NodeIndexDict = dict:new(),
    NodeIndexDict1 = dict:store(Node, [{Index, RestPrefList}], NodeIndexDict),
    reduce_pls(RestPrefLists, NodeIndexDict1, VNodeSet1).

reduce_pls([], NodeIndexDict, VNodeSet) ->
    {NodeIndexDict, sets:size(VNodeSet)};
reduce_pls(HeadPrefList=[[{Index, Node} | RestPrefList] | RestPrefLists], NodeIndexDict, VNodeSet) ->
    case sets:is_disjoint(sets:from_list(HeadPrefList), VNodeSet) of 
        true ->
            %% Check if there an entry for Node in NodeIndexDict
            case dict:is_key(Node, NodeIndexDict) of
                true ->
                    %% An entry for the physical node is already present
                    %% so just append the index information to the value
                    %% for the node.
                    NodeIndexDict1 = dict:append(Node, {Index, RestPrefList}, NodeIndexDict);
                false ->
                    %% This is the first vnode for this physical node 
                    %% so add an entry for the node in NodeIndexList
                    NodeIndexDict1 = dict:store([{Node, [{Index, RestPrefList}]}], NodeIndexDict)                
            end,
            %% Add an entry to the VNode set
            VNodeSet1 = sets:add_element({Index, Node}, VNodeSet),
            reduce_pls(RestPrefLists, NodeIndexDict1, VNodeSet1);
        false ->
            %% A vnode from this preference list has already
            %% been seen so move on to the next preference list.
            reduce_pls(RestPrefLists, NodeIndexDict, VNodeSet)
    end.

start_keylisters(ReqId, Bucket, NodeIndexDict, Timeout) ->
    StartListerFunc = fun(Node, Indexes) ->
                              VNodes = [{Index, Node} || {Index, _} <- Indexes],
                              case riak_kv_keylister_sup:start_keylister(Node, [ReqId, self(), Bucket, VNodes, Timeout]) of
                                  {ok, _Pid} ->
                                      Indexes;                                
                                  {error, Error}->
                                      %% TODO: More robust error handling 
                                      error_logger:warning_msg("Unable to start a keylister process on ~p~n", [Node]),
                                      Indexes
                              end
                      end,
    %% Map over the node keys to start the keylister process on each node 
    dict:map(StartListerFunc, NodeIndexDict).


%% @private
process_keys(Index, Keys, Bucket, ClientType, Ring, NVal, UpNodes, From) ->
    process_keys(Index, Keys, Bucket, ClientType, Ring, NVal, UpNodes, From, []).

%% @private
process_keys(_, [], Bucket, ClientType, _, _, _, {raw,ReqId,ClientPid}, Acc) ->
    case ClientType of
        mapred ->
            try
                luke_flow:add_inputs(ClientPid, [{Bucket,K} || K <- Acc])
            catch _:_ ->
                    exit(self(), normal)
            end;
        plain -> ClientPid ! {ReqId, {keys, Acc}}
    end;
process_keys(Index, [K|Rest], Bucket, ClientType, Ring, NVal, UpNodes, From, Acc) ->
    %% Get the Index for the BKey and check if it's in the list of 
    %% Indexes that have already reported.
    ChashKey = riak_core_util:chash_key({Bucket, K}),
    [HeadPrefList | _] = riak_core_apl:get_apl_ann(ChashKey, NVal, Ring, UpNodes),
    {{HeadPrefListIndex, _}, primary} = HeadPrefList,
    case Index == HeadPrefListIndex of
        true ->            
            process_keys(Index, Rest, Bucket, ClientType, Ring, NVal, UpNodes, From, [K|Acc]);
        false ->
            process_keys(Index, Rest, Bucket, ClientType, Ring, NVal, UpNodes, From, Acc)
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
