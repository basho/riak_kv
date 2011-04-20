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
                bloom :: term(),
                pls :: [list()],
                wait_pls :: [term()],
                simul_pls :: integer(),
                vns :: term(),
                bucket :: riak_object:bucket(),
                input,
                timeout :: pos_integer(),
                ring :: riak_core_ring:riak_core_ring(),
                listers :: [{atom(), pid()}]
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
init([From={raw,ReqId,ClientPid},Input,Timeout,ClientType,ErrorTolerance]) ->
    process_flag(trap_exit, true),
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    {ok, Bloom} = ebloom:new(10000000,ErrorTolerance,ReqId),
    Bucket = case Input of
                 {B, _} ->
                     B;
                 _ ->
                     Input
             end,
    StateData = #state{client_type=ClientType, timeout=Timeout,
                       bloom=Bloom, from=From, input=Input, bucket=Bucket, ring=Ring},
    case ClientType of
        %% Link to the mapred job so we die if the job dies
        mapred ->
            link(ClientPid);
        _ ->
            ok
    end,
    {ok,initialize,StateData,0};
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
initialize(timeout, StateData0=#state{input=_Input, bucket=Bucket, ring=Ring, from={_, _ReqId, _}, timeout=_Timeout}) ->
    BucketProps = riak_core_bucket:get_bucket(Bucket, Ring),
    N = proplists:get_value(n_val,BucketProps),
    PLS0 = riak_core_ring:all_preflists(Ring,N),
    {LA1, LA2} = lists:partition(fun({A,_B}) ->
                                       A rem N == 0 orelse A rem (N + 1) == 0
                               end,
                               lists:zip(lists:seq(0,(length(PLS0)-1)), PLS0)),
    {_, PLS} = lists:unzip(LA1 ++ LA2),
    Simul_PLS = trunc(length(PLS) / N),
    %% Listers = start_listers(ReqId, Input, Timeout),
    StateData = StateData0#state{pls=PLS,simul_pls=Simul_PLS, listers=[],
                                 wait_pls=[],vns=sets:from_list([])},
    reduce_pls(StateData).

waiting_kl({ReqId, {kl, _Idx, Keys}},
           StateData=#state{bloom=Bloom,
                            from=From={raw,ReqId,_},timeout=Timeout,
                            bucket=Bucket,client_type=ClientType}) ->
    process_keys(Keys,Bucket,ClientType,Bloom,From),
    {next_state, waiting_kl, StateData, Timeout};

waiting_kl({ReqId, Idx, done}, StateData0=#state{wait_pls=WPL0,vns=VNS0,pls=PLS,
                                                  from={raw,ReqId,_},timeout=Timeout}) ->
    WPL = [{W_Idx,W_Node,W_PL} || {W_Idx,W_Node,W_PL} <- WPL0, W_Idx /= Idx],
    WNs = [W_Node || {W_Idx,W_Node,_W_PL} <- WPL0, W_Idx =:= Idx],
    Node = case WNs of
        [WN] -> WN;
        _ -> undefined
    end,
    VNS = sets:add_element({Idx,Node},VNS0),
    StateData = StateData0#state{wait_pls=WPL,vns=VNS},
    case PLS of
        [] ->
            case WPL of
                [] -> finish(StateData);
                _ -> {next_state, waiting_kl, StateData, Timeout}
            end;
        _ -> reduce_pls(StateData)
    end;

waiting_kl(timeout, StateData=#state{pls=PLS,wait_pls=WPL}) ->
    NewPLS = lists:append(PLS, [W_PL || {_W_Idx,_W_Node,W_PL} <- WPL]),
    reduce_pls(StateData#state{pls=NewPLS,wait_pls=[]}).

finish(StateData=#state{from={raw,ReqId,ClientPid},client_type=ClientType, vns=VNS}) ->
    VnsList = sets:to_list(VNS),
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

reduce_pls(StateData0=#state{timeout=Timeout, wait_pls=WPL, bucket=Bucket, from={raw,ReqId,_}, listers=Listers,
                             simul_pls=Simul_PLS}) ->
    case find_free_pl(StateData0) of
        {none_free,NewPLS} ->
            StateData = StateData0#state{pls=NewPLS},
            case NewPLS =:= [] andalso WPL =:= [] of
                true -> finish(StateData);
                false -> {next_state, waiting_kl, StateData, Timeout}
            end;
        {[{Idx,Node}|RestPL],PLS} ->
            case proplists:get_value(Node, Listers) of
                undefined ->
                    case riak_kv_keylister_sup:start_keylister(Node, [ReqId, self(), Bucket, {Idx, Node}]) of
                        {ok, LPid} ->
                            %% Send the keylist request to the lister
                            %% riak_kv_keylister:list_keys(LPid, {Idx, Node}),
                            WaitPLS = [{Idx,Node,RestPL}|WPL],
                            StateData = StateData0#state{pls=PLS, wait_pls=WaitPLS, listers=[{Node, LPid} | Listers]},
                            case length(WaitPLS) > Simul_PLS of
                                true ->
                                    {next_state, waiting_kl, StateData, Timeout};
                                false ->
                                    reduce_pls(StateData)
                            end;
                        _Error ->
                            %% Node is down or hasn't been removed from preflists yet
                            %% Log a warning, skip the node and continue sending
                            %% out key list requests
                            error_logger:warning_msg("Skipping keylist request for unknown node: ~p~n", [Node]),
                            WaitPLS = [{Idx,Node,RestPL}|WPL],
                            StateData = StateData0#state{pls=PLS, wait_pls=WaitPLS},
                            reduce_pls(StateData)
                    end;
                Pid ->
                    %% Send the keylist request to the lister
                    riak_kv_keylister:list_keys(Pid, {Idx, Node}),
                    WaitPLS = [{Idx,Node,RestPL}|WPL],
                    StateData = StateData0#state{pls=PLS, wait_pls=WaitPLS},
                    case length(WaitPLS) > Simul_PLS of
                        true ->
                            {next_state, waiting_kl, StateData, Timeout};
                        false ->
                            reduce_pls(StateData)
                    end

            end
    end.

find_free_pl(StateData) -> find_free_pl1(StateData, []).
find_free_pl1(_StateData=#state{pls=[]}, NotFree) -> {none_free,NotFree};
find_free_pl1(StateData=#state{wait_pls=WPL,pls=[PL|PLS],vns=VNS}, NotFree) ->
    case PL of
        [] -> find_free_pl1(StateData#state{pls=PLS}, NotFree);
        _ ->
            case check_pl(PL,VNS,WPL) of
                redundant -> find_free_pl1(StateData#state{pls=PLS},NotFree);
                notfree -> find_free_pl1(StateData#state{pls=PLS},[PL|NotFree]);
                free -> {PL,lists:append(PLS,NotFree)}
            end
    end.

check_pl(PL,VNS,WPL) ->
    case sets:is_disjoint(sets:from_list(PL),VNS) of
        false -> 
            redundant;
        true ->
            PL_Nodes = sets:from_list([Node || {_Idx,Node} <- PL]),
            WaitNodes = sets:from_list([Node || {_Idx,Node,_RestPL} <- WPL]),
            case sets:is_disjoint(PL_Nodes,WaitNodes) of
                false -> notfree;
                true -> free
            end
    end.

%% @private
process_keys(Keys,Bucket,ClientType,Bloom,From) ->
    process_keys(Keys,Bucket,ClientType,Bloom,From,[]).
%% @private
process_keys([],Bucket,ClientType,_Bloom,{raw,ReqId,ClientPid},Acc) ->
    case ClientType of
        mapred ->
            try
                luke_flow:add_inputs(ClientPid, [{Bucket,K} || K <- Acc])
            catch _:_ ->
                    exit(self(), normal)
            end;
        plain -> ClientPid ! {ReqId, {keys, Acc}}
    end,
    ok;
process_keys([K|Rest],Bucket,ClientType,Bloom,From,Acc) ->
    case ebloom:contains(Bloom,K) of
        true ->
            process_keys(Rest,Bucket,ClientType,
                         Bloom,From,Acc);
        false ->
            ebloom:insert(Bloom,K),
            process_keys(Rest,Bucket,ClientType,
                         Bloom,From,[K|Acc])
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
terminate(Reason, _StateName, #state{bloom=Bloom}) ->
    ebloom:clear(Bloom),
    Reason.

%% @private
code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.
