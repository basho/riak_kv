%% -------------------------------------------------------------------
%%
%% riak_get_fsm: coordination of Riak GET requests
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

-module(riak_kv_get_fsm).
-behaviour(gen_fsm).
-include_lib("riak_kv_vnode.hrl").
-export([start/6]).
-export([init/1, handle_event/3, handle_sync_event/4,
         handle_info/3, terminate/3, code_change/4]).
-export([prepare/2,execute/2,waiting_vnode_r/2,waiting_read_repair/2]).

-record(state, {client :: {pid(), reference()},
                n :: pos_integer(),
                r :: pos_integer(),
                fail_threshold :: pos_integer(),
                allowmult :: boolean(),
                preflist2 :: riak_core_apl:preflist2(),
                waiting_for=[] :: [{pos_integer(), atom(), atom()}],
                req_id :: pos_integer(),
                starttime :: pos_integer(),
                replied_r = [] :: list(),
                replied_notfound = [] :: list(),
                replied_fail = [] :: list(),
                num_r = 0,
                num_notfound = 0,
                num_fail = 0,
                repair_sent = [] :: list(),
                final_obj :: undefined | {ok, riak_object:riak_object()} |
                             tombstone | {error, notfound},
                timeout :: pos_integer(),
                tref    :: reference(),
                bkey :: {riak_object:bucket(), riak_object:key()},
                bucket_props,
                startnow :: {pos_integer(), pos_integer(), pos_integer()}
               }).

start(ReqId,Bucket,Key,R,Timeout,From) ->
    gen_fsm:start(?MODULE, [ReqId,Bucket,Key,R,Timeout,From], []).

%% @private
init([ReqId,Bucket,Key,R,Timeout,Client]) ->
    StateData = #state{client=Client,r=R, timeout=Timeout,
                req_id=ReqId, bkey={Bucket,Key}},
    {ok,prepare,StateData,0}.

prepare(timeout, StateData=#state{bkey=BKey={Bucket,_Key}}) ->
    StartNow = now(),
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    BucketProps = riak_core_bucket:get_bucket(Bucket, Ring),
    DocIdx = riak_core_util:chash_key(BKey),
    N = proplists:get_value(n_val,BucketProps),
    UpNodes = riak_core_node_watcher:nodes(riak_kv),
    Preflist2 = riak_core_apl:get_apl2(DocIdx, N, Ring, UpNodes),
    {next_state, execute, StateData#state{starttime=riak_core_util:moment(),
                                          n = N,
                                          bucket_props=BucketProps,
                                          preflist2 = Preflist2,
                                          startnow=StartNow}, 0}.

%% @private
execute(timeout, StateData0=#state{timeout=Timeout, n=N, r=R0, req_id=ReqId,
                                   bkey=BKey, 
                                   bucket_props=BucketProps,
                                   preflist2 = Preflist2}) ->
    TRef = schedule_timeout(Timeout),
    R = riak_kv_util:expand_rw_value(r, R0, BucketProps, N),
    FailThreshold = erlang:min((N div 2)+1, % basic quorum, or
                               (N-R+1)), % cannot ever get R 'ok' replies
    case R > N of
        true ->
            client_reply({error, {n_val_violation, N}}, StateData0),
            {stop, normal, StateData0};
        false ->
            AllowMult = proplists:get_value(allow_mult,BucketProps),
            Preflist = [IndexNode || {IndexNode, _Type} <- Preflist2],
            riak_kv_vnode:get(Preflist, BKey, ReqId),
            StateData = StateData0#state{n=N,r=R,fail_threshold=FailThreshold,
                                         allowmult=AllowMult,
                                         waiting_for=Preflist2,tref=TRef},
            {next_state,waiting_vnode_r,StateData}
    end.

waiting_vnode_r({r, VnodeResult, Idx, _ReqId}, StateData) ->
    NewStateData1 = add_vnode_result(Idx, VnodeResult, StateData),
    case enough_results(NewStateData1) of
        {reply, Reply, NewStateData2} ->
            client_reply(Reply, NewStateData2),
            update_stats(NewStateData2),
            finalize(NewStateData2);
        {false, NewStateData2} ->
            {next_state, waiting_vnode_r, NewStateData2}
    end;
waiting_vnode_r(timeout, StateData=#state{replied_r=Replied,allowmult=AllowMult}) ->
    update_stats(StateData),
    client_reply({error,timeout}, StateData),
    really_finalize(StateData#state{final_obj=merge(Replied, AllowMult)}).

waiting_read_repair({r, VnodeResult, Idx, _ReqId}, StateData) ->
    NewStateData1 = add_vnode_result(Idx, VnodeResult, StateData),
    finalize(NewStateData1#state{final_obj = undefined});
waiting_read_repair(timeout, StateData) ->
    really_finalize(StateData).

has_all_replies(#state{replied_r=R,replied_fail=F,replied_notfound=NF, n=N}) ->
    length(R) + length(F) + length(NF) >= N.

finalize(StateData=#state{replied_r=[]}) ->
    case has_all_replies(StateData) of
        true -> {stop,normal,StateData};
        false -> {next_state,waiting_read_repair,StateData}
    end;
finalize(StateData) ->
    case has_all_replies(StateData) of
        true -> really_finalize(StateData);
        false -> {next_state,waiting_read_repair,StateData}
    end.

really_finalize(StateData=#state{allowmult = AllowMult,
                                 final_obj = FinalObj,
                                 waiting_for=Sent,
                                 replied_r=RepliedR,
                                 bkey=BKey,
                                 req_id=ReqId,
                                 replied_notfound=NotFound,
                                 starttime=StartTime}) ->
    Final = case FinalObj of
                undefined -> %% Recompute if extra read repairs have arrived
                    merge(RepliedR,AllowMult);
                _ ->
                    FinalObj
            end,
    case Final of
        tombstone ->
            maybe_finalize_delete(StateData);
        {ok,_} ->
            maybe_do_read_repair(Sent,Final,RepliedR,NotFound,BKey,
                                 ReqId,StartTime);
        _ -> nop
    end,
    {stop,normal,StateData}.

maybe_finalize_delete(_StateData=#state{replied_notfound=NotFound,n=N,
                                        replied_r=RepliedR,
                                        waiting_for=Sent,req_id=ReqId,
                                        bkey=BKey}) ->
    IdealNodes = [{I,Node} || {{I,Node},primary} <- Sent],
    case length(IdealNodes) of
        N -> % this means we sent to a perfect preflist
            case (length(RepliedR) + length(NotFound)) of
                N -> % and we heard back from all nodes with non-failure
                    case lists:all(fun(X) -> riak_kv_util:is_x_deleted(X) end,
                                   [O || {O,_I} <- RepliedR]) of
                        true -> % and every response was X-Deleted, go!
                            riak_kv_vnode:del(IdealNodes, BKey, ReqId);
                        _ -> nop
                    end;
                _ -> nop
            end;
        _ -> nop
    end.

maybe_do_read_repair(Sent,Final,RepliedR,NotFound,BKey,ReqId,StartTime) ->
    Targets = ancestor_indices(Final, RepliedR) ++ NotFound,
    {ok, FinalRObj} = Final,
    case Targets of
        [] -> nop;
        _ ->
            RepairPreflist = [{Idx, Node} || {{Idx,Node},_Type} <- Sent, 
                                            lists:member(Idx, Targets)],
            riak_kv_vnode:readrepair(RepairPreflist, BKey, FinalRObj, ReqId, 
                                     StartTime, [{returnbody, false}]),
            riak_kv_stat:update(read_repairs)
    end.


%% @private
handle_event(_Event, _StateName, StateData) ->
    {stop,badmsg,StateData}.

%% @private
handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop,badmsg,StateData}.

%% @private
handle_info(timeout, StateName, StateData) ->
    ?MODULE:StateName(timeout, StateData);
%% @private
handle_info(_Info, _StateName, StateData) ->
    {stop,badmsg,StateData}.

%% @private
terminate(Reason, _StateName, _State) ->
    Reason.

%% @private
code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.

add_vnode_result(Idx, {ok, RObj}, StateData = #state{replied_r = Replied,
                                                     num_r = NumR}) ->
    StateData#state{replied_r = [{RObj, Idx} | Replied],
                    num_r = NumR + 1};
add_vnode_result(Idx, {error, notfound}, StateData = #state{replied_notfound = NotFound,
                                                            num_notfound = NumNotFound}) ->
    StateData#state{replied_notfound = [Idx | NotFound],
                    num_notfound = NumNotFound + 1};
add_vnode_result(Idx, {error, Err}, StateData = #state{replied_fail = Fail,
                                                       num_fail = NumFail}) ->
    StateData#state{replied_fail = [{Err, Idx} | Fail],
                   num_fail = NumFail + 1}.

enough_results(StateData = #state{r = R, allowmult = AllowMult,
                                  fail_threshold = FailThreshold,
                                  replied_r = Replied, num_r = NumR,
                                  replied_notfound = NotFound, num_notfound = NumNotFound,
                                  replied_fail = Fails, num_fail = NumFail}) ->
    if
        NumR >= R ->
            {Reply, Final} = respond(Replied, AllowMult),
            {reply, Reply, StateData#state{final_obj = Final}};
        NumNotFound + NumFail >= FailThreshold ->
            Reply = case length(NotFound) of
                        0 ->
                            {error, [E || {E,_I} <- Fails]};
                        _ ->
                            {error, notfound}
                    end,
            Final = merge(Replied, AllowMult),
            {reply, Reply, StateData#state{final_obj = Final}};
        true ->
            {false, StateData}
    end.
                
    
schedule_timeout(infinity) ->
    undefined;
schedule_timeout(Timeout) ->
    erlang:send_after(Timeout, self(), timeout).

client_reply(Reply, #state{client = Client, req_id = ReqId}) ->
    Client ! {ReqId, Reply}.

merge(VResponses, AllowMult) ->
   merge_robjs([R || {R,_I} <- VResponses],AllowMult).

respond(VResponses,AllowMult) ->
    Merged = merge(VResponses, AllowMult),
    case Merged of
        tombstone ->
            Reply = {error,notfound};
        {ok, Obj} ->
            case riak_kv_util:is_x_deleted(Obj) of
                true ->
                    Reply = {error, notfound};
                false ->
                    Reply = {ok, Obj}
            end;
        X ->
            Reply = X
    end,
    {Reply, Merged}.

merge_robjs([], _) ->
    {error, notfound};
merge_robjs(RObjs0,AllowMult) ->
    RObjs1 = [X || X <- [riak_kv_util:obj_not_deleted(O) ||
                            O <- RObjs0], X /= undefined],
    case RObjs1 of
        [] -> tombstone;
        _ ->
            RObj = riak_object:reconcile(RObjs0,AllowMult),
            {ok, RObj}
    end.

strict_descendant(O1, O2) ->
    vclock:descends(riak_object:vclock(O1),riak_object:vclock(O2)) andalso
    not vclock:descends(riak_object:vclock(O2),riak_object:vclock(O1)).

ancestor_indices({ok, Final},AnnoObjects) ->
    [Idx || {O,Idx} <- AnnoObjects, strict_descendant(Final, O)].


update_stats(#state{startnow=StartNow}) ->
    EndNow = now(),
    riak_kv_stat:update({get_fsm_time, timer:now_diff(EndNow, StartNow)}).    
