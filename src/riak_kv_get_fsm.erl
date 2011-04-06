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
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-export([test_link/7, test_link/5]).
-endif.
-export([start/6, start_link/6, start_link/4]).
-export([init/1, handle_event/3, handle_sync_event/4,
         handle_info/3, terminate/3, code_change/4]).
-export([prepare/2,validate/2,execute/2,waiting_vnode_r/2,waiting_read_repair/2]).

-type option() :: {r, pos_integer()} |
                  {timeout, pos_integer() | infinity}.
-type options() :: [option()].

-record(state, {from :: {integer(), pid()},
                options=[] :: options(),
                n :: pos_integer(),
                r :: pos_integer(),
                fail_threshold :: pos_integer(),
                allowmult :: boolean(),
                notfound_ok :: boolean(),
                preflist2 :: riak_core_apl:preflist2(),
                req_id :: pos_integer(),
                starttime :: pos_integer(),
                replied_r = [] :: list(),
                replied_notfound = [] :: list(),
                replied_fail = [] :: list(),
                num_r = 0,
                num_notfound = 0,
                num_fail = 0,
                final_obj :: undefined | {ok, riak_object:riak_object()} |
                             tombstone | {error, notfound},
                timeout :: infinity | pos_integer(),
                tref    :: reference(),
                bkey :: {riak_object:bucket(), riak_object:key()},
                bucket_props,
                startnow :: {pos_integer(), pos_integer(), pos_integer()},
                get_usecs :: undefined | non_neg_integer()
               }).

-define(DEFAULT_TIMEOUT, 60000).
-define(DEFAULT_R, default).
-define(DEFAULT_PR, 0).

%% In place only for backwards compatibility
start(ReqId,Bucket,Key,R,Timeout,From) ->
    start_link({raw, ReqId, From}, Bucket, Key, [{r, R}, {timeout, Timeout}]).

start_link(ReqId,Bucket,Key,R,Timeout,From) ->
    start_link({raw, ReqId, From}, Bucket, Key, [{r, R}, {timeout, Timeout}]).

start_link(From, Bucket, Key, GetOptions) ->
    gen_fsm:start_link(?MODULE, [From, Bucket, Key, GetOptions], []).

-ifdef(TEST).
%% Create a get FSM for testing.  StateProps must include
%% starttime - start time in gregorian seconds
%% n - N-value for request (is grabbed from bucket props in prepare)
%% bucket_props - bucket properties
%% preflist2 - [{{Idx,Node},primary|fallback}] preference list
%% 
test_link(ReqId,Bucket,Key,R,Timeout,From,StateProps) ->
    test_link({raw, ReqId, From}, Bucket, Key, [{r, R}, {timeout, Timeout}], StateProps).

test_link(From, Bucket, Key, GetOptions, StateProps) ->
    gen_fsm:start_link(?MODULE, {test, [From, Bucket, Key, GetOptions], StateProps}, []).

-endif.

%% @private
init([From, Bucket, Key, Options]) ->
    StartNow = now(),
    StateData = #state{from = From,
                       options = Options,
                       bkey = {Bucket, Key},
                       startnow = StartNow},
    {ok, prepare, StateData, 0};
init({test, Args, StateProps}) ->
    %% Call normal init
    {ok, prepare, StateData, 0} = init(Args),

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
    {ok, validate, TestStateData, 0}.

%% @private
prepare(timeout, StateData=#state{bkey=BKey={Bucket,_Key}}) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    BucketProps = riak_core_bucket:get_bucket(Bucket, Ring),
    DocIdx = riak_core_util:chash_key(BKey),
    N = proplists:get_value(n_val,BucketProps),
    UpNodes = riak_core_node_watcher:nodes(riak_kv),
    Preflist2 = riak_core_apl:get_apl_ann(DocIdx, N, Ring, UpNodes),
    {next_state, validate, StateData#state{starttime=riak_core_util:moment(),
                                          n = N,
                                          bucket_props=BucketProps,
                                          preflist2 = Preflist2}, 0}.

validate(timeout, StateData=#state{from = {raw, ReqId, _Pid}, options = Options,
                                   n = N, bucket_props = BucketProps, preflist2 = PL2}) ->
    Timeout = get_option(timeout, Options, ?DEFAULT_TIMEOUT),
    R0 = get_option(r, Options, ?DEFAULT_R),
    PR0 = get_option(pr, Options, ?DEFAULT_PR),
    R = riak_kv_util:expand_rw_value(r, R0, BucketProps, N),
    PR = riak_kv_util:expand_rw_value(pr, PR0, BucketProps, N),
    NumPrimaries = length([x || {_,primary} <- PL2]),
    if
        R =:= error ->
            client_reply({error, {r_val_violation, R0}}, StateData),
            {stop, normal, StateData};
        R > N ->
            client_reply({error, {n_val_violation, N}}, StateData),
            {stop, normal, StateData};
        PR =:= error ->
            client_reply({error, {pr_val_violation, PR0}}, StateData),
            {stop, normal, StateData};
        PR > N ->
            client_reply({error, {n_val_violation, N}}, StateData),
            {stop, normal, StateData};
        PR > NumPrimaries ->
            client_reply({error, {pr_val_unsatisfied, PR, NumPrimaries}}, StateData),
            {stop, normal, StateData};
        true ->
            FailThreshold = 
                case get_option(basic_quorum, Options, true) of
                    true ->
                        erlang:min((N div 2)+1, % basic quorum, or
                                   (N-R+1)); % cannot ever get R 'ok' replies
                    false ->
                        N - R + 1 % cannot ever get R 'ok' replies
                end,
            AllowMult = proplists:get_value(allow_mult,BucketProps),
            NotFoundOk = get_option(notfound_ok, Options, false),
            {next_state, execute, StateData#state{r = R,
                                                  fail_threshold = FailThreshold,
                                                  timeout = Timeout,
                                                  allowmult = AllowMult,
                                                  notfound_ok = NotFoundOk,
                                                  req_id = ReqId}, 0}
    end.

%% @private
execute(timeout, StateData0=#state{timeout=Timeout,req_id=ReqId,
                                   bkey=BKey, 
                                   
                                   preflist2 = Preflist2}) ->
    TRef = schedule_timeout(Timeout),
    Preflist = [IndexNode || {IndexNode, _Type} <- Preflist2],
    riak_kv_vnode:get(Preflist, BKey, ReqId),
    StateData = StateData0#state{tref=TRef},
    {next_state,waiting_vnode_r,StateData}.

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
waiting_vnode_r(request_timeout, StateData=#state{replied_r=Replied,allowmult=AllowMult}) ->
    update_stats(StateData),
    client_reply({error,timeout}, StateData),
    really_finalize(StateData#state{final_obj=merge(Replied, AllowMult)}).

waiting_read_repair({r, VnodeResult, Idx, _ReqId}, StateData) ->
    NewStateData1 = add_vnode_result(Idx, VnodeResult, StateData),
    finalize(NewStateData1#state{final_obj = undefined});
waiting_read_repair(request_timeout, StateData) ->
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
                                 preflist2=Sent,
                                 replied_r=RepliedR,
                                 bkey=BKey,
                                 req_id=ReqId,
                                 replied_notfound=NotFound,
                                 starttime=StartTime,
                                 bucket_props=BucketProps}) ->
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
                                 ReqId,StartTime,BucketProps);
        _ -> nop
    end,
    {stop,normal,StateData}.

maybe_finalize_delete(_StateData=#state{replied_notfound=NotFound,n=N,
                                        replied_r=RepliedR,
                                        preflist2=Sent,req_id=ReqId,
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

maybe_do_read_repair(Sent,Final,RepliedR,NotFound,BKey,ReqId,StartTime,BucketProps) ->
    Targets = ancestor_indices(Final, RepliedR) ++ NotFound,
    {ok, FinalRObj} = Final,
    case Targets of
        [] -> nop;
        _ ->
            RepairPreflist = [{Idx, Node} || {{Idx,Node},_Type} <- Sent, 
                                            lists:member(Idx, Targets)],
            riak_kv_vnode:readrepair(RepairPreflist, BKey, FinalRObj, ReqId, 
                                     StartTime, [{returnbody, false},
                                                 {bucket_props, BucketProps}]),
            riak_kv_stat:update(read_repairs)
    end.


%% @private
handle_event(_Event, _StateName, StateData) ->
    {stop,badmsg,StateData}.

%% @private
handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop,badmsg,StateData}.

%% @private
handle_info(request_timeout, StateName, StateData) ->
    ?MODULE:StateName(request_timeout, StateData);
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
                                                            num_notfound = NumNotFound,
                                                            notfound_ok = false}) ->
    StateData#state{replied_notfound = [Idx | NotFound],
                    num_notfound = NumNotFound + 1};
add_vnode_result(Idx, {error, notfound}, StateData = #state{replied_notfound = NotFound,
                                                            num_r = NumR,
                                                            notfound_ok = true}) ->
    StateData#state{replied_notfound = [Idx | NotFound],
                    num_r = NumR + 1};
add_vnode_result(Idx, {error, Err}, StateData = #state{replied_fail = Fail,
                                                       num_fail = NumFail}) ->
    StateData#state{replied_fail = [{Err, Idx} | Fail],
                   num_fail = NumFail + 1}.

enough_results(StateData = #state{r = R, allowmult = AllowMult,
                                  fail_threshold = FailThreshold,
                                  replied_r = Replied, num_r = NumR,
                                  num_notfound = NumNotFound,
                                  replied_fail = Fails, num_fail = NumFail}) ->
    if
        NumR >= R ->
            {Reply, Final} = respond(Replied, AllowMult),
            {reply, Reply, update_timing(StateData#state{final_obj = Final})};
        NumNotFound + NumFail >= FailThreshold ->
            DelObjs = length([xx || {RObj, _Idx} <- Replied, riak_kv_util:is_x_deleted(RObj)]),
            Reply = fail_reply(R, NumR, NumR - DelObjs, NumNotFound + DelObjs, Fails),
            Final = merge(Replied, AllowMult),
            {reply, Reply, update_timing(StateData#state{final_obj = Final})};
        true ->
            {false, StateData}
    end.
                    
fail_reply(_R, _NumR, 0, NumNotFound, []) when NumNotFound > 0 ->
    {error, notfound};
fail_reply(R, NumR, _NumNotDeleted, _NumNotFound, _Fails) ->
    {error, {r_val_unsatisfied, R,  NumR}}.

get_option(Name, Options, Default) ->
    proplists:get_value(Name, Options, Default).

schedule_timeout(infinity) ->
    undefined;
schedule_timeout(Timeout) ->
    erlang:send_after(Timeout, self(), request_timeout).

client_reply(Reply, StateData = #state{from = {raw, ReqId, Pid}, options = Options}) ->
    Msg = case proplists:get_value(details, Options, false) of
              false ->
                  {ReqId, Reply};
              [] ->
                  {ReqId, Reply};
              Details ->
                  {OkError, ObjReason} = Reply,
                  Info = client_info(Details, StateData, []),
                  {ReqId, {OkError, ObjReason, Info}}
          end,
    Pid ! Msg. 

respond(VResponses,AllowMult) ->
    Merged = merge(VResponses, AllowMult),
    case Merged of
        tombstone ->
            Reply = {error,notfound};
        {error, notfound} ->
            Reply = Merged;
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

merge(VResponses, AllowMult) ->
   merge_robjs([R || {R,_I} <- VResponses],AllowMult).

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

update_timing(StateData = #state{startnow = StartNow}) ->
    EndNow = now(),
    StateData#state{get_usecs = timer:now_diff(EndNow, StartNow)}.

update_stats(#state{get_usecs = GetUsecs}) ->
    riak_kv_stat:update({get_fsm_time, GetUsecs}).    

client_info(true, StateData, Acc) ->
    client_info(details(), StateData, Acc);
client_info([], _StateData, Acc) ->
    Acc;
client_info([timing | Rest], StateData = #state{get_usecs = GetUsecs}, Acc) ->
    client_info(Rest, StateData, [{duration, GetUsecs} | Acc]);
client_info([vnodes | Rest], StateData = #state{num_r = NumOks,
                                                replied_fail = Fail}, Acc) ->
    Oks = [{vnode_oks, NumOks}],
    Info = case Fail of
               [] ->
                   Oks;
               _ ->
                   Errors = [Err || {Err, _Idx} <- Fail],
                   [{vnode_errors, Errors} | Oks]
           end,
    client_info(Rest, StateData, Info ++ Acc);
client_info([Unknown | Rest], StateData, Acc) ->
    client_info(Rest, StateData, [{Unknown, unknown_detail} | Acc]).

details() ->
    [timing,
     vnodes].

-ifdef(TEST).
-define(expect_msg(Exp,Timeout), 
        ?assertEqual(Exp, receive Exp -> Exp after Timeout -> timeout end)).

get_fsm_test_() ->
    {spawn, [{ setup,
               fun setup/0,
               fun cleanup/1,
               [
                fun happy_path_case/0,
                fun n_val_violation_case/0
               ]
             }]}.

setup() ->
    %% Set infinity timeout for the vnode inactivity timer so it does not
    %% try to handoff.
    application:load(riak_core),
    application:set_env(riak_core, vnode_inactivity_timeout, infinity),
    application:load(riak_kv),
    application:set_env(riak_kv, storage_backend, riak_kv_ets_backend),

    %% Have tracer on hand to grab any traces we want
    riak_core_tracer:start_link(),
    riak_core_tracer:reset(),
    riak_core_tracer:filter([{riak_kv_vnode, readrepair}],
                   fun({trace, _Pid, call,
                        {riak_kv_vnode, readrepair, 
                         [Preflist, _BKey, Obj, ReqId, _StartTime, _Options]}}) ->
                           [{rr, Preflist, Obj, ReqId}]
                   end),
    ok.

cleanup(_) ->
    application:unload(riak_kv),
    application:unload(riak_core),
    dbg:stop_clear().

happy_path_case() ->
    riak_core_tracer:collect(5000),
    
    %% Start 3 vnodes
    Indices = [1, 2, 3],
    Preflist2 = [begin 
                     {ok, Pid} = riak_kv_vnode:test_vnode(Idx),
                     {{Idx, Pid}, primary}
                 end || Idx <- Indices],
    Preflist = [IdxPid || {IdxPid,_Type} <- Preflist2],

    %% Decide on some parameters
    Bucket = <<"mybucket">>,
    Key = <<"mykey">>,
    Nval = 3,
    BucketProps = bucket_props(Bucket, Nval),

    %% Start the FSM to issue a get and  check notfound

    ReqId1 = 112381838, % erlang:phash2(erlang:now()).
    R = 2,
    Timeout = 1000,
    {ok, _FsmPid1} = test_link(ReqId1, Bucket, Key, R, Timeout, self(),
                               [{starttime, 63465712389},
                               {n, Nval},
                               {bucket_props, BucketProps},
                               {preflist2, Preflist2}]),
    ?assertEqual({error, notfound}, wait_for_reqid(ReqId1, Timeout + 1000)),
   
    %% Update the first two vnodes with a value
    ReqId2 = 49906465,
    Value = <<"value">>,
    Obj1 = riak_object:new(Bucket, Key, Value),
    riak_kv_vnode:put(lists:sublist(Preflist, 2), {Bucket, Key}, Obj1, ReqId2,
                      63465715958, [{bucket_props, BucketProps}], {raw, ReqId2, self()}),
    ?expect_msg({ReqId2, {w, 1, ReqId2}}, Timeout + 1000),
    ?expect_msg({ReqId2, {w, 2, ReqId2}}, Timeout + 1000),
    ?expect_msg({ReqId2, {dw, 1, ReqId2}}, Timeout + 1000),
    ?expect_msg({ReqId2, {dw, 2, ReqId2}}, Timeout + 1000),
                     
    %% Issue a get, check value returned.
    ReqId3 = 30031523,
    {ok, _FsmPid2} = test_link(ReqId3, Bucket, Key, R, Timeout, self(),
                              [{starttime, 63465712389},
                               {n, Nval},
                               {bucket_props, BucketProps},
                               {preflist2, Preflist2}]),
    ?assertEqual({ok, Obj1}, wait_for_reqid(ReqId3, Timeout + 1000)),

    %% Check readrepair issued to third node
    ExpRRPrefList = lists:sublist(Preflist, 3, 1),
    riak_kv_test_util:wait_for_pid(_FsmPid2),
    riak_core_tracer:stop_collect(),
    ?assertEqual([{0, {rr, ExpRRPrefList, Obj1, ReqId3}}],
                 riak_core_tracer:results()).


n_val_violation_case() ->
    ReqId1 = 13210434, % erlang:phash2(erlang:now()).
    Bucket = <<"mybucket">>,
    Key = <<"badnvalkey">>,
    Nval = 3,
    R = 5,
    Timeout = 1000,
    BucketProps = bucket_props(Bucket, Nval),
    %% Fake three nodes
    Indices = [1, 2, 3],
    Preflist2 = [begin 
                     {{Idx, self()}, primary}
                 end || Idx <- Indices],
    {ok, _FsmPid1} = test_link(ReqId1, Bucket, Key, R, Timeout, self(),
                               [{starttime, 63465712389},
                               {n, Nval},
                               {bucket_props, BucketProps},
                               {preflist2, Preflist2}]),
    ?assertEqual({error, {n_val_violation, 3}}, wait_for_reqid(ReqId1, Timeout + 1000)).
 
    
wait_for_reqid(ReqId, Timeout) ->
    receive
        {ReqId, Msg} -> Msg
    after Timeout ->
            {error, req_timeout}
    end.

bucket_props(Bucket, Nval) -> % riak_core_bucket:get_bucket(Bucket).
    [{name, Bucket},
     {allow_mult,false},
     {big_vclock,50},
     {chash_keyfun,{riak_core_util,chash_std_keyfun}},
     {dw,quorum},
     {last_write_wins,false},
     {linkfun,{modfun,riak_kv_wm_link_walker,mapreduce_linkfun}},
     {n_val,Nval},
     {old_vclock,86400},
     {postcommit,[]},
     {precommit,[]},
     {r,quorum},
     {rw,quorum},
     {small_vclock,10},
     {w,quorum},
     {young_vclock,20}].
 

-endif.
