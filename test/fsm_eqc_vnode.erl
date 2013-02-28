%% -------------------------------------------------------------------
%%
%% fsm_eqc_vnode: mock vnode for get/put FSM testing
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
%%
%% Mock vnode for FSM testing.  Originally tried to use riak_core_vnode
%% directly but we need the index for the tests and no clean way to do
%% the sync events for resetting, so for now just use a gen_fsm.
%%
%% -------------------------------------------------------------------

-module(fsm_eqc_vnode).
-behaviour(gen_fsm).
-include_lib("riak_kv_vnode.hrl").

-export([start_link/0, start_link/1, set_data/2, set_vput_replies/1, 
         get_history/0, get_put_history/0,
         get_reply_history/0, log_postcommit/1, get_postcommits/0]).
-export([init/1, 
         active/2, 
         active/3, 
         handle_event/3,
         handle_sync_event/4, 
         handle_info/3, 
         terminate/3, 
         code_change/4]).

-record(state, {objects, partvals,

                %% Put request handling.
                %% Before running the put FSM the test sets the mock vnode up 
                %% with responses for each vnode.  Rather than
                %% hashing the key and working out a preference
                %% list, responses are created for 'logical' indices
                %% from 1..N.  Each logical index will have one or two
                %% replies (only one if the first is a timeout).  
                lidx_map=[],
                vput_replies=[],

                %% What happened during test
                reply_history=[],
                history=[], put_history=[],
                postcommit=[]}).

%% ===================================================================
%% Test API
%% ===================================================================

start_link() ->
    start_link(?MODULE).

start_link(undefined) ->
    gen_fsm:start_link(?MODULE, [], []);
start_link(RegName) ->
    gen_fsm:start_link({local, RegName}, ?MODULE, [], []).

set_data(Objs, PartVals) ->
    ok = gen_fsm:sync_send_all_state_event(?MODULE, {set_data, Objs, PartVals}).

set_vput_replies(VPutReplies) ->
    ok = gen_fsm:sync_send_all_state_event(?MODULE, {set_vput_replies, VPutReplies}).

get_history() ->
    gen_fsm:sync_send_all_state_event(?MODULE, get_history).

get_put_history() ->
    gen_fsm:sync_send_all_state_event(?MODULE, get_put_history).

get_reply_history() ->
    gen_fsm:sync_send_all_state_event(?MODULE, get_reply_history).

log_postcommit(Obj) ->
    gen_fsm:sync_send_all_state_event(?MODULE, {log_postcommit, Obj}).
    
get_postcommits() ->
    gen_fsm:sync_send_all_state_event(?MODULE, get_postcommits).

%% ====================================================================
%% gen_fsm callbacks
%% ====================================================================

init([]) ->
    {ok, active, #state{}}.

active(?VNODE_REQ{index=Idx,
                  sender=Sender,
                  request=?KV_GET_REQ{req_id=ReqId}}, State) ->
    {Value, State1} = get_data(Idx,State),
    case Value of
        {error, timeout} ->
            ok;
        _ ->
            riak_core_vnode:reply(Sender, {r, Value, Idx, ReqId})
    end,
    %% Speed things up by banging along a timeout if the last entry
    case State1#state.partvals of
        [] ->
            {fsm, undefined, Pid} = Sender,
            Pid ! request_timeout;
        _ ->
            ok
    end,
    {next_state, active, State1};
%% Handle a put request - send the responses prepared by call to
%% 'set_vput_replies' up until the next 'w' response for a different partition.
active(?VNODE_REQ{index=Idx,
                  sender=Sender,
                  request=?KV_PUT_REQ{req_id=ReqId,
                                      object = PutObj,
                                      options = Options}=Msg}, State) ->
    %% Send up to the next w or timeout message, substituting indices
    State1 = send_vput_replies(State#state.vput_replies, Idx,
                               Sender, ReqId, PutObj, Options, State),
    {next_state, active, State1#state{put_history=[{Idx,Msg}|State#state.put_history]}};    
active(?VNODE_REQ{index=Idx, request=?KV_DELETE_REQ{}=Msg}, State) ->
    {next_state, active, State#state{put_history=[{Idx,Msg}|State#state.put_history]}}.

active(Req=?VNODE_REQ{request=?KV_DELETE_REQ{}},
            _From, State) ->
    {next_state, active, NewState} = active(Req, State),
    {reply, ok, NewState};
active(_Request, _From, State) ->
    {stop, bad_request, State}.

%% @private
handle_event(_Event, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_sync_event({set_data, Objects, Partvals}, _From, StateName, State) ->
    {reply, ok, StateName, set_data(Objects, Partvals, State)};
handle_sync_event({set_vput_replies, VPutReplies}, _From, StateName, State) ->
    {reply, ok, StateName, set_vput_replies(VPutReplies, State)};
handle_sync_event(get_history, _From, StateName, State) ->
    {reply, lists:reverse(State#state.history), StateName, State};
handle_sync_event(get_put_history, _From, StateName, State) -> %
    {reply, lists:reverse(State#state.put_history), StateName, State};
handle_sync_event(get_reply_history, _From, StateName, State) -> %
    {reply, lists:reverse(State#state.reply_history), StateName, State};
handle_sync_event({log_postcommit, Obj}, _From, StateName, State) -> %
    {reply, ok, StateName, State#state{postcommit = [Obj | State#state.postcommit]}};
handle_sync_event(get_postcommits, _From, StateName, State) -> %
    {reply, lists:reverse(State#state.postcommit), StateName, State}.

%% @private
handle_info(_Info, _StateName, StateData) ->
    {stop,badmsg,StateData}.

%% @private
terminate(Reason, _StateName, _State) ->
    Reason.

code_change(_OldVsn, StateName, _State, _Extra) -> 
    {ok, StateName, #state{}}.

%% ====================================================================
%% Internal functions
%% ====================================================================

set_data(Objects, Partvals, State) ->
    State#state{objects=Objects, partvals=Partvals,
                history=[], put_history=[]}.

set_vput_replies(VPutReplies, State) ->
    State#state{vput_replies = VPutReplies,
                reply_history = [],
                lidx_map=[],
                postcommit = []}.

get_data(_Partition, #state{partvals=[]} = State) ->
    {{error, timeout}, State};
get_data(Partition, #state{objects=Objects, partvals=[Res|Rest]} = State) ->
    State1 = State#state{partvals = Rest,
                         history=[{Partition,Res}|State#state.history]},
    case Res of
        {ok, Lineage} ->
            {{ok, proplists:get_value(Lineage, Objects)}, State1};
        notfound ->
            {{error, notfound}, State1};
        timeout ->
            {{error, timeout}, State1};
        error ->
            {{error, error}, State1}
    end.


%% Send the next vnode put response then send any 'extra' responses in sequence
%% until the next w or the list is emptied.
send_vput_replies([], _Idx, _Sender, _ReqId, _Obj, _Options, State) ->
    State;
send_vput_replies([{LIdx, FirstResp} | Rest], Idx, Sender, ReqId, PutObj, Options,
               #state{lidx_map = LIdxMap} = State) ->
    %% Check have not seen this logical index before and store it
    error = orddict:find(LIdx, LIdxMap),
    NewState1 = State#state{lidx_map = orddict:store(LIdx, {Idx, PutObj, Options}, LIdxMap)},
    Reply = case FirstResp of
                w ->
                    {w, Idx, ReqId};
                {timeout, 1} ->
                    {{timeout, 1}, Idx, ReqId}
            end,
    NewState2 = record_reply(Sender, Reply, NewState1),
    Rest2 = fail_on_bad_obj(LIdx, PutObj, Rest),
                    
    send_vput_extra(Rest2, Sender, ReqId, NewState2).

send_vput_extra([], {fsm, undefined, Pid} = _Sender, _ReqId, State) ->
    Pid ! request_timeout, % speed things along by sending the request timeout
    State#state{vput_replies = []};
send_vput_extra([{LIdx, {dw, CurObj, _CurLin}} | Rest], Sender, ReqId,
                #state{lidx_map = LIdxMap} = State) ->
    {Idx, PutObj, Options} = orddict:fetch(LIdx, LIdxMap),
    Obj = put_merge(CurObj, PutObj, Options),
    NewState = case (proplists:get_value(returnbody, Options, false) orelse
                     proplists:get_value(coord, Options, false)) of
                   true ->
                       record_reply(Sender, {dw, Idx, Obj, ReqId}, State);
                   false ->
                       record_reply(Sender, {dw, Idx, ReqId}, State)
               end,
    send_vput_extra(Rest, Sender, ReqId, NewState);
send_vput_extra([{LIdx, fail} | Rest], Sender, ReqId,
                #state{lidx_map = LIdxMap} = State) ->
    {Idx, _PutObj, _Options} = orddict:fetch(LIdx, LIdxMap),
    NewState = record_reply(Sender, {fail, Idx, ReqId}, State),
    send_vput_extra(Rest, Sender, ReqId, NewState);
send_vput_extra([{LIdx, {timeout, 2}} | Rest], Sender, ReqId,
                #state{lidx_map = LIdxMap} = State) ->
    {Idx, _PutObj, _Options} = orddict:fetch(LIdx, LIdxMap),
    NewState = record_reply(Sender, {{timeout, 2}, Idx, ReqId}, State),
    send_vput_extra(Rest, Sender, ReqId, NewState);
send_vput_extra(VPutReplies, _Sender, _ReqId, State) ->
    State#state{vput_replies = VPutReplies}.

record_reply(Sender, Reply, #state{reply_history = H} = State) ->
    case Reply of
        {{timeout,_N},_Idx,_ReqId} ->
            nop;
        _ ->
            riak_core_vnode:reply(Sender, Reply)
    end,
    State#state{reply_history = [Reply | H]}.


%% If the requested object is not an r_object tuple, make sure the {dw}
%% response is fail Leave any other failures the same.
fail_on_bad_obj(_LIdx, Obj, VPutReplies) when element(1, Obj) =:= r_object ->
   VPutReplies;
fail_on_bad_obj(LIdx, _Obj, VPutReplies) ->
    case lists:keysearch(LIdx, 1, VPutReplies) of
        {value, {LIdx, {dw, _, _}}} ->
            lists:keyreplace(LIdx, 1, VPutReplies, {LIdx, fail});
        _ ->
            VPutReplies
    end.

%% TODO: The riak_kv_vnode code should be refactored to expose this function
%%       so we are close to testing the real thing.
put_merge(notfound, UpdObj, Options) ->
    case proplists:get_value(coord, Options, false) of
        true ->
            VnodeId = <<1, 1, 1, 1, 1, 1, 1, 1>>,
            Ts = vclock:timestamp(),
            riak_object:increment_vclock(UpdObj, VnodeId, Ts);
        false ->
            UpdObj
    end;                
put_merge(CurObj, UpdObj, Options) ->
    Coord = proplists:get_value(coord, Options, false),
    VnodeId = <<1, 1, 1, 1, 1, 1, 1, 1>>,
    Ts = vclock:timestamp(),
    {_, ResObj} = riak_kv_vnode:put_merge(Coord, false, CurObj, UpdObj, VnodeId, Ts),
    ResObj.
