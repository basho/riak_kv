%% -------------------------------------------------------------------
%%
%% get_fsm_qc_vnode_master: mock vnode for get/put FSM testing
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
-module(get_fsm_qc_vnode_master).

-behaviour(gen_server).
-include_lib("riak_kv_vnode.hrl").

%% API
-export([start_link/0,
         start/0]).
-compile(export_all).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

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

%%====================================================================
%% API
%%====================================================================

start_link() ->
    gen_server:start_link({local, riak_kv_vnode_master}, ?MODULE, [], []).

start() ->
    gen_server:start({local, riak_kv_vnode_master}, ?MODULE, [], []).

get_history() ->
    gen_server:call(riak_kv_vnode_master, get_history).

get_put_history() ->
    gen_server:call(riak_kv_vnode_master, get_put_history).

get_reply_history() ->
    gen_server:call(riak_kv_vnode_master, get_reply_history).

log_postcommit(Obj) ->
    gen_server:call(riak_kv_vnode_master, {log_postcommit, Obj}).
    
get_postcommits() ->
    gen_server:call(riak_kv_vnode_master, get_postcommits).
    

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    {ok, #state{}}.

handle_call({set_data, Objects, Partvals}, _From, State) ->
    {reply, ok, set_data(Objects, Partvals, State)};
handle_call({set_vput_replies, VPutReplies}, _From, State) ->
    {reply, ok, set_vput_replies(VPutReplies, State)};
handle_call(get_history, _From, State) ->
    {reply, lists:reverse(State#state.history), State};
handle_call(get_put_history, _From, State) -> %
    {reply, lists:reverse(State#state.put_history), State};
handle_call(get_reply_history, _From, State) -> %
    {reply, lists:reverse(State#state.reply_history), State};
handle_call({log_postcommit, Obj}, _From, State) -> %
    {reply, ok, State#state{postcommit = [Obj | State#state.postcommit]}};
handle_call(get_postcommits, _From, State) -> %
    {reply, lists:reverse(State#state.postcommit), State};

handle_call(?VNODE_REQ{index=Idx, request=?KV_DELETE_REQ{}=Msg},
            _From, State) ->
    {reply, ok, State#state{put_history=[{Idx,Msg}|State#state.put_history]}};
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%% Handle a get request - return the next get response prepared by call to 'set_data'
handle_cast(?VNODE_REQ{index=Idx,
                       sender=Sender,
                       request=?KV_GET_REQ{req_id=ReqId}}, State) ->
    {Value, State1} = get_data(Idx,State),
    case Value of
        {error, timeout} ->
            ok;
        _ ->
            riak_core_vnode:reply(Sender, {r, Value, Idx, ReqId})
    end,
    {noreply, State1};

%% Handle a put request - send the responses prepared by call to
%% 'set_vput_replies' up until the next 'w' response for a different partition.
handle_cast(?VNODE_REQ{index=Idx,
                       sender=Sender,
                       request=?KV_PUT_REQ{req_id=ReqId,
                                           object = PutObj,
                                           options = Options}=Msg}, State) ->
    %% Send up to the next w or timeout message, substituting indices
    State1 = send_vput_replies(State#state.vput_replies, Idx,
                               Sender, ReqId, PutObj, Options, State),
    {noreply, State1#state{put_history=[{Idx,Msg}|State#state.put_history]}};    
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.
code_change(_OldVsn, _State, _Extra) ->
    {ok, #state{history=[]}}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

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
    NewState1 = State#state{lidx_map = orddict:store(LIdx, {Idx, PutObj}, LIdxMap)},
    Reply = case FirstResp of
                w ->
                    {w, Idx, ReqId};
                {timeout, 1} ->
                    {{timeout, 1}, Idx, ReqId}
            end,
    NewState2 = record_reply(Sender, Reply, NewState1),
    Rest2 = fail_on_bad_obj(LIdx, PutObj, Rest),
    send_vput_extra(Rest2, Sender, ReqId, Options, NewState2).

send_vput_extra([], _Sender, _ReqId, _Options, State) ->
    State#state{vput_replies = []};
send_vput_extra([{LIdx, {dw, CurObj, _CurLin}} | Rest], Sender, ReqId, Options,
                #state{lidx_map = LIdxMap} = State) ->
    {Idx, PutObj} = orddict:fetch(LIdx, LIdxMap),
    Obj = put_merge(CurObj, PutObj, ReqId),
    NewState = case proplists:get_value(returnbody, Options, false) of
                   true ->
                       record_reply(Sender, {dw, Idx, Obj, ReqId}, State);
                   false ->
                       record_reply(Sender, {dw, Idx, ReqId}, State)
               end,
    send_vput_extra(Rest, Sender, ReqId, Options, NewState);
send_vput_extra([{LIdx, fail} | Rest], Sender, ReqId, Options,
                #state{lidx_map = LIdxMap} = State) ->
    Idx = orddict:fetch(LIdx, LIdxMap),
    NewState = record_reply(Sender, {fail, Idx, ReqId}, State),
    send_vput_extra(Rest, Sender, ReqId, Options, NewState);
send_vput_extra([{LIdx, {timeout, 2}} | Rest], Sender, ReqId, Options,
                #state{lidx_map = LIdxMap} = State) ->
    Idx = orddict:fetch(LIdx, LIdxMap),
    NewState = record_reply(Sender, {{timeout, 2}, Idx, ReqId}, State),
    send_vput_extra(Rest, Sender, ReqId, Options, NewState);
send_vput_extra(VPutReplies, _Sender, _ReqId, _Options, State) ->
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
put_merge(notfound, NewObj, _ReqId) ->
    NewObj;
put_merge(CurObj, NewObj, ReqId) ->
    riak_object:syntactic_merge(CurObj,NewObj, ReqId).

