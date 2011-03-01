%%%-------------------------------------------------------------------
%%% File    : get_fsm_qc_vnode_master.erl
%%% Author  : Bryan Fink <bryan@mashtun.local>
%%% Description : 
%%%
%%% Created : 28 Apr 2010 by Bryan Fink <bryan@mashtun.local>
%%%-------------------------------------------------------------------
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
                lidx_map=[],
                vput_replies=[],
                reply_history=[],
                history=[], put_history=[]}).

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
%%--------------------------------------------------------------------
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

%%====================================================================
%% gen_server callbacks
%%====================================================================

%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, State} |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
%%--------------------------------------------------------------------
init([]) ->
    {ok, #state{}}.

%%--------------------------------------------------------------------
%% Function: %% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% Description: Handling call messages
%%--------------------------------------------------------------------
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

handle_call(?VNODE_REQ{index=Idx, request=?KV_DELETE_REQ{}=Msg},
            _From, State) ->
    {reply, ok, State#state{put_history=[{Idx,Msg}|State#state.put_history]}};
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------
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
handle_cast(?VNODE_REQ{index=Idx,
                       sender=Sender,
                       request=?KV_PUT_REQ{req_id=ReqId,
                                           options = Options}=Msg}, State) ->
    %% Options to handle
    %% {returnbody, true|false}
    %% {update_last_modified, true|false}
    %% Needs to respond with a {w, Idx, ReqId}
    %% Then another message
    %%   return_body=true, {dw, Idx, Obj, ReqId}
    %%   return_body=false, {dw, Idx, ReqId}
    %%   on error {fail, Idx, ReqId}
    %% How to order the messages?  For now, just send immediately if responding.

    %% Send up to the next w or timeout message, substituting indices
    State1 = send_vput_replies(State#state.vput_replies, Idx,
                               Sender, ReqId, Options, State),
    {noreply, State1#state{put_history=[{Idx,Msg}|State#state.put_history]}};    
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: terminate(Reason, State) -> void()
%% Description: This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% Func: code_change(OldVsn, State, Extra) -> {ok, NewState}
%% Description: Convert process state when code is changed
%%--------------------------------------------------------------------
code_change(_OldVsn, _State, _Extra) ->
    {ok, #state{history=[]}}.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------

set_data(Objects, Partvals, State) ->
    State#state{objects=Objects, partvals=Partvals,
                history=[], put_history=[]}.

set_vput_replies(VPutReplies, State) ->
    State#state{vput_replies = VPutReplies,
                reply_history = [],
                lidx_map=[]}.

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
%% until the next w/timeout
send_vput_replies([], _Idx, _Sender, _ReqId, _Options, State) ->
    State;
send_vput_replies([{LIdx, FirstResp} | Rest], Idx, Sender, ReqId, Options,
               #state{lidx_map = LIdxMap} = State) ->
    %% Check have not seen this logical index before and store it
    error = orddict:find(LIdx, LIdxMap),
    NewState1 = State#state{lidx_map = orddict:store(LIdx, Idx, LIdxMap)},
    Reply = case FirstResp of
                w ->
                    {w, Idx, ReqId};
                {timeout, 1} ->
                    {{timeout, 1}, Idx, ReqId}
            end,
    NewState2 = record_reply(Sender, Reply, NewState1),
    send_vput_extra(Rest, Sender, ReqId, Options, NewState2).

send_vput_extra([], _Sender, _ReqId, _Options, State) ->
    State#state{vput_replies = []};
send_vput_extra([{LIdx, {dw, Obj}} | Rest], Sender, ReqId, Options,
                #state{lidx_map = LIdxMap} = State) ->
    Idx = orddict:fetch(LIdx, LIdxMap),
    NewState = case proplists:get_value(returnbody, Options, false) of
                   true ->
                       record_reply(Sender, {dw, Idx, ReqId}, State);
                   false ->
                       record_reply(Sender, {dw, Idx, Obj, ReqId}, State)
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
