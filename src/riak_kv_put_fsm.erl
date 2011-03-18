%% -------------------------------------------------------------------
%%
%% riak_put_fsm: coordination of Riak PUT requests
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

%% @doc coordination of Riak PUT requests

-module(riak_kv_put_fsm).
%-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
%-endif.
-include_lib("riak_kv_vnode.hrl").
-include_lib("riak_kv_js_pools.hrl").
-include("riak_kv_wm_raw.hrl").

-behaviour(gen_fsm).
-define(DEFAULT_OPTS, [{returnbody, false}, {update_last_modified, true}]).
-export([start/6,start/7]).
-export([start_link/6,start_link/7]).
-export([init/1, handle_event/3, handle_sync_event/4,
         handle_info/3, terminate/3, code_change/4]).
-export([prepare/2, validate/2, execute/2, waiting_vnode/2, postcommit/2]).

-record(state, {robj :: riak_object:riak_object(),
                client :: {pid(), reference()},
                n :: pos_integer(),
                w :: pos_integer(),
                dw :: non_neg_integer(),
                preflist2 :: riak_core_apl:preflist2(),
                bkey :: {riak_object:bucket(), riak_object:key()},
                req_id :: pos_integer(),
                starttime :: pos_integer(), % start time to send to vnodes
                replied_w :: list(),
                replied_dw :: list(),
                replied_fail :: list(),
                timeout :: pos_integer(),
                tref    :: reference(),
                startnow :: {pos_integer(), pos_integer(), pos_integer()}, % for FSM duration
                options=[] :: list(),
                vnode_options=[] :: list(),
                returnbody :: boolean(),
                resobjs=[] :: list(),
                allowmult :: boolean(),
                precommit=[] :: list(),
                postcommit=[] :: list(),
                update_last_modified :: boolean(),
                bucket_props:: list(),
                num_w = 0 :: non_neg_integer(),
                num_dw = 0 :: non_neg_integer(),
                num_fail = 0 :: non_neg_integer(),
                w_fail_threshold :: undefined | non_neg_integer(),
                dw_fail_threshold :: undefined | non_neg_integer(),
                final_obj :: undefined | riak_object:riak_object()
               }).

%% In place only for backwards compatibility
start(ReqId,RObj,W,DW,Timeout,From) ->
    start_link(ReqId,RObj,W,DW,Timeout,From,[]).

%% In place only for backwards compatibility
start(ReqId,RObj,W,DW,Timeout,From,Options) ->
    start_link(ReqId,RObj,W,DW,Timeout,From,Options).

start_link(ReqId,RObj,W,DW,Timeout,From) ->
    start_link(ReqId,RObj,W,DW,Timeout,From,[]).

start_link(ReqId,RObj,W,DW,Timeout,From,Options) ->
    gen_fsm:start_link(?MODULE, [ReqId,RObj,W,DW,Timeout,From,Options], []).

%% @private
init([ReqId,RObj0,W0,DW0,Timeout,Client,Options0]) ->
    StartNow = now(),
    StateData = #state{robj=RObj0, 
                       client=Client, w=W0, dw=DW0,
                       req_id=ReqId, timeout=Timeout, options = Options0,
                       startnow=StartNow},
    {ok,prepare,StateData,0}.

%%
%% Given an expanded proplist of options, take the first entry for any given key
%% and ignore the rest
%%
%% @private
flatten_options([], Opts) ->
    Opts;
flatten_options([{Key, Value} | Rest], Opts) ->
    case lists:keymember(Key, 1, Opts) of
        true ->
            flatten_options(Rest, Opts);
        false ->
            flatten_options(Rest, [{Key, Value} | Opts])
    end.

%% @private
handle_options([], State) ->
    State;
handle_options([{update_last_modified, Value}|T], State) ->
    handle_options(T, State#state{update_last_modified=Value});
handle_options([{returnbody, true}|T], State) ->
    VnodeOpts = [{returnbody, true} | State#state.vnode_options],
    %% Force DW>0 if requesting return body to ensure the dw event 
    %% returned by the vnode includes the object.
    handle_options(T, State#state{vnode_options=VnodeOpts,
                                  dw=erlang:max(1,State#state.dw),
                                  returnbody=true});
handle_options([{returnbody, false}|T], State = #state{postcommit = Postcommit}) ->
    case Postcommit of
        [] ->
            handle_options(T, State#state{returnbody=false});
            
        _ ->
            %% We have post-commit hooks, we'll need to get the body back
            %% from the vnode, even though we don't plan to return that to the
            %% original caller.  Force DW>0 to ensure the dw event returned by
            %% the vnode includes the object.
            VnodeOpts = [{returnbody, true} | State#state.vnode_options],
            handle_options(T, State#state{vnode_options=VnodeOpts,
                                          dw=erlang:max(1,State#state.dw),
                                          returnbody=false})
    end;
handle_options([{_,_}|T], State) -> handle_options(T, State).

find_fail_threshold(State = #state{n = N, w = W, dw = DW}) ->
    State#state{ w_fail_threshold = N-W+1,    % cannot ever get W replies
                 dw_fail_threshold = N-DW+1}. % cannot ever get DW replies

%% @private
prepare(timeout, StateData0 = #state{robj = RObj0}) ->
    {ok,Ring} = riak_core_ring_manager:get_my_ring(),
    BucketProps = riak_core_bucket:get_bucket(riak_object:bucket(RObj0), Ring),
    BKey = {riak_object:bucket(RObj0), riak_object:key(RObj0)},
    DocIdx = riak_core_util:chash_key(BKey),
    N = proplists:get_value(n_val,BucketProps),
    UpNodes = riak_core_node_watcher:nodes(riak_kv),
    Preflist2 = riak_core_apl:get_apl_ann(DocIdx, N, Ring, UpNodes),
    StartTime = riak_core_util:moment(),
    
    StateData = StateData0#state{n = N,
                                 bkey = BKey,
                                 bucket_props = BucketProps,
                                 preflist2 = Preflist2,
                                 starttime = StartTime},
    {next_state, validate, StateData, 0}.
    
%% @private
validate(timeout, StateData0 = #state{n=N, w=W0, dw=DW0, bucket_props = BucketProps, 
                                      options = Options0, preflist2 = Preflist2}) ->
    W = riak_kv_util:expand_rw_value(w, W0, BucketProps, N),

    %% Expand the DW value, but also ensure that DW <= W
    DW1 = riak_kv_util:expand_rw_value(dw, DW0, BucketProps, N),
    %% If no error occurred expanding DW also ensure that DW <= W
    case DW1 of
         error ->
             DW = error;
         _ ->
             DW = erlang:min(DW1, W)
    end,
    NumVnodes = length(Preflist2),
    MinVnodes = erlang:max(W, DW),
    if
        W =:= error ->
            client_reply({error, {w_val_violation, W0}}, StateData0),
            {stop, normal, StateData0};
        DW =:= error ->
            client_reply({error, {dw_val_violation, DW0}}, StateData0),
            {stop, normal, StateData0};
        (W > N) or (DW > N) ->
            client_reply({error, {n_val_violation, N}}, StateData0),
            {stop, normal, StateData0};
        NumVnodes < MinVnodes ->
            client_reply({error, {insufficient_vnodes, NumVnodes, need, MinVnodes}},
                         StateData0),
            {stop, normal, StateData0};
        true ->
            AllowMult = proplists:get_value(allow_mult,BucketProps),
            Precommit = get_hooks(precommit, BucketProps),
            Postcommit = get_hooks(postcommit, BucketProps),
            StateData1 = StateData0#state{n=N, w=W, dw=DW, allowmult=AllowMult,
                                          precommit = Precommit,
                                          postcommit = Postcommit},
            Options = flatten_options(proplists:unfold(Options0 ++ ?DEFAULT_OPTS), []),
            StateData2 = handle_options(Options, StateData1),
            StateData = find_fail_threshold(StateData2), 
            {next_state,execute,StateData,0}
    end.

%% @private
execute(timeout, StateData0=#state{robj=RObj0, req_id = ReqId,
                                   update_last_modified=UpdateLastMod,
                                   timeout=Timeout, preflist2 = Preflist2, bkey=BKey,
                                   vnode_options=VnodeOptions,
                                   starttime = StartTime,
                                   precommit = Precommit}) ->
    case invoke_precommit(Precommit, update_last_modified(UpdateLastMod, RObj0)) of
        fail ->
            client_reply({error, precommit_fail}, StateData0),
            {stop, normal, StateData0};
        {fail, Reason} ->
            client_reply({error, {precommit_fail, Reason}}, StateData0),
            {stop, normal, StateData0};
        RObj1 ->
            TRef = schedule_timeout(Timeout),
            Preflist = [IndexNode || {IndexNode, _Type} <- Preflist2],
            riak_kv_vnode:put(Preflist, BKey, RObj1, ReqId, StartTime, VnodeOptions),
            StateData = StateData0#state{
                          robj=RObj1,
                          replied_w=[], replied_dw=[], replied_fail=[],
                          tref=TRef},
            case enough_results(StateData) of
                {reply, Reply, StateData1} ->
                    client_reply(Reply, StateData1),
                    update_stats(StateData1),
                    case Reply of
                        ok ->
                            {next_state, postcommit, StateData1, 0};
                        {ok, _} ->
                            {next_state, postcommit, StateData1, 0};
                        _ ->
                            {stop, normal, StateData1}
                    end;
                {false, StateData} ->
                    {next_state, waiting_vnode, StateData}
            end
    end.

%% @private
waiting_vnode(request_timeout, StateData) ->
    update_stats(StateData),
    client_reply({error,timeout}, StateData),
    {stop, normal, StateData};
waiting_vnode(Result, StateData) ->
    StateData1 = add_vnode_result(Result, StateData),
    case enough_results(StateData1) of
        {reply, Reply, StateData2} ->
            client_reply(Reply, StateData2),
            update_stats(StateData2),
            case Reply of
                ok ->
                    {next_state, postcommit, StateData2, 0};
                {ok, _} ->
                    {next_state, postcommit, StateData2, 0};
                _ ->
                    {stop, normal, StateData2}
            end;
        {false, StateData2} ->
            {next_state, waiting_vnode, StateData2}
    end.

%% @private
postcommit(timeout, StateData = #state{postcommit = []}) ->
    {stop, normal, StateData};
postcommit(timeout, StateData = #state{postcommit = [Hook | Rest],
                                       final_obj = ReplyObj}) ->
    %% Process the next hook - gives sys:get_status messages a chance if hooks
    %% take a long time.  No checking error returns for postcommit hooks.
    invoke_hook(Hook, ReplyObj),
    {next_state, postcommit, StateData#state{postcommit = Rest}, 0};
postcommit(request_timeout, StateData) -> % still process hooks even if request timed out
    {next_state, postcommit, StateData, 0};
postcommit(Reply, StateData) -> % late responses - add to state
    StateData1 = add_vnode_result(Reply, StateData),
    {next_state, postcommit, StateData1, 0}.


%% @private
handle_event(_Event, _StateName, StateData) ->
    {stop,badmsg,StateData}.

%% @private
handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop,badmsg,StateData}.

%% @private

handle_info(request_timeout, StateName, StateData) ->
    ?MODULE:StateName(request_timeout, StateData);
handle_info(_Info, _StateName, StateData) ->
    {stop,badmsg,StateData}.

%% @private
terminate(Reason, _StateName, _State) ->
    Reason.

%% @private
code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.

%% Add a vnode result to the state structure and update the counts
add_vnode_result({w, Idx, _ReqId}, StateData = #state{replied_w = Replied,
                                                      num_w = NumW}) ->
    StateData#state{replied_w = [Idx | Replied], num_w = NumW + 1};
add_vnode_result({dw, Idx, _ReqId}, StateData = #state{replied_dw = Replied,
                                                       num_dw = NumDW}) ->
    StateData#state{replied_dw = [Idx | Replied], num_dw = NumDW + 1};
add_vnode_result({dw, Idx, ResObj, _ReqId}, StateData = #state{replied_dw = Replied,
                                                               resobjs = ResObjs,
                                                               num_dw = NumDW}) ->
    StateData#state{replied_dw = [Idx | Replied],
                    resobjs = [ResObj | ResObjs],
                    num_dw = NumDW + 1};
add_vnode_result({fail, Idx, _ReqId}, StateData = #state{replied_fail = Replied,
                                                         num_fail = NumFail}) ->
    StateData#state{replied_fail = [Idx | Replied],
                    num_fail = NumFail + 1};
add_vnode_result(_Other, StateData = #state{num_fail = NumFail}) ->
    %% Treat unrecognized messages as failures
    StateData#state{num_fail = NumFail + 1}.

enough_results(StateData = #state{w = W, num_w = NumW, dw = DW, num_dw = NumDW,
                                  num_fail = NumFail,
                                  w_fail_threshold = WFailThreshold,
                                  dw_fail_threshold = DWFailThreshold}) ->
    if
        NumW >= W andalso NumDW >= DW ->
            maybe_return_body(StateData);
        
        NumW >= W andalso NumFail >= DWFailThreshold ->
            {reply, {error,too_many_fails}, StateData};
        
        NumW < W andalso NumFail >= WFailThreshold ->
            {reply, {error,too_many_fails}, StateData};
        
        true ->
            {false, StateData}
    end.

maybe_return_body(StateData = #state{returnbody=false, postcommit=[]}) ->
    {reply, ok, StateData};
maybe_return_body(StateData = #state{resobjs = ResObjs, allowmult = AllowMult,
                                     returnbody = ReturnBody}) ->
    ReplyObj = merge_robjs(ResObjs, AllowMult),
    Reply = case ReturnBody of
                true  -> {ok, ReplyObj};
                false -> ok
            end,
    {reply, Reply, StateData#state{final_obj = ReplyObj}}.

%%
%% Update X-Riak-VTag and X-Riak-Last-Modified in the object's metadata, if
%% necessary.
%%
%% @private
update_last_modified(false, RObj) ->
    RObj;
update_last_modified(true, RObj) ->
    MD0 = case dict:find(clean, riak_object:get_update_metadata(RObj)) of
              {ok, true} ->
                  %% There have been no changes to updatemetadata. If we stash the
                  %% last modified in this dict, it will cause us to lose existing
                  %% metadata (bz://508). If there is only one instance of metadata,
                  %% we can safely update that one, but in the case of multiple siblings,
                  %% it's hard to know which one to use. In that situation, use the update
                  %% metadata as is.
                  case riak_object:get_metadatas(RObj) of
                      [MD] ->
                          MD;
                      _ ->
                          riak_object:get_update_metadata(RObj)
                  end;
               _ ->
                  riak_object:get_update_metadata(RObj)
          end,
    NewMD = dict:store(?MD_VTAG, make_vtag(RObj),
                       dict:store(?MD_LASTMOD, erlang:now(),
                                  MD0)),
    riak_object:update_metadata(RObj, NewMD).

make_vtag(RObj) ->
    <<HashAsNum:128/integer>> = crypto:md5(term_to_binary(riak_object:vclock(RObj))),
    riak_core_util:integer_to_list(HashAsNum,62).

update_stats(#state{startnow=StartNow}) ->
    EndNow = now(),
    riak_kv_stat:update({put_fsm_time, timer:now_diff(EndNow, StartNow)}).

%% Run the precommit hooks
invoke_precommit([], RObj) ->
    riak_object:apply_updates(RObj);
invoke_precommit([Hook | Rest], RObj) ->
    Result = decode_precommit(invoke_hook(Hook, RObj)),
    case Result of
        Obj when element(1, Obj) == r_object ->
            invoke_precommit(Rest, riak_object:apply_updates(RObj));
        _ ->
            Result
    end.


%% Invokes the hook and returns a tuple of
%% {Lang, Called, Result}
%% Where Called = {Mod, Fun} if Lang = erlang
%%       Called = JSName if Lang = javascript
invoke_hook({struct, Hook}, RObj) ->
    Mod = proplists:get_value(<<"mod">>, Hook),
    Fun = proplists:get_value(<<"fun">>, Hook),
    JSName = proplists:get_value(<<"name">>, Hook),
    invoke_hook(Mod, Fun, JSName, RObj);
invoke_hook(HookDef, _RObj) ->
    {error, {invalid_hook_def, HookDef}}.

invoke_hook(Mod0, Fun0, undefined, RObj) when Mod0 /= undefined, Fun0 /= undefined ->
    Mod = binary_to_atom(Mod0, utf8),
    Fun = binary_to_atom(Fun0, utf8),
    try
        {erlang, {Mod, Fun}, Mod:Fun(RObj)}
    catch
        Class:Exception ->
            {erlang, {Mod, Fun}, {'EXIT', Mod, Fun, Class, Exception}}
    end;
invoke_hook(undefined, undefined, JSName, RObj) when JSName /= undefined ->
    {js, JSName, riak_kv_js_manager:blocking_dispatch(?JSPOOL_HOOK, {{jsfun, JSName}, RObj}, 5)};
invoke_hook(_, _, _, _) ->
    {error, {invalid_hook_def, no_hook}}.

decode_precommit({erlang, {Mod, Fun}, Result}) ->
    case Result of
        fail ->
            fail;
        {fail, _Reason} ->
            Result;
        Obj when element(1, Obj) == r_object ->
            Obj;
       {'EXIT',  Mod, Fun, Class, Exception} ->
           error_logger:error_msg("problem invoking hook ~p:~p -> ~p:~p~n~p~n",
                                  [Mod,Fun,Class,Exception,
                                   erlang:get_stacktrace()]),
    
           {fail, {hook_crashed, {Mod, Fun, Class, Exception}}};
       _ ->
           {fail, {invalid_return, {Mod, Fun, Result}}}
   end;
decode_precommit({js, JSName, Result}) ->
    case Result of
        {ok, <<"fail">>} ->
            fail;
        {ok, [{<<"fail">>, Message}]} ->
            {fail, Message};
        {ok, Json} ->
            case catch riak_object:from_json(Json) of
                {'EXIT', _} ->
                    {fail, {invalid_return, {JSName, Json}}};
                Obj ->
                    Obj
            end;
        {error, Error} ->
            error_logger:error_msg("Error executing pre-commit hook: ~s",
                                   [Error]),
            fail
    end;
decode_precommit({error, Reason}) ->
    {fail, Reason}.

get_hooks(HookType, BucketProps) ->
    Hooks = proplists:get_value(HookType, BucketProps, []),
    case Hooks of
        <<"none">> ->
            [];
        Hooks when is_list(Hooks) ->
            Hooks
    end.
              
merge_robjs(RObjs0,AllowMult) ->
    RObjs1 = [X || X <- RObjs0,
                   X /= undefined],
    case RObjs1 of
        [] -> {error, notfound};
        _ -> riak_object:reconcile(RObjs1,AllowMult)
    end.

schedule_timeout(infinity) ->
    undefined;
schedule_timeout(Timeout) ->
    erlang:send_after(Timeout, self(), request_timeout).

client_reply(Reply, #state{client = Client, req_id = ReqId}) ->
    Client ! {ReqId, Reply}.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

make_vtag_test() ->
    Obj = riak_object:new(<<"b">>,<<"k">>,<<"v1">>),
    ?assertNot(make_vtag(Obj) =:=
               make_vtag(riak_object:increment_vclock(Obj,<<"client_id">>))).

-endif.
