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
-export([prepare/2, validate/2, execute/2, waiting_vnode_w/2,waiting_vnode_dw/2]).

-record(state, {robj :: riak_object:riak_object(),
                client :: {pid(), reference()},
                rclient :: riak_client:riak_client(),
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
                ring :: riak_core_ring:riak_core_ring(),
                startnow :: {pos_integer(), pos_integer(), pos_integer()}, % for FSM duration
                options=[] :: list(),
                vnode_options=[] :: list(),
                returnbody :: boolean(),
                resobjs=[] :: list(),
                allowmult :: boolean(),
                update_last_modified :: boolean(),
                bucket_props:: list()
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
handle_options([{returnbody, false}|T], State) ->
    case has_postcommit_hooks(element(1,State#state.bkey)) of
        true ->
            %% We have post-commit hooks, we'll need to get the body back
            %% from the vnode, even though we don't plan to return that to the
            %% original caller.  Force DW>0 to ensure the dw event returned by
            %% the vnode includes the object.
            VnodeOpts = [{returnbody, true} | State#state.vnode_options],
            handle_options(T, State#state{vnode_options=VnodeOpts,
                                          dw=erlang:max(1,State#state.dw),
                                          returnbody=false});
        false ->
            handle_options(T, State#state{returnbody=false})
    end;
handle_options([{_,_}|T], State) -> handle_options(T, State).

%% @private
prepare(timeout, StateData0 = #state{robj = RObj0}) ->
    {ok,Ring} = riak_core_ring_manager:get_my_ring(),
    BucketProps = riak_core_bucket:get_bucket(riak_object:bucket(RObj0), Ring),
    BKey = {riak_object:bucket(RObj0), riak_object:key(RObj0)},
    DocIdx = riak_core_util:chash_key(BKey),
    N = proplists:get_value(n_val,BucketProps),
    UpNodes = riak_core_node_watcher:nodes(riak_kv),
    Preflist2 = riak_core_apl:get_apl_ann(DocIdx, N, Ring, UpNodes),
    {ok, RClient} = riak:local_client(),
    StartTime = riak_core_util:moment(),
    
    StateData = StateData0#state{n = N,
                                 bkey = BKey,
                                 ring = Ring,
                                 bucket_props = BucketProps,
                                 preflist2 = Preflist2,
                                 rclient = RClient,
                                 starttime = StartTime},
    {next_state, validate, StateData, 0}.
    
validate(timeout, StateData0 = #state{robj = RObj0, n=N, w=W0, dw=DW0, bucket_props = BucketProps, 
                                     options = Options0}) ->
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

    if
        W =:= error ->
            client_reply({error, {w_val_violation, W0}}, StateData0),
            {stop, normal, none};
        DW =:= error ->
            client_reply({error, {dw_val_violation, DW0}}, StateData0),
            {stop, normal, none};
        (W > N) or (DW > N) ->
            client_reply({error, {n_val_violation, N}}, StateData0),
            {stop, normal, none};
        true ->
            AllowMult = proplists:get_value(allow_mult,BucketProps),
            Bucket = riak_object:bucket(RObj0),
            Key = riak_object:key(RObj0),
            StateData1 = StateData0#state{n=N, w=W, dw=DW, bkey={Bucket, Key},
                                         allowmult=AllowMult},
            Options = flatten_options(proplists:unfold(Options0 ++ ?DEFAULT_OPTS), []),
            StateData = handle_options(Options, StateData1),
            {next_state,execute,StateData,0}
    end.

execute(timeout, StateData0=#state{robj=RObj0, req_id = ReqId,
                                   update_last_modified=UpdateLastMod,
                                   timeout=Timeout, preflist2 = Preflist2, bkey=BKey,
                                   rclient=RClient, vnode_options=VnodeOptions,
                                   starttime = StartTime}) ->
    case invoke_hook(precommit, RClient, update_last_modified(UpdateLastMod, RObj0)) of
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
            {next_state,waiting_vnode_w,StateData}
    end.

waiting_vnode_w({w, Idx, ReqId},
                StateData=#state{w=W,dw=DW,req_id=ReqId,replied_w=Replied0}) ->
    Replied = [Idx|Replied0],
    case length(Replied) >= W of
        true ->
            case DW of
                0 ->
                    client_reply(ok, StateData),
                    update_stats(StateData),
                    {stop,normal,StateData};
                _ ->
                    NewStateData = StateData#state{replied_w=Replied},
                    {next_state,waiting_vnode_dw,NewStateData}
            end;
        false ->
            NewStateData = StateData#state{replied_w=Replied},
            {next_state,waiting_vnode_w,NewStateData}
    end;
waiting_vnode_w({dw, Idx, _ReqId},
                  StateData=#state{replied_dw=Replied0}) ->
    Replied = [Idx|Replied0],
    NewStateData = StateData#state{replied_dw=Replied},
    {next_state,waiting_vnode_w,NewStateData};
waiting_vnode_w({dw, Idx, ResObj, _ReqId},
                  StateData=#state{replied_dw=Replied0, resobjs=ResObjs0}) ->
    Replied = [Idx|Replied0],
    ResObjs = [ResObj|ResObjs0],
    NewStateData = StateData#state{replied_dw=Replied, resobjs=ResObjs},
    {next_state,waiting_vnode_w,NewStateData};
waiting_vnode_w({fail, Idx, ReqId},
                  StateData=#state{n=N,w=W, req_id = ReqId,
                                   replied_fail=Replied0}) ->
    Replied = [Idx|Replied0],
    NewStateData = StateData#state{replied_fail=Replied},
    case (N - length(Replied)) >= W of
        true ->
            {next_state,waiting_vnode_w,NewStateData};
        false ->
            update_stats(NewStateData),
            client_reply({error,too_many_fails}, NewStateData),
            {stop,normal,NewStateData}
    end;
waiting_vnode_w(timeout, StateData) ->
    update_stats(StateData),
    client_reply({error,timeout}, StateData),
    {stop,normal,StateData}.

waiting_vnode_dw({w, _Idx, ReqId},
          StateData=#state{req_id=ReqId}) ->
    {next_state,waiting_vnode_dw,StateData};
waiting_vnode_dw({dw, Idx, ReqId},
                 StateData=#state{dw=DW, req_id=ReqId, replied_dw=Replied0}) ->
    Replied = [Idx|Replied0],
    case length(Replied) >= DW of
        true ->
            client_reply(ok, StateData),
            update_stats(StateData),
            {stop,normal,StateData};
        false ->
            NewStateData = StateData#state{replied_dw=Replied},
            {next_state,waiting_vnode_dw,NewStateData}
    end;
waiting_vnode_dw({dw, Idx, ResObj, ReqId},
                 StateData=#state{dw=DW, req_id=ReqId, replied_dw=Replied0,
                                  allowmult=AllowMult, returnbody=ReturnBody,
                                  rclient=RClient, resobjs=ResObjs0}) ->
    Replied = [Idx|Replied0],
    ResObjs = [ResObj|ResObjs0],
    case length(Replied) >= DW of
        true ->
            ReplyObj = merge_robjs(ResObjs, AllowMult),
            Reply = case ReturnBody of
                        true  -> {ok, ReplyObj};
                        false -> ok
                    end,
            client_reply(Reply, StateData),
            invoke_hook(postcommit, RClient, ReplyObj),
            update_stats(StateData),
            {stop,normal,StateData};
        false ->
            NewStateData = StateData#state{replied_dw=Replied,resobjs=ResObjs},
            {next_state,waiting_vnode_dw,NewStateData}
    end;
waiting_vnode_dw({fail, Idx, ReqId},
                  StateData=#state{n=N,dw=DW, req_id = ReqId, replied_fail=Replied0}) ->
    Replied = [Idx|Replied0],
    NewStateData = StateData#state{replied_fail=Replied},
    case (N - length(Replied)) >= DW of
        true ->
            {next_state,waiting_vnode_dw,NewStateData};
        false ->
            client_reply({error,too_many_fails}, NewStateData),
            {stop,normal,NewStateData}
    end;
waiting_vnode_dw(timeout, StateData) ->
    update_stats(StateData),
    client_reply({error,timeout}, StateData),
    {stop,normal,StateData}.

%% @private
handle_event(_Event, _StateName, StateData) ->
    {stop,badmsg,StateData}.

%% @private
handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop,badmsg,StateData}.

%% @private

handle_info(timeout, StateName, StateData) ->
    ?MODULE:StateName(timeout, StateData);
handle_info(_Info, _StateName, StateData) ->
    {stop,badmsg,StateData}.

%% @private
terminate(Reason, _StateName, _State) ->
    Reason.

%% @private
code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.

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
    riak_object:apply_updates(riak_object:update_metadata(RObj, NewMD)).

make_vtag(RObj) ->
    <<HashAsNum:128/integer>> = crypto:md5(term_to_binary(riak_object:vclock(RObj))),
    riak_core_util:integer_to_list(HashAsNum,62).

update_stats(#state{startnow=StartNow}) ->
    EndNow = now(),
    riak_kv_stat:update({put_fsm_time, timer:now_diff(EndNow, StartNow)}).

%% Internal functions
invoke_hook(HookType, RClient, RObj) ->
    Bucket = riak_object:bucket(RObj),
    BucketProps = RClient:get_bucket(Bucket),
    R = proplists:get_value(HookType, BucketProps, []),
    case R of
        <<"none">> ->
            RObj;
        [] ->
            RObj;
        Hooks when is_list(Hooks) ->
            run_hooks(HookType, RObj, Hooks)
    end.

run_hooks(_HookType, RObj, []) ->
    RObj;
run_hooks(HookType, RObj, [{struct, Hook}|T]) ->
    Mod = proplists:get_value(<<"mod">>, Hook),
    Fun = proplists:get_value(<<"fun">>, Hook),
    JSName = proplists:get_value(<<"name">>, Hook),
    Result = invoke_hook(HookType, Mod, Fun, JSName, RObj),
    case HookType of
        precommit ->
            case Result of
                fail ->
                    Result;
                _ ->
                    run_hooks(HookType, Result, T)
            end;
        postcommit ->
            run_hooks(HookType, RObj, T)
    end.


invoke_hook(precommit, Mod0, Fun0, undefined, RObj) ->
    Mod = binary_to_atom(Mod0, utf8),
    Fun = binary_to_atom(Fun0, utf8),
    wrap_hook(Mod, Fun, RObj);
invoke_hook(precommit, undefined, undefined, JSName, RObj) ->
    case riak_kv_js_manager:blocking_dispatch(?JSPOOL_HOOK, {{jsfun, JSName}, RObj}, 5) of
        {ok, <<"fail">>} ->
            fail;
        {ok, [{<<"fail">>, Message}]} ->
            {fail, Message};
        {ok, NewObj} ->
            riak_object:from_json(NewObj);
        {error, Error} ->
            error_logger:error_msg("Error executing pre-commit hook: ~s",
                                   [Error]),
            fail
    end;
invoke_hook(postcommit, Mod0, Fun0, undefined, Obj) ->
    Mod = binary_to_atom(Mod0, utf8),
    Fun = binary_to_atom(Fun0, utf8),
    proc_lib:spawn_link(fun() -> wrap_hook(Mod, Fun, Obj) end);
invoke_hook(postcommit, undefined, undefined, _JSName, _Obj) ->
    error_logger:warning_msg("Javascript post-commit hooks aren't implemented");
%% NOP to handle all other cases
invoke_hook(_, _, _, _, RObj) ->
    RObj.

wrap_hook(Mod, Fun, Obj)->
    try Mod:Fun(Obj)
    catch
        EType:X ->
            error_logger:error_msg("problem invoking hook ~p:~p -> ~p:~p~n~p~n",
                                   [Mod,Fun,EType,X,erlang:get_stacktrace()]),
            fail
    end.

merge_robjs(RObjs0,AllowMult) ->
    RObjs1 = [X || X <- RObjs0,
                   X /= undefined],
    case RObjs1 of
        [] -> {error, notfound};
        _ -> riak_object:reconcile(RObjs1,AllowMult)
    end.

has_postcommit_hooks(Bucket) ->
    lists:flatten(proplists:get_all_values(postcommit, riak_core_bucket:get_bucket(Bucket))) /= [].

schedule_timeout(infinity) ->
    undefined;
schedule_timeout(Timeout) ->
    erlang:send_after(Timeout, self(), timeout).

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

start_test_() ->
    %% Start erlang node
    net_kernel:start([testnode, shortnames]),
    %% Execute the test cases
    {spawn,
     { foreach, 
       fun setup/0,
       fun cleanup/1,
       [
        fun successful_start/0,
        fun invalid_w_start/0,
        fun invalid_dw_start/0,
        fun invalid_n_val_start/0
       ]
     }
    }.

successful_start() ->
    W = DW = 1,
    process_flag(trap_exit, true),
    {ok, _Pid} = put_fsm_start(W, DW).

invalid_w_start() ->
    W = <<"abc">>,
    DW = 1,
    process_flag(trap_exit, true),
    ?assertEqual({error, {bad_return_value, {stop, normal, none}}}, put_fsm_start(W, DW)),
    %% Wait for error response
    receive
        {_RequestId, Result} ->
            ?assertEqual({error, {w_val_violation, <<"abc">>}}, Result)
    after
        5000 ->
            ?assert(false)
    end.                

invalid_dw_start() ->
    W = 1,
    DW = <<"abc">>,
    process_flag(trap_exit, true),
    ?assertEqual({error, {bad_return_value, {stop, normal, none}}}, put_fsm_start(W, DW)),
    %% Wait for error response
    receive
        {_RequestId, Result} ->
            ?assertEqual({error, {dw_val_violation, <<"abc">>}}, Result)
    after
        5000 ->
            ?assert(false)
    end.                

invalid_n_val_start() ->
    W = 4,
    DW = 1,
    process_flag(trap_exit, true),
    ?assertEqual({error, {bad_return_value, {stop, normal, none}}}, put_fsm_start(W, DW)),
    %% Wait for error response
    receive
        {_RequestId, Result} ->
            ?assertEqual({error, {n_val_violation, 3}}, Result)
    after
        5000 ->
            ?assert(false)
    end.                
    
put_fsm_start(W, DW) ->
    %% Start the gen_fsm process
    RequestId = erlang:phash2(erlang:now()),
    RObj = riak_object:new(<<"testbucket">>, <<"testkey">>, <<"testvalue">>),
    Timeout = 60000,
    riak_kv_put_fsm:start_link(RequestId, RObj, W, DW, Timeout, self()).

setup() ->
    %% Start the applications required for riak_kv to start
    %% Start net_kernel - hopefully can remove this after FSM is purified..
    State = case net_kernel:stop() of
                {error, not_allowed} ->
                    running;
                _X ->
                    %% Make sure epmd is started - will not be if erl -name has
                    %% not been run from the commandline.
                    os:cmd("epmd -daemon"),
                    timer:sleep(100),
                    case net_kernel:start(['kvputfsm@localhost', shortnames]) of
                        {ok, _Pid} ->
                            started;
                        ER ->
                            throw({net_kernel_start_failed, ER})
                    end
            end,
    application:start(sasl),
    application:start(crypto),
    application:start(riak_sysmon),
    application:start(webmachine),
    application:start(riak_core),
    application:start(luke),
    application:start(erlang_js),
    application:start(mochiweb),
    application:start(os_mon),
    timer:sleep(500),
    %% Set some missing env vars that are normally 
    %% part of release packaging.
    application:set_env(riak_core, ring_creation_size, 64),
    application:set_env(riak_core, default_bucket_props, []),
    riak_core_bucket:append_bucket_defaults([{n_val, 3}]),
    application:set_env(riak_kv, storage_backend, riak_kv_ets_backend),
    %% Create a fresh ring for the test
    Ring = riak_core_ring:fresh(),
    riak_core_ring_manager:set_my_ring(Ring),
    %% Start riak_kv
    application:start(riak_kv),
    timer:sleep(500),
    State.

cleanup(State) ->
    application:stop(riak_kv),
    application:stop(os_mon),
    application:stop(mochiweb),
    application:stop(erlang_js),
    application:stop(luke),
    application:stop(riak_core),
    application:stop(webmachine),
    application:stop(riak_sysmon),
    application:stop(crypto),
    application:stop(sasl),    
    case State of
        started ->
            ok = net_kernel:stop();
        _ ->
            ok
    end,

    %% Reset the riak_core vnode_modules
    application:set_env(riak_core, vnode_modules, []).

-endif.
