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
-export([start_link/3,start_link/6,start_link/7]).
-ifdef(TEST).
-export([test_link/4]).
-endif.
-export([init/1, handle_event/3, handle_sync_event/4,
         handle_info/3, terminate/3, code_change/4]).
-export([prepare/2, validate/2, precommit/2, execute/2, waiting_vnode/2, postcommit/2, finish/2]).


-type detail_info() :: timing.
-type detail() :: true |
                  false |
                  [detail_info()].

-type option() :: {pw, non_neg_integer()} | %% Min number of primary (owner) vnodes participating
                  {w, pos_integer()} |      %% Minimum number of vnodes receiving write
                  {dw, pos_integer()} |     %% Minimum number of vnodes completing write
                  {timeout, pos_integer() | infinity} |
                  {detail, detail()}.       %% Request additional details about request
                                            %% added as extra element at the end of result tuplezd 
-type options() :: [option()].

-export_type([option/0, options/0, detail/0, detail_info/0]).

-record(state, {from :: {raw, integer(), pid()},
                robj :: riak_object:riak_object(),
                options=[] :: options(),
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
                timeout :: pos_integer()|infinity,
                tref    :: reference(),
                vnode_options=[] :: list(),
                returnbody :: boolean(),
                resobjs=[] :: list(),
                allowmult :: boolean(),
                precommit=[] :: list(),
                postcommit=[] :: list(),
                bucket_props:: list(),
                num_w = 0 :: non_neg_integer(),
                num_dw = 0 :: non_neg_integer(),
                num_fail = 0 :: non_neg_integer(),
                w_fail_threshold :: undefined | non_neg_integer(),
                dw_fail_threshold :: undefined | non_neg_integer(),
                final_obj :: undefined | riak_object:riak_object(),
                put_usecs :: undefined | non_neg_integer(),
                timing = [] :: [{atom(), {non_neg_integer(), non_neg_integer(),
                                          non_neg_integer()}}],
                reply % reply sent to client
               }).


-define(DEFAULT_TIMEOUT, 60000).
-define(DEFAULT_PW, 0).
-define(DEFAULT_W, default).
-define(DEFAULT_DW, default).

%% ===================================================================
%% Public API
%% ===================================================================

%% In place only for backwards compatibility
start(ReqId,RObj,W,DW,Timeout,ResultPid) ->
    start_link(ReqId,RObj,W,DW,Timeout,ResultPid,[]).

%% In place only for backwards compatibility
start(ReqId,RObj,W,DW,Timeout,ResultPid,Options) ->
    start_link(ReqId,RObj,W,DW,Timeout,ResultPid,Options).

start_link(ReqId,RObj,W,DW,Timeout,ResultPid) ->
    start_link(ReqId,RObj,W,DW,Timeout,ResultPid,[]).

start_link(ReqId,RObj,W,DW,Timeout,ResultPid,Options) ->
    start_link({raw, ReqId, ResultPid}, RObj, [{w, W}, {dw, DW}, {timeout, Timeout} | Options]).

start_link(From, Object, PutOptions) ->
    gen_fsm:start_link(?MODULE, [From, Object, PutOptions], []).

%% ===================================================================
%% Test API
%% ===================================================================

-ifdef(TEST).
%% Create a put FSM for testing.  StateProps must include
%% starttime - start time in gregorian seconds
%% n - N-value for request (is grabbed from bucket props in prepare)
%% bkey - Bucket / Key
%% bucket_props - bucket properties
%% preflist2 - [{{Idx,Node},primary|fallback}] preference list
%% 
%% As test, but linked to the caller
test_link(From, Object, PutOptions, StateProps) ->
    gen_fsm:start_link(?MODULE, {test, [From, Object, PutOptions], StateProps}, []).

-endif.

%% ====================================================================
%% gen_fsm callbacks
%% ====================================================================

%% @private
init([From, RObj, Options]) ->
    StateData = add_timing(prepare, #state{from = From,
                                           robj = RObj, 
                                           options = Options}),
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

    %% Enter into the validate state, skipping any code that relies on the
    %% state of the rest of the system
    {ok, validate, TestStateData, 0}.

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
    new_state_timeout(validate, StateData).
    
%% @private
validate(timeout, StateData0 = #state{from = {raw, ReqId, _Pid},
                                      options = Options0,
                                      n=N, bucket_props = BucketProps,
                                      preflist2 = Preflist2}) ->
    Timeout = get_option(timeout, Options0, ?DEFAULT_TIMEOUT),
    PW0 = get_option(pw, Options0, ?DEFAULT_PW),
    W0 = get_option(w, Options0, ?DEFAULT_W),
    DW0 = get_option(dw, Options0, ?DEFAULT_DW),

    PW = riak_kv_util:expand_rw_value(pw, PW0, BucketProps, N),
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
    NumPrimaries = length([x || {_,primary} <- Preflist2]),
    NumVnodes = length(Preflist2),
    MinVnodes = erlang:max(1, erlang:max(W, DW)), % always need at least one vnode
    if
        PW =:= error ->
            process_reply({error, {pw_val_violation, PW0}}, StateData0);
        W =:= error ->
            process_reply({error, {w_val_violation, W0}}, StateData0);
        DW =:= error ->
            process_reply({error, {dw_val_violation, DW0}}, StateData0);
        (W > N) or (DW > N) ->
            process_reply({error, {n_val_violation, N}}, StateData0);
        PW > NumPrimaries ->
            process_reply({error, {pw_val_unsatisfied, PW, NumPrimaries}}, StateData0);
        NumVnodes < MinVnodes ->
            process_reply({error, {insufficient_vnodes, NumVnodes,
                                   need, MinVnodes}}, StateData0);
        true ->
            AllowMult = proplists:get_value(allow_mult,BucketProps),
            Precommit = get_hooks(precommit, BucketProps),
            Postcommit = get_hooks(postcommit, BucketProps),
            StateData1 = StateData0#state{n=N, w=W, dw=DW, allowmult=AllowMult,
                                          precommit = Precommit,
                                          postcommit = Postcommit,
                                          req_id = ReqId,
                                          timeout = Timeout},
            Options = flatten_options(proplists:unfold(Options0 ++ ?DEFAULT_OPTS), []),
            StateData2 = handle_options(Options, StateData1),
            StateData3 = apply_updates(StateData2),
            StateData = find_fail_threshold(StateData3),
            case Precommit of
                [] -> % Nothing to run, spare the timing code
                    new_state_timeout(execute, StateData);
                _ ->
                    new_state_timeout(precommit, StateData)
            end
    end.

%% Run the precommit hooks
precommit(timeout, State = #state{precommit = []}) ->
    new_state_timeout(execute, State);
precommit(timeout, State = #state{precommit = [Hook | Rest], robj = RObj}) ->
    Result = decode_precommit(invoke_hook(Hook, RObj)),
    case Result of
        fail ->
            process_reply({error, precommit_fail}, State);
        {fail, Reason} ->
            process_reply({error, {precommit_fail, Reason}}, State);
        Result ->
            {next_state, precommit, State#state{robj = riak_object:apply_updates(Result),
                                                precommit = Rest}, 0}
    end.

%% @private
execute(timeout, StateData0=#state{robj=RObj, req_id = ReqId,
                                   timeout=Timeout, preflist2 = Preflist2, bkey=BKey,
                                   vnode_options=VnodeOptions,
                                   starttime = StartTime}) ->
    TRef = schedule_timeout(Timeout),
    Preflist = [IndexNode || {IndexNode, _Type} <- Preflist2],
    riak_kv_vnode:put(Preflist, BKey, RObj, ReqId, StartTime, VnodeOptions),
    StateData = StateData0#state{
                  replied_w=[], replied_dw=[], replied_fail=[],
                  tref=TRef},
    case enough_results(StateData) of
        {reply, Reply, StateData1} ->
            process_reply(Reply, StateData1);
        {false, StateData} ->
            new_state(waiting_vnode, StateData)
    end.

%% @private
waiting_vnode(request_timeout, StateData) ->
    process_reply({error,timeout}, StateData);
waiting_vnode(Result, StateData) ->
    StateData1 = add_vnode_result(Result, StateData),
    case enough_results(StateData1) of
        {reply, Reply, StateData2} ->
            process_reply(Reply, StateData2);

        {false, StateData2} ->
            {next_state, waiting_vnode, StateData2}
    end.

%% @private
postcommit(timeout, StateData = #state{postcommit = []}) ->
    new_state_timeout(finish, StateData);
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

finish(timeout, StateData = #state{timing = Timing, reply = Reply}) ->
    case Reply of
        {error, _} ->
            ok;
        _Ok ->
            %% TODO: Improve reporting of timing
            %% For now can add debug tracers to view the return from calc_timing
            {Duration, _Stages} = calc_timing(Timing),
            riak_kv_stat:update({put_fsm_time, Duration})
    end,
    {stop, normal, StateData};
finish(Reply, StateData) -> % late responses - add to state
    StateData1 = add_vnode_result(Reply, StateData),
    {next_state, finish, StateData1, 0}.

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

%% ====================================================================
%% Internal functions
%% ====================================================================

%% Move to the new state, marking the time it started
new_state(StateName, StateData) ->
    {next_state, StateName, add_timing(StateName, StateData)}.

%% Move to the new state, marking the time it started and trigger an immediate
%% timeout.
new_state_timeout(StateName, StateData) ->
    {next_state, StateName, add_timing(StateName, StateData), 0}.

%% What to do once enough responses from vnodes have been received to reply
process_reply(Reply, StateData) ->
    StateData1 = client_reply(Reply, StateData),
    case Reply of
        ok ->
            new_state_timeout(postcommit, StateData1);
        {ok, _} ->
            new_state_timeout(postcommit, StateData1);
        _ ->
            new_state_timeout(finish, StateData1)
    end.

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
handle_options([{update_last_modified, false}|T], State) ->
    handle_options(T, State);
handle_options([{update_last_modified, true}|T], State = #state{robj = RObj}) ->
    handle_options(T, State#state{robj = update_last_modified(RObj)});
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

%% Apply any pending updates to robj
apply_updates(State = #state{robj = RObj}) ->
    State#state{robj = riak_object:apply_updates(RObj)}.

%%
%% Update X-Riak-VTag and X-Riak-Last-Modified in the object's metadata, if
%% necessary.
%%
%% @private
update_last_modified(RObj) ->
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

-spec decode_precommit(any()) -> fail | {fail, any()} | riak_object:riak_object().
decode_precommit({erlang, {Mod, Fun}, Result}) ->
    try
        case Result of
            fail ->
                fail;
            {fail, _Reason} ->
                Result;
            {'EXIT',  Mod, Fun, Class, Exception} ->
                error_logger:error_msg("problem invoking hook ~p:~p -> ~p:~p~n~p~n",
                                       [Mod,Fun,Class,Exception,
                                        erlang:get_stacktrace()]),
                {fail, {hook_crashed, {Mod, Fun, Class, Exception}}};
            Obj ->
                riak_object:ensure_robject(Obj)
        end
    catch
        _:_ ->
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

get_option(Name, Options, Default) ->
    proplists:get_value(Name, Options, Default).

schedule_timeout(infinity) ->
    undefined;
schedule_timeout(Timeout) ->
    erlang:send_after(Timeout, self(), request_timeout).

client_reply(Reply, State = #state{from = {raw, ReqId, Pid}, options = Options}) ->
    State2 = add_timing(reply, State),
    Reply2 = case proplists:get_value(details, Options, false) of
                 false ->
                     Reply;
                 [] ->
                     Reply;
                 Details ->
                     add_client_info(Reply, Details, State2)
             end,
    Pid ! {ReqId, Reply2},
    add_timing(reply, State2#state{reply = Reply}).

add_client_info(Reply, Details, State) ->
    Info = client_info(Details, State, []),
    case Reply of
        ok ->
            {ok, Info};
        {OkError, ObjReason} ->
            {OkError, ObjReason, Info}
    end.

client_info(true, StateData, Info) ->
    client_info(default_details(), StateData, Info);
client_info([], _StateData, Info) ->
    Info;
client_info([timing | Rest], StateData = #state{timing = Timing}, Info) ->
    %% Duration is time from receiving request to responding
    {ResponseUsecs, Stages} = calc_timing(Timing),
    client_info(Rest, StateData, [{response_usecs, ResponseUsecs},
                                  {stages, Stages} | Info]).

default_details() ->
    [timing].


%% Add timing information to the state
add_timing(Stage, State = #state{timing = Timing}) ->
    State#state{timing = [{Stage, os:timestamp()} | Timing]}.

%% Calc timing information - stored as {Stage, StageStart} in reverse order. 
%% ResponseUsecs is calculated as time from reply to start.
calc_timing([{Stage, Now} | Timing]) ->
    ReplyNow = case Stage of
                   reply ->
                       Now;
                   _ ->
                       undefined
               end,
    calc_timing(Timing, Now, ReplyNow, []).

%% Each timing stage has start time.
calc_timing([], StageEnd, ReplyNow, Stages) ->
    %% StageEnd is prepare time
    {timer:now_diff(ReplyNow, StageEnd), Stages}; 
calc_timing([{reply, ReplyNow}|_]=Timing, StageEnd, undefined, Stages) ->
    %% Populate ReplyNow then handle normally.
    calc_timing(Timing, StageEnd, ReplyNow, Stages);
calc_timing([{Stage, StageStart} | Rest], StageEnd, ReplyNow, Stages) ->
    calc_timing(Rest, StageStart, ReplyNow,
                [{Stage, timer:now_diff(StageEnd, StageStart)} | Stages]).

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

make_vtag_test() ->
    Obj = riak_object:new(<<"b">>,<<"k">>,<<"v1">>),
    ?assertNot(make_vtag(Obj) =:=
               make_vtag(riak_object:increment_vclock(Obj,<<"client_id">>))).

-endif.
