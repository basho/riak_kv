%% -------------------------------------------------------------------
%%
%% riak_put_fsm: coordination of Riak PUT requests
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved.
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
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.
-include_lib("riak_kv_vnode.hrl").
-include_lib("riak_kv_js_pools.hrl").
-include("riak_kv_wm_raw.hrl").
-include("riak_kv_types.hrl").

-behaviour(gen_fsm).
-define(DEFAULT_OPTS, [{returnbody, false}, {update_last_modified, true}]).
-export([start/3,start/6,start/7]).
-export([start_link/3,start_link/6,start_link/7]).
-export([set_put_coordinator_failure_timeout/1,
         get_put_coordinator_failure_timeout/0]).
-ifdef(TEST).
-export([test_link/4]).
-endif.
-export([init/1, handle_event/3, handle_sync_event/4,
         handle_info/3, terminate/3, code_change/4]).
-export([prepare/2, validate/2, precommit/2,
         waiting_local_vnode/2,
         waiting_remote_vnode/2,
         postcommit/2, finish/2]).

-type detail_info() :: timing.
-type detail() :: true |
                  false |
                  [detail_info()].

-type option() ::
        %% Min number of primary (owner) vnodes participating
        {pw, non_neg_integer()} |
        %% Minimum number of vnodes receiving write
        {w,  non_neg_integer()} |
        %% Minimum number of vnodes completing write
        {dw, non_neg_integer()} |
        {timeout, timeout()} |
        %% Prevent precommit/postcommit hooks from running
        disable_hooks |
        %% Request additional details about request added as extra
        %% element at the end of result tuple
        {details, detail()} |
        %% Put the value as-is, do not increment the vclocks
        %% to make the value a frontier.
        asis |
        %% Use a sloppy quorum, default = true
        {sloppy_quorum, boolean()} |
        %% The N value, default = value from bucket properties
        {n_val, pos_integer()} |
        %% Control server-side put failure retry, default = true.
        %% Some CRDTs and other client operations that cannot tolerate
        %% an automatic retry on the server side; those operations should
        %% use {retry_put_coordinator_failure, false}.
        {retry_put_coordinator_failure, boolean()}.

-type options() :: [option()].

-export_type([option/0, options/0, detail/0, detail_info/0]).

-record(state, {from :: {raw, integer(), pid()},
                robj :: riak_object:riak_object(),
                options=[] :: options(),
                n :: pos_integer(),
                w :: non_neg_integer(),
                dw :: non_neg_integer(),
                pw :: non_neg_integer(),
                node_confirms :: non_neg_integer(),
                coord_pl_entry :: {integer(), atom()},
                preflist2 :: riak_core_apl:preflist_ann(),
                bkey :: {riak_object:bucket(), riak_object:key()},
                req_id :: pos_integer(),
                starttime :: pos_integer(), % start time to send to vnodes
                timeout :: pos_integer()|infinity,
                tref    :: reference(),
                vnode_options=[] :: list(),
                returnbody :: boolean(),
                allowmult :: boolean(),
                precommit=[] :: list(),
                postcommit=[] :: list(),
                bucket_props:: list(),
                putcore :: riak_kv_put_core:putcore(),
                put_usecs :: undefined | non_neg_integer(),
                timing = [] :: [{atom(), {non_neg_integer(), non_neg_integer(),
                                          non_neg_integer()}}],
                reply, % reply sent to client,
                trace = false :: boolean(), 
                tracked_bucket=false :: boolean(), %% track per bucket stats
                bad_coordinators = [] :: [atom()],
                coordinator_timeout :: integer()
               }).

-include("riak_kv_dtrace.hrl").

-define(PARSE_INDEX_PRECOMMIT, {struct, [{<<"mod">>, <<"riak_index">>}, {<<"fun">>, <<"parse_object_hook">>}]}).
-define(DEFAULT_TIMEOUT, 60000).

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
    start({raw, ReqId, ResultPid}, RObj, [{w, W}, {dw, DW}, {timeout, Timeout} | Options]).

start(From, Object, PutOptions) ->
    Args = [From, Object, PutOptions],
    case sidejob_supervisor:start_child(riak_kv_put_fsm_sj,
                                        gen_fsm, start_link,
                                        [?MODULE, Args, []]) of
        {error, overload} ->
            riak_kv_util:overload_reply(From),
            {error, overload};
        {ok, Pid} ->
            {ok, Pid}
    end.

%% Included for backward compatibility, in case someone is, say, passing around
%% a riak_client instace between nodes during a rolling upgrade. The old
%% `start_link' function has been renamed `start' since it doesn't actually link
%% to the caller.
start_link(From, Object, PutOptions) -> start(From, Object, PutOptions).

set_put_coordinator_failure_timeout(MS) when is_integer(MS), MS >= 0 ->
    application:set_env(riak_kv, put_coordinator_failure_timeout, MS);
set_put_coordinator_failure_timeout(Bad) ->
    lager:error("~s:set_put_coordinator_failure_timeout(~p) invalid",
                [?MODULE, Bad]),
    set_put_coordinator_failure_timeout(3000).

get_put_coordinator_failure_timeout() ->
    app_helper:get_env(riak_kv, put_coordinator_failure_timeout, 3000).

make_ack_options(Options) ->
    case (riak_core_capability:get(
            {riak_kv, put_fsm_ack_execute}, disabled) == disabled
          orelse not
          app_helper:get_env(
            riak_kv, retry_put_coordinator_failure, true)) of
        true ->
            {false, Options};
        false ->
            case get_option(retry_put_coordinator_failure, Options, true) of
                true ->
                    {true, [{ack_execute, self()}|Options]};
                _Else ->
                    {false, Options}
            end
    end.

spawn_coordinator_proc(CoordNode, Mod, Fun, Args) ->
    %% If the net_kernel cannot talk to CoordNode, then any variation
    %% of the spawn BIF will block.  The whole point of picking a new
    %% coordinator node is being able to pick a new coordinator node
    %% and try it ... without blocking for dozens of seconds.
    spawn(fun() ->
                  proc_lib:spawn(CoordNode, Mod, Fun, Args)
          end).

monitor_remote_coordinator(false = _UseAckP, _MiddleMan, _CoordNode, StateData) ->
    {stop, normal, StateData};
monitor_remote_coordinator(true = _UseAckP, MiddleMan, CoordNode, StateData) ->
    receive
        {ack, CoordNode, now_executing} ->
            {stop, normal, StateData}
    after StateData#state.coordinator_timeout ->
            exit(MiddleMan, kill),
            Bad = StateData#state.bad_coordinators,
            prepare(timeout, StateData#state{bad_coordinators=[CoordNode|Bad]})
    end.

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
init([From, RObj, Options0]) ->
    BKey = {Bucket, Key} = {riak_object:bucket(RObj), riak_object:key(RObj)},
    CoordTimeout = get_put_coordinator_failure_timeout(),
    Trace = app_helper:get_env(riak_kv, fsm_trace_enabled),
    Options = proplists:unfold(Options0),
    StateData = #state{from = From,
                       robj = RObj,
                       bkey = BKey,
                       trace = Trace,
                       options = Options,
                       timing = riak_kv_fsm_timing:add_timing(prepare, []),
                       coordinator_timeout=CoordTimeout},
    case Trace of
        true ->
            riak_core_dtrace:put_tag([Bucket, $,, Key]),
            case riak_kv_util:is_x_deleted(RObj) of
                true  ->
                    TombNum = 1,
                    TombStr = <<"tombstone">>;
                false ->
                    TombNum = 0,
                    TombStr = <<>>
            end,
            ?DTRACE(?C_PUT_FSM_INIT, [TombNum], ["init", TombStr]);
        _ ->
            ok
    end,
    gen_fsm:send_event(self(), timeout),
    {ok, prepare, StateData};
init({test, Args, StateProps}) ->
    %% Call normal init
    {ok, prepare, StateData} = init(Args),

    %% Then tweak the state record with entries provided by StateProps
    Fields = record_info(fields, state),
    FieldPos = lists:zip(Fields, lists:seq(2, length(Fields)+1)),
    F = fun({Field, Value}, State0) ->
                Pos = get_option(Field, FieldPos),
                setelement(Pos, State0, Value)
        end,
    TestStateData = lists:foldl(F, StateData, StateProps),

    %% Enter into the validate state, skipping any code that relies on the
    %% state of the rest of the system
    {ok, validate, TestStateData}.

%% @private
prepare(timeout, State = #state{robj = RObj, options=Options}) ->
    Bucket = riak_object:bucket(RObj),
    BucketProps = get_bucket_props(Bucket),
    StatTracked = get_option(stat_tracked, BucketProps, false),
    N = get_n_val(Options, BucketProps),
    get_preflist(N, State#state{tracked_bucket=StatTracked, bucket_props=BucketProps}).

%% @private
validate(timeout, StateData0 = #state{from = {raw, ReqId, _Pid},
                                      options = Options0,
                                      robj = RObj0,
                                      n=N, bucket_props = BucketProps,
                                      trace = Trace,
                                      preflist2 = Preflist2}) ->
    Timeout = get_option(timeout, Options0, ?DEFAULT_TIMEOUT),
    PW0 = get_option(pw, Options0, default),
    NodeConfirms0 = get_option(node_confirms, Options0, default),
    W0 = get_option(w, Options0, default),
    DW0 = get_option(dw, Options0, default),

    PW = riak_kv_util:expand_rw_value(pw, PW0, BucketProps, N),
    NodeConfirms = riak_kv_util:expand_rw_value(node_confirms, NodeConfirms0, BucketProps, N),
    W = riak_kv_util:expand_rw_value(w, W0, BucketProps, N),

    %% Expand the DW value, but also ensure that DW <= W
    DW1 = riak_kv_util:expand_rw_value(dw, DW0, BucketProps, N),
    %% If no error occurred expanding DW also ensure that DW <= W
    case DW1 of
        error ->
            DW = error;
        _ ->
            %% DW must always be 1 with node-based vclocks.
            %% The coord vnode is responsible for incrementing the vclock
            DW = erlang:max(DW1, 1)
    end,

    IdxType = [{Part, Type, Node} || {{Part, Node}, Type} <- Preflist2],
    NumPrimaries = length([x || {_,primary} <- Preflist2]),
    NumVnodes = length(Preflist2),
    MinVnodes = lists:max([1, W, DW, PW]), % always need at least one vnode

    if
        PW =:= error ->
            process_reply({error, {pw_val_violation, PW0}}, StateData0);
        NodeConfirms =:= error ->
            process_reply({error, {node_confirms_val_violation, NodeConfirms0}}, StateData0);
        W =:= error ->
            process_reply({error, {w_val_violation, W0}}, StateData0);
        DW =:= error ->
            process_reply({error, {dw_val_violation, DW0}}, StateData0);
        (W > N) or (DW > N) or (PW > N) or (NodeConfirms > N)->
            process_reply({error, {n_val_violation, N}}, StateData0);
        PW > NumPrimaries ->
            process_reply({error, {pw_val_unsatisfied, PW, NumPrimaries}}, StateData0);
        NumVnodes < MinVnodes ->
            process_reply({error, {insufficient_vnodes, NumVnodes,
                                   need, MinVnodes}}, StateData0);
        true ->
            AllowMult = get_option(allow_mult, BucketProps),
            Options = flatten_options(Options0 ++ ?DEFAULT_OPTS, []),
            Disable = get_option(disable_hooks, Options),
            Precommit =
                if Disable -> [];
                   true ->
                        L = get_hooks(precommit, BucketProps),
                        L ++ [?PARSE_INDEX_PRECOMMIT]
                end,
            Postcommit =
                if Disable -> [];
                   true -> get_hooks(postcommit, BucketProps, StateData0)
                end,
            {VNodeOpts0, ReturnBody} =
                case get_option(returnbody, Options) of
                    true ->
                        {[], true};
                    _ ->
                        case Postcommit of
                            [] -> 
                                {[], false};
                            _ -> 
                                {[{returnbody,true}], false}
                        end
                end,
            PutCore = riak_kv_put_core:init(N, W, PW, NodeConfirms, DW,
                                            AllowMult,
                                            ReturnBody,
                                            IdxType),
            VNodeOpts = handle_options(Options, VNodeOpts0),
            StateData = StateData0#state{n=N,
                                         w=W,
                                         pw=PW, node_confirms=NodeConfirms, dw=DW,
                                         allowmult=AllowMult,
                                         precommit = Precommit,
                                         postcommit = Postcommit,
                                         req_id = ReqId,
                                         robj = apply_updates(RObj0, Options),
                                         putcore = PutCore,
                                         vnode_options = VNodeOpts,
                                         timeout = Timeout},
            ?DTRACE(Trace, ?C_PUT_FSM_VALIDATE, [N, W, PW, NodeConfirms, DW], []),
            case Precommit of
                [] -> % Nothing to run, spare the timing code
                    execute(StateData);
                _ ->
                    new_state_timeout(precommit, StateData)
            end
    end.

apply_updates(RObj0, Options) ->
    RObj1 = 
        case get_option(update_last_modified, Options) of
            true ->
                riak_object:update_last_modified(RObj0);
            _ ->
                RObj0
        end,
    riak_object:apply_updates(RObj1).
    

%% Run the precommit hooks
precommit(timeout, State = #state{precommit = []}) ->
    execute(State);
precommit(timeout, State = #state{precommit = [Hook | Rest], 
                                  robj = RObj,
                                  trace = Trace}) ->
    Result = decode_precommit(invoke_hook(Hook, RObj), Trace),
    case Result of
        fail ->
            ?DTRACE(Trace, ?C_PUT_FSM_PRECOMMIT, [-1], []),
            process_reply({error, precommit_fail}, State);
        {fail, Reason} ->
            ?DTRACE(Trace, ?C_PUT_FSM_PRECOMMIT, [-1], 
                    [dtrace_errstr(Reason)]),
            process_reply({error, {precommit_fail, Reason}}, State);
        Result ->
            ?DTRACE(Trace, ?C_PUT_FSM_PRECOMMIT, [0], []),
            {next_state, precommit, State#state{robj = riak_object:apply_updates(Result),
                                                precommit = Rest}, 0}
    end.

%% @private
execute(State=#state{options = Options, timeout = Timeout, coord_pl_entry = CPL}) ->
    %% If we are a forwarded coordinator, the originating node is expecting
    %% an ack from us.
    case get_option(ack_execute, Options) of
        undefined ->
            ok;
        Pid ->
            Pid ! {ack, node(), now_executing}
    end,
    TRef = schedule_timeout(Timeout),
    NewState = State#state{tref = TRef},
    case CPL of
        undefined ->
            execute_remote(NewState);
        _ ->
            execute_local(NewState)
    end.

%% @private
%% Send the put coordinating put requests to the local vnode - the returned object
%% will guarantee a frontier object.
%% N.B. Not actually a state - here in the source to make reading the flow easier
execute_local(StateData=#state{robj=RObj, req_id = ReqId, bkey=BKey,
                               coord_pl_entry = {_Index, Node} = CoordPLEntry,
                               vnode_options=VnodeOptions,
                               trace = Trace,
                               starttime = StartTime}) ->
    StateData1 = 
        case Trace of 
            true ->
                ?DTRACE(?C_PUT_FSM_EXECUTE_LOCAL, [], [atom2list(Node)]),
                add_timing(execute_local, StateData);
            _ ->
                StateData
        end,
    riak_kv_vnode:coord_put(CoordPLEntry, BKey, RObj, ReqId, StartTime, VnodeOptions),
    StateData2 = StateData1#state{robj = RObj},
    %% Must always wait for local vnode - it contains the object with updated vclock
    %% to use for the remotes. (Ignore optimization for N=1 case for now).
    new_state(waiting_local_vnode, StateData2).

%% @private
waiting_local_vnode(request_timeout, StateData=#state{trace = Trace}) ->
    ?DTRACE(Trace, ?C_PUT_FSM_WAITING_LOCAL_VNODE, [-1], []),
    process_reply({error,timeout}, StateData);
waiting_local_vnode(Result, StateData = #state{putcore = PutCore,
                                               trace = Trace}) ->
    UpdPutCore1 = riak_kv_put_core:add_result(Result, PutCore),
    case Result of
        {fail, Idx, Reason} ->
            ?DTRACE(Trace, ?C_PUT_FSM_WAITING_LOCAL_VNODE, [-1],
                    [integer_to_list(Idx)]),
            %% Local vnode failure is enough to sink whole operation
            process_reply({error, Reason}, StateData#state{putcore = UpdPutCore1});
        {w, Idx, _ReqId} ->
            ?DTRACE(Trace, ?C_PUT_FSM_WAITING_LOCAL_VNODE, [1],
                    [integer_to_list(Idx)]),
            {next_state, waiting_local_vnode, StateData#state{putcore = UpdPutCore1}};
        {dw, Idx, PutObj, _ReqId} ->
            %% Either returnbody is true or coord put merged with the existing
            %% object and bumped the vclock.  Either way use the returned
            %% object for the remote vnode
            ?DTRACE(Trace, ?C_PUT_FSM_WAITING_LOCAL_VNODE, [2],
                    [integer_to_list(Idx)]),
            execute_remote(StateData#state{robj = PutObj, putcore = UpdPutCore1});
        {dw, Idx, _ReqId} ->
            %% Write succeeded without changes to vclock required and returnbody false
            ?DTRACE(Trace, ?C_PUT_FSM_WAITING_LOCAL_VNODE, [2],
                    [integer_to_list(Idx)]),
            execute_remote(StateData#state{putcore = UpdPutCore1})
    end.

%% @private
%% Send the put requests to any remote nodes if necessary and decided if
%% enough responses have been received yet (i.e. if W/DW=1)
%% N.B. Not actually a state - here in the source to make reading the flow easier
execute_remote(StateData=#state{robj=RObj, req_id = ReqId,
                                preflist2 = Preflist2, bkey = BKey,
                                coord_pl_entry = CoordPLEntry,
                                vnode_options = VnodeOptions,
                                putcore = PutCore,
                                trace = Trace,
                                starttime = StartTime}) ->
    Preflist = [IndexNode || {IndexNode, _Type} <- Preflist2,
                             IndexNode /= CoordPLEntry],
    StateData1 = 
        case Trace of
            true ->
                Ps = [[atom2list(Nd), $,, integer_to_list(Idx)] ||
                         {Idx, Nd} <- lists:sublist(Preflist, 4)],
                ?DTRACE(?C_PUT_FSM_EXECUTE_REMOTE, [], [Ps]),
                add_timing(execute_remote, StateData);
            _ ->
                StateData
        end,
    riak_kv_vnode:put(Preflist, BKey, RObj, ReqId, StartTime, VnodeOptions),
    case riak_kv_put_core:enough(PutCore) of
        true ->
            {Reply, UpdPutCore} = riak_kv_put_core:response(PutCore),
            process_reply(Reply, StateData1#state{putcore = UpdPutCore});
        false ->
            new_state(waiting_remote_vnode, StateData1)
    end.


%% @private
waiting_remote_vnode(request_timeout, StateData=#state{trace = Trace}) ->
    ?DTRACE(Trace, ?C_PUT_FSM_WAITING_REMOTE_VNODE, [-1], []),
    process_reply({error,timeout}, StateData);
waiting_remote_vnode(Result, StateData = #state{putcore = PutCore,
                                                trace = Trace}) ->
    case Trace of
        true ->
            ShortCode = riak_kv_put_core:result_shortcode(Result),
            IdxStr = integer_to_list(riak_kv_put_core:result_idx(Result)),
            ?DTRACE(?C_PUT_FSM_WAITING_REMOTE_VNODE, [ShortCode], [IdxStr]);
        _ ->
            ok
    end,
    UpdPutCore1 = riak_kv_put_core:add_result(Result, PutCore),
    case riak_kv_put_core:enough(UpdPutCore1) of
        true ->
            {Reply, UpdPutCore2} = riak_kv_put_core:response(UpdPutCore1),
            process_reply(Reply, StateData#state{putcore = UpdPutCore2});
        false ->
            {next_state, waiting_remote_vnode, StateData#state{putcore = UpdPutCore1}}
    end.

%% @private
postcommit(timeout, StateData = #state{postcommit = [], trace = Trace}) ->
    ?DTRACE(Trace, ?C_PUT_FSM_POSTCOMMIT, [0], []),
    new_state_timeout(finish, StateData);
postcommit(timeout, StateData = #state{postcommit = [Hook | Rest],
                                       trace = Trace,
                                       putcore = PutCore}) ->
    ?DTRACE(Trace, ?C_PUT_FSM_POSTCOMMIT, [-2], []),
    %% Process the next hook - gives sys:get_status messages a chance if hooks
    %% take a long time.
    {ReplyObj, UpdPutCore} =  riak_kv_put_core:final(PutCore),
    decode_postcommit(invoke_hook(Hook, ReplyObj), Trace),
    {next_state, postcommit, StateData#state{postcommit = Rest,
                                             trace = Trace,
                                             putcore = UpdPutCore}, 0};
%% still process hooks even if request timed out  
postcommit(request_timeout, StateData = #state{trace = Trace}) -> 
    ?DTRACE(Trace, ?C_PUT_FSM_POSTCOMMIT, [-3], []),
    {next_state, postcommit, StateData, 0};
postcommit(Reply, StateData = #state{putcore = PutCore,
                                     trace = Trace}) ->
    case Trace of
        true ->
            ShortCode = riak_kv_put_core:result_shortcode(Reply),
            IdxStr = integer_to_list(riak_kv_put_core:result_idx(Reply)),
            ?DTRACE(?C_PUT_FSM_POSTCOMMIT, [0, ShortCode], [IdxStr]);
        _ ->
            ok
    end,
    %% late responses - add to state.  *Does not* recompute finalobj
    UpdPutCore = riak_kv_put_core:add_result(Reply, PutCore),
    {next_state, postcommit, StateData#state{putcore = UpdPutCore}, 0}.

finish(timeout, StateData = #state{timing = Timing, reply = Reply,
                                   bkey = {Bucket, _Key},
                                   trace = Trace,
                                   tracked_bucket = StatTracked,
                                   options = Options}) ->
    case Reply of
        {error, _} -> 
            ?DTRACE(Trace, ?C_PUT_FSM_FINISH, [-1], []),
            ok;
        _Ok ->
            %% TODO: Improve reporting of timing
            %% For now can add debug tracers to view the return from calc_timing
            CRDTMod = case get_option(crdt_op, Options) of
                #crdt_op{mod=Mod} -> Mod;
                _ -> undefined
            end,
            {Duration, Stages} = riak_kv_fsm_timing:calc_timing(Timing),
            ok = riak_kv_stat:update({put_fsm_time, Bucket, Duration,
                                      Stages, StatTracked, CRDTMod}),
            ?DTRACE(Trace, ?C_PUT_FSM_FINISH, [0, Duration], [])
    end,
    {stop, normal, StateData};
finish(Reply, StateData = #state{putcore = PutCore,
                                 trace = Trace}) ->
    case Trace of
        true ->
            ShortCode = riak_kv_put_core:result_shortcode(Reply),
            IdxStr = integer_to_list(riak_kv_put_core:result_idx(Reply)),
            ?DTRACE(?C_PUT_FSM_FINISH, [1, ShortCode], [IdxStr]);
        _ ->
            ok
    end,
    %% late responses - add to state.  *Does not* recompute finalobj
    UpdPutCore = riak_kv_put_core:add_result(Reply, PutCore),
    {next_state, finish, StateData#state{putcore = UpdPutCore}, 0}.


%% @private
handle_event(_Event, _StateName, StateData) ->
    {stop,badmsg,StateData}.

%% @private
handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop,badmsg,StateData}.

%% @private

handle_info(request_timeout, StateName, StateData) ->
    ?MODULE:StateName(request_timeout, StateData);
handle_info({ack, Node, now_executing}, StateName, StateData) ->
    late_put_fsm_coordinator_ack(Node),
    ok = riak_kv_stat:update(late_put_fsm_coordinator_ack),
    {next_state, StateName, StateData};
handle_info({mbox, _}, StateName, StateData) ->
    %% Delayed mailbox size check response, ignore it
    {next_state, StateName, StateData};
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
new_state(StateName, StateData=#state{trace = true}) ->
    {next_state, StateName, add_timing(StateName, StateData)};
new_state(StateName, StateData) ->
    {next_state, StateName, StateData}.

%% Move to the new state, marking the time it started and trigger an immediate
%% timeout.
new_state_timeout(StateName, StateData=#state{trace = true}) ->
    gen_fsm:send_event(self(), timeout),
    {next_state, StateName, add_timing(StateName, StateData)};
new_state_timeout(StateName, StateData) ->
    gen_fsm:send_event(self(), timeout),
    {next_state, StateName, StateData}.

%% What to do once enough responses from vnodes have been received to reply
process_reply(Reply, StateData = #state{postcommit = PostCommit,
                                        putcore = PutCore,
                                        robj = RObj,
                                        trace = Trace,
                                        bkey = {Bucket, Key}}) ->
    StateData1 = client_reply(Reply, StateData),
    StateData2 = case PostCommit of
                     [] ->
                         StateData1;
                     _ ->
                         %% If postcommits defined, calculate final object
                         %% before any replies received after responding to
                         %% the client for a consistent view.
                         {_, UpdPutCore} = riak_kv_put_core:final(PutCore),
                         StateData1#state{putcore = UpdPutCore}
                 end,
    case Reply of
        ok ->
            ?DTRACE(Trace, ?C_PUT_FSM_PROCESS_REPLY, [0], []),
            new_state_timeout(postcommit, StateData2);
        {ok, _} ->
            Values = riak_object:get_values(RObj),
            %% TODO: more accurate sizing method
            case Trace of
                true ->
                    ApproxBytes = size(Bucket) + size(Key) +
                        lists:sum([size(V) || V <- Values]),
                    NumSibs = length(Values),
                    ?DTRACE(?C_PUT_FSM_PROCESS_REPLY, 
                            [1, ApproxBytes, NumSibs], []);
                _ ->
                    ok
            end,
            new_state_timeout(postcommit, StateData2);
        _ ->
            ?DTRACE(Trace, ?C_PUT_FSM_PROCESS_REPLY, [-1], []),
            new_state_timeout(finish, StateData2)
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
handle_options([], Acc) ->
    Acc;
handle_options([{returnbody, true}|T], Acc) ->
    VNodeOpts = [{returnbody, true} | Acc],
    handle_options(T, VNodeOpts);
handle_options([{counter_op, _Amt}=COP|T], Acc) ->
    VNodeOpts = [COP | Acc],
    handle_options(T, VNodeOpts);
handle_options([{crdt_op, _Op}=COP|T], Acc) ->
    VNodeOpts = [COP | Acc],
    handle_options(T, VNodeOpts);
handle_options([{K, _V} = Opt|T], Acc)
  when K == sloppy_quorum; K == n_val ->
    %% Take these options as-is
    handle_options(T, [Opt|Acc]);
handle_options([{_,_}|T], Acc) -> handle_options(T, Acc).

%% Invokes the hook and returns a tuple of
%% {Lang, Called, Result}
%% Where Called = {Mod, Fun} if Lang = erlang
%%       Called = JSName if Lang = javascript
invoke_hook({struct, Hook}=HookDef, RObj) ->
    Mod = get_option(<<"mod">>, Hook),
    Fun = get_option(<<"fun">>, Hook),
    JSName = get_option(<<"name">>, Hook),
    if (Mod == undefined orelse Fun == undefined) andalso JSName == undefined ->
            {error, {invalid_hook_def, HookDef}};
       true -> invoke_hook(Mod, Fun, JSName, RObj)
    end;
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

-spec decode_precommit(any(), boolean()) -> fail | {fail, any()} | 
                                            riak_object:riak_object().
decode_precommit({erlang, {Mod, Fun}, Result}, Trace) ->
    %% TODO: For DTrace things, we will err on the side of taking the
    %%       time to format the error results into strings to pass to
    %%       the probes.  If this ends up being too slow, then revisit.
    case Result of
        fail ->
            ?DTRACE(Trace, ?C_PUT_FSM_DECODE_PRECOMMIT, [-1], []),
            ok = riak_kv_stat:update(precommit_fail),
            lager:debug("Pre-commit hook ~p:~p failed, no reason given",
                        [Mod, Fun]),
            fail;
        {fail, Reason} ->
            ?DTRACE(Trace, ?C_PUT_FSM_DECODE_PRECOMMIT, [-2], 
                    [dtrace_errstr(Reason)]),
            ok = riak_kv_stat:update(precommit_fail),
            lager:debug("Pre-commit hook ~p:~p failed with reason ~p",
                        [Mod, Fun, Reason]),
            Result;
        {'EXIT',  Mod, Fun, Class, Exception} ->
            ?DTRACE(Trace, ?C_PUT_FSM_DECODE_PRECOMMIT, [-3],
                    [dtrace_errstr({Mod, Fun, Class, Exception})]),
            ok = riak_kv_stat:update(precommit_fail),
            lager:debug("Problem invoking pre-commit hook ~p:~p -> ~p:~p~n~p",
                        [Mod,Fun,Class,Exception, erlang:get_stacktrace()]),
            {fail, {hook_crashed, {Mod, Fun, Class, Exception}}};
        Obj ->
            try
                riak_object:ensure_robject(Obj)
            catch X:Y ->
                    ?DTRACE(Trace, ?C_PUT_FSM_DECODE_PRECOMMIT, [-4],
                                    [dtrace_errstr({Mod, Fun, X, Y})]),
                    ok = riak_kv_stat:update(precommit_fail),
                    lager:debug("Problem invoking pre-commit hook ~p:~p,"
                                " invalid return ~p",
                                [Mod, Fun, Result]),
                    {fail, {invalid_return, {Mod, Fun, Result}}}

            end
    end;
decode_precommit({js, JSName, Result}, Trace) ->
    case Result of
        {ok, <<"fail">>} ->
            ?DTRACE(Trace, ?C_PUT_FSM_DECODE_PRECOMMIT, [-5], []),
            ok = riak_kv_stat:update(precommit_fail),
            lager:debug("Pre-commit hook ~p failed, no reason given",
                        [JSName]),
            fail;
        {ok, [{<<"fail">>, Message}]} ->
            ?DTRACE(Trace, ?C_PUT_FSM_DECODE_PRECOMMIT, [-6],
                    [dtrace_errstr(Message)]),
            ok = riak_kv_stat:update(precommit_fail),
            lager:debug("Pre-commit hook ~p failed with reason ~p",
                        [JSName, Message]),
            {fail, Message};
        {ok, Json} ->
            case catch riak_object:from_json(Json) of
                {'EXIT', _} ->
                    ?DTRACE(Trace, ?C_PUT_FSM_DECODE_PRECOMMIT, [-7], []),
                    {fail, {invalid_return, {JSName, Json}}};
                Obj ->
                    Obj
            end;
        {error, Error} ->
            ok = riak_kv_stat:update(precommit_fail),
            ?DTRACE(Trace, ?C_PUT_FSM_DECODE_PRECOMMIT, [-7], 
                    [dtrace_errstr(Error)]),
            lager:debug("Problem invoking pre-commit hook: ~p", [Error]),
            fail
    end;
decode_precommit({error, Reason}, Trace) ->
    ?DTRACE(Trace, ?C_PUT_FSM_DECODE_PRECOMMIT, [-8], 
            [dtrace_errstr(Reason)]),
    ok = riak_kv_stat:update(precommit_fail),
    lager:debug("Problem invoking pre-commit hook: ~p", [Reason]),
    {fail, Reason}.

decode_postcommit({erlang, {M,F}, Res}, Trace) ->
    case Res of
        fail ->
            ?DTRACE(Trace, ?C_PUT_FSM_DECODE_POSTCOMMIT, [-1], []),
            ok = riak_kv_stat:update(postcommit_fail),
            lager:debug("Post-commit hook ~p:~p failed, no reason given",
                       [M, F]);
        {fail, Reason} ->
            ?DTRACE(Trace, ?C_PUT_FSM_DECODE_POSTCOMMIT, [-2],
                    [dtrace_errstr(Reason)]),
            ok = riak_kv_stat:update(postcommit_fail),
            lager:debug("Post-commit hook ~p:~p failed with reason ~p",
                        [M, F, Reason]);
        {'EXIT', _, _, Class, Ex} ->
            ?DTRACE(Trace, ?C_PUT_FSM_DECODE_POSTCOMMIT, [-3],
                    [dtrace_errstr({M, F, Class, Ex})]),
            ok = riak_kv_stat:update(postcommit_fail),
            Stack = erlang:get_stacktrace(),
            lager:debug("Problem invoking post-commit hook ~p:~p -> ~p:~p~n~p",
                        [M, F, Class, Ex, Stack]),
            ok;
        _ ->
            ok
    end;
decode_postcommit({error, {invalid_hook_def, Def}}, Trace) ->
    ?DTRACE(Trace, ?C_PUT_FSM_DECODE_POSTCOMMIT, [-4], [dtrace_errstr(Def)]),
    ok = riak_kv_stat:update(postcommit_fail),
    lager:debug("Invalid post-commit hook definition ~p", [Def]).


get_hooks(HookType, BucketProps) ->
    Hooks = get_option(HookType, BucketProps, []),
    case Hooks of
        <<"none">> ->
            [];
        Hooks when is_list(Hooks) ->
            Hooks
    end.

get_hooks(postcommit, BucketProps, #state{bkey=BKey}) ->
    BaseHooks = get_hooks(postcommit, BucketProps),
    CondHooks = riak_kv_hooks:get_conditional_postcommit(BKey, BucketProps),
    BaseHooks ++ (CondHooks -- BaseHooks).

get_option(Name, Options) ->
    get_option(Name, Options, undefined).

get_option(Name, Options, Default) ->
    case lists:keyfind(Name, 1, Options) of
        {_, Val} ->
            Val;
        false ->
            Default
    end.

schedule_timeout(infinity) ->
    undefined;
schedule_timeout(Timeout) ->
    erlang:send_after(Timeout, self(), request_timeout).

client_reply(Reply, State = #state{from = {raw, ReqId, Pid},
                                   timing = Timing0,
                                   options = Options}) ->
    Timing = riak_kv_fsm_timing:add_timing(reply, Timing0),
    Reply2 = case get_option(details, Options, false) of
                 false ->
                     Reply;
                 [] ->
                     Reply;
                 Details ->
                     add_client_info(Reply, Details, 
                                     State#state{timing = Timing})
             end,
    Pid ! {ReqId, Reply2},
    State#state{reply = Reply, 
                timing = Timing}.

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
    {ResponseUsecs, Stages} = riak_kv_fsm_timing:calc_timing(Timing),
    client_info(Rest, StateData, [{response_usecs, ResponseUsecs},
                                  {stages, Stages} | Info]).

default_details() ->
    [timing].


%% Add timing information to the state
add_timing(Stage, State = #state{timing = Timing}) ->
    State#state{timing = riak_kv_fsm_timing:add_timing(Stage, Timing)}.

atom2list(A) when is_atom(A) ->
    atom_to_list(A);
atom2list(P) when is_pid(P)->
    pid_to_list(P).                             % eunit tests

dtrace_errstr(Term) ->
    io_lib:format("~P", [Term, 12]).

%% This function is for dbg tracing purposes
late_put_fsm_coordinator_ack(_Node) ->
    ok.

-spec get_bucket_props(riak_object:bucket()) -> list().
get_bucket_props(Bucket) ->
    {ok, DefaultProps} = application:get_env(riak_core,
                                             default_bucket_props),
    BucketProps = riak_core_bucket:get_bucket(Bucket),
    %% typed buckets never fall back to defaults
    case is_tuple(Bucket) of
        false ->
            lists:keymerge(1, lists:keysort(1, BucketProps),
                           lists:keysort(1, DefaultProps));
        true ->
            BucketProps
    end.

%% @private decide on the N Val for the put request, and error if
%% there is a violation.
get_n_val(Options, BucketProps) ->
    Bucket_N = get_option(n_val, BucketProps),
    case get_option(n_val, Options, false) of
        false ->
            Bucket_N;
        N_val when is_integer(N_val), N_val > 0, N_val =< Bucket_N ->
            %% don't allow custom N to exceed bucket N
            N_val;
        Bad_N ->
            {error, {n_val_violation, Bad_N}}
    end.

%% @private given no n-val violation, generate a preflist.
get_preflist({error, _Reason}=Err, State) ->
    process_reply(Err, State);
get_preflist(N, State) ->
    #state{bkey = BKey,
           options = Options,
           bucket_props=BucketProps,
           bad_coordinators = BadCoordinators} = State,

    DocIdx = riak_core_util:chash_key(BKey, BucketProps),

    Preflist =
        case get_option(sloppy_quorum, Options, true) of
            true ->
                UpNodes = riak_core_node_watcher:nodes(riak_kv),
                riak_core_apl:get_apl_ann(DocIdx, N,
                                          UpNodes -- BadCoordinators);
            false ->
                Preflist0 =
                    riak_core_apl:get_primary_apl(DocIdx, N, riak_kv),
                [X || X = {{_Index, Node}, _Type} <- Preflist0,
                      not lists:member(Node, BadCoordinators)]
        end,

    coordinate_or_forward(Preflist, State#state{n=N}).

%% @private if there is a non-empty preflist, select a coordinator, as
%% needed.
coordinate_or_forward([], State=#state{trace=Trace}) ->
    %% Empty preflist
    ?DTRACE(Trace, ?C_PUT_FSM_PREPARE, [-1],
            ["prepare",<<"all nodes down">>]),
    process_reply({error, all_nodes_down}, State);
coordinate_or_forward(Preflist, State) ->
    #state{options = Options, n = N, trace=Trace} = State,
    CoordinatorType = get_coordinator_type(Options),
    MBoxCheck = get_option(mbox_check, Options, true),
    {LocalPL, RemotePL} = partition_local_remote(Preflist),

    case select_coordinator(LocalPL, RemotePL, CoordinatorType, MBoxCheck) of
        {local, CoordPLEntry} ->
            CoordPlNode = case CoordPLEntry of
                              undefined  -> undefined;
                              {_Idx, Nd} -> atom2list(Nd)
                          end,
            StartTime = riak_core_util:moment(),
            StateData = State#state{n = N,
                                    coord_pl_entry = CoordPLEntry,
                                    preflist2 = Preflist,
                                    starttime = StartTime},
            ?DTRACE(Trace, ?C_PUT_FSM_PREPARE, [0],
                    ["prepare", CoordPlNode]),
            new_state_timeout(validate, StateData);
        {loaded_forward, ForwardNode} ->
            %% This is a soft-overload forward, we don't want to
            %% bounce around, we chose the "best" coordinator, update
            %% the options to short-circuit mailbox checking in the
            %% new, forwarded put fsm
            State2 = State#state{options=[{mbox_check, false} | Options]},
            forward(ForwardNode, State2);
        {forward, ForwardNode} ->
            forward(ForwardNode, State)
    end.

select_coordinator(_LocalPreflist=[], RemotePreflist, _CoordinatorType=local, _MBoxCheck) ->
    %% Wants local, no local PL, chose a node at random to forward to
    %% NOTE: `RemotePreflist' cannot be empty, if LocalPreflist is empty, or
    %% the original Preflist was empty, and that is handled in
    %% `coordinate_or_forward/2'
    %% @TODO - Do we want to check the mbox size first?
    {ListPos, _} = random:uniform_s(length(RemotePreflist), os:timestamp()),
    {_Idx, CoordNode} = lists:nth(ListPos, RemotePreflist),
    {forward, CoordNode};
select_coordinator(LocalPreflist, RemotePreflist, _CoordinatorType=local, true=_MBoxCheck) ->
    %% wants local, there are local entries, check mailbox soft
    %% limits (see riak#1661)
    case check_mailboxes(LocalPreflist) of
        {true, Entry} ->
            {local, Entry};
        {false, LocalMBoxData} ->
            case check_mailboxes(RemotePreflist) of
                {true, {_Idx, Remote}} ->
                    lager:info("loaded forward"),
                    {loaded_forward, Remote};
                {false, RemoteMBoxData} ->
                    select_least_loaded_coordinator(LocalMBoxData, RemoteMBoxData)
            end
    end;
select_coordinator(LocalPreflist, _RemotePreflist, _CoordinatorType=local, false=_MBoxCheck) ->
    %% No mailbox check, don't change behaviour from pre-gh1661 work
    {local, hd(LocalPreflist)};
select_coordinator(_LocalPreflist, _RemotePreflist, any=_CoordinatorType, _MBoxCheck) ->
    %% for `any' type coordinator downstream code expects `undefined',
    %% no coordinator, no mailbox queues to route around
    {local, undefined}.

-define(DEFAULT_MBOX_CHECK_TIMEOUT_MILLIS, 100).

%% @private check the mailboxes for the preflist.
check_mailboxes(Preflist) ->
    TimeLimit = get_timestamp_millis() + ?DEFAULT_MBOX_CHECK_TIMEOUT_MILLIS,
    _ = [check_mailbox(Entry) || Entry <- Preflist],
    case join_mbox_replies(length(Preflist),
                           TimeLimit,
                           []) of
        {true, Entry} ->
            {true, Entry};
        {ok, MBoxData} ->
            {false, MBoxData};
        {timeout, MBoxData0} ->
            lager:warning("Mailbox soft-load poll timout ~p",
                          [?DEFAULT_MBOX_CHECK_TIMEOUT_MILLIS]),
            MBoxData = add_errors_to_mbox_data(Preflist, MBoxData0),
            {false, MBoxData}
    end.

%% @private cast off to vnode proxy for mailbox size.
check_mailbox({Idx, Node}=Entry) ->
    RegName = riak_core_vnode_proxy:reg_name(riak_kv_vnode, Idx, Node),
    riak_core_vnode_proxy:cast(RegName, {mailbox_size, self(), Entry}).

%% @private wait at most `TimeOutMillis' for `HowMany' `{mbox, _}'
%% replies from riak_core_vnode_proxy. Returns either `Acc' as mbox
%% data or short-circuits to `{true, Entry}' if any proxy replies
%% below the soft limit.
join_mbox_replies(0, _TimeLimit, Acc) ->
    {ok, Acc};
join_mbox_replies(HowMany, TimeLimit, Acc) ->
    TimeOut = max(0, TimeLimit - get_timestamp_millis()),
    receive
        {mbox, {Entry, {ok, _Mbox, _Limit}}} ->
            %% shortcut it
            {true, Entry};
        {mbox, {Entry, {soft_loaded, MBox, Limit}}} ->
            %% Keep waiting
            lager:warning("Mailbox for ~p with soft-overload", [Entry]),
            join_mbox_replies(HowMany-1, TimeLimit,
                 [{Entry, MBox, Limit} | Acc])
    after TimeOut ->
            {timeout, Acc}
    end.

%% @private in the case that some preflist entr(y|ies) did not respond
%% in time, add them to the mbox data result as `error' entries.
add_errors_to_mbox_data(Preflist, Acc) ->
    lists:map(fun(Entry) ->
                      case lists:keyfind(Entry, 1, Acc) of
                          false ->
                              lager:warning("Mailbox for ~p with did not return in time", [Entry]),
                              {Entry, error, error};
                          Res ->
                              Res
                      end
              end, Preflist).

%% @private @TODO test to find the best strategy
select_least_loaded_coordinator([]=_LocalMboxData, RemoteMBoxData) ->
    [{Entry, _, _} | _Rest] = lists:sort(fun mbox_data_sort/2, RemoteMBoxData),
    lager:info("loaded forward"),
    {loaded_forward, Entry};
select_least_loaded_coordinator(LocalMBoxData, _RemoteMBoxData) ->
    [{Entry, _, _} | _Rest] = lists:sort(fun mbox_data_sort/2, LocalMBoxData),
    lager:warning("soft-loaded local coordinator"),
    {local, Entry}.

%% @private used by select_least_loaded_coordinator/2 to sort mbox
%% data results
mbox_data_sort({_, error, error}, {_, error, error}) ->
    true;
mbox_data_sort({_, error, error}, {_, _Load, _Limit}) ->
    false;
mbox_data_sort({_, _LoadA, _LimitA}, {_, error, error}) ->
    true;
mbox_data_sort({_, LoadA, _LimitA}, {_, LoadB, _LimitB})  ->
    LoadA =< LoadB.

%% @private decide if the coordinator has to be a local, causality
%% advancing vnode, or just any/all at once
-spec get_coordinator_type(list()) -> any | local.
get_coordinator_type(Options) ->
    case get_option(asis, Options, false) of
        false ->
            local;
        true ->
            any
    end.

%% @private used by `coordinate_or_forward' above. Removes `Type' info
%% from preflist entries and splits a preflist into local and remote
partition_local_remote(Preflist) ->
    lists:foldl(fun partition_local_remote/2,
                {[], []},
                Preflist).

%% @private fold fun for `partition_local_remote/1'
partition_local_remote({{_Index, Node}=IN, _Type}, {L, R})
  when Node == node() ->
    {[IN | L], R};
partition_local_remote({IN, _Type}, {L, R}) ->
    {L, [IN | R]}.

%% @private the local node is not in the preflist, or is overloaded,
%% forward to another node
forward(CoordNode, State) ->
    #state{trace=Trace,
           options=Options,
           from=From,
           robj=RObj,
           bkey=BKey} = State,
    %% This node is not in the preference list, or it's too loaded,
    %% forward on to (a less loaded?) node

    ?DTRACE(Trace, ?C_PUT_FSM_PREPARE, [1],
            ["prepare", atom2list(CoordNode)]),
    try
        {UseAckP, Options2} = make_ack_options(
                                [{ack_execute, self()}|Options]),
        MiddleMan = spawn_coordinator_proc(
                      CoordNode, riak_kv_put_fsm, start_link,
                      [From, RObj, Options2]),
        ?DTRACE(Trace, ?C_PUT_FSM_PREPARE, [2],
                ["prepare", atom2list(CoordNode)]),
        ok = riak_kv_stat:update(coord_redir),
        monitor_remote_coordinator(UseAckP, MiddleMan,
                                   CoordNode, State)
    catch
        _:Reason ->
            ?DTRACE(Trace, ?C_PUT_FSM_PREPARE, [-2],
                    ["prepare", dtrace_errstr(Reason)]),
            lager:error("Unable to forward put for ~p to ~p - ~p @ ~p\n",
                        [BKey, CoordNode, Reason, erlang:get_stacktrace()]),
            process_reply({error, {coord_handoff_failed, Reason}}, State)
    end.

-spec get_timestamp_millis() -> pos_integer().
get_timestamp_millis() ->
  {Mega, Sec, Micro} = os:timestamp(),
  (Mega*1000000 + Sec)*1000 + round(Micro/1000).

-ifdef(TEST).

mbox_data_sort_test() ->
    ?assert(mbox_data_sort({x, error, error}, {x, error, error})),
    ?assert(mbox_data_sort({x, 1000, 10}, {x, error, error})),
    ?assertNot(mbox_data_sort({x, error, error}, {x, 100, 10})),
    ?assertNot(mbox_data_sort({x, 10, 10}, {x, 1, 10})),
    ?assert(mbox_data_sort({x, 10, 10}, {x, 10, 10})),
    ?assert(mbox_data_sort({x, 1, 10}, {x, 10, 10})).

select_least_loaded_coordinator_test() ->
    MBoxData = [{x, 100, 10},
                {y, 10, 10},
                {z, error, error},
                {a, 1, 10},
                {b, error, error},
                {c, 10001, 10}],
    ?assertEqual({local, a}, select_least_loaded_coordinator(MBoxData, [])),
    ?assertEqual({loaded_forward, a}, select_least_loaded_coordinator([], MBoxData)),
    ?assertEqual({local, a}, select_least_loaded_coordinator(MBoxData, MBoxData)).

get_coordinator_type_test() ->
    Opts0 = proplists:unfold([asis]),
    ?assertEqual(any, get_coordinator_type(Opts0)),
    Opts1 = proplists:unfold([]),
    ?assertEqual(local, get_coordinator_type(Opts1)).

get_bucket_props_test_() ->
    BucketProps = [{bprop1, bval1},
                   {bprop2, bval2},
                   {prop2, bval9},
                   {prop1, val1}],
    DefaultProps = [{prop1, val1},
                    {prop2, val2},
                    {prop3, val3}],
    {setup, fun() ->
                    DefPropsOrig = application:get_env(riak_core,
                                                       default_bucket_props),
                    application:set_env(riak_core,
                                        default_bucket_props,
                                        DefaultProps),

                    meck:new(riak_core_bucket),
                    meck:expect(riak_core_bucket, get_bucket,
                                fun(_) ->
                                        BucketProps
                                end),

                    DefPropsOrig
            end,
     fun(DefPropsOrig) ->
             application:set_env(riak_core,
                                 default_bucket_props,
                                 DefPropsOrig),
             meck:unload(riak_core_bucket)

     end,
     [{"untyped bucket",
       ?_test(
          begin
              Bucket = <<"bucket">>,
              %% amazing, not a ukeymerge
              Props = get_bucket_props(Bucket),
              %% i.e. a merge with defaults
              ?assertEqual([
                            {bprop1,bval1},
                            {bprop2,bval2},
                            {prop1,val1},
                            {prop1,val1},
                            {prop2,bval9},
                            {prop2,val2},
                            {prop3,val3}
                           ], Props)
          end)
      },
      {"typed bucket",
       ?_test(
          begin
              Bucket = {<<"type">>, <<"bucket">>},
              Props = get_bucket_props(Bucket),
              %% i.e. no merge with defaults
              ?assertEqual(BucketProps, Props)
          end
         )
      }
     ]}.

partition_local_remote_test() ->
    Node = node(),
    PL1 = [
           {{0, 'node0'}, primary},
           {{1, Node}, fallback},
           {{2, 'node1'}, primary}
          ],
    ?assertEqual({
                   [{1, Node}],
                   [{2, 'node1'},
                    {0, 'node0'}]
                 }, partition_local_remote(PL1)),

    PL2 = [
           {{0, 'node0'}, primary},
           {{1, Node}, fallback},
           {{2, Node}, primary},
           {{3, 'node0'}, fallback}
          ],

    ?assertEqual({
                   [{2, Node},
                    {1, Node}],
                   [{3, 'node0'},
                    {0, 'node0'}]
                 }, partition_local_remote(PL2)),
    ?assertEqual({[], []}, partition_local_remote([])),
    ?assertEqual({[], [{3, 'node0'},
                       {0, 'node0'}]}, partition_local_remote([
                                                               {{0, 'node0'}, primary},
                                                               {{3, 'node0'}, fallback}
                                                              ])),
    ?assertEqual({[{1, Node}], []}, partition_local_remote([
                                                            {{1, Node}, primary}
                                                           ])).

get_n_val_test() ->
    Opts0 = [],
    BProps = [{n_val, 3}, {other, props}],
    ?assertEqual(3, get_n_val(Opts0, BProps)),
    Opts1 = [{n_val, 1}],
    ?assertEqual(1, get_n_val(Opts1, BProps)),
    Opts2 = [{n_val, 4}],
    ?assertEqual({error, {n_val_violation, 4}}, get_n_val(Opts2, BProps)).

-endif.
