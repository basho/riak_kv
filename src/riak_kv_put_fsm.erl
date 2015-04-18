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
%-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
%-endif.
-include_lib("riak_kv_vnode.hrl").
-include_lib("riak_kv_js_pools.hrl").
-include("riak_kv_wm_raw.hrl").
-include("riak_kv_types.hrl").

-behaviour(gen_fsm).
-define(DEFAULT_OPTS, [{returnbody, false}, {update_last_modified, true}]).
-export([start/3, start/6,start/7]).
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

start(From, Object, PutOptions) ->
    gen_fsm:start(?MODULE, [From, Object, PutOptions], []).

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
    case whereis(riak_kv_put_fsm_sj) of
        undefined ->
            %% Overload protection disabled
            Args = [From, Object, PutOptions, true],
            gen_fsm:start_link(?MODULE, Args, []);
        _ ->
            Args = [From, Object, PutOptions, false],
            case sidejob_supervisor:start_child(riak_kv_put_fsm_sj,
                                                gen_fsm, start_link,
                                                [?MODULE, Args, []]) of
                {error, overload} ->
                    riak_kv_util:overload_reply(From),
                    {error, overload};
                {ok, Pid} ->
                    {ok, Pid}
            end
    end.

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
    gen_fsm:start_link(?MODULE, {test, [From, Object, PutOptions, true], StateProps}, []).

-endif.


%% ====================================================================
%% gen_fsm callbacks
%% ====================================================================

%% @private
init([From, RObj, Options0, Monitor]) ->
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
    (Monitor =:= true) andalso riak_kv_get_put_monitor:put_fsm_spawned(self()),
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
    {ok, prepare, StateData, 0};
init({test, Args, StateProps}) ->
    %% Call normal init
    {ok, prepare, StateData, 0} = init(Args),

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
    {ok, validate, TestStateData, 0}.

%% @private
prepare(timeout, StateData0 = #state{from = From, robj = RObj,
                                     bkey = BKey = {Bucket, _Key},
                                     options = Options,
                                     trace = Trace,
                                     bad_coordinators = BadCoordinators}) ->
    {ok, DefaultProps} = application:get_env(riak_core, 
                                             default_bucket_props),
    BucketProps = riak_core_bucket:get_bucket(riak_object:bucket(RObj)),
    %% typed buckets never fall back to defaults
    Props = 
        case is_tuple(Bucket) of
            false -> 
                lists:keymerge(1, lists:keysort(1, BucketProps), 
                               lists:keysort(1, DefaultProps));
            true ->
                BucketProps
        end,
    DocIdx = riak_core_util:chash_key(BKey, BucketProps),
    Bucket_N = get_option(n_val, BucketProps),
    N = case get_option(n_val, Options, false) of
            false ->
                Bucket_N;
            N_val when is_integer(N_val), N_val > 0, N_val =< Bucket_N ->
                %% don't allow custom N to exceed bucket N
                N_val;
            Bad_N ->
                {error, {n_val_violation, Bad_N}}
        end,
    case N of
        {error, _} = Error ->
            process_reply(Error, StateData0);
        _ ->
            StatTracked = get_option(stat_tracked, BucketProps, false),
            Preflist2 = 
                case get_option(sloppy_quorum, Options, true) of
                    true ->
                        UpNodes = riak_core_node_watcher:nodes(riak_kv),
                        riak_core_apl:get_apl_ann(DocIdx, N, 
                                                  UpNodes -- BadCoordinators);
                    false ->
                        Preflist1 =
                            riak_core_apl:get_primary_apl(DocIdx, N, riak_kv),
                        [X || X = {{_Index, Node}, _Type} <- Preflist1,
                              not lists:member(Node, BadCoordinators)]
                end,
            %% Check if this node is in the preference list so it can coordinate
            LocalPL = [IndexNode || {{_Index, Node} = IndexNode, _Type} <- Preflist2,
                                Node == node()],
            Must = (get_option(asis, Options, false) /= true),
            case {Preflist2, LocalPL =:= [] andalso Must == true} of
                {[], _} ->
                    %% Empty preflist
                    ?DTRACE(Trace, ?C_PUT_FSM_PREPARE, [-1], 
                            ["prepare",<<"all nodes down">>]),
                    process_reply({error, all_nodes_down}, StateData0);
                {_, true} ->
                    %% This node is not in the preference list
                    %% forward on to a random node
                    {ListPos, _} = random:uniform_s(length(Preflist2), os:timestamp()),
                    {{_Idx, CoordNode},_Type} = lists:nth(ListPos, Preflist2),
                    ?DTRACE(Trace, ?C_PUT_FSM_PREPARE, [1],
                            ["prepare", atom2list(CoordNode)]),
                    try
                        {UseAckP, Options2} = make_ack_options(
                                               [{ack_execute, self()}|Options]),
                        MiddleMan = spawn_coordinator_proc(
                                      CoordNode, riak_kv_put_fsm, start_link,
                                      [From,RObj,Options2]),
                        ?DTRACE(Trace, ?C_PUT_FSM_PREPARE, [2],
                                ["prepare", atom2list(CoordNode)]),
                        ok = riak_kv_stat:update(coord_redir),
                        monitor_remote_coordinator(UseAckP, MiddleMan,
                                                   CoordNode, StateData0)
                    catch
                        _:Reason ->
                            ?DTRACE(Trace, ?C_PUT_FSM_PREPARE, [-2],
                                    ["prepare", dtrace_errstr(Reason)]),
                            lager:error("Unable to forward put for ~p to ~p - ~p @ ~p\n",
                                        [BKey, CoordNode, Reason, erlang:get_stacktrace()]),
                            process_reply({error, {coord_handoff_failed, Reason}}, StateData0)
                    end;
                _ ->
                    %% Putting asis, no need to handle locally on a node in the
                    %% preflist, can coordinate from anywhere
                    CoordPLEntry = case Must of
                                    true ->
                                        hd(LocalPL);
                                    _ ->
                                        undefined
                                end,
                    CoordPlNode = case CoordPLEntry of
                                    undefined  -> undefined;
                                    {_Idx, Nd} -> atom2list(Nd)
                                end,
                    %% This node is in the preference list, continue
                    StartTime = riak_core_util:moment(),
                    StateData = StateData0#state{n = N,
                                                bucket_props = Props,
                                                coord_pl_entry = CoordPLEntry,
                                                preflist2 = Preflist2,
                                                starttime = StartTime,
                                                tracked_bucket = StatTracked},
                    ?DTRACE(Trace, ?C_PUT_FSM_PREPARE, [0], 
                            ["prepare", CoordPlNode]),
                    new_state_timeout(validate, StateData)
            end
    end.

%% @private
validate(timeout, StateData0 = #state{from = {raw, ReqId, _Pid},
                                      options = Options0,
                                      robj = RObj0,
                                      n=N, bucket_props = BucketProps,
                                      trace = Trace,
                                      preflist2 = Preflist2}) ->
    Timeout = get_option(timeout, Options0, ?DEFAULT_TIMEOUT),
    PW0 = get_option(pw, Options0, default),
    W0 = get_option(w, Options0, default),
    DW0 = get_option(dw, Options0, default),

    PW = riak_kv_util:expand_rw_value(pw, PW0, BucketProps, N),
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

    IdxType = [{Part, Type} || {{Part, _Node}, Type} <- Preflist2],
    NumPrimaries = length([x || {_,primary} <- Preflist2]),
    NumVnodes = length(Preflist2),
    MinVnodes = lists:max([1, W, DW, PW]), % always need at least one vnode

    if
        PW =:= error ->
            process_reply({error, {pw_val_violation, PW0}}, StateData0);
        W =:= error ->
            process_reply({error, {w_val_violation, W0}}, StateData0);
        DW =:= error ->
            process_reply({error, {dw_val_violation, DW0}}, StateData0);
        (W > N) or (DW > N) or (PW > N) ->
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
            PutCore = riak_kv_put_core:init(N, W, PW, DW,
                                            N-PW+1,  % cannot ever get PW replies
                                            N-DW+1,  % cannot ever get DW replies
                                            AllowMult,
                                            ReturnBody,
                                            IdxType),
            VNodeOpts = handle_options(Options, VNodeOpts0),
            StateData = StateData0#state{n=N,
                                         w=W,
                                         pw=PW, dw=DW, allowmult=AllowMult,
                                         precommit = Precommit,
                                         postcommit = Postcommit,
                                         req_id = ReqId,
                                         robj = apply_updates(RObj0, Options),
                                         putcore = PutCore,
                                         vnode_options = VNodeOpts,
                                         timeout = Timeout},
            ?DTRACE(Trace, ?C_PUT_FSM_VALIDATE, [N, W, PW, DW], []),
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
execute(State=#state{options = Options, coord_pl_entry = CPL}) ->
    %% If we are a forwarded coordinator, the originating node is expecting
    %% an ack from us.
    case get_option(ack_execute, Options) of
        undefined ->
            ok;
        Pid ->
            Pid ! {ack, node(), now_executing}
    end,
    case CPL of
        undefined ->
            execute_remote(State);
        _ ->
            execute_local(State)
    end.

%% @private
%% Send the put coordinating put requests to the local vnode - the returned object
%% will guarantee a frontier object.
%% N.B. Not actually a state - here in the source to make reading the flow easier
execute_local(StateData=#state{robj=RObj, req_id = ReqId,
                               timeout=Timeout, bkey=BKey,
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
    TRef = schedule_timeout(Timeout),
    riak_kv_vnode:coord_put(CoordPLEntry, BKey, RObj, ReqId, StartTime, VnodeOptions),
    StateData2 = StateData1#state{robj = RObj, tref = TRef},
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
    {next_state, StateName, add_timing(StateName, StateData), 0};
new_state_timeout(StateName, StateData) ->
    {next_state, StateName, StateData, 0}.

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
