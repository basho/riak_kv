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

-behaviour(gen_fsm).
-define(DEFAULT_OPTS, [{returnbody, false}, {update_last_modified, true}]).
-export([start/3, start/6,start/7]).
-export([start_link/3,start_link/6,start_link/7]).
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
        {n_val, pos_integer()}.

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
                preflist2 :: riak_core_apl:preflist2(),
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
                tracked_bucket=false :: boolean() %% tracke per bucket stats
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
init([From, RObj, Options, Monitor]) ->
    BKey = {Bucket, Key} = {riak_object:bucket(RObj), riak_object:key(RObj)},
    StateData = add_timing(prepare, #state{from = From,
                                           robj = RObj,
                                           bkey = BKey,
                                           options = Options}),
    (Monitor =:= true) andalso riak_kv_get_put_monitor:put_fsm_spawned(self()),
    riak_core_dtrace:put_tag(io_lib:format("~p,~p", [Bucket, Key])),
    case riak_kv_util:is_x_deleted(RObj) of
        true  ->
            TombNum = 1,
            TombStr = <<"tombstone">>;
        false ->
            TombNum = 0,
            TombStr = <<>>
    end,
    ?DTRACE(?C_PUT_FSM_INIT, [TombNum], ["init", TombStr]),
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
prepare(timeout, StateData0 = #state{from = From, robj = RObj,
                                     bkey = BKey,
                                     options = Options}) ->
    BucketProps = riak_core_bucket:get_bucket(riak_object:bucket(RObj)),
    DocIdx = riak_core_util:chash_key(BKey),
    Bucket_N = proplists:get_value(n_val,BucketProps),
    N = case proplists:get_value(n_val, Options) of
            undefined ->
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
            StatTracked = proplists:get_value(stat_tracked, BucketProps, false),
            Preflist2 = case proplists:get_value(sloppy_quorum, Options, true) of
                            true ->
                                UpNodes = riak_core_node_watcher:nodes(riak_kv),
                                riak_core_apl:get_apl_ann(DocIdx, N, UpNodes);
                            false ->
                                riak_core_apl:get_primary_apl(DocIdx, N, riak_kv)
                        end,
            %% Check if this node is in the preference list so it can coordinate
            LocalPL = [IndexNode || {{_Index, Node} = IndexNode, _Type} <- Preflist2,
                                Node == node()],
            Must = (get_option(asis, Options, false) /= true),
            case {Preflist2, LocalPL =:= [] andalso Must == true} of
                {[], _} ->
                    %% Empty preflist
                    ?DTRACE(?C_PUT_FSM_PREPARE, [-1], ["prepare",<<"all nodes down">>]),
                    process_reply({error, all_nodes_down}, StateData0);
                {_, true} ->
                    %% This node is not in the preference list
                    %% forward on to a random node
                    ListPos = crypto:rand_uniform(1, length(Preflist2)+1),
                    {{_Idx, CoordNode},_Type} = lists:nth(ListPos, Preflist2),
                    _Timeout = get_option(timeout, Options, ?DEFAULT_TIMEOUT),
                    ?DTRACE(?C_PUT_FSM_PREPARE, [1],
                            ["prepare", atom2list(CoordNode)]),
                    try
                        proc_lib:spawn(CoordNode,riak_kv_put_fsm,start_link,[From,RObj,Options]),
                        ?DTRACE(?C_PUT_FSM_PREPARE, [2],
                                    ["prepare", atom2list(CoordNode)]),
                        riak_kv_stat:update(coord_redir),
                        {stop, normal, StateData0}
                    catch
                        _:Reason ->
                            ?DTRACE(?C_PUT_FSM_PREPARE, [-2],
                                    ["prepare", dtrace_errstr(Reason)]),
                            lager:error("Unable to forward put for ~p to ~p - ~p\n",
                                        [BKey, CoordNode, Reason]),
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
                                                bucket_props = BucketProps,
                                                coord_pl_entry = CoordPLEntry,
                                                preflist2 = Preflist2,
                                                starttime = StartTime,
                                                tracked_bucket = StatTracked},
                    ?DTRACE(?C_PUT_FSM_PREPARE, [0], ["prepare", CoordPlNode]),
                    new_state_timeout(validate, StateData)
            end
    end.

%% @private
validate(timeout, StateData0 = #state{from = {raw, ReqId, _Pid},
                                      options = Options0,
                                      n=N, bucket_props = BucketProps,
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
            AllowMult = proplists:get_value(allow_mult,BucketProps),
            Disable = proplists:get_bool(disable_hooks, Options0),
            Precommit =
                if Disable -> [];
                   true ->
                        L = get_hooks(precommit, BucketProps),
                        L ++ [?PARSE_INDEX_PRECOMMIT]
                end,
            Postcommit =
                if Disable -> [];
                   true -> get_hooks(postcommit, BucketProps)
                end,
            StateData1 = StateData0#state{n=N,
                                          w=W,
                                          pw=PW, dw=DW, allowmult=AllowMult,
                                          precommit = Precommit,
                                          postcommit = Postcommit,
                                          req_id = ReqId,
                                          timeout = Timeout},
            Options = flatten_options(proplists:unfold(Options0 ++ ?DEFAULT_OPTS), []),
            StateData2 = handle_options(Options, StateData1),
            StateData3 = apply_updates(StateData2),
            StateData = init_putcore(StateData3, IdxType),
            ?DTRACE(?C_PUT_FSM_VALIDATE, [N, W, PW, DW], []),
            case Precommit of
                [] -> % Nothing to run, spare the timing code
                    execute(StateData);
                _ ->
                    new_state_timeout(precommit, StateData)
            end
    end.

%% Run the precommit hooks
precommit(timeout, State = #state{precommit = []}) ->
    execute(State);
precommit(timeout, State = #state{precommit = [Hook | Rest], robj = RObj}) ->
    Result = decode_precommit(invoke_hook(Hook, RObj)),
    case Result of
        fail ->
            ?DTRACE(?C_PUT_FSM_PRECOMMIT, [-1], []),
            process_reply({error, precommit_fail}, State);
        {fail, Reason} ->
            ?DTRACE(?C_PUT_FSM_PRECOMMIT, [-1], [dtrace_errstr(Reason)]),
            process_reply({error, {precommit_fail, Reason}}, State);
        Result ->
            ?DTRACE(?C_PUT_FSM_PRECOMMIT, [0], []),
            {next_state, precommit, State#state{robj = riak_object:apply_updates(Result),
                                                precommit = Rest}, 0}
    end.

%% @private
execute(State=#state{coord_pl_entry = CPL}) ->
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
                                starttime = StartTime}) ->
    ?DTRACE(?C_PUT_FSM_EXECUTE_LOCAL, [], [atom2list(Node)]),
    StateData1 = add_timing(execute_local, StateData),
    TRef = schedule_timeout(Timeout),
    riak_kv_vnode:coord_put(CoordPLEntry, BKey, RObj, ReqId, StartTime, VnodeOptions),
    StateData2 = StateData1#state{robj = RObj, tref = TRef},
    %% Must always wait for local vnode - it contains the object with updated vclock
    %% to use for the remotes. (Ignore optimization for N=1 case for now).
    new_state(waiting_local_vnode, StateData2).



%% @private
waiting_local_vnode(request_timeout, StateData) ->
    ?DTRACE(?C_PUT_FSM_WAITING_LOCAL_VNODE, [-1], []),
    process_reply({error,timeout}, StateData);
waiting_local_vnode(Result, StateData = #state{putcore = PutCore}) ->
    UpdPutCore1 = riak_kv_put_core:add_result(Result, PutCore),
    case Result of
        {fail, Idx, ReqIdSubvertedToReason} ->
            ?DTRACE(?C_PUT_FSM_WAITING_LOCAL_VNODE, [-1],
                    [integer_to_list(Idx)]),
            %% Local vnode failure is enough to sink whole operation
            process_reply({error, ReqIdSubvertedToReason}, StateData#state{putcore = UpdPutCore1});
        {w, Idx, _ReqId} ->
            ?DTRACE(?C_PUT_FSM_WAITING_LOCAL_VNODE, [1],
                    [integer_to_list(Idx)]),
            {next_state, waiting_local_vnode, StateData#state{putcore = UpdPutCore1}};
        {dw, Idx, PutObj, _ReqId} ->
            %% Either returnbody is true or coord put merged with the existing
            %% object and bumped the vclock.  Either way use the returned
            %% object for the remote vnode
            ?DTRACE(?C_PUT_FSM_WAITING_LOCAL_VNODE, [2],
                    [integer_to_list(Idx)]),
            execute_remote(StateData#state{robj = PutObj, putcore = UpdPutCore1});
        {dw, Idx, _ReqId} ->
            %% Write succeeded without changes to vclock required and returnbody false
            ?DTRACE(?C_PUT_FSM_WAITING_LOCAL_VNODE, [2],
                    [integer_to_list(Idx)]),
            execute_remote(StateData#state{putcore = UpdPutCore1})
    end.

%% @private
%% Send the put requests to any remote nodes if necessary and decided if
%% enough responses have been received yet (i.e. if W/DW=1)
%% N.B. Not actually a state - here in the source to make reading the flow easier
execute_remote(StateData=#state{robj=RObj, req_id = ReqId,
                                preflist2 = Preflist2, bkey=BKey,
                                coord_pl_entry = CoordPLEntry,
                                vnode_options=VnodeOptions,
                                putcore=PutCore,
                                starttime = StartTime}) ->
    StateData1 = add_timing(execute_remote, StateData),
    Preflist = [IndexNode || {IndexNode, _Type} <- Preflist2,
                             IndexNode /= CoordPLEntry],
    Ps = [[atom2list(Nd), $,, integer_to_list(Idx)] ||
             {Idx, Nd} <- lists:sublist(Preflist, 4)],
    ?DTRACE(?C_PUT_FSM_EXECUTE_REMOTE, [], [Ps]),
    riak_kv_vnode:put(Preflist, BKey, RObj, ReqId, StartTime, VnodeOptions),
    case riak_kv_put_core:enough(PutCore) of
        true ->
            {Reply, UpdPutCore} = riak_kv_put_core:response(PutCore),
            process_reply(Reply, StateData#state{putcore = UpdPutCore});
        false ->
            new_state(waiting_remote_vnode, StateData1)
    end.


%% @private
waiting_remote_vnode(request_timeout, StateData) ->
    ?DTRACE(?C_PUT_FSM_WAITING_REMOTE_VNODE, [-1], []),
    process_reply({error,timeout}, StateData);
waiting_remote_vnode(Result, StateData = #state{putcore = PutCore}) ->
    ShortCode = riak_kv_put_core:result_shortcode(Result),
    IdxStr = integer_to_list(riak_kv_put_core:result_idx(Result)),
    ?DTRACE(?C_PUT_FSM_WAITING_REMOTE_VNODE, [ShortCode], [IdxStr]),
    UpdPutCore1 = riak_kv_put_core:add_result(Result, PutCore),
    case riak_kv_put_core:enough(UpdPutCore1) of
        true ->
            {Reply, UpdPutCore2} = riak_kv_put_core:response(UpdPutCore1),
            process_reply(Reply, StateData#state{putcore = UpdPutCore2});
        false ->
            {next_state, waiting_remote_vnode, StateData#state{putcore = UpdPutCore1}}
    end.

%% @private
postcommit(timeout, StateData = #state{postcommit = []}) ->
    ?DTRACE(?C_PUT_FSM_POSTCOMMIT, [0], []),
    new_state_timeout(finish, StateData);
postcommit(timeout, StateData = #state{postcommit = [Hook | Rest],
                                       putcore = PutCore}) ->
    ?DTRACE(?C_PUT_FSM_POSTCOMMIT, [-2], []),
    %% Process the next hook - gives sys:get_status messages a chance if hooks
    %% take a long time.
    {ReplyObj, UpdPutCore} =  riak_kv_put_core:final(PutCore),
    decode_postcommit(invoke_hook(Hook, ReplyObj)),
    {next_state, postcommit, StateData#state{postcommit = Rest,
                                             putcore = UpdPutCore}, 0};
postcommit(request_timeout, StateData) -> % still process hooks even if request timed out
    ?DTRACE(?C_PUT_FSM_POSTCOMMIT, [-3], []),
    {next_state, postcommit, StateData, 0};
postcommit(Reply, StateData = #state{putcore = PutCore}) ->
    ShortCode = riak_kv_put_core:result_shortcode(Reply),
    IdxStr = integer_to_list(riak_kv_put_core:result_idx(Reply)),
    ?DTRACE(?C_PUT_FSM_POSTCOMMIT, [0, ShortCode], [IdxStr]),
    %% late responses - add to state.  *Does not* recompute finalobj
    UpdPutCore = riak_kv_put_core:add_result(Reply, PutCore),
    {next_state, postcommit, StateData#state{putcore = UpdPutCore}, 0}.

finish(timeout, StateData = #state{timing = Timing, reply = Reply,
                                   bkey = {Bucket, _Key},
                                   tracked_bucket = StatTracked}) ->
    case Reply of
        {error, _} ->
            ?DTRACE(?C_PUT_FSM_FINISH, [-1], []),
            ok;
        _Ok ->
            %% TODO: Improve reporting of timing
            %% For now can add debug tracers to view the return from calc_timing
            {Duration, Stages} = riak_kv_fsm_timing:calc_timing(Timing),
            riak_kv_stat:update({put_fsm_time, Bucket, Duration, Stages, StatTracked}),
            ?DTRACE(?C_PUT_FSM_FINISH, [0, Duration], [])
    end,
    {stop, normal, StateData};
finish(Reply, StateData = #state{putcore = PutCore}) ->
    ShortCode = riak_kv_put_core:result_shortcode(Reply),
    IdxStr = integer_to_list(riak_kv_put_core:result_idx(Reply)),
    ?DTRACE(?C_PUT_FSM_FINISH, [1, ShortCode], [IdxStr]),
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
process_reply(Reply, StateData = #state{postcommit = PostCommit,
                                        putcore = PutCore,
                                        robj = RObj,
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
            ?DTRACE(?C_PUT_FSM_PROCESS_REPLY, [0], []),
            new_state_timeout(postcommit, StateData2);
        {ok, _} ->
            Values = riak_object:get_values(RObj),
            %% TODO: more accurate sizing method
            ApproxBytes = size(Bucket) + size(Key) +
                lists:sum([size(V) || V <- Values]),
            NumSibs = length(Values),
            ?DTRACE(?C_PUT_FSM_PROCESS_REPLY, [1, ApproxBytes, NumSibs], []),
            new_state_timeout(postcommit, StateData2);
        _ ->
            ?DTRACE(?C_PUT_FSM_PROCESS_REPLY, [-1], []),
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
handle_options([{crdt_op, _Op}=COP|T], State) ->
    VNodeOpts = [COP | State#state.vnode_options],
    handle_options(T, State#state{vnode_options=VNodeOpts});
handle_options([{K, _V} = Opt|T], State = #state{vnode_options = VnodeOpts})
  when K == sloppy_quorum; K == n_val ->
    %% Take these options as-is
    handle_options(T, State#state{vnode_options = [Opt|VnodeOpts]});
handle_options([{_,_}|T], State) -> handle_options(T, State).

init_putcore(State = #state{n = N, w = W, pw = PW, dw = DW, allowmult = AllowMult,
                            returnbody = ReturnBody}, IdxType) ->
    PutCore = riak_kv_put_core:init(N, W, PW, DW,
                                    N-PW+1,  % cannot ever get PW replies
                                    N-DW+1,  % cannot ever get DW replies
                                    AllowMult,
                                    ReturnBody,
                                    IdxType),
    State#state{putcore = PutCore}.


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
    %% Post-0.14.2 changed vtags to be generated from node/now rather the vclocks.
    %% The vclock has not been updated at this point.  Vtags/etags should really
    %% be an external interface concern and are only used for sibling selection
    %% and if-modified type tests so they could be generated on retrieval instead.
    %% This changes from being a hash on the value to a likely-to-be-unique value
    %% which should serve the same purpose.  It was possible to generate two
    %% objects with the same vclock on 0.14.2 if the same clientid was used in
    %% the same second.  It can be revisited post-1.0.0.
    Now = os:timestamp(),
    NewMD = dict:store(?MD_VTAG, riak_kv_util:make_vtag(Now),
                       dict:store(?MD_LASTMOD, Now, MD0)),
    riak_object:update_metadata(RObj, NewMD).

%% Invokes the hook and returns a tuple of
%% {Lang, Called, Result}
%% Where Called = {Mod, Fun} if Lang = erlang
%%       Called = JSName if Lang = javascript
invoke_hook({struct, Hook}=HookDef, RObj) ->
    Mod = proplists:get_value(<<"mod">>, Hook),
    Fun = proplists:get_value(<<"fun">>, Hook),
    JSName = proplists:get_value(<<"name">>, Hook),
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

-spec decode_precommit(any()) -> fail | {fail, any()} | riak_object:riak_object().
decode_precommit({erlang, {Mod, Fun}, Result}) ->
    %% TODO: For DTrace things, we will err on the side of taking the
    %%       time to format the error results into strings to pass to
    %%       the probes.  If this ends up being too slow, then revisit.
    case Result of
        fail ->
            ?DTRACE(?C_PUT_FSM_DECODE_PRECOMMIT, [-1], []),
            riak_kv_stat:update(precommit_fail),
            lager:debug("Pre-commit hook ~p:~p failed, no reason given",
                        [Mod, Fun]),
            fail;
        {fail, Reason} ->
            ?DTRACE(?C_PUT_FSM_DECODE_PRECOMMIT, [-2], [dtrace_errstr(Reason)]),
            riak_kv_stat:update(precommit_fail),
            lager:debug("Pre-commit hook ~p:~p failed with reason ~p",
                        [Mod, Fun, Reason]),
            Result;
        {'EXIT',  Mod, Fun, Class, Exception} ->
            ?DTRACE(?C_PUT_FSM_DECODE_PRECOMMIT, [-3],
                    [dtrace_errstr({Mod, Fun, Class, Exception})]),
            riak_kv_stat:update(precommit_fail),
            lager:debug("Problem invoking pre-commit hook ~p:~p -> ~p:~p~n~p",
                        [Mod,Fun,Class,Exception, erlang:get_stacktrace()]),
            {fail, {hook_crashed, {Mod, Fun, Class, Exception}}};
        Obj ->
            try
                riak_object:ensure_robject(Obj)
            catch X:Y ->
                    ?DTRACE(?C_PUT_FSM_DECODE_PRECOMMIT, [-4],
                            [dtrace_errstr({Mod, Fun, X, Y})]),
                    riak_kv_stat:update(precommit_fail),
                    lager:debug("Problem invoking pre-commit hook ~p:~p,"
                                " invalid return ~p",
                                [Mod, Fun, Result]),
                    {fail, {invalid_return, {Mod, Fun, Result}}}

            end
    end;
decode_precommit({js, JSName, Result}) ->
    case Result of
        {ok, <<"fail">>} ->
            ?DTRACE(?C_PUT_FSM_DECODE_PRECOMMIT, [-5], []),
            riak_kv_stat:update(precommit_fail),
            lager:debug("Pre-commit hook ~p failed, no reason given",
                        [JSName]),
            fail;
        {ok, [{<<"fail">>, Message}]} ->
            ?DTRACE(?C_PUT_FSM_DECODE_PRECOMMIT, [-6],[dtrace_errstr(Message)]),
            riak_kv_stat:update(precommit_fail),
            lager:debug("Pre-commit hook ~p failed with reason ~p",
                        [JSName, Message]),
            {fail, Message};
        {ok, Json} ->
            case catch riak_object:from_json(Json) of
                {'EXIT', _} ->
                    ?DTRACE(?C_PUT_FSM_DECODE_PRECOMMIT, [-7], []),
                    {fail, {invalid_return, {JSName, Json}}};
                Obj ->
                    Obj
            end;
        {error, Error} ->
            riak_kv_stat:update(precommit_fail),
            ?DTRACE(?C_PUT_FSM_DECODE_PRECOMMIT, [-7], [dtrace_errstr(Error)]),
            lager:debug("Problem invoking pre-commit hook: ~p", [Error]),
            fail
    end;
decode_precommit({error, Reason}) ->
    ?DTRACE(?C_PUT_FSM_DECODE_PRECOMMIT, [-8], [dtrace_errstr(Reason)]),
    riak_kv_stat:update(precommit_fail),
    lager:debug("Problem invoking pre-commit hook: ~p", [Reason]),
    {fail, Reason}.

decode_postcommit({erlang, {M,F}, Res}) ->
    case Res of
        fail ->
            ?DTRACE(?C_PUT_FSM_DECODE_POSTCOMMIT, [-1], []),
            riak_kv_stat:update(postcommit_fail),
            lager:debug("Post-commit hook ~p:~p failed, no reason given",
                       [M, F]);
        {fail, Reason} ->
            ?DTRACE(?C_PUT_FSM_DECODE_POSTCOMMIT, [-2],[dtrace_errstr(Reason)]),
            riak_kv_stat:update(postcommit_fail),
            lager:debug("Post-commit hook ~p:~p failed with reason ~p",
                        [M, F, Reason]);
        {'EXIT', _, _, Class, Ex} ->
            ?DTRACE(?C_PUT_FSM_DECODE_POSTCOMMIT, [-3],
                    [dtrace_errstr({M, F, Class, Ex})]),
            riak_kv_stat:update(postcommit_fail),
            Stack = erlang:get_stacktrace(),
            lager:debug("Problem invoking post-commit hook ~p:~p -> ~p:~p~n~p",
                        [M, F, Class, Ex, Stack]),
            ok;
        _ ->
            ok
    end;
decode_postcommit({error, {invalid_hook_def, Def}}) ->
    ?DTRACE(?C_PUT_FSM_DECODE_POSTCOMMIT, [-4], [dtrace_errstr(Def)]),
    riak_kv_stat:update(postcommit_fail),
    lager:debug("Invalid post-commit hook definition ~p", [Def]).


get_hooks(HookType, BucketProps) ->
    Hooks = proplists:get_value(HookType, BucketProps, []),
    case Hooks of
        <<"none">> ->
            [];
        Hooks when is_list(Hooks) ->
            Hooks
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
