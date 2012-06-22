%% -------------------------------------------------------------------
%%
%% riak_kv_vnode: VNode Implementation
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
-module(riak_kv_vnode).
-author('Kevin Smith <kevin@basho.com>').
-author('John Muellerleile <johnm@basho.com>').

-behaviour(riak_core_vnode).

%% API
-export([test_vnode/1, put/7]).
-export([start_vnode/1,
         get/3,
         mget/3,
         del/3,
         put/6,
         coord_put/6,
         readrepair/6,
         list_keys/4,
         fold/3,
         get_vclocks/2,
         vnode_status/1,
         ack_keys/1,
         repair/1,
         repair_status/1,
         repair_filter/1]).

%% riak_core_vnode API
-export([init/1,
         terminate/2,
         handle_command/3,
         handle_coverage/4,
         is_empty/1,
         delete/1,
         handle_handoff_command/3,
         handoff_starting/2,
         handoff_cancelled/1,
         handoff_finished/2,
         handle_handoff_data/2,
         encode_handoff_item/2,
         handle_exit/3,
         handle_info/2]).

-include_lib("riak_kv_vnode.hrl").
-include_lib("riak_kv_map_phase.hrl").
-include_lib("riak_core/include/riak_core_pb.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-export([put_merge/6]). %% For fsm_eqc_vnode
-endif.

-record(mrjob, {cachekey :: term(),
                bkey :: term(),
                reqid :: term(),
                target :: pid()}).

-record(state, {idx :: partition(),
                mod :: module(),
                modstate :: term(),
                mrjobs :: term(),
                vnodeid :: undefined | binary(),
                delete_mode :: keep | immediate | pos_integer(),
                bucket_buf_size :: pos_integer(),
                index_buf_size :: pos_integer(),
                key_buf_size :: pos_integer(),
                async_folding :: boolean(),
                in_handoff = false :: boolean() }).

-type index_op() :: add | remove.
-type index_value() :: integer() | binary().

-record(putargs, {returnbody :: boolean(),
                  coord:: boolean(),
                  lww :: boolean(),
                  bkey :: {binary(), binary()},
                  robj :: term(),
                  index_specs=[] :: [{index_op(), binary(), index_value()}],
                  reqid :: non_neg_integer(),
                  bprops :: maybe_improper_list(),
                  starttime :: non_neg_integer(),
                  prunetime :: undefined| non_neg_integer(),
                  is_index=false :: boolean() %% set if the b/end supports indexes
                 }).

%% API
start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, riak_kv_vnode).

test_vnode(I) ->
    riak_core_vnode:start_link(riak_kv_vnode, I, infinity).

get(Preflist, BKey, ReqId) ->
    Req = ?KV_GET_REQ{bkey=BKey,
                      req_id=ReqId},
    %% Assuming this function is called from a FSM process
    %% so self() == FSM pid
    riak_core_vnode_master:command(Preflist,
                                   Req,
                                   {fsm, undefined, self()},
                                   riak_kv_vnode_master).

mget(Preflist, BKeys, ReqId) ->
    Req = ?KV_MGET_REQ{bkeys=BKeys,
                       req_id=ReqId,
                       from={fsm, self()}},
    riak_core_vnode_master:command(Preflist,
                                   Req,
                                   riak_kv_vnode_master).

del(Preflist, BKey, ReqId) ->
    riak_core_vnode_master:command(Preflist,
                                   ?KV_DELETE_REQ{bkey=BKey,
                                                  req_id=ReqId},
                                   riak_kv_vnode_master).

%% Issue a put for the object to the preflist, expecting a reply
%% to an FSM.
put(Preflist, BKey, Obj, ReqId, StartTime, Options) when is_integer(StartTime) ->
    put(Preflist, BKey, Obj, ReqId, StartTime, Options, {fsm, undefined, self()}).

put(Preflist, BKey, Obj, ReqId, StartTime, Options, Sender)
  when is_integer(StartTime) ->
    riak_core_vnode_master:command(Preflist,
                                   ?KV_PUT_REQ{
                                      bkey = BKey,
                                      object = Obj,
                                      req_id = ReqId,
                                      start_time = StartTime,
                                      options = Options},
                                   Sender,
                                   riak_kv_vnode_master).

%% Issue a put for the object to the preflist, expecting a reply
%% to an FSM.
coord_put(IndexNode, BKey, Obj, ReqId, StartTime, Options) when is_integer(StartTime) ->
    coord_put(IndexNode, BKey, Obj, ReqId, StartTime, Options, {fsm, undefined, self()}).

coord_put(IndexNode, BKey, Obj, ReqId, StartTime, Options, Sender)
  when is_integer(StartTime) ->
    riak_core_vnode_master:command(IndexNode,
                                   ?KV_PUT_REQ{
                                      bkey = BKey,
                                      object = Obj,
                                      req_id = ReqId,
                                      start_time = StartTime,
                                      options = [coord | Options]},
                                   Sender,
                                   riak_kv_vnode_master).

%% Do a put without sending any replies
readrepair(Preflist, BKey, Obj, ReqId, StartTime, Options) ->
    put(Preflist, BKey, Obj, ReqId, StartTime, [rr | Options], ignore).

list_keys(Preflist, ReqId, Caller, Bucket) ->
    riak_core_vnode_master:command(Preflist,
                                   #riak_kv_listkeys_req_v2{
                                     bucket=Bucket,
                                     req_id=ReqId,
                                     caller=Caller},
                                   ignore,
                                   riak_kv_vnode_master).

fold(Preflist, Fun, Acc0) ->
    riak_core_vnode_master:sync_spawn_command(Preflist,
                                              ?FOLD_REQ{
                                                 foldfun=Fun,
                                                 acc0=Acc0},
                                              riak_kv_vnode_master).

get_vclocks(Preflist, BKeyList) ->
    riak_core_vnode_master:sync_spawn_command(Preflist,
                                              ?KV_VCLOCK_REQ{bkeys=BKeyList},
                                              riak_kv_vnode_master).

%% @doc Get status information about the node local vnodes.
-spec vnode_status([{partition(), pid()}]) -> [{atom(), term()}].
vnode_status(PrefLists) ->
    ReqId = erlang:phash2(erlang:now()),
    %% Get the status of each vnode
    riak_core_vnode_master:command(PrefLists,
                                   ?KV_VNODE_STATUS_REQ{},
                                   {raw, ReqId, self()},
                                   riak_kv_vnode_master),
    wait_for_vnode_status_results(PrefLists, ReqId, []).

%% @doc Repair the given `Partition'.
-spec repair(partition()) ->
                    {ok, Pairs::[{partition(), node()}]} |
                    {down, Down::[{partition(), node()}]}.
repair(Partition) ->
    Service = riak_kv,
    MP = {riak_kv_vnode, Partition},
    FilterModFun = {?MODULE, repair_filter},
    riak_core_vnode_manager:repair(Service, MP, FilterModFun).

%% @doc Get the status of the repair process for the given `Partition'.
-spec repair_status(partition()) -> no_repair | repair_in_progress.
repair_status(Partition) ->
    riak_core_vnode_manager:repair_status({riak_kv_vnode, Partition}).

%% @doc Given a `Target' partition generate a `Filter' fun to use
%%      during partition repair.
-spec repair_filter(partition()) -> Filter::function().
repair_filter(Target) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    riak_core_repair:gen_filter(Target,
                                Ring,
                                bucket_nval_map(Ring),
                                default_object_nval(),
                                fun object_info/1).


%% VNode callbacks

init([Index]) ->
    Mod = app_helper:get_env(riak_kv, storage_backend),
    Configuration = app_helper:get_env(riak_kv),
    BucketBufSize = app_helper:get_env(riak_kv, bucket_buffer_size, 1000),
    IndexBufSize = app_helper:get_env(riak_kv, index_buffer_size, 100),
    KeyBufSize = app_helper:get_env(riak_kv, key_buffer_size, 100),
    {ok, VId} = get_vnodeid(Index),
    DeleteMode = app_helper:get_env(riak_kv, delete_mode, 3000),
    AsyncFolding = app_helper:get_env(riak_kv, async_folds, true) == true,
    case catch Mod:start(Index, [{async_folds, AsyncFolding}|Configuration]) of
        {ok, ModState} ->
            %% Get the backend capabilities
            State = #state{idx=Index,
                           async_folding=AsyncFolding,
                           mod=Mod,
                           modstate=ModState,
                           vnodeid=VId,
                           delete_mode=DeleteMode,
                           bucket_buf_size=BucketBufSize,
                           index_buf_size=IndexBufSize,
                           key_buf_size=KeyBufSize,
                           mrjobs=dict:new()},
            case AsyncFolding of
                true ->
                    %% Create worker pool initialization tuple
                    FoldWorkerPool = {pool, riak_kv_worker, 10, []},
                    {ok, State, [FoldWorkerPool]};
                false ->
                    {ok, State}
            end;
        {error, Reason} ->
            lager:error("Failed to start ~p Reason: ~p",
                        [Mod, Reason]),
            riak:stop("backend module failed to start."),
            {error, Reason};
        {'EXIT', Reason1} ->
            lager:error("Failed to start ~p Reason: ~p",
                        [Mod, Reason1]),
            riak:stop("backend module failed to start."),
            {error, Reason1}
    end.


handle_command(?KV_PUT_REQ{bkey=BKey,
                           object=Object,
                           req_id=ReqId,
                           start_time=StartTime,
                           options=Options},
               Sender, State=#state{idx=Idx}) ->
    riak_kv_mapred_cache:eject(BKey),
    riak_core_vnode:reply(Sender, {w, Idx, ReqId}),
    UpdState = do_put(Sender, BKey,  Object, ReqId, StartTime, Options, State),
    {noreply, UpdState};

handle_command(?KV_GET_REQ{bkey=BKey,req_id=ReqId},Sender,State) ->
    do_get(Sender, BKey, ReqId, State);
handle_command(?KV_MGET_REQ{bkeys=BKeys, req_id=ReqId, from=From}, _Sender, State) ->
    do_mget(From, BKeys, ReqId, State);
handle_command(#riak_kv_listkeys_req_v1{bucket=Bucket, req_id=ReqId}, _Sender,
               State=#state{mod=Mod, modstate=ModState, idx=Idx}) ->
    do_legacy_list_bucket(ReqId,Bucket,Mod,ModState,Idx,State);
handle_command(#riak_kv_listkeys_req_v2{bucket=Input, req_id=ReqId, caller=Caller}, _Sender,
               State=#state{async_folding=AsyncFolding,
                            key_buf_size=BufferSize,
                            mod=Mod,
                            modstate=ModState,
                            idx=Idx}) ->
    case Input of
        {filter, Bucket, Filter} ->
            ok;
        Bucket ->
            Filter = none
    end,
    BufferMod = riak_kv_fold_buffer,
    case Bucket of
        '_' ->
            {ok, Capabilities} = Mod:capabilities(ModState),
            AsyncBackend = lists:member(async_fold, Capabilities),
            case AsyncFolding andalso AsyncBackend of
                true ->
                    Opts = [async_fold];
                false ->
                    Opts = []
            end,
            BufferFun =
                fun(Results) ->
                        UniqueResults = lists:usort(Results),
                        Caller ! {ReqId, {kl, Idx, UniqueResults}}
                end,
            FoldFun = fold_fun(buckets, BufferMod, Filter),
            ModFun = fold_buckets;
        _ ->
            {ok, Capabilities} = Mod:capabilities(Bucket, ModState),
            AsyncBackend = lists:member(async_fold, Capabilities),
            case AsyncFolding andalso AsyncBackend of
                true ->
                    Opts = [async_fold, {bucket, Bucket}];
                false ->
                    Opts = [{bucket, Bucket}]
            end,
            BufferFun =
                fun(Results) ->
                        Caller ! {ReqId, {kl, Idx, Results}}
                end,
            FoldFun = fold_fun(keys, BufferMod, Filter),
            ModFun = fold_keys
    end,
    Buffer = BufferMod:new(BufferSize, BufferFun),
    FinishFun =
        fun(Buffer1) ->
                riak_kv_fold_buffer:flush(Buffer1),
                Caller ! {ReqId, Idx, done}
        end,
    case list(FoldFun, FinishFun, Mod, ModFun, ModState, Opts, Buffer) of
        {async, AsyncWork} ->
            {async, {fold, AsyncWork, FinishFun}, Caller, State};
        _ ->
            {noreply, State}
    end;
handle_command(?KV_DELETE_REQ{bkey=BKey, req_id=ReqId}, _Sender, State) ->
    do_delete(BKey, ReqId, State);
handle_command(?KV_VCLOCK_REQ{bkeys=BKeys}, _Sender, State) ->
    {reply, do_get_vclocks(BKeys, State), State};
handle_command(?FOLD_REQ{foldfun=FoldFun, acc0=Acc0}, Sender, State) ->
    %% The function in riak_core used for object folding expects the
    %% bucket and key pair to be passed as the first parameter, but in
    %% riak_kv the bucket and key have been separated. This function
    %% wrapper is to address this mismatch.
    FoldWrapper = fun(Bucket, Key, Value, Acc) ->
                          FoldFun({Bucket, Key}, Value, Acc)
                  end,
    do_fold(FoldWrapper, Acc0, Sender, State);

%% Commands originating from inside this vnode
handle_command({backend_callback, Ref, Msg}, _Sender,
               State=#state{mod=Mod, modstate=ModState}) ->
    Mod:callback(Ref, Msg, ModState),
    {noreply, State};
handle_command({mapexec_error_noretry, JobId, Err}, _Sender, #state{mrjobs=Jobs}=State) ->
    NewState = case dict:find(JobId, Jobs) of
                   {ok, Job} ->
                       Jobs1 = dict:erase(JobId, Jobs),
                       #mrjob{target=Target} = Job,
                       gen_fsm:send_event(Target, {mapexec_error_noretry, self(), Err}),
                       State#state{mrjobs=Jobs1};
                   error ->
                       State
               end,
    {noreply, NewState};
handle_command({mapexec_reply, JobId, Result}, _Sender, #state{mrjobs=Jobs}=State) ->
    NewState = case dict:find(JobId, Jobs) of
                   {ok, Job} ->
                       Jobs1 = dict:erase(JobId, Jobs),
                       #mrjob{target=Target} = Job,
                       gen_fsm:send_event(Target, {mapexec_reply, Result, self()}),
                       State#state{mrjobs=Jobs1};
                   error ->
                       State
               end,
    {noreply, NewState};
handle_command(?KV_VNODE_STATUS_REQ{},
               _Sender,
               State=#state{idx=Index,
                            mod=Mod,
                            modstate=ModState}) ->
    BackendStatus = {backend_status, Mod, Mod:status(ModState)},
    VNodeStatus = [BackendStatus],
    {reply, {vnode_status, Index, VNodeStatus}, State}.

%% @doc Handle a coverage request.
%% More information about the specification for the ItemFilter
%% parameter can be found in the documentation for the
%% {@link riak_kv_coverage_filter} module.
handle_coverage(?KV_LISTBUCKETS_REQ{item_filter=ItemFilter},
                _FilterVNodes,
                Sender,
                State=#state{async_folding=AsyncFolding,
                             bucket_buf_size=BufferSize,
                             mod=Mod,
                             modstate=ModState}) ->
    %% Construct the filter function
    Filter = riak_kv_coverage_filter:build_filter(all, ItemFilter, undefined),
    BufferMod = riak_kv_fold_buffer,
    Buffer = BufferMod:new(BufferSize, result_fun(Sender)),
    FoldFun = fold_fun(buckets, BufferMod, Filter),
    FinishFun = finish_fun(BufferMod, Sender),
    {ok, Capabilities} = Mod:capabilities(ModState),
    AsyncBackend = lists:member(async_fold, Capabilities),
    case AsyncFolding andalso AsyncBackend of
        true ->
            Opts = [async_fold];
        false ->
            Opts = []
    end,
    case list(FoldFun, FinishFun, Mod, fold_buckets, ModState, Opts, Buffer) of
        {async, AsyncWork} ->
            {async, {fold, AsyncWork, FinishFun}, Sender, State};
        _ ->
            {noreply, State}
    end;
handle_coverage(#riak_kv_listkeys_req_v3{bucket=Bucket,
                                         item_filter=ItemFilter},
                FilterVNodes, Sender, State) ->
    %% v3 == no backpressure
    ResultFun = result_fun(Bucket, Sender),
    handle_coverage_listkeys(Bucket, ItemFilter, ResultFun,
                             FilterVNodes, Sender, State);
handle_coverage(?KV_LISTKEYS_REQ{bucket=Bucket,
                                 item_filter=ItemFilter},
                FilterVNodes, Sender, State) ->
    %% v4 == ack-based backpressure
    ResultFun = result_fun_ack(Bucket, Sender),
    handle_coverage_listkeys(Bucket, ItemFilter, ResultFun,
                             FilterVNodes, Sender, State);
handle_coverage(?KV_INDEX_REQ{bucket=Bucket,
                              item_filter=ItemFilter,
                              qry=Query},
                FilterVNodes,
                Sender,
                State=#state{async_folding=AsyncFolding,
                             idx=Index,
                             index_buf_size=BufferSize,
                             mod=Mod,
                             modstate=ModState}) ->

    {ok, Capabilities} = Mod:capabilities(Bucket, ModState),
    IndexBackend = lists:member(indexes, Capabilities),
    AsyncBackend = lists:member(async_fold, Capabilities),
    case IndexBackend of
        true ->
            %% Update stats...
            riak_kv_stat:update(vnode_index_read),

            %% Construct the filter function
            FilterVNode = proplists:get_value(Index, FilterVNodes),
            Filter = riak_kv_coverage_filter:build_filter(Bucket, ItemFilter, FilterVNode),
            BufferMod = riak_kv_fold_buffer,
            Buffer = BufferMod:new(BufferSize, result_fun(Bucket, Sender)),
            FoldFun = fold_fun(keys, BufferMod, Filter),
            FinishFun = finish_fun(BufferMod, Sender),
            case AsyncFolding andalso AsyncBackend of
                true ->
                    Opts = [async_fold,
                            {index, Bucket, Query},
                            {bucket, Bucket}];
                false ->
                    Opts = [{index, Bucket, Query},
                            {bucket, Bucket}]
            end,
            case list(FoldFun, FinishFun, Mod, fold_keys, ModState, Opts, Buffer) of
                {async, AsyncWork} ->
                    {async, {fold, AsyncWork, FinishFun}, Sender, State};
                _ ->
                    {noreply, State}
            end;
        false ->
            {reply, {error, {indexes_not_supported, Mod}}, State}
    end.

%% Convenience for handling both v3 and v4 coverage-based listkeys
handle_coverage_listkeys(Bucket, ItemFilter, ResultFun,
                         FilterVNodes, Sender,
                         State=#state{async_folding=AsyncFolding,
                             idx=Index,
                             key_buf_size=BufferSize,
                             mod=Mod,
                             modstate=ModState}) ->
    %% Construct the filter function
    FilterVNode = proplists:get_value(Index, FilterVNodes),
    Filter = riak_kv_coverage_filter:build_filter(Bucket, ItemFilter, FilterVNode),
    BufferMod = riak_kv_fold_buffer,
    Buffer = BufferMod:new(BufferSize, ResultFun),
    FoldFun = fold_fun(keys, BufferMod, Filter),
    FinishFun = finish_fun(BufferMod, Sender),
    {ok, Capabilities} = Mod:capabilities(Bucket, ModState),
    AsyncBackend = lists:member(async_fold, Capabilities),
    case AsyncFolding andalso AsyncBackend of
        true ->
            Opts = [async_fold, {bucket, Bucket}];
        false ->
            Opts = [{bucket, Bucket}]
    end,
    case list(FoldFun, FinishFun, Mod, fold_keys, ModState, Opts, Buffer) of
        {async, AsyncWork} ->
            {async, {fold, AsyncWork, FinishFun}, Sender, State};
        _ ->
            {noreply, State}
    end.

%% While in handoff, vnodes have the option of returning {forward, State}
%% which will cause riak_core to forward the request to the handoff target
%% node. For riak_kv, we issue a put locally as well as forward it in case
%% the vnode has already handed off the previous version. All other requests
%% are handled locally and not forwarded since the relevant data may not have
%% yet been handed off to the target node. Since we do not forward deletes it
%% is possible that we do not clear a tombstone that was already handed off.
%% This is benign as the tombstone will eventually be re-deleted.
handle_handoff_command(Req=?KV_PUT_REQ{}, Sender, State) ->
    {noreply, NewState} = handle_command(Req, Sender, State),
    {forward, NewState};
%% Handle all unspecified cases locally without forwarding
handle_handoff_command(Req, Sender, State) ->
    handle_command(Req, Sender, State).


handoff_starting(_TargetNode, State) ->
    {true, State#state{in_handoff=true}}.

handoff_cancelled(State) ->
    {ok, State#state{in_handoff=false}}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

handle_handoff_data(BinObj, State) ->
    PBObj = riak_core_pb:decode_riakobject_pb(zlib:unzip(BinObj)),
    BKey = {PBObj#riakobject_pb.bucket,PBObj#riakobject_pb.key},
    case do_diffobj_put(BKey, binary_to_term(PBObj#riakobject_pb.val), State) of
        {ok, UpdModState} ->
            {reply, ok, State#state{modstate=UpdModState}};
        {error, Reason, UpdModState} ->
            {reply, {error, Reason}, State#state{modstate=UpdModState}};
        Err ->
            {reply, {error, Err}, State}
    end.

encode_handoff_item({B, K}, V) ->
    zlib:zip(riak_core_pb:encode_riakobject_pb(
               #riakobject_pb{bucket=B, key=K, val=V})).

is_empty(State=#state{mod=Mod, modstate=ModState}) ->
    {Mod:is_empty(ModState), State}.

delete(State=#state{idx=Index,mod=Mod, modstate=ModState}) ->
    %% clear vnodeid first, if drop removes data but fails
    %% want to err on the side of creating a new vnodeid
    {ok, cleared} = clear_vnodeid(Index),
    case Mod:drop(ModState) of
        {ok, UpdModState} ->
            ok;
        {error, Reason, UpdModState} ->
            lager:error("Failed to drop ~p. Reason: ~p~n", [Mod, Reason]),
            ok
    end,
    {ok, State#state{modstate=UpdModState,vnodeid=undefined}}.

terminate(_Reason, #state{mod=Mod, modstate=ModState}) ->
    Mod:stop(ModState),
    ok.

handle_info({final_delete, BKey, RObjHash}, State = #state{mod=Mod, modstate=ModState}) ->
    UpdState = case do_get_term(BKey, Mod, ModState) of
                   {ok, RObj} ->
                       case delete_hash(RObj) of
                           RObjHash ->
                               do_backend_delete(BKey, RObj, State);
                         _ ->
                               State
                       end;
                   _ ->
                       State
               end,
    {ok, UpdState}.

handle_exit(_Pid, Reason, State) ->
    %% A linked processes has died so the vnode
    %% process should take appropriate action here.
    %% The default behavior is to crash the vnode
    %% process so that it can be respawned
    %% by riak_core_vnode_master to prevent
    %% messages from stacking up on the process message
    %% queue and never being processed.
    lager:error("Linked process exited. Reason: ~p", [Reason]),
    {stop, linked_process_crash, State}.


%% @private
%% upon receipt of a client-initiated put
do_put(Sender, {Bucket,_Key}=BKey, RObj, ReqID, StartTime, Options, State) ->
    case proplists:get_value(bucket_props, Options) of
        undefined ->
            {ok,Ring} = riak_core_ring_manager:get_my_ring(),
            BProps = riak_core_bucket:get_bucket(Bucket, Ring);
        BProps ->
            BProps
    end,
    case proplists:get_value(rr, Options, false) of
        true ->
            PruneTime = undefined;
        false ->
            PruneTime = StartTime
    end,
    Coord = proplists:get_value(coord, Options, false),
    PutArgs = #putargs{returnbody=proplists:get_value(returnbody,Options,false) orelse Coord,
                       coord=Coord,
                       lww=proplists:get_value(last_write_wins, BProps, false),
                       bkey=BKey,
                       robj=RObj,
                       reqid=ReqID,
                       bprops=BProps,
                       starttime=StartTime,
                       prunetime=PruneTime},
    {PrepPutRes, UpdPutArgs} = prepare_put(State, PutArgs),
    {Reply, UpdState} = perform_put(PrepPutRes, State, UpdPutArgs),
    riak_core_vnode:reply(Sender, Reply),

    update_index_write_stats(UpdPutArgs#putargs.is_index, UpdPutArgs#putargs.index_specs),
    riak_kv_stat:update(vnode_put),
    UpdState.

do_backend_delete(BKey, RObj, State = #state{mod = Mod, modstate = ModState}) ->
    %% object is a tombstone or all siblings are tombstones
    riak_kv_mapred_cache:eject(BKey),

    %% Calculate the index specs to remove...
    %% JDM: This should just be a tombstone by this point, but better
    %% safe than sorry.
    IndexSpecs = riak_object:diff_index_specs(undefined, RObj),

    %% Do the delete...
    {Bucket, Key} = BKey,
    case Mod:delete(Bucket, Key, IndexSpecs, ModState) of
        {ok, UpdModState} ->
            update_index_delete_stats(IndexSpecs),
            State#state{modstate = UpdModState};
        {error, _Reason, UpdModState} ->
            State#state{modstate = UpdModState}
    end.

%% Compute a hash of the deleted object
delete_hash(RObj) ->
    erlang:phash2(RObj, 4294967296).

prepare_put(State=#state{vnodeid=VId,
                         mod=Mod,
                         modstate=ModState},
            PutArgs=#putargs{bkey={Bucket, _Key},
                             lww=LWW,
                             robj=RObj,
                             starttime=StartTime}) ->
    %% Can we avoid reading the existing object? If this is not an
    %% index backend, and the bucket is set to last-write-wins, then
    %% no need to incur additional get. Otherwise, we need to read the
    %% old object to know how the indexes have changed.
    {ok, Capabilities} = Mod:capabilities(Bucket, ModState),
    IndexBackend = lists:member(indexes, Capabilities),
    case LWW andalso not IndexBackend of
        true ->
            ObjToStore = riak_object:increment_vclock(RObj, VId, StartTime),
            {{true, ObjToStore}, PutArgs#putargs{is_index = false}};
        false ->
            prepare_put(State, PutArgs, IndexBackend)
    end.
prepare_put(#state{vnodeid=VId,
                   mod=Mod,
                   modstate=ModState},
            PutArgs=#putargs{bkey={Bucket, Key},
                             robj=RObj,
                             bprops=BProps,
                             coord=Coord,
                             lww=LWW,
                             starttime=StartTime,
                             prunetime=PruneTime},
            IndexBackend) ->
    case Mod:get(Bucket, Key, ModState) of
        {error, not_found, _UpdModState} ->
            case IndexBackend of
                true ->
                    IndexSpecs = riak_object:index_specs(RObj);
                false ->
                    IndexSpecs = []
            end,
            ObjToStore = case Coord of
                             true ->
                                 riak_object:increment_vclock(RObj, VId, StartTime);
                             false ->
                                 RObj
                         end,
            {{true, ObjToStore}, PutArgs#putargs{index_specs=IndexSpecs, is_index=IndexBackend}};
        {ok, Val, _UpdModState} ->
            OldObj = binary_to_term(Val),
            case put_merge(Coord, LWW, OldObj, RObj, VId, StartTime) of
                {oldobj, OldObj1} ->
                    {{false, OldObj1}, PutArgs};
                {newobj, NewObj} ->
                    VC = riak_object:vclock(NewObj),
                    AMObj = enforce_allow_mult(NewObj, BProps),
                    case IndexBackend of
                        true ->
                            IndexSpecs =
                                riak_object:diff_index_specs(AMObj,
                                                             OldObj);
                        false ->
                            IndexSpecs = []
                    end,
                    case PruneTime of
                        undefined ->
                            ObjToStore = AMObj;
                        _ ->
                            ObjToStore =
                                riak_object:set_vclock(AMObj,
                                                       vclock:prune(VC,
                                                                    PruneTime,
                                                                    BProps))
                    end,
                    {{true, ObjToStore},
                     PutArgs#putargs{index_specs=IndexSpecs, is_index=IndexBackend}}
            end
    end.

perform_put({false, Obj},
            #state{idx=Idx}=State,
            #putargs{returnbody=true,
                     reqid=ReqID}) ->
    {{dw, Idx, Obj, ReqID}, State};
perform_put({false, _Obj},
            #state{idx=Idx}=State,
            #putargs{returnbody=false,
                     reqid=ReqId}) ->
    {{dw, Idx, ReqId}, State};
perform_put({true, Obj},
            #state{idx=Idx,
                   mod=Mod,
                   modstate=ModState}=State,
            #putargs{returnbody=RB,
                     bkey={Bucket, Key},
                     reqid=ReqID,
                     index_specs=IndexSpecs}) ->
    Val = term_to_binary(Obj),
    case Mod:put(Bucket, Key, IndexSpecs, Val, ModState) of
        {ok, UpdModState} ->
            case RB of
                true ->
                    Reply = {dw, Idx, Obj, ReqID};
                false ->
                    Reply = {dw, Idx, ReqID}
            end;
        {error, _Reason, UpdModState} ->
            Reply = {fail, Idx, ReqID}
    end,
    {Reply, State#state{modstate=UpdModState}}.

%% @private
%% enforce allow_mult bucket property so that no backend ever stores
%% an object with multiple contents if allow_mult=false for that bucket
enforce_allow_mult(Obj, BProps) ->
    case proplists:get_value(allow_mult, BProps) of
        true -> Obj;
        _ ->
            case riak_object:get_contents(Obj) of
                [_] -> Obj;
                Mult ->
                    {MD, V} = select_newest_content(Mult),
                    riak_object:set_contents(Obj, [{MD, V}])
            end
    end.

%% @private
%% choose the latest content to store for the allow_mult=false case
select_newest_content(Mult) ->
    hd(lists:sort(
         fun({MD0, _}, {MD1, _}) ->
                 riak_core_util:compare_dates(
                   dict:fetch(<<"X-Riak-Last-Modified">>, MD0),
                   dict:fetch(<<"X-Riak-Last-Modified">>, MD1))
         end,
         Mult)).

%% @private
put_merge(false, true, _CurObj, UpdObj, _VId, _StartTime) -> % coord=false, LWW=true
    {newobj, UpdObj};
put_merge(false, false, CurObj, UpdObj, _VId, _StartTime) -> % coord=false, LWW=false
    ResObj = riak_object:syntactic_merge(CurObj, UpdObj),
    case ResObj =:= CurObj of
        true ->
            {oldobj, CurObj};
        false ->
            {newobj, ResObj}
    end;
put_merge(true, true, _CurObj, UpdObj, VId, StartTime) -> % coord=false, LWW=true
    {newobj, riak_object:increment_vclock(UpdObj, VId, StartTime)};
put_merge(true, false, CurObj, UpdObj, VId, StartTime) ->
    UpdObj1 = riak_object:increment_vclock(UpdObj, VId, StartTime),
    UpdVC = riak_object:vclock(UpdObj1),
    CurVC = riak_object:vclock(CurObj),

    %% Check the coord put will replace the existing object
    case vclock:get_counter(VId, UpdVC) > vclock:get_counter(VId, CurVC) andalso
        vclock:descends(CurVC, UpdVC) == false andalso
        vclock:descends(UpdVC, CurVC) == true of
        true ->
            {newobj, UpdObj1};
        false ->
            %% If not, make sure it does
            {newobj, riak_object:increment_vclock(
                       riak_object:merge(CurObj, UpdObj1), VId, StartTime)}
    end.

%% @private
do_get(_Sender, BKey, ReqID,
       State=#state{idx=Idx,mod=Mod,modstate=ModState}) ->
    Retval = do_get_term(BKey, Mod, ModState),
    riak_kv_stat:update(vnode_get),
    {reply, {r, Retval, Idx, ReqID}, State}.

do_mget({fsm, Sender}, BKeys, ReqId, State=#state{idx=Idx, mod=Mod, modstate=ModState}) ->
    F = fun(BKey) ->
                R = do_get_term(BKey, Mod, ModState),
                case R of
                    {ok, Obj} ->
                        gen_fsm:send_event(Sender, {r, Obj, Idx, ReqId});
                    _ ->
                        gen_fsm:send_event(Sender, {r, {R, BKey}, Idx, ReqId})
                end,
                riak_kv_stat:update(vnode_get) end,
    [F(BKey) || BKey <- BKeys],
    {noreply, State}.

%% @private
do_get_term(BKey, Mod, ModState) ->
    case do_get_binary(BKey, Mod, ModState) of
        {ok, Bin, _UpdModState} ->
            {ok, binary_to_term(Bin)};
        %% @TODO Eventually it would be good to
        %% make the use of not_found or notfound
        %% consistent throughout the code.
        {error, not_found, _UpdatedModstate} ->
            {error, notfound};
        {error, Reason, _UpdatedModstate} ->
            {error, Reason};
        Err ->
            Err
    end.

do_get_binary({Bucket, Key}, Mod, ModState) ->
    Mod:get(Bucket, Key, ModState).

%% @private
%% @doc This is a generic function for operations that involve
%% listing things from the backend. Examples are listing buckets,
%% listing keys, or doing secondary index queries.
list(FoldFun, FinishFun, Mod, ModFun, ModState, Opts, Buffer) ->
    case Mod:ModFun(FoldFun, Buffer, Opts, ModState) of
        {ok, Acc} ->
            FinishFun(Acc);
        {async, AsyncWork} ->
            {async, AsyncWork}
    end.

%% @private
fold_fun(buckets, BufferMod, none) ->
    fun(Bucket, Buffer) ->
            BufferMod:add(Bucket, Buffer)
    end;
fold_fun(buckets, BufferMod, Filter) ->
    fun(Bucket, Buffer) ->
            case Filter(Bucket) of
                true ->
                    BufferMod:add(Bucket, Buffer);
                false ->
                    Buffer
            end
    end;
fold_fun(keys, BufferMod, none) ->
    fun(_, Key, Buffer) ->
            BufferMod:add(Key, Buffer)
    end;
fold_fun(keys, BufferMod, Filter) ->
    fun(_, Key, Buffer) ->
            case Filter(Key) of
                true ->
                    BufferMod:add(Key, Buffer);
                false ->
                    Buffer
            end
    end.

%% @private
result_fun(Sender) ->
    fun(Items) ->
            riak_core_vnode:reply(Sender, Items)
    end.

%% @private
result_fun(Bucket, Sender) ->
    fun(Items) ->
            riak_core_vnode:reply(Sender, {Bucket, Items})
    end.

%% wait for acknowledgement that results were received before
%% continuing, as a way of providing backpressure for processes that
%% can't handle results as fast as we can send them
result_fun_ack(Bucket, Sender) ->
    fun(Items) ->
            Monitor = riak_core_vnode:monitor(Sender),
            riak_core_vnode:reply(Sender, {{self(), Monitor}, Bucket, Items}),
            receive
                {Monitor, ok} ->
                    erlang:demonitor(Monitor, [flush]);
                {'DOWN', Monitor, process, _Pid, _Reason} ->
                    throw(receiver_down)
            end
    end.

%% @doc If a listkeys request sends a result of `{From, Bucket,
%% Items}', that means it wants acknowledgement of those items before
%% it will send more.  Call this function with that `From' to trigger
%% the next batch.
-spec ack_keys(From::{pid(), reference()}) -> term().
ack_keys({Pid, Ref}) ->
    Pid ! {Ref, ok}.

%% @private
finish_fun(BufferMod, Sender) ->
    fun(Buffer) ->
            finish_fold(BufferMod, Buffer, Sender)
    end.

%% @private
finish_fold(BufferMod, Buffer, Sender) ->
    BufferMod:flush(Buffer),
    riak_core_vnode:reply(Sender, done).

%% @private
%% @deprecated This function is only here to support
%% rolling upgrades and will be removed.
do_legacy_list_bucket(ReqID,'_',Mod,ModState,Idx,State) ->
    FoldBucketsFun =
        fun(Bucket, Buf) ->
                [Bucket | Buf]
        end,
    RetVal = Mod:fold_buckets(FoldBucketsFun, [], [], ModState),
    {reply, {kl, RetVal, Idx, ReqID}, State};
do_legacy_list_bucket(ReqID,Bucket,Mod,ModState,Idx,State) ->
    FoldKeysFun =
        fun(_, Key, Buf) ->
                [Key | Buf]
        end,
    Opts = [{bucket, Bucket}],
    case Mod:fold_keys(FoldKeysFun, [], Opts, ModState) of
        {ok, RetVal} ->
            {reply, {kl, RetVal, Idx, ReqID}, State};
        {error, Reason} ->
            {reply, {error, Reason, ReqID}, State}
    end.

%% @private
do_delete(BKey, ReqId, State) ->
    Mod = State#state.mod,
    ModState = State#state.modstate,
    Idx = State#state.idx,
    DeleteMode = State#state.delete_mode,

    %% Get the existing object.
    case do_get_term(BKey, Mod, ModState) of
        {ok, RObj} ->
            %% Object exists, check if it should be deleted.
            case riak_kv_util:obj_not_deleted(RObj) of
                undefined ->
                    case DeleteMode of
                        keep ->
                            %% keep tombstones indefinitely
                            {reply, {fail, Idx, ReqId}, State};
                        immediate ->
                            UpdState = do_backend_delete(BKey, RObj, State),
                            {reply, {del, Idx, ReqId}, UpdState};
                        Delay when is_integer(Delay) ->
                            erlang:send_after(Delay, self(),
                                              {final_delete, BKey,
                                               delete_hash(RObj)}),
                            %% Nothing checks these messages - will just reply
                            %% del for now until we can refactor.
                            {reply, {del, Idx, ReqId}, State}
                    end;
                _ ->
                    %% not a tombstone or not all siblings are tombstones
                    {reply, {fail, Idx, ReqId}, State}
            end;
        _ ->
            %% does not exist in the backend
            {reply, {fail, Idx, ReqId}, State}
    end.

%% @private
do_fold(Fun, Acc0, Sender, State=#state{async_folding=AsyncFolding,
                                        mod=Mod,
                                        modstate=ModState}) ->
    {ok, Capabilities} = Mod:capabilities(ModState),
    AsyncBackend = lists:member(async_fold, Capabilities),
    case AsyncFolding andalso AsyncBackend of
        true ->
            Opts = [async_fold];
        false ->
            Opts = []
    end,
    case Mod:fold_objects(Fun, Acc0, Opts, ModState) of
        {ok, Acc} ->
            {reply, Acc, State};
        {async, Work} ->
            FinishFun =
                fun(Acc) ->
                        riak_core_vnode:reply(Sender, Acc)
                end,
            {async, {fold, Work, FinishFun}, Sender, State};
        ER ->
            {reply, ER, State}
    end.

%% @private
do_get_vclocks(KeyList,_State=#state{mod=Mod,modstate=ModState}) ->
    [{BKey, do_get_vclock(BKey,Mod,ModState)} || BKey <- KeyList].
%% @private
do_get_vclock({Bucket, Key}, Mod, ModState) ->
    case Mod:get(Bucket, Key, ModState) of
        {error, not_found, _UpdModState} -> vclock:fresh();
        {ok, Val, _UpdModState} -> riak_object:vclock(binary_to_term(Val))
    end.

%% @private
%% upon receipt of a handoff datum, there is no client FSM
do_diffobj_put({Bucket, Key}, DiffObj,
               _StateData=#state{mod=Mod,
                                 modstate=ModState}) ->
    {ok, Capabilities} = Mod:capabilities(Bucket, ModState),
    IndexBackend = lists:member(indexes, Capabilities),
    case Mod:get(Bucket, Key, ModState) of
        {error, not_found, _UpdModState} ->
            case IndexBackend of
                true ->
                    IndexSpecs = riak_object:index_specs(DiffObj);
                false ->
                    IndexSpecs = []
            end,
            Val = term_to_binary(DiffObj),
            Res = Mod:put(Bucket, Key, IndexSpecs, Val, ModState),
            case Res of
                {ok, _UpdModState} ->
                    update_index_write_stats(IndexBackend, IndexSpecs),
                    riak_kv_stat:update(vnode_put);
                _ -> nop
            end,
            Res;
        {ok, Val0, _UpdModState} ->
            OldObj = binary_to_term(Val0),
            %% Merge handoff values with the current - possibly discarding
            %% if out of date.  Ok to set VId/Starttime undefined as
            %% they are not used for non-coordinating puts.
            case put_merge(false, false, OldObj, DiffObj, undefined, undefined) of
                {oldobj, _} ->
                    {ok, ModState};
                {newobj, NewObj} ->
                    AMObj = enforce_allow_mult(NewObj, riak_core_bucket:get_bucket(Bucket)),
                    case IndexBackend of
                        true ->
                            IndexSpecs = riak_object:diff_index_specs(AMObj, OldObj);
                        false ->
                            IndexSpecs = []
                    end,
                    Val = term_to_binary(AMObj),
                    Res = Mod:put(Bucket, Key, IndexSpecs, Val, ModState),
                    case Res of
                        {ok, _UpdModState} ->
                            update_index_write_stats(IndexBackend, IndexSpecs),
                            riak_kv_stat:update(vnode_put);
                        _ ->
                            nop
                    end,
                    Res
            end
    end.

%% @private

%% Get the vnodeid, assigning and storing if necessary
get_vnodeid(Index) ->
    F = fun(Status) ->
                case proplists:get_value(vnodeid, Status, undefined) of
                    undefined ->
                        assign_vnodeid(os:timestamp(),
                                       riak_core_nodeid:get(),
                                       Status);
                    VnodeId ->
                        {VnodeId, Status}
                end
        end,
    update_vnode_status(F, Index). % Returns {ok, VnodeId} | {error, Reason}

%% Assign a unique vnodeid, making sure the timestamp is unique by incrementing
%% into the future if necessary.
assign_vnodeid(Now, NodeId, Status) ->
    {Mega, Sec, _Micro} = Now,
    NowEpoch = 1000000*Mega + Sec,
    LastVnodeEpoch = proplists:get_value(last_epoch, Status, 0),
    VnodeEpoch = erlang:max(NowEpoch, LastVnodeEpoch+1),
    VnodeId = <<NodeId/binary, VnodeEpoch:32/integer>>,
    UpdStatus = [{vnodeid, VnodeId}, {last_epoch, VnodeEpoch} |
                 proplists:delete(vnodeid,
                                  proplists:delete(last_epoch, Status))],
    {VnodeId, UpdStatus}.

%% Clear the vnodeid - returns {ok, cleared}
clear_vnodeid(Index) ->
    F = fun(Status) ->
                {cleared, proplists:delete(vnodeid, Status)}
        end,
    update_vnode_status(F, Index). % Returns {ok, VnodeId} | {error, Reason}

update_vnode_status(F, Index) ->
    VnodeFile = vnode_status_filename(Index),
    ok = filelib:ensure_dir(VnodeFile),
    case read_vnode_status(VnodeFile) of
        {ok, Status} ->
            update_vnode_status2(F, Status, VnodeFile);
        {error, enoent} ->
            update_vnode_status2(F, [], VnodeFile);
        ER ->
            ER
    end.

update_vnode_status2(F, Status, VnodeFile) ->
    case F(Status) of
        {Ret, Status} -> % No change
            {ok, Ret};
        {Ret, UpdStatus} ->
            case write_vnode_status(UpdStatus, VnodeFile) of
                ok ->
                    {ok, Ret};
                ER ->
                    ER
            end
    end.

vnode_status_filename(Index) ->
    P_DataDir = app_helper:get_env(riak_core, platform_data_dir),
    VnodeStatusDir = app_helper:get_env(riak_kv, vnode_status,
                                        filename:join(P_DataDir, "kv_vnode")),
    filename:join(VnodeStatusDir, integer_to_list(Index)).

read_vnode_status(File) ->
    case file:consult(File) of
        {ok, [Status]} when is_list(Status) ->
            {ok, proplists:delete(version, Status)};
        ER ->
            ER
    end.

write_vnode_status(Status, File) ->
    VersionedStatus = [{version, 1} | proplists:delete(version, Status)],
    TmpFile = File ++ "~",
    case file:write_file(TmpFile, io_lib:format("~p.", [VersionedStatus])) of
        ok ->
            file:rename(TmpFile, File);
        ER ->
            ER
    end.

%% @private
wait_for_vnode_status_results([], _ReqId, Acc) ->
    Acc;
wait_for_vnode_status_results(PrefLists, ReqId, Acc) ->
    receive
        {ReqId, {vnode_status, Index, Status}} ->
            UpdPrefLists = proplists:delete(Index, PrefLists),
            wait_for_vnode_status_results(UpdPrefLists,
                                          ReqId,
                                          [{Index, Status} | Acc]);
         _ ->
            wait_for_vnode_status_results(PrefLists, ReqId, Acc)
    end.


%% @private
update_index_write_stats(false, _IndexSpecs) ->
    ok;
update_index_write_stats(true, IndexSpecs) ->
    {Added, Removed} = count_index_specs(IndexSpecs),
    riak_kv_stat:update({vnode_index_write, Added, Removed}).

%% @private
update_index_delete_stats(IndexSpecs) ->
    {_Added, Removed} = count_index_specs(IndexSpecs),
    riak_kv_stat:update({vnode_index_delete, Removed}).

%% @private
%% @doc Given a list of index specs, return the number to add and
%% remove.
count_index_specs(IndexSpecs) ->
    %% Count index specs...
    F = fun({add, _, _}, {AddAcc, RemoveAcc}) ->
                {AddAcc + 1, RemoveAcc};
           ({remove, _, _}, {AddAcc, RemoveAcc}) ->
                {AddAcc, RemoveAcc + 1}
        end,
    lists:foldl(F, {0, 0}, IndexSpecs).

%% @private
bucket_nval_map(Ring) ->
    [{riak_core_bucket:name(B), riak_core_bucket:n_val(B)} ||
        B <- riak_core_bucket:get_buckets(Ring)].

%% @private
default_object_nval() ->
    riak_core_bucket:n_val(riak_core_config:default_bucket_props()).

%% @private
object_info({Bucket, _Key}=BKey) ->
    Hash = riak_core_util:chash_key(BKey),
    {Bucket, Hash}.


-ifdef(TEST).

%% Check assigning a vnodeid twice in the same second
assign_vnodeid_restart_same_ts_test() ->
    Now1 = {1314,224520,343446}, %% TS=1314224520
    Now2 = {1314,224520,345865}, %% as unsigned net-order int <<78,85,121,136>>
    NodeId = <<1, 2, 3, 4>>,
    {Vid1, Status1} = assign_vnodeid(Now1, NodeId, []),
    ?assertEqual(<<1, 2, 3, 4, 78, 85, 121, 136>>, Vid1),
    %% Simulate clear
    Status2 = proplists:delete(vnodeid, Status1),
    %% Reassign
    {Vid2, _Status3} = assign_vnodeid(Now2, NodeId, Status2),
    ?assertEqual(<<1, 2, 3, 4, 78, 85, 121, 137>>, Vid2).

%% Check assigning a vnodeid with a later date
assign_vnodeid_restart_later_ts_test() ->
    Now1 = {1000,000000,0}, %% <<59,154,202,0>>
    Now2 = {2000,000000,0}, %% <<119,53,148,0>>
    NodeId = <<1, 2, 3, 4>>,
    {Vid1, Status1} = assign_vnodeid(Now1, NodeId, []),
    ?assertEqual(<<1, 2, 3, 4, 59,154,202,0>>, Vid1),
    %% Simulate clear
    Status2 = proplists:delete(vnodeid, Status1),
    %% Reassign
    {Vid2, _Status3} = assign_vnodeid(Now2, NodeId, Status2),
    ?assertEqual(<<1, 2, 3, 4, 119,53,148,0>>, Vid2).

%% Check assigning a vnodeid with a later date - just in case of clock skew
assign_vnodeid_restart_earlier_ts_test() ->
    Now1 = {2000,000000,0}, %% <<119,53,148,0>>
    Now2 = {1000,000000,0}, %% <<59,154,202,0>>
    NodeId = <<1, 2, 3, 4>>,
    {Vid1, Status1} = assign_vnodeid(Now1, NodeId, []),
    ?assertEqual(<<1, 2, 3, 4, 119,53,148,0>>, Vid1),
    %% Simulate clear
    Status2 = proplists:delete(vnodeid, Status1),
    %% Reassign
    %% Should be greater than last offered - which is the 2mil timestamp
    {Vid2, _Status3} = assign_vnodeid(Now2, NodeId, Status2),
    ?assertEqual(<<1, 2, 3, 4, 119,53,148,1>>, Vid2).

%% Test
vnode_status_test_() ->
    {setup,
     fun() ->
             os:cmd("chmod u+rwx kv_vnode_status_test"),
             os:cmd("rm -rf kv_vnode_status_test"),
             application:set_env(riak_kv, vnode_status, "kv_vnode_status_test"),
             ok
     end,
     fun(_) ->
             application:unset_env(riak_kv, vnode_status),
             ?cmd("chmod u+rwx kv_vnode_status_test"),
             ?cmd("rm -rf kv_vnode_status_test"),
             ok
     end,
     [?_test(begin % initial create failure
                 ?cmd("rm -rf kv_vnode_status_test || true"),
                 ?cmd("mkdir kv_vnode_status_test"),
                 ?cmd("chmod -w kv_vnode_status_test"),
                 F = fun([]) ->
                             {shouldfail, [badperm]}
                     end,
                 Index = 0,
                 ?assertEqual({error, eacces},  update_vnode_status(F, Index))
             end),
      ?_test(begin % create successfully
                 ?cmd("chmod +w kv_vnode_status_test"),

                 F = fun([]) ->
                             {created, [created]}
                     end,
                 Index = 0,
                 ?assertEqual({ok, created}, update_vnode_status(F, Index))
             end),
      ?_test(begin % update successfully
                 F = fun([created]) ->
                             {updated, [updated]}
                     end,
                 Index = 0,
                 ?assertEqual({ok, updated}, update_vnode_status(F, Index))
             end),
      ?_test(begin % update failure
                 ?cmd("chmod 000 kv_vnode_status_test/0"),
                 ?cmd("chmod 500 kv_vnode_status_test"),
                 F = fun([updated]) ->
                             {shouldfail, [updatedagain]}
                     end,
                 Index = 0,
                 ?assertEqual({error, eacces},  update_vnode_status(F, Index))
             end)

     ]}.

dummy_backend(BackendMod) ->
    Ring = riak_core_ring:fresh(16,node()),
    riak_core_ring_manager:set_ring_global(Ring),
    application:set_env(riak_kv, async_folds, false),
    application:set_env(riak_kv, storage_backend, BackendMod),
    application:set_env(riak_core, default_bucket_props, []),
    application:set_env(bitcask, data_root, bitcask_test_dir()),
    application:set_env(eleveldb, data_root, eleveldb_test_dir()),
    application:set_env(riak_kv, multi_backend_default, multi_dummy_memory1),
    application:set_env(riak_kv, multi_backend,
                        [{multi_dummy_memory1, riak_kv_memory_backend, []},
                         {multi_dummy_memory2, riak_kv_memory_backend, []}]).

bitcask_test_dir() ->
    "./test.bitcask-temp-data".

eleveldb_test_dir() ->
    "./test.eleveldb-temp-data".


backend_with_known_key(BackendMod) ->
    dummy_backend(BackendMod),
    {ok, S1} = init([0]),
    B = <<"f">>,
    K = <<"b">>,
    O = riak_object:new(B, K, <<"z">>),
    {noreply, S2} = handle_command(?KV_PUT_REQ{bkey={B,K},
                                               object=O,
                                               req_id=123,
                                               start_time=riak_core_util:moment(),
                                               options=[]},
                                   {raw, 456, self()},
                                   S1),
    {S2, B, K}.

must_be_first_setup_stuff_test() ->
    application:start(sasl),
    erlang:put({?MODULE, kv}, application:get_all_env(riak_kv)).

list_buckets_test_() ->
    {foreach,
     fun() ->
             application:start(sasl),
             application:get_all_env(riak_kv),
             application:start(folsom),
             riak_kv_stat:register_stats()
     end,
     fun(Env) ->
             application:stop(sasl),
             [application:unset_env(riak_kv, K) ||
                 {K, _V} <- application:get_all_env(riak_kv)],
             [application:set_env(riak_kv, K, V) || {K, V} <- Env]
     end,
     [
      fun(_) ->
              {"bitcask list buckets",
               fun() ->
                       list_buckets_test_i(riak_kv_bitcask_backend)
               end
              }
      end,
      fun(_) ->
              {"eleveldb list buckets",
               fun() ->
                       list_buckets_test_i(riak_kv_eleveldb_backend)
               end
              }
      end,
      fun(_) ->
              {"memory list buckets",
               fun() ->
                       list_buckets_test_i(riak_kv_memory_backend),
                       ok
               end
              }
      end,
      fun(_) ->
              {"multi list buckets",
               fun() ->
                       list_buckets_test_i(riak_kv_multi_backend),
                       ok
               end
              }
      end
     ]
    }.

list_buckets_test_i(BackendMod) ->
    {S, B, _K} = backend_with_known_key(BackendMod),
    Caller = new_result_listener(buckets),
    handle_coverage(?KV_LISTBUCKETS_REQ{item_filter=none}, [],
                    {fsm, {456, {0, node()}}, Caller}, S),
    ?assertEqual({ok, [B]}, results_from_listener(Caller)),
    flush_msgs().

filter_keys_test() ->
    {S, B, K} = backend_with_known_key(riak_kv_memory_backend),
    Caller1 = new_result_listener(keys),
    handle_coverage(?KV_LISTKEYS_REQ{bucket=B,
                                     item_filter=fun(_) -> true end}, [],
                    {fsm, {124, {0, node()}}, Caller1}, S),
    ?assertEqual({ok, [K]}, results_from_listener(Caller1)),

    Caller2 = new_result_listener(keys),
    handle_coverage(?KV_LISTKEYS_REQ{bucket=B,
                                     item_filter=fun(_) -> false end}, [],
                    {fsm, {125, {0, node()}}, Caller2}, S),
    ?assertEqual({ok, []}, results_from_listener(Caller2)),

    Caller3 = new_result_listener(keys),
    handle_coverage(?KV_LISTKEYS_REQ{bucket= <<"g">>,
                                     item_filter=fun(_) -> true end}, [],
                    {fsm, {126, {0, node()}}, Caller3}, S),
    ?assertEqual({ok, []}, results_from_listener(Caller3)),

    flush_msgs().

must_be_last_cleanup_stuff_test() ->
    [application:unset_env(riak_kv, K) ||
        {K, _V} <- application:get_all_env(riak_kv)],
    [application:set_env(riak_kv, K, V) || {K, V} <- erlang:get({?MODULE, kv})].

new_result_listener(Type) ->
    case Type of
        buckets ->
            ResultFun = fun() -> result_listener_buckets([]) end;
        keys ->
            ResultFun = fun() -> result_listener_keys([]) end
    end,
    spawn(ResultFun).

result_listener_buckets(Acc) ->
    receive
        {'$gen_event', {_, done}} ->
            result_listener_done(Acc);
        {'$gen_event', {_, Results}} ->
            result_listener_buckets(Results ++ Acc)

    after 5000 ->
            result_listener_done({timeout, Acc})
    end.

result_listener_keys(Acc) ->
    receive
        {'$gen_event', {_, done}} ->
            result_listener_done(Acc);
        {'$gen_event', {_, {_Bucket, Results}}} ->
            result_listener_keys(Results ++ Acc);
        {'$gen_event', {_, {From, _Bucket, Results}}} ->
            riak_kv_vnode:ack_keys(From),
            result_listener_keys(Results ++ Acc)
    after 5000 ->
            result_listener_done({timeout, Acc})
    end.

result_listener_done(Result) ->
    receive
        {get_results, Pid} ->
            Pid ! {listener_results, Result}
    end.

results_from_listener(Listener) ->
    Listener ! {get_results, self()},
    receive
        {listener_results, Result} ->
            {ok, Result}
    after 5000 ->
            {error, listener_timeout}
    end.

flush_msgs() ->
    receive
        _Msg ->
            flush_msgs()
    after
        0 ->
            ok
    end.

-endif.
