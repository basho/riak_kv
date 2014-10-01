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
-behaviour(riak_core_vnode).

%% API
-export([test_vnode/1, put/7]).
-export([start_vnode/1,
         start_vnodes/1,
         get/3,
         get/4,
         del/3,
         put/6,
         local_get/2,
         local_put/2,
         local_put/3,
         coord_put/6,
         readrepair/6,
         list_keys/4,
         fold/3,
         fold/4,
         get_vclocks/2,
         vnode_status/1,
         ack_keys/1,
         repair/1,
         repair_status/1,
         repair_filter/1,
         hashtree_pid/1,
         rehash/3,
         refresh_index_data/4,
         request_hashtree_pid/1,
         request_hashtree_pid/2,
         reformat_object/2,
         stop_fold/1]).

%% riak_core_vnode API
-export([init/1,
         terminate/2,
         handle_command/3,
         handle_overload_command/3,
         handle_coverage/4,
         is_empty/1,
         delete/1,
         request_hash/1,
         object_info/1,
         nval_map/1,
         handle_handoff_command/3,
         handoff_starting/2,
         handoff_started/2,     %% Note: optional function of the behaviour
         handoff_cancelled/1,
         handoff_finished/2,
         handle_handoff_data/2,
         encode_handoff_item/2,
         handle_exit/3,
         handle_info/2,
         handle_overload_info/2,
         ready_to_exit/0]). %% Note: optional function of the behaviour

-export([handoff_data_encoding_method/0]).
-export([set_vnode_forwarding/2]).

-include_lib("riak_kv_vnode.hrl").
-include_lib("riak_kv_index.hrl").
-include_lib("riak_kv_map_phase.hrl").
-include_lib("riak_core_pb.hrl").
-include("riak_kv_types.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-include_lib("riak_core/include/riak_core_bg_manager.hrl").
-export([put_merge/6]). %% For fsm_eqc_vnode
-endif.

%% N.B. The ?INDEX macro should be called any time the object bytes on
%% disk are modified.
-ifdef(TEST).
%% Use values so that test compile doesn't give 'unused vars' warning.
-define(INDEX(A,B,C), _=element(1,{A,B,C}), ok).
-else.
-define(INDEX(Obj, Reason, Partition), yz_kv:index(Obj, Reason, Partition)).
-endif.

-ifdef(TEST).
-define(YZ_SHOULD_HANDOFF(X), true).
-else.
-define(YZ_SHOULD_HANDOFF(X), yz_kv:should_handoff(X)).
-endif.

-record(mrjob, {cachekey :: term(),
                bkey :: term(),
                reqid :: term(),
                target :: pid()}).

-record(counter_state, {
          %% The number of writes co-ordinated by this vnode
          counter :: non_neg_integer(),
          %% Counter leased up-to. For totally new state/id
          %% this will be that flush threshold See config value
          %% `{riak_kv, counter_lease_size}'
          counter_lease :: non_neg_integer(),
          counter_lease_size :: non_neg_integer()}).

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
                in_handoff = false :: boolean(),
                handoff_target :: node(),
                handoffs_rejected = 0 :: integer(),
                forward :: node() | [{integer(), node()}],
                hashtrees :: pid(),
                md_cache :: ets:tab(),
                md_cache_size :: pos_integer(),
                counter :: #counter_state{},
                status_mgr_pid :: pid() %% a process that manages vnode status persistence
               }).

-type index_op() :: add | remove.
-type index_value() :: integer() | binary().
-type index() :: non_neg_integer().
-type state() :: #state{}.
-type vnodeid() :: binary().

-define(MD_CACHE_BASE, "riak_kv_vnode_md_cache").
-define(DEFAULT_HASHTREE_TOKENS, 90).

%% default value for `counter_lease' in `#counter_state{}'
-define(DEFAULT_CNTR_LEASE, 1000).

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
                  is_index=false :: boolean(), %% set if the b/end supports indexes
                  crdt_op = undefined :: undefined | term() %% if set this is a crdt operation
                 }).

-spec maybe_create_hashtrees(state()) -> state().
maybe_create_hashtrees(State) ->
    maybe_create_hashtrees(riak_kv_entropy_manager:enabled(), State).

-spec maybe_create_hashtrees(boolean(), state()) -> state().
maybe_create_hashtrees(false, State) ->
    State;
maybe_create_hashtrees(true, State=#state{idx=Index,
                                          mod=Mod, modstate=ModState}) ->
    %% Only maintain a hashtree if a primary vnode
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    case riak_core_ring:vnode_type(Ring, Index) of
        primary ->
            {ok, ModCaps} = Mod:capabilities(ModState),
            Empty = case is_empty(State) of
                        {true, _}     -> true;
                        {false, _, _} -> false
                    end,
            Opts = [use_2i || lists:member(indexes, ModCaps)]
                   ++ [vnode_empty || Empty],
            case riak_kv_index_hashtree:start(Index, self(), Opts) of
                {ok, Trees} ->
                    monitor(process, Trees),
                    State#state{hashtrees=Trees};
                Error ->
                    lager:info("riak_kv/~p: unable to start index_hashtree: ~p",
                               [Index, Error]),
                    erlang:send_after(1000, self(), retry_create_hashtree),
                    State#state{hashtrees=undefined}
            end;
        _ ->
            State
    end.

%% API
start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, riak_kv_vnode).

start_vnodes(IdxList) ->
    riak_core_vnode_master:get_vnode_pid(IdxList, riak_kv_vnode).

test_vnode(I) ->
    riak_core_vnode:start_link(riak_kv_vnode, I, infinity).

get(Preflist, BKey, ReqId) ->
    %% Assuming this function is called from a FSM process
    %% so self() == FSM pid
    get(Preflist, BKey, ReqId, {fsm, undefined, self()}).

get(Preflist, BKey, ReqId, Sender) ->
    Req = ?KV_GET_REQ{bkey=sanitize_bkey(BKey),
                      req_id=ReqId},
    riak_core_vnode_master:command(Preflist,
                                   Req,
                                   Sender,
                                   riak_kv_vnode_master).

del(Preflist, BKey, ReqId) ->
    riak_core_vnode_master:command(Preflist,
                                   ?KV_DELETE_REQ{bkey=sanitize_bkey(BKey),
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
                                      bkey = sanitize_bkey(BKey),
                                      object = Obj,
                                      req_id = ReqId,
                                      start_time = StartTime,
                                      options = Options},
                                   Sender,
                                   riak_kv_vnode_master).

local_put(Index, Obj) ->
    local_put(Index, Obj, []).

local_put(Index, Obj, Options) ->
    BKey = {riak_object:bucket(Obj), riak_object:key(Obj)},
    Ref = make_ref(),
    ReqId = erlang:phash2(erlang:now()),
    StartTime = riak_core_util:moment(),
    Sender = {raw, Ref, self()},
    put({Index, node()}, BKey, Obj, ReqId, StartTime, Options, Sender),
    receive
        {Ref, Reply} ->
            Reply
    end.

local_get(Index, BKey) ->
    Ref = make_ref(),
    ReqId = erlang:phash2(erlang:now()),
    Sender = {raw, Ref, self()},
    get({Index,node()}, BKey, ReqId, Sender),
    receive
        {Ref, {r, Result, Index, ReqId}} ->
            Result;
        {Ref, Reply} ->
            {error, Reply}
    end.

refresh_index_data(Partition, BKey, IdxData, TimeOut) ->
    riak_core_vnode_master:sync_command({Partition, node()},
                                        {refresh_index_data, BKey, IdxData},
                                        riak_kv_vnode_master,
                                        TimeOut).

%% Issue a put for the object to the preflist, expecting a reply
%% to an FSM.
coord_put(IndexNode, BKey, Obj, ReqId, StartTime, Options) when is_integer(StartTime) ->
    coord_put(IndexNode, BKey, Obj, ReqId, StartTime, Options, {fsm, undefined, self()}).

coord_put(IndexNode, BKey, Obj, ReqId, StartTime, Options, Sender)
  when is_integer(StartTime) ->
    riak_core_vnode_master:command(IndexNode,
                                   ?KV_PUT_REQ{
                                      bkey = sanitize_bkey(BKey),
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
    Req = riak_core_util:make_fold_req(Fun, Acc0),
    riak_core_vnode_master:sync_spawn_command(Preflist,
                                              Req,
                                              riak_kv_vnode_master).

fold(Preflist, Fun, Acc0, Options) ->
    Req = riak_core_util:make_fold_req(Fun, Acc0, false, Options),
    riak_core_vnode_master:sync_spawn_command(Preflist,
                                              Req,
                                              riak_kv_vnode_master).

get_vclocks(Preflist, BKeyList) ->
    riak_core_vnode_master:sync_spawn_command(Preflist,
                                              ?KV_VCLOCK_REQ{bkeys=BKeyList},
                                              riak_kv_vnode_master).

%% @doc Get status information about the node local vnodes.
vnode_status(PrefLists) ->
    ReqId = erlang:phash2({self(), os:timestamp()}),
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
-spec repair_status(partition()) -> not_found | in_progress.
repair_status(Partition) ->
    riak_core_vnode_manager:repair_status({riak_kv_vnode, Partition}).

%% @doc Given a `Target' partition generate a `Filter' fun to use
%%      during partition repair.
-spec repair_filter(partition()) -> Filter::function().
repair_filter(Target) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    riak_core_repair:gen_filter(Target,
                                Ring,
                                nval_map(Ring),
                                riak_core_bucket:default_object_nval(),
                                fun object_info/1).

-spec hashtree_pid(index()) -> {ok, pid()} | {error, wrong_node}.
hashtree_pid(Partition) ->
    riak_core_vnode_master:sync_command({Partition, node()},
                                        {hashtree_pid, node()},
                                        riak_kv_vnode_master,
                                        infinity).

%% Asynchronous version of {@link hashtree_pid/1} that sends a message back to
%% the calling process. Used by the {@link riak_kv_entropy_manager}.
-spec request_hashtree_pid(index()) -> ok.
request_hashtree_pid(Partition) ->
    ReqId = {hashtree_pid, Partition},
    request_hashtree_pid(Partition, {raw, ReqId, self()}).

%% Version of {@link request_hashtree_pid/1} that takes a sender argument,
%% which could be a raw process, fsm, gen_server, etc.
request_hashtree_pid(Partition, Sender) ->
    riak_core_vnode_master:command({Partition, node()},
                                   {hashtree_pid, node()},
                                   Sender,
                                   riak_kv_vnode_master).

%% Used by {@link riak_kv_exchange_fsm} to force a vnode to update the hashtree
%% for repaired keys. Typically, repairing keys will trigger read repair that
%% will update the AAE hash in the write path. However, if the AAE tree is
%% divergent from the KV data, it is possible that AAE will try to repair keys
%% that do not have divergent KV replicas. In that case, read repair is never
%% triggered. Always rehashing keys after any attempt at repair ensures that
%% AAE does not try to repair the same non-divergent keys over and over.
rehash(Preflist, Bucket, Key) ->
    riak_core_vnode_master:command(Preflist,
                                   {rehash, Bucket, Key},
                                   ignore,
                                   riak_kv_vnode_master).

-spec reformat_object(index(), {riak_object:bucket(), riak_object:key()}) ->
                             ok | {error, term()}.
reformat_object(Partition, BKey) ->
    riak_core_vnode_master:sync_spawn_command({Partition, node()},
                                              {reformat_object,
                                               sanitize_bkey(BKey)},
                                              riak_kv_vnode_master).

%% VNode callbacks

init([Index]) ->
    Mod = app_helper:get_env(riak_kv, storage_backend),
    Configuration = app_helper:get_env(riak_kv),
    BucketBufSize = app_helper:get_env(riak_kv, bucket_buffer_size, 1000),
    IndexBufSize = app_helper:get_env(riak_kv, index_buffer_size, 100),
    KeyBufSize = app_helper:get_env(riak_kv, key_buffer_size, 100),
    WorkerPoolSize = app_helper:get_env(riak_kv, worker_pool_size, 10),
    CounterLeaseSize = app_helper:get_env(riak_kv, counter_lease_size, ?DEFAULT_CNTR_LEASE),
    {ok, StatusMgr} = riak_kv_vnode_status_mgr:start_link(Index),
    {ok, {VId, CounterState}} = get_vnodeid_and_counter(StatusMgr, CounterLeaseSize),
    DeleteMode = app_helper:get_env(riak_kv, delete_mode, 3000),
    AsyncFolding = app_helper:get_env(riak_kv, async_folds, true) == true,
    MDCacheSize = app_helper:get_env(riak_kv, vnode_md_cache_size),
    MDCache =
        case MDCacheSize of
            N when is_integer(N),
                   N > 0 ->
                lager:debug("Initializing metadata cache with size limit: ~p bytes",
                           [MDCacheSize]),
                new_md_cache(VId);
            _ ->
                lager:debug("No metadata cache size defined, not starting"),
                undefined
        end,
    case catch Mod:start(Index, Configuration) of
        {ok, ModState} ->
            %% Get the backend capabilities
            State = #state{idx=Index,
                           async_folding=AsyncFolding,
                           mod=Mod,
                           modstate=ModState,
                           vnodeid=VId,
                           counter = CounterState,
                           status_mgr_pid = StatusMgr,
                           delete_mode=DeleteMode,
                           bucket_buf_size=BucketBufSize,
                           index_buf_size=IndexBufSize,
                           key_buf_size=KeyBufSize,
                           mrjobs=dict:new(),
                           md_cache=MDCache,
                           md_cache_size=MDCacheSize},
            try_set_vnode_lock_limit(Index),
            case AsyncFolding of
                true ->
                    %% Create worker pool initialization tuple
                    FoldWorkerPool = {pool, riak_kv_worker, WorkerPoolSize, []},
                    State2 = maybe_create_hashtrees(State),
                    {ok, State2, [FoldWorkerPool]};
                false ->
                    {ok, State}
            end;
        {error, Reason} ->
            lager:error("Failed to start ~p backend for index ~p error: ~p",
                        [Mod, Index, Reason]),
            riak:stop("backend module failed to start."),
            {error, Reason};
        {'EXIT', Reason1} ->
            lager:error("Failed to start ~p backend for index ~p crash: ~p",
                        [Mod, Index, Reason1]),
            riak:stop("backend module failed to start."),
            {error, Reason1}
    end.


handle_overload_command(?KV_PUT_REQ{}, Sender, Idx) ->
    riak_core_vnode:reply(Sender, {fail, Idx, overload});
handle_overload_command(?KV_GET_REQ{req_id=ReqID}, Sender, Idx) ->
    riak_core_vnode:reply(Sender, {r, {error, overload}, Idx, ReqID});
handle_overload_command(?KV_VNODE_STATUS_REQ{}, Sender, Idx) ->
    riak_core_vnode:reply(Sender, {vnode_status, Idx, [{error, overload}]});
handle_overload_command(_, Sender, _) ->
    riak_core_vnode:reply(Sender, {error, mailbox_overload}).

%% Handle all SC overload messages here
handle_overload_info({ensemble_ping, _From}, _Idx) ->
    %% Don't respond to pings in overload
    ok;
handle_overload_info({ensemble_get, _, From}, _Idx) ->
    riak_kv_ensemble_backend:reply(From, {error, vnode_overload});
handle_overload_info({ensemble_put, _, _, From}, _Idx) ->
    riak_kv_ensemble_backend:reply(From, {error, vnode_overload});
handle_overload_info({raw_forward_put, _, _, From}, _Idx) ->
    riak_kv_ensemble_backend:reply(From, {error, vnode_overload});
handle_overload_info({raw_forward_get, _, From}, _Idx) ->
    riak_kv_ensemble_backend:reply(From, {error, vnode_overload});
handle_overload_info(_, _) ->
    ok.

handle_command(?KV_PUT_REQ{bkey=BKey,
                           object=Object,
                           req_id=ReqId,
                           start_time=StartTime,
                           options=Options},
               Sender, State=#state{idx=Idx}) ->
    StartTS = os:timestamp(),
    riak_core_vnode:reply(Sender, {w, Idx, ReqId}),
    UpdState = do_put(Sender, BKey,  Object, ReqId, StartTime, Options, State),
    update_vnode_stats(vnode_put, Idx, StartTS),
    {noreply, UpdState};

handle_command(?KV_GET_REQ{bkey=BKey,req_id=ReqId},Sender,State) ->
    do_get(Sender, BKey, ReqId, State);
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
            FoldFun = fold_fun(buckets, BufferMod, Filter, undefined),
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
            Extras = fold_extras_keys(Idx, Bucket),
            FoldFun = fold_fun(keys, BufferMod, Filter, Extras),
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
handle_command(?KV_DELETE_REQ{bkey=BKey}, _Sender, State) ->
    do_delete(BKey, State);
handle_command(?KV_VCLOCK_REQ{bkeys=BKeys}, _Sender, State) ->
    {reply, do_get_vclocks(BKeys, State), State};
handle_command(#riak_core_fold_req_v1{} = ReqV1,
               Sender, State) ->
    %% Use make_fold_req() to upgrade to the most recent ?FOLD_REQ
    handle_command(riak_core_util:make_newest_fold_req(ReqV1), Sender, State);
handle_command(?FOLD_REQ{foldfun=FoldFun, acc0=Acc0,
                         forwardable=_Forwardable, opts=Opts}, Sender, State) ->
    %% The riak_core layer takes care of forwarding/not forwarding, so
    %% we ignore forwardable here.
    %%
    %% The function in riak_core used for object folding expects the
    %% bucket and key pair to be passed as the first parameter, but in
    %% riak_kv the bucket and key have been separated. This function
    %% wrapper is to address this mismatch.
    FoldWrapper = fun(Bucket, Key, Value, Acc) ->
                          FoldFun({Bucket, Key}, Value, Acc)
                  end,
    do_fold(FoldWrapper, Acc0, Sender, Opts, State);

%% entropy exchange commands
handle_command({hashtree_pid, Node}, _, State=#state{hashtrees=HT}) ->
    %% Handle riak_core request forwarding during ownership handoff.
    case node() of
        Node ->
            %% Following is necessary in cases where anti-entropy was enabled
            %% after the vnode was already running
            case HT of
                undefined ->
                    State2 = maybe_create_hashtrees(State),
                    {reply, {ok, State2#state.hashtrees}, State2};
                _ ->
                    {reply, {ok, HT}, State}
            end;
        _ ->
            {reply, {error, wrong_node}, State}
    end;
handle_command({rehash, Bucket, Key}, _, State=#state{mod=Mod, modstate=ModState}) ->
    case do_get_binary(Bucket, Key, Mod, ModState) of
        {ok, Bin, _UpdModState} ->
            update_hashtree(Bucket, Key, Bin, State);
        _ ->
            %% Make sure hashtree isn't tracking deleted data
            delete_from_hashtree(Bucket, Key, State)
    end,
    {noreply, State};

handle_command({refresh_index_data, BKey, OldIdxData}, Sender,
               State=#state{mod=Mod, modstate=ModState}) ->
    {Bucket, Key} = BKey,
    {ok, Caps} = Mod:capabilities(Bucket, ModState),
    IndexCap = lists:member(indexes, Caps),
    case IndexCap of
        true ->
            {Exists, RObj, IdxData, ModState2} =
            case do_get_term(BKey, Mod, ModState) of
                {{ok, ExistingObj}, UpModState} ->
                    {true, ExistingObj, riak_object:index_data(ExistingObj),
                     UpModState};
                {{error, _}, UpModState} ->
                    {false, undefined, [], UpModState}
            end,
            IndexSpecs = riak_object:diff_index_data(OldIdxData, IdxData),
            {Reply, ModState3} =
            case Mod:put(Bucket, Key, IndexSpecs, undefined, ModState2) of
                {ok, UpModState2} ->
                    riak_kv_stat:update(vnode_index_refresh),
                    {ok, UpModState2};
                {error, Reason, UpModState2} ->
                    {{error, Reason}, UpModState2}
            end,
            case Exists of
                true ->
                    update_hashtree(Bucket, Key, RObj, State);
                false ->
                    delete_from_hashtree(Bucket, Key, State)
            end,
            riak_core_vnode:reply(Sender, Reply),
            {noreply, State#state{modstate=ModState3}};
        false ->
            {reply, {error, {indexes_not_supported, Mod}}, State}
    end;

handle_command({fold_indexes, FoldIndexFun, Acc}, Sender, State=#state{mod=Mod, modstate=ModState}) ->
    {ok, Caps} = Mod:capabilities(ModState),
    case lists:member(indexes, Caps) of
        true ->
            {async, AsyncWork} = Mod:fold_indexes(FoldIndexFun, Acc, [async_fold], ModState),
            FinishFun = fun(FinalAcc) ->
                                riak_core_vnode:reply(Sender, FinalAcc)
                        end,
            {async, {fold, AsyncWork, FinishFun}, Sender, State};
        false ->
            {reply, {error, {indexes_not_supported, Mod}}, State}
    end;

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
                            modstate=ModState,
                            vnodeid=VId}) ->
    BackendStatus = {backend_status, Mod, Mod:status(ModState)},
    VNodeStatus = [BackendStatus, {vnodeid, VId}],
    {reply, {vnode_status, Index, VNodeStatus}, State};
handle_command({reformat_object, BKey}, _Sender, State) ->
    {Reply, UpdState} = do_reformat(BKey, State),
    {reply, Reply, UpdState};
handle_command({fix_incorrect_index_entry, {done, ForUpgrade}}, _Sender,
               State=#state{mod=Mod, modstate=ModState}) ->
    case Mod:mark_indexes_fixed(ModState, ForUpgrade) of %% only defined for eleveldb backend
        {ok, NewModState} ->
            {reply, ok, State#state{modstate=NewModState}};
        {error, _Reason} ->
            {reply, error, State}
    end;
handle_command({fix_incorrect_index_entry, Keys, ForUpgrade},
               _Sender,
               State=#state{mod=Mod,
                            modstate=ModState}) ->
    Reply =
        case Mod:fix_index(Keys, ForUpgrade, ModState) of
            {ok, _UpModState} ->
                ok;
            {ignore, _UpModState} ->
                ignore;
            {error, Reason, _UpModState} ->
                {error, Reason};
            {reply, Totals, _UpModState} ->
                Totals
        end,
    {reply, Reply, State};
handle_command({get_index_entries, Opts},
               Sender,
               State=#state{mod=Mod,
                            modstate=ModState0}) ->
    ForUpgrade = not proplists:get_value(downgrade, Opts, false),
    BufferSize = proplists:get_value(batch_size, Opts, 1),
    {ok, Caps} = Mod:capabilities(ModState0),
    case lists:member(index_reformat, Caps) of
        true ->
            ModState = Mod:set_legacy_indexes(ModState0, not ForUpgrade),
            Status = Mod:fixed_index_status(ModState),
            case {ForUpgrade, Status} of
                {true, true} -> {reply, done, State};
                {_,  _} ->
                    BufferMod = riak_kv_fold_buffer,
                    ResultFun =
                        fun(Results) ->
                            % Send result batch and wait for acknowledgement
                            % before moving on (backpressure to avoid flooding caller).
                            BatchRef = make_ref(),
                            riak_core_vnode:reply(Sender, {self(), BatchRef, Results}),
                            Monitor = riak_core_vnode:monitor(Sender),
                            receive
                                {ack_keys, BatchRef} ->
                                    erlang:demonitor(Monitor, [flush]);
                                {'DOWN', Monitor, process, _Pid, _Reason} ->
                                    throw(index_reformat_client_died)
                            end
                        end,
                    Buffer = BufferMod:new(BufferSize, ResultFun),
                    FoldFun = fun(B, K, Buf) -> BufferMod:add({B, K}, Buf) end,
                    FinishFun =
                        fun(FinalBuffer) ->
                            BufferMod:flush(FinalBuffer),
                            riak_core_vnode:reply(Sender, done)
                        end,
                    FoldOpts = [{index, incorrect_format, ForUpgrade}, async_fold],
                    case list(FoldFun, FinishFun, Mod, fold_keys, ModState, FoldOpts, Buffer) of
                        {async, AsyncWork} ->
                            {async, {fold, AsyncWork, FinishFun}, Sender, State};
                        _ ->
                            {noreply, State}
                    end
            end;
        false ->
            lager:error("Backend ~p does not support incorrect index query", [Mod]),
            {reply, ignore, State}
    end.

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
    Filter = riak_kv_coverage_filter:build_filter(ItemFilter),
    BufferMod = riak_kv_fold_buffer,

    Buffer = BufferMod:new(BufferSize, result_fun(Sender)),
    FoldFun = fold_fun(buckets, BufferMod, Filter, undefined),
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
    Opts = [{bucket, Bucket}],
    handle_coverage_keyfold(Bucket, ItemFilter, ResultFun,
                            FilterVNodes, Sender, Opts, State);
handle_coverage(?KV_LISTKEYS_REQ{bucket=Bucket,
                                 item_filter=ItemFilter},
                FilterVNodes, Sender, State) ->
    %% v4 == ack-based backpressure
    ResultFun = result_fun_ack(Bucket, Sender),
    Opts = [{bucket, Bucket}],
    handle_coverage_keyfold(Bucket, ItemFilter, ResultFun,
                            FilterVNodes, Sender, Opts, State);
handle_coverage(#riak_kv_index_req_v1{bucket=Bucket,
                              item_filter=ItemFilter,
                              qry=Query},
                FilterVNodes, Sender, State) ->
    %% v1 == no backpressure
    handle_coverage_index(Bucket, ItemFilter, Query,
                          FilterVNodes, Sender, State, fun result_fun/2);
handle_coverage(?KV_INDEX_REQ{bucket=Bucket,
                              item_filter=ItemFilter,
                              qry=Query},
                FilterVNodes, Sender, State) ->
    %% v2 = ack-based backpressure
    handle_coverage_index(Bucket, ItemFilter, Query,
                          FilterVNodes, Sender, State, fun result_fun_ack/2).

prepare_index_query(#riak_kv_index_v3{term_regex=RE} = Q) when
        RE =/= undefined ->
    {ok, CompiledRE} = re:compile(RE),
    Q#riak_kv_index_v3{term_regex=CompiledRE};
prepare_index_query(Q) ->
    Q.

%% @doc Batch size for results is set to 2i max_results if that is less
%% than the default size. Without this the vnode may send back to the FSM
%% more items than could ever be sent back to the client.
buffer_size_for_index_query(#riak_kv_index_v3{max_results=N}, DefaultSize)
  when is_integer(N), N < DefaultSize ->
    N;
buffer_size_for_index_query(_Q, DefaultSize) ->
    DefaultSize.

handle_coverage_index(Bucket, ItemFilter, Query,
                      FilterVNodes, Sender,
                      State=#state{mod=Mod,
                                   key_buf_size=DefaultBufSz,
                                   modstate=ModState},
                      ResultFunFun) ->
    {ok, Capabilities} = Mod:capabilities(Bucket, ModState),
    IndexBackend = lists:member(indexes, Capabilities),
    case IndexBackend of
        true ->
            %% Update stats...
            riak_kv_stat:update(vnode_index_read),

            ResultFun = ResultFunFun(Bucket, Sender),
            BufSize = buffer_size_for_index_query(Query, DefaultBufSz),
            Opts = [{index, Bucket, prepare_index_query(Query)},
                    {bucket, Bucket}, {buffer_size, BufSize}],
            %% @HACK
            %% Really this should be decided in the backend
            %% if there was a index_query fun.
            FoldType = case riak_index:return_body(Query) of
                           true -> fold_objects;
                           false -> fold_keys
                       end,
            handle_coverage_fold(FoldType, Bucket, ItemFilter, ResultFun,
                                    FilterVNodes, Sender, Opts, State);
        false ->
            {reply, {error, {indexes_not_supported, Mod}}, State}
    end.

%% Convenience for handling both v3 and v4 coverage-based key fold operations
handle_coverage_keyfold(Bucket, ItemFilter, Query,
                      FilterVNodes, Sender, State,
                      ResultFunFun) ->
    handle_coverage_fold(fold_keys, Bucket, ItemFilter, Query,
                            FilterVNodes, Sender, State, ResultFunFun).

%% Until a bit of a refactor can occur to better abstract
%% index operations, allow the ModFun for folding to be declared
%% to support index operations that can return objects
handle_coverage_fold(FoldType, Bucket, ItemFilter, ResultFun,
                        FilterVNodes, Sender, Opts0,
                        State=#state{async_folding=AsyncFolding,
                                     idx=Index,
                                     key_buf_size=DefaultBufSz,
                                     mod=Mod,
                                     modstate=ModState}) ->
    %% Construct the filter function
    FilterVNode = proplists:get_value(Index, FilterVNodes),
    Filter = riak_kv_coverage_filter:build_filter(Bucket, ItemFilter, FilterVNode),
    BufferMod = riak_kv_fold_buffer,
    BufferSize = proplists:get_value(buffer_size, Opts0, DefaultBufSz),
    Buffer = BufferMod:new(BufferSize, ResultFun),
    Extras = fold_extras_keys(Index, Bucket),
    FoldFun = fold_fun(keys, BufferMod, Filter, Extras),
    FinishFun = finish_fun(BufferMod, Sender),
    {ok, Capabilities} = Mod:capabilities(Bucket, ModState),
    AsyncBackend = lists:member(async_fold, Capabilities),
    Opts = case AsyncFolding andalso AsyncBackend of
               true ->
                   [async_fold | Opts0];
               false ->
                   Opts0
           end,
    case list(FoldFun, FinishFun, Mod, FoldType, ModState, Opts, Buffer) of
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

%% callback used by dynamic ring sizing to determine where
%% requests should be forwarded. Puts/deletes are forwarded
%% during the operation, all other requests are not
request_hash(?KV_PUT_REQ{bkey=BKey}) ->
    riak_core_util:chash_key(BKey);
request_hash(?KV_DELETE_REQ{bkey=BKey}) ->
    riak_core_util:chash_key(BKey);
request_hash(_Req) ->
    undefined.

handoff_starting({_HOType, TargetNode}=_X, State=#state{handoffs_rejected=RejectCount}) ->
    MaxRejects = app_helper:get_env(riak_kv, handoff_rejected_max, 6),
    case MaxRejects =< RejectCount orelse ?YZ_SHOULD_HANDOFF(_X) of
        true ->
            {true, State#state{in_handoff=true, handoff_target=TargetNode}};
        false ->
            {false, State#state{in_handoff=false, handoff_target=undefined, handoffs_rejected=RejectCount + 1 }}
    end.

%% @doc Optional callback that is exported, but not part of the behaviour.
handoff_started(SrcPartition, WorkerPid) ->
    case maybe_get_vnode_lock(SrcPartition, WorkerPid) of
        ok ->
            FoldOpts = [{iterator_refresh, true}],
            {ok, FoldOpts};
        max_concurrency -> {error, max_concurrency}
    end.

handoff_cancelled(State) ->
    {ok, State#state{in_handoff=false, handoff_target=undefined}}.

handoff_finished(_TargetNode, State) ->
    {ok, State#state{in_handoff=false, handoff_target=undefined}}.

handle_handoff_data(BinObj, State) ->
    try
        {BKey, Val} = decode_binary_object(BinObj),
        {B, K} = BKey,
        case do_diffobj_put(BKey, riak_object:from_binary(B, K, Val),
                            State) of
            {ok, UpdModState} ->
                {reply, ok, State#state{modstate=UpdModState}};
            {error, Reason, UpdModState} ->
                {reply, {error, Reason}, State#state{modstate=UpdModState}}
        end
    catch Error:Reason2 ->
            lager:warning("Unreadable object discarded in handoff: ~p:~p",
                          [Error, Reason2]),
            {reply, ok, State}
    end.

encode_handoff_item({B, K}, V) ->
    %% before sending data to another node change binary version
    %% to one supported by the cluster. This way we don't send
    %% unsupported formats to old nodes
    ObjFmt = riak_core_capability:get({riak_kv, object_format}, v0),
    try
        Value  = riak_object:to_binary_version(ObjFmt, B, K, V),
        encode_binary_object(B, K, Value)
    catch Error:Reason ->
            lager:warning("Handoff encode failed: ~p:~p",
                          [Error,Reason]),
            corrupted
    end.

set_vnode_forwarding(Forward, State) ->
    State#state{forward=Forward}.

is_empty(State=#state{mod=Mod, modstate=ModState}) ->
    IsEmpty = Mod:is_empty(ModState),
    case IsEmpty of
        true ->
            {true, State};
        false ->
            Size = maybe_calc_handoff_size(State),
            {false, Size, State}
    end.

maybe_calc_handoff_size(#state{mod=Mod,modstate=ModState}) ->
    {ok, Capabilities} = Mod:capabilities(ModState),
    case lists:member(size, Capabilities) of
        true -> Mod:data_size(ModState);
        false -> undefined
    end.

delete(State=#state{status_mgr_pid=StatusMgr, mod=Mod, modstate=ModState}) ->
    %% clear vnodeid first, if drop removes data but fails
    %% want to err on the side of creating a new vnodeid
    {ok, cleared} = clear_vnodeid(StatusMgr),
    UpdModState = case Mod:drop(ModState) of
                      {ok, S} ->
                          S;
                      {error, Reason, S2} ->
                          lager:error("Failed to drop ~p. Reason: ~p~n", [Mod, Reason]),
                          S2
                  end,
    case State#state.hashtrees of
        undefined ->
            ok;
        HT ->
            riak_kv_index_hashtree:destroy(HT)
    end,
    {ok, State#state{modstate=UpdModState,vnodeid=undefined,hashtrees=undefined}}.

terminate(_Reason, #state{mod=Mod, modstate=ModState}) ->
    Mod:stop(ModState),
    ok.

handle_info({set_concurrency_limit, Lock, Limit}, State) ->
    try_set_concurrency_limit(Lock, Limit),
    {ok, State};

handle_info({ensemble_ping, From}, State) ->
    riak_ensemble_backend:pong(From),
    {ok, State};

handle_info({ensemble_get, Key, From}, State=#state{idx=Idx, forward=Fwd}) ->
    case Fwd of
        undefined ->
            {reply, {r, Retval, _, _}, State2} = do_get(undefined, Key, undefined, State),
            Reply = case Retval of
                        {ok, Obj} ->
                            Obj;
                        _ ->
                            notfound
                    end,
            riak_kv_ensemble_backend:reply(From, Reply),
            {ok, State2};
        Fwd when is_atom(Fwd) ->
            forward_get({Idx, Fwd}, Key, From),
            {ok, State}
    end;

handle_info({ensemble_put, Key, Obj, From}, State=#state{handoff_target=HOTarget,
                                                         idx=Idx,
                                                         forward=Fwd}) ->
    case Fwd of
        undefined ->
            {Result, State2} = actual_put_tracked(Key, Obj, [], false, undefined, State),
            Reply = case Result of
                        {dw, _Idx, _Obj, _ReqID} ->
                            Obj;
                        {dw, _Idx, _ReqID} ->
                            Obj;
                        {fail, _Idx, _ReqID} ->
                            failed
                    end,
            ((Reply =/= failed) and (HOTarget =/= undefined)) andalso raw_put(HOTarget, Key, Obj),
            riak_kv_ensemble_backend:reply(From, Reply),
            {ok, State2};
        Fwd when is_atom(Fwd) ->
            forward_put({Idx, Fwd}, Key, Obj, From),
            {ok, State}
    end;

handle_info({raw_forward_put, Key, Obj, From}, State) ->
    {Result, State2} = actual_put_tracked(Key, Obj, [], false, undefined, State),
    Reply = case Result of
                {dw, _Idx, _Obj, _ReqID} ->
                    Obj;
                {dw, _Idx, _ReqID} ->
                    Obj;
                {fail, _Idx, _ReqID} ->
                    failed
            end,
    riak_kv_ensemble_backend:reply(From, Reply),
    {ok, State2};
handle_info({raw_forward_get, Key, From}, State) ->
    {reply, {r, Retval, _, _}, State2} = do_get(undefined, Key, undefined, State),
    Reply = case Retval of
                {ok, Obj} ->
                    Obj;
                _ ->
                    notfound
            end,
    riak_kv_ensemble_backend:reply(From, Reply),
    {ok, State2};
handle_info({raw_put, Key, Obj}, State) ->
    {_, State2} = actual_put_tracked(Key, Obj, [], false, undefined, State),
    {ok, State2};

handle_info(retry_create_hashtree, State=#state{hashtrees=undefined}) ->
    State2 = maybe_create_hashtrees(State),
    case State2#state.hashtrees of
        undefined ->
            ok;
        _ ->
            lager:info("riak_kv/~p: successfully started index_hashtree on retry",
                       [State#state.idx])
    end,
    {ok, State2};
handle_info(retry_create_hashtree, State) ->
    {ok, State};
handle_info({'DOWN', _, _, Pid, _}, State=#state{hashtrees=Pid}) ->
    State2 = State#state{hashtrees=undefined},
    State3 = maybe_create_hashtrees(State2),
    {ok, State3};
handle_info({'DOWN', _, _, _, _}, State) ->
    {ok, State};
handle_info({final_delete, BKey, RObjHash}, State = #state{mod=Mod, modstate=ModState}) ->
    UpdState = case do_get_term(BKey, Mod, ModState) of
                   {{ok, RObj}, ModState1} ->
                       case delete_hash(RObj) of
                           RObjHash ->
                               do_backend_delete(BKey, RObj, State#state{modstate=ModState1});
                         _ ->
                               State#state{modstate=ModState1}
                       end;
                   {{error, _}, ModState1} ->
                       State#state{modstate=ModState1}
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

%% Optional Callback. A node is about to exit. Ensure that this node doesn't
%% have any current ensemble members.
ready_to_exit() ->
    [] =:= riak_kv_ensembles:local_ensembles().

%% @private
forward_put({Idx, Node}, Key, Obj, From) ->
    Proxy = riak_core_vnode_proxy:reg_name(riak_kv_vnode, Idx, Node),
    riak_core_send_msg:bang_unreliable(Proxy, {raw_forward_put, Key, Obj, From}),
    ok.

%% @private
forward_get({Idx, Node}, Key, From) ->
    Proxy = riak_core_vnode_proxy:reg_name(riak_kv_vnode, Idx, Node),
    riak_core_send_msg:bang_unreliable(Proxy, {raw_forward_get, Key, From}),
    ok.

%% @private
raw_put({Idx, Node}, Key, Obj) ->
    Proxy = riak_core_vnode_proxy:reg_name(riak_kv_vnode, Idx, Node),
    %% Note: This cannot be bang_unreliable. Don't change.
    Proxy ! {raw_put, Key, Obj},
    ok.

%% @private
%% upon receipt of a client-initiated put
do_put(Sender, {Bucket,_Key}=BKey, RObj, ReqID, StartTime, Options, State) ->
    BProps =  case proplists:get_value(bucket_props, Options) of
                  undefined ->
                      riak_core_bucket:get_bucket(Bucket);
                  Props ->
                      Props
              end,
    PruneTime = case proplists:get_value(rr, Options, false) of
                    true ->
                        undefined;
                    false ->
                        StartTime
                end,
    Coord = proplists:get_value(coord, Options, false),
    CRDTOp = proplists:get_value(counter_op, Options, proplists:get_value(crdt_op, Options, undefined)),
    PutArgs = #putargs{returnbody=proplists:get_value(returnbody,Options,false) orelse Coord,
                       coord=Coord,
                       lww=proplists:get_value(last_write_wins, BProps, false),
                       bkey=BKey,
                       robj=RObj,
                       reqid=ReqID,
                       bprops=BProps,
                       starttime=StartTime,
                       prunetime=PruneTime,
                       crdt_op = CRDTOp},
    {PrepPutRes, UpdPutArgs} = prepare_put(State, PutArgs),
    {Reply, UpdState} = perform_put(PrepPutRes, State, UpdPutArgs),
    riak_core_vnode:reply(Sender, Reply),

    update_index_write_stats(UpdPutArgs#putargs.is_index, UpdPutArgs#putargs.index_specs),
    UpdState.

do_backend_delete(BKey, RObj, State = #state{idx = Idx,
                                             mod = Mod,
                                             modstate = ModState}) ->
    %% object is a tombstone or all siblings are tombstones
    %% Calculate the index specs to remove...
    %% JDM: This should just be a tombstone by this point, but better
    %% safe than sorry.
    IndexSpecs = riak_object:diff_index_specs(undefined, RObj),

    %% Do the delete...
    {Bucket, Key} = BKey,
    case Mod:delete(Bucket, Key, IndexSpecs, ModState) of
        {ok, UpdModState} ->
            ?INDEX(RObj, delete, Idx),
            delete_from_hashtree(Bucket, Key, State),
            maybe_cache_evict(BKey, State),
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
                             coord=Coord,
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
            ObjToStore =
                case Coord of
                    true ->
                        riak_object:increment_vclock(RObj, VId, StartTime);
                    false ->
                        RObj
                end,
            {{true, ObjToStore}, PutArgs#putargs{is_index = false}};
        false ->
            prepare_put(State, PutArgs, IndexBackend)
    end.
prepare_put(#state{vnodeid=VId,
                   mod=Mod,
                   modstate=ModState,
                   idx=Idx,
                   md_cache=MDCache},
            PutArgs=#putargs{bkey={Bucket, Key}=BKey,
                             robj=RObj,
                             bprops=BProps,
                             coord=Coord,
                             lww=LWW,
                             starttime=StartTime,
                             prunetime=PruneTime,
                             crdt_op = CRDTOp},
            IndexBackend) ->
    {CacheClock, CacheData} = maybe_check_md_cache(MDCache, BKey),

    RequiresGet =
        case CacheClock of
            undefined ->
                true;
            Clock ->
                case vclock:descends(riak_object:vclock(RObj), Clock) of
                    true ->
                        false;
                    _ ->
                        true
                end
        end,
    GetReply =
        case RequiresGet of
            true ->
                case do_get_object(Bucket, Key, Mod, ModState) of
                    {error, not_found, _UpdModState} ->
                        ok;
                    {ok, TheOldObj, _UpdModState} ->
                        {ok, TheOldObj}
                end;
            false ->
                FakeObj0 = riak_object:new(Bucket, Key, <<>>),
                FakeObj = riak_object:set_vclock(FakeObj0, CacheClock),
                {ok, FakeObj}
        end,
    case GetReply of
        ok ->
            case IndexBackend of
                true ->
                    IndexSpecs = riak_object:index_specs(RObj);
                false ->
                    IndexSpecs = []
            end,
            case prepare_new_put(Coord, RObj, VId, StartTime, CRDTOp) of
                {error, E} ->
                    {{fail, Idx, E}, PutArgs};
                ObjToStore ->
                    {{true, ObjToStore},
                     PutArgs#putargs{index_specs=IndexSpecs,
                                     is_index=IndexBackend}}
            end;
        {ok, OldObj} ->
            case put_merge(Coord, LWW, OldObj, RObj, VId, StartTime) of
                {oldobj, OldObj1} ->
                    {{false, OldObj1}, PutArgs};
                {newobj, NewObj} ->
                    VC = riak_object:vclock(NewObj),
                    AMObj = enforce_allow_mult(NewObj, BProps),
                    IndexSpecs = case IndexBackend of
                                     true ->
                                         case CacheData /= undefined andalso
                                             RequiresGet == false of
                                             true ->
                                                 NewData = riak_object:index_data(AMObj),
                                                 riak_object:diff_index_data(NewData,
                                                                             CacheData);
                                             false ->
                                                 riak_object:diff_index_specs(AMObj,
                                                                              OldObj)
                                         end;
                                    false ->
                                         []
                    end,
                    ObjToStore = case PruneTime of
                                     undefined ->
                                         AMObj;
                                     _ ->
                                         riak_object:set_vclock(AMObj,
                                                                vclock:prune(VC,
                                                                             PruneTime,
                                                                             BProps))
                    end,
                    case handle_crdt(Coord, CRDTOp, VId, ObjToStore) of
                        {error, E} ->
                            {{fail, Idx, E}, PutArgs};
                        ObjToStore2 ->
                            {{true, ObjToStore2},
                             PutArgs#putargs{index_specs=IndexSpecs,
                                             is_index=IndexBackend}}
                    end
            end
    end.

%% @Doc in the case that this a co-ordinating put, prepare the object.
prepare_new_put(true, RObj, VId, StartTime, undefined) ->
    riak_object:increment_vclock(RObj, VId, StartTime);
prepare_new_put(true, RObj, VId, StartTime, CRDTOp) ->
    VClockUp = riak_object:increment_vclock(RObj, VId, StartTime),
    %% coordinating a _NEW_ crdt operation means
    %% creating + updating the crdt.
    %% Make a new crdt, stuff it in the riak_object
    do_crdt_update(VClockUp, VId, CRDTOp);
prepare_new_put(false, RObj, _VId, _StartTime, _CounterOp) ->
    RObj.

handle_crdt(_, undefined, _VId, RObj) ->
    RObj;
handle_crdt(true, CRDTOp, VId, RObj) ->
    do_crdt_update(RObj, VId, CRDTOp);
handle_crdt(false, _CRDTOp, _Vid, RObj) ->
    RObj.

do_crdt_update(RObj, VId, CRDTOp) ->
    {Time, Value} = timer:tc(riak_kv_crdt, update, [RObj, VId, CRDTOp]),
    riak_kv_stat:update({vnode_dt_update, get_crdt_mod(CRDTOp), Time}),
    Value.

get_crdt_mod(Int) when is_integer(Int) -> ?COUNTER_TYPE;
get_crdt_mod(#crdt_op{mod=Mod}) -> Mod;
get_crdt_mod(Atom) when is_atom(Atom) -> Atom.

perform_put({fail, _, _}=Reply, State, _PutArgs) ->
    {Reply, State};
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
            State,
            #putargs{returnbody=RB,
                     bkey=BKey,
                     reqid=ReqID,
                     index_specs=IndexSpecs}) ->
    {Reply, State2} = actual_put(BKey, Obj, IndexSpecs, RB, ReqID, State),
    {Reply, State2}.

actual_put(BKey={Bucket, Key}, Obj, IndexSpecs, RB, ReqID,
           State=#state{idx=Idx,
                        mod=Mod,
                        modstate=ModState}) ->
    case encode_and_put(Obj, Mod, Bucket, Key, IndexSpecs, ModState,
                       do_max_check) of
        {{ok, UpdModState}, EncodedVal} ->
            update_hashtree(Bucket, Key, EncodedVal, State),
            maybe_cache_object(BKey, Obj, State),
            ?INDEX(Obj, put, Idx),
            case RB of
                true ->
                    Reply = {dw, Idx, Obj, ReqID};
                false ->
                    Reply = {dw, Idx, ReqID}
            end;
        {{error, Reason, UpdModState}, _EncodedVal} ->
            Reply = {fail, Idx, Reason}
    end,
    {Reply, State#state{modstate=UpdModState}}.

actual_put_tracked(BKey, Obj, IndexSpecs, RB, ReqId, State) ->
    StartTS = os:timestamp(),
    Result = actual_put(BKey, Obj, IndexSpecs, RB, ReqId, State),
    update_vnode_stats(vnode_put, State#state.idx, StartTS),
    Result.

do_reformat({Bucket, Key}=BKey, State=#state{mod=Mod, modstate=ModState}) ->
    case do_get_object(Bucket, Key, Mod, ModState) of
        {error, not_found, _UpdModState} ->
            Reply = {error, not_found},
            UpdState = State;
        {ok, RObj, _UpdModState} ->
            %% since it is assumed capabilities have been properly set
            %% to the desired version, to reformat, all we need to do
            %% is submit a new write
            PutArgs = #putargs{returnbody=false,
                               bkey=BKey,
                               reqid=undefined,
                               index_specs=[]},
            case perform_put({true, RObj}, State, PutArgs) of
                {{fail, _, Reason}, UpdState}  ->
                    Reply = {error, Reason};
                {_, UpdState} ->
                    Reply = ok
            end
    end,
    {Reply, UpdState}.

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
    %% a downstream merge, or replication of a coordinated PUT
    %% Merge the value received with local replica value
    %% and store the value IFF it is different to what we already have
    ResObj = riak_object:syntactic_merge(CurObj, UpdObj),
    case riak_object:equal(ResObj, CurObj) of
        true ->
            {oldobj, CurObj};
        false ->
            {newobj, ResObj}
    end;
put_merge(true, LWW, CurObj, UpdObj, VId, StartTime) ->
    {newobj, riak_object:update(LWW, CurObj, UpdObj, VId, StartTime)}.

%% @private
do_get(_Sender, BKey, ReqID,
       State=#state{idx=Idx, mod=Mod, modstate=ModState}) ->
    StartTS = os:timestamp(),
    {Retval, ModState1} = do_get_term(BKey, Mod, ModState),
    case Retval of
        {ok, Obj} ->
            maybe_cache_object(BKey, Obj, State);
        _ ->
            ok
    end,
    update_vnode_stats(vnode_get, Idx, StartTS),
    {reply, {r, Retval, Idx, ReqID}, State#state{modstate=ModState1}}.

%% @private
-spec do_get_term({binary(), binary()}, atom(), tuple()) ->
                         {{ok, riak_object:riak_object()}, tuple()} |
                         {{error, notfound}, tuple()} |
                         {{error, any()}, tuple()}.
do_get_term({Bucket, Key}, Mod, ModState) ->
    case do_get_object(Bucket, Key, Mod, ModState) of
        {ok, Obj, UpdModState} ->
            {{ok, Obj}, UpdModState};
        %% @TODO Eventually it would be good to
        %% make the use of not_found or notfound
        %% consistent throughout the code.
        {error, not_found, UpdModState} ->
            {{error, notfound}, UpdModState};
        {error, Reason, UpdModState} ->
            {{error, Reason}, UpdModState};
        Err ->
            Err
    end.

do_get_binary(Bucket, Key, Mod, ModState) ->
    case uses_r_object(Mod, ModState, Bucket) of
        true ->
            Mod:get_object(Bucket, Key, true, ModState);
        false ->
            Mod:get(Bucket, Key, ModState)
    end.

do_get_object(Bucket, Key, Mod, ModState) ->
    case uses_r_object(Mod, ModState, Bucket) of
        true ->
            %% Non binary returns do not trigger size warnings
            Mod:get_object(Bucket, Key, false, ModState);
        false ->
            case do_get_binary(Bucket, Key, Mod, ModState) of
                {ok, ObjBin, _UpdModState} ->
                    BinSize = size(ObjBin),
                    WarnSize = app_helper:get_env(riak_kv, warn_object_size),
                    case BinSize > WarnSize of
                        true ->
                            lager:warning("Read large object ~p/~p (~p bytes)",
                                          [Bucket, Key, BinSize]);
                        false ->
                            ok
                    end,
                    try
                        case riak_object:from_binary(Bucket, Key, ObjBin) of
                            {error, Reason} ->
                                throw(Reason);
                            RObj ->
                                {ok, RObj, _UpdModState}
                        end
                    catch _:_ ->
                            lager:warning("Unreadable object ~p/~p discarded",
                                          [Bucket,Key]),
                            {error, not_found, _UpdModState}
                    end;
                Else ->
                    Else
            end
    end.

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
fold_fun(buckets, BufferMod, none, _Extra) ->
    fun(Bucket, Buffer) ->
            BufferMod:add(Bucket, Buffer)
    end;
fold_fun(buckets, BufferMod, Filter, _Extra) ->
    fun(Bucket, Buffer) ->
            case Filter(Bucket) of
                true ->
                    BufferMod:add(Bucket, Buffer);
                false ->
                    Buffer
            end
    end;
fold_fun(keys, BufferMod, none, undefined) ->
    fun(_, Key, Buffer) ->
            BufferMod:add(Key, Buffer)
    end;
fold_fun(keys, BufferMod, none, {Bucket, Index, N, NumPartitions}) ->
    fun(_, Key, Buffer) ->
            Hash = riak_core_util:chash_key({Bucket, Key}),
            case riak_core_ring:future_index(Hash, Index, N, NumPartitions, NumPartitions) of
                Index ->
                    BufferMod:add(Key, Buffer);
                _ ->
                    Buffer
            end
    end;
fold_fun(keys, BufferMod, Filter, undefined) ->
    fun(_, Key, Buffer) ->
            case Filter(Key) of
                true ->
                    BufferMod:add(Key, Buffer);
                false ->
                    Buffer
            end
    end;
fold_fun(keys, BufferMod, Filter, {Bucket, Index, N, NumPartitions}) ->
    fun(_, Key, Buffer) ->
            Hash = riak_core_util:chash_key({Bucket, Key}),
            case riak_core_ring:future_index(Hash, Index, N, NumPartitions, NumPartitions) of
                Index ->
                    case Filter(Key) of
                        true ->
                            BufferMod:add(Key, Buffer);
                        false ->
                            Buffer
                    end;
                _ ->
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

fold_extras_keys(Index, Bucket) ->
    case app_helper:get_env(riak_kv, fold_preflist_filter, false) of
        true ->
            {ok, R} = riak_core_ring_manager:get_my_ring(),
            NValMap = nval_map(R),
            N = case lists:keyfind(Bucket, 1, NValMap) of
                    false -> riak_core_bucket:default_object_nval();
                    {Bucket, NVal} -> NVal
                end,
            NumPartitions = riak_core_ring:num_partitions(R),
            {Bucket, Index, N, NumPartitions};
        false ->
            undefined
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
                {Monitor, stop_fold} ->
                    erlang:demonitor(Monitor, [flush]),
                    throw(stop_fold);
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

stop_fold({Pid, Ref}) ->
    Pid ! {Ref, stop_fold}.

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
do_delete(BKey, State) ->
    Mod = State#state.mod,
    ModState = State#state.modstate,
    Idx = State#state.idx,
    DeleteMode = State#state.delete_mode,

    %% Get the existing object.
    case do_get_term(BKey, Mod, ModState) of
        {{ok, RObj}, UpdModState} ->
            %% Object exists, check if it should be deleted.
            case riak_kv_util:obj_not_deleted(RObj) of
                undefined ->
                    case DeleteMode of
                        keep ->
                            %% keep tombstones indefinitely
                            {reply, {fail, Idx, del_mode_keep},
                             State#state{modstate=UpdModState}};
                        immediate ->
                            UpdState = do_backend_delete(BKey, RObj,
                                                         State#state{modstate=UpdModState}),
                            {reply, {del, Idx, del_mode_immediate}, UpdState};
                        Delay when is_integer(Delay) ->
                            erlang:send_after(Delay, self(),
                                              {final_delete, BKey,
                                               delete_hash(RObj)}),
                            %% Nothing checks these messages - will just reply
                            %% del for now until we can refactor.
                            {reply, {del, Idx, del_mode_delayed},
                             State#state{modstate=UpdModState}}
                    end;
                _ ->
                    %% not a tombstone or not all siblings are tombstones
                    {reply, {fail, Idx, not_tombstone}, State#state{modstate=UpdModState}}
            end;
        {{error, notfound}, UpdModState} ->
            %% does not exist in the backend
            {reply, {fail, Idx, not_found}, State#state{modstate=UpdModState}}
    end.

%% @private
do_fold(Fun, Acc0, Sender, ReqOpts, State=#state{async_folding=AsyncFolding,
                                                 mod=Mod,
                                                 modstate=ModState}) ->
    {ok, Capabilities} = Mod:capabilities(ModState),
    Opts0 = maybe_enable_async_fold(AsyncFolding, Capabilities, ReqOpts),
    Opts = maybe_enable_iterator_refresh(Capabilities, Opts0),
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
maybe_enable_async_fold(AsyncFolding, Capabilities, Opts) ->
    AsyncBackend = lists:member(async_fold, Capabilities),
    case AsyncFolding andalso AsyncBackend of
        true ->
            [async_fold|Opts];
        false ->
            Opts
    end.

%% @private
maybe_enable_iterator_refresh(Capabilities, Opts) ->
    Refresh = app_helper:get_env(riak_kv, iterator_refresh, true),
    case Refresh andalso lists:member(iterator_refresh, Capabilities) of
        true ->
            Opts;
        false ->
            lists:keydelete(iterator_refresh, 1, Opts)
    end.

%% @private
do_get_vclocks(KeyList,_State=#state{mod=Mod,modstate=ModState}) ->
    [{BKey, do_get_vclock(BKey,Mod,ModState)} || BKey <- KeyList].
%% @private
do_get_vclock({Bucket, Key}, Mod, ModState) ->
    case do_get_object(Bucket, Key, Mod, ModState) of
        {error, not_found, _UpdModState} -> vclock:fresh();
        {ok, Obj, _UpdModState} -> riak_object:vclock(Obj)
    end.

%% @private
%% upon receipt of a handoff datum, there is no client FSM
do_diffobj_put({Bucket, Key}=BKey, DiffObj,
               StateData=#state{mod=Mod,
                                modstate=ModState,
                                idx=Idx}) ->
    StartTS = os:timestamp(),
    {ok, Capabilities} = Mod:capabilities(Bucket, ModState),
    IndexBackend = lists:member(indexes, Capabilities),
    maybe_cache_evict(BKey, StateData),
    case do_get_object(Bucket, Key, Mod, ModState) of
        {error, not_found, _UpdModState} ->
            case IndexBackend of
                true ->
                    IndexSpecs = riak_object:index_specs(DiffObj);
                false ->
                    IndexSpecs = []
            end,
            case encode_and_put(DiffObj, Mod, Bucket, Key,
                                IndexSpecs, ModState, no_max_check) of
                {{ok, _UpdModState} = InnerRes, _EncodedVal} ->
                    update_hashtree(Bucket, Key, DiffObj, StateData),
                    update_index_write_stats(IndexBackend, IndexSpecs),
                    update_vnode_stats(vnode_put, Idx, StartTS),
                    ?INDEX(DiffObj, handoff, Idx),
                    InnerRes;
                {InnerRes, _Val} ->
                    InnerRes
            end;
        {ok, OldObj, _UpdModState} ->
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
                    case encode_and_put(AMObj, Mod, Bucket, Key,
                                        IndexSpecs, ModState, no_max_check) of
                        {{ok, _UpdModState} = InnerRes, _EncodedVal} ->
                            update_hashtree(Bucket, Key, AMObj, StateData),
                            update_index_write_stats(IndexBackend, IndexSpecs),
                            update_vnode_stats(vnode_put, Idx, StartTS),
                            ?INDEX(AMObj, handoff, Idx),
                            InnerRes;
                        {InnerRes, _EncodedVal} ->
                            InnerRes
                    end
            end
    end.

-spec update_hashtree(binary(), binary(),
                      riak_object:riak_object() | binary(),
                      state()) -> ok.
update_hashtree(Bucket, Key, BinObj, State) when is_binary(BinObj) ->
    RObj = riak_object:from_binary(Bucket, Key, BinObj),
    update_hashtree(Bucket, Key, RObj, State);
update_hashtree(Bucket, Key, RObj, #state{hashtrees=Trees}) ->
    Items = [{object, {Bucket, Key}, RObj}],
    case get_hashtree_token() of
        true ->
            riak_kv_index_hashtree:async_insert(Items, [], Trees),
            ok;
        false ->
            riak_kv_index_hashtree:insert(Items, [], Trees),
            put(hashtree_tokens, max_hashtree_tokens()),
            ok
    end.

delete_from_hashtree(Bucket, Key, #state{hashtrees=Trees})->
    Items = [{object, {Bucket, Key}}],
    case get_hashtree_token() of
        true ->
            riak_kv_index_hashtree:async_delete(Items, Trees),
            ok;
        false ->
            riak_kv_index_hashtree:delete(Items, Trees),
            put(hashtree_tokens, max_hashtree_tokens()),
            ok
    end.

get_hashtree_token() ->
    Tokens = get(hashtree_tokens),
    case Tokens of
        undefined ->
            put(hashtree_tokens, max_hashtree_tokens() - 1),
            true;
        N when N > 0 ->
            put(hashtree_tokens, Tokens - 1),
            true;
        _ ->
            false
    end.

-spec max_hashtree_tokens() -> pos_integer().
max_hashtree_tokens() ->
    app_helper:get_env(riak_kv,
                       anti_entropy_max_async,
                       ?DEFAULT_HASHTREE_TOKENS).

%% @private Get the vnodeid, assigning and storing if necessary.  Also
%% get the current op counter, using the (new?) threshold to assign
%% and store a new leased counter if needed.  @todo document the need
%% for, and invariants of the counter
-spec get_vnodeid_and_counter(pid(), non_neg_integer()) ->
                         {ok, {vnodeid(), #counter_state{}}} |
                         {error, Reason::term()}.
get_vnodeid_and_counter(StatusMgr, CounterThreshold) ->
    {ok, {VId, Counter, Lease}} = riak_kv_vnode_status_mgr:get_vnodeid_and_counter(StatusMgr, CounterThreshold),
    {ok, {VId, #counter_state{counter=Counter, counter_lease=Lease}}}.

%% Clear the vnodeid - returns {ok, cleared}
clear_vnodeid(StatusMgr) ->
    riak_kv_vnode_status_mgr:clear_vnodeid(StatusMgr).

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
-spec update_vnode_stats(vnode_get | vnode_put, partition(), erlang:timestamp()) ->
                                ok.
update_vnode_stats(Op, Idx, StartTS) ->
    riak_kv_stat:update({Op, Idx, timer:now_diff( os:timestamp(), StartTS)}).

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


nval_map(Ring) ->
    riak_core_bucket:bucket_nval_map(Ring).

%% @private
object_info({Bucket, _Key}=BKey) ->
    Hash = riak_core_util:chash_key(BKey),
    {Bucket, Hash}.

%% @private
%% Encoding and decoding selection:

handoff_data_encoding_method() ->
    riak_core_capability:get({riak_kv, handoff_data_encoding}, encode_zlib).

%% Decode a binary object. We first try to interpret the data as a "new format" object which indicates
%% its encoding method, but if that fails we use the legacy zlib and protocol buffer decoding:
decode_binary_object(BinaryObject) ->
    try binary_to_term(BinaryObject) of
        { Method, BinObj } ->
                                case Method of
                                    encode_raw  -> {B, K, Val} = BinObj,
                                                   BKey = {B, K},
                                                   {BKey, Val};

                                    _           -> lager:error("Invalid handoff encoding ~p", [Method]),
                                                   throw(invalid_handoff_encoding)
                                end;

        _                   ->  lager:error("Request to decode invalid handoff object"),
                                throw(invalid_handoff_object)

    %% An exception means we have a legacy handoff object:
    catch
        _:_                 -> do_zlib_decode(BinaryObject)
    end.

do_zlib_decode(BinaryObject) ->
    DecodedObject = zlib:unzip(BinaryObject),
    PBObj = riak_core_pb:decode_riakobject_pb(DecodedObject),
    BKey = {PBObj#riakobject_pb.bucket,PBObj#riakobject_pb.key},
    {BKey, PBObj#riakobject_pb.val}.

encode_binary_object(Bucket, Key, Value) ->
    Method = handoff_data_encoding_method(),

    case Method of
        encode_raw  -> EncodedObject = { Bucket, Key, iolist_to_binary(Value) },
                       return_encoded_binary_object(Method, EncodedObject);

        %% zlib encoding is a special case, we return the legacy format:
        encode_zlib -> PBEncodedObject = riak_core_pb:encode_riakobject_pb(#riakobject_pb{bucket=Bucket, key=Key, val=Value}),
                       zlib:zip(PBEncodedObject)
    end.

%% Return objects in a consistent form:
return_encoded_binary_object(Method, EncodedObject) ->
    term_to_binary({ Method, EncodedObject }).

-spec encode_and_put(
      Obj::riak_object:riak_object(), Mod::term(), Bucket::riak_object:bucket(),
      Key::riak_object:key(), IndexSpecs::list(), ModState::term(),
       MaxCheckFlag::no_max_check | do_max_check) ->
           {{ok, UpdModState::term()}, EncodedObj::binary()} |
           {{error, Reason::term(), UpdModState::term()}, EncodedObj::binary()}.

encode_and_put(Obj, Mod, Bucket, Key, IndexSpecs, ModState, MaxCheckFlag) ->
    DoMaxCheck = MaxCheckFlag == do_max_check,
    NumSiblings = riak_object:value_count(Obj),
    case DoMaxCheck andalso
         NumSiblings > app_helper:get_env(riak_kv, max_siblings) of
        true ->
            lager:error("Put failure: too many siblings for object ~p/~p (~p)",
                        [Bucket, Key, NumSiblings]),
            {{error, {too_many_siblings, NumSiblings}, ModState},
             undefined};
        false ->
            case NumSiblings > app_helper:get_env(riak_kv, warn_siblings) of
                true ->
                    lager:warning("Too many siblings for object ~p/~p (~p)",
                                  [Bucket, Key, NumSiblings]);
                false ->
                    ok
            end,
            encode_and_put_no_sib_check(Obj, Mod, Bucket, Key, IndexSpecs,
                                        ModState, MaxCheckFlag)
    end.

encode_and_put_no_sib_check(Obj, Mod, Bucket, Key, IndexSpecs, ModState,
                            MaxCheckFlag) ->
    DoMaxCheck = MaxCheckFlag == do_max_check,
    case uses_r_object(Mod, ModState, Bucket) of
        true ->
            %% Non binary returning backends will have to handle size warnings
            %% and errors themselves.
            Mod:put_object(Bucket, Key, IndexSpecs, Obj, ModState);
        false ->
            ObjFmt = riak_core_capability:get({riak_kv, object_format}, v0),
            EncodedVal = riak_object:to_binary(ObjFmt, Obj),
            BinSize = size(EncodedVal),
            %% Report or fail on large objects
            case DoMaxCheck andalso
                 BinSize > app_helper:get_env(riak_kv, max_object_size) of
                true ->
                    lager:error("Put failure: object too large to write ~p/~p ~p bytes",
                                [Bucket, Key, BinSize]),
                    {{error, {too_large, BinSize}, ModState},
                     EncodedVal};
                false ->
                    WarnSize = app_helper:get_env(riak_kv, warn_object_size),
                    case BinSize > WarnSize of
                       true ->
                            lager:warning("Writing very large object " ++
                                          "(~p bytes) to ~p/~p",
                                          [BinSize, Bucket, Key]);
                        false ->
                            ok
                    end,
                    PutRet = Mod:put(Bucket, Key, IndexSpecs, EncodedVal,
                                     ModState),
                    {PutRet, EncodedVal}
            end
    end.

uses_r_object(Mod, ModState, Bucket) ->
    {ok, Capabilities} = Mod:capabilities(Bucket, ModState),
    lists:member(uses_r_object, Capabilities).

sanitize_bkey({{<<"default">>, B}, K}) ->
    {B, K};
sanitize_bkey(BKey) ->
    BKey.

%% @private
%% @doc Unless skipping the background manager, try to acquire the per-vnode lock.
%%      Sets our task meta-data in the lock as 'handoff', which is useful for
%%      seeing what's holding the lock via @link riak_core_background_mgr:ps/0.
-spec maybe_get_vnode_lock(SrcPartition::integer(), pid()) -> ok | max_concurrency.
maybe_get_vnode_lock(SrcPartition, Pid) ->
    case riak_core_bg_manager:use_bg_mgr(riak_kv, handoff_use_background_manager) of
        true  ->
            Lock = ?KV_VNODE_LOCK(SrcPartition),
            case riak_core_bg_manager:get_lock(Lock, Pid, [{task, handoff}]) of
                {ok, _Ref} -> ok;
                max_concurrency -> max_concurrency
            end;
        false ->
            ok
    end.

%% @private
%% @doc Query the application environment for 'vnode_lock_concurrency', and
%%      if it's an integer, use it to set the maximum vnode lock concurrency
%%      for Idx. If the background manager is not available yet, schedule a
%%      retry for later. If the application environment variable
%%      'riak_core/use_background_manager' is false, this code just
%%      returns ok without registering.
try_set_vnode_lock_limit(Idx) ->
    %% By default, register per-vnode concurrency limit "lock" with 1 so that only a
    %% single participating subsystem can run a vnode fold at a time. Participation is
    %% voluntary :-)
    Concurrency = case app_helper:get_env(riak_kv, vnode_lock_concurrency, 1) of
                      N when is_integer(N) -> N;
                      _NotNumber -> 1
                  end,
    try_set_concurrency_limit(?KV_VNODE_LOCK(Idx), Concurrency).

try_set_concurrency_limit(Lock, Limit) ->
    try_set_concurrency_limit(Lock, Limit, riak_core_bg_manager:use_bg_mgr()).

try_set_concurrency_limit(_Lock, _Limit, false) ->
    %% skip background manager
    ok;
try_set_concurrency_limit(Lock, Limit, true) ->
    %% this is ok to do more than once
    case riak_core_bg_manager:set_concurrency_limit(Lock, Limit) of
        unregistered ->
            %% not ready yet, try again later
            lager:debug("Background manager unavailable. Will try to set: ~p later.", [Lock]),
            erlang:send_after(250, ?MODULE, {set_concurrency_limit, Lock, Limit});
        _ ->
            lager:debug("Registered lock: ~p", [Lock]),
            ok
    end.

maybe_check_md_cache(Table, BKey) ->
    case Table of
        undefined ->
            {undefined, undefined};
        _ ->
            case ets:lookup(Table, BKey) of
                [{_TS, BKey, MD}] ->
                    MD;
                [] ->
                    {undefined, undefined}
            end
    end.

maybe_cache_object(BKey, Obj, #state{md_cache = MDCache,
                                     md_cache_size = MDCacheSize}) ->
    case MDCache of
        undefined ->
            ok;
        _ ->
            VClock = riak_object:vclock(Obj),
            IndexData = riak_object:index_data(Obj),
            insert_md_cache(MDCache, MDCacheSize, BKey, {VClock, IndexData})
    end.

maybe_cache_evict(BKey, #state{md_cache = MDCache}) ->
    case MDCache of
        undefined ->
            ok;
        _ ->
            ets:delete(MDCache, BKey)
    end.

insert_md_cache(Table, MaxSize, BKey, MD) ->
    TS = os:timestamp(),
    case ets:insert(Table, {TS, BKey, MD}) of
        true ->
            Size = ets:info(Table, memory),
            case Size > MaxSize of
                true ->
                    trim_md_cache(Table, MaxSize);
                false ->
                    ok
            end
    end.

trim_md_cache(Table, MaxSize) ->
    Oldest = ets:first(Table),
    case Oldest of
        '$end_of_table' ->
            ok;
        BKey ->
            ets:delete(Table, BKey),
            Size = ets:info(Table, memory),
            case Size > MaxSize of
                true ->
                    trim_md_cache(Table, MaxSize);
                false ->
                    ok
            end
    end.

new_md_cache(VId) ->
    MDCacheName = list_to_atom(?MD_CACHE_BASE ++ integer_to_list(binary:decode_unsigned(VId))),
    %% ordered set to make sure that the first key is the oldest
    %% term format is {TimeStamp, Key, ValueTuple}
    ets:new(MDCacheName, [ordered_set, {keypos,2}]).

-ifdef(TEST).

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

clean_test_dirs() ->
    ?cmd("rm -rf " ++ bitcask_test_dir()),
    ?cmd("rm -rf " ++ eleveldb_test_dir()).

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

list_buckets_test_() ->
    {foreach,
     fun() ->
             riak_core_ring_manager:setup_ets(test),
             clean_test_dirs(),
             application:start(sasl),
             Env = application:get_all_env(riak_kv),
	     exometer:start(),
             riak_kv_stat:register_stats(),
             {ok, _} = riak_core_bg_manager:start(),
             riak_core_metadata_manager:start_link([{data_dir, "kv_vnode_test_meta"}]),
             Env
     end,
     fun(Env) ->
             riak_core_ring_manager:cleanup_ets(test),
             riak_kv_test_util:stop_process(riak_core_metadata_manager),
             riak_kv_test_util:stop_process(riak_core_bg_manager),
	     exometer:stop(),
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
    riak_core_ring_manager:setup_ets(test),
    clean_test_dirs(),
    riak_core_metadata_manager:start_link([{data_dir, "kv_vnode_test_meta"}]),
    riak_core_bg_manager:start(),
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

    riak_core_ring_manager:cleanup_ets(test),
    riak_kv_test_util:stop_process(riak_core_metadata_manager),
    riak_kv_test_util:stop_process(riak_core_bg_manager),
    flush_msgs().

%% include bitcask.hrl for HEADER_SIZE macro
-include_lib("bitcask/include/bitcask.hrl").

%% Verify that a bad CRC on read will not crash the vnode, which when done in
%% preparation for a write prevents the write from going through.
bitcask_badcrc_test() ->
    riak_core_ring_manager:setup_ets(test),
    riak_core_metadata_manager:start_link([{data_dir, "kv_vnode_test_meta"}]),
    riak_core_bg_manager:start(),
    clean_test_dirs(),
    {S, B, K} = backend_with_known_key(riak_kv_bitcask_backend),
    DataDir = filename:join(bitcask_test_dir(), "0"),
    [DataFile] = filelib:wildcard(DataDir ++ "/*.data"),
    {ok, Fh} = file:open(DataFile, [read, write]),
    ok = file:pwrite(Fh, ?HEADER_SIZE, <<0>>),
    file:close(Fh),
    O = riak_object:new(B, K, <<"y">>),
    {noreply, _} = handle_command(?KV_PUT_REQ{bkey={B,K},
                                               object=O,
                                               req_id=123,
                                               start_time=riak_core_util:moment(),
                                               options=[]},
                                   {raw, 456, self()},
                                   S),
    riak_core_ring_manager:cleanup_ets(test),
    riak_kv_test_util:stop_process(riak_core_metadata_manager),
    riak_kv_test_util:stop_process(riak_core_bg_manager),
    flush_msgs().


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
