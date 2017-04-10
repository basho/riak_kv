%% -------------------------------------------------------------------
%%
%% riak_kv_vnode: VNode Implementation
%%
%% Copyright (c) 2007-2016 Basho Technologies, Inc.  All Rights Reserved.
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
         local_reap/3,
         coord_put/6,
         readrepair/6,
         list_keys/4,
         fold/3,
         fold/4,
         sweep/3,
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
         upgrade_hashtree/1,
         reformat_object/2,
         stop_fold/1,
         get_modstate/1]).

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

-record(mrjob, {cachekey :: term(),
                bkey :: term(),
                reqid :: term(),
                target :: pid()}).

-record(counter_state, {
          %% kill switch, if for any reason one wants disable per-key-epoch, then set
          %% [{riak_kv, [{per_key_epoch, false}]}].
          use = true :: boolean(),
          %% The number of new epoch writes co-ordinated by this vnode
          %% What even is a "key epoch?" It is any time a key is
          %% (re)created. A new write, a write not yet coordinated by
          %% this vnode, a write where local state is unreadable.
          cnt = 0 :: non_neg_integer(),
          %% Counter leased up-to. For totally new state/id
          %% this will be that flush threshold See config value
          %% `{riak_kv, counter_lease_size}'
          lease = 0 :: non_neg_integer(),
          lease_size = 0 :: non_neg_integer(),
          %% Has a lease been requested but not granted yet
          leasing = false :: boolean()
         }).

-type update_hook() :: module().

-record(state, {idx :: partition(),
                mod :: module(),
                async_put :: boolean(),
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
                upgrade_hashtree = false :: boolean(),
                md_cache :: ets:tab(),
                md_cache_size :: pos_integer(),
                counter :: #counter_state{},
                status_mgr_pid :: pid(), %% a process that manages vnode status persistence
                update_hook = riak_kv_noop_update_hook :: update_hook()
               }).

-type index_op() :: add | remove.
-type index_value() :: integer() | binary().
-type index() :: non_neg_integer().
-type state() :: #state{}.
-type vnodeid() :: binary().
-type counter_lease_error() :: {error, counter_lease_max_errors | counter_lease_timeout}.
-type hashtree_action() :: delete | tombstone | update.


-define(MD_CACHE_BASE, "riak_kv_vnode_md_cache").
-define(DEFAULT_HASHTREE_TOKENS, 90).

%% default value for `counter_lease' in `#counter_state{}'
%% NOTE: these MUST be positive integers!
%% @see non_neg_env/3
-define(DEFAULT_CNTR_LEASE, 10000).
%% On advise/review from Scott decided to cap the size of leases. 50m
%% is a lot of new epochs for a single vnode, and it saves us from
%% buring through vnodeids in the worst case.
-define(MAX_CNTR_LEASE, 50000000).
%% Should these cuttlefish-able?  If it takes more than 20 attempts to
%% fsync the vnode counter to disk, die. (NOTE this is not ERRS*TO but
%% first to trip see blocking_lease_counter/3)
-define(DEFAULT_CNTR_LEASE_ERRS, 20).
%% If it takes more than 20 seconds to fsync the vnode counter to disk,
%% die
-define(DEFAULT_CNTR_LEASE_TO, 20000). % 20 seconds!
%%

%% Erlang's if Bool -> thing; true -> thang end. syntax hurts my
%% brain. It scans as if true -> thing; true -> thang end. So, here is
%% a macro, ?ELSE to use in if statements. You're welcome.
-define(ELSE, true).

-record(putargs, {returnbody :: boolean(),
                  coord:: boolean(),
                  lww :: boolean(),
                  bkey :: {binary(), binary()},
                  robj :: term(),
                  index_specs=[] :: [{index_op(), binary(), index_value()}],
                  reqid :: non_neg_integer(),
                  bprops :: riak_kv_bucket:props(),
                  starttime :: non_neg_integer(),
                  prunetime :: undefined| non_neg_integer(),
                  readrepair=false :: boolean(),
                  is_index=false :: boolean(), %% set if the b/end supports indexes
                  crdt_op = undefined :: undefined | term(), %% if set this is a crdt operation
                  hash_ops = no_hash_ops
                 }).

-spec maybe_create_hashtrees(state()) -> state().
maybe_create_hashtrees(State) ->
    maybe_create_hashtrees(riak_kv_entropy_manager:enabled(), State).

-spec maybe_create_hashtrees(boolean(), state()) -> state().
maybe_create_hashtrees(false, State) ->
    State#state{upgrade_hashtree=false};
maybe_create_hashtrees(true, State=#state{idx=Index, upgrade_hashtree=Upgrade,
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
                   ++ [vnode_empty || Empty]
                   ++ [upgrade || Upgrade],
            case riak_kv_index_hashtree:start(Index, self(), Opts) of
                {ok, Trees} ->
                    monitor(process, Trees),
                    State#state{hashtrees=Trees, upgrade_hashtree=false};
                Error ->
                    lager:info("riak_kv/~p: unable to start index_hashtree: ~p",
                               [Index, Error]),
                    erlang:send_after(1000, self(), retry_create_hashtree),
                    State#state{hashtrees=undefined}
            end;
        _ ->
            State#state{upgrade_hashtree=false}
    end.

%% @doc Reveal the underlying module state for testing
-spec(get_modstate(state()) -> {atom(), state()}).
get_modstate(_State=#state{mod=Mod,
                           modstate=ModState}) ->
    {Mod, ModState}.

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
    Req = riak_kv_requests:new_get_request(sanitize_bkey(BKey), ReqId),
    riak_core_vnode_master:command(Preflist,
                                   Req,
                                   Sender,
                                   riak_kv_vnode_master).

del(Preflist, BKey, ReqId) ->
    Req = riak_kv_requests:new_delete_request(sanitize_bkey(BKey), ReqId),
    riak_core_vnode_master:command(Preflist, Req, riak_kv_vnode_master).

reap(Preflist, BKey, RObj, Sender) ->
    riak_core_vnode_master:command(Preflist,
                                   {reap, sanitize_bkey(BKey), RObj},
                                   Sender,
                                   riak_kv_vnode_master).

%% Issue a put for the object to the preflist, expecting a reply
%% to an FSM.
put(Preflist, BKey, Obj, ReqId, StartTime, Options) when is_integer(StartTime) ->
    put(Preflist, BKey, Obj, ReqId, StartTime, Options, {fsm, undefined, self()}).

put(Preflist, BKey, Obj, ReqId, StartTime, Options, Sender)
  when is_integer(StartTime) ->
    Req = riak_kv_requests:new_put_request(
        sanitize_bkey(BKey), Obj, ReqId, StartTime, Options),
    riak_core_vnode_master:command(Preflist,
                                   Req,
                                   Sender,
                                   riak_kv_vnode_master).

local_put(Index, Obj) ->
    local_put(Index, Obj, []).

local_put(Index, Obj, Options) ->
    BKey = {riak_object:bucket(Obj), riak_object:key(Obj)},
    ReqId = erlang:phash2(erlang:now()),
    StartTime = riak_core_util:moment(),
    put({Index, node()}, BKey, Obj, ReqId, StartTime, Options, ignore).

local_reap(Index, BKey, RObj) ->
    Ref = make_ref(),
    Sender = {raw, Ref, self()},
    reap({Index, node()}, BKey, RObj, Sender),
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
    put([IndexNode], BKey, Obj, ReqId, StartTime, [coord | Options], Sender).

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

sweep(Preflist, Participants, EstimatedKeys) ->
    riak_core_vnode_master:sync_spawn_command(Preflist,
                                              {sweep, Participants, EstimatedKeys},
                                              riak_kv_vnode_master).

get_vclocks(Preflist, BKeyList) ->
    riak_core_vnode_master:sync_spawn_command(Preflist,
                                              riak_kv_requests:new_vclock_request(BKeyList),
                                              riak_kv_vnode_master).

%% @doc Get status information about the node local vnodes.
vnode_status(PrefLists) ->
    ReqId = erlang:phash2({self(), os:timestamp()}),
    %% Get the status of each vnode
    riak_core_vnode_master:command(PrefLists,
                                   riak_kv_requests:new_vnode_status_request(),
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

%% @doc Destroy and restart the hashtrees associated with Partitions vnode.
-spec upgrade_hashtree(index()) -> ok | {error, wrong_node}.
upgrade_hashtree(Partition) ->
    riak_core_vnode_master:command({Partition, node()},
                                   {upgrade_hashtree, node()},
                                   ignore,
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
    UseEpochCounter = app_helper:get_env(riak_kv, use_epoch_counter, true),
    %%  This _has_ to be a non_neg_integer(), and really, if it is
    %%  zero, you are fsyncing every.single.key epoch.
    CounterLeaseSize = min(?MAX_CNTR_LEASE,
                           non_neg_env(riak_kv, counter_lease_size, ?DEFAULT_CNTR_LEASE)),
    {ok, StatusMgr} = riak_kv_vnode_status_mgr:start_link(self(), Index, UseEpochCounter),
    {ok, {VId, CounterState}} = get_vnodeid_and_counter(StatusMgr, CounterLeaseSize, UseEpochCounter),
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
            DoAsyncPut =  case app_helper:get_env(riak_kv, allow_async_put, true) of
                true ->
                    erlang:function_exported(Mod, async_put, 5);
                _ ->
                    false
            end,
            State = #state{idx=Index,
                           async_folding=AsyncFolding,
                           mod=Mod,
                           async_put = DoAsyncPut,
                           modstate=ModState,
                           vnodeid=VId,
                           counter=CounterState,
                           status_mgr_pid=StatusMgr,
                           delete_mode=DeleteMode,
                           bucket_buf_size=BucketBufSize,
                           index_buf_size=IndexBufSize,
                           key_buf_size=KeyBufSize,
                           mrjobs=dict:new(),
                           md_cache=MDCache,
                           md_cache_size=MDCacheSize,
                           update_hook=update_hook()},
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


handle_overload_command(Req, Sender, Idx) ->
    handle_overload_request(riak_kv_requests:request_type(Req), Req, Sender, Idx).

handle_overload_request(kv_put_request, _Req, Sender, Idx) ->
    riak_core_vnode:reply(Sender, {fail, Idx, overload});
handle_overload_request(kv_get_request, Req, Sender, Idx) ->
    ReqId = riak_kv_requests:get_request_id(Req),
    riak_core_vnode:reply(Sender, {r, {error, overload}, Idx, ReqId});
handle_overload_request(kv_w1c_put_request, Req, Sender, _Idx) ->
    Type = riak_kv_requests:get_replica_type(Req),
    riak_core_vnode:reply(Sender, ?KV_W1C_PUT_REPLY{reply={error, overload}, type=Type});
handle_overload_request(kv_vnode_status_request, _Req, Sender, Idx) ->
    riak_core_vnode:reply(Sender, {vnode_status, Idx, [{error, overload}]});
handle_overload_request(_, _Req, Sender, _Idx) ->
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
            Opts = maybe_enable_async_fold(AsyncFolding, Capabilities, []),
            BufferFun =
                fun(Results) ->
                        UniqueResults = lists:usort(Results),
                        Caller ! {ReqId, {kl, Idx, UniqueResults}}
                end,
            FoldFun = fold_fun(buckets, BufferMod, Filter, undefined),
            ModFun = fold_buckets;
        _ ->
            {ok, Capabilities} = Mod:capabilities(Bucket, ModState),
            Opts = maybe_enable_async_fold(AsyncFolding, Capabilities, [{bucket, Bucket}]),
            BufferFun =
                fun(Results) ->
                        Caller ! {ReqId, {kl, Idx, Results}}
                end,
            Extras = fold_extras_keys(Idx, Bucket),
            FoldFun = fold_fun(keys, BufferMod, Filter, Extras),
            ModFun = fold_keys
    end,
    Buffer = riak_kv_fold_buffer:new(BufferSize, BufferFun),
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

handle_command(#riak_core_fold_req_v1{} = ReqV1,
               Sender, State) ->
    %% Use make_fold_req() to upgrade to the most recent ?FOLD_REQ
    handle_command(riak_core_util:make_newest_fold_req(ReqV1), Sender, State);
handle_command(?FOLD_REQ{foldfun=FoldFun, acc0=Acc0,
                         forwardable=_Forwardable, opts=Opts}, Sender, State) ->
    %% The riak_core layer takes care of forwarding/not forwarding, so
    %% we ignore forwardable here.
    do_fold(FoldFun, Acc0, Sender, Opts, State);

handle_command({sweep, Participants, EstimatedKeys}, Sender, State) ->
    do_sweep(Participants, EstimatedKeys, Sender, State);

handle_command({reap, BKey, RObj}, _Sender, State) ->
     State1 = do_backend_delete(BKey, RObj, tombstone, State),
     {reply, ok, State1};

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
                    case State2#state.hashtrees of
                        undefined ->
                            {reply, {error, wrong_node}, State2};
                        _ ->
                            {reply, {ok, State2#state.hashtrees}, State2}
                    end;
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
                    ok = riak_kv_stat:update(vnode_index_refresh),
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

handle_command({upgrade_hashtree, Node}, _, State=#state{hashtrees=HT}) ->
    %% Make sure we dont kick off an upgrade during a possible handoff
    case node() of
        Node ->
            case HT of
                undefined ->
                    {reply, {error, wrong_node}, State};
                _  ->
                    case {riak_kv_index_hashtree:get_version(HT),
                        riak_kv_entropy_manager:get_pending_version()} of
                        {legacy, legacy} ->
                            {reply, ok, State};
                        {legacy, _} ->
                            lager:notice("Destroying and upgrading index_hashtree for Index: ~p", [State#state.idx]),
                            _ = riak_kv_index_hashtree:destroy(HT),
                            riak_kv_entropy_info:clear_tree_build(State#state.idx),
                            State1 = State#state{upgrade_hashtree=true,hashtrees=undefined},
                            {reply, ok, State1};
                        _ ->
                            {reply, ok, State}
                    end
            end;
        _ ->
            {reply, {error, wrong_node}, State}
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
handle_command({reformat_object, BKey}, _Sender, State) ->
    {Reply, UpdState} = do_reformat(BKey, State),
    {reply, Reply, UpdState};

handle_command(Req, Sender, State) ->
    handle_request(riak_kv_requests:request_type(Req), Req, Sender, State).


%% @todo: pre record encapsulation there was no catch all clause in handle_command,
%%        so crashing on unknown should work.
handle_request(kv_put_request, Req, Sender, #state{idx = Idx} = State) ->
    StartTS = os:timestamp(),
    ReqId = riak_kv_requests:get_request_id(Req),
    riak_core_vnode:reply(Sender, {w, Idx, ReqId}),
    {_Reply, UpdState} = do_put(Sender, Req, State),
    update_vnode_stats(vnode_put, Idx, StartTS),
    {noreply, UpdState};
handle_request(kv_get_request, Req, Sender, State) ->
    BKey = riak_kv_requests:get_bucket_key(Req),
    ReqId = riak_kv_requests:get_request_id(Req),
    do_get(Sender, BKey, ReqId, State);
%% NB. The following two function clauses discriminate on the async_put State field
handle_request(kv_w1c_put_request, Req, Sender, State=#state{async_put=true}) ->
    {Bucket, Key} = riak_kv_requests:get_bucket_key(Req),
    EncodedVal = riak_kv_requests:get_encoded_obj(Req),
    ReplicaType = riak_kv_requests:get_replica_type(Req),
    Mod = State#state.mod,
    ModState = State#state.modstate,
    StartTS = os:timestamp(),
    Context = {w1c_async_put, Sender, ReplicaType, Bucket, Key, EncodedVal, StartTS},
    case Mod:async_put(Context, Bucket, Key, EncodedVal, ModState) of
        {ok, UpModState} ->
            {noreply, State#state{modstate=UpModState}};
        {error, Reason, UpModState} ->
            {reply, ?KV_W1C_PUT_REPLY{reply={error, Reason}, type=ReplicaType}, State#state{modstate=UpModState}}
    end;
handle_request(kv_w1c_put_request, Req, _Sender, State=#state{async_put=false, update_hook=UpdateHook}) ->
    {Bucket, Key} = riak_kv_requests:get_bucket_key(Req),
    EncodedVal = riak_kv_requests:get_encoded_obj(Req),
    ReplicaType = riak_kv_requests:get_replica_type(Req),
    Mod = State#state.mod,
    ModState = State#state.modstate,
    Idx = State#state.idx,
    StartTS = os:timestamp(),
    case Mod:put(Bucket, Key, [], EncodedVal, ModState) of
        {ok, UpModState} ->
            update_hashtree(Bucket, Key, EncodedVal, State),
            update_binary(UpdateHook, Bucket, Key, EncodedVal, put, Idx),
            update_vnode_stats(vnode_put, Idx, StartTS),
            {reply, ?KV_W1C_PUT_REPLY{reply=ok, type=ReplicaType}, State#state{modstate=UpModState}};
        {error, Reason, UpModState} ->
            {reply, ?KV_W1C_PUT_REPLY{reply={error, Reason}, type=ReplicaType}, State#state{modstate=UpModState}}
    end;
handle_request(kv_vnode_status_request, _Req, _Sender, State=#state{idx=Index,
                                                                   mod=Mod,
                                                                   modstate=ModState,
                                                                   counter=CS,
                                                                   vnodeid=VId}) ->
    BackendStatus = {backend_status, Mod, Mod:status(ModState)},
    #counter_state{cnt=Cnt, lease=Lease, lease_size=LeaseSize, leasing=Leasing} = CS,
    CounterStatus = [{counter, Cnt}, {counter_lease, Lease},
                     {counter_lease_size, LeaseSize}, {counter_leasing, Leasing}],
    VNodeStatus = [BackendStatus, {vnodeid, VId} | CounterStatus],
    {reply, {vnode_status, Index, VNodeStatus}, State};
handle_request(kv_delete_request, Req, _Sender, State) ->
    BKey = riak_kv_requests:get_bucket_key(Req),
    do_delete(BKey, State);
handle_request(kv_vclock_request, Req, _Sender, State) ->
    BKeys = riak_kv_requests:get_bucket_keys(Req),
    {reply, do_get_vclocks(BKeys, State), State}.


%% @doc Handle a coverage request.
%% More information about the specification for the ItemFilter
%% parameter can be found in the documentation for the
%% {@link riak_kv_coverage_filter} module.
handle_coverage(Req, FilterVNodes, Sender, State) ->
    handle_coverage_request(riak_kv_requests:request_type(Req),
                            Req,
                            FilterVNodes,
                            Sender,
                            State).

handle_coverage_request(kv_listkeys_request, Req, FilterVNodes, Sender, State) ->
    Bucket = riak_kv_requests:get_bucket(Req),
    ResultFun = get_result_fun(Req, Bucket, Sender),
    Opts = [{bucket, Bucket}],
    ItemFilter = riak_kv_requests:get_item_filter(Req),
    handle_coverage_keyfold(Bucket, ItemFilter, ResultFun,
                            FilterVNodes, Sender, Opts, State);
handle_coverage_request(kv_list_group_request, Req, FilterVNodes, Sender, State) ->
    Bucket = riak_kv_requests:get_bucket(Req),
    ResultFun = get_result_fun(Req, Bucket, Sender),
    Opts = [{bucket, Bucket}],
    GroupParams = riak_kv_requests:get_group_params(Req),
    handle_coverage_groupkeyfold(
        Bucket, GroupParams, ResultFun,
        FilterVNodes, Sender, Opts, State
    );
handle_coverage_request(kv_listbuckets_request,
                        Req,
                        _FilterVNodes,
                        Sender,
                        State=#state{async_folding=AsyncFolding,
                                     bucket_buf_size=BufferSize,
                                     mod=Mod,
                                     modstate=ModState}) ->
    ItemFilter = riak_kv_requests:get_item_filter(Req),
    %% Construct the filter function
    Filter = riak_kv_coverage_filter:build_filter(ItemFilter),
    BufferMod = riak_kv_fold_buffer,

    Buffer = riak_kv_fold_buffer:new(BufferSize, result_fun(Sender)),
    FoldFun = fold_fun(buckets, BufferMod, Filter, undefined),
    FinishFun = finish_fun(BufferMod, Sender),
    {ok, Capabilities} = Mod:capabilities(ModState),
    Opts = maybe_enable_async_fold(AsyncFolding, Capabilities, []),
    case list(FoldFun, FinishFun, Mod, fold_buckets, ModState, Opts, Buffer) of
        {async, AsyncWork} ->
            {async, {fold, AsyncWork, FinishFun}, Sender, State};
        _ ->
            {noreply, State}
    end;
handle_coverage_request(kv_index_request, Req, FilterVNodes, Sender, State) ->
    Bucket = riak_kv_requests:get_bucket(Req),
    ItemFilter = riak_kv_requests:get_item_filter(Req),
    Query = riak_kv_requests:get_query(Req),
    ResultFun = case riak_kv_requests:get_ack_backpressure(Req) of
                    true  -> result_fun_ack(Bucket, Sender);
                    false -> result_fun(Bucket, Sender)
                end,
    handle_coverage_index(Bucket, ItemFilter, Query, FilterVNodes, Sender, State, ResultFun).

get_result_fun(Req, Bucket, Sender) ->
    case riak_kv_requests:get_ack_backpressure(Req) of
        true -> result_fun_ack(Bucket, Sender);
        false -> result_fun(Bucket, Sender)
    end.

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
                      ResultFun) ->
    {ok, Capabilities} = Mod:capabilities(Bucket, ModState),
    IndexBackend = lists:member(indexes, Capabilities),
    case IndexBackend of
        true ->
            ok = riak_kv_stat:update(vnode_index_read),

            BufSize = buffer_size_for_index_query(Query, DefaultBufSz),
            Opts = [{index, Bucket, Query}, {bucket, Bucket}, {buffer_size, BufSize}],
            FoldType = fold_type_for_query(Query),
            handle_coverage_fold(FoldType, keys, Bucket, ItemFilter, ResultFun,
                                    FilterVNodes, Sender, Opts, State);
        false ->
            {reply, {error, {indexes_not_supported, Mod}}, State}
    end.

%% Convenience for handling both v3 and v4 coverage-based key fold operations
handle_coverage_keyfold(Bucket, ItemFilter, ResultFun,
                      FilterVNodes, Sender, Opts, State) ->
    handle_coverage_fold(fold_keys, keys, Bucket, ItemFilter, ResultFun,
                            FilterVNodes, Sender, Opts, State).

handle_coverage_groupkeyfold(
    Bucket, GroupParams, ResultFun,
    FilterVNodes, Sender, Opts, State
) ->
    Opts1 = [{fold_keys_type, fold_group_keys}, {group_params, GroupParams} | Opts],
    ItemFilter = none,
    FoldType = grouped_fold,
    FoldFunType = objects,
    handle_coverage_fold(
        FoldType, FoldFunType, Bucket, ItemFilter, ResultFun,
        FilterVNodes, Sender, Opts1, State).

%% Until a bit of a refactor can occur to better abstract
%% index operations, allow the ModFun for folding to be declared
%% to support index operations that can return objects
handle_coverage_fold(FoldType, FoldFunType, Bucket, ItemFilter, ResultFun,
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
    Buffer = riak_kv_fold_buffer:new(BufferSize, ResultFun),
    Extras = fold_extras_keys(Index, Bucket),
    FoldFun = fold_fun(FoldFunType, BufferMod, Filter, Extras),
    FinishFun = finish_fun(BufferMod, Sender),
    {ok, Capabilities} = Mod:capabilities(Bucket, ModState),
    Opts = maybe_enable_async_fold(AsyncFolding, Capabilities, Opts0),
    case list(FoldFun, FinishFun, Mod, FoldType, ModState, Opts, Buffer) of
        {async, AsyncWork} ->
            {async, {fold, AsyncWork, FinishFun}, Sender, State};
        _ ->
            {noreply, State}
    end.

%% While in handoff, vnodes have the option of returning {forward,
%% State}, or indeed, {forward, Req, State} which will cause riak_core
%% to forward the request to the handoff target node. For riak_kv, we
%% issue a put locally as well as forward it in case the vnode has
%% already handed off the previous version. All other requests are
%% handled locally and not forwarded since the relevant data may not
%% have yet been handed off to the target node. Since we do not
%% forward deletes it is possible that we do not clear a tombstone
%% that was already handed off.  This is benign as the tombstone will
%% eventually be re-deleted. NOTE: this makes write requests N+M where
%% M is the number of vnodes forwarding.
handle_handoff_command(Req, Sender, State) ->
    ReqType = riak_kv_requests:request_type(Req),
    handle_handoff_request(ReqType, Req, Sender, State).

handle_handoff_request(kv_put_request, Req, Sender, State) ->
    case riak_kv_requests:is_coordinated_put(Req) of
        false ->
            {noreply, NewState} = handle_command(Req, Sender, State),
            {forward, NewState};
        true ->
            %% riak_kv#1046 - don't make fake siblings. Perform the
            %% put, and create a new request to forward on, that
            %% contains the frontier, much like the value returned to
            %% a put fsm, then replicated.
            #state{idx = Idx} = State,
            ReqId = riak_kv_requests:get_request_id(Req),
            StartTS = os:timestamp(),
            riak_core_vnode:reply(Sender, {w, Idx, ReqId}),
            {Reply, UpdState} = do_put(Sender, Req, State),
            update_vnode_stats(vnode_put, Idx, StartTS),

            case Reply of
                %%  NOTE: Coord is always `returnbody` as a put arg
                {dw, Idx, NewObj, ReqId} ->
                    %% DO NOT coordinate again at the next owner!
                    NewReq1 = riak_kv_requests:remove_option(Req, coord),
                    NewReq = riak_kv_requests:set_object(NewReq1, NewObj),
                    {forward, NewReq, UpdState};
                _Error ->
                    %% Don't forward a failed attempt to put, as you
                    %% need the successful object
                    {noreply, UpdState}
            end
    end;
handle_handoff_request(kv_w1c_put_request, Request, Sender, State) ->
    NewState0 = case handle_command(Request, Sender, State) of
        {noreply, NewState} ->
            NewState;
        {reply, Reply, NewState} ->
            %% reply directly to the sender, as we will be forwarding the
            %% the request on to the handoff node.
            riak_core_vnode:reply(Sender, Reply),
            NewState
    end,
    {forward, NewState0};
handle_handoff_request(_Other, Req, Sender, State) ->
    %% @todo: this should be based on the type of the request when the
    %%        hiding of records is complete.
    handle_command(Req, Sender, State).

%% callback used by dynamic ring sizing to determine where requests should be forwarded.
request_hash(Req) ->
    do_request_hash(riak_kv_requests:request_type(Req), Req).

%% Puts/deletes are forwarded during the operation, all other requests are not
do_request_hash(RequestType, Req) when RequestType == kv_put_request orelse
                                       RequestType == kv_delete_request ->
    BKey = riak_kv_requests:get_bucket_key(Req),
    riak_core_util:chash_key(BKey);
do_request_hash(_, _) ->
    undefined.



handoff_starting({_HOType, TargetNode}=HandoffDest, State=#state{handoffs_rejected=RejectCount, update_hook=UpdateHook}) ->
    MaxRejects = app_helper:get_env(riak_kv, handoff_rejected_max, 6),
    case MaxRejects =< RejectCount orelse should_handoff(UpdateHook, HandoffDest) of
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

terminate(_Reason, #state{idx=Idx, mod=Mod, modstate=ModState,hashtrees=Trees}) ->
    Mod:stop(ModState),

    %% Explicitly stop the hashtree rather than relying on the process monitor
    %% to detect the vnode exit.  As riak_kv_index_hashtree is not a supervised
    %% process in the riak_kv application, on graceful shutdown riak_kv and
    %% riak_core can complete their shutdown before the hashtree is written
    %% to disk causing the hashtree to be closed dirty.
    riak_kv_index_hashtree:sync_stop(Trees),
    riak_kv_stat:unregister_vnode_stats(Idx),
    ok.

handle_info({{w1c_async_put, From, Type, Bucket, Key, EncodedVal, StartTS} = _Context, Reply},
            State=#state{idx=Idx, update_hook=UpdateHook}) ->
    update_hashtree(Bucket, Key, EncodedVal, State),
    update_binary(UpdateHook, Bucket, Key, EncodedVal, put, Idx),
    riak_core_vnode:reply(From, ?KV_W1C_PUT_REPLY{reply=Reply, type=Type}),
    update_vnode_stats(vnode_put, Idx, StartTS),
    {ok, State};

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
            {Result, State2} = actual_put_tracked(Key, Obj, [], false, undefined, update, State),
            Reply = case Result of
                        {dw, _Idx, _Obj, _ReqID} ->
                            Obj;
                        {dw, _Idx, _ReqID} ->
                            Obj;
                        {fail, _Idx, _ReqID} ->
                            failed
                    end,
            ((Reply =/= failed) and (HOTarget =/= undefined)) andalso raw_put({Idx, HOTarget}, Key, Obj),
            riak_kv_ensemble_backend:reply(From, Reply),
            {ok, State2};
        Fwd when is_atom(Fwd) ->
            forward_put({Idx, Fwd}, Key, Obj, From),
            {ok, State}
    end;

handle_info({raw_forward_put, Key, Obj, From}, State) ->
    {Result, State2} = actual_put_tracked(Key, Obj, [], false, undefined, update, State),
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
    {_, State2} = actual_put_tracked(Key, Obj, [], false, undefined, update, State),
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
                               do_backend_delete(BKey, RObj, delete, State#state{modstate=ModState1});
                           _ ->
                               State#state{modstate=ModState1}
                       end;
                   {{error, _}, ModState1} ->
                       State#state{modstate=ModState1}
               end,
    {ok, UpdState};
handle_info({counter_lease, {FromPid, VnodeId, NewLease}}, State=#state{status_mgr_pid=FromPid, vnodeid=VnodeId}) ->
    #state{counter=CounterState} = State,
    CS1 = CounterState#counter_state{lease=NewLease, leasing=false},
    State2 = State#state{counter=CS1},
    {ok, State2};
handle_info({counter_lease, {FromPid, NewVnodeId, NewLease}}, State=#state{status_mgr_pid=FromPid, idx=Idx}) ->
    %% Lease rolled over MAX_INT (new vnodeid signifies this) so reset counter
    #state{counter=CounterState} = State,
    CS1 = CounterState#counter_state{lease=NewLease, leasing=false, cnt=1},
    State2 = State#state{vnodeid=NewVnodeId, counter=CS1},
    lager:info("New Vnode id for ~p. Epoch counter rolled over.", [Idx]),
    {ok, State2}.

handle_exit(Pid, Reason, State=#state{status_mgr_pid=Pid, idx=Index, counter=CntrState}) ->
    lager:error("Vnode status manager exit ~p", [Reason]),
    %% The status manager died, start a new one
    #counter_state{lease_size=LeaseSize, leasing=Leasing, use=UseEpochCounter} = CntrState,
    {ok, NewPid} = riak_kv_vnode_status_mgr:start_link(self(), Index, UseEpochCounter),

    if Leasing ->
            %% Crashed when getting a lease, try again pathalogical
            %% bad case here is that the lease gets to disk and the
            %% manager crashes, meaning an ever growing lease/new ids
            ok = riak_kv_vnode_status_mgr:lease_counter(NewPid, LeaseSize);
       ?ELSE ->
            ok
    end,
    {noreply, State#state{status_mgr_pid=NewPid}};
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
do_put(Sender, Request, State) ->
    BKey = riak_kv_requests:get_bucket_key(Request),
    Object = riak_kv_requests:get_object(Request),
    RequestId = riak_kv_requests:get_request_id(Request),
    StartTime = riak_kv_requests:get_start_time(Request),
    Options = riak_kv_requests:get_options(Request),
    do_put(Sender, BKey, Object, RequestId, StartTime, Options, State).

%% @private
%% upon receipt of a client-initiated put
do_put(Sender, {Bucket,_Key}=BKey, RObj, ReqID, StartTime, Options, State) ->
    BProps =  case proplists:get_value(bucket_props, Options) of
                  undefined ->
                      riak_core_bucket:get_bucket(Bucket);
                  Props ->
                      Props
              end,
    ReadRepair = proplists:get_value(rr, Options, false),
    PruneTime = case ReadRepair of
                    true ->
                        undefined;
                    false ->
                        StartTime
                end,
    Coord = proplists:get_value(coord, Options, false),
    CRDTOp = proplists:get_value(counter_op, Options, proplists:get_value(crdt_op, Options, undefined)),
    HashOps = proplists:get_value(hashtree_action, Options, update),
    PutArgs = #putargs{returnbody=proplists:get_value(returnbody,Options,false) orelse Coord,
                       coord=Coord,
                       lww=proplists:get_value(last_write_wins, BProps, false),
                       bkey=BKey,
                       robj=RObj,
                       reqid=ReqID,
                       bprops=BProps,
                       starttime=StartTime,
                       readrepair = ReadRepair,
                       prunetime=PruneTime,
                       crdt_op = CRDTOp,
                       hash_ops = HashOps},
    {PrepPutRes, UpdPutArgs, State2} = prepare_put(State, PutArgs),
    {Reply, UpdState} = perform_put(PrepPutRes, State2, UpdPutArgs),
    riak_core_vnode:reply(Sender, Reply),

    update_index_write_stats(UpdPutArgs#putargs.is_index, UpdPutArgs#putargs.index_specs),
    {Reply, UpdState}.

-spec do_backend_delete(
    {riak_core_bucket:bucket(), riak_object:key()},
    riak_object:riak_object(), hashtree_action(), #state{}
) -> #state{}.
do_backend_delete(BKey, RObj, HashtreeAction,  
    State = #state{
        idx = Idx,
        mod = Mod,
        modstate = ModState,
        update_hook = UpdateHook
}) ->
    %% object is a tombstone or all siblings are tombstones
    %% Calculate the index specs to remove...
    %% JDM: This should just be a tombstone by this point, but better
    %% safe than sorry.
    IndexSpecs = riak_object:diff_index_specs(undefined, RObj),
    %% Do the delete...
    {Bucket, Key} = BKey,
    case Mod:delete(Bucket, Key, IndexSpecs, ModState) of
        {ok, UpdModState} ->
            update(UpdateHook, {RObj, no_old_object}, delete, Idx),
            hashtree_action(Bucket, Key, RObj, HashtreeAction, State),
            maybe_cache_evict(BKey, State),
            update_index_delete_stats(IndexSpecs),
            State#state{modstate = UpdModState};
        {error, _Reason, UpdModState} ->
            State#state{modstate = UpdModState}
    end.

hashtree_action(Bucket, Key, RObj, HashtreeAction, State) ->
    case HashtreeAction of
        delete ->
            delete_from_hashtree(Bucket, Key, State);
        tombstone ->
            update_hashtree(Bucket, Key, tombstone, State);
        update ->
            update_hashtree(Bucket, Key, RObj, State)
    end.

%% Compute a hash of the deleted object
delete_hash(RObj) ->
    erlang:phash2(RObj, 4294967296).

prepare_put(State=#state{vnodeid=VId,
                         mod=Mod,
                         modstate=ModState,
                         update_hook=UpdateHook},
            PutArgs=#putargs{bkey={Bucket, _Key},
                             lww=LWW,
                             coord=Coord,
                             robj=RObj,
                             starttime=StartTime,
                             bprops = BProps}) ->
    %% Can we avoid reading the existing object? If this is not an
    %% index backend, and the bucket is set to last-write-wins, then
    %% no need to incur additional get. Otherwise, we need to read the
    %% old object to know how the indexes have changed.
    IndexBackend = is_indexed_backend(Mod, Bucket, ModState),
    IsSearchable = requires_existing_object(UpdateHook, BProps),
    SkipReadBeforeWrite = LWW andalso (not IndexBackend) andalso (not IsSearchable),
    case SkipReadBeforeWrite of
        true ->
            prepare_blind_put(Coord, RObj, VId, StartTime, PutArgs, State);
        false ->
            prepare_read_before_write_put(State, PutArgs, IndexBackend, IsSearchable)
    end.

is_indexed_backend(Mod, Bucket, ModState) ->
    {ok, Capabilities} = Mod:capabilities(Bucket, ModState),
    IndexBackend = lists:member(indexes, Capabilities),
    IndexBackend.

prepare_blind_put(Coord, RObj, VId, StartTime, PutArgs, State) ->
    ObjToStore = case Coord of
        true ->
            %% Do we need to use epochs here? I guess we
            %% don't care, and since we don't read, we
            %% can't.
            riak_object:increment_vclock(RObj, VId, StartTime);
        false ->
            RObj
    end,
    {{true, {ObjToStore, no_old_object}}, PutArgs#putargs{is_index = false}, State}.

prepare_read_before_write_put(#state{mod = Mod,
                                     modstate = ModState,
                                     md_cache = MDCache}=State,
                              #putargs{bkey={Bucket, Key}=BKey,
                                       robj=RObj}=PutArgs,
                              IndexBackend, IsSearchable) ->
    {CacheClock, CacheData} = maybe_check_md_cache(MDCache, BKey),

    RequiresGet = determine_requires_get(CacheClock, RObj, IsSearchable),
    GetReply = get_old_object_or_fake(RequiresGet, Bucket, Key, Mod, ModState, CacheClock),
    case GetReply of
        not_found ->
            prepare_put_new_object(State, PutArgs, IndexBackend);
        {ok, OldObj} ->
            prepare_put_existing_object(State, PutArgs, OldObj, IndexBackend, CacheData, RequiresGet)
    end.

prepare_put_existing_object(#state{idx =Idx} = State,
                    #putargs{coord=Coord,
                             robj = RObj,
                             lww=LWW,
                             starttime = StartTime,
                             bprops = BProps,
                             prunetime=PruneTime,
                             crdt_op = CRDTOp}=PutArgs,
                            OldObj, IndexBackend, CacheData, RequiresGet) ->
    {ActorId, State2} = maybe_new_key_epoch(Coord, State, OldObj, RObj),
    case put_merge(Coord, LWW, OldObj, RObj, ActorId, StartTime) of
        {oldobj, OldObj} ->
            {{false, {OldObj, no_old_object}}, PutArgs, State2};
        {newobj, NewObj} ->
            AMObj = enforce_allow_mult(NewObj, BProps),
            IndexSpecs = get_index_specs(IndexBackend, CacheData, RequiresGet, AMObj, OldObj),
            ObjToStore0 = maybe_prune_vclock(PruneTime, AMObj, BProps),
            ObjectToStore = maybe_do_crdt_update(Coord, CRDTOp, ActorId, ObjToStore0),
            determine_put_result(ObjectToStore, OldObj, Idx, PutArgs, State2, IndexSpecs, IndexBackend)
    end.

determine_put_result({error, E}, _, Idx, PutArgs, State, _IndexSpecs, _IndexBackend) ->
    {{fail, Idx, E}, PutArgs, State};
determine_put_result(ObjToStore, OldObj, _Idx, PutArgs, State, IndexSpecs, IndexBackend) ->
    {{true, {ObjToStore, OldObj}},
     PutArgs#putargs{index_specs = IndexSpecs,
                     is_index    = IndexBackend}, State}.

maybe_prune_vclock(_PruneTime=undefined, RObj, _BProps) ->
    RObj;
maybe_prune_vclock(PruneTime, RObj, BProps) ->
    riak_object:prune_vclock(RObj, PruneTime, BProps).

get_index_specs(_IndexedBackend=true, CacheData, RequiresGet, NewObj, OldObj) ->
    case CacheData /= undefined andalso
         RequiresGet == false of
        true ->
            NewData = riak_object:index_data(NewObj),
            riak_object:diff_index_data(NewData,
                                        CacheData);
        false ->
            riak_object:diff_index_specs(NewObj,
                                         OldObj)
    end;

get_index_specs(_IndexedBackend=false, _CacheData, _RequiresGet, _NewObj, _OldObj) ->
    [].

prepare_put_new_object(#state{idx =Idx} = State,
               #putargs{robj = RObj,
                        coord=Coord,
                        starttime=StartTime,
                        crdt_op=CRDTOp} = PutArgs,
                       IndexBackend) ->
    IndexSpecs = case IndexBackend of
                     true ->
                         riak_object:index_specs(RObj);
                     false ->
                         []
                 end,
    {EpochId, State2} = new_key_epoch(State),
    RObj2 = maybe_update_vclock(Coord, RObj, EpochId, StartTime),
    RObj3 = maybe_do_crdt_update(Coord, CRDTOp, EpochId, RObj2),
    determine_put_result(RObj3, no_old_object, Idx, PutArgs, State2, IndexSpecs, IndexBackend).

get_old_object_or_fake(true, Bucket, Key, Mod, ModState, _CacheClock) ->
    case do_get_object(Bucket, Key, Mod, ModState) of
        {error, not_found, _UpdModState} ->
            not_found;
        {ok, TheOldObj, _UpdModState} ->
            {ok, TheOldObj}
    end;
get_old_object_or_fake(false, Bucket, Key, _Mod, _ModState, CacheClock) ->
    FakeObj0 = riak_object:new(Bucket, Key, <<>>),
    FakeObj = riak_object:set_vclock(FakeObj0, CacheClock),
    {ok, FakeObj}.

determine_requires_get(CacheClock, RObj, IsSearchable) ->
    RequiresGet =
    case CacheClock of
        undefined ->
            true;
        Clock ->
            %% We need to perform a local get, to merge contents,
            %% if the local object has events unseen by the
            %% incoming object. If the incoming object descends
            %% the cache (i.e. has seen all its events) no need to
            %% do a local get and merge, just overwrite.
            not riak_object:vclock_descends(RObj, Clock) orelse IsSearchable
    end,
    RequiresGet.

%% @Doc in the case that this a co-ordinating put, prepare the object.
%% NOTE the `VId' is a new epoch actor for this object
maybe_update_vclock(_Coord=true, RObj, VId, StartTime) ->
    riak_object:increment_vclock(RObj, VId, StartTime);
maybe_update_vclock(_Coord=false, RObj, _VId, _StartTime) ->
    %% @TODO Not coordindating, not found local, is there an entry for
    %% us in the clock? If so, mark as dirty
    RObj.

maybe_do_crdt_update(_Coord = _, undefined, _VId, RObj) ->
    RObj;
maybe_do_crdt_update(_Coord = true, CRDTOp, VId, RObj) ->
    do_crdt_update(RObj, VId, CRDTOp);
maybe_do_crdt_update(_Coord = false, _CRDTOp, _Vid, RObj) ->
    RObj.

do_crdt_update(RObj, VId, CRDTOp) ->
    {Time, Value} = timer:tc(riak_kv_crdt, update, [RObj, VId, CRDTOp]),
    ok = riak_kv_stat:update({vnode_dt_update, get_crdt_mod(CRDTOp), Time}),
    Value.

get_crdt_mod(Int) when is_integer(Int) -> ?COUNTER_TYPE;
get_crdt_mod(#crdt_op{mod=Mod}) -> Mod;
get_crdt_mod(Atom) when is_atom(Atom) -> Atom.

perform_put({fail, _, _}=Reply, State, _PutArgs) ->
    {Reply, State};
perform_put({false, {Obj, _OldObj}},
            #state{idx=Idx}=State,
            #putargs{returnbody=true,
                     reqid=ReqID}) ->
    {{dw, Idx, Obj, ReqID}, State};
perform_put({false, {_Obj, _OldObj}},
            #state{idx=Idx}=State,
            #putargs{returnbody=false,
                     reqid=ReqId}) ->
    {{dw, Idx, ReqId}, State};
perform_put({true, {_Obj, _OldObj}=Objects},
            State,
            #putargs{returnbody=RB,
                     bkey=BKey,
                     reqid=ReqID,
                     index_specs=IndexSpecs,
                     readrepair=ReadRepair,
                     hash_ops=HashtreeAction}) ->
    case ReadRepair of
      true ->
        MaxCheckFlag = no_max_check;
      false ->
        MaxCheckFlag = do_max_check
    end,
    actual_put(BKey, Objects, IndexSpecs, RB, ReqID, MaxCheckFlag, HashtreeAction, State).

actual_put(BKey, {Obj, OldObj}, IndexSpecs, RB, ReqID, HashtreeAction, State) ->
    actual_put(BKey, {Obj, OldObj}, IndexSpecs, RB, ReqID, do_max_check, HashtreeAction, State).

actual_put(BKey={Bucket, Key}, {Obj, OldObj}, IndexSpecs, RB, ReqID, MaxCheckFlag, HashtreeAction,
           State=#state{idx=Idx,
                        mod=Mod,
                        modstate=ModState,
                        update_hook=UpdateHook}) ->
    case encode_and_put(Obj, Mod, Bucket, Key, IndexSpecs, ModState,
                       MaxCheckFlag) of
        {{ok, UpdModState}, EncodedVal} ->
            hashtree_action(Bucket, Key, EncodedVal, HashtreeAction, State),
            maybe_cache_object(BKey, Obj, State),
            update(UpdateHook, {Obj, OldObj}, put, Idx),
            Reply = case RB of
                true ->
                    {dw, Idx, Obj, ReqID};
                false ->
                    {dw, Idx, ReqID}
            end;
        {{error, Reason, UpdModState}, _EncodedVal} ->
            Reply = {fail, Idx, Reason}
    end,
    {Reply, State#state{modstate=UpdModState}}.

actual_put_tracked(BKey, {_NewObj, _OldObj} = Objs, IndexSpecs, RB, ReqId, HashtreeAction, State) ->
    StartTS = os:timestamp(),
    Result = actual_put(BKey, Objs, IndexSpecs, RB, ReqId, HashtreeAction, State),
    update_vnode_stats(vnode_put, State#state.idx, StartTS),
    Result;
actual_put_tracked(BKey, Obj, IndexSpecs, RB, ReqId, HashtreeAction, State) ->
    actual_put_tracked(BKey, {Obj, no_old_object}, IndexSpecs, RB, ReqId, HashtreeAction, State).

do_reformat({Bucket, Key}=BKey, State=#state{mod=Mod, modstate=ModState}) ->
    case do_get_object(Bucket, Key, Mod, ModState) of
        {error, not_found, _UpdModState} ->
            Reply = {error, not_found},
            UpdState = State;
        {ok, RObj, _UpdModState} ->
            %% since it is assumed capabilities have been properly set
            %% to the desired version, to reformat, all we need to do
            %% is submit a new write
            PutArgs = #putargs{hash_ops = update,
                               returnbody=false,
                               bkey=BKey,
                               reqid=undefined,
                               index_specs=[]},
            case perform_put({true, {RObj, no_old_object}}, State, PutArgs) of
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
    %% @TODO Do we need to mark the clock dirty here? I think so
    %% @TODO Check the clock of the incoming object, if it is more advanced
    %% for our actor than we are then something is amiss, and we need
    %% to mark the actor as dirty for this key
    {newobj, UpdObj};
put_merge(false, false, CurObj, UpdObj, _VId, _StartTime) -> % coord=false, LWW=false
    %% a downstream merge, or replication of a coordinated PUT
    %% Merge the value received with local replica value
    %% and store the value IFF it is different to what we already have
    %%
    %% @TODO Check the clock of the incoming object, if it is more advanced
    %% for our actor than we are then something is amiss, and we need
    %% to mark the actor as dirty for this key
    ResObj = riak_object:syntactic_merge(CurObj, UpdObj),
    case riak_object:equal(ResObj, CurObj) of
        true ->
            {oldobj, CurObj};
        false ->
            {newobj, ResObj}
    end;
put_merge(true, LWW, CurObj, UpdObj, VId, StartTime) ->
    %% @TODO If the current object has a dirty clock, we need to start
    %% a new per key epoch and mark clock as clean.
    {newobj, riak_object:update(LWW, CurObj, UpdObj, VId, StartTime)}.

%% @private
do_get(_Sender, BKey, ReqID,
       State=#state{idx=Idx, mod=Mod, modstate=ModState}) ->
    StartTS = os:timestamp(),
    {Retval, ModState1} = do_get_term(BKey, Mod, ModState),
    State1 = State#state{modstate=ModState1},
    {Retval1, State3} =
        case Retval of
            {ok, Obj} ->
                case riak_kv_util:is_x_expired(Obj) of
                    true ->
                         State2 = do_backend_delete(BKey, Obj, tombstone, State1),
                        {{error, notfound}, State2};
                    _ ->
                        maybe_cache_object(BKey, Obj, State1),
                        {Retval, State1}
                end;
            _ ->
                {Retval, State1}
        end,
    update_vnode_stats(vnode_get, Idx, StartTS),
    {reply, {r, Retval1, Idx, ReqID}, State3}.

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
    end;
fold_fun(objects, BufferMod, _Filter, undefined) ->
    fun(Bucket, Key, Item, Buffer) ->
        BufferMod:add({{Bucket, Key}, Item}, Buffer)
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
                            %% keep tombstones indefinitely or
                            %% until the reaper sweep remove it
                            {reply, {fail, Idx, del_mode_keep},
                             State#state{modstate=UpdModState}};
                        immediate ->
                            UpdState = do_backend_delete(BKey, RObj, delete,
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
do_fold(FoldFun, Acc0, Sender, ReqOpts, State=#state{mod=Mod, modstate=ModState}) ->
    Fun = convert_fun(FoldFun),
    Opts = get_fold_opts(ReqOpts, State),
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
get_fold_opts(ReqOpts, #state{async_folding=AsyncFolding,
                           mod=Mod,
                           modstate=ModState}) ->
    {ok, Capabilities} = Mod:capabilities(ModState),
    Opts0 = maybe_enable_async_fold(AsyncFolding, Capabilities, ReqOpts),
    maybe_enable_iterator_refresh(Capabilities, Opts0).

%% The function in riak_core used for object folding expects the
%% bucket and key pair to be passed as the first parameter, but in
%% riak_kv the bucket and key have been separated. This function
%% wrapper is to address this mismatch.
convert_fun(FoldFun) ->
    fun(Bucket, Key, Value, Acc) ->
            FoldFun({Bucket, Key}, Value, Acc)
    end.


do_sweep([], _EstimatedKeys, _Sender, State=#state{idx=Index}) ->
    lager:info("No participants in sweep ~p", [Index]),
    {reply, no_participant, State};

do_sweep(ActiveParticipants, EstimatedKeys, Sender, State) ->
    Opts = get_fold_opts([sweep_fold, {iterator_refresh, true}], State),
    do_sweep(ActiveParticipants, EstimatedKeys, Sender, Opts, State).

do_sweep(ActiveParticipants, EstimatedKeys, Sender, Opts, State=#state{idx=Index, mod=Mod, modstate=ModState}) ->
    riak_kv_sweeper_fold:do_sweep(ActiveParticipants, EstimatedKeys, Sender, Opts, Index, Mod, ModState, State).

-spec maybe_enable_async_fold(boolean(), list(), list()) -> list().
maybe_enable_async_fold(AsyncFolding, Capabilities, Opts) ->
    AsyncBackend = lists:member(async_fold, Capabilities),
    options_for_folding_and_backend(Opts, AsyncFolding andalso AsyncBackend).

-spec options_for_folding_and_backend(list(), UseAsyncFolding :: boolean()) -> list().
options_for_folding_and_backend(Opts, true) ->
    [async_fold | Opts];
options_for_folding_and_backend(Opts, false) ->
    Opts.

fold_type_for_query(Query) ->
    %% @HACK
    %% Really this should be decided in the backend
    %% if there was a index_query fun.
    case riak_index:return_body(Query) of
        true -> fold_objects;
        false -> fold_keys
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
                                idx=Idx,
                                update_hook=UpdateHook}) ->
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
                    update(UpdateHook, {DiffObj, no_old_object}, handoff, Idx),
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
                            update(UpdateHook, {AMObj, OldObj}, handoff, Idx),
                            InnerRes;
                        {InnerRes, _EncodedVal} ->
                            InnerRes
                    end
            end
    end.

-spec update_hashtree(binary(), binary(),
                      riak_object:riak_object() | binary() | tombstone,
                      state()) -> ok.
update_hashtree(_Bucket, _Key, _RObj, #state{hashtrees=undefined}) ->
    ok;
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
%% and store a new leased counter if needed. NOTE: this is different
%% to the previous function, as it will now _always_ store a new
%% status to disk as it will grab a new lease.
%%
%%  @TODO document the need for, and invariants of the counter
-spec get_vnodeid_and_counter(pid(), non_neg_integer(), boolean()) ->
                                     {ok, {vnodeid(), #counter_state{}}} |
                                     {error, Reason::term()}.
get_vnodeid_and_counter(StatusMgr, CounterLeaseSize, UseEpochCounter) ->
    {ok, {VId, Counter, Lease}} = riak_kv_vnode_status_mgr:get_vnodeid_and_counter(StatusMgr, CounterLeaseSize),
    {ok, {VId, #counter_state{cnt=Counter, lease=Lease, lease_size=CounterLeaseSize, use=UseEpochCounter}}}.

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
    ok = riak_kv_stat:update({Op, Idx, timer:now_diff( os:timestamp(), StartTS)}).

%% @private
update_index_write_stats(false, _IndexSpecs) ->
    ok;
update_index_write_stats(true, IndexSpecs) ->
    {Added, Removed} = count_index_specs(IndexSpecs),
    ok = riak_kv_stat:update({vnode_index_write, Added, Removed}).

%% @private
update_index_delete_stats(IndexSpecs) ->
    {_Added, Removed} = count_index_specs(IndexSpecs),
    ok = riak_kv_stat:update({vnode_index_delete, Removed}).

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

%% @private increment the per vnode coordinating put counter,
%% flushing/leasing if needed
-spec update_counter(state()) -> state().
update_counter(State=#state{counter=CounterState}) ->
    #counter_state{cnt=Counter0} = CounterState,
    Counter = Counter0 +  1,
    maybe_lease_counter(State#state{counter=CounterState#counter_state{cnt=Counter}}).


%% @private we can never use a counter that is greater or equal to the
%% one fsynced to disk. If the incremented counter is == to the
%% current Lease, we must block until the new Lease is stored. If the
%% current counter is 80% through the current lease, and we have not
%% asked for a new lease, ask for one.  If we're less than 80% through
%% the lease, do nothing.  If not yet blocking(equal) and we already
%% asked for a new lease, do nothing.
maybe_lease_counter(#state{vnodeid=VId, counter=#counter_state{cnt=Cnt, lease=Lease}})
  when Cnt > Lease ->
    %% Holy broken invariant. Log and crash.
    lager:error("Broken invariant, epoch counter ~p greater than lease ~p for vnode ~p. Crashing.",
                [Cnt, Lease, VId]),
    exit(epoch_counter_invariant_broken);
maybe_lease_counter(State=#state{counter=#counter_state{cnt=Lease, lease=Lease,
                                                        leasing=true}}) ->
    %% Block until we get a new lease, or crash the vnode
    {ok, NewState} = blocking_lease_counter(State),
    NewState;
maybe_lease_counter(State=#state{counter=#counter_state{leasing=true}}) ->
    %% not yet at the blocking stage, waiting on a lease
    State;
maybe_lease_counter(State) ->
    #state{status_mgr_pid=MgrPid, counter=CS=#counter_state{cnt=Cnt, lease=Lease,
                                                            lease_size=LeaseSize}} = State,
    %% @TODO (rdb) configurable??
    %% has more than 80% of the lease been used?
    CS2 = if (Lease - Cnt) =< 0.2 * LeaseSize  ->
                  ok = riak_kv_vnode_status_mgr:lease_counter(MgrPid, LeaseSize),
                  CS#counter_state{leasing=true};
             ?ELSE ->
                  CS
          end,
    State#state{counter=CS2}.

%% @private by now, we have to be waiting for a lease, or an exit,
%% from the mgr. Block until we receive a lease. If we get an exit,
%% retry a number of times. If we get neither in a reasonable time,
%% return an error.
-spec blocking_lease_counter(#state{}) ->
                                    {ok, #state{}} |
                                    counter_lease_error().
blocking_lease_counter(State) ->
    {MaxErrs, MaxTime} = get_counter_wait_values(),
    blocking_lease_counter(State, {0, MaxErrs, MaxTime}).

-spec blocking_lease_counter(#state{}, {Errors :: non_neg_integer(),
                                        MaxErrors :: non_neg_integer(),
                                        TimeRemainingMillis :: non_neg_integer()}
                            ) ->
                                    {ok, #state{}} |
                                    counter_lease_error().
blocking_lease_counter(_State, {MaxErrs, MaxErrs, _MaxTime}) ->
    {error, counter_lease_max_errors};
blocking_lease_counter(State, {ErrCnt, MaxErrors, MaxTime}) ->
    #state{idx=Index, vnodeid=VId, status_mgr_pid=Pid, counter=CounterState} = State,
    #counter_state{lease_size=LeaseSize, use=UseEpochCounter} = CounterState,
    Start = os:timestamp(),
    receive
        {'EXIT', Pid, Reason} ->
            lager:error("Failed to lease counter for ~p : ~p", [Index, Reason]),
            {ok, NewPid} = riak_kv_vnode_status_mgr:start_link(self(), Index, UseEpochCounter),
            ok = riak_kv_vnode_status_mgr:lease_counter(NewPid, LeaseSize),
            NewState = State#state{status_mgr_pid=NewPid},
            Elapsed = timer:now_diff(os:timestamp(), Start),
            blocking_lease_counter(NewState, {ErrCnt+1, MaxErrors, MaxTime - Elapsed});
        {counter_lease, {Pid, VId, NewLease}} ->
            NewCS = CounterState#counter_state{lease=NewLease, leasing=false},
            {ok, State#state{counter=NewCS}};
        {counter_lease, {Pid, NewVId, NewLease}} ->
            lager:info("New Vnode id for ~p. Epoch counter rolled over.", [Index]),
            NewCS = CounterState#counter_state{lease=NewLease, leasing=false, cnt=1},
            {ok, State#state{vnodeid=NewVId, counter=NewCS}}
    after
        MaxTime ->
            {error, counter_lease_timeout}
    end.

%% @private get the configured values for blocking waiting on
%% lease/ID. Ensure that non invalid values come in.
-spec get_counter_wait_values() ->
                                     {MaxErrors :: non_neg_integer(),
                                      MaxTime :: non_neg_integer()}.
get_counter_wait_values() ->
    MaxErrors = non_neg_env(riak_kv, counter_lease_errors, ?DEFAULT_CNTR_LEASE_ERRS),
    MaxTime = non_neg_env(riak_kv, counter_lease_timeout, ?DEFAULT_CNTR_LEASE_TO),
    {MaxErrors, MaxTime}.

%% @private we don't want to crash riak because of a dodgy config
%% value, and by this point, cuttlefish be praised, we should have
%% sane values. However, if not, ignore negative values for timeout
%% and max errors, use the defaults, and log the insanity. Note this
%% expects the macro configured defaults to be positive integers!
-spec non_neg_env(atom(), atom(), pos_integer()) -> pos_integer().
non_neg_env(App, EnvVar, Default) when is_integer(Default),
                                       Default > 0 ->
    case app_helper:get_env(App, EnvVar, Default) of
        N when is_integer(N),
               N > 0 ->
            N;
        X ->
            lager:warning("Non-integer/Negative integer ~p for vnode counter config ~p."
                          " Using default ~p",
                          [X, EnvVar, Default]),
            Default
    end.

%% @private to keep put_merge/6 side effect free (as it is exported
%% for testing) introspect the state and local/incoming objects and
%% return the actor ID for a put, and possibly updated state. NOTE
%% side effects, in that the vnode status on disk can change.
%% Why might we need a new key epoch?
%% 1. local not found (handled in prepare_put/3
%% 2. local found, but never acted on this key before (or don't remember it!)
%% 3. local found, local acted, but incoming has greater count or actor epoch
%%    This one is tricky, since it indicates some byzantine failure somewhere.
-spec maybe_new_key_epoch(boolean(), #state{},
                          riak_object:riak_object(),
                          riak_object:riak_object()) ->
                                 {binary(), #state{}}.
maybe_new_key_epoch(false, State, _, _) ->
    %% Never add a new key epoch when not coordinating
    %% @TODO (rdb) need to mark actor as dirty though.
    {State#state.vnodeid, State};
maybe_new_key_epoch(true, State=#state{counter=#counter_state{use=false}, vnodeid=VId}, _, _) ->
    %% Per-Key-Epochs is off, use the base vnodeid
    {VId, State};
maybe_new_key_epoch(true, State, LocalObj, IncomingObj) ->
    #state{vnodeid=VId} = State,
    %% @TODO (rdb) maybe optimise since highly likey both objects
    %% share the majority of actors, maybe a single umerged list of
    %% actors, somehow tagged by local | incoming?
    case highest_actor(VId, LocalObj) of
        {undefined, 0, 0} -> %% Not present locally
            %% Never acted on this object before, new epoch.
            new_key_epoch(State);
        {LocalId, LocalEpoch, LocalCntr} -> %% Present locally
            case highest_actor(VId, IncomingObj) of
                {_InId, InEpoch, InCntr} when InEpoch > LocalEpoch;
                                              InCntr > LocalCntr ->
                    %% In coming actor-epoch or counter greater than
                    %% local, some byzantine failure, new epoch.
                    B = riak_object:bucket(LocalObj),
                    K = riak_object:key(LocalObj),

                    lager:warning("Inbound clock entry for ~p in ~p/~p greater than local." ++
                                      "Epochs: {In:~p Local:~p}. Counters: {In:~p Local:~p}.",
                                  [VId, B, K, InEpoch, LocalEpoch, InCntr, LocalCntr]),
                    new_key_epoch(State);
                _ ->
                    %% just use local id
                    %% Return the highest local epoch ID for this
                    %% key. This may be the pre-epoch ID (i.e. no
                    %% epoch), which is good, no reason to force a new
                    %% epoch on all old keys.
                    {LocalId, State}
            end
    end.

%% @private generate an epoch actor, and update the vnode state.
-spec new_key_epoch(#state{}) -> {EpochActor :: binary(), #state{}}.
new_key_epoch(State=#state{vnodeid=VId, counter=#counter_state{use=false}}) ->
    {VId, State};
new_key_epoch(State) ->
    NewState=#state{counter=#counter_state{cnt=Cntr}, vnodeid=VId} = update_counter(State),
    EpochId = key_epoch_actor(VId, Cntr),
    {EpochId, NewState}.

%% @private generate a new epoch ID for a key
-spec key_epoch_actor(vnodeid(), pos_integer()) -> binary().
key_epoch_actor(ActorBin, Cntr) ->
    <<ActorBin/binary, Cntr:32/integer>>.

%% @private highest actor is the latest/greatest epoch actor for a
%% key. It is the actor we want to increment for the current event,
%% given that we are not starting a new epoch for the key.  Must work
%% with non-epochal and epochal actors.  The return tuple is
%% `{ActorId, Epoch, Counter}' where `ActorId' is the highest ID
%% starting with `ActorBase' that has acted on this key, undefined if
%% never acted before. `KeyEpoch' is the highest epoch for the
%% `ActorBase'. `Counter' is the greatest event seen by the `VnodeId'.
-spec highest_actor(binary(), riak_object:riak_object()) ->
    {ActorId :: binary() | undefined,
     KeyEpoch :: non_neg_integer(),
     Counter :: non_neg_integer()}.
highest_actor(ActorBase, Obj) ->
    ActorSize = size(ActorBase),
    Actors = riak_object:all_actors(Obj),

    {Actor, Epoch} = lists:foldl(fun(Actor, {HighestActor, HighestEpoch}) ->
                                         case Actor of
                                             <<ActorBase:ActorSize/binary, Epoch:32/integer>>
                                               when Epoch > HighestEpoch ->
                                                 {Actor, Epoch};
                                             %% Since an actor without
                                             %% an epoch is lower than
                                             %% an actor with one,
                                             %% this means in the
                                             %% unmatched case, `undefined'
                                             %% through as the highest
                                             %% actor, and the epoch
                                             %% (of zero) passes
                                             %% through too.
                                             _ ->  {HighestActor, HighestEpoch}
                                         end
                                 end,
                                 {undefined, 0},
                                 Actors),
    %% get the greatest event for the highest/latest actor
    {Actor, Epoch, riak_object:actor_counter(Actor, Obj)}.

%%
%% Maintenance note:  The riak_kv.update_hook configuration parameter should
%% contain a module name which must implement riak_kv_update_hook behavior.
%% Currently, this behavior is completely internal and is not intended for
%% use outside of Riak; Yokozuna is the only repository that currently
%% implements this behavior.  (C.f., Yokozuna cuttlefish schema, to see
%% where this configuration is set.)  In the future, we may want
%% make the riak_kv.update_hook configuration parameter a list of hooks, and
%% call a sequence of hooks on updates, but since we currently only have one
%% use-case, and since this is a purely internal API (and local to the riak node),
%% we can safely make that change in the future without an impact on
%% backwards compatibility.
%%

-spec update_hook()-> update_hook().
update_hook() ->
    app_helper:get_env(riak_kv, update_hook, riak_kv_noop_update_hook).

-spec update(
    update_hook(),
    riak_kv_update_hook:object_pair(),
    riak_kv_update_hook:update_reason(),
    riak_kv_update_hook:partition()
)           ->
    ok.
update(UpdateHook, RObjPair, Reason, Idx) ->
    UpdateHook:update(RObjPair, Reason, Idx).

-spec update_binary(
    update_hook(),
    riak_core_bucket:bucket(),
    riak_object:key(),
    binary(),
    riak_kv_update_hook:update_reason(),
    riak_kv_update_hook:partition()
)                  -> ok.
update_binary(UpdateHook, Bucket, Key, Binary, Reason, Idx) ->
    UpdateHook:update_binary(Bucket, Key, Binary, Reason, Idx).

-spec requires_existing_object(
    update_hook(),
    riak_kv_bucket:props()
)                             ->
    boolean().
requires_existing_object(UpdateHook, BProps) ->
    UpdateHook:requires_existing_object(BProps).

-spec should_handoff(
    update_hook(),
    riak_kv_update_hook:handoff_dest()
)                   ->
    boolean().
should_handoff(UpdateHook, HandoffDest) ->
    UpdateHook:should_handoff(HandoffDest).

-ifdef(TEST).

-define(MGR, riak_kv_vnode_status_mgr).
-define(MAX_INT, 4294967295).
-define(DATA_DIR, "riak_kv_vnode_blocking_test").

blocking_setup() ->
    application:set_env(riak_core, platform_data_dir, ?DATA_DIR),
    (catch file:delete(?DATA_DIR ++ "/kv_vnode/0")).

blocking_teardown() ->
    application:unset_env(riak_core, platform_data_dir),
    (catch file:delete(?DATA_DIR ++ "/kv_vnode/0")).

%% @private test the vnode and vnode mgr interaction NOTE: sets up and
%% tearsdown inside the test, the mgr needs the pid of the test
%% process to send messages. @TODO(rdb) find a better way
blocking_test_() ->
    {setup, fun() -> blocking_setup() end,
     fun(_) -> blocking_teardown() end,
     {spawn, [{"Blocking",
               fun() ->
                       {ok, Pid} = ?MGR:start_link(self(), 0, true),
                       {ok, {VId, CounterState}} = get_vnodeid_and_counter(Pid, 100, true),
                       #counter_state{cnt=Cnt, lease=Leased, lease_size=LS, leasing=L} = CounterState,
                       ?assertEqual(0, Cnt),
                       ?assertEqual(100, Leased),
                       ?assertEqual(100, LS),
                       ?assertEqual(false, L),
                       State = #state{vnodeid=VId, status_mgr_pid=Pid, counter=CounterState},
                       S2=#state{counter=#counter_state{leasing=L2, cnt=C2}} = update_counter(State),
                       ?assertEqual(false, L2),
                       ?assertEqual(1, C2),
                       S3 = lists:foldl(fun(_, S) ->
                                                update_counter(S)
                                        end,
                                        S2,
                                        lists:seq(1, 98)),
                       #state{counter=#counter_state{leasing=L3, cnt=C3}} = S3,
                       ?assertEqual(true, L3),
                       ?assertEqual(99, C3),
                       S4 = update_counter(S3),
                       #state{counter=#counter_state{lease=Leased2, leasing=L4, cnt=C4}} = S4,
                       ?assertEqual(false, L4),
                       ?assertEqual(100, C4),
                       ?assertEqual(200, Leased2),
                       {ok, cleared} = ?MGR:clear_vnodeid(Pid),
                       ok = ?MGR:stop(Pid)
               end}
             ]
     }
    }.

%% @private tests that the counter rolls over to 1 when a new vnode id
%% is assigned
rollover_test_() ->
    {setup, fun() -> (catch file:delete("undefined/kv_vnode/0")) end,
     fun(_) ->
             file:delete("undefined/kv_vnode/0") end,
     {spawn, [{"Rollover",
               fun() ->
                       {ok, Pid} = ?MGR:start_link(self(), 0, true),
                       {ok, {VId, CounterState}} = get_vnodeid_and_counter(Pid, ?MAX_INT, true),
                       #counter_state{cnt=Cnt, lease=Leased, lease_size=LS, leasing=L} = CounterState,
                       ?assertEqual(0, Cnt),
                       ?assertEqual((?MAX_INT), Leased),
                       ?assertEqual((?MAX_INT), LS),
                       ?assertEqual(false, L),
                       %% fiddle the counter to the max int size-2
                       State = #state{vnodeid=VId, status_mgr_pid=Pid, counter=CounterState#counter_state{cnt=(?MAX_INT-2)}},
                       S2=#state{counter=#counter_state{leasing=L2, cnt=C2}} = update_counter(State),
                       ?assertEqual(true, L2),
                       ?assertEqual((?MAX_INT-1), C2),
                       %% Vnode ID should roll over, and counter reset
                       #state{vnodeid=VId2, counter=#counter_state{leasing=L3, cnt=C3}} = update_counter(S2),

                       ?assert(VId /= VId2),
                       ?assertEqual(false, L3),
                       ?assertEqual(1, C3),

                       {ok, cleared} = ?MGR:clear_vnodeid(Pid),
                       ok = ?MGR:stop(Pid)
               end}
             ]}
    }.



-endif.
