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
         start_vnodes/1,
         get/3,
         del/3,
         put/6,
         local_get/2,
         local_put/2,
         local_put/3,
         coord_put/6,
         readrepair/6,
         list_keys/4,
         fold/3,
         get_vclocks/2,
         vnode_status/1,
         ack_keys/1,
         repair/1,
         repair_status/1,
         repair_filter/1,
         hashtree_pid/1,
         rehash/3,
         request_hashtree_pid/1,
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
         handoff_cancelled/1,
         handoff_finished/2,
         handle_handoff_data/2,
         encode_handoff_item/2,
         handle_exit/3,
         handle_info/2]).

-export([handoff_data_encoding_method/0]).

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
                in_handoff = false :: boolean(),
                hashtrees :: pid() }).

-type index_op() :: add | remove.
-type index_value() :: integer() | binary().
-type index() :: non_neg_integer().
-type state() :: #state{}.

-define(DEFAULT_HASHTREE_TOKENS, 90).

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
                  counter_op = undefined :: undefined | integer() %% if set this is a counter operation
                 }).

-spec maybe_create_hashtrees(state()) -> state().
maybe_create_hashtrees(State) ->
    maybe_create_hashtrees(riak_kv_entropy_manager:enabled(), State).

-spec maybe_create_hashtrees(boolean(), state()) -> state().
maybe_create_hashtrees(false, State) ->
    State;
maybe_create_hashtrees(true, State=#state{idx=Index}) ->
    %% Only maintain a hashtree if a primary vnode
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    case riak_core_ring:vnode_type(Ring, Index) of
        primary ->
            RP = riak_kv_util:responsible_preflists(Index),
            case riak_kv_index_hashtree:start(Index, RP, self()) of
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
    Req = ?KV_GET_REQ{bkey=BKey,
                      req_id=ReqId},
    riak_core_vnode_master:command(Preflist,
                                   Req,
                                   Sender,
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

-spec hashtree_pid(index()) -> {ok, pid()}.
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
    riak_core_vnode_master:command({Partition, node()},
                                   {hashtree_pid, node()},
                                   {raw, ReqId, self()},
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
                                              {reformat_object, BKey},
                                              riak_kv_vnode_master).

%% VNode callbacks

init([Index]) ->
    Mod = app_helper:get_env(riak_kv, storage_backend),
    Configuration = app_helper:get_env(riak_kv),
    BucketBufSize = app_helper:get_env(riak_kv, bucket_buffer_size, 1000),
    IndexBufSize = app_helper:get_env(riak_kv, index_buffer_size, 100),
    KeyBufSize = app_helper:get_env(riak_kv, key_buffer_size, 100),
    WorkerPoolSize = app_helper:get_env(riak_kv, worker_pool_size, 10),
    {ok, VId} = get_vnodeid(Index),
    DeleteMode = app_helper:get_env(riak_kv, delete_mode, 3000),
    AsyncFolding = app_helper:get_env(riak_kv, async_folds, true) == true,
    case catch Mod:start(Index, Configuration) of
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
                    FoldWorkerPool = {pool, riak_kv_worker, WorkerPoolSize, []},
                    State2 = maybe_create_hashtrees(State),
                    {ok, State2, [FoldWorkerPool]};
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


handle_overload_command(?KV_PUT_REQ{}, Sender, Idx) ->
    riak_core_vnode:reply(Sender, {fail, Idx, overload}); % subvert ReqId
handle_overload_command(?KV_GET_REQ{req_id=ReqID}, Sender, Idx) ->
    riak_core_vnode:reply(Sender, {r, {error, overload}, Idx, ReqID});
handle_overload_command(_, _, _) ->
    %% not handled yet
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
            riak_kv_index_hashtree:delete({Bucket, Key}, State#state.hashtrees)
    end,
    {noreply, State};

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

handle_coverage_index(Bucket, ItemFilter, Query,
                      FilterVNodes, Sender,
                      State=#state{mod=Mod,
                                   modstate=ModState},
                      ResultFunFun) ->
    {ok, Capabilities} = Mod:capabilities(Bucket, ModState),
    IndexBackend = lists:member(indexes, Capabilities),
    case IndexBackend of
        true ->
            %% Update stats...
            riak_kv_stat:update(vnode_index_read),

            ResultFun = ResultFunFun(Bucket, Sender),
            Opts = [{index, Bucket, Query},
                    {bucket, Bucket}],
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

handoff_starting(_TargetNode, State) ->
    {true, State#state{in_handoff=true}}.

handoff_cancelled(State) ->
    {ok, State#state{in_handoff=false}}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

handle_handoff_data(BinObj, State) ->
    try
        {BKey, Val} = decode_binary_object(BinObj),
        {B, K} = BKey,
        case do_diffobj_put(BKey, riak_object:from_binary(B, K, Val), 
                            State) of
            {ok, UpdModState} ->
                {reply, ok, State#state{modstate=UpdModState}};
            {error, Reason, UpdModState} ->
                {reply, {error, Reason}, State#state{modstate=UpdModState}};
            Err ->
                {reply, {error, Err}, State}
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
            BProps = riak_core_bucket:get_bucket(Bucket);
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
    CounterOp = proplists:get_value(counter_op, Options, undefined),
    PutArgs = #putargs{returnbody=proplists:get_value(returnbody,Options,false) orelse Coord,
                       coord=Coord,
                       lww=proplists:get_value(last_write_wins, BProps, false),
                       bkey=BKey,
                       robj=RObj,
                       reqid=ReqID,
                       bprops=BProps,
                       starttime=StartTime,
                       prunetime=PruneTime,
                       counter_op = CounterOp},
    {PrepPutRes, UpdPutArgs} = prepare_put(State, PutArgs),
    {Reply, UpdState} = perform_put(PrepPutRes, State, UpdPutArgs),
    riak_core_vnode:reply(Sender, Reply),

    update_index_write_stats(UpdPutArgs#putargs.is_index, UpdPutArgs#putargs.index_specs),
    UpdState.

do_backend_delete(BKey, RObj, State = #state{mod = Mod, modstate = ModState}) ->
    %% object is a tombstone or all siblings are tombstones
    %% Calculate the index specs to remove...
    %% JDM: This should just be a tombstone by this point, but better
    %% safe than sorry.
    IndexSpecs = riak_object:diff_index_specs(undefined, RObj),

    %% Do the delete...
    {Bucket, Key} = BKey,
    case Mod:delete(Bucket, Key, IndexSpecs, ModState) of
        {ok, UpdModState} ->
            riak_kv_index_hashtree:delete(BKey, State#state.hashtrees),
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
                   modstate=ModState},
            PutArgs=#putargs{bkey={Bucket, Key},
                             robj=RObj,
                             bprops=BProps,
                             coord=Coord,
                             lww=LWW,
                             starttime=StartTime,
                             prunetime=PruneTime,
                             counter_op = CounterOp},
            IndexBackend) ->
    GetReply =
        case do_get_object(Bucket, Key, Mod, ModState) of
            {error, not_found, _UpdModState} ->
                ok;
            {ok, TheOldObj, _UpdModState} ->
                {ok, TheOldObj}
        end,
    case GetReply of
        ok ->
            case IndexBackend of
                true ->
                    IndexSpecs = riak_object:index_specs(RObj);
                false ->
                    IndexSpecs = []
            end,
            ObjToStore = prepare_new_put(Coord, RObj, VId, StartTime, CounterOp),
            {{true, ObjToStore}, PutArgs#putargs{index_specs=IndexSpecs, is_index=IndexBackend}};
        {ok, OldObj} ->
            case put_merge(Coord, LWW, OldObj, RObj, VId, StartTime) of
                {oldobj, OldObj1} ->
                    {{false, OldObj1}, PutArgs};
                {newobj, NewObj} ->
                    VC = riak_object:vclock(NewObj),
                    AMObj = enforce_allow_mult(NewObj, BProps),
                    IndexSpecs = case IndexBackend of
                                     true ->
                                         riak_object:diff_index_specs(AMObj,
                                                             OldObj);
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
                    ObjToStore2 = handle_counter(Coord, CounterOp, VId, ObjToStore),
                    {{true, ObjToStore2},
                     PutArgs#putargs{index_specs=IndexSpecs, is_index=IndexBackend}}
            end
    end.

%% @Doc in the case that this a co-ordinating put, prepare the object.
prepare_new_put(true, RObj, VId, StartTime, CounterOp) when is_integer(CounterOp) ->
    VClockUp = riak_object:increment_vclock(RObj, VId, StartTime),
    %% coordinating a _NEW_ counter operation means
    %% creating + incrementing the counter.
    %% Make a new counter, stuff it in the riak_object
    riak_kv_counter:update(VClockUp, VId, CounterOp);
prepare_new_put(true, RObj, VId, StartTime, _CounterOp) ->
    riak_object:increment_vclock(RObj, VId, StartTime);
prepare_new_put(false, RObj, _VId, _StartTime, _CounterOp) ->
    RObj.

handle_counter(true, CounterOp, VId, RObj) when is_integer(CounterOp) ->
    riak_kv_counter:update(RObj, VId, CounterOp);
handle_counter(false, CounterOp, _Vid, RObj) when is_integer(CounterOp) ->
    %% non co-ord put, merge the values if there are siblings
    %% 'cos that is the point of CRDTs / counters: no siblings
    riak_kv_counter:merge(RObj);
handle_counter(_Coord, _CounterOp, _VId, RObj) ->
    %% i.e. not a counter op
    RObj.

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
    case encode_and_put(Obj, Mod, Bucket, Key, IndexSpecs, ModState) of
        {{ok, UpdModState}, EncodedVal} ->
            update_hashtree(Bucket, Key, EncodedVal, State),
            case RB of
                true ->
                    Reply = {dw, Idx, Obj, ReqID};
                false ->
                    Reply = {dw, Idx, ReqID}
            end;
        {{error, _Reason, UpdModState}, _EncodedVal} ->
            Reply = {fail, Idx, ReqID}
    end,
    {Reply, State#state{modstate=UpdModState}}.

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
                {{fail, _, _}, UpdState}  ->
                    Reply = {error, backend_error};
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
    ResObj = riak_object:syntactic_merge(CurObj, UpdObj),
    case ResObj =:= CurObj of
        true ->
            {oldobj, CurObj};
        false ->
            {newobj, ResObj}
    end;
put_merge(true, true, _CurObj, UpdObj, VId, StartTime) -> % coord=true, LWW=true
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
    StartTS = os:timestamp(),
    Retval = do_get_term(BKey, Mod, ModState),
    update_vnode_stats(vnode_get, Idx, StartTS),
    {reply, {r, Retval, Idx, ReqID}, State}.

%% @private
do_get_term({Bucket, Key}, Mod, ModState) ->
    case do_get_object(Bucket, Key, Mod, ModState) of
        {ok, Obj, _UpdModState} ->
            {ok, Obj};
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
            Mod:get_object(Bucket, Key, false, ModState);
        false ->
            case do_get_binary(Bucket, Key, Mod, ModState) of
                {ok, ObjBin, _UpdModState} ->
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
    case do_get_object(Bucket, Key, Mod, ModState) of
        {error, not_found, _UpdModState} -> vclock:fresh();
        {ok, Obj, _UpdModState} -> riak_object:vclock(Obj)
    end.

%% @private
%% upon receipt of a handoff datum, there is no client FSM
do_diffobj_put({Bucket, Key}, DiffObj,
               StateData=#state{mod=Mod,
                                modstate=ModState,
                                idx=Idx}) ->
    StartTS = os:timestamp(),
    {ok, Capabilities} = Mod:capabilities(Bucket, ModState),
    IndexBackend = lists:member(indexes, Capabilities),
    case do_get_object(Bucket, Key, Mod, ModState) of
        {error, not_found, _UpdModState} ->
            case IndexBackend of
                true ->
                    IndexSpecs = riak_object:index_specs(DiffObj);
                false ->
                    IndexSpecs = []
            end,
            case encode_and_put(DiffObj, Mod, Bucket, Key,
                                IndexSpecs, ModState) of
                {{ok, _UpdModState} = InnerRes, EncodedVal} ->
                    update_hashtree(Bucket, Key, EncodedVal, StateData),
                    update_index_write_stats(IndexBackend, IndexSpecs),
                    update_vnode_stats(vnode_put, Idx, StartTS),
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
                                        IndexSpecs, ModState) of
                        {{ok, _UpdModState} = InnerRes, EncodedVal} ->
                            update_hashtree(Bucket, Key, EncodedVal, StateData),
                            update_index_write_stats(IndexBackend, IndexSpecs),
                            update_vnode_stats(vnode_put, Idx, StartTS),
                            InnerRes;
                        {InnerRes, _EncodedVal} ->
                            InnerRes
                    end
            end
    end.

-spec update_hashtree(binary(), binary(), binary(), state()) -> ok.
update_hashtree(Bucket, Key, Val, #state{hashtrees=Trees}) ->
    case get_hashtree_token() of
        true ->
            riak_kv_index_hashtree:async_insert_object({Bucket, Key}, Val, Trees),
            ok;
        false ->
            riak_kv_index_hashtree:insert_object({Bucket, Key}, Val, Trees),
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
      Key::riak_object:key(), IndexSpecs::list(), ModState::term()) ->
           {{ok, UpdModState::term()}, EncodedObj::binary()} |
           {{error, Reason::term(), UpdModState::term()}, EncodedObj::binary()}.

encode_and_put(Obj, Mod, Bucket, Key, IndexSpecs, ModState) ->
    case uses_r_object(Mod, ModState, Bucket) of
        true ->
            Mod:put_object(Bucket, Key, IndexSpecs, Obj, ModState);
        false ->
            ObjFmt = riak_core_capability:get({riak_kv, object_format}, v0),
            EncodedVal = riak_object:to_binary(ObjFmt, Obj),
            {Mod:put(Bucket, Key, IndexSpecs, EncodedVal, ModState), EncodedVal}
    end.

uses_r_object(Mod, ModState, Bucket) ->
    {ok, Capabilities} = Mod:capabilities(Bucket, ModState),
    lists:member(uses_r_object, Capabilities).

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
             filelib:ensure_dir("kv_vnode_status_test/.test"),
             ?cmd("chmod u+rwx kv_vnode_status_test"),
             ?cmd("rm -rf kv_vnode_status_test"),
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
             application:start(folsom),
             riak_core_stat_cache:start_link(),
             riak_kv_stat:register_stats(),
             Env
     end,
     fun(Env) ->
             riak_core_ring_manager:cleanup_ets(test),
             riak_core_stat_cache:stop(),
             application:stop(folsom),
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
    flush_msgs().

%% include bitcask.hrl for HEADER_SIZE macro
-include_lib("bitcask/include/bitcask.hrl").

%% Verify that a bad CRC on read will not crash the vnode, which when done in
%% preparation for a write prevents the write from going through.
bitcask_badcrc_test() ->
    riak_core_ring_manager:setup_ets(test),
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
