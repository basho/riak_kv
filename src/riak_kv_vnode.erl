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
         readrepair/6,
         index_query/7,
         list_buckets/5,
         list_keys/4,
         list_keys/6,
         fold/3,
         get_vclocks/2]).

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
         handle_exit/3]).

-include_lib("riak_kv_vnode.hrl").
-include_lib("riak_kv_map_phase.hrl").
-include_lib("riak_core/include/riak_core_pb.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(mrjob, {cachekey :: term(),
                bkey :: term(),
                reqid :: term(),
                target :: pid()}).

-record(state, {idx :: partition(),
                index_backend :: boolean(),
                mod :: module(),
                modstate :: term(),
                mrjobs :: term(),
                bucket_buf_size :: pos_integer(),
                index_buf_size :: pos_integer(),
                key_buf_size :: pos_integer(),
                in_handoff = false :: boolean()}).

-type index_op() :: add | remove.
-type index_value() :: integer() | binary().

-record(putargs, {returnbody :: boolean(),
                  lww :: boolean(),
                  bkey :: {binary(), binary()},
                  robj :: term(),
                  index_specs=[] :: [{index_op(), binary(), index_value()}],
                  reqid :: non_neg_integer(),
                  bprops :: maybe_improper_list(),
                  starttime :: non_neg_integer(),
                  prunetime :: undefined| non_neg_integer()}).

%% TODO: add -specs to all public API funcs, this module seems fragile?

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
    %% The function used for object folding expects the
    %% bucket and key pair to be passed as the first parameter, but in
    %% riak_kv the bucket and key have been separated. This function
    %% wrapper is to address this mismatch.
    FoldFun = fun(Bucket, Key, Value, Acc) ->
                         Fun({Bucket, Key}, Value, Acc)
                 end,
    riak_core_vnode_master:sync_spawn_command(Preflist,
                                              ?FOLD_REQ{
                                                 foldfun=FoldFun,
                                                 acc0=Acc0},
                                              riak_kv_vnode_master).

get_vclocks(Preflist, BKeyList) ->
    riak_core_vnode_master:sync_spawn_command(Preflist,
                                              ?KV_VCLOCK_REQ{bkeys=BKeyList},
                                              riak_kv_vnode_master).

%% VNode callbacks

init([Index]) ->
    Mod = app_helper:get_env(riak_kv, storage_backend),
    Configuration = app_helper:get_env(riak_kv),
    BucketBufSize = app_helper:get_env(riak_kv, bucket_buffer_size, 1000),
    IndexBufSize = app_helper:get_env(riak_kv, index_buffer_size, 100),
    KeyBufSize = app_helper:get_env(riak_kv, key_buffer_size, 100),

    case catch Mod:start(Index, Configuration) of
        {ok, ModState} ->
            %% Get the backend capabilities
            {_, Capabilities} = Mod:api_version(),
            IndexBackend = lists:member(indexes, Capabilities),
            {ok, #state{idx=Index,
                        index_backend=IndexBackend,
                        mod=Mod,
                        modstate=ModState,
                        bucket_buf_size=BucketBufSize,
                        index_buf_size=IndexBufSize,
                        key_buf_size=KeyBufSize,
                        mrjobs=dict:new()}};
        {error, Reason} ->
            lager:error("Failed to start ~p Reason: ~p",
                                   [Mod, Reason]),
            riak:stop("backend module failed to start.");
        {'EXIT', Reason1} ->
            lager:error("Failed to start ~p Reason: ~p",
                                   [Mod, Reason1]),
            riak:stop("backend module failed to start.")
    end.


handle_command(?KV_PUT_REQ{bkey=BKey,
                           object=Object,
                           req_id=ReqId,
                           start_time=StartTime,
                           options=Options},
               Sender, State=#state{idx=Idx}) ->
    riak_kv_mapred_cache:eject(BKey),
    riak_core_vnode:reply(Sender, {w, Idx, ReqId}),
    do_put(Sender, BKey,  Object, ReqId, StartTime, Options, State),
    {noreply, State};

handle_command(?KV_GET_REQ{bkey=BKey,req_id=ReqId},Sender,State) ->
    do_get(Sender, BKey, ReqId, State);
handle_command(?KV_MGET_REQ{bkeys=BKeys, req_id=ReqId, from=From}, _Sender, State) ->
    do_mget(From, BKeys, ReqId, State);
handle_command(#riak_kv_listkeys_req_v1{bucket=Bucket, req_id=ReqId}, _Sender,
               State=#state{mod=Mod, modstate=ModState, idx=Idx}) ->
    do_legacy_list_bucket(ReqId,Bucket,Mod,ModState,Idx,State);
handle_command(#riak_kv_listkeys_req_v2{bucket=Bucket, req_id=ReqId, caller=Caller}, _Sender,
               State=#state{mod=Mod, modstate=ModState, idx=Idx}) ->
    do_legacy_list_keys(Caller,ReqId,Bucket,Idx,Mod,ModState),
    {noreply, State};
handle_command(?KV_DELETE_REQ{bkey=BKey, req_id=ReqId}, _Sender,
               State=#state{mod=Mod, modstate=ModState,
                            idx=Idx}) ->
    case do_get_term(BKey, Mod, ModState) of
        {ok, Obj} ->
            case riak_kv_util:obj_not_deleted(Obj) of
                undefined ->
                    %% object is a tombstone or all siblings are tombstones
                    riak_kv_mapred_cache:eject(BKey),
                    Res = do_delete(BKey, Mod, ModState),
                    {reply, {Res, Idx, ReqId}, State};
                _ ->
                    %% not a tombstone or not all siblings are tombstones
                    {reply, {fail, Idx, ReqId}, State}
            end;
        _ ->
            %% does not exist in the backend
            {reply, {fail, Idx, ReqId}, State}
    end;
handle_command(?KV_VCLOCK_REQ{bkeys=BKeys}, _Sender, State) ->
    {reply, do_get_vclocks(BKeys, State), State};
handle_command(?FOLD_REQ{foldfun=Fun, acc0=Acc},_Sender,State) ->
    Reply = do_fold(Fun, Acc, State),
    {reply, Reply, State};

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
    {noreply, NewState}.

%% @doc Handle a coverage request.
%% More information about the specification for the ItemFilter
%% parameter can be found in the documentation for the
%% {@link riak_kv_coverage_filter} module.
handle_coverage(?KV_LISTBUCKETS_REQ{item_filter=ItemFilter},
                _FilterVNodes,
                Sender,
                State=#state{mod=Mod,
                             modstate=ModState,
                             bucket_buf_size=BucketBufSize}) ->
    %% Construct the filter function
    Filter = riak_kv_coverage_filter:build_filter(all, ItemFilter, undefined),
    list_buckets(Sender, Filter, Mod, ModState, BucketBufSize),
    {noreply, State};
handle_coverage(?KV_LISTKEYS_REQ{bucket=Bucket,
                                 item_filter=ItemFilter},
                FilterVNodes,
                Sender,
                State=#state{idx=Index,
                             mod=Mod,
                             modstate=ModState,
                             key_buf_size=KeyBufSize}) ->
    %% Construct the filter function
    FilterVNode = proplists:get_value(Index, FilterVNodes),
    Filter = riak_kv_coverage_filter:build_filter(Bucket, ItemFilter, FilterVNode),
    list_keys(Sender, Bucket, Filter, Mod, ModState, KeyBufSize),
    {noreply, State};
handle_coverage(?KV_INDEX_REQ{bucket=Bucket,
                              item_filter=ItemFilter,
                              qry=Query},
                FilterVNodes,
                Sender,
                State=#state{idx=Index,
                             index_backend=IndexBackend,
                             index_buf_size=IndexBufSize,
                             mod=Mod,
                             modstate=ModState}) ->
    case IndexBackend of
        true ->
            %% Construct the filter function
            FilterVNode = proplists:get_value(Index, FilterVNodes),
            Filter = riak_kv_coverage_filter:build_filter(Bucket, ItemFilter, FilterVNode),
            index_query(Sender, Bucket, Query, Filter, Mod, ModState, IndexBufSize);
        false ->
            riak_core_vnode:reply(Sender, {error, {indexes_not_supported, Mod}})
    end,
    {noreply, State}.

handle_handoff_command(Req=?FOLD_REQ{foldfun=FoldFun}, Sender, State) ->
    %% The function in riak_core used for object folding
    %% during handoff expects the bucket and key pair to be
    %% passed as the first parameter, but in riak_kv the bucket
    %% and key have been separated. This function wrapper is
    %% to address this mismatch.
    HandoffFun = fun(Bucket, Key, Value, Acc) ->
                         FoldFun({Bucket, Key}, Value, Acc)
                 end,
    handle_command(Req?FOLD_REQ{foldfun=HandoffFun}, Sender, State);
handle_handoff_command(Req={backend_callback, _Ref, _Msg}, Sender, State) ->
    handle_command(Req, Sender, State);
handle_handoff_command(_Req, _Sender, State) -> {forward, State}.


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

delete(State=#state{mod=Mod, modstate=ModState}) ->
    case Mod:drop(ModState) of
        {ok, UpdModState} ->
            ok;
        {error, Reason, UpdModState} ->
            lager:error("Failed to drop ~p. Reason: ~p~n", [Mod, Reason]),
            ok
    end,
    {ok, State#state{modstate=UpdModState}}.

terminate(_Reason, #state{mod=Mod, modstate=ModState}) ->
    Mod:stop(ModState),
    ok.

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

%% old vnode helper functions


                                                %store_call(State=#state{mod=Mod, modstate=ModState}, Msg) ->
                                                %    Mod:call(ModState, Msg).

%% @private
                                                % upon receipt of a client-initiated put
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
    PutArgs = #putargs{returnbody=proplists:get_value(returnbody,Options,false),
                       lww=proplists:get_value(last_write_wins, BProps, false),
                       bkey=BKey,
                       robj=RObj,
                       reqid=ReqID,
                       bprops=BProps,
                       starttime=StartTime,
                       prunetime=PruneTime},
    {PrepPutRes, UpdPutArgs} = prepare_put(State, PutArgs),
    Reply = perform_put(PrepPutRes, State, UpdPutArgs),
    riak_core_vnode:reply(Sender, Reply),
    riak_kv_stat:update(vnode_put).

prepare_put(#state{index_backend=false}, PutArgs=#putargs{lww=true, robj=RObj}) ->
    {{true, RObj}, PutArgs};
prepare_put(#state{index_backend=true,
                   mod=Mod,
                   modstate=ModState},
            PutArgs=#putargs{bkey=BKey,
                             lww=true,
                             robj=RObj}) ->
    %% Check for and merge indexes
    IndexSpecs = get_index_specs(Mod, ModState, BKey, RObj),
    {{true, RObj}, PutArgs#putargs{index_specs=IndexSpecs}};
prepare_put(#state{index_backend=IndexBackend,
                   mod=Mod,
                   modstate=ModState},
            PutArgs=#putargs{bkey=BKey,
                             robj=RObj,
                             reqid=ReqID,
                             bprops=BProps,
                             starttime=StartTime,
                             prunetime=PruneTime}) ->
    case syntactic_put_merge(Mod, ModState, BKey, RObj, ReqID, IndexBackend, StartTime) of
        {oldobj, OldObj, IndexSpecs} ->
            {{false, OldObj}, PutArgs#putargs{index_specs=IndexSpecs}};
        {newobj, NewObj, IndexSpecs} ->
            VC = riak_object:vclock(NewObj),
            AMObj = enforce_allow_mult(NewObj, BProps),
            case PruneTime of
                undefined ->
                    ObjToStore = AMObj;
                _ ->
                    ObjToStore = riak_object:set_vclock(
                                   AMObj,
                                   vclock:prune(VC,PruneTime,BProps)
                                  )
            end,
            {{true, ObjToStore}, PutArgs#putargs{index_specs=IndexSpecs}}
    end.

perform_put({false, Obj},#state{idx=Idx},#putargs{returnbody=true,reqid=ReqID}) ->
    {dw, Idx, Obj, ReqID};
perform_put({false, _Obj}, #state{idx=Idx}, #putargs{returnbody=false,reqid=ReqId}) ->
    {dw, Idx, ReqId};
perform_put({true, Obj},
            #state{idx=Idx,
                   mod=Mod,
                   modstate=ModState},
            #putargs{returnbody=RB,
                     bkey={Bucket, Key},
                     reqid=ReqID,
                     index_specs=IndexSpecs}) ->
    Val = term_to_binary(Obj),
    case Mod:put(Bucket, Key, IndexSpecs, Val, ModState) of
        {ok, _UpdModstate} ->
            case RB of
                true -> {dw, Idx, Obj, ReqID};
                false -> {dw, Idx, ReqID}
            end;
        {error, _Reason, _UpdModState} ->
            {fail, Idx, ReqID}
    end.

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
syntactic_put_merge(Mod, ModState, BKey, Obj1, ReqId, IndexBackend) ->
    syntactic_put_merge(Mod, ModState, BKey, Obj1, ReqId, IndexBackend, vclock:timestamp()).

syntactic_put_merge(Mod, ModState, {Bucket, Key}, Obj1, ReqId, IndexBackend, StartTime) ->
    case Mod:get(Bucket, Key, ModState) of
        {error, not_found, _UpdModState} ->
            case IndexBackend of
                true ->
                    NewIndexSpecs = riak_object:get_index_specs(Obj1),
                    {newobj, Obj1, NewIndexSpecs};
                false ->
                    {newobj, Obj1, []}
            end;
        {ok, Val0, _UpdModState} ->
            Obj0 = binary_to_term(Val0),
            ResObj = riak_object:syntactic_merge(Obj0,
                                                 Obj1,
                                                 term_to_binary(ReqId),
                                                 StartTime),
            case IndexBackend of
                true ->
                    case riak_object:value_count(ResObj) of
                        1 ->
                            IndexSpecs = riak_object:get_index_specs(Obj0, Obj1);
                        _ ->
                            IndexSpecs = riak_object:resolve_index_specs(Obj0, Obj1)
                    end;
                false ->
                    IndexSpecs = []
            end,
            case riak_object:vclock(ResObj) =:= riak_object:vclock(Obj0) of
                true ->
                    {oldobj, ResObj, IndexSpecs};
                false ->
                    {newobj, ResObj, IndexSpecs}
            end
    end.

%% @private
get_index_specs(Mod, ModState, {Bucket, Key}, Obj1) ->
    case Mod:get(Bucket, Key, ModState) of
        {error, not_found, _UpdModState} ->
            riak_object:get_index_specs(Obj1);
        {ok, Val0, _UpdModState} ->
            Obj0 = binary_to_term(Val0),
            riak_object:get_index_specs(Obj0, Obj1)
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
list_buckets(Sender, Filter, Mod, ModState, BufferSize) ->
    BufferFun = fun(Results) ->
                        riak_core_vnode:reply(Sender, {results, Results})
                end,
    Buffer = riak_kv_fold_buffer:new(BufferSize, BufferFun),
    case Filter of
        none ->
            FoldBucketsFun =
                fun(Bucket, Buf) ->
                        riak_kv_fold_buffer:add(Bucket, Buf)
                end;
        _ ->
            FoldBucketsFun =
                fun(Bucket, Buf) ->
                        case Filter(Bucket) of
                            true ->
                                riak_kv_fold_buffer:add(Bucket, Buf);
                            false ->
                                Buf
                        end
                end
    end,
    case Mod:fold_buckets(FoldBucketsFun, Buffer, [], ModState) of
        {ok, Buffer1} ->
            FlushFun = fun(FinalResults) ->
                               riak_core_vnode:reply(Sender,
                                                     {final_results,
                                                      FinalResults})
                       end,
            riak_kv_fold_buffer:flush(Buffer1, FlushFun);
        {error, Reason} ->
            riak_core_vnode:reply(Sender, {error, Reason})
    end.

%% @private
list_keys(Sender, Bucket, Filter, Mod, ModState, BufferSize) ->
    BufferFun = fun(Results) ->
                        riak_core_vnode:reply(Sender,
                                              {results, {Bucket, Results}})
                end,
    Buffer = riak_kv_fold_buffer:new(BufferSize, BufferFun),
    case Filter of
        none ->
            FoldKeysFun =
                fun(_, Key, Buf) ->
                        riak_kv_fold_buffer:add(Key, Buf)
                end;
        _ ->
            FoldKeysFun =
                fun(_, Key, Buf) ->
                        case Filter(Key) of
                            true ->
                                riak_kv_fold_buffer:add(Key, Buf);
                            false ->
                                Buf
                        end
                end
    end,
    Opts = [{bucket, Bucket}],
    case Mod:fold_keys(FoldKeysFun, Buffer, Opts, ModState) of
        {ok, Buffer1} ->
            FlushFun = fun(FinalResults) ->
                               riak_core_vnode:reply(Sender,
                                                     {final_results,
                                                      {Bucket,
                                                       FinalResults}})
                       end,
            riak_kv_fold_buffer:flush(Buffer1, FlushFun);
        {error, Reason} ->
            riak_core_vnode:reply(Sender, {error, Reason})
    end.

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
%% @deprecated This function is only here to support
%% rolling upgrades and will be removed.
do_legacy_list_keys(Caller,ReqId,'_',Idx,Mod,ModState) ->
    do_legacy_list_buckets(Caller,ReqId,Idx,Mod,ModState);
do_legacy_list_keys(Caller,ReqId,Input,Idx,Mod,ModState) ->
    case Input of
        {filter, Bucket, Filter} ->
            ok;
        Bucket ->
            Filter = none
    end,
    BufferSize = 100,
    BufferFun = fun(Results) ->
                        Caller ! {ReqId, {kl, Idx, Results}}
                end,
    Buffer = riak_kv_fold_buffer:new(BufferSize, BufferFun),
    case Filter of
        none ->
            FoldKeysFun =
                fun(_, Key, Buf) ->
                        riak_kv_fold_buffer:add(Key, Buf)
                end;
        _ ->
            FoldKeysFun =
                fun(_, Key, Buf) ->
                        case Filter(Key) of
                            true ->
                                riak_kv_fold_buffer:add(Key, Buf);
                            false ->
                                Buf
                        end
                end
    end,
    Opts = [{bucket, Bucket}],
    case Mod:fold_keys(FoldKeysFun, Buffer, Opts, ModState) of
        {ok, Buffer1} ->
            FlushFun = fun(FinalResults) ->
                               Caller ! {ReqId, {kl, Idx, FinalResults}},
                               Caller ! {ReqId, Idx, done}
                       end,
            riak_kv_fold_buffer:flush(Buffer1, FlushFun);
        {error, Reason} ->
            Caller ! {ReqId, {error, Reason}}
    end.

%% @private
%% @deprecated This function is only here to support
%% rolling upgrades and will be removed.
do_legacy_list_buckets(Caller,ReqId,Idx,Mod,ModState) ->
    BufferSize = 1000,
    BufferFun = fun(Results) ->
                        UniqueResults = lists:usort(Results),
                        Caller ! {ReqId, {kl, Idx, UniqueResults}}
                end,
    Buffer = riak_kv_fold_buffer:new(BufferSize, BufferFun),
    FoldBucketsFun =
        fun(Bucket, Buf) ->
                riak_kv_fold_buffer:add(Bucket, Buf)
        end,
    case Mod:fold_buckets(FoldBucketsFun, Buffer, [], ModState) of
        {ok, Buffer1} ->
            FlushFun = fun(FinalResults) ->
                               Caller ! {ReqId, {kl, Idx, FinalResults}},
                               Caller ! {ReqId, Idx, done}
                       end,
            riak_kv_fold_buffer:flush(Buffer1, FlushFun);
        {error, Reason} ->
            Caller ! {ReqId, {error, Reason}}
    end.

%% @private
index_query(Sender, Bucket, Query, Filter, Mod, ModState, BufferSize) ->
    BufferFun = fun(Results) ->
                        riak_core_vnode:reply(Sender,
                                              {results, {Bucket, Results}})
                end,
    Buffer = riak_kv_fold_buffer:new(BufferSize, BufferFun),
    case Filter of
        none ->
            FoldKeysFun =
                fun(_, Key, Buf) ->
                        riak_kv_fold_buffer:add(Key, Buf)
                end;
        _ ->
            FoldKeysFun =
                fun(_, Key, Buf) ->
                        case Filter(Key) of
                            true ->
                                riak_kv_fold_buffer:add(Key, Buf);
                            false ->
                                Buf
                        end
                end
    end,
    Opts = [{index, Bucket, Query}],
    case Mod:fold_keys(FoldKeysFun, Buffer, Opts, ModState) of
        {ok, Buffer1} ->
            FlushFun = fun(FinalResults) ->
                               riak_core_vnode:reply(Sender,
                                                     {final_results,
                                                      {Bucket,
                                                       FinalResults}})
                       end,
            riak_kv_fold_buffer:flush(Buffer1, FlushFun);
        {error, Reason} ->
            riak_core_vnode:reply(Sender, {error, Reason})
    end.


%% @private
do_delete({Bucket, Key}, Mod, ModState) ->
    case Mod:delete(Bucket, Key, ModState) of
        {ok, _UpdModState} ->
            del;
        {error, _Reason, _UpdModState} ->
            fail
    end.

%% @private
do_fold(Fun, Acc0, _State=#state{mod=Mod, modstate=ModState}) ->
    {ok, Objects} = Mod:fold_objects(Fun, Acc0, [], ModState),
    Objects.

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
do_diffobj_put(BKey={Bucket, Key}, DiffObj,
               _StateData=#state{index_backend=IndexBackend,
                                 mod=Mod,
                                 modstate=ModState}) ->
    ReqID = erlang:phash2(erlang:now()),
    case syntactic_put_merge(Mod, ModState, BKey, DiffObj, ReqID, IndexBackend) of
        {newobj, NewObj, IndexSpecs} ->
            AMObj = enforce_allow_mult(NewObj, riak_core_bucket:get_bucket(Bucket)),
            Val = term_to_binary(AMObj),
            Res = Mod:put(Bucket, Key, IndexSpecs, Val, ModState),
            case Res of
                {ok, _UpdModState} -> riak_kv_stat:update(vnode_put);
                _ -> nop
            end,
            Res;
        _ -> {ok, ModState}
    end.

%% @private

-ifdef(TEST).

dummy_backend(BackendMod) ->
    Ring = riak_core_ring:fresh(16,node()),
    riak_core_ring_manager:set_ring_global(Ring),
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
             application:get_all_env(riak_kv)
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
        {'$gen_event', {_,{results,Results}}} ->
            result_listener_keys(Results ++ Acc);
        {'$gen_event', {_,{final_results,Results}}} ->
            result_listener_done(Results ++ Acc)
    after 5000 ->
            result_listener_done({timeout, Acc})
    end.

result_listener_keys(Acc) ->
    receive
        {'$gen_event', {_,{results,{_Bucket, Results}}}} ->
            result_listener_keys(Results ++ Acc);
        {'$gen_event', {_,{final_results,{_Bucket, Results}}}} ->
            result_listener_done(Results ++ Acc)
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
