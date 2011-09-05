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
-export([put_merge/6]). %% For fsm_eqc_vnode
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
                vnodeid :: undefined | binary(),
                bucket_buf_size :: pos_integer(),
                index_buf_size :: pos_integer(),
                key_buf_size :: pos_integer(),
                in_handoff = false :: boolean()}).

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
    {ok, VId} = get_vnodeid(Index),

    case catch Mod:start(Index, Configuration) of
        {ok, ModState} ->
            %% Get the backend capabilities
            {_, Capabilities} = Mod:api_version(),
            IndexBackend = lists:member(indexes, Capabilities),
            {ok, #state{idx=Index,
                        index_backend=IndexBackend,
                        mod=Mod,
                        modstate=ModState,
                        vnodeid=VId,
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
    UpdState = do_put(Sender, BKey,  Object, ReqId, StartTime, Options, State),
    {noreply, UpdState};

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
handle_command(?KV_DELETE_REQ{bkey=BKey, req_id=ReqId}, _Sender, State) ->
    do_delete(BKey, ReqId, State);
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
    riak_kv_stat:update(vnode_put),
    UpdState.

prepare_put(#state{index_backend=false, vnodeid=VId}, 
            PutArgs=#putargs{lww=true, robj=RObj, starttime=StartTime}) ->
    {{true, riak_object:increment_vclock(RObj, VId, StartTime)}, PutArgs};
prepare_put(#state{index_backend=IndexBackend,
                   vnodeid=VId,
                   mod=Mod,
                   modstate=ModState},
            PutArgs=#putargs{bkey={Bucket, Key},
                             robj=RObj,
                             bprops=BProps,
                             coord=Coord,
                             lww=LWW,
                             starttime=StartTime,
                             prunetime=PruneTime}) ->
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
            {{true, ObjToStore}, PutArgs#putargs{index_specs=IndexSpecs}};
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
                     PutArgs#putargs{index_specs=IndexSpecs}}
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
list_buckets(Sender, Filter, Mod, ModState, BufferSize) ->
    Buffer = riak_kv_fold_buffer:new(BufferSize, get_buffer_fun(false, Sender)),
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
            riak_kv_fold_buffer:flush(Buffer1, get_buffer_fun(true, Sender));
        {error, Reason} ->
            riak_core_vnode:reply(Sender, {error, Reason})
    end.

%% @private
list_keys(Sender, Bucket, Filter, Mod, ModState, BufferSize) ->
    Buffer = riak_kv_fold_buffer:new(BufferSize,
                                     get_buffer_fun({false, Bucket}, Sender)),
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
            riak_kv_fold_buffer:flush(Buffer1,
                                      get_buffer_fun({true, Bucket}, Sender));
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
    Buffer = riak_kv_fold_buffer:new(BufferSize,
                                     get_buffer_fun({false, Bucket}, Sender)),
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
            riak_kv_fold_buffer:flush(Buffer1,
                                      get_buffer_fun({true, Bucket}, Sender));
        {error, Reason} ->
            riak_core_vnode:reply(Sender, {error, Reason})
    end.

%% @private
get_buffer_fun(true, Sender) ->
    fun(Results) ->
            riak_core_vnode:reply(Sender,
                                  {final_results, Results})
    end;
get_buffer_fun(false, Sender) ->
    fun(Results) ->
            riak_core_vnode:reply(Sender,
                                  {results, Results})
    end;
get_buffer_fun({true, Bucket}, Sender) ->
    fun(Results) ->
            riak_core_vnode:reply(Sender,
                                  {final_results, {Bucket, Results}})
    end;
get_buffer_fun({false, Bucket}, Sender) ->
    fun(Results) ->
            riak_core_vnode:reply(Sender,
                                  {results, {Bucket, Results}})
    end.

%% @private
do_delete(BKey, ReqId, State) ->
    Mod = State#state.mod,
    ModState = State#state.modstate,
    Idx = State#state.idx,

    %% Get the existing object.
    case do_get_term(BKey, Mod, ModState) of
        {ok, RObj} ->
            %% Object exists, check if it should be deleted.
            case riak_kv_util:obj_not_deleted(RObj) of
                undefined ->
                    %% object is a tombstone or all siblings are tombstones
                    riak_kv_mapred_cache:eject(BKey),

                    %% Calculate the index specs to remove...
                    IndexSpecs = riak_object:diff_index_specs(undefined, RObj),

                    %% Do the delete...
                    {Bucket, Key} = BKey,
                    case Mod:delete(Bucket, Key, IndexSpecs, ModState) of
                        {ok, UpdModState} ->
                            UpdState = State#state {modstate = UpdModState },
                            {reply, {del, Idx, ReqId}, UpdState};
                        {error, _Reason, UpdModState} ->
                            UpdState = State#state {modstate = UpdModState },
                            {reply, {fail, Idx, ReqId}, UpdState}
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
do_diffobj_put({Bucket, Key}, DiffObj,
               _StateData=#state{index_backend=IndexBackend,
                                 mod=Mod,
                                 modstate=ModState}) ->
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
                {ok, _UpdModState} -> riak_kv_stat:update(vnode_put);
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
                        {ok, _UpdModState} -> riak_kv_stat:update(vnode_put);
                        _ -> nop
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
    VnodeStatusDir = app_helper:get_env(riak_kv, vnode_status, "data/kv_vnode"),
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
