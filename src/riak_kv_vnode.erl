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
-export([start_vnode/1,
         get/3,
         mget/3,
         del/3,
         put/6,
         readrepair/6,
         list_keys/4,
         fold/3,
         get_vclocks/2]).

%% riak_core_vnode API
-export([init/1,
         terminate/2,
         handle_command/3,
         is_empty/1,
         delete/1,
         handle_handoff_command/3,
         handoff_starting/2,
         handoff_cancelled/1,
         handoff_finished/2,
         handle_handoff_data/2,
         encode_handoff_item/2]).

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
                mod :: module(),
                modstate :: term(),
                mrjobs :: term(),
                in_handoff = false :: boolean()}).

-record(putargs, {returnbody :: boolean(),
                  lww :: boolean(),
                  bkey :: {binary(), binary()},
                  robj :: term(),
                  reqid :: non_neg_integer(),
                  bprops :: maybe_improper_list(),
                  prunetime :: non_neg_integer()}).

%% TODO: add -specs to all public API funcs, this module seems fragile?

%% API
start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, riak_kv_vnode).

get(Preflist, BKey, ReqId) ->
    Req = ?KV_GET_REQ{bkey=BKey,
                      req_id=ReqId},
    %% Assuming this function is called from a FSM process
    %% so self() == FSM pid
    riak_core_vnode_master:sync_spawn_command(Preflist,
                                              Req,
                                              {fsm, ReqId, self()},
                                              riak_kv_vnode_master).

mget(Preflist, BKeys, ReqId) ->
    Req = ?KV_MGET_REQ{bkeys=BKeys,
                       req_id=ReqId,
                       from={fsm, self()}},
    riak_core_vnode_master:command(Preflist,
                                   Req,
                                   riak_kv_vnode_master).

del(Preflist, BKey, ReqId) ->
    riak_core_vnode_master:sync_command(Preflist,
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
    put(Preflist, BKey, Obj, ReqId, StartTime, Options, ignore).

list_keys(Preflist, ReqId, Caller, Bucket) ->
  riak_core_vnode_master:command(Preflist,
                                 ?KV_LISTKEYS_REQ{
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

%% VNode callbacks

init([Index]) ->
    Mod = app_helper:get_env(riak_kv, storage_backend),
    Configuration = app_helper:get_env(riak_kv),
    {ok, ModState} = Mod:start(Index, Configuration),

    {ok, #state{idx=Index, mod=Mod, modstate=ModState, mrjobs=dict:new()}}.

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
    do_list_bucket(ReqId,Bucket,Mod,ModState,Idx,State);
handle_command(?KV_LISTKEYS_REQ{bucket=Bucket, req_id=ReqId, caller=Caller}, _Sender,
               State=#state{mod=Mod, modstate=ModState, idx=Idx}) ->
    do_list_keys(Caller,ReqId,Bucket,Idx,Mod,ModState),
    {noreply, State};

handle_command(?KV_DELETE_REQ{bkey=BKey, req_id=ReqId}, _Sender,
               State=#state{mod=Mod, modstate=ModState,
                            idx=Idx}) ->
    riak_kv_mapred_cache:eject(BKey),
    case Mod:delete(ModState, BKey) of
        ok ->
            {reply, {del, Idx, ReqId}, State};
        {error, _Reason} ->
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
    Mod:callback(ModState, Ref, Msg),
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

handle_handoff_command(Req=?FOLD_REQ{}, Sender, State) ->
    handle_command(Req, Sender, State);
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
        ok ->
            {reply, ok, State};
        Err ->
            {reply, {error, Err}, State}
    end.

encode_handoff_item({B,K}, V) ->
    zlib:zip(riak_core_pb:encode_riakobject_pb(
               #riakobject_pb{bucket=B, key=K, val=V})).

is_empty(State=#state{mod=Mod, modstate=ModState}) ->
    {Mod:is_empty(ModState), State}.

delete(State=#state{mod=Mod, modstate=ModState}) ->
    ok = Mod:drop(ModState),
    {ok, State}.

terminate(_Reason, #state{mod=Mod, modstate=ModState}) ->
    Mod:stop(ModState),
    ok.

%% old vnode helper functions


%store_call(State=#state{mod=Mod, modstate=ModState}, Msg) ->
%    Mod:call(ModState, Msg).

%% @private
% upon receipt of a client-initiated put
do_put(Sender, {Bucket,_Key}=BKey, RObj, ReqID, PruneTime, Options, State) ->
    {ok,Ring} = riak_core_ring_manager:get_my_ring(),
    BProps = riak_core_bucket:get_bucket(Bucket, Ring),
    PutArgs = #putargs{returnbody=proplists:get_value(returnbody,Options,false),
                       lww=proplists:get_value(last_write_wins, BProps, false),
                       bkey=BKey,
                       robj=RObj,
                       reqid=ReqID,
                       bprops=BProps,
                       prunetime=PruneTime},
    Reply = perform_put(prepare_put(State, PutArgs), State, PutArgs),
    riak_core_vnode:reply(Sender, Reply),
    riak_kv_stat:update(vnode_put).

prepare_put(#state{}, #putargs{lww=true, robj=RObj}) ->
    {true, RObj};
prepare_put(#state{mod=Mod,modstate=ModState}, #putargs{bkey=BKey,
                                                        robj=RObj,
                                                        reqid=ReqID,
                                                        bprops=BProps,
                                                        prunetime=PruneTime}) ->
    case syntactic_put_merge(Mod, ModState, BKey, RObj, ReqID) of
        {oldobj, OldObj} ->
            {false, OldObj};
        {newobj, NewObj} ->
            VC = riak_object:vclock(NewObj),
            AMObj = enforce_allow_mult(NewObj, BProps),
            ObjToStore = riak_object:set_vclock(
                           AMObj,
                           vclock:prune(VC,PruneTime,BProps)
                           ),
            {true, ObjToStore}
    end.

perform_put({false, Obj},#state{idx=Idx},#putargs{returnbody=true,reqid=ReqID}) ->
    {dw, Idx, Obj, ReqID};
perform_put({false, _Obj}, #state{idx=Idx}, #putargs{returnbody=false,reqid=ReqId}) ->
    {dw, Idx, ReqId};
perform_put({true, Obj}, #state{idx=Idx,mod=Mod,modstate=ModState},
            #putargs{returnbody=RB, bkey=BKey, reqid=ReqID}) ->
    Val = term_to_binary(Obj),
    case Mod:put(ModState, BKey, Val) of
        ok ->
            case RB of
                true -> {dw, Idx, Obj, ReqID};
                false -> {dw, Idx, ReqID}
            end;
        {error, _Reason} ->
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
syntactic_put_merge(Mod, ModState, BKey, Obj1, ReqId) ->
    case Mod:get(ModState, BKey) of
        {error, notfound} -> {newobj, Obj1};
        {ok, Val0} ->
            Obj0 = binary_to_term(Val0),
            ResObj = riak_object:syntactic_merge(
                       Obj0,Obj1,term_to_binary(ReqId)),
            case riak_object:vclock(ResObj) =:= riak_object:vclock(Obj0) of
                true -> {oldobj, ResObj};
                false -> {newobj, ResObj}
            end
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
        {ok, Bin} ->
            {ok, binary_to_term(Bin)};
        Err ->
            Err
    end.

do_get_binary(BKey, Mod, ModState) ->
    Mod:get(ModState,BKey).


%% @private
do_list_bucket(ReqID,Bucket,Mod,ModState,Idx,State) ->
    RetVal = Mod:list_bucket(ModState,Bucket),
    {reply, {kl, RetVal, Idx, ReqID}, State}.

%% Use in-memory key list for bitcask backend
%% @private
do_list_keys(Caller,ReqId,Bucket,Idx,Mod,ModState)
  when Mod =:= riak_kv_bitcask_backend ->
    F = fun(BKey, Acc) ->
                process_keys(Caller, ReqId, Idx, Bucket, BKey, Acc) end,
    case Mod:fold_keys(ModState, F, []) of
        [] ->
            ok;
        Remainder ->
            Caller ! {ReqId, {kl, Idx, Remainder}}
    end,
    Caller ! {ReqId, Idx, done};
do_list_keys(Caller,ReqId,Bucket,Idx,Mod,ModState)
  when Mod =:= riak_kv_innostore_backend ->
    F = fun(Key, Acc) ->
                process_keys(Caller, ReqId, Idx, Bucket, {Bucket, Key}, Acc) end,
    case Mod:fold_bucket_keys(ModState, Bucket, F) of
        [] ->
            ok;
        Remainder ->
            Caller ! {ReqId, {kl, Idx, Remainder}}
    end,
    Caller ! {ReqId, Idx, done};
%% @private
do_list_keys(Caller,ReqId,Bucket,Idx,Mod,ModState) ->
    F = fun(BKey, _, Acc) ->
                process_keys(Caller, ReqId, Idx, Bucket, BKey, Acc) end,
    case Mod:fold(ModState, F, []) of
        [] ->
            ok;
        Remainder ->
            Caller ! {ReqId, {kl, Idx, Remainder}}
    end,
    Caller ! {ReqId, Idx, done}.

%% @private
process_keys(Caller, ReqId, Idx, '_', {Bucket, _K}, Acc) ->
    %% Bucket='_' means "list buckets" instead of "list keys"
    buffer_key_result(Caller, ReqId, Idx, [Bucket|Acc]);
process_keys(Caller, ReqId, Idx, {filter, Bucket, Fun}, {Bucket, K}, Acc) ->
    %% Bucket={filter,Bucket,Fun} means "only include keys
    %% in Bucket that make Fun(K) return 'true'"
    case Fun(K) of
        true ->
            buffer_key_result(Caller, ReqId, Idx, [K|Acc]);
        false ->
            Acc
    end;
process_keys(Caller, ReqId, Idx, Bucket, {Bucket, K}, Acc) ->
    buffer_key_result(Caller, ReqId, Idx, [K|Acc]);
process_keys(_Caller, _ReqId, _Idx, _Bucket, {_B, _K}, Acc) ->
    Acc.

buffer_key_result(Caller, ReqId, Idx, Acc) ->
    case length(Acc) >= 100 of
        true ->
            Caller ! {ReqId, {kl, Idx, Acc}},
            [];
        false ->
            Acc
    end.

%% @private
do_fold(Fun, Acc0, _State=#state{mod=Mod, modstate=ModState}) ->
    Mod:fold(ModState, Fun, Acc0).

%% @private
do_get_vclocks(KeyList,_State=#state{mod=Mod,modstate=ModState}) ->
    [{BKey, do_get_vclock(BKey,Mod,ModState)} || BKey <- KeyList].
%% @private
do_get_vclock(BKey,Mod,ModState) ->
    case Mod:get(ModState, BKey) of
        {error, notfound} -> vclock:fresh();
        {ok, Val} -> riak_object:vclock(binary_to_term(Val))
    end.

%% @private
% upon receipt of a handoff datum, there is no client FSM
do_diffobj_put(BKey={Bucket,_}, DiffObj,
       _StateData=#state{mod=Mod,modstate=ModState}) ->
    ReqID = erlang:phash2(erlang:now()),
    case syntactic_put_merge(Mod, ModState, BKey, DiffObj, ReqID) of
        {newobj, NewObj} ->
            AMObj = enforce_allow_mult(NewObj, riak_core_bucket:get_bucket(Bucket)),
            Val = term_to_binary(AMObj),
            Res = Mod:put(ModState, BKey, Val),
            case Res of
                ok -> riak_kv_stat:update(vnode_put);
                _ -> nop
            end,
            Res;
        _ -> ok
    end.

%% @private

-ifdef(TEST).

dummy_backend() ->
    Ring = riak_core_ring:fresh(16,node()),
    riak_core_ring_manager:set_ring_global(Ring),
    application:set_env(riak_kv, storage_backend, riak_kv_ets_backend),
    application:set_env(riak_core, default_bucket_props, []).

backend_with_known_key() ->
    dummy_backend(),
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

list_buckets_test() ->
    {S, B, _K} = backend_with_known_key(),
    Caller = new_result_listener(),
    handle_command(?KV_LISTKEYS_REQ{bucket='_',
                                    req_id=124,
                                    caller=Caller},
                   {raw, 456, self()}, S),
    ?assertEqual({ok, [B]}, results_from_listener(Caller)),
    flush_msgs().

filter_keys_test() ->
    {S, B, K} = backend_with_known_key(),

    Caller1 = new_result_listener(),
    handle_command(?KV_LISTKEYS_REQ{
                      bucket={filter,B,fun(_) -> true end},
                      req_id=124,
                      caller=Caller1},
                   {raw, 456, self()}, S),
    ?assertEqual({ok, [K]}, results_from_listener(Caller1)),

    Caller2 = new_result_listener(),
    handle_command(?KV_LISTKEYS_REQ{
                      bucket={filter,B,fun(_) -> false end},
                      req_id=125,
                      caller=Caller2},
                   {raw, 456, self()}, S),
    ?assertEqual({ok, []}, results_from_listener(Caller2)),

    Caller3 = new_result_listener(),
    handle_command(?KV_LISTKEYS_REQ{
                      bucket={filter,<<"g">>,fun(_) -> true end},
                      req_id=126,
                      caller=Caller3},
                   {raw, 456, self()}, S),
    ?assertEqual({ok, []}, results_from_listener(Caller3)),

    flush_msgs().

new_result_listener() ->
    spawn(fun result_listener/0).

result_listener() ->
    result_listener_keys([]).

result_listener_keys(Acc) ->
    receive
        {_,{kl,_,Keys}} ->
            result_listener_keys(Keys++Acc);
        {_, _, done} ->
            result_listener_done(Acc)
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
