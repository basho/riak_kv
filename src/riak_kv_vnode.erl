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
         list_buckets/6,
         list_keys/7,
         fold/3,
         get_vclocks/2]).

%% riak_core_vnode API
-export([init/1,
         terminate/2,
         handle_command/3,
         handle_coverage/2,
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

%% list_keys(Preflist, ReqId, Caller, Bucket) ->
%%   riak_core_vnode_master:coverage(?KV_LISTKEYS_REQ{
%%                                     bucket=Bucket,
%%                                     req_id=ReqId,
%%                                     caller=Caller},
%%                                  ignore,
%%                                  riak_kv_vnode_master).

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
handle_command(?COVERAGE_VNODE_REQ{args=Args,
                                   module=CoverageMod,
                                   function=CoverageFun,
                                   filter=Filter},
               _Sender,
               State=#state{mod=Mod, 
                            modstate=ModState,
                            idx=Idx}) ->
    apply(CoverageMod, CoverageFun, Args ++ [Filter, Idx, Mod, ModState]),
    {noreply, State};
 
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


handle_coverage(?COVERAGE_REQ{args=Args,
                              modfun={CoverageMod, CoverageFun}}=Req,
               State=#state{mod=Mod, 
                            modstate=ModState,
                            idx=Idx}) ->
    CoverageArgs0 = [element(X, Req) || {_, X} <- Args],
    CoverageArgs = CoverageArgs0 ++ [Idx, Mod, ModState],
    apply(CoverageMod, CoverageFun, CoverageArgs),
    {noreply, State}.

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

handle_exit(_Pid, _Reason, State) ->    
    %% A linked processes has died so the vnode
    %% process should take appropriate action here.
    %% The default behavior is to crash the vnode 
    %% process so that it can be respawned
    %% by riak_core_vnode_master to prevent 
    %% messages from stacking up on the process message
    %% queue and never being processed.
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
    Reply = perform_put(prepare_put(State, PutArgs), State, PutArgs),
    riak_core_vnode:reply(Sender, Reply),
    riak_kv_stat:update(vnode_put).

prepare_put(#state{}, #putargs{lww=true, robj=RObj}) ->
    {true, RObj};
prepare_put(#state{mod=Mod,modstate=ModState}, #putargs{bkey=BKey,
                                                        robj=RObj,
                                                        reqid=ReqID,
                                                        bprops=BProps,
                                                        starttime=StartTime,
                                                        prunetime=PruneTime}) ->
    case syntactic_put_merge(Mod, ModState, BKey, RObj, ReqID, StartTime) of
        {oldobj, OldObj} ->
            {false, OldObj};
        {newobj, NewObj} ->
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
    syntactic_put_merge(Mod, ModState, BKey, Obj1, ReqId, vclock:timestamp()).

syntactic_put_merge(Mod, ModState, BKey, Obj1, ReqId, StartTime) ->
    case Mod:get(ModState, BKey) of
        {error, notfound} -> {newobj, Obj1};
        {ok, Val0} ->
            Obj0 = binary_to_term(Val0),
            ResObj = riak_object:syntactic_merge(
                       Obj0,Obj1,term_to_binary(ReqId), StartTime), 
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
list_buckets(Caller, ReqId, Filter, Index, Mod, ModState) ->
    %% TODO: Decide if we want to continue to allow key filters
    %% to be used to filter the list of buckets. I think it is
    %% more useful to move all filtering out of the backend and 
    %% not have to force all backends to fold over all keys
    %% to generate a list of buckets.
    Buckets = Mod:list_bucket(ModState, '_'),
    case Filter of
        none ->
            gen_fsm:send_event(Caller, {ReqId, {final_results, {Index, node()}, Buckets}});
        _ ->
            FilteredBuckets = lists:foldl(Filter, [], Buckets),
            gen_fsm:send_event(Caller, {ReqId, {final_results, {Index, node()}, FilteredBuckets}})
    end.

%% @private
list_keys(Caller, ReqId, Bucket, Filter, Index, Mod, ModState) ->
    F = fun({_, _} = BKey, _Val, Acc) ->
                process_keys(Caller, ReqId, Index, Bucket, Filter, BKey, Acc);
           (Key, _Val, Acc) when is_binary(Key) ->
                %% Backend's fold gives us keys only, so add bucket.
                process_keys(Caller, ReqId, Index, Bucket, Filter, {Bucket, Key}, Acc)
        end,
    TryFuns = [fun() ->
                       %% Difficult to coordinate external backend API, so
                       %% we'll live with it for the moment/eternity.
                       F2 = fun(Key, Acc) ->
                            process_keys(Caller, ReqId, Index, Bucket,
                                         Filter, {Bucket, Key}, Acc)
                            end,
                       Mod:fold_bucket_keys(ModState, Bucket, F2)
               end,
               fun() ->
                       %% Newer backend API
                       Mod:fold_bucket_keys(ModState, Bucket, F, [])
               end,
               fun() ->
                       %% Older API for third-parties
                       Mod:fold(ModState, F, [])
               end],
    Keys = lists:foldl(fun(TryFun, try_next) ->
                                try TryFun() catch error:undef -> try_next end;
                           (_TryFun, Res) ->
                                Res
                        end, try_next, TryFuns),
    case Filter of
        none ->
            gen_fsm:send_event(Caller, {ReqId, {final_results, {Index, node()}, {Bucket, Keys}}});
        _ ->
            FilteredKeys = lists:foldl(Filter, [], Keys),
            gen_fsm:send_event(Caller, {ReqId, {final_results, {Index, node()}, {Bucket, FilteredKeys}}})
    end.

%% @private
do_delete(BKey, Mod, ModState) ->
    case Mod:delete(ModState, BKey) of
        ok ->
            del;
        {error, _Reason} ->
            fail
    end.

%% @private
process_keys(Caller, ReqId, Index, Bucket, Filter, {Bucket, Key}, Acc) ->
       buffer_key_result(Caller, ReqId, Bucket, Filter, Index, [Key | Acc]);
process_keys(_Caller, _ReqId, _Index, _Bucket, _Filter, {_B, _K}, Acc) ->
    Acc.

buffer_key_result(Caller, ReqId, Bucket, Filter, Index, Acc) ->
    %% Use arbitrary fixed buffer size of 100. Not 
    %% sure there is a good 'why' for that number.
    case length(Acc) >= 100 of
        true ->
            %% Filter the buffer keys as needed
            case Filter of
                none ->
                    gen_fsm:send_event(Caller, {ReqId, {results, {Index, node()}, {Bucket, Acc}}});    
                _ ->
                    FilteredKeys = lists:foldl(Filter, [], Acc),
                    case FilteredKeys of
                        [] ->
                            ok;
                        _ ->
                            gen_fsm:send_event(Caller, {ReqId, {results, {Index, node()}, {Bucket, FilteredKeys}}})
                    end
            end,
            %% Reset the buffer results are not duplicated
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

dummy_backend(BackendMod) ->
    Ring = riak_core_ring:fresh(16,node()),
    riak_core_ring_manager:set_ring_global(Ring),
    application:set_env(riak_kv, storage_backend, BackendMod),
    application:set_env(riak_core, default_bucket_props, []),
    application:set_env(bitcask, data_root, bitcask_test_dir()),
    application:set_env(riak_kv, riak_kv_dets_backend_root, dets_test_dir()),
    application:set_env(riak_kv, riak_kv_fs_backend_root, fs_test_dir()),
    application:set_env(riak_kv, multi_backend_default, multi_dummy_ets),
    application:set_env(riak_kv, multi_backend,
                        [{multi_dummy_ets, riak_kv_ets_backend, []},
                         {multi_dummy_gb, riak_kv_gb_trees_backend, []}]).

bitcask_test_dir() ->
    "./test.bitcask-temp-data".

dets_test_dir() ->
    "./test.dets-temp-data".

fs_test_dir() ->
    "./test.fs-temp-data".

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
    dets_server:stop(),
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
            {"cache list buckets",
                fun() ->
                    list_buckets_test_i(riak_kv_cache_backend)
                end
            }
        end,
        fun(_) ->
            {"dets list buckets",
                fun() ->
                    dets_server:stop(),
                    redbug:start({dets, apply_op}, [{msgs,100}, {print_file, "zoozoo"}]),
                    os:cmd("rm -rf " ++ dets_test_dir()),
                    list_buckets_test_i(riak_kv_dets_backend),
                    redbug:stop()
                end
            }
        end,
        fun(_) ->
            {"ets list buckets",
                fun() ->
                    list_buckets_test_i(riak_kv_ets_backend),
                    ok
                end
            }
        end,
        fun(_) ->
            {"fs list buckets",
                fun() ->
                    list_buckets_test_i(riak_kv_fs_backend),
                    ok
                end
            }
        end,
        fun(_) ->
            {"gb_trees list buckets",
                fun() ->
                    list_buckets_test_i(riak_kv_gb_trees_backend),
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
    Caller = new_result_listener(),
    handle_command(?KV_LISTKEYS_REQ{bucket='_',
                                    req_id=124,
                                    caller=Caller},
                   {raw, 456, self()}, S),
    ?assertEqual({ok, [B]}, results_from_listener(Caller)),
    flush_msgs().

filter_keys_test() ->
    {S, B, K} = backend_with_known_key(riak_kv_ets_backend),

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

must_be_last_cleanup_stuff_test() ->
    [application:unset_env(riak_kv, K) ||
        {K, _V} <- application:get_all_env(riak_kv)],
    [application:set_env(riak_kv, K, V) || {K, V} <- erlang:get({?MODULE, kv})].

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
