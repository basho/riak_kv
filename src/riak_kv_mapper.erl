%% -------------------------------------------------------------------
%%
%% riak_kv_mapper: Executes map functions on input batches
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
-module(riak_kv_mapper).

-behaviour(gen_fsm).

-include_lib("riak_kv_map_phase.hrl").
-include_lib("riak_kv_js_pools.hrl").

-define(READ_TIMEOUT, 30000).

%% API
-export([start_link/5]).

%% States
-export([prepare/2,
         recv_data/2,
         do_map/2]).

%% gen_fsm callbacks
-export([init/1,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4]).

-record(state, {id,
                cache_ref,
                cache_key_base,
                vnode,
                vm,
                qterm,
                pending,
                reqid,
                data=[],
                inputs,
                phase}).

start_link(VNode, Id, QTerm, MapInputs, PhasePid) ->
    gen_fsm:start_link(?MODULE, [VNode, Id, QTerm, MapInputs, PhasePid], []).

init([VNode, Id, QTerm0, MapInputs, PhasePid]) ->
    QTermFun = xform_link_walk(QTerm0),
    {_, _, ReqId} = erlang:now(),
    gen_fsm:send_event(PhasePid, {register_mapper, Id, self()}),
    erlang:link(PhasePid),
    riak_kv_stat:update(mapper_start),
    {ok, CacheRef} = riak_kv_mapred_cache:cache_ref(),
    CacheKeyBase = generate_cache_key_base(QTermFun(undefined)),
    {ok, VM} = reserve_jsvm(QTermFun(undefined)),
    %% we need some way to reclaim the JS VM if it is busy doing something
    %% when the rest of the MapReduce phase exists (e.g. on timeout)
    %% easiest method is simply to link, such that the VM is also killed,
    %% which will cause the supervisor to spin up a fresh one
    if is_pid(VM) -> erlang:link(VM);
       true       -> ok %% erlang phases do not use VMs
    end,
    {ok, prepare, #state{id=Id, vnode=VNode, qterm=QTermFun, inputs=MapInputs,
                         cache_key_base=CacheKeyBase, reqid=ReqId, phase=PhasePid,
                         cache_ref=CacheRef, vm=VM}, 0}.

prepare(timeout, State) ->
    case fetch_cached_results(State) of
        done ->
            {stop, normal, State};
        State1 ->
            case fetch_data(State1) of
                done ->
                    {stop, normal, State1};
                NewState ->
                    {next_state, recv_data, NewState}
            end
    end.

recv_data({r, Result, _Idx, ReqId}, #state{reqid=ReqId, data=Data0,
                                           pending=Pending}=State) ->
    Data = [Result|Data0],

    %% When we receive all data for the keys we sent out
    %% switch to "map mode" and evaluate the map function
    case length(Data) == length(Pending) of
        false ->
            {next_state, recv_data, State#state{data=Data}, ?READ_TIMEOUT};
        true ->
            {next_state, do_map, State#state{data=Data}, 0}
    end;
recv_data(timeout, #state{phase=Phase, id=Id}=State) ->
    riak_kv_phase_proto:mapexec_error(Phase, {error, read_timeout}, Id),
    {stop, normal, State#state{data=[], pending=[], inputs=[]}}.

do_map(timeout, #state{data=Data, vm=VM, qterm=QTermFun, pending=Pending,
                       phase=Phase, id=Id, cache_key_base=CacheKeyBase, vnode=VNode, cache_ref=CacheRef}=State) ->
    lists:foldl(fun(Obj, WorkingSet) ->
                        case find_input(obj_bkey(Obj), WorkingSet) of
                            {none, WorkingSet} ->
                                WorkingSet;
                            {Input, WorkingSet1} ->
                                case QTermFun(Input) of
                                    {error, Error} ->
                                        riak_kv_phase_proto:mapexec_error(Phase, {error, Error}, Id);
                                    QTerm ->
                                        CacheKey = generate_final_cachekey(CacheKeyBase,
                                                                           Input#riak_kv_map_input.kd),
                                        run_map(VNode, Id, QTerm,
                                                Input#riak_kv_map_input.kd,
                                                Obj, Phase, VM, CacheKey, CacheRef)
                                end,
                                WorkingSet1
                        end end, Pending, Data),
    case fetch_data(State) of
        done ->
            {stop, normal, State};
        NewState ->
            {next_state, recv_data, NewState#state{data=[]}}
    end.

handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

handle_sync_event(_Event, _From, StateName, State) ->
    {reply, ignored, StateName, State}.

handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

terminate(_Reason, _StateName, #state{vm=VM, id=_Id, vnode=_VNode, reqid=_ReqId, phase=_PhasePid}=_State) ->
    release_jsvm(VM),
    riak_kv_stat:update(mapper_end),
    _Reason.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%% Internal functions
fetch_data(#state{inputs=[]}) ->
    done;
fetch_data(#state{inputs=Inputs, reqid=ReqId,
                  vnode=VNode}=State) ->
    {Current, Next} = split(Inputs),
    BKeys = [Input#riak_kv_map_input.bkey || Input <- Current],
    riak_kv_vnode:mget(VNode, BKeys, ReqId),
    State#state{inputs=Next, pending=Current}.

reserve_jsvm({erlang, _}) ->
    {ok, undefined};
reserve_jsvm({javascript, _}) ->
    riak_kv_js_manager:reserve_batch_vm(?JSPOOL_MAP, 10).

release_jsvm(undefined) ->
    ok;
release_jsvm(VM) when is_pid(VM) ->
    riak_kv_js_vm:finish_batch(VM).

obj_bkey({{error, notfound},Bkey}) ->
    Bkey;
obj_bkey(Obj) ->
    {riak_object:bucket(Obj), riak_object:key(Obj)}.

find_input(BKey, WorkingSet) ->
    find_input(BKey, WorkingSet, WorkingSet).

find_input(_BKey, [], CompleteSet) ->
    {none, CompleteSet};
find_input(BKey, [#riak_kv_map_input{bkey=BKey}=H|_], CompleteSet) ->
    {H, lists:delete(H, CompleteSet)};
find_input(BKey, [_|T], CompleteSet) ->
    find_input(BKey, T, CompleteSet).

run_map(VNode, Id, {erlang, {map, FunTerm, Arg, _}}, KD, Obj0, Phase, _VM, CacheKey, CacheRef) ->
    Obj = case Obj0 of
              {{error,notfound},_} ->
                  {error, notfound};
              _ ->
                  Obj0
          end,
    BKey = obj_bkey(Obj0),
    Result = try
                 case FunTerm of
                     {qfun, F} ->
                         {ok, (F)(Obj, KD, Arg)};
                     {modfun, M, F} ->
                         {ok, M:F(Obj, KD, Arg)}
                 end
             catch C:R ->
                     Reason = {C, R, erlang:get_stacktrace()},
                     {error, Reason}
             end,
    case Result of
        {ok, Value} ->
            riak_kv_phase_proto:mapexec_result(Phase, VNode, obj_bkey(Obj0), Value, Id),
            if
                is_list(Value) ->
                    case CacheKey of
                        not_cached ->
                            ok;
                        _ ->
                            riak_kv_lru:put(CacheRef, BKey, CacheKey, Value)
                    end;
                true ->
                    ok
            end;
        {error, _} ->
            riak_kv_phase_proto:mapexec_error(Phase, Result, Id)
    end;

run_map(VNode, Id, {javascript, {map, _FunTerm, _Arg, _}}, KD, {{error, notfound},_}=Obj, Phase, _VM, _CacheKey, _CacheRef) ->
    BKey = obj_bkey(Obj),
    riak_kv_phase_proto:mapexec_result(
      Phase, VNode, BKey, [{not_found, BKey, KD}], Id);
run_map(VNode, Id, {javascript, {map, FunTerm, Arg, _}}, KD, Obj, Phase, VM, CacheKey, CacheRef) ->
    BKey = {riak_object:bucket(Obj), riak_object:key(Obj)},
    JSArgs = [riak_object:to_json(Obj), KD, Arg],
    JSCall = {map, FunTerm, JSArgs},
    case riak_kv_js_vm:batch_blocking_dispatch(VM, JSCall) of
        {ok, Result} ->
            riak_kv_phase_proto:mapexec_result(Phase, VNode, obj_bkey(Obj), Result, Id),
            if
                is_list(Result) ->
                    case CacheKey of
                        not_cached ->
                            ok;
                        _ ->
                            riak_kv_lru:put(CacheRef, BKey, CacheKey, Result)
                    end;
                true ->
                    ok
            end;
        Error ->
            riak_kv_phase_proto:mapexec_error(Phase, Error, Id)
    end.

split(L) when length(L) =< 5 ->
    {L, []};
split(L) ->
    lists:split(5, L).

generate_cache_key_base({erlang, {map, {modfun, Mod, Fun}, Arg, _}}) ->
    term_to_binary([Mod, Fun, Arg], [compressed]);
generate_cache_key_base({erlang, _}) ->
    not_cached;
generate_cache_key_base({javascript, {map, {jsanon, Source}, Arg, _}}) ->
    term_to_binary([Source, Arg], [compressed]);
generate_cache_key_base({javascript, {map, {jsfun, Name}, Arg, _}}) ->
    term_to_binary([Name, Arg]).

generate_final_cachekey(not_cached, _KD) ->
    not_cached;
generate_final_cachekey(CacheKey, KD) ->
    CacheKey1 = list_to_binary([CacheKey, term_to_binary(KD)]),
    mochihex:to_hex(crypto:sha(CacheKey1)).

fetch_cached_results(#state{cache_key_base=not_cached}=State) ->
    State;
fetch_cached_results(#state{vnode=VNode, id=Id, phase=Phase, cache_ref=CacheRef,
                            cache_key_base=CacheKeyBase, inputs=Inputs}=State) ->
    case fetch_cached_results(VNode, Id, Phase, CacheRef, CacheKeyBase, Inputs, []) of
        done ->
            done;
        Remainder ->
            State#state{inputs=Remainder}
    end.

fetch_cached_results(_VNode, _Id, _Phase, _CacheRef, _CacheKeyBase, [], []) ->
    done;
fetch_cached_results(_VNode, _Id, _Phase, _CacheRef, _CacheKeyBase, [], Accum) ->
    Accum;
fetch_cached_results(VNode, Id, Phase, CacheRef, CacheKeyBase, [#riak_kv_map_input{bkey=BKey, kd=KD}=H|T], Accum) ->
    CacheKey = generate_final_cachekey(CacheKeyBase, KD),
    case riak_kv_lru:fetch(CacheRef, BKey, CacheKey) of
        notfound ->
            fetch_cached_results(VNode, Id, Phase, CacheRef, CacheKey, T, [H|Accum]);
        Result ->
            riak_kv_phase_proto:mapexec_result(Phase, VNode, BKey, Result, Id),
            fetch_cached_results(VNode, Id, Phase, CacheRef, CacheKey, T, Accum)
    end.

xform_link_walk({erlang, {link, LB, LT, LAcc}}=QTerm) ->
    fun(Input) ->
            case Input of
                undefined ->
                    QTerm;
                _ ->
                    case proplists:get_value(linkfun, Input#riak_kv_map_input.bprops) of
                        undefined ->
                            {error, missing_linkfun};
                        LinkFun ->
                            {erlang, {map, LinkFun, {LB, LT}, LAcc}}
                    end
            end end;
xform_link_walk(QTerm) ->
    fun(_) -> QTerm end.
