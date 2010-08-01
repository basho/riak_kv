%% -------------------------------------------------------------------
%%
%% riak_js_manager: dispatch work to JavaScript VMs
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

%% @doc dispatch work to JavaScript VMs
-module(riak_kv_js_manager).

-behaviour(gen_server).

%% API
-export([start_link/1,
         add_vm/0,
         reload/0,
         reload/1,
         mark_idle/0,
         reserve_vm/0,
         dispatch/2,
         blocking_dispatch/2,
         pool_size/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-define(SERVER, ?MODULE).

-record('DOWN', {ref, type, pid, info}).
-record(vm_state, {pid, needs_reload=false}).
-record(state, {master, idle, reserve}).

start_link(ChildCount) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [ChildCount], []).

reload([]) ->
    reload().
reload() ->
    gen_server:call(?SERVER, reload_vms).

add_vm() ->
    gen_server:cast(?SERVER, {add_vm, self()}).

mark_idle() ->
    gen_server:call(?SERVER, {mark_idle, self()}).

dispatch(JSCall, Tries) ->
    dispatch(JSCall, Tries, Tries).

blocking_dispatch(JSCall, Tries) ->
    blocking_dispatch(JSCall, Tries, Tries).

reserve_vm() ->
    gen_server:call(?SERVER, reserve_vm).

pool_size() ->
    gen_server:call(?SERVER, pool_size).

init([ChildCount]) ->
    Master = ets:new(jsvm_master, [private, {keypos, 2}]),
    Idle = ets:new(jsvm_idle, [private]),
    start_vms(ChildCount),
    {ok, #state{master=Master, idle=Idle}}.

handle_call({mark_idle, VM}, _From, #state{master=Master,
                                           idle=Idle}=State) ->
    case needs_reload(Master, VM) of
        true ->
            riak_kv_js_vm:reload(VM),
            clear_reload(Master, VM);
        false ->
            ok
    end,
    ets:insert(Idle, {VM}),
    {reply, ok, State};

handle_call(reload_vms, _From, #state{master=Master, idle=Idle}=State) ->
    reload_idle_vms(Idle),
    mark_pending_reloads(Master, Idle),
    riak_kv_vnode:purge_mapcaches(),
    {reply, ok, State};

handle_call(reserve_vm, _From, #state{idle=Idle}=State) ->
    try
        ets:safe_fixtable(Idle, true),
        Reply = case ets:first(Idle) of
                    '$end_of_table' ->
                        {error, no_vms};
                    VM ->
                        ets:delete(Idle, VM),
                        {ok, VM}
                end,
        {reply, Reply, State}
    after
        ets:safe_fixtable(Idle, false)
    end;

handle_call(pool_size, _From, #state{idle=Idle}=State) ->
    {reply, ets:info(Idle, size), State};

handle_call(_Request, _From, State) ->
    {reply, ignore, State}.

handle_cast({add_vm, VMPid}, #state{master=Master, idle=Idle}=State) ->
    erlang:monitor(process, VMPid),
    VMState = #vm_state{pid=VMPid},
    ets:insert(Master, VMState),
    ets:insert(Idle, {VMPid}),
    {noreply, State};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(#'DOWN'{pid=Pid}, #state{master=Master, idle=Idle}=State) ->
    ets:delete(Master, Pid),
    ets:delete(Idle, Pid),
    riak_kv_js_sup:start_js(self()),
    {noreply, State};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Internal functions
needs_reload(Master, VMPid) ->
    [VMState] = ets:lookup(Master, VMPid),
    VMState#vm_state.needs_reload.

clear_reload(Master, VMPid) ->
    [VMState] = ets:lookup(Master, VMPid),
    VMState1 = VMState#vm_state{needs_reload=false},
    ets:insert(Master, VMState1).

is_vm_idle(Idle, VMPid) ->
    case ets:lookup(Idle, {VMPid}) of
        [] ->
            false;
        _ ->
            true
    end.

start_vms(0) ->
    ok;
start_vms(Count) ->
    riak_kv_js_sup:start_js(self()),
    start_vms(Count - 1).

reload_idle_vms(Tid) ->
    try
        ets:safe_fixtable(Tid, true),
        reload_idle_vms(ets:first(Tid), Tid)
    after
        ets:safe_fixtable(Tid, false)
    end.

reload_idle_vms('$end_of_table', _Tid) ->
    ok;
reload_idle_vms(Current, Tid) ->
    riak_kv_js_vm:reload(Current),
    reload_idle_vms(ets:next(Tid), Tid).

mark_pending_reloads(Master, Idle) ->
    try
        ets:safe_fixtable(Master, true),
        mark_pending_reloads(ets:first(Master), Master, Idle)
    after
        ets:safe_fixtable(Master, false)
    end.

mark_pending_reloads('$end_of_table', _Master, _Idle) ->
    ok;
mark_pending_reloads(VMState, Master, Idle) ->
    case is_vm_idle(Idle, VMState#vm_state.pid) of
        true ->
            ok;
        false ->
            VMState1 = VMState#vm_state{needs_reload=true},
            ets:insert(Master, VMState1)
    end,
    mark_pending_reloads(ets:next(Master), Master, Idle).

dispatch(_JSCall, _MaxCount, 0) ->
    error_logger:info_msg("JS call failed: All VMs are busy.~n"),
    {error, no_vms};
dispatch(JSCall, MaxCount, Count) ->
    case reserve_vm() of
        {ok, VM} ->
            JobId = {VM, make_ref()},
            riak_kv_js_vm:dispatch(VM, self(), JobId, JSCall),
            {ok, JobId};
        {error, no_vms} ->
             ScalingFactor = (1 + (MaxCount - Count)) *
                (0.1 + random:uniform(100) * 0.001),
            timer:sleep(erlang:round(500 * ScalingFactor)),
            dispatch(JSCall, MaxCount, Count - 1)
    end.

blocking_dispatch(_JSCall, _MaxCount, 0) ->
    error_logger:info_msg("JS call failed: All VMs are busy.~n"),
    {error, no_vms};
blocking_dispatch(JSCall, MaxCount, Count) ->
    case reserve_vm() of
        {ok, VM} ->
            JobId = {VM, make_ref()},
            riak_kv_js_vm:blocking_dispatch(VM, JobId, JSCall);
        {error, no_vms} ->
            ScalingFactor = (1 + (MaxCount - Count)) *
                (0.1 + random:uniform(100) * 0.001),
            timer:sleep(erlang:round(500 * ScalingFactor)),
            blocking_dispatch(JSCall, MaxCount, Count - 1)
    end.
