%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 Basho Technologies, Inc.  All Rights Reserved.
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

%% For example: what ETS tables are being called the most by ets:lookup/2?
%% The 1st arg of ets:lookup/2 is the table name.
%% Watch for 10 seconds.
%%
%% > func_args_tracer:start(ets, lookup, 2, 10, fun(Args) -> hd(Args) end).
%% 
%% Tracer pid: <0.16102.15>, use func_args_tracer:stop() to stop
%% Otherwise, tracing stops in 10 seconds
%% Current date & time: {2013,9,19} {18,5,48}
%% {started,<0.16102.15>}
%% Total calls: 373476 
%% Call stats:         
%% [{folsom_histograms,114065},
%%  {ac_tab,69689},
%%  {ets_riak_core_ring_manager,67147},
%%  {folsom_spirals,57076},
%%  {riak_capability_ets,48862},
%%  {riak_core_node_watcher,8149},
%%  {riak_api_pb_registrations,8144},
%%  {folsom,243},
%%  {folsom_meters,43},
%%  {folsom_durations,20},
%%  {timer_tab,18},
%%  {folsom_gauges,8},
%%  {riak_core_stat_cache,5},
%%  {sys_dist,3},
%%  {inet_db,1},
%%  {21495958,1},
%%  {3145765,1},
%%  {3407910,1}]
%% 

-module(tracer_func_args).
-compile(export_all).

start(Mod, Func, Arity, RunSeconds) ->
    start(Mod, Func, Arity, RunSeconds, fun(Args) -> Args end).

start(Mod, Func, Arity, RunSeconds, ArgMangler) ->
    catch ets:delete(foo),
    ets:new(foo, [named_table, public, set]),
    dbg:tracer(process, {fun trace/2, new_stats({foo, ArgMangler})}),
    dbg:p(all, call),
    dbg:tpl(Mod, Func, Arity, [{'_', [], []}]),
    
    {ok, TPid} = dbg:get_tracer(),
    io:format("Tracer pid: ~p, use ~p:stop() to stop\n", [TPid, ?MODULE]),
    io:format("Otherwise, tracing stops in ~p seconds\n", [RunSeconds]),
    io:format("Current date & time: ~p ~p\n", [date(), time()]),
    spawn(fun() -> timer:sleep(RunSeconds * 1000), stop() end),
    {started, TPid}.

stop() ->
    Sort = fun({_,A}, {_, B}) -> A > B end,
    Res = ets:tab2list(foo),
    TotalCalls = lists:sum([Count || {_Arg, Count} <- Res]),
    io:format("Total calls: ~p\n", [TotalCalls]),
    io:format("Call stats:\n~p\n", [catch lists:sort(Sort, Res)]),
    dbg:stop_clear(),
    catch exit(element(2,dbg:get_tracer()), kill),
    timer:sleep(100),
    stopped.

trace({trace, _Pid, call, {_, _, Args}}, {Tab, ArgMangler} = Acc) ->
    Args2 = ArgMangler(Args),
    try
        ets:update_counter(Tab, Args2, {2, 1})
    catch _:_ ->
            ets:insert(Tab, {Args2, 1})
    end,
    Acc;
trace(Unknown, DictStats) ->
    io:format("Unknown! ~P\n", [Unknown, 20]),
    DictStats.

new_stats({Tab, _ArgMangler} = Acc) ->
    ets:delete_all_objects(Tab),
    Acc.

print_stats(_DictStats) ->
    ok.

