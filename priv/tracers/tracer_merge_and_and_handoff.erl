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

-module(tracer_merge_and_and_handoff).
-compile(export_all).

start() ->
    start(1*1000).

start(Interval) ->
    dbg:tracer(process, {fun trace/2, {orddict:new(), orddict:new()}}),
    dbg:p(all, call),
    dbg:tpl(bitcask, merge_single_entry, 6, [{'_', [], []}]),
    dbg:tpl(riak_kv_vnode, encode_handoff_item, 2, [{'_', [], []}]),
    dbg:tpl(riak_core_handoff_receiver, process_message, 3, [{'_', [], []}]),
    
    %% Don't need return_trace events for this use case, but here's
    %% how to do it if needed.
    %%dbg:tpl(bitcask, merge_single_entry, 6, [{'_', [], [{return_trace}]}]).

    {ok, TPid} = dbg:get_tracer(),
    io:format("Tracer pid: ~p, use ~p:stop() to stop\n", [TPid, ?MODULE]),
    timer:send_interval(Interval, TPid, print_report),
    {started, TPid}.

stop() ->
    dbg:stop_clear(),
    catch exit(element(2,dbg:get_tracer()), kill),
    stopped.

trace({trace, _Pid, call, {bitcask, merge_single_entry,
                           [K, V, _TS, _FId, {File,_,_,_}, _State]}},
      {MDict, HDict}) ->
    Dir = re:replace(File, "/[^/]*\$", "", [{return, binary}]),
    Bytes = size(K) + size(V),
    MDict2 = increment_cbdict(MDict, Dir, Bytes),
    {MDict2, HDict};
trace({trace, _Pid, call, {riak_kv_vnode, encode_handoff_item,
                           [{B, K}, V]}},
      {MDict, HDict}) ->
    Bytes = size(B) + size(K) + size(V),
    Key = "all-sending-handoff",
    HDict2 = increment_cbdict(HDict, Key, Bytes),
    {MDict, HDict2};
trace({trace, _Pid, call, {riak_core_handoff_receiver, process_message,
                           [_Type, Msg, State]}},
      {MDict, HDict}) ->
    Bytes = size(Msg),
    Partition = element(5, State),              % ugly hack
    Key = Partition,
    HDict2 = increment_cbdict(HDict, Key, Bytes),
    {MDict, HDict2};
trace(print_report, {MDict, HDict}) ->
    print_stats(MDict, merge),
    print_stats(HDict, handoff),
    {orddict:new(), orddict:new()}.

%% "cb" = count + bytes
increment_cbdict(Dict, Key, Bytes) ->
    orddict:update(Key, fun({Count, Bs}) -> {Count + 1, Bs + Bytes} end,
                   {1, Bytes}, Dict).

print_stats(Dict, Type) ->
    F = fun(Key, {Count, Bytes}, {SumC, SumB}) when Count > 0 ->
                io:format("~p ~p: ~p items ~p bytes ~p avg-size ~p\n",
                          [date(), time(), Count, Bytes, Bytes div Count, Key]),
                {SumC + Count, SumB + Bytes};
           (_, _, Acc) ->
                Acc
        end,
    {Count, Bytes} = orddict:fold(F, {0, 0}, Dict),
    Avg = if Count > 0 -> Bytes div Count;
             true      -> 0
          end,
    io:format("~p ~p: ~p total: ~p items ~p bytes ~p avg-size\n",
              [date(), time(), Type, Count, Bytes, Avg]).
