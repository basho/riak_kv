%% -------------------------------------------------------------------
%%
%% riak_kv_get_put_monitor:  Oberserve and stat log get/put fsm's
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

%% @doc Monitor reporting processes and notify folsom about live and error
%% exits. It is up to the fsm to report it's existence to the monitor. If this
%% module exits unexpectadly, the stats in folsom are not lost; a clean exit
%% will reset the stats.
%%
%% The stat names are all 4 element tuples, the first two elements being
%% 'riak_kv', then the local node. The third is either 'get' or 'put' depending
%% on if the fsm reporting in was for a get or a put action.  The final element
%% is either 'active' or 'errors'.
%%
%% The 'active' is a simple counter reporting the number of get or put fsm's
%% currently alive.  'errors' is a spiral for all fsm's that exited with a
%% reason other than normal or shutdown.
-module(riak_kv_get_put_monitor).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([get_fsm_spawned/1, put_fsm_spawned/1,
         gets_active/0, puts_active/0, spawned/2]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------


%% @doc Begin monitoring the passed pid and tag its stats for a get fsm.
-spec get_fsm_spawned(Pid :: pid()) -> 'ok'.
get_fsm_spawned(Pid) ->
    spawn(?MODULE, spawned, [gets, Pid]).

spawned(Type, Pid) ->
    MonRef = monitor(process, Pid),
    riak_kv_stat:update({fsm_spawned, Type}),
    monitor_loop(MonRef, Pid, Type).

monitor_loop(MonRef, Pid, Type) ->
    receive
        {'DOWN', MonRef, process, Pid, Cause}
          when Cause == normal; Cause == shutdown; Cause == noproc ->
            riak_kv_stat:update({fsm_exit, Type});
        {'DOWN', MonRef, process, Pid, _Cause} ->
            riak_kv_stat:update({fsm_error, Type});
        _ ->
            monitor_loop(MonRef, Pid, Type)
    end.

%% @doc Begin monitoring the passed pid and tag its stats for a put fsm.
-spec put_fsm_spawned(Pid :: pid()) -> 'ok'.
put_fsm_spawned(Pid) ->
    spawn(?MODULE, spawned, [puts, Pid]).

%% Returns the last count for the get fms's in progress.
-spec gets_active() -> non_neg_integer().
gets_active() ->
    riak_kv_stat:active_gets().

%% Returns the last count for the put fms's in progress.
-spec puts_active() -> non_neg_integer().
puts_active() ->
    riak_kv_stat:active_puts().
