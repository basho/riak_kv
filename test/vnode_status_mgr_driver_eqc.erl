-module(vnode_status_mgr_driver_eqc).
%% -------------------------------------------------------------------
%%
%% vnode_status_mgr_driver_eqc: An eqc that exercises the
%% `vnode_status_mgr_driver' API
%%
%% Copyright (c) 2007-2014 Basho Technologies, Inc.  All Rights Reserved.
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

-ifdef(EQC).

-include("vnode_status_mgr_test.hrl").
-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).
-eqc_group_commands(true).

-record(test_state, {driver_pid :: pid(),
                     status_mgr_pid :: pid(),
                     lease_size :: pos_integer(),
                     lease :: pos_integer(),
                     previous_lease :: pos_integer(),
                     next_increment=1 :: pos_integer(),
                     increments :: pos_integer(),
                     counter=0 :: non_neg_integer(),
                     cleared=false :: boolean(),
                     current_result :: term(),
                     previous_result :: term()}).
-define(STATE, #test_state).

-spec prop_driver_api() -> eqc:property().
prop_driver_api() ->
    ?FORALL(Cmds,
            non_empty(noshrink(commands(?MODULE))),
            begin
                %% TODO: Much less kludgy way of cleanup
                file:delete("undefined/kv_vnode/0"),
                {ok, Pid} = vnode_status_mgr_driver:start_link(10),
                MgrPid = status_mgr_pid(vnode_status_mgr_driver:status(Pid)),
                {H, S, Res} = run_commands(?MODULE, Cmds, [{driver_pid, Pid},
                                                           {status_mgr_pid, MgrPid},
                                                           {lease_size, 10}]),
                aggregate(command_names(Cmds),
                          pretty_commands(?MODULE,
                                          Cmds,
                                          {H, S, Res},
                                          equals(ok, Res)))
            end).


-spec initial_state() -> eqc_statem:symbolic_state().
initial_state() ->
    ?STATE{driver_pid={var, driver_pid},
           status_mgr_pid={var, status_mgr_pid},
           lease_size={var, lease_size},
           lease={var, lease_size}}.

increment_pre(S, _Args) ->
    S?STATE.driver_pid =/= undefined
        andalso not S?STATE.cleared.

increment_args(S) ->
    [
     S?STATE.driver_pid,
     S?STATE.next_increment
    ].

increment(Pid, Increments) ->
    vnode_status_mgr_driver:increment_counter(Pid, Increments).

increment_next(S, R, [_, Increments]) ->
    Counter = S?STATE.counter,
    Lease = S?STATE.lease,
    LeaseSize = S?STATE.lease_size,
    UpdLease = case Counter =:= Lease of
                   true ->
                       Lease + LeaseSize;
                   false ->
                       Lease
               end,
    NextIncrement=1,
    S?STATE{counter=Counter+Increments,
            current_result=R,
            lease=UpdLease,
            previous_lease=Lease,
            previous_result=S?STATE.current_result,
            next_increment=NextIncrement}.

%% Cases to check:
%% * Increment just below 80% counter threshold, verify counter_state has only incremented as expected and status mgr status is the same
%% * Increment to 80% threshold, verify counter_state has leasing set to true. can't make assumptions about status_mgr state.
%% * Increment counter to max to engage blocking for counter_lease if necessary, verify counter_state lease info matches status from status_mgr.
increment_post(?STATE{counter=Counter,
                      lease_size=LeaseSize,
                      previous_lease=Lease},
               [_, _Increments],
               {DriverStatus, MgrStatus})
  when (Lease - Counter) > trunc(0.2 * LeaseSize) + 1 ->
    Counter =:= driver_counter(DriverStatus)
        andalso driver_and_mgr_lease_match(DriverStatus, MgrStatus)
        andalso not leasing(DriverStatus);
increment_post(?STATE{counter=Counter,
                      lease_size=LeaseSize,
                      previous_lease=Lease},
               [_, _Increments],
               {DriverStatus, _MgrStatus})
  when (Lease - Counter) =:= trunc(0.2 * LeaseSize) + 1 ->
    Counter =:= driver_counter(DriverStatus)
        andalso not leasing(DriverStatus);
increment_post(?STATE{counter=Counter,
                      lease_size=LeaseSize,
                      previous_lease=Lease,
                      previous_result={OldDriverStatus, OldMgrStatus}},
               [_, _Increments],
               {DriverStatus, MgrStatus})
  when (Lease - Counter) =:= trunc(0.2 * LeaseSize) ->
    Counter =:= driver_counter(DriverStatus)
        andalso status_mgr_lease_changed(OldMgrStatus, MgrStatus)
        andalso driver_lease_changed(OldDriverStatus, DriverStatus);
increment_post(?STATE{counter=Counter,
                      previous_lease=Lease,
                      previous_result={_OldDriverStatus, _OldMgrStatus}},
               [_, _Increments],
               {DriverStatus, MgrStatus})
  when Lease =:= Counter ->
    Counter =:= driver_counter(DriverStatus)
        andalso driver_and_mgr_lease_match(DriverStatus, MgrStatus)
        andalso not leasing(DriverStatus);
increment_post(_S, _Args, _R) ->
    true.

clear_pre(S, _Args) ->
    S?STATE.driver_pid =/= undefined.

clear_args(S) ->
    [S?STATE.driver_pid].

clear(DriverPid) ->
    vnode_status_mgr_driver:clear_counter(DriverPid).

clear_next(S, _V, _A) ->
    S?STATE{cleared=true}.

clear_post(?STATE{counter = Counter},
           _Args,
           {DriverStatus, MgrStatus}) ->
    Counter =:= driver_counter(DriverStatus)
        andalso counter_cleared(MgrStatus)
        andalso vnodeid_cleared(MgrStatus).

stop_pre(S, _Args) ->
    S?STATE.driver_pid =/= undefined.

stop_args(S) ->
    [S?STATE.driver_pid].

stop(DriverPid) ->
    vnode_status_mgr_driver:stop(DriverPid).

stop_next(S, _V, _A) ->
    S?STATE{driver_pid=undefined,
            status_mgr_pid=undefined}.

stop_post(S, _Args, ok) ->
    not (is_process_alive(S?STATE.driver_pid) orelse
         is_process_alive(S?STATE.status_mgr_pid));
stop_post(_S, _Args, _) ->
    false.

kill_status_mgr_pre(S, _Args) ->
    S?STATE.driver_pid =/= undefined.

kill_status_mgr_args(S) ->
    [S?STATE.driver_pid,
     S?STATE.status_mgr_pid].

kill_status_mgr(DriverPid, MgrPid) ->
    _ = exit(MgrPid, kill),
    %%TODO Replace this with a driver API call to wait until the EXIT
    %%message is handled and avoid postcondition failures.
    timer:sleep(200),
    vnode_status_mgr_driver:status(DriverPid).

kill_status_mgr_next(S, _R, _A) ->
    S.

kill_status_mgr_post(?STATE{counter=Counter},
                     [_, OldMgrPid],
                     DriverStatus) ->
    Counter =:= driver_counter(DriverStatus)
        andalso not is_process_alive(OldMgrPid)
        andalso is_process_alive(status_mgr_pid(DriverStatus))
        andalso not leasing(DriverStatus);
kill_status_mgr_post(_S, _Args, _) ->
    false.

-spec weight(S :: eqc_statem:symbolic_state(), Command :: atom()) -> integer().
weight(_S, increment) -> 25;
weight(_S, clear) -> 2;
weight(_S, stop) -> 1;
weight(_S, kill_status_mgr) -> 3.

%%%===================================================================
%%% wee little helpers
%%%===================================================================

-spec driver_counter(driver_state()) -> non_neg_integer().
driver_counter(Status) ->
    Status#state.counter#counter_state.cnt.

-spec status_mgr_lease_changed(orddict:orddict(), orddict:orddict()) -> boolean().
status_mgr_lease_changed(Status, Status) ->
    false;
status_mgr_lease_changed(OldStatus, CurrentStatus) ->
    not (element(2, orddict:find(counter, orddict:from_list(OldStatus))) =:=
             element(2, orddict:find(counter, orddict:from_list(CurrentStatus)))).

-spec driver_lease_changed(driver_state(), driver_state()) -> boolean().
driver_lease_changed(Status, Status) ->
    false;
driver_lease_changed(OldStatus, CurrentStatus) ->
    not (OldStatus#state.counter#counter_state.lease =:=
             CurrentStatus#state.counter#counter_state.lease).

-spec driver_and_mgr_lease_match(driver_state(), orddict:orddict()) -> boolean().
driver_and_mgr_lease_match(DriverStatus, MgrStatus) ->
    DriverStatus#state.counter#counter_state.lease =:=
        element(2, orddict:find(counter, orddict:from_list(MgrStatus))).

-spec leasing(driver_state()) -> boolean().
leasing(Status) ->
    Status#state.counter#counter_state.leasing.

-spec counter_cleared(orddict:orddict()) -> boolean().
counter_cleared(Status) ->
    orddict:find(counter, orddict:from_list(Status)) =:= error.

-spec vnodeid_cleared(orddict:orddict()) -> boolean().
vnodeid_cleared(Status) ->
    orddict:find(vnodeid, orddict:from_list(Status)) =:= error.

-spec status_mgr_pid(driver_state()) -> pid().
status_mgr_pid(DriverStatus) ->
    DriverStatus#state.status_mgr_pid.

-endif.
