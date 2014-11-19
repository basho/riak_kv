%%% @author Russell Brown <russelldb@basho.com>
%%% @copyright (C) 2014, Russell Brown
%%% @doc
%%%
%%% @end
%%% Created : 14 Nov 2014 by Russell Brown <russelldb@basho.com>

-module(kv_vnode_status_mgr_eqc).


-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").

-compile(export_all).

-record(state,{}).

-define(MAX_INT, ((1 bsl 32) -1)).

%% @doc Returns the state in which each test case starts. (Unless a different
%%      initial state is supplied explicitly to, e.g. commands/2.)
-spec initial_state() -> eqc_statem:symbolic_state().
initial_state() ->
    #state{}.


%% ------ Grouped operator: lease_counter
lease_counter_args(_S) ->
    [?SUCHTHAT(Lease, ?LET(I, largeint(), abs(I)), Lease =< ?MAX_INT)].

lease_counter(Lease) ->
    %% @TODO (rdb) handle the 32 bit threshold, roll over to new
    %% id. Basics first.
    [{status, Id, MoCnt, Pid}] = ets:lookup(vnode_status, status),
    NewLease = MoCnt + Lease,
    {_, _, NewCntrModel, _}=NewRec = case {MoCnt == ?MAX_INT, NewLease >= ?MAX_INT} of
                 {true, _} ->
                     {status, Id, Lease, Pid};
                 {false, true} ->
                     {status, Id, ?MAX_INT, Pid};
                 {false, false} ->
                     {status, Id, NewLease, Pid}
             end,
    ets:insert(vnode_status, NewRec),
    ok = riak_kv_vnode_status_mgr:lease_counter(Pid, Lease),
    NewCntr = receive
                  {counter_lease, {Pid, _VnodeId, NewLease}} ->
                      NewLease
              after
                  5000 -> %% 5 seconds (is this ok?)
                      timeout
              end,
    {NewCntrModel, NewCntr}.

%% @doc lease_counter_post - Postcondition for lease_counter
-spec lease_counter_post(S :: eqc_statem:dynamic_state(),
                         Args :: [term()], R :: term()) -> true | term().
lease_counter_post(_S, _Args, {Cnt, Cnt}) ->
    true;
lease_counter_post(_S, _Args, {MoCnt, Cnt}) ->
    {postcondition_failed, "Ets and Disk don't match", MoCnt, Cnt}.

%% @doc weight/2 - Distribution of calls
-spec weight(S :: eqc_statem:symbolic_state(), Command :: atom()) -> integer().
weight(_S, lease_counter) -> 1;
weight(_S, _Cmd) -> 1.

%% @doc Default generated property
-spec prop_monotonic() -> eqc:property().
prop_monotonic() ->
    ?FORALL(Cmds, commands(?MODULE),
            begin
                ets:new(vnode_status, [named_table, set]),
                {ok, Pid} = riak_kv_vnode_status_mgr:start_link(self(), 1),
                ets:insert(vnode_status, {status, <<>>, 0, Pid}),
                {H, S, Res} = run_commands(?MODULE,Cmds),
                [{status, _, MoCntr, Pid}] = ets:lookup(vnode_status, status),
                {ok, Status} = riak_kv_vnode_status_mgr:status(Pid),
                Cnt = proplists:get_value(counter, Status),

                ets:delete(vnode_status),
                riak_kv_vnode_status_mgr:clear_vnodeid(Pid),
                ok = riak_kv_vnode_status_mgr:stop(Pid),

                aggregate(command_names(Cmds),
                          pretty_commands(?MODULE, Cmds, {H, S, Res},
                                          conjunction([{result, equals(Res, ok)},
                                                        {values, equals(MoCntr, Cnt)}
                                                      ])
                                         )
                         )
            end).
