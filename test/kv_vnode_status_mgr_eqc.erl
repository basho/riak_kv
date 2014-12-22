%%% @author Russell Brown <russelldb@basho.com>
%%% @copyright (C) 2014, Russell Brown
%%% @doc
%%%
%%% @end
%%% Created : 14 Nov 2014 by Russell Brown <russelldb@basho.com>

-module(kv_vnode_status_mgr_eqc).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

-record(state,{}).

-define(NUMTESTS, 1000).
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) ->
                              io:format(user, Str, Args) end, P)).

-define(MAX_INT, ((1 bsl 32) -1)).

%%====================================================================
%% eunit test
%%====================================================================

eqc_test_() ->
    {timeout, 40,
     ?_assertEqual(true, eqc:quickcheck(eqc:testing_time(20,
                                                         ?QC_OUT(prop_monotonic())
                                                        )
                                       )
                  )}.

run() ->
    run(?NUMTESTS).

run(Count) ->
    eqc:quickcheck(eqc:numtests(Count, prop_monotonic())).

check() ->
    eqc:check(prop_monotonic()).


%% @doc Returns the state in which each test case starts. (Unless a different
%%      initial state is supplied explicitly to, e.g. commands/2.)
-spec initial_state() -> eqc_statem:symbolic_state().
initial_state() ->
    #state{}.

%% ------ Grouped operator: lease_counter
lease_counter_args(_S) ->
    [

     frequency([{5, ?SUCHTHAT(Lease, ?LET(I, largeint(), abs(I)), Lease > 0)},
                {5, ?SUCHTHAT(Lease, ?LET(I, int(), abs(I)), Lease > 0)}])
    ].

lease_counter(Lease) ->
    [{status, LastId, MoCnt, Pid}] = ets:lookup(vnode_status, status),
    NewMoLease = MoCnt + Lease,
    {NewMoId, NewCntrModel} = case {MoCnt == ?MAX_INT, NewMoLease >  ?MAX_INT} of
                                  {true, _} ->
                                      %% New Id
                                      {LastId+1, min(Lease, ?MAX_INT)};
                                  {false, true} ->
                                      {LastId+1, min(?MAX_INT, Lease)};
                                  {false, false} ->
                                      {LastId, NewMoLease}
                              end,
    ok = riak_kv_vnode_status_mgr:lease_counter(Pid, Lease),
    {VnodeId, NewCntr} = receive
                             {counter_lease, {_, Id, NewLease}} ->
                                 {Id, NewLease}
                         after
                             60000 -> %% one minute!
                                 io:format("timeout!!!! ~p ~n", [erlang:is_process_alive(Pid)]),
                                 timeout
                         end,

    true = ets:insert(vnode_status, {status, NewMoId, NewCntrModel, Pid}),
    true = ets:insert(vnodeids, {VnodeId}),
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
    ?FORALL(Cmds, non_empty(commands(?MODULE)),
            begin
                ets:new(vnode_status, [named_table, set]),
                ets:new(vnodeids, [named_table, set]),
                {ok, Pid} = riak_kv_vnode_status_mgr:start_link(self(), 1),
                {ok, {ID, _Counter, _Lease}} = riak_kv_vnode_status_mgr:get_vnodeid_and_counter(Pid, 1),
                true =  ets:insert(vnode_status, {status, 1, 1, Pid}),
                true = ets:insert(vnodeids, {ID}),
                {H, S, Res} = run_commands(?MODULE,Cmds),
                [{status, Id, MoCntr, Pid}] = ets:lookup(vnode_status, status),
                VnodeIds = ets:info(vnodeids, size),
                {ok, Status} = riak_kv_vnode_status_mgr:status(Pid),
                Cnt = proplists:get_value(counter, Status, 0),

                ets:delete(vnode_status),
                ets:delete(vnodeids),
                riak_kv_vnode_status_mgr:clear_vnodeid(Pid),
                ok = riak_kv_vnode_status_mgr:stop(Pid),

                measure(vnodeid_changes, Id,
                        aggregate(command_names(Cmds),
                                  pretty_commands(?MODULE, Cmds, {H, S, Res},
                                                  conjunction([{result, equals(Res, ok)},
                                                               {values, equals(MoCntr, Cnt)},
                                                               {ids, equals(Id, VnodeIds)}
                                                              ])
                                                 )
                                 )
                       )
            end).

-endif.
