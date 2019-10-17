%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
-module(riak_repl2_rt_eqc).

%% @doc Integration level QuickCheck for realtime replication
%%
%% Expects 6 nodes to be configured and up with memory backend.
%% This test checks outbound replication from n1 to n2-n6.
%%
%% Properties
%%  - test writes an increasing sequence of values
%%  - remotes are enabled at random
%%  - remotes are started/stopped at random (which will start/stop connections)
%%  - once a remote is enabled, all puts should appear on remote
%%       regardless of disconnection, process death (except rtq).
%%       once rtq size bound is implemented, may miss up to dropped messages.
%%       as reported by stats.
%%
%% Ideas, speed up/slow down ack stuff by suspend/resume on the put worker pool workers.
%%


-include_lib("eqc/include/eqc.hrl").
-eqc_group_commands(true).
-include_lib("eqc/include/eqc_statem.hrl").
-compile(export_all).

-record(remote,
        {node,              % node running as the remote
         rtstate=disabled,  % disabled, enabled, started
         expected=[]}).
-record(ts, {client,        % riak client
             next_val = <<1:64>>,  % next value to write
             local,         % local node name
             remotes=[]
            }).

-define(BUCKET, <<"riak_repl2_rt_eqc">>).

%% Requires manual configuration
%% add to riak_repl on dev1
%%  [{remotes, [{c2, {"127.0.0.1","9902"}},
%%    {c2, {"127.0.0.1","9902"}},

%% c1 Replication config
%% {riak_repl, [
%%       {conn_port, 9901},
%%       {remotes, [{c2, {"127.0.0.1",9902}}]},
%%       {remotes, [{c3, {"127.0.0.1",9903}}]},
%%       {remotes, [{c4, {"127.0.0.1",9904}}]},
%%       {remotes, [{c5, {"127.0.0.1",9905}}]},
%%       {remotes, [{c6, {"127.0.0.1",9906}}]}],
%%       {data_root, "./data/riak_repl/"}
%%   ]},

%% other nodes
       %% {conn_port, 990N},
       %% {min_put_workers, 1},
       %% {max_put_workers, 3},



%% initial one, and the value of the initial state data:
initial_state() ->
    LocalNode = node_name(1),
    {ok, Client} = riak:client_connect(LocalNode),
    Remotes = orddict:from_list(
                [{cluster_name(I), #remote{node = node_name(I)}} || I <- lists:seq(2, 3)]),
    #ts{client = Client,
        local = LocalNode,
        remotes = Remotes}.

%% ====================================================================
%% enable callbacks
%% ====================================================================

enable_command(S) ->
    {call, ?MODULE, enable, [S#ts.local, elements(remote_names(S))]}.

enable_next(S, _R, [Local, Remote]) ->
    case lists:member(Remote, expect_disabled(S)) of
        true ->
            update_tr(Remote, fun(TR) -> TR#remote{rtstate=enabled} end, S);
        false ->
            S
    end.

enable_post(S, [Local, Remote], Res) ->
    Expect = case lists:member(Remote, expect_disabled(S)) of
                true ->
                    ok;
                false ->
                    {not_changed, {already_present, Remote}}
            end,
    post_expect([{return, Expect, Res}]).
            
enable(Local, Remote) ->
    rpc:call(Local, riak_repl2_rt, enable, [Remote]).

%% ====================================================================
%% disable callbacks
%% ====================================================================

disable_command(S) ->
    {call, ?MODULE, disable, [S#ts.local, elements(remote_names(S))]}.

disable_next(S, _R, [Local, Remote]) ->
    case lists:member(Remote, expect_disabled(S)) of
        false ->
            update_tr(Remote, fun(TR) -> TR#remote{rtstate=disabled} end, S);
        true ->
            S
    end.

disable_post(S, [Local, Remote], Res) ->
    Expect = case lists:member(Remote, expect_disabled(S)) of
                false ->
                    ok;
                true ->
                    {not_changed, {not_present, Remote}}
            end,
    post_expect([{return, Expect, Res}]).
            
disable(Local, Remote) ->
    rpc:call(Local, riak_repl2_rt, disable, [Remote]).

%% ====================================================================
%% start callbacks
%% ====================================================================

start_command(S) ->
    {call, ?MODULE, start, [S#ts.local, elements(remote_names(S))]}.

start_next(S, _R, [Local, Remote]) ->
    case expect_rtstate(Remote, S) of
        enabled ->
            update_tr(Remote, fun(TR) -> TR#remote{rtstate=started} end, S);
        _ ->
            S
    end.

start_post(S, [Local, Remote], Res) ->
    Expect = case expect_rtstate(Remote, S) of
                 disabled ->
                     {not_changed, {not_enabled, Remote}};
                 enabled ->
                     ok;
                 started ->
                     {not_changed, {already_present, Remote}}
            end,
    post_expect([{return, Expect, Res}]).
            
start(Local, Remote) ->
    rpc:call(Local, riak_repl2_rt, start, [Remote]).

%% ====================================================================
%% stop callbacks
%% ====================================================================

stop_command(S) ->
    {call, ?MODULE, stop, [S#ts.local, elements(remote_names(S))]}.

stop_next(S, _R, [Local, Remote]) ->
    case expect_rtstate(Remote, S) of
        started ->
            update_tr(Remote, fun(TR) -> TR#remote{rtstate=enabled} end, S);
        _ ->
            S
    end.

stop_post(S, [Local, Remote], Res) ->
    Expect = case expect_rtstate(Remote, S) of
                 disabled ->
                     {not_changed, {not_present, Remote}};
                 enabled ->
                     {not_changed, {not_present, Remote}};
                 started ->
                     ok
            end,
    post_expect([{return, Expect, Res}]).
            
stop(Local, Remote) ->
    rpc:call(Local, riak_repl2_rt, stop, [Remote]).

%% ====================================================================
%% status callbacks
%% ====================================================================

status_command(S) ->
    {call, ?MODULE, status, [S#ts.local]}.

status_post(S, [Local], Status) ->
    %% Check facts about status...
    post_expect([{enabled, % must be enabled to be started
                  lists:sort(expect_enabled(S)++expect_started(S)), 
                  proplists:get_value(enabled, Status)},
                 {started, lists:sort(expect_started(S)), 
                  proplists:get_value(started, Status)}]).

status(Local) ->
    rpc:call(Local, riak_repl2_rt, status, []).

%% ====================================================================
%% write object callbacks
%% ====================================================================

write_object_command(S) ->
    {call, ?MODULE, write_object, [S#ts.client, S#ts.next_val, binary()]}.

write_object_next(S = #ts{remotes = Remotes},
                  _Res, [_Client, NextVal = <<NextValInt:64>>, _Extra]) ->
    %% Add the expected value
    AddVal = fun(R) -> R#remote{expected = [NextVal | R#remote.expected]} end,
    Remotes2 = 
        lists:foldl(fun(Cluster, Remotes) -> orddict:update(Cluster, AddVal, Remotes) end,
                    Remotes, orddict:fetch_keys(Remotes)),
    %% Update next val
    S#ts{next_val = <<(NextValInt + 1):64>>, remotes = Remotes2}.

write_object_post(S, [_Client, _NextVal, _Extra], Res) ->
    post_expect([{return, ok, Res}]).

write_object(Client, NextVal, Extra) ->
    O = riak_object:new(?BUCKET, NextVal, <<NextVal/binary, Extra/binary>>),
    Client:put(O).

%% ====================================================================
%% Property
%% ====================================================================

prop_repl2_rt() ->
    ?FORALL(Cmds, more_commands(10, commands(?MODULE)),
            begin
                %% be stupid for now, make same initial state for node names
                S0 = initial_state(),
                reset_nodes(S0),
                %% run the test
                {H,S,Result} = HSRes = 
                    run_commands(?MODULE, Cmds),
                pretty_commands(?MODULE, Cmds, HSRes, 
                                aggregate(command_names(Cmds),
                                          conjunction(check_remote_results(S) ++
                                                          [{commands, equals(Result, ok)}])))
            end).

weight(S, write_object) ->
    20;
weight(_S, _) ->
    1.


%% ====================================================================
%% Helpers
%% ====================================================================

node_name(I) ->
    list_to_atom(lists:flatten(io_lib:format("dev~b@127.0.0.1", [I]))).
cluster_name(I) ->
    list_to_atom(lists:flatten(io_lib:format("c~b", [I]))).


remote_names(S) ->
    orddict:fetch_keys(S#ts.remotes).

expect_rtstate(Node, S) ->
    #remote{rtstate = RTS} = orddict:fetch(Node, S#ts.remotes),
    RTS.

expect_started(S) ->
    [C || {C,#remote{rtstate = RTS}} <- S#ts.remotes, RTS == started].
expect_enabled(S) ->
    [C || {C,#remote{rtstate = RTS}} <- S#ts.remotes, RTS == enabled].
expect_disabled(S) ->
    [C || {C,#remote{rtstate = RTS}} <- S#ts.remotes, RTS == disabled].

update_tr(Node, Fun, S = #ts{remotes = Remotes}) ->
    S#ts{remotes = orddict:update(Node, Fun, Remotes)}.


check_remote_results(S = #ts{client = Client, remotes = Remotes}) ->
    orddict:fold(fun(Cluster, #remote{expected = Expected}, Acc) ->
                      [check_remote_result(Client, Cluster, Expected) | Acc]
              end, [], Remotes).

check_remote_result(Client, Cluster, Expected) ->
    {ok, Values} = Client:list_keys(?BUCKET),
%%    io:format("Cluster: ~p\nExpected: ~p\nValues\: ~p\n", [Cluster, Expected, Values]),
    {{results, Cluster}, 
     equals(lists:sort(Expected), lists:sort(Values))}.
    

post_expect(Expectations) ->
    post_expect(Expectations, []).

post_expect([], []) ->
    true;
post_expect([], Acc) ->
    {false, Acc};
post_expect([{What, Exp, Got} | Rest], Acc) ->
    case Exp of
        Got ->
            post_expect(Rest, Acc);
        _ ->
            post_expect(Rest, [{What, expected, Exp, got, Got} | Acc])
    end.
    
            
%% %% For each node, reset the backend, reset the ring
reset_nodes(S) ->
    reset_node(S#ts.local),
    [begin
         ok = reset_node(N)
     end || {_,#remote{node = N}} <- S#ts.remotes].

reset_node(N) ->
    ok = rpc:call(N, riak_kv_memory_backend, reset, []),
    R = rpc:call(N, riak_core_ring, fresh, []),
    R1 = rpc:call(N, riak_repl_ring, ensure_config, [R]),
    ok = rpc:call(N, riak_core_ring_manager, set_my_ring, [R1]).
    %% was worried there were stale ring events, don't think so
    %%ok = rpc:call(N, riak_core_ring_events, ring_sync_update, [R1]),
    

%% reset_node(N) ->
%%     F = reset_fun(),
%%     rpc:call(N, erlang, apply, [F, []]).

%% reset_fun() ->
%%     fun() ->
%%             ok = riak_kv_memory_backend:reset(),
%%             R = riak_core_ring:fresh(),
%%             riak_core_ring_manager:set_my_ring(R)
%%     end.
