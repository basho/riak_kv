%% Riak EnterpriseDS
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.

%% @doc This is the client-side component of the new fullsync strategy
%% introduced in riak 1.1. See the `repl_keylist_server' module for more
%% information on the protocol and the improvements.
%%
%% @see repl_keylist_server
-module(riak_repl_keylist_client).

-behaviour(gen_fsm_compat).

-include("riak_repl.hrl").

%% API
-export([start_link/4]).

%% gen_fsm_compat
-export([init/1, 
         handle_event/3,
         handle_sync_event/4, 
         handle_info/3, 
         terminate/3, 
         code_change/4]).

%% states
-export([wait_for_fullsync/2,
        request_partition/2,
        send_keylist/2,
        wait_ack/2]).

-record(state, {
        sitename :: repl_sitename(),
        socket,
        transport,
        work_dir,
        partitions = [],
        partition,
        kl_fn,
        kl_fh,
        kl_pid,
        kl_ref,
        kl_ack_freq,
        kl_counter,
        our_kl_ready,
        their_kl_ready,
        stage_start,
        partition_start,
        skipping=false
    }).

start_link(SiteName, Transport, Socket, WorkDir) ->
    gen_fsm_compat:start_link(?MODULE, [SiteName, Transport, Socket, WorkDir], []).

init([SiteName, Transport, Socket, WorkDir]) ->
    AckFreq = app_helper:get_env(riak_repl,client_ack_frequency,
        ?REPL_DEFAULT_ACK_FREQUENCY),
    {ok, wait_for_fullsync,
        #state{sitename=SiteName,transport=Transport,socket=Socket,work_dir=WorkDir,
            kl_ack_freq=AckFreq}}.

wait_for_fullsync(Command, State)
        when Command == start_fullsync; Command == resume_fullsync ->
    Partitions = case State#state.partitions of
        [] ->
            case app_helper:get_env(riak_repl_progress, 
                                    list_to_atom(State#state.sitename),
                                    []) of
                [] ->
                    %% last sync completed or was cancelled
                    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
                    Partitions0 = riak_repl_util:get_partitions(Ring),
                    case app_helper:get_env(riak_repl, shuffle_ring, true) of
                        true ->
                            %% randomly shuffle the partitions so that if we
                            %% restart, we have a good chance of not re-doing
                            %% partitions we already synced
                            riak_repl_util:shuffle_partitions(Partitions0, os:timestamp());
                        _ ->
                            Partitions0
                    end;
                Progress ->
                    lager:info("Full-sync with site ~p; resuming failed fullsync at ~p",
                        [State#state.sitename, hd(Progress)]),
                    Progress
            end;
        _ ->
            [State#state.partition | State#state.partitions] % resuming from pause
    end,
    Remaining = length(Partitions),
    lager:info("Full-sync with site ~p starting; ~p partitions.",
                          [State#state.sitename, Remaining]),
    gen_fsm_compat:send_event(self(), continue),
    {next_state, request_partition, State#state{partitions=Partitions}};
wait_for_fullsync({start_fullsync, Partitions}, State) ->
    Remaining = length(Partitions),
    lager:info("Full-sync with site ~p starting; ~p partitions.",
                          [State#state.sitename, Remaining]),
    gen_fsm_compat:send_event(self(), continue),
    {next_state, request_partition, State#state{partitions=Partitions}};
wait_for_fullsync(_Other, State) ->
    {next_state, wait_for_fullsync, State}.

request_partition(cancel_fullsync,
                  #state{kl_pid=Pid, sitename=SiteName} = State) ->
    catch(riak_repl_fullsync_helper:stop(Pid)),
    _ = file:delete(State#state.kl_fn),
    log_stop(cancel_fullsync, State),
    application:unset_env(riak_repl_progress, list_to_atom(SiteName)),
    NewState = State#state{partitions=[], partition=undefined},
    {next_state, wait_for_fullsync, NewState};
request_partition(pause_fullsync,
                  #state{kl_pid=Pid} = State) ->
    catch(riak_repl_fullsync_helper:stop(Pid)),
    _ = file:delete(State#state.kl_fn),
    log_stop(pause_fullsync, State),
    {next_state, wait_for_fullsync, State};
%% Start from beginning or resume failed sync
request_partition(continue,
                  #state{partitions=[], sitename=SiteName} = State) ->
    application:unset_env(riak_repl_progress, list_to_atom(SiteName)),
    lager:info("Full-sync with site ~p completed", [State#state.sitename]),
    riak_repl_tcp_client:send(State#state.transport, State#state.socket, fullsync_complete),
    {next_state, wait_for_fullsync, State#state{partition=undefined}};
request_partition(continue, #state{partitions=[P|T], work_dir=WorkDir, socket=Socket} = State) ->
    %% Possibly try to obtain the per-vnode lock before connecting.
    %% If we return error, we expect the coordinator to start us again later.
    case riak_repl_util:maybe_get_vnode_lock(P) of
        ok ->
            lager:info("Full-sync with site ~p; starting fullsync for ~p",
                       [State#state.sitename, P]),
            riak_repl2_fs_node_reserver:claim_reservation(P),
            application:set_env(riak_repl_progress, list_to_atom(State#state.sitename), [P|T]),
            riak_repl_tcp_client:send(State#state.transport, Socket, {partition, P}),
            KeyListFn = riak_repl_util:keylist_filename(WorkDir, P, ours),
            lager:info("Full-sync with site ~p; building keylist for ~p, ~p remain",
                       [State#state.sitename, P, length(T)]),
            {ok, KeyListPid} = riak_repl_fullsync_helper:start_link(self()),
            {ok, KeyListRef} = riak_repl_fullsync_helper:make_keylist(KeyListPid,
                                                                      P,
                                                                      KeyListFn),
            {next_state, request_partition, State#state{kl_fn=KeyListFn,
                                                        our_kl_ready=false,
                                                        their_kl_ready=false,
                                                        partition_start=os:timestamp(),
                                                        stage_start=os:timestamp(), skipping=false,
                                                        kl_pid=KeyListPid, kl_ref=KeyListRef,
                                                        partition=P, partitions=T}};
        {error, Reason} ->
            %% the vnode is probably busy. Quit all the way back.
            {stop, Reason, State}
    end;
%% @plu client <- key-lister
request_partition({Ref, keylist_built, _Size}, State=#state{kl_ref = Ref}) ->
    lager:info("Full-sync with site ~p; built keylist for ~p, (built in ~p secs)",
        [State#state.sitename, State#state.partition,
            riak_repl_util:elapsed_secs(State#state.stage_start)]),
    case State#state.their_kl_ready of
        true ->
            gen_fsm_compat:send_event(self(), continue),
            {next_state, send_keylist, State#state{stage_start=os:timestamp(),
                    kl_counter=State#state.kl_ack_freq}};
        _ ->
            {next_state, request_partition, State#state{our_kl_ready=true,
                    kl_pid=undefined}}
    end;
request_partition({kl_exchange, P}, #state{partition=P} = State) ->
    case State#state.our_kl_ready of
        true ->
            %% @plu client -> client: continue
            gen_fsm_compat:send_event(self(), continue),
            %% @plu note over client: (send_keylist)
            {next_state, send_keylist, State#state{stage_start=os:timestamp(),
                    kl_counter=State#state.kl_ack_freq}};
        _ ->
            {next_state, request_partition, State#state{their_kl_ready=true}}
    end;
request_partition({kl_exchange, P},  State) ->
    lager:warning("Stale kl_exchange message received for ~p, ignoring",
        [P]),
    {next_state, request_partition, State};
request_partition({Ref, {error, Reason}}, #state{socket=Socket, kl_ref=Ref,
        transport=Transport, skipping=Skip} = State) ->
    lager:warning("Full-sync with site ~p; skipping partition ~p because of error ~p",
        [State#state.sitename, State#state.partition, Reason]),
    case Skip of
        false ->
            _ = riak_repl_tcp_server:send(Transport, Socket, {skip_partition, State#state.partition}),
            gen_fsm_compat:send_event(self(), continue);
        _ ->
            %% we've already decided to skip this partition, so do nothing
            ok
    end,
    {next_state, request_partition, State#state{skipping=true}};
request_partition({skip_partition, Partition}, #state{partition=Partition,
        kl_pid=Pid} = State) ->
    lager:warning("Full-sync with site ~p; skipping partition ~p as requested by server",
        [State#state.sitename, Partition]),
    catch(riak_repl_fullsync_helper:stop(Pid)),
    case State#state.skipping of
        false ->
            gen_fsm_compat:send_event(self(), continue);
        _ ->
            %% we've already decided to skip this partition, so do nothing
            ok
    end,
    {next_state, request_partition, State#state{skipping=true}};
request_partition({skip_partition, Partition}, State) ->
    lager:warning("Full-sync with site ~p; asked to skip partition ~p, but current partition is ~p",
        [State#state.sitename, Partition, State#state.partition]),
    {next_state, request_partition, State}.

send_keylist(cancel_fullsync,
             #state{kl_fh=FH, sitename=SiteName} = State) ->
    % stop sending the keylist and delete the file
    _ = file:close(FH),
    _ = file:delete(State#state.kl_fn),
    log_stop(cancel_fullsync, State),
    application:unset_env(riak_repl_progress, list_to_atom(SiteName)),
    NewState = State#state{partitions=[], partition=undefined},
    {next_state, wait_for_fullsync, NewState};
send_keylist(pause_fullsync,
             #state{kl_fh=FH} = State) ->
    _ = file:close(FH),
    _ = file:delete(State#state.kl_fn),
    log_stop(pause_fullsync, State),
    {next_state, wait_for_fullsync, State};
send_keylist(kl_ack, State) ->
    gen_fsm_compat:send_event(self(), continue),
    {next_state, send_keylist,
        State#state{kl_counter=State#state.kl_ack_freq}};
send_keylist(continue, #state{kl_fh=FH0,transport=Transport,socket=Socket,kl_counter=Count} = State) ->
    FH = case FH0 of
        undefined ->
            lager:info("Full-sync for ~p; sending keylist for ~p",
                [State#state.sitename, State#state.partition]),
            {ok, F} = file:open(State#state.kl_fn, [read, binary, raw, read_ahead]),
            F;
        _ ->
            FH0
    end,
    case file:read(FH, ?MERKLE_CHUNKSZ) of
        {ok, Data} ->
            _ = riak_repl_tcp_client:send(Transport, Socket, {kl_hunk, Data}),
            _ = case Count =< 0 of
                true ->
                    _ = riak_repl_tcp_client:send(Transport, Socket, kl_wait);
                _ ->
                    gen_fsm_compat:send_event(self(), continue)
            end,
            {next_state, send_keylist, State#state{kl_fh=FH,
                    kl_counter=Count-1}};
        eof ->
            _ = file:close(FH),
            _ = file:delete(State#state.kl_fn),
            _ = riak_repl_tcp_client:send(Transport, Socket, kl_eof),
            lager:info("Full-sync with site ~p; sent keylist for ~p (sent in ~p secs)",
                [State#state.sitename, State#state.partition,
                    riak_repl_util:elapsed_secs(State#state.stage_start)]),
            lager:info("Full-sync with site ~p; exchanging differences for ~p",
                [State#state.sitename, State#state.partition]),
            {next_state, wait_ack, State#state{kl_fh=undefined,
                    stage_start=os:timestamp()}}
    end.

wait_ack(cancel_fullsync, #state{sitename=SiteName} = State) ->
    log_stop(cancel_fullsync, State),
    application:unset_env(riak_repl_progress, list_to_atom(SiteName)),
    NewState = State#state{partitions=[], partition=undefined},
    {next_state, wait_for_fullsync, NewState};
wait_ack(pause_fullsync, State) ->
    log_stop(pause_fullsync, State),
    {next_state, wait_for_fullsync, State};
wait_ack({diff_ack, Partition}, #state{partition=Partition,
        transport=Transport,socket=Socket} = State) ->
    _ = riak_repl_tcp_client:send(Transport, Socket, {diff_ack, Partition}),
    {next_state, wait_ack, State};
wait_ack(diff_done, State) ->
    lager:info("Full-sync with site ~p; differences exchanged for ~p (done in ~p secs)",
        [State#state.sitename, State#state.partition,
            riak_repl_util:elapsed_secs(State#state.stage_start)]),
    lager:info("Full-sync with site ~p; full-sync for partition ~p complete (done in ~p secs)",
        [State#state.sitename, State#state.partition,
            riak_repl_util:elapsed_secs(State#state.partition_start)]),
    gen_fsm_compat:send_event(self(), continue),
    {next_state, request_partition, State}.


%% gen_fsm_compat callbacks

handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

handle_sync_event(status, _From, StateName, State) ->
    Res = [{state, StateName}] ++
    case State#state.partitions of
        [] ->
            [];
        Partitions ->
            [
                {fullsync, length(Partitions), left},
                {partition, State#state.partition},
                {partition_start,
                    riak_repl_util:elapsed_secs(State#state.partition_start)},
                {stage_start,
                    riak_repl_util:elapsed_secs(State#state.stage_start)}
            ]
    end,
    {reply, Res, StateName, State};
handle_sync_event(stop,_F,_StateName,State) ->
    {stop, normal, ok, State};
handle_sync_event(_Event,_F,StateName,State) ->
    {reply, ok, StateName, State}.

handle_info(_I, StateName, State) ->
    {next_state, StateName, State}.

terminate(_Reason, _StateName, State) ->
    catch(file:close(State#state.kl_fh)),
    %% Clean up the working directory on crash/exit
    Cmd = lists:flatten(io_lib:format("rm -rf ~s", [State#state.work_dir])),
    os:cmd(Cmd).

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%% internal funtions

log_stop(Command, State) ->
    lager:info("Full-sync for site ~p ~s at partition ~p (after ~p secs)",
        [State#state.sitename, command_verb(Command), State#state.partition,
            riak_repl_util:elapsed_secs(State#state.partition_start)]).

command_verb(cancel_fullsync) ->
    "cancelled";
command_verb(pause_fullsync) ->
    "paused".
