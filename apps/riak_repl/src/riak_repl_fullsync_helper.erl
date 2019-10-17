%% Riak EnterpriseDS
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.

%%
%% Fullsync helper process - offloads key/hash generation and comparison
%% from the main TCP server/client processes.
%%
-module(riak_repl_fullsync_helper).
-behaviour(riak_core_gen_server).

%% API
-export([start_link/1,
         stop/1,
         make_keylist/3,
         diff/4,
         diff_stream/5,
         itr_new/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% HOFs
-export([keylist_fold/3]).

-include("riak_repl.hrl").

-record(state, {owner_fsm,
                ref,
                folder_pid,
                kl_fp,
                kl_total,
                filename,
                buf=[],
                size=0}).

-record(diff_state, {fsm,
                     ref,
                     preflist,
                     count = 0,
                     replies = 0,
                     diff_hash = 0,
                     missing = 0,
                     need_vclocks = true,
                     errors = []}).

%% ===================================================================
%% Public API
%% ===================================================================

start_link(OwnerFsm) ->
    riak_core_gen_server:start_link(?MODULE, [OwnerFsm], []).

stop(Pid) ->
    riak_core_gen_server:call(Pid, stop, infinity).

%% Make a sorted file of key/object hashes.
%% 
%% Return {ok, Ref} if build starts successfully, then sends
%% a gen_fsm_compat event {Ref, keylist_built} to the OwnerFsm or
%% a {Ref, {error, Reason}} event on failures
make_keylist(Pid, Partition, Filename) ->
    riak_core_gen_server:call(Pid, {make_keylist, Partition, Filename}, ?LONG_TIMEOUT).
   
%% Computes the difference between two keylist sorted files.
%% Returns {ok, Ref} or {error, Reason}
%% Differences are sent as {Ref, {merkle_diff, {Bkey, Vclock}}}
%% and finally {Ref, diff_done}.  Any errors as {Ref, {error, Reason}}.
diff(Pid, Partition, TheirFn, OurFn) ->
    riak_core_gen_server:call(Pid, {diff, Partition, TheirFn, OurFn, -1, true}, ?LONG_TIMEOUT).

diff_stream(Pid, Partition, TheirFn, OurFn, Count) ->
    riak_core_gen_server:call(Pid, {diff, Partition, TheirFn, OurFn, Count, false}, ?LONG_TIMEOUT).

%% ====================================================================
%% gen_server callbacks
%% ====================================================================

init([OwnerFsm]) ->
    process_flag(trap_exit, true),
    {ok, #state{owner_fsm = OwnerFsm}}.

handle_call(stop, _From, State) ->
    case State#state.folder_pid of
        undefined ->
            ok;
        Pid ->
            unlink(Pid),
            exit(Pid, kill)
    end,
    _ = file:close(State#state.kl_fp),
    _ = file:delete(State#state.filename),
    {stop, normal, ok, State};
%% request from client of server to write a keylist of hashed key/value to Filename for Partition
handle_call({make_keylist, Partition, Filename}, From, State) ->
    Ref = make_ref(),
    riak_core_gen_server:reply(From, {ok, Ref}),

    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    OwnerNode = riak_core_ring:index_owner(Ring, Partition),
    case lists:member(OwnerNode, riak_core_node_watcher:nodes(riak_kv)) of
        true ->
            {ok, FP} = file:open(Filename, [raw, write, binary]),
            Self = self(),
            FoldRef = make_ref(),
            Worker = fun() ->
                    %% Spend as little time on the vnode as possible,
                    %% accept there could be a potentially huge message queue
                    Req = case riak_core_capability:get({riak_repl, bloom_fold}, false) of
                        true ->
                            riak_core_util:make_fold_req(fun ?MODULE:keylist_fold/3,
                                                         {Self, 0, 0},
                                                         false,
                                                         [{iterator_refresh,
                                                                 true}]);
                        false ->
                            %% use old accumulator without the total
                            riak_core_util:make_fold_req(fun ?MODULE:keylist_fold/3,
                                                         {Self, 0},
                                                         false,
                                                         [{iterator_refresh,
                                                                 true}])
                    end,
                    try riak_core_vnode_master:command_return_vnode(
                            {Partition, OwnerNode},
                            Req,
                            {raw, FoldRef, self()},
                            riak_kv_vnode_master) of
                        {ok, VNodePid} ->
                            MonRef = erlang:monitor(process, VNodePid),
                            receive
                                {FoldRef, {Self, _}} ->
                                    %% total is 0, sorry
                                    riak_core_gen_server:cast(Self,
                                                              {kl_finish, 0});
                                {FoldRef, {Self, _, Total}} ->
                                    riak_core_gen_server:cast(Self,
                                                              {kl_finish, Total});
                                {'DOWN', MonRef, process, VNodePid, Reason} ->
                                    lager:warning("Keylist fold of ~p exited with ~p",
                                               [Partition, Reason]),
                                    exit({vnode_terminated, Reason})
                            end
                    catch exit:{{nodedown, Node}, _GenServerCall} ->
                            %% node died between services check and gen_server:call
                            exit({nodedown, Node})
                    end
            end,
            FolderPid = spawn_link(Worker),
            NewState = State#state{ref = Ref, 
                                   folder_pid = FolderPid,
                                   filename = Filename,
                                   kl_fp = FP},
            {noreply, NewState};
        false ->
            gen_fsm_compat:send_event(State#state.owner_fsm, {Ref, {error, node_not_available}}),
            {stop, normal, State}
    end;
%% sent from keylist_fold every 100 key/value hashes
handle_call(keylist_ack, _From, State) ->
    {reply, ok, State};
handle_call({diff, Partition, RemoteFilename, LocalFilename, Count, NeedVClocks}, From, State) ->
    %% Return to the caller immediately, if we are unable to open/
    %% read files this process will crash and the caller
    %% will discover the problem.
    Ref = make_ref(),
    riak_core_gen_server:reply(From, {ok, Ref}),
    try
        {ok, RemoteFile} = file:open(RemoteFilename,
            [read, binary, raw, read_ahead]),
        {ok, LocalFile} = file:open(LocalFilename,
            [read, binary, raw, read_ahead]),
        try
            {ok, Ring} = riak_core_ring_manager:get_my_ring(),
            OwnerNode = riak_core_ring:index_owner(Ring, Partition),
            case lists:member(OwnerNode, riak_core_node_watcher:nodes(riak_kv)) of
                true ->
                    DiffState = diff_keys(itr_new(RemoteFile, remote_reads),
                                        itr_new(LocalFile, local_reads),
                                        #diff_state{fsm = State#state.owner_fsm,
                                                    count=Count,
                                                    replies=Count,
                                                    ref = Ref,
                                                    need_vclocks = NeedVClocks,
                                                    preflist = {Partition, OwnerNode}}),
                    lager:info("Partition ~p: ~p remote / ~p local: ~p missing, ~p differences.",
                                        [Partition, 
                                        erlang:get(remote_reads),
                                        erlang:get(local_reads),
                                        DiffState#diff_state.missing,
                                        DiffState#diff_state.diff_hash]),
                    case DiffState#diff_state.errors of
                        [] ->
                            ok;
                        Errors ->
                            lager:error("Partition ~p: Read Errors.",
                                                [Partition, Errors])
                    end,
                    gen_fsm_compat:send_event(State#state.owner_fsm, {Ref, diff_done});
                false ->
                    gen_fsm_compat:send_event(State#state.owner_fsm, {Ref, {error, node_not_available}})
            end
        after
            _ = file:close(RemoteFile),
            _ = file:close(LocalFile)
        end
    after
        _ = file:delete(RemoteFilename),
        _ = file:delete(LocalFilename)
    end,

    {stop, normal, State}.

%% write Key/Value Hash to file
handle_cast({keylist, Row}, State) ->
    ok = file:write(State#state.kl_fp, <<(size(Row)):32, Row/binary>>),
    {noreply, State};
handle_cast({kl_finish, Count}, State) ->
    %% note: delayed_write has been removed as of 1.4.0
    %% delayed_write can mean sync/close might not work the first time around
    %% because of a previous error that is only now being reported. In this case,
    %% call close again. See http://www.erlang.org/doc/man/file.html#open-2
    case file:sync(State#state.kl_fp) of
        ok ->
            ok;
        _ ->
            _ = file:sync(State#state.kl_fp),
            ok
    end,
    case file:close(State#state.kl_fp) of
        ok ->
            ok;
        _ ->
            _ = file:close(State#state.kl_fp),
            ok
    end,
    riak_core_gen_server:cast(self(), kl_sort),
    {noreply, State#state{kl_total=Count}};
handle_cast(kl_sort, State) ->
    Filename = State#state.filename,
    %% we want the GC to stop running, so set a giant heap size
    %% this process is about to die, so this is OK
    lager:info("Sorting keylist ~p", [Filename]),
    erlang:process_flag(min_heap_size, 1000000),
    {ElapsedUsec, ok} = timer:tc(file_sorter, sort, [Filename]),
    lager:info("Sorted ~s of ~p keys in ~.2f seconds",
                          [Filename, State#state.kl_total, ElapsedUsec / 1000000]),
    gen_fsm_compat:send_event(State#state.owner_fsm, {State#state.ref, keylist_built,
        State#state.kl_total}),
    {stop, normal, State}.

handle_info({'EXIT', Pid,  Reason}, State) when Pid =:= State#state.folder_pid ->
    case Reason of
        normal ->
            {noreply, State};
        _ ->
            gen_fsm_compat:send_event(State#state.owner_fsm, 
                               {State#state.ref, {error, {folder_died, Reason}}}),
            {stop, normal, State}
    end;
handle_info({'EXIT', _Pid,  _Reason}, State) ->
    %% The calling repl_tcp_server/client has gone away, so should we
    {stop, normal, State}.

terminate(_Reason, State) ->
    %% close file handles, in case they're open
    catch(file:close(State#state.kl_fp)),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ====================================================================
%% Internal functions
%% ====================================================================

pack_key(K) ->
    riak_repl_util:binpack_bkey(K).

unpack_key(K) ->
    riak_repl_util:binunpack_bkey(K).

%% Hash an object, making sure the vclock is in sorted order
%% as it varies depending on whether it has been pruned or not
hash_object(B,K,RObjBin) ->
    RObj = riak_object:from_binary(B,K,RObjBin),
    Vclock = riak_object:vclock(RObj),
    UpdObj = riak_object:set_vclock(RObj, lists:sort(Vclock)),
    %% can't use the new binary version yet
    erlang:phash2(term_to_binary(UpdObj)).

itr_new(File, Tag) ->
    erlang:put(Tag, 0),
    case file:read(File, 4) of
        {ok, <<Size:32/unsigned>>} ->
            itr_next(Size, File, Tag);
        _ ->
            _ = file:close(File),
            eof
    end.

itr_next(Size, File, Tag) ->
    case file:read(File, Size + 4) of
        {ok, <<Data:Size/bytes>>} ->
            erlang:put(Tag, erlang:get(Tag) + 1),
            _ = file:close(File),
            {binary_to_term(Data), fun() -> eof end};
        {ok, <<Data:Size/bytes, NextSize:32/unsigned>>} ->
            erlang:put(Tag, erlang:get(Tag) + 1),
            {binary_to_term(Data), fun() -> itr_next(NextSize, File, Tag) end};
        eof ->
            _ = file:close(File),
            eof
    end.

diff_keys(R, L, #diff_state{replies=0, fsm=FSM, ref=Ref, count=Count} = DiffState) 
  when Count /= 0 ->
    gen_fsm_compat:send_event(FSM, {Ref, diff_paused}),
    %% wait for a message telling us to stop, or to continue.
    %% TODO do this more correctly when there's more time.
    receive
        {'$gen_call', From, stop} ->
            riak_core_gen_server:reply(From, ok),
            DiffState;
        {Ref, diff_resume} ->
            %% Resuming the diff stream generation
            diff_keys(R, L, DiffState#diff_state{replies=Count});
        {'EXIT', FSM, _Reason} ->
            %% fullsync process exited, follow suit
            DiffState
    end;
diff_keys({{Key, Hash}, RNext}, {{Key, Hash}, LNext}, DiffState) ->
    %% Remote and local keys/hashes match
    diff_keys(RNext(), LNext(), DiffState);
diff_keys({{Key, _}, RNext}, {{Key, _}, LNext}, DiffState) ->
    %% Both keys match, but hashes do not
    diff_keys(RNext(), LNext(), diff_hash(Key, DiffState));
diff_keys({{RKey, _RHash}, RNext}, {{LKey, _LHash}, _LNext} = L, DiffState)
  when RKey < LKey ->
    diff_keys(RNext(), L, missing_key(RKey, DiffState));
diff_keys({{RKey, _RHash}, _RNext} = R, {{LKey, _LHash}, LNext}, DiffState)
  when RKey > LKey ->
    %% Remote is ahead of local list
    %% TODO: This may represent a deleted key...
    diff_keys(R, LNext(), DiffState);
diff_keys({{RKey, _RHash}, RNext}, eof, DiffState) ->
    %% End of local stream; all keys from remote should be processed
    diff_keys(RNext(), eof, missing_key(RKey, DiffState));
diff_keys(eof, _, DiffState) ->
    %% End of remote stream; all remaining keys are local to this side or
    %% deleted ops
    DiffState.

%% Called when the hashes differ with the packed bkey
diff_hash(PBKey, DiffState = #diff_state{need_vclocks=false, fsm=FSM, ref=Ref}) ->
    BKey = unpack_key(PBKey),
    gen_fsm_compat:send_event(FSM, {Ref, {merkle_diff, {BKey, undefined}}}),
    DiffState#diff_state{diff_hash = DiffState#diff_state.diff_hash + 1,
        replies=DiffState#diff_state.replies - 1};
diff_hash(PBKey, DiffState) ->
    UpdDiffHash = DiffState#diff_state.diff_hash + 1,
    BKey = unpack_key(PBKey),
    case catch riak_kv_vnode:get_vclocks(DiffState#diff_state.preflist, 
                                         [BKey]) of
        [{BKey, _Vclock} = BkeyVclock] ->
            Fsm = DiffState#diff_state.fsm,
            Ref = DiffState#diff_state.ref,
            gen_fsm_compat:send_event(Fsm, {Ref, {merkle_diff, BkeyVclock}}),
            DiffState#diff_state{diff_hash = UpdDiffHash,
                replies=DiffState#diff_state.replies - 1};
        Reason ->
            UpdErrors = orddict:update_counter(Reason, 1, DiffState#diff_state.errors),
            DiffState#diff_state{errors = UpdErrors}
    end.
    
%% Called when the key is missing on the local side
missing_key(PBKey, DiffState) ->
    BKey = unpack_key(PBKey),
    Fsm = DiffState#diff_state.fsm,
    Ref = DiffState#diff_state.ref,
    gen_fsm_compat:send_event(Fsm, {Ref, {merkle_diff, {BKey, vclock:fresh()}}}),
    UpdMissing = DiffState#diff_state.missing + 1,
    DiffState#diff_state{missing = UpdMissing,
        replies=DiffState#diff_state.replies - 1}.

%% @private
%%
%% @doc Visting function for building keylist.  This function was
%% purposefully created because if you use a lambda then things will
%% go wrong when the MD5 of this module changes. I.e. if the lambda is
%% shipped to another node with a different version of
%% `riak_repl_fullsync_helper', even if the code inside the lambda is
%% the same, then a badfun error will occur since the MD5s of the
%% modules are not the same.
%%
%% See http://www.javalimit.com/2010/05/passing-funs-to-other-erlang-nodes.html
keylist_fold({B,Key}=K, V, {MPid, Count, Total}) ->
    try
        H = hash_object(B,Key,V),
        Bin = term_to_binary({pack_key(K), H}),
        %% write key/value hash to file
        riak_core_gen_server:cast(MPid, {keylist, Bin}),
        case Count of
            100 ->
                %% send keylist_ack to "self" every 100 key/value hashes
                ok = riak_core_gen_server:call(MPid, keylist_ack, infinity),
                {MPid, 0, Total+1};
            _ ->
                {MPid, Count+1, Total+1}
        end
    catch _:_ ->
            {MPid, Count, Total}
    end;
%% legacy support for the 2-tuple accumulator in 1.2.0 and earlier
keylist_fold({B,Key}=K, V, {MPid, Count}) ->
    try
        H = hash_object(B,Key,V),
        Bin = term_to_binary({pack_key(K), H}),
        %% write key/value hash to file
        riak_core_gen_server:cast(MPid, {keylist, Bin}),
        case Count of
            100 ->
                %% send keylist_ack to "self" every 100 key/value hashes
                ok = riak_core_gen_server:call(MPid, keylist_ack, infinity),
                {MPid, 0};
            _ ->
                {MPid, Count+1}
        end
    catch _:_ ->
            {MPid, Count}
    end.
