%% Riak EnterpriseDS
%% Copyright 2013 Basho Technologies, Inc. All Rights Reserved.

-module(riak_repl_aae_source).
-behaviour(gen_fsm_compat).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include("riak_repl.hrl").
-include("riak_repl_aae_fullsync.hrl").

%% API
-export([start_link/7, start_exchange/1]).

%% FSM states
-export([prepare_exchange/2,
         update_trees/2,
         cancel_fullsync/1,
         key_exchange/2,
         compute_differences/2,
         send_diffs/2,
         send_diffs/3,
         send_missing/3,
         bloom_fold/3]).

%% gen_fsm_compat callbacks
-export([init/1, handle_event/3, handle_sync_event/4, handle_info/3,
         terminate/3, code_change/4]).

-type index() :: non_neg_integer().
-type index_n() :: {index(), pos_integer()}.

-record(exchange, {mode   :: inline | buffered,
                   buffer :: ets:tid(),
                   bloom  :: reference() | undefined, %% ebloom
                   limit  :: non_neg_integer(),
                   count  :: non_neg_integer()
                  }).

-type exchange() :: #exchange{}.
-type version() :: legacy | non_neg_integer().

%% Per state transition timeout used by certain transitions
-define(DEFAULT_ACTION_TIMEOUT, 300000). %% 5 minutes

%% the first this many differences are not put in the bloom
%% filter, but simply sent to the remote site directly.
-define(GET_OBJECT_LIMIT, 1000).

%% Diff percentage needed to use bloom filter
-define(DIFF_PERCENTAGE, 5).

-record(state, {cluster,
                client,     %% riak:local_client()
                transport,
                socket,
                index       :: index(),
                indexns     :: [index_n()] | undefined,
                tree_pid    :: pid() | undefined,
                tree_mref   :: reference() | undefined,
                tree_version :: version() | undefined,
                built       :: non_neg_integer(),
                timeout = ?DEFAULT_ACTION_TIMEOUT  :: pos_integer(),
                wire_ver    :: atom() | undefined,
                diff_batch_size = 1000 :: non_neg_integer(),
                estimated_nr_keys :: non_neg_integer() | undefined,
                local_lock = false :: boolean(),
                owner       :: pid(),
                proto       :: term(),
                exchange    :: exchange() | undefined
               }).


%%%===================================================================
%%% API
%%%===================================================================

-spec start_link(term(), term(), term(), term(), index(), pid(), term())
                -> {ok,pid()} | ignore | {error, term()}.
start_link(Cluster, Client, Transport, Socket, Partition, OwnerPid, Proto) ->
    gen_fsm_compat:start_link(?MODULE, [Cluster, Client, Transport, Socket, Partition, OwnerPid, Proto], []).

start_exchange(AAESource) ->
    lager:debug("Send start_exchange to AAE fullsync sink worker"),
    gen_fsm_compat:send_event(AAESource, start_exchange).

cancel_fullsync(Pid) ->
    gen_fsm_compat:send_event(Pid, cancel_fullsync).

%%%===================================================================
%%% gen_fsm_compat callbacks
%%%===================================================================

init([Cluster, Client, Transport, Socket, Partition, OwnerPid, Proto]) ->
    lager:debug("AAE fullsync source worker started for partition ~p",
               [Partition]),

    Ver = riak_repl_util:deduce_wire_version_from_proto(Proto),
    {_, ClientVer, _} = Proto,
    State = #state{cluster=Cluster,
                   client=Client,
                   transport=Transport,
                   socket=Socket,
                   index=Partition,
                   built=0,
                   owner=OwnerPid,
                   wire_ver=Ver,
                   proto=ClientVer},
    {ok, prepare_exchange, State}.

handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

handle_sync_event(stop,_F,_StateName,State) ->
    {stop, normal, ok, State};

handle_sync_event(status, _From, StateName, State) ->
    Res = [{state, StateName},
           {partition_syncing, State#state.index},
           {wire_ver, State#state.wire_ver},
           {trees_built, State#state.built}
          ],
    {reply, Res, StateName, State};

handle_sync_event(_Event, _From, StateName, State) ->
    {reply, ok, StateName, State}.

handle_info({'DOWN', TreeMref, process, Pid, Why}, _StateName, State=#state{tree_mref=TreeMref}) ->
    %% Local hashtree process went down. Stop exchange.
    lager:info("Monitored pid ~p, AAE Hashtree process went down because: ~p", [Pid, Why]),
    send_complete(State),
    State#state.owner ! {soft_exit, self(), {aae_hastree_went_down, Why}},
    {stop, normal, State};
handle_info(Error={'DOWN', _, _, _, _}, _StateName, State) ->
    %% Something else exited. Stop exchange.
    lager:info("Something went down ~p", [Error]),
    send_complete(State),
    {stop, something_went_down, State};
handle_info({tcp_closed, Socket}, _StateName, State=#state{socket=Socket}) ->
    lager:info("AAE source connection to ~p closed", [State#state.cluster]),
    {stop, {tcp_closed, Socket}, State};
handle_info({tcp_error, Socket, Reason}, _StateName, State) ->
    lager:error("AAE source connection to ~p closed unexpectedly: ~p",
                [State#state.cluster, Reason]),
    {stop, {tcp_error, Socket, Reason}, State};
handle_info({ssl_closed, Socket}, _StateName, State=#state{socket=Socket}) ->
    lager:info("AAE source ssl connection to ~p closed", [State#state.cluster]),
    {stop, {ssl_closed, Socket}, State};
handle_info({ssl_error, Socket, Reason}, _StateName, State) ->
    lager:error("AAE source ssl connection to ~p closed unexpectedly with: ~p",
                [State#state.cluster, Reason]),
    {stop, {ssl_error, Socket, Reason}, State};
handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

terminate(_Reason, _StateName, _State) ->
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%===================================================================
%%% gen_fsm_compat states
%%%===================================================================

%% @doc Initial state. Attempt to acquire all necessary exchange locks.
%%      In order, acquire local concurrency lock, local tree lock,
%%      remote concurrency lock, and remote tree lock. Exchange will
%%      timeout if locks cannot be acquired in a timely manner.
prepare_exchange(cancel_fullsync, State) ->
    lager:info("AAE fullsync source cancelled for partition ~p", [State#state.index]),
    send_complete(State),
    {stop, normal, State};
prepare_exchange(start_exchange, State0=#state{transport=Transport,
                                              socket=Socket,
                                              index=Partition,
                                              local_lock=Lock}) when Lock == false ->
    TcpOptions = [{keepalive, true},
                  {packet, 4},
                  {active, false},  %% passive mode, messages must be received with recv
                  {nodelay, true},
                  {header, 1}],
    %% try to get local lock of the tree
    ok = Transport:setopts(Socket, TcpOptions),
    ok = send_synchronous_msg(?MSG_INIT, Partition, State0),

    %% Timeout for AAE.
    Timeout = app_helper:get_env(riak_kv,
                                 anti_entropy_timeout,
                                 ?DEFAULT_ACTION_TIMEOUT),

    %% List of IndexNs to iterate over.
    IndexNs = riak_kv_util:responsible_preflists(Partition),

    case riak_kv_vnode:hashtree_pid(Partition) of
        {ok, TreePid} ->
            TreeMref = monitor(process, TreePid),
            State = State0#state{timeout=Timeout,
                                 indexns=IndexNs,
                                 tree_pid=TreePid,
                                 tree_mref=TreeMref},
            case riak_kv_index_hashtree:get_lock_and_version(TreePid, fullsync_source) of
                {ok, Version} ->
                    prepare_exchange(start_exchange, State#state{local_lock=true, tree_version=Version});
                {Error, _Version} ->
                    lager:info("AAE source failed get_lock for partition ~p, got ~p",
                               [Partition, Error]),
                    State#state.owner ! {soft_exit, self(), Error},
                    {stop, normal, State}
            end;
        {error, wrong_node} ->
            {stop, wrong_node, State0}
    end;
    
prepare_exchange(start_exchange, State=#state{index=Partition, tree_version=legacy}) ->
     case send_synchronous_msg(?MSG_LOCK_TREE, State) of
        ok ->
            update_trees(init, State);
        Error ->
            lager:info("Remote lock tree for partition ~p failed, got ~p",
                          [Partition, Error]),
            send_complete(State),
            State#state.owner ! {soft_exit, self(), {remote, Error}},
            {stop, normal, State}
    end;
prepare_exchange(start_exchange, State=#state{index=Partition, tree_version=Version}) ->
    %% try to get the remote lock, need to pass Version into this message
    case send_synchronous_msg(?MSG_LOCK_TREE, Version, State) of
        ok ->
            update_trees(init, State);
        Error ->
            lager:info("Remote lock tree for partition ~p failed, got ~p",
                          [Partition, Error]),
            send_complete(State),
            State#state.owner ! {soft_exit, self(), {remote, Error}},
            {stop, normal, State}
    end.

%% @doc Now that locks have been acquired, ask both the local and remote
%%      hashtrees to perform a tree update. If updates do not occur within
%%      a timely manner, the exchange will timeout. Since the trees will
%%      continue to finish the update even after the exchange times out,
%%      a future exchange should eventually make progress.
update_trees(init, State=#state{indexns=IndexNs}) ->
    update_trees({start_exchange, IndexNs}, State);
update_trees(cancel_fullsync, State) ->
    lager:info("AAE fullsync source cancelled for partition ~p", [State#state.index]),
    send_complete(State),
    {stop, normal, State};
update_trees({start_exchange, [IndexHead|IndexTail]}, State=#state{tree_pid=TreePid,
                                                                   index=Partition,
                                                                   owner=Owner}) ->
    update_request(TreePid, {Partition, undefined}, IndexHead),
    case send_synchronous_msg(?MSG_UPDATE_TREE, IndexHead, State) of
        ok ->
            gen_fsm_compat:send_event(self(), tree_built),
            case IndexTail of
                [] -> ok;
                _  -> gen_fsm_compat:send_event(self(), {start_exchange, IndexTail})
            end,
            {next_state, update_trees, State};
        not_responsible ->
            lager:debug("Skipping AAE fullsync tree update for vnode ~p because"
                        " it is not responsible for the preflist ~p", [Partition, IndexHead]),
            gen_server:cast(Owner, not_responsible),
            {stop, normal, State}
    end;

update_trees({not_responsible, Partition, IndexN}, State=#state{owner=Owner}) ->
    lager:debug("VNode ~p does not cover preflist ~p", [Partition, IndexN]),
    gen_server:cast(Owner, not_responsible),
    {stop, normal, State};

update_trees(tree_built, State = #state{indexns=IndexNs}) ->
    Built = State#state.built + 1,
    NeededBuilts = length(IndexNs) * 2, %% All local and remote
    case Built of
        NeededBuilts ->
            %% Trees built now we can estimate how many keys
            {ok, EstimatedNrKeys} = riak_kv_index_hashtree:estimate_keys(State#state.tree_pid),
            lager:debug("EstimatedNrKeys ~p for partition ~p", [EstimatedNrKeys, State#state.index]),

            lager:debug("Moving to key exchange state"),
            key_exchange(init, State#state{built=Built, estimated_nr_keys = EstimatedNrKeys});
        _ ->
            {next_state, update_trees, State#state{built=Built}}
    end.

%% @doc Now that locks have been acquired and both hashtrees have been updated,
%%      perform a key exchange and trigger replication for any divergent keys.
key_exchange(init, State) ->
    EstimatedNrKeys = State#state.estimated_nr_keys,
    Limit = app_helper:get_env(riak_repl, fullsync_direct_limit, ?GET_OBJECT_LIMIT),
    PercentLimit = app_helper:get_env(riak_repl, fullsync_direct_percentage_limit, ?DIFF_PERCENTAGE),
    UsedLimit = max(Limit, EstimatedNrKeys * PercentLimit div 100),
    Mode = app_helper:get_env(riak_repl, fullsync_direct_mode, inline),
    Buffer = case Mode of
                 inline ->
                     undefined;
                 buffered ->
                     ets:new(?MODULE, [public, set])
             end,
    Exchange = #exchange{mode=Mode,
                         buffer=Buffer,
                         bloom=undefined,
                         limit=UsedLimit,
                         count=0},
    State2 = State#state{exchange=Exchange},
    key_exchange(start_key_exchange, State2);
key_exchange(cancel_fullsync, State) ->
    lager:info("AAE fullsync source cancelled for partition ~p", [State#state.index]),
    send_complete(State),
    {stop, normal, State};
key_exchange(finish_fullsync, State=#state{owner=Owner}) ->
    send_complete(State),
    lager:debug("AAE fullsync source completed partition ~p",
                [State#state.index]),
    riak_repl2_fssource:fullsync_complete(Owner),
    %% TODO: Why stay in key_exchange? Should we stop instead?
    {next_state, key_exchange, State};
key_exchange(continue, State=#state{indexns=IndexNs}) ->
    case IndexNs of
        [_] ->
            send_diffs(init, State);
        [_|RestNs] ->
            State2 = State#state{built=0, indexns=RestNs},
            gen_fsm_compat:send_event(self(), start_key_exchange),
            {next_state, key_exchange, State2}
    end;
key_exchange(start_key_exchange, State=#state{cluster=Cluster,
                                              transport=Transport,
                                              socket=Socket,
                                              index=Partition,
                                              tree_pid=TreePid,
                                              exchange=Exchange,
                                              indexns=[IndexN|_IndexNs]}) ->
    lager:debug("Starting fullsync key exchange with ~p for ~p/~p",
               [Cluster, Partition, IndexN]),

    SourcePid = self(),

    %% A function that receives callbacks from the hashtree:compare engine.
    %% This will send messages to ourself, handled in compare_loop(), that
    %% allow us to pass control of the TCP socket around. This is needed so
    %% that the process needing to send/receive on that socket has ownership
    %% of it.
    Remote = fun(init, _) ->
                     %% cause control of the socket to be given to AAE so that
                     %% the get_bucket and key_hashes can send messages via the
                     %% socket (with correct ownership). We'll send a 'ready'
                     %% back here once the socket ownership is transfered and
                     %% we are ready to proceed with the compare.
                     gen_fsm_compat:send_event(SourcePid, {'$aae_src', worker_pid, self()}),
                     receive
                         {'$aae_src', ready, SourcePid} ->
                             ok
                     end;
                (get_bucket, {L, B}) ->
                     wait_get_bucket(L, B, IndexN, State);
                (key_hashes, Segment) ->
                     wait_get_segment(Segment, IndexN, State);
                (start_exchange_level, {Level, Buckets}) ->
                     _ = [async_get_bucket(Level, B, IndexN, State) || B <- Buckets],
                     ok;
                (start_exchange_segments, Segments) ->
                     _ = [async_get_segment(Segment, IndexN, State) || Segment <- Segments],
                     ok;
                (final, _) ->
                     %% give ourself control of the socket again
                     ok = Transport:controlling_process(Socket, SourcePid)
             end,

    %% Unclear if we should allow exchange to run indefinitely or enforce
    %% a timeout. The problem is that depending on the number of keys and
    %% key differences, exchange can take arbitrarily long. For now, go with
    %% unbounded exchange, with the ability to cancel exchanges through the
    %% entropy manager if needed.

    %% accumulates a list of one element that is the count of
    %% keys that differed. We can't prime the accumulator. It
    %% always starts as the empty list. KeyDiffs is a list of hashtree::keydiff()
    AccFun = fun(KeyDiffs, Exchange0) ->
            %% Gather diff keys into a bloom filter
            lists:foldl(fun(KeyDiff, ExchangeIn) ->
                                accumulate_diff(KeyDiff, ExchangeIn, State)
                        end,
                        Exchange0,
                        KeyDiffs)
    end,

    %% TODO: Add stats for AAE
    lager:debug("Starting compare for partition ~p", [Partition]),
    spawn_link(fun() ->
                       StageStart=os:timestamp(),
                       Exchange2 = riak_kv_index_hashtree:compare(IndexN, Remote, AccFun, Exchange, TreePid),
                       lager:debug("Full-sync with site ~p; fullsync difference generator for ~p complete (completed in ~p secs)",
                                   [State#state.cluster, Partition, riak_repl_util:elapsed_secs(StageStart)]),
                       gen_fsm_compat:send_event(SourcePid, {'$aae_src', done, Exchange2})
               end),

    %% wait for differences from bloom_folder or to be done
    {next_state, compute_differences, State}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

async_get_bucket(Level, Bucket, IndexN, State) ->
    send_asynchronous_msg(?MSG_GET_AAE_BUCKET, {Level,Bucket,IndexN}, State).

wait_get_bucket(_Level, _Bucket, _IndexN, State) ->
    Reply = get_reply(State),
    Reply.

async_get_segment(Segment, IndexN, State) ->
    send_asynchronous_msg(?MSG_GET_AAE_SEGMENT, {Segment,IndexN}, State).

wait_get_segment(_Segment, _IndexN, State) ->
    Reply = get_reply(State),
    Reply.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

compute_differences({'$aae_src', worker_pid, WorkerPid},
                    #state{transport=Transport, socket=Socket} = State) ->
    ok = Transport:controlling_process(Socket, WorkerPid),
    WorkerPid ! {'$aae_src', ready, self()},
    {next_state, compute_differences, State};

compute_differences({'$aae_src', done, Exchange}, State) ->
    %% Just move on to the next tree exchange since we accumulate
    %% differences across all trees.
    State2 = State#state{exchange=Exchange},
    key_exchange(continue, State2).

maybe_send_direct(#exchange{mode=inline, count=Count, limit=Limit},
                  #state{index=Partition}) ->
    %% Inline sends already occured inline
    Sent = erlang:min(Count, Limit),
    lager:info("Directly sent ~b differences inline for partition ~p",
               [Sent, Partition]),
    ok;
maybe_send_direct(#exchange{buffer=Buffer}, State=#state{index=Partition}) ->
    Keys = [{Bucket, Key} || {_, {Bucket, Key}} <- ets:tab2list(Buffer)],
    true = ets:delete(Buffer),
    Sorted = lists:sort(Keys),
    Count = length(Sorted),
    lager:debug("Directly sending ~p differences for partition ~p", [Count, Partition]),
    _ = [send_missing(Bucket, Key, State) || {Bucket, Key} <- Sorted],
    ok.

%% state send_diffs is where we wait for diff_obj messages from the bloom folder
%% and send them to the sink for each diff_obj. We eventually finish upon receipt
%% of the diff_done event. Note: recv'd from a sync send event.
send_diffs({diff_obj, RObj}, _From, State) ->
    %% send missing object to remote sink
    send_missing(RObj, State),
    {reply, ok, send_diffs, State}.

send_diffs(init, State=#state{exchange=Exchange}) ->
    %% if we have anything in our bloom filter, start sending them now.
    %% this will start a worker process, which will tell us it's done with
    %% diffs_done once all differences are sent.
    _ = maybe_send_direct(Exchange, State),
    _ = finish_sending_differences(Exchange, State),

    %% wait for differences from bloom_folder or to be done
    {next_state, send_diffs, State};

%% All indexes in this Partition are done.
%% Note: recv'd from an async send event
send_diffs(diff_done, State) ->
    key_exchange(finish_fullsync, State).

%%%===================================================================
%%% Internal functions
%%%===================================================================
finish_sending_differences(#exchange{bloom=undefined, count=DiffCnt},
                           #state{index=Partition, estimated_nr_keys=EstimatedNrKeys}) ->
    lager:info("Syncing without bloom ~p/~p differences for partition ~p with EstimatedNrKeys ~p",
               [0, DiffCnt, Partition, EstimatedNrKeys]),
    gen_fsm_compat:send_event(self(), diff_done);

finish_sending_differences(#exchange{bloom=Bloom, count=DiffCnt},
                           #state{index=Partition, estimated_nr_keys=EstimatedNrKeys}) ->
    case ebloom:elements(Bloom) of
        Count = 0 ->
            lager:info("Syncing without bloom ~p/~p differences for partition ~p with EstimatedNrKeys ~p",
                       [Count, DiffCnt, Partition, EstimatedNrKeys]),
            gen_fsm_compat:send_event(self(), diff_done);
        Count ->
            {ok, Ring} = riak_core_ring_manager:get_my_ring(),
            OwnerNode = riak_core_ring:index_owner(Ring, Partition),
            lager:info("Bloom folding over ~p/~p differences for partition ~p with EstimatedNrKeys ~p",
                       [Count, DiffCnt, Partition, EstimatedNrKeys]),
            Self = self(),
            Worker = fun() ->
                    FoldRef = make_ref(),
                    try riak_core_vnode_master:command_return_vnode(
                            {Partition, OwnerNode},
                            riak_core_util:make_fold_req(
                                fun ?MODULE:bloom_fold/3,
                                {Self, Bloom},
                                false,
                                [{iterator_refresh, true}]),
                            {raw, FoldRef, self()},
                            riak_kv_vnode_master) of
                        {ok, VNodePid} ->
                            MonRef = erlang:monitor(process, VNodePid),
                            receive
                                {FoldRef, _Reply} ->
                                    %% we don't care about the reply
                                    gen_fsm_compat:send_event(Self, diff_done);
                                {'DOWN', MonRef, process, VNodePid, normal} ->
                                    lager:warning("VNode ~p exited before fold for partition ~p",
                                        [VNodePid, Partition]),
                                    exit({bloom_fold, vnode_exited_before_fold});
                                {'DOWN', MonRef, process, VNodePid, Reason}  ->
                                    lager:warning("Fold of ~p exited with ~p",
                                                  [Partition, Reason]),
                                    exit({bloom_fold, Reason})
                            end
                    catch exit:{{nodedown, Node}, _GenServerCall} ->
                            %% node died between services check and gen_server:call
                            exit({bloom_fold, {nodedown, Node}})
                    end
            end,
            spawn_link(Worker) %% this isn't the Pid we need because it's just the vnode:fold
    end.

maybe_create_bloom(#exchange{bloom=undefined}, State) ->
    EstimatedNrKeys = State#state.estimated_nr_keys,
    {ok, Bloom} = ebloom:new(max(10000, EstimatedNrKeys), 0.01, rand:uniform(1000)),
    Bloom;
maybe_create_bloom(#exchange{bloom=Bloom}, _State) ->
    Bloom.

bloom_fold({B, K}, V, {MPid, Bloom}) ->
    BKBin = bucket_key_to_binary(B, K),
    case ebloom:contains(Bloom, BKBin) of
        true ->
            RObj = riak_object:from_binary(B,K,V),
            gen_fsm_compat:sync_send_event(MPid, {diff_obj, RObj}, infinity);
        false ->
            ok
    end,
    {MPid, Bloom}.

accumulate_diff(KeyDiff, Exchange, State=#state{index=Partition}) ->
    case KeyDiff of
        {remote_missing, Bin} ->
            %% send object and related objects to remote
            {Bucket,Key} = binary_to_term(Bin),
            lager:debug("Keydiff: remote partition ~p remote missing: ~p:~p",
                        [Partition, Bucket, Key]),
            maybe_accumulate_key(Bucket, Key, Exchange, State);
        {different, Bin} ->
            %% send object and related objects to remote
            {Bucket,Key} = binary_to_term(Bin),
            lager:debug("Keydiff: remote partition ~p different: ~p:~p",
                        [Partition, Bucket, Key]),
            maybe_accumulate_key(Bucket, Key, Exchange, State);
        {missing, Bin} ->
            %% remote has a key we don't have. Ignore it.
            {Bucket,Key} = binary_to_term(Bin),
            lager:debug("Keydiff: remote partition ~p local missing: ~p:~p (ignored)",
                        [Partition, Bucket, Key]),
            Exchange;
        Other ->
            lager:warning("Unexpected error keydiff: ~p (ignored)", [Other]),
            Exchange
    end.

maybe_accumulate_key(Bucket, Key,
                     Exchange=#exchange{mode=Mode,
                                        count=Count,
                                        limit=Limit},
                     State) ->
    if Count < Limit ->
            %% Below threshold, handle directly
            Exchange2 = handle_direct(Mode, Bucket, Key, Exchange, State),
            Exchange2#exchange{count=Count+1};
       true ->
            %% Past threshold, add to bloom filter for future bloom fold
            Bloom = maybe_create_bloom(Exchange, State),
            BKBin = bucket_key_to_binary(Bucket, Key),
            ebloom:insert(Bloom, BKBin),
            Exchange#exchange{bloom=Bloom, count=Count+1}
    end.

%% @private See https://github.com/basho/riak_repl/issues/774. Turn
%% tuple-typed buckets and binary buckets and keys into a canonical
%% binary
-spec bucket_key_to_binary(Bucket::tuple() | binary(), Key::binary())
                          -> BKBin::binary().
bucket_key_to_binary({Type, Bucket}, Key) when
      is_binary(Type),
      is_binary(Bucket),
      is_binary(Key) ->
    <<Type/binary, Bucket/binary, Key/binary>>;
bucket_key_to_binary(Bucket, Key) when
      is_binary(Bucket),
      is_binary(Key) ->
    <<Bucket/binary, Key/binary>>.

handle_direct(inline, Bucket, Key, Exchange, State) ->
    %% Send key inline
    _ = send_missing(Bucket, Key, State),
    Exchange;
handle_direct(buffered, Bucket, Key, Exchange, _State) ->
    %% Enqueue in "direct send" buffer that will be sorted/sent later
    #exchange{buffer=Buffer, count=Count} = Exchange,
    ets:insert(Buffer, {Count, {Bucket, Key}}),
    Exchange.

send_missing(RObj, State=#state{client=Client, wire_ver=Ver, proto=Proto}) ->
    %% we don't actually have the vclock to compare, so just send the
    %% key and let the other side sort things out.
    case riak_repl_util:maybe_send(RObj, Client, Proto) of
        cancel ->
            0;
        Objects when is_list(Objects) ->
            %% source -> sink : fs_diff_obj
            %% binarize here instead of in the send() so that our wire
            %% format for the riak_object is more compact.
            _ = [begin
                 Data = riak_repl_util:encode_obj_msg(Ver, {fs_diff_obj,O}),
                 send_asynchronous_msg(?MSG_PUT_OBJ, Data, State)
             end || O <- Objects],
            Data2 = riak_repl_util:encode_obj_msg(Ver, {fs_diff_obj,RObj}),
            send_asynchronous_msg(?MSG_PUT_OBJ, Data2, State),
            1 + length(Objects)
    end.

%% Copied from riak_kv_vnode:local_get to extend with timeout,
%% might port back to riak_kv_vnode.erl in the future.
%%
%% Note: responses that timeout can result in future late
%% messages arriving from the vnode. This is currently safe
%% because of the catch-all handle_info that will ignore these
%% messages. But, something to keep in mind in the future.
kv_local_get(Index, BKey, Timeout) ->
    Ref = make_ref(),
    ReqId = erlang:phash2({self(), os:timestamp()}), % only has to be unique per-pid
    Sender = {raw, Ref, self()},
    riak_kv_vnode:get({Index,node()}, BKey, ReqId, Sender),
    receive
        {Ref, {r, Result, Index, ReqId}} ->
            Result;
        {Ref, Reply} ->
            {error, Reply}
    after Timeout ->
            {error, timeout}
    end.

%% Get the K/V directly from the local vnode
local_get(Bucket, Key, #state{index=Index}) ->
    kv_local_get(Index, {Bucket, Key}, ?REPL_FSM_TIMEOUT).

send_missing(Bucket, Key, State=#state{client=Client, wire_ver=Ver, proto=Proto}) ->
    case local_get(Bucket, Key, State) of
        {ok, RObj} ->
            %% we don't actually have the vclock to compare, so just send the
            %% key and let the other side sort things out.
            case riak_repl_util:maybe_send(RObj, Client, Proto) of
                cancel ->
                    0;
                Objects when is_list(Objects) ->
                    %% source -> sink : fs_diff_obj
                    %% binarize here instead of in the send() so that our wire
                    %% format for the riak_object is more compact.
                    _ = [begin
                         Data = riak_repl_util:encode_obj_msg(Ver, {fs_diff_obj,O}),
                         send_asynchronous_msg(?MSG_PUT_OBJ, Data, State)
                     end || O <- Objects],
                    Data2 = riak_repl_util:encode_obj_msg(Ver, {fs_diff_obj,RObj}),
                    send_asynchronous_msg(?MSG_PUT_OBJ, Data2, State),
                    1 + length(Objects)
            end;
        {error, notfound} ->
            %% can't find the key!
            lager:warning("not_found returned for fullsync client get on Bucket: ~p Key:~p", [Bucket,Key]),
            0;
        {error, timeout} ->
            lager:warning("timeout during fullsync client get on Bucket: ~p Key:~p", [Bucket,Key]),
            0
    end.

%% @private
update_request(Tree, {Index, _}, IndexN) ->
    as_event(fun() ->
                     case riak_kv_index_hashtree:update(IndexN, Tree) of
                         ok ->
                             tree_built;
                         not_responsible ->
                             {not_responsible, Index, IndexN}
                     end
             end).

%% @private
as_event(F) ->
    Self = self(),
    spawn_link(fun() ->
                       Result = F(),
                       gen_fsm_compat:send_event(Self, Result)
               end),
    ok.

send_complete(State) ->
    send_asynchronous_msg(?MSG_COMPLETE, State).

%%------------
%% Synchronous messaging with the AAE fullsync "sink" on the remote cluster
%%------------
%% send a message with type tag only, no data
send_synchronous_msg(MsgType, State=#state{transport=Transport,
                                           socket=Socket}) ->
    ok = Transport:send(Socket, <<MsgType:8>>),
    get_reply(State).

%% send a tagged message with type and binary data. return the reply
send_synchronous_msg(MsgType, Data, State=#state{transport=Transport,
                                                 socket=Socket}) when is_binary(Data) ->
    lager:debug("sending message type ~p", [MsgType]),
    ok = Transport:send(Socket, <<MsgType:8, Data/binary>>),
    Response = get_reply(State),
    lager:debug("got reply ~p", [Response]),
    Response;
%% send a tagged message with type and msg. return the reply
send_synchronous_msg(MsgType, Msg, State) ->
    Data = term_to_binary(Msg),
    send_synchronous_msg(MsgType, Data, State).

%% Async message send with tag and (binary or term data).
send_asynchronous_msg(MsgType, Data, #state{transport=Transport,
                                            socket=Socket}) when is_binary(Data) ->
    ok = Transport:send(Socket, <<MsgType:8, Data/binary>>);
send_asynchronous_msg(MsgType, Msg, State) ->
    Data = term_to_binary(Msg),
    send_asynchronous_msg(MsgType, Data, State).

%% send a message with type tag only, no data
send_asynchronous_msg(MsgType, #state{transport=Transport,
                                      socket=Socket}) ->
    ok = Transport:send(Socket, <<MsgType:8>>).

%% Receive a syncrhonous reply from the sink node.
%% TODO: change to use async io and thread the expected message
%% states through the fsm. That will allow us to service a status
%% request without blocking. We could also handle lates messages
%% without having to die.
get_reply(State=#state{transport=Transport, socket=Socket}) ->
    %% don't block forever, but if we timeout, then die with reason
    case Transport:recv(Socket, 0, ?AAE_FULLSYNC_REPLY_TIMEOUT) of
        {ok, [?MSG_REPLY|Data]} ->
            binary_to_term(Data);
        {error, Reason} ->
            %% This generate a return value that the fssource can
            %% display and possibly retry the partition from.
            throw({stop, Reason, State})
    end.

-ifdef(TEST).
%% test for basho/riak_repl issue 774
maybe_accumulate_key_test() ->
    State = #state{estimated_nr_keys=10000},
    %% ensure the count > limit to hit bloom
    Exchange = #exchange{count=10, limit=9},
    UnTypedBucket = <<"b">>,
    Key = <<"k">>,
    #exchange{bloom=Bloom} = E2 = maybe_accumulate_key(UnTypedBucket, Key, Exchange, State),
    ?assert(ebloom:contains(Bloom, <<UnTypedBucket/binary, Key/binary>>)),
    TypedBucket = {UnTypedBucket, UnTypedBucket},
    #exchange{bloom=Bloom} = _E3 = maybe_accumulate_key(TypedBucket, Key, E2, State),
    ExpectedBin = bucket_key_to_binary(TypedBucket, Key),
    ?assert(ebloom:contains(Bloom, ExpectedBin)).

-endif.
