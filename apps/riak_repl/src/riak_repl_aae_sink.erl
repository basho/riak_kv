%% @doc
%% This module implements a fullsync "sink" strategy that uses Active Anti-Entropy (AAE).
%% It takes full control of the socket to the source side and implements the entire protocol
%% here. 
%%
-module(riak_repl_aae_sink).
-include("riak_repl.hrl").
-include("riak_repl_aae_fullsync.hrl").

-behaviour(gen_server).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% API
-export([start_link/4, init_sync/1]).
-export([sender_init/2, sender_loop/1]).

-record(state, {
          clustername,
          socket,
          transport,
          tree_pid      :: pid() | undefined,  %% pid of the AAE tree
          partition,
          sender        :: pid() | undefined,
          owner         :: pid()   %% our fssource owner
         }).

%%%===================================================================
%%% API
%%%===================================================================

start_link(ClusterName, Transport, Socket, OwnerPid) ->
    gen_server:start_link(?MODULE, [ClusterName, Transport, Socket, OwnerPid], []).

%% Called after ownership of socket has been given to AAE sink worker
init_sync(AAEWorker) ->
    gen_server:call(AAEWorker, init_sync, infinity).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([ClusterName, Transport, Socket, OwnerPid]) ->
    lager:info("Starting AAE fullsync sink worker with Socket ~w", [Socket]),
    {ok, #state{clustername=ClusterName, socket=Socket, transport=Transport, owner=OwnerPid}}.

handle_call(init_sync, _From, State=#state{transport=Transport, socket=Socket}) ->
    TcpOptions = [{keepalive, true}, % find out if connection is dead, this end doesn't send
                  {packet, 4},
                  {active, once},
                  {nodelay, true},
                  {header, 1}],
    ok = Transport:setopts(Socket, TcpOptions),
    Sender = spawn_sender(Transport, Socket),
    {reply, ok, State#state{sender=Sender}};

handle_call(status, _From, State) ->
    Reply = [{partition_syncing, State#state.partition}],
    {reply, Reply, State};

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(stop, State) ->
    {stop, normal, State};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({Proto, _Socket, Data}, State=#state{transport=Transport,
                                                 socket=Socket}) when Proto==tcp; Proto==ssl ->
    TcpOptions = [{active, once}], %% reset to receive next tcp message
    Transport:setopts(Socket, TcpOptions),
    case Data of
        [MsgType] ->
            process_msg(MsgType, State);
        [MsgType|<<>>] ->
            process_msg(MsgType, State);
        [MsgType|MsgData] ->
            process_msg(MsgType, binary_to_term(MsgData), State)
    end;

handle_info({'DOWN', _, _, _, _}, State) ->
    {stop, tree_down, State};

handle_info({tcp_closed, Socket}, State=#state{socket=Socket}) ->
    lager:info("AAE sink connection to ~p closed", [State#state.clustername]),
    {stop, {tcp_closed, Socket}, State};
handle_info({tcp_error, Socket, Reason}, State) ->
    lager:error("AAE sink connection to ~p closed unexpectedly: ~p",
                [State#state.clustername, Reason]),
    {stop, {tcp_error, Socket, Reason}, State};
handle_info({ssl_closed, Socket}, State=#state{socket=Socket}) ->
    lager:info("AAE sink ssl connection to ~p closed", [State#state.clustername]),
    {stop, {ssl_closed, Socket}, State};
handle_info({ssl_error, Socket, Reason}, State) ->
    lager:error("AAE sink ssl connection to ~p closed unexpectedly with: ~p",
                [State#state.clustername, Reason]),
    {stop, {ssl_error, Socket, Reason}, State};
handle_info({Error, Socket, Reason},
            State=#state{socket=MySocket}) when Socket == MySocket ->
    lager:info("AAE sink connection to ~p closed unexpectedly: ~p",
               [State#state.clustername, {Socket, Error, Reason}]),
    {stop, {Socket, Error, Reason}, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% replies: ok
process_msg(?MSG_INIT, Partition, State) ->
    case riak_kv_vnode:hashtree_pid(Partition) of
        {ok, TreePid} ->
            %% monitor the tree and crash if the tree goes away
            monitor(process, TreePid),
            %% tell the reservation coordinator that we are taking this partition.
            riak_repl2_fs_node_reserver:claim_reservation(Partition),
            send_reply(ok, State#state{partition=Partition, tree_pid=TreePid});
        {error, wrong_node} ->
            {stop, wrong_node, State}
    end;

process_msg(?MSG_GET_AAE_BUCKET, {Level,BucketNum,IndexN}, State=#state{tree_pid=TreePid}) ->
    ResponseMsg = riak_kv_index_hashtree:exchange_bucket(IndexN, Level, BucketNum, TreePid),
    send_reply(ResponseMsg, State);

process_msg(?MSG_GET_AAE_SEGMENT, {SegmentNum,IndexN}, State=#state{tree_pid=TreePid}) ->
    ResponseMsg = riak_kv_index_hashtree:exchange_segment(IndexN, SegmentNum, TreePid),
    send_reply(ResponseMsg, State);

%% no reply
process_msg(?MSG_PUT_OBJ, {fs_diff_obj, BObj}, State) ->
    %% may block on worker pool, ok return means work was submitted
    %% to pool, not that put FSM completed successfully.
    ok = riak_repl2_fssink_pool:bin_put(BObj),
    {noreply, State};

%% replies: ok | not_responsible
process_msg(?MSG_UPDATE_TREE, IndexN, State=#state{tree_pid=TreePid}) ->
    ResponseMsg = riak_kv_index_hashtree:update(IndexN, TreePid),
    send_reply(ResponseMsg, State);

%% replies: ok | not_built | already_locked | bad_version
process_msg(?MSG_LOCK_TREE, Version, State=#state{tree_pid=TreePid}) ->
    ResponseMsg = riak_kv_index_hashtree:get_lock(TreePid, fullsync_sink, Version),
    send_reply(ResponseMsg, State).

%% replies: ok | not_built | already_locked | bad_version
process_msg(?MSG_LOCK_TREE, State) ->
    %% NOTE: be sure to die if tcp connection dies, to give back lock
    %% Message coming from an old aae source. Only allow lock if old undefined version
    %% is in use locally.
    process_msg(?MSG_LOCK_TREE, legacy, State);

%% no reply
process_msg(?MSG_COMPLETE, State=#state{owner=Owner}) ->
    riak_repl2_fssink:fullsync_complete(Owner),
    {stop, normal, State}.

%% Send a response back to the aae_source worker

send_reply(Msg, State=#state{sender=Sender}) ->
    Data = term_to_binary(Msg),
    Sender ! <<?MSG_REPLY:8, Data/binary>>,
    {noreply, State}.

%%%===================================================================
%%% TCP send helper
%%%===================================================================

%% Sending is performed in a separate process to ensure that the sink
%% never blocks and can therefore always receive messages + reset the
%% active once flag. If a separate process is not used, it is possible
%% for both the source and sink to deadlock on a TCP send that each
%% stall waiting for the remote side to receive.
%%
%% There is currently no backpressure between the sink and this helper
%% process. Adding backpressure runs the risk of once again hitting
%% the aforementioned deadlock scenario.
%%
%% The correct approach is to add backpressure to the actual fullsync
%% protocol itself. This remains as future work.
%%
%% NOTE: the SSL transport wraps the underlying socket in a gen_server,
%% and all socket functions route through this server. As such, splitting
%% sending and receiving between two processes does not help. The SSL
%% transport is thus known to *not* be safe and *can* deadlock.

spawn_sender(Transport, Socket) ->
    spawn_link(?MODULE, sender_init, [Transport, Socket]).

sender_init(Transport, Socket) ->
    sender_loop({Transport, Socket}).

sender_loop(State={Transport, Socket}) ->
    receive Msg ->
            ok = Transport:send(Socket, Msg)
    end,
    ?MODULE:sender_loop(State).
