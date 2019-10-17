-module(riak_repl_bq).
-behaviour(gen_server).

-export([start_link/2, q_ack/2, status/1, stop/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include("riak_repl.hrl").

-record(state, {
        transport,
        socket,
        q,
        pending,
        max_pending,
        client
        }).

start_link(Transport, Socket) ->
    gen_server:start_link(?MODULE, [Transport, Socket], []).

q_ack(Pid, Count) ->
    gen_server:cast(Pid, {q_ack, Count}).

status(Pid) ->
    try
        gen_server:call(Pid, status, infinity)
    catch
        _:_ ->
            [{queue, too_busy}]
    end.

stop(Pid) ->
    gen_server:call(Pid, stop, infinity).

%% gen_server

init([Transport, Socket]) ->
    QSize = app_helper:get_env(riak_repl,queue_size,
                               ?REPL_DEFAULT_QUEUE_SIZE),
    MaxPending = app_helper:get_env(riak_repl,server_max_pending,
                                    ?REPL_DEFAULT_MAX_PENDING),
    {ok, C} = riak:local_client(),
    {ok, #state{q = bounded_queue:new(QSize),
                max_pending = MaxPending,
                pending = 0,
                socket=Socket,
                transport=Transport,
                client=C
               }}.


handle_cast({q_ack, Count}, State = #state{pending=Pending}) ->
    drain(State#state{pending=Pending - Count}).

handle_call(status, _From, State = #state{q=Q}) ->
    Status = [{queue_pid, self()},
              {dropped_count, bounded_queue:dropped_count(Q)},
              {queue_length, bounded_queue:len(Q)},
              {queue_byte_size, bounded_queue:byte_size(Q)},
              {queue_max_size, bounded_queue:max_size(Q)},
              {queue_percentage, (bounded_queue:byte_size(Q) * 100) div
               bounded_queue:max_size(Q)},
              {queue_pending, State#state.pending},
              {queue_max_pending, State#state.max_pending}
             ],
    {reply, Status, State};
handle_call(stop, _From, State) ->
    {stop, normal, ok, State}.

handle_info({repl, RObj}, State) ->
    case riak_repl_util:repl_helper_send_realtime(RObj, State#state.client) of
        [] ->
            %% no additional objects to queue
            drain(enqueue(term_to_binary({diff_obj, RObj}), State));
        Objects when is_list(Objects) ->
            %% enqueue all the objects the hook asked us to send as a list.
            %% They're enqueued together so that they can't be dumped from the
            %% queue piecemeal if it overflows
            NewState = enqueue([term_to_binary({diff_obj, O}) ||
                        O <- Objects ++ [RObj]], State),
            drain(NewState);
        cancel ->
            {noreply, State}
    end.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

%% internal

drain(State=#state{q=Q,pending=P,max_pending=M}) when P < M ->
    case bounded_queue:out(Q) of
        {{value, Msg}, NewQ} ->
            drain(send_diffobj(Msg, State#state{q=NewQ}));
        {empty, NewQ} ->
            {noreply, State#state{q=NewQ}}
    end;
drain(State) ->
    {noreply, State}.

enqueue(Msg, State=#state{q=Q}) ->
    State#state{q=bounded_queue:in(Q,Msg)}.

send_diffobj(Msgs, State0) when is_list(Msgs) ->
    %% send all the messages in the list
    %% we correctly increment pending, so we should get enough q_acks
    %% to restore pending to be less than max_pending when we're done.
    lists:foldl(fun(Msg, State) ->
                send_diffobj(Msg, State)
        end, State0, Msgs);
send_diffobj(Msg,State=#state{transport=Transport,socket=Socket,pending=Pending}) ->
    _ = riak_repl_tcp_server:send(Transport, Socket, Msg),
    State#state{pending=Pending+1}.

