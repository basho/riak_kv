%%% File        : replrtq_snk_monitor.erl
%%% Author      : Ulf Norell
%%% Description :
%%% Created     : 10 Jun 2019 by Ulf Norell
-module(replrtq_snk_monitor).

-compile([export_all, nowarn_export_all]).

-behaviour(gen_server).

%% API
-export([start_link/0, stop/0, fetch/2, push/4, suspend/1, resume/1,
         add_queue/4, remove_queue/1, update_workers/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {queues = [], peers = #{}, traces = #{}}).
-record(queue, {ref, name, peers, workers, peerlimit}).

%% -- API functions ----------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

stop() ->
    gen_server:call(?SERVER, stop).

fetch(Client, QueueName) ->
    gen_server:call(?SERVER, {fetch, Client, QueueName}).

push(RObj, Bool, List, LocalClient) ->
    gen_server:call(?SERVER, {push, RObj, Bool, List, LocalClient}).

add_queue(Queue, Peers, Workers, Peerlimit) ->
    gen_server:call(?SERVER, {add_queue, Queue, Peers, Workers, Peerlimit}).

remove_queue(Queue) ->
    gen_server:call(?SERVER, {remove, Queue}).

suspend(Queue) ->
    gen_server:call(?SERVER, {suspend, Queue}).

resume(Queue) ->
    gen_server:call(?SERVER, {resume, Queue}).

update_workers(Queue, Workers) ->
    gen_server:call(?SERVER, {update_workers, Queue, Workers}).

create(Host, Port, _, _) ->
    {Host, Port}.

%% -- Callbacks --------------------------------------------------------------

init([]) ->
    {ok, #state{}}.

handle_call({add_queue, Queue, Peers, Workers, Peerlimit}, _From, State) ->
    Ref = make_ref(),
    PeerMap = maps:from_list([{{{Host, Port}, Queue}, {Ref, Cfg}} || {{Host, Port, http}, Cfg} <- Peers]),
    Q = #queue{ref = Ref, name = Queue, peers = Peers, workers = Workers, peerlimit = Peerlimit},
    State1 = State#state{ queues = [Q | State#state.queues],
                          peers  = maps:merge(State#state.peers, PeerMap) },
    {reply, ok, add_trace(State1, Queue, {workers, Workers})};
handle_call({fetch, Client, QueueName}, From, State = #state{ peers = Peers }) ->
    State1 = add_trace(State, QueueName, {fetch, Client}),
    case maps:get({Client, QueueName}, Peers, undefined) of
        undefined       ->
            catch replrtq_mock:error({bad_fetch, Client, QueueName}),
            {reply, error, State};
        {_Ref, {Active, Delay}} ->
            erlang:send_after(Delay, self(), {return, From, QueueName, Active}),
            {noreply, State1}
    end;
handle_call({push, _RObj, _Bool, _List, _LocalClient}, _From, State) ->
    {reply, {ok, os:timestamp()}, State};
handle_call(stop, _From, State) ->
    State1 = lists:foldl(fun(R, S) -> add_trace(S, R, stop) end, State,
                         maps:keys(State#state.traces)),
    Ret = [ final_trace(State1, R) || R <- maps:keys(State1#state.traces) ],
    {stop, normal, Ret, State1};
handle_call({remove, Queue}, _From, State) ->
    {reply, ok, add_trace(State, Queue, remove)};
handle_call({suspend, Queue}, _From, State) ->
    {reply, ok, add_trace(State, Queue, suspend)};
handle_call({resume, Queue}, _From, State) ->
    {reply, ok, add_trace(State, Queue, resume)};
handle_call({update_workers, Q, Workers}, _From, State) ->
    Queue = lists:keyfind(Q, #queue.name, State#state.queues),
    NewQueue = Queue#queue{workers = Workers, peerlimit = Workers},
    State1 = State#state{ queues = [NewQueue | State#state.queues -- [Queue]]},
    {reply, ok, add_trace(State1, Q, {workers, Workers})};
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({return, From, QueueName, Active}, State) ->
    Reply =
        case Active of
            active   -> {ok, <<"riak_obj">>};
            inactive -> {ok, queue_empty};
            error    -> {error, no_client}
        end,
    gen_server:reply(From, Reply),
    {noreply, add_trace(State, QueueName, {return, Active})};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% -- Internal functions -----------------------------------------------------

add_trace(S, undefined, _) -> S;
add_trace(S = #state{traces = Traces}, Ref, Event) when is_reference(Ref) ->
    Trace0 = maps:get(Ref, Traces, []),
    S#state{ traces = Traces#{ Ref => [{os:timestamp(), Event} | Trace0] } };
add_trace(S, QueueName, Event) ->
    add_trace(S, get_ref(S, QueueName), Event).

get_ref(#state{queues = Queues}, QueueName) ->
    case lists:keyfind(QueueName, #queue.name, Queues) of
        #queue{ref = Ref} when is_reference(Ref) -> Ref;
        false -> undefined
    end.

final_trace(#state{ queues = Queues, traces = Traces }, Ref) ->
    Trace = maps:get(Ref, Traces),
    #queue{name = Name, peers = Peers, workers = Workers, peerlimit = PL} = lists:keyfind(Ref, #queue.ref, Queues),
    {Name, PL, Peers, Workers, lists:reverse(Trace)}.
