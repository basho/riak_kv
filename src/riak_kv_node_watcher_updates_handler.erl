%%%-------------------------------------------------------------------
%%% @author paulhunt
%%% @copyright (C) 2019, bet365
%%% @doc
%%%
%%% @end
%%% Created : 10. Dec 2019 08:51
%%%-------------------------------------------------------------------
-module(riak_kv_node_watcher_updates_handler).
-author("paulhunt").

-behaviour(gen_server).

%% API
-export([
    start_link/0,
    add_subscriber/1,
    remove_subscriber/1
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-define(SERVER, ?MODULE).
-define(NODE_WATCHER_SUBS_TABLE, node_watcher_subscribers).
-define(RING_UPDATE, ring_update).
-define(ADD_SUBSCRIBER, add_subscriber).
-define(REMOVE_SUBSCRIBER, remove_subscriber).

-record(state, {
    node_watcher_subs_tid :: ets:tid()
}).

%%%===================================================================
%%% API
%%%===================================================================
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec add_subscriber(Pid :: pid()) ->
    ok.
add_subscriber(Pid) ->
    gen_server:cast(?SERVER, {?ADD_SUBSCRIBER, Pid}),
    ok.

-spec remove_subscriber(Pid :: pid()) ->
    ok.
remove_subscriber(Pid) ->
    gen_server:cast(?SERVER, {?REMOVE_SUBSCRIBER, Pid}),
    ok.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
init([]) ->
    NodeWatcherSubsTid = ets:new(?NODE_WATCHER_SUBS_TABLE, []),
    setup_ring_updates_callback(),
    {ok, #state{node_watcher_subs_tid = NodeWatcherSubsTid}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({?RING_UPDATE, _Update}, State = #state{node_watcher_subs_tid = Tid}) ->
    handle_ring_update(Tid),
    {noreply, State};
handle_cast({?ADD_SUBSCRIBER, Pid}, State = #state{node_watcher_subs_tid = Tid}) ->
    ets:insert(Tid, {Pid, []}),
    {noreply, State};
handle_cast({?REMOVE_SUBSCRIBER, Pid}, State = #state{node_watcher_subs_tid = Tid}) ->
    ets:insert(Tid, Pid),
    {noreply, State};
handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
setup_ring_updates_callback() ->
    Self = erlang:self(),
    Fun = fun(Update) ->
        gen_server:cast(Self, {?RING_UPDATE, Update})
    end,
    riak_core_node_watcher_events:add_sup_callback(Fun),
    ok.

handle_ring_update(NodeWatcherSubsTid) ->
    Subscribers = lists:flatten(ets:match(NodeWatcherSubsTid, {'$1', []})),
    riak_api_pb_sup:node_watcher_update(Subscribers),
    ok.
