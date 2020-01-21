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

-record(state, {}).

%%%===================================================================
%%% API
%%%===================================================================
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec add_subscriber(Pid :: pid()) ->
    ok.
add_subscriber(Pid) ->
    ets:insert(?NODE_WATCHER_SUBS_TABLE, {Pid, []}),
    ok.

-spec remove_subscriber(Pid :: pid()) ->
    ok.
remove_subscriber(Pid) ->
    ets:delete(?NODE_WATCHER_SUBS_TABLE, Pid),
    ok.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
init([]) ->
    ets:new(?NODE_WATCHER_SUBS_TABLE, [named_table, sets]),
    setup_ring_updates_callback(),
    {ok, #state{}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({ring_update, _Update}, State) ->
    handle_ring_update(),
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
        gen_server:cast(Self, {ring_update, Update})
    end,
    riak_core_node_watcher_events:add_sup_callback(Fun),
    ok.

handle_ring_update() ->
    Subscribers = lists:flatten(ets:match(?NODE_WATCHER_SUBS_TABLE, {'$1', []})),
    riak_api_pb_sup:node_watcher_update(Subscribers),
    ok.
