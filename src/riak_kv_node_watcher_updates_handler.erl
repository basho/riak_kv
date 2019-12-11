%%%-------------------------------------------------------------------
%%% @author paulhunt
%%% @copyright (C) 2019, <COMPANY>
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
    start_link/0
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

-record(state, {}).

%%%===================================================================
%%% API
%%%===================================================================
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
init([]) ->
    setup_ring_updates_callback(),
    {ok, #state{}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({ring_update, _Update}, State) ->
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
