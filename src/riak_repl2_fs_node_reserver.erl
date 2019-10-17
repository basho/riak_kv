%% @doc Hold reservations for new sink processes on the node. It takes into
%% account the running sinks and how many reservations there are for that node.
-module(riak_repl2_fs_node_reserver).
-include("riak_repl.hrl").
-behaviour(gen_server).
-define(SERVER, ?MODULE).

-record(state, {
    reservations = []
    }).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/0]).
-export([reserve/1, unreserve/1, claim_reservation/1]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

%% @doc Start the reservation server on the local node, registering to the
%% module name.
-spec start_link() -> {'ok', pid()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% @doc Find the node for given partition, and issue a reservation on that node.
%% If the node is down or a reservation process is not running, `down' is
%% returned. A reservation that is unclaimed for 20 seconds is released.
-spec reserve(Partition :: any()) -> 'ok' | 'busy' | 'down'.
reserve(Partition) ->
    Node = get_partition_node(Partition),
    % I don't want to crash the caller if the node is down
    %% TODO: add explicit timeout to this call
    %% TODO: add timeout handling to catch?
    try gen_server:call({?SERVER, Node}, {reserve, Partition}) of
        Out -> Out
    catch
        exit:{noproc, _} ->
            down;
        exit:{{nodedown, _}, _} ->
            down
    end.

%% @doc Release a reservation for the given partition on the correct
%%      node.  If the node is down or a reservation process is not running,
%%      `down' is returned.
-spec unreserve(Partition :: any()) -> 'ok' | 'busy' | 'down'.
unreserve(Partition) ->
    Node = get_partition_node(Partition),
    try gen_server:call({?SERVER, Node}, {unreserve, Partition}) of
        Out ->
            Out
    catch
        exit:{noproc, _} ->
            down;
        exit:{{nodedown, _}, _} ->
            down
    end.

%% @doc Indicates a reservation has been converted to a running sink. Usually
%% used by a sink.
-spec claim_reservation(Partition :: any()) -> 'ok'.
claim_reservation(Partition) ->
    Node = get_partition_node(Partition),
    gen_server:cast({?SERVER, Node}, {claim_reservation, Partition}).


%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

%% @hidden
init(_Args) ->
    {ok, #state{}}.

%% @hidden
handle_call({reserve, Partition}, _From, State) ->
    Kids = supervisor:which_children(riak_repl2_fssink_sup),
    Max = app_helper:get_env(riak_repl, max_fssink_node, ?DEFAULT_MAX_SINKS_NODE),
    Running = length(Kids),
    Reserved = length(State#state.reservations),
    if
        (Running + Reserved) < Max ->
            Tref = erlang:send_after(?RESERVATION_TIMEOUT, self(), {reservation_expired, Partition}),
            Reserved2 = [{Partition, Tref} | State#state.reservations],
            {reply, ok, State#state{reservations = Reserved2}};
        true ->
            lager:info("Node busy for partition ~p. running=~p reserved=~p max=~p",
                        [Partition, Running, Reserved, Max]),
            {reply, busy, State}
    end;

%% @hidden
%% This message is a call to prevent unreserve/reserve races, as well as
%% detect failed processes.
handle_call({unreserve, Partition}, _From, State) ->
    Reserved2 = cancel_reservation_timeout(Partition, State#state.reservations),
    {reply, ok, State#state{reservations = Reserved2}};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.


%% @hidden
handle_cast({claim_reservation, Partition}, State) ->
    Reserved2 = cancel_reservation_timeout(Partition, State#state.reservations),
    {noreply, State#state{reservations = Reserved2}};

handle_cast(_Msg, State) ->
    {noreply, State}.


%% @hidden
handle_info({reservation_expired, Partition}, State) ->
    Reserved2 = cancel_reservation_timeout(Partition, State#state.reservations),
    {noreply, State#state{reservations = Reserved2}};

handle_info(_Info, State) ->
    {noreply, State}.


%% @hidden
terminate(_Reason, _State) ->
    ok.


%% @hidden
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

get_partition_node(Partition) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Owners = riak_core_ring:all_owners(Ring),
    proplists:get_value(Partition, Owners).

cancel_reservation_timeout(Partition, Reserved) ->
    case proplists:get_value(Partition, Reserved) of
        undefined ->
            Reserved;
        Tref ->
            _ = erlang:cancel_timer(Tref),
            proplists:delete(Partition, Reserved)
    end.

