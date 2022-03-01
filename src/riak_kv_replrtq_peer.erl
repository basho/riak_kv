%% -------------------------------------------------------------------
%%
%% riak_kv_replrtq_snk: coordination of full-sync replication
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

%% @doc Peer discovery for real-time replication

-module(riak_kv_replrtq_peer).
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-behaviour(gen_server).

-export([init/1,
        handle_call/3,
        handle_cast/2,
        handle_info/2,
        terminate/2,
        code_change/3]).

-export([start_link/0,
            update_discovery/1,
            prompt_discovery/1,
            update_workers/2]).

-type discovery_peer() ::
    {riak_kv_replrtq_snk:queue_name(), [riak_kv_replrtq_snk:peer_info()]}.

-define(DISCOVERY_TIMEOUT_SECONDS, 60).
-define(AUTO_DISCOVERY_MAXIMUM_SECONDS, 3600).
-define(AUTO_DISCOVERY_MINIMUM_SECONDS, 60).

-record(state, {discovery_peers = [] :: list(discovery_peer())}).

%%%============================================================================
%%% API
%%%============================================================================

-spec start_link() -> {ok, pid()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc
%% Prompt for the discovery of peers
-spec update_discovery(riak_kv_replrtq_snk:queue_name()) -> boolean().
update_discovery(QueueName) ->
    gen_server:call(?MODULE, {update_discovery, QueueName}, 60 * 1000).

-spec update_workers(pos_integer(), pos_integer()) -> boolean().
update_workers(WorkerCount, PerPeerLimit) ->
    gen_server:call(?MODULE,
        {update_workers, WorkerCount, PerPeerLimit},
        60 * 1000).

-spec prompt_discovery(discovery_peer()) -> ok.
prompt_discovery({QueueName, PeerInfo}) ->
    gen_server:cast(?MODULE, {prompt_discovery, {QueueName, PeerInfo}}).


%%%============================================================================
%%% gen_server callbacks
%%%============================================================================

init([]) ->
    case app_helper:get_env(riak_kv, replrtq_peer_discovery, false) of
        true -> 
            SinkPeers = app_helper:get_env(riak_kv, replrtq_sinkpeers, ""),
            DefaultQueue = app_helper:get_env(riak_kv, replrtq_sinkqueue),
            SnkQueuePeerInfo =
                riak_kv_replrtq_snk:tokenise_peers(DefaultQueue, SinkPeers),

            MinDelay = 
                app_helper:get_env(riak_kv,
                    replrtq_prompt_min_seconds,
                    ?AUTO_DISCOVERY_MINIMUM_SECONDS),

            lists:foreach(
                fun({QueueName, _PeerInfo}) -> 
                    erlang:send_after(MinDelay * 1000,
                        self(),
                        {prompt_discovery, QueueName})
                end,
                SnkQueuePeerInfo),
            {ok, #state{discovery_peers = SnkQueuePeerInfo}};
        false ->
            {ok, #state{}}
    end.

handle_call({update_discovery, QueueName}, _From, State) ->
    case lists:keyfind(QueueName, 1, State#state.discovery_peers) of
        false ->
            lager:info("Type=~w discovery for unconfigured QueueName=~w",
                [update, QueueName]),
            {reply, false, State};
        {QueueName, PeerInfo} ->
            R = prompt_discovery(QueueName, PeerInfo, update),
            {reply, R, State}
    end;
handle_call({update_workers, WorkerCount, PerPeerLimit}, _From, State) ->
    case riak_kv_replrtq_snk:get_worker_counts() of
        {WorkerCount, PerPeerLimit} ->
            {reply, false, State};
        _ ->
            riak_kv_replrtq_snk:set_worker_counts(WorkerCount, PerPeerLimit),
            lists:foreach(
                fun({QN, PI}) -> prompt_discovery(QN, PI, count_change) end,
                State#state.discovery_peers),
            {reply, true, State}
    end.

handle_cast({prompt_discovery, {QueueName, PeerInfo}}, State) ->
    _ = prompt_discovery(QueueName, PeerInfo, regular),
    {noreply, State}.

handle_info({prompt_discovery, QueueName}, State) ->
    {QueueName, PeerInfo} =
        lists:keyfind(QueueName, 1, State#state.discovery_peers),
    ok = prompt_discovery({QueueName, PeerInfo}),
    MinDelay = 
        app_helper:get_env(riak_kv,
            replrtq_prompt_min_seconds,
            ?AUTO_DISCOVERY_MINIMUM_SECONDS),
    MaxDelay =
        app_helper:get_env(riak_kv,
            replrtq_prompt_max_seconds,
            ?AUTO_DISCOVERY_MAXIMUM_SECONDS),
    Delay = rand:uniform(max(1, MaxDelay - MinDelay)) + MinDelay,
    erlang:send_after(Delay * 1000, self(), {prompt_discovery, QueueName}),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%============================================================================
%%% Internal functions
%%%============================================================================

-spec prompt_discovery(riak_kv_replrtq_snk:queue_name(),
            list(riak_kv_replrtq_snk:peer_info()),
            update|regular|count_change) -> boolean().
prompt_discovery(QueueName, PeerInfo, Type) ->
    {SnkWorkerCount, PerPeerLimit} = riak_kv_replrtq_snk:get_worker_counts(),
    CurrentPeers = 
        case Type of
            count_change ->
                %% Ignore current peers, to update worker counts
                [];
            _ ->
                riak_kv_replrtq_snk:current_peers(QueueName)
        end,
    case discover_peers(PeerInfo) of
        CurrentPeers ->
            lager:info("Type=~w discovery led to no change", [Type]),
            false;
        [] ->
            lager:info("Type=~w discovery led to reset of peers", [Type]),
            riak_kv_replrtq_snk:add_snkqueue(QueueName,
                PeerInfo,
                SnkWorkerCount,
                PerPeerLimit),
            false;
        DiscoveredPeers ->
            lager:info("Type=~w discovery old_peers=~w new_peers=~w",
                [Type, length(CurrentPeers), length(DiscoveredPeers)]),
            riak_kv_replrtq_snk:add_snkqueue(QueueName,
                DiscoveredPeers,
                SnkWorkerCount,
                PerPeerLimit),
            true
    end.

-spec discover_peers(list(riak_kv_replrtq_snk:peer_info()))
            -> list(riak_kv_replrtq_snk:peer_info()).
discover_peers(PeerInfo) ->
    Peers = lists:usort(lists:foldl(fun discover_from_peer/2, [], PeerInfo)),
    StartingDelayMS = riak_kv_replrtq_snk:starting_delay(),
    ConvertToPeerInfoFun =
        fun({IP, Port, Protocol}, Acc) ->
            [{length(Acc) + 1,
                StartingDelayMS,
                binary_to_list(IP), Port, Protocol}|Acc]
        end,
    lists:foldl(ConvertToPeerInfoFun, [], Peers).

    
-spec discover_from_peer(
    riak_kv_replrtq_snk:peer_info(),
    list({binary(), pos_integer(), pb|http}))
            -> list({binary(), pos_integer(), pb|http}).
discover_from_peer(PeerInfo, Acc) ->
    {_PeerID, _Delay, Host, Port, Protocol} = PeerInfo,
    RemoteGenFun = riak_kv_replrtq_snk:remote_client_fun(Protocol, Host, Port),
    RemoteFun = RemoteGenFun(),
    UpdAcc = 
        try
            case RemoteFun(peer_discovery) of
                {ok, IPPorts} ->
                    Acc ++
                        lists:map(
                            fun({IPa, Pa}) -> {IPa, Pa, Protocol} end,
                            lists:usort(IPPorts));
                _ ->
                    Acc
            end
        catch
            Type:Exception ->
                lager:warning(
                    "Peer discovery failed at Peer ~p due to ~w error ~w",
                    [PeerInfo, Type, Exception]),
                Acc
        end,
    RemoteFun(close),
    UpdAcc.
    
    