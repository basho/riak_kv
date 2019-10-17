%% Riak EnterpriseDS
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved.
%%
%% Riak MDC "Advanced Mode" Proxy-Get

-module(riak_repl2_pg).

%% @doc Riak CS Proxy-get
%%
%%
-export([start_link/0, status/0]).
-export([enable/1, disable/1, enabled/0]).
-export([register_remote_locator/0, ensure_pg/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-record(state, {sinks = []}).

%% API - is there any state? who watches ring events?
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

status() ->
    LeaderNode = riak_repl2_leader:leader_node(),
    case LeaderNode of
        undefined ->
            [{proxy_get, []}];
        _ ->
            ReqStats = try riak_repl2_pg_block_requester_sup:started(LeaderNode) of
                [] ->
                    [];
                PGRs ->
                    [riak_repl2_pg_block_requester:status(Pid) || {_Remote, Pid} <- PGRs]
            catch
                _:_ ->
                    []
            end,
            ProStats = try riak_repl2_pg_block_provider_sup:enabled(LeaderNode) of
                [] ->
                    [];
                PGPs ->
                    [{Remote, riak_repl2_pg_block_provider:status(Pid)} || {Remote, Pid} <- PGPs]
                catch
                    _:_ ->
                        []
            end,
            [{proxy_get,[{requester,ReqStats}, {provider, ProStats}]}]
    end.

enabled() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    riak_repl_ring:pg_enabled(Ring).

enable(Remote) ->
    do_ring_trans(fun riak_repl_ring:pg_enable_trans/2, Remote).

disable(Remote) ->
    F = fun(Ring, Remote1) ->
            R2 = case riak_repl_ring:pg_disable_trans(Ring, Remote1) of
                {new_ring, R1} ->
                    R1;
                {ignore, _} ->
                    Ring
            end,
            riak_repl_ring:pg_disable_trans(R2, Remote1)
    end,
    do_ring_trans(F, Remote).

ensure_pg(WantEnabled0) ->
    WantEnabled = lists:usort(WantEnabled0),
    Enabled = [Remote || {Remote, _Pid} <- riak_repl2_pg_block_provider_sup:enabled()],
    ToEnable  = WantEnabled -- Enabled,
    ToDisable = Enabled -- WantEnabled,

    case ToEnable ++ ToDisable of
        [] ->
            [];
        _ ->
            lager:debug("proxy_get ToEnable : ~p", [ToEnable]),
            lager:debug("proxy_get ToDisable: ~p", [ToDisable]),
            _ = [riak_repl2_pg_block_provider_sup:enable(Remote) ||
                Remote <- ToEnable],
            _ = [riak_repl2_pg_block_provider_sup:disable(Remote) ||
                Remote<- ToDisable],
            [{enabled, ToEnable},
             {disabled, ToDisable}]
    end.

register_remote_locator() ->
    Locator = fun(Name, _Policy) ->
            riak_core_cluster_mgr:get_ipaddrs_of_cluster(Name)
    end,
    ok = riak_core_connection_mgr:register_locator(proxy_get, Locator).

%% gen_server callbacks
init([]) ->
     {ok, #state{}}.

handle_call(status, _From, State) ->
    Status = {proxy_get, not_implemented},
    {reply, Status, State}.

handle_cast(Msg, State) ->
    lager:warning("Proxy-get received an unknown cast: ~p", [Msg]),
    {noreply, State}.

handle_info({'DOWN', _MRef, process, SinkPid, _Reason}, 
            State = #state{sinks = Sinks}) ->
    Sinks2 = Sinks -- [SinkPid],
    {noreply, State#state{sinks = Sinks2}};
handle_info(Msg, State) ->
    lager:warning("unhandled message - e.g. timed out status result: ~p", Msg),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


do_ring_trans(F, A) ->
    case riak_core_ring_manager:ring_trans(F, A) of
        {ok, _} ->
            ok;
        ER ->
            ER
    end.
