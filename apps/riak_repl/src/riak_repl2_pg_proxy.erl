%% Riak EnterpriseDS
%% Copyright 2007-2013 Basho Technologies, Inc. All Rights Reserved.
%%
%% pg_proxy keeps track of which node is servicing proxy_get requests
%% in the cluster. A client can always make requests to the leader
%% pg_proxy, which will then be routed to the appropriate node in the
%% cluster.
%%

-module(riak_repl2_pg_proxy).

-behaviour(gen_server).

%% API
-export([start_link/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
        terminate/2, code_change/3, proxy_get/4]).

-include("riak_repl.hrl").

-define(SERVER, ?MODULE).

-record(state, {
        pg_pids = []
        }).

%%%===================================================================
%%% API
%%%===================================================================
proxy_get(Pid, Bucket, Key, Options) ->
    gen_server:call(Pid, {proxy_get, Bucket, Key, Options}, ?LONG_TIMEOUT).

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @end
%%--------------------------------------------------------------------
-spec start_link(ProxyName :: string()) -> {'ok', pid()}.
start_link(ProxyName) ->
    gen_server:start_link(?MODULE, ProxyName, []).

%%%===================================================================
%%% Gen_server callbacks
%%%===================================================================

init(ProxyName) ->
    lager:debug("Registering pg_proxy ~p", [ProxyName]),
    erlang:register(ProxyName, self()),
    {ok, #state{}}.

handle_call({proxy_get, Bucket, Key, GetOptions}, _From,
            #state{pg_pids=RequesterPids} = State) ->
    case RequesterPids of
        [] ->
            lager:warning("No proxy_get node registered"),
            {reply, {error, no_proxy_get_node}, State};
        [{_RNode, RPid, _} | T] ->
            try gen_server:call(RPid, {proxy_get, Bucket, Key, GetOptions}, ?LONG_TIMEOUT) of
                Result ->
                    {reply, Result, State}
            catch
                %% remove this bad pid from the list and try another
                exit:{noproc, _} ->
                    handle_call({proxy_get, Bucket, Key, GetOptions}, _From,
                        State#state{pg_pids=T});
                exit:{{nodedown, _}, _} ->
                    handle_call({proxy_get, Bucket, Key, GetOptions}, _From,
                        State#state{pg_pids=T})
            end
    end;

handle_call({register, ClusterName, RequesterNode, RequesterPid},
            _From, State = #state{pg_pids = RequesterPids}) ->
    lager:info("registered node for cluster name ~p ~p ~p", [ClusterName,
                                                             RequesterNode,
                                                             RequesterPid]),
    Monitor = erlang:monitor(process, RequesterPid),
    NewState = State#state{pg_pids = [{RequesterNode, RequesterPid, Monitor} |
                                      RequesterPids]},
    {reply, ok, NewState}.

handle_info({'DOWN', MRef, process, _Pid, _Reason}, State =
            #state{pg_pids=RequesterPids}) ->
    NewRequesterPids = [ {RNode, RPid, RMon} ||
            {RNode,RPid,RMon} <- RequesterPids,
            RMon /= MRef],
    {noreply, State#state{pg_pids=NewRequesterPids}};
handle_info(_Info, State) ->
    {noreply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

