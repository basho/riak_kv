%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc
%% This gen_server is reponsible for bootstrapping consensus ensembles
%% used by riak_kv to provide strong consistency. The server polls the
%% ring periodically and registers any missing ensembles with the
%% riak_ensemble_manager.

-module(riak_kv_ensembles).
-behaviour(gen_server).

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {last_ring_id :: term()}).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    schedule_tick(),
    {ok, #state{last_ring_id = undefined}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(tick, State) ->
    State2 = tick(State),
    {noreply, State2};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

schedule_tick() ->
    erlang:send_after(10000, ?MODULE, tick).

tick(State=#state{last_ring_id=LastID}) ->
    case riak_core_ring_manager:get_ring_id() of
        LastID ->
            State;
        RingID ->
            {ok, Ring, CHBin} = riak_core_ring_manager:get_raw_ring_chashbin(),
            maybe_bootstrap_ensembles(Ring, CHBin),
            schedule_tick(),
            State#state{last_ring_id=RingID}
    end.

maybe_bootstrap_ensembles(Ring, CHBin) ->
    IsClaimant = (riak_core_ring:claimant(Ring) == node()),
    IsReady = riak_core_ring:ring_ready(Ring),
    case IsClaimant and IsReady of
        true ->
            bootstrap_preflists(Ring, CHBin);
        false ->
            ok
    end.

bootstrap_preflists(Ring, CHBin) ->
    AllN = riak_core_bucket:all_n(Ring),
    Owners = riak_core_ring:all_owners(Ring),
    AllPL = [{Idx, N} || {Idx, _} <- Owners,
                         N <- AllN],
    Ensembles = riak_ensemble_manager:rget(ensembles, []),
    Known = [{Idx, N} || {{kv, Idx, N}, _} <- Ensembles],
    Need = AllPL -- Known,
    io:format("All: ~p~nKnown ~p~nNeed: ~p~n", [AllPL, Known, Need]),
    [begin
         {PL, _} = chashbin:itr_pop(N, chashbin:exact_iterator(Idx, CHBin)),
         %% TODO: Make ensembles/peers use ensemble/peer as actual peer name so this is unneeded
         Peers = [{{kv, Idx, N, Idx2}, Node} || {Idx2, Node} <- PL],
         riak_ensemble_manager:create_ensemble({kv, Idx, N}, undefined, Peers,
                                               riak_kv_ensemble_backend, []),
         ok
     end || {Idx, N} <- Need],
    ok.
