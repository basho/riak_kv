%% -------------------------------------------------------------------
%%
%% riak_api_pb_registration_helper: PB API Registration table manager
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc A gen_server process that creates and serves as heir to the
%% message-code registration ETS table. Should the registrar process
%% exit, this server will inherit the ETS table and hand it back to
%% the registrar process when it restarts.
-module(riak_api_pb_registration_helper).

-behaviour(gen_server).

-include("riak_api_pb_registrar.hrl").

%% API
-export([start_link/0,
         claim_table/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @end
%%--------------------------------------------------------------------
-spec start_link() -> {ok, Pid::pid()} | ignore | {error, Error::term()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).


%%--------------------------------------------------------------------
%% @doc
%% Gives the registration table away to the caller, which should be
%% the registrar process.
%%
%% @end
%%--------------------------------------------------------------------
-spec claim_table() -> ok.
claim_table() ->
    gen_server:call(?SERVER, claim_table, infinity).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%% @end
%%--------------------------------------------------------------------
-spec init([]) -> {ok, undefined}.
init([]) ->
    case ets:info(?ETS_NAME) of
        undefined ->
            %% Table does not exist, so we create the table and wait
            %% for the registrar to claim it.
            ?ETS_NAME = ets:new(?ETS_NAME, ?ETS_OPTS),
            {ok, undefined};
        List when is_list(List) ->
            %% This process must have been restarted, because the table
            %% already exists. Let's try to become the heir again.
            lager:debug("PB registration helper restarted as ~p, becoming heir", [self()]),
            riak_api_pb_registrar:set_heir(self()),
            {ok, undefined}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%% @end
%%--------------------------------------------------------------------
-spec handle_call(Msg::term(), From::{pid(), term()}, State::term()) ->
                         {reply, Reply::term(), State::term()} |
                         {noreply, State::term()}.
handle_call(claim_table, {Pid, _Tag}, State) ->
    %% The registrar is (re-)claiming the table, let's give it away. We
    %% assume this process is the heir, which is set on startup or
    %% transfer of the table.
    lager:debug("Giving away PB registration table to ~p", [Pid]),
    ets:give_away(?ETS_NAME, Pid, undefined),
    Reply = ok,
    {reply, Reply, State};

handle_call(_Msg, _From, State) ->
    {noreply, State}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%% @end
%%--------------------------------------------------------------------
-spec handle_cast(term(), term()) -> {noreply, State::term()}.
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
-spec handle_info(term(), term()) -> {noreply, State::term()}.
handle_info({'ETS-TRANSFER', ?ETS_NAME, FromPid, _HeirData}, State) ->
    %% The registrar process exited and transferred the table back to
    %% the helper.
    lager:debug("PB Registrar ~p exited, ~p received table", [FromPid, self()]),
    {noreply, State};

handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @end
%%--------------------------------------------------------------------
-spec terminate(term(), term()) -> ok.
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @end
%%--------------------------------------------------------------------
-spec code_change(term(), term(), term()) -> {ok, term()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
