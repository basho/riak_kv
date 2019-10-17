%% -------------------------------------------------------------------
%%
%% riak_api_pb_registrar: Riak Client APIs Protocol Buffers Service Registration
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

%% @doc Encapsulates the Protocol Buffers service registration and
%% deregistration as a gen_server process. This is used to serialize
%% write access to the registration table so that it is less prone to
%% race-conditions.

-module(riak_api_pb_registrar).

-behaviour(gen_server).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-compile([export_all, nowarn_export_all]).
-endif.

-define(SERVER, ?MODULE).

%% External exports
-export([
         start_link/0,
         register/1,
         deregister/1,
         swap/3,
         set_heir/1,
         services/0,
         lookup/1
        ]).

-record(state, {
          opq = [] :: [ tuple() ], %% A list of registrations to reply to once we have the table
          owned = false :: boolean() %% Whether the registrar owns the table yet
         }).

-include("riak_api_pb_registrar.hrl").

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%%--------------------------------------------------------------------
%%% Public API
%%--------------------------------------------------------------------

%% @doc Starts the registrar server
-spec start_link() -> {ok, pid()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% @doc Registers a range of message codes to named services.
-spec register([riak_api_pb_service:registration()]) -> ok | {error, Reason::term()}.
register(Registrations) ->
    gen_server:call(?SERVER, {register, Registrations}, infinity).

%% @doc Deregisters a range of message codes registered to named services.
-spec deregister([riak_api_pb_service:registration()]) -> ok | {error, Reason::term()}.
deregister(Registrations) ->
    try
        gen_server:call(?SERVER, {deregister, Registrations}, infinity)
    catch
        exit:{noproc, _} ->
            %% We assume riak_api is shutting down, and so silently
            %% ignore the deregistration.
            lager:debug("Deregistration ~p ignored, ~s not present", [Registrations, ?SERVER]),
            ok
    end.

%% @doc Atomically swap currently registered module with `NewModule'.
-spec swap(module(), pos_integer(), pos_integer()) -> ok | {error, Reason::term()}.
swap(NewModule, MinCode, MaxCode) ->
    gen_server:call(?SERVER, {swap, {NewModule, MinCode, MaxCode}}, infinity).

%% @doc Sets the heir of the registrations table on behalf of the
%%      helper process.
%% @private
-spec set_heir(pid()) -> ok.
set_heir(Pid) ->
    gen_server:call(?SERVER, {set_heir, Pid}, infinity).

%% @doc Lists registered service modules.
-spec services() -> [ module() ].
services() ->
    lists:usort([ Service || {_Code, Service} <- ets:tab2list(?ETS_NAME)]).

%% @doc Looks up the registration of a given message code.
-spec lookup(non_neg_integer()) -> {ok, module()} | error.
lookup(Code) ->
    case ets:lookup(?ETS_NAME, Code) of
        [{Code, Service}] ->  {ok, Service};
        _ -> error
    end.

%%--------------------------------------------------------------------
%%% gen_server callbacks
%%--------------------------------------------------------------------
init([]) ->
    ok = riak_api_pb_registration_helper:claim_table(),
    {ok, #state{}}.

handle_call({Op, Args}, From, #state{opq=OpQ, owned=false}=State) ->
    %% Since we don't own the table yet, we enqueue the registration
    %% operations until we get the ETS-TRANSFER message.
    {noreply, State#state{opq=[{{Op, Args}, From}|OpQ]}};
handle_call({set_heir, Pid}, _From, #state{owned=true}=State) ->
    ets:setopts(?ETS_NAME, [{heir, Pid, undefined}]),
    {reply, ok, State};
handle_call({register, Registrations}, _From, State) ->
    Reply = do_register(Registrations),
    {reply, Reply, State};
handle_call({deregister, Registrations}, _From, State) ->
    Reply = do_deregister(Registrations),
    {reply, Reply, State};
handle_call({swap, {NewModule, MinCode, MaxCode}}, _From, State) ->
    Reply = do_swap(NewModule, MinCode, MaxCode),
    {reply, Reply, State}.


handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'ETS-TRANSFER', ?ETS_NAME, _, _}, #state{opq=OpQ, owned=false}=State) ->
    %% We've queued up a bunch of registration/deregistration ops,
    %% lets process them now that we have the table.
    NewState = lists:foldr(fun queue_folder/2, State#state{opq=[], owned=true}, OpQ),
    {noreply, NewState};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------
queue_folder({Op, From}, FState) ->
    {reply, Reply, NewFState} = handle_call(Op, From, FState),
    gen_server:reply(From, Reply),
    NewFState.

do_register([]) ->
    ok;
do_register([{Module, MinCode, MaxCode}|Rest]) ->
    case do_register(Module, MinCode, MaxCode) of
        ok ->
            do_register(Rest);
        Error ->
            Error
    end.

do_register(_Module, MinCode, MaxCode) when MinCode > MaxCode orelse
                                            MinCode < 1 orelse
                                            MaxCode < 1 ->
    {error, invalid_message_code_range};
do_register(Module, MinCode, MaxCode) ->
    CodeRange = lists:seq(MinCode, MaxCode),
    case lists:filter(fun is_registered/1, CodeRange) of
        [] ->
            ets:insert(?ETS_NAME, [{Code, Module} || Code <- CodeRange ]),
            riak_api_pb_sup:service_registered(Module),
            ok;
        AlreadyClaimed ->
            {error, {already_registered, AlreadyClaimed}}
    end.

do_swap(NewModule, MinCode, MaxCode) ->
    CodeRange = lists:seq(MinCode, MaxCode),
    Matching = lists:filter(fun is_registered/1, CodeRange),
    case length(Matching) == length(CodeRange) of
        true ->
            ets:insert(?ETS_NAME, [{Code, NewModule} || Code <- CodeRange]),
            riak_api_pb_sup:service_registered(NewModule);
        false ->
            {error, {range_not_registered, CodeRange}}
    end.

do_deregister([]) ->
    ok;
do_deregister([{Module, MinCode, MaxCode}|Rest]) ->
    case do_deregister(Module, MinCode, MaxCode) of
        ok ->
            do_deregister(Rest);
        Other ->
            Other
    end.

do_deregister(_Module, MinCode, MaxCode) when MinCode > MaxCode orelse
MinCode < 1 orelse
MaxCode < 1 ->
    {error, invalid_message_code_range};
do_deregister(Module, MinCode, MaxCode) ->
    CodeRange = lists:seq(MinCode, MaxCode),
    %% Figure out whether all of the codes can be deregistered.
    Mapper = fun(I) ->
                     case ets:lookup(?ETS_NAME, I) of
                         [] ->
                             {error, {unregistered, I}};
                         [{I, Module}] ->
                             I;
                         [{I, _OtherModule}] ->
                             {error, {not_owned, I}}
                     end
             end,
    ToRemove = [ Mapper(I) || I <- CodeRange ],
    case ToRemove of
        CodeRange ->
            %% All codes are valid, so remove them.
            _ = [ ets:delete(?ETS_NAME, Code) || Code <- CodeRange ],
            riak_api_pb_sup:service_registered(Module),
            ok;
        _ ->
            %% There was at least one error, return it.
            lists:keyfind(error, 1, ToRemove)
    end.

is_registered(Code) ->
    ets:member(?ETS_NAME, Code).

-ifdef(TEST).

test_start() ->
    %% Since registration is now a pair of processes, we need both.
    {ok, test_start(riak_api_pb_registration_helper), test_start(?MODULE)}.

test_start(Module) ->
    case gen_server:start({local, Module}, Module, [], []) of
        {error, {already_started, Pid}} ->
            exit(Pid, brutal_kill),
            test_start(Module);
        {ok, Pid} ->
            Pid
    end.

setup() ->
    {ok, HelperPid, Pid} = test_start(),
    {Pid, HelperPid}.

cleanup({Pid, HelperPid}) ->
    exit(Pid, brutal_kill),
    exit(HelperPid, brutal_kill).

deregister_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      %% Deregister a previously registered service
      ?_assertEqual(ok, begin
                            ok = riak_api_pb_service:register(foo, 1, 2),
                            riak_api_pb_service:deregister(foo, 1, 2)
                        end),
      %% Invalid deregistration: range is invalid
      ?_assertEqual({error, invalid_message_code_range}, riak_api_pb_service:deregister(foo, 2, 1)),
      %% Invalid deregistration: unregistered range
      ?_assertEqual({error, {unregistered, 1}}, riak_api_pb_service:deregister(foo, 1, 1)),
      %% Invalid deregistration: registered to other service
      ?_assertEqual({error, {not_owned, 1}}, begin
                                                 ok = riak_api_pb_service:register(foo, 1, 2),
                                                 riak_api_pb_service:deregister(bar, 1)
                                             end),
      %% Deregister multiple
      ?_assertEqual(ok, begin
                            ok = riak_api_pb_service:register([{foo, 1, 2}, {bar, 3, 4}]),
                            riak_api_pb_service:deregister([{bar, 3, 4}, {foo, 1, 2}])
                        end)
     ]}.

register_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      %% Valid registration range
      ?_assertEqual({ok, foo}, begin
                             ok = riak_api_pb_service:register(foo,1,2),
                             lookup(1)
                         end),
      %% Registration ranges that are invalid
      ?_assertEqual({error, invalid_message_code_range},
                    riak_api_pb_service:register(foo, 2, 1)),
      ?_assertEqual({error, {already_registered, [1, 2]}},
                    begin
                        ok = riak_api_pb_service:register(foo, 1, 2),
                        riak_api_pb_service:register(bar, 1, 3)
                    end),
      %% Register multiple
      ?_assertEqual(ok, register([{foo, 1, 2}, {bar, 3, 4}]))
     ]}.

services_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      ?_assertEqual([], services()),
      ?_assertEqual([bar, foo], begin
                                    riak_api_pb_service:register(foo, 1, 2),
                                    riak_api_pb_service:register(bar, 3, 4),
                                    services()
                                end)
     ]}.

registration_inheritance_test_() ->
    {foreach,
     fun setup/0,
     fun({Pid, HelperPid}) ->
         [ exit(P, brutal_kill) || P <- [Pid, HelperPid],
                                   is_process_alive(P) ]
     end,
     [
      %% Killing registrar causes helper to receive table. Restarting
      %% it causes it to become owner again.
      ?_test(begin
                 Helper = whereis(riak_api_pb_registration_helper),
                 Registrar = whereis(?MODULE),
                 exit(Registrar, brutal_kill),
                 erlang:yield(),
                 ?assertEqual(Helper, proplists:get_value(owner, ets:info(?ETS_NAME))),
                 NewReg = test_start(?MODULE),
                 erlang:yield(),
                 ?assertEqual(NewReg, proplists:get_value(owner, ets:info(?ETS_NAME))),
                 ?assertEqual(Helper, proplists:get_value(heir, ets:info(?ETS_NAME))),
                 exit(NewReg, brutal_kill)
             end),

      %% Killing and restarting helper causes it to become
      %% the heir again.
      ?_test(begin
                 Helper = whereis(riak_api_pb_registration_helper),
                 exit(Helper, brutal_kill),
                 erlang:yield(),
                 NewHelper = test_start(riak_api_pb_registration_helper),
                 erlang:yield(),
                 ?assertEqual(NewHelper, proplists:get_value(heir, ets:info(?ETS_NAME)))
             end),

      %% Registrar should queue up requests while it is not in
      %% ownership of the table.
      ?_test(begin
                 Outer = self(),
                 %% Helper = whereis(riak_api_pb_registration_helper),
                 ?assertEqual(ok, riak_api_pb_service:register(bar, 1000, 1000)),
                 Registrar = whereis(?MODULE),
                 exit(Registrar, brutal_kill),
                 meck:new(riak_api_pb_registration_helper, [passthrough]),
                 meck:expect(riak_api_pb_registration_helper, handle_call,
                             fun(claim_table, {Pid, _Tag}=From, State) ->
                                     gen_server:reply(From, ok),
                                     timer:sleep(100),
                                     ets:give_away(?ETS_NAME, Pid, undefined),
                                     {noreply, State}
                             end),
                 NewReg = test_start(?MODULE),
                 spawn(fun() ->
                               Outer ! {100, riak_api_pb_service:register(foo, 100, 100)}
                       end),
                 spawn(fun() ->
                               Outer ! {101, riak_api_pb_service:register(foo, 101, 101)}
                       end),
                 spawn(fun() ->
                               Outer ! {1000, riak_api_pb_service:swap(foo, 1000, 1000)}
                       end),
                 ?assertEqual(ok, receive {100, Msg} -> Msg after 1500 -> fail end),
                 ?assertEqual(ok, receive {101, Msg} -> Msg after 1500 -> fail end),
                 ?assertEqual(ok, receive {1000, Msg} -> Msg after 1500 -> fail end),
                 meck:unload(),
                 exit(NewReg, brutal_kill)
             end)

     ]}.

-endif.
