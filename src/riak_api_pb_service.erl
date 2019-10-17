%% -------------------------------------------------------------------
%%
%% riak_api_pb_service: Riak Client APIs Protocol Buffers Services
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

%% @doc Encapsulates the behaviour and registration of
%% application-specific interfaces exposed over the Protocol Buffers
%% API. Service modules should implement the behaviour, and the host
%% applications should register them on startup like so:
%%
%% <pre>
%%   %% Register a single range of message codes to the `foo' service
%%   ok = riak_api_pb_service:register(foo, 101, 102),
%%
%%   %% Register multiple services at once
%%   ok = riak_api_pb_service:register([{bar, 200, 210},
%%                                      {baz, 211, 214}]).
%% </pre>
%%
%% On shutdown, host applications should deregister services they
%% registered on startup. The same arguments as were sent to
%% `register/1,2,3' are valid arguments to `deregister/1,2,3'.
%%
%% <pre>
%%    %% Deregister all my services
%%    ok = riak_api_pb_service:deregister([{foo, 101, 102},
%%                                         {bar, 200, 210},
%%                                         {baz, 211, 214}]).
%% </pre>
%%
%% == Behaviour Callbacks ==
%%
%% Service modules must export five callbacks:
%%
%% ```
%% init() ->
%%     State.
%%
%%     State = term()
%% '''
%%
%% The `init/0' callback is called to create any state that the
%% service may need to operate. The return value of this callback will
%% be passed to the service in other callbacks.
%%
%% ```
%% decode(Code, Message) ->
%%     {ok, DecodedMessage} |
%%     {ok, DecodedMessage, PermAndTarget} |
%%     {error, Reason}.
%%
%%     Code = non_neg_integer()
%%     Message = binary()
%%     DecodedMessage = Reason = term()
%%     PermAndTarget = perm_and_target()
%% '''
%%
%% The `decode/2' callback is handed a message code and wire message
%% that is registered to this service and should decode it into an
%% Erlang term that can be handled by the `process/2' callback. If the
%% message does not decode properly, it should return an `error' tuple
%% with an appropriate reason. The decoded message may optionally
%% include a permission and target tuple used by the security system
%% to restrict access to the operation.  Most services will simply
%% delegate encoding to the `riak_pb' application.
%%
%% ```
%% encode(Message) ->
%%     {ok, EncodedMessage} |
%%     Error.
%%
%%     Message = Error = term()
%%     EncodedMessage = iodata()
%% '''
%%
%% The `encode/1' callback is given an Erlang term that is a reply
%% message to a client and should encode that message as `iodata',
%% including an appropriate message code on the head. Any return value
%% other than `{ok, iodata()}' will be interpreted as an error. Again,
%% most services will simply delegate encoding to the `riak_pb'
%% application.
%%
%% ```
%% process(Message, State) ->
%%     {reply, ReplyMessage, NewState} |
%%     {reply, {stream, ReqId}, NewState} |
%%     {error, Error, NewState}.
%%
%%     Message = State = ReqId = ReplyMessage = NewState = term()
%%     Error = iodata() | {format, term()} | {format, io:format(), [term()]}
%% '''
%%
%% The `process/2' callback is where the work of servicing a client
%% request is done. The return value of the `decode/2' callback will
%% be passed as `Message' and the last value of the service state as
%% `State' (as returned from `init/0' or a previous invocation of
%% `process/2' or `process_stream/3'). The callback should return
%% 3-tuples, any of which include the modified service state as the
%% last entry. The first form is a simple reply in which a single
%% message is returned to the client (passing through `encode/1' along
%% the way). The second form moves the service into streaming mode,
%% using the returned `ReqId' (see `process_stream/3').
%%
%% The third form replies with an error to the client, the second
%% entry (`Error') becoming the `errmsg' field of the `rpberrorresp'
%% message. If it is iodata, it will be passed verbatim to the client.
%% If it is the `format' 2-tuple, it will be formatted with the "~p"
%% format string before being sent to the client. If it is the
%% `format' 3-tuple, the list of terms will be formatted against the
%% format string before being sent to the client.
%%
%% ```
%% process_stream(Message, ReqId, State) ->
%%     {reply, Reply, NewState} |
%%     {ignore, NewState} |
%%     {done, Reply, NewState} |
%%     {done, NewState} |
%%     {error, Error, NewState}.
%%
%%     Message = ReqId = State = NewState = term()
%%     Reply = [ term() ] | term()
%%     Error = iodata() | {format, term()} | {format, io:format(), [term()]}
%% '''
%%
%% The `process_stream/3' callback is invoked when the socket/server
%% process receives a message from another Erlang process while in
%% streaming mode. The passed `ReqId' is the value returned from
%% `process/2' when streaming mode was started, and is usually used to
%% identify or ignore incoming messages from other processes. Like
%% `process/2', the state of the service is passed as the last
%% argument.
%%
%% As with `process/2', the last entry of all return-value tuples
%% should be the updated state of the service. Also, similarly to
%% `process/2', a `reply' tuple will result in sending a normal
%% message to the client. When `Reply' is a list, this will be
%% interpreted as sending multiple messages to the client in one pass.
%% Error tuples have similar semantics to `process/2', but will also
%% cause the service to exit streaming mode. The `ignore' tuple will
%% cause the server to do nothing. The `done' tuples have the same
%% semantics as `reply' (including multi-message replies) and `ignore'
%% but signal a normal end of the streaming operation.
%% @end

-module(riak_api_pb_service).
-compile([{no_auto_import, [register/2]}]).

%% Service-provider API
-export([register/1,
         register/2,
         register/3,
         deregister/1,
         deregister/2,
         deregister/3,
         swap/3]).

-type registration() :: {Service::module(), MinCode::pos_integer(), MaxCode::pos_integer()}.

-export_type([registration/0]).

-callback init() -> State :: term().

-type perm_and_target() :: {Permission :: string(), Target :: term()}.
-callback decode(Code :: non_neg_integer(), Message :: binary()) ->
    {ok, DecodedMessage :: term()} |
    {ok, DecodedMessage :: term(), perm_and_target()} |
    {error, Reason :: term()}.

-callback encode(Message :: term()) ->
    {ok, EncodedMessage :: iodata()} |
    term().

-type process_error() :: iodata() |
                         {format, term()} |
                         {format, io:format(), [term()]}.

-callback process(Message :: term(), State :: term()) ->
    {reply, ReplyMessage :: term(), NewState :: term()} |
    {reply, {stream, ReqId :: term()}, NewState :: term()} |
    {error, Error :: process_error(), NewState :: term()}.

-callback process_stream(Message :: term(), ReqId :: term(), State :: term()) ->
    {reply, Reply :: [term()] | term(), NewState :: term()} |
    {ignore, NewState :: term()} |
    {done, Reply :: [term()] | term(), NewState :: term()} |
    {done, NewState :: term()} |
    {error, Error :: process_error(), NewState :: term()}.

%% @doc Registers a number of services at once.
%% @see register/3
-spec register([registration()]) -> ok | {error, Reason::term()}.
register([]) ->
    ok;
register(List) ->
    riak_api_pb_registrar:register(List).

%% @doc Registers a service module for a given message code.
%% @equiv register(Module, Code, Code)
%% @see register/3
-spec register(Module::module(), Code::pos_integer()) -> ok | {error, Err::term()}.
register(Module, Code) ->
    register([{Module, Code, Code}]).

%% @doc Registers a service module for a given range of message
%% codes. The service module must implement the behaviour and be able
%% to decode and process messages for the given range of message
%% codes.  Service modules should be registered before the riak_api
%% application starts.
-spec register(Module::module(), MinCode::pos_integer(), MaxCode::pos_integer()) -> ok | {error, Err::term()}.
register(Module, MinCode, MaxCode) ->
    register([{Module, MinCode, MaxCode}]).

%% @doc Removes the registration of a number of services modules at
%% once.
%% @see deregister/3
-spec deregister([registration()]) -> ok | {error, Reason::term()}.
deregister([]) ->
    ok;
deregister(List) ->
    riak_api_pb_registrar:deregister(List).

%% @doc Removes the registration of a previously-registered service
%% module. Inputs will be validated such that the registered module
%% must match the one being removed.
-spec deregister(Module::module(), Code::pos_integer()) -> ok | {error, Err::term()}.
deregister(Module, Code) ->
    deregister([{Module, Code, Code}]).

%% @doc Removes the registration of a previously-registered service
%% module.
%% @see deregister/2
-spec deregister(Module::module(), MinCode::pos_integer(), MaxCode::pos_integer()) -> ok | {error, Err::term()}.
deregister(Module, MinCode, MaxCode) ->
    deregister([{Module, MinCode, MaxCode}]).

%% @doc Perform an atomic swap of current module to `NewModule' for
%% the given code range.  The code range must exactly match an
%% existing range.  Otherwise an error is returned.
-spec swap(module(), pos_integer(), pos_integer()) -> ok | {error, Err::term()}.
swap(NewModule, MinCode, MaxCode) ->
    riak_api_pb_registrar:swap(NewModule, MinCode, MaxCode).
