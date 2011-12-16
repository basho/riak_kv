%% -------------------------------------------------------------------
%%
%% riak_kv_pb_socket: service protocol buffer clients
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

%% @doc service protocol buffer clients

-module(riak_kv_pb_socket).
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.
-include_lib("riakc/include/riakclient_pb.hrl").
-include_lib("riakc/include/riakc_pb.hrl").
-include_lib("riak_pipe/include/riak_pipe.hrl").
-behaviour(gen_server).

-export([start_link/0, set_socket/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-type msg() ::  atom() | tuple().

-record(state, {sock,      % protocol buffers socket
                client,    % local client
                req,       % current request (for multi-message requests like list keys)
                req_ctx,   % context to go along with request (partial results, request ids etc)
                client_id = <<0,0,0,0>> }). % emulate legacy API when vnode_vclocks is true

-record(pipe_ctx, {pipe,     % pipe handling mapred request
                   ref,      % easier-access ref/reqid
                   timer,    % ref() for timeout send_after
                   sender,   % {pid(), monitor()} of process sending inputs
                   has_mr_query}). % true if the request contains a query.

-define(PROTO_MAJOR, 1).
-define(PROTO_MINOR, 0).
-define(DEFAULT_TIMEOUT, 60000).

%% ===================================================================
%% Public API
%% ===================================================================

start_link() ->
    gen_server2:start_link(?MODULE, [], []).

set_socket(Pid, Socket) ->
    gen_server2:call(Pid, {set_socket, Socket}).

init([]) -> 
    riak_kv_stat:update(pbc_connect),
    {ok, C} = riak:local_client(),
    {ok, #state{client = C}}.

handle_call({set_socket, Socket}, _From, State) ->
    inet:setopts(Socket, [{active, once}, {packet, 4}, {header, 1}]),
    {reply, ok, State#state{sock = Socket}}.

handle_cast(_Msg, State) -> 
    {noreply, State}.

handle_info({tcp_closed, Socket}, State=#state{sock=Socket}) ->
    {stop, normal, State};
handle_info({tcp_error, Socket, _Reason}, State=#state{sock=Socket}) ->
    {stop, normal, State};
handle_info({tcp, _Sock, Data}, State=#state{sock=Socket, req=undefined}) ->
    [MsgCode|MsgData] = Data,
    Msg = riakc_pb:decode(MsgCode, MsgData),
    case process_message(Msg, State) of
        {pause, NewState} ->
            ok;
        NewState ->
            inet:setopts(Socket, [{active, once}])
    end,
    {noreply, NewState};
handle_info({tcp, _Sock, _Data}, State) ->
    %% req =/= undefined: received a new request while another was in
    %% progress -> Error
    lager:error("Received a new PB socket request"
                " while another was in progress"),
    {stop, normal, State};

%% Handle responses from stream_list_keys 
handle_info({ReqId, done},
            State=#state{sock = Socket, req=#rpblistkeysreq{}, req_ctx=ReqId}) ->
    NewState = send_msg(#rpblistkeysresp{done = 1}, State),
    inet:setopts(Socket, [{active, once}]),
    {noreply, NewState#state{req = undefined, req_ctx = undefined}};
handle_info({ReqId, From, {keys, []}}, State=#state{req=#rpblistkeysreq{}, req_ctx=ReqId}) ->
    riak_kv_keys_fsm:ack_keys(From),
    {noreply, State}; % No keys - no need to send a message, will send done soon.
handle_info({ReqId, {keys, []}}, State=#state{req=#rpblistkeysreq{}, req_ctx=ReqId}) ->
    {noreply, State}; % No keys - no need to send a message, will send done soon.
handle_info({ReqId, From, {keys, Keys}}, State=#state{req=#rpblistkeysreq{}, req_ctx=ReqId}) ->
    riak_kv_keys_fsm:ack_keys(From),
    {noreply, send_msg(#rpblistkeysresp{keys = Keys}, State)};
handle_info({ReqId, {keys, Keys}}, State=#state{req=#rpblistkeysreq{}, req_ctx=ReqId}) ->
    {noreply, send_msg(#rpblistkeysresp{keys = Keys}, State)};
handle_info({ReqId, Error},
            State=#state{sock = Socket, req=#rpblistkeysreq{}, req_ctx=ReqId}) ->
    NewState = send_error("~p", [Error], State),
    inet:setopts(Socket, [{active, once}]),
    {noreply, NewState#state{req = undefined, req_ctx = undefined}};

%% PIPE Handle response from mapred_stream
handle_info(#pipe_eoi{ref=ReqId},
            State=#state{req=#rpbmapredreq{},
                         req_ctx=#pipe_ctx{ref=ReqId,
                                           timer=Timer}}) ->
    NewState = send_msg(#rpbmapredresp{done = 1}, State),
    erlang:cancel_timer(Timer),
    {noreply, NewState#state{req = undefined, req_ctx = undefined}};

handle_info(#pipe_result{ref=ReqId, from=PhaseId, result=Res},
            State=#state{req=#rpbmapredreq{content_type = ContentType}, 
                         req_ctx=#pipe_ctx{ref=ReqId, has_mr_query=HasMRQuery}=PipeCtx}) ->
    case encode_mapred_phase([Res], ContentType, HasMRQuery) of
        {error, Reason} ->
            erlang:cancel_timer(PipeCtx#pipe_ctx.timer),
            %% destroying the pipe will automatically kill the sender
            riak_pipe:destroy(PipeCtx#pipe_ctx.pipe),
            NewState = send_error("~p", [Reason], State),
            {noreply, NewState#state{req = undefined, req_ctx = undefined}};
        Response ->
            {noreply, send_msg(#rpbmapredresp{phase=PhaseId, 
                                              response=Response}, State)}
    end;
handle_info(#pipe_log{ref=ReqId, from=From, msg=Msg},
            State=#state{req=#rpbmapredreq{},
                         req_ctx=#pipe_ctx{ref=ReqId}=PipeCtx}) ->
    case Msg of
        {trace, [error], {error, Info}} ->
            erlang:cancel_timer(PipeCtx#pipe_ctx.timer),
            %% destroying the pipe will automatically kill the sender
            riak_pipe:destroy(PipeCtx#pipe_ctx.pipe),
            JsonInfo = {struct, riak_kv_mapred_json:jsonify_pipe_error(
                                  From, Info)},
            NewState = send_error(mochijson2:encode(JsonInfo), [], State),
            {noreply, NewState#state{req = undefined, req_ctx = undefined}};
        _ ->
            {noreply, State}
    end;
handle_info({'DOWN', Ref, process, Pid, Reason},
            State=#state{req=#rpbmapredreq{},
                         req_ctx=#pipe_ctx{sender={Pid, Ref}}=PipeCtx}) ->
    %% the async input sender exited
    if Reason == normal ->
            %% just reached the end of the input sending - all is
            %% well, continue processing
            NewPipeCtx = PipeCtx#pipe_ctx{sender=undefined},
            {noreply, State#state{req_ctx=NewPipeCtx}};
       true ->
            %% something went wrong sending inputs - tell the client
            %% about it, and shutdown the pipe
            erlang:cancel_timer(PipeCtx#pipe_ctx.timer),
            riak_pipe:destroy(PipeCtx#pipe_ctx.pipe),
            lager:error("Error sending inputs: ~p", [Reason]),
            NewState = send_error("Error sending inputs: ~p", [Reason], State),
            {noreply, NewState#state{req=undefined, req_ctx=undefined}}
    end;
handle_info({pipe_timeout, Ref},
            State=#state{req=#rpbmapredreq{},
                         req_ctx=#pipe_ctx{ref=Ref,
                                           pipe=Pipe}}) ->
    NewState = send_error("timeout", [], State),
    %% destroying the pipe will automatically kill the sender
    riak_pipe:destroy(Pipe),
    {noreply, NewState#state{req=undefined, req_ctx=undefined}};
%% ignore #pipe_log for now, since riak_kv_mrc_pipe does not enable it

%% LEGACY Handle response from mapred_stream/mapred_bucket_stream
handle_info({flow_results, ReqId, done},
            State=#state{sock = Socket, req=#rpbmapredreq{}, req_ctx=ReqId}) ->
    NewState = send_msg(#rpbmapredresp{done = 1}, State),
    inet:setopts(Socket, [{active, once}]),
    {noreply, NewState#state{req = undefined, req_ctx = undefined}};

handle_info({flow_results, ReqId, {error, Reason}},
            State=#state{sock = Socket, req=#rpbmapredreq{}, req_ctx=ReqId}) ->
    NewState = send_error("~p", [Reason], State),
    inet:setopts(Socket, [{active, once}]),
    {noreply, NewState#state{req = undefined, req_ctx = undefined}};

handle_info({flow_results, PhaseId, ReqId, Res},
            State=#state{sock=Socket,
                         req=#rpbmapredreq{content_type = ContentType}, 
                         req_ctx=ReqId}) ->
    case encode_mapred_phase(Res, ContentType, true) of
        {error, Reason} ->
            NewState = send_error("~p", [Reason], State),
            inet:setopts(Socket, [{active, once}]),
            {noreply, NewState#state{req = undefined, req_ctx = undefined}};
        Response ->
            {noreply, send_msg(#rpbmapredresp{phase=PhaseId, 
                                              response=Response}, State)}
    end;

handle_info({flow_error, ReqId, Error},
            State=#state{sock = Socket, req=#rpbmapredreq{}, req_ctx=ReqId}) ->
    NewState = send_error("~p", [Error], State),
    inet:setopts(Socket, [{active, once}]),
    {noreply, NewState#state{req = undefined, req_ctx = undefined}};

handle_info(_, State) -> % Ignore any late replies from gen_servers/messages from fsms
    {noreply, State}.

terminate(_Reason, _State) -> 
    riak_kv_stat:update(pbc_disconnect),
    ok.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

%% ===================================================================
%% Message Handling
%% ===================================================================

%% Process an incoming protocol buffers message.  Return either
%% a new #state{} if new incoming messages should be received
%% or {pause, #state{}} if the incoming TCP socket should not be
%% set active again.
%%
%% If 'pause' is returned, it needs to be re-enabled by whatever
%% callbacks are waiting for it.
%%
-spec process_message(msg(), #state{}) ->  #state{} | {pause, #state{}}.
process_message(rpbpingreq, State) ->
    send_msg(rpbpingresp, State);

process_message(rpbgetclientidreq, #state{client=C, client_id=CID} = State) ->
    ClientId = case app_helper:get_env(riak_kv, vnode_vclocks, false) of
                   true -> CID;
                   false -> C:get_client_id()
               end,
    Resp = #rpbgetclientidresp{client_id = ClientId},
    send_msg(Resp, State);

process_message(#rpbsetclientidreq{client_id = ClientId}, State) ->
    NewState = case app_helper:get_env(riak_kv, vnode_vclocks, false) of
                   true -> State#state{client_id=ClientId};
                   false ->
                       {ok, C} = riak:local_client(ClientId),
                       State#state{client = C}
               end,
    send_msg(rpbsetclientidresp, NewState);

process_message(rpbgetserverinforeq, State) ->
    Resp = #rpbgetserverinforesp{node = riakc_pb:to_binary(node()), 
                                 server_version = get_riak_version()},
    send_msg(Resp, State);

process_message(#rpbgetreq{bucket=B, key=K, r=R0, pr=PR0, notfound_ok=NFOk,
                           basic_quorum=BQ, if_modified=VClock,
                           head=Head, deletedvclock=DeletedVClock}, #state{client=C} = State) ->
    R = normalize_rw_value(R0),
    PR = normalize_rw_value(PR0),
    case C:get(B, K, make_option(deletedvclock, DeletedVClock) ++
                     make_option(r, R) ++
                     make_option(pr, PR) ++
                     make_option(notfound_ok, NFOk) ++
                     make_option(basic_quorum, BQ)) of
        {ok, O} ->
            case erlify_rpbvc(VClock) == riak_object:vclock(O) of
                true ->
                    send_msg(#rpbgetresp{unchanged = true}, State);
                _ ->
                    Contents = riak_object:get_contents(O),
                    PbContent = case Head of
                        true ->
                            %% Remove all the 'value' fields from the contents
                            %% This is a rough equivalent of a REST HEAD
                            %% request
                            BlankContents = [{MD, <<>>} || {MD, _} <- Contents],
                            riakc_pb:pbify_rpbcontents(BlankContents, []);
                        _ ->
                            riakc_pb:pbify_rpbcontents(Contents, [])
                    end,
                    GetResp = #rpbgetresp{content = PbContent,
                        vclock = pbify_rpbvc(riak_object:vclock(O))},
                    send_msg(GetResp, State)
            end;
        {error, {deleted, TombstoneVClock}} ->
            %% Found a tombstone - return its vector clock so it can
            %% be properly overwritten
            send_msg(#rpbgetresp{vclock = pbify_rpbvc(TombstoneVClock)}, State);
        {error, notfound} ->
            send_msg(#rpbgetresp{}, State);
        {error, Reason} ->
            send_error("~p", [Reason], State)
    end;

process_message(#rpbputreq{bucket=B, key=K, vclock=PbVC,
                           if_not_modified=NotMod, if_none_match=NoneMatch} = Req,
                #state{client=C} = State) when NotMod; NoneMatch ->
    case C:get(B, K) of
        {ok, _} when NoneMatch ->
            send_error("match_found", [], State);
        {ok, O} when NotMod ->
            case erlify_rpbvc(PbVC) == riak_object:vclock(O) of
                true ->
                    process_message(Req#rpbputreq{if_not_modified=undefined,
                                                  if_none_match=undefined},
                                    State);
                _ ->
                    send_error("modified", [], State)
            end;
        {error, _} when NoneMatch ->
            process_message(Req#rpbputreq{if_not_modified=undefined,
                                          if_none_match=undefined},
                            State);
        {error, notfound} when NotMod ->
            send_error("notfound", [], State);
        {error, Reason} ->
            send_error("~p", [Reason], State)
    end;
process_message(#rpbputreq{bucket=B, key=K, vclock=PbVC, content=RpbContent,
                           w=W0, dw=DW0, pw=PW0, return_body=ReturnBody,
                           return_head=ReturnHead},
                #state{client=C} = State) ->

    case K of
        undefined ->
            % Generate a key, the user didn't supply one
            Key = list_to_binary(riak_core_util:unique_id_62()),
            ReturnKey = Key;
        _ ->
            Key = K,
            % Don't return the key since we're not generating one
            ReturnKey = undefined
    end,
    O0 = riak_object:new(B, Key, <<>>),
    O1 = update_rpbcontent(O0, RpbContent),
    O  = update_pbvc(O1, PbVC),
    % erlang_protobuffs encodes as 1/0/undefined
    W = normalize_rw_value(W0),
    DW = normalize_rw_value(DW0),
    PW = normalize_rw_value(PW0),
    Options = case ReturnBody of
        1 -> [returnbody];
        true -> [returnbody];
        _ ->
            case ReturnHead of
                true -> [returnbody];
                _ -> []
            end
    end,
    case C:put(O, make_option(w, W) ++ make_option(dw, DW) ++
                   make_option(pw, PW) ++ [{timeout, default_timeout()} | Options]) of
        ok when is_binary(ReturnKey) ->
            PutResp = #rpbputresp{key = ReturnKey},
            send_msg(PutResp, State);
        ok ->
            send_msg(#rpbputresp{}, State);
        {ok, Obj} ->
            Contents = riak_object:get_contents(Obj),
            PbContents = case ReturnHead of
                true ->
                    %% Remove all the 'value' fields from the contents
                    %% This is a rough equivalent of a REST HEAD
                    %% request
                    BlankContents = [{MD, <<>>} || {MD, _} <- Contents],
                    riakc_pb:pbify_rpbcontents(BlankContents, []);
                _ ->
                    riakc_pb:pbify_rpbcontents(Contents, [])
            end,
            PutResp = #rpbputresp{content = PbContents,
                                  vclock = pbify_rpbvc(riak_object:vclock(Obj)),
                                  key = ReturnKey
                              },
            send_msg(PutResp, State);
        {error, notfound} ->
            send_msg(#rpbputresp{}, State);
        {error, Reason} ->
            send_error("~p", [Reason], State)
    end;

process_message(#rpbdelreq{bucket=B, key=K, vclock=PbVc,
                          r=R0, w=W0, pr=PR0, pw=PW0, dw=DW0, rw=RW0},
                #state{client=C} = State) ->
    W = normalize_rw_value(W0),
    PW = normalize_rw_value(PW0),
    DW = normalize_rw_value(DW0),
    R = normalize_rw_value(R0),
    PR = normalize_rw_value(PR0),
    RW = normalize_rw_value(RW0),

    Options = make_option(r, R) ++
              make_option(w, W) ++
              make_option(rw, RW) ++
              make_option(pr, PR) ++
              make_option(pw, PW) ++
              make_option(dw, DW),
    Result = case PbVc of
        undefined ->
            C:delete(B, K, Options);
        _ ->
            VClock = erlify_rpbvc(PbVc),
            C:delete_vclock(B, K, VClock, Options)
    end,
    case Result of
        ok ->
            send_msg(rpbdelresp, State);
        {error, notfound} ->  %% delete succeeds if already deleted
            send_msg(rpbdelresp, State);
        {error, Reason} ->
            send_error("~p", [Reason], State)
    end;

process_message(rpblistbucketsreq, 
                #state{client=C} = State) ->
    case C:list_buckets() of
        {ok, Buckets} ->
            send_msg(#rpblistbucketsresp{buckets = Buckets}, State);
        {error, Reason} ->
            send_error("~p", [Reason], State)
    end;

%% Start streaming in list keys 
process_message(#rpblistkeysreq{bucket=B}=Req, 
                #state{client=C} = State) ->
    %% Pause incoming packets - stream_list_keys results
    %% will be processed by handle_info, it will 
    %% set socket active again on completion of streaming.
    {ok, ReqId} = C:stream_list_keys(B),
    {pause, State#state{req = Req, req_ctx = ReqId}};

%% Get bucket properties
process_message(#rpbgetbucketreq{bucket=B}, 
                #state{client=C} = State) ->
    Props = C:get_bucket(B),
    PbProps = riakc_pb:pbify_rpbbucketprops(Props),
    send_msg(#rpbgetbucketresp{props = PbProps}, State);

%% Set bucket properties
process_message(#rpbsetbucketreq{bucket=B, props = PbProps}, 
                #state{client=C} = State) ->
    Props = riakc_pb:erlify_rpbbucketprops(PbProps),
    case C:set_bucket(B, Props) of
        ok ->
            send_msg(rpbsetbucketresp, State);
        {error, Details} ->
            send_error("Invalid bucket properties: ~p", [Details], State)
    end;

%% TODO: refactor, cleanup
%% Start map/reduce job - results will be processed in handle_info
process_message(#rpbmapredreq{request=MrReq, content_type=ContentType}=Req, 
                State) ->
    case decode_mapred_query(MrReq, ContentType) of
        {error, Reason} ->
            send_error("~p", [Reason], State);

        {ok, Inputs, Query, Timeout} ->
            case riak_kv_util:mapred_system() of
                pipe ->
                    pipe_mapreduce(Req, State, Inputs, Query, Timeout);
                legacy ->
                    legacy_mapreduce(Req, State, Inputs, Query, Timeout)
            end
    end.

pipe_mapreduce(Req, State, Inputs, Query, Timeout) ->
    try riak_kv_mrc_pipe:mapred_stream(Query) of
        {{ok, Pipe}, _NumKeeps} ->
            PipeRef = (Pipe#pipe.sink)#fitting.ref,
            Timer = erlang:send_after(Timeout, self(),
                                      {pipe_timeout, PipeRef}),
            {InputSender, SenderMonitor} =
                riak_kv_mrc_pipe:send_inputs_async(Pipe, Inputs),
            Ctx = #pipe_ctx{pipe=Pipe,
                            ref=PipeRef,
                            timer=Timer,
                            sender={InputSender, SenderMonitor},
                            has_mr_query = (Query /= [])},
            State#state{req=Req, req_ctx=Ctx}
    catch throw:{badarg, Fitting, Reason} ->
            send_error("Phase ~p: ~s", [Fitting, Reason], State),
            State
    end.

legacy_mapreduce(#rpbmapredreq{content_type=ContentType}=Req,
                 #state{client=C}=State, Inputs, Query, Timeout) ->
    ResultTransformer = get_result_transformer(ContentType),
    case is_binary(Inputs) orelse is_key_filter(Inputs) of
        true ->
            case C:mapred_bucket_stream(Inputs, Query, 
                                        self(), ResultTransformer, Timeout) of
                {stop, Error} ->
                    send_error("~p", [Error], State);

                {ok, ReqId} ->
                    {pause, State#state{req = Req, req_ctx = ReqId}}
            end;
        false ->
            case is_list(Inputs) of
                true ->
                    case C:mapred_stream(Query, self(), ResultTransformer, Timeout) of
                        {stop, Error} ->
                            send_error("~p", [Error], State);

                        {ok, {ReqId, FSM}} ->
                            luke_flow:add_inputs(FSM, Inputs),
                            luke_flow:finish_inputs(FSM),
                            %% Pause incoming packets - map/reduce results
                            %% will be processed by handle_info, it will 
                            %% set socket active again on completion of streaming.
                            {pause, State#state{req = Req, req_ctx = ReqId}}
                    end;
                false ->
                    case is_tuple(Inputs) andalso size(Inputs)==4 andalso
                        element(1, Inputs) == modfun andalso
                        is_atom(element(2, Inputs)) andalso
                        is_atom(element(3, Inputs)) of
                        true ->
                            case C:mapred_stream(Query, self(), ResultTransformer, Timeout) of
                                {stop, Error} ->
                                    send_error("~p", [Error], State);

                                {ok, {ReqId, FSM}} ->
                                    C:mapred_dynamic_inputs_stream(
                                      FSM, Inputs, Timeout),
                                    luke_flow:finish_inputs(FSM),
                                    %% Pause incoming packets - map/reduce results
                                    %% will be processed by handle_info, it will 
                                    %% set socket active again on completion of streaming.
                                    {pause, State#state{req = Req, req_ctx = ReqId}}
                            end;
                        false -> 
                            send_error("~p", [bad_mapred_inputs], State)
                    end
            end
    end.

%% Send a message to the client
-spec send_msg(msg(), #state{}) -> #state{}.
send_msg(Msg, State) ->
    Pkt = riakc_pb:encode(Msg),
    gen_tcp:send(State#state.sock, Pkt),
    State.
    
%% Send an error to the client
-spec send_error(string(), list(), #state{}) -> #state{}.
send_error(Msg, Fmt, State) ->
    send_error(Msg, Fmt, ?RIAKC_ERR_GENERAL, State).

-spec send_error(string(), list(), non_neg_integer(), #state{}) -> #state{}.
send_error(Msg, Fmt, ErrCode, State) ->
    %% protocol buffers accepts nested lists for binaries so no need to flatten the list
    ErrMsg = io_lib:format(Msg, Fmt),
    send_msg(#rpberrorresp{errmsg=ErrMsg, errcode=ErrCode}, State).

%% Update riak_object with the pbcontent provided
update_rpbcontent(O0, RpbContent) -> 
    {MetaData, Value} = riakc_pb:erlify_rpbcontent(RpbContent),
    O1 = riak_object:update_metadata(O0, MetaData),
    riak_object:update_value(O1, Value).

%% Update riak_object with vector clock 
update_pbvc(O0, PbVc) ->
    Vclock = erlify_rpbvc(PbVc),
    riak_object:set_vclock(O0, Vclock).

%% return a key/value tuple that we can ++ to other options so long as the
%% value is not default or undefined -- those values are pulled from the
%% bucket by the get/put FSMs.
make_option(_, undefined) ->
    [];
make_option(_, default) ->
    [];
make_option(K, V) ->
    [{K, V}].

default_timeout() ->
    60000.

%% Convert a vector clock to erlang
erlify_rpbvc(undefined) ->
    vclock:fresh();
erlify_rpbvc(<<>>) ->
    vclock:fresh();
erlify_rpbvc(PbVc) ->
    binary_to_term(zlib:unzip(PbVc)).

%% Convert a vector clock to protocol buffers
pbify_rpbvc(Vc) ->
    zlib:zip(term_to_binary(Vc)).

%% Return the current version of riak_kv
-spec get_riak_version() -> binary().
get_riak_version() ->
    {ok, Vsn} = application:get_key(riak_kv, vsn),
    riakc_pb:to_binary(Vsn).

%% Decode a mapred query
%% {ok, ParsedInputs, ParsedQuery, Timeout};
decode_mapred_query(Query, <<"application/json">>) ->
    riak_kv_mapred_json:parse_request(Query);
decode_mapred_query(Query, <<"application/x-erlang-binary">>) ->
    riak_kv_mapred_term:parse_request(Query);
decode_mapred_query(_Query, ContentType) ->
    {error, {unknown_content_type, ContentType}}.

%% Detect key filtering
is_key_filter({Bucket, Filters}) when is_binary(Bucket),
                                      is_list(Filters) ->
    true;
is_key_filter(_) ->
    false.

%% Convert a map/reduce phase to the encoding requested
encode_mapred_phase(Res, <<"application/json">>, HasMRQuery) ->
    Res1 = riak_kv_mapred_json:jsonify_bkeys(Res, HasMRQuery),
    mochijson2:encode(Res1);
encode_mapred_phase(Res, <<"application/x-erlang-binary">>, _) ->
    term_to_binary(Res);
encode_mapred_phase(_Res, ContentType, _) ->
    {error, {unknown_content_type, ContentType}}.

normalize_rw_value(?RIAKC_RW_ONE) -> one;
normalize_rw_value(?RIAKC_RW_QUORUM) -> quorum;
normalize_rw_value(?RIAKC_RW_ALL) -> all;
normalize_rw_value(?RIAKC_RW_DEFAULT) -> default;
normalize_rw_value(V) -> V.

%% get a result transformer for the content-type
%% jsonify not_founds for application/json
%% do nothing otherwise
get_result_transformer(<<"application/json">>) ->
    fun riak_kv_mapred_json:jsonify_not_found/1;
get_result_transformer(_) ->
    undefined.
