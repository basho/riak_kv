%% -------------------------------------------------------------------
%%
%% riak_kv_pb_mapred: Expose KV MapReduce functionality to Protocol Buffers
%%
%% Copyright (c) 2012 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc <p>The PB Service for MapReduce processing in Riak KV. This
%% covers the following request messages in the original protocol:</p>
%%
%% <pre>
%% 23 - RpbMapRedReq
%% </pre>
%%
%% <p>This service produces the following responses:</p>
%%
%% <pre>
%% 24 - RpbMapRedResp{1,}
%% </pre>
%% @end

-module(riak_kv_pb_mapred).

-include_lib("riak_pb/include/riak_kv_pb.hrl").
-include_lib("riak_pipe/include/riak_pipe.hrl").
-include("riak_kv_mrc_sink.hrl").

-behaviour(riak_api_pb_service).

-export([init/0,
         decode/2,
         encode/1,
         process/2,
         process_stream/3]).

-record(state, {req,
                req_ctx,
                client}).

-record(pipe_ctx, {pipe,     % pipe handling mapred request
                   ref,      % easier-access ref/reqid
                   timer,    % ref() for timeout send_after
                   sender,   % {pid(), monitor()} of process sending inputs
                   sink,     % {pid(), monitor()} of process collecting outputs
                   has_mr_query}). % true if the request contains a query.

-record(sink_part, {ref :: reference(),
                    resps=[] :: list(),
                    done=false :: boolean()}).

init() ->
    {ok, C} = riak:local_client(),
    #state{client=C}.

decode(Code, Bin) ->
    {ok, riak_pb_codec:decode(Code, Bin)}.

encode(Message) ->
    {ok, riak_pb_codec:encode(Message)}.

%% Start map/reduce job - results will be processed in handle_info
process(#rpbmapredreq{request=MrReq, content_type=ContentType}=Req,
        State) ->
    case decode_mapred_query(MrReq, ContentType) of
        {error, Reason} ->
            {error, {format, Reason}, State};
        {ok, Inputs, Query, Timeout} ->
            case riak_kv_util:mapred_system() of
                pipe ->
                    pipe_mapreduce(Req, State, Inputs, Query, Timeout);
                legacy ->
                    legacy_mapreduce(Req, State, Inputs, Query, Timeout)
            end
    end.

process_stream(#kv_mrc_sink{ref=ReqId,
                             results=Results,
                             logs=Logs,
                             done=Done},
               ReqId,
               State=#state{req=#rpbmapredreq{},
                            req_ctx=#pipe_ctx{ref=ReqId}=PipeCtx}) ->
    case riak_kv_mrc_pipe:error_exists(Logs) of
        false ->
            %% PB can only return responses for one phase at a time,
            %% so after we have sorted by phase, we'll send each one
            %% (this could be short-circuited here, but to avoid code
            %% duplication, we'll handle the start of the iteration
            %% the same as the last)
            self() ! #sink_part{ref=ReqId, resps=Results, done=Done},
            {ignore, State};
        {true, From, Info} ->
            destroy_pipe(PipeCtx),
            JsonInfo = {struct, riak_kv_mapred_json:jsonify_pipe_error(
                                  From, Info)},
            {error,
             mochijson2:encode(JsonInfo),
             State#state{req = undefined, req_ctx = undefined}}
    end;
process_stream(#sink_part{ref=ReqId, resps=[], done=true}, ReqId,
               State=#state{req=#rpbmapredreq{},
                            req_ctx=#pipe_ctx{ref=ReqId}=PipeCtx}) ->
    %% no more results to send, and the pipe is finished
    cleanup_pipe(PipeCtx),
    {done, #rpbmapredresp{done=1}, clear_state_req(State)};
process_stream(#sink_part{ref=ReqId, resps=[], done=false}, ReqId,
               State=#state{req=#rpbmapredreq{},
                            req_ctx=#pipe_ctx{ref=ReqId, sink={Sink,_}}}) ->
    %% no more results to send, but the pipe is not finished
    riak_kv_mrc_sink:next(Sink),
    {ignore, State};
process_stream(#sink_part{ref=ReqId,
                          resps=[{PhaseId, Response}|Rest]}=MR, ReqId,
               State=#state{req=#rpbmapredreq{content_type = ContentType},
                            req_ctx=#pipe_ctx{ref=ReqId}=PipeCtx}) ->
    #pipe_ctx{has_mr_query=HasMRQuery} = PipeCtx,
    %% more results to send
    case encode_mapred_phase(Response, ContentType, HasMRQuery) of
        {error, Reason} ->
            destroy_pipe(PipeCtx),
            {error, Reason, clear_state_req(State)};
        EncResponse ->
            self() ! MR#sink_part{resps=Rest},
            {reply, #rpbmapredresp{phase=PhaseId, response=EncResponse}, State}
    end;

process_stream({'DOWN', Ref, process, Pid, Reason}, Ref,
               State=#state{req=#rpbmapredreq{},
                            req_ctx=#pipe_ctx{sender={Pid, Ref}}=PipeCtx}) ->
    %% the async input sender exited
    if Reason == normal ->
            %% just reached the end of the input sending - all is
            %% well, continue processing
            NewPipeCtx = PipeCtx#pipe_ctx{sender=undefined},
            {ignore, State#state{req_ctx=NewPipeCtx}};
       true ->
            %% something went wrong sending inputs - tell the client
            %% about it, and shutdown the pipe
            destroy_pipe(PipeCtx),
            lager:error("Error sending inputs: ~p", [Reason]),
            {error, {format, "Error sending inputs: ~p", [Reason]},
             clear_state_req(State)}
    end;
process_stream({'DOWN', Mon, process, Pid, Reason}, _,
               State=#state{req=#rpbmapredreq{},
                            req_ctx=#pipe_ctx{sink={Pid, Mon}}=PipeCtx}) ->
    if Reason == normal ->
            %% the sink exited normally, which means it sent us a
            %% message with multiple phases in it, and we're chugging
            %% through them now, but our messages to ourself are
            %% behind this DOWN message in the mailbox
            {ignore, State};
       true ->
            %% the sink died, which it shouldn't be able to do before
            %% delivering our final results
            destroy_pipe(PipeCtx),
            lager:error("Error receiving outputs: ~p", [Reason]),
            {error,
             {format, "Error receiving outputs: ~p", [Reason]},
             clear_state_req(State)}
    end;
process_stream({pipe_timeout, Ref}, Ref,
               State=#state{req=#rpbmapredreq{},
                            req_ctx=#pipe_ctx{ref=Ref}=PipeCtx}) ->
    destroy_pipe(PipeCtx),
    {error, "timeout", clear_state_req(State)};

%% LEGACY Handle response from mapred_stream/mapred_bucket_stream
process_stream({flow_results, ReqId, done}, ReqId,
            State=#state{req=#rpbmapredreq{}, req_ctx=ReqId}) ->
    {done, #rpbmapredresp{done = 1}, State#state{req = undefined, req_ctx = undefined}};

process_stream({flow_results, ReqId, {error, Reason}}, ReqId,
            State=#state{req=#rpbmapredreq{}, req_ctx=ReqId}) ->
    {error, {format, Reason}, clear_state_req(State)};

process_stream({flow_results, PhaseId, ReqId, Res}, ReqId,
            State=#state{req=#rpbmapredreq{content_type = ContentType},
                         req_ctx=ReqId}) ->
    case encode_mapred_phase(Res, ContentType, true) of
        {error, Reason} ->
            {error, {format, Reason},
             State#state{req = undefined, req_ctx = undefined}};
        Response ->
            {reply, #rpbmapredresp{phase=PhaseId,response=Response}, State}
    end;

process_stream({flow_error, ReqId, Error}, ReqId,
            State=#state{req=#rpbmapredreq{}, req_ctx=ReqId}) ->
    {error, {format, Error}, State#state{req = undefined, req_ctx = undefined}};

process_stream(_,_,State) -> % Ignore any late replies from gen_servers/messages from fsms
    {ignore, State}.


%% ===================================================================
%% Internal functions
%% ===================================================================

clear_state_req(State) ->
     State#state{req=undefined, req_ctx=undefined}.

cleanup_pipe(#pipe_ctx{sink={Sink,SinkMon},timer=Timer,ref=Ref}) ->
    erlang:demonitor(SinkMon, [flush]),
    %% killing the sink should tear down the pipe
    riak_kv_mrc_sink:stop(Sink),
    %% receive just in case the sink had sent us one last response
    receive #kv_mrc_sink{} -> ok after 0 -> ok end,
    %% ... or in case we had an outstanding message to ourself
    receive #sink_part{} -> ok after 0 -> ok end,
    cleanup_timer(Timer, Ref).

cleanup_timer(Timer, PipeRef) ->
    case erlang:cancel_timer(Timer) of
        false ->
            receive
                {pipe_timeout, PipeRef} ->
                    ok
            after 0 ->
                    ok
            end;
        _ ->
            ok
    end.

destroy_pipe(#pipe_ctx{pipe=Pipe}=PipeCtx) ->
    cleanup_pipe(PipeCtx),
    %% destroying the pipe will automatically kill the sender
    riak_pipe:destroy(Pipe).

pipe_mapreduce(Req, State, Inputs, Query, Timeout) ->
    {ok, Sink} = riak_kv_mrc_sink:start(self()),
    try riak_kv_mrc_pipe:mapred_stream(Query, Sink) of
        {{ok, Pipe}, _NumKeeps} ->
            SinkMon = erlang:monitor(process, Sink),
            %% catch just in case the pipe or sink has already died
            %% for any reason - we'll get the DOWN later
            catch riak_kv_mrc_sink:use_pipe(Sink, Pipe),
            PipeRef = (Pipe#pipe.sink)#fitting.ref,
            Timer = erlang:send_after(Timeout, self(),
                                      {pipe_timeout, PipeRef}),
            {InputSender, SenderMonitor} =
                riak_kv_mrc_pipe:send_inputs_async(Pipe, Inputs),
            riak_kv_mrc_sink:next(Sink),
            Ctx = #pipe_ctx{pipe=Pipe,
                            sink={Sink, SinkMon},
                            ref=PipeRef,
                            timer=Timer,
                            sender={InputSender, SenderMonitor},
                            has_mr_query = (Query /= [])},
            {reply, {stream, PipeRef}, State#state{req=Req, req_ctx=Ctx}}
    catch throw:{badarg, Fitting, Reason} ->
            riak_kv_mrc_sink:stop(Sink),
            {error, {format, "Phase ~p: ~s", [Fitting, Reason]}, State}
    end.

legacy_mapreduce(#rpbmapredreq{content_type=ContentType}=Req,
                 #state{client=C}=State, Inputs, Query, Timeout) ->
    ResultTransformer = get_result_transformer(ContentType),
    case is_binary(Inputs) orelse is_key_filter(Inputs) of
        true ->
            case C:mapred_bucket_stream(Inputs, Query,
                                        self(), ResultTransformer, Timeout) of
                {stop, Error} ->
                    {error, {format, Error}, State};

                {ok, ReqId} ->
                    {reply, {stream,ReqId}, State#state{req = Req, req_ctx = ReqId}}
            end;
        false ->
            case is_list(Inputs) of
                true ->
                    case C:mapred_stream(Query, self(), ResultTransformer, Timeout) of
                        {stop, Error} ->
                            {error, {format, Error}, State};

                        {ok, {ReqId, FSM}} ->
                            luke_flow:add_inputs(FSM, Inputs),
                            luke_flow:finish_inputs(FSM),
                            {reply, {stream,ReqId}, State#state{req = Req, req_ctx = ReqId}}
                    end;
                false ->
                    case is_tuple(Inputs) andalso size(Inputs)==4 andalso
                        element(1, Inputs) == modfun andalso
                        is_atom(element(2, Inputs)) andalso
                        is_atom(element(3, Inputs)) of
                        true ->
                            case C:mapred_stream(Query, self(), ResultTransformer, Timeout) of
                                {stop, Error} ->
                                    {error, {format,Error}, State};

                                {ok, {ReqId, FSM}} ->
                                    C:mapred_dynamic_inputs_stream(
                                      FSM, Inputs, Timeout),
                                    luke_flow:finish_inputs(FSM),
                                    {reply, {stream, ReqId}, State#state{req = Req, req_ctx = ReqId}}
                            end;
                        false ->
                            {error, {format, bad_mapred_inputs}, State}
                    end
            end
    end.

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

%% get a result transformer for the content-type
%% jsonify not_founds for application/json
%% do nothing otherwise
get_result_transformer(<<"application/json">>) ->
    fun riak_kv_mapred_json:jsonify_not_found/1;
get_result_transformer(_) ->
    undefined.
