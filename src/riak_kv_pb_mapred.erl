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
                req_ctx}).

-record(pipe_ctx, {ref,      % easier-access ref/reqid
                   mrc,      % #mrc_ctx{}
                   sender,   % {pid(), monitor()} of process sending inputs
                   sink,     % {pid(), monitor()} of process collecting outputs
                   has_mr_query}). % true if the request contains a query.

init() ->
    #state{}.

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
            pipe_mapreduce(Req, State, Inputs, Query, Timeout)
    end.

process_stream(#kv_mrc_sink{ref=ReqId,
                             results=Results,
                             logs=Logs,
                             done=Done},
               ReqId,
               State=#state{req=#rpbmapredreq{},
                            req_ctx=#pipe_ctx{ref=ReqId,
                                              mrc=Mrc}=PipeCtx}) ->
    case riak_kv_mrc_pipe:error_exists(Logs) of
        false ->
            case msgs_for_results(Results, State) of
                {ok, Msgs} ->
                    if Done ->
                            cleanup_pipe(PipeCtx),
                            %% we could set the done=1 flag on the
                            %% final results message, but that has
                            %% never been done, so there are probably
                            %% client libs that aren't expecting it;
                            %% play it safe for now
                            {done,
                             Msgs++[#rpbmapredresp{done=1}],
                             clear_state_req(State)};
                       true ->
                            {Sink, _} = Mrc#mrc_ctx.sink,
                            riak_kv_mrc_sink:next(Sink),
                            {reply, Msgs, State}
                    end;
                {error, Reason} ->
                    destroy_pipe(PipeCtx),
                    {error, Reason, clear_state_req(State)}
            end;
        {true, From, Info} ->
            destroy_pipe(PipeCtx),
            JsonInfo = {struct, riak_kv_mapred_json:jsonify_pipe_error(
                                  From, Info)},
            {error,
             mochijson2:encode(JsonInfo),
             clear_state_req(State)}
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
    %% the sink died, which it shouldn't be able to do before
    %% delivering our final results
    destroy_pipe(PipeCtx),
    lager:error("Error receiving outputs: ~p", [Reason]),
    {error,
     {format, "Error receiving outputs: ~p", [Reason]},
     clear_state_req(State)};
process_stream({pipe_timeout, Ref}, Ref,
               State=#state{req=#rpbmapredreq{},
                            req_ctx=#pipe_ctx{ref=Ref}=PipeCtx}) ->
    destroy_pipe(PipeCtx),
    {error, "timeout", clear_state_req(State)};

process_stream(_,_,State) -> % Ignore any late replies from gen_servers/messages from fsms
    {ignore, State}.


%% ===================================================================
%% Internal functions
%% ===================================================================

clear_state_req(State) ->
     State#state{req=undefined, req_ctx=undefined}.

destroy_pipe(#pipe_ctx{mrc=Mrc}) ->
    riak_kv_mrc_pipe:destroy_sink(Mrc).

cleanup_pipe(#pipe_ctx{mrc=Mrc}) ->
    riak_kv_mrc_pipe:cleanup_sink(Mrc).

pipe_mapreduce(Req, State, Inputs, Query, Timeout) ->
    case riak_kv_mrc_pipe:mapred_stream_sink(Inputs, Query, Timeout) of
        {ok, #mrc_ctx{ref=PipeRef,
                      sink={Sink,SinkMon},
                      sender={Sender,SenderMon}}=Mrc} ->
            riak_kv_mrc_sink:next(Sink),
            %% pulling ref, sink, and sender out to make matches less
            %% nested in process callbacks
            Ctx = #pipe_ctx{ref=PipeRef,
                            mrc=Mrc,
                            sink={Sink,SinkMon},
                            sender={Sender,SenderMon},
                            has_mr_query = (Query /= [])},
            {reply, {stream, PipeRef}, State#state{req=Req, req_ctx=Ctx}};
        {error, {Fitting, Reason}} ->
            {error, {format, "Phase ~p: ~s", [Fitting, Reason]}, State}
    end.

%% Decode a mapred query
%% {ok, ParsedInputs, ParsedQuery, Timeout};
decode_mapred_query(Query, <<"application/json">>) ->
    riak_kv_mapred_json:parse_request(Query);
decode_mapred_query(Query, <<"application/x-erlang-binary">>) ->
    riak_kv_mapred_term:parse_request(Query);
decode_mapred_query(_Query, ContentType) ->
    {error, {unknown_content_type, ContentType}}.

%% PB can only return responses for one phase at a time,
%% so we have to build a message for each
msgs_for_results(Results, #state{req=Req, req_ctx=PipeCtx}) ->
    msgs_for_results(Results,
                   Req#rpbmapredreq.content_type,
                   PipeCtx#pipe_ctx.has_mr_query,
                   []).

msgs_for_results([{PhaseId, Results}|Rest], CType, HasMRQuery, Acc) ->
    case encode_mapred_phase(Results, CType, HasMRQuery) of
        {error, _}=Error ->
            Error;
        Encoded ->
            Msg=#rpbmapredresp{phase=PhaseId, response=Encoded},
            msgs_for_results(Rest, CType, HasMRQuery, [Msg|Acc])
    end;
msgs_for_results([], _, _, Acc) ->
    {ok, lists:reverse(Acc)}.

%% Convert a map/reduce phase to the encoding requested
encode_mapred_phase(Res, <<"application/json">>, HasMRQuery) ->
    Res1 = riak_kv_mapred_json:jsonify_bkeys(Res, HasMRQuery),
    mochijson2:encode(Res1);
encode_mapred_phase(Res, <<"application/x-erlang-binary">>, _) ->
    term_to_binary(Res);
encode_mapred_phase(_Res, ContentType, _) ->
    {error, {unknown_content_type, ContentType}}.
