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

-include_lib("riakc/include/riakclient_pb.hrl").
-include_lib("riakc/include/riakc_pb.hrl").
-include_lib("riak_pipe/include/riak_pipe.hrl").

-behaviour(riak_api_pb_service).

-export([init/0,
         decode/2,
         encode/1,
         process/2,
         process_stream/3]).

-record(?MODULE, {req,
                  req_ctx,
                  client}).
-define(state, #?MODULE).

-record(pipe_ctx, {pipe,     % pipe handling mapred request
                   ref,      % easier-access ref/reqid
                   timer,    % ref() for timeout send_after
                   sender,   % {pid(), monitor()} of process sending inputs
                   has_mr_query}). % true if the request contains a query.

init() ->
    {ok, C} = riak:local_client(),
    ?state{client=C}.

decode(Code, Bin) ->
    {ok, riakc_pb:decode(Code, Bin)}.

encode(Message) ->
    {ok, riakc_pb:encode(Message)}.

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


process_stream(#pipe_eoi{ref=ReqId}, ReqId,
               State=?state{req=#rpbmapredreq{},
                            req_ctx=#pipe_ctx{ref=ReqId,
                                              timer=Timer}}) ->
    erlang:cancel_timer(Timer),
    {done, #rpbmapredresp{done = 1}, State?state{req = undefined, req_ctx = undefined}};

process_stream(#pipe_result{ref=ReqId, from=PhaseId, result=Res},
               ReqId,
               State=?state{req=#rpbmapredreq{content_type = ContentType},
                            req_ctx=#pipe_ctx{ref=ReqId, has_mr_query=HasMRQuery}=PipeCtx}) ->
    case encode_mapred_phase([Res], ContentType, HasMRQuery) of
        {error, Reason} ->
            erlang:cancel_timer(PipeCtx#pipe_ctx.timer),
            %% destroying the pipe will automatically kill the sender
            riak_pipe:destroy(PipeCtx#pipe_ctx.pipe),
            {error, Reason, State?state{req = undefined, req_ctx = undefined}};
        Response ->
            {reply, #rpbmapredresp{phase=PhaseId, response=Response}, State}
    end;

process_stream(#pipe_log{ref=ReqId, from=From, msg=Msg},
               ReqId,
               State=?state{req=#rpbmapredreq{},
                            req_ctx=#pipe_ctx{ref=ReqId}=PipeCtx}) ->
    case Msg of
        {trace, [error], {error, Info}} ->
            erlang:cancel_timer(PipeCtx#pipe_ctx.timer),
            %% destroying the pipe will automatically kill the sender
            riak_pipe:destroy(PipeCtx#pipe_ctx.pipe),
            JsonInfo = {struct, riak_kv_mapred_json:jsonify_pipe_error(
                                  From, Info)},
            {error, mochijson2:encode(JsonInfo), State?state{req = undefined, req_ctx = undefined}};
        _ ->
            {ignore, State}
    end;

process_stream({'DOWN', Ref, process, Pid, Reason}, Ref,
               State=?state{req=#rpbmapredreq{},
                            req_ctx=#pipe_ctx{sender={Pid, Ref}}=PipeCtx}) ->
    %% the async input sender exited
    if Reason == normal ->
            %% just reached the end of the input sending - all is
            %% well, continue processing
            NewPipeCtx = PipeCtx#pipe_ctx{sender=undefined},
            {ignore, State?state{req_ctx=NewPipeCtx}};
       true ->
            %% something went wrong sending inputs - tell the client
            %% about it, and shutdown the pipe
            erlang:cancel_timer(PipeCtx#pipe_ctx.timer),
            riak_pipe:destroy(PipeCtx#pipe_ctx.pipe),
            lager:error("Error sending inputs: ~p", [Reason]),
            {error, {format, "Error sending inputs: ~p", [Reason]},
             State?state{req=undefined, req_ctx=undefined}}
    end;

process_stream({pipe_timeout, Ref}, Ref,
               State=?state{req=#rpbmapredreq{},
                            req_ctx=#pipe_ctx{ref=Ref,pipe=Pipe}}) ->
    %% destroying the pipe will automatically kill the sender
    riak_pipe:destroy(Pipe),
    {error, "timeout", State?state{req=undefined, req_ctx=undefined}};

%% LEGACY Handle response from mapred_stream/mapred_bucket_stream
process_stream({flow_results, ReqId, done}, ReqId,
            State=?state{req=#rpbmapredreq{}, req_ctx=ReqId}) ->
    {done, #rpbmapredresp{done = 1}, State?state{req = undefined, req_ctx = undefined}};

process_stream({flow_results, ReqId, {error, Reason}}, ReqId,
            State=?state{req=#rpbmapredreq{}, req_ctx=ReqId}) ->
    {error, {format, Reason}, State?state{req=undefined, req_ctx=undefined}};

process_stream({flow_results, PhaseId, ReqId, Res}, ReqId,
            State=?state{req=#rpbmapredreq{content_type = ContentType},
                         req_ctx=ReqId}) ->
    case encode_mapred_phase(Res, ContentType, true) of
        {error, Reason} ->
            {error, {format, Reason},
             State?state{req = undefined, req_ctx = undefined}};
        Response ->
            {reply, #rpbmapredresp{phase=PhaseId,response=Response}, State}
    end;

process_stream({flow_error, ReqId, Error}, ReqId,
            State=?state{req=#rpbmapredreq{}, req_ctx=ReqId}) ->
    {error, {format, Error}, State?state{req = undefined, req_ctx = undefined}};

process_stream(_,_,State) -> % Ignore any late replies from gen_servers/messages from fsms
    {ignore, State}.


%% ===================================================================
%% Internal functions
%% ===================================================================

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
            {reply, {stream, PipeRef}, State?state{req=Req, req_ctx=Ctx}}
    catch throw:{badarg, Fitting, Reason} ->
            {error, {format, "Phase ~p: ~s", [Fitting, Reason]}, State}
    end.

legacy_mapreduce(#rpbmapredreq{content_type=ContentType}=Req,
                 ?state{client=C}=State, Inputs, Query, Timeout) ->
    ResultTransformer = get_result_transformer(ContentType),
    case is_binary(Inputs) orelse is_key_filter(Inputs) of
        true ->
            case C:mapred_bucket_stream(Inputs, Query,
                                        self(), ResultTransformer, Timeout) of
                {stop, Error} ->
                    {error, {format, Error}, State};

                {ok, ReqId} ->
                    {reply, {stream,ReqId}, State?state{req = Req, req_ctx = ReqId}}
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
                            {reply, {stream,ReqId}, State?state{req = Req, req_ctx = ReqId}}
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
                                    {reply, {stream, ReqId}, State?state{req = Req, req_ctx = ReqId}}
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
