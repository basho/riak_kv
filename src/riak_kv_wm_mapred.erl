%% -------------------------------------------------------------------
%%
%% riak_kv_wm_mapred: webmachine resource for mapreduce requests
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

%% @doc webmachine resource for mapreduce requests

-module(riak_kv_wm_mapred).

-export([init/1, service_available/2, allowed_methods/2, known_content_type/2, forbidden/2]).
-export([malformed_request/2, process_post/2, content_types_provided/2]).
-export([nop/2]).

-include_lib("webmachine/include/webmachine.hrl").
-include_lib("riak_pipe/include/riak_pipe.hrl").
-include("riak_kv_mrc_sink.hrl").

-define(MAPRED_CTYPE, "application/json").
-define(DEFAULT_TIMEOUT, 60000).


-record(state, {client, inputs, timeout, mrquery, boundary}).
-type state() :: #state{}.

init(_) ->
    {ok, undefined}.

service_available(RD, State) ->
    case riak:local_client() of
        {ok, Client} ->
            {true, RD, #state{client=Client}};
        Error ->
            lager:error("~s", Error),
            {false, RD, State}
    end.

forbidden(RD, State) ->
    {riak_kv_wm_utils:is_forbidden(RD), RD, State}.

allowed_methods(RD, State) ->
    {['GET','HEAD','POST'], RD, State}.

-spec known_content_type(wrq:reqdata(), state()) ->
    {boolean(), wrq:reqdata(), state()}.
known_content_type(RD, State) ->
    {ctype_ok(RD), RD, State}.

malformed_request(RD, State) ->
    check_body(RD, State).

content_types_provided(RD, State) ->
    {[{"application/json", nop}], RD, State}.

nop(RD, State) ->
    {usage(), RD, State}.

process_post(RD, State) ->
    case riak_kv_util:mapred_system() of
        pipe ->
            pipe_mapred(RD, State);
        legacy ->
            legacy_mapred(RD, State)
    end.

%% Internal functions
send_error(Error, RD)  ->
    wrq:set_resp_body(format_error(Error), RD).

format_error({error, Message}=Error) when is_atom(Message);
                                          is_binary(Message) ->
    mochijson2:encode({struct, [Error]});
format_error({error, {_,_}=Error}) ->
    mochijson2:encode({struct, [Error]});
format_error({error, Error}) when is_list(Error) ->
    mochijson2:encode({struct, Error});
format_error(_Error) ->
    mochijson2:encode({struct, [{error, map_reduce_error}]}).

-spec ctype_ok(wrq:reqdata()) -> boolean().
%% @doc Return true if the content type from
%% this request is appropriate.
ctype_ok(RD) ->
    valid_ctype(get_base_ctype(RD)).

-spec get_base_ctype(wrq:reqdata()) -> string().
%% @doc Return the "base" content-type, that
%% is, not including the subtype parameters
get_base_ctype(RD) ->
    base_type(wrq:get_req_header("content-type", RD)).

-spec base_type(string()) -> string().
%% @doc Return the base media type
base_type(CType) ->
    {BaseType, _SubTypeParameters} = mochiweb_util:parse_header(CType),
    BaseType.

-spec valid_ctype(string()) -> boolean().
%% @doc Return true if the base content type
%% is equivalent to ?MAPRED_CTYPE
valid_ctype(?MAPRED_CTYPE) -> true;
valid_ctype(_Ctype) -> false.

check_body(RD, State) ->
    {Verified, Message, NewState} =
        case {wrq:method(RD), wrq:req_body(RD)} of
            {'POST', Body} when Body /= undefined ->
                verify_body(Body, State);
            _ ->
                {false, usage(), State}
        end,
    {not Verified,
     if Verified -> RD;
        true ->
             wrq:set_resp_header(
               "Content-Type", "text/plain",
               wrq:set_resp_body(Message, RD))
     end,
     NewState}.

verify_body(Body, State) ->
    case riak_kv_mapred_json:parse_request(Body) of
        {ok, ParsedInputs, ParsedQuery, Timeout} ->
            {true, [], State#state{inputs=ParsedInputs,
                                   mrquery=ParsedQuery,
                                   timeout=Timeout}};
        {error, {'query', Reason}} ->
            Message = lists:flatten(io_lib:format("~p", [Reason])),
            {false, ["An error occurred parsing the \"query\" field.\n",
                     Message], State};
        {error, {inputs, Reason}} ->
            Message = lists:flatten(io_lib:format("~p", [Reason])),
            {false, ["An error occurred parsing the \"inputs\" field.\n",
                     Message], State};
        {error, missing_field} ->
            {false, "The post body was missing the "
             "\"inputs\" or \"query\" field.\n", State};
        {error, {invalid_json, Message}} ->
            {false,
             io_lib:format("The POST body was not valid JSON.~n"
                           "The error from the parser was:~n~p~n",
                           [Message]),
             State};
        {error, not_json} ->
            {false, "The POST body was not a JSON object.\n", State}
    end.

usage() ->
    "This resource accepts POSTs with bodies containing JSON of the form:\n"
        "{\n"
        " \"inputs\":[...list of inputs...],\n"
        " \"query\":[...list of map/reduce phases...]\n"
        "}\n".

is_key_filter({Bucket, Filters}) when is_binary(Bucket),
                                      is_list(Filters) ->
    true;
is_key_filter(_) ->
    false.

%% PIPE MAPRED

pipe_mapred(RD,
            #state{inputs=Inputs,
                   mrquery=Query,
                   timeout=Timeout}=State) ->
    case riak_kv_mrc_pipe:mapred_stream(Query) of
        {ok, Pipe, Sink, NumKeeps} ->
            SinkMon = erlang:monitor(process, Sink),
            PipeRef = (Pipe#pipe.sink)#fitting.ref,
            Tref = erlang:send_after(Timeout, self(), {pipe_timeout, PipeRef}),
            {InputSender, SenderMonitor} =
                riak_kv_mrc_pipe:send_inputs_async(Pipe, Inputs),
            case wrq:get_qs_value("chunked", "false", RD) of
                "true" ->
                    pipe_mapred_chunked(RD, State, Pipe,
                                        {InputSender, SenderMonitor},
                                        {Tref, PipeRef},
                                        {Sink, SinkMon});
                _ ->
                    pipe_mapred_nonchunked(RD, State, Pipe, NumKeeps,
                                           {InputSender, SenderMonitor},
                                           {Tref, PipeRef},
                                           {Sink, SinkMon})
            end;
        {error, {Fitting, Reason}} ->
            {{halt, 400}, 
             send_error({error, [{phase, Fitting},
                                 {error, iolist_to_binary(Reason)}]}, RD),
             State}
    end.

%% Destroying the pipe via riak_pipe_builder:destroy/1 does not kill
%% the sender immediately, because it causes the builder to exit with
%% reason `normal', so no exit signal is sent. The sender will
%% eventually receive `worker_startup_error's from vnodes that can no
%% longer find the fittings, but to help the process along, we kill
%% them immediately here.
cleanup_sender({SenderPid, SenderRef}) ->
    erlang:demonitor(SenderRef, [flush]),
    exit(SenderPid, kill).

cleanup_pipe({Sink,SinkMon}, Timer) ->
    erlang:demonitor(SinkMon, [flush]),
    %% killing the sink should tear down the pipe
    riak_kv_mrc_sink:stop(Sink),
    %% receive just in case the sink had sent us one last response
    receive #kv_mrc_sink{} -> ok after 0 -> ok end,
    cleanup_timer(Timer).

cleanup_timer({Tref, PipeRef}) ->
    case erlang:cancel_timer(Tref) of
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

pipe_mapred_nonchunked(RD, State, Pipe, NumKeeps, Sender, PipeTref, Sink) ->
    case pipe_collect_outputs(Pipe, NumKeeps, Sender, Sink) of
        {ok, Results} ->
            JSONResults =
                case NumKeeps < 2 of
                    true ->
                        [riak_kv_mapred_json:jsonify_not_found(R)
                         || R <- Results];
                    false ->
                        [[riak_kv_mapred_json:jsonify_not_found(PR)
                          || PR <- PhaseResults]
                         || PhaseResults <- Results]
                end,
            HasMRQuery = State#state.mrquery /= [],
            JSONResults1 = riak_kv_mapred_json:jsonify_bkeys(JSONResults, HasMRQuery),
            cleanup_pipe(Sink, PipeTref),
            {true,
             wrq:set_resp_body(mochijson2:encode(JSONResults1), RD),
             State};
        {error, {sender_error, Error}} ->
            %% the sender links to the builder, so the builder has
            %% already been torn down
            cleanup_pipe(Sink, PipeTref),
            {{halt, 500}, send_error(Error, RD), State};
        {error, timeout} ->
            cleanup_pipe(Sink, PipeTref),
            riak_pipe:destroy(Pipe),
            cleanup_sender(Sender),
            {{halt, 500}, send_error({error, timeout}, RD), State};
        {error, Error} ->
            cleanup_pipe(Sink, PipeTref),
            riak_pipe:destroy(Pipe),
            cleanup_sender(Sender),
            {{halt, 500}, send_error({error, Error}, RD), State}
    end.

pipe_collect_outputs(Pipe, NumKeeps, Sender, Sink) ->
    Ref = (Pipe#pipe.sink)#fitting.ref,
    case pipe_collect_outputs1(Ref, Sender, Sink, []) of
        {ok, [{_,Outputs}]} when NumKeeps < 2 ->
            {ok, Outputs};
        {ok, Outputs} ->
            {ok, [ O || {_, O} <- Outputs]};
        Error ->
            Error
    end.

pipe_collect_outputs1(Ref, Sender, Sink, Acc) ->
    case pipe_receive_output(Ref, Sender, Sink) of
        {ok, false, Output} ->
            pipe_collect_outputs1(Ref, Sender, Sink, [Output|Acc]);
        {ok, true, Output} ->
            {ok, riak_kv_mrc_sink:merge_outputs([Output|Acc])};
        Error        -> Error
    end.

pipe_receive_output(Ref, {SenderPid, SenderRef}, {SinkPid, SinkMon}) ->
    riak_kv_mrc_sink:next(SinkPid),
    receive
        #kv_mrc_sink{ref=Ref, results=Results, logs=Logs, done=Done} ->
            case riak_kv_mrc_pipe:error_exists(Logs) of
                {true, From, Info} ->
                    {error, riak_kv_mapred_json:jsonify_pipe_error(
                              From, Info)};
                false ->
                    {ok, Done, Results}
            end;
        {'DOWN', SenderRef, process, SenderPid, Reason} ->
            if Reason == normal ->
                    %% just done sending inputs, nothing to worry about
                    pipe_receive_output(
                      Ref, {SenderPid, SenderRef}, {SinkPid, SinkMon});
               true ->
                    {error, {sender_error, Reason}}
            end;
        {'DOWN', SinkMon, process, SinkPid, Reason} ->
            %% we should never receive the sink's DOWN before its
            %% final message to us
            {error, {sink_error, Reason}};
        {pipe_timeout, Ref} ->
            {error, timeout}
    end.

pipe_mapred_chunked(RD, State, Pipe, Sender, PipeTref, Sink) ->
    Boundary = riak_core_util:unique_id_62(),
    CTypeRD = wrq:set_resp_header(
                "Content-Type",
                "multipart/mixed;boundary="++Boundary,
                RD),
    BoundaryState = State#state{boundary=Boundary},
    Streamer = pipe_stream_mapred_results(
                 CTypeRD, Pipe, BoundaryState, Sender, PipeTref, Sink),
    {true,
     wrq:set_resp_body({stream, Streamer}, CTypeRD),
     BoundaryState}.

pipe_stream_mapred_results(RD, Pipe,
                           #state{boundary=Boundary}=State, 
                           Sender, PipeTref, Sink) ->
    case pipe_receive_output((Pipe#pipe.sink)#fitting.ref, Sender, Sink) of
        {ok, Done, Outputs} ->
            BodyA = case Outputs of
                        [] ->
                            [];
                        _ ->
                            HasMRQuery = State#state.mrquery /= [],
                            [ result_part(O, HasMRQuery, Boundary)
                              || O <- Outputs ]
                    end,
            {BodyB,Next} = case Done of
                               true ->
                                   cleanup_pipe(Sink, PipeTref),
                                   {iolist_to_binary(
                                      ["\r\n--", Boundary, "--\r\n"]),
                                    done};
                               false ->
                                   {[],
                                    fun() -> pipe_stream_mapred_results(
                                               RD, Pipe, State,
                                               Sender, PipeTref, Sink)
                                    end}
                           end,
            {iolist_to_binary([BodyA,BodyB]), Next};
        {error, timeout} ->
            cleanup_pipe(Sink, PipeTref),
            riak_pipe:destroy(Pipe),
            cleanup_sender(Sender),
            {format_error({error, timeout}), done};
        {error, {sender_error, Error}} ->
            cleanup_pipe(Sink, PipeTref),
            {format_error(Error), done};
        {error, {Error, _Input}} ->
            cleanup_pipe(Sink, PipeTref),
            riak_pipe:destroy(Pipe),
            cleanup_sender(Sender),
            {format_error({error, Error}), done}
    end.

result_part({PhaseId, Results}, HasMRQuery, Boundary) ->
    Data = [ riak_kv_mapred_json:jsonify_bkeys(
               riak_kv_mapred_json:jsonify_not_found(R),
               HasMRQuery)
             || R <- Results ],
    JSON = {struct, [{phase, PhaseId},
                     {data, Data}]},
    ["\r\n--", Boundary, "\r\n",
     "Content-Type: application/json\r\n\r\n",
     mochijson2:encode(JSON)].

%% LEGACY MAPRED

legacy_mapred(RD,
              #state{inputs=Inputs,
                     mrquery=Query,
                     timeout=Timeout}=State) ->
    Me = self(),
    {ok, Client} = riak:local_client(),
    ResultTransformer = fun riak_kv_mapred_json:jsonify_not_found/1,
    case wrq:get_qs_value("chunked", RD) of
        "true" ->
            {ok, ReqId} =
                case is_binary(Inputs) orelse is_key_filter(Inputs) of
                    true ->
                        Client:mapred_bucket_stream(Inputs, Query, Me, ResultTransformer, Timeout);
                    false ->
                        if is_list(Inputs) ->
                                {ok, {RId, FSM}} = Client:mapred_stream(Query, Me, ResultTransformer, Timeout),
                                luke_flow:add_inputs(FSM, Inputs),
                                luke_flow:finish_inputs(FSM),
                                {ok, RId};
                           is_tuple(Inputs) ->
                                {ok, {RId, FSM}} = Client:mapred_stream(Query, Me, ResultTransformer, Timeout),
                                Client:mapred_dynamic_inputs_stream(FSM, Inputs, Timeout),
                                luke_flow:finish_inputs(FSM),
                                {ok, RId}
                        end
                end,
            Boundary = riak_core_util:unique_id_62(),
            RD1 = wrq:set_resp_header("Content-Type", "multipart/mixed;boundary=" ++ Boundary, RD),
            State1 = State#state{boundary=Boundary},
            {true, wrq:set_resp_body({stream, legacy_stream_mapred_results(RD1, ReqId, State1)}, RD1), State1};
        Param when Param =:= "false";
                   Param =:= undefined ->
            Results = case is_binary(Inputs) orelse is_key_filter(Inputs) of
                          true ->
                              Client:mapred_bucket(Inputs, Query, ResultTransformer, Timeout);
                          false ->
                              if is_list(Inputs) ->
                                      Client:mapred(Inputs, Query, ResultTransformer, Timeout);
                                 is_tuple(Inputs) ->
                                      case Client:mapred_stream(Query,Me,ResultTransformer,Timeout) of
                                          {ok, {ReqId, FlowPid}} ->
                                              Client:mapred_dynamic_inputs_stream(FlowPid, Inputs, Timeout),
                                              luke_flow:finish_inputs(FlowPid),
                                              luke_flow:collect_output(ReqId, Timeout);
                                          Error ->
                                              Error
                                      end
                              end
                      end,
            RD1 = wrq:set_resp_header("Content-Type", "application/json", RD),
            case Results of
                "all nodes failed" ->
                    {{halt, 500}, wrq:set_resp_body("All nodes failed", RD), State};
                {error, _} ->
                    {{halt, 500}, send_error(Results, RD1), State};
                {ok, Result} ->
                    {true, wrq:set_resp_body(mochijson2:encode(Result), RD1), State}
            end
    end.

legacy_stream_mapred_results(RD, ReqId, #state{timeout=Timeout}=State) ->
    FinalTimeout = erlang:trunc(Timeout * 1.02),
    receive
        {flow_results, ReqId, done} -> {iolist_to_binary(["\r\n--", State#state.boundary, "--\r\n"]), done};
        {flow_results, ReqId, {error, Error}} ->
            {format_error(Error), done};
        {flow_error, ReqId, Error} ->
            {format_error({error, Error}), done};
        {flow_results, PhaseId, ReqId, Res} ->
            Data = mochijson2:encode({struct, [{phase, PhaseId}, {data, Res}]}),
            Body = ["\r\n--", State#state.boundary, "\r\n",
                    "Content-Type: application/json\r\n\r\n",
                    Data],
            {iolist_to_binary(Body), fun() -> legacy_stream_mapred_results(RD, ReqId, State) end}
    after FinalTimeout ->
            {format_error({error, timeout}), done}
    end.
