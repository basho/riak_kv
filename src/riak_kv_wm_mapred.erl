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

-export([init/1, service_available/2, allowed_methods/2]).
-export([malformed_request/2, process_post/2, content_types_provided/2]).
-export([nop/2]).

-include_lib("webmachine/include/webmachine.hrl").

-define(DEFAULT_TIMEOUT, 60000).


-record(state, {client, inputs, timeout, mrquery, boundary}).

init(_) ->
    {ok, undefined}.

service_available(RD, State) ->
    case riak:local_client() of
        {ok, Client} ->
            {true, RD, #state{client=Client}};
        Error ->
            error_logger:error_report(Error),
            {false, RD, State}
    end.

allowed_methods(RD, State) ->
    {['GET','HEAD','POST'], RD, State}.

malformed_request(RD, State) ->
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
format_error({error, Error}) when is_list(Error) ->
    mochijson2:encode({struct, Error});
format_error(_Error) ->
    mochijson2:encode({struct, [{error, map_reduce_error}]}).

verify_body(Body, State) ->
    case riak_kv_mapred_json:parse_request(Body) of
        {ok, ParsedInputs, ParsedQuery, Timeout} ->
            {true, [], State#state{inputs=ParsedInputs,
                                   mrquery=ParsedQuery,
                                   timeout=Timeout}};
        {error, {'query', Message}} ->
            {false, ["An error occurred parsing the \"query\" field.\n",
                     Message], State};
        {error, {inputs, Message}} ->
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
                   mrquery=Query}=State) ->
    {{ok, Pipe}, NumKeeps} =
        riak_kv_mrc_pipe:mapred_stream(Query),
    case riak_kv_mrc_pipe:send_inputs(Pipe, Inputs) of
        ok ->
            case wrq:get_qs_value("chunked", "false", RD) of
                "true" ->
                    pipe_mapred_chunked(RD, State, Pipe);
                _ ->
                    pipe_mapred_nonchunked(RD, State, Pipe, NumKeeps)
            end;
        {error, {bad_filter, _}} ->
            riak_kv_pipe:eoi(Pipe),
            {{halt, 500}, send_error({error, bad_mapred_filter}, RD), State};
        Error ->
            riak_kv_pipe:eoi(Pipe),
            {{halt, 500}, send_error(Error, RD), State}
    end.

pipe_mapred_nonchunked(RD, State, Pipe, NumKeeps) ->
    %% TODO: timeout
    case riak_kv_mrc_pipe:collect_outputs(Pipe, NumKeeps) of
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
            {true,
             wrq:set_resp_body(mochijson2:encode(JSONResults), RD),
             State};
        {error, _}=Error ->
            {{halt, 500}, send_error(Error, RD), State}
    end.

pipe_mapred_chunked(RD, State, Pipe) ->
    Boundary = riak_core_util:unique_id_62(),
    CTypeRD = wrq:set_resp_header(
                "Content-Type",
                "multipart/mixed;boundary="++Boundary,
                RD),
    BoundaryState = State#state{boundary=Boundary},
    Streamer = pipe_stream_mapred_results(
                 CTypeRD, Pipe, BoundaryState),
    {true,
     wrq:set_resp_body({stream, Streamer}, CTypeRD),
     BoundaryState}.

pipe_stream_mapred_results(RD, Pipe,
                           #state{timeout=Timeout,
                                  boundary=Boundary}=State) ->
    case riak_pipe:receive_result(Pipe, Timeout) of
        {result, {PhaseId, Result}} ->
            %% results come out of pipe one
            %% at a time but they're supposed to
            %% be in a list at the client end
            JSONResult = [riak_kv_mapred_json:jsonify_not_found(Result)],
            Data = mochijson2:encode({struct, [{phase, PhaseId},
                                               {data, JSONResult}]}),
            Body = ["\r\n--", Boundary, "\r\n",
                    "Content-Type: application/json\r\n\r\n",
                    Data],
            {iolist_to_binary(Body),
             fun() -> pipe_stream_mapred_results(RD, Pipe, State) end};
        eoi ->
            {iolist_to_binary(["\r\n--", Boundary, "--\r\n"]), done};
        timeout ->
            {format_error({error, timeout}), done};
        {log, {_,_}} ->
            %% no logging is enabled in riak_kv_mrc_pipe, but the
            %% match is here just so this doesn't blow up during
            %% debugging
            pipe_stream_mapred_results(RD, Pipe, State)
    end.

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

