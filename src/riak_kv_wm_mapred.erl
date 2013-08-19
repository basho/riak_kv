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

-export([init/1,allowed_methods/2, known_content_type/2, forbidden/2]).
-export([malformed_request/2, process_post/2, content_types_provided/2]).
-export([nop/2]).

-include_lib("webmachine/include/webmachine.hrl").
-include_lib("riak_pipe/include/riak_pipe.hrl").
-include("riak_kv_mrc_sink.hrl").

-define(MAPRED_CTYPE, "application/json").
-define(DEFAULT_TIMEOUT, 60000).


-record(state, {inputs, timeout, mrquery, boundary}).
-type state() :: #state{}.

init(_) ->
    {ok, #state{}}.

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
    pipe_mapred(RD, State).

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

%% PIPE MAPRED

pipe_mapred(RD,
            #state{inputs=Inputs,
                   mrquery=Query,
                   timeout=Timeout}=State) ->
    case riak_kv_mrc_pipe:mapred_stream_sink(Inputs, Query, Timeout) of
        {ok, Mrc} ->
            case wrq:get_qs_value("chunked", "false", RD) of
                "true" ->
                    pipe_mapred_chunked(RD, State, Mrc);
                _ ->
                    pipe_mapred_nonchunked(RD, State, Mrc)
            end;
        {error, {Fitting, Reason}} ->
            {{halt, 400}, 
             send_error({error, [{phase, Fitting},
                                 {error, iolist_to_binary(Reason)}]}, RD),
             State}
    end.


pipe_mapred_nonchunked(RD, State, Mrc) ->
    case riak_kv_mrc_pipe:collect_sink(Mrc) of
        {ok, Results} ->
            JSONResults =
                case Mrc#mrc_ctx.keeps < 2 of
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
            riak_kv_mrc_pipe:cleanup_sink(Mrc),
            {true,
             wrq:set_resp_body(mochijson2:encode(JSONResults1), RD),
             State};
        {error, {sender_died, Error}} ->
            %% the sender links to the builder, so the builder has
            %% already been torn down
            riak_kv_mrc_pipe:cleanup_sink(Mrc),
            {{halt, 500}, send_error(Error, RD), State};
        {error, {sink_died, Error}} ->
            %% pipe monitors the sink, so the sink death has already
            %% detroyed the pipe
            riak_kv_mrc_pipe:cleanup_sink(Mrc),
            {{halt, 500}, send_error(Error, RD), State};
        {error, timeout} ->
            riak_kv_mrc_pipe:destroy_sink(Mrc),
            {{halt, 500}, send_error({error, timeout}, RD), State};
        {error, {From, Info}} ->
            riak_kv_mrc_pipe:destroy_sink(Mrc),
            Json = riak_kv_mapred_json:jsonify_pipe_error(From, Info),
            {{halt, 500}, send_error({error, Json}, RD), State}
    end.

pipe_mapred_chunked(RD, State, Mrc) ->
    Boundary = riak_core_util:unique_id_62(),
    CTypeRD = wrq:set_resp_header(
                "Content-Type",
                "multipart/mixed;boundary="++Boundary,
                RD),
    BoundaryState = State#state{boundary=Boundary},
    Streamer = pipe_stream_mapred_results(
                 CTypeRD, BoundaryState, Mrc),
    {true,
     wrq:set_resp_body({stream, Streamer}, CTypeRD),
     BoundaryState}.

pipe_stream_mapred_results(RD,
                           #state{boundary=Boundary}=State, 
                           Mrc) ->
    case riak_kv_mrc_pipe:receive_sink(Mrc) of
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
                                   riak_kv_mrc_pipe:cleanup_sink(Mrc),
                                   {iolist_to_binary(
                                      ["\r\n--", Boundary, "--\r\n"]),
                                    done};
                               false ->
                                   {[],
                                    fun() -> pipe_stream_mapred_results(
                                               RD, State, Mrc)
                                    end}
                           end,
            {iolist_to_binary([BodyA,BodyB]), Next};
        {error, timeout, _} ->
            riak_kv_mrc_pipe:destroy_sink(Mrc),
            {format_error({error, timeout}), done};
        {error, {sender_died, Error}, _} ->
            %% sender links to the builder, so the builder death has
            %% already destroyed the pipe
            riak_kv_mrc_pipe:cleanup_sink(Mrc),
            {format_error(Error), done};
        {error, {sink_died, Error}, _} ->
            %% pipe monitors the sink, so the sink death has already
            %% detroyed the pipe
            riak_kv_mrc_pipe:cleanup_sink(Mrc),
            {format_error(Error), done};
        {error, {From, Info}, _} ->
            riak_kv_mrc_pipe:destroy_sink(Mrc),
            Json = riak_kv_mapred_json:jsonify_pipe_error(From, Info),
            {format_error({error, Json}), done}
    end.

result_part({PhaseId, Results}, HasMRQuery, Boundary) ->
    Data = riak_kv_mapred_json:jsonify_bkeys(
             [riak_kv_mapred_json:jsonify_not_found(R)
              || R <- Results ],
             HasMRQuery),
    JSON = {struct, [{phase, PhaseId},
                     {data, Data}]},
    ["\r\n--", Boundary, "\r\n",
     "Content-Type: application/json\r\n\r\n",
     mochijson2:encode(JSON)].
