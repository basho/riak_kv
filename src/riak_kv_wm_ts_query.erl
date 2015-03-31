%% -------------------------------------------------------------------
%%
%% Copyright (c) 2015 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc webmachine resource for streaming TS range queries.

-module(riak_kv_wm_ts_query).

-export([init/1,
         allowed_methods/2,
         known_content_type/2,
         is_authorized/2,
         forbidden/2]).
-export([malformed_request/2, process_post/2, content_types_provided/2]).
-export([nop/2]).

-include_lib("webmachine/include/webmachine.hrl").

-define(TS_QUERY_CTYPE, "text/plain").
-define(DEFAULT_TIMEOUT, 60000).


-record(state, {ts_query,
                msg_ref :: reference(),
                timeout = ?DEFAULT_TIMEOUT,
                boundary,
                security}).
-type state() :: #state{}.

-record(stream_state, {boundary,
                       timeout,
                       msg_ref,
                       mon_ref}).

init(_) ->
    {ok, #state{}}.

is_authorized(ReqData, State) ->
    case riak_api_web_security:is_authorized(ReqData) of
        false ->
            {"Basic realm=\"Riak\"", ReqData, State};
        {true, SecContext} ->
            {true, ReqData, State#state{security=SecContext}};
        insecure ->
            %% XXX 301 may be more appropriate here, but since the http and
            %% https port are different and configurable, it is hard to figure
            %% out the redirect URL to serve.
            {{halt, 426}, wrq:append_to_resp_body(<<"Security is enabled and "
                    "Riak does not accept credentials over HTTP. Try HTTPS "
                    "instead.">>, ReqData), State}
    end.

forbidden(RD, State = #state{security=undefined}) ->
    {riak_kv_wm_utils:is_forbidden(RD), RD, State};
forbidden(RD, State) ->
    case riak_kv_wm_utils:is_forbidden(RD) of
        true ->
            {true, RD, State};
        false ->
            {false, RD, State}
    end.

allowed_methods(RD, State) ->
    {['POST'], RD, State}.

-spec known_content_type(#wm_reqdata{}, state()) ->
    {boolean(), #wm_reqdata{}, state()}.
known_content_type(RD, State) ->
    {ctype_ok(RD), RD, State}.

malformed_request(RD, State) ->
    check_body(RD, State).

content_types_provided(RD, State) ->
    {[{"text/plain", nop}], RD, State}.

nop(RD, State) ->
    {usage(), RD, State}.

process_post(RD,
             #state{ts_query = TSQuery}=State) ->
    case do_ts_query(TSQuery) of
        {ok, CmdRef} ->
            StreamState = #stream_state{msg_ref=CmdRef},
            ts_results_chunked(RD, StreamState);
        {error, Reason} ->
            {{halt, 400},
             send_error({error, Reason}, RD),
             State}
    end.

do_ts_query({Family, Series, _, _} = TSQuery) ->
    Idx = chash:key_of({Family, Series}),
    N = riak_core_bucket:n_val(riak_core_bucket:get_bucket(Family)),
    Preflist = [{VnodeIdx, Node} ||
                {{VnodeIdx, Node}, primary}
                <- riak_core_apl:get_primary_apl(Idx, N, riak_kv)],
    lager:debug("Query TS ~p", [TSQuery]),
    ReplyRef = make_ref(),
    riak_core_vnode_master:command(Preflist,
                                   {ts_query, TSQuery},
                                   {raw, ReplyRef, self()},
                                   riak_kv_vnode_master),
    {ok, ReplyRef}.

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

-spec ctype_ok(#wm_reqdata{}) -> boolean().
%% @doc Return true if the content type from
%% this request is appropriate.
ctype_ok(RD) ->
    valid_ctype(get_base_ctype(RD)).

-spec get_base_ctype(#wm_reqdata{}) -> string().
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
%% is equivalent to ?TS_QUERY_CTYPE
valid_ctype(?TS_QUERY_CTYPE) -> true;
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
    case parse_ts_query(Body) of
        {ok, TSQuery} ->
            {true, [], State#state{ts_query = TSQuery}};
        _ ->
            {false,
             "The POST body was not a valid time series query.\n",
             State}
    end.

parse_line(Bin) ->
    parse_line(<<>>, Bin).

parse_line(In, <<"\r\n", More/binary>>) ->
    {In, More};
parse_line(In, <<"\n", More/binary>>) ->
    {In, More};
parse_line(In, <<"\r", More/binary>>) ->
    {In, More};
parse_line(In, <<C:8, More/binary>>) ->
    parse_line(<<In/binary, C:8>>, More).

parse_ts_query(B0) ->
    {Family, B1} = parse_line(B0),
    {Series, B2} = parse_line(B1),
    {StrTime1, B3} = parse_line(B2),
    {StrTime2, _} = parse_line(B3),
    Time1 = binary_to_integer(StrTime1),
    Time2 = binary_to_integer(StrTime2),
    {ok, {Family, Series, Time1, Time2}}.

usage() ->
    "This resource accepts POSTs with bodies containing JSON of the form:\n"
        "{\n"
        " \"inputs\":[...list of inputs...],\n"
        " \"query\":[...list of map/reduce phases...]\n"
        "}\n".


ts_results_chunked(RD, State=#stream_state{}) ->
    Boundary = riak_core_util:unique_id_62(),
    CTypeRD = wrq:set_resp_header(
                "Content-Type",
                "multipart/mixed;boundary="++Boundary,
                RD),
    State2 = State#stream_state{boundary=Boundary, timeout=?DEFAULT_TIMEOUT},
    Streamer = stream_ts_results(State2),
    {true,
     wrq:set_resp_body({stream, Streamer}, CTypeRD),
     State2}.

stream_ts_results(State = #stream_state{msg_ref = Ref,
                                        mon_ref = undefined,
                                        boundary = Boundary,
                                        timeout = Timeout}) ->
    receive
        {Ref, {ok, Pid}} ->
            NewMonRef = erlang:monitor(process, Pid),
            stream_ts_results(State#stream_state{mon_ref=NewMonRef})
    after
       Timeout ->
            lager:error("Time out for Pid!!"),
            drain_batches(Ref),
            {["\r\n--", Boundary, "\r\n"
              "Content-Type: text/plain\r\n\r\n"
              "ERROR\r\n"
              "Initial request time out time out"
              "\r\n--", Boundary, "--\r\n"],
             done}
    end;
stream_ts_results(State = #stream_state{msg_ref = Ref,
                                        mon_ref = MonRef,
                                        boundary = Boundary,
                                        timeout = Timeout}) ->
    receive
        {Ref, ts_batch_end} ->
            lager:debug("TS batch end"),
            erlang:demonitor(MonRef, [flush]),
            {iolist_to_binary(
               ["\r\n--", Boundary, "--\r\n"]),
             done};
        {Ref, {ts_batch, TSBatch}} ->
            OutBatch = result_part(TSBatch, Boundary),
            lager:debug("TS batch:\n~p\n", [OutBatch]),
            {OutBatch,
             fun() -> stream_ts_results(State) end};
        {'DOWN', MonRef, _, _, Info} ->
            lager:warning("TS query producer died ~p", [Info]),
            {["\r\n--", Boundary, "\r\n",
              "Content-Type: text/plain\r\n\r\n",
              "TS query producer died", "\r\n--", Boundary, "--\r\n"],
             done}
    after
       Timeout ->
            lager:error("Time out!!"),
            {["\r\n--", Boundary, "\r\n",
              "Content-Type: text/plain\r\n\r\n",
              "ERROR\r\n"
              "Batch time out",
              "\r\n--", Boundary, "--\r\n"],
             done}
    end.

drain_batches(Ref) ->
    receive
        {Ref, _} ->
            drain_batches(Ref)
    after
        0 ->
            ok
    end.

result_part(TSBatch, Boundary) ->
    OutBatch = parse_batch(TSBatch, <<>>),
    ["\r\n--", Boundary, "\r\n",
     "Content-Type: text/plain\r\n\r\n",
     OutBatch].

parse_batch(<<>>, Bin) ->
    Bin;
parse_batch(B, Out) ->
    {Key, B1} = eleveldb:parse_string(B),
    {Val, B2} = eleveldb:parse_string(B1),
    Val2 = case msgpack:unpack(Val, [{format, jsx}]) of
               {error, _} ->
                   Val;
               {ok, KVPairs} ->
                   lists:foldl(
                       fun({K, V0}, <<>>) ->
                            V = typecast(V0),
                            <<K/binary, "=", V/binary>>;
                          ({K, V0}, Acc) ->
                            V = typecast(V0),
                            <<Acc/binary, ", ", K/binary, "=", V/binary>>
                       end, <<>>, KVPairs)

           end,
    <<Time:64, _/binary>> = Key,
    TimeStr = integer_to_binary(Time),
    Out2 = <<Out/binary, TimeStr/binary, " ", Val2/binary, "\r\n">>,
    parse_batch(B2, Out2).

typecast(Val) when is_integer(Val) ->
    integer_to_binary(Val);
typecast(Val) ->
    Val.

