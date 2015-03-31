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

%% @doc webmachine resource for inserting time series batches

-module(riak_kv_wm_ts_insert).

-export([init/1,allowed_methods/2, known_content_type/2, is_authorized/2, forbidden/2]).
-export([malformed_request/2, process_post/2, content_types_provided/2]).
-export([nop/2]).

-include_lib("webmachine/include/webmachine.hrl").

-define(TS_INSERT_CTYPE, "text/plain").
-define(DEFAULT_TIMEOUT, 60000).


-record(state, {ts_input :: {{binary(), binary()}, binary()},
                timeout,
                security}).
-type state() :: #state{}.

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

process_post(RD, #state{ts_input = TSInput}=State) ->
    case store_ts_batch(TSInput) of
        ok ->
            {true, RD, State};
        {error, Reason} ->
            {{halt, 400},
             send_error({error, iolist_to_binary(Reason)}, RD),
             State}
    end.

store_ts_batch({{Family, Series}, _TSBatch} = TSInput) ->
    %% TODO: Determine vnodes to call, send batch to all of them.
    Idx = chash:key_of({Family, Series}),
    N = riak_core_bucket:n_val(riak_core_bucket:get_bucket(Family)),
    Preflist = [{VnodeIdx, Node} ||
                {{VnodeIdx, Node}, primary}
                <- riak_core_apl:get_primary_apl(Idx, N, riak_kv)],
    % lager:info("Storing batch ~p", [TSInput]),
    riak_core_vnode_master:sync_command(hd(Preflist), {ts_write_batch, TSInput},
                                        riak_kv_vnode_master,
                                        60000),
    ok.

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
%% is equivalent to ?TS_INSERT_CTYPE
valid_ctype(?TS_INSERT_CTYPE) -> true;
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

parse_ts_input(B0) ->
    {Family, B1} = parse_line(B0),
    Batch1 = append_string(Family, <<>>),
    {Series, B2} = parse_line(B1),
    Batch2 = append_string(Series, Batch1),
    Batch3 = append_points(Batch2, B2),
    {ok, {{Family, Series}, Batch3}}.

append_points(Batch, <<>>) ->
    Batch;
append_points(Batch, <<C:8, _/binary>>)
  when C < $0; C > $9 ->
    Batch;
append_points(Batch, Input) ->
    {TimestampBin, Input1} = parse_timestamp(<<>>, Input),
    {Val0, Input2} = parse_line(Input1),
    Val = pack(Val0),
    Batch1 = append_string(Val, <<Batch/binary, TimestampBin/binary>>),
    append_points(Batch1, Input2).

pack(Val) ->
    KVPairs = binary:split(Val, <<", ">>, [global, trim]),
    Data = lists:foldl(fun(KVPair, Acc) ->
                           [K, V0] = binary:split(KVPair, <<"=">>),
                           V = typecast(V0),
                           [{K, V} | Acc]
                       end, [], KVPairs),
    msgpack:pack(lists:reverse(Data), [{format, jsx}]).

typecast(Val) ->
    try
        binary_to_integer(Val)
    catch _:_ ->
        Val
    end.

parse_line(Bin) ->
    parse_line(<<>>, Bin).

parse_timestamp(Out, <<" ", Rest/binary>>) ->
    IntTime = binary_to_integer(Out),
    {<<IntTime:64>>, Rest};
parse_timestamp(Out, <<C:8, Rest/binary>>) ->
    parse_timestamp(<<Out/binary, C:8>>, Rest).

parse_line(In, <<"\r\n", More/binary>>) ->
    {In, More};
parse_line(In, <<"\n", More/binary>>) ->
    {In, More};
parse_line(In, <<"\r", More/binary>>) ->
    {In, More};
parse_line(In, <<C:8, More/binary>>) ->
    parse_line(<<In/binary, C:8>>, More).

append_varint(N, Bin) ->
    N2 = N bsr 7,
    case N2 of
        0 ->
            C = N rem 128,
            <<Bin/binary, C:8>>;
        _ ->
            C = (N rem 128) + 128,
            append_varint(N2, <<Bin/binary, C:8>>)
    end.

append_string(S, Bin) ->
    L = byte_size(S),
    B2 = append_varint(L, Bin),
    <<B2/binary, S/binary>>.

verify_body(Body, State) ->
    case parse_ts_input(Body) of
        {ok, TSInput} ->
            {true, [], State#state{ts_input = TSInput}};
        _ ->
            {false,
             "The POST body was not a valid timeseries batch.\n",
             State}
    end.

usage() ->
    "This resource accepts POSTs with bodies containing test of the form:\n"
        "family\n"
        "series\n"
        "timestamp1 value1\n"
        "timestamp2 value2\n"
        "...".

