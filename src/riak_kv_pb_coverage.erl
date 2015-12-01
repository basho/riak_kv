%% -------------------------------------------------------------------
%%
%% riak_kv_pb_coverage: Expose coverage queries to Protocol Buffers
%%
%% Copyright (c) 2013 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc <p>The Coverage PB service for Riak KV. This covers the
%% following request messages:</p>
%%
%% <pre>
%%  70 - RpbCoverageReq
%% </pre>
%%
%% <p>This service produces the following responses:</p>
%%
%% <pre>
%%  71 - RpbCoverageResp
%% </pre>
%% @end

-module(riak_kv_pb_coverage).

-include_lib("riak_pb/include/riak_kv_pb.hrl").

-behaviour(riak_api_pb_service).
-compile(export_all).
-export([init/0,
         decode/2,
         encode/1,
         process/2,
         process_stream/3,
         checksum_binary_to_term/1,
         term_to_checksum_binary/1
        ]).

-record(state, {client}).

%% @doc init/0 callback. Returns the service internal start
%% state.
-spec init() -> any().
init() ->
    {ok, C} = riak:local_client(),
    #state{client=C}.

%% @doc decode/2 callback. Decodes an incoming message.
decode(Code, Bin) ->
    Msg = riak_pb_codec:decode(Code, Bin),
    case Msg of
        #rpbcoveragereq{type=T, bucket=B} ->
            Bucket = bucket_type(T, B),
            {ok, Msg, {"riak_kv.cover", Bucket}}
    end.

%% @doc encode/1 callback. Encodes an outgoing response message.
encode(Message) ->
    {ok, riak_pb_codec:encode(Message)}.

%% @doc process_stream/3 callback. This service does not create any
%% streaming responses and so ignores all incoming messages.
process_stream(_,_,State) ->
    {ignore, State}.

%% @doc process/2 callback. Handles an incoming request message.
process(#rpbcoveragereq{type=T, bucket=B, min_partitions=P, replace_cover=undefined}, #state{client=Client} = State) ->
    Bucket = bucket_type(T, B),
    convert_list(Client:get_cover(Bucket, P), State);
process(#rpbcoveragereq{type=T, bucket=B, min_partitions=P, replace_cover=R, unavailable_cover=U}, #state{client=Client} = State) ->
    Bucket = bucket_type(T, B),
    convert_list(
      Client:replace_cover(
        Bucket, P,
        checksum_binary_to_term(R),
        lists:map(fun checksum_binary_to_term/1, U)),
      State).

-spec term_to_checksum_binary(term()) -> term().
term_to_checksum_binary(C) ->
    Csum = erlang:adler32(term_to_binary(C)),
    term_to_binary({Csum, C}).

-spec checksum_binary_to_term(term()) -> {ok, term()} |
                                         {'error', atom()}.
checksum_binary_to_term(C) ->
    verify_checksum_tuple(binary_to_term(C)).

verify_checksum_tuple({Csum, Term}) ->
    case Csum =:= erlang:adler32(term_to_binary(Term)) of
        true ->
            {ok, Term};
        false ->
            {error, invalid_checksum}
    end;
verify_checksum_tuple(_) ->
    {error, invalid_structure}.

convert_list({error, Error}, State) ->
    {error, Error, State};
convert_list(Results, State) ->
    %% Pull hostnames & ports
    %% Wrap each element of this list into a rpbcoverageentry
    Resp = #rpbcoverageresp{
              entries=
                  lists:map(fun(P) ->
                      Node = proplists:get_value(node, P),
                      Desc = format_coverage_md(
                               proplists:get_value(vnode_hash, P),
                               proplists:get_value(node, P),
                               proplists:get_value(filters, P, []),
                               proplists:get_value(subpartition, P)),
                      {IP, Port} = node_to_pb_details(Node),

                      #rpbcoverageentry{
                         cover_context=term_to_checksum_binary(P),
                         ip=IP,
                         port=Port,
                         keyspace_desc=Desc
                        }
              end,
              Results)
             },
    {reply, Resp, State}.

node_to_pb_details(NodeName) ->
    handle_pb_response(
      NodeName,
      riak_core_util:safe_rpc(NodeName, application, get_env,
                              [riak_api, pb])).

handle_pb_response(NodeName, {badrpc, Error}) ->
    %% XXX: Todo: do something more smarter here
    lager:info("Coverage request: unable to request api endpoint "
               "from node ~p (~p)",
               [NodeName, Error]),
    {unreachable, 0};
handle_pb_response(NodeName, undefined) ->
    %% XXX: Todo: do something more smarter here
    lager:error("Coverage request: no api endpoint defined on "
                "node ~p",
                [NodeName]),
    {undefined, 0};
handle_pb_response(_NodeName, {ok, [{IP, Port}]}) ->
    {IP, Port}.

partition_id(Hash) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    riak_core_ring_util:hash_to_partition_id(Hash,
                                             riak_core_ring:num_partitions(Ring)).

format_coverage_md(Hash, Node, [], undefined) ->
    PartID = partition_id(Hash),
    unicode:characters_to_binary(io_lib:format("~s : ~B", [Node, PartID]), utf8);
format_coverage_md(Hash, Node, [], {Mask, _BSL}) ->
    PartID = partition_id(Hash),
    unicode:characters_to_binary(io_lib:format("~s : ~B (subpartition ~B)", [Node, PartID, Mask]), utf8);
format_coverage_md(Hash, Node, _Filters, undefined) ->
    PartID = partition_id(Hash),
    unicode:characters_to_binary(io_lib:format("~s : ~B (subselection)", [Node, PartID]), utf8).

%% always construct {Type, Bucket} tuple, filling in default type if needed
bucket_type(undefined, B) ->
    {<<"default">>, B};
bucket_type(T, B) ->
    {T, B}.
