%% -------------------------------------------------------------------
%%
%% riak_kv_pb_object: Expose KV functionality to Protocol Buffers
%%
%% Copyright (c) 2012-2013 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc <p>The Object/Key PB service for Riak KV. This covers the
%% following request messages in the original protocol:</p>
%%
%% <pre>
%%  3 - RpbGetClientIdReq
%%  5 - RpbSetClientIdReq
%%  9 - RpbGetReq
%% 11 - RpbPutReq
%% 13 - RpbDelReq
%% </pre>
%%
%% <p>This service produces the following responses:</p>
%%
%% <pre>
%%  4 - RpbGetClientIdResp
%%  6 - RpbSetClientIdResp
%% 10 - RpbGetResp
%% 12 - RpbPutResp - 0 length
%% 14 - RpbDelResp
%% </pre>
%%
%% <p>The semantics are unchanged from their original
%% implementations.</p>
%% @end

-module(riak_kv_pb_object).

-include_lib("riak_pb/include/riak_kv_pb.hrl").
-include_lib("riak_pb/include/riak_pb_kv_codec.hrl").

-ifdef(TEST).
-compile([export_all]).
-include_lib("eunit/include/eunit.hrl").
-endif.

-behaviour(riak_api_pb_service).

-export([init/0,
         decode/2,
         encode/1,
         process/2,
         process_stream/3]).

-import(riak_pb_kv_codec, [decode_quorum/1]).

-record(state, {client,    % local client
                req,       % current request (for multi-message requests like list keys)
                req_ctx,   % context to go along with request (partial results, request ids etc)
                client_id = <<0,0,0,0>> }). % emulate legacy API when vnode_vclocks is true

%% @doc init/0 callback. Returns the service internal start
%% state.
-spec init() -> any().
init() ->
    {ok, C} = riak:local_client(),
    #state{client=C}.

%% @doc decode/2 callback. Decodes an incoming message.
decode(Code, Bin) ->
    {ok, riak_pb_codec:decode(Code, Bin)}.

%% @doc encode/1 callback. Encodes an outgoing response message.
encode(Message) ->
    {ok, riak_pb_codec:encode(Message)}.

%% @doc process/2 callback. Handles an incoming request message.
process(rpbgetclientidreq, #state{client=C, client_id=CID} = State) ->
    ClientId = case riak_core_capability:get({riak_kv, vnode_vclocks}) of
                   true -> CID;
                   false -> C:get_client_id()
               end,
    Resp = #rpbgetclientidresp{client_id = ClientId},
    {reply, Resp, State};

process(#rpbsetclientidreq{client_id = ClientId}, State) ->
    NewState = case riak_core_capability:get({riak_kv, vnode_vclocks}) of
                   true -> State#state{client_id=ClientId};
                   false ->
                       {ok, C} = riak:local_client(ClientId),
                       State#state{client = C}
               end,
    {reply, rpbsetclientidresp, NewState};

process(#rpbgetreq{bucket = <<>>}, State) ->
    {error, "Bucket cannot be zero-length", State};
process(#rpbgetreq{key = <<>>}, State) ->
    {error, "Key cannot be zero-length", State};
process(#rpbgetreq{bucket=B, key=K, r=R0, pr=PR0, notfound_ok=NFOk,
                   basic_quorum=BQ, if_modified=VClock,
                   head=Head, deletedvclock=DeletedVClock,
                   n_val=N_val, sloppy_quorum=SloppyQuorum,
                   timeout=Timeout}, #state{client=C} = State) ->
    R = decode_quorum(R0),
    PR = decode_quorum(PR0),
    case C:get(B, K, make_option(deletedvclock, DeletedVClock) ++
                   make_option(r, R) ++
                   make_option(pr, PR) ++
                   make_option(timeout, Timeout) ++
                   make_option(notfound_ok, NFOk) ++
                   make_option(basic_quorum, BQ) ++
                   make_option(n_val, N_val) ++
                   make_option(sloppy_quorum, SloppyQuorum)) of
        {ok, O} ->

            case erlify_rpbvc(VClock) == riak_object:vclock(O) of
                true ->
                    {reply, #rpbgetresp{unchanged = true}, State};
                _ ->
                    Contents = riak_object:get_contents(O),
                    PbContent = case Head of
                                    true ->
                                        %% Remove all the 'value' fields from the contents
                                        %% This is a rough equivalent of a REST HEAD
                                        %% request
                                        BlankContents = [{MD, <<>>} || {MD, _} <- Contents],
                                        riak_pb_kv_codec:encode_contents(BlankContents);
                                    _ ->
                                        riak_pb_kv_codec:encode_contents(Contents)
                                end,
                    {reply, #rpbgetresp{content = PbContent,
                                        vclock = pbify_rpbvc(riak_object:vclock(O))}, State}
            end;
        {error, {deleted, TombstoneVClock}} ->
            %% Found a tombstone - return its vector clock so it can
            %% be properly overwritten
            {reply, #rpbgetresp{vclock = pbify_rpbvc(TombstoneVClock)}, State};
        {error, notfound} ->
            {reply, #rpbgetresp{}, State};
        {error, Reason} ->
            {error, {format,Reason}, State}
    end;

process(#rpbputreq{bucket = <<>>}, State) ->
    {error, "Bucket cannot be zero-length", State};
process(#rpbputreq{key = <<>>}, State) ->
    {error, "Key cannot be zero-length", State};
process(#rpbputreq{bucket=B, key=K, vclock=PbVC,
                   if_not_modified=NotMod, if_none_match=NoneMatch,
                   n_val=N_val, sloppy_quorum=SloppyQuorum} = Req,
        #state{client=C} = State) when NotMod; NoneMatch ->
    GetOpts = make_option(n_val, N_val) ++
              make_option(sloppy_quorum, SloppyQuorum),
    case C:get(B, K, GetOpts) of
        {ok, _} when NoneMatch ->
            {error, "match_found", State};
        {ok, O} when NotMod ->
            case erlify_rpbvc(PbVC) == riak_object:vclock(O) of
                true ->
                    process(Req#rpbputreq{if_not_modified=undefined,
                                          if_none_match=undefined},
                            State);
                _ ->
                    {error, "modified", State}
            end;
        {error, _} when NoneMatch ->
            process(Req#rpbputreq{if_not_modified=undefined,
                                  if_none_match=undefined},
                    State);
        {error, notfound} when NotMod ->
            {error, "notfound", State};
        {error, Reason} ->
            {error, {format, Reason}, State}
    end;

process(#rpbputreq{bucket=B, key=K, vclock=PbVC, content=RpbContent,
                   w=W0, dw=DW0, pw=PW0, return_body=ReturnBody,
                   return_head=ReturnHead, timeout=Timeout, asis=AsIs,
                   n_val=N_val, sloppy_quorum=SloppyQuorum},
        #state{client=C} = State) ->

    case K of
        undefined ->
            %% Generate a key, the user didn't supply one
            Key = list_to_binary(riak_core_util:unique_id_62()),
            ReturnKey = Key;
        _ ->
            Key = K,
            %% Don't return the key since we're not generating one
            ReturnKey = undefined
    end,
    O0 = riak_object:new(B, Key, <<>>),
    O1 = update_rpbcontent(O0, RpbContent),
    O  = update_pbvc(O1, PbVC),
    %% erlang_protobuffs encodes as 1/0/undefined
    W = decode_quorum(W0),
    DW = decode_quorum(DW0),
    PW = decode_quorum(PW0),
    Options = case ReturnBody of
                  1 -> [returnbody];
                  true -> [returnbody];
                  _ ->
                      case ReturnHead of
                          true -> [returnbody];
                          _ -> []
                      end
              end,
    case C:put(O, make_options([{w, W}, {dw, DW}, {pw, PW}, 
                                {timeout, Timeout}, {asis, AsIs},
                                {n_val, N_val},
                                {sloppy_quorum, SloppyQuorum}]) ++ Options) of
        ok when is_binary(ReturnKey) ->
            PutResp = #rpbputresp{key = ReturnKey},
            {reply, PutResp, State};
        ok ->
            {reply, #rpbputresp{}, State};
        {ok, Obj} ->
            Contents = riak_object:get_contents(Obj),
            PbContents = case ReturnHead of
                             true ->
                                 %% Remove all the 'value' fields from the contents
                                 %% This is a rough equivalent of a REST HEAD
                                 %% request
                                 BlankContents = [{MD, <<>>} || {MD, _} <- Contents],
                                 riak_pb_kv_codec:encode_contents(BlankContents);
                             _ ->
                                 riak_pb_kv_codec:encode_contents(Contents)
                         end,
            PutResp = #rpbputresp{content = PbContents,
                                  vclock = pbify_rpbvc(riak_object:vclock(Obj)),
                                  key = ReturnKey
                                 },
            {reply, PutResp, State};
        {error, notfound} ->
            {reply, #rpbputresp{}, State};
        {error, Reason} ->
            {error, {format, Reason}, State}
    end;

process(#rpbdelreq{bucket=B, key=K, vclock=PbVc,
                   r=R0, w=W0, pr=PR0, pw=PW0, dw=DW0, rw=RW0,
                   timeout=Timeout, n_val=N_val, sloppy_quorum=SloppyQuorum},
        #state{client=C} = State) ->
    W = decode_quorum(W0),
    PW = decode_quorum(PW0),
    DW = decode_quorum(DW0),
    R = decode_quorum(R0),
    PR = decode_quorum(PR0),
    RW = decode_quorum(RW0),

    Options = make_options([{r, R}, {w, W}, {rw, RW}, {pr, PR}, {pw, PW}, 
                            {dw, DW}, {timeout, Timeout}, {n_val, N_val},
                            {sloppy_quorum, SloppyQuorum}]),
    Result = case PbVc of
                 undefined ->
                     C:delete(B, K, Options);
                 _ ->
                     VClock = erlify_rpbvc(PbVc),
                     C:delete_vclock(B, K, VClock, Options)
             end,
    case Result of
        ok ->
            {reply, rpbdelresp, State};
        {error, notfound} ->  %% delete succeeds if already deleted
            {reply, rpbdelresp, State};
        {error, Reason} ->
            {error, {format, Reason}, State}
    end.

%% @doc process_stream/3 callback. This service does not create any
%% streaming responses and so ignores all incoming messages.
process_stream(_,_,State) ->
    {ignore, State}.

%% ===================================================================
%% Internal functions
%% ===================================================================

%% Update riak_object with the pbcontent provided
update_rpbcontent(O0, RpbContent) ->
    {MetaData, Value} = riak_pb_kv_codec:decode_content(RpbContent),
    O1 = riak_object:update_metadata(O0, MetaData),
    riak_object:update_value(O1, Value).

%% Update riak_object with vector clock
update_pbvc(O0, PbVc) ->
    Vclock = erlify_rpbvc(PbVc),
    riak_object:set_vclock(O0, Vclock).

make_options(List) ->
    lists:flatmap(fun({K,V}) -> make_option(K,V) end, List).

%% return a key/value tuple that we can ++ to other options so long as the
%% value is not default or undefined -- those values are pulled from the
%% bucket by the get/put FSMs.
make_option(_, undefined) ->
    [];
make_option(_, default) ->
    [];
make_option(K, V) ->
    [{K, V}].

%% Convert a vector clock to erlang
erlify_rpbvc(undefined) ->
    vclock:fresh();
erlify_rpbvc(<<>>) ->
    vclock:fresh();
erlify_rpbvc(PbVc) ->
    riak_object:decode_vclock(PbVc).

%% Convert a vector clock to protocol buffers
pbify_rpbvc(Vc) ->
    riak_object:encode_vclock(Vc).

%% ===================================================================
%% Tests
%% ===================================================================
-ifdef(TEST).

-define(CODE(Msg), riak_pb_codec:msg_code(Msg)).
-define(PAYLOAD(Msg), riak_kv_pb:encode(Msg)).

empty_bucket_key_test_() ->
    Name = "empty_bucket_key_test",
    SetupFun =  fun (load) ->
                        application:set_env(riak_kv, storage_backend, riak_kv_memory_backend),
                        application:set_env(riak_api, pb_ip, "127.0.0.1"),
                        application:set_env(riak_api, pb_port, 32767);
                    (_) -> ok end,
    {setup,
     riak_kv_test_util:common_setup(Name, SetupFun),
     riak_kv_test_util:common_cleanup(Name, SetupFun),
     [{"RpbPutReq with empty key is disallowed",
       ?_assertMatch([0|_], request(#rpbputreq{bucket = <<"foo">>,
                                               key = <<>>,
                                               content=#rpbcontent{value = <<"dummy">>}}))},
      {"RpbPutReq with empty bucket is disallowed",
       ?_assertMatch([0|_], request(#rpbputreq{bucket = <<>>,
                                               key = <<"foo">>,
                                               content=#rpbcontent{value = <<"dummy">>}}))},
      {"RpbGetReq with empty key is disallowed",
       ?_assertMatch([0|_], request(#rpbgetreq{bucket = <<"foo">>,
                                               key = <<>>}))},
      {"RpbGetReq with empty bucket is disallowed",
       ?_assertMatch([0|_], request(#rpbgetreq{bucket = <<>>,
                                               key = <<"foo">>}))}]}.

%% Utility funcs copied from riak_api/test/pb_service_test.erl

request(Msg) when is_tuple(Msg) andalso is_atom(element(1, Msg)) ->
    request(?CODE(element(1,Msg)), iolist_to_binary(?PAYLOAD(Msg))).

request(Code, Payload) when is_binary(Payload), is_integer(Code) ->
    Connection = new_connection(),
    ?assertMatch({ok, _}, Connection),
    {ok, Socket} = Connection,
    request(Code, Payload, Socket).

request(Code, Payload, Socket) when is_binary(Payload), is_integer(Code) ->
    ?assertEqual(ok, gen_tcp:send(Socket, <<Code:8, Payload/binary>>)),
    Result = gen_tcp:recv(Socket, 0),
    ?assertMatch({ok, _}, Result),
    {ok, Response} = Result,
    Response.

new_connection() ->
    new_connection([{packet,4}, {header, 1}]).

new_connection(Options) ->
    Host = app_helper:get_env(riak_api, pb_ip),
    Port = app_helper:get_env(riak_api, pb_port),
    gen_tcp:connect(Host, Port, [binary, {active, false},{nodelay, true}|Options]).

-endif.
