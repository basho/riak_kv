%% -------------------------------------------------------------------
%%
%% riak_kv_pb_object: Expose KV functionality to Protocol Buffers
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

-include_lib("riakc/include/riakclient_pb.hrl").
-include_lib("riakc/include/riakc_pb.hrl").

-behaviour(riak_api_pb_service).

-export([init/0,
         decode/2,
         encode/1,
         process/2,
         process_stream/3]).

-record(?MODULE, {client,    % local client
                  req,       % current request (for multi-message requests like list keys)
                  req_ctx,   % context to go along with request (partial results, request ids etc)
                  client_id = <<0,0,0,0>> }). % emulate legacy API when vnode_vclocks is true


-define(state, #?MODULE).
-define(DEFAULT_TIMEOUT, 60000).

%% @doc init/0 callback. Returns the service internal start
%% state.
-spec init() -> any().
init() ->
    {ok, C} = riak:local_client(),
    ?state{client=C}.

%% @doc decode/2 callback. Decodes an incoming message.
%% @todo Factor this out of riakc_pb to remove the dependency.
decode(Code, Bin) ->
    riakc_pb:decode(Code, Bin).

%% @doc encode/1 callback. Encodes an outgoing response message.
%% @todo Factor this out of riakc_pb to remove the dependency.
encode(Message) ->
    riakc_pb:encode(Message).

%% @doc process/2 callback. Handles an incoming request message.
process(rpbgetclientidreq, ?state{client=C, client_id=CID} = State) ->
    ClientId = case app_helper:get_env(riak_kv, vnode_vclocks, false) of
                   true -> CID;
                   false -> C:get_client_id()
               end,
    Resp = #rpbgetclientidresp{client_id = ClientId},
    {reply, Resp, State};

process(#rpbsetclientidreq{client_id = ClientId}, State) ->
    NewState = case app_helper:get_env(riak_kv, vnode_vclocks, false) of
                   true -> State?state{client_id=ClientId};
                   false ->
                       {ok, C} = riak:local_client(ClientId),
                       State?state{client = C}
               end,
    {reply, rpbsetclientidresp, NewState};

process(#rpbgetreq{bucket=B, key=K, r=R0, pr=PR0, notfound_ok=NFOk,
                   basic_quorum=BQ, if_modified=VClock,
                   head=Head, deletedvclock=DeletedVClock}, ?state{client=C} = State) ->
    R = normalize_rw_value(R0),
    PR = normalize_rw_value(PR0),
    case C:get(B, K, make_option(deletedvclock, DeletedVClock) ++
                   make_option(r, R) ++
                   make_option(pr, PR) ++
                   make_option(notfound_ok, NFOk) ++
                   make_option(basic_quorum, BQ)) of
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
                                        riakc_pb:pbify_rpbcontents(BlankContents, []);
                                    _ ->
                                        riakc_pb:pbify_rpbcontents(Contents, [])
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

process(#rpbputreq{bucket=B, key=K, vclock=PbVC,
                   if_not_modified=NotMod, if_none_match=NoneMatch} = Req,
        ?state{client=C} = State) when NotMod; NoneMatch ->
    case C:get(B, K) of
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
            {error, io_lib:format("~p", [Reason]), State}
    end;

process(#rpbputreq{bucket=B, key=K, vclock=PbVC, content=RpbContent,
                   w=W0, dw=DW0, pw=PW0, return_body=ReturnBody,
                   return_head=ReturnHead},
        ?state{client=C} = State) ->

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
    W = normalize_rw_value(W0),
    DW = normalize_rw_value(DW0),
    PW = normalize_rw_value(PW0),
    Options = case ReturnBody of
                  1 -> [returnbody];
                  true -> [returnbody];
                  _ ->
                      case ReturnHead of
                          true -> [returnbody];
                          _ -> []
                      end
              end,
    case C:put(O, make_option(w, W) ++ make_option(dw, DW) ++
                   make_option(pw, PW) ++ [{timeout, default_timeout()} | Options]) of
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
                                 riakc_pb:pbify_rpbcontents(BlankContents, []);
                             _ ->
                                 riakc_pb:pbify_rpbcontents(Contents, [])
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
                   r=R0, w=W0, pr=PR0, pw=PW0, dw=DW0, rw=RW0},
        ?state{client=C} = State) ->
    W = normalize_rw_value(W0),
    PW = normalize_rw_value(PW0),
    DW = normalize_rw_value(DW0),
    R = normalize_rw_value(R0),
    PR = normalize_rw_value(PR0),
    RW = normalize_rw_value(RW0),

    Options = make_option(r, R) ++
        make_option(w, W) ++
        make_option(rw, RW) ++
        make_option(pr, PR) ++
        make_option(pw, PW) ++
        make_option(dw, DW),
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
    {MetaData, Value} = riakc_pb:erlify_rpbcontent(RpbContent),
    O1 = riak_object:update_metadata(O0, MetaData),
    riak_object:update_value(O1, Value).

%% Update riak_object with vector clock
update_pbvc(O0, PbVc) ->
    Vclock = erlify_rpbvc(PbVc),
    riak_object:set_vclock(O0, Vclock).

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
    binary_to_term(zlib:unzip(PbVc)).

%% Convert a vector clock to protocol buffers
pbify_rpbvc(Vc) ->
    zlib:zip(term_to_binary(Vc)).

normalize_rw_value(?RIAKC_RW_ONE) -> one;
normalize_rw_value(?RIAKC_RW_QUORUM) -> quorum;
normalize_rw_value(?RIAKC_RW_ALL) -> all;
normalize_rw_value(?RIAKC_RW_DEFAULT) -> default;
normalize_rw_value(V) -> V.

default_timeout() ->
    ?DEFAULT_TIMEOUT.
