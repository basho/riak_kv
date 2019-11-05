%%%-------------------------------------------------------------------
%%% @author paulhunt
%%% @copyright (C) 2019,
%%% @doc
%%%		<p>The Information PB service for Riak KV. This covers the
%%% 	following request messages in the original protocol:</p>
%%%
%%%		<pre>
%%%		 210 - RpbGetRingReq
%%%		</pre>
%%%
%%%		<p>This service produces the following responses:</p>
%%%
%%%		<pre>
%%%		 211 - RpbGetRingResp
%%%		</pre>
%%% @end
%%% Created : 22. Oct 2019 15:11
%%%-------------------------------------------------------------------
-module(riak_kv_pb_information).
-author("paulhunt").
-behaviour(riak_api_pb_service).

-include_lib("riak_pb/include/riak_kv_pb.hrl").
-include_lib("riak_pb/include/riak_pb_kv_codec.hrl").

-ifdef(TEST).
-compile([export_all, nowarn_export_all]).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([
	init/0,
	decode/2,
	encode/1,
	process/2,
	process_stream/3
]).

-import(riak_pb_kv_codec, [decode_quorum/1]).

-record(state, {}).

-type process_return() :: 	{reply, riak_kv_pb_information_response(), #state{}} |
							{error, string(), #state{}}.
-type riak_kv_pb_information_request() :: rpbgetringreq | rpbgetdefaultbucketpropsreq.
-type riak_kv_pb_information_response() :: #rpbgetringresp{} | #rpbgetdefaultbucketpropsresp{}.

%%====================================================================
%% API Functions
%%====================================================================
%% TODO - Write specs for the api functions.
-spec init() ->
	#state{}.
init() ->
	#state{}.

decode(Code, Bin) ->
	Message = riak_pb_codec:decode(Code, Bin),
	handle_decode(Message).

encode(Message) ->
	{ok, riak_pb_codec:encode(Message)}.

-spec process(Req :: riak_kv_pb_information_request(), State :: #state{}) ->
	process_return().
process(Req, State) when Req == rpbgetringreq ->
	process_get_ring_req(Req, State);
process(Req, State) when Req == rpbgetdefaultbucketpropsreq ->
	process_get_default_bucket_props(Req, State).

-spec process_stream(_Message :: term(), _ReqId :: term(), State :: #state{}) ->
	{ignore, #state{}}.
process_stream(_, _, State) ->
	{ignore, State}.

%%====================================================================
%% Internal Functions
%%====================================================================
handle_decode(Message) when Message == rpbgetringreq ->
	{ok, Message};
handle_decode(Message) when Message == rpbgetdefaultbucketprops ->
	{ok, Message}.

process_get_ring_req(_Req, State) ->
	{ok, {_, NodeName, Vclock, ChRing, Meta, ClusterName, Next, Members, Claimant, Seen, Rvsn}} =
		riak_core_ring_manager:get_my_ring(),
	Ring = #riak_pb_ring{nodename = NodeName, vclock = Vclock, chring = ChRing, meta = Meta, clustername = ClusterName,
		next = Next, members = Members, claimant = Claimant, seen = Seen, rvsn = Rvsn},
	Resp = riak_pb_kv_codec:encode_ring(Ring),
	{reply, Resp, State}.

process_get_default_bucket_props(_Req, State) ->
	{ok, DefaultBucketPropsList} = application:get_env(riak_core, default_bucket_props),
	Resp = riak_pb_kv_codec:encode_bucket_props(DefaultBucketPropsList),
	{reply, Resp, State}.
