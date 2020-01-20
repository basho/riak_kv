%%%-------------------------------------------------------------------
%%% @author paulhunt
%%% @copyright (C) 2019,
%%% @doc
%%%		<p>The Information PB service for Riak KV. This covers the
%%% 	following request messages in the original protocol:</p>
%%%
%%%		<pre>
%%%		 210 - RpbGetRingReq
%%%		 212 - RpbGetDefaultBucketPropsReq
%%%		</pre>
%%%
%%%		<p>This service produces the following responses:</p>
%%%
%%%		<pre>
%%%		 211 - RpbGetRingResp
%%%		 213 - RpbGetDefaultBucketPropsResp
%%%		</pre>
%%% @end
%%% Created : 22. Oct 2019 15:11
%%%-------------------------------------------------------------------
-module(riak_kv_pb_information).
-author("paulhunt").
-behaviour(riak_api_pb_service).

-include_lib("riak_pb/include/riak_kv_pb.hrl").
-include_lib("riak_pb/include/riak_pb_kv_codec.hrl").

%% API
-export([
	init/0,
	init/1,
	decode/2,
	encode/1,
	process/2,
	process_stream/3
]).

-import(riak_pb_kv_codec, [decode_quorum/1]).

-define(handle_decode_guard(Message), Message == rpbgetringreq orelse Message == rpbgetdefaultbucketpropsreq orelse
                                      Message == rpbgetnodesreq orelse Message == rpbnodewatchersubscribe).

-record(state, {
	node_watcher_update_timestamp :: integer()
}).

-type process_return() :: {reply, pb_information_resp(), #state{}} | {error, string(), #state{}}.
-type pb_information_req_code() :: 210 | 212 | 214.
-type pb_information_req_tag() :: rpbgetringreq | rpbgetdefaultbucketpropsreq | rpbgetnodesreq | rpbnodewatcherupdate.
-type pb_information_resp() :: #rpbgetringresp{} | #rpbgetdefaultbucketpropsresp{} | #rpbgetnodesresp{} |
                               #rpbnodewatcherupdate{}.

%%====================================================================
%% API Functions
%%====================================================================
-spec init() ->
	#state{}.
init() ->
	#state{}.

init(Timestamp) when erlang:is_integer(Timestamp) ->
	#state{node_watcher_update_timestamp = Timestamp}.

-spec decode(Code :: pb_information_req_code(), Bin :: binary()) ->
	{ok, pb_information_req_tag()}.
decode(Code, Bin) ->
	Message = riak_pb_codec:decode(Code, Bin),
	handle_decode(Message).

-spec encode(Message :: pb_information_resp()) ->
	{ok, iolist()}.
encode(Message) ->
	{ok, riak_pb_codec:encode(Message)}.

-spec process(Req :: pb_information_req_tag(), State :: #state{}) ->
	process_return().
process(Req, State) when Req == rpbgetringreq ->
	process_get_ring_req(State);
process(Req, State) when Req == rpbgetdefaultbucketpropsreq ->
	process_get_default_bucket_props(State);
process(Req, State) when Req == rpbgetnodesreq ->
	process_get_nodes_req(State);
process(Req, State) when Req == rpbnodewatcherupdate ->
	process_node_watcher_update(State);
process(Req, State) when Req == rpbnodewatchersubscribe ->
	process_node_watcher_subscribe(State).

-spec process_stream(_Message :: term(), _ReqId :: term(), State :: #state{}) ->
	{ignore, #state{}}.
process_stream(_, _, State) ->
	{ignore, State}.

%%====================================================================
%% Internal Functions
%%====================================================================
handle_decode(Message) when ?handle_decode_guard(Message) ->
	{ok, Message}.

process_get_ring_req(State) ->
	{ok, {_, NodeName, Vclock, ChRing, Meta, ClusterName, Next, Members, Claimant, Seen, Rvsn}} =
		riak_core_ring_manager:get_my_ring(),
	Ring = #riak_pb_ring{nodename = NodeName, vclock = Vclock, chring = ChRing, meta = Meta, clustername = ClusterName,
		next = Next, members = Members, claimant = Claimant, seen = Seen, rvsn = Rvsn},
	Resp = riak_pb_kv_codec:encode_ring(Ring),
	{reply, Resp, State}.

process_get_default_bucket_props(State) ->
	{ok, DefaultBucketPropsList} = application:get_env(riak_core, default_bucket_props),
	Resp = riak_pb_kv_codec:encode_bucket_props(DefaultBucketPropsList),
	{reply, Resp, State}.

process_get_nodes_req(State) ->
	NodesList = riak_core_node_watcher:nodes(riak_kv),
	Resp = riak_pb_kv_codec:encode_nodes(NodesList),
	{reply, Resp, State}.

process_node_watcher_update(State = #state{node_watcher_update_timestamp = Timestamp}) ->
	NodesList = riak_core_node_watcher:nodes(riak_kv),
	Resp = riak_pb_kv_codec:encode_node_watcher_update({Timestamp, NodesList}),
	{reply, Resp, State}.

process_node_watcher_subscribe(State) ->
	%% TODO - Add functionality here.
	{reply, ok, State}.
