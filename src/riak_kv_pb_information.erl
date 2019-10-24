%%%-------------------------------------------------------------------
%%% @author paulhunt
%%% @copyright (C) 2019,
%%% @doc
%%%		<p>The Information PB service for Riak KV. This covers the
%%% 	following request messages in the original protocol:</p>
%%%
%%%		<pre>
%%%		 210 - RpbGetPreflistReq
%%%		</pre>
%%%
%%%		<p>This service produces the following responses:</p>
%%%
%%%		<pre>
%%%		 211 - RpbGetPreflistResp
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

-type riak_kv_pb_information_request() :: #rpbgetpreflistinforeq{}.

-type riak_kv_pb_information_response() :: #rpbgetpreflistinforesp{}.

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
process(Req, State) when erlang:is_record(Req, rpbgetpreflistinforeq) ->
	process_get_preflist_info_req(Req, State).

-spec process_stream(_Message :: term(), _ReqId :: term(), State :: #state{}) ->
	{ignore, #state{}}.
process_stream(_, _, State) ->
	{ignore, State}.

%%====================================================================
%% Internal Functions
%%====================================================================
handle_decode(Message) when erlang:is_record(Message, rpbgetpreflistinforeq) ->
	{ok, Message}.

process_get_preflist_info_req(#rpbgetpreflistinforeq{bucket = <<>>}, State) ->
	{error, "Bucket cannot be zero-length", State};
process_get_preflist_info_req(#rpbgetpreflistinforeq{key = <<>>}, State) ->
	{error, "Key cannot be zero-length", State};
process_get_preflist_info_req(Req, State) ->
	#rpbgetpreflistinforeq{bucket = Bucket, key = Key, n_val = OptionsNVal, sloppy_quorum = SloppyQuorum} = Req,
	BucketProps = riak_core_bucket:get_bucket(Bucket),
	BKey = {Bucket, Key},
	BucketNVal = get_prop(n_val, BucketProps),
	DocIdx = riak_core_util:chash_key(BKey, BucketProps),
	case get_n_val(OptionsNVal, BucketNVal) of
		{error, {n_val_violation, _BadNVal}} ->
			{error, "n_val must be an non-negative integer, which doesn't exceed bucket's n_val.", State};
		N ->
			Preflist2 = get_preflist2(SloppyQuorum, DocIdx, N),
			Preflist = [IndexNode || {IndexNode, _Type} <- Preflist2],
			Resp = #rpbgetpreflistinforesp{preflist = Preflist},
			{reply, Resp, State}
	end.

get_n_val(undefined, BucketNVal) ->
	BucketNVal;
get_n_val(NVal, BucketNVal) when erlang:is_integer(NVal), NVal > 0, NVal =< BucketNVal ->
	NVal;
get_n_val(NVal, _BucketProps) ->
	{error, {n_val_violation, NVal}}.

get_preflist2(SloppyQuorum, DocIdx, N) when SloppyQuorum == false ->
	riak_core_apl:get_primary_apl(DocIdx, N, riak_kv);
get_preflist2(_SloppyQuorum, DocIdx, N) ->
	UpNodes = riak_core_node_watcher:nodes(riak_kv),
	riak_core_apl:get_apl_ann(DocIdx, N, UpNodes).

get_prop(Name, PropsList) ->
	case lists:keyfind(Name, 1, PropsList) of
		{_, Val} ->
			Val;
		false ->
			undefined
	end.
