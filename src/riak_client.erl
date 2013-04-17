%% -------------------------------------------------------------------
%%
%% riak_client: object used for access into the riak system
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

%% @doc object used for access into the riak system

-module(riak_client, [Node,ClientId]).
-author('Justin Sheehy <justin@basho.com>').

-export([get/2, get/3,get/4]).
-export([put/1, put/2,put/3,put/4,put/5]).
-export([delete/2,delete/3,delete/4]).
-export([delete_vclock/3,delete_vclock/4,delete_vclock/5]).
-export([list_keys/1,list_keys/2,list_keys/3]).
-export([stream_list_keys/1,stream_list_keys/2,stream_list_keys/3]).
-export([filter_buckets/1]).
-export([filter_keys/2,filter_keys/3]).
-export([list_buckets/0,list_buckets/2]).
-export([get_index/3,get_index/2]).
-export([stream_get_index/3,stream_get_index/2]).
-export([set_bucket/2,get_bucket/1,reset_bucket/1]).
-export([reload_all/1]).
-export([remove_from_cluster/1]).
-export([get_stats/1]).
-export([get_client_id/0]).
-export([for_dialyzer_only_ignore/2]).
-compile({no_auto_import,[put/2]}).
%% @type default_timeout() = 60000
-define(DEFAULT_TIMEOUT, 60000).
-define(DEFAULT_ERRTOL, 0.00003).

-type riak_client() :: term().

%% @spec get(riak_object:bucket(), riak_object:key()) ->
%%       {ok, riak_object:riak_object()} |
%%       {error, notfound} |
%%       {error, timeout} |
%%       {error, {n_val_violation, N::integer()}} |
%%       {error, {r_val_unsatisfied, R::integer(), Replies::integer()}} |
%%       {error, Err :: term()}
%% @doc Fetch the object at Bucket/Key.  Return a value as soon as the default
%%      R-value for the nodes have responded with a value or error.
%% @equiv get(Bucket, Key, R, default_timeout())
get(Bucket, Key) ->
    get(Bucket, Key, []).

%% @spec get(riak_object:bucket(), riak_object:key(), options()) ->
%%       {ok, riak_object:riak_object()} |
%%       {error, notfound} |
%%       {error, {deleted, vclock()}} |
%%       {error, timeout} |
%%       {error, {n_val_violation, N::integer()}} |
%%       {error, {r_val_unsatisfied, R::integer(), Replies::integer()}} |
%%       {error, Err :: term()}
%% @doc Fetch the object at Bucket/Key.  Return a value as soon as R-value for the nodes
%%      have responded with a value or error.
get(Bucket, Key, Options) when is_list(Options) ->
    Me = self(),
    ReqId = mk_reqid(),
    case node() of
        Node ->
            riak_kv_get_fsm:start_link({raw, ReqId, Me}, Bucket, Key, Options);
        _ ->
            proc_lib:spawn_link(Node, riak_kv_get_fsm, start_link,
                                [{raw, ReqId, Me}, Bucket, Key, Options])
    end,
    %% TODO: Investigate adding a monitor here and eliminating the timeout.
    Timeout = recv_timeout(Options),
    wait_for_reqid(ReqId, Timeout);

%% @spec get(riak_object:bucket(), riak_object:key(), R :: integer()) ->
%%       {ok, riak_object:riak_object()} |
%%       {error, notfound} |
%%       {error, timeout} |
%%       {error, {n_val_violation, N::integer()}} |
%%       {error, {r_val_unsatisfied, R::integer(), Replies::integer()}} |
%%       {error, Err :: term()}
%% @doc Fetch the object at Bucket/Key.  Return a value as soon as R
%%      nodes have responded with a value or error.
%% @equiv get(Bucket, Key, R, default_timeout())
get(Bucket, Key, R) ->
    get(Bucket, Key, [{r, R}]).

%% @spec get(riak_object:bucket(), riak_object:key(), R :: integer(),
%%           TimeoutMillisecs :: integer()) ->
%%       {ok, riak_object:riak_object()} |
%%       {error, notfound} |
%%       {error, timeout} |
%%       {error, {n_val_violation, N::integer()}} |
%%       {error, {r_val_unsatisfied, R::integer(), Replies::integer()}} |
%%       {error, Err :: term()}
%% @doc Fetch the object at Bucket/Key.  Return a value as soon as R
%%      nodes have responded with a value or error, or TimeoutMillisecs passes.
get(Bucket, Key, R, Timeout) when is_binary(Bucket), is_binary(Key),
                                  (is_atom(R) or is_integer(R)),
                                  is_integer(Timeout) ->
    get(Bucket, Key, [{r, R}, {timeout, Timeout}]).


%% @spec put(RObj :: riak_object:riak_object()) ->
%%        ok |
%%       {error, too_many_fails} |
%%       {error, timeout} |
%%       {error, {n_val_violation, N::integer()}}
%% @doc Store RObj in the cluster.
%%      Return as soon as the default W value number of nodes for this bucket
%%      nodes have received the request.
%% @equiv put(RObj, [])
put(RObj) -> THIS:put(RObj, []).


%% @spec put(RObj :: riak_object:riak_object(), riak_kv_put_fsm::options()) ->
%%       ok |
%%       {ok, details()} |
%%       {ok, riak_object:riak_object()} |
%%       {ok, riak_object:riak_object(), details()} |
%%       {error, notfound} |
%%       {error, timeout} |
%%       {error, {n_val_violation, N::integer()}} |
%%       {error, Err :: term()}
%%       {error, Err :: term(), details()}
%% @doc Store RObj in the cluster.
put(RObj, Options) when is_list(Options) ->
    Me = self(),
    ReqId = mk_reqid(),
    case ClientId of
        undefined ->
            case node() of
                Node ->
                    riak_kv_put_fsm:start_link({raw, ReqId, Me}, RObj, Options);
                _ ->
                    proc_lib:spawn_link(Node, riak_kv_put_fsm, start_link,
                                        [{raw, ReqId, Me}, RObj, Options])
            end;
        _ ->
            UpdObj = riak_object:increment_vclock(RObj, ClientId),
            case node() of
                Node ->
                    riak_kv_put_fsm:start_link({raw, ReqId, Me}, UpdObj, [asis|Options]);
                _ ->
                    proc_lib:spawn_link(Node, riak_kv_put_fsm, start_link,
                                        [{raw, ReqId, Me}, RObj, [asis|Options]])
            end
    end,
    %% TODO: Investigate adding a monitor here and eliminating the timeout.
    Timeout = recv_timeout(Options),
    wait_for_reqid(ReqId, Timeout);

%% @spec put(RObj :: riak_object:riak_object(), W :: integer()) ->
%%        ok |
%%       {error, too_many_fails} |
%%       {error, timeout} |
%%       {error, {n_val_violation, N::integer()}}
%% @doc Store RObj in the cluster.
%%      Return as soon as at least W nodes have received the request.
%% @equiv put(RObj, [{w, W}, {dw, W}])
put(RObj, W) -> THIS:put(RObj, [{w, W}, {dw, W}]).

%% @spec put(RObj::riak_object:riak_object(),W :: integer(),RW :: integer()) ->
%%        ok |
%%       {error, too_many_fails} |
%%       {error, timeout} |
%%       {error, {n_val_violation, N::integer()}}
%% @doc Store RObj in the cluster.
%%      Return as soon as at least W nodes have received the request, and
%%      at least DW nodes have stored it in their storage backend.
%% @equiv put(Robj, W, DW, default_timeout())
put(RObj, W, DW) -> THIS:put(RObj, [{w, W}, {dw, DW}]).

%% @spec put(RObj::riak_object:riak_object(), W :: integer(), RW :: integer(),
%%           TimeoutMillisecs :: integer()) ->
%%        ok |
%%       {error, too_many_fails} |
%%       {error, timeout} |
%%       {error, {n_val_violation, N::integer()}}
%% @doc Store RObj in the cluster.
%%      Return as soon as at least W nodes have received the request, and
%%      at least DW nodes have stored it in their storage backend, or
%%      TimeoutMillisecs passes.
put(RObj, W, DW, Timeout) -> THIS:put(RObj,  [{w, W}, {dw, DW}, {timeout, Timeout}]).

%% @spec put(RObj::riak_object:riak_object(), W :: integer(), RW :: integer(),
%%           TimeoutMillisecs :: integer(), Options::list()) ->
%%        ok |
%%       {error, too_many_fails} |
%%       {error, timeout} |
%%       {error, {n_val_violation, N::integer()}}
%% @doc Store RObj in the cluster.
%%      Return as soon as at least W nodes have received the request, and
%%      at least DW nodes have stored it in their storage backend, or
%%      TimeoutMillisecs passes.
put(RObj, W, DW, Timeout, Options) ->
   THIS:put(RObj, [{w, W}, {dw, DW}, {timeout, Timeout} | Options]).

%% @spec delete(riak_object:bucket(), riak_object:key()) ->
%%        ok |
%%       {error, too_many_fails} |
%%       {error, notfound} |
%%       {error, timeout} |
%%       {error, Err :: term()}
%% @doc Delete the object at Bucket/Key.  Return a value as soon as RW
%%      nodes have responded with a value or error.
%% @equiv delete(Bucket, Key, RW, default_timeout())
delete(Bucket,Key) -> delete(Bucket,Key,[],?DEFAULT_TIMEOUT).

%% @spec delete(riak_object:bucket(), riak_object:key(), RW :: integer()) ->
%%        ok |
%%       {error, too_many_fails} |
%%       {error, notfound} |
%%       {error, timeout} |
%%       {error, Err :: term()}
%% @doc Delete the object at Bucket/Key.  Return a value as soon as W/DW (or RW)
%%      nodes have responded with a value or error.
%% @equiv delete(Bucket, Key, RW, default_timeout())
delete(Bucket,Key,Options) when is_list(Options) ->
    delete(Bucket,Key,Options,?DEFAULT_TIMEOUT);
delete(Bucket,Key,RW) ->
    delete(Bucket,Key,[{rw, RW}],?DEFAULT_TIMEOUT).

%% @spec delete(riak_object:bucket(), riak_object:key(), RW :: integer(),
%%           TimeoutMillisecs :: integer()) ->
%%        ok |
%%       {error, too_many_fails} |
%%       {error, notfound} |
%%       {error, timeout} |
%%       {error, {n_val_violation, N::integer()}} |
%%       {error, Err :: term()}
%% @doc Delete the object at Bucket/Key.  Return a value as soon as W/DW (or RW)
%%      nodes have responded with a value or error, or TimeoutMillisecs passes.
delete(Bucket,Key,Options,Timeout) when is_list(Options) ->
    Me = self(),
    ReqId = mk_reqid(),
    riak_kv_delete_sup:start_delete(Node, [ReqId, Bucket, Key, Options, Timeout,
                                           Me, ClientId]),
    RTimeout = recv_timeout(Options),
    wait_for_reqid(ReqId, erlang:min(Timeout, RTimeout));
delete(Bucket,Key,RW,Timeout) ->
    delete(Bucket,Key,[{rw, RW}], Timeout).

%% @spec delete_vclock(riak_object:bucket(), riak_object:key(), vclock:vclock()) ->
%%        ok |
%%       {error, too_many_fails} |
%%       {error, notfound} |
%%       {error, timeout} |
%%       {error, Err :: term()}
%% @doc Delete the object at Bucket/Key.  Return a value as soon as W/DW (or RW)
%%      nodes have responded with a value or error.
%% @equiv delete(Bucket, Key, RW, default_timeout())
delete_vclock(Bucket,Key,VClock) ->
    delete_vclock(Bucket,Key,VClock,[{rw,default}],?DEFAULT_TIMEOUT).

%% @spec delete_vclock(riak_object:bucket(), riak_object:key(), vclock::vclock(), RW :: integer()) ->
%%        ok |
%%       {error, too_many_fails} |
%%       {error, notfound} |
%%       {error, timeout} |
%%       {error, Err :: term()}
%% @doc Delete the object at Bucket/Key.  Return a value as soon as W/DW (or RW)
%%      nodes have responded with a value or error.
%% @equiv delete(Bucket, Key, RW, default_timeout())
delete_vclock(Bucket,Key,VClock,Options) when is_list(Options) ->
    delete_vclock(Bucket,Key,VClock,Options,?DEFAULT_TIMEOUT);
delete_vclock(Bucket,Key,VClock,RW) ->
    delete_vclock(Bucket,Key,VClock,[{rw, RW}],?DEFAULT_TIMEOUT).

%% @spec delete_vclock(riak_object:bucket(), riak_object:key(), vclock:vclock(), RW :: integer(),
%%           TimeoutMillisecs :: integer()) ->
%%        ok |
%%       {error, too_many_fails} |
%%       {error, notfound} |
%%       {error, timeout} |
%%       {error, {n_val_violation, N::integer()}} |
%%       {error, Err :: term()}
%% @doc Delete the object at Bucket/Key.  Return a value as soon as W/DW (or RW)
%%      nodes have responded with a value or error, or TimeoutMillisecs passes.
delete_vclock(Bucket,Key,VClock,Options,Timeout) when is_list(Options) ->
    Me = self(),
    ReqId = mk_reqid(),
    riak_kv_delete_sup:start_delete(Node, [ReqId, Bucket, Key, Options, Timeout,
                                           Me, ClientId, VClock]),
    RTimeout = recv_timeout(Options),
    wait_for_reqid(ReqId, erlang:min(Timeout, RTimeout));
delete_vclock(Bucket,Key,VClock,RW,Timeout) ->
    delete_vclock(Bucket,Key,VClock,[{rw, RW}],Timeout).


%% @spec list_keys(riak_object:bucket()) ->
%%       {ok, [Key :: riak_object:key()]} |
%%       {error, timeout} |
%%       {error, Err :: term()}
%% @doc List the keys known to be present in Bucket.
%%      Key lists are updated asynchronously, so this may be slightly
%%      out of date if called immediately after a put or delete.
%% @equiv list_keys(Bucket, default_timeout()*8)
list_keys(Bucket) ->
    list_keys(Bucket, ?DEFAULT_TIMEOUT*8).

%% @spec list_keys(riak_object:bucket(), TimeoutMillisecs :: integer()) ->
%%       {ok, [Key :: riak_object:key()]} |
%%       {error, timeout} |
%%       {error, Err :: term()}
%% @doc List the keys known to be present in Bucket.
%%      Key lists are updated asynchronously, so this may be slightly
%%      out of date if called immediately after a put or delete.
list_keys(Bucket, Timeout) ->
    list_keys(Bucket, none, Timeout).

%% @spec list_keys(riak_object:bucket(), TimeoutMillisecs :: integer()) ->
%%       {ok, [Key :: riak_object:key()]} |
%%       {error, timeout} |
%%       {error, Err :: term()}
%% @doc List the keys known to be present in Bucket.
%%      Key lists are updated asynchronously, so this may be slightly
%%      out of date if called immediately after a put or delete.
list_keys(Bucket, Filter, Timeout) ->
    Me = self(),
    ReqId = mk_reqid(),
    riak_kv_keys_fsm_sup:start_keys_fsm(Node, [{raw, ReqId, Me}, [Bucket, Filter, Timeout]]),
    wait_for_listkeys(ReqId, Timeout).

stream_list_keys(Bucket) ->
    stream_list_keys(Bucket, ?DEFAULT_TIMEOUT).

stream_list_keys(Bucket, Timeout) ->
    Me = self(),
    stream_list_keys(Bucket, Timeout, Me).

%% @spec stream_list_keys(riak_object:bucket(),
%%                        TimeoutMillisecs :: integer(),
%%                        Client :: pid()) ->
%%       {ok, ReqId :: term()}
%% @doc List the keys known to be present in Bucket.
%%      Key lists are updated asynchronously, so this may be slightly
%%      out of date if called immediately after a put or delete.
%%      The list will not be returned directly, but will be sent
%%      to Client in a sequence of {ReqId, {keys,Keys}} messages
%%      and a final {ReqId, done} message.
%%      None of the Keys lists will be larger than the number of
%%      keys in Bucket on any single vnode.
stream_list_keys(Input, Timeout, Client) when is_pid(Client) ->
    ReqId = mk_reqid(),
    case Input of
        {Bucket, FilterInput} ->
            case riak_kv_mapred_filters:build_filter(FilterInput) of
                {error, _Error} ->
                    {error, _Error};
                {ok, FilterExprs} ->
                    riak_kv_keys_fsm_sup:start_keys_fsm(Node,
                                                        [{raw,
                                                          ReqId,
                                                          Client},
                                                         [Bucket,
                                                          FilterExprs,
                                                          Timeout]]),
                    {ok, ReqId}
            end;
        Bucket ->
            riak_kv_keys_fsm_sup:start_keys_fsm(Node,
                                                [{raw, ReqId, Client},
                                                 [Bucket,
                                                  none,
                                                  Timeout]]),
            {ok, ReqId}
    end.

%% @spec filter_keys(riak_object:bucket(), Fun :: function()) ->
%%       {ok, [Key :: riak_object:key()]} |
%%       {error, timeout} |
%%       {error, Err :: term()}
%% @doc List the keys known to be present in Bucket,
%%      filtered at the vnode according to Fun, via lists:filter.
%%      Key lists are updated asynchronously, so this may be slightly
%%      out of date if called immediately after a put or delete.
%% @equiv filter_keys(Bucket, Fun, default_timeout())
filter_keys(Bucket, Fun) ->
    list_keys(Bucket, Fun, ?DEFAULT_TIMEOUT).

%% @spec filter_keys(riak_object:bucket(), Fun :: function(), TimeoutMillisecs :: integer()) ->
%%       {ok, [Key :: riak_object:key()]} |
%%       {error, timeout} |
%%       {error, Err :: term()}
%% @doc List the keys known to be present in Bucket,
%%      filtered at the vnode according to Fun, via lists:filter.
%%      Key lists are updated asynchronously, so this may be slightly
%%      out of date if called immediately after a put or delete.
filter_keys(Bucket, Fun, Timeout) ->
            list_keys(Bucket, Fun, Timeout).

%% @spec list_buckets() ->
%%       {ok, [Bucket :: riak_object:bucket()]} |
%%       {error, timeout} |
%%       {error, Err :: term()}
%% @doc List buckets known to have keys.
%%      Key lists are updated asynchronously, so this may be slightly
%%      out of date if called immediately after any operation that
%%      either adds the first key or removes the last remaining key from
%%      a bucket.
%% @equiv list_buckets(default_timeout())
list_buckets() ->
    list_buckets(none, ?DEFAULT_TIMEOUT).

%% @spec list_buckets(TimeoutMillisecs :: integer()) ->
%%       {ok, [Bucket :: riak_object:bucket()]} |
%%       {error, timeout} |
%%       {error, Err :: term()}
%% @doc List buckets known to have keys.
%%      Key lists are updated asynchronously, so this may be slightly
%%      out of date if called immediately after any operation that
%%      either adds the first key or removes the last remaining key from
%%      a bucket.
list_buckets(Filter, Timeout) ->
    Me = self(),
    ReqId = mk_reqid(),
    riak_kv_buckets_fsm_sup:start_buckets_fsm(Node, [{raw, ReqId, Me}, [Filter, Timeout]]),
    wait_for_listbuckets(ReqId, Timeout).

%% @spec filter_buckets(Fun :: function()) ->
%%       {ok, [Bucket :: riak_object:bucket()]} |
%%       {error, timeout} |
%%       {error, Err :: term()}
%% @doc Return a list of filtered buckets.
filter_buckets(Fun) ->
    list_buckets(Fun, ?DEFAULT_TIMEOUT).

%% @spec get_index(Bucket :: binary(),
%%                 Query :: riak_index:query_def()) ->
%%       {ok, [Key :: riak_object:key()]} |
%%       {error, timeout} |
%%       {error, Err :: term()}.
%%
%% @doc Run the provided index query.
get_index(Bucket, Query) ->
    get_index(Bucket, Query, ?DEFAULT_TIMEOUT).

%% @spec get_index(Bucket :: binary(),
%%                 Query :: riak_index:query_def(),
%%                 TimeoutMillisecs :: integer()) ->
%%       {ok, [Key :: riak_object:key()]} |
%%       {error, timeout} |
%%       {error, Err :: term()}.
%%
%% @doc Run the provided index query.
get_index(Bucket, Query, Timeout) ->
    Me = self(),
    ReqId = mk_reqid(),
    riak_kv_index_fsm_sup:start_index_fsm(Node, [{raw, ReqId, Me}, [Bucket, none, Query, Timeout]]),
    wait_for_query_results(ReqId, Timeout).

%% @spec stream_get_index(Bucket :: binary(),
%%                        Query :: riak_index:query_def()) ->
%%       {ok, pid()} |
%%       {error, timeout} |
%%       {error, Err :: term()}.
%%
%% @doc Run the provided index query, return a stream handle.
stream_get_index(Bucket, Query) ->
    stream_get_index(Bucket, Query, ?DEFAULT_TIMEOUT).

%% @spec stream_get_index(Bucket :: binary(),
%%                        Query :: riak_index:query_def(),
%%                        TimeoutMillisecs :: integer()) ->
%%       {ok, pid()} |
%%       {error, timeout} |
%%       {error, Err :: term()}.
%%
%% @doc Run the provided index query, return a stream handle.
stream_get_index(Bucket, Query, Timeout) ->
    Me = self(),
    ReqId = mk_reqid(),
    riak_kv_index_fsm_sup:start_index_fsm(Node, [{raw, ReqId, Me}, [Bucket, none, Query, Timeout]]),
    {ok, ReqId}.

%% @spec set_bucket(riak_object:bucket(), [BucketProp :: {atom(),term()}]) -> ok
%% @doc Set the given properties for Bucket.
%%      This is generally best if done at application start time,
%%      to ensure expected per-bucket behavior.
%% See riak_core_bucket for expected useful properties.
set_bucket(BucketName,BucketProps) ->
    rpc:call(Node,riak_core_bucket,set_bucket,[BucketName,BucketProps]).
%% @spec get_bucket(riak_object:bucket()) -> [BucketProp :: {atom(),term()}]
%% @doc Get all properties for Bucket.
%% See riak_core_bucket for expected useful properties.
get_bucket(BucketName) ->
    rpc:call(Node,riak_core_bucket,get_bucket,[BucketName]).
%% @spec reset_bucket(riak_object:bucket()) -> ok
%% @doc Reset properties for this Bucket to the default values
reset_bucket(BucketName) ->
    rpc:call(Node,riak_core_bucket,reset_bucket,[BucketName]).
%% @spec reload_all(Module :: atom()) -> term()
%% @doc Force all Riak nodes to reload Module.
%%      This is used when loading new modules for map/reduce functionality.
reload_all(Module) -> rpc:call(Node,riak_core_util,reload_all,[Module]).

%% @spec remove_from_cluster(ExitingNode :: atom()) -> term()
%% @doc Cause all partitions owned by ExitingNode to be taken over
%%      by other nodes.
remove_from_cluster(ExitingNode) ->
    rpc:call(Node, riak_core_gossip, remove_from_cluster,[ExitingNode]).

get_stats(local) ->
    [{Node, rpc:call(Node, riak_kv_stat, get_stats, [])}];
get_stats(global) ->
    {ok, Ring} = rpc:call(Node, riak_core_ring_manager, get_my_ring, []),
    Nodes = riak_core_ring:all_members(Ring),
    [{N, rpc:call(N, riak_kv_stat, get_stats, [])} || N <- Nodes].

%% @doc Return the client id beign used for this client
get_client_id() ->
    ClientId.

%% @private
%% This function exists only to avoid compiler errors (unused type).
%% Unfortunately, I can't figure out how to suppress the bogus "Contract for
%% function that does not exist" warning from Dialyzer, so ignore that one.
-spec for_dialyzer_only_ignore(term(), term()) -> riak_client().
for_dialyzer_only_ignore(X, Y) ->
    ?MODULE:new(X, Y).

%% @private
mk_reqid() ->
    erlang:phash2({self(), os:timestamp()}). % only has to be unique per-pid

%% @private
wait_for_reqid(ReqId, Timeout) ->
    receive
        {ReqId, Response} -> Response
    after Timeout ->
            {error, timeout}
    end.

%% @private
wait_for_listkeys(ReqId, Timeout) ->
    wait_for_listkeys(ReqId,Timeout,[]).
%% @private
wait_for_listkeys(ReqId,Timeout,Acc) ->
    receive
        {ReqId, done} -> {ok, lists:flatten(Acc)};
        {ReqId, From, {keys, Res}} ->
            riak_kv_keys_fsm:ack_keys(From),
            wait_for_listkeys(ReqId, Timeout, [Res|Acc]);
        {ReqId,{keys,Res}} -> wait_for_listkeys(ReqId,Timeout,[Res|Acc]);
        {ReqId, Error} -> {error, Error}
    after Timeout ->
            {error, timeout, Acc}
    end.

%% @private
wait_for_listbuckets(ReqId, Timeout) ->
    receive
        {ReqId,{buckets, Buckets}} -> {ok, Buckets};
        {ReqId, Error} -> {error, Error}
    after Timeout ->
            {error, timeout}
    end.

%% @private
wait_for_query_results(ReqId, Timeout) ->
    wait_for_query_results(ReqId, Timeout, []).
%% @private
wait_for_query_results(ReqId, Timeout, Acc) ->
    receive
        {ReqId, done} -> {ok, lists:flatten(Acc)};
        {ReqId,{results, Res}} -> wait_for_query_results(ReqId, Timeout, [Res | Acc]);
        {ReqId, Error} -> {error, Error}
    after Timeout ->
            {error, timeout}
    end.

recv_timeout(Options) ->
    case proplists:get_value(recv_timeout, Options) of
        undefined ->
            %% If no reply timeout given, use the FSM timeout + 100ms to give it a chance
            %% to respond.
            proplists:get_value(timeout, Options, ?DEFAULT_TIMEOUT) + 100;
        Timeout ->
            %% Otherwise use the directly supplied timeout.
            Timeout
    end.
