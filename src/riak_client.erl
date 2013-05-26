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

-module(riak_client).
-author('Justin Sheehy <justin@basho.com>').

-export([new/2, get/3, get/4,get/5]).
-export([put/2, put/3,put/4,put/5,put/6]).
-export([delete/3,delete/4,delete/5]).
-export([delete_vclock/4,delete_vclock/5,delete_vclock/6]).
-export([list_keys/2,list_keys/3,list_keys/4]).
-export([stream_list_keys/2,stream_list_keys/3,stream_list_keys/4]).
-export([filter_buckets/2]).
-export([filter_keys/3,filter_keys/4]).
-export([list_buckets/1, list_buckets/2, list_buckets/3]).
-export([stream_list_buckets/1, stream_list_buckets/2, 
         stream_list_buckets/3, stream_list_buckets/4]).
-export([get_index/4,get_index/3]).
-export([stream_get_index/4,stream_get_index/3]).
-export([set_bucket/3,get_bucket/2,reset_bucket/2]).
-export([reload_all/2]).
-export([remove_from_cluster/2]).
-export([get_stats/2]).
-export([get_client_id/1]).

%% @type default_timeout() = 60000
-define(DEFAULT_TIMEOUT, 60000).
-define(DEFAULT_ERRTOL, 0.00003).

-record(riak_client, {node :: node(), client_id :: term()}).
-opaque riak_client() :: #riak_client{}.
-export_type([riak_client/0]).

-spec new(node(), term()) -> riak_client().
%% @doc Initialize a client instance.
new(Node, ClientId) ->
    #riak_client{node = Node, client_id = ClientId}.

-spec get(riak_client(), riak_object:bucket(), riak_object:key()) ->
      {ok, riak_object:riak_object()} |
      {error, notfound} |
      {error, timeout} |
      {error, {n_val_violation, N::integer()}} |
      {error, {r_val_unsatisfied, R::integer(), Replies::integer()}} |
      {error, Err :: term()}.
%% @doc Fetch the object at Bucket/Key.  Return a value as soon as the default
%%      R-value for the nodes have responded with a value or error.
%% @equiv get(RClient, Bucket, Key, R, default_timeout())
get(#riak_client{} = RClient, Bucket, Key) ->
    get(RClient, Bucket, Key, []).

-spec get(riak_client(), riak_object:bucket(), riak_object:key(),
          riak_kv_get_fsm:options() | integer()) ->
      {ok, riak_object:riak_object()} |
      {error, notfound} |
      {error, {deleted, vclock:vclock()}} |
      {error, timeout} |
      {error, {n_val_violation, N::integer()}} |
      {error, {r_val_unsatisfied, R::integer(), Replies::integer()}} |
      {error, Err :: term()}.
%% @doc Fetch the object at Bucket/Key.  Return a value as soon as R-value for the nodes
%%      have responded with a value or error.
get(#riak_client{node = Node}, Bucket, Key, Options)
  when is_list(Options) ->
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
get(#riak_client{} = RClient, Bucket, Key, R) ->
    get(RClient, Bucket, Key, [{r, R}]).

-spec get(riak_client(), riak_object:bucket(), riak_object:key(),
          R :: integer(), TimeoutMillisecs :: integer()) ->
      {ok, riak_object:riak_object()} |
      {error, notfound} |
      {error, timeout} |
      {error, {n_val_violation, N::integer()}} |
      {error, {r_val_unsatisfied, R::integer(), Replies::integer()}} |
      {error, Err :: term()}.
%% @doc Fetch the object at Bucket/Key.  Return a value as soon as R
%%      nodes have responded with a value or error, or TimeoutMillisecs passes.
get(#riak_client{} = RClient, Bucket, Key, R, Timeout)
  when is_binary(Bucket), is_binary(Key),
       (is_atom(R) or is_integer(R)),
       is_integer(Timeout) ->
    get(RClient, Bucket, Key, [{r, R}, {timeout, Timeout}]).


-spec put(riak_client(), RObj :: riak_object:riak_object()) ->
       ok |
      {error, too_many_fails} |
      {error, timeout} |
      {error, {n_val_violation, N::integer()}}.
%% @doc Store RObj in the cluster.
%%      Return as soon as the default W value number of nodes for this bucket
%%      nodes have received the request.
%% @equiv put(RClient, RObj, [])
put(#riak_client{} = RClient, RObj) -> put(RClient, RObj, []).


-spec put(riak_client(), RObj :: riak_object:riak_object(),
          riak_kv_put_fsm:options() | integer()) ->
      ok |
      {ok, riak_kv_put_fsm:detail()} |
      {ok, riak_object:riak_object()} |
      {ok, riak_object:riak_object(), riak_kv_put_fsm:detail()} |
      {error, notfound} |
      {error, timeout} |
      {error, {n_val_violation, N::integer()}} |
      {error, Err :: term()} |
      {error, Err :: term(), riak_kv_put_fsm:detail()}.
%% @doc Store RObj in the cluster.
put(#riak_client{node = Node, client_id = ClientId}, RObj, Options)
  when is_list(Options) ->
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
put(#riak_client{} = RClient, RObj, W) -> put(RClient, RObj, [{w, W}, {dw, W}]).

-spec put(riak_client(), RObj::riak_object:riak_object(),
          W :: integer(),RW :: integer()) ->
       ok |
      {error, too_many_fails} |
      {error, timeout} |
      {error, {n_val_violation, N::integer()}}.
%% @doc Store RObj in the cluster.
%%      Return as soon as at least W nodes have received the request, and
%%      at least DW nodes have stored it in their storage backend.
%% @equiv put(RClient, Robj, W, DW, default_timeout())
put(#riak_client{} = RClient, RObj, W, DW) -> put(RClient, RObj, [{w, W}, {dw, DW}]).

-spec put(riak_client(), RObj::riak_object:riak_object(), W :: integer(),
          RW :: integer(), TimeoutMillisecs :: integer()) ->
       ok |
      {error, too_many_fails} |
      {error, timeout} |
      {error, {n_val_violation, N::integer()}}.
%% @doc Store RObj in the cluster.
%%      Return as soon as at least W nodes have received the request, and
%%      at least DW nodes have stored it in their storage backend, or
%%      TimeoutMillisecs passes.
put(#riak_client{} = RClient, RObj, W, DW, Timeout) ->
    put(RClient, RObj,  [{w, W}, {dw, DW}, {timeout, Timeout}]).

-spec put(riak_client(), RObj::riak_object:riak_object(), W :: integer(),
          RW :: integer(), TimeoutMillisecs :: integer(), Options::list()) ->
       ok |
      {error, too_many_fails} |
      {error, timeout} |
      {error, {n_val_violation, N::integer()}}.
%% @doc Store RObj in the cluster.
%%      Return as soon as at least W nodes have received the request, and
%%      at least DW nodes have stored it in their storage backend, or
%%      TimeoutMillisecs passes.
put(#riak_client{} = RClient, RObj, W, DW, Timeout, Options) ->
   put(RClient, RObj, [{w, W}, {dw, DW}, {timeout, Timeout} | Options]).

-spec delete(riak_client(), riak_object:bucket(), riak_object:key()) ->
       ok |
      {error, too_many_fails} |
      {error, notfound} |
      {error, timeout} |
      {error, Err :: term()}.
%% @doc Delete the object at Bucket/Key.  Return a value as soon as RW
%%      nodes have responded with a value or error.
%% @equiv delete(RClient, Bucket, Key, RW, default_timeout())
delete(#riak_client{} = RClient,Bucket,Key) ->
    delete(RClient,Bucket,Key,[],?DEFAULT_TIMEOUT).

-spec delete(riak_client(), riak_object:bucket(),
             riak_object:key(), RW :: integer()) ->
       ok |
      {error, too_many_fails} |
      {error, notfound} |
      {error, timeout} |
      {error, Err :: term()}.
%% @doc Delete the object at Bucket/Key.  Return a value as soon as W/DW (or RW)
%%      nodes have responded with a value or error.
%% @equiv delete(RClient, Bucket, Key, RW, default_timeout())
delete(#riak_client{} = RClient,Bucket,Key,Options) when is_list(Options) ->
    delete(RClient,Bucket,Key,Options,?DEFAULT_TIMEOUT);
delete(#riak_client{} = RClient,Bucket,Key,RW) ->
    delete(RClient,Bucket,Key,[{rw, RW}],?DEFAULT_TIMEOUT).

-spec delete(riak_client(), riak_object:bucket(), riak_object:key(),
             RW :: integer(), TimeoutMillisecs :: integer()) ->
       ok |
      {error, too_many_fails} |
      {error, notfound} |
      {error, timeout} |
      {error, {n_val_violation, N::integer()}} |
      {error, Err :: term()}.
%% @doc Delete the object at Bucket/Key.  Return a value as soon as W/DW (or RW)
%%      nodes have responded with a value or error, or TimeoutMillisecs passes.
delete(#riak_client{node = Node, client_id = ClientId},
       Bucket,Key,Options,Timeout) when is_list(Options) ->
    Me = self(),
    ReqId = mk_reqid(),
    riak_kv_delete_sup:start_delete(Node, [ReqId, Bucket, Key, Options, Timeout,
                                           Me, ClientId]),
    RTimeout = recv_timeout(Options),
    wait_for_reqid(ReqId, erlang:min(Timeout, RTimeout));
delete(#riak_client{} = RClient,Bucket,Key,RW,Timeout) ->
    delete(RClient,Bucket,Key,[{rw, RW}], Timeout).

-spec delete_vclock(riak_client(), riak_object:bucket(),
                    riak_object:key(), vclock:vclock()) ->
       ok |
      {error, too_many_fails} |
      {error, notfound} |
      {error, timeout} |
      {error, Err :: term()}.
%% @doc Delete the object at Bucket/Key.  Return a value as soon as W/DW (or RW)
%%      nodes have responded with a value or error.
%% @equiv delete(RClient, Bucket, Key, RW, default_timeout())
delete_vclock(#riak_client{} = RClient,Bucket,Key,VClock) ->
    delete_vclock(RClient,Bucket,Key,VClock,[{rw,default}],?DEFAULT_TIMEOUT).

-spec delete_vclock(riak_client(), riak_object:bucket(),
                    riak_object:key(), vclock:vclock(), RW :: integer()) ->
       ok |
      {error, too_many_fails} |
      {error, notfound} |
      {error, timeout} |
      {error, Err :: term()}.
%% @doc Delete the object at Bucket/Key.  Return a value as soon as W/DW (or RW)
%%      nodes have responded with a value or error.
%% @equiv delete(RClient, Bucket, Key, RW, default_timeout())
delete_vclock(#riak_client{} = RClient,Bucket,Key,VClock,Options)
  when is_list(Options) ->
    delete_vclock(RClient,Bucket,Key,VClock,Options,?DEFAULT_TIMEOUT);
delete_vclock(#riak_client{} = RClient,Bucket,Key,VClock,RW) ->
    delete_vclock(RClient,Bucket,Key,VClock,[{rw, RW}],?DEFAULT_TIMEOUT).

-spec delete_vclock(riak_client(), riak_object:bucket(),
                    riak_object:key(), vclock:vclock(), RW :: integer(),
                    TimeoutMillisecs :: integer()) ->
       ok |
      {error, too_many_fails} |
      {error, notfound} |
      {error, timeout} |
      {error, {n_val_violation, N::integer()}} |
      {error, Err :: term()}.
%% @doc Delete the object at Bucket/Key.  Return a value as soon as W/DW (or RW)
%%      nodes have responded with a value or error, or TimeoutMillisecs passes.
delete_vclock(#riak_client{node = Node, client_id = ClientId},
              Bucket,Key,VClock,Options,Timeout) when is_list(Options) ->
    Me = self(),
    ReqId = mk_reqid(),
    riak_kv_delete_sup:start_delete(Node, [ReqId, Bucket, Key, Options, Timeout,
                                           Me, ClientId, VClock]),
    RTimeout = recv_timeout(Options),
    wait_for_reqid(ReqId, erlang:min(Timeout, RTimeout));
delete_vclock(#riak_client{} = RClient,Bucket,Key,VClock,RW,Timeout) ->
    delete_vclock(RClient,Bucket,Key,VClock,[{rw, RW}],Timeout).


-spec list_keys(riak_client(), riak_object:bucket()) ->
      {ok, [Key :: riak_object:key()]} |
      {error, timeout} |
      {error, Err :: term()}.
%% @doc List the keys known to be present in Bucket.
%%      Key lists are updated asynchronously, so this may be slightly
%%      out of date if called immediately after a put or delete.
%% @equiv list_keys(RClient, Bucket, default_timeout()*8)
list_keys(#riak_client{} = RClient, Bucket) ->
    list_keys(RClient, Bucket, ?DEFAULT_TIMEOUT*8).

-spec list_keys(riak_client(), riak_object:bucket(),
                TimeoutMillisecs :: integer()) ->
      {ok, [Key :: riak_object:key()]} |
      {error, timeout} |
      {error, Err :: term()}.
%% @doc List the keys known to be present in Bucket.
%%      Key lists are updated asynchronously, so this may be slightly
%%      out of date if called immediately after a put or delete.
list_keys(#riak_client{} = RClient, Bucket, Timeout) ->
    list_keys(RClient, Bucket, none, Timeout).

-spec list_keys(riak_client(), riak_object:bucket(),
                Filter :: fun() | none, TimeoutMillisecs :: integer()) ->
      {ok, [Key :: riak_object:key()]} |
      {error, timeout} |
      {error, Err :: term()}.
%% @doc List the keys known to be present in Bucket.
%%      Key lists are updated asynchronously, so this may be slightly
%%      out of date if called immediately after a put or delete.
list_keys(#riak_client{node = Node}, Bucket, Filter, Timeout0) ->
    Timeout = 
        case Timeout0 of
            T when is_integer(T) -> T; 
            _ -> ?DEFAULT_TIMEOUT*8
        end,
    Me = self(),
    ReqId = mk_reqid(),
    riak_kv_keys_fsm_sup:start_keys_fsm(Node, [{raw, ReqId, Me}, [Bucket, Filter, Timeout]]),
    wait_for_listkeys(ReqId).

stream_list_keys(#riak_client{} = RClient, Bucket) ->
    stream_list_keys(RClient, Bucket, ?DEFAULT_TIMEOUT).

stream_list_keys(#riak_client{} = RClient, Bucket, undefined) ->
    stream_list_keys(RClient, Bucket, ?DEFAULT_TIMEOUT);
stream_list_keys(#riak_client{} = RClient, Bucket, Timeout) ->
    Me = self(),
    stream_list_keys(RClient, Bucket, Timeout, Me).

-spec stream_list_keys(riak_client(),
                       riak_object:bucket(),
                       TimeoutMillisecs :: integer(),
                       Client :: pid()) ->
      {ok, ReqId :: term()}.
%% @doc List the keys known to be present in Bucket.
%%      Key lists are updated asynchronously, so this may be slightly
%%      out of date if called immediately after a put or delete.
%%      The list will not be returned directly, but will be sent
%%      to Client in a sequence of {ReqId, {keys,Keys}} messages
%%      and a final {ReqId, done} message.
%%      None of the Keys lists will be larger than the number of
%%      keys in Bucket on any single vnode.
stream_list_keys(#riak_client{node = Node},
                 Input, Timeout, Client) when is_pid(Client) ->
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

-spec filter_keys(riak_client(), riak_object:bucket(), Fun :: function()) ->
      {ok, [Key :: riak_object:key()]} |
      {error, timeout} |
      {error, Err :: term()}.
%% @doc List the keys known to be present in Bucket,
%%      filtered at the vnode according to Fun, via lists:filter.
%%      Key lists are updated asynchronously, so this may be slightly
%%      out of date if called immediately after a put or delete.
%% @equiv filter_keys(RClient, Bucket, Fun, default_timeout())
filter_keys(#riak_client{} = RClient, Bucket, Fun) ->
    list_keys(RClient, Bucket, Fun, ?DEFAULT_TIMEOUT).

-spec filter_keys(riak_client(), riak_object:bucket(),
                  Fun :: function(), TimeoutMillisecs :: integer()) ->
      {ok, [Key :: riak_object:key()]} |
      {error, timeout} |
      {error, Err :: term()}.
%% @doc List the keys known to be present in Bucket,
%%      filtered at the vnode according to Fun, via lists:filter.
%%      Key lists are updated asynchronously, so this may be slightly
%%      out of date if called immediately after a put or delete.
filter_keys(#riak_client{} = RClient, Bucket, Fun, Timeout) ->
            list_keys(RClient, Bucket, Fun, Timeout).

-spec list_buckets(riak_client()) ->
      {ok, [Bucket :: riak_object:bucket()]} |
      {error, timeout} |
      {error, Err :: term()}.
%% @doc List buckets known to have keys.
%%      Key lists are updated asynchronously, so this may be slightly
%%      out of date if called immediately after any operation that
%%      either adds the first key or removes the last remaining key from
%%      a bucket.
%% @equiv list_buckets(RClient, default_timeout())
list_buckets(#riak_client{} = RClient) ->
    list_buckets(RClient, none, ?DEFAULT_TIMEOUT).

-spec list_buckets(riak_client(), timeout()) ->
      {ok, [Bucket :: riak_object:bucket()]} |
      {error, timeout} |
      {error, Err :: term()}.
%% @doc List buckets known to have keys.
%%      Key lists are updated asynchronously, so this may be slightly
%%      out of date if called immediately after any operation that
%%      either adds the first key or removes the last remaining key from
%%      a bucket.
%% @equiv list_buckets(RClient, none, default_timeout())
list_buckets(#riak_client{} = RClient, undefined) ->
    list_buckets(RClient, none, ?DEFAULT_TIMEOUT*8);
list_buckets(#riak_client{} = RClient, Timeout) ->
    list_buckets(RClient, none, Timeout).

-spec list_buckets(riak_client(), Filter :: fun() | none,
                   TimeoutMillisecs :: integer()) ->
      {ok, [Bucket :: riak_object:bucket()]} |
      {error, timeout} |
      {error, Err :: term()}.
%% @doc List buckets known to have keys.
%%      Key lists are updated asynchronously, so this may be slightly
%%      out of date if called immediately after any operation that
%%      either adds the first key or removes the last remaining key from
%%      a bucket.
list_buckets(#riak_client{node = Node}, Filter, Timeout) ->
    Me = self(),
    ReqId = mk_reqid(),
    {ok, _Pid} = riak_kv_buckets_fsm_sup:start_buckets_fsm(Node, 
                                                           [{raw, ReqId, Me}, 
                                                            [Filter, Timeout, 
                                                             false]]),
    wait_for_listbuckets(ReqId).

-spec filter_buckets(riak_client(), Fun :: function()) ->
      {ok, [Bucket :: riak_object:bucket()]} |
      {error, timeout} |
      {error, Err :: term()}.
%% @doc Return a list of filtered buckets.
filter_buckets(#riak_client{} = RClient, Fun) ->
    list_buckets(RClient, Fun, ?DEFAULT_TIMEOUT).

stream_list_buckets(#riak_client{} = RClient) ->
    stream_list_buckets(RClient, none, ?DEFAULT_TIMEOUT).

stream_list_buckets(#riak_client{} = RClient, undefined) ->
    stream_list_buckets(RClient, none, ?DEFAULT_TIMEOUT);
stream_list_buckets(#riak_client{} = RClient, Timeout) when is_integer(Timeout) ->
    stream_list_buckets(RClient, none, Timeout);
stream_list_buckets(#riak_client{} = RClient, Filter) when is_function(Filter) ->
    stream_list_buckets(RClient, Filter, ?DEFAULT_TIMEOUT).    

stream_list_buckets(#riak_client{} = RClient, Filter, Timeout) ->
    Me = self(),
    stream_list_buckets(RClient, Filter, Timeout, Me).

-spec stream_list_buckets(riak_client(), FilterFun :: fun(),
                          TimeoutMillisecs :: integer(),
                          Client :: pid()) ->
      {ok, [Bucket :: riak_object:bucket()]} |
      {error, timeout} |
      {error, Err :: term()}.
%% @doc List buckets known to have keys.
%%      Key lists are updated asynchronously, so this may be slightly
%%      out of date if called immediately after any operation that
%%      either adds the first key or removes the last remaining key from
%%      a bucket.
stream_list_buckets(#riak_client{node = Node}, Filter, Timeout, Client) ->
    ReqId = mk_reqid(),
    {ok, _Pid} = riak_kv_buckets_fsm_sup:start_buckets_fsm(Node, 
                                                           [{raw, ReqId, 
                                                             Client}, 
                                                            [Filter, Timeout, 
                                                             true]]),
    {ok, ReqId}.

-spec get_index(riak_client(), Bucket :: binary(),
                Query :: riak_index:query_def()) ->
      {ok, [Key :: riak_object:key()]} |
      {error, timeout} |
      {error, Err :: term()}.
%% @doc Run the provided index query.
get_index(#riak_client{} = RClient, Bucket, Query) ->
    get_index(RClient, Bucket, Query, [{timeout, ?DEFAULT_TIMEOUT}]).

-spec get_index(riak_client(), Bucket :: binary(),
                Query :: riak_index:query_def(),
                TimeoutMillisecs :: integer()) ->
      {ok, [Key :: riak_object:key()]} |
      {error, timeout} |
      {error, Err :: term()}.
%% @doc Run the provided index query.
get_index(#riak_client{node = Node}, Bucket, Query, Opts) ->
    Timeout = proplists:get_value(timeout, Opts, ?DEFAULT_TIMEOUT),
    MaxResults = proplists:get_value(max_results, Opts, all),
    Me = self(),
    ReqId = mk_reqid(),
    riak_kv_index_fsm_sup:start_index_fsm(Node, [{raw, ReqId, Me}, [Bucket, none, Query, Timeout, MaxResults]]),
    wait_for_query_results(ReqId, Timeout).

-spec stream_get_index(riak_client(), Bucket :: binary(),
                       Query :: riak_index:query_def()) ->
      {ok, pid()} |
      {error, timeout} |
      {error, Err :: term()}.
%% @doc Run the provided index query, return a stream handle.
stream_get_index(#riak_client{} = RClient, Bucket, Query) ->
    stream_get_index(RClient, Bucket, Query, [{timeout, ?DEFAULT_TIMEOUT}]).

-spec stream_get_index(riak_client(), Bucket :: binary(),
                       Query :: riak_index:query_def(),
                       TimeoutMillisecs :: integer()) ->
      {ok, pid()} |
      {error, timeout} |
      {error, Err :: term()}.
%% @doc Run the provided index query, return a stream handle.
stream_get_index(#riak_client{node = Node}, Bucket, Query, Opts) ->
    Timeout = proplists:get_value(timeout, Opts, ?DEFAULT_TIMEOUT),
    MaxResults = proplists:get_value(max_results, Opts, all),
    Me = self(),
    ReqId = mk_reqid(),
    riak_kv_index_fsm_sup:start_index_fsm(Node, [{raw, ReqId, Me}, [Bucket, none, Query, Timeout, MaxResults]]),
    {ok, ReqId}.

-spec set_bucket(riak_client(), riak_object:bucket(),
                 [BucketProp :: {atom(),term()}]) -> ok.
%% @doc Set the given properties for Bucket.
%%      This is generally best if done at application start time,
%%      to ensure expected per-bucket behavior.
%% See riak_core_bucket for expected useful properties.
set_bucket(#riak_client{node = Node}, BucketName,BucketProps) ->
    rpc:call(Node,riak_core_bucket,set_bucket,[BucketName,BucketProps]).
-spec get_bucket(riak_client(),
                 riak_object:bucket()) -> [BucketProp :: {atom(),term()}].
%% @doc Get all properties for Bucket.
%% See riak_core_bucket for expected useful properties.
get_bucket(#riak_client{node = Node}, BucketName) ->
    rpc:call(Node,riak_core_bucket,get_bucket,[BucketName]).
-spec reset_bucket(riak_client(), riak_object:bucket()) -> ok.
%% @doc Reset properties for this Bucket to the default values
reset_bucket(#riak_client{node = Node}, BucketName) ->
    rpc:call(Node,riak_core_bucket,reset_bucket,[BucketName]).
-spec reload_all(riak_client(), Module :: atom()) -> term().
%% @doc Force all Riak nodes to reload Module.
%%      This is used when loading new modules for map/reduce functionality.
reload_all(#riak_client{node = Node}, Module) ->
    rpc:call(Node,riak_core_util,reload_all,[Module]).

-spec remove_from_cluster(riak_client(), ExitingNode :: atom()) -> term().
%% @doc Cause all partitions owned by ExitingNode to be taken over
%%      by other nodes.
remove_from_cluster(#riak_client{node = Node}, ExitingNode) ->
    rpc:call(Node, riak_core_gossip, remove_from_cluster,[ExitingNode]).

get_stats(#riak_client{node = Node}, local) ->
    [{Node, rpc:call(Node, riak_kv_stat, get_stats, [])}];
get_stats(#riak_client{node = Node}, global) ->
    {ok, Ring} = rpc:call(Node, riak_core_ring_manager, get_my_ring, []),
    Nodes = riak_core_ring:all_members(Ring),
    [{N, rpc:call(N, riak_kv_stat, get_stats, [])} || N <- Nodes].

%% @doc Return the client id beign used for this client
get_client_id(#riak_client{client_id = ClientId}) ->
    ClientId.

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
wait_for_listkeys(ReqId) ->
    wait_for_listkeys(ReqId, []).
%% @private
wait_for_listkeys(ReqId, Acc) ->
    receive
        {ReqId, done} -> {ok, lists:flatten(Acc)};
        {ReqId, From, {keys, Res}} ->
            riak_kv_keys_fsm:ack_keys(From),
            wait_for_listkeys(ReqId, [Res|Acc]);
        {ReqId,{keys,Res}} -> wait_for_listkeys(ReqId, [Res|Acc]);
        {ReqId, {error, Error}} -> {error, Error}
    end.

%% @private
wait_for_listbuckets(ReqId) ->
    receive
        {ReqId,{buckets, Buckets}} -> 
            {ok, Buckets};
        {ReqId, Error} -> 
            {error, Error}
    end.

%% @private
wait_for_query_results(ReqId, Timeout) ->
    wait_for_query_results(ReqId, Timeout, []).
%% @private
wait_for_query_results(ReqId, Timeout, Acc) ->
    receive
        {ReqId, done} -> {ok, lists:flatten(lists:reverse(Acc))};
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
