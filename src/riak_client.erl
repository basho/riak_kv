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

-export([mapred/2,mapred/3,mapred/4]).
-export([mapred_stream/2,mapred_stream/3,mapred_stream/4]).
-export([mapred_bucket/2,mapred_bucket/3,mapred_bucket/4]).
-export([mapred_bucket_stream/3,mapred_bucket_stream/4,mapred_bucket_stream/5,
         mapred_bucket_stream/6]).
-export([mapred_dynamic_inputs_stream/3]).
-export([get/2, get/3,get/4]).
-export([put/1, put/2,put/3,put/4,put/5]).
-export([delete/2,delete/3,delete/4]).
-export([delete_vclock/3,delete_vclock/4,delete_vclock/5]).
-export([list_keys/1,list_keys/2,list_keys/3]).
-export([stream_list_keys/1,stream_list_keys/2,stream_list_keys/3,
         stream_list_keys/4,stream_list_keys/5]).
-export([filter_keys/2,filter_keys/3]).
-export([list_buckets/0,list_buckets/1]).
-export([set_bucket/2,get_bucket/1]).
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

%% @spec mapred(Inputs :: riak_kv_mapred_term:mapred_inputs(),
%%              Query :: [riak_kv_mapred_query:mapred_queryterm()]) ->
%%       {ok, riak_kv_mapred_query:mapred_result()} |
%%       {error, {bad_qterm, riak_kv_mapred_query:mapred_queryterm()}} |
%%       {error, timeout} |
%%       {error, Err :: term()}
%% @doc Perform a map/reduce job across the cluster.
%%      See the map/reduce documentation for explanation of behavior.
%% @equiv mapred(Inputs, Query, default_timeout())
mapred(Inputs,Query) -> mapred(Inputs,Query,?DEFAULT_TIMEOUT).

%% @spec mapred(Inputs :: riak_kv_mapred_term:mapred_inputs(),
%%              Query :: [riak_kv_mapred_query:mapred_queryterm()],
%%              TimeoutMillisecs :: integer()  | 'infinity') ->
%%       {ok, riak_kv_mapred_query:mapred_result()} |
%%       {error, {bad_qterm, riak_kv_mapred_query:mapred_queryterm()}} |
%%       {error, timeout} |
%%       {error, Err :: term()}
%% @doc Perform a map/reduce job across the cluster.
%%      See the map/reduce documentation for explanation of behavior.
mapred(Inputs,Query,Timeout) ->
    mapred(Inputs,Query,undefined,Timeout).

%% @spec mapred(Inputs :: riak_kv_mapred_term:mapred_inputs(),
%%              Query :: [riak_kv_mapred_query:mapred_queryterm()],
%%              TimeoutMillisecs :: integer()  | 'infinity',
%%              ResultTransformer :: function()) ->
%%       {ok, riak_kv_mapred_query:mapred_result()} |
%%       {error, {bad_qterm, riak_kv_mapred_query:mapred_queryterm()}} |
%%       {error, timeout} |
%%       {error, Err :: term()}
%% @doc Perform a map/reduce job across the cluster.
%%      See the map/reduce documentation for explanation of behavior.
mapred(Inputs,Query,ResultTransformer,Timeout) when is_binary(Inputs) orelse
                                                    is_tuple(Inputs) ->
    case is_binary(Inputs) orelse is_key_filter(Inputs) of
        true ->
            mapred_bucket(Inputs, Query, ResultTransformer, Timeout);
        false ->
            Me = self(),
            case mapred_stream(Query,Me,ResultTransformer,Timeout) of
                {ok, {ReqId, FlowPid}} ->
                    mapred_dynamic_inputs_stream(FlowPid, Inputs, Timeout),
                    luke_flow:finish_inputs(FlowPid),
                    luke_flow:collect_output(ReqId, Timeout);
                Error ->
                    Error
            end
    end;
mapred(Inputs,Query,ResultTransformer,Timeout)
  when is_list(Query),
       (is_integer(Timeout) orelse Timeout =:= infinity) ->
    Me = self(),
    case mapred_stream(Query,Me,ResultTransformer,Timeout) of
        {ok, {ReqId, FlowPid}} ->
            case is_list(Inputs) of
                true ->
                    add_inputs(FlowPid, Inputs);
                false ->
                    mapred_dynamic_inputs_stream(FlowPid, Inputs, Timeout)
            end,
            luke_flow:finish_inputs(FlowPid),
            luke_flow:collect_output(ReqId, Timeout);
        Error ->
            Error
    end.

%% @spec mapred_stream(Query :: [riak_kv_mapred_query:mapred_queryterm()],
%%                     ClientPid :: pid()) ->
%%       {ok, {ReqId :: term(), MR_FSM_PID :: pid()}} |
%%       {error, {bad_qterm, riak_kv_mapred_query:mapred_queryterm()}} |
%%       {error, Err :: term()}
%% @doc Perform a streaming map/reduce job across the cluster.
%%      See the map/reduce documentation for explanation of behavior.
mapred_stream(Query,ClientPid) ->
    mapred_stream(Query,ClientPid,?DEFAULT_TIMEOUT).

%% @spec mapred_stream(Query :: [riak_kv_mapred_query:mapred_queryterm()],
%%                     ClientPid :: pid(),
%%                     TimeoutMillisecs :: integer() | 'infinity') ->
%%       {ok, {ReqId :: term(), MR_FSM_PID :: pid()}} |
%%       {error, {bad_qterm, riak_kv_mapred_query:mapred_queryterm()}} |
%%       {error, Err :: term()}
%% @doc Perform a streaming map/reduce job across the cluster.
%%      See the map/reduce documentation for explanation of behavior.
mapred_stream(Query, ClientPid, Timeout) ->
    mapred_stream(Query, ClientPid, undefined, Timeout).

%% @spec mapred_stream(Query :: [riak_kv_mapred_query:mapred_queryterm()],
%%                     ClientPid :: pid(),
%%                     TimeoutMillisecs :: integer() | 'infinity',
%%                     ResultTransformer :: function()) ->
%%       {ok, {ReqId :: term(), MR_FSM_PID :: pid()}} |
%%       {error, {bad_qterm, riak_kv_mapred_query:mapred_queryterm()}} |
%%       {error, Err :: term()}
%% @doc Perform a streaming map/reduce job across the cluster.
%%      See the map/reduce documentation for explanation of behavior.
mapred_stream(Query,ClientPid,ResultTransformer,Timeout)
  when is_list(Query), is_pid(ClientPid),
       (is_integer(Timeout) orelse Timeout =:= infinity) ->
    ReqId = mk_reqid(),
    case riak_kv_mapred_query:start(Node, ClientPid, ReqId, Query, ResultTransformer, Timeout) of
        {ok, Pid} ->
            {ok, {ReqId, Pid}};
        Error ->
            Error
    end.

mapred_bucket_stream(Bucket, Query, ClientPid) ->
    mapred_bucket_stream(Bucket, Query, ClientPid, ?DEFAULT_TIMEOUT).

mapred_bucket_stream(Bucket, Query, ClientPid, Timeout) ->
    mapred_bucket_stream(Bucket, Query, ClientPid, undefined, Timeout).

mapred_bucket_stream(Bucket, Query, ClientPid, ResultTransformer, Timeout) ->
    mapred_bucket_stream(Bucket, Query, ClientPid, ResultTransformer, Timeout,
                         ?DEFAULT_ERRTOL).

mapred_bucket_stream(Bucket, Query, ClientPid, ResultTransformer, Timeout, ErrorTolerance) ->
    {ok,{MR_ReqId,MR_FSM}} = mapred_stream(Query,ClientPid,ResultTransformer,Timeout),
    {ok,_Stream_ReqID} = stream_list_keys(Bucket, Timeout, ErrorTolerance,
                                  MR_FSM, mapred),
    {ok,MR_ReqId}.

mapred_bucket(Bucket, Query) ->
    mapred_bucket(Bucket, Query, ?DEFAULT_TIMEOUT).

mapred_bucket(Bucket, Query, Timeout) ->
    mapred_bucket(Bucket, Query, undefined, Timeout, ?DEFAULT_ERRTOL).

mapred_bucket(Bucket, Query, ResultTransformer, Timeout) ->
    mapred_bucket(Bucket, Query, ResultTransformer, Timeout, ?DEFAULT_ERRTOL).

mapred_bucket(Bucket, Query, ResultTransformer, Timeout, ErrorTolerance) ->
    Me = self(),
    {ok,MR_ReqId} = mapred_bucket_stream(Bucket, Query, Me,
                                         ResultTransformer, Timeout, ErrorTolerance),
    luke_flow:collect_output(MR_ReqId, Timeout).


-define(PRINT(Var), io:format("DEBUG: ~p:~p - ~p~n~n ~p~n~n", [?MODULE, ?LINE, ??Var, Var])).

%% An InputDef defines a Module and Function to call to generate
%% inputs for a map/reduce job. Should return {ok,
%% LukeReqID}. Ideally, we'd combine both the other input types (BKeys
%% and Bucket) into this approach, but postponing until after a code
%% review of Map/Reduce.
mapred_dynamic_inputs_stream(FSMPid, InputDef, Timeout) ->
    case InputDef of
        {modfun, Mod, Fun, Options} ->
            Mod:Fun(FSMPid, Options, Timeout);
        _ ->
            throw({invalid_inputdef, InputDef})
    end.

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
    riak_kv_get_fsm_sup:start_get_fsm(Node, [{raw, ReqId, Me}, Bucket, Key, Options]),
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
    UpdObj = riak_object:increment_vclock(RObj, ClientId),
    Me = self(),
    ReqId = mk_reqid(),
    riak_kv_put_fsm_sup:start_put_fsm(Node, [{raw, ReqId, Me}, UpdObj, Options]),
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
delete(Bucket,Key) -> delete(Bucket,Key,default,?DEFAULT_TIMEOUT).

%% @spec delete(riak_object:bucket(), riak_object:key(), RW :: integer()) ->
%%        ok |
%%       {error, too_many_fails} |
%%       {error, notfound} |
%%       {error, timeout} |
%%       {error, Err :: term()}
%% @doc Delete the object at Bucket/Key.  Return a value as soon as RW
%%      nodes have responded with a value or error.
%% @equiv delete(Bucket, Key, RW, default_timeout())
delete(Bucket,Key,RW) -> delete(Bucket,Key,RW,?DEFAULT_TIMEOUT).

%% @spec delete(riak_object:bucket(), riak_object:key(), RW :: integer(),
%%           TimeoutMillisecs :: integer()) ->
%%        ok |
%%       {error, too_many_fails} |
%%       {error, notfound} |
%%       {error, timeout} |
%%       {error, {n_val_violation, N::integer()}} |
%%       {error, Err :: term()}
%% @doc Delete the object at Bucket/Key.  Return a value as soon as RW
%%      nodes have responded with a value or error, or TimeoutMillisecs passes.
delete(Bucket,Key,RW,Timeout) ->
    Me = self(),
    ReqId = mk_reqid(),
    riak_kv_delete_sup:start_delete(Node, [ReqId, Bucket, Key, RW, Timeout,
                                           Me, ClientId]),
    wait_for_reqid(ReqId, Timeout).

%% @spec delete_vclock(riak_object:bucket(), riak_object:key(), vclock:vclock()) ->
%%        ok |
%%       {error, too_many_fails} |
%%       {error, notfound} |
%%       {error, timeout} |
%%       {error, Err :: term()}
%% @doc Delete the object at Bucket/Key.  Return a value as soon as RW
%%      nodes have responded with a value or error.
%% @equiv delete(Bucket, Key, RW, default_timeout())
delete_vclock(Bucket,Key,VClock) -> delete_vclock(Bucket,Key,VClock,default,?DEFAULT_TIMEOUT).

%% @spec delete_vclock(riak_object:bucket(), riak_object:key(), vclock::vclock(), RW :: integer()) ->
%%        ok |
%%       {error, too_many_fails} |
%%       {error, notfound} |
%%       {error, timeout} |
%%       {error, Err :: term()}
%% @doc Delete the object at Bucket/Key.  Return a value as soon as RW
%%      nodes have responded with a value or error.
%% @equiv delete(Bucket, Key, RW, default_timeout())
delete_vclock(Bucket,Key,VClock,RW) -> delete_vclock(Bucket,Key,VClock,RW,?DEFAULT_TIMEOUT).

%% @spec delete_vclock(riak_object:bucket(), riak_object:key(), vclock:vclock(), RW :: integer(),
%%           TimeoutMillisecs :: integer()) ->
%%        ok |
%%       {error, too_many_fails} |
%%       {error, notfound} |
%%       {error, timeout} |
%%       {error, {n_val_violation, N::integer()}} |
%%       {error, Err :: term()}
%% @doc Delete the object at Bucket/Key.  Return a value as soon as RW
%%      nodes have responded with a value or error, or TimeoutMillisecs passes.
delete_vclock(Bucket,Key,VClock,RW,Timeout) ->
    Me = self(),
    ReqId = mk_reqid(),
    riak_kv_delete_sup:start_delete(Node, [ReqId, Bucket, Key, RW, Timeout,
                                           Me, ClientId, VClock]),
    wait_for_reqid(ReqId, Timeout).


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
    list_keys(Bucket, Timeout, ?DEFAULT_ERRTOL).
list_keys(Bucket, Timeout, ErrorTolerance) ->
    Me = self(),
    ReqId = mk_reqid(),
    FSM_Timeout = trunc(Timeout / 8),
    riak_kv_keys_fsm_sup:start_keys_fsm(Node, [ReqId, Bucket, FSM_Timeout, plain, ErrorTolerance, Me]),
    wait_for_listkeys(ReqId, Timeout).

stream_list_keys(Bucket) ->
    stream_list_keys(Bucket, ?DEFAULT_TIMEOUT).

stream_list_keys(Bucket, Timeout) ->
    stream_list_keys(Bucket, Timeout, ?DEFAULT_ERRTOL).

stream_list_keys(Bucket, Timeout, ErrorTolerance) ->
    Me = self(),
    stream_list_keys(Bucket, Timeout, ErrorTolerance, Me).

stream_list_keys(Bucket, Timeout, ErrorTolerance, Client) ->
    stream_list_keys(Bucket, Timeout, ErrorTolerance, Client, plain).

%% @spec stream_list_keys(riak_object:bucket(),
%%                        TimeoutMillisecs :: integer(),
%%                        ErrorTolerance :: float(),
%%                        Client :: pid(),
%%                        ClientType :: atom()) ->
%%       {ok, ReqId :: term()}
%% @doc List the keys known to be present in Bucket.
%%      Key lists are updated asynchronously, so this may be slightly
%%      out of date if called immediately after a put or delete.
%%      The list will not be returned directly, but will be sent
%%      to Client in a sequence of {ReqId, {keys,Keys}} messages
%%      and a final {ReqId, done} message.
%%      None of the Keys lists will be larger than the number of
%%      keys in Bucket on any single vnode.
%%      If ClientType is set to 'mapred' instead of 'plain', then the
%%      messages will be sent in the form of a MR input stream.
stream_list_keys(Bucket0, Timeout, ErrorTolerance, Client, ClientType) ->
    ReqId = mk_reqid(),
    case build_filter(Bucket0) of
        {ok, Filter} ->
            riak_kv_keys_fsm_sup:start_keys_fsm(Node, [ReqId, Filter, Timeout, ClientType, ErrorTolerance, Client]),
            {ok, ReqId};
        Error ->
            Error
    end.

%% @spec filter_keys(riak_object:bucket(), Fun :: function()) ->
%%       {ok, [Key :: riak_object:key()]} |
%%       {error, timeout} |
%%       {error, Err :: term()}
%% @doc List the keys known to be present in Bucket,
%%      filtered at the vnode according to Fun, via lists:filter.
%%      Key lists are updated asynchronously, so this may be slightly
%%      out of date if called immediately after a put or delete.
%% @equiv filter_keys(Bucket, Fun, default_timeout()*8)
filter_keys(Bucket, Fun) ->
    list_keys({filter, Bucket, Fun}, ?DEFAULT_TIMEOUT*8).

%% @spec filter_keys(riak_object:bucket(), Fun :: function(), TimeoutMillisecs :: integer()) ->
%%       {ok, [Key :: riak_object:key()]} |
%%       {error, timeout} |
%%       {error, Err :: term()}
%% @doc List the keys known to be present in Bucket,
%%      filtered at the vnode according to Fun, via lists:filter.
%%      Key lists are updated asynchronously, so this may be slightly
%%      out of date if called immediately after a put or delete.
filter_keys(Bucket, Fun, Timeout) ->
    list_keys({filter, Bucket, Fun}, Timeout).

%% @spec list_buckets() ->
%%       {ok, [Bucket :: riak_object:bucket()]} |
%%       {error, timeout} |
%%       {error, Err :: term()}
%% @doc List buckets known to have keys.
%%      Key lists are updated asynchronously, so this may be slightly
%%      out of date if called immediately after any operation that
%%      either adds the first key or removes the last remaining key from
%%      a bucket.
%% @equiv list_buckets(default_timeout()*8)
list_buckets() ->
    list_buckets(?DEFAULT_TIMEOUT*8).

%% @spec list_buckets(TimeoutMillisecs :: integer()) ->
%%       {ok, [Bucket :: riak_object:bucket()]} |
%%       {error, timeout} |
%%       {error, Err :: term()}
%% @doc List buckets known to have keys.
%%      Key lists are updated asynchronously, so this may be slightly
%%      out of date if called immediately after any operation that
%%      either adds the first key or removes the last remaining key from
%%      a bucket.
list_buckets(Timeout) ->
    list_keys('_', Timeout).

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
mk_reqid() -> erlang:phash2(erlang:now()). % only has to be unique per-pid

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
        {ReqId,{keys,Res}} -> wait_for_listkeys(ReqId,Timeout,[Res|Acc]);
        {ReqId, Error} -> {error, Error}
    after Timeout ->
            {error, timeout, Acc}
    end.

add_inputs(_FlowPid, []) ->
    ok;
add_inputs(FlowPid, Inputs) when length(Inputs) < 100 ->
    luke_flow:add_inputs(FlowPid, Inputs);
 add_inputs(FlowPid, Inputs) ->
    {Current, Next} = lists:split(100, Inputs),
    luke_flow:add_inputs(FlowPid, Current),
    add_inputs(FlowPid, Next).

is_key_filter({Bucket, Filters}) when is_binary(Bucket),
                                      is_list(Filters) ->
    true;
is_key_filter(_) ->
    false.

build_filter({Bucket, Exprs}) ->
    case build_exprs(Exprs) of
        {ok, Filters} ->
            {ok, {Bucket, Filters}};
        Error ->
            Error
    end;
build_filter(Bucket) when is_binary(Bucket) ->
    {ok, {Bucket, []}}.

build_exprs(Exprs) ->
    build_exprs(Exprs, []).

build_exprs([], Accum) ->
    {ok, lists:reverse(Accum)};
build_exprs([[FunName|Args]|T], Accum) ->
    case riak_kv_mapred_filters:resolve_name(FunName) of
        error ->
            {error, {bad_filter, FunName}};
        Fun ->
            build_exprs(T, [{riak_kv_mapred_filters, Fun, Args}|Accum])
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
