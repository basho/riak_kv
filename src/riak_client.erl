%% -------------------------------------------------------------------
%%
%% riak_client: object used for access into the riak system
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved.
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

-export([new/2]).
-export([get/3,get/4,get/5]).
-export([put/2,put/3,put/4,put/5,put/6]).
-export([delete/3,delete/4,delete/5]).
-export([delete_vclock/4,delete_vclock/5,delete_vclock/6]).
-export([list_keys/2,list_keys/3,list_keys/4]).
-export([stream_list_keys/2,stream_list_keys/3,stream_list_keys/4]).
-export([filter_buckets/2]).
-export([filter_keys/3,filter_keys/4]).
-export([list_buckets/1,list_buckets/2,list_buckets/3, list_buckets/4]).
-export([stream_list_buckets/1,stream_list_buckets/2,
         stream_list_buckets/3,stream_list_buckets/4, stream_list_buckets/5]).
-export([get_index/4,get_index/3]).
-export([aae_fold/2]).
-export([hotbackup/4]).
-export([stream_get_index/4,stream_get_index/3]).
-export([set_bucket/3,get_bucket/2,reset_bucket/2]).
-export([reload_all/2]).
-export([remove_from_cluster/2]).
-export([get_stats/2]).
-export([get_client_id/1]).
-export([for_dialyzer_only_ignore/3]).
-export([ensemble/1]).

-compile({no_auto_import,[put/2]}).
%% @type default_timeout() = 60000
-define(DEFAULT_TIMEOUT, 60000).
-define(DEFAULT_FOLD_TIMEOUT, 3600000).
-define(DEFAULT_ERRTOL, 0.00003).

%% TODO: This type needs to be better specified and validated against
%%       any dependents on riak_kv.
%%
%%       We want this term to be opaque, but can't because Dialyzer
%%       doesn't like the way it's specified.
%%
%%       opaque type riak_client() is underspecified and therefore meaningless
-type riak_client() :: term().

-export_type([riak_client/0]).

%% @spec new(Node, ClientId) -> riak_client()
%% @doc Return a riak client instance.
new(Node, ClientId) ->
    {?MODULE, [Node,ClientId]}.

%% @spec get(riak_object:bucket(), riak_object:key(), riak_client()) ->
%%       {ok, riak_object:riak_object()} |
%%       {error, notfound} |
%%       {error, timeout} |
%%       {error, {n_val_violation, N::integer()}} |
%%       {error, {r_val_unsatisfied, R::integer(), Replies::integer()}} |
%%       {error, Err :: term()}
%% @doc Fetch the object at Bucket/Key.  Return a value as soon as the default
%%      R-value for the nodes have responded with a value or error.
%% @equiv get(Bucket, Key, R, default_timeout())
get(Bucket, Key, {?MODULE, [_Node, _ClientId]}=THIS) ->
    get(Bucket, Key, [], THIS).

normal_get(Bucket, Key, Options, {?MODULE, [Node, _ClientId]}) ->
    Me = self(),
    ReqId = mk_reqid(),
    case node() of
        Node ->
            riak_kv_get_fsm:start({raw, ReqId, Me}, Bucket, Key, Options);
        _ ->
            %% Still using the deprecated `start_link' alias for `start' here, in
            %% case the remote node is pre-2.2:
            proc_lib:spawn_link(Node, riak_kv_get_fsm, start_link,
                                [{raw, ReqId, Me}, Bucket, Key, Options])
    end,
    %% TODO: Investigate adding a monitor here and eliminating the timeout.
    Timeout = recv_timeout(Options),
    wait_for_reqid(ReqId, Timeout).

consistent_get(Bucket, Key, Options, {?MODULE, [Node, _ClientId]}) ->
    BKey = {Bucket, Key},
    Ensemble = ensemble(BKey),
    Timeout = recv_timeout(Options),
    StartTS = os:timestamp(),
    Result = case riak_ensemble_client:kget(Node, Ensemble, BKey, Timeout) of
                 {error, _}=Err ->
                     Err;
                 {ok, Obj} ->
                     case riak_object:get_value(Obj) of
                         notfound ->
                             {error, notfound};
                         _ ->
                             {ok, Obj}
                     end
             end,
    maybe_update_consistent_stat(Node, consistent_get, Bucket, StartTS, Result),
    Result.

maybe_update_consistent_stat(Node, Stat, Bucket, StartTS, Result) ->
    case node() of
        Node ->
            Duration = timer:now_diff(os:timestamp(), StartTS),
            ObjFmt = riak_core_capability:get({riak_kv, object_format}, v0),
            ObjSize = case Result of
                          {ok, Obj} ->
                              riak_object:approximate_size(ObjFmt, Obj);
                          _ ->
                              undefined
                      end,
            ok = riak_kv_stat:update({Stat, Bucket, Duration, ObjSize});
        _ ->
            ok
    end.


%% @spec get(riak_object:bucket(), riak_object:key(), options(), riak_client()) ->
%%       {ok, riak_object:riak_object()} |
%%       {error, notfound} |
%%       {error, {deleted, vclock()}} |
%%       {error, timeout} |
%%       {error, {n_val_violation, N::integer()}} |
%%       {error, {r_val_unsatisfied, R::integer(), Replies::integer()}} |
%%       {error, Err :: term()}
%% @doc Fetch the object at Bucket/Key.  Return a value as soon as R-value for the nodes
%%      have responded with a value or error.
get(Bucket, Key, Options, {?MODULE, [Node, _ClientId]}=THIS) when is_list(Options) ->
    case consistent_object(Node, Bucket) of
        true ->
            consistent_get(Bucket, Key, Options, THIS);
        false ->
            normal_get(Bucket, Key, Options, THIS);
        {error,_}=Err ->
            Err
    end;

%% @spec get(riak_object:bucket(), riak_object:key(), R :: integer(), riak_client()) ->
%%       {ok, riak_object:riak_object()} |
%%       {error, notfound} |
%%       {error, timeout} |
%%       {error, {n_val_violation, N::integer()}} |
%%       {error, {r_val_unsatisfied, R::integer(), Replies::integer()}} |
%%       {error, Err :: term()}
%% @doc Fetch the object at Bucket/Key.  Return a value as soon as R
%%      nodes have responded with a value or error.
%% @equiv get(Bucket, Key, R, default_timeout())
get(Bucket, Key, R, {?MODULE, [_Node, _ClientId]}=THIS) ->
    get(Bucket, Key, [{r, R}], THIS).

%% @spec get(riak_object:bucket(), riak_object:key(), R :: integer(),
%%           TimeoutMillisecs :: integer(), riak_client()) ->
%%       {ok, riak_object:riak_object()} |
%%       {error, notfound} |
%%       {error, timeout} |
%%       {error, {n_val_violation, N::integer()}} |
%%       {error, {r_val_unsatisfied, R::integer(), Replies::integer()}} |
%%       {error, Err :: term()}
%% @doc Fetch the object at Bucket/Key.  Return a value as soon as R
%%      nodes have responded with a value or error, or TimeoutMillisecs passes.
get(Bucket, Key, R, Timeout, {?MODULE, [_Node, _ClientId]}=THIS) when
                                  (is_binary(Bucket) orelse is_tuple(Bucket)),
                                  is_binary(Key),
                                  (is_atom(R) or is_integer(R)),
                                  is_integer(Timeout) ->
    get(Bucket, Key, [{r, R}, {timeout, Timeout}], THIS).


%% @spec put(RObj :: riak_object:riak_object(), riak_client()) ->
%%        ok |
%%       {error, too_many_fails} |
%%       {error, timeout} |
%%       {error, {n_val_violation, N::integer()}}
%% @doc Store RObj in the cluster.
%%      Return as soon as the default W value number of nodes for this bucket
%%      nodes have received the request.
%% @equiv put(RObj, [])
put(RObj, {?MODULE, [_Node, _ClientId]}=THIS) -> put(RObj, [], THIS).


normal_put(RObj, Options, {?MODULE, [Node, ClientId]}) ->
    Me = self(),
    ReqId = mk_reqid(),
    case ClientId of
        undefined ->
            case node() of
                Node ->
                    riak_kv_put_fsm:start({raw, ReqId, Me}, RObj, Options);
                _ ->
                    %% Still using the deprecated `start_link' alias for `start'
                    %% here, in case the remote node is pre-2.2:
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
    wait_for_reqid(ReqId, Timeout).

consistent_put(RObj, Options, {?MODULE, [Node, _ClientId]}) ->
    Bucket = riak_object:bucket(RObj),
    BKey = {Bucket, riak_object:key(RObj)},
    Ensemble = ensemble(BKey),
    NewObj = riak_object:apply_updates(RObj),
    Timeout = recv_timeout(Options),
    StartTS = os:timestamp(),
    Result = case consistent_put_type(RObj, Options) of
                 update ->
                     riak_ensemble_client:kupdate(Node, Ensemble, BKey, RObj, NewObj, Timeout);
                 put_once ->
                     riak_ensemble_client:kput_once(Node, Ensemble, BKey, NewObj, Timeout)
                %% TODO: Expose client option to explicitly request overwrite
                 %overwrite ->
                     %riak_ensemble_client:kover(Node, Ensemble, BKey, NewObj, Timeout)
             end,
    maybe_update_consistent_stat(Node, consistent_put, Bucket, StartTS, Result),
    ReturnBody = lists:member(returnbody, Options),
    case Result of
        {error, _}=Error ->
            Error;
        {ok, Obj} when ReturnBody ->
            {ok, Obj};
        {ok, _Obj} ->
            ok
    end.

consistent_put_type(RObj, Options) ->
    VClockGiven = (riak_object:vclock(RObj) =/= []),
    IfMissing = lists:member({if_none_match, true}, Options),
    if VClockGiven ->
            update;
       IfMissing ->
            put_once;
       true ->
            %% Defaulting to put_once here for safety.
            %% Our client API makes it too easy to accidently send requests
            %% without a provided vector clock and clobber your data.
            %% overwrite
            %% TODO: Expose client option to explicitly request overwrite
            put_once
    end.

%% @spec put(RObj :: riak_object:riak_object(), riak_kv_put_fsm:options(), riak_client()) ->
%%       ok |
%%       {ok, details()} |
%%       {ok, riak_object:riak_object()} |
%%       {ok, riak_object:riak_object(), details()} |
%%       {error, notfound} |
%%       {error, timeout} |
%%       {error, {n_val_violation, N::integer()}} |
%%       {error, Err :: term()} |
%%       {error, Err :: term(), details()}
%% @doc Store RObj in the cluster.
put(RObj, Options, {?MODULE, [Node, _ClientId]}=THIS) when is_list(Options) ->
    case consistent_object(Node, riak_object:bucket(RObj)) of
        true ->
            consistent_put(RObj, Options, THIS);
        false ->
            maybe_normal_put(RObj, Options, THIS);
        {error,_}=Err ->
            Err
    end;

%% @spec put(RObj :: riak_object:riak_object(), W :: integer(), riak_client()) ->
%%        ok |
%%       {error, too_many_fails} |
%%       {error, timeout} |
%%       {error, {n_val_violation, N::integer()}}
%% @doc Store RObj in the cluster.
%%      Return as soon as at least W nodes have received the request.
%% @equiv put(RObj, [{w, W}, {dw, W}])
put(RObj, W, {?MODULE, [_Node, _ClientId]}=THIS) -> put(RObj, [{w, W}, {dw, W}], THIS).

%% @spec put(RObj::riak_object:riak_object(),W :: integer(),RW :: integer(), riak_client()) ->
%%        ok |
%%       {error, too_many_fails} |
%%       {error, timeout} |
%%       {error, {n_val_violation, N::integer()}}
%% @doc Store RObj in the cluster.
%%      Return as soon as at least W nodes have received the request, and
%%      at least DW nodes have stored it in their storage backend.
%% @equiv put(Robj, W, DW, default_timeout())
put(RObj, W, DW, {?MODULE, [_Node, _ClientId]}=THIS) -> put(RObj, [{w, W}, {dw, DW}], THIS).

%% @spec put(RObj::riak_object:riak_object(), W :: integer(), RW :: integer(),
%%           TimeoutMillisecs :: integer(), riak_client()) ->
%%        ok |
%%       {error, too_many_fails} |
%%       {error, timeout} |
%%       {error, {n_val_violation, N::integer()}}
%% @doc Store RObj in the cluster.
%%      Return as soon as at least W nodes have received the request, and
%%      at least DW nodes have stored it in their storage backend, or
%%      TimeoutMillisecs passes.
put(RObj, W, DW, Timeout, {?MODULE, [_Node, _ClientId]}=THIS) ->
    put(RObj,  [{w, W}, {dw, DW}, {timeout, Timeout}], THIS).

%% @spec put(RObj::riak_object:riak_object(), W :: integer(), RW :: integer(),
%%           TimeoutMillisecs :: integer(), Options::list(), riak_client()) ->
%%        ok |
%%       {error, too_many_fails} |
%%       {error, timeout} |
%%       {error, {n_val_violation, N::integer()}}
%% @doc Store RObj in the cluster.
%%      Return as soon as at least W nodes have received the request, and
%%      at least DW nodes have stored it in their storage backend, or
%%      TimeoutMillisecs passes.
put(RObj, W, DW, Timeout, Options, {?MODULE, [_Node, _ClientId]}=THIS) ->
    put(RObj, [{w, W}, {dw, DW}, {timeout, Timeout} | Options], THIS).

maybe_normal_put(RObj, Options, {?MODULE, [Node, _ClientId]}=THIS) when is_list(Options) ->
    case write_once(Node, riak_object:bucket(RObj)) of
        true ->
            write_once_put(Node, RObj, Options, THIS);
        false ->
            normal_put(RObj, Options, THIS);
        {error,_}=Err ->
            Err
    end.

write_once_put(Node, RObj, Options, {?MODULE, [_Node, _ClientId]}) when Node =:= node()->
    riak_kv_w1c_worker:put(RObj, Options);
write_once_put(Node, RObj, Options, {?MODULE, [_Node, _ClientId]}) ->
    rpc:call(Node, riak_kv_w1c_worker, put, [RObj, Options]).

%% @spec delete(riak_object:bucket(), riak_object:key(), riak_client()) ->
%%        ok |
%%       {error, too_many_fails} |
%%       {error, notfound} |
%%       {error, timeout} |
%%       {error, Err :: term()}
%% @doc Delete the object at Bucket/Key.  Return a value as soon as RW
%%      nodes have responded with a value or error.
%% @equiv delete(Bucket, Key, RW, default_timeout())
delete(Bucket,Key,{?MODULE, [_Node, _ClientId]}=THIS) -> delete(Bucket,Key,[],?DEFAULT_TIMEOUT,THIS).

%% @spec delete(riak_object:bucket(), riak_object:key(), RW :: integer(), riak_client()) ->
%%        ok |
%%       {error, too_many_fails} |
%%       {error, notfound} |
%%       {error, timeout} |
%%       {error, Err :: term()}
%% @doc Delete the object at Bucket/Key.  Return a value as soon as W/DW (or RW)
%%      nodes have responded with a value or error.
%% @equiv delete(Bucket, Key, RW, default_timeout())
delete(Bucket,Key,Options,{?MODULE, [_Node, _ClientId]}=THIS) when is_list(Options) ->
    delete(Bucket,Key,Options,?DEFAULT_TIMEOUT,THIS);
delete(Bucket,Key,RW,{?MODULE, [_Node, _ClientId]}=THIS) ->
    delete(Bucket,Key,[{rw, RW}],?DEFAULT_TIMEOUT,THIS).

%% @spec delete(riak_object:bucket(), riak_object:key(), RW :: integer(),
%%           TimeoutMillisecs :: integer(), riak_client()) ->
%%        ok |
%%       {error, too_many_fails} |
%%       {error, notfound} |
%%       {error, timeout} |
%%       {error, {n_val_violation, N::integer()}} |
%%       {error, Err :: term()}
%% @doc Delete the object at Bucket/Key.  Return a value as soon as W/DW (or RW)
%%      nodes have responded with a value or error, or TimeoutMillisecs passes.
delete(Bucket,Key,Options,Timeout,{?MODULE, [Node, _ClientId]}=THIS) when is_list(Options) ->
    case consistent_object(Node, Bucket) of
        true ->
            consistent_delete(Bucket, Key, Options, Timeout, THIS);
        false ->
            normal_delete(Bucket, Key, Options, Timeout, THIS);
        {error,_}=Err ->
            Err
    end;
delete(Bucket,Key,RW,Timeout,{?MODULE, [_Node, _ClientId]}=THIS) ->
    delete(Bucket,Key,[{rw, RW}], Timeout, THIS).

normal_delete(Bucket, Key, Options, Timeout, {?MODULE, [Node, ClientId]}) ->
    Me = self(),
    ReqId = mk_reqid(),
    riak_kv_delete_sup:start_delete(Node, [ReqId, Bucket, Key, Options, Timeout,
                                           Me, ClientId]),
    RTimeout = recv_timeout(Options),
    wait_for_reqid(ReqId, erlang:min(Timeout, RTimeout)).

consistent_delete(Bucket, Key, Options, _Timeout, {?MODULE, [Node, _ClientId]}) ->
    BKey = {Bucket, Key},
    Ensemble = ensemble(BKey),
    RTimeout = recv_timeout(Options),
    case riak_ensemble_client:kdelete(Node, Ensemble, BKey, RTimeout) of
        {error, _}=Err ->
            Err;
        {ok, Obj} when element(1, Obj) =:= r_object ->
            ok
    end.

%% @spec delete_vclock(riak_object:bucket(), riak_object:key(), vclock:vclock(), riak_client()) ->
%%        ok |
%%       {error, too_many_fails} |
%%       {error, notfound} |
%%       {error, timeout} |
%%       {error, Err :: term()}
%% @doc Delete the object at Bucket/Key.  Return a value as soon as W/DW (or RW)
%%      nodes have responded with a value or error.
%% @equiv delete(Bucket, Key, RW, default_timeout())
delete_vclock(Bucket,Key,VClock,{?MODULE, [_Node, _ClientId]}=THIS) ->
    delete_vclock(Bucket,Key,VClock,[{rw,default}],?DEFAULT_TIMEOUT,THIS).

%% @spec delete_vclock(riak_object:bucket(), riak_object:key(), vclock:vclock(),
%%                     RW :: integer(), riak_client()) ->
%%        ok |
%%       {error, too_many_fails} |
%%       {error, notfound} |
%%       {error, timeout} |
%%       {error, Err :: term()}
%% @doc Delete the object at Bucket/Key.  Return a value as soon as W/DW (or RW)
%%      nodes have responded with a value or error.
%% @equiv delete(Bucket, Key, RW, default_timeout())
delete_vclock(Bucket,Key,VClock,Options,{?MODULE, [_Node, _ClientId]}=THIS) when is_list(Options) ->
    delete_vclock(Bucket,Key,VClock,Options,?DEFAULT_TIMEOUT,THIS);
delete_vclock(Bucket,Key,VClock,RW,{?MODULE, [_Node, _ClientId]}=THIS) ->
    delete_vclock(Bucket,Key,VClock,[{rw, RW}],?DEFAULT_TIMEOUT,THIS).

%% @spec delete_vclock(riak_object:bucket(), riak_object:key(), vclock:vclock(), RW :: integer(),
%%           TimeoutMillisecs :: integer(), riak_client()) ->
%%        ok |
%%       {error, too_many_fails} |
%%       {error, notfound} |
%%       {error, timeout} |
%%       {error, {n_val_violation, N::integer()}} |
%%       {error, Err :: term()}
%% @doc Delete the object at Bucket/Key.  Return a value as soon as W/DW (or RW)
%%      nodes have responded with a value or error, or TimeoutMillisecs passes.
delete_vclock(Bucket,Key,VClock,Options,Timeout,{?MODULE, [Node, _ClientId]}=THIS) when is_list(Options) ->
    case consistent_object(Node, Bucket) of
        true ->
            consistent_delete_vclock(Bucket, Key, VClock, Options, Timeout, THIS);
        false ->
            normal_delete_vclock(Bucket, Key, VClock, Options, Timeout, THIS);
        {error,_}=Err ->
            Err
    end;
delete_vclock(Bucket,Key,VClock,RW,Timeout,{?MODULE, [_Node, _ClientId]}=THIS) ->
    delete_vclock(Bucket,Key,VClock,[{rw, RW}],Timeout,THIS).

normal_delete_vclock(Bucket, Key, VClock, Options, Timeout, {?MODULE, [Node, ClientId]}) ->
    Me = self(),
    ReqId = mk_reqid(),
    riak_kv_delete_sup:start_delete(Node, [ReqId, Bucket, Key, Options, Timeout,
                                           Me, ClientId, VClock]),
    RTimeout = recv_timeout(Options),
    wait_for_reqid(ReqId, erlang:min(Timeout, RTimeout)).

consistent_delete_vclock(Bucket, Key, VClock, Options, _Timeout, {?MODULE, [Node, _ClientId]}) ->
    BKey = {Bucket, Key},
    Ensemble = ensemble(BKey),
    Current = riak_object:set_vclock(riak_object:new(Bucket, Key, <<>>),
                                     VClock),
    RTimeout = recv_timeout(Options),
    case riak_ensemble_client:ksafe_delete(Node, Ensemble, BKey, Current, RTimeout) of
        {error, _}=Err ->
            Err;
        {ok, Obj} when element(1, Obj) =:= r_object ->
            ok
    end.

%% @spec list_keys(riak_object:bucket(), riak_client()) ->
%%       {ok, [Key :: riak_object:key()]} |
%%       {error, timeout} |
%%       {error, Err :: term()}
%% @doc List the keys known to be present in Bucket.
%%      Key lists are updated asynchronously, so this may be slightly
%%      out of date if called immediately after a put or delete.
%% @equiv list_keys(Bucket, default_timeout()*8)
list_keys(Bucket, {?MODULE, [_Node, _ClientId]}=THIS) ->
    list_keys(Bucket, ?DEFAULT_TIMEOUT*8, THIS).

%% @spec list_keys(riak_object:bucket(), TimeoutMillisecs :: integer(), riak_client()) ->
%%       {ok, [Key :: riak_object:key()]} |
%%       {error, timeout} |
%%       {error, Err :: term()}
%% @doc List the keys known to be present in Bucket.
%%      Key lists are updated asynchronously, so this may be slightly
%%      out of date if called immediately after a put or delete.
list_keys(Bucket, Timeout, {?MODULE, [_Node, _ClientId]}=THIS) ->
    list_keys(Bucket, none, Timeout, THIS).

%% @spec list_keys(riak_object:bucket(), Filter :: term(),
%% TimeoutMillisecs :: integer(), riak_client()) ->
%%       {ok, [Key :: riak_object:key()]} |
%%       {error, timeout} |
%%       {error, Err :: term()}
%% @doc List the keys known to be present in Bucket.
%%      Key lists are updated asynchronously, so this may be slightly
%%      out of date if called immediately after a put or delete.
list_keys(Bucket, Filter, Timeout0, {?MODULE, [Node, _ClientId]}) ->
    Timeout =
        case Timeout0 of
            T when is_integer(T) -> T;
            _ -> ?DEFAULT_TIMEOUT*8
        end,
    Me = self(),
    ReqId = mk_reqid(),
    riak_kv_keys_fsm_sup:start_keys_fsm(Node, [{raw, ReqId, Me}, [Bucket, Filter, Timeout]]),
    wait_for_listkeys(ReqId).

stream_list_keys(Bucket, {?MODULE, [_Node, _ClientId]}=THIS) ->
    stream_list_keys(Bucket, ?DEFAULT_TIMEOUT, THIS).

stream_list_keys(Bucket, undefined, {?MODULE, [_Node, _ClientId]}=THIS) ->
    stream_list_keys(Bucket, ?DEFAULT_TIMEOUT, THIS);
stream_list_keys(Bucket, Timeout, {?MODULE, [_Node, _ClientId]}=THIS) ->
    Me = self(),
    stream_list_keys(Bucket, Timeout, Me, THIS).

%% @spec stream_list_keys(riak_object:bucket(),
%%                        TimeoutMillisecs :: integer(),
%%                        Client :: pid(),
%%                        riak_client()) ->
%%       {ok, ReqId :: term()}
%% @doc List the keys known to be present in Bucket.
%%      Key lists are updated asynchronously, so this may be slightly
%%      out of date if called immediately after a put or delete.
%%      The list will not be returned directly, but will be sent
%%      to Client in a sequence of {ReqId, {keys,Keys}} messages
%%      and a final {ReqId, done} message.
%%      None of the Keys lists will be larger than the number of
%%      keys in Bucket on any single vnode.
stream_list_keys(Input, Timeout, Client, {?MODULE, [Node, _ClientId]}) when is_pid(Client) ->
    ReqId = mk_reqid(),
    case Input of
        %% buckets with bucket types are also a 2-tuple, so be careful not to
        %% treat the bucket type like a filter
        {Bucket, FilterInput} when not is_binary(FilterInput) ->
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

%% @spec filter_keys(riak_object:bucket(), Fun :: function(), riak_client()) ->
%%       {ok, [Key :: riak_object:key()]} |
%%       {error, timeout} |
%%       {error, Err :: term()}
%% @doc List the keys known to be present in Bucket,
%%      filtered at the vnode according to Fun, via lists:filter.
%%      Key lists are updated asynchronously, so this may be slightly
%%      out of date if called immediately after a put or delete.
%% @equiv filter_keys(Bucket, Fun, default_timeout())
filter_keys(Bucket, Fun, {?MODULE, [_Node, _ClientId]}=THIS) ->
    list_keys(Bucket, Fun, ?DEFAULT_TIMEOUT, THIS).

%% @spec filter_keys(riak_object:bucket(), Fun :: function(), TimeoutMillisecs :: integer(),
%%                   riak_client()) ->
%%       {ok, [Key :: riak_object:key()]} |
%%       {error, timeout} |
%%       {error, Err :: term()}
%% @doc List the keys known to be present in Bucket,
%%      filtered at the vnode according to Fun, via lists:filter.
%%      Key lists are updated asynchronously, so this may be slightly
%%      out of date if called immediately after a put or delete.
filter_keys(Bucket, Fun, Timeout, {?MODULE, [_Node, _ClientId]}=THIS) ->
            list_keys(Bucket, Fun, Timeout, THIS).

%% @spec list_buckets(riak_client()) ->
%%       {ok, [Bucket :: riak_object:bucket()]} |
%%       {error, timeout} |
%%       {error, Err :: term()}
%% @doc List buckets known to have keys.
%%      Key lists are updated asynchronously, so this may be slightly
%%      out of date if called immediately after any operation that
%%      either adds the first key or removes the last remaining key from
%%      a bucket.
%% @equiv list_buckets(default_timeout())
list_buckets({?MODULE, [_Node, _ClientId]}=THIS) ->
    list_buckets(none, ?DEFAULT_TIMEOUT, <<"default">>, THIS).

%% @spec list_buckets(timeout(), riak_client()) ->
%%       {ok, [Bucket :: riak_object:bucket()]} |
%%       {error, timeout} |
%%       {error, Err :: term()}
%% @doc List buckets known to have keys.
%%      Key lists are updated asynchronously, so this may be slightly
%%      out of date if called immediately after any operation that
%%      either adds the first key or removes the last remaining key from
%%      a bucket.
%% @equiv list_buckets(default_timeout())
list_buckets(undefined, {?MODULE, [_Node, _ClientId]}=THIS) ->
    list_buckets(none, ?DEFAULT_TIMEOUT*8, <<"default">>, THIS);
list_buckets(Timeout, {?MODULE, [_Node, _ClientId]}=THIS) ->
    list_buckets(none, Timeout, <<"default">>, THIS).

%% @spec list_buckets(TimeoutMillisecs :: integer(), Filter :: term(),
%% riak_client()) ->
%%       {ok, [Bucket :: riak_object:bucket()]} |
%%       {error, timeout} |
%%       {error, Err :: term()}
%% @doc List buckets known to have keys.
%%      Key lists are updated asynchronously, so this may be slightly
%%      out of date if called immediately after any operation that
%%      either adds the first key or removes the last remaining key from
%%      a bucket.
list_buckets(Filter, Timeout, {?MODULE, [_Node, _ClientId]}=THIS) ->
    list_buckets(Filter, Timeout, <<"default">>, THIS).

list_buckets(Filter, Timeout, Type, {?MODULE, [Node, _ClientId]}) ->
    Me = self(),
    ReqId = mk_reqid(),
    {ok, _Pid} = riak_kv_buckets_fsm_sup:start_buckets_fsm(Node,
                                                           [{raw, ReqId, Me},
                                                            [Filter, Timeout,
                                                             false, Type]]),
    wait_for_listbuckets(ReqId).

%% @spec filter_buckets(Fun :: function(), riak_client()) ->
%%       {ok, [Bucket :: riak_object:bucket()]} |
%%       {error, timeout} |
%%       {error, Err :: term()}
%% @doc Return a list of filtered buckets.
filter_buckets(Fun, {?MODULE, [_Node, _ClientId]}=THIS) ->
    list_buckets(Fun, ?DEFAULT_TIMEOUT, THIS).

stream_list_buckets({?MODULE, [_Node, _ClientId]}=THIS) ->
    stream_list_buckets(none, ?DEFAULT_TIMEOUT, THIS).

stream_list_buckets(undefined, {?MODULE, [_Node, _ClientId]}=THIS) ->
    stream_list_buckets(none, ?DEFAULT_TIMEOUT, THIS);
stream_list_buckets(Timeout, {?MODULE, [_Node, _ClientId]}=THIS)
  when is_integer(Timeout) ->
    stream_list_buckets(none, Timeout, THIS);
stream_list_buckets(Filter, {?MODULE, [_Node, _ClientId]}=THIS)
  when is_function(Filter) ->
    stream_list_buckets(Filter, ?DEFAULT_TIMEOUT, THIS).

stream_list_buckets(Filter, Timeout, {?MODULE, [_Node, _ClientId]}=THIS) ->
    Me = self(),
    stream_list_buckets(Filter, Timeout, Me, <<"default">>, THIS).

%% @spec stream_list_buckets(FilterFun :: fun(),
%%                           TimeoutMillisecs :: integer(),
%%                           Client :: pid(),
%%                           riak_client()) ->
%%       {ok, [Bucket :: riak_object:bucket()]} |
%%       {error, timeout} |
%%       {error, Err :: term()}
%% @doc List buckets known to have keys.
%%      Key lists are updated asynchronously, so this may be slightly
%%      out of date if called immediately after any operation that
%%      either adds the first key or removes the last remaining key from
%%      a bucket.
stream_list_buckets(Filter, Timeout, Client,
                    {?MODULE, [_Node, _ClientId]}=THIS) when is_pid(Client) ->
    stream_list_buckets(Filter, Timeout, Client, <<"default">>, THIS);
stream_list_buckets(Filter, Timeout, Type,
                    {?MODULE, [_Node, _ClientId]}=THIS) ->
    Me = self(),
    stream_list_buckets(Filter, Timeout, Me, Type, THIS).

stream_list_buckets(Filter, Timeout, Client, Type,
                    {?MODULE, [Node, _ClientId]}) ->
    ReqId = mk_reqid(),
    {ok, _Pid} = riak_kv_buckets_fsm_sup:start_buckets_fsm(Node,
                                                           [{raw, ReqId,
                                                             Client},
                                                            [Filter, Timeout,
                                                             true, Type]]),
    {ok, ReqId}.


%% @doc
%%
%% Run a cluster-wide AAE query - which can either access cached AAE
%% data across the cluster, or fold over ranges of the AAE store
%% (which in the case of Leveled can be the native AAE store.
-spec aae_fold(riak_kv_clusteraae_fsm:query_definition(), riak_client())
                    -> {ok, any()}|{error, timeout}|{error, Err :: term()}.
aae_fold(Query, {?MODULE, [Node, _ClientId]}) ->
    Me = self(),
    ReqId = mk_reqid(),
    TimeOut = ?DEFAULT_FOLD_TIMEOUT,
    riak_kv_clusteraae_fsm_sup:start_clusteraae_fsm(Node,
                                                    [{raw, ReqId, Me},
                                                    [Query, TimeOut]]),
    wait_for_fold_results(ReqId, TimeOut).

%% @doc
%% Run a hot backup - returns {ok, true} if successful
-spec hotbackup(string(), pos_integer(), pos_integer(), riak_client())
                                    -> {ok, boolean()}|{error, Err :: term()}.
hotbackup(BackupPath, DefaultNVal, PlanNVal, {?MODULE, [Node, _ClientId]}) ->
    Me = self(),
    ReqId = mk_reqid(),
    TimeOut = ?DEFAULT_FOLD_TIMEOUT,
    riak_kv_hotbackup_fsm_sup:start_hotbackup_fsm(Node,
                                                    [{raw, ReqId, Me},
                                                    [BackupPath,
                                                        {DefaultNVal, PlanNVal},
                                                        TimeOut]]),
    wait_for_fold_results(ReqId, TimeOut).


%% @spec get_index(Bucket :: binary(),
%%                 Query :: riak_index:query_def(),
%%                 riak_client()) ->
%%       {ok, [Key :: riak_object:key()]} |
%%       {error, timeout} |
%%       {error, Err :: term()}
%% @doc Run the provided index query.
get_index(Bucket, Query, {?MODULE, [_Node, _ClientId]}=THIS) ->
    get_index(Bucket, Query, [{timeout, ?DEFAULT_TIMEOUT}], THIS).

%% @spec get_index(Bucket :: binary(),
%%                 Query :: riak_index:query_def(),
%%                 TimeoutMillisecs :: integer(),
%%                 riak_client()) ->
%%       {ok, [Key :: riak_object:key()]} |
%%       {error, timeout} |
%%       {error, Err :: term()}
%% @doc Run the provided index query.
get_index(Bucket, Query, Opts, {?MODULE, [Node, _ClientId]}) ->
    Timeout = proplists:get_value(timeout, Opts, ?DEFAULT_TIMEOUT),
    MaxResults = proplists:get_value(max_results, Opts, all),
    PgSort = proplists:get_value(pagination_sort, Opts),
    Me = self(),
    ReqId = mk_reqid(),
    riak_kv_index_fsm_sup:start_index_fsm(Node, [{raw, ReqId, Me}, [Bucket, none, Query, Timeout, MaxResults, PgSort]]),
    wait_for_query_results(ReqId, Timeout).

%% @doc Run the provided index query, return a stream handle.
-spec stream_get_index(Bucket :: binary(), Query :: riak_index:query_def(),
                       riak_client()) ->
    {ok, ReqId :: term(), FSMPid :: pid()} | {error, Reason :: term()}.
stream_get_index(Bucket, Query, {?MODULE, [_Node, _ClientId]}=THIS) ->
    stream_get_index(Bucket, Query, [{timeout, ?DEFAULT_TIMEOUT}], THIS).

%% @doc Run the provided index query, return a stream handle.
-spec stream_get_index(Bucket :: binary(), Query :: riak_index:query_def(),
                       Opts :: proplists:proplist(), riak_client()) ->
    {ok, ReqId :: term(), FSMPid :: pid()} | {error, Reason :: term()}.
stream_get_index(Bucket, Query, Opts, {?MODULE, [Node, _ClientId]}) ->
    Timeout = proplists:get_value(timeout, Opts, ?DEFAULT_TIMEOUT),
    MaxResults = proplists:get_value(max_results, Opts, all),
    PgSort = proplists:get_value(pagination_sort, Opts),
    Me = self(),
    ReqId = mk_reqid(),
    case riak_kv_index_fsm_sup:start_index_fsm(Node,
                                               [{raw, ReqId, Me},
                                                [Bucket, none,
                                                 Query, Timeout,
                                                 MaxResults, PgSort]]) of
        {ok, Pid} ->
            {ok, ReqId, Pid};
        {error, Reason} ->
            {error, Reason}
    end.

%% @spec set_bucket(riak_object:bucket(), [BucketProp :: {atom(),term()}], riak_client()) -> ok
%% @doc Set the given properties for Bucket.
%%      This is generally best if done at application start time,
%%      to ensure expected per-bucket behavior.
%% See riak_core_bucket for expected useful properties.
set_bucket(BucketName,BucketProps,{?MODULE, [Node, _ClientId]}) ->
    rpc:call(Node,riak_core_bucket,set_bucket,[BucketName,BucketProps]).
%% @spec get_bucket(riak_object:bucket(), riak_client()) -> [BucketProp :: {atom(),term()}]
%% @doc Get all properties for Bucket.
%% See riak_core_bucket for expected useful properties.
get_bucket(BucketName, {?MODULE, [Node, _ClientId]}) ->
    rpc:call(Node,riak_core_bucket,get_bucket,[BucketName]).
%% @spec reset_bucket(riak_object:bucket(), riak_client()) -> ok
%% @doc Reset properties for this Bucket to the default values
reset_bucket(BucketName, {?MODULE, [Node, _ClientId]}) ->
    rpc:call(Node,riak_core_bucket,reset_bucket,[BucketName]).
%% @spec reload_all(Module :: atom(), riak_client()) -> term()
%% @doc Force all Riak nodes to reload Module.
%%      This is used when loading new modules for map/reduce functionality.
reload_all(Module, {?MODULE, [Node, _ClientId]}) -> rpc:call(Node,riak_core_util,reload_all,[Module]).

%% @spec remove_from_cluster(ExitingNode :: atom(), riak_client()) -> term()
%% @doc Cause all partitions owned by ExitingNode to be taken over
%%      by other nodes.
remove_from_cluster(ExitingNode, {?MODULE, [Node, _ClientId]}) ->
    rpc:call(Node, riak_core_gossip, remove_from_cluster,[ExitingNode]).

get_stats(local, {?MODULE, [Node, _ClientId]}) ->
    [{Node, rpc:call(Node, riak_kv_stat, get_stats, [])}];
get_stats(global, {?MODULE, [Node, _ClientId]}) ->
    {ok, Ring} = rpc:call(Node, riak_core_ring_manager, get_my_ring, []),
    Nodes = riak_core_ring:all_members(Ring),
    [{N, rpc:call(N, riak_kv_stat, get_stats, [])} || N <- Nodes].

%% @doc Return the client id being used for this client
get_client_id({?MODULE, [_Node, ClientId]}) ->
    ClientId.

%% @private
%% This function exists only to avoid compiler errors (unused type).
%% Unfortunately, I can't figure out how to suppress the bogus "Contract for
%% function that does not exist" warning from Dialyzer, so ignore that one.
-spec for_dialyzer_only_ignore(term(), term(), riak_client()) -> riak_client().
for_dialyzer_only_ignore(_X, _Y, {?MODULE, [_Node, _ClientId]}=THIS) ->
    THIS.

%% @private
mk_reqid() ->
    erlang:phash2({self(), os:timestamp()}). % only has to be unique per-pid

%% @private
wait_for_reqid(ReqId, Timeout) ->
    receive
        {ReqId, {error, overload}=Response} ->
            case app_helper:get_env(riak_kv, overload_backoff, undefined) of
                Msecs when is_number(Msecs) ->
                    timer:sleep(Msecs);
                undefined ->
                    ok
            end,
            Response;
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
            _ = riak_kv_keys_fsm:ack_keys(From),
            wait_for_listkeys(ReqId, [Res|Acc]);
        {ReqId,{keys,Res}} -> wait_for_listkeys(ReqId, [Res|Acc]);
        {ReqId, {error, Error}} ->
            {error, Error}
    end.

%% @private
wait_for_listbuckets(ReqId) ->
    receive
        {ReqId,{buckets, Buckets}} ->
            {ok, Buckets};
        {ReqId, {error, Error}} ->
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

%% @private
%% @doc
%% Only final result will be received, so do not expect a separate "done"
%% response.
wait_for_fold_results(ReqId, Timeout) ->
    receive
        {ReqId, {results, Results}} -> {ok, Results};
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

ensemble(BKey={Bucket, _Key}) ->
    {ok, CHBin} = riak_core_ring_manager:get_chash_bin(),
    DocIdx = riak_core_util:chash_key(BKey),
    Partition = chashbin:responsible_index(DocIdx, CHBin),
    N = riak_core_bucket:n_val(riak_core_bucket:get_bucket(Bucket)),
    {kv, Partition, N}.

consistent_object(Node, Bucket) when Node =:= node() ->
    riak_kv_util:consistent_object(Bucket);
consistent_object(Node, Bucket) ->
    case rpc:call(Node, riak_kv_util, consistent_object, [Bucket]) of
        {badrpc, {'EXIT', {undef, _}}} ->
            false;
        {badrpc, _}=Err ->
            {error, Err};
        Result ->
            Result
    end.

write_once(Node, Bucket) when Node =:= node() ->
    riak_kv_util:get_write_once(Bucket);
write_once(Node, Bucket) ->
    case rpc:call(Node, riak_kv_util, get_write_once, [Bucket]) of
        {badrpc, {'EXIT', {undef, _}}} ->
            false;
        {badrpc, _}=Err ->
            {error, Err};
        Result ->
            Result
    end.
