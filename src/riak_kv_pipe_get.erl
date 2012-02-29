%% -------------------------------------------------------------------
%%
%% Copyright (c) 2011 Basho Technologies, Inc.
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

%% @doc Riak Pipe fitting that reads objects from Riak KV.  The
%% primary purpose of this fitting is to serve as the first half of a
%% 'map' MapReduce phase.
%%
%% This fitting accepts bucket/key pairs as inputs, which may be
%% represented as either a 2-tuple of `{Bucket, Key}' or a 2-element
%% list of `[Bucket, Key]' (`Bucket' and `Key' should each be a
%% binary).  An optional third argument, `KeyData' may be specified as
%% well, as `{{Bucket, Key}, KeyData}' or `[Bucket, Key, KeyData]'.
%% `KeyData' is an opaque term that will be passed with the object to
%% the next fitting.
%%
%% The fitting reads the object from the KV vnode hosting the same
%% partition number as the Pipe vnode owning this worker.  For this
%% reason, it is important to use a `chashfun' for this fitting that
%% gives the same answer as the consistent hashing function for the KV
%% object.
%%
%% If the object is found, the tuple `{ok, Object, Keydata}' is sent
%% as output.  If an error occurs looking up the object, and the
%% preflist has been exhausted, the tuple `{Error, {Bucket, Key},
%% KeyData}' is sent as output (where `Error' is usually `not_found').
%% The atom `undefined' is used as `KeyData' if none is specified.

-module(riak_kv_pipe_get).
-behaviour(riak_pipe_vnode_worker).

-export([init/2,
         process/3,
         done/1]).
-export([bkey/1,
         keydata/1,
         bkey_chash/1,
         bkey_nval/1]).

-include("riak_kv_vnode.hrl").
-include_lib("riak_pipe/include/riak_pipe.hrl").
-include_lib("riak_pipe/include/riak_pipe_log.hrl").

-export_type([input/0]).

-record(state, {partition, fd}).
-opaque state() :: #state{}.

-type input() :: {Bucket :: binary(), Key :: binary()}
               | {{Bucket :: binary(), Key :: binary()}, KeyData :: term()}
                     %% unfortunate type spec: this list should be
                     %% either 2 or three elements in length, exactly
                     %% like the tuples above
               | [BucketKeyKeyData :: term()].

%% @doc Stashes `Partition' and `FittingDetails' away for use while
%% processing inputs.
-spec init(riak_pipe_vnode:partition(), riak_pipe_fitting:details()) ->
         {ok, state()}.
init(Partition, FittingDetails) ->
    {ok, #state{partition=Partition, fd=FittingDetails}}.

%% @doc Lookup the bucket/key pair on the Riak KV vnode, and send it
%% downstream.
-spec process(riak_kv_mrc_pipe:key_input(), boolean(), state())
         -> {ok | forward_preflist | {error, term()}, state()}.
process(Input, Last, #state{partition=Partition, fd=FittingDetails}=State) ->
    ReqId = make_req_id(),
    riak_core_vnode_master:command(
      {Partition, node()}, %% assume local chashfun was used
      ?KV_GET_REQ{bkey=bkey(Input), req_id=ReqId},
      {raw, ReqId, self()},
      riak_kv_vnode_master),
    receive
        {ReqId, {r, {ok, Obj}, _, _}} ->
            case riak_pipe_vnode_worker:send_output(
                   {ok, Obj, keydata(Input)}, Partition, FittingDetails) of
                ok ->
                    {ok, State};
                ER ->
                    {ER, State}
            end;
        {ReqId, {r, {error, _} = Error, _, _}} ->
            if Last ->
                    case riak_pipe_vnode_worker:send_output(
                           {Error, bkey(Input), keydata(Input)},
                           Partition, FittingDetails) of
                        ok ->
                            {ok, State};
                        ER ->
                            {ER, State}
                    end;
               true ->
                    {forward_preflist, State}
            end
    end.

%% @doc Not used.
-spec done(state()) -> ok.
done(_State) ->
    ok.

make_req_id() ->
    erlang:phash2(erlang:now()). % stolen from riak_client

%% useful utilities

%% @doc Convert a valid pipe_get input into a standard bkey.
%%      Valid inputs are:
%%      - `{Bucket, Key}'
%%      - `{{Bucket, Key}, KeyData}'
%%      - `[Bucket, Key]'
%%      - `[Bucket, Key, KeyData]'
-spec bkey(input()) -> {Bucket :: binary(), Key :: binary()}.
bkey({{_,_}=Bkey,_}) -> Bkey;
bkey({_,_}=Bkey)     -> Bkey;
bkey([Bucket,Key])   -> {Bucket, Key};
bkey([Bucket,Key,_]) -> {Bucket, Key}.

%% @doc Extract KeyData from input.  The atom `undefined' is returned
%%      if no keydata is specified.
-spec keydata(input()) -> KeyData :: term().
keydata({{_,_},KeyData}) -> KeyData;
keydata({_,_})           -> undefined;
keydata([_,_])           -> undefined;
keydata([_,_,KeyData])   -> KeyData.

%% @doc Compute the KV hash of the input.
-spec bkey_chash(riak_kv_mrc_pipe:key_input()) -> chash:index().
bkey_chash(Input) ->
    riak_core_util:chash_key(bkey(Input)).

%% @doc Find the N value for the bucket of the input.
-spec bkey_nval(riak_kv_mrc_pipe:key_input()) -> integer().
bkey_nval(Input) ->
    {Bucket,_} = bkey(Input),
    BucketProps = riak_core_bucket:get_bucket(Bucket),
    {n_val, NVal} = lists:keyfind(n_val, 1, BucketProps),
    NVal.
