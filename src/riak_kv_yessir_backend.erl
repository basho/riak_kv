%% -------------------------------------------------------------------
%%
%% riak_kv_yessir_backend: simulation backend for Riak
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

%% @doc riak_kv_yessir_backend is a backend for benchmarking Riak without
%%      any disk I/O or RAM constraints.
%%
%% Riak: "Store this key/value pair."
%% Backend: "Yes, sir!"
%% Riak: "Get me that key/value pair."
%% Backend: "Yes, sir!"
%%
%% This backend uses zero disk resources and uses constant memory.
%%
%% * All put requests are immediately acknowledged 'ok'.  No
%%   data about the put request is stored.
%% * All get requests are fulfilled by creating a constant binary for
%%   the value.  No attempt is made to correlate get keys with
%%   previously-put keys or to correlate get values with previously-put
%%   values.
%%   - Get operation keys that are formatted in with the convention
%%     <<"yessir.{integer}.anything">> will use integer (interpreted in
%%     base 10) as the returned binary's Size.
%%
%% This backend is the Riak storage manager equivalent of:
%%
%% * cat > /dev/null
%% * cat < /dev/zero
%%
%% TODO list:
%%
%% * Add configuration option for random percent of not_found replies for get
%%   - Anything non-zero would trigger read-repair, which could be useful
%%     for some simulations.
%% * Is there a need for simulations for get to return different vclocks?
%% * Add variable latency before responding.  This callback API is
%%   synchronous, but adding constant- & uniform- & pareto-distributed
%%   delays would simulate disk I/O latencies because all other backend
%%   APIs are also synchronous.

-module(riak_kv_yessir_backend).
-behavior(riak_kv_backend).

%% KV Backend API
-export([api_version/0,
         capabilities/1,
         capabilities/2,
         start/2,
         stop/1,
         get/3,
         put/5,
         delete/4,
         drop/1,
         fold_buckets/4,
         fold_keys/4,
         fold_objects/4,
         is_empty/1,
         status/1,
         callback/3]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(API_VERSION, 1).
-define(CAPABILITIES, [async_fold]).

-record(state, {
          default_get = <<>>,
          op_get = 0,
          op_put = 0,
          op_delete = 0
         }).
-type state() :: #state{}.
-type config() :: [{atom(), term()}].

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Return the major version of the
%% current API.
-spec api_version() -> {ok, integer()}.
api_version() ->
    {ok, ?API_VERSION}.

%% @doc Return the capabilities of the backend.
-spec capabilities(state()) -> {ok, [atom()]}.
capabilities(_) ->
    {ok, ?CAPABILITIES}.

%% @doc Return the capabilities of the backend.
-spec capabilities(riak_object:bucket(), state()) -> {ok, [atom()]}.
capabilities(_, _) ->
    {ok, ?CAPABILITIES}.

%% @doc Start this backend, yes, sir!
-spec start(integer(), config()) -> {ok, state()} | {error, term()}.
start(_Partition, Config) ->
    DefaultLen = case app_helper:get_prop_or_env(
                        default_size, Config, yessir_backend) of
                     undefined -> 1024;
                     N         -> N
                 end,
    {ok, #state{default_get = <<42:(DefaultLen*8)>>}}.

%% @doc Stop this backend, yes, sir!
-spec stop(state()) -> ok.
stop(_State) ->
    ok.

%% @doc Get a fake object, yes, sir!
-spec get(riak_object:bucket(), riak_object:key(), state()) ->
                 {ok, any(), state()}.
get(Bucket, Key, #state{op_get = Gets} = S) ->
    Bin = case get_binsize(Key) of
              undefined    -> S#state.default_get;
              N            -> <<42:(N*8)>>
          end,
    O = riak_object:increment_vclock(riak_object:new(Bucket, Key, Bin),
                                     <<"yessir!">>, 1),
    {ok, term_to_binary(O), S#state{op_get = Gets + 1}}.

%% @doc Store an object, yes, sir!
-type index_spec() :: {add, Index, SecondaryKey} | {remove, Index, SecondaryKey}.
-spec put(riak_object:bucket(), riak_object:key(), [index_spec()], binary(), state()) ->
                 {ok, state()}.
put(_Bucket, _PKey, _IndexSpecs, _Val, #state{op_put = Puts} = S) ->
    {ok, S#state{op_put = Puts + 1}}.

%% @doc Delete an object, yes, sir!
-spec delete(riak_object:bucket(), riak_object:key(), [index_spec()], state()) ->
                    {ok, state()}.
delete(_Bucket, _Key, _IndexSpecs, #state{op_delete = Deletes} = S) ->
    {ok, S#state{op_delete = Deletes + 1}}.

%% @doc Fold over all the buckets, yes, sir!
-spec fold_buckets(riak_kv_backend:fold_buckets_fun(),
                   any(),
                   [],
                   state()) -> {ok, any()}.
fold_buckets(_FoldBucketsFun, Acc, _Opts, _S) ->
    {ok, Acc}.

%% @doc Fold over all the keys for one or all buckets, yes, sir!
-spec fold_keys(riak_kv_backend:fold_keys_fun(),
                any(),
                [{atom(), term()}],
                state()) -> {ok, term()}.
fold_keys(_FoldKeysFun, Acc, _Opts, _S) ->
    {ok, Acc}.

%% @doc Fold over all the objects for one or all buckets, yes, sir!
-spec fold_objects(riak_kv_backend:fold_objects_fun(),
                   any(),
                   [{atom(), term()}],
                   state()) -> {ok, any()}.
fold_objects(_FoldObjectsFun, Acc, _Opts, _S) ->
    {ok, Acc}.

%% @doc Delete all objects from this backend, yes, sir!
-spec drop(state()) -> {ok, state()}.
drop(S) ->
    {ok, S}.

%% @doc Returns true if this bitcasks backend contains any
%% non-tombstone values; otherwise returns false.
-spec is_empty(state()) -> false.
is_empty(_S) ->
    false.

-spec status(state()) -> [{atom(), term()}].
status(#state{op_put = Puts, op_get = Gets, op_delete = Deletes}) ->
    [{puts, Puts}, {gets, Gets}, {deletes, Deletes}].

%% @doc Register an asynchronous callback
-spec callback(reference(), any(), state()) -> {ok, state()}.
callback(_Ref, _Whatever, S) ->
    {ok, S}.


%% ===================================================================
%% Internal functions
%% ===================================================================

get_binsize(<<"yessir.", Rest/binary>>) ->
    get_binsize(Rest, 0);
get_binsize(_) ->
    undefined.

get_binsize(<<X:8, Rest/binary>>, Val) when $0 =< X, X =< $9->
    get_binsize(Rest, (Val * 10) + (X - $0));
get_binsize(_, Val) ->
    Val.

%%
%% Test
%%
-ifdef(USE_BROKEN_TESTS).
-ifdef(TEST).
simple_test() ->
   Config = [],
   riak_kv_backend:standard_test(?MODULE, Config).

-ifdef(EQC).
eqc_test() ->
    Cleanup = fun(_State,_Olds) -> ok end,
    Config = [],
    ?assertEqual(true, backend_eqc:test(?MODULE, false, Config, Cleanup)).
-endif. % EQC
-endif. % TEST
-endif. % USE_BROKEN_TESTS
