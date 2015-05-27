%% -------------------------------------------------------------------
%%
%% riak_kv_yessir_backend: simulation backend for Riak
%%
%% Copyright (c) 2012-2013 Basho Technologies, Inc.  All Rights Reserved.
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
%%
%% Backend: "Yes, sir!"
%%
%% Riak: "Get me that key/value pair."
%%
%% Backend: "Yes, sir!"
%%
%% This backend uses zero disk resources and uses constant memory.
%%
%% All put requests are immediately acknowledged 'ok'.  No
%% data about the put request is stored.
%%
%% All get requests are fulfilled by creating a constant binary for
%% the value.  No attempt is made to correlate get keys with
%% previously-put keys or to correlate get values with previously-put
%% values.
%%
%% Get operation keys that are formatted in with the convention
%% `<<"yessir.{integer}.anything">>' will use integer (interpreted in
%% base 10) as the returned binary's Size.
%%
%% fold_keys and fold_objects are implemented for both sync and async.
%% Each will return the same deterministic set of results for every call,
%% given the same set of configuration parameters. The size of the object
%% folded over is controlled by the "default_size" config var. The number
%% of keys folded over is controlled by the "key_count" config var. Folding
%% over the keys and objects will each return the same set of keys, so if
%% you fold over the keys and collect the list; and then you fold over the
%% objects and collect the list of keys again, the two lists will match.
%%
%% This backend is the Riak storage manager equivalent of:
%%
%% <ul>
%% <li>`cat > /dev/null'</li>
%% <li>`cat < /dev/zero'</li>
%% </ul>
%%
%% === Configuration Options ===
%%
%% The following configuration options are available for the yessir backend.
%% The options should be specified in the `riak_kv' section of your
%% app.config file.
%%
%% <ul>
%% <li>`yessir_return_same_r_obj' - Fastest and dumbest mode.  Return
%%                                  exactly the same object, regardless of
%%                                  the BKey asked for.  The returned object's
%%                                  BKey is constant, so any part of Riak that
%%                                  cares about matching/valid BKey inside of
%%                                  the object will be confused and/or break.</li>
%% <li>`yessir_aae_mode_encoding' - Specify which mode of behavior to
%%                                  use when interacting with Riak KV's
%%                                  anti-entropy mode for put and put_object
%%                                  calls.
%%   <ul>
%%   <li>`constant_binary' - The default mode: lie to AAE by returning
%%                           a constant binary, `<<>>'. This will cause
%%                           AAE to maintain a tiny tree of order-size(1).
%%                           This is the fastest mode but also causes the
%%                           least amount of AAE work and thus may or may
%%                           not meet all users' needs. </li>
%%   <li>`bkey' - Return term_to_binary({Bucket, Key}), which will
%%                      cause AAE's tree to churn with order-size(NumKeys)
%%                      but will be much smaller than the entire serialized
%%                      #r_object{}, so AAE will spend less CPU time during
%%                      its processing. </li>
%%   <li>`r_object' - Serialize the entire #r_object{}, like Riak KV
%%                          does in normal operation.  This mode has the
%%                          highest overhead per operation. </li>
%%   </ul></li>
%% <li>`yessir_default_size' - The number of bytes of generated data for the value.</li>
%% <li>`yessir_key_count'    - The number of keys that will be folded over, e.g. list_keys().</li>
%% <li>`yessir_bucket_prefix_list'  - A list {BucketPrefixBin, {Module, Fun}}
%%                              tuples, where Module:Fun/3 is called if a
%%                              Bucket's name matches BucketPrefixBin.</li>
%% </ul>
%%
%% TODO list:
%%
%% <ul>
%%   <li>Add configuration option for random percent of not_found
%%   replies for get. Anything non-zero would trigger read-repair,
%%   which could be useful for some simulations.</li>
%%   <li>Is there a need for simulations for get to return different
%%   vclocks?</li>
%%   <li>Add variable latency before responding.  This callback API is
%%   synchronous, but adding constant- and uniform- and
%%   pareto-distributed delays would simulate disk `I/O' latencies
%%   because all other backend APIs are also synchronous.</li>
%% </ul>

-module(riak_kv_yessir_backend).
-behavior(riak_kv_backend).

%% KV Backend API
-export([api_version/0,
         capabilities/1,
         capabilities/2,
         start/2,
         stop/1,
         get/3,                                 % Old Riak KV API
         get_object/4,                          % New Riak KV API
         put/5,
         put_object/5,
         delete/4,
         drop/1,
         fold_buckets/4,
         fold_keys/4,
         fold_objects/4,
         is_empty/1,
         status/1,
         callback/3]).
-export([make_riak_safe_obj/3, make_riak_safe_obj/4]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(API_VERSION, 1).
-define(CAPABILITIES, [uses_r_object, async_fold]).

-record(state, {
          aae_mode :: atom(),
          constant_r_object,
          same_r_object,
          same_r_object_bin,
          default_get = <<>>,
          default_size = 0,
          key_count = 0,
          bprefix_list = [],
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
    AAE_Mode = case app_helper:get_prop_or_env(
                      yessir_aae_mode_encoding, Config, yessir_backend) of
                   undefined           -> constant_binary;
                   constant_binary = X -> X;
                   bkey = X            -> X;
                   r_object = X        -> X
               end,
    DefaultLen = case app_helper:get_prop_or_env(
                        yessir_default_size, Config, yessir_backend) of
                     undefined -> 1024;
                     Len       -> Len
                 end,
    KeyCount = case riak_kv_util:get_backend_config(
                      yessir_key_count, Config, yessir_backend) of
                   undefined -> 1024;
                   Count     -> Count
               end,
    BPrefixList = case app_helper:get_prop_or_env(
                        yessir_bucket_prefix_list, Config, yessir_backend) of
                     undefined -> [];
                     BPL       -> BPL
                 end,
    DefaultValue = <<42:(DefaultLen*8)>>,
    {SameRObj, SameRObjBin} =
        case app_helper:get_prop_or_env(
               yessir_return_same_r_obj, Config, yessir_backend) of
            true ->
                RObj = make_riak_safe_obj(<<>>, <<>>, DefaultValue),
                {RObj, riak_object:to_binary(v0, RObj)};
            _ ->
                {undefined, undefined}
        end,
    {ok, #state{aae_mode = AAE_Mode,
                constant_r_object = riak_object:new(<<>>, <<>>, <<>>),
                same_r_object = SameRObj,
                same_r_object_bin = SameRObjBin,
                default_get = DefaultValue,
                default_size = DefaultLen,
                key_count = KeyCount,
                bprefix_list = BPrefixList}}.

%% @doc Stop this backend, yes, sir!
-spec stop(state()) -> ok.
stop(_State) ->
    ok.

%% @doc Get a fake object, yes, sir!
-spec get(riak_object:bucket(), riak_object:key(), state()) ->
                 {ok, any(), state()}.
get(_Bucket, _Key, #state{same_r_object_bin=RObjBin})
  when RObjBin /= undefined ->
    RObjBin;
get(Bucket, Key, S) ->
    RObj = make_get_object(Bucket, Key, S),
    make_get_return_val(RObj, true, S).

get_object(Bucket, Key, WantsBinary, S) ->
    get_object_bprefix(S#state.bprefix_list, Bucket, Key, WantsBinary, S).

get_object_bprefix([], Bucket, Key, WantsBinary, S) ->
    get_object_default(Bucket, Key, WantsBinary, S);
get_object_bprefix([{P, {Mod, Fun}}|Ps], Bucket, Key, WantsBinary, S) ->
    P_len = byte_size(P),
    case Bucket of
        <<P:P_len/binary, Rest/binary>> ->
            make_get_return_val(Mod:Fun(Rest, Bucket, Key), WantsBinary, S);
        _ ->
            get_object_bprefix(Ps, Bucket, Key, WantsBinary, S)
    end.

get_object_default(Bucket, Key, WantsBinary, S) ->
    make_get_return_val(make_get_object(Bucket, Key, S), WantsBinary, S).

make_get_return_val(Error, _WantsBinary, #state{op_get = Gets} = S)
  when Error == not_found; Error == bad_crc ->
    {error, Error, S#state{op_get = Gets + 1}};
make_get_return_val(RObj, true = _WantsBinary, #state{op_get = Gets} = S) ->
    {ok, riak_object:to_binary(v0, RObj), S#state{op_get = Gets + 1}};
make_get_return_val(RObj, false = _WantsBinary, #state{op_get = Gets} = S) ->
    {ok, RObj, S#state{op_get = Gets + 1}}.

%% @doc Store an object, yes, sir!
-type index_spec() :: {add, Index, SecondaryKey} | {remove, Index, SecondaryKey}.
-spec put(riak_object:bucket(), riak_object:key(), [index_spec()], binary(), state()) ->
                 {ok, state()}.
put(_Bucket, _PKey, _IndexSpecs, _Val, #state{op_put = Puts} = S) ->
    {ok, S#state{op_put = Puts + 1}}.

put_object(Bucket, PKey, _IndexSpecs, RObj, #state{op_put = Puts} = S) ->
    EncodedVal = case S#state.aae_mode of
                     constant_binary ->
                         S#state.constant_r_object;
                     bkey ->
                         term_to_binary(riak_object:new(Bucket, PKey, <<>>));
                     r_object ->
                         ObjFmt = riak_core_capability:get(
                                    {riak_kv, object_format}, v0),
                         riak_object:to_binary(ObjFmt, RObj)
                 end,
    {{ok, S#state{op_put = Puts + 1}}, EncodedVal}.

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
fold_keys(FoldKeysFun, Accum, Opts, State) ->
    KeyCount = State#state.key_count,
    BucketOpt = lists:keyfind(bucket, 1, Opts),
    Folder = case BucketOpt of
                 {bucket, Bucket} ->
                     FoldFun = fold_keys_fun(FoldKeysFun, Bucket),
                     get_folder(FoldFun, Accum, KeyCount);
                 _ ->
                     FoldFun = fold_keys_fun(FoldKeysFun, <<"all">>),
                     get_folder(FoldFun, Accum, KeyCount)
             end,
    case lists:member(async_fold, Opts) of
        true ->
            {async, Folder};
        false ->
            {ok, Folder()}
    end.

%% @doc Fold over all the objects for one or all buckets, yes, sir!
-spec fold_objects(riak_kv_backend:fold_objects_fun(),
                   any(),
                   [{atom(), term()}],
                   state()) -> {ok, any()} | {async, fun()}.
fold_objects(FoldObjectsFun, Accum, Opts, State) ->
    KeyCount = State#state.key_count,
    ValueSize = State#state.default_size,
    BucketOpt = lists:keyfind(bucket, 1, Opts),
    Folder = case BucketOpt of
                 {bucket, Bucket} ->
                     FoldFun = fold_objects_fun(FoldObjectsFun, Bucket, ValueSize),
                     get_folder(FoldFun, Accum, KeyCount);
                 _ ->
                     FoldFun = fold_objects_fun(FoldObjectsFun, <<"all">>, ValueSize),
                     get_folder(FoldFun, Accum, KeyCount)
             end,
    case lists:member(async_fold, Opts) of
        true ->
            {async, Folder};
        false ->
            {ok, Folder()}
    end.

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

get_folder(FoldFun, Acc, KeyCount) ->
    fun() ->
            fold_anything_fun(FoldFun, Acc, KeyCount)
    end.

key_of_integer(Range, State) ->
    {N, S} = random:uniform_s(Range, State),
    Key = integer_to_list(N) ++ ".1000", %% e.g. "10.1000"
    BKey = list_to_binary(Key),          %% e.g. <<"10.1000">>
    {BKey, S}.

value_for_random(VR, Size) ->
    <<VR:(Size*8)>>.

fold_anything_fun(FoldFunc, Acc, KeyCount) ->
    Range = 1000000,
    KeyState = random:seed0(),
    ValueState = random:seed0(),
    all_keys_folder(FoldFunc, Acc, Range, {KeyState, ValueState}, KeyCount).

all_keys_folder(FoldFunc, Acc, _Range, _S, 0) ->
    FoldFunc(undefined, 0, Acc);
all_keys_folder(FoldFunc, Acc, Range, {KS,VS}, N) ->
    {Key,KSS} = key_of_integer(Range, KS),
    {VR,VSS} = random:uniform_s(255,VS),
    Acc1 = FoldFunc(Key, VR, Acc),
    all_keys_folder(FoldFunc, Acc1, Range, {KSS,VSS}, N-1).

%% @private
%% Return a function to fold over keys on this backend
fold_keys_fun(FoldKeysFun, Bucket) ->
    fun(Key, _VR, Acc) when Key /= undefined ->
            FoldKeysFun(Bucket, Key, Acc);
       (_, _, Acc) ->
            Acc
    end.

%% @private
%% Return a function to fold over keys on this backend
fold_objects_fun(FoldObjectsFun, Bucket, Size) ->
    fun(Key, VR, Acc) when Key /= undefined ->
            Bin = value_for_random(VR, Size),
            O = make_riak_safe_obj(Bucket, Key, Bin),
            FoldObjectsFun(Bucket, Key, riak_object:to_binary(v0, O), Acc);
       (_, _, Acc) ->
            Acc
    end.

get_binsize(<<"yessir.", Rest/binary>>) ->
    get_binsize(Rest, 0);
get_binsize(_) ->
    undefined.

get_binsize(<<X:8, Rest/binary>>, Val) when $0 =< X, X =< $9->
    get_binsize(Rest, (Val * 10) + (X - $0));
get_binsize(_, Val) ->
    Val.

make_get_object(_Bucket, _Key, #state{same_r_object=RObj})
  when RObj /= undefined ->
    RObj;
make_get_object(Bucket, Key, S) ->
    Bin = case get_binsize(Key) of
              undefined    -> S#state.default_get;
              N            -> <<42:(N*8)>>
          end,
    make_riak_safe_obj(Bucket, Key, Bin).

make_riak_safe_obj(Bucket, Key, Bin) when is_binary(Bin) ->
    make_riak_safe_obj(Bucket, Key, Bin, []).

make_riak_safe_obj(Bucket, Key, Bin, Metas)
  when is_binary(Bin), is_list(Metas) ->
    Meta = dict:from_list(
             [{<<"X-Riak-Meta">>, Metas},
              {<<"X-Riak-Last-Modified">>, {44, 45, 46}},
              {<<"X-Riak-VTag">>, riak_kv_util:make_vtag({42,42,42})}]),
    RObj = riak_object:new(Bucket, Key, Bin, Meta),
    riak_object:increment_vclock(RObj, <<"yessir!">>, 1).

%%
%% Test
%%

-ifdef(USE_BROKEN_TESTS).

t0() ->
    B0 = <<"foo">>,
    {ok, S0} = start(0, [{yessir_aae_mode_encoding, constant_binary},
                         {yessir_default_size, 2},
                         {yessir_key_count, 5},
                         {yessir_bucket_prefix_list,
                          [{B0, {?MODULE, t0_number1}}]
                         }]),
    get_object(B0, <<"key">>, true, S0).

t0_number1(_BucketSuffix, Bucket, Key) ->
    riak_object:new(Bucket, Key, <<"HEYHEY!">>).

-ifdef(TEST).
simple_test() ->
   Config = [],
   backend_test_util:standard_test(?MODULE, Config).

-ifdef(EQC).
eqc_test() ->
    Cleanup = fun(_State,_Olds) -> ok end,
    Config = [],
    ?assertEqual(true, backend_eqc:test(?MODULE, false, Config, Cleanup)).
-endif. % EQC
-endif. % TEST
-endif. % USE_BROKEN_TESTS
