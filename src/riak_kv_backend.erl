%% -------------------------------------------------------------------
%%
%% riak_kv_backend: Riak backend behaviour
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

-module(riak_kv_backend).

-export([behaviour_info/1]).
-export([callback_after/3]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-compile(export_all).
-export([standard_test/2]).
-endif.

-type fold_buckets_fun() :: fun((binary(), any()) -> any() | no_return()).
-type fold_keys_fun() :: fun((binary(), binary(), any()) -> any() |
                                                            no_return()).
-type fold_objects_fun() :: fun((binary(), binary(), term(), any()) ->
                                       any() |
                                       no_return()).
-export_type([fold_buckets_fun/0,
              fold_keys_fun/0,
              fold_objects_fun/0]).

-spec behaviour_info(atom()) -> 'undefined' | [{atom(), arity()}].
behaviour_info(callbacks) ->
    [
     {api_version,0},
     {capabilities, 1},  % (State)
     {capabilities, 2},  % (Bucket, State)
     {start,2},          % (Partition, Config)
     {stop,1},           % (State)
     {get,3},            % (Bucket, Key, State)
     {put,5},            % (Bucket, Key, IndexSpecs, Val, State)
     {delete,4},         % (Bucket, Key, IndexSpecs, State)
     {drop,1},           % (State)
     {fold_buckets,4},   % (FoldBucketsFun, Acc, Opts, State),
                         %   FoldBucketsFun(Bucket, Acc)
     {fold_keys,4},      % (FoldKeysFun, Acc, Opts, State),
                         %   FoldKeysFun(Bucket, Key, Acc)
     {fold_objects,4},   % (FoldObjectsFun, Acc, Opts, State),
                         %   FoldObjectsFun(Bucket, Key, Object, Acc)
     {is_empty,1},       % (State)
     {status,1},         % (State)
     {callback,3}];      % (Ref, Msg, State) ->
behaviour_info(_Other) ->
    undefined.

%% Queue a callback for the backend after Time ms.
-spec callback_after(integer(), reference(), term()) -> reference().
callback_after(Time, Ref, Msg) when is_integer(Time), is_reference(Ref) ->
    riak_core_vnode:send_command_after(Time, {backend_callback, Ref, Msg}).

-ifdef(TEST).

standard_test(BackendMod, Config) ->
    {"Basic Backend",
     fun() ->
             {Mod, State} = setup({BackendMod, Config}),
             State2 = basic_store_and_fetch(Mod, State),
             cleanup({Mod, State2})
     end}.

make_test_bucket(Backend, Suffix) ->
    try
        Backend:backend_eunit_bucket(Suffix)
    catch error:undef ->
            <<"b", ($0 + Suffix):8>>
    end.

make_test_key(Backend, Suffix) ->
    try
        Backend:backend_eunit_key(Suffix)
    catch error:undef ->
            <<"k", ($0 + Suffix):8>>
    end.

make_bs_and_ks(Backend) ->
    {make_test_bucket(Backend, 1),
     make_test_bucket(Backend, 2),
     make_test_key(Backend, 1),
     make_test_key(Backend, 2)}.

basic_store_and_fetch(Backend, State) ->
    {B1, B2, K1, K2} = make_bs_and_ks(Backend),
    {ok, State2} = Backend:put(B1, K1, [], <<"v1">>, State),
    {ok, State3} = Backend:put(B2, K2, [], <<"v2">>, State2),
    {ok,<<"v2">>, State4} = Backend:get(B2, K2, State3),
    {error, not_found, State5} = Backend:get(B1, <<"k3">>, State4),
    fold_buckets(Backend, State5).

fold_buckets(Backend, State) ->
    {B1, B2, _K1, _K2} = make_bs_and_ks(Backend),
    FoldBucketsFun =
        fun(Bucket, Acc) ->
                [Bucket | Acc]
        end,

    ?assertEqual([B1, B2],
                 begin
                     {ok, Buckets1} =
                         Backend:fold_buckets(FoldBucketsFun,
                                              [],
                                              [],
                                              State),
                     lists:sort(Buckets1)
                 end),
    fold_keys(Backend, State).

fold_keys(Backend, State) ->
    {B1, B2, K1, K2} = make_bs_and_ks(Backend),

    FoldKeysFun =
        fun(Bucket, Key, Acc) ->
                [{Bucket, Key} | Acc]
        end,
    FoldKeysFun1 =
        fun(_Bucket, Key, Acc) ->
                [Key | Acc]
        end,
    FoldKeysFun2 =
        fun(Bucket, Key, Acc) ->
                case Bucket =:= B1 of
                    true ->
                        [Key | Acc];
                    false ->
                        Acc
                end
        end,
    FoldKeysFun3 =
        fun(Bucket, Key, Acc) ->
                case Bucket =:= B1 of
                    true ->
                        Acc;
                    false ->
                        [Key | Acc]
                end
        end,

    ?assertEqual([{B1, K1}, {B2, K2}],
                 begin
                     {ok, Keys1} =
                         Backend:fold_keys(FoldKeysFun,
                                           [],
                                           [],
                                           State),
                     lists:sort(Keys1)
                 end),
    ?assertEqual({ok, [K1]},
                 Backend:fold_keys(FoldKeysFun1,
                                   [],
                                   [{bucket, B1}],
                                   State)),
    ?assertEqual({ok, [K2]},
                 Backend:fold_keys(FoldKeysFun1,
                                   [],
                                   [{bucket, B2}],
                                   State)),
    ?assertEqual({ok, [K1]},
                 Backend:fold_keys(FoldKeysFun2, [], [], State)),
    ?assertEqual({ok, [K1]},
                 Backend:fold_keys(FoldKeysFun2,
                                   [],
                                   [{bucket, B1}],
                                   State)),
    ?assertEqual({ok, [K2]},
                 Backend:fold_keys(FoldKeysFun3, [], [], State)),
    ?assertEqual({ok, []},
                 Backend:fold_keys(FoldKeysFun3,
                                   [],
                                   [{bucket, B1}],
                                   State)),
        delete_object(Backend, State).

delete_object(Backend, State) ->
    {_B1, B2, _K1, K2} = make_bs_and_ks(Backend),
    {ok, State2} =  Backend:delete(B2, K2, [], State),
    ?assertMatch({error, not_found, _},
                 Backend:get(B2, K2, State2)),
    fold_objects(Backend, State2).

fold_objects(Backend, State) ->
    {B1, _B2, K1, _K2} = make_bs_and_ks(Backend),
    B3 = make_test_bucket(Backend, 3),
    K3 = make_test_key(Backend, 3),
    ObjFilter = fun(Os) -> [if is_binary(X) -> {BK, X};
                               true         -> {BK, riak_object:get_value(X)}
                            end || {BK, X} <- Os]
                end,
    FoldKeysFun =
        fun(Bucket, Key, Acc) ->
                [{Bucket, Key} | Acc]
        end,
    FoldObjectsFun =
        fun(Bucket, Key, Value, Acc) ->
                [{{Bucket, Key}, Value} | Acc]
        end,
    ?assertEqual([{B1, K1}],
                 begin
                     {ok, Keys} =
                         Backend:fold_keys(FoldKeysFun,
                                           [],
                                           [],
                                           State),
                     lists:sort(Keys)
                 end),

    ?assertEqual([{{B1,K1}, <<"v1">>}],
                 begin
                     {ok, Objects1} =
                         Backend:fold_objects(FoldObjectsFun,
                                              [],
                                              [],
                                              State),
                     lists:sort(ObjFilter(Objects1))
                 end),
    {ok, State2} =  Backend:put(B3, K3, [], <<"v3">>, State),
    ?assertEqual([{{B1,K1},<<"v1">>},
                  {{B3,K3},<<"v3">>}],
                 begin
                     {ok, Objects} =
                         Backend:fold_objects(FoldObjectsFun,
                                              [],
                                              [],
                                              State2),
                     lists:sort(ObjFilter(Objects))
                 end),
    empty_check(Backend, State2).

empty_check(Backend, State) ->
    {B1, _B2, K1, _K2} = make_bs_and_ks(Backend),
    B3 = make_test_bucket(Backend, 3),
    K3 = make_test_key(Backend, 3),
    ?assertEqual(false, Backend:is_empty(State)),
    {ok, State2} = Backend:delete(B1,K1, [], State),
    {ok, State3} = Backend:delete(B3,K3, [], State2),
    ?assertEqual(true, Backend:is_empty(State3)),
    State3.

setup({BackendMod, Config}) ->
    %% Start the backend
    {ok, S} = BackendMod:start(42, Config),
    {BackendMod, S}.

cleanup({BackendMod, S}) ->
    ok = BackendMod:stop(S).

-endif. % TEST
