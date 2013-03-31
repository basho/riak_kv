%% -------------------------------------------------------------------
%%
%% riak_kv_backend: Riak backend behaviour
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

-module(riak_kv_backend).

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

-record(state, {
          default_get = <<>>,
          default_size = 0,
          key_count = 0,
          op_get = 0,
          op_put = 0,
          op_delete = 0
         }).
-type state() :: #state{}.
-type config() :: [{atom(), term()}].
-type index_spec() :: {add, Index, SecondaryKey} | {remove, Index, SecondaryKey}.

-callback api_version() -> {ok, integer()}.
-callback capabilities(state()) -> {ok, [atom()]}.
-callback capabilities(riak_object:bucket(), state()) -> {ok, [atom()]}.
-callback start(integer(), config()) -> {ok, state()} | {error, term()}.
-callback stop(state()) -> ok.
-callback get(riak_object:bucket(), riak_object:key(), state()) ->
                 {ok, any(), state()}.
-callback put(riak_object:bucket(), riak_object:key(),
              [index_spec()], binary(), state()) -> {ok, state()}.
-callback delete(riak_object:bucket(), riak_object:key(), [index_spec()], state()) ->
                    {ok, state()}.
-callback fold_buckets(riak_kv_backend:fold_buckets_fun(),
                   any(),
                   [],
                   state()) -> {ok, any()}.
-callback fold_keys(riak_kv_backend:fold_keys_fun(),
                any(),
                [{atom(), term()}],
                state()) -> {ok, term()}.
-callback fold_objects(riak_kv_backend:fold_objects_fun(),
                   any(),
                   [{atom(), term()}],
                   state()) -> {ok, any()} | {async, fun()}.
-callback drop(state()) -> {ok, state()}.
-callback is_empty(state()) -> false.
-callback status(state()) -> [{atom(), term()}].
-callback callback(reference(), any(), state()) -> {ok, state()}.

%% Queue a callback for the backend after Time ms.
-spec callback_after(integer(), reference(), term()) -> reference().
callback_after(Time, Ref, Msg) when is_integer(Time), is_reference(Ref) ->
    riak_core_vnode:send_command_after(Time, {backend_callback, Ref, Msg}).

-ifdef(TEST).

standard_test(BackendMod, Config) ->
    {spawn,
     [
      {setup,
       fun() -> setup({BackendMod, Config}) end,
       fun cleanup/1,
       fun(X) ->
               [basic_store_and_fetch(X),
                fold_buckets(X),
                fold_keys(X),
                delete_object(X),
                fold_objects(X),
                empty_check(X)
               ]
       end
      }]}.

basic_store_and_fetch({Backend, State}) ->
    {"basic store and fetch test",
     fun() ->
             [
              ?_assertMatch({ok, _},
                            Backend:put(<<"b1">>, <<"k1">>, [], <<"v1">>, State)),
              ?_assertMatch({ok, _},
                            Backend:put(<<"b2">>, <<"k2">>, [], <<"v2">>, State)),
              ?_assertMatch({ok,<<"v2">>, _},
                            Backend:get(<<"b2">>, <<"k2">>, State)),
              ?_assertMatch({error, not_found, _},
                            Backend:get(<<"b1">>, <<"k3">>, State))
             ]
     end
    }.

fold_buckets({Backend, State}) ->
    {"bucket folding test",
     fun() ->
             FoldBucketsFun =
                 fun(Bucket, Acc) ->
                         [Bucket | Acc]
                 end,

             ?_assertEqual([<<"b1">>, <<"b2">>],
                           begin
                               {ok, Buckets1} =
                                   Backend:fold_buckets(FoldBucketsFun,
                                                        [],
                                                        [],
                                                        State),
                               lists:sort(Buckets1)
                           end)
     end
    }.

fold_keys({Backend, State}) ->
    {"key folding test",
     fun() ->
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
                         case Bucket =:= <<"b1">> of
                             true ->
                                 [Key | Acc];
                             false ->
                                 Acc
                         end
                 end,
             FoldKeysFun3 =
                 fun(Bucket, Key, Acc) ->
                         case Bucket =:= <<"b1">> of
                             true ->
                                 Acc;
                             false ->
                                 [Key | Acc]
                         end
                 end,
             [
              ?_assertEqual([{<<"b1">>, <<"k1">>}, {<<"b2">>, <<"k2">>}],
                            begin
                                {ok, Keys1} =
                                    Backend:fold_keys(FoldKeysFun,
                                                      [],
                                                      [],
                                                      State),
                                lists:sort(Keys1)
                            end),
              ?_assertEqual({ok, [<<"k1">>]},
                            Backend:fold_keys(FoldKeysFun1,
                                              [],
                                              [{bucket, <<"b1">>}],
                                              State)),
              ?_assertEqual([<<"k2">>],
                            Backend:fold_keys(FoldKeysFun1,
                                              [],
                                              [{bucket, <<"b2">>}],
                                              State)),
              ?_assertEqual({ok, [<<"k1">>]},
                            Backend:fold_keys(FoldKeysFun2, [], [], State)),
              ?_assertEqual({ok, [<<"k1">>]},
                            Backend:fold_keys(FoldKeysFun2,
                                              [],
                                              [{bucket, <<"b1">>}],
                                              State)),
              ?_assertEqual({ok, [<<"k2">>]},
                            Backend:fold_keys(FoldKeysFun3, [], [], State)),
              ?_assertEqual({ok, []},
                            Backend:fold_keys(FoldKeysFun3,
                                              [],
                                              [{bucket, <<"b1">>}],
                                              State))
             ]
     end
    }.

delete_object({Backend, State}) ->
    {"object deletion test",
     fun() ->
             [
              ?_assertMatch({ok, _}, Backend:delete(<<"b2">>, <<"k2">>, State)),
              ?_assertMatch({error, not_found, _},
                            Backend:get(<<"b2">>, <<"k2">>, State))
             ]
     end
    }.

fold_objects({Backend, State}) ->
    {"object folding test",
     fun() ->
             FoldKeysFun =
                 fun(Bucket, Key, Acc) ->
                         [{Bucket, Key} | Acc]
                 end,
             FoldObjectsFun =
                 fun(Bucket, Key, Value, Acc) ->
                         [{{Bucket, Key}, Value} | Acc]
                 end,
             [
              ?_assertEqual([{<<"b1">>, <<"k1">>}],
                            begin
                                {ok, Keys} =
                                    Backend:fold_keys(FoldKeysFun,
                                                      [],
                                                      [],
                                                      State),
                                lists:sort(Keys)
                            end),

              ?_assertEqual([{{<<"b1">>,<<"k1">>}, <<"v1">>}],
                            begin
                                {ok, Objects1} =
                                    Backend:fold_objects(FoldObjectsFun,
                                                         [],
                                                         [],
                                                         State),
                                lists:sort(Objects1)
                            end),
              ?_assertMatch({ok, _},
                            Backend:put(<<"b3">>, <<"k3">>, [], <<"v3">>, State)),
              ?_assertEqual([{{<<"b1">>,<<"k1">>},<<"v1">>},
                             {{<<"b3">>,<<"k3">>},<<"v3">>}],
                            begin
                                {ok, Objects} =
                                    Backend:fold_objects(FoldObjectsFun,
                                                         [],
                                                         [],
                                                         State),
                                lists:sort(Objects)
                            end)
             ]
     end
    }.

empty_check({Backend, State}) ->
    {"is_empty test",
     fun() ->
             [
              ?_assertEqual(false, Backend:is_empty(State)),
              ?_assertMatch({ok, _}, Backend:delete(<<"b1">>,<<"k1">>, State)),
              ?_assertMatch({ok, _}, Backend:delete(<<"b3">>,<<"k3">>, State)),
              ?_assertEqual(true, Backend:is_empty(State))
             ]
     end
    }.

setup({BackendMod, Config}) ->
    %% Start the backend
    {ok, S} = BackendMod:start(42, Config),
    {BackendMod, S}.

cleanup({BackendMod, S}) ->
    ok = BackendMod:stop(S).

-endif. % TEST
