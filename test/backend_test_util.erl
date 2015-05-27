%% -------------------------------------------------------------------
%%
%% backend_test_util: Riak backend test utilities
%%
%% Copyright (c) 2007-2015 Basho Technologies, Inc.  All Rights Reserved.
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

-module(backend_test_util).

-include_lib("eunit/include/eunit.hrl").
-compile(export_all).

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
