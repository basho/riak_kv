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
    [{api_version,0},
     {start,2},       % (Partition, Config)
     {stop,1},        % (State)
     {get,3},         % (Bucket, Key, State)
     {put,4},         % (Bucket, Key, Val, State)
     {put,5},         % (Bucket, Key, IndexSpecs, Val, State)
     {delete,3},      % (Bucket, Key, State)
     {drop,1},        % (State)
     {fold_buckets,4},% (FoldBucketsFun, Acc, Opts, State),
                      %   FoldBucketsFun(Bucket, Acc)
     {fold_keys,4},   % (FoldKeysFun, Acc, Opts, State),
                      %   FoldKeysFun(Bucket, Key, Acc)
     {fold_objects,4},% (FoldObjectsFun, Acc, Opts, State),
                      %   FoldObjectsFun(Bucket, Key, Object, Acc)
     {is_empty,1},    % (State)
     {status,1},      % (State)
     {callback,3}];   % (Ref, Msg, State) ->
behaviour_info(_Other) ->
    undefined.

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
                            Backend:put(<<"b1">>, <<"k1">>, <<"v1">>, State)),
              ?_assertMatch({ok, _},
                            Backend:put(<<"b2">>, <<"k2">>, <<"v2">>, State)),
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
                            Backend:put(<<"b3">>,<<"k3">>,<<"v3">>, State)),
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
