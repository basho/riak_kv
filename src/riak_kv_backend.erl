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

    {ok, S} = BackendMod:start(42, Config),
    ?assertMatch({ok, _}, BackendMod:put(<<"b1">>,<<"k1">>,<<"v1">>,S)),
    ?assertMatch({ok, _}, BackendMod:put(<<"b2">>,<<"k2">>,<<"v2">>,S)),
    ?assertMatch({ok,<<"v2">>, _}, BackendMod:get(<<"b2">>,<<"k2">>,S)),
    ?assertMatch({error, not_found, _}, BackendMod:get(<<"b1">>,<<"k3">>,S)),
    FoldBucketsFun =
        fun(Bucket, Acc) ->
                [Bucket | Acc]
        end,
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
    FoldObjectsFun =
        fun(Bucket, Key, Value, Acc) ->
                [{{Bucket, Key}, Value} | Acc]
        end,
    ?assertEqual([<<"b1">>, <<"b2">>],
                 lists:sort(BackendMod:fold_buckets(FoldBucketsFun, [], [], S))),
    ?assertEqual([{<<"b1">>, <<"k1">>}, {<<"b2">>, <<"k2">>}],
                 lists:sort(BackendMod:fold_keys(FoldKeysFun, [], [], S))),
    ?assertEqual([<<"k1">>],
                 lists:sort(BackendMod:fold_keys(FoldKeysFun1,
                                                 [],
                                                 [{bucket, <<"b1">>}],
                                                 S))),
    ?assertEqual([<<"k2">>],
                 lists:sort(BackendMod:fold_keys(FoldKeysFun1,
                                                 [],
                                                 [{bucket, <<"b2">>}],
                                                 S))),
    ?assertEqual([<<"k1">>],
                 BackendMod:fold_keys(FoldKeysFun2, [], [], S)),
    ?assertEqual([<<"k1">>],
                 BackendMod:fold_keys(FoldKeysFun2, [], [{bucket, <<"b1">>}], S)),

    ?assertEqual([<<"k2">>],
                 BackendMod:fold_keys(FoldKeysFun3, [], [], S)),
    ?assertEqual([],
                 BackendMod:fold_keys(FoldKeysFun3, [], [{bucket, <<"b1">>}], S)),
    ?assertMatch({ok, _}, BackendMod:delete(<<"b2">>,<<"k2">>,S)),
    ?assertMatch({error, not_found, _}, BackendMod:get(<<"b2">>, <<"k2">>, S)),
    ?assertEqual([{<<"b1">>, <<"k1">>}],
                 lists:sort(BackendMod:fold_keys(FoldKeysFun, [], [], S))),
    ?assertEqual([{{<<"b1">>,<<"k1">>},<<"v1">>}],
                 lists:sort(BackendMod:fold_objects(FoldObjectsFun, [], [], S))),
    ?assertMatch({ok, _}, BackendMod:put(<<"b3">>,<<"k3">>,<<"v3">>,S)),

    ?assertEqual([{{<<"b1">>,<<"k1">>},<<"v1">>},
                  {{<<"b3">>,<<"k3">>},<<"v3">>}],
                 lists:sort(BackendMod:fold_objects(FoldObjectsFun, [], [], S))),
    ?assertEqual(false, BackendMod:is_empty(S)),
    ?assertMatch({ok, _}, BackendMod:delete(<<"b1">>,<<"k1">>,S)),
    ?assertMatch({ok, _}, BackendMod:delete(<<"b3">>,<<"k3">>,S)),
    ?assertEqual(true, BackendMod:is_empty(S)),
    ok = BackendMod:stop(S).

-endif. % TEST
