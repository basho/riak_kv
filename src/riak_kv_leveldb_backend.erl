%% -------------------------------------------------------------------
%%
%% riak_kv_leveldb_backend: Bitcask Driver for LevelDB
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
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

-module(riak_kv_leveldb_backend).
-behavior(riak_kv_backend).

%% KV Backend API
-export([start/2,
         stop/1,
         get/2,
         put/3,
         delete/2,
         list/1,
         list_bucket/2,
         fold/3,
         fold_keys/3,
         fold_bucket_keys/4,
         drop/1,
         is_empty/1,
         callback/3]).

-record(state, { ref,
                 data_root,
                 read_opts = [],
                 write_opts = [],
                 fold_opts = [{fill_cache, false}]}).


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

config_value(Key, Config) ->
    case proplists:get_value(Key, Config) of
        undefined ->
            app_helper:get_env(e_leveldb, Key);
        Value ->
            Value
    end.

start(Partition, Config) ->
    %% Get the data root directory
    DataDir = filename:join(config_value(data_root, Config),
                            integer_to_list(Partition)),
    filelib:ensure_dir(filename:join(DataDir, "dummy")),
    case e_leveldb:open(DataDir, [{create_if_missing, true}]) of
        {ok, Ref} ->
            {ok, #state { ref = Ref,
                          data_root = DataDir }};
        {error, Reason} ->
            {error, Reason}
    end.

stop(_State) ->
    %% No-op; GC handles cleanup
    ok.

get(State, BKey) ->
    Key = term_to_binary(BKey),
    case e_leveldb:get(State#state.ref, Key, State#state.read_opts) of
        {ok, Value} ->
            {ok, Value};
        not_found  ->
            {error, notfound};
        {error, Reason} ->
            {error, Reason}
    end.

put(State, BKey, Val) ->
    Key = term_to_binary(BKey),
    ok = e_leveldb:put(State#state.ref, Key, Val,
                       State#state.write_opts).

delete(State, BKey) ->
    ok = e_leveldb:delete(State#state.ref, term_to_binary(BKey),
                          State#state.write_opts).

drop(State) ->
    ok = e_leveldb:destroy(State#state.data_root, []).

is_empty(State) ->
    e_leveldb:is_empty(State#state.ref).

callback(_State, _Ref, _Msg) ->
    ok.

list(State) ->
    %% Return a list of all keys in all buckets: [{bucket(), key()}]
    e_leveldb:fold(State#state.ref,
                   fun({BK, _V}, Acc) -> [binary_to_term(BK) | Acc] end,
                   [], State#state.fold_opts).

list_bucket(State, {filter, Bucket, Fun}) ->
    %% Return a filtered list keys within a bucket: [key()]
    F = fun({BK, _V}, Acc) ->
                {B, K} = binary_to_term(BK),
                case (B =:= Bucket) andalso Fun(K) of
                    true ->
                        [K | Acc];
                    false ->
                        Acc
                end
        end,
    e_leveldb:fold(State#state.ref, F, [], State#state.fold_opts);
list_bucket(State, '_') ->
    %% Return a list of all unique buckets: [bucket()]
    F = fun({BK, _V}, Acc) ->
                {B, _} = binary_to_term(BK),
                ordsets:add_element(B, Acc)
        end,
    e_leveldb:fold(State#state.ref, F, [], State#state.fold_opts);
list_bucket(State, Bucket) ->
    %% Return a list of keys in a bucket: [key()]
    list_bucket(State, {filter, Bucket, fun(_) -> true end}).

fold(State, Fun0, Acc0) ->
    %% Apply a fold across all buckets/keys and values
    F = fun({BK, V}, Acc) ->
                Fun0(binary_to_term(BK), V, Acc)
        end,
    e_leveldb:fold(State#state.ref, F, Acc0, State#state.fold_opts).

fold_keys(State, Fun0, Acc0) ->
    %% Apply a fold across all buckets/keys (but NOT values)
    F = fun({BK, _V}, Acc) ->
                Fun0(binary_to_term(BK), Acc)
        end,
    e_leveldb:fold(State#state.ref, F, Acc0, State#state.fold_opts).

fold_bucket_keys(State, Bucket, Fun0, Acc0) ->
    %% Apply a fold across just the keys in a single bucket
    F = fun({BK, _V}, Acc) ->
                case binary_to_term(BK) of
                    {Bucket, Key} ->
                        Fun0(Key, dummy_val, Acc);
                    _ ->
                        Acc
                end
        end,
    e_leveldb:fold(State#state.ref, F, Acc0, State#state.fold_opts).


%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

simple_test() ->
    ?assertCmd("rm -rf test/leveldb-backend"),
    application:set_env(bitcask, data_root, "test/leveldb-backend"),
    riak_kv_backend:standard_test(?MODULE, []).

custom_config_test() ->
    ?assertCmd("rm -rf test/leveldb-backend"),
    application:set_env(e_leveldb, data_root, ""),
    riak_kv_backend:standard_test(?MODULE, [{data_root, "test/leveldb-backend"}]).

-endif.
