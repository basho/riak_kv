%% -------------------------------------------------------------------
%%
%% riak_kv_eleveldb_backend: Backend Driver for LevelDB
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

-module(riak_kv_eleveldb_backend).
-behavior(riak_kv_backend).

%% KV Backend API
-export([api_version/0,
         start/2,
         stop/1,
         get/3,
         put/4,
         delete/3,
         drop/1,
         fold_buckets/3,
         fold_keys/4,
         fold_objects/4,
         is_empty/1,
         status/1,
         callback/3]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(API_VERSION, 1).
-define(CAPABILITIES, [api_version,
                       start,
                       stop,
                       get,
                       put,
                       delete,
                       drop,
                       fold_buckets,
                       fold_keys,
                       fold_objects,
                       is_empty,
                       status,
                       callback]).

-record(state, {ref :: reference(),
                data_root :: string(),
                read_opts = [],
                write_opts = [],
                fold_opts = [{fill_cache, false}]}).

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Return the major version of the 
%% current API and a capabilities list.
api_version() ->
    {?API_VERSION, ?CAPABILITIES}.

%% @doc Start the eleveldb backend
start(Partition, Config) ->
    %% Get the data root directory
    DataDir = filename:join(config_value(data_root, Config),
                            integer_to_list(Partition)),
    filelib:ensure_dir(filename:join(DataDir, "dummy")),
    case eleveldb:open(DataDir, [{create_if_missing, true}]) of
        {ok, Ref} ->
            {ok, #state { ref = Ref,
                          data_root = DataDir }};
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Stop the eleveldb backend
stop(_State) ->
    %% No-op; GC handles cleanup
    ok.

%% @doc Retrieve an object from the eleveldb backend
get(Bucket, Key, #state{ref=Ref}=State) ->
    Key = sext:encode({Bucket, Key}),
    case eleveldb:get(Ref, Key, State#state.read_opts) of
        {ok, Value} ->
            {ok, Value};
        not_found  ->
            {error, notfound};
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Insert an object into the eleveldb backend
put(Bucket, Key, Val, #state{ref=Ref}=State) ->
    Key = sext:encode({Bucket, Key}),
    eleveldb:put(Ref, Key, Val,
                 State#state.write_opts).

%% @doc Delete an object from the eleveldb backend
delete(Bucket, Key, #state{ref=Ref}=State) ->
    eleveldb:delete(Ref, sext:encode({Bucket, Key}),
                    State#state.write_opts).

%% @doc Fold over all the buckets. If the fold
%% function is `none' just list all of the buckets.
fold_buckets(none, Acc, #state{fold_opts=FoldOpts,
                               ref=Ref}) ->
    FoldFun = fun(BK, Acc1) ->
                      {B, _} = sext:decode(BK),
                      [B | Acc1]
              end,
    eleveldb:fold_keys(Ref, FoldFun, Acc, FoldOpts);
fold_buckets(FoldBucketsFun, Acc, #state{fold_opts=FoldOpts,
                                         ref=Ref}) ->
    %% Apply a fold across all buckets/keys and values
    FoldFun = fun(BK, Acc1) ->
                      {B, _} = sext:decode(BK),
                      FoldBucketsFun(B, Acc1)
              end,
    eleveldb:fold_keys(Ref, FoldFun, Acc, FoldOpts).

%% @doc Fold over all the keys for a bucket. If the
%% fold function is `none' just list all of the keys.
fold_keys(none, Acc, Opts, #state{fold_opts=FoldOpts,
                                  ref=Ref}) ->
    case proplists:get_value(bucket, Opts) of
        undefined -> % Return a list of all keys in all buckets
            FoldFun = fun(BK, Acc1) -> 
                              [sext:decode(BK) | Acc1]
                      end,
            FoldOpts1 = FoldOpts;
        Bucket -> % Return a list of all keys in the specified bucket
            %% Encode the initial bkey so we can start listing from that point
            BKey0 = sext:encode({Bucket, <<>>}),

            FoldFun = fun(BK, Acc1) -> 
                              {B, K} = sext:decode(BK),
                              %% Take advantage of the fact that sext-encoding ensures all
                              %% keys in a bucket occur sequentially. Once we encounter a
                              %% different bucket, the fold is complete
                              if B =:= Bucket ->
                                      [{B, K} | Acc1];
                                 true ->
                                      throw({break, Acc1})
                              end
                      end,
            FoldOpts1 = [{first_key, BKey0} | FoldOpts]
    end,
    try
        eleveldb:fold_keys(Ref, FoldFun, Acc, FoldOpts1)
    catch
        {break, AccFinal} ->
            AccFinal
    end;
fold_keys(FoldKeysFun, Acc, Opts, #state{fold_opts=FoldOpts,
                                         ref=Ref}) ->
    case proplists:get_value(bucket, Opts) of
        undefined ->
            %% Apply a fold across the keys in all buckets
            FoldFun = fun(BK, Acc1) ->
                              {B, K} = sext:decode(BK),
                              FoldKeysFun(B, K, Acc1)
                      end,
            FoldOpts1 = FoldOpts;
        Bucket ->
            %% Encode the initial bkey so we can start listing from that point
            BKey0 = sext:encode({Bucket, <<>>}),

            %% Apply a fold across just the keys in a single bucket
            FoldFun = fun(BK, Acc1) ->
                              {B, K} = sext:decode(BK),
                              %% Take advantage of the fact that sext-encoding ensures all
                              %% keys in a bucket occur sequentially. Once we encounter a
                              %% different bucket, the fold is complete
                              if B =:= Bucket ->
                                      FoldKeysFun(B, K, Acc1);
                                 true ->
                                      throw({break, Acc1})
                              end
                      end,
            FoldOpts1 = [{first_key, BKey0} | FoldOpts]
    end,
    try
        eleveldb:fold_keys(Ref, FoldFun, Acc, FoldOpts1)
    catch
        {break, AccFinal} ->
            AccFinal
    end.

%% @doc Fold over all the objects for a bucket. If the
%% fold function is `none' just list all of the objects.
fold_objects(none, Acc, Opts, #state{fold_opts=FoldOpts,
                                      ref=Ref}) ->
    case proplists:get_value(bucket, Opts) of
        undefined ->
            %% List all keys and values for all buckets
            FoldFun = fun({BK, Value}, Acc1) ->
                              {Bucket, Key} = sext:decode(BK),
                              [{{Bucket, Key}, Value} |  Acc1]
                      end,
            FoldOpts1 = FoldOpts;
        Bucket ->
            %% Encode the initial bkey so we can start listing from that point
            BKey0 = sext:encode({Bucket, <<>>}),

            %% Apply a fold across just the keys in a single bucket
            FoldFun = fun({BK, Value}, Acc1) ->
                              {B, K} = sext:decode(BK),
                              %% Take advantage of the fact that sext-encoding ensures all
                              %% keys in a bucket occur sequentially. Once we encounter a
                              %% different bucket, the fold is complete
                              if B =:= Bucket ->
                                      [{{B, K}, Value} |  Acc1];
                                 true ->
                                      throw({break, Acc1})
                              end
                      end,
            FoldOpts1 = [{first_key, BKey0} | FoldOpts]
    end,
    try
        eleveldb:fold_keys(Ref, FoldFun, Acc, FoldOpts1)
    catch
        {break, AccFinal} ->
            AccFinal
    end;
fold_objects(FoldObjectsFun, Acc, Opts, #state{fold_opts=FoldOpts,
                                               ref=Ref}) ->
    case proplists:get_value(bucket, Opts) of
        undefined ->
            %% Apply a fold across the keys in all buckets
            FoldFun = fun({BK, Value}, Acc1) ->
                              {B, K} = sext:decode(BK),
                              FoldObjectsFun(B, K, Value, Acc1)
                      end,
            FoldOpts1 = FoldOpts;
        Bucket ->
            %% Encode the initial bkey so we can start listing from that point
            BKey0 = sext:encode({Bucket, <<>>}),

            %% Apply a fold across just the keys in a single bucket
            FoldFun = fun({BK, Value}, Acc1) ->
                              {B, K} = sext:decode(BK),
                              %% Take advantage of the fact that sext-encoding ensures all
                              %% keys in a bucket occur sequentially. Once we encounter a
                              %% different bucket, the fold is complete
                              if B =:= Bucket ->
                                      FoldObjectsFun(B, K, Value, Acc1);
                                 true ->
                                      throw({break, Acc1})
                              end
                      end,
            FoldOpts1 =   [{first_key, BKey0} | FoldOpts]
    end,
    try
        eleveldb:fold_keys(Ref, FoldFun, Acc, FoldOpts1)
    catch
        {break, AccFinal} ->
            AccFinal
    end.

%% @doc Delete all objects from this eleveldb backend
drop(#state{data_root=DataRoot}) ->
    eleveldb:destroy(DataRoot, []).

%% @doc Returns true if this eleveldb backend contains any 
%% non-tombstone values; otherwise returns false.
is_empty(#state{ref=Ref}) ->
    eleveldb:is_empty(Ref).

%% @doc Register an asynchronous callback
callback(_Ref, _Msg, _State) ->
    ok.

%% @doc Get the status information for this eleveldb backend
status(#state{data_root=DataRoot}) ->
    [{Dir, get_status(filename:join(DataRoot, Dir))} || Dir <- element(2, file:list_dir(DataRoot))].


%% ===================================================================
%% Internal functions
%% ===================================================================

config_value(Key, Config) ->
    case proplists:get_value(Key, Config) of
        undefined ->
            app_helper:get_env(eleveldb, Key);
        Value ->
            Value
    end.

get_status(Dir) ->
    case eleveldb:open(Dir, [{create_if_missing, true}]) of
        {ok, Ref} ->
            {ok, Status} = eleveldb:status(Ref, <<"leveldb.stats">>),
            Status;
        {error, Reason} ->
            {error, Reason}
    end.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

simple_test() ->
    ?assertCmd("rm -rf test/leveldb-backend"),
    application:set_env(eleveldb, data_root, "test/leveldb-backend"),
    riak_kv_backend:standard_test(?MODULE, []).

custom_config_test() ->
    ?assertCmd("rm -rf test/leveldb-backend"),
    application:set_env(eleveldb, data_root, ""),
    riak_kv_backend:standard_test(?MODULE, [{data_root, "test/leveldb-backend"}]).

-endif.
