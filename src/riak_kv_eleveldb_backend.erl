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
-define(CAPABILITIES, []).

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
fold_buckets(FoldBucketsFun, Acc, _Opts, #state{fold_opts=FoldOpts,
                                                ref=Ref}) ->
    FoldFun = fold_buckets_fun(FoldBucketsFun),
    eleveldb:fold_keys(Ref, FoldFun, Acc, FoldOpts).

%% @doc Fold over all the keys for one or all buckets.
%% If the fold function is `none' just list the keys.
fold_keys(FoldKeysFun, Acc, Opts, #state{fold_opts=FoldOpts1,
                                         ref=Ref}) ->
    Bucket =  proplists:get_value(bucket, Opts),
    FoldOpts = fold_opts(Bucket, FoldOpts1),
    FoldFun = fold_keys_fun(FoldKeysFun, Bucket),
    try
        eleveldb:fold_keys(Ref, FoldFun, Acc, FoldOpts)
    catch
        {break, AccFinal} ->
            AccFinal
    end.

%% @doc Fold over all the objects for one or all buckets.
%% If the fold function is `none' just list the objects.
fold_objects(FoldObjectsFun, Acc, Opts, #state{fold_opts=FoldOpts1,
                                               ref=Ref}) ->
    Bucket = proplists:get_value(bucket, Opts),
    FoldOpts = fold_opts(Bucket, FoldOpts1),
    FoldFun = fold_objects_fun(FoldObjectsFun, Bucket),   
    try
        eleveldb:fold_keys(Ref, FoldFun, Acc, FoldOpts)
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
    [{Dir, get_status(filename:join(DataRoot, Dir))} || 
        Dir <- element(2, file:list_dir(DataRoot))].


%% ===================================================================
%% Internal functions
%% ===================================================================

%% @private
config_value(Key, Config) ->
    case proplists:get_value(Key, Config) of
        undefined ->
            app_helper:get_env(eleveldb, Key);
        Value ->
            Value
    end.

%% @private
%% Return a function to fold over the buckets on this backend
fold_buckets_fun(FoldBucketsFun) ->
    fun(BK, Acc) ->
            {Bucket, _} = sext:decode(BK),
            FoldBucketsFun(Bucket, Acc)
    end.

%% @private
%% Return a function to fold over keys on this backend
fold_keys_fun(FoldKeysFun, undefined) ->
    fun(BK, Acc) ->
            {Bucket, Key} = sext:decode(BK),
            FoldKeysFun(Bucket, Key, Acc)
    end;
fold_keys_fun(FoldKeysFun, Bucket) ->
    fun(BK, Acc) ->
            {B, Key} = sext:decode(BK),
            if B =:= Bucket ->
                    FoldKeysFun(B, Key, Acc);
               true ->
                    Acc
            end
    end.

%% @private
%% Return a function to fold over the objects on this backend
fold_objects_fun(FoldObjectsFun, undefined) ->
    fun({BK, Value}, Acc) ->
            {Bucket, Key} = sext:decode(BK),
            FoldObjectsFun(Bucket, Key, Value, Acc)
    end;
fold_objects_fun(FoldObjectsFun, Bucket) ->
    fun({BK, Value}, Acc) ->
            {B, Key} = sext:decode(BK),
            %% Take advantage of the fact that sext-encoding ensures all
            %% keys in a bucket occur sequentially. Once we encounter a
            %% different bucket, the fold is complete
            if B =:= Bucket ->
                    FoldObjectsFun(B, Key, Value, Acc);
               true ->
                    throw({break, Acc})
            end
    end.

%% @private
%% Augment the fold options list if a
%% bucket is defined.
fold_opts(undefined, FoldOpts) ->
    FoldOpts;
fold_opts(Bucket, FoldOpts) ->
    BKey = sext:encode({Bucket, <<>>}),
    [{first_key, BKey} | FoldOpts].

%% @private
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
