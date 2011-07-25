%% -------------------------------------------------------------------
%%
%% riak_kv_bitcask_backend: Bitcask Driver for Riak
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

-module(riak_kv_bitcask_backend).
-behavior(riak_kv_backend).
-author('Andy Gross <andy@basho.com>').
-author('Dave Smith <dizzyd@basho.com>').

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

%% Helper API
-export([key_counts/0,
         key_counts/1]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include_lib("bitcask/include/bitcask.hrl").

-define(MERGE_CHECK_INTERVAL, timer:minutes(3)).
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
                root :: string()}).

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Return the major version of the 
%% current API and a capabilities list.
api_version() ->
    {?API_VERSION, ?CAPABILITIES}.

%% @doc Start the bitcask backend
start(Partition, Config) ->
    %% Get the data root directory
    DataDir =
        case proplists:get_value(data_root, Config) of
            undefined ->
                case application:get_env(bitcask, data_root) of
                    {ok, Dir} ->
                        Dir;
                    _ ->
                        riak:stop("bitcask data_root unset, failing")
                end;
            Value ->
                Value
        end,

    %% Setup actual bitcask dir for this partition
    BitcaskRoot = filename:join([DataDir,
                                 integer_to_list(Partition)]),
    case filelib:ensure_dir(BitcaskRoot) of
        ok ->
            ok;
        {error, Reason} ->
            lager:critical("Failed to create bitcask dir ~s: ~p",
                                   [BitcaskRoot, Reason]),
            riak:stop("riak_kv_bitcask_backend failed to start.")
    end,

    BitcaskOpts = [{read_write, true}|Config],
    case bitcask:open(BitcaskRoot, BitcaskOpts) of
        Ref when is_reference(Ref) ->
            schedule_merge(Ref),
            maybe_schedule_sync(Ref),
            {ok, #state{ref=Ref, root=BitcaskRoot}};
        {error, Reason2} ->
            {error, Reason2}
    end.


%% @doc Stop the bitcask backend
stop(#state{ref=Ref}) ->
    bitcask:close(Ref).

%% @doc Retrieve an object from the bitcask backend
get(Bucket, Key, #state{ref=Ref}=State) ->
    BitcaskKey = term_to_binary({Bucket, Key}),
    case bitcask:get(Ref, BitcaskKey) of
        {ok, Value} ->
            {ok, Value, State};
        not_found  ->
            {error, notfound, State};
        {error, Reason} ->
            {error, Reason, State}
    end.

%% @doc Insert an object into the bitcask backend
put(Bucket, Key, Val, #state{ref=Ref}=State) ->
    BitcaskKey = term_to_binary({Bucket, Key}),
    case bitcask:put(Ref, BitcaskKey, Val) of
        ok ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end.

%% @doc Delete an object from the bitcask backend
delete(Bucket, Key, #state{ref=Ref}=State) ->
    BitcaskKey = term_to_binary({Bucket, Key}),
    case bitcask:delete(Ref, BitcaskKey) of
        ok ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end.

%% @doc Fold over all the buckets. If the fold
%% function is `none' just list all of the buckets.
fold_buckets(none, Acc, State) ->
    list_buckets(Acc, State);
fold_buckets(FoldBucketsFun, Acc, #state{ref=Ref}) ->
    FoldFun = fun(#bitcask_entry{key=K}, Acc1) ->
                {Bucket, _} = binary_to_term(K),
                FoldBucketsFun(Bucket, Acc1) end,
    bitcask:fold_keys(Ref, FoldFun, Acc).

%% @doc Fold over all the keys for a bucket. If the
%% fold function is `none' just list all of the keys.
fold_keys(none, _Acc, _Opts, State) ->
    list_keys(State);
fold_keys(FoldKeysFun, Acc, _Opts, #state{ref=Ref}) ->
    FoldFun = fun(#bitcask_entry{key=K}, Acc1) ->
                {Bucket, Key} = binary_to_term(K),
                FoldKeysFun(Bucket, Key, Acc1) end,
    bitcask:fold_keys(Ref, FoldFun, Acc).

%% @doc Fold over all the objects for a bucket. If the
%% fold function is `none' just list all of the objects.
fold_objects(none, Acc, _Opts, #state{ref=Ref}) ->
    FoldFun = fun(#bitcask_entry{key=K}, Val, Acc1) ->
                {Bucket, Key} = binary_to_term(K),
                [{{Bucket, Key}, Val} | Acc1] end,
    bitcask:fold(Ref, FoldFun, Acc);
fold_objects(FoldObjectsFun, Acc, _Opts, #state{ref=Ref}) ->
    FoldFun = fun(#bitcask_entry{key=K}, Val, Acc1) ->
                {Bucket, Key} = binary_to_term(K),
                FoldObjectsFun(Bucket, Key, Val, Acc1) end,
    bitcask:fold(Ref, FoldFun, Acc).

%% @doc Delete all objects from this bitcask backend
drop(#state{ref=Ref, root=BitcaskRoot}) ->
    %% @TODO once bitcask has a more friendly drop function
    %%  of its own, use that instead.
    bitcask:close(Ref),
    {ok, FNs} = file:list_dir(BitcaskRoot),
    [file:delete(filename:join(BitcaskRoot, FN)) || FN <- FNs],
    file:del_dir(BitcaskRoot),
    ok.

%% @doc Returns true if this bitcasks backend contains any 
%% non-tombstone values; otherwise returns false.
is_empty(#state{ref=Ref}) ->
    %% Determining if a bitcask is empty requires us to find at least
    %% one value that is NOT a tombstone. Accomplish this by doing a fold_keys
    %% that forcibly bails on the very first key encountered.
    F = fun(_K, _Acc0) ->
                throw(found_one_value)
        end,
    (catch bitcask:fold_keys(Ref, F, undefined)) /= found_one_value.

%% @doc Register an asynchronous callback
callback(Ref, {sync, SyncInterval}, #state{ref=Ref}) when is_reference(Ref) ->
    bitcask:sync(Ref),
    schedule_sync(Ref, SyncInterval);
callback(Ref, merge_check, #state{ref=Ref, root=BitcaskRoot}) when is_reference(Ref) ->
    case bitcask:needs_merge(Ref) of
        {true, Files} ->
            bitcask_merge_worker:merge(BitcaskRoot, [], Files);
        false ->
            ok
    end,
    schedule_merge(Ref);
%% Ignore callbacks for other backends so multi backend works
callback(_Ref, _Msg, _State) ->
    ok.

%% @doc Get the status information for this bitcask backend
status(#state{ref=Ref}) ->
    bitcask:status(Ref);
status(Dir) ->
    Ref = bitcask:open(Dir),
    try bitcask:status(Ref)
    after bitcask:close(Ref)
    end.      

key_counts() ->
    case application:get_env(bitcask, data_root) of
        {ok, RootDir} ->
            key_counts(RootDir);
        undefined ->
            {error, data_root_not_set}
    end.

key_counts(RootDir) ->
    [begin
         {Keys, _} = status(filename:join(RootDir, Dir)),
         {Dir, Keys}
     end || Dir <- element(2, file:list_dir(RootDir))].

%% ===================================================================
%% Internal functions
%% ===================================================================

%% @private
%% Filter the keys for a bucket on this backend
%% filter_bucket_keys(Bucket, Fun, Acc, #state{ref=Ref}) ->
%%     FoldFun = 
%%         fun(#bitcask_entry{key=BK}) ->
%%                 {B, K} = binary_to_term(BK),
%% 		case (B =:= Bucket) andalso Fun(K) of
%% 		    true ->
%% 			[K | Acc];
%% 		    false ->
%%                         Acc
%%                 end
%%         end,
%%     bitcask:fold_keys(Ref, FoldFun, []).

%% @private
%% List the buckets on this backend
list_buckets(Acc, #state{ref=Ref}) ->
    FoldFun = 
        fun(#bitcask_entry{key=BK}) ->
                {B, _} = binary_to_term(BK),
                case lists:member(B, Acc) of
                    true -> Acc;
                    false -> [B | Acc]
                end
        end,
    bitcask:fold_keys(Ref, FoldFun, []).

%% @private
%% List the keys for a bucket on this backend
%% list_bucket_keys(Bucket, Acc, #state{ref=Ref}) ->
%%     FoldFun = 
%%         fun(#bitcask_entry{key=BK}) ->
%%                 {B, K} = binary_to_term(BK),
%%                 case B of
%%                     Bucket -> [K | Acc];
%%                     _ -> Acc
%%                 end
%%         end,
%%     bitcask:fold_keys(Ref, FoldFun, []).

%% @private
%% List all keys stored by this backend
list_keys(#state{ref=Ref}) ->
    case bitcask:list_keys(Ref) of
        KeyList when is_list(KeyList) ->
            [binary_to_term(K) || K <- KeyList];
        Other ->
            Other
    end.

%% @private
%% Schedule sync (if necessary)
maybe_schedule_sync(Ref) when is_reference(Ref) ->
    case application:get_env(bitcask, sync_strategy) of
        {ok, {seconds, Seconds}} ->
            SyncIntervalMs = timer:seconds(Seconds),
            schedule_sync(Ref, SyncIntervalMs);
            %% erlang:send_after(SyncIntervalMs, self(),
            %%                   {?MODULE, {sync, SyncIntervalMs}});
        {ok, none} ->
            ok;
        {ok, o_sync} ->
            ok;
        BadStrategy ->
            lager:notice("Ignoring invalid bitcask sync strategy: ~p",
                                  [BadStrategy]),
            ok
    end.

schedule_sync(Ref, SyncIntervalMs) when is_reference(Ref) ->
    riak_kv_backend:callback_after(SyncIntervalMs, Ref, {sync, SyncIntervalMs}).

schedule_merge(Ref) when is_reference(Ref) ->
    riak_kv_backend:callback_after(?MERGE_CHECK_INTERVAL, Ref, merge_check).

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

simple_test() ->
    ?assertCmd("rm -rf test/bitcask-backend"),
    application:set_env(bitcask, data_root, "test/bitcask-backend"),
    riak_kv_backend:standard_test(?MODULE, []).

custom_config_test() ->
    ?assertCmd("rm -rf test/bitcask-backend"),
    application:set_env(bitcask, data_root, ""),
    riak_kv_backend:standard_test(?MODULE, [{data_root, "test/bitcask-backend"}]).

-endif.
