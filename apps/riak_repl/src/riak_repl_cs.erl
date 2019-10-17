%% Copyright (c) 2012-2015 Basho Technologies, Inc.
%% This repl hook skips some objects in Riak CS

%% @doc Handle filters on replicating Riak CS specific data. See also
%% test/riak_repl_cs_eqc.erl to know which is replicated or not, in
%% fullsync or realtime.
%%
%% For blocks, all tombstones are replicated by default, which is
%% exception for tombstones. This is to reclaim data space faster. CS
%% wants to delete blocks as fast as possible, because keys of blocks
%% consumes memory space, disk space and disk IO. Deletion conflict in
%% cross-replicated configuration won't be any problem because they
%% are just deletion, and blocks are to be written just once, no
%% update.
%%
%% You should not replicate blocks neither in fullsync or realtime as
%% there are a race condition related to CS garbage collection.

-module(riak_repl_cs).

-ifdef(TEST).
-compile([export_all, nowarn_export_all]).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([send_realtime/2, send/2, recv/1]).

-define(BLOCK_BUCKET_PREFIX, "0b:").
-define(USER_BUCKET, <<"moss.users">>).
-define(ACCESS_BUCKET, <<"moss.access">>).
-define(STORAGE_BUCKET, <<"moss.storage">>).
-define(BUCKETS_BUCKET, <<"moss.buckets">>).

-define(CONFIG_REPL_BLOCKS, replicate_cs_blocks_realtime).
-define(CONFIG_REPL_BLOCK_TOMBSTONE_RT, replicate_cs_block_tombstone_realtime).
-define(CONFIG_REPL_BLOCK_TOMBSTONE_FS, replicate_cs_block_tombstone_fullsync).
-define(CONFIG_REPL_USERS, replicate_cs_user_objects).
-define(CONFIG_REPL_BUCKETS, replicate_cs_bucket_objects).

-spec send(riak_object:riak_object(), riak_client:riak_client()) ->
    ok | cancel.
send(Object, _RiakClient) ->
    bool_to_ok_or_cancel(replicate_object(
                           riak_object:bucket(Object),
                           riak_kv_util:is_x_deleted(Object),
                           fullsync)).

-spec recv(riak_object:riak_object()) -> ok | cancel.
recv(_Object) ->
    ok.

-spec send_realtime(riak_object:riak_object(), riak_client:riak_client()) ->
    ok | cancel.
send_realtime(Object, _RiakClient) ->
    bool_to_ok_or_cancel(replicate_object(
                           riak_object:bucket(Object),
                           riak_kv_util:is_x_deleted(Object),
                           realtime)).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @doc decides replicate or not, according to the combination of
%% bucket name, whether tombstone or not, and fullsync or realtime.
-spec replicate_object(binary(), boolean(), fullsync|realtime) -> boolean().
replicate_object(<<?BLOCK_BUCKET_PREFIX, _Rest/binary>>, IsTombstone, FSorRT) ->
    case {IsTombstone, FSorRT} of
        {false, fullsync} ->
            true;
        {false, realtime} ->
            app_helper:get_env(riak_repl, ?CONFIG_REPL_BLOCKS, false);
        {true, fullsync} ->
            %% Default for tombstoned blocks is *NO PROPAGATE*.
            %% Tombstoned blocks at fullsync are probably lingering ones.
            %% Propagating them to sink will trigger putting tombstones and
            %% reaping them almost in vein. Coming fullsyncs will again propagate
            %% them again and again.
            app_helper:get_env(riak_repl, ?CONFIG_REPL_BLOCK_TOMBSTONE_FS, false);
        {true, realtime} ->
            %% Default for tombstoned blocks is GO AHEAD.
            %% Tombstons put at sink side carry information about overwrite
            %% by its vclock, it enables that sink avoid read phase of
            %% read-before-delete and eases disk load caused by read.
            app_helper:get_env(riak_repl, ?CONFIG_REPL_BLOCK_TOMBSTONE_RT, true)
    end;
replicate_object(_, true, _) -> false;
replicate_object(?STORAGE_BUCKET, _, _) -> false;
replicate_object(?ACCESS_BUCKET, _, _) -> false;
replicate_object(?USER_BUCKET, _, _) ->
    app_helper:get_env(riak_repl, ?CONFIG_REPL_USERS, true);
replicate_object(?BUCKETS_BUCKET, _, _) ->
    app_helper:get_env(riak_repl, ?CONFIG_REPL_BUCKETS, true);
replicate_object(_, _, _) -> true.


-spec bool_to_ok_or_cancel(boolean()) -> ok | cancel.
bool_to_ok_or_cancel(true) ->
    ok;
bool_to_ok_or_cancel(false) ->
    cancel.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

reset_app_env() ->
    ok = application:unset_env(riak_repl, ?CONFIG_REPL_BLOCKS),
    ok = application:unset_env(riak_repl, ?CONFIG_REPL_BLOCK_TOMBSTONE_RT),
    ok = application:unset_env(riak_repl, ?CONFIG_REPL_BLOCK_TOMBSTONE_FS),
    ok = application:unset_env(riak_repl, ?CONFIG_REPL_USERS),
    ok = application:unset_env(riak_repl, ?CONFIG_REPL_BUCKETS).

repl_blocks_test() ->
    reset_app_env(),
    %% the riak client isn't even used
    Client = fake_client,
    Bucket = <<"0b:foo">>,
    Object = riak_object:new(Bucket, <<"key">>, <<"val">>),
    ?assert(ok_or_cancel_to_bool(send(Object, Client))),
    ?assertNot(ok_or_cancel_to_bool(send_realtime(Object, Client))).

dont_repl_access_objects_test() ->
    reset_app_env(),
    %% the riak client isn't even used
    Client = fake_client,
    Bucket = <<"moss.access">>,
    Object = riak_object:new(Bucket, <<"key">>, <<"val">>),
    ?assertNot(ok_or_cancel_to_bool(send(Object, Client))),
    ?assertNot(ok_or_cancel_to_bool(send_realtime(Object, Client))).

dont_repl_storage_objects_test() ->
    reset_app_env(),
    %% the riak client isn't even used
    Client = fake_client,
    Bucket = <<"moss.storage">>,
    Object = riak_object:new(Bucket, <<"key">>, <<"val">>),
    ?assertNot(ok_or_cancel_to_bool(send(Object, Client))),
    ?assertNot(ok_or_cancel_to_bool(send_realtime(Object, Client))).

dont_repl_tombstoned_object_test() ->
    reset_app_env(),
    %% the riak client isn't even used
    Client = fake_client,
    Bucket = <<"almost_anything">>,
    Object = riak_object:new(Bucket, <<"key">>, <<"val">>),
    M = dict:from_list([{<<"X-Riak-Deleted">>, true}]),
    Object2 = riak_object:update_metadata(Object, M),
    Object3 = riak_object:apply_updates(Object2),
    ?assertNot(ok_or_cancel_to_bool(send(Object3, Client))),
    ?assertNot(ok_or_cancel_to_bool(send_realtime(Object3, Client))).

repl_block_tombstone_by_default_test() ->
    reset_app_env(),
    %% the riak client isn't even used
    Client = fake_client,
    Bucket = <<"0b:HASHEDUP">>,
    Object = riak_object:new(Bucket, <<"key">>, <<"val">>),
    M = dict:from_list([{<<"X-Riak-Deleted">>, true}]),
    Object2 = riak_object:update_metadata(Object, M),
    Object3 = riak_object:apply_updates(Object2),
    ?assertNot(ok_or_cancel_to_bool(send(Object3, Client))),
    ?assert(ok_or_cancel_to_bool(send_realtime(Object3, Client))).

repl_block_tombstone_customized_test() ->
    reset_app_env(),
    ok = application:set_env(riak_repl, ?CONFIG_REPL_BLOCK_TOMBSTONE_RT, false),
    ok = application:set_env(riak_repl, ?CONFIG_REPL_BLOCK_TOMBSTONE_FS, true),
    %% the riak client isn't even used
    Client = fake_client,
    Bucket = <<"0b:HASHEDUP">>,
    Object = riak_object:new(Bucket, <<"key">>, <<"val">>),
    M = dict:from_list([{<<"X-Riak-Deleted">>, true}]),
    Object2 = riak_object:update_metadata(Object, M),
    Object3 = riak_object:apply_updates(Object2),
    ?assert(ok_or_cancel_to_bool(send(Object3, Client))),
    ?assertNot(ok_or_cancel_to_bool(send_realtime(Object3, Client))).

repl_user_object_test() ->
    reset_app_env(),
    %% the riak client isn't even used
    Client = fake_client,
    Bucket = <<"moss.users">>,
    Object = riak_object:new(Bucket, <<"key">>, <<"val">>),
    application:set_env(riak_repl, replicate_cs_user_objects, true),
    ?assert(ok_or_cancel_to_bool(send(Object, Client))),
    ?assert(ok_or_cancel_to_bool(send_realtime(Object, Client))),
    application:set_env(riak_repl, replicate_cs_user_objects, false),
    ?assertNot(ok_or_cancel_to_bool(send(Object, Client))),
    ?assertNot(ok_or_cancel_to_bool(send_realtime(Object, Client))).

repl_bucket_object_test() ->
    reset_app_env(),
    %% the riak client isn't even used
    Client = fake_client,
    Bucket = <<"moss.buckets">>,
    Object = riak_object:new(Bucket, <<"key">>, <<"val">>),
    application:set_env(riak_repl, replicate_cs_bucket_objects, true),
    ?assert(ok_or_cancel_to_bool(send(Object, Client))),
    ?assert(ok_or_cancel_to_bool(send_realtime(Object, Client))),
    application:set_env(riak_repl, replicate_cs_bucket_objects, false),
    ?assertNot(ok_or_cancel_to_bool(send(Object, Client))),
    ?assertNot(ok_or_cancel_to_bool(send_realtime(Object, Client))).


do_repl_gc_object_test() ->
    reset_app_env(),
    Client = fake_client,
    Bucket = <<"riak-cs-gc">>,
    Object = riak_object:new(Bucket, <<"key">>, <<"val">>),
    ?assert(ok_or_cancel_to_bool(send_realtime(Object, Client))),
    ?assert(ok_or_cancel_to_bool(send(Object, Client))),
    %% Tombstoned gc objects are NOT replicated. The property is very
    %% important to avoid block leak. See also:
    %% https://github.com/basho/riak_cs/blob/develop/src/riak_cs_gc_worker.erl#L251-L257
    M = dict:from_list([{<<"X-Riak-Deleted">>, true}]),
    Object2 = riak_object:update_metadata(Object, M),
    Object3 = riak_object:apply_updates(Object2),
    ?assertNot(ok_or_cancel_to_bool(send_realtime(Object3, Client))),
    ?assertNot(ok_or_cancel_to_bool(send(Object3, Client))).


do_repl_mb_weight_test() ->
    reset_app_env(),
    Client = fake_client,
    Bucket = <<"riak-cs-multibag">>,
    Object = riak_object:new(Bucket, <<"key">>, <<"val">>),
    ?assert(ok_or_cancel_to_bool(send_realtime(Object, Client))),
    ?assert(ok_or_cancel_to_bool(send(Object, Client))).


%% ===================================================================
%% EUnit helpers
%% ===================================================================

ok_or_cancel_to_bool(ok) ->
    true;
ok_or_cancel_to_bool(cancel) ->
    false.

-endif.
