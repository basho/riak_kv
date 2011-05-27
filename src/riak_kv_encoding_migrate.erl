%% Support code to help migrate clusters with URL encoded buckets/keys.
%%
%% Usage:
%% 1. Attach to a running riak console.
%%
%% 2. Check if cluster needs migration:
%%      1> riak_kv_encoding_migrate:check_cluster().
%%
%% 3. If migration necessary and safe, migrate objects:
%%      1> riak_kv_encoding_migrate:migrate_objects().
%%
%% 4. If all went well, delete migrated objects:
%%      1> riak_kv_encoding_migrate:delete_migrated_objects().
%%
%% This module also provides the post-commit function 'postcommit_rewrite'
%% that can be installed on a bucket to perform live copying of inserted
%% objects. This could be used to perfrom live migration of new objects
%% while the above migration process is running in background copying
%% existing objects.

-module(riak_kv_encoding_migrate).
-export([check_cluster/0, migrate_objects/0, delete_migrated_objects/0]).
-export([map_check/3, reduce_check/2, map_rewrite_unsafe/3,
         postcommit_rewrite/1, map_delete_unsafe/3]).
-export([test_migration/0]).

%% Check if the cluster contains unsafe values that need to be migrated
check_cluster() ->
    {ok, RC} = riak:local_client(),
    riak_kv_mapred_cache:clear(),
    {ok, Buckets} = RC:list_buckets(),
    case Buckets of
        [] ->
            io:format("Cluster is empty. No migration needed.~n", []),
            {ok, empty};
        _ ->
            MR = [RC:mapred(Bucket, [map_check(), reduce_check()])
                  || Bucket <- Buckets],
            {ok, [Res]} = lists:foldl(fun erlang:max/2, 0, MR),
            case Res of
                0 ->
                    io:format("Cluster does not contain URL unsafe values. "
                              "No migration needed.~n", []),
                    {ok, not_needed};
                1 ->
                    io:format("Cluster contains URL unsafe values. "
                              "Migration needed.~n", []),
                    {ok, needed};
                2 ->
                    io:format("Cluster contains URL unsafe values. However,~n"
                              "one or more bucket/key exists in both URL~n"
                              "encoded and non-encoded form. Custom migration "
                              "necessary.~n", []),
                    {ok, custom}
            end
    end.

map_check() ->
    {map, {modfun, ?MODULE, map_check}, none, false}.
map_check(RO, _, _) ->
    case check_object(RO) of
        safe ->
            [0];
        {_, _, B2, K2} ->
            {ok, RC} = riak:local_client(),
            case RC:get(B2, K2) of
                {error, notfound} ->
                    [1];
                _ ->
                    [2]
            end
    end.

reduce_check() ->    
    {reduce, {modfun, ?MODULE, reduce_check}, none, true}.
reduce_check(L, _) ->
    [lists:foldl(fun erlang:max/2, 0, L)].

%% Perform first phase of migration: copying encoded values to
%% unencoded equivalents.
migrate_objects() ->
    {ok, RC} = riak:local_client(),
    riak_kv_mapred_cache:clear(),
    {ok, Buckets} = RC:list_buckets(),
    [RC:mapred(Bucket, [map_rewrite_unsafe()]) || Bucket <- Buckets],
    io:format("All objects with URL encoded buckets/keys have been copied to "
              "unencoded equivalents.~n", []).

map_rewrite_unsafe() ->
    {map, {modfun, ?MODULE, map_rewrite_unsafe}, none, true}.
map_rewrite_unsafe(RO, _, _) ->
    case check_object(RO) of
        safe ->
            [];
        {_, _, B2, K2} ->
            copy_object(RO, B2, K2),
            []
    end.

%% Post-commit that can be installed to force live migration of newly
%% inserted buckets/keys before cluster is switched over to proper
%% behavior (eg. during migration).
postcommit_rewrite(RO) ->
    case check_object(RO) of
        safe ->
            ok;
        {_, _, B2, K2} ->
            copy_object(RO, B2, K2),
            ok
    end.

%% Perform second phase of migration: delete objects with encoded buckets/keys.
delete_migrated_objects() ->
    {ok, RC} = riak:local_client(),
    riak_kv_mapred_cache:clear(),
    {ok, Buckets} = RC:list_buckets(),
    [RC:mapred(Bucket, [map_delete_unsafe()]) || Bucket <- Buckets],
    io:format("All objects with URL encoded buckets/keys have been "
              "deleted.~n", []).

map_delete_unsafe() ->
    {map, {modfun, ?MODULE, map_delete_unsafe}, none, true}.
map_delete_unsafe(RO, _, _) ->
    case check_object(RO) of
        safe ->
            [];
        {B1, K1, _, _} ->
            {ok, RC} = riak:local_client(),
            RC:delete(B1, K1),
            []
    end.

%% @private
check_object(RO) ->
    B1 = riak_object:bucket(RO),
    K1 = riak_object:key(RO),
    B2 = list_to_binary(mochiweb_util:unquote(B1)),
    K2 = list_to_binary(mochiweb_util:unquote(K1)),

    case {B2, K2} of
        {B1, K1} ->
            safe;
        _ ->
            {B1, K1, B2, K2}
    end.

%% @private
copy_object(RO, B, K) ->
    {ok, RC} = riak:local_client(),
    NO1 = riak_object:new(B, K, <<>>),
    NO2 = riak_object:set_vclock(NO1, riak_object:vclock(RO)),
    NO3 = riak_object:set_contents(NO2, riak_object:get_contents(RO)),
    RC:put(NO3).

%% This test is designed to run directly on an empty development
%% node to avoid cluster bring up/down plumping. It is not an
%% eunit test.
test_migration() ->
    {ok, RC} = riak:local_client(),
    {ok, empty} = riak_kv_encoding_migrate:check_cluster(),

    O1 = riak_object:new(<<"bucket">>, <<"key">>, <<"val">>),
    RC:put(O1),
    {ok, not_needed} = riak_kv_encoding_migrate:check_cluster(),

    MD1 = dict:store(<<"X-MyTag">>, <<"A">>, dict:new()),
    O2 = riak_object:new(<<"me%40mine">>, <<"key">>, <<"A">>, MD1),
    RC:put(O2),
    {ok, needed} = riak_kv_encoding_migrate:check_cluster(),
    
    O3 = riak_object:new(<<"me@mine">>, <<"key">>, <<"A">>, MD1),
    RC:put(O3),
    {ok, custom} = riak_kv_encoding_migrate:check_cluster(),

    RC:delete(<<"me%40mine">>, <<"key">>),
    {ok, not_needed} = riak_kv_encoding_migrate:check_cluster(),

    MD2 = dict:store(<<"X-MyTag">>, <<"B">>, dict:new()),
    O4 = riak_object:new(<<"bucket">>, <<"key%40">>, <<"B">>, MD2),
    RC:put(O4),
    {ok, needed} = riak_kv_encoding_migrate:check_cluster(),
    
    O5 = riak_object:new(<<"bucket">>, <<"key@">>, <<"B">>, MD2),
    RC:put(O5),
    {ok, custom} = riak_kv_encoding_migrate:check_cluster(),

    RC:delete(<<"me@mine">>, <<"key">>),
    RC:delete(<<"bucket">>, <<"key@">>),
    RC:put(O2),
    {ok, needed} = riak_kv_encoding_migrate:check_cluster(),

    riak_kv_encoding_migrate:migrate_objects(),
    riak_kv_encoding_migrate:delete_migrated_objects(),
    {ok, not_needed} = riak_kv_encoding_migrate:check_cluster(),

    C1 = riak_object:get_contents(O2),
    V1 = riak_object:vclock(O2),

    C2 = riak_object:get_contents(O4),
    V2 = riak_object:vclock(O4),

    {ok, MO1} = RC:get(<<"me@mine">>, <<"key">>),
    nearly_equal_contents(C1, riak_object:get_contents(MO1)),
    true = vclock:descends(riak_object:vclock(MO1), V1),

    {ok, MO2} = RC:get(<<"bucket">>, <<"key@">>),
    nearly_equal_contents(C2, riak_object:get_contents(MO2)),
    true = vclock:descends(riak_object:vclock(MO2), V2),

    ok.

nearly_equal_contents([], []) ->
    true;
nearly_equal_contents([{MD1, V1}|R1], [{MD2, V2}|R2]) ->
    %% VTag and Last-Modified metadata should be different, but
    %% everything else should be equal.
    MD12 = dict:erase(<<"X-Riak-VTag">>, MD1),
    MD13 = dict:erase(<<"X-Riak-Last-Modified">>, MD12),
    L1 = lists:keysort(1, dict:to_list(MD13)),

    MD22 = dict:erase(<<"X-Riak-VTag">>, MD2),
    MD23 = dict:erase(<<"X-Riak-Last-Modified">>, MD22),
    L2 = lists:keysort(1, dict:to_list(MD23)),

    true = (L1 =:= L2),
    true = (V1 =:= V2),
    
    nearly_equal_contents(R1, R2).
