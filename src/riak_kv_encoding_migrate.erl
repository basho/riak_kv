%% -------------------------------------------------------------------
%%
%% Copyright (c) 2011 Basho Technologies, Inc.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License. You may obtain
%% a copy of the License at
%%
%% http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied. See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

%%
%% Support code to help migrate clusters with URL encoded buckets/keys.
%%
%% Usage:
%% 1. Attach to a running riak console.
%%
%% 2. Check if cluster needs migration (message printed to console):
%%     1> {_, Objs, Custom} = riak_kv_encoding_migrate:check_cluster().
%%
%% 3. If migration necessary and safe, migrate objects:
%%     1> {_, MFail} = riak_kv_encoding_migrate:migrate_objects(Objs).
%%
%% 4. If all went well, delete migrated objects:
%%     1> {_, DFail} = riak_kv_encoding_migrate:delete_migrated_objects(Objs).
%%
%% Possible Failure Cases:
%% -- check_cluster may state that you need to perform custom migration,
%%    and will return the list of problem keys as the third tuple element.
%%
%% -- migrate_objects and delete_migrated_objects may fail. Objects that
%%    failed to be copied/deleted will be returned as the second element.
%%
%% This module also provides the post-commit function 'postcommit_rewrite'
%% that can be installed on a bucket to perform live copying of inserted
%% objects. This could be used to perfrom live migration of new objects
%% while the above migration process is running in background copying
%% existing objects.
%%
%% Alternatively, steps 2-4 can be repeated several times in order to
%% identify and migrate new objects created during the previous migration
%% process.
%%


-module(riak_kv_encoding_migrate).
-export([check_cluster/0, migrate_objects/1, delete_migrated_objects/1,
         get_encoded_keys/0]).
-export([reduce_check_encoded/2, map_unsafe/3, map_rewrite_encoded/3,
         postcommit_rewrite/1]).
-export([test_migration/0, precommit_fail/1]).

%% Check if the cluster contains encoded values that need to be migrated
check_cluster() ->
    {ok, RC} = riak:local_client(),
    {ok, Buckets} = RC:list_buckets(),
    case Buckets of
        [] ->
            io:format("Cluster is empty. No migration needed.~n", []),
            {empty, [], []};
        _ ->
            EObjs = get_encoded_keys(RC),
            check_cluster2(EObjs)
    end.

check_cluster2([]) ->
    io:format("Cluster does not contain URL encoded values. "
              "No migration needed.~n", []),
    {not_needed, [], []};

check_cluster2(EObjs) ->
    case {check_safe(EObjs), check_double_encoding(EObjs)} of
        {{safe, _}, {false, _}} ->
            io:format("Cluster contains URL encoded values. "
                      "Migration needed.~n", []),
            {needed, EObjs, []};
        {{unsafe, Unsafe}, {false, _}} ->
            io:format("Cluster contains URL encoded values. However, "
                      "custom migration necessary:~n"
                      "  -- Some keys exists in both URL encoded and "
                      "non-encoded form.~n", []),
            {custom, EObjs, Unsafe};
        {{unsafe, Unsafe}, {true, Double}} ->
            io:format("Cluster contains URL encoded values. However, "
                      "custom migration necessary:~n"
                      "  -- Some keys exists in both URL encoded and "
                      "non-encoded form.~n"
                      "  -- Some keys are double URL encoded.~n", []),
            {custom, EObjs, Unsafe ++ Double};
        {{safe, _}, {true, Double}} ->
            io:format("Cluster contains URL encoded values. However, "
                      "custom migration necessary:~n"
                      "  -- Some keys are double URL encoded.~n", []),
            {custom, EObjs, Double}
    end.

%% Returns a list of URL encoded objects needing migration.
get_encoded_keys() ->
    {ok, RC} = riak:local_client(),
    get_encoded_keys(RC).

get_encoded_keys(RC) ->
    {ok, Buckets} = RC:list_buckets(),
    EObjs = [begin
                 {ok, Objs} = riak_kv_mrc_pipe:mapred(
                                Bucket, [reduce_check_encoded()]),
                 Objs
             end || Bucket <- Buckets],
    lists:flatten(EObjs).

reduce_check_encoded() ->
    {reduce, {modfun, ?MODULE, reduce_check_encoded}, none, true}.
reduce_check_encoded(L, _) ->
    lists:foldl(fun check_objects/2, [], L).

check_objects(Name={B1, K1}, Objs) ->
    {B2, K2} = decode_name(Name),

    case {B2, K2} of
        {B1, K1} ->
            Objs;
        _ ->
            [{B1, K1} | Objs]
    end.

%% Returns true if any value is double (or more) encoded.
check_double_encoding(EObjs) ->
    DL = lists:filter(fun(Name) ->
                              Name2 = decode_name(Name),
                              Name2 /= decode_name(Name2)
                      end, EObjs),
    case DL of
        [] ->
            {false, []};
        _ ->
            {true, DL}
    end.

%% Determine if it is safe to perform migration (no bucket/key conflicts).
check_safe(EObjs) ->
    EObjs2 = [decode_name(Name) || Name <- EObjs],
    MR = riak_kv_mrc_pipe:mapred(EObjs2, [map_unsafe()]),
    case MR of
        {ok, []} ->
            {safe, []};
        {ok, Unsafe} ->
            {unsafe, Unsafe}
    end.

map_unsafe() ->
    {map, {modfun, ?MODULE, map_unsafe}, none, true}.
map_unsafe({error, notfound}, _, _) ->
    [];
map_unsafe(RO, _, _) ->
    [{list_to_binary(mochiweb_util:quote_plus(riak_object:bucket(RO))),
      list_to_binary(mochiweb_util:quote_plus(riak_object:key(RO)))}].

%% Perform first phase of migration: copying encoded values to
%% unencoded equivalents.
migrate_objects(EObjs) ->
    MR = riak_kv_mrc_pipe:mapred(EObjs, [map_rewrite_encoded()]),
    case MR of
        {ok, []} ->
            io:format("All objects with URL encoded buckets/keys have been "
                      "copied to unencoded equivalents.~n", []),
            {ok, []};
        {ok, Failed} ->
            io:format("Some URL encoded objects failed to copy.~n", []),
            {failed, Failed};
        _ ->
            io:format("There was an error copying objects.~n", []),
            error
    end.

map_rewrite_encoded() ->
    {map, {modfun, ?MODULE, map_rewrite_encoded}, none, true}.
map_rewrite_encoded({error, not_found}, _, _) ->
    [];
map_rewrite_encoded(RO, _, _) ->
    {_, _, B2, K2} = decode_object(RO),
    case copy_object(RO, B2, K2) of
        ok ->
            [];
        _ ->
            [{riak_object:bucket(RO), riak_object:key(RO)}]
    end.

%% Post-commit that can be installed to force live migration of newly
%% inserted buckets/keys before cluster is switched over to proper
%% behavior (eg. during migration).
postcommit_rewrite(RO) ->
    {B1, K1, B2, K2} = decode_object(RO),
    case {B2, K2} of
        {B1, K1} ->
            ok;
        _ ->
            copy_object(RO, B2, K2),
            ok
    end.

%% Perform second phase of migration: delete objects with encoded buckets/keys.
delete_migrated_objects(EObjs) ->
    {ok, RC} = riak:local_client(),
    Failed = lists:foldl(fun(Name={B, K}, Acc) ->
                                 case RC:delete(B, K) of
                                     ok ->
                                         Acc;
                                     _ ->
                                         [Name|Acc]
                                 end
                         end, [], EObjs),

    case Failed of
        [] ->
            io:format("All objects with URL encoded buckets/keys have been "
                      "deleted.~n", []),
            {ok, []};
        _ ->
            io:format("Some objects with URL encoded buckets/keys failed to "
                      "be deleted.~n", []),
            {failed, Failed}
    end.

%% @private
decode_name({B1, K1}) ->
    B2 = list_to_binary(mochiweb_util:unquote(B1)),
    K2 = list_to_binary(mochiweb_util:unquote(K1)),
    {B2, K2}.

%% @private
decode_object(RO) ->
    B1 = riak_object:bucket(RO),
    K1 = riak_object:key(RO),
    B2 = list_to_binary(mochiweb_util:unquote(B1)),
    K2 = list_to_binary(mochiweb_util:unquote(K1)),
    {B1, K1, B2, K2}.

%% @private
copy_object(RO, B, K) ->
    {ok, RC} = riak:local_client(),
    NO1 = riak_object:new(B, K, <<>>),
    NO2 = riak_object:set_vclock(NO1, riak_object:vclock(RO)),
    NO3 = riak_object:set_contents(NO2, riak_object:get_contents(RO)),
    RC:put(NO3).

%% Force writes to fail to test failure behavior
precommit_fail(_) ->
    fail.

%% This test is designed to run directly on an empty development
%% node to avoid cluster bring up/down plumping. It is not an
%% eunit test.
test_migration() ->
    {ok, RC} = riak:local_client(),
    {empty, [], []} = riak_kv_encoding_migrate:check_cluster(),

    O1 = riak_object:new(<<"bucket">>, <<"key">>, <<"val">>),
    RC:put(O1),
    {not_needed, [], []} = riak_kv_encoding_migrate:check_cluster(),

    MD1 = dict:store(<<"X-MyTag">>, <<"A">>, dict:new()),
    O2 = riak_object:new(<<"me%40mine">>, <<"key">>, <<"A">>, MD1),
    RC:put(O2),
    {needed, [{<<"me%40mine">>, <<"key">>}], []} =
        riak_kv_encoding_migrate:check_cluster(),

    O3 = riak_object:new(<<"me@mine">>, <<"key">>, <<"A">>, MD1),
    RC:put(O3),
    {custom, _, [{<<"me%40mine">>, <<"key">>}]} =
        riak_kv_encoding_migrate:check_cluster(),

    RC:delete(<<"me%40mine">>, <<"key">>),
    {not_needed, [], []} = riak_kv_encoding_migrate:check_cluster(),

    MD2 = dict:store(<<"X-MyTag">>, <<"B">>, dict:new()),
    O4 = riak_object:new(<<"bucket">>, <<"key%40">>, <<"B">>, MD2),
    RC:put(O4),
    {needed, [{<<"bucket">>, <<"key%40">>}], []} =
        riak_kv_encoding_migrate:check_cluster(),
    
    O5 = riak_object:new(<<"bucket">>, <<"key@">>, <<"B">>, MD2),
    RC:put(O5),
    {custom, _, [{<<"bucket">>, <<"key%40">>}]} =
        riak_kv_encoding_migrate:check_cluster(),

    RC:delete(<<"me@mine">>, <<"key">>),
    RC:delete(<<"bucket">>, <<"key@">>),
    RC:put(O2),

    O6 = riak_object:new(<<"bdouble%2540">>, <<"bkey%2540">>, <<"C">>),
    RC:put(O6),
    {custom, _, [{<<"bdouble%2540">>, <<"bkey%2540">>}]} =
        riak_kv_encoding_migrate:check_cluster(),

    RC:delete(<<"bdouble%2540">>, <<"bkey%2540">>),
    {needed, EObjs, []} = riak_kv_encoding_migrate:check_cluster(),

    {ok, []} = riak_kv_encoding_migrate:migrate_objects(EObjs),
    {ok, []} = riak_kv_encoding_migrate:delete_migrated_objects(EObjs),
    {not_needed, [], []} = riak_kv_encoding_migrate:check_cluster(),

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

    %% Use precommit hook to test failure scenarios
    O7 = riak_object:new(<<"fail">>, <<"key%40">>, <<"value">>),
    RC:put(O7),
    {needed, EObjs2, []} = riak_kv_encoding_migrate:check_cluster(),

    FailHook = {struct, [{<<"mod">>, <<"riak_kv_encoding_migrate">>},
                         {<<"fun">>, <<"precommit_fail">>}]},

    RC:set_bucket(<<"fail">>, [{precommit, [FailHook]}]),
    {failed, [{<<"fail">>, <<"key%40">>}]} =
        riak_kv_encoding_migrate:migrate_objects(EObjs2),

    RC:set_bucket(<<"fail">>, [{precommit, []}]),
    {ok, []} = riak_kv_encoding_migrate:migrate_objects(EObjs2),

    RC:set_bucket(<<"fail">>, [{precommit, [FailHook]}]),
    {failed, [{<<"fail">>, <<"key%40">>}]} =
        riak_kv_encoding_migrate:delete_migrated_objects(EObjs2),

    RC:set_bucket(<<"fail">>, [{precommit, []}]),
    {ok, []} = riak_kv_encoding_migrate:delete_migrated_objects(EObjs2),

    {not_needed, [], []} = riak_kv_encoding_migrate:check_cluster(),

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
