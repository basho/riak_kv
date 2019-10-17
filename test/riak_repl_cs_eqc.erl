%%
%% EQC test for CS replication, with just another impl
%%

-module(riak_repl_cs_eqc).
-compile([export_all, nowarn_export_all]).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).


decision_table() ->
    %% type, rt, fs
    [{buckets,   ok,     ok}, %% Configurable, but default
     {users,     ok,     ok}, %% Configurable, but default
     {blocks,    cancel, ok},
     {block_ts,  cancel, cancel},
     {manifests, ok,     ok},
     {gc,        ok,     ok}, %% riak-cs-gc
     {access,    cancel, cancel},
     {storage,   cancel, cancel},
     {mb,        ok,     ok}, %% Multibag
     {tss,       cancel, cancel}, %% Tombstones in short
     {other,     ok,     ok}].

decision_table_v2() ->
    %% type, rt, fs
    [{buckets,   ok,     ok}, %% Configurable, but default
     {users,     ok,     ok}, %% Configurable, but default
     {blocks,    cancel, ok},
     {block_ts,  ok,     cancel},
     {manifests, ok,     ok},
     {gc,        ok,     ok}, %% riak-cs-gc
     {access,    cancel, cancel},
     {storage,   cancel, cancel},
     {mb,        ok,     ok}, %% Multibag
     {tss,       cancel, cancel}, %% Tombstones in short
     {other,     ok,     ok}].

decision_table_v2_blockrt() ->
    %% type, rt, fs
    [{buckets,   ok,     ok}, %% Configurable, but default
     {users,     ok,     ok}, %% Configurable, but default
     {blocks,    ok,     ok},
     {block_ts,  ok,     cancel},
     {manifests, ok,     ok},
     {gc,        ok,     ok}, %% riak-cs-gc
     {access,    cancel, cancel},
     {storage,   cancel, cancel},
     {mb,        ok,     ok}, %% Multibag
     {tss,       cancel, cancel}, %% Tombstones in short
     {other,     ok,     ok}].

decision_table_v2_no_blockts_rt() ->
    %% type, rt, fs
    [{buckets,   ok,     ok}, %% Configurable, but default
     {users,     ok,     ok}, %% Configurable, but default
     {blocks,    cancel, ok},
     {block_ts,  cancel, cancel},
     {manifests, ok,     ok},
     {gc,        ok,     ok}, %% riak-cs-gc
     {access,    cancel, cancel},
     {storage,   cancel, cancel},
     {mb,        ok,     ok}, %% Multibag
     {tss,       cancel, cancel}, %% Tombstones in short
     {other,     ok,     ok}].

bucket_name_to_type(<<"0b:", _/binary>>, true) -> block_ts; %% Block tombstones
bucket_name_to_type(<<"0b:", _/binary>>, false) -> blocks;
bucket_name_to_type(_, true) -> tss; %% Tombstones
bucket_name_to_type(<<"moss.buckets">>, _) -> buckets;
bucket_name_to_type(<<"moss.users">>, _) -> users;
bucket_name_to_type(<<"0o:", _/binary>>, _) -> manifests;
bucket_name_to_type(<<"riak-cs-gc">>, _) -> gc;
bucket_name_to_type(<<"moss.access">>, _) -> access;
bucket_name_to_type(<<"moss.storage">>, _) -> storage;
bucket_name_to_type(<<"riak-cs-multibag">>, _) -> mb;
bucket_name_to_type(Bin, _) when is_binary(Bin) -> other.


do_send(RTorFS, Object, DecisionTable) ->
    Type = bucket_name_to_type(riak_object:bucket(Object),
                               riak_kv_util:is_x_deleted(Object)),
    {Type, RT, FS} = lists:keyfind(Type, 1, DecisionTable),
    case RTorFS of
        rt -> RT;
        fs -> FS
    end.

%% Bucket names in Riak, used by CS
raw_cs_bucket() ->
    oneof([<<"moss.buckets">>,
           <<"moss.users">>,
           <<"0b:deadbeef">>,
           <<"0o:deadbeef">>,
           <<"riak-cs-gc">>,
           <<"moss.access">>,
           <<"moss.storage">>,
           <<"riak-cs-multibag">>,
           %% And this binary() is default fallback, although this is
           %% not supposed to happen in CS use cases. But just in
           %% case.
           binary()]).

riak_object() ->
    ?LET({Bucket, Key, HasTombstone},
         {raw_cs_bucket(), non_empty(binary()), bool()},
         begin
             Object = riak_object:new(Bucket, Key, <<"val">>),
             case HasTombstone of
                 true ->
                     M = dict:from_list([{<<"X-Riak-Deleted">>, true}]),
                     Object2 = riak_object:update_metadata(Object, M),
                     riak_object:apply_updates(Object2);
                 false ->
                     Object
             end
         end).

eqc_test_() ->
    NumTests = 1024,
    {inorder,
     [ % run qc tests
       ?_assertEqual(true, eqc:quickcheck(eqc:numtests(NumTests, ?QC_OUT(prop_main(v1))))),
       ?_assertEqual(true, eqc:quickcheck(eqc:numtests(NumTests, ?QC_OUT(prop_main(v2))))),
       ?_assertEqual(true, eqc:quickcheck(eqc:numtests(NumTests, ?QC_OUT(prop_main(v2_blockrt))))),
       ?_assertEqual(true, eqc:quickcheck(eqc:numtests(NumTests, ?QC_OUT(prop_main(v2_no_blockts_rt)))))
     ]}.

fs_or_rt() -> oneof([fs, rt]).

prop_main(DecisionTableVersion) ->
    ok = application:unset_env(riak_repl, replicate_cs_block_tombstone_realtime),
    ok = application:unset_env(riak_repl, replicate_cs_block_tombstone_fullsync),
    ok = application:unset_env(riak_repl, replicate_cs_blocks_realtime),
    DecisionTable
        = case DecisionTableVersion of
              v1 ->
                  %% Default behaviour in Riak EE 1.4~2.1.1
                  ok = application:set_env(riak_repl,
                                           replicate_cs_block_tombstone_realtime,
                                           false),
                  decision_table();
              v2 ->
                  %% New behaviour since Riak EE 2.1.2?
                  decision_table_v2();
              v2_blockrt ->
                  %% Replicate blocks in realtime, off by default
                  ok = application:set_env(riak_repl,
                                           replicate_cs_blocks_realtime,
                                           true),
                  decision_table_v2_blockrt();
              v2_no_blockts_rt ->
                  %% Prevent block tombstone propagation in realtime, on by default
                  ok = application:set_env(riak_repl,
                                           replicate_cs_block_tombstone_realtime,
                                           false),
                  decision_table_v2_no_blockts_rt()
          end,
    ?FORALL({Object, FSorRT}, {riak_object(), fs_or_rt()},
            begin
                Impl =
                    case FSorRT of
                        fs -> riak_repl_cs:send(Object, fake_client);
                        rt -> riak_repl_cs:send_realtime(Object, fake_client)
                    end,
                Verify = do_send(FSorRT, Object, DecisionTable),
                %% ?debugVal({Verify, Impl, DecisionTableVersion}),
                Verify =:= Impl
            end).

-endif.
