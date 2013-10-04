-module(riak_kv_mutator_tests).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-export([mutate_put/5, mutate_get/1]).

functionality_test_() ->
    {foreach, fun() ->
        purge_data_dir(),
        {ok, MetadataManagerPid} = riak_core_metadata_manager:start_link([{data_dir, "test_data"}]),
        {ok, HashtreePid} = riak_core_metadata_hashtree:start_link("test_data"),
        {HashtreePid, MetadataManagerPid}
    end,
    fun({HashtreePid, MetadataManagerPid}) ->
        unlink(MetadataManagerPid),
        exit(MetadataManagerPid, kill),
        unlink(HashtreePid),
        exit(HashtreePid, kill),
        Mon = erlang:monitor(process, MetadataManagerPid),
        receive
            {'DOWN', Mon, process, MetadataManagerPid, _Why} ->
                ok
        end,
        Mon2 = erlang:monitor(process, HashtreePid),
        receive
            {'DOWN', Mon2, process, HashtreePid, _AlsoWhy} ->
                ok
        end,
        purge_data_dir()
    end, [

        fun(_) -> {"register a mutator", fun() ->
            Got = riak_kv_mutator:register(fake_module),
            ?assertEqual(ok, Got)
        end} end,

        fun(_) -> {"retrieve mutators", fun() ->
            ok = riak_kv_mutator:register(fake_module),
            ok = riak_kv_mutator:register(fake_module_2),
            Got = riak_kv_mutator:get(),
            ?assertEqual({ok, [fake_module, fake_module_2]}, Got)
        end} end,

        fun(_) -> {"retrieve an empty list of mutators", fun() ->
            Got = riak_kv_mutator:get(),
            ?assertEqual({ok, []}, Got)
        end} end,

        fun(_) -> {"unregister", fun() ->
            ok = riak_kv_mutator:register(fake_module),
            Got1 = riak_kv_mutator:unregister(fake_module),
            ?assertEqual(ok, Got1),
            Got2 = riak_kv_mutator:get(),
            ?assertEqual({ok, []}, Got2)
        end} end,

        fun(_) -> {"mutator list ordered set", fun() ->
            Mods = [a,z,c,b],
            [riak_kv_mutator:register(M) || M <- Mods],
            Got1 = riak_kv_mutator:get(),
            Expected = ordsets:from_list(Mods),
            ?assertEqual({ok, Expected}, Got1)
        end} end,

        fun(_) -> {"register a mutator with a priority", fun() ->
            Got1 = riak_kv_mutator:register(fake_module, 3),
            ?assertEqual(ok, Got1),
            Got2 = riak_kv_mutator:get(),
            ?assertEqual({ok, [fake_module]}, Got2)
        end} end,

        fun(_) -> {"register a mutator twice, differing priorities", fun() ->
            ok = riak_kv_mutator:register(fake_module, 3),
            ok = riak_kv_mutator:register(fake_module, 7),
            Got = riak_kv_mutator:get(),
            ?assertEqual({ok, [fake_module]}, Got)
        end} end,

        fun(_) -> {"priority determines mutator order", fun() ->
            ok = riak_kv_mutator:register(fake_module, 7),
            ok = riak_kv_mutator:register(fake_module_2, 2),
            Got = riak_kv_mutator:get(),
            ?assertEqual({ok, [fake_module_2, fake_module]}, Got)
        end} end,

        fun(_) -> {"mutate a put", fun() ->
            Object = riak_object:new(<<"bucket">>, <<"key">>, <<"original_data">>, dict:from_list([{<<"mutations">>, 0}])),
            riak_kv_mutator:register(?MODULE),
            {FullMutate, MetaMutates} = riak_kv_mutator:mutate_put(Object, [{<<"bucket_prop">>, <<"bprop">>}]),
            ExpectedVal = <<"mutatedbprop">>,
            ExpectedMetaMutations = 1,
            ?assertEqual(ExpectedVal, riak_object:get_value(FullMutate)),
            ?assertEqual(ExpectedMetaMutations, dict:fetch(<<"mutations">>, riak_object:get_metadata(MetaMutates)))
        end} end,

        fun(_) -> {"do not mutate on get if not mutated on put", fun() ->
            Data = <<"original_data">>,
            Object = riak_object:new(<<"bucket">>, <<"key">>, Data, dict:from_list([{<<"mutations">>, 0}])),
            riak_kv_mutator:register(?MODULE),
            Got = riak_kv_mutator:mutate_get(Object),
            ?assertEqual(Data, riak_object:get_value(Got)),
            ?assertEqual(0, dict:fetch(<<"mutations">>, riak_object:get_metadata(Got)))

        end} end,

        fun(_) -> {"mutate a get", fun() ->
            riak_kv_mutator:register(?MODULE),
            Object = riak_object:new(<<"bucket">>, <<"key">>, <<"original_data">>, dict:from_list([{<<"mutations">>, 0}])),
            {Object2, _MetaObject} = riak_kv_mutator:mutate_put(Object, [{<<"bucket_prop">>, <<"warble">>}]),
            Object3 = riak_kv_mutator:mutate_get(Object2),
            ExpectedVal = <<"mutatedwarble">>,
            ExpectedMetaMutations = 2,
            ?assertEqual(ExpectedVal, riak_object:get_value(Object3)),
            ?assertEqual(ExpectedMetaMutations, dict:fetch(<<"mutations">>, riak_object:get_metadata(Object3)))
        end} end,

        fun(_) -> {"get mutations are reversed order from put mutators", fun() ->
            meck:new(m1, [non_strict]),
            meck:new(m2, [non_strict]),
            meck:expect(m1, mutate_put, fun(Meta, Value, MetaChanges, _Object, _Props) ->
                Mutations = case dict:find(<<"mutations">>, Meta) of
                    {ok, N} ->
                        N * 2;
                    _ ->
                        7
                end,
                Meta2 = dict:store(<<"mutations">>, Mutations, Meta),
                MetaChanges1 = dict:store(<<"mutations">>, Mutations, MetaChanges),
                {Meta2, Value, MetaChanges1}
            end),
            meck:expect(m2, mutate_put, fun(Meta, Value, MetaChanges, _Object, _Props) ->
                Mutations = case dict:find(<<"mutations">>, Meta) of
                    {ok, N} ->
                        N + 3;
                    _ ->
                        20
                end,
                Meta2 = dict:store(<<"mutations">>, Mutations, Meta),
                MetaChanges1 = dict:store(<<"mutations">>, Mutations, MetaChanges),
                {Meta2, Value, MetaChanges1}
            end),
            meck:expect(m1, mutate_get, fun(Object) ->
                MetaValues = riak_object:get_contents(Object),
                MetaValues2 = lists:map(fun({Meta, Value}) ->
                    Mutations = case dict:find(<<"mutations">>, Meta) of
                        {ok, N} ->
                            N div 2;
                        _ ->
                            9
                    end,
                    Meta2 = dict:store(<<"mutations">>, Mutations, Meta),
                    {Meta2, Value}
                end, MetaValues),
                riak_object:set_contents(Object, MetaValues2)
            end),
            meck:expect(m2, mutate_get, fun(Obj) ->
                Contents = riak_object:get_contents(Obj),
                Contents2 = lists:map(fun({Meta, Value}) ->
                    Mutations = case dict:find(<<"mutations">>, Meta) of
                        {ok, N} ->
                            N - 3;
                        _ ->
                            77
                    end,
                    Meta2 =  dict:store(<<"mutations">>, Mutations, Meta),
                    {Meta2, Value}
                end, Contents),
                riak_object:set_contents(Obj, Contents2)
            end),
            riak_kv_mutator:register(m1),
            riak_kv_mutator:register(m2),
            Obj = riak_object:new(<<"bucket">>, <<"key">>, <<"data">>, dict:from_list([{<<"mutations">>, 11}])),
            {Obj2, _Faked} = riak_kv_mutator:mutate_put(Obj, []),
            Obj3 = riak_kv_mutator:mutate_get(Obj2),
            Meta = riak_object:get_metadata(Obj3),
            ?assertEqual({ok, 11}, dict:find(<<"mutations">>, Meta)),
            meck:unload([m1,m2])
        end} end,

        fun(_) -> {"notfound is a valid return for a get mutator", fun() ->
            meck:new(m1, [non_strict]),
            meck:new(m2, [non_strict]),
            meck:expect(m1, mutate_put, fun(Meta, Value, MetaChanges, _Object, _Props) ->
                {Meta, Value, MetaChanges}
            end),
            meck:expect(m2, mutate_put, fun(Meta, Value, MetaChanges, _Object, _Props) ->
                {Meta, Value, MetaChanges}
            end),
            meck:expect(m1, mutate_get, fun(Object) ->
                Object
            end),
            meck:expect(m2, mutate_get, fun(_Obj) ->
                notfound
            end),
            riak_kv_mutator:register(m1),
            riak_kv_mutator:register(m2),
            Obj = riak_object:new(<<"bucket">>, <<"key">>, <<"data">>, dict:from_list([{<<"mutations">>, 11}])),
            {Obj2, _Faked} = riak_kv_mutator:mutate_put(Obj, []),
            Obj3 = riak_kv_mutator:mutate_get(Obj2),
            ?assertEqual(notfound, Obj3),
            meck:unload([m1,m2])
        end} end,

        fun(_) -> {"notfound prevents other mutators being called", fun() ->
            meck:new(m1, [non_strict]),
            meck:new(m2, [non_strict]),
            meck:expect(m1, mutate_put, fun(Meta, Value, MetaChanges, _Object, _Props) ->
                {Meta, Value, MetaChanges}
            end),
            meck:expect(m2, mutate_put, fun(Meta, Value, MetaChanges, _Object, _Props) ->
                {Meta, Value, MetaChanges}
            end),
            meck:expect(m1, mutate_get, fun(Object) ->
                Contents = riak_object:get_contents(Object),
                Contents2 = lists:map(fun({Meta, Value}) ->
                    Meta2 = dict:store(mutated, true, Meta),
                    {Meta2, Value}
                end, Contents),
                riak_object:set_contents(Object, Contents2)
            end),
            meck:expect(m2, mutate_get, fun(_Obj) ->
                notfound
            end),
            riak_kv_mutator:register(m1),
            riak_kv_mutator:register(m2),
            Obj = riak_object:new(<<"bucket">>, <<"key">>, <<"data">>, dict:from_list([{<<"mutations">>, 11}])),
            {Obj2, _Faked} = riak_kv_mutator:mutate_put(Obj, []),
            Obj3 = riak_kv_mutator:mutate_get(Obj2),
            ?assertEqual(notfound, Obj3),
            meck:unload([m1,m2])
        end} end

    ]}.

purge_data_dir() ->
    {ok, CWD} = file:get_cwd(),
    DataDir = filename:join(CWD, "test_data"),
    DataFiles = filename:join([DataDir, "*"]),
    [file:delete(File) || File <- filelib:wildcard(DataFiles)],
    file:del_dir(DataDir).

mutate_put(Meta, _Value, ExposedMeta, _Object, BucketProps) ->
    NewVal = case proplists:get_value(<<"bucket_prop">>, BucketProps) of
        BProp when is_binary(BProp) ->
            <<"mutated", BProp/binary>>;
        _ ->
            <<"mutated">>
    end,
    Mutations = case dict:find(<<"mutations">>, Meta) of
        {ok, N} when is_integer(N) ->
            N + 1;
        _ ->
            0
    end,
    Meta2 = dict:store(<<"mutations">>, Mutations, Meta),
    RevealedMeta2 = dict:store(<<"mutations">>, Mutations, ExposedMeta),
    {Meta2, NewVal, RevealedMeta2}.

mutate_get(Object) ->
    MetaValues = riak_object:get_contents(Object),
    MetaValues2 = lists:map(fun({Meta, Value}) ->
        Mutations = case dict:find(<<"mutations">>, Meta) of
            {ok, N} when is_integer(N) ->
                N + 1;
            _ ->
                0
        end,
        Meta2 = dict:store(<<"mutations">>, Mutations, Meta),
        {Meta2, Value}
    end, MetaValues),
    riak_object:set_contents(Object, MetaValues2).

-endif.