-module(riak_kv_mutator_tests).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-export([mutate_put/2, mutate_get/2]).

creation_destruction_test() ->
    Got = riak_kv_mutator:create(),
    ?assertMatch({ok, _Ref}, Got),
    Got2 = riak_kv_mutator:destroy(),
    ?assertEqual(ok, Got2).

functionaltiy_test_() ->
    {foreach, fun() ->
        riak_kv_mutator:create()
    end,
    fun(_) ->
        riak_kv_mutator:destroy()
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

        fun(_) -> {"unregister", fun() ->
            ok = riak_kv_mutator:register(fake_module),
            Got1 = riak_kv_mutator:unregister(fake_module),
            ?assertEqual(ok, Got1),
            Got2 = riak_kv_mutator:get(),
            ?assertEqual({ok, []}, Got2)
        end} end,

        fun(_) -> {"mutate a put", fun() ->
            Object = riak_object:new(<<"bucket">>, <<"key">>, <<"original_data">>, dict:from_list([{<<"mutations">>, 0}])),
            riak_kv_mutator:register(?MODULE),
            Got = riak_kv_mutator:mutate_put(Object, [{<<"bucket_prop">>, <<"bprop">>}]),
            ExpectedVal = <<"mutatedbprop">>,
            ExpectedMetaMutations = 1,
            ?assertEqual(ExpectedVal, riak_object:get_value(Got)),
            ?assertEqual(ExpectedMetaMutations, dict:fetch(<<"mutations">>, riak_object:get_metadata(Got)))
        end} end,

        fun(_) -> {"mutate a get", fun() ->
            Object = riak_object:new(<<"bucket">>, <<"key">>, <<"original_data">>, dict:from_list([{<<"mutations">>, 7}])),
            riak_kv_mutator:register(?MODULE),
            Got = riak_kv_mutator:mutate_get(Object, [{<<"bucket_prop">>, <<"warble">>}]),
            ExpectedVal = <<"mutatedwarble">>,
            ExpectedMetaMutations = 8,
            ?assertEqual(ExpectedVal, riak_object:get_value(Got)),
            ?assertEqual(ExpectedMetaMutations, dict:fetch(<<"mutations">>, riak_object:get_metadata(Got)))
        end} end

    ]}.

mutate_put(Object, BucketProps) ->
    mutate(Object, BucketProps).

mutate_get(Object, BucketProps) ->
    mutate(Object, BucketProps).

mutate(Object, BucketProps) ->
    ?debugMsg("bing"),
    NewVal = case proplists:get_value(<<"bucket_prop">>, BucketProps) of
        BProp when is_binary(BProp) ->
    ?debugMsg("bing"),
            <<"mutated", BProp/binary>>;
        undefined ->
    ?debugMsg("bing"),
            <<"mutated">>
    end,
    ?debugMsg("bing"),
    Meta = riak_object:get_metadata(Object),
    Mutations = case dict:find(<<"mutations">>, Meta) of
        {ok, N} when is_integer(N) ->
    ?debugMsg("bing"),
            N + 1;
        _ ->
    ?debugMsg("bing"),
            0
    end,
    ?debugMsg("bing"),
    Meta2 = dict:store(<<"mutations">>, Mutations, Meta),
    ?debugMsg("bing"),
    Object2 = riak_object:update_value(Object, NewVal),
    ?debugMsg("bing"),
    Object3 = riak_object:update_metadata(Object2, Meta2),
    riak_object:apply_updates(Object3).

-endif.