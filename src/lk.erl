-module(lk).

-export([fsm/1, pn/1]).

fsm(Bucket) ->
    ReqId = random:uniform(10000),
    Start = erlang:now(),
    riak_kv_keys_fsm:start(ReqId, Bucket, 60000, plain, 0.0001, self()),
    {ok, Count} = gather_fsm_results(ReqId, 0),
    io:format("~n"),
    End = erlang:now(),
    Ms = erlang:round(timer:now_diff(End, Start) / 1000),
    io:format("Found ~p keys in ~pms.~n", [Count, Ms]).

pn(Bucket) ->
    ReqId = random:uniform(10000),
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    {ok, Bloom} = ebloom:new(10000000,0.0001,ReqId),
    BucketProps = riak_core_bucket:get_bucket(Bucket, Ring),
    N = proplists:get_value(n_val,BucketProps),
    PLS = lists:flatten(riak_core_ring:all_preflists(Ring,N)),
    Nodes = [node()|nodes()],
    Start = erlang:now(),
    start_listers(Nodes, ReqId, Bucket, PLS),
    {ok, Count} = gather_pn_results(ReqId, Bloom, length(Nodes), 0),
    End = erlang:now(),
    Ms = erlang:round(timer:now_diff(End, Start) / 1000),
    io:format("Found ~p keys in ~pms.~n", [Count, Ms]).

gather_fsm_results(ReqId, Count) ->
    receive
        {ReqId, {keys, Keys}} ->
            io:format("."),
            gather_fsm_results(ReqId, Count + length(Keys));
        {ReqId, done} ->
            {ok, Count}
    after 120000 ->
            {error, timeout}
    end.

start_listers([], _ReqId, _Bucket, _VNodes) ->
    ok;
start_listers([H|T], ReqId, Bucket, VNodes) ->
    riak_kv_keylister_master:start_keylist(H, ReqId, self(), Bucket, VNodes, 60000),
    start_listers(T, ReqId, Bucket, VNodes).

gather_pn_results(_, BF, 0, Count) ->
    ebloom:clear(BF),
    {ok, Count};
gather_pn_results(ReqId, BF, NodeCount, Count) ->
    %%io:format("NodeCount: ~p, key count: ~p~n", [NodeCount, Count]),
    receive
        {ReqId, {kl, Keys0}} ->
            F = fun(Key, Acc) ->
                        case ebloom:contains(BF, Key) of
                            false ->
                                ebloom:insert(BF, Key),
                                [Key|Acc];
                            true ->
                                Acc
                        end end,
            Keys = lists:foldl(F, [], Keys0),
            gather_pn_results(ReqId, BF, NodeCount, Count + length(Keys));
        {ReqId, done} ->
            gather_pn_results(ReqId, BF, NodeCount - 1, Count)
    after 10000 ->
            {error, timeout}
    end.
