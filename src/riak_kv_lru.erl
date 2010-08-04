-module(riak_kv_lru).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([new/1,
         put/4,
         remove/3,
         fetch/3,
         size/1,
         max_size/1,
         clear/1,
         clear_bkey/2,
         destroy/1]).

-record(kv_lru, {max_size,
                 bucket_idx,
                 age_idx,
                 cache}).

-record(kv_lru_entry, {key,
                       value,
                       ts}).

new(Size) ->
    IdxName = pid_to_list(self()) ++ "_cache_age_idx",
    BucketIdxName = pid_to_list(self()) ++ "_bucket_idx",
    CacheName = pid_to_list(self()) ++ "_cache",
    Idx = ets:new(list_to_atom(IdxName), [ordered_set, private]),
    BucketIdx = ets:new(list_to_atom(BucketIdxName), [bag, private]),
    Cache = ets:new(list_to_atom(CacheName), [private, {keypos, 2}]),
    #kv_lru{max_size=Size, age_idx=Idx, bucket_idx=BucketIdx, cache=Cache}.

put(#kv_lru{max_size=MaxSize, age_idx=Idx, bucket_idx=BucketIdx,
            cache=Cache}, BKey, Key, Value) ->
    remove_existing(Idx, BucketIdx, Cache, BKey, Key),
    insert_value(Idx, BucketIdx, Cache, BKey, Key, Value),
    prune_oldest_if_needed(MaxSize, Idx, BucketIdx, Cache).

fetch(#kv_lru{cache=Cache}=LRU, BKey, Key) ->
    case fetch_value(Cache, BKey, Key) of
        notfound ->
            notfound;
        Value ->
            %% Do a put to update the timestamp in the cache
            riak_kv_lru:put(LRU, BKey, Key, Value),
            Value
    end.

remove(#kv_lru{age_idx=Idx, bucket_idx=BucketIdx, cache=Cache}, BKey, Key) ->
    remove_existing(Idx, BucketIdx, Cache, BKey, Key).

size(#kv_lru{age_idx=Idx}) ->
    ets:info(Idx, size).

max_size(#kv_lru{max_size=MaxSize}) ->
    MaxSize.

clear(#kv_lru{age_idx=Idx, cache=Cache}) ->
    ets:delete_all_objects(Idx),
    ets:delete_all_objects(Cache).

clear_bkey(#kv_lru{bucket_idx=BucketIdx}=LRU, BKey) ->
    R = ets:match(BucketIdx, {BKey, '$1'}),
    case R of
        [] ->
            ok;
        Keys ->
            [remove(LRU, BKey, Key) || [Key] <- Keys],
            ok
    end.

destroy(#kv_lru{age_idx=Idx, bucket_idx=BucketIdx, cache=Cache}) ->
    ets:delete(Idx),
    ets:delete(BucketIdx),
    ets:delete(Cache).

%% Internal functions
remove_existing(Idx, BucketIdx, Cache, BKey, Key) ->
    CacheKey = {BKey, Key},
    case ets:lookup(Cache, CacheKey) of
        [Entry] ->
            ets:delete(Idx, Entry#kv_lru_entry.ts),
            ets:delete(BucketIdx, CacheKey),
            ets:delete(Cache, CacheKey),
            ok;
        [] ->
            ok
    end.

insert_value(Idx, BucketIdx, Cache, BKey, Key, Value) ->
    CacheKey = {BKey, Key},
    TS = erlang:now(),
    Entry = #kv_lru_entry{key=CacheKey, value=Value, ts=TS},
    ets:insert_new(Cache, Entry),
    ets:insert_new(Idx, {TS, CacheKey}),
    ets:insert(BucketIdx, CacheKey).

prune_oldest_if_needed(MaxSize, Idx, BucketIdx, Cache) ->
    OverSize = MaxSize + 1,
    case ets:info(Idx, size) of
        OverSize ->
            TS = ets:first(Idx),
            [{TS, {BKey, Key}}] = ets:lookup(Idx, TS),
            remove_existing(Idx, BucketIdx, Cache, BKey, Key),
            ok;
        _ ->
            ok
    end.

fetch_value(Cache, BKey, Key) ->
    CacheKey = {BKey, Key},
    case ets:lookup(Cache, CacheKey) of
        [] ->
            notfound;
        [Entry] ->
            Entry#kv_lru_entry.value
    end.

-ifdef(TEST).
put_fetch_test() ->
    BKey = {<<"test">>, <<"foo">>},
    C = riak_kv_lru:new(5),
    riak_kv_lru:put(C, BKey, <<"hello">>, <<"world">>),
    <<"world">> = riak_kv_lru:fetch(C, BKey, <<"hello">>),
    riak_kv_lru:destroy(C).

delete_test() ->
    BKey = {<<"test">>, <<"foo">>},
    C = riak_kv_lru:new(5),
    riak_kv_lru:put(C, BKey, "hello", "world"),
    riak_kv_lru:remove(C, BKey, "hello"),
    notfound = riak_kv_lru:fetch(C, BKey, "hello"),
    riak_kv_lru:destroy(C).

size_test() ->
    BKey = {<<"test">>, <<"foo">>},
    C = riak_kv_lru:new(5),
    [riak_kv_lru:put(C, BKey, X, X) || X <- lists:seq(1, 6)],
    notfound = riak_kv_lru:fetch(C, BKey, 1),
    5 = riak_kv_lru:size(C),
    5 = riak_kv_lru:max_size(C),
    2 = riak_kv_lru:fetch(C, BKey, 2),
    6 = riak_kv_lru:fetch(C, BKey, 6),
    riak_kv_lru:destroy(C).

age_test() ->
    BKey = {<<"test">>, <<"foo">>},
    C = riak_kv_lru:new(3),
    [riak_kv_lru:put(C, BKey, X, X) || X <- lists:seq(1, 3)],
    timer:sleep(500),
    2 = riak_kv_lru:fetch(C, BKey, 2),
    riak_kv_lru:put(C, BKey, 4, 4),
    2 = riak_kv_lru:fetch(C, BKey, 2),
    4 = riak_kv_lru:fetch(C, BKey, 4),
    notfound = riak_kv_lru:fetch(C, BKey, 1),
    riak_kv_lru:destroy(C).

clear_bkey_test() ->
    BKey1 = {<<"test">>, <<"foo">>},
    BKey2 = {<<"test">>, <<"bar">>},
    C = riak_kv_lru:new(10),
    F = fun(X) ->
                riak_kv_lru:put(C, BKey1, X, X),
                riak_kv_lru:put(C, BKey2, X, X) end,
    [F(X) || X <- lists:seq(1, 5)],
    riak_kv_lru:clear_bkey(C, BKey2),
    notfound = riak_kv_lru:fetch(C, BKey1, 3),
    3 = riak_kv_lru:fetch(C, BKey2, 3).

-endif.
