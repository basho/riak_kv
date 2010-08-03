-module(riak_kv_lru).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([new/1,
         put/3,
         remove/2,
         fetch/2,
         size/1,
         max_size/1,
         clear/1,
         destroy/1]).

-record(kv_lru, {max_size,
                 idx,
                 cache}).

-record(kv_lru_entry, {key,
                       value,
                       ts}).

new(Size) ->
    IdxName = pid_to_list(self()) ++ "_cache_idx",
    CacheName = pid_to_list(self()) ++ "_cache",
    Idx = ets:new(list_to_atom(IdxName), [ordered_set, private]),
    Cache = ets:new(list_to_atom(CacheName), [private, {keypos, 2}]),
    #kv_lru{max_size=Size, idx=Idx, cache=Cache}.

put(#kv_lru{max_size=MaxSize, idx=Idx, cache=Cache}, Key, Value) ->
    remove_existing_if_needed(Idx, Cache, Key),
    insert_value(Idx, Cache, Key, Value),
    prune_oldest_if_needed(MaxSize, Idx, Cache).

fetch(#kv_lru{cache=Cache}=LRU, Key) ->
    case fetch_value(Cache, Key) of
        notfound ->
            notfound;
        Value ->
            %% Do a put to update the timestamp in the cache
            riak_kv_lru:put(LRU, Key, Value),
            Value
    end.

remove(#kv_lru{idx=Idx, cache=Cache}, Key) ->
    remove_existing_if_needed(Idx, Cache, Key).

size(#kv_lru{idx=Idx}) ->
    ets:info(Idx, size).

max_size(#kv_lru{max_size=MaxSize}) ->
    MaxSize.

clear(#kv_lru{idx=Idx, cache=Cache}) ->
    ets:delete_all_objects(Idx),
    ets:delete_all_objects(Cache).

destroy(#kv_lru{idx=Idx, cache=Cache}) ->
    ets:delete(Idx),
    ets:delete(Cache).

%% Internal functions
remove_existing_if_needed(Idx, Cache, Key) ->
    case ets:lookup(Cache, Key) of
        [Entry] ->
            ets:delete(Idx, Entry#kv_lru_entry.ts),
            ets:delete(Cache, Key),
            ok;
        [] ->
            ok
    end.

insert_value(Idx, Cache, Key, Value) ->
    TS = erlang:now(),
    Entry = #kv_lru_entry{key=Key, value=Value, ts=TS},
    ets:insert_new(Cache, Entry),
    ets:insert(Idx, {TS, Key}).

prune_oldest_if_needed(MaxSize, Idx, Cache) ->
    OverSize = MaxSize + 1,
    case ets:info(Idx, size) of
        OverSize ->
            TS = ets:first(Idx),
            [{TS, Key}] = ets:lookup(Idx, TS),
            ets:delete(Idx, TS),
            ets:delete(Cache, Key),
            ok;
        _ ->
            ok
    end.

fetch_value(Cache, Key) ->
    case ets:lookup(Cache, Key) of
        [] ->
            notfound;
        [Entry] ->
            Entry#kv_lru_entry.value
    end.

-ifdef(TEST).

put_fetch_test() ->
    C = riak_kv_lru:new(5),
    riak_kv_lru:put(C, <<"hello">>, <<"world">>),
    <<"world">> = riak_kv_lru:fetch(C, <<"hello">>),
    riak_kv_lru:destroy(C).

delete_test() ->
    C = riak_kv_lru:new(5),
    riak_kv_lru:put(C, "hello", "world"),
    riak_kv_lru:remove(C, "hello"),
    notfound = riak_kv_lru:fetch(C, "hello"),
    riak_kv_lru:destroy(C).

size_test() ->
    C = riak_kv_lru:new(5),
    [riak_kv_lru:put(C, X, X) || X <- lists:seq(1, 6)],
    notfound = riak_kv_lru:fetch(C, 1),
    5 = riak_kv_lru:size(C),
    5 = riak_kv_lru:max_size(C),
    2 = riak_kv_lru:fetch(C, 2),
    6 = riak_kv_lru:fetch(C, 6),
    riak_kv_lru:destroy(C).

age_test() ->
    C = riak_kv_lru:new(3),
    [riak_kv_lru:put(C, X, X) || X <- lists:seq(1, 3)],
    timer:sleep(500),
    2 = riak_kv_lru:fetch(C, 2),
    riak_kv_lru:put(C, 4, 4),
    2 = riak_kv_lru:fetch(C, 2),
    4 = riak_kv_lru:fetch(C, 4),
    notfound = riak_kv_lru:fetch(C, 1),
    riak_kv_lru:destroy(C).
-endif.
