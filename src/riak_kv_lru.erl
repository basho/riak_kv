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

-record(kv_lru_entry, {ts,
                       key,
                       value}).

new(Size) ->
    IdxName = pid_to_list(self()) ++ "_cache_idx",
    CacheName = pid_to_list(self()) ++ "_cache",
    Idx = ets:new(list_to_atom(IdxName), [set, private]),
    Cache = ets:new(list_to_atom(CacheName), [ordered_set, private, {keypos, 2}]),
    #kv_lru{max_size=Size, idx=Idx, cache=Cache}.

put(#kv_lru{max_size=MaxSize, idx=Idx, cache=Cache}, Key, Value) ->
    remove_existing_if_needed(Idx, Cache, Key),
    insert_value(Idx, Cache, Key, Value),
    prune_oldest_if_needed(MaxSize, Idx, Cache).

fetch(#kv_lru{idx=Idx, cache=Cache}=LRU, Key) ->
    case fetch_value(Idx, Cache, Key) of
        notfound ->
            notfound;
        Value ->
            %% Do a put to update the timestamp in the cache
            put(LRU, Key, Value),
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
    case ets:lookup(Idx, Key) of
        [] ->
            ok;
        [{Key, TS}] ->
            ets:delete(Cache, TS),
            ets:delete(Idx, Key)
    end.

insert_value(Idx, Cache, Key, Value) ->
    TS = erlang:now(),
    Entry = #kv_lru_entry{ts=TS, key=Key, value=Value},
    ets:insert_new(Cache, Entry),
    ets:insert(Idx, {Key, TS}).

prune_oldest_if_needed(MaxSize, Idx, Cache) ->
    OverSize = MaxSize + 1,
    case ets:info(Idx, size) of
        OverSize ->
            Key = ets:first(Cache),
            [Entry] = ets:lookup(Cache, Key),
            ets:delete(Cache, Entry#kv_lru_entry.ts),
            ets:delete(Idx, Entry#kv_lru_entry.key),
            ok;
        _ ->
            ok
    end.

fetch_value(Idx, Cache, Key) ->
    case ets:lookup(Idx, Key) of
        [] ->
            notfound;
        [{Key, TS}] ->
            [Entry] = ets:lookup(Cache, TS),
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

-endif.
