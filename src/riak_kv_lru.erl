%% -------------------------------------------------------------------
%%
%% riak_kv_lru: ETS-based LRU cache
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
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
         destroy/1,
         table_sizes/1]).
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2]).

-record(kv_lru, {max_size,
                 bucket_idx,
                 age_idx,
                 cache}).

-record(kv_lru_entry, {key,
                       value,
                       ts}).

new(0) ->
    nocache;
new(Size) ->
    {ok, Pid} = gen_server:start_link(?MODULE, [Size], []),
    Pid.

put(nocache, _BKey, _Key, _Value) ->
    ok;
put(Pid, BKey, Key, Value) ->
    gen_server:cast(Pid, {put, BKey, Key, Value}).

fetch(nocache, _BKey, _Key) ->
    notfound;
fetch(Pid, BKey, Key) ->
    gen_server:call(Pid, {fetch, BKey, Key}).

remove(nocache, _BKey, _Key) ->
    ok;
remove(Pid, BKey, Key) ->
    gen_server:cast(Pid, {remove, BKey, Key}).

size(nocache) ->
    0;
size(Pid) ->
    gen_server:call(Pid, size).

max_size(nocache) ->
    0;
max_size(Pid) ->
    gen_server:call(Pid, max_size).

clear(nocache) ->
    ok;
clear(Pid) ->
    gen_server:cast(Pid, clear).

clear_bkey(nocache, _BKey) ->
    ok;
clear_bkey(Pid, BKey) ->
    gen_server:cast(Pid, {clear_bkey, BKey}).

destroy(nocache) ->
    ok;
destroy(Pid) ->
    gen_server:call(Pid, destroy).

%% for test usage
table_sizes(Pid) ->
    gen_server:call(Pid, table_sizes).

init([Size]) ->
    IdxName = pid_to_list(self()) ++ "_cache_age_idx",
    BucketIdxName = pid_to_list(self()) ++ "_bucket_idx",
    CacheName = pid_to_list(self()) ++ "_cache",
    Idx = ets:new(list_to_atom(IdxName), [ordered_set, private]),
    BucketIdx = ets:new(list_to_atom(BucketIdxName), [bag, private]),
    Cache = ets:new(list_to_atom(CacheName), [private, {keypos, 2}]),
    {ok, #kv_lru{max_size=Size, age_idx=Idx, bucket_idx=BucketIdx, cache=Cache}}.

handle_call({fetch, BKey, Key}, _From, State) ->
    Reply = fetch_internal(State, BKey, Key),
    {reply, Reply, State};
handle_call(size, _From, State) ->
    Reply = size_internal(State),
    {reply, Reply, State};
handle_call(max_size, _From, State) ->
    Reply = max_size_internal(State),
    {reply, Reply, State};
handle_call(destroy, _From, State) ->
    {Reply, NewState} = destroy_internal(State),
    {stop, normal, Reply, NewState};
handle_call(table_sizes, _From, State) ->
    Reply = table_sizes_internal(State),
    {reply, Reply, State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({put, BKey, Key, Value}, State) ->
    put_internal(State, BKey, Key, Value),
    {noreply, State};
handle_cast({remove, BKey, Key}, State) ->
    remove_internal(State, BKey, Key),
    {noreply, State};
handle_cast(clear, State) ->
    clear_internal(State),
    {noreply, State};
handle_cast({clear_bkey, BKey}, State) ->
    clear_bkey_internal(State, BKey),
    {noreply, State};
handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    destroy_internal(State),
    ok.

put_internal(#kv_lru{max_size=MaxSize, age_idx=Idx,
                     bucket_idx=BucketIdx, cache=Cache},
             BKey, Key, Value) ->
    remove_existing(Idx, BucketIdx, Cache, BKey, Key),
    insert_value(Idx, BucketIdx, Cache, BKey, Key, Value),
    prune_oldest_if_needed(MaxSize, Idx, BucketIdx, Cache).

fetch_internal(#kv_lru{cache=Cache}=LRU, BKey, Key) ->
    case fetch_value(Cache, BKey, Key) of
        notfound ->
            notfound;
        Value ->
            %% Do a put to update the timestamp in the cache
            put_internal(LRU, BKey, Key, Value),
            Value
    end.

remove_internal(#kv_lru{age_idx=Idx, bucket_idx=BucketIdx, cache=Cache},
                BKey, Key) ->
    remove_existing(Idx, BucketIdx, Cache, BKey, Key),
    ok.

size_internal(#kv_lru{age_idx=Idx}) ->
    ets:info(Idx, size).

max_size_internal(#kv_lru{max_size=MaxSize}) ->
    MaxSize.

clear_internal(#kv_lru{age_idx=Idx, cache=Cache}) ->
    ets:delete_all_objects(Idx),
    ets:delete_all_objects(Cache),
    ok.

clear_bkey_internal(#kv_lru{bucket_idx=BucketIdx}=LRU, BKey) ->
     case ets:lookup(BucketIdx, BKey) of
        [] ->
            ok;
        BK_Ks ->
            [remove_internal(LRU, BKey, Key) || {_BKey, Key} <- BK_Ks],
            ok
    end.

destroy_internal(#kv_lru{age_idx=undefined, bucket_idx=undefined, cache=undefined}=State) ->
    {ok, State};
destroy_internal(#kv_lru{age_idx=Idx, bucket_idx=BucketIdx, cache=Cache}) ->
    ets:delete(Idx),
    ets:delete(BucketIdx),
    ets:delete(Cache),
    {ok, #kv_lru{age_idx=undefined, bucket_idx=undefined, cache=undefined}}.

table_sizes_internal(#kv_lru{age_idx=Idx, bucket_idx=BucketIdx, cache=Cache}) ->
    [{age_idx, ets:info(Idx, size)},
     {bucket_idx, ets:info(BucketIdx, size)},
     {cache, ets:info(Cache, size)}].

%% Internal functions
remove_existing(Idx, BucketIdx, Cache, BKey, Key) ->
    CacheKey = {BKey, Key},
    case ets:lookup(Cache, CacheKey) of
        [Entry] ->
            ets:delete(Idx, Entry#kv_lru_entry.ts),
            ets:delete_object(BucketIdx, CacheKey),
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
    notfound = riak_kv_lru:fetch(C, BKey2, 3),
    3 = riak_kv_lru:fetch(C, BKey1, 3),
    riak_kv_lru:destroy(C).

zero_size_test() ->
    BKey = {<<"test">>, <<"foo">>},
    C = riak_kv_lru:new(0),
    ok = riak_kv_lru:put(C, BKey, 1, 1),
    notfound = riak_kv_lru:fetch(C, BKey, 1),
    0 = riak_kv_lru:size(C),
    riak_kv_lru:destroy(C).

consistency_test() ->
    BKey = {<<"test">>, <<"foo">>},
    C = riak_kv_lru:new(3),
    F = fun(X) ->
		riak_kv_lru:put(C, BKey, X, X)
	end,
    [F(X) || X <- lists:seq(1,10)],
    consistency_check(C).

%% Make sure that riak_kv_lru is correct under concurrent modification
%% by spawning 10 processes that each do 1000 puts on the same LRU, then
%% checking that the size limit of the cache has been respected
%% (added to check https://issues.basho.com/show_bug.cgi?id=969)
concurrency_test() ->
    Size = 10,
    C = riak_kv_lru:new(Size),
    Pids = [ spawn_link(concurrent_incrementer(C, K, self()))
             || K <- lists:seq(10000, 10010) ],
    wait_for_incrementers(Pids),
    consistency_check(C),
    ?assertEqual(Size, riak_kv_lru:size(C)).

concurrent_incrementer(C, K, Test) ->
    fun() ->
            [ riak_kv_lru:put(C, N+K, N+K, N+K)
              || N <- lists:seq(1, 1000) ],
            Test ! {increment_done, self()}
    end.

wait_for_incrementers([]) -> ok;
wait_for_incrementers(Pids) ->
    receive {increment_done, Pid} ->
            wait_for_incrementers(lists:delete(Pid, Pids))
    after 5000 ->
            throw(incrementer_timeout)
    end.

consistency_check(LRU) ->
    Ts = table_sizes(LRU),
    %% make sure all tables report same size
    UniqueSizes = lists:usort([ Size || {_Name, Size} <- Ts]),
    ?assertEqual(1, length(UniqueSizes)).

-endif.
