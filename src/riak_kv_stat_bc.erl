%% -------------------------------------------------------------------
%%
%% riak_kv_stat_bc: backwards compatible stats module. Maps new folsom stats
%%                  to legacy riak_kv stats.
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

%% @doc riak_kv_stat_bc is a module that maps the new riak_kv_stats metrics
%% to the old set of stats. It exists to maintain backwards compatibility for
%% those using the `/stats` endpoint and `riak-admin status`. This module
%% should be considered soon to be deprecated and temporary.
%%
%%      Legacy stats:
%%<dl><dt>  vnode_gets
%%</dt><dd> Total number of gets handled by all vnodes on this node
%%          in the last minute.
%%</dd>
%%<dt> vnode_puts
%%</dt><dd> Total number of puts handled by all vnodes on this node
%%          in the last minute.
%%</dd>
%%<dt> vnode_index_reads
%%</dt><dd> The number of index reads handled by all vnodes on this node.
%%          Each query counts as an index read.
%%</dd><
%%<dt> vnode_index_writes
%%</dt><dd> The number of batched writes handled by all vnodes on this node.
%%</dd>
%%<dt> vnode_index_writes_postings
%%</dt><dd> The number of postings written to all vnodes on this node.
%%</dd>
%%<dt> vnode_index_deletes
%%</dt><dd> The number of batched writes handled by all vnodes on this node.
%%</dd><dd> update({vnode_index_delete, PostingsRemoved})
%%
%%</dd><dt> vnode_index_deletes_postings
%%</dt><dd> The number of postings written to all vnodes on this node.
%%</dd><dt> node_gets
%%</dt><dd> Number of gets coordinated by this node in the last
%%          minute.
%%</dd><dt> node_get_fsm_siblings
%%</dt><dd> Stats about number of siblings per object in the last minute.
%%</dd><dt> node_get_fsm_objsize
%%</dt><dd> Stats about object size over the last minute. The object
%%          size is an estimate calculated by summing the size of the
%%          bucket name, key name, and serialized vector clock, plus
%%          the value and serialized metadata of each sibling.
%%</dd><dt> node_get_fsm_time_mean
%%</dt><dd> Mean time, in microseconds, between when a riak_kv_get_fsm is
%%          started and when it sends a reply to the client, for the
%%          last minute.
%%</dd><dt> node_get_fsm_time_median
%%</dt><dd> Median time, in microseconds, between when a riak_kv_get_fsm
%%          is started and when it sends a reply to the client, for
%%          the last minute.
%%</dd><dt> node_get_fsm_time_95
%%</dt><dd> Response time, in microseconds, met or beaten by 95% of
%%          riak_kv_get_fsm executions.
%%</dd><dt> node_get_fsm_time_99
%%</dt><dd> Response time, in microseconds, met or beaten by 99% of
%%          riak_kv_get_fsm executions.
%%</dd><dt> node_get_fsm_time_100
%%</dt><dd> Response time, in microseconds, met or beaten by 100% of
%%          riak_kv_get_fsm executions.
%%</dd><dt> node_puts
%%</dt><dd> Number of puts coordinated by this node in the last
%%          minute.
%%</dd><dt> node_put_fsm_time_mean
%%</dt><dd> Mean time, in microseconds, between when a riak_kv_put_fsm is
%%          started and when it sends a reply to the client, for the
%%          last minute.
%%</dd><dt> node_put_fsm_time_median
%%</dt><dd> Median time, in microseconds, between when a riak_kv_put_fsm
%%          is started and when it sends a reply to the client, for
%%          the last minute.
%%</dd><dt> node_put_fsm_time_95
%%</dt><dd> Response time, in microseconds, met or beaten by 95% of
%%          riak_kv_put_fsm executions.
%%</dd><dt> node_put_fsm_time_99
%%</dt><dd> Response time, in microseconds, met or beaten by 99% of
%%          riak_kv_put_fsm executions.
%%</dd><dt> node_put_fsm_time_100
%%</dt><dd> Response time, in microseconds, met or beaten by 100% of
%%          riak_kv_put_fsm executions.
%%</dd><dt> cpu_nprocs
%%</dt><dd> Value returned by {@link cpu_sup:nprocs/0}.
%%
%%</dd><dt> cpu_avg1
%%</dt><dd> Value returned by {@link cpu_sup:avg1/0}.
%%
%%</dd><dt> cpu_avg5
%%</dt><dd> Value returned by {@link cpu_sup:avg5/0}.
%%
%%</dd><dt> cpu_avg15
%%</dt><dd> Value returned by {@link cpu_sup:avg15/0}.
%%
%%</dd><dt> mem_total
%%</dt><dd> The first element of the tuple returned by
%%          {@link memsup:get_memory_data/0}.
%%
%%</dd><dt> mem_allocated
%%</dt><dd> The second element of the tuple returned by
%%          {@link memsup:get_memory_data/0}.
%%
%%</dd><dt> disk
%%</dt><dd> Value returned by {@link disksup:get_disk_data/0}.
%%
%%</dd><dt> pbc_connects_total
%%</dt><dd> Total number of pb socket connections since start
%%
%%</dd><dt> pbc_active
%%</dt><dd> Number of active pb socket connections
%%
%%</dd><dt> coord_redirs_total
%%</dt><dd> Number of puts forwarded to be coordinated on a node
%%          in the preflist.
%%
%%</dd></dl>
%%
%%
-module(riak_kv_stat_bc).

-compile(export_all).

%% @spec produce_stats(state(), integer()) -> proplist()
%% @doc Produce a proplist-formatted view of the current aggregation
%%      of stats.
produce_stats() ->
    lists:append(
      [lists:flatten(legacy_stats()),
       sidejob_stats(),
       read_repair_stats(),
       level_stats(),
       pipe_stats(),
       cpu_stats(),
       mem_stats(),
       disk_stats(),
       system_stats(),
       ring_stats(),
       config_stats(),
       app_stats(),
       memory_stats()
      ]).

%% Stats in folsom are stored with tuples as keys, the
%% tuples mimic an hierarchical structure. To be free of legacy
%% naming constraints the new names are not simply the old names
%% with commas for underscores. Uses legacy_stat_map to generate
%% legacys stats from the new list of stats.
legacy_stats() ->
    {Legacy, _Calculated} = lists:foldl(fun({Old, New, Type}, {Acc, Cache}) ->
                                                bc_stat({Old, New, Type}, Acc, Cache) end,
                                        {[], []},
                                        legacy_stat_map()),
    lists:reverse(Legacy).

%% @doc legacy stats uses multifield stats for multiple stats
%% don't calculate the same stat many times
get_stat(Name, Type, Cache) ->
    get_stat(Name, Type, Cache, fun(S) -> S end).

get_stat(Name, Type, Cache, ValFun) ->
    case proplists:get_value(Name, Cache) of
        undefined ->
            case riak_core_stat_q:calc_stat({Name, Type}) of
                unavailable -> {unavailable, Cache};
                Stat ->
                    {ValFun(Stat), [{Name, Stat} | Cache]}
            end;
        Cached -> {ValFun(Cached), Cache}
    end.

bc_stat({Old, {NewName, Field}, histogram}, Acc, Cache) ->
    ValFun = fun(Stat) -> trunc(proplists:get_value(Field, Stat)) end,
    {Val, Cache1} = get_stat(NewName, histogram, Cache, ValFun),
    {[{Old, Val} | Acc], Cache1};
bc_stat({Old, {NewName, Field}, histogram_percentile}, Acc, Cache) ->
    ValFun = fun(Stat) ->
                     Percentile = proplists:get_value(percentile, Stat),
                     Val = proplists:get_value(Field, Percentile),
                     trunc(Val) end,
    {Val, Cache1} = get_stat(NewName, histogram, Cache, ValFun),
    {[{Old, Val} | Acc], Cache1};
bc_stat({Old, {NewName, Field}, spiral}, Acc, Cache) ->
    ValFun = fun(Stat) ->
                     proplists:get_value(Field, Stat)
             end,
    {Val, Cache1} = get_stat(NewName, spiral, Cache, ValFun),
    {[{Old, Val} | Acc], Cache1};
bc_stat({Old, NewName, counter}, Acc, Cache) ->
    {Val, Cache1} = get_stat(NewName, counter, Cache),
    {[{Old, Val} | Acc], Cache1};
bc_stat({Old, NewName, function}, Acc, Cache) ->
    {Val, Cache1} = get_stat(NewName, gauge, Cache),
    {[{Old, Val} | Acc], Cache1}.

%% hard coded mapping of stats to legacy format
%% There was a enough variation in the old names that a simple
%% concatenation of the elements in the new stat key would not suffice
%% applications depend on these exact legacy names.
legacy_stat_map() ->
    [{vnode_gets, {{riak_kv, vnode, gets}, one}, spiral},
     {vnode_gets_total, {{riak_kv, vnode, gets}, count}, spiral},
     {vnode_puts, {{riak_kv, vnode, puts}, one}, spiral},
     {vnode_puts_total, {{riak_kv, vnode, puts}, count}, spiral},
     {vnode_index_reads, {{riak_kv, vnode, index, reads}, one}, spiral},
     {vnode_index_reads_total, {{riak_kv, vnode, index, reads}, count}, spiral},
     {vnode_index_writes, {{riak_kv, vnode, index, writes}, one}, spiral},
     {vnode_index_writes_total, {{riak_kv, vnode, index, writes}, count}, spiral},
     {vnode_index_writes_postings, {{riak_kv,vnode,index,writes,postings}, one}, spiral},
     {vnode_index_writes_postings_total, {{riak_kv,vnode,index,writes,postings}, count}, spiral},
     {vnode_index_deletes, {{riak_kv,vnode,index,deletes}, one}, spiral},
     {vnode_index_deletes_total, {{riak_kv,vnode,index,deletes}, count}, spiral},
     {vnode_index_deletes_postings, {{riak_kv,vnode,index,deletes,postings}, one}, spiral},
     {vnode_index_deletes_postings_total, {{riak_kv,vnode,index,deletes,postings}, count}, spiral},
     {node_gets, {{riak_kv,node,gets}, one}, spiral},
     {node_gets_total, {{riak_kv,node,gets}, count}, spiral},
     {node_get_fsm_siblings_mean, {{riak_kv,node,gets,siblings}, arithmetic_mean}, histogram},
     {node_get_fsm_siblings_median, {{riak_kv,node,gets,siblings}, median}, histogram},
     {node_get_fsm_siblings_95, {{riak_kv,node,gets,siblings}, 95}, histogram_percentile},
     {node_get_fsm_siblings_99, {{riak_kv,node,gets,siblings}, 99}, histogram_percentile},
     {node_get_fsm_siblings_100, {{riak_kv,node,gets,siblings}, max}, histogram},
     {node_get_fsm_objsize_mean, {{riak_kv,node,gets,objsize}, arithmetic_mean}, histogram},
     {node_get_fsm_objsize_median, {{riak_kv,node,gets,objsize}, median}, histogram},
     {node_get_fsm_objsize_95, {{riak_kv,node,gets,objsize}, 95}, histogram_percentile},
     {node_get_fsm_objsize_99, {{riak_kv,node,gets,objsize}, 99}, histogram_percentile},
     {node_get_fsm_objsize_100, {{riak_kv,node,gets,objsize}, max}, histogram},
     {node_get_fsm_time_mean, {{riak_kv,node,gets,time}, arithmetic_mean}, histogram},
     {node_get_fsm_time_median, {{riak_kv,node,gets,time}, median}, histogram},
     {node_get_fsm_time_95, {{riak_kv,node,gets,time}, 95}, histogram_percentile},
     {node_get_fsm_time_99, {{riak_kv,node,gets,time}, 99}, histogram_percentile},
     {node_get_fsm_time_100, {{riak_kv,node,gets,time}, max}, histogram},
     {node_puts, {{riak_kv,node, puts}, one}, spiral},
     {node_puts_total, {{riak_kv,node, puts}, count}, spiral},
     {node_put_fsm_time_mean, {{riak_kv,node, puts, time}, arithmetic_mean}, histogram},
     {node_put_fsm_time_median, {{riak_kv,node, puts, time}, median}, histogram},
     {node_put_fsm_time_95,  {{riak_kv,node, puts, time}, 95}, histogram_percentile},
     {node_put_fsm_time_99,  {{riak_kv,node, puts, time}, 99}, histogram_percentile},
     {node_put_fsm_time_100, {{riak_kv,node, puts, time}, max}, histogram},
     {read_repairs, {{riak_kv,node,gets,read_repairs}, one}, spiral},
     {read_repairs_total, {{riak_kv,node,gets,read_repairs}, count}, spiral},
     {coord_redirs_total, {riak_kv,node,puts,coord_redirs}, counter},
     {executing_mappers, {riak_kv,mapper_count}, counter},
     {precommit_fail, {riak_kv, precommit_fail}, counter},
     {postcommit_fail, {riak_kv, postcommit_fail}, counter},
     {index_fsm_create, {{riak_kv, index, fsm, create}, one}, spiral},
     {index_fsm_create_error, {{riak_kv, index, fsm, create, error}, one}, spiral},
     {index_fsm_active, {riak_kv, index, fsm, active}, counter},
     {list_fsm_create, {{riak_kv, list, fsm, create}, one}, spiral},
     {list_fsm_create_error, {{riak_kv, list, fsm, create, error}, one}, spiral},
     {list_fsm_active, {riak_kv, list, fsm, active}, counter},
     {pbc_active, {riak_api, pbc_connects, active}, function},
     {pbc_connects, {{riak_api, pbc_connects}, one}, spiral},
     {pbc_connects_total, {{riak_api, pbc_connects}, count}, spiral}] ++ legacy_fsm_stats().

legacy_fsm_stats() ->
    %% When not using sidejob to manage FSMs, include the legacy FSM stats.
    legacy_get_fsm_stats() ++ legacy_put_fsm_stats().

legacy_get_fsm_stats() ->
    case whereis(riak_kv_get_fsm_sj) of
        undefined ->
            [{node_get_fsm_active, {riak_kv, node, gets, fsm, active}, counter},
             {node_get_fsm_errors, {{riak_kv, node, gets, fsm, errors}, one}, spiral},
             {node_get_fsm_errors_total, {{riak_kv, node, gets, fsm, errors}, count}, spiral}];
        _ ->
            []
    end.

legacy_put_fsm_stats() ->
    case whereis(riak_kv_put_fsm_sj) of
        undefined ->
            [{node_put_fsm_active, {riak_kv, node, puts, fsm, active}, counter},
             {node_put_fsm_errors, {{riak_kv, node, puts, fsm, errors}, one}, spiral},
             {node_put_fsm_errors_total, {{riak_kv, node, puts, fsm, errors}, count}, spiral}];
        _ ->
            []
    end.

sidejob_stats() ->
    sidejob_get_fsm_stats() ++ sidejob_put_fsm_stats().

sidejob_get_fsm_stats() ->
    Resource = riak_kv_get_fsm_sj,
    case whereis(Resource) of
        undefined ->
            [];
        _ ->
            Stats = sidejob_resource_stats:stats(Resource),
            Map = [{node_get_fsm_active,         usage},
                   {node_get_fsm_active_60s,     usage_60s},
                   {node_get_fsm_in_rate,        in_rate},
                   {node_get_fsm_out_rate,       out_rate},
                   {node_get_fsm_rejected,       rejected},
                   {node_get_fsm_rejected_60s,   rejected_60s},
                   {node_get_fsm_rejected_total, rejected_total}],
            [{Rename, proplists:get_value(Stat, Stats)} || {Rename, Stat} <- Map]
    end.

sidejob_put_fsm_stats() ->
    Resource = riak_kv_put_fsm_sj,
    case whereis(Resource) of
        undefined ->
            [];
        _ ->
            Stats = sidejob_resource_stats:stats(Resource),
            Map = [{node_put_fsm_active,         usage},
                   {node_put_fsm_active_60s,     usage_60s},
                   {node_put_fsm_in_rate,        in_rate},
                   {node_put_fsm_out_rate,       out_rate},
                   {node_put_fsm_rejected,       rejected},
                   {node_put_fsm_rejected_60s,   rejected_60s},
                   {node_put_fsm_rejected_total, rejected_total}],
            [{Rename, proplists:get_value(Stat, Stats)} || {Rename, Stat} <- Map]
    end.

%% @spec cpu_stats() -> proplist()
%% @doc Get stats on the cpu, as given by the cpu_sup module
%%      of the os_mon application.
cpu_stats() ->
    [{cpu_nprocs, cpu_sup:nprocs()},
     {cpu_avg1, cpu_sup:avg1()},
     {cpu_avg5, cpu_sup:avg5()},
     {cpu_avg15, cpu_sup:avg15()}].

%% @spec mem_stats() -> proplist()
%% @doc Get stats on the memory, as given by the memsup module
%%      of the os_mon application.
mem_stats() ->
    {Total, Alloc, _} = memsup:get_memory_data(),
    [{mem_total, Total},
     {mem_allocated, Alloc}].

%% @spec disk_stats() -> proplist()
%% @doc Get stats on the disk, as given by the disksup module
%%      of the os_mon application.
disk_stats() ->
    [{disk, disksup:get_disk_data()}].

system_stats() ->
    [{nodename, node()},
     {connected_nodes, nodes()},
     {sys_driver_version, list_to_binary(erlang:system_info(driver_version))},
     {sys_global_heaps_size, safe_global_heap_size()},
     {sys_heap_type, erlang:system_info(heap_type)},
     {sys_logical_processors, erlang:system_info(logical_processors)},
     {sys_otp_release, list_to_binary(erlang:system_info(otp_release))},
     {sys_process_count, erlang:system_info(process_count)},
     {sys_smp_support, erlang:system_info(smp_support)},
     {sys_system_version, list_to_binary(string:strip(erlang:system_info(system_version), right, $\n))},
     {sys_system_architecture, list_to_binary(erlang:system_info(system_architecture))},
     {sys_threads_enabled, erlang:system_info(threads)},
     {sys_thread_pool_size, erlang:system_info(thread_pool_size)},
     {sys_wordsize, erlang:system_info(wordsize)}].

safe_global_heap_size() ->
    try erlang:system_info(global_heaps_size) of
        N -> N
    catch
        error:badarg ->
            deprecated
    end.

app_stats() ->
    [{list_to_atom(atom_to_list(A) ++ "_version"), list_to_binary(V)}
     || {A,_,V} <- application:which_applications()].

memory_stats() ->
    [{list_to_atom("memory_" ++ atom_to_list(K)), V} || {K,V} <- erlang:memory()].

ring_stats() ->
    {ok, R} = riak_core_ring_manager:get_my_ring(),
    [{ring_members, riak_core_ring:all_members(R)},
     {ring_num_partitions, riak_core_ring:num_partitions(R)},
     {ring_ownership, list_to_binary(lists:flatten(io_lib:format("~p", [dict:to_list(
                        lists:foldl(fun({_P, N}, Acc) ->
                                            case dict:find(N, Acc) of
                                                {ok, V} ->
                                                    dict:store(N, V+1, Acc);
                                                error ->
                                                    dict:store(N, 1, Acc)
                                            end
                                    end, dict:new(), riak_core_ring:all_owners(R)))])))}].


config_stats() ->
    [{ring_creation_size, app_helper:get_env(riak_core, ring_creation_size)},
     {storage_backend, app_helper:get_env(riak_kv, storage_backend)}].

%% @doc add the pipe stats to the blob in a style consistent
%% with those stats already in the blob
pipe_stats() ->
    lists:flatten([bc_stat(Name, Val) || {Name, Val} <- riak_pipe_stat:get_stats()]).

%% old style blob stats don't have the app name
%% and they have underscores, not commas
bc_stat(Name, Val) when is_tuple(Name) ->
    StatName = join(tl(tuple_to_list(Name))),
    bc_stat_val(StatName, Val);
bc_stat(Name, Val) ->
    bc_stat_val(Name, Val).

%% Old style stats don't have tuple lists as values
%% they have an entry per element in the complex stats tuple list
%% so a spiral with both a count and a one minute reading
%% would be two stats, of NAME_count and NAME_one
%% let's do that
bc_stat_val(StatName, Val) when is_list(Val) ->
    [{join([StatName, ValName]), ValVal} || {ValName, ValVal} <- Val];
bc_stat_val(StatName, Val) ->
    {StatName, Val}.

%% Leveldb stats are a last minute new edition to the blob
level_stats() ->
    Stats = riak_core_stat_q:get_stats([riak_kv, vnode, backend, leveldb, read_block_error]),
    [{join(lists:nthtail(3, tuple_to_list(Name))), Val} || {Name, Val} <- Stats].

%% Read repair stats are a new edition to the legacy blob.
%% Added to the blob since the stat query interface was not ready for the 1.3
%% release.
%% The read repair stats are stored as dimensions with
%% the key {riak_kv, node, gets, read_repairs, Node, Type, Reason}.
%% The CSEs are only interested in aggregations of Type and Reason
%% which are elements 6 and 7 in the key.
read_repair_stats() ->
    aggregate(read_repairs, [riak_kv, node, gets, read_repairs, '_', '_', '_'], [6,7]).

%% TODO generalise for riak_core_stat_q
%% aggregates spiral values for stats retrieved by `Query'
%% aggregates by the key field(s) indexed at `Fields'
%% produces a flat list of `BaseName_NameOfFieldAtIndex[_count]'
%% to fit in with the existing naming convention in the legacy stat blob
aggregate(BaseName, Query, Fields) ->
    Stats = riak_core_stat_q:get_stats(Query),
    Aggregates = do_aggregate(Stats, Fields),
    FlatStats = flatten_aggregate_stats(BaseName, Aggregates),
    lists:flatten(FlatStats).

do_aggregate(Stats, Fields) ->
    lists:foldl(fun({Name, [{count, C0}, {one, O0}]}, Acc) ->
                        Key = key_from_fields(Name, Fields),
                        [{count, C}, {one, O}] = case orddict:find(Key, Acc) of
                                                     error -> [{count, 0}, {one, 0}];
                                                     {ok, V} -> V
                                                 end,
                        orddict:store(Key, [{count, C+C0}, {one, O+O0}], Acc)
                end,
                orddict:new(),
                Stats).

%% Generate a dictionary key for the running
%% aggregation using key `Name' elements at index(es)
%% in `Fields'
key_from_fields(Name, Fields) ->
    Key = [element(N, Name) || N <- Fields],
    join(Key).

%% Folds over the aggregate nested dictionaries to create
%% a flat list of stats whose names are made by
%% joining key names to `BaseName'
flatten_aggregate_stats(BaseName, Aggregates) ->
    orddict:fold(fun(K, V, Acc) when not is_list(V) ->
                         [{join([BaseName, K]), V}|Acc];
                    (K, V, Acc)  ->
                         [flatten_aggregate_stats(join([BaseName, K]), V)|Acc]
                 end,
                 [],
                 Aggregates).

%% Join a list of atoms into a single atom
%% with elements separated by '_'
join(L) ->
    join(L, <<>>).

join([], Bin) ->
    binary_to_atom(Bin, latin1);
join([Atom|Rest], <<>>) ->
    Bin2 = atom_to_binary(Atom, latin1),
    join(Rest, <<Bin2/binary>>);
join([Atom|Rest], Bin) ->
    Bin2 = atom_to_binary(Atom, latin1),
    join(Rest, <<Bin/binary, $_, Bin2/binary>>).
