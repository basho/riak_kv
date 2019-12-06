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
%% those using the `/stats' endpoint and `riak-admin status'. This module
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
%%</dd>
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
-compile(nowarn_export_all).

%% @spec produce_stats() -> proplist()
%% @doc Produce a proplist-formatted view of the current aggregation
%%      of stats.
produce_stats() ->
    lists:append(
      [lists:flatten(legacy_stats()),
       read_repair_stats(),
       level_stats(),
       disk_stats(),
       ring_stats(),
       config_stats(),
       app_stats()
      ]).

%% Stats in folsom are stored with tuples as keys, the
%% tuples mimic an hierarchical structure. To be free of legacy
%% naming constraints the new names are not simply the old names
%% with commas for underscores. Uses legacy_stat_map to generate
%% legacys stats from the new list of stats.
legacy_stats() ->
    riak_kv_status:get_stats(console).

%% @doc legacy stats uses multifield stats for multiple stats
%% don't calculate the same stat many times
get_stat(Name, Type, Cache) ->
    get_stat(Name, Type, Cache, fun(S) -> S end).

get_stat(Name, Type, Cache, ValFun) ->
    case proplists:get_value(Name, Cache) of
        undefined ->
            Value = case riak_core_stat_q:calc_stat({Name, Type}) of
			unavailable -> [];
			Stat        -> Stat
		    end,
	    {ValFun(Value), [{Name, Value} | Cache]};
        Cached -> {ValFun(Cached), Cache}
    end.

%% @spec disk_stats() -> proplist()
%% @doc Get stats on the disk, as given by the disksup module
%%      of the os_mon application.
disk_stats() ->
    [{disk, disksup:get_disk_data()}].

otp_release() ->
    list_to_binary(erlang:system_info(otp_release)).

sys_driver_version() ->
    list_to_binary(erlang:system_info(driver_version)).

system_version() ->
    list_to_binary(string:strip(erlang:system_info(system_version), right, $\n)).

system_architecture() ->
    list_to_binary(erlang:system_info(system_architecture)).

%% Count up all monitors, unfortunately has to obtain process_info
%% from all processes to work it out.
sys_monitor_count() ->
    lists:foldl(fun(Pid, Count) ->
                        case erlang:process_info(Pid, monitors) of
                            {monitors, Mons} ->
                                Count + length(Mons);
                            _ ->
                                Count
                        end
                end, 0, processes()).

app_stats() ->
    [{list_to_atom(atom_to_list(A) ++ "_version"), list_to_binary(V)}
     || {A,_,V} <- application:which_applications()].

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

%% Leveldb stats are a last minute new edition to the blob
level_stats() ->
    Stats = riak_core_stat_q:get_stats([riak_kv, vnode, backend, leveldb, read_block_error]),
    [{join(lists:nthtail(3, Name)), Val} || {Name, Val} <- Stats].

%% Read repair stats are a new edition to the legacy blob.
%% Added to the blob since the stat query interface was not ready for the 1.3
%% release.
%% The read repair stats are stored as dimensions with
%% the key {riak_kv, node, gets, read_repairs, Node, Type, Reason}.
%% The CSEs are only interested in aggregations of Type and Reason
%% which are elements 6 and 7 in the key.
read_repair_stats() ->
    Pfx = riak_stat:prefix(),
    aggregate(read_repairs, [Pfx, riak_kv, node, gets, read_repairs, '_', '_', '_'], [7,8]).

%% TODO generalise for riak_core_stat_q
%% aggregates spiral values for stats retrieved by `Query'
%% aggregates by the key field(s) indexed at `Fields'
%% produces a flat list of `BaseName_NameOfFieldAtIndex[_count]'
%% to fit in with the existing naming convention in the legacy stat blob
aggregate(BaseName, Query, Fields) ->
    Stats = riak_kv_stat:get_values(Query),
    Aggregates = do_aggregate(Stats, Fields),
    FlatStats = flatten_aggregate_stats(BaseName, Aggregates),
    lists:flatten(FlatStats).

do_aggregate(Stats, Fields) ->
    lists:foldl(fun({Name, [{count, C0}, {one, O0}]}, Acc) ->
                        Key = key_from_fields(list_to_tuple(Name), Fields),
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
