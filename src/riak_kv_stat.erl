%% -------------------------------------------------------------------
%%
%% riak_stat: collect, aggregate, and provide stats about the local node
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

%% @doc riak_kv_stat provides stat_specs to the riak_core_metric stat
%%      system for aggregating stats about the Riak node on which it
%%      is running.
%%
%%      Update each stat with the exported function update/1.  Modify
%%      the function stat_specs/0 to add processes for new stats.
%%
%%      Get the latest aggregation of stats with the exported function
%%      get_stats/0.  Modify the internal function produce_stats/1 to
%%      change which stats are represented. Modify the 'display' entry
%%      in a stat_spec args proplist to modify how that stat is
%%      displayed.
%%
%%      Riak will start a process per stat in
%%      riak_kv_stat:stat_specs/0 for you, if you have specified
%%      {riak_kv_stat, true} in your config .erlenv file.
%%
%%      Current stats:
%%<dl><dt>  vnode_gets
%%</dt><dd> Total number of gets handled by all vnodes on this node
%%          in the last minute.
%%</dd><dd> update(vnode_get)
%%
%%</dd><dt> vnode_puts
%%</dt><dd> Total number of puts handled by all vnodes on this node
%%          in the last minute.
%%</dd><dd> update(vnode_put)
%%
%%</dd><dt> vnode_index_reads
%%</dt><dd> The number of index reads handled by all vnodes on this node.
%%          Each query counts as an index read.
%%</dd><dd> update(vnode_index_read)
%%
%%</dd><dt> vnode_index_writes
%%</dt><dd> The number of batched writes handled by all vnodes on this node.
%%</dd><dd> update({vnode_index_write, PostingsAdded, PostingsRemoved})
%%
%%</dd><dt> vnode_index_writes_postings
%%</dt><dd> The number of postings written to all vnodes on this node.
%%</dd><dd> update({vnode_index_write, PostingsAdded, PostingsRemoved})
%%
%%</dd><dt> vnode_index_deletes
%%</dt><dd> The number of batched writes handled by all vnodes on this node.
%%</dd><dd> update({vnode_index_delete, PostingsRemoved})
%%
%%</dd><dt> vnode_index_deletes_postings
%%</dt><dd> The number of postings written to all vnodes on this node.
%%</dd><dd> update({vnode_index_delete, PostingsRemoved})
%%
%%</dd><dt> node_gets
%%</dt><dd> Number of gets coordinated by this node in the last
%%          minute.
%%</dd><dd> update({get_fsm, _Bucket, Microseconds, NumSiblings, ObjSize})
%%
%%</dd><dt> node_get_fsm_siblings
%%</dt><dd> Stats about number of siblings per object in the last minute.
%%</dd><dd> Updated via node_gets.
%%
%%</dd><dt> node_get_fsm_objsize
%%</dt><dd> Stats about object size over the last minute. The object
%%          size is an estimate calculated by summing the size of the
%%          bucket name, key name, and serialized vector clock, plus
%%          the value and serialized metadata of each sibling.
%%</dd><dd> Updated via node_gets.
%%
%%</dd><dt> node_get_fsm_time_mean
%%</dt><dd> Mean time, in microseconds, between when a riak_kv_get_fsm is
%%          started and when it sends a reply to the client, for the
%%          last minute.
%%</dd><dd> update({get_fsm_time, Microseconds})
%%
%%</dd><dt> node_get_fsm_time_median
%%</dt><dd> Median time, in microseconds, between when a riak_kv_get_fsm
%%          is started and when it sends a reply to the client, for
%%          the last minute.
%%</dd><dd> update({get_fsm_time, Microseconds})
%%
%%</dd><dt> node_get_fsm_time_95
%%</dt><dd> Response time, in microseconds, met or beaten by 95% of
%%          riak_kv_get_fsm executions.
%%</dd><dd> update({get_fsm_time, Microseconds})
%%
%%</dd><dt> node_get_fsm_time_99
%%</dt><dd> Response time, in microseconds, met or beaten by 99% of
%%          riak_kv_get_fsm executions.
%%</dd><dd> update({get_fsm_time, Microseconds})
%%
%%</dd><dt> node_get_fsm_time_100
%%</dt><dd> Response time, in microseconds, met or beaten by 100% of
%%          riak_kv_get_fsm executions.
%%</dd><dd> update({get_fsm_time, Microseconds})
%%
%%</dd><dt> node_puts
%%</dt><dd> Number of puts coordinated by this node in the last
%%          minute.
%%</dd><dd> update({put_fsm_time, Microseconds})
%%
%%</dd><dt> node_put_fsm_time_mean
%%</dt><dd> Mean time, in microseconds, between when a riak_kv_put_fsm is
%%          started and when it sends a reply to the client, for the
%%          last minute.
%%</dd><dd> update({put_fsm_time, Microseconds})
%%
%%</dd><dt> node_put_fsm_time_median
%%</dt><dd> Median time, in microseconds, between when a riak_kv_put_fsm
%%          is started and when it sends a reply to the client, for
%%          the last minute.
%%</dd><dd> update({put_fsm_time, Microseconds})
%%
%%</dd><dt> node_put_fsm_time_95
%%</dt><dd> Response time, in microseconds, met or beaten by 95% of
%%          riak_kv_put_fsm executions.
%%</dd><dd> update({put_fsm_time, Microseconds})
%%
%%</dd><dt> node_put_fsm_time_99
%%</dt><dd> Response time, in microseconds, met or beaten by 99% of
%%          riak_kv_put_fsm executions.
%%</dd><dd> update({put_fsm_time, Microseconds})
%%
%%</dd><dt> node_put_fsm_time_100
%%</dt><dd> Response time, in microseconds, met or beaten by 100% of
%%          riak_kv_put_fsm executions.
%%</dd><dd> update({put_fsm_time, Microseconds})
%%
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
-module(riak_kv_stat).

%% API
-export([get_stats/0, update/1]).

%% stats
-export([stat_specs/0]).

-define(APP, riak_kv).

-spec stat_specs() -> riak_core_metric:stat_specs().
stat_specs() ->
    [{vnode_gets, [{type, meter}, {group, vnode}]},
     {vnode_puts, [{type, meter}, {group, vnode}]},
     {vnode_gets_total, [{type, counter}, {group, vnode}]},
     {vnode_puts_total, [{type, counter}, {group, vnode}]},
     {vnode_index_reads, [{type, meter}, {group, vnode}]},
     {vnode_index_reads_total, [{type, counter}, {group, vnode}]},
     {vnode_index_writes, [{type, meter}, {group, vnode}]},
     {vnode_index_writes_total, [{type, counter}, {group, vnode}]},
     {vnode_index_writes_postings, [{type, meter}, {group, vnode}]},
     {vnode_index_writes_postings_total, [{type, counter}, {group, vnode}]},
     {vnode_index_deletes, [{type, meter}, {group, vnode}]},
     {vnode_index_deletes_total, [{type, counter}, {group, vnode}]},
     {vnode_index_deletes_postings, [{type, meter}, {group, vnode}]},
     {vnode_index_deletes_postings_total, [{type, counter}, {group, vnode}]},
     {node_gets_total, [{type, counter}, {group, node}]},
     {node_puts_total, [{type, counter}, {group, node}]},
     {node_get_fsm_siblings, [{type, histogram}, {group, node},
                              {presentation,
                               [{legacy, [{args, {0, 1000, 1000, up}},
                                          {fields,[mean, median, '95', '99', '100']}]}]}]},
     {node_get_fsm_objsize, [{type, histogram}, {group, node},
                             {presentation,
                              [{legacy, [{args, { 0, 16 * 1024 * 1024, 16 * 1024, down}},
                                         {fields,[mean, median, '95', '99', '100']}]}]}]},
     {get_fsm_time, [{type, histogram}, {group, node},
                     {presentation,
                      [{legacy, [{args, {0, 5000000, 20000, down}},
                                 {fields,[{count, node_gets}, mean, median, '95', '99', '100']},
                                 {prefix, node}]}]}]},
     {put_fsm_time, [{type, histogram}, {group, node},
                     {presentation,
                      [{legacy, [{args, {0, 5000000, 20000, down}},
                                 {fields,[{count, node_puts}, mean, median, '95', '99', '100']},
                                 {prefix, node}]}]}]},
     {pbc_connects, [{type, meter}, {group, pbc}]},
     {pbc_connects_total, [{type, counter}, {group, pbc}]},
     {pbc_active, [{type, counter}, {group, pbc}]},
     {read_repairs, [{type, meter}, {group, node}]},
     {read_repairs_total, [{type, counter}, {group, node}]},
     {coord_redirs_total, [{type, counter}, {group, node}]},
     {mapper_count, [{type, counter}, {group, mapper},
                    {presentation,
                    [{legacy, {display_name, executing_mappers}}]}]},
     {precommit_fail, [{type, counter}, {group, node}]},
     {postcommit_fail, [{type, counter}, {group, node}]}
    ].

%% @spec get_stats() -> proplist()
%% @doc Get the current aggregation of stats.
get_stats() ->
   produce_stats(legacy).

%% @spec update(term()) -> ok
%% @doc Update the given stat.
update(Stat) ->
    update(Stat, slide:moment()).

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------

%% @spec update(Stat::term(), integer(), state()) -> state()
%% @doc Update the given stat in State, returning a new State.
update(vnode_get, Moment) ->
    riak_core_metric_meter:increment(?APP, vnode_gets, Moment),
    riak_core_metric_counter:increment(?APP, vnode_gets_total);
update(vnode_put, Moment) ->
    riak_core_metric_meter:increment(?APP, vnode_puts, Moment),
    riak_core_metric_counter:increment(?APP, vnode_puts_total);
update(vnode_index_read, Moment) ->
    riak_core_metric_meter:increment(?APP, vnode_index_reads, Moment),
    riak_core_metric_counter:increment(?APP, vnode_index_reads_total);
update({vnode_index_write, PostingsAdded, PostingsRemoved}, Moment) ->
    riak_core_metric_meter:increment(?APP, vnode_index_writes, Moment),
    riak_core_metric_counter:increment(?APP, vnode_index_writes_total),
    riak_core_metric_meter:increment(?APP, vnode_index_writes_postings, PostingsAdded, Moment),
    riak_core_metric_counter:increment(?APP, vnode_index_writes_postings_total, PostingsAdded),
    riak_core_metric_meter:increment(?APP, vnode_index_deletes_postings, PostingsRemoved, Moment),
    riak_core_metric_counter:increment(?APP, vnode_index_deletes_postings_total, PostingsRemoved);
update({vnode_index_delete, Postings}, Moment) ->
    riak_core_metric_meter:increment(?APP, vnode_index_deletes, Moment),
    riak_core_metric_counter:increment(?APP, vnode_index_deletes_total),
    riak_core_metric_meter:increment(?APP, vnode_index_deletes_postings, Postings, Moment),
    riak_core_metric_counter:increment(?APP, vnode_index_deletes_postings_total, Postings);
update({get_fsm, _Bucket, Microsecs, undefined, undefined}, Moment) ->
    riak_core_metric_histogram:increment(?APP, get_fsm_time, Microsecs,  Moment),
    riak_core_metric_counter:increment(?APP, node_gets_total);
update({get_fsm, _Bucket, Microsecs, NumSiblings, ObjSize}, Moment) ->
    riak_core_metric_histogram:increment(?APP, get_fsm_time, Microsecs,  Moment),
    riak_core_metric_counter:increment(?APP, node_gets_total),
    riak_core_metric_histogram:increment(?APP, node_get_fsm_siblings, NumSiblings, Moment),
    riak_core_metric_histogram:increment(?APP, node_get_fsm_objsize, ObjSize, Moment);
update({get_fsm_time, Microsecs}, Moment) ->
    update({get_fsm, undefined, Microsecs, undefined, undefined}, Moment);
update({put_fsm_time, Microsecs}, Moment) ->
    riak_core_metric_histogram:increment(?APP, put_fsm_time, Microsecs, Moment),
    riak_core_metric_counter:increment(?APP, node_puts_total);
update(pbc_connect, Moment) ->
    riak_core_metric_meter:increment(?APP, pbc_connects, Moment),
    riak_core_metric_counter:increment(?APP, pbc_connects_total),
    riak_core_metric_counter:increment(?APP, pbc_active);
update(pbc_disconnect, _Moment) ->
    riak_core_metric_counter:decrement(?APP, pbc_active);
update(read_repairs, Moment) ->
    riak_core_metric_meter:increment(?APP, read_repairs, Moment),
    riak_core_metric_counter:increment(?APP, read_repairs_total);
update(coord_redir, _Moment) ->
    riak_core_metric_counter:increment(?APP, coord_redirs_total);
update(mapper_start, _Moment) ->
    riak_core_metric_counter:increment(?APP, mapper_count);
update(mapper_end, _Moment) ->
    riak_core_metric_counter:decrement(?APP, mapper_count);
update(precommit_fail, _Moment) ->
    riak_core_metric_counter:increment(?APP, precommit_fail);
update(postcommit_fail, _Moment) ->
    riak_core_metric_counter:increment(?APP, postcommit_fail);
update(Stat, _) ->
    lager:warning("Update called for unkown stat: ~p", [Stat]).

%% @spec produce_stats(integer()) -> proplist()
%% @doc Produce a proplist-formatted view of the current aggregation
%%      of stats.
produce_stats(Presentation) ->
    lists:append(
      [produce_stats(Presentation, vnode),
       produce_stats(Presentation, node),
       cpu_stats(),
       mem_stats(),
       disk_stats(),
       system_stats(),
       ring_stats(),
       config_stats(),
       pbc_stats(Presentation),
       app_stats(),
       produce_stats(Presentation, mapper),
       memory_stats()
      ]).

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
     {sys_global_heaps_size, erlang:system_info(global_heaps_size)},
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

%% @spec pbc_stats(integer(), state()) -> proplist()
%% @doc Get stats on the disk, as given by the disksup module
%%      of the os_mon application.
pbc_stats(Presentation) ->
    case whereis(riak_kv_pb_socket_sup) of
        undefined ->
            [];
        _ ->  produce_stats(Presentation, pbc)
    end.

%% @doc produce a proplist for the given Group of stats,
%% formatted for the Given Presentation.
-spec produce_stats(atom(), atom()) -> [{atom(), integer()}].
produce_stats(Presentation, Group) when is_atom(Presentation), is_atom(Group) ->
    Stats = [riak_core_metric_proc:value(?APP, Name, Presentation) || {Name, Spec} <- stat_specs(), lists:keyfind(Group, 2, Spec) /= false],
    lists:flatten(Stats).
