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

%% @doc riak_kv_stat is a module for aggregating
%%      stats about the Riak node on which it is runing.
%%
%%      Update each stat with the exported function update/1. Add
%%      a new stat to the internal stats/0 func to register a new stat with
%%      folsom.
%%
%%      Get the latest aggregation of stats with the exported function
%%      get_stats/0. Or use folsom_metrics:get_metric_value/1
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

-behaviour(gen_server).

%% API
-export([start_link/0, get_stats/0,
         update/1, register_stats/0, produce_stats/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(APP, riak_kv).

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

register_stats() ->
    [register_stat({?APP, Name}, Type) || {Name, Type} <- stats()],
    riak_core_stat_cache:register_app(?APP, {?MODULE, produce_stats, []}).

%% @spec get_stats() -> proplist()
%% @doc Get the current aggregation of stats.
get_stats() ->
    case riak_core_stat_cache:get_stats(?APP) of
        {ok, Stats, _TS} ->
            Stats;
        Error -> Error
    end.

update(Arg) ->
    gen_server:cast(?SERVER, {update, Arg}).

%% gen_server

init([]) ->
    {ok, ok}.

handle_call(_Req, _From, State) ->
    {reply, ok, State}.

handle_cast({update, Arg}, State) ->
    update1(Arg),
    {noreply, State};
handle_cast(_Req, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% @doc Update the given stat
update1(vnode_get) ->
    folsom_metrics:notify_existing_metric({?APP, vnode_gets}, 1, spiral);
update1(vnode_put) ->
    folsom_metrics:notify_existing_metric({?APP, vnode_puts}, 1, spiral);
update1(vnode_index_read) ->
    folsom_metrics:notify_existing_metric({?APP, vnode_index_reads}, 1, spiral);
update1({vnode_index_write, PostingsAdded, PostingsRemoved}) ->
    folsom_metrics:notify_existing_metric({?APP, vnode_index_writes}, 1, spiral),
    folsom_metrics:notify_existing_metric({?APP, vnode_index_writes_postings}, PostingsAdded, spiral),
    folsom_metrics:notify_existing_metric({?APP, vnode_index_deletes_postings}, PostingsRemoved, spiral);
update1({vnode_index_delete, Postings}) ->
    folsom_metrics:notify_existing_metric({?APP, vnode_index_deletes}, Postings, spiral),
    folsom_metrics:notify_existing_metric({?APP, vnode_index_deletes_postings}, Postings, spiral);
update1({get_fsm, Bucket, Microsecs, undefined, undefined, PerBucket}) ->
    folsom_metrics:notify_existing_metric({?APP, node_gets}, 1, spiral),
    folsom_metrics:notify_existing_metric({?APP, node_get_fsm_time}, Microsecs, histogram),
    do_get_bucket(PerBucket, {Bucket, Microsecs, undefined, undefined});
update1({get_fsm, Bucket, Microsecs, NumSiblings, ObjSize, PerBucket}) ->
    folsom_metrics:notify_existing_metric({?APP, node_gets}, 1, spiral),
    folsom_metrics:notify_existing_metric({?APP, node_get_fsm_time}, Microsecs, histogram),
    folsom_metrics:notify_existing_metric({?APP, node_get_fsm_siblings}, NumSiblings, histogram),
    folsom_metrics:notify_existing_metric({?APP, node_get_fsm_objsize}, ObjSize, histogram),
    do_get_bucket(PerBucket, {Bucket, Microsecs, NumSiblings, ObjSize});
update1({put_fsm_time, Bucket,  Microsecs, PerBucket}) ->
    folsom_metrics:notify_existing_metric({?APP, node_puts}, 1, spiral),
    folsom_metrics:notify_existing_metric({?APP, node_put_fsm_time}, Microsecs, histogram),
    do_put_bucket(PerBucket, {Bucket, Microsecs});
update1(read_repairs) ->
    folsom_metrics:notify_existing_metric({?APP, read_repairs}, 1, spiral);
update1(coord_redir) ->
    folsom_metrics:notify_existing_metric({?APP, coord_redirs_total}, {inc, 1}, counter);
update1(mapper_start) ->
    folsom_metrics:notify_existing_metric({?APP, mapper_count}, {inc, 1}, counter);
update1(mapper_end) ->
    folsom_metrics:notify_existing_metric({?APP, mapper_count}, {dec, 1}, counter);
update1(precommit_fail) ->
    folsom_metrics:notify_existing_metric({?APP, precommit_fail}, {inc, 1}, counter);
update1(postcommit_fail) ->
    folsom_metrics:notify_existing_metric({?APP, postcommit_fail}, {inc, 1}, counter).

%% private
%%  per bucket get_fsm stats
do_get_bucket(false, _) ->
    ok;
do_get_bucket(true, {Bucket, Microsecs, NumSiblings, ObjSize}=Args) ->
    BucketAtom = binary_to_atom(Bucket, latin1),
    case (catch folsom_metrics:notify_existing_metric({?APP, join(node_gets, BucketAtom)}, 1, spiral)) of
        ok ->
            [folsom_metrics:notify_existing_metric({?APP, join(Stat, BucketAtom)}, Arg, histogram)
             || {Stat, Arg} <- [{node_get_fsm_time, Microsecs},
                                {node_get_fsm_siblings, NumSiblings},
                                {node_get_fsm_objsize, ObjSize}], Arg /= undefined];
        {'EXIT', _} ->
            folsom_metrics:new_spiral({?APP, join(node_gets, BucketAtom)}),
            [register_stat({?APP, join(Stat, BucketAtom)}, histogram) || Stat <- [node_get_fsm_time,
                                                                                  node_get_fsm_siblings,
                                                                                  node_get_fsm_objsize]],
            do_get_bucket(true, Args)
    end.

%% per bucket put_fsm stats
do_put_bucket(false, _) ->
    ok;
do_put_bucket(true, {Bucket, Microsecs}=Args) ->
    BucketAtom = binary_to_atom(Bucket, latin1),
    case (catch folsom_metrics:notify_existing_metric({?APP, join(node_puts, BucketAtom)}, 1, spiral)) of
        ok ->
            folsom_metrics:notify_existing_metric({?APP, join(node_put_fsm_time, BucketAtom)}, Microsecs, histogram);
        {'EXIT', _} ->
            register_stat({?APP, join(node_puts, BucketAtom)}, spiral),
            register_stat({?APP, join(node_put_fsm_time, BucketAtom)}, histogram),
            do_put_bucket(true, Args)
    end.

%% @spec produce_stats(state(), integer()) -> proplist()
%% @doc Produce a proplist-formatted view of the current aggregation
%%      of stats.
produce_stats() ->
    lists:append(
      [lists:flatten([backwards_compat(Name, Type, get_stat({?APP, Name}, Type)) || {Name, Type} <- stats()]),
       backwards_compat_pb(riak_api_stat:produce_stats()),
       cpu_stats(),
       mem_stats(),
       disk_stats(),
       system_stats(),
       ring_stats(),
       config_stats(),
       app_stats(),
       memory_stats()
      ]).

get_stat(Name, histogram) ->
    folsom_metrics:get_histogram_statistics(Name);
get_stat(Name, _Type) ->
    folsom_metrics:get_metric_value(Name).

backwards_compat_pb({riak_api, Stats}) ->
    [{pbc_active, proplists:get_value(pbc_connects_active, Stats)} |
     backwards_compat(pbc_connects, spiral, proplists:get_value(pbc_connects, Stats))].

backwards_compat(Name, spiral, Stats) ->
    [{Name, trunc(proplists:get_value(one, Stats))},
     {join(Name, total), proplists:get_value(count, Stats)}];
backwards_compat(mapper_count, counter, Stats) ->
    {executing_mappers, Stats};
backwards_compat(pbc_connects_active, counter, Stats) ->
    {pbc_active, Stats};
backwards_compat(Name, counter, Stats) ->
    {Name, Stats};
backwards_compat(Name, histogram, Stats) ->
    backwards_compat_histo(Name, Stats).

backwards_compat_histo(Name, Stats) ->
    Percentiles = proplists:get_value(percentile, Stats),
    [{join(Name, mean), trunc(proplists:get_value(arithmetic_mean, Stats))},
     {join(Name, median), trunc(proplists:get_value(median, Stats))},
     {join(Name, '95'), trunc(proplists:get_value(95, Percentiles))},
     {join(Name, '99'), trunc(proplists:get_value(99, Percentiles))},
     {join(Name, '100'), trunc(proplists:get_value(max, Stats))}].

join(Atom1, Atom2) ->
    Bin1 = atom_to_binary(Atom1, latin1),
    Bin2 = atom_to_binary(Atom2, latin1),
    binary_to_atom(<<Bin1/binary, $_, Bin2/binary>>, latin1).

stats() ->
    [{vnode_gets, spiral},
     {vnode_puts, spiral},
     {vnode_index_reads, spiral},
     {vnode_index_writes, spiral},
     {vnode_index_writes_postings, spiral},
     {vnode_index_deletes, spiral},
     {vnode_index_deletes_postings, spiral},
     {node_gets, spiral},
     {node_get_fsm_siblings, histogram},
     {node_get_fsm_objsize, histogram},
     {node_get_fsm_time, histogram},
     {node_puts, spiral},
     {node_put_fsm_time, histogram},
     {read_repairs, spiral},
     {coord_redirs_total, counter},
     {mapper_count, counter},
     {precommit_fail, counter},
     {postcommit_fail, counter}].

register_stat(Name, spiral) ->
    folsom_metrics:new_spiral(Name);
register_stat(Name, counter) ->
    folsom_metrics:new_counter(Name);
register_stat(Name, histogram) ->
    %% get the global default histo type
    {SampleType, SampleArgs} = get_sample_type(Name),
    folsom_metrics:new_histogram(Name, SampleType, SampleArgs).

get_sample_type(Name) ->
    SampleType0 = app_helper:get_env(riak_kv, stat_sample_type, {slide_uniform, {60, 1028}}),
    app_helper:get_env(riak_kv, Name, SampleType0).

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
