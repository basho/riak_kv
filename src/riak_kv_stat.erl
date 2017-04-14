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
%%      get_stats/0. Or use folsom_metrics:get_metric_value/1,
%%      or riak_core_stat_q:get_stats/1.
%%

-module(riak_kv_stat).

-behaviour(gen_server).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([start_link/0, get_stats/0,
         update/1, perform_update/1, register_stats/0, unregister_vnode_stats/1, produce_stats/0,
         leveldb_read_block_errors/0, stat_update_error/3, stop/0]).
-export([track_bucket/1, untrack_bucket/1]).
-export([active_gets/0, active_puts/0]).
-export([value/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3, monitor_loop/1]).

-record(state, {repair_mon, monitors}).

-define(SERVER, ?MODULE).
-define(APP, riak_kv).
-define(PFX, riak_core_stat:prefix()).

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

register_stats() ->
    riak_core_stat:register_stats(?APP, stats()).

unregister_vnode_stats(Index) ->
    unregister_per_index(gets, Index),
    unregister_per_index(puts, Index).

%% @spec get_stats() -> proplist()
%% @doc Get the current aggregation of stats.
get_stats() ->
    riak_kv_wm_stats:get_stats().


%% Creation of a dynamic stat _must_ be serialized.
register_stat(Name, Type) ->
    do_register_stat(Name, Type).
%% gen_server:call(?SERVER, {register, Name, Type}).

update(Arg) ->
    maybe_dispatch_to_sidejob(erlang:module_loaded(riak_kv_stat_sj), Arg).

maybe_dispatch_to_sidejob(true, Arg) ->
    riak_kv_stat_worker:update(Arg);
maybe_dispatch_to_sidejob(false, Arg) ->
    try perform_update(Arg) catch Class:Error ->
       stat_update_error(Arg, Class, Error)
    end,
    ok.

stat_update_error(Arg, Class, Error) ->
    lager:debug("Failed to update stat ~p due to (~p) ~p.", [Arg, Class, Error]).

%% @doc
%% Callback used by a {@link riak_kv_stat_worker} to perform actual update
perform_update(Arg) ->
    do_update(Arg).

track_bucket(Bucket) when is_binary(Bucket) ->
    riak_core_bucket:set_bucket(Bucket, [{stat_tracked, true}]).

untrack_bucket(Bucket) when is_binary(Bucket) ->
    riak_core_bucket:set_bucket(Bucket, [{stat_tracked, false}]).

%% The current number of active get fsms in riak
active_gets() ->
    counter_value([?PFX, ?APP, node, gets, fsm, active]).

%% The current number of active put fsms in riak
active_puts() ->
    counter_value([?PFX, ?APP, node, puts, fsm, active]).

counter_value(Name) ->
    case exometer:get_value(Name, [value]) of
	{ok, [{value, N}]} ->
	    N;
	_ ->
	    0
    end.

stop() ->
    gen_server:cast(?SERVER, stop).

%% gen_server

init([]) ->
    register_stats(),
    Me = self(),
    State = #state{monitors = [{index, spawn_link(?MODULE, monitor_loop, [index])},
                               {list, spawn_link(?MODULE, monitor_loop, [list])},
                               {list_group, spawn_link(?MODULE,
                                                            monitor_loop,
                                                            [list_group])}
                              ],
                   repair_mon = spawn_monitor(fun() -> stat_repair_loop(Me) end)},
    {ok, State}.

handle_call({register, Name, Type}, _From, State) ->
    Rep = do_register_stat(Name, Type),
    {reply, Rep, State}.

handle_cast({re_register_stat, Arg}, State) ->
    %% To avoid massive message queues
    %% riak_kv stats are updated in the calling process.
    %% See update/1.
    %% The downside is that errors updating a stat don't crash
    %% the server, so broken stats stay broken.
    %% This re-creates the same behaviour as when a broken stat
    %% crashes the gen_server by re-registering that stat.
    #state{repair_mon={Pid, _Mon}} = State,
    Pid ! {re_register_stat, Arg},
    {noreply, State};
handle_cast({monitor, Type, Pid}, State) ->
    case proplists:get_value(Type, State#state.monitors) of
        Monitor when is_pid(Monitor) ->
            Monitor ! {add_pid, Pid};
        _ -> lager:error("Couldn't find process for ~p to add monitor", [Type])
    end,
    {noreply, State};
handle_cast(stop, State) ->
    {stop, normal, State};
handle_cast(_Req, State) ->
    {noreply, State}.

handle_info({'DOWN', MonRef, process, Pid, _Cause}, State=#state{repair_mon={Pid, MonRef}}) ->
    Me = self(),
    RepairMonitor = spawn_monitor(fun() -> stat_repair_loop(Me) end),
    {noreply, State#state{repair_mon=RepairMonitor}};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% @doc Update the given stat
do_update({vnode_get, Idx, USecs}) ->
    ok = exometer:update([?PFX, ?APP, vnode, gets], 1),
    ok = create_or_update([?PFX, ?APP, vnode, gets, time], USecs, histogram),
    do_per_index(gets, Idx, USecs);
do_update({vnode_put, Idx, USecs}) ->
    ok = exometer:update([?PFX, ?APP, vnode, puts], 1),
    ok = create_or_update([?PFX, ?APP, vnode, puts, time], USecs, histogram),
    do_per_index(puts, Idx, USecs);
do_update({reap_tombstone, _Index}) ->
    ok = exometer:update([?PFX, ?APP, vnode, reap_tombstone], 1);
do_update({object_ttl_expired, _Index}) ->
    ok = exometer:update([?PFX, ?APP, vnode, object_ttl_expired], 1);
do_update(vnode_index_refresh) ->
    exometer:update([?PFX, ?APP, vnode, index, refreshes], 1);
do_update(vnode_index_read) ->
    exometer:update([?PFX, ?APP, vnode, index, reads], 1);
do_update({vnode_index_write, PostingsAdded, PostingsRemoved}) ->
    ok = exometer:update([?PFX, ?APP, vnode, index, writes], 1),
    ok = exometer:update([?PFX, ?APP, vnode, index, writes, postings], PostingsAdded),
    exometer:update([?PFX, ?APP, vnode, index, deletes, postings], PostingsRemoved);
do_update({vnode_index_delete, Postings}) ->
    ok = exometer:update([?PFX, ?APP, vnode, index, deletes], Postings),
    exometer:update([?PFX, ?APP, vnode, index, deletes, postings], Postings);
do_update({vnode_dt_update, Mod, Micros}) ->
    Type = riak_kv_crdt:from_mod(Mod),
    ok = create_or_update([?PFX, ?APP, vnode, Type, update], 1, spiral),
    create_or_update([?PFX, ?APP, vnode, Type, update, time], Micros, histogram);
do_update({riak_object_merge, undefined, Micros}) ->
    ok = exometer:update([?PFX, ?APP, object, merge], 1),
    exometer:update([?PFX, ?APP, object, merge, time], Micros);
do_update({riak_object_merge, Mod, Micros}) ->
    Type = riak_kv_crdt:from_mod(Mod),
    ok = create_or_update([?PFX, ?APP, object, Type, merge], 1, spiral),
    create_or_update([?PFX, ?APP, object, Type, merge, time], Micros, histogram);
do_update({get_fsm, Bucket, Microsecs, Stages, undefined, undefined, PerBucket, undefined}) ->
    ok = exometer:update([?PFX, ?APP, node, gets], 1),
    ok = exometer:update([?PFX, ?APP, node, gets, time], Microsecs),
    ok = do_stages([?PFX, ?APP, node, gets, time], Stages),
    do_get_bucket(PerBucket, {Bucket, Microsecs, Stages, undefined, undefined});
do_update({get_fsm, Bucket, Microsecs, Stages, NumSiblings, ObjSize, PerBucket, undefined}) ->
    ok = exometer:update([?PFX, ?APP, node, gets], 1),
    ok = exometer:update([?PFX, ?APP, node, gets, time], Microsecs),
    ok = exometer:update([?PFX, ?APP, node, gets, siblings], NumSiblings),
    ok = exometer:update([?PFX, ?APP, node, gets, objsize], ObjSize),
    ok = do_stages([?PFX, ?APP, node, gets, time], Stages),
    do_get_bucket(PerBucket, {Bucket, Microsecs, Stages, NumSiblings, ObjSize});
do_update({get_fsm, Bucket, Microsecs, Stages, undefined, undefined, PerBucket, CRDTMod}) ->
    Type = riak_kv_crdt:from_mod(CRDTMod),
    ok = create_or_update([?PFX, ?APP, node, gets, Type], 1, spiral),
    ok = create_or_update([?PFX, ?APP, node, gets, Type, time], Microsecs, histogram),
    ok = do_stages([?PFX, ?APP, node, gets, Type, time], Stages),
    do_get_bucket(PerBucket, {Bucket, Microsecs, Stages, undefined, undefined, Type});
do_update({get_fsm, Bucket, Microsecs, Stages, NumSiblings, ObjSize, PerBucket, CRDTMod}) ->
    Type = riak_kv_crdt:from_mod(CRDTMod),
    ok = create_or_update([?PFX, ?APP, node, gets, Type], 1, spiral),
    ok = create_or_update([?PFX, ?APP, node, gets, Type, time], Microsecs, histogram),
    ok = create_or_update([?PFX, ?APP, node, gets, Type, siblings], NumSiblings, histogram),
    ok = create_or_update([?PFX, ?APP, node, gets, Type, objsize], ObjSize, histogram),
    ok = do_stages([?PFX, ?APP, node, gets, Type, time], Stages),
    do_get_bucket(PerBucket, {Bucket, Microsecs, Stages, NumSiblings, ObjSize, Type});
do_update({put_fsm_time, Bucket,  Microsecs, Stages, PerBucket, undefined}) ->
    ok = exometer:update([?PFX, ?APP, node, puts], 1),
    ok = exometer:update([?PFX, ?APP, node, puts, time], Microsecs),
    ok = do_stages([?PFX, ?APP, node, puts, time], Stages),
    do_put_bucket(PerBucket, {Bucket, Microsecs, Stages});
do_update({put_fsm_time, Bucket,  Microsecs, Stages, PerBucket, CRDTMod}) ->
    Type = riak_kv_crdt:from_mod(CRDTMod),
    ok = create_or_update([?PFX, ?APP, node, puts, Type], 1, spiral),
    ok = create_or_update([?PFX, ?APP, node, puts, Type, time], Microsecs, histogram),
    ok = do_stages([?PFX, ?APP, node, puts, Type, time], Stages),
    do_put_bucket(PerBucket, {Bucket, Microsecs, Stages, Type});
do_update({read_repairs, Indices, Preflist}) ->
    ok = exometer:update([?PFX, ?APP, node, gets, read_repairs], 1),
    do_repairs(Indices, Preflist);
do_update(skipped_read_repairs) ->
    ok = exometer:update([?PFX, ?APP, node, gets, skipped_read_repairs], 1);
do_update(coord_redir) ->
    exometer:update([?PFX, ?APP, node, puts, coord_redirs], 1);
do_update(mapper_start) ->
    exometer:update([?PFX, ?APP, mapper_count], 1);
do_update(mapper_end) ->
    exometer:update([?PFX, ?APP, mapper_count], -1);
do_update(precommit_fail) ->
    exometer:update([?PFX, ?APP, precommit_fail], 1);
do_update(postcommit_fail) ->
    exometer:update([?PFX, ?APP, postcommit_fail], 1);
do_update(write_once_merge) ->
    exometer:update([?PFX, ?APP, write_once_merge], 1);
do_update({fsm_spawned, Type}) when Type =:= gets; Type =:= puts ->
    exometer:update([?PFX, ?APP, node, Type, fsm, active], 1);
do_update({fsm_exit, Type}) when Type =:= gets; Type =:= puts  ->
    exometer:update([?PFX, ?APP, node, Type, fsm, active], -1);
do_update({fsm_error, Type}) when Type =:= gets; Type =:= puts ->
    ok = do_update({fsm_exit, Type}),
    exometer:update([?PFX, ?APP, node, Type, fsm, errors], 1);
do_update({index_create, Pid}) ->
    ok = exometer:update([?PFX, ?APP, index, fsm, create], 1),
    ok = exometer:update([?PFX, ?APP, index, fsm, active], 1),
    add_monitor(index, Pid),
    ok;
do_update(index_create_error) ->
    exometer:update([?PFX, ?APP, index, fsm, create, error], 1);
do_update({list_create, Pid}) ->
    ok = exometer:update([?PFX, ?APP, list, fsm, create], 1),
    ok = exometer:update([?PFX, ?APP, list, fsm, active], 1),
    add_monitor(list, Pid),
    ok;
do_update(list_create_error) ->
    exometer:update([?PFX, ?APP, list, fsm, create, error], 1);
do_update({list_group_create, Pid}) ->
    ok = exometer:update([?PFX, ?APP, list_group, fsm, create], 1),
    ok = exometer:update([?PFX, ?APP, list_group, fsm, active], 1),
    add_monitor(list_group, Pid),
    ok;
do_update(list_group_create_error) ->
    exometer:update([?PFX, ?APP, list_group, fsm, create, error], 1);
do_update({list_group_fsm_finish_count, Count}) ->
    exometer:update([?PFX, ?APP, list_group, fsm, finish, count], Count);
do_update(list_group_fsm_finish_error) ->
    exometer:update([?PFX, ?APP, list_group, fsm, finish, error], 1);
do_update({fsm_destroy, Type}) ->
    exometer:update([?PFX, ?APP, Type, fsm, active], -1);
do_update({Type, actor_count, Count}) ->
    exometer:update([?PFX, ?APP, Type, actor_count], Count);
do_update({Type, bytes, Bytes}) ->
    ok = exometer:update([?PFX, ?APP, Type, bytes], Bytes),
    exometer:update([?PFX, ?APP, Type, bytes, total], Bytes);
do_update(late_put_fsm_coordinator_ack) ->
    exometer:update([?PFX, ?APP, late_put_fsm_coordinator_ack], 1);
do_update({consistent_get, _Bucket, Microsecs, undefined}) ->
    ok = exometer:update([?PFX, ?APP, consistent, gets], 1),
    ok = exometer:update([?PFX, ?APP, consistent, gets, time], Microsecs);
do_update({consistent_get, _Bucket, Microsecs, ObjSize}) ->
    ok = exometer:update([?PFX, ?APP, consistent, gets], 1),
    ok = exometer:update([?PFX, ?APP, consistent, gets, time], Microsecs),
    create_or_update([?PFX, ?APP, consistent, gets, objsize], ObjSize, histogram);
do_update({consistent_put, _Bucket, Microsecs, undefined}) ->
    ok = exometer:update([?PFX, ?APP, consistent, puts], 1),
    ok = exometer:update([?PFX, ?APP, consistent, puts, time], Microsecs);
do_update({consistent_put, _Bucket, Microsecs, ObjSize}) ->
    ok = exometer:update([?PFX, ?APP, consistent, puts], 1),
    ok = exometer:update([?PFX, ?APP, consistent, puts, time], Microsecs),
    create_or_update([?PFX, ?APP, consistent, puts, objsize], ObjSize, histogram);
do_update({write_once_put, Microsecs, ObjSize}) ->
    ok = exometer:update([?PFX, ?APP, write_once, puts], 1),
    ok = exometer:update([?PFX, ?APP, write_once, puts, time], Microsecs),
    create_or_update([?PFX, ?APP, write_once, puts, objsize], ObjSize, histogram);
do_update({sweeper, Index, keys, NumKeys}) ->
    create_or_update([?PFX, ?APP, sweeper, Index, keys], NumKeys, spiral);
do_update({sweeper, Index, bytes, NumBytes}) ->
    create_or_update([?PFX, ?APP, sweeper, Index, bytes], NumBytes, spiral);
do_update({sweeper, Index, deleted, Count}) ->
    create_or_update([?PFX, ?APP, sweeper, Index, deleted], Count, spiral);
do_update({sweeper, Index, mutated, Count}) ->
    create_or_update([?PFX, ?APP, sweeper, Index, mutated], Count, spiral);
do_update({sweeper, Index, failed, Module, Count}) ->
    create_or_update([?PFX, ?APP, sweeper, Index, failed, Module], Count, spiral);
do_update({sweeper, Index, successful, Module, Count}) ->
    create_or_update([?PFX, ?APP, sweeper, Index, successful, Module], Count, spiral).

%% private

add_monitor(Type, Pid) ->
    gen_server:cast(?SERVER, {monitor, Type, Pid}).

monitor_loop(Type) ->
    receive
        {add_pid, Pid} ->
            erlang:monitor(process, Pid);
        {'DOWN', _Ref, process, _Pid, _Reason} ->
            do_update({fsm_destroy, Type})
    end,
    monitor_loop(Type).

%% Per index stats (by op)
do_per_index(Op, Idx, USecs) ->
    IdxAtom = list_to_atom(integer_to_list(Idx)),
    create_or_update([?PFX, ?APP, vnode, Op, IdxAtom], 1, spiral),
    create_or_update([?PFX, ?APP, vnode, Op, time, IdxAtom], USecs, histogram).

unregister_per_index(Op, Idx) ->
    IdxAtom = list_to_atom(integer_to_list(Idx)),
    exometer:delete([?PFX, ?APP, vnode, Op, IdxAtom]),
    exometer:delete([?PFX, ?APP, vnode, Op, time, IdxAtom]).

%%  per bucket get_fsm stats
do_get_bucket(false, _) ->
    ok;
do_get_bucket(true, {Bucket, Microsecs, Stages, NumSiblings, ObjSize}=Args) ->
    case exometer:update([?PFX, ?APP, node, gets, Bucket], 1) of
        ok ->
            [exometer:update([?PFX, ?APP, node, gets, Dimension, Bucket], Arg)
             || {Dimension, Arg} <- [{time, Microsecs},
                                     {siblings, NumSiblings},
                                     {objsize, ObjSize}], Arg /= undefined],
            do_stages([?PFX, ?APP, node, gets, time, Bucket], Stages);
        {error, not_found} ->
            exometer:new([?PFX, ?APP, node, gets, Bucket], spiral),
            [register_stat([?PFX, ?APP, node, gets, Dimension, Bucket], histogram) || Dimension <- [time,
                                                                                                 siblings,
                                                                                                 objsize]],
            do_get_bucket(true, Args)
    end;
do_get_bucket(true, {Bucket, Microsecs, Stages, NumSiblings, ObjSize, Type}=Args) ->
    case exometer:update([?PFX, ?APP, node, gets, Type, Bucket], 1) of
	ok ->
	    [exometer:update([?PFX, ?APP, node, gets, Dimension, Bucket], Arg)
	     || {Dimension, Arg} <- [{time, Microsecs},
				     {siblings, NumSiblings},
				     {objsize, ObjSize}], Arg /= undefined],
	    do_stages([?PFX, ?APP, node, gets, Type, time, Bucket], Stages);
	{error, not_found} ->
	    exometer:new([?PFX, ?APP, node, gets, Type, Bucket], spiral),
	    [register_stat([?PFX, ?APP, node, gets, Type, Dimension, Bucket], histogram)
	     || Dimension <- [time, siblings, objsize]],
	    do_get_bucket(true, Args)
    end.

%% per bucket put_fsm stats
do_put_bucket(false, _) ->
    ok;
do_put_bucket(true, {Bucket, Microsecs, Stages}=Args) ->
    case exometer:update([?PFX, ?APP, node, puts, Bucket], 1) of
        ok ->
            exometer:update([?PFX, ?APP, node, puts, time, Bucket], Microsecs),
            do_stages([?PFX, ?APP, node, puts, time, Bucket], Stages);
        {error, _} ->
            register_stat([?PFX, ?APP, node, puts, Bucket], spiral),
            register_stat([?PFX, ?APP, node, puts, time, Bucket], histogram),
            do_put_bucket(true, Args)
    end;
do_put_bucket(true, {Bucket, Microsecs, Stages, Type}=Args) ->
    case exometer:update([?PFX, ?APP, node, puts, Type, Bucket], 1) of
	ok ->
	    exometer:update([?PFX, ?APP, node, puts, Type, time, Bucket], Microsecs),
	    do_stages([?PFX, ?APP, node, puts, Type, time, Bucket], Stages);
	{error, not_found} ->
	    register_stat([?PFX, ?APP, node, puts, Type, Bucket], spiral),
	    register_stat([?PFX, ?APP, node, puts, Type, time, Bucket], histogram),
	    do_put_bucket(true, Args)
    end.


%% Path is list that provides a conceptual path to a stat
%% folsom uses the tuple as flat name
%% but some ets query magic means we can get stats by APP, Stat, DimensionX
%% Path, then is a list like [?APP, StatName]
%% Both get and put fsm have a list of {state, microseconds}
%% that they provide for stats.
%% Use the state to append to the stat "path" to create a further dimension on the stat
do_stages(_Path, []) ->
    ok;
do_stages(Path, [{Stage, Time}|Stages]) ->
    create_or_update(Path ++ [Stage], Time, histogram),
    do_stages(Path, Stages).

%% create dimensioned stats for read repairs.
%% The indexes are from get core [{Index, Reason::notfound|outofdate}]
%% preflist is a preflist of [{{Index, Node}, Type::primary|fallback}]
do_repairs(Indices, Preflist) ->
    Pfx = riak_core_stat:prefix(),
    lists:foreach(fun({{Idx, Node}, Type}) ->
                          case proplists:get_value(Idx, Indices) of
                              undefined ->
                                  ok;
                              Reason ->
                                  create_or_update([Pfx, ?APP, node, gets, read_repairs, Node, Type, Reason], 1, spiral)
                          end
                  end,
                  Preflist).

%% for dynamically created / dimensioned stats
%% that can't be registered at start up
create_or_update(Name, UpdateVal, Type) ->
    exometer:update_or_create(Name, UpdateVal, Type, []).

%% @doc list of {Name, Type} for static
%% stats that we can register at start up
stats() ->
    Pfx = riak_core_stat:prefix(),

    [%% vnode stats
     {[vnode, gets], spiral, [], [{one  , vnode_gets},
                                  {count, vnode_gets_total}]},
     {[vnode, gets, time], histogram, [], [{mean  , vnode_get_fsm_time_mean},
                                           {median, vnode_get_fsm_time_median},
                                           {95    , vnode_get_fsm_time_95},
                                           {99    , vnode_get_fsm_time_99},
                                           {max   , vnode_get_fsm_time_100}]},
     {[vnode, puts], spiral, [], [{one  , vnode_puts},
                                  {count, vnode_puts_total}]},
     {[vnode, puts, time], histogram, [], [{mean  , vnode_put_fsm_time_mean},
                                           {median, vnode_put_fsm_time_median},
                                           {95    , vnode_put_fsm_time_95},
                                           {99    , vnode_put_fsm_time_99},
                                           {max   , vnode_put_fsm_time_100}]},
     {[vnode, reap_tombstone], spiral, [], [{one  , vnode_reap_tombstone},
                                            {count, vnode_reap_tombstone_total}]},
     {[vnode, object_ttl_expired], spiral, [], [{one  , vnode_reap_object_ttl_expired},
                                                {count, vnode_reap_object_ttl_expired_total}]},
     {[vnode, index, refreshes], spiral, [], [{one  ,vnode_index_refreshes},
                                              {count, vnode_index_refreshes_total}]},
     {[vnode, index, reads], spiral, [], [{one  , vnode_index_reads},
                                          {count, vnode_index_reads_total}]},
     {[vnode, index, writes], spiral, [], [{one  , vnode_index_writes},
                                           {count, vnode_index_writes_total}]},
     {[vnode, index, writes, postings], spiral, [], [{one  , vnode_index_writes_postings},
                                                     {count, vnode_index_writes_postings_total}]},
     {[vnode, index, deletes], spiral, [], [{one  , vnode_index_deletes},
                                            {count, vnode_index_deletes_total}]},
     {[vnode, index, deletes, postings], spiral, [], [{one  , vnode_index_deletes_postings},
                                                      {count, vnode_index_deletes_postings_total}]},
     {[vnode, counter, update], spiral, [], [{one  , vnode_counter_update},
                                             {count, vnode_counter_update_total}]},
     {[vnode, counter, update, time], histogram, [], [{mean  , vnode_counter_update_time_mean},
                                                      {median, vnode_counter_update_time_median},
                                                      {95    , vnode_counter_update_time_95},
                                                      {99    , vnode_counter_update_time_99},
                                                      {max   , vnode_counter_update_time_100}]},
     {[vnode, set, update], spiral, [], [{one  , vnode_set_update},
                                         {count, vnode_set_update_total}]},
     {[vnode, set, update, time], histogram, [], [{mean  , vnode_set_update_time_mean},
                                                  {median, vnode_set_update_time_median},
                                                  {95    , vnode_set_update_time_95},
                                                  {99    , vnode_set_update_time_99},
                                                  {max   , vnode_set_update_time_100}]},
     {[vnode, hll, update], spiral, [], [{one  , vnode_hll_update},
                                         {count, vnode_hll_update_total}]},
     {[vnode, hll, update, time], histogram, [], [{mean  , vnode_hll_update_time_mean},
                                                  {median, vnode_hll_update_time_median},
                                                  {95    , vnode_hll_update_time_95},
                                                  {99    , vnode_hll_update_time_99},
                                                  {max   , vnode_hll_update_time_100}]},
     {[vnode, map, update], spiral, [], [{one  , vnode_map_update},
                                         {count, vnode_map_update_total}]},
     {[vnode, map, update, time], histogram, [], [{mean  , vnode_map_update_time_mean},
                                                  {median, vnode_map_update_time_median},
                                                  {95    , vnode_map_update_time_95},
                                                  {99    , vnode_map_update_time_99},
                                                  {max   , vnode_map_update_time_100}]},

     %% node stats: gets
     {[node, gets], spiral, [], [{one  , node_gets},
                                 {count, node_gets_total}]},
     {[node, gets, fsm, active], counter, [], [{value, node_get_fsm_active}]},
     {[node, gets, fsm, errors], spiral, [], [{one, node_get_fsm_errors},
                                              {count, node_get_fsm_errors_total}]},
     {[node, gets, objsize], histogram, [], [{mean  , node_get_fsm_objsize_mean},
                                             {median, node_get_fsm_objsize_median},
                                             {95    , node_get_fsm_objsize_95},
                                             {99    , node_get_fsm_objsize_99},
                                             {max   , node_get_fsm_objsize_100}]},
     {[node, gets, read_repairs], spiral, [], [{one, read_repairs},
                                               {count, read_repairs_total}]},
     {[node, gets, skipped_read_repairs], spiral, [], [{one, skipped_read_repairs},
                                                       {count, skipped_read_repairs_total}]},
     {[node, gets, siblings], histogram, [], [{mean  , node_get_fsm_siblings_mean},
                                              {median, node_get_fsm_siblings_median},
                                              {95    , node_get_fsm_siblings_95},
                                              {99    , node_get_fsm_siblings_99},
                                              {max   , node_get_fsm_siblings_100}]},
     {[node, gets, time], histogram, [], [{mean  , node_get_fsm_time_mean},
                                          {median, node_get_fsm_time_median},
                                          {95    , node_get_fsm_time_95},
                                          {99    , node_get_fsm_time_99},
                                          {max   , node_get_fsm_time_100}]},
     {[node, gets, counter], spiral, [], [{one  , node_gets_counter},
                                          {count, node_gets_counter_total}]},
     {[node, gets, counter, objsize], histogram, [], [{mean  , node_get_fsm_counter_objsize_mean},
                                                      {median, node_get_fsm_counter_objsize_median},
                                                      {95    , node_get_fsm_counter_objsize_95},
                                                      {99    , node_get_fsm_counter_objsize_99},
                                                      {max   , node_get_fsm_counter_objsize_100}]},
     {[node, gets, counter, read_repairs], spiral, [], [{one  , read_repairs_counter},
                                                        {count, read_repairs_counter_total}]},
     {[node, gets, counter, siblings], histogram, [], [{mean  , node_get_fsm_counter_siblings_mean},
                                                       {median, node_get_fsm_counter_siblings_median},
                                                       {95    , node_get_fsm_counter_siblings_95},
                                                       {99    , node_get_fsm_counter_siblings_99},
                                                       {max   , node_get_fsm_counter_siblings_100}]},
     {[node, gets, counter, time], histogram, [], [{mean  , node_get_fsm_counter_time_mean},
                                                   {median, node_get_fsm_counter_time_median},
                                                   {95    , node_get_fsm_counter_time_95},
                                                   {99    , node_get_fsm_counter_time_99},
                                                   {max   , node_get_fsm_counter_time_100}]},
     {[node, gets, set], spiral, [], [{one  , node_gets_set},
                                      {count, node_gets_set_total}]},
     {[node, gets, set, objsize], histogram, [], [{mean  , node_get_fsm_set_objsize_mean},
                                                  {median, node_get_fsm_set_objsize_median},
                                                  {95    , node_get_fsm_set_objsize_95},
                                                  {99    , node_get_fsm_set_objsize_99},
                                                  {max   , node_get_fsm_set_objsize_100}]},
     {[node, gets, set, read_repairs], spiral, [], [{one  , read_repairs_set},
                                                    {count, read_repairs_set_total}]},
     {[node, gets, set, siblings], histogram, [], [{mean  , node_get_fsm_set_siblings_mean},
                                                   {median, node_get_fsm_set_siblings_median},
                                                   {95    , node_get_fsm_set_siblings_95},
                                                   {99    , node_get_fsm_set_siblings_99},
                                                   {max   , node_get_fsm_set_siblings_100}]},
     {[node, gets, set, time], histogram, [], [{mean  , node_get_fsm_set_time_mean},
                                               {median, node_get_fsm_set_time_median},
                                               {95    , node_get_fsm_set_time_95},
                                               {99    , node_get_fsm_set_time_99},
                                               {max   , node_get_fsm_set_time_100}]},
     {[node, gets, hll], spiral, [], [{one  , node_gets_hll},
                                      {count, node_gets_hll_total}]},
     {[node, gets, hll, objsize], histogram, [], [{mean  , node_get_fsm_hll_objsize_mean},
                                                  {median, node_get_fsm_hll_objsize_median},
                                                  {95    , node_get_fsm_hll_objsize_95},
                                                  {99    , node_get_fsm_hll_objsize_99},
                                                  {max   , node_get_fsm_hll_objsize_100}]},
     {[node, gets, hll, read_repairs], spiral, [], [{one  , read_repairs_hll},
                                                    {count, read_repairs_hll_total}]},
     {[node, gets, hll, siblings], histogram, [], [{mean  , node_get_fsm_hll_siblings_mean},
                                                   {median, node_get_fsm_hll_siblings_median},
                                                   {95    , node_get_fsm_hll_siblings_95},
                                                   {99    , node_get_fsm_hll_siblings_99},
                                                   {max   , node_get_fsm_hll_siblings_100}]},
     {[node, gets, hll, time], histogram, [], [{mean  , node_get_fsm_hll_time_mean},
                                               {median, node_get_fsm_hll_time_median},
                                               {95    , node_get_fsm_hll_time_95},
                                               {99    , node_get_fsm_hll_time_99},
                                               {max   , node_get_fsm_hll_time_100}]},
     {[node, gets, map], spiral, [], [{one  , node_gets_map},
                                      {count, node_gets_map_total}]},
     {[node, gets, map, objsize], histogram, [], [{mean  , node_get_fsm_map_objsize_mean},
                                                  {median, node_get_fsm_map_objsize_median},
                                                  {95    , node_get_fsm_map_objsize_95},
                                                  {99    , node_get_fsm_map_objsize_99},
                                                  {max   , node_get_fsm_map_objsize_100}]},
     {[node, gets, map, read_repairs], spiral, [], [{one  , read_repairs_map},
                                                    {count, read_repairs_map_total}]},
     {[node, gets, map, siblings], histogram, [], [{mean  , node_get_fsm_map_siblings_mean},
                                                   {median, node_get_fsm_map_siblings_median},
                                                   {95    , node_get_fsm_map_siblings_95},
                                                   {99    , node_get_fsm_map_siblings_99},
                                                   {max   , node_get_fsm_map_siblings_100}]},
     {[node, gets, map, time], histogram, [], [{mean  , node_get_fsm_map_time_mean},
                                               {median, node_get_fsm_map_time_median},
                                               {95    , node_get_fsm_map_time_95},
                                               {99    , node_get_fsm_map_time_99},
                                               {max   , node_get_fsm_map_time_100}]},

     %% node stats: puts
     {[node, puts], spiral, [], [{one, node_puts},
                                 {count, node_puts_total}]},
     {[node, puts, coord_redirs], counter, [], [{value,coord_redirs_total}]},
     {[node, puts, fsm, active], counter},
     {[node, puts, fsm, errors], spiral},
     {[node, puts, time], histogram, [], [{mean  , node_put_fsm_time_mean},
                                          {median, node_put_fsm_time_median},
                                          {95    , node_put_fsm_time_95},
                                          {99    , node_put_fsm_time_99},
                                          {max   , node_put_fsm_time_100}]},
     {[node, puts, counter], spiral, [], [{one  , node_puts_counter},
                                          {count, node_puts_counter_total}]},
     {[node, puts, counter, time], histogram, [], [{mean  , node_put_fsm_counter_time_mean},
                                                   {median, node_put_fsm_counter_time_median},
                                                   {95    , node_put_fsm_counter_time_95},
                                                   {99    , node_put_fsm_counter_time_99},
                                                   {max   , node_put_fsm_counter_time_100}]},
     {[node, puts, set], spiral, [], [{one  , node_puts_set},
                                      {count, node_puts_set_total}]},
     {[node, puts, set, time], histogram, [], [{mean  , node_put_fsm_set_time_mean},
                                               {median, node_put_fsm_set_time_median},
                                               {95    , node_put_fsm_set_time_95},
                                               {99    , node_put_fsm_set_time_99},
                                               {max   , node_put_fsm_set_time_100}]},
     {[node, puts, hll], spiral, [], [{one  , node_puts_hll},
                                      {count, node_puts_hll_total}]},
     {[node, puts, hll, time], histogram, [], [{mean  , node_put_fsm_hll_time_mean},
                                               {median, node_put_fsm_hll_time_median},
                                               {95    , node_put_fsm_hll_time_95},
                                               {99    , node_put_fsm_hll_time_99},
                                               {max   , node_put_fsm_hll_time_100}]},
     {[node, puts, map], spiral, [], [{one  , node_puts_map},
                                      {count, node_puts_map_total}]},
     {[node, puts, map, time], histogram, [], [{mean  , node_put_fsm_map_time_mean},
                                               {median, node_put_fsm_map_time_median},
                                               {95    , node_put_fsm_map_time_95},
                                               {99    , node_put_fsm_map_time_99},
                                               {max   , node_put_fsm_map_time_100}]},

     %% index & list{keys,buckets} stats
     {[index, fsm, create], spiral, [], [{one, index_fsm_create}]},
     {[index, fsm, create, error], spiral, [], [{one, index_fsm_create_error}]},
     {[index, fsm, active], counter, [], [{value, index_fsm_active}]},
     {[list, fsm, create], spiral, [], [{one  , list_fsm_create},
					{count, list_fsm_create_total}]},
     {[list, fsm, create, error], spiral, [], [{one  , list_fsm_create_error},
                                               {count, list_fsm_create_error_total}]},
     {[list, fsm, active], counter, [], [{value, list_fsm_active}]},

     {[list_group, fsm, create],
      spiral, [], [{one  , list_group_fsm_create},
                   {count, list_group_fsm_create_total}]},
     {[list_group, fsm, create, error],
      spiral, [], [{one  , list_group_fsm_create_error},
                   {count, list_group_fsm_create_error_total}]},
     {[list_group, fsm, active],
      counter, [], [{value, list_group_fsm_active}]},

     {[list_group, fsm, finish, count],
      histogram, [], [{mean, list_group_count_mean},
                      {median, list_group_count_median},
                      {95, list_group_count_95},
                      {99, list_group_count_99},
                      {max, list_group_count_max}]},
     {[list_group, fsm, finish, error],
      counter, [], [{value, list_group_keys_finish_error}]},

     %% misc stats
     {mapper_count, counter, [], [{value, executing_mappers}]},
     {precommit_fail, counter, [], [{value, precommit_fail}]},
     {postcommit_fail, counter, [], [{value, postcommit_fail}]},
     {write_once_merge, counter, [], [{value, write_once_merge}]},
     {[vnode, backend, leveldb, read_block_error],
      {function, ?MODULE, leveldb_read_block_errors, [], match, value}, [],
      [{value, leveldb_read_block_error}]},

     %% datatype stats
     {[counter, actor_count], histogram, [], [{mean  , counter_actor_counts_mean},
                                              {median, counter_actor_counts_median},
                                              {95    , counter_actor_counts_95},
                                              {99    , counter_actor_counts_99},
                                              {max   , counter_actor_counts_100}]},
     {[set, actor_count], histogram, [], [{mean  , set_actor_counts_mean},
                                          {median, set_actor_counts_median},
                                          {95    , set_actor_counts_95},
                                          {99    , set_actor_counts_99},
                                          {max   , set_actor_counts_100}]},
     {[map, actor_count], histogram, [], [{mean  , map_actor_counts_mean},
                                          {median, map_actor_counts_median},
                                          {95    , map_actor_counts_95},
                                          {99    , map_actor_counts_99},
                                          {max   , map_actor_counts_100}]},
     {[hll, bytes], spiral, [], [{one  , hll_bytes},
                                 {count, hll_bytes_total}]},
     {[hll, bytes, total], histogram, [], [{mean  , hll_bytes_mean},
                                           {median, hll_bytes_median},
                                           {95    , hll_bytes_95},
                                           {99    , hll_bytes_99},
                                           {max   , hll_bytes_100}]},
     {[object, merge], spiral, [], [{one  , object_merge},
                                    {count, object_merge_total}]},
     {[object, merge, time], histogram, [], [{mean  , object_merge_time_mean},
                                             {median, object_merge_time_median},
                                             {95    , object_merge_time_95},
                                             {99    , object_merge_time_99},
                                             {max   , object_merge_time_100}]},
     {[object, counter, merge], spiral, [], [{one, object_counter_merge},
                                             {count, object_counter_merge_total}]},
     {[object, counter, merge, time], histogram, [], [{mean  , object_counter_merge_time_mean},
                                                      {median, object_counter_merge_time_median},
                                                      {95    , object_counter_merge_time_95},
                                                      {99    , object_counter_merge_time_99},
                                                      {max   , object_counter_merge_time_100}]},
     {[object, set, merge], spiral, [], [{one  , object_set_merge},
                                         {count, object_set_merge_total}]},
     {[object, set, merge, time], histogram, [], [{mean  , object_set_merge_time_mean},
                                                  {median, object_set_merge_time_median},
                                                  {95    , object_set_merge_time_95},
                                                  {99    , object_set_merge_time_99},
                                                  {max   , object_set_merge_time_100}]},
     {[object, hll, merge], spiral, [], [{one  , object_hll_merge},
                                         {count, object_hll_merge_total}]},
     {[object, hll, merge, time], histogram, [], [{mean  , object_hll_merge_time_mean},
                                                  {median, object_hll_merge_time_median},
                                                  {95    , object_hll_merge_time_95},
                                                  {99    , object_hll_merge_time_99},
                                                  {max   , object_hll_merge_time_100}]},
     {[object, map, merge], spiral, [], [{one  , object_map_merge},
                                         {count, object_map_merge_total}]},
     {[object, map, merge, time], histogram, [], [{mean  , object_map_merge_time_mean},
						  {median, object_map_merge_time_median},
						  {95    , object_map_merge_time_95},
						  {99    , object_map_merge_time_99},
						  {max   , object_map_merge_time_100}]},
     {late_put_fsm_coordinator_ack, counter, [], [{value, late_put_fsm_coordinator_ack}]},

     %% strong-consistency stats
     {[consistent, gets], spiral, [], [{one, consistent_gets},
                                       {count, consistent_gets_total}]},
     {[consistent, gets, time], histogram, [], [{mean  , consistent_get_time_mean},
                                                {median, consistent_get_time_median},
                                                {95    , consistent_get_time_95},
                                                {99    , consistent_get_time_99},
                                                {max   , consistent_get_time_100}]},
     {[consistent, gets, objsize], histogram, [], [{mean  , consistent_get_objsize_mean},
                                                   {median, consistent_get_objsize_median},
                                                   {95    , consistent_get_objsize_95},
                                                   {99    , consistent_get_objsize_99},
                                                   {max   , consistent_get_objsize_100}]},
     {[consistent, puts], spiral, [], [{one, consistent_puts},
                                       {count, consistent_puts_total}]},
     {[consistent, puts, time], histogram, [], [{mean  , consistent_put_time_mean},
                                                {median, consistent_put_time_median},
                                                {95    , consistent_put_time_95},
                                                {99    , consistent_put_time_99},
                                                {max   , consistent_put_time_100}]},
     {[consistent, puts, objsize], histogram, [], [{mean  , consistent_put_objsize_mean},
                                                   {median, consistent_put_objsize_median},
                                                   {95    , consistent_put_objsize_95},
                                                   {99    , consistent_put_objsize_99},
                                                   {max   , consistent_put_objsize_100}]},

     %% write-once stats
     {[write_once, puts], spiral, [], [{one, write_once_puts},
                                       {count, write_once_puts_total}]},
     {[write_once, puts, time], histogram, [], [{mean  , write_once_put_time_mean},
                                                {median, write_once_put_time_median},
                                                {95    , write_once_put_time_95},
                                                {99    , write_once_put_time_99},
                                                {max   , write_once_put_time_100}]},
     {[write_once, puts, objsize], histogram, [], [{mean  , write_once_put_objsize_mean},
                                                   {median, write_once_put_objsize_median},
                                                   {95    , write_once_put_objsize_95},
                                                   {99    , write_once_put_objsize_99},
                                                   {max   , write_once_put_objsize_100}]},

     {[storage_backend], {function, app_helper, get_env, [riak_kv, storage_backend], match, value},
      [], [{value, storage_backend}]},
     {[ring_stats], {function, riak_kv_stat_bc, ring_stats, [], proplist, [ring_members,
									   ring_num_partitions,
									   ring_ownership]},
      [], [{ring_members       , ring_members},
           {ring_num_partitions, ring_num_partitions},
           {ring_ownership     , ring_ownership}]}
     | read_repair_aggr_stats(Pfx)] ++ bc_stats(Pfx).

read_repair_aggr_stats(Pfx) ->
    Spec = fun(Type, Reason) ->
                   {function,exometer,aggregate,
                    [ [{{[Pfx,?APP,node,gets,read_repairs,'_',Type,Reason],'_','_'},
                        [], [true]}], [one,count] ], proplist, [one,count]}
           end,
    [
     {[read_repairs,primary,notfound], Spec(primary, notfound), [],
      [{one  , read_repairs_primary_notfound_one},
       {count, read_repairs_primary_notfound_count}]},
     {[read_repairs,primary,outofdate], Spec(primary, outofdate), [],
      [{one  , read_repairs_primary_outofdate_one},
       {count, read_repairs_primary_outofdate_count}]},
     {[read_repairs,fallback,notfound], Spec(fallback,notfound), [],
      [{one  , read_repairs_fallback_notfound_one},
       {count, read_repairs_fallback_notfound_count}]},
     {[read_repairs,fallback,outofdate], Spec(fallback,outofdate), [],
      [{one  , read_repairs_fallback_outofdate_one},
       {count, read_repairs_fallback_outofdate_count}]}
    ].

bc_stats(Pfx) ->
    Spec = fun(N, M, F, As) ->
                   {[Pfx,?APP,bc,N], {function, M, F, As, match, value}, [], [{value, N}]}
           end,
    [Spec(N, M, F, As) ||
        {N, M, F, As} <- [{nodename, erlang, node, []},
                          {connected_nodes, erlang, nodes, []},
                          {sys_driver_version, riak_kv_stat_bc, sys_driver_version, []},
                          {sys_global_heaps_size, ?MODULE, value, [deprecated]},
                          {sys_heap_type, erlang, system_info, [heap_type]},
                          {sys_logical_processors, erlang, system_info, [logical_processors]},
                          {sys_monitor_count, riak_kv_stat_bc, sys_monitor_count, []},
                          {sys_otp_release, riak_kv_stat_bc, otp_release, []},
                          {sys_port_count, erlang, system_info, [port_count]},
                          {sys_process_count, erlang, system_info, [process_count]},
                          {sys_smp_support, erlang, system_info, [smp_support]},
                          {sys_system_version, riak_kv_stat_bc, system_version, []},
                          {sys_system_architecture, riak_kv_stat_bc, system_architecture, []},
                          {sys_threads_enabled, erlang, system_info, [threads]},
                          {sys_thread_pool_size, erlang, system_info, [thread_pool_size]},
                          {sys_wordsize, erlang, system_info, [wordsize]}]].

%% Wrapper for exometer function stats.
value(V) ->
    V.

do_register_stat(Name, Type) ->
    exometer:new(Name, Type).

%% @doc produce the legacy blob of stats for display.
produce_stats() ->
    riak_kv_stat_bc:produce_stats().

%% @doc get the leveldb.ReadBlockErrors counter.
%% non-zero values mean it is time to consider replacing
%% this nodes disk.
leveldb_read_block_errors() ->
    %% level stats are per node
    %% but the way to get them is
    %% is with riak_kv_vnode:vnode_status/1
    %% for that reason just chose a partition
    %% on this node at random
    %% and ask for it's stats
    {ok, R} = riak_core_ring_manager:get_my_ring(),
    case riak_core_ring:my_indices(R) of
        [] -> undefined;
        [Idx] ->
            Status = vnode_status(Idx),
            leveldb_read_block_errors(Status);
        Indices ->
            %% technically a call to status is a vnode
            %% operation, so spread the load by picking
            %% a vnode at random.
            Nth = crypto:rand_uniform(1, length(Indices)),
            Idx = lists:nth(Nth, Indices),
            Status = vnode_status(Idx),
            leveldb_read_block_errors(Status)
    end.

vnode_status(Idx) ->
    PList = [{Idx, node()}],
    [{Idx, Status}] = riak_kv_vnode:vnode_status(PList),
    case lists:keyfind(backend_status, 1, Status) of
        false ->
            %% if for some reason backend_status is absent from the
            %% status list
            {error, no_backend_status};
        BEStatus ->
            BEStatus
    end.

leveldb_read_block_errors({backend_status, riak_kv_eleveldb_backend, Status}) ->
    rbe_val(proplists:get_value(read_block_error, Status));
leveldb_read_block_errors({backend_status, riak_kv_multi_backend, Statuses}) ->
    multibackend_read_block_errors(Statuses, undefined);
leveldb_read_block_errors({error, Reason}) ->
    {error, Reason};
leveldb_read_block_errors(_) ->
    undefined.

multibackend_read_block_errors([], Val) ->
    rbe_val(Val);
multibackend_read_block_errors([{_Name, Status}|Rest], undefined) ->
    RBEVal = case proplists:get_value(mod, Status) of
                 riak_kv_eleveldb_backend ->
                     proplists:get_value(read_block_error, Status);
                 _ -> undefined
             end,
    multibackend_read_block_errors(Rest, RBEVal);
multibackend_read_block_errors(_, Val) ->
    rbe_val(Val).

rbe_val(Bin) when is_binary(Bin) ->
    list_to_integer(binary_to_list(Bin));
rbe_val(_) ->
    undefined.

%% All stat creation is serialized through riak_kv_stat.
%% Some stats are created on demand as part of the call to `update/1'.
%% When a stat error is caught, the stat must be deleted and recreated.
%% Since stat updates can happen from many processes concurrently
%% a stat that throws an error may already have been deleted and
%% recreated. To protect against needlessly deleting and recreating
%% an already 'fixed stat' first retry the stat update. There is a chance
%% that the retry succeeds as the stat has been recreated, but some on
%% demand stat it uses has not yet. Since stat creates are serialized
%% in riak_kv_stat re-registering a stat could cause a deadlock.
%% This loop is spawned as a process to avoid that.
stat_repair_loop() ->
    receive
        {'DOWN', _, process, _, _} ->
            ok;
        _ ->
            stat_repair_loop()
    end.

stat_repair_loop(Dad) ->
    erlang:monitor(process, Dad),
    stat_repair_loop().

-ifdef(TEST).
-define(LEVEL_STATUS(Idx, Val),  [{Idx, [{backend_status, riak_kv_eleveldb_backend,
                                          [{read_block_error, Val}]}]}]).
-define(BITCASK_STATUS(Idx),  [{Idx, [{backend_status, riak_kv_bitcask_backend,
                                       []}]}]).
-define(MULTI_STATUS(Idx, Val), [{Idx,  [{backend_status, riak_kv_multi_backend, Val}]}]).

leveldb_rbe_test_() ->
    {foreach,
     fun() ->
	     exometer:start(),
             meck:new(riak_core_ring_manager),
             meck:new(riak_core_ring),
             meck:new(riak_kv_vnode),
             meck:expect(riak_core_ring_manager, get_my_ring, fun() -> {ok, [fake_ring]} end)
     end,
     fun(_) ->
	     exometer:stop(),
             meck:unload(riak_kv_vnode),
             meck:unload(riak_core_ring),
             meck:unload(riak_core_ring_manager)
     end,
     [{"Zero indexes", fun zero_indexes/0},
      {"Single index", fun single_index/0},
      {"Multi indexes", fun multi_index/0},
      {"Bitcask Backend", fun bitcask_backend/0},
      {"Multi Backend", fun multi_backend/0}]
    }.

start_exometer_test_env() ->
    ok = exometer:start(),
    ok = meck:new(riak_core_ring_manager),
    ok = meck:new(riak_core_ring),
    ok = meck:new(riak_kv_vnode),
    ok = meck:expect(riak_core_stat, vnodeq_stats, fun() -> [] end),
    meck:expect(riak_core_ring_manager, get_my_ring, fun() -> {ok, [fake_ring]} end).

stop_exometer_test_env() ->
    ok = exometer:stop(),
    ok = meck:unload(riak_kv_vnode),
    ok = meck:unload(riak_core_ring),
    meck:unload(riak_core_ring_manager).

create_or_update_histogram_test() ->
    ok = start_exometer_test_env(),
    try
        Metric = [riak_kv,put_fsm,counter,time],
        ok = repeat_create_or_update(Metric, 1, histogram, 100),
        ?assertNotEqual(exometer:get_value(Metric), 0),
        Stats = get_stats(),
        %%lager:info("stats prop list ~s", [Stats]),
        ?assertNotEqual(proplists:get_value({node_put_fsm_counter_time_mean}, Stats), 0)
    after
        ok = stop_exometer_test_env()
    end.

repeat_create_or_update(Name, UpdateVal, Type, Times) when Times > 0 ->
    repeat_create_or_update(Name, UpdateVal, Type, Times, 0).
repeat_create_or_update(Name, UpdateVal, Type, Times, Ops) when Ops < Times ->
    ok = create_or_update(Name, UpdateVal, Type),
    repeat_create_or_update(Name, UpdateVal, Type, Times, Ops + 1);
repeat_create_or_update(_Name, _UpdateVal, _Type, Times, Ops) when Ops >= Times ->
    ok.

zero_indexes() ->
    meck:expect(riak_core_ring, my_indices, fun(_R) -> [] end),
    ?assertEqual(undefined, leveldb_read_block_errors()).

single_index() ->
    meck:expect(riak_core_ring, my_indices, fun(_R) -> [index1] end),
    meck:expect(riak_kv_vnode, vnode_status, fun([{Idx, _}]) -> ?LEVEL_STATUS(Idx, <<"100">>) end),
    ?assertEqual(100, leveldb_read_block_errors()),

    meck:expect(riak_kv_vnode, vnode_status, fun([{Idx, _}]) -> ?LEVEL_STATUS(Idx, nonsense) end),
    ?assertEqual(undefined, leveldb_read_block_errors()).

multi_index() ->
    meck:expect(riak_core_ring, my_indices, fun(_R) -> [index1, index2, index3] end),
    meck:expect(riak_kv_vnode, vnode_status, fun([{Idx, _}]) -> ?LEVEL_STATUS(Idx, <<"100">>) end),
    ?assertEqual(100, leveldb_read_block_errors()).

bitcask_backend() ->
    meck:expect(riak_core_ring, my_indices, fun(_R) -> [index1, index2, index3] end),
    meck:expect(riak_kv_vnode, vnode_status, fun([{Idx, _}]) -> ?BITCASK_STATUS(Idx) end),
    ?assertEqual(undefined, leveldb_read_block_errors()).

multi_backend() ->
    meck:expect(riak_core_ring, my_indices, fun(_R) -> [index1, index2, index3] end),
    %% some backends, none level
    meck:expect(riak_kv_vnode, vnode_status, fun([{Idx, _}]) ->
                                                     ?MULTI_STATUS(Idx,
                                                                   [{name1, [{mod, bitcask}]},
                                                                    {name2, [{mod, fired_chicked}]}]
                                                                  )
                                             end),
    ?assertEqual(undefined, leveldb_read_block_errors()),

    %% one or movel leveldb backends (first level answer is returned)
    meck:expect(riak_kv_vnode, vnode_status, fun([{Idx, _}]) ->
                                                     ?MULTI_STATUS(Idx,
                                                                   [{name1, [{mod, bitcask}]},
                                                                    {name2, [{mod, riak_kv_eleveldb_backend},
                                                                             {read_block_error, <<"99">>}]},
                                                                    {name2, [{mod, riak_kv_eleveldb_backend},
                                                                             {read_block_error, <<"1000">>}]}]
                                                                  )
                                             end),
    ?assertEqual(99, leveldb_read_block_errors()).

-endif.
