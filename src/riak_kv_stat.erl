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

%% @doc riak_kv_stat is a long-lived gen_server process for aggregating
%%      stats about the Riak node on which it is runing.
%%
%%      Update each stat with the exported function update/1.  Modify
%%      the internal function update/3 to add storage for new stats.
%%
%%      Get the latest aggregation of stats with the exported function
%%      get_stats/0.  Modify the internal function produce_stats/1 to
%%      change how stats are represented.
%%
%%      Riak will start riak_kv_stat for you, if you have specified
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
%%</dd><dt> vnode_index_read
%%</dt><dd> The number of index reads handled by all vnodes on this node.
%%          Each query counts as an index read.
%%</dd><dd> update(index_read)
%%
%%</dd><dt> vnode_index_write
%%</dt><dd> The number of batched writes handled by all vnodes on this node.
%%</dd><dd> update({index_write, Postings})
%%
%%</dd><dt> vnode_index_write_postings
%%</dt><dd> The number of postings written to all vnodes on this node.
%%</dd><dd> update({index_write, Postings})
%%
%%</dd><dt> vnode_index_delete
%%</dt><dd> The number of batched writes handled by all vnodes on this node.
%%</dd><dd> update({index_delete, Postings})
%%
%%</dd><dt> vnode_index_delete_postings
%%</dt><dd> The number of postings written to all vnodes on this node.
%%</dd><dd> update({index_delete, Postings})
%%
%%</dd><dt> node_gets
%%</dt><dd> Number of gets coordinated by this node in the last
%%          minute.
%%</dd><dd> update({get_fsm_time, Microseconds})
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

-behaviour(gen_server2).

-define(FORCE_LEGACY_STATS, yes).

%% API
-export([start_link/0, get_stats/0, get_stats/1, update/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state,{vnode_gets,vnode_puts,vnode_gets_total,vnode_puts_total,
               vnode_index_reads, vnode_index_reads_total,
               vnode_index_writes, vnode_index_writes_total,
               vnode_index_writes_postings, vnode_index_writes_postings_total,
               vnode_index_deletes, vnode_index_deletes_total,
               vnode_index_deletes_postings, vnode_index_deletes_postings_total,
               node_gets_total, node_puts_total,
               get_fsm_time,put_fsm_time,
               pbc_connects,pbc_connects_total,pbc_active,
               read_repairs, read_repairs_total,
               coord_redirs, coord_redirs_total, mapper_count, 
               get_meter, put_meter, legacy}).

%% @spec start_link() -> {ok,Pid} | ignore | {error,Error}
%% @doc Start the server.  Also start the os_mon application, if it's
%%      not already running.
start_link() ->
    case application:start(os_mon) of
        ok -> ok;
        {error, {already_started, os_mon}} -> ok
    %% die if os_mon doesn't start
    end,
    gen_server2:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @spec get_stats() -> proplist()
%% @doc Get the current aggregation of stats.
get_stats() ->
    get_stats(slide:moment()).

get_stats(Moment) ->
    gen_server2:call(?MODULE, {get_stats, Moment}, infinity).

%% @spec update(term()) -> ok
%% @doc Update the given stat.
update(Stat) ->
    gen_server2:cast(?MODULE, {update, Stat, slide:moment()}).

%% @private
init([]) ->
    process_flag(trap_exit, true),
    remove_slide_private_dirs(),
    case application:get_env(riak_kv, legacy_stats) of
        {ok, true} ->
            legacy_init();
        _ ->
            lager:warning("Overriding user-setting and using legacy stats."),
            legacy_init()
            %v2_init()
    end.

-ifdef(not_defined).
make_meter() ->
    {ok, M} = basho_metrics_nifs:meter_new(),
    {meter, M}.

make_histogram() ->
    {ok, H} = basho_metrics_nifs:histogram_new(),
    {histogram, H}.

v2_init() ->
    timer:send_interval(5000, tick),
    {ok, #state{vnode_gets=make_meter(),
                vnode_puts=make_meter(),
                vnode_gets_total=0,
                vnode_puts_total=0,
                vnode_index_reads=make_meter(),
                vnode_index_reads_total=0,
                vnode_index_writes=make_meter(),
                vnode_index_writes_total=0,
                vnode_index_writes_postings=make_meter(),
                vnode_index_writes_postings_total=0,
                vnode_index_deletes=make_meter(),
                vnode_index_deletes_total=0,
                vnode_index_deletes_postings=make_meter(),
                vnode_index_deletes_postings_total=0,
                node_gets_total=0,
                node_puts_total=0,
                get_fsm_time=make_histogram(),
                put_fsm_time=make_histogram(),
                pbc_connects=make_meter(),
                pbc_connects_total=0,
                pbc_active=0,
                read_repairs=make_meter(),
                read_repairs_total=0,
                coord_redirs=make_meter(),
                coord_redirs_total=0,
                mapper_count=0,
                get_meter=make_meter(),
                put_meter=make_meter(),
                legacy=false}}.
-endif.

legacy_init() ->
    {ok, #state{vnode_gets=spiraltime:fresh(),
                vnode_puts=spiraltime:fresh(),
                vnode_gets_total=0,
                vnode_puts_total=0,
                vnode_index_reads=spiraltime:fresh(),
                vnode_index_reads_total=0,
                vnode_index_writes=spiraltime:fresh(),
                vnode_index_writes_total=0,
                vnode_index_writes_postings=spiraltime:fresh(),
                vnode_index_writes_postings_total=0,
                vnode_index_deletes=spiraltime:fresh(),
                vnode_index_deletes_total=0,
                vnode_index_deletes_postings=spiraltime:fresh(),
                vnode_index_deletes_postings_total=0,
                node_gets_total=0,
                node_puts_total=0,
                get_fsm_time=slide:fresh(),
                put_fsm_time=slide:fresh(),
                pbc_connects=spiraltime:fresh(),
                pbc_connects_total=0,
                pbc_active=0,
                read_repairs=spiraltime:fresh(),
                read_repairs_total=0,
                coord_redirs_total=0,
                mapper_count=0,
                legacy=true}}.

%% @private
handle_call({get_stats, Moment}, _From, State) ->
    {reply, produce_stats(State, Moment), State};
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%% @private
handle_cast({update, Stat, Moment}, State) ->
    {noreply, update(Stat, Moment, State, State#state.legacy)};
handle_cast(_Msg, State) ->
    {noreply, State}.

%% @private
handle_info(tick, State) ->
    tick(#state.get_meter, State),
    tick(#state.put_meter, State),
    tick(#state.vnode_gets, State),
    tick(#state.vnode_puts, State),
    tick(#state.vnode_index_reads, State),
    tick(#state.vnode_index_writes, State),
    tick(#state.vnode_index_writes_postings, State),
    tick(#state.vnode_index_deletes, State),
    tick(#state.vnode_index_deletes_postings, State),
    tick(#state.pbc_connects, State),
    tick(#state.read_repairs, State),
    tick(#state.coord_redirs, State),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

%% @private
terminate(_Reason, _State) ->
    remove_slide_private_dirs(),
    ok.

%% @private
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------

update(Stat, Moment, State, true) ->
    update(Stat, Moment, State);
update(Stat, Moment, State, false) ->
    update1(Stat, Moment, State).

%% @spec update(Stat::term(), integer(), state()) -> state()
%% @doc Update the given stat in State, returning a new State.
update(vnode_get, Moment, State=#state{vnode_gets_total=VGT}) ->
    spiral_incr(#state.vnode_gets, Moment, State#state{vnode_gets_total=VGT+1});
update(vnode_put, Moment, State=#state{vnode_puts_total=VPT}) ->
    spiral_incr(#state.vnode_puts, Moment, State#state{vnode_puts_total=VPT+1});
update(vnode_index_read, Moment, State=#state{vnode_index_reads_total=VPT}) ->
    spiral_incr(#state.vnode_index_reads, Moment, State#state{vnode_index_reads_total=VPT+1});
update({vnode_index_write, Postings}, Moment, State=#state{vnode_index_writes_total=VIW, vnode_index_writes_postings_total=VIWP}) ->
    NewState = spiral_incr(#state.vnode_index_writes, Moment, State#state{vnode_index_writes_total=VIW+1}),
    spiral_incr(#state.vnode_index_writes_postings, Postings, Moment, NewState#state{vnode_index_writes_postings_total=VIWP+Postings});
update({vnode_index_delete, Postings}, Moment, State=#state{vnode_index_deletes_total=VID, vnode_index_deletes_postings_total=VIDP}) ->
    NewState = spiral_incr(#state.vnode_index_deletes, Moment, State#state{vnode_index_deletes_total=VID+1}),
    spiral_incr(#state.vnode_index_deletes_postings, Postings, Moment, NewState#state{vnode_index_deletes_postings_total=VIDP+Postings});
update({get_fsm_time, Microsecs}, Moment, State=#state{node_gets_total=NGT}) ->
    slide_incr(#state.get_fsm_time, Microsecs, Moment, State#state{node_gets_total=NGT+1});
update({put_fsm_time, Microsecs}, Moment, State=#state{node_puts_total=NPT}) ->
    slide_incr(#state.put_fsm_time, Microsecs, Moment, State#state{node_puts_total=NPT+1});
update(pbc_connect, Moment, State=#state{pbc_connects_total=NCT, pbc_active=Active}) ->
    spiral_incr(#state.pbc_connects, Moment, State#state{pbc_connects_total=NCT+1,
                                                         pbc_active=Active+1});
update(pbc_disconnect, _Moment, State=#state{pbc_active=Active}) ->
    State#state{pbc_active=decrzero(Active)};
update(read_repairs, Moment, State=#state{read_repairs_total=RRT}) ->
    spiral_incr(#state.read_repairs, Moment, State#state{read_repairs_total=RRT+1});
update(coord_redir, _Moment, State=#state{coord_redirs_total=CRT}) ->
    State#state{coord_redirs_total=CRT+1};
update(mapper_start, _Moment, State=#state{mapper_count=Count0}) ->
    State#state{mapper_count=Count0 + 1};
update(mapper_end, _Moment, State=#state{mapper_count=Count0}) ->
    State#state{mapper_count=decrzero(Count0)};
update(_, _, State) ->
    State.

tick(Field, State) ->
    basho_metrics_nifs:meter_tick(element(2, element(Field, State))).

update_metric(Field, Value, State) when is_integer(Value) ->
    case element(Field, State) of
        {meter, M} ->
            basho_metrics_nifs:meter_update(M, Value);
        {histogram, H} ->
            basho_metrics_nifs:histogram_update(H, Value)
    end,
    State;
update_metric(Field, Value, State) ->
    lager:error("Ignoring non-integer stats update for field ~p, value ~p", [Field, Value]),
    State.

%% @spec update(Stat::term(), integer(), state()) -> state()
%% @doc Update the given stat in State, returning a new State.
update1(vnode_get, _, State) -> 
    update_metric(#state.vnode_gets, 1, State);
update1(vnode_put, _, State) -> 
    update_metric(#state.vnode_puts, 1, State);
update1(vnode_index_read, _, State) -> 
    update_metric(#state.vnode_index_reads, 1, State);
update1({vnode_index_write, Postings}, _, State) ->
    update_metric(#state.vnode_index_writes_postings,
                  Postings, 
                  update_metric(#state.vnode_index_writes, 1, State));
update1({vnode_index_delete, Postings}, _, State) ->
    update_metric(#state.vnode_index_deletes_postings,
                  Postings, 
                  update_metric(#state.vnode_index_deletes, 1, State));
update1({get_fsm_time, Microsecs}, _, State) ->
    update_metric(#state.get_meter, 1, 
                  update_metric(#state.get_fsm_time, Microsecs, State));
update1({put_fsm_time, Microsecs}, _, State) ->
    update_metric(#state.put_meter, 1, 
                  update_metric(#state.put_fsm_time, Microsecs, State));
update1(pbc_connect, _, State=#state{pbc_active=Active}) ->
    update_metric(#state.pbc_connects, 1, State#state{pbc_active=Active+1});
update1(pbc_disconnect, _, State=#state{pbc_active=Active}) ->
    State#state{pbc_active=decrzero(Active)};
update1(read_repairs, _, State) ->
    update_metric(#state.read_repairs, 1, State);
update1(coord_redir, _, State) ->
    update_metric(#state.coord_redirs, 1, State);
update1(mapper_start, _Moment, State=#state{mapper_count=Count0}) ->
    State#state{mapper_count=Count0+1};
update1(mapper_end, _Moment, State=#state{mapper_count=Count0}) ->
    State#state{mapper_count=decrzero(Count0)};
update1(_, _, State) ->
    State.

%% @doc decrement down to zero - do not go negative
decrzero(0) ->
    0;
decrzero(N) ->
    N-1.

%% @spec spiral_incr(integer(), integer(), state()) -> state()
%% @doc Increment the value of a spiraltime structure at a given
%%      position of the State tuple.
spiral_incr(Elt, Moment, State) ->
    setelement(Elt, State,
               spiraltime:incr(1, Moment, element(Elt, State))).

%% @spec spiral_incr(integer(), integer(), integer(), state()) -> state()
%% @doc Increment the value of a spiraltime structure at a given
%%      position of the State tuple.
spiral_incr(Elt, Amount, Moment, State) ->
    setelement(Elt, State,
               spiraltime:incr(Amount, Moment, element(Elt, State))).

%% @spec slide_incr(integer(), term(), integer(), state()) -> state()
%% @doc Update a slide structure at a given position in the
%%      STate tuple.
slide_incr(Elt, Reading, Moment, State) ->
    setelement(Elt, State,
               slide:update(element(Elt, State), Reading, Moment)).

%% @spec produce_stats(state(), integer()) -> proplist()
%% @doc Produce a proplist-formatted view of the current aggregation
%%      of stats.
produce_stats(State, Moment) ->
    lists:append(
      [vnode_stats(Moment, State),
       node_stats(Moment, State),
       cpu_stats(),
       mem_stats(),
       disk_stats(),
       system_stats(),
       ring_stats(),
       config_stats(),
       pbc_stats(Moment, State),
       app_stats(),
       mapper_stats(State),
       memory_stats()
      ]).

%% @spec spiral_minute(integer(), integer(), state()) -> integer()
%% @doc Get the count of events in the last minute from the spiraltime
%%      structure at the given element of the state tuple.
spiral_minute(_Moment, Elt, State) ->
    {_,Count} = spiraltime:rep_minute(element(Elt, State)),
    Count.

%% @spec slide_minute(integer(), integer(), state()) ->
%%         {Count::integer(), Mean::ustat(),
%%          {Median::ustat(), NinetyFive::ustat(),
%%           NinetyNine::ustat(), Max::ustat()}}
%% @type ustat() = undefined | number()
%% @doc Get the Count of readings, the Mean of those readings, and the
%%      Median, 95th percentile, 99th percentile, and Maximum readings
%%      for the last minute from the slide structure at the given
%%      element of the state tuple.
%%      If Count is 0, then all other elements will be the atom
%%      'undefined'.
slide_minute(Moment, Elt, State) ->
    {Count, Mean, Nines} = slide:mean_and_nines(element(Elt, State), Moment),
    {Count, Mean, Nines}.

metric_stats({meter, M}) ->
    basho_metrics_nifs:meter_stats(M);
metric_stats({histogram, H}) ->
    basho_metrics_nifs:histogram_stats(H).

meter_minute(Stats) ->
    trunc(proplists:get_value(one, Stats)).

%% @spec vnode_stats(integer(), state()) -> proplist()
%% @doc Get the vnode-sum stats proplist.
vnode_stats(Moment, State=#state{legacy=true}) ->
    lists:append(
      [{F, spiral_minute(Moment, Elt, State)}
       || {F, Elt} <- [{vnode_gets, #state.vnode_gets},
                       {vnode_puts, #state.vnode_puts},
                       {vnode_index_reads, #state.vnode_index_reads},
                       {vnode_index_writes, #state.vnode_index_writes},
                       {vnode_index_writes_postings, #state.vnode_index_writes_postings},
                       {vnode_index_deletes, #state.vnode_index_deletes},
                       {vnode_index_deletes_postings, #state.vnode_index_deletes_postings},
                       {read_repairs,#state.read_repairs}]],
      [{vnode_gets_total, State#state.vnode_gets_total},
       {vnode_puts_total, State#state.vnode_puts_total},
       {vnode_index_reads_total, State#state.vnode_index_reads_total},
       {vnode_index_writes_total, State#state.vnode_index_writes_total},
       {vnode_index_writes_postings_total, State#state.vnode_index_writes_postings_total},
       {vnode_index_deletes_total, State#state.vnode_index_deletes_total},
       {vnode_index_deletes_postings_total, State#state.vnode_index_deletes_postings_total}]);
vnode_stats(_, State=#state{legacy=false}) ->
    VG = metric_stats(State#state.vnode_gets),
    VP = metric_stats(State#state.vnode_puts),
    VIR = metric_stats(State#state.vnode_index_reads),
    VIW = metric_stats(State#state.vnode_index_writes),
    VIWP = metric_stats(State#state.vnode_index_writes_postings),
    VID = metric_stats(State#state.vnode_index_deletes),
    VIDP = metric_stats(State#state.vnode_index_deletes_postings),
    RR = metric_stats(State#state.read_repairs),
    CR = metric_stats(State#state.coord_redirs),
    [{vnode_gets, meter_minute(VG)},
     {vnode_puts, meter_minute(VP)},
     {vnode_index_reads, meter_minute(VIR)},
     {vnode_index_writes, meter_minute(VIW)},
     {vnode_index_writes_postings, meter_minute(VIWP)},
     {vnode_index_deletes, meter_minute(VID)},
     {vnode_index_deletes_postings, meter_minute(VIDP)},
     {read_repairs, meter_minute(RR)},
     {coord_redirs, meter_minute(CR)},
     {vnode_gets_total, proplists:get_value(count, VG)},
     {vnode_puts_total, proplists:get_value(count, VP)},
     {vnode_index_reads_total, proplists:get_value(count, VIR)},
     {vnode_index_writes_total, proplists:get_value(count, VIW)},
     {vnode_index_writes_postings_total, proplists:get_value(count, VIWP)},
     {vnode_index_deletes_total, proplists:get_value(count, VID)},
     {vnode_index_deletes_postings_total, proplists:get_value(count, VIDP)}].

%% @spec node_stats(integer(), state()) -> proplist()
%% @doc Get the node stats proplist.
node_stats(Moment, State=#state{node_gets_total=NGT,
                                node_puts_total=NPT,
                                read_repairs_total=RRT,
                                coord_redirs_total=CRT,
                                legacy=true}) ->
    {Gets, GetMean, {GetMedian, GetNF, GetNN, GetH}} =
        slide_minute(Moment, #state.get_fsm_time, State),
    {Puts, PutMean, {PutMedian, PutNF, PutNN, PutH}} =
        slide_minute(Moment, #state.put_fsm_time, State),
    [{node_gets, Gets},
     {node_gets_total, NGT},
     {node_get_fsm_time_mean, GetMean},
     {node_get_fsm_time_median, GetMedian},
     {node_get_fsm_time_95, GetNF},
     {node_get_fsm_time_99, GetNN},
     {node_get_fsm_time_100, GetH},
     {node_puts, Puts},
     {node_puts_total, NPT},
     {node_put_fsm_time_mean, PutMean},
     {node_put_fsm_time_median, PutMedian},
     {node_put_fsm_time_95, PutNF},
     {node_put_fsm_time_99, PutNN},
     {node_put_fsm_time_100, PutH},
     {read_repairs_total, RRT},
     {coord_redirs_total, CRT}];
node_stats(_, State=#state{legacy=false}) ->
    PutInfo = metric_stats(State#state.put_fsm_time),
    GetInfo = metric_stats(State#state.get_fsm_time),
    RRInfo =  metric_stats(State#state.read_repairs),
    CRInfo =  metric_stats(State#state.coord_redirs),
    NodeGets = meter_minute(metric_stats(State#state.get_meter)),
    NodePuts = meter_minute(metric_stats(State#state.put_meter)),
    [{node_gets, NodeGets},
     {node_gets_total, proplists:get_value(count, GetInfo)},
     {node_get_fsm_time_mean, proplists:get_value(mean, GetInfo)},
     {node_get_fsm_time_median, proplists:get_value(p50, GetInfo)},
     {node_get_fsm_time_95, proplists:get_value(p95, GetInfo)},
     {node_get_fsm_time_99, proplists:get_value(p99, GetInfo)},
     {node_get_fsm_time_100, proplists:get_value(max, GetInfo)},
     {node_puts, NodePuts},
     {node_puts_total, proplists:get_value(count, PutInfo)},
     {node_put_fsm_time_mean, proplists:get_value(mean, PutInfo)},
     {node_put_fsm_time_median, proplists:get_value(p50, PutInfo)},
     {node_put_fsm_time_95, proplists:get_value(p95, PutInfo)},
     {node_put_fsm_time_99, proplists:get_value(p99, PutInfo)},
     {node_put_fsm_time_100, proplists:get_value(max, PutInfo)},
     {read_repairs_total, proplists:get_value(count, RRInfo)},
     {coord_redirs_total, proplists:get_value(count, CRInfo)}].


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

mapper_stats(#state{mapper_count=Count}) ->
    [{executing_mappers, Count}].

%% @spec pbc_stats(integer(), state()) -> proplist()
%% @doc Get stats on the disk, as given by the disksup module
%%      of the os_mon application.
pbc_stats(Moment, State=#state{pbc_connects_total=NCT, pbc_active=Active, legacy=true}) ->
    case whereis(riak_kv_pb_socket_sup) of
        undefined ->
            [];
        _ -> [{pbc_connects_total, NCT},
              {pbc_connects, spiral_minute(Moment, #state.pbc_connects, State)},
              {pbc_active, Active}]
    end;
pbc_stats(_, State=#state{pbc_connects_total=NCT, 
                          pbc_active=Active, legacy=false}) ->
    case whereis(riak_kv_pb_socket_sup) of
        undefined ->
            [];
        _ ->
            NC = metric_stats(State#state.pbc_connects),
            [{pbc_connects_total, NCT},
             {pbc_connects, meter_minute(NC)},
             {pbc_active, Active}]
    end.    


remove_slide_private_dirs() ->
    os:cmd("rm -rf " ++ slide:private_dir()).
