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
    [(catch folsom_metrics:delete_metric(Stat)) || Stat <- folsom_metrics:get_metrics(),
                                                           is_tuple(Stat), element(1, Stat) == ?APP],
    [register_stat(stat_name(Name), Type) || {Name, Type} <- stats()],
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
    register_stats(),
    {ok, ok}.

handle_call(_Req, _From, State) ->
    {reply, ok, State}.

handle_cast({update, Arg}, State) ->
    do_update(Arg),
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
do_update({vnode_get, Idx, USecs}) ->
    folsom_metrics:notify_existing_metric({?APP, vnode, gets}, 1, spiral),
    create_or_update({?APP, vnode, gets, time}, USecs, histogram),
    do_per_index(gets, Idx, USecs);
do_update({vnode_put, Idx, USecs}) ->
    folsom_metrics:notify_existing_metric({?APP, vnode, puts}, 1, spiral),
    create_or_update({?APP, vnode, puts, time}, USecs, histogram),
    do_per_index(puts, Idx, USecs);
do_update(vnode_index_read) ->
    folsom_metrics:notify_existing_metric({?APP, vnode, index, reads}, 1, spiral);
do_update({vnode_index_write, PostingsAdded, PostingsRemoved}) ->
    folsom_metrics:notify_existing_metric({?APP, vnode, index, writes}, 1, spiral),
    folsom_metrics:notify_existing_metric({?APP, vnode, index, writes, postings}, PostingsAdded, spiral),
    folsom_metrics:notify_existing_metric({?APP, vnode, index, deletes, postings}, PostingsRemoved, spiral);
do_update({vnode_index_delete, Postings}) ->
    folsom_metrics:notify_existing_metric({?APP, vnode, index, deletes}, Postings, spiral),
    folsom_metrics:notify_existing_metric({?APP, vnode, index, deletes, postings}, Postings, spiral);
do_update({get_fsm, Bucket, Microsecs, Stages, undefined, undefined, PerBucket}) ->
    folsom_metrics:notify_existing_metric({?APP, node, gets}, 1, spiral),
    folsom_metrics:notify_existing_metric({?APP, node, gets, time}, Microsecs, histogram),
    do_stages([?APP, node, gets, time], Stages),
    do_get_bucket(PerBucket, {Bucket, Microsecs, Stages, undefined, undefined});
do_update({get_fsm, Bucket, Microsecs, Stages, NumSiblings, ObjSize, PerBucket}) ->
    folsom_metrics:notify_existing_metric({?APP, node, gets}, 1, spiral),
    folsom_metrics:notify_existing_metric({?APP, node, gets, time}, Microsecs, histogram),
    folsom_metrics:notify_existing_metric({?APP, node, gets, siblings}, NumSiblings, histogram),
    folsom_metrics:notify_existing_metric({?APP, node, gets, objsize}, ObjSize, histogram),
    do_stages([?APP, node, gets, time], Stages),
    do_get_bucket(PerBucket, {Bucket, Microsecs, Stages, NumSiblings, ObjSize});
do_update({put_fsm_time, Bucket,  Microsecs, Stages, PerBucket}) ->
    folsom_metrics:notify_existing_metric({?APP, node, puts}, 1, spiral),
    folsom_metrics:notify_existing_metric({?APP, node, puts, time}, Microsecs, histogram),
    do_stages([?APP, node, puts, time], Stages),
    do_put_bucket(PerBucket, {Bucket, Microsecs, Stages});
do_update({read_repairs, Indices, Preflist}) ->
    folsom_metrics:notify_existing_metric({?APP, node, gets, read_repairs}, 1, spiral),
    do_repairs(Indices, Preflist);
do_update(coord_redir) ->
    folsom_metrics:notify_existing_metric({?APP, node, puts, coord_redirs}, {inc, 1}, counter);
do_update(mapper_start) ->
    folsom_metrics:notify_existing_metric({?APP, mapper_count}, {inc, 1}, counter);
do_update(mapper_end) ->
    folsom_metrics:notify_existing_metric({?APP, mapper_count}, {dec, 1}, counter);
do_update(precommit_fail) ->
    folsom_metrics:notify_existing_metric({?APP, precommit_fail}, {inc, 1}, counter);
do_update(postcommit_fail) ->
    folsom_metrics:notify_existing_metric({?APP, postcommit_fail}, {inc, 1}, counter).

%% private
%% Per index stats (by op)
do_per_index(Op, Idx, USecs) ->
    IdxAtom = list_to_atom(integer_to_list(Idx)),
    create_or_update({?APP, vnode, Op, IdxAtom}, 1, spiral),
    create_or_update({?APP, vnode, Op, time, IdxAtom}, USecs, histogram).

%%  per bucket get_fsm stats
do_get_bucket(false, _) ->
    ok;
do_get_bucket(true, {Bucket, Microsecs, Stages, NumSiblings, ObjSize}=Args) ->
    case (catch folsom_metrics:notify_existing_metric({?APP, node, gets, Bucket}, 1, spiral)) of
        ok ->
            [folsom_metrics:notify_existing_metric({?APP, node, gets, Dimension, Bucket}, Arg, histogram)
             || {Dimension, Arg} <- [{time, Microsecs},
                                     {siblings, NumSiblings},
                                     {objsize, ObjSize}], Arg /= undefined],
            do_stages([?APP, node, gets, time, Bucket], Stages);
        {'EXIT', _} ->
            folsom_metrics:new_spiral({?APP, node, gets, Bucket}),
            [register_stat({?APP, node, gets, Dimension, Bucket}, histogram) || Dimension <- [time,
                                                                                  siblings,
                                                                                  objsize]],
            do_get_bucket(true, Args)
    end.

%% per bucket put_fsm stats
do_put_bucket(false, _) ->
    ok;
do_put_bucket(true, {Bucket, Microsecs, Stages}=Args) ->
    case (catch folsom_metrics:notify_existing_metric({?APP, node, puts, Bucket}, 1, spiral)) of
        ok ->
            folsom_metrics:notify_existing_metric({?APP, node, puts, time, Bucket}, Microsecs, histogram),
            do_stages([?APP, node, puts, time, Bucket], Stages);
        {'EXIT', _} ->
            register_stat({?APP, node, puts, Bucket}, spiral),
            register_stat({?APP, node, puts, time, Bucket}, histogram),
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
    create_or_update(list_to_tuple(Path ++ [Stage]), Time, histogram),
    do_stages(Path, Stages).

%% create dimensioned stats for read repairs.
%% The indexes are from get core [{Index, Reason::notfound|outofdate}]
%% preflist is a preflist of [{{Index, Node}, Type::primary|fallback}]
do_repairs(Indices, Preflist) ->
    lists:foreach(fun({{Idx, Node}, Type}) ->
                          case proplists:get_value(Idx, Indices) of
                              undefined ->
                                  ok;
                              Reason ->
                                  create_or_update({?APP, node, gets,  read_repairs, Node, Type, Reason}, 1, spiral)
                          end
                  end,
                  Preflist).

%% for dynamically created / dimensioned stats
%% that can't be registered at start up
create_or_update(Name, UpdateVal, Type) ->
    case (catch folsom_metrics:notify_existing_metric(Name, UpdateVal, Type)) of
        ok ->
            ok;
        {'EXIT', _} ->
            register_stat(Name, Type),
            create_or_update(Name, UpdateVal, Type)
    end.

%% Stats are namespaced by APP in folsom
%% so that we don't need to co-ordinate on naming
%% between apps.
stat_name(Name) when is_list(Name) ->
    list_to_tuple([?APP] ++ Name);
stat_name(Name) when is_atom(Name) ->
    {?APP, Name}.

%% @doc list of {Name, Type} for static
%% stats that we can register at start up
stats() ->
    [{[vnode, gets], spiral},
     {[vnode, gets, time], histogram},
     {[vnode, puts], spiral},
     {[vnode, puts, time], histogram},
     {[vnode, index, reads], spiral},
     {[vnode, index ,writes], spiral},
     {[vnode, index, writes, postings], spiral},
     {[vnode, index, deletes], spiral},
     {[vnode, index, deletes, postings], spiral},
     {[node, gets], spiral},
     {[node, gets, siblings], histogram},
     {[node, gets, objsize], histogram},
     {[node, gets, time], histogram},
     {[node, puts], spiral},
     {[node, puts, time], histogram},
     {[node, gets, read_repairs], spiral},
     {[node, puts, coord_redirs], counter},
     {mapper_count, counter},
     {precommit_fail, counter},
     {postcommit_fail, counter}].

%% @doc register a stat with folsom
register_stat(Name, spiral) ->
    folsom_metrics:new_spiral(Name);
register_stat(Name, counter) ->
    folsom_metrics:new_counter(Name);
register_stat(Name, histogram) ->
    %% get the global default histo type
    {SampleType, SampleArgs} = get_sample_type(Name),
    folsom_metrics:new_histogram(Name, SampleType, SampleArgs).

%% @doc the histogram sample type may be set in app.config
%% use key `stat_sample_type' in the `riak_kv' section. Or the
%% name of an `histogram' stat.
%% Check the folsom homepage for available types.
%% Defaults to `{slide_uniform, {60, 1028}}' (a uniform sliding window
%% of 60 seconds, with a uniform sample of at most 1028 entries)
get_sample_type(Name) ->
    SampleType0 = app_helper:get_env(riak_kv, stat_sample_type, {slide_uniform, {60, 1028}}),
    app_helper:get_env(riak_kv, Name, SampleType0).

%% @doc produce the legacy blob of stats for display.
produce_stats() ->
    riak_kv_stat_bc:produce_stats().
