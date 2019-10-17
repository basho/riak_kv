%% -------------------------------------------------------------------
%%
%% Copyright (c) 2011 Basho Technologies, Inc.
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

%% @doc `cluster_info' interrogation module.
-module(riak_pipe_cinfo).

-export([cluster_info_init/0,
         cluster_info_generator_funs/0]).

-include("riak_pipe.hrl").
-include("riak_pipe_debug.hrl").

%% @doc Unused.
-spec cluster_info_init() -> ok.
cluster_info_init() ->
    ok.

%% @doc Get the list of cluster_info-generating functions.  Current
%%      functions print information about pipelines, and vnode queues.
-spec cluster_info_generator_funs() ->
         [ {Name::string(), Interrogator::function()} ].
cluster_info_generator_funs() ->
    [
     {"Riak Pipelines", fun pipelines/1},
     {"Riak Pipeline Vnode Queues", fun queues/1}
    ].

%% @doc Print information about the active pipelines.
-spec pipelines(ClusterInfoHandle::term()) -> ok.
pipelines(C) ->
    Pipes = riak_pipe_builder_sup:builder_pids(),
    cluster_info:format(C, "Pipelines active: ~b~n", [length(Pipes)]),
    _ = [ pipeline(C, Pipe) || Pipe <- Pipes],
    ok.

%% @doc Print information about the given pipeline.
%%      The pid given should be that of the builder of the pipeline.
-spec pipeline(ClusterInfoHandle::term(), Builder::pid()) -> ok.
pipeline(C, Pipe) ->
    case riak_pipe_builder:fitting_pids(Pipe) of
        {ok, Fits} ->
            cluster_info:format(C, " - ~p fittings: ~b alive~n",
                                [Pipe, length(Fits)]),
            [ fitting(C, Fit) || Fit <- Fits ];
        gone ->
            cluster_info:format(C, " - ~p *gone*~n", [Pipe])
    end,
    ok.

%% @doc Print information about the given fitting.
%%      The pid given should be that of the fitting.
-spec fitting(ClusterInfoHandle::term(), Fitting::pid()) -> ok.
fitting(C, Fit) ->
    case riak_pipe_fitting:workers(Fit) of
        {ok, Workers} ->
            %% TODO: add 'name' from details? maybe module/etc. too?
            cluster_info:format(C, "   + ~p worker partitions: ~b~n",
                                [Fit, length(Workers)]),
            [ fitting_worker(C, W) || W <- Workers ];
        gone ->
            cluster_info:format(C, "   + ~p *gone*~n", [Fit])
    end,
    ok.

%% @doc Print the ring partition index of a vnode doing work for some
%%      fitting.  The `Worker' should be the index to print.
-spec fitting_worker(ClusterInfoHandle::term(),
                     Worker::riak_pipe_vnode:partition()) -> ok.
fitting_worker(C, W) ->
    cluster_info:format(C, "     * ~p~n", [W]),
    ok.

%% @doc Print information about each active `riak_pipe' vnode.
-spec queues(ClusterInfoHandle::term()) -> ok.
queues(C) ->
    VnodePids = riak_core_vnode_master:all_nodes(riak_pipe_vnode),
    cluster_info:format(C, "Vnodes active: ~b~n", [length(VnodePids)]),
    _ = [ queues(C, V) || V <- VnodePids],
    ok.

%% @doc Print information about the workers on the given vnode.
-spec queues(ClusterInfoHandle::term(), Vnode::pid()) -> ok.
queues(C, V) ->
    {Partition, Workers} = riak_pipe_vnode:status(V),
    cluster_info:format(C, " - ~p workers: ~b~n",
                        [Partition, length(Workers)]),
    _ = [ queue(C, W) || W <- Workers ],
    ok.

%% @doc Print information about the given worker.
-spec queue(ClusterInfoHandle::term(), [{atom(), term()}]) -> ok.
queue(C, WorkerProps) ->
    cluster_info:format(C, "   + ~p~n", [WorkerProps]),
    ok.
