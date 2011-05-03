%% -------------------------------------------------------------------
%%
%% riak_map_master: spins up batched map tasks on behalf of map phases
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

-module(riak_kv_map_master).
-include_lib("riak_kv_js_pools.hrl").

-behaviour(gen_server2).

%% API
-export([start_link/0,
         queue_depth/0,
         new_mapper/4]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(mapper, {vnode,
                 qterm,
                 inputs,
                 phase}).

-record(state, {datadir,
                store,
                highest,
                next}).

new_mapper({_, Node}=VNode, QTerm, MapInputs, PhasePid) ->
    gen_server2:pcall({?SERVER, Node}, 5, {new_mapper, VNode,
                                           QTerm, MapInputs, PhasePid}, infinity).

queue_depth() ->
    Nodes = [node()|nodes()],
    [{Node, gen_server2:pcall({?SERVER, Node}, 0, queue_depth,
                              infinity)} || Node <- Nodes].


start_link() ->
    gen_server2:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
    process_flag(trap_exit, true),
    DataDir = init_data_dir(),
    Store = bitcask:open(DataDir, [read_write]),
    {ok, NextCounter} = file:open(filename:join(DataDir, "_next_"), [read, write, raw, binary]),
    {ok, HighestCounter} = file:open(filename:join(DataDir, "_highest_"), [read, write, raw, binary]),
    State =  #state{datadir=DataDir, store=Store, highest=HighestCounter,
                    next=NextCounter},
    reset_counters(State),
    timer:send_interval(60000, merge_storage),
    {ok, State}.

handle_call({new_mapper, VNode, {erlang, _}=QTerm, MapInputs, PhasePid}, _From, State) ->
    Id = make_id(),
    case riak_kv_mapper_sup:new_mapper(VNode, Id, QTerm, MapInputs, PhasePid) of
        {ok, _Pid} ->         
            {reply, {ok, Id}, State};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;

handle_call({new_mapper, VNode, {javascript, _}=QTerm, MapInputs, PhasePid}, _From, State) ->
    case riak_kv_js_manager:pool_size(?JSPOOL_MAP) > 0 of
        true ->
            Id = make_id(),
            case riak_kv_mapper_sup:new_mapper(VNode, Id, QTerm, MapInputs, PhasePid) of
                {ok, Pid} ->
                    erlang:monitor(process, Pid),
                    {reply, {ok, Id}, State};
                {error, Reason} ->
                    {reply, {error, Reason}, State}
            end;
        false ->
            Id = defer_mapper(VNode, QTerm, MapInputs, PhasePid, State),
            {reply, {ok, {Id, node()}}, State}
    end;

handle_call(queue_depth, _From, #state{highest=Highest, next=Next}=State) ->
    H = read_counter(Highest),
    N = read_counter(Next),
    Reply = H - N,
    {reply, Reply, State};

handle_call(_Request, _From, State) ->
    {reply, ignore, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

%% Dequeue a deferred mapper when a mapper exits
handle_info({'DOWN', _A, _B, _Mapper, _C}, State) ->
    dequeue_mapper(State),
    {noreply, State};

handle_info(merge_storage, #state{store=Store, datadir=DataDir}=State) ->
    case bitcask:needs_merge(Store) of
        {true, Files} ->
            bitcask_merge_worker:merge(DataDir, [], Files);
        false ->
            ok
    end,
    {noreply, State};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{store=Store, highest=Highest, next=Next}) ->
    file:close(Highest),
    file:close(Next),
    bitcask:close(Store).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Internal functions
make_id() ->
    {_, _, T3} = erlang:now(),
    {T3, node()}.

dequeue_mapper(State) ->
    case are_mappers_waiting(State) of
        false ->
            ok;
        true ->
            Id = read(State#state.next),
            Mapper = read_entry(Id, State#state.store),
            case is_mapper_runnable(Mapper) of
                false ->
                    incr(State#state.next),
                    delete_entry(Id, State#state.store),
                    dequeue_mapper(State);
                true ->
                    #mapper{vnode=VNode, qterm=QTerm,
                            inputs=MapInputs, phase=Phase} = Mapper,
                    case riak_kv_js_manager:pool_size(?JSPOOL_MAP) > 0 of
                        true ->
                            {ok, Pid} = riak_kv_mapper_sup:new_mapper(VNode, {Id, node()}, QTerm,
                                                                      MapInputs, Phase),
                            erlang:monitor(process, Pid),
                            incr(State#state.next),
                            delete_entry(Id, State#state.store),
                            dequeue_mapper(State);
                        false ->
                            ok
                    end
            end
    end.

defer_mapper(VNode, QTerm, MapInputs, PhasePid, State) ->
    Mapper = #mapper{vnode=VNode, qterm=QTerm, inputs=MapInputs, phase=PhasePid},
    Id = read_incr(State#state.highest),
    write_entry(Id, Mapper, State#state.store).

reset_counters(State) ->
    case are_mappers_waiting(State) of
        false ->
            file:pwrite(State#state.highest, 0, <<0:64>>),
            file:sync(State#state.highest),
            file:pwrite(State#state.next, 0, <<0:64>>),
            file:sync(State#state.next);
        true ->
            dequeue_mapper(State)
    end.

read(CounterFile) ->
    Counter = read_counter(CounterFile),
    list_to_binary(integer_to_list(Counter)).

incr(CounterFile) ->
    Counter = read_counter(CounterFile),
    NewCounter = Counter + 1,
    ok = file:pwrite(CounterFile, 0, <<NewCounter:64>>),
    file:sync(CounterFile).

read_incr(CounterFile) ->
    Counter = read_counter(CounterFile),
    NewCounter = Counter + 1,
    ok = file:pwrite(CounterFile, 0, <<NewCounter:64>>),
    file:sync(CounterFile),
    list_to_binary(integer_to_list(Counter)).

read_counter(Counter) ->
    case file:pread(Counter, 0, 8) of
        eof ->
            0;
        {ok, Data} ->
            <<V:64/integer>> = Data,
            V;
        Error ->
            throw(Error)
    end.

are_mappers_waiting(State) ->
    Highest = read_counter(State#state.highest),
    Next = read_counter(State#state.next),
    Next < Highest.

is_mapper_runnable({error,_}) -> false;
is_mapper_runnable(not_found) -> false;
is_mapper_runnable(#mapper{phase=Phase}) ->
    Node = node(Phase),
    ClusterNodes = riak_core_node_watcher:nodes(riak_kv),
    lists:member(Node, ClusterNodes) andalso rpc:call(Node, erlang, is_process_alive,
                                                      [Phase]).

write_entry(Id, Mapper, Store) ->
    ok = bitcask:put(Store, Id, term_to_binary(Mapper, [compressed])),
    Id.

read_entry(Id, Store) ->
    case bitcask:get(Store, Id) of
        {ok, D} ->  binary_to_term(D);
        Err -> Err
    end.

delete_entry(Id, Store) ->
    bitcask:delete(Store, Id).

ensure_dir(Dir) ->
    filelib:ensure_dir(filename:join(Dir, ".empty")).

init_data_dir() ->
    %% There are some upgrade situations where the mapred_queue_dir, is not
    %% specified and as such we'll wind up using the default data/mr_queue,
    %% relative to current working dir. This causes problems in a packaged env
    %% (such as rpm/deb) where the current working dir is NOT writable. To
    %% accomodate these situations, we fallback to creating the mr_queue in
    %% /tmp.
    {ok, Cwd} = file:get_cwd(),
    DataDir0 = app_helper:get_env(riak_kv, mapred_queue_dir,
                                  filename:join(Cwd, "data/mr_queue")),
    case ensure_dir(DataDir0) of
        ok ->
            DataDir0;
        {error, Reason} ->
            error_logger:warning_msg("Failed to create ~p for mapred_queue_dir (~p); "
                                     "defaulting to /tmp/mr_queue\n",
                                     [DataDir0, Reason]),
            ok = ensure_dir("/tmp/mr_queue"),
            "/tmp/mr_queue"
    end.

