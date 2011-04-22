%% -------------------------------------------------------------------
%%
%% riak_map_phase: manage the mechanics of a map phase of a MR job
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

-module(riak_kv_map_phase).
-author('Kevin Smith <kevin@basho.com>').
-author('John Muellerleile <johnm@basho.com>').

-include("riak_kv_map_phase.hrl").

-behaviour(luke_phase).

-export([init/1, handle_input/3, handle_input_done/1, handle_event/2,
         handle_sync_event/3, handle_info/2, handle_timeout/1, terminate/2]).

-record(state, {done=false, qterm, fsms=dict:new(), mapper_data=[]}).

init([QTerm]) ->
    process_flag(trap_exit, true),
    {ok, #state{qterm=QTerm}}.

handle_input(Inputs0, #state{fsms=FSMs0, qterm=QTerm, mapper_data=MapperData}=State, _Timeout) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Inputs1 = [build_input(I, Ring) || I <- Inputs0],
    case length(Inputs1) > 0 of
        true ->
            ClaimLists = riak_kv_mapred_planner:plan_map(Inputs1),
            {NewFSMs, _ClaimLists1, FsmKeys} = schedule_input(Inputs1, ClaimLists, QTerm, FSMs0, State),
            MapperData1 = MapperData ++ FsmKeys,
            {no_output, State#state{fsms=NewFSMs, mapper_data=MapperData1}};
        false ->
            {no_output, State}
    end.

handle_input_done(State) ->
    {no_output, maybe_done(State#state{done=true})}.

handle_event({register_mapper, Id, MapperPid}, #state{mapper_data=MapperData}=State) ->
    MapperData0 = case lists:keyfind(Id, 1, MapperData) of
        {Id, MapperProps} -> lists:keyreplace(Id, 1, MapperData, {Id, [{pid, MapperPid}|MapperProps]});
        false -> MapperData
    end,
    MapperData1 = MapperData0 ++ [{MapperPid, Id}],
    {no_output, State#state{mapper_data=MapperData1}};

handle_event({mapexec_reply, VNode, BKey, Reply, Executor}, #state{fsms=FSMs, mapper_data=MapperData,
                                                                   pending=Pending}=State) ->
    case dict:is_key(Executor, FSMs) of
        false ->
            %% node retry case will produce dictionary miss
            maybe_done(State);
        true ->
            case Reply of
                [{not_found, _, _}] ->
                    handle_not_found_reply(VNode, BKey, Executor, State, Reply);
                [{error, notfound}] ->
                    handle_not_found_reply(VNode, BKey, Executor, State, Reply);
                _ ->
                    Pending1 = Pending ++ Reply,
                    FSMs1 = update_counter(Executor, FSMs),
                    MapperData1 = update_inputs(Executor, VNode, BKey, MapperData),
                    maybe_done(State#state{fsms=FSMs1, mapper_data=MapperData1, pending=Pending1})
            end
    end;

handle_event({mapexec_error, _Executor, Reply}, State) ->
    %{no_output, State};
    {stop, Reply, State#state{fsms=[]}};
handle_event(_Event, State) ->
    {no_output, State}.


handle_info({'EXIT', Pid, _Reason}, #state{mapper_data=MapperData, fsms=FSMs, qterm=QTerm}=State) ->
    case lists:keyfind(Pid, 1, MapperData) of
        {Pid, Id} ->
            case lists:keyfind(Id, 1, MapperData) of
                {Id, MapperProps} ->
                    {keys, {VNode, Keys}} = lists:keyfind(keys, 1, MapperProps),
                    case length(Keys) of
                        0 ->
                            MapperData1 = lists:keydelete(Id, 1, lists:keydelete(Pid, 1, MapperData)),
                            {no_output, maybe_done(State#state{mapper_data=MapperData1})};
                        _C ->
                            try
                                {_Partition, BadNode} = VNode,
                                NewKeys = prune_input_nodes(Keys, BadNode),
                                ClaimLists = riak_kv_mapred_planner:plan_map(NewKeys),
                                {NewFSMs, _ClaimLists1, FsmKeys} = schedule_input(NewKeys, ClaimLists, QTerm, FSMs, State),
                                MapperData1 = lists:keydelete(Id, 1, lists:keydelete(Pid, 1, MapperData ++ FsmKeys)),
                                {no_output, maybe_done(State#state{mapper_data=MapperData1, fsms=NewFSMs})}
                            catch
                                _:Error ->
                                    {stop, {error, {no_candidate_nodes, Error, erlang:get_stacktrace(), MapperData}}, State}
                            end
                    end;
                false ->
                    MapperData1 = lists:keydelete(Pid, 1, MapperData),
                    {no_output, maybe_done(State#state{mapper_data=MapperData1})}
            end;
        false ->
            {stop, {error, {dead_mapper, erlang:get_stacktrace(), MapperData}}, State}
    end;

handle_info(_Info, State) ->
    {no_output, State}.

handle_sync_event(_Event, _From, State) ->
    {reply, ignored, State}.

handle_timeout(State) ->
    {no_output, State}.

terminate(_Reason, _State) ->
    _Reason.

%% Internal functions

schedule_input(Inputs1, ClaimLists, QTerm, FSMs0, State) ->
    try
        {FSMs1, FsmKeys} = start_mappers(ClaimLists, QTerm, FSMs0, []),
        {FSMs1, ClaimLists, FsmKeys}
    catch
        exit:{{nodedown, Node}, _} ->
            Inputs2 = prune_input_nodes(Inputs1, Node),
            ClaimLists2 = riak_kv_mapred_planner:plan_map(Inputs2),
            schedule_input(Inputs2, ClaimLists2, QTerm, FSMs0, State);
        Error ->
            throw(Error)
    end.

prune_input_nodes(Inputs, BadNode) ->
    prune_input_nodes(Inputs, BadNode, []).
prune_input_nodes([], _BadNode, NewInputs) ->
    NewInputs;
prune_input_nodes([Input|T], BadNode, NewInputs) ->
    #riak_kv_map_input{preflist=Targets} = Input,
    Targets2 = lists:keydelete(BadNode, 2, Targets),
    prune_input_nodes(T, BadNode, [Input#riak_kv_map_input{preflist=Targets2}|NewInputs]).

prune_input_partitions(Inputs, BadNode) ->
    prune_input_partitions(Inputs, BadNode, []).
prune_input_partitions([], _BadNode, NewInputs) ->
    NewInputs;
prune_input_partitions([Input|T], BadPartition, NewInputs) ->
    #riak_kv_map_input{preflist=Targets} = Input,
    Targets2 = lists:keydelete(BadPartition, 1, Targets),
    prune_input_partitions(T, BadPartition, [Input#riak_kv_map_input{preflist=Targets2}|NewInputs]).


build_input(I, Ring) ->
    {{Bucket, Key}, KD} = convert_input(I),
    Props = riak_core_bucket:get_bucket(Bucket, Ring),
    {value, {_, NVal}} = lists:keysearch(n_val, 1, Props),
    Idx = riak_core_util:chash_key({Bucket, Key}),
    PL = riak_core_ring:preflist(Idx, Ring),
    {Targets, _} = lists:split(NVal, PL),
    #riak_kv_map_input{bkey={Bucket, Key},
                       bprops=Props,
                       kd=KD,
                       preflist=Targets}.

convert_input(I={{_B,_K},_D})
  when is_binary(_B) andalso (is_list(_K) orelse is_binary(_K)) -> I;
convert_input(I={_B,_K})
  when is_binary(_B) andalso (is_list(_K) orelse is_binary(_K)) -> {I,undefined};
convert_input([B,K]) when is_binary(B), is_binary(K) -> {{B,K},undefined};
convert_input([B,K,D]) when is_binary(B), is_binary(K) -> {{B,K},D};
convert_input({struct, [{<<"not_found">>,
                     {struct, [{<<"bucket">>, Bucket},
                               {<<"key">>, Key}]}}]}) ->
    {{Bucket, Key}, undefined};
convert_input({not_found, {Bucket, Key}, KD}) ->
    {{Bucket, Key}, KD};
convert_input(I) -> I.

start_mappers([], _QTerm, Accum, FsmKeys) ->
    {Accum, FsmKeys};
start_mappers([{Partition, Inputs}|T], QTerm, Accum, FsmKeys) ->
    case riak_kv_map_master:new_mapper(Partition, QTerm, Inputs, self()) of
        {ok, FSM} ->
            Accum1 = dict:store(FSM, length(Inputs), Accum),
            start_mappers(T, QTerm, Accum1, FsmKeys ++ [{FSM, [{keys, {Partition, Inputs}}]}]);
        Error ->
            throw(Error)
    end.

update_counter(Executor, FSMs) ->
    case dict:find(Executor, FSMs) of
        {ok, 1} ->
            dict:erase(Executor, FSMs);
        {ok, _C} ->
            dict:update_counter(Executor, -1, FSMs)
    end.

maybe_done(#state{done=Done, fsms=FSMs, mapper_data=MapperData}=State) ->
    case Done =:= true andalso dict:size(FSMs) == 0 andalso MapperData == [] of
        true ->
            luke_phase:complete();
        false -> ok
    end,
    State.

update_inputs(Id, VNode, BKey, MapperData) ->
    case lists:keyfind(Id, 1, MapperData) of
        {Id, MapperProps} ->
            case lists:keyfind(keys, 1, MapperProps) of
                {keys, {VNode, Keys}} ->
                    MapperProps1 = lists:keyreplace(keys, 1, MapperProps,
                                     {keys, {VNode, lists:keydelete(BKey, 2, Keys)}}),
                    lists:keyreplace(Id, 1, MapperData, {Id, MapperProps1});
                false -> throw(bad_mapper_props_no_keys);
                _ -> MapperData
            end;
        false -> throw(bad_mapper_props_no_id)
    end.

handle_not_found_reply(VNode, BKey, Executor, #state{fsms=FSMs, mapper_data=MapperData, qterm=QTerm, pending=Pending}=State, Reply) ->
    %% If the reply is not_found, then check if there are other
    %% preflist entries that can be tried before giving up.

    %% Look up the properties for the replying mapper
    {_Id, MapperProps} = lists:keyfind(Executor, 1, MapperData),
    %% Extract the vnode data and the list of inputs
    {keys, {VNode, Keys}} = lists:keyfind(keys, 1, MapperProps),

    %% Remove the current partition from
    %% the list of potential inputs.
    {BadPartition, _Node} = VNode,
    NewKeys = prune_input_partitions(Keys, BadPartition),

    %% Create a new map plan using a different preflist entry.
    %% The call to plan_map will call exit with reason 
    %% exhausted_preflist if all the preference list 
    %% entries have been checked.
    try riak_kv_mapred_planner:plan_map(NewKeys) of
        ClaimLists ->
            FSMs1 = update_counter(Executor, FSMs),
            {NewFSMs, _ClaimLists1, FsmKeys} = schedule_input(NewKeys, ClaimLists, QTerm, FSMs1, State),
            MapperData1 = lists:keydelete(Executor, 1, MapperData ++ FsmKeys),
            maybe_done(State#state{mapper_data=MapperData1, fsms=NewFSMs})
    catch
        exit:exhausted_preflist ->
            %% At this point the preflist has been exhausted
            Pending1 = Pending ++ Reply,
            FSMs2 = update_counter(Executor, FSMs),
            MapperData2 = update_inputs(Executor, VNode, BKey, MapperData),
            maybe_done(State#state{fsms=FSMs2, mapper_data=MapperData2, pending=Pending1})
    end.




