%% -------------------------------------------------------------------
%%
%% riak_object_merge_statem: quickcheck that merge function
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
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

-module(riak_object_merge_statem).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

-record(state,{vnodes=[], vnode_data=[], n=0, r=0, time=0}).

-define(B, {<<"t">>, <<"b">>}).
-define(K, <<"k">>).

-define(NUMTESTS, 1000).
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) ->
                              io:format(user, Str, Args) end, P)).

run(Module, Count) ->
    {atom_to_list(Module), {timeout, 120, [?_assert(prop_merge(Count))]}}.

%% Initialize the state
initial_state() ->
    #state{}.

%% Command generator, S is the state
command(#state{vnodes=VNodes, vnode_data=VNodeData, n=N, r=R, time=Time}) ->
    oneof([{call, ?MODULE, set_nr, [?LET(NVal, choose(3, 10), {NVal, choose(1, NVal)})]} || N == 0] ++
           [{call, ?MODULE, make_ring, [VNodes, vector(N, binary(8))]} || N > 0, length(VNodes) < N * 2] ++
           [{call, ?MODULE, get, [elements(VNodes), R, VNodes, VNodeData]} || length(VNodes) >= N, N > 0] ++
           [{call, ?MODULE, put, [binary(), elements(VNodes), VNodeData, Time]} || length(VNodes) >= N, N > 0] ++
           [{call, ?MODULE, replicate, [elements(VNodes), elements(VNodes), VNodeData, Time]} || length(VNodes) >= N, N > 0] ++
           [{call, ?MODULE, update, [binary(), elements(VNodes), R, elements(VNodes), VNodes, VNodeData, Time]} || length(VNodes) > N, N > 0]).

%% Next state transformation, S is the current state
next_state(S=#state{time=T}, _V, {call, ?MODULE, set_nr, [{N, R}]}) ->
    S#state{n=N, r=R, time=T+1};
next_state(S=#state{vnodes=VNodes, time=T}, _V, {call, ?MODULE, make_ring, [_, NewVNodes0]}) ->
    NewVNodes = lists:filter(fun(Id) -> not lists:member(Id, VNodes) end, NewVNodes0),
    S#state{vnodes=VNodes ++ NewVNodes, time=T+1};
next_state(S=#state{vnode_data=VNodeData, time=T}, Res, {call, ?MODULE, put, _}) ->
    S#state{time=T+1, vnode_data=[Res | VNodeData]};
next_state(S=#state{vnode_data=VNodeData, time=T}, Res, {call, ?MODULE, replicate, _}) ->
    S#state{time=T+1, vnode_data=[Res | VNodeData]};
next_state(S=#state{vnode_data=VNodeData, time=T}, Res, {call, ?MODULE, update, _}) ->
    S#state{time=T+1, vnode_data=[Res | VNodeData]};
next_state(S=#state{time=T}, _V, _C) ->
    S#state{time=T+1}.

precondition(_S, _) ->
    true.

%% Postcondition, checked after command has been evaluated
%% OBS: S is the state before next_state(S,_,<command>)
postcondition(_S, {call, ?MODULE, get, _}, {undefined, undefined}) ->
    true;
postcondition(_S, {call, ?MODULE, get, _}, {O, DVV}) ->
    lists:sort(riak_object:get_values(O))
        ==
    lists:sort(dvvset:values(DVV));
postcondition(_S,{call,_,_,_},_Res) ->
    true.

prop_merge(NumTests) ->
    eqc:quickcheck(eqc:numtests(NumTests, ?QC_OUT(prop_merge()))).

prop_merge() ->
    ?FORALL(Cmds,commands(?MODULE),
            begin
                {H, S=#state{vnodes=VNodes, vnode_data=VNodeData}, Res} = run_commands(?MODULE,Cmds),
                %% Check that collapsing all values leads to a correct(?) result
                {OValues, DVVValues} = case VNodes of
                                           [] ->
                                               {[undefined], [undefined]};
                                           _L ->
                                               case get(hd(VNodes), length(VNodes), VNodes, VNodeData) of
                                                   {undefined, undefined} ->
                                                       {[], []};
                                                   {O, DVV} ->
                                                       {riak_object:get_values(O), dvvset:values(DVV)}
                                               end
                                       end,
                collect(with_title(sibling_count), length(OValues),
                        aggregate(command_names(Cmds),
                                  pretty_commands(?MODULE,Cmds, {H,S,Res},
                                                  conjunction([{result,  equals(Res, ok)},
                                                               {values, equals(lists:sort(OValues), lists:sort(DVVValues))}])
                                                 )))
            end).

set_nr(_) ->
    ok.

make_ring(_, _) ->
    ok.

get(VNode1, R, VNodes, VNodeData) ->
    Start = index_of(VNode1, VNodes),
    Preflist = preflist(Start, R, VNodes),
    {Objects, DVVSets} = lists:foldl(fun(VNodeN, {Os, DVVs}) ->
                                             {ROBj, DVVSet} = get(VNodeN, VNodeData),
                                             {[ROBj |  Os], [DVVSet | DVVs]}
                                     end,
                                     {[], []},
                                     Preflist),
    case {lists:filter(fun dropundef/1, Objects), lists:filter(fun dropundef/1, DVVSets)} of
        {[], []} ->
            {undefined, undefined};
        {FilterO, FilterDVV} ->
            {riak_object:reconcile(FilterO, true), dvvset:sync(FilterDVV)}
    end.

%% Like when a user does a put without first fetching (blank vv)
put(Value, Coord, VNodeData, Time) ->
    {RObj, DVV} = coord_put(Coord, Value, Time, VNodeData),
    {Coord, RObj, DVV, Time}.

coord_put(VNode, Value, Time, VNodeData) ->
    {RObj, DVV} = get(VNode, VNodeData),
    RObj2 = coord_put_ro(VNode, riak_object:new(?B, ?K, Value), RObj, Time),
    DVV2 = coord_put_dvv(VNode, dvvset:new(Value), DVV),
    {RObj2, DVV2}.

coord_put_ro(VNode, NewObj, undefined, Time) ->
    riak_object:increment_vclock(NewObj, VNode, Time);
coord_put_ro(VNode, NewObj, OldObj, Time) ->
    LocalVC = riak_object:vclock(OldObj),
    PutVC = riak_object:vclock(NewObj),

    case vclock:descends(PutVC, LocalVC) of
        true ->
            riak_object:increment_vclock(NewObj, VNode, Time);
        false ->
            %% The PUT object is concurrent with some other PUT,
            %% so merge the PUT object and the local object.
            MergedClock = vclock:merge([PutVC, LocalVC]),
            FrontierClock = vclock:increment(VNode, Time, MergedClock),
            {ok, Dot} = vclock:get_entry(VNode, FrontierClock),

            DottedPutObject = riak_object:assign_dot(NewObj, Dot),
            MergedObject = riak_object:merge(DottedPutObject, OldObj),
            riak_object:set_vclock(MergedObject, FrontierClock)
    end.

coord_put_dvv(VNode, DVV, undefined) ->
    dvvset:update(DVV, VNode);
coord_put_dvv(VNode, NewDVV, OldDVV) ->
    dvvset:update(NewDVV, OldDVV, VNode).

replicate(From, To, VNodeData, Time) ->
    {FromObj, FromDVV} = get(From, VNodeData),
    {ToObj, ToDVV} = get(To, VNodeData),
    RObj = merge_put_ro(FromObj, ToObj),
    DVV = merge_put_dvv(FromDVV, ToDVV),
    {To, RObj, DVV, Time}.

merge_put_ro(undefined, New) ->
    New;
merge_put_ro(Old, undefined) ->
    Old;
merge_put_ro(Old, New) ->
    riak_object:syntactic_merge(Old, New).

merge_put_dvv(undefined, New) ->
    New;
merge_put_dvv(Old, undefined) ->
    Old;
merge_put_dvv(Old, New) ->
    dvvset:sync([Old, New]).

update(Value, ReadVNode, R, WriteVNode, VNodes, VNodeData, Time) ->
    {RObj, DVV} = get(ReadVNode, R, VNodes, VNodeData),
    RObj1 = update_ro(Value, RObj),
    DVV1 = update_dvv(Value, DVV),
    {OldObj, OldDVV} = get(WriteVNode, VNodeData),
    ROBj2 = coord_put_ro(WriteVNode, RObj1, OldObj, Time),
    DVV2 = coord_put_dvv(WriteVNode, DVV1, OldDVV),
    {WriteVNode, ROBj2, DVV2, Time}.

update_ro(Value, undefined) ->
    riak_object:new(?B, ?K, Value);
update_ro(Value, RObj) ->
    RObj2 = riak_object:update_value(RObj, Value),
    riak_object:apply_updates(RObj2).

update_dvv(Value, undefined) ->
    dvvset:new(Value);
update_dvv(Value, DVV) ->
    Context = dvvset:join(DVV),
    dvvset:new(Context, Value).

dropundef(undefined) ->
    false;
dropundef(_E) ->
    true.

get(VNode, VNodeData) ->
    case lists:keyfind(VNode, 1, VNodeData) of
        {VNode, RObj, DVV, _T0} ->
            {RObj, DVV};
        false -> {undefined, undefined}
    end.

preflist(Start, RW, VNodes) ->
    preflist(Start, RW, VNodes, 0 , []).

preflist(_Start, RW, _VNodes, RW, PL) ->
    lists:reverse(PL);
preflist(Start, RW, VNodes, Cnt, PL) when (Start + Cnt) < length(VNodes) ->
    preflist(Start, RW, VNodes, Cnt+1, [lists:nth((Start + Cnt), VNodes) | PL]);
preflist(Start, RW, VNodes, Cnt, PL) ->
    preflist(Start, RW, VNodes, Cnt+1, [lists:nth(((Start + Cnt) rem length(VNodes)) + 1, VNodes) | PL]).

index_of(VNode, VNodes) ->
    index_of(VNode, VNodes, 1).

index_of(_VNode, [], _) ->
    undefined;
index_of(VNode, [VNode | _Rest], Index) ->
    Index;
index_of(VNode, [_ | Rest], Index) ->
    index_of(VNode, Rest, Index+1).


-endif. % EQC
