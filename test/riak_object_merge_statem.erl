%% -------------------------------------------------------------------
%%
%% riak_object_merge_statem: EQC that the riak_object merge functions
%%                           are equivalent to the DVVSet ones.
%%
%% TODO DVV disabled? Get, interleave writes, Put
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

-record(state,{vnodes=[] :: [binary()], %% Sort of like the ring, upto N*2 vnodeids
               vnode_data=[] :: [{binary(), riak_object:object(), dvvset:dvvset(), pos_integer()}],
                                 %% The data, duplicated values for vnodes
                                 %% Newest at the head of the list.
                                 %% Prepend only data 'cos symbolic / dynamic state.
               last_get={undefined, undefined} :: {undefined | riak_object:object(), undefined | dvvset:dvvset()},
                                 %% Stash the last `get' result so we can use it to put later
               n=0 :: integer(), %% Generated NVal
               r=0 :: integer(), %% Generated R quorum
               time=0 :: integer()  %% For the vclocks
              }).

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
%% At first only a command to generate N and R vals is in the list.
%% Once that has been run, it is never run again, and instead a command to generate vnodes is run.
%% If we have at least N vnodes, the commands to get, put, replicate and update are run.
%% @see get/4, put/4, replicate/4, update/7 (7??!)
command(#state{vnodes=VNodes, vnode_data=VNodeData, n=N, r=R, time=Time, last_get=LastGet}) ->
    %% choose an N between 3 and 10 and an R between 1 and N
    oneof([{call, ?MODULE, set_nr, [?LET(NVal, choose(3, 10), {NVal, choose(1, NVal)})]} || N == 0] ++
              %% Make upto N*2 Vnodes (why? fallbacks maybe?)
           [{call, ?MODULE, make_ring, [VNodes, vector(N, binary(8))]} || N > 0, length(VNodes) < N * 2] ++
           [{call, ?MODULE, get, [elements(VNodes), R, VNodes, VNodeData]} || length(VNodes) >= N, N > 0] ++
           [{call, ?MODULE, put, [binary(), elements(VNodes), VNodeData, Time]} || length(VNodes) >= N, N > 0] ++
           [{call, ?MODULE, put, [binary(), LastGet, elements(VNodes), VNodeData, Time]} || length(VNodes) >= N, N > 0, LastGet /= {undefined, undefined}] ++
           [{call, ?MODULE, replicate, [elements(VNodes), elements(VNodes), VNodeData, Time]} || length(VNodes) >= N, N > 0] ++
           [{call, ?MODULE, update, [binary(), elements(VNodes), R, elements(VNodes), VNodes, VNodeData, Time]} || length(VNodes) > N, N > 0]).

%% Next state transformation, S is the current state
%% Note: time gets incremented after every command
next_state(S=#state{time=T}, _V, {call, ?MODULE, set_nr, [{N, R}]}) ->
    S#state{n=N, r=R, time=T+1};
next_state(S=#state{vnodes=VNodes, time=T}, _V, {call, ?MODULE, make_ring, [_, NewVNodes0]}) ->
    %% No duplicate vnode ids please!
    NewVNodes = lists:filter(fun(Id) -> not lists:member(Id, VNodes) end, NewVNodes0),
    S#state{vnodes=VNodes ++ NewVNodes, time=T+1};
next_state(S=#state{vnode_data=VNodeData, time=T}, Res, {call, ?MODULE, put, _}) ->
    %% The state data is prepend only, it grows and grows, but it's based on older state
    %% Newest at the front.
    S#state{time=T+1, vnode_data=[Res | VNodeData]};
next_state(S=#state{vnode_data=VNodeData, time=T}, Res, {call, ?MODULE, replicate, _}) ->
    S#state{time=T+1, vnode_data=[Res | VNodeData]};
next_state(S=#state{vnode_data=VNodeData, time=T}, Res, {call, ?MODULE, update, _}) ->
    S#state{time=T+1, vnode_data=[Res | VNodeData]};
next_state(S=#state{time=T}, Res, {call, ?MODULE, get, _}) ->
    %% Store the get result, so we can update and put it later
    S#state{time=T+1, last_get=Res};
next_state(S=#state{time=T}, _V, _C) ->
    S#state{time=T+1}.

%% TODO: do we need something here for shrinking valid vnodes?
precondition(_S, _) ->
    true.

%% Postcondition, checked after command has been evaluated
%% OBS: S is the state before next_state(S,_,<command>)
postcondition(_S, {call, ?MODULE, get, _}, {undefined, undefined}) ->
    true;
postcondition(_S, {call, ?MODULE, get, _}, {O, DVV}) ->
    %% What matters is that both types have the exact same results.
    lists:sort(riak_object:get_values(O)) == lists:sort(dvvset:values(DVV));
postcondition(_S,{call,_,_,_},_Res) ->
    true.

prop_merge(NumTests) ->
    eqc:quickcheck(eqc:numtests(NumTests, ?QC_OUT(prop_merge()))).

prop_merge() ->
    ?FORALL(Cmds,commands(?MODULE),
            begin
                application:set_env(riak_kv, dvv_enabled, true), %% set false for fails
                {H, S=#state{vnodes=VNodes, vnode_data=VNodeData}, Res} = run_commands(?MODULE,Cmds),
                %% Check that collapsing all values leads to the same results for dvv and riak_object
                {OValues, DVVValues} = case VNodes of
                                           [] ->
                                               {[], []};
                                           _L ->
                                               %% Get ALL vnodes values, not just R
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

%% Just for state mutation
%% @see next_state/3
set_nr(_) ->
    ok.

%% Just for state mutation
%% @see next_state/3
make_ring(_, _) ->
    ok.

%% get the data from `R' vnodes, starting with `VNode1'.  Simulates a
%% riak_kv Get. Gets results from all vnodes, then calls reconcile to
%% get only causally concurrent siblings.
%% @see put/4, put/5, replicate/4 and update/7 for how data gets stored
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

%% Like when a user does a put without first fetching (blank vv) @see
%% next_state/3 for the return tuple being prepended to vnode_data
put(Value, Coord, VNodeData, Time) ->
    {RObj, DVV} = coord_put(Coord, Value, Time, VNodeData),
    {Coord, RObj, DVV, Time}.

%% Put, but use the result from the last fetch operation
%% hopefully this simulates stale-ish puts
put(Value, {GotObj0, GotDVV0}, Coord, VNodeData, Time) ->
    {RObj, DVV} = get(Coord, VNodeData),
    GotObj = update_ro(Value, GotObj0),
    GotDVV = update_dvv(Value, GotDVV0),
    RObj2 = coord_put_ro(Coord, GotObj, RObj, Time),
    DVV2 = coord_put_dvv(Coord, GotDVV, DVV),
    {Coord, RObj2, DVV2, Time}.

%% Perform a riak_object and DVV coordinated put
coord_put(VNode, Value, Time, VNodeData) ->
    {RObj, DVV} = get(VNode, VNodeData),
    RObj2 = coord_put_ro(VNode, riak_object:new(?B, ?K, Value), RObj, Time),
    DVV2 = coord_put_dvv(VNode, dvvset:new(Value), DVV),
    {RObj2, DVV2}.

%% This copies directly from riak_kv_vnode put_merge.  This is the
%% second test that does this, clearly this code needs refactoring
%% into riak_obect, at least the second clause here (where there is a
%% local value)
coord_put_ro(VNode, NewObj, undefined, Time) ->
    riak_object:increment_vclock(NewObj, VNode, Time);
coord_put_ro(VNode, NewObj, OldObj, Time) ->
    riak_object:update(false, OldObj, NewObj, VNode, Time).

%% So much simpler!
coord_put_dvv(VNode, DVV, undefined) ->
    dvvset:update(DVV, VNode);
coord_put_dvv(VNode, NewDVV, OldDVV) ->
    dvvset:update(NewDVV, OldDVV, VNode).

%% Mutating multiple elements in vnode_data in place is bad idea
%% (symbolic vs dynamic state), so instead of treating Put and
%% replicate as the same action, this command handles the replicate
%% part of the put fsm. Data from some random vnode (From) is
%% replicated to some random vnode (To)
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

%% A well behaved client. Fetch, mutate, Put.  It would be nice to
%% interleave the get with some other puts in future
update(Value, ReadVNode, R, WriteVNode, VNodes, VNodeData, Time) ->
    {RObj, DVV} = get(ReadVNode, R, VNodes, VNodeData),
    RObj1 = update_ro(Value, RObj),
    DVV1 = update_dvv(Value, DVV),
    {OldObj, OldDVV} = get(WriteVNode, VNodeData),
    ROBj2 = coord_put_ro(WriteVNode, RObj1, OldObj, Time),
    DVV2 = coord_put_dvv(WriteVNode, DVV1, OldDVV),
    {WriteVNode, ROBj2, DVV2, Time}.

%% Ensure the context data is added to the put object.
update_ro(Value, undefined) ->
    riak_object:new(?B, ?K, Value);
update_ro(Value, RObj) ->
    RObj2 = riak_object:update_value(RObj, Value),
    riak_object:apply_updates(RObj2).

%% Ensure the context data is maintained
update_dvv(Value, undefined) ->
    dvvset:new(Value);
update_dvv(Value, DVV) ->
    Context = dvvset:join(DVV),
    dvvset:new(Context, Value).

%% Filter fun
dropundef(undefined) ->
    false;
dropundef(_E) ->
    true.

%% if a vnode does not yet have vnode data, return `undefined` for the
%% riak_object and DVVSet.
get(VNode, VNodeData) ->
    case lists:keyfind(VNode, 1, VNodeData) of
        {VNode, RObj, DVV, _T0} ->
            {RObj, DVV};
        false -> {undefined, undefined}
    end.

%% Generate a list of vnodes, RW long, starting with the vnode at
%% index Start
preflist(Start, RW, VNodes) ->
    preflist(Start, RW, VNodes, 0 , []).

preflist(_Start, RW, _VNodes, RW, PL) ->
    lists:reverse(PL);
preflist(Start, RW, VNodes, Cnt, PL) when (Start + Cnt) < length(VNodes) ->
    preflist(Start, RW, VNodes, Cnt+1, [lists:nth((Start + Cnt), VNodes) | PL]);
preflist(Start, RW, VNodes, Cnt, PL) ->
    preflist(Start, RW, VNodes, Cnt+1, [lists:nth(((Start + Cnt) rem length(VNodes)) + 1, VNodes) | PL]).

%% What is the index position of `VNode' in `VNodes'?
index_of(VNode, VNodes) ->
    index_of(VNode, VNodes, 1).

index_of(_VNode, [], _) ->
    undefined;
index_of(VNode, [VNode | _Rest], Index) ->
    Index;
index_of(VNode, [_ | Rest], Index) ->
    index_of(VNode, Rest, Index+1).


-endif. % EQC
