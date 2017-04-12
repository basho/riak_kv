%% -------------------------------------------------------------------
%%
%% riak_object_dvv_statem: EQC that the riak_object merge functions
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
%%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

-module(riak_object_dvv_statem).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

-record(state,{vnodes=[] :: [binary()], %% Sort of like the ring, upto N*2 vnodeids
               vnode_data=[] :: [{VNodeID :: binary(),
                                  riak_object:riak_object(),
                                  dvvset:dvvset(),
                                  Timestamp :: pos_integer()}],
                                 %% The data, duplicated values for vnodes
                                 %% Newest at the head of the list.
                                 %% Prepend only data 'cos symbolic / dynamic state.
               last_get=undefined :: undefined | {undefined | riak_object:riak_object(), undefined | dvvset:dvvset()},
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

%%====================================================================
%% eunit test
%%====================================================================

eqc_test_() ->
    {setup,
     fun() ->
             meck:new(riak_core_bucket),
             meck:expect(riak_core_bucket, get_bucket,
                         fun(_Bucket) -> [dvv_enabled] end)
     end,
     fun(_) ->
             meck:unload(riak_core_bucket)
     end,
     %% Kelly and Andrew T. have both recommended setting the eunit
     %% timeout at 2x the `eqc:testing_time'.
     [{timeout, 120, ?_assertEqual(true, eqc:quickcheck(eqc:testing_time(60, ?QC_OUT(prop_merge()))))}]
    }.

run() ->
    run(?NUMTESTS).

run(Count) ->
    eqc:quickcheck(eqc:numtests(Count, prop_merge())).

check() ->
    eqc:check(prop_merge()).

%% Initialize the state
-spec initial_state() -> eqc_statem:symbolic_state().
initial_state() ->
    #state{}.


%% ------ Grouped operator: set_nr
%% Only set N, R if N has not been set (ie run once, as the first command)
set_nr_pre(#state{n=N}) ->
     N == 0.

set_nr_args(_S) ->
    [?LET(NVal, choose(3, 10), {NVal, choose(1, NVal)})].

set_nr(_) ->
    %% Command args used for next state only
    ok.

set_nr_next(S=#state{time=T}, _V, [{N, R}]) ->
    S#state{n=N, r=R, time=T+1}.


%% ------ Grouped operator: make_ring
%% Generate a bunch of vnodes, only runs until enough are generated
make_ring_pre(#state{vnodes=VNodes, n=N}) ->
    N > 0 andalso length(VNodes) < N * 2.

make_ring_args(#state{vnodes=VNodes, n=N}) ->
    [VNodes, vector(N, binary(8))].

make_ring(_,_) ->
    %% Command args used for next state only
    ok.

make_ring_next(S=#state{vnodes=VNodes, time=T}, _V, [_, NewVNodes0]) ->
    %% No duplicate vnode ids please!
    NewVNodes = lists:filter(fun(Id) -> not lists:member(Id, VNodes) end, NewVNodes0),
    S#state{vnodes=VNodes ++ NewVNodes, time=T+1}.

%% ------ Grouped operator: get
%% Fetch from some vnodes, merge, and store for later use
get_pre(S) ->
    vnodes_ready(S).

get_args(#state{vnodes=VNodes, r=R, vnode_data=VNodeData}) ->
         [elements(VNodes), R, VNodes, VNodeData].

%% get the data from `R' vnodes, starting with `VNode1'.  Simulates a
%% riak_kv Get. Calls reconcile to get only causally concurrent
%% siblings.
%%
%% @see put/4, put/5, replicate/4 and update/7 for how data gets
%% stored
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

get_next(S=#state{time=T}, Res, _Args) ->
    %% Store the get result, so we can update and put it later
    S#state{time=T+1, last_get=Res}.

get_post(_S, _Args, {undefined, undefined}) ->
    true;
get_post(_S, _Args, {O, DVV}) ->
    %% What matters is that both types have the exact same results.
    lists:sort(riak_object:get_values(O)) == lists:sort(dvvset:values(DVV)).

%% ------ Grouped operator: put
%% Store a new value, a contextless put
put_pre(S) ->
    vnodes_ready(S).

put_args(#state{vnodes=VNodes, vnode_data=VNodeData, time=Time}) ->
    [
     binary(), %% a new value
     elements(VNodes), % a% coordinator
     VNodeData, %% The existing vnode data
     Time
    ].

%% Like when a user does a put without first fetching (blank vv) @see
%% put_next/3 for the return tuple being prepended to vnode_data
put(Value, Coord, VNodeData, Time) ->
    {RObj, DVV} = coord_put(Coord, Value, Time, VNodeData),
    {Coord, RObj, DVV, Time}.

put_next(S=#state{vnode_data=VNodeData, time=T}, Res, _Args) ->
    %% The state data is prepend only, it grows and grows, but it's based on older state
    %% Newest at the front.
    S#state{time=T+1, vnode_data=[Res | VNodeData]}.

%% ------ Grouped operator: update
%% Use the last fetched value as a context for a put.
update_pre(S=#state{last_get=LastGet}) ->
    %% Only do an update if you already did a get
    vnodes_ready(S) andalso LastGet /= undefined.

update_args(#state{last_get=LastGet, vnodes=VNodes, vnode_data=VNodeData, time=Time}) ->
    [
     binary(), %% A new value
     LastGet, %% The result of the last `Get' command execution
     elements(VNodes), %% A vnode to coordinate
     VNodeData, %% All the vnode data
     Time
    ].

%% Put, but use the result from the last fetch operation
%% hopefully this simulates stale / conflicting puts
update(Value, {GotObj0, GotDVV0}, Coord, VNodeData, Time) ->
    {RObj, DVV} = get(Coord, VNodeData),
    GotObj = update_ro(Value, GotObj0),
    GotDVV = update_dvv(Value, GotDVV0),
    RObj2 = coord_put_ro(Coord, GotObj, RObj, Time),
    DVV2 = coord_put_dvv(Coord, GotDVV, DVV),
    {Coord, RObj2, DVV2, Time}.

update_next(S, Res, Args) ->
    put_next(S, Res, Args).


%% ------ Grouped operator: get_put
%% A frontier generating, sibling resolving, fetch then put
get_put_pre(S) ->
    vnodes_ready(S).

get_put_args(#state{vnodes=VNodes, vnode_data=VNodeData, r=R, time=Time}) ->
    [
     binary(),  %% a new value
     elements(VNodes), %% A vnode to "coordinate" the get
     R, %% How many vnodes to read from
     elements(VNodes), %% a vnode to coordinate the put
     VNodes,  %% all vnodes
     VNodeData, %% All vnode data
     Time
    ].

%% A well behaved client. Get, mutate, put.
get_put(Value, ReadVNode, R, WriteVNode, VNodes, VNodeData, Time) ->
    {RObj, DVV} = get(ReadVNode, R, VNodes, VNodeData),
    RObj1 = update_ro(Value, RObj),
    DVV1 = update_dvv(Value, DVV),
    {OldObj, OldDVV} = get(WriteVNode, VNodeData),
    ROBj2 = coord_put_ro(WriteVNode, RObj1, OldObj, Time),
    DVV2 = coord_put_dvv(WriteVNode, DVV1, OldDVV),
    {WriteVNode, ROBj2, DVV2, Time}.

get_put_next(S, Res, Args) ->
    put_next(S, Res, Args).

%% ------ Grouped operator: replicate
replicate_pre(S) ->
    vnodes_ready(S).

replicate_args(#state{vnodes=VNodes, vnode_data=VNodeData, time=Time}) ->
    [
     elements(VNodes), %% Replicate from
     elements(VNodes), %% replicate too
     VNodeData,
     Time
    ].

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

replicate_next(S, Res, Args) ->
    put_next(S, Res, Args).


%% Test the property thet riak_objects
%% update/merge/reconcile/syntactic_merge functions are identical to
%% dvvset's sync/join/update functions.
prop_merge() ->
    ?FORALL(Cmds,commands(?MODULE),
            begin
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

%% -----------
%% Helpers
%% ----------
vnodes_ready(#state{vnodes=VNodes, n=N}) ->
    length(VNodes) >= N andalso N > 0.

%% Perform a riak_object and DVV coordinated put
coord_put(VNode, Value, Time, VNodeData) ->
    {RObj, DVV} = get(VNode, VNodeData),
    RObj2 = coord_put_ro(VNode, riak_object:new(?B, ?K, Value), RObj, Time),
    DVV2 = coord_put_dvv(VNode, dvvset:new(Value), DVV),
    {RObj2, DVV2}.

%% Update the riak_object, as if in the co-ordinating vnode
coord_put_ro(VNode, NewObj, undefined, Time) ->
    riak_object:increment_vclock(NewObj, VNode, Time);
coord_put_ro(VNode, NewObj, OldObj, Time) ->
    riak_object:update(false, OldObj, NewObj, VNode, Time).

%% Update the DVVSet as if in a co-ordinating vnode
coord_put_dvv(VNode, DVV, undefined) ->
    dvvset:update(DVV, VNode);
coord_put_dvv(VNode, NewDVV, OldDVV) ->
    dvvset:update(NewDVV, OldDVV, VNode).

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
