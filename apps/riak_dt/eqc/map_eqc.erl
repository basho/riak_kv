%% -------------------------------------------------------------------
%%
%% map_eqc: Drive out the merge bugs the other statem couldn't
%%
%% Copyright (c) 2007-2014 Basho Technologies, Inc.  All Rights Reserved.
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

-module(map_eqc).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([initial_state/0, weight/2, prop_merge/0]).

-export([create_replica_pre/1, create_replica_args/1, create_replica_pre/2,
         create_replica/1, create_replica_next/3]).

-export([remove_pre/1, remove_args/1, remove_pre/2, remove/2, remove_post/3]).

-export([ctx_remove/3, replicate/2, ctx_update/5, update/4, idempotent/1,
         commutative/2, associative/3]).

-record(state,{replicas=[],
               %% a unique tag per add
               counter=1 :: pos_integer(),
               %% fields that have been added
               adds=[] :: [{atom(), module()}]
              }).

%% Initialize the state
-spec initial_state() -> eqc_statem:symbolic_state().
initial_state() ->
    #state{}.

%% ------ Grouped operator: create_replica
create_replica_pre(#state{replicas=Replicas}) ->
    length(Replicas) < 10.

%% @doc create_replica_arge - Generate a replica
create_replica_args(_S) ->
    %% don't waste time shrinking actor id binaries
    [noshrink(binary(8))].

%% @doc create_replica_pre - Don't duplicate replicas
-spec create_replica_pre(S :: eqc_statem:symbolic_state(),
                         Args :: [term()]) -> boolean().
create_replica_pre(#state{replicas=Replicas}, [Id]) ->
    not lists:member(Id, Replicas).

%% @doc store new replica ID and bottom map/model in ets
create_replica(Id) ->
    ets:insert(map_eqc, {Id, riak_dt_map:new(), model_new()}).

%% @doc create_replica_next - Add replica ID to state
-spec create_replica_next(S :: eqc_statem:symbolic_state(),
                          V :: eqc_statem:var(),
                          Args :: [term()]) -> eqc_statem:symbolic_state().
create_replica_next(S=#state{replicas=R0}, _Value, [Id]) ->
    S#state{replicas=R0++[Id]}.


%% ------ Grouped operator: remove
%% @doc remove, but only something that has been added already
remove_pre(#state{replicas=Replicas, adds=Adds}) ->
    Replicas /= [] andalso Adds /= [].

%% @doc remove something that has been added already from any replica
remove_args(#state{adds=Adds, replicas=Replicas}) ->
    [
     elements(Replicas),
     elements(Adds) %% A Field that has been added
    ].

%% @doc correct shrinking
remove_pre(#state{replicas=Replicas}, [Replica, _]) ->
    lists:member(Replica, Replicas).

%% @doc perform a remove operation, remove `Field' from Map/Model at
%% `Replica'
remove(Replica, Field) ->
    [{Replica, Map, Model}] = ets:lookup(map_eqc, Replica),
    %% even though we only remove what has been added, there is no
    %% guarantee a merge from another replica hasn't led to the
    %% Field being removed already, so ignore precon errors (they
    %% don't change state)
    {ok, Map2} = ignore_precon_error(riak_dt_map:update({update, [{remove, Field}]}, Replica, Map), Map),
    Model2 = model_remove_field(Field, Model),
    ets:insert(map_eqc, {Replica, Map2, Model2}),
    {Map2, Model2}.

%% @doc remove post condition, @see post_all/2
remove_post(_S, [_Replica, Field], Res) ->
    post_all(Res, remove) andalso field_not_present(Field, Res).

%% ------ Grouped operator: ctx_remove
%% @doc remove, but with a context
ctx_remove_pre(#state{replicas=Replicas, adds=Adds}) ->
        Replicas /= [] andalso Adds /= [].

%% @doc generate ctx rmeove args
ctx_remove_args(#state{replicas=Replicas, adds=Adds}) ->
    [
     elements(Replicas),        %% read from
     elements(Replicas),          %% send op to
     elements(Adds)       %% which field to remove
    ].

%% @doc ensure correct shrinking
ctx_remove_pre(#state{replicas=Replicas, adds=Adds}, [From, To, Field]) ->
    lists:member(From, Replicas) andalso lists:member(To, Replicas)
        andalso lists:member(Field, Adds).

%% @doc dynamic precondition, only context remove if the `Field' is in
%% the `From' replicas
ctx_remove_dynamicpre(_S, [From, _To, Field]) ->
    [{From, Map, _Model}] = ets:lookup(map_eqc, From),
    lists:keymember(Field, 1, riak_dt_map:value(Map)).

%% @doc perform a context remove on the map and model using context
%% from `From'
ctx_remove(From, To, Field) ->
    [{From, FromMap, FromModel}] = ets:lookup(map_eqc, From),
    [{To, ToMap, ToModel}] = ets:lookup(map_eqc, To),
    Ctx = riak_dt_map:precondition_context(FromMap),
    {ok, Map} = riak_dt_map:update({update, [{remove, Field}]}, To, ToMap, Ctx),
    Model = model_ctx_remove(Field, FromModel, ToModel),
    ets:insert(map_eqc, {To, Map, Model}),
    {Map, Model}.

%% @doc @see post_all/2
ctx_remove_post(_S, _Args, Res) ->
    post_all(Res, ctx_remove).

%% ------ Grouped operator: replicate Merges two replicas' values.

%% @doc must be a replica at least.
replicate_pre(#state{replicas=Replicas}) ->
    Replicas /= [].

%% @doc chose from/to replicas, can be the same
replicate_args(#state{replicas=Replicas}) ->
    [
     elements(Replicas), %% Replicate from
     elements(Replicas) %% Replicate to
    ].

%% @doc replicate_pre - shrink correctly
-spec replicate_pre(S :: eqc_statem:symbolic_state(),
                    Args :: [term()]) -> boolean().
replicate_pre(#state{replicas=Replicas}, [From, To]) ->
    lists:member(From, Replicas) andalso lists:member(To, Replicas).

%% @doc Replicate a CRDT from `From' to `To'
replicate(From, To) ->
    [{From, FromMap, FromModel}] = ets:lookup(map_eqc, From),
    [{To, ToMap, ToModel}] = ets:lookup(map_eqc, To),

    Map = riak_dt_map:merge(FromMap, ToMap),
    Model = model_merge(FromModel, ToModel),

    ets:insert(map_eqc, {To, Map, Model}),
    {Map, Model}.

%% @doc @see post_all/2
replicate_post(_S, _Args, Res) ->
    post_all(Res, rep).

%% ------ Grouped operator: ctx_update
%% Update a Field in the Map, using the Map context

%% @doc there must be at least one replica
ctx_update_pre(#state{replicas=Replicas}) ->
    Replicas /= [].

%% @doc generate an operation
ctx_update_args(#state{replicas=Replicas, counter=Cnt}) ->
    ?LET({Field, Op}, gen_field_and_op(),
         [
          Field,
          Op,
          elements(Replicas),
          elements(Replicas),
          Cnt
         ]).

%% @doc ensure correct shrinking
ctx_update_pre(#state{replicas=Replicas}, [_Field, _Op, From, To, _Cnt]) ->
    lists:member(From, Replicas) andalso lists:member(To, Replicas).

%% @doc much like context_remove, get a contet from `From' and apply
%% `Op' at `To'
ctx_update(Field, Op, From, To, Cnt) ->
    [{From, CtxMap, CtxModel}] = ets:lookup(map_eqc, From),
    [{To, ToMap, ToModel}] = ets:lookup(map_eqc, To),

    Ctx = riak_dt_map:precondition_context(CtxMap),
    ModCtx = model_ctx(CtxModel),
    {ok, Map} = riak_dt_map:update({update, [{update, Field, Op}]}, To, ToMap, Ctx),
    {ok, Model} = model_update_field(Field, Op, To, Cnt, ToModel, ModCtx),

    ets:insert(map_eqc, {To, Map, Model}),
    {Map, Model}.

%% @doc update the model state, incrementing the counter represents logical time.
ctx_update_next(S=#state{counter=Cnt, adds=Adds}, _Res, [Field, _Op, _From, _To, _Cnt]) ->
    S#state{adds=lists:umerge(Adds, [Field]), counter=Cnt+1}.

%% @doc @see post_all/2
ctx_update_post(_S, _Args, Res) ->
    post_all(Res, update).

%% ------ Grouped operator: update
%% Update a Field in the Map

%% @doc there must be at least one replica
update_pre(#state{replicas=Replicas}) ->
    Replicas /= [].

%% @doc choose a field, operation and replica
update_args(#state{replicas=Replicas, counter=Cnt}) ->
    ?LET({Field, Op}, gen_field_and_op(),
         [
          Field,
          Op,
          elements(Replicas),
          Cnt
         ]).

%% @doc shrink correctly
update_pre(#state{replicas=Replicas}, [_Field, _Op, Replica, _Cnt]) ->
    lists:member(Replica, Replicas).

%% @doc apply `Op' to `Field' at `Replica'
update(Field, Op, Replica, Cnt) ->
    [{Replica, Map0, Model0}] = ets:lookup(map_eqc, Replica),

    {ok, Map} = ignore_precon_error(riak_dt_map:update({update, [{update, Field, Op}]}, Replica, Map0), Map0),
    {ok, Model} = model_update_field(Field, Op, Replica, Cnt, Model0,  undefined),

    ets:insert(map_eqc, {Replica, Map, Model}),
    {Map, Model}.

%% @doc increment the time counter, and add this field to adds as a
%% candidate to be removed later.
update_next(S=#state{counter=Cnt, adds=Adds}, _Res, [Field, _, _, _]) ->
    S#state{adds=lists:umerge(Adds, [Field]), counter=Cnt+1}.

%% @doc @see post_all/2
update_post(_S, _Args, Res) ->
    post_all(Res, update).

%% ------ Grouped operator: idempotent

idempotent_args(#state{replicas=Replicas}) ->
    [elements(Replicas)].

%% @doc idempotent_pre - Precondition for generation
-spec idempotent_pre(S :: eqc_statem:symbolic_state()) -> boolean().
idempotent_pre(#state{replicas=Replicas}) ->
    Replicas /= [].

%% @doc idempotent_pre - Precondition for idempotent
-spec idempotent_pre(S :: eqc_statem:symbolic_state(),
                     Args :: [term()]) -> boolean().
idempotent_pre(#state{replicas=Replicas}, [Replica]) ->
    lists:member(Replica, Replicas).

%% @doc idempotent - Merge replica with itself, result used for post condition only
idempotent(Replica) ->
    [{Replica, Map, _Model}] = ets:lookup(map_eqc, Replica),
    {Map, riak_dt_map:merge(Map, Map)}.

%% @doc idempotent_post - Postcondition for idempotent
-spec idempotent_post(S :: eqc_statem:dynamic_state(),
                      Args :: [term()], R :: term()) -> true | term().
idempotent_post(_S, [_Replica], {Map, MergedSelfMap}) ->
    riak_dt_map:equal(Map, MergedSelfMap).

%% ------ Grouped operator: commutative

commutative_args(#state{replicas=Replicas}) ->
    [elements(Replicas), elements(Replicas)].

%% @doc commutative_pre - Precondition for generation
-spec commutative_pre(S :: eqc_statem:symbolic_state()) -> boolean().
commutative_pre(#state{replicas=Replicas}) ->
    Replicas /= [].

%% @doc commutative_pre - Precondition for commutative
-spec commutative_pre(S :: eqc_statem:symbolic_state(),
                     Args :: [term()]) -> boolean().
commutative_pre(#state{replicas=Replicas}, [Replica, Replica2]) ->
    lists:member(Replica, Replicas) andalso lists:member(Replica2, Replicas).

%% @doc commutative - Merge maps both ways (result used for post condition)
commutative(Replica1, Replica2) ->
    [{Replica1, Map1, _Model1}] = ets:lookup(map_eqc, Replica1),
    [{Replica2, Map2, _Model2}] = ets:lookup(map_eqc, Replica2),
    {riak_dt_map:merge(Map1, Map2), riak_dt_map:merge(Map2, Map1)}.

%% @doc commutative_post - Postcondition for commutative
-spec commutative_post(S :: eqc_statem:dynamic_state(),
                      Args :: [term()], R :: term()) -> true | term().
commutative_post(_S, [_Replica1, _Replica2], {OneMergeTwo, TwoMergeOne}) ->
    riak_dt_map:equal(OneMergeTwo, TwoMergeOne).

%% ------ Grouped operator: associative

associative_args(#state{replicas=Replicas}) ->
    [elements(Replicas), elements(Replicas), elements(Replicas)].

%% @doc associative_pre - Precondition for generation
-spec associative_pre(S :: eqc_statem:symbolic_state()) -> boolean().
associative_pre(#state{replicas=Replicas}) ->
    Replicas /= [].

%% @doc associative_pre - Precondition for associative
-spec associative_pre(S :: eqc_statem:symbolic_state(),
                     Args :: [term()]) -> boolean().
associative_pre(#state{replicas=Replicas}, [Replica, Replica2, Replica3]) ->
    lists:member(Replica, Replicas)
        andalso lists:member(Replica2, Replicas)
        andalso lists:member(Replica3, Replicas).

%% @doc associative - Merge maps three ways (result used for post condition)
associative(Replica1, Replica2, Replica3) ->
    [{Replica1, Map1, _Model1}] = ets:lookup(map_eqc, Replica1),
    [{Replica2, Map2, _Model2}] = ets:lookup(map_eqc, Replica2),
    [{Replica3, Map3, _Model3}] = ets:lookup(map_eqc, Replica3),
    {riak_dt_map:merge(riak_dt_map:merge(Map1, Map2), Map3),
     riak_dt_map:merge(riak_dt_map:merge(Map1, Map3), Map2),
     riak_dt_map:merge(riak_dt_map:merge(Map2, Map3), Map1)}.

%% @doc associative_post - Postcondition for associative
-spec associative_post(S :: eqc_statem:dynamic_state(),
                      Args :: [term()], R :: term()) -> true | term().
associative_post(_S, [_Replica1, _Replica2, _Replica3], {ABC, ACB, BCA}) ->
%%    case {riak_dt_map:equal(ABC, ACB),  riak_dt_map:equal(ACB, BCA)} of
    case {map_values_equal(ABC, ACB), map_values_equal(ACB, BCA)} of
        {true, true} ->
            true;
        {false, true} ->
            {postcondition_failed, {ACB, not_associative, ABC}};
        {true, false} ->
            {postcondition_failed, {ACB, not_associative, BCA}};
        {false, false} ->
            {postcondition_failed, {{ACB, not_associative, ABC}, '&&', {ACB, not_associative, BCA}}}
    end.

map_values_equal(Map1, Map2) ->
    lists:sort(riak_dt_map:value(Map1)) == lists:sort(riak_dt_map:value(Map2)).

%% @Doc Weights for commands. Don't create too many replicas, but
%% prejudice in favour of creating more than 1. Try and balance
%% removes with adds. But favour adds so we have something to
%% remove. See the aggregation output.
weight(S, create_replica) when length(S#state.replicas) > 2 ->
    1;
weight(S, create_replica) when length(S#state.replicas) < 5 ->
    4;
weight(_S, remove) ->
    2;
weight(_S, context_remove) ->
    5;
weight(_S, update) ->
    3;
weight(_S, ctx_update) ->
    3;
weight(_S, _) ->
    1.


%% @doc Tests the property that a riak_dt_map is equivalent to the Map
%% Model. The Map Model is based roughly on an or-set design. Inspired
%% by a draft spec in sent in private email by Carlos Baquero. The
%% model extends the spec to onclude context operations, deferred
%% operations, and reset-remove semantic (with tombstones.)
prop_merge() ->
    ?FORALL(Cmds, commands(?MODULE),
            begin
                %% store state in eqc, external to statem state
                ets:new(map_eqc, [named_table, set]),
                {H, S, Res} = run_commands(?MODULE,Cmds),
                ReplicaData =  ets:tab2list(map_eqc),
                %% Check that merging all values leads to the same results for Map and the Model
                {Map, Model} = lists:foldl(fun({_Actor, InMap, InModel}, {M, Mo}) ->
                                                   {riak_dt_map:merge(M, InMap),
                                                    model_merge(Mo, InModel)}
                                           end,
                                           {riak_dt_map:new(), model_new()},
                                           ReplicaData),
                MapValue = riak_dt_map:value(Map),
                ModelValue = model_value(Model),
                %% clean up
                ets:delete(map_eqc),
                %% prop
                pretty_commands(?MODULE, Cmds, {H, S, Res},
                                measure(actors, length(ReplicaData),
                                        measure(length, length(MapValue),
                                                measure(depth, map_depth(MapValue),
                                                        aggregate(command_names(Cmds),
                                                                  conjunction([{results, equals(Res, ok)},
                                                                               {value, equals(lists:sort(MapValue), lists:sort(ModelValue))}
                                                                              ])
                                                                 )))))
            end).

%% -----------
%% Generators
%% ----------

%% @doc Keep the number of possible field names down to a minimum. The
%% smaller state space makes EQC more likely to find bugs since there
%% will be more action on the fields. Learned this from
%% crdt_statem_eqc having to large a state space and missing bugs.
gen_field() ->
    {growingelements(['A', 'B', 'C', 'D', 'X', 'Y', 'Z']),
     elements([
               riak_dt_orswot,
               riak_dt_emcntr,
               riak_dt_lwwreg,
               riak_dt_map,
               riak_dt_od_flag
              ])}.

%% @use the generated field to generate an op. Delegates to type. Some
%% Type generators are recursive, pass in Size to limit the depth of
%% recusrions. @see riak_dt_map:gen_op/1.
gen_field_op({_Name, Type}) ->
    ?SIZED(Size, Type:gen_op(Size)).

%% @doc geneate a field, and a valid operation for the field
gen_field_and_op() ->
    ?LET(Field, gen_field(), {Field, gen_field_op(Field)}).


%% -----------
%% Helpers
%% ----------

%% @doc how deeply nested is `Map'? Recurse down the Map and return
%% the deepest nesting. A depth of `1' means only a top-level Map. `2'
%% means a map in the seocnd level, `3' in the third, and so on.
map_depth(Map) ->
    map_depth(Map, 1).

%% @doc iterate a maps fields, and recurse down map fields to get a
%% max depth.
map_depth(Map, D) ->
    lists:foldl(fun({{_, riak_dt_map}, SubMap}, MaxDepth) ->
                        Depth = map_depth(SubMap, D+1),
                        max(MaxDepth, Depth);
                   (_, Depth) ->
                        Depth
                end,
                D,
                Map).

%% @doc precondition errors don't change the state of a map, so ignore
%% them.
ignore_precon_error({ok, NewMap}, _) ->
    {ok, NewMap};
ignore_precon_error(_, Map) ->
    {ok, Map}.

%% @doc for all mutating operations enusre that the state at the
%% mutated replica is equal for the map and the model
post_all({Map, Model}, Cmd) ->
    %% What matters is that both types have the exact same results.
    case lists:sort(riak_dt_map:value(Map)) == lists:sort(model_value(Model)) of
        true ->
            true;
        _ ->
            {postcondition_failed, "Map and Model don't match", Cmd, Map, Model, riak_dt_map:value(Map), model_value(Model)}
    end.

%% @doc `true' if `Field' is not in `Map'
field_not_present(Field, {Map, _Model}) ->
    case lists:keymember(Field, 1, riak_dt_map:value(Map)) of
        false ->
            true;
        true ->
            {Field, present, Map}
    end.

%% -----------
%% Model
%% ----------

%% The model has to be a Map CRDT, with the same deferred operations
%% and reset-remove semantics as riak_dt_map. Luckily it has no
%% efficiency constraints. It's modelled as three lists. Added fields,
%% removed fields, and deferred remove operations. There is also an
%% entry for tombstones, and vclock. Whenever a field is removed, an
%% empty CRDT of that fields type is stored, with the clock at removal
%% time, in tombstones. Merging a fields value with this tombstone
%% ensures reset-remove semantics. The vector clock entry is only for
%% tombstones and context operations. The add/remove sets use a unique
%% tag, like in OR-Set, for elements.

-record(model, {
          %% Things added to the Map, a Set really, but uses a list
          %% for ease of reading in case of a counter example
          adds=[],
          %% Tombstones of things removed from the map
          removes=[],
          %% Removes that are waiting for adds before they
          %% can be run (like context ops in the
          %% riak_dt_map)
          deferred=[],
          %% For reset-remove semantic, the field+clock at time of
          %% removal
          tombstones=orddict:new() :: orddict:orddict(),
          %% for embedded context operations
          clock=riak_dt_vclock:fresh() :: riak_dt_vclock:vclock()
         }).

-type map_model() :: #model{}.

%% @doc create a new model. This is the bottom element.
-spec model_new() -> map_model().
model_new() ->
    #model{}.

%% @doc update `Field' with `Op', by `Actor' at time `Cnt'. very
%% similar to riak_dt_map.
-spec model_update_field({atom(), module()}, term(), binary(), pos_integer(),
                         map_model(), undefined | riak_dt_vclock:vclock()) -> map_model().
model_update_field({_Name, Type}=Field, Op, Actor, Cnt, Model, Ctx) ->
    #model{adds=Adds, removes=Removes, clock=Clock, tombstones=TS} = Model,
    %% generate a new clock
    Clock2 = riak_dt_vclock:merge([[{Actor, Cnt}], Clock]),
    %% Get those fields that are present, that is, only in the Adds
    %% set
    InMap = lists:subtract(Adds, Removes),
    %% Merge all present values to get a current CRDT, and add each
    %% field entry we merge to a set so we can move them to `removed'
    {CRDT0, ToRem} = lists:foldl(fun({F, Value, _X}=E, {CAcc, RAcc}) when F == Field ->
                                         {Type:merge(CAcc, Value), lists:umerge([E], RAcc)};
                                    (_, Acc) -> Acc
                                 end,
                                 {Type:new(), []},
                                 InMap),
    %% If we have a tombstone for this field, merge with it to ensure
    %% reset-remove
    CRDT1 = case orddict:find(Field, TS) of
                error ->
                    CRDT0;
                {ok, TSVal} ->
                    Type:merge(CRDT0, TSVal)
            end,
    %% Update the clock to ensure a shared causal context
    CRDT = Type:parent_clock(Clock2, CRDT1),
    %% Apply the operation
    case Type:update(Op, {Actor, Cnt}, CRDT, Ctx) of
        {ok, Updated} ->
            %% Op succeded, store the new value as the field, using
            %% `Cnt' (which is time since the statem acts serially) as
            %% a unique tag.
            Model2 = Model#model{adds=lists:umerge([{Field, Updated, Cnt}], Adds),
                                 removes=lists:umerge(ToRem, Removes),
                                 clock=Clock2},
            {ok, Model2};
        _ ->
            %% if the op failed just return the state un changed
            {ok, Model}
    end.

%% @doc remove a field from the model, doesn't enforce a precondition,
%% removing a non-present field does nothing.
-spec model_remove_field({atom(), module()}, map_model()) -> map_model().
model_remove_field({_Name, Type}=Field, Model) ->
    #model{adds=Adds, removes=Removes, tombstones=Tombstones0, clock=Clock} = Model,
    ToRemove = [{F, Val, Token} || {F, Val, Token} <- Adds, F == Field],
    TS = Type:parent_clock(Clock, Type:new()),
    Tombstones = orddict:update(Field, fun(T) -> Type:merge(TS, T) end, TS, Tombstones0),
    Model#model{removes=lists:umerge(Removes, ToRemove), tombstones=Tombstones}.

%% @doc merge two models. Very simple since the model truley always
%% grows, it is just a union of states, and then applies the deferred
%% operations.
-spec model_merge(map_model(), map_model()) -> map_model().
model_merge(Model1, Model2) ->
    #model{adds=Adds1, removes=Removes1, deferred=Deferred1, clock=Clock1, tombstones=TS1} = Model1,
    #model{adds=Adds2, removes=Removes2, deferred=Deferred2, clock=Clock2, tombstones=TS2} = Model2,
    Clock = riak_dt_vclock:merge([Clock1, Clock2]),
    Adds0 = lists:umerge(Adds1, Adds2),
    Tombstones = orddict:merge(fun({_Name, Type}, V1, V2) -> Type:merge(V1, V2) end, TS1, TS2),
    Removes0 = lists:umerge(Removes1, Removes2),
    Deferred0 = lists:umerge(Deferred1, Deferred2),
    {Adds, Removes, Deferred} = model_apply_deferred(Adds0, Removes0, Deferred0),
    #model{adds=Adds, removes=Removes, deferred=Deferred, clock=Clock, tombstones=Tombstones}.

%% @doc apply deferred operations that may be relevant now a merge as taken place.
-spec model_apply_deferred(list(), list(), list()) -> {list(), list(), list()}.
model_apply_deferred(Adds, Removes, Deferred) ->
    D2 = lists:subtract(Deferred, Adds),
    ToRem = lists:subtract(Deferred, D2),
    {Adds, lists:umerge(ToRem, Removes), D2}.

%% @doc remove `Field' from `To' using `From's context.
-spec model_ctx_remove({atom(), module()}, map_model(), map_model()) -> map_model().
model_ctx_remove({_N, Type}=Field, From, To) ->
    %% get adds for Field, any adds for field in ToAdds that are in
    %% FromAdds should be removed any others, put in deferred
    #model{adds=FromAdds, clock=FromClock} = From,
    #model{adds=ToAdds, removes=ToRemoves, deferred=ToDeferred, tombstones=TS} = To,
    ToRemove = lists:filter(fun({F, _Val, _Token}) -> F == Field end, FromAdds),
    Defer = lists:subtract(ToRemove, ToAdds),
    Remove = lists:subtract(ToRemove, Defer),
    Tombstone = Type:parent_clock(FromClock, Type:new()),
    TS2 = orddict:update(Field, fun(T) -> Type:merge(T, Tombstone) end, Tombstone, TS),
    To#model{removes=lists:umerge(Remove, ToRemoves),
             deferred=lists:umerge(Defer, ToDeferred),
             tombstones=TS2}.

%% @doc get the actual value for a model
-spec model_value(map_model()) ->
                         [{Field::{atom(), module()}, Value::term()}].
model_value(Model) ->
    #model{adds=Adds, removes=Removes, tombstones=TS} = Model,
    Remaining = lists:subtract(Adds, Removes),
    %% fold over fields that are in the map and merge each to a merge
    %% CRDT per field
    Res = lists:foldl(fun({{_Name, Type}=Key, Value, _X}, Acc) ->
                              %% if key is in Acc merge with it and replace
                              dict:update(Key, fun(V) ->
                                                       Type:merge(V, Value) end,
                                          Value, Acc) end,
                      dict:new(),
                      Remaining),
    %% fold over merged CRDTs and merge with the tombstone (if any)
    %% for each field
    Res2 = dict:fold(fun({_N, Type}=Field, Val, Acc) ->
                             case orddict:find(Field, TS) of
                                 error ->
                                     dict:store(Field, Val, Acc);
                                 {ok, TSVal} ->
                                     dict:store(Field, Type:merge(TSVal, Val), Acc)
                             end
                     end,
                     dict:new(),
                     Res),
    %% Call `value/1' on each CRDT in the model
    [{K, Type:value(V)} || {{_Name, Type}=K, V} <- dict:to_list(Res2)].

%% @doc get a context for a model context operation
-spec model_ctx(map_model()) -> riak_dt_vclock:vclock().
model_ctx(#model{clock=Ctx}) ->
    Ctx.


-endif. % EQC
