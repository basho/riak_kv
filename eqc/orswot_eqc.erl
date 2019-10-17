%% -------------------------------------------------------------------
%%
%% orswot_eqc: Try and catch bugs crdt_statem could not.
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

-module(orswot_eqc).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([initial_state/0, weight/2, prop_merge/0]).

-export([create_replica_pre/2, create_replica/1, create_replica_next/3]).

-export([add_args/1, add_pre/1, add_pre/2, add/2, add_next/3, add_post/3]).

-export([remove_pre/1, remove_args/1, remove_pre/2, remove/2, remove_post/3]).

-export([replicate_args/1, replicate_pre/1, replicate_pre/2, replicate/2,
         replicate_post/3]).

-export([context_remove_args/1, context_remove_pre/1, context_remove_pre/2,
         context_remove_dynamicpre/2, context_remove/3]).

-export([idempotent_args/1, idempotent_pre/1, idempotent_pre/2, idempotent/1,
         idempotent_post/3]).

-export([commutative_args/1, commutative_pre/1, commutative_pre/2, commutative/2,
         commutative_post/3]).

-export([associative_args/1, associative_pre/1, associative_pre/2, associative/3,
         associative_post/3]).


-record(state, {replicas=[], %% Actor Ids for replicas in the system
                adds=[]      %% Elements that have been added to the set
               }).

%% The set of possible elements in the set
-define(ELEMENTS, ['A', 'B', 'C', 'D', 'X', 'Y', 'Z']).

initial_state() ->
    #state{}.

%% ------ Grouped operator: create_replica
create_replica_pre(#state{replicas=Replicas}) ->
    length(Replicas) < 10.

%% @doc create_replica_command - Command generator
create_replica_args(_S) ->
    %% Don't waste time shrinking the replicas ID binaries, they 8
    %% byte binaris as that is riak-esque.
    [noshrink(binary(8))].

%% @doc create_replica_pre - don't create a replica that already
%% exists
-spec create_replica_pre(S :: eqc_statem:symbolic_state(),
                         Args :: [term()]) -> boolean().
create_replica_pre(#state{replicas=Replicas}, [Id]) ->
    not lists:member(Id, Replicas).

%% @doc create a new replica, and store a bottom orswot/orset+deferred
%% in ets
create_replica(Id) ->
    ets:insert(orswot_eqc, {Id, riak_dt_orswot:new(), {riak_dt_orset:new(), riak_dt_orset:new()}}).

%% @doc create_replica_next - Add the new replica ID to state
-spec create_replica_next(S :: eqc_statem:symbolic_state(),
                          V :: eqc_statem:var(),
                          Args :: [term()]) -> eqc_statem:symbolic_state().
create_replica_next(S=#state{replicas=R0}, _Value, [Id]) ->
    S#state{replicas=R0++[Id]}.

%% ------ Grouped operator: add
add_args(#state{replicas=Replicas}) ->
    [elements(Replicas),
     %% Start of with earlier/fewer elements
     growingelements(?ELEMENTS)].

%% @doc add_pre - Don't add to a set until we have a replica
-spec add_pre(S :: eqc_statem:symbolic_state()) -> boolean().
add_pre(#state{replicas=Replicas}) ->
    Replicas /= [].

%% @doc add_pre - Ensure correct shrinking, only select a replica that
%% is in the state
-spec add_pre(S :: eqc_statem:symbolic_state(),
              Args :: [term()]) -> boolean().
add_pre(#state{replicas=Replicas}, [Replica, _]) ->
    lists:member(Replica, Replicas).

%% @doc add the `Element' to the sets at `Replica'
add(Replica, Element) ->
    [{Replica, ORSWOT, {ORSet, Def}}] = ets:lookup(orswot_eqc, Replica),
    {ok, ORSWOT2} = riak_dt_orswot:update({add, Element}, Replica, ORSWOT),
    {ok, ORSet2} = riak_dt_orset:update({add, Element}, Replica, ORSet),
    ets:insert(orswot_eqc, {Replica, ORSWOT2, {ORSet2, Def}}),
    {ORSWOT2, ORSet2}.

%% @doc add_next - Add the `Element' to the `adds' list so we can
%% select from it when we come to remove. This increases the liklihood
%% of a remove actuallybeing meaningful.
-spec add_next(S :: eqc_statem:symbolic_state(),
               V :: eqc_statem:var(),
               Args :: [term()]) -> eqc_statem:symbolic_state().
add_next(S=#state{adds=Adds}, _Value, [_, Element]) ->
    S#state{adds=lists:umerge(Adds, [Element])}.

%% @doc add_post - The specification and implementation should be
%% equal in intermediate states as well as at the end.
-spec add_post(S :: eqc_statem:dynamic_state(),
               Args :: [term()], R :: term()) -> true | term().
add_post(_S, _Args, {ORSWOT, ORSet}) ->
    sets_equal(ORSWOT, ORSet).

%% ------ Grouped operator: remove
%% @doc remove_command - Command generator

%% @doc remove_pre - Only remove if there are replicas and elements
%% added already.
-spec remove_pre(S :: eqc_statem:symbolic_state()) -> boolean().
remove_pre(#state{replicas=Replicas, adds=Adds}) ->
    Replicas /= [] andalso Adds /= [].

remove_args(#state{replicas=Replicas, adds=Adds}) ->
    [elements(Replicas), elements(Adds)].

%% @doc ensure correct shrinking
remove_pre(#state{replicas=Replicas}, [Replica, _]) ->
    lists:member(Replica, Replicas).

%% @doc perform an element remove.
remove(Replica, Value) ->
    [{Replica, ORSWOT, {ORSet, Def}}] = ets:lookup(orswot_eqc, Replica),
    %% even though we only remove what has been added, there is no
    %% guarantee a merge from another replica hasn't led to the
    %% element being removed already, so ignore precon errors (they
    %% don't change state)
    ORSWOT2 = ignore_preconerror_remove(Value, Replica, ORSWOT, riak_dt_orswot),
    ORSet2 = ignore_preconerror_remove(Value, Replica, ORSet, riak_dt_orset),
    ets:insert(orswot_eqc, {Replica, ORSWOT2, {ORSet2, Def}}),
    {ORSWOT2, ORSet2}.

%% @doc in a non-context remove, not only must the spec and impl be
%% equal, but the `Element' MUST be absent from the set
remove_post(_S, [_, Element], {ORSWOT2, ORSet2}) ->
    eqc_statem:conj([sets_not_member(Element, ORSWOT2),
          sets_equal(ORSWOT2, ORSet2)]).

%% ------ Grouped operator: replicate
%% @doc replicate_args - Choose a From and To for replication
replicate_args(#state{replicas=Replicas}) ->
    [elements(Replicas), elements(Replicas)].

%% @doc replicate_pre - There must be at least on replica to replicate
-spec replicate_pre(S :: eqc_statem:symbolic_state()) -> boolean().
replicate_pre(#state{replicas=Replicas}) ->
    Replicas /= [].

%% @doc replicate_pre - Ensure correct shrinking
-spec replicate_pre(S :: eqc_statem:symbolic_state(),
                    Args :: [term()]) -> boolean().
replicate_pre(#state{replicas=Replicas}, [From, To]) ->
    lists:member(From, Replicas) andalso lists:member(To, Replicas).

%% @doc simulate replication by merging state at `To' with state from `From'
replicate(From, To) ->
    [{From, FromORSWOT, {FromORSet, FromDef}}] = ets:lookup(orswot_eqc, From),
    [{To, ToORSWOT, {ToORSet, ToDef}}] = ets:lookup(orswot_eqc, To),

    ORSWOT = riak_dt_orswot:merge(FromORSWOT, ToORSWOT),
    ORSet0 = riak_dt_orset:merge(FromORSet, ToORSet),
    Def0 = riak_dt_orset:merge(FromDef, ToDef),

    {ORSet, Def} = model_apply_deferred(ORSet0, Def0),

    ets:insert(orswot_eqc, {To, ORSWOT, {ORSet, Def}}),
    {ORSWOT, ORSet}.

%% @doc replicate_post - ORSet and ORSWOT must be equal throughout.
-spec replicate_post(S :: eqc_statem:dynamic_state(),
                     Args :: [term()], R :: term()) -> true | term().
replicate_post(_S, [_From, _To], {SWOT, Set}) ->
    sets_equal(SWOT, Set).


%% ------ Grouped operator: context_remove
context_remove_args(#state{replicas=Replicas, adds=Adds}) ->
    [elements(Replicas),
     elements(Replicas),
     elements(Adds)].

%% @doc context_remove_pre - As for `remove/1'
-spec context_remove_pre(S :: eqc_statem:symbolic_state()) -> boolean().
context_remove_pre(#state{replicas=Replicas, adds=Adds}) ->
    Replicas /= [] andalso Adds /= [].

%% @doc context_remove_pre - Ensure correct shrinking
-spec context_remove_pre(S :: eqc_statem:symbolic_state(),
                         Args :: [term()]) -> boolean().
context_remove_pre(#state{replicas=Replicas, adds=Adds}, [From, To, Element]) ->
    lists:member(From, Replicas) andalso lists:member(To, Replicas)
        andalso lists:member(Element, Adds).

%% @doc a dynamic precondition uses concrete state, check that the
%% `From' set contains `Element'
context_remove_dynamicpre(_S, [From, _To, Element]) ->
    [{From, SWOT, _FromORSet}] = ets:lookup(orswot_eqc, From),
    lists:member(Element, riak_dt_orswot:value(SWOT)).

%% @doc perform a context remove using the context+element at `From'
%% and removing from `To'
context_remove(From, To, Element) ->
    [{From, FromORSWOT, {FromORSet, _FromDef}}] = ets:lookup(orswot_eqc, From),
    [{To, ToORSWOT, {ToORSet, ToDef}}] = ets:lookup(orswot_eqc, To),

    Ctx = riak_dt_orswot:precondition_context(FromORSWOT),
    Fragment = riak_dt_orset:value({fragment, Element}, FromORSet),
    {ok, ToORSWOT2} = riak_dt_orswot:update({remove, Element}, To, ToORSWOT, Ctx),

    {ToORSet2, ToDef2} = context_remove_model(Element, Fragment, ToORSet, ToDef),

    ets:insert(orswot_eqc, {To, ToORSWOT2, {ToORSet2, ToDef2}}),
    {ToORSWOT2, ToORSet2}.


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
    [{Replica, Swot, _Model}] = ets:lookup(orswot_eqc, Replica),
    {Swot, riak_dt_orswot:merge(Swot, Swot)}.

%% @doc idempotent_post - Postcondition for idempotent
-spec idempotent_post(S :: eqc_statem:dynamic_state(),
                      Args :: [term()], R :: term()) -> true | term().
idempotent_post(_S, [_Replica], {Swot, MergedSelfSwot}) ->
    riak_dt_orswot:equal(Swot, MergedSelfSwot).

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
    [{Replica1, Swot1, _Model1}] = ets:lookup(orswot_eqc, Replica1),
    [{Replica2, Swot2, _Model2}] = ets:lookup(orswot_eqc, Replica2),
    {riak_dt_orswot:merge(Swot1, Swot2), riak_dt_orswot:merge(Swot2, Swot1)}.

%% @doc commutative_post - Postcondition for commutative
-spec commutative_post(S :: eqc_statem:dynamic_state(),
                      Args :: [term()], R :: term()) -> true | term().
commutative_post(_S, [_Replica1, _Replica2], {OneMergeTwo, TwoMergeOne}) ->
    riak_dt_orswot:equal(OneMergeTwo, TwoMergeOne).

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
    [{Replica1, Swot1, _Model1}] = ets:lookup(orswot_eqc, Replica1),
    [{Replica2, Swot2, _Model2}] = ets:lookup(orswot_eqc, Replica2),
    [{Replica3, Swot3, _Model3}] = ets:lookup(orswot_eqc, Replica3),
    {riak_dt_orswot:merge(riak_dt_orswot:merge(Swot1, Swot2), Swot3),
     riak_dt_orswot:merge(riak_dt_orswot:merge(Swot1, Swot3), Swot2),
     riak_dt_orswot:merge(riak_dt_orswot:merge(Swot2, Swot3), Swot1)}.

%% @doc associative_post - Postcondition for associative
-spec associative_post(S :: eqc_statem:dynamic_state(),
                      Args :: [term()], R :: term()) -> true | term().
associative_post(_S, [_Replica1, _Replica2, _Replica3], {ABC, ACB, BCA}) ->
  case {riak_dt_orswot:equal(ABC, ACB),  riak_dt_orswot:equal(ACB, BCA)} of
        {true, true} ->
            true;
        {false, true} ->
            {postcondition_failed, {ACB, not_associative, ABC}};
        {true, false} ->
            {postcondition_failed, {ACB, not_associative, BCA}};
        {false, false} ->
            {postcondition_failed, {{ACB, not_associative, ABC}, '&&', {ACB, not_associative, BCA}}}
    end.

%% @doc weights for commands. Don't create too many replicas, but
%% prejudice in favour of creating more than 1. Try and balance
%% removes with adds. But favour adds so we have something to
%% remove. See the aggregation output.
weight(S, create_replica) when length(S#state.replicas) > 2 ->
    1;
weight(S, create_replica) when length(S#state.replicas) < 5 ->
    4;
weight(_S, remove) ->
    3;
weight(_S, context_remove) ->
    3;
weight(_S, add) ->
    8;
weight(_S, _) ->
    1.

%% @doc check that the implementation of the ORSWOT is equivalent to
%% the OR-Set impl.
-spec prop_merge() -> eqc:property().
prop_merge() ->
    ?FORALL(Cmds, commands(?MODULE),
            begin
                %% Store the state external to the statem for correct
                %% shrinking. This is best practice.
                ets:new(orswot_eqc, [named_table, set]),
                {H, S, Res} = run_commands(?MODULE,Cmds),
                {MergedSwot, {MergedSet, ModelDeferred}} = lists:foldl(fun({_Id, ORSWOT, ORSetAndDef}, {MO, MOS}) ->
                                                                               {riak_dt_orswot:merge(ORSWOT, MO),
                                                                                model_merge(MOS, ORSetAndDef)}
                                                                       end,
                                                                       {riak_dt_orswot:new(), {riak_dt_orset:new(), riak_dt_orset:new()}},
                                                                       ets:tab2list(orswot_eqc)),
                SwotDeferred = proplists:get_value(deferred_length, riak_dt_orswot:stats(MergedSwot)),

                ets:delete(orswot_eqc),
                pretty_commands(?MODULE, Cmds, {H, S, Res},
                                aggregate(command_names(Cmds),
                                          measure(replicas, length(S#state.replicas),
                                                  measure(elements, riak_dt_orswot:stat(element_count, MergedSwot),
                                                          conjunction([{result, Res == ok},
                                                                       {equal, sets_equal(MergedSwot, MergedSet)},
                                                                       {m_ec, equals(length(ModelDeferred), 0)},
                                                                       {ec, equals(SwotDeferred, 0)}
                                                                      ])))))

            end).

%% Helpers @doc a non-context remove of an absent element generates a
%% precondition error, but does not mutate the state, so just ignore
%% and return original state.
ignore_preconerror_remove(Value, Actor, Set, Mod) ->
    case Mod:update({remove, Value}, Actor, Set) of
        {ok, Set2} ->
            Set2;
        _E ->
            Set
    end.

%% @doc common precondition and property, do SWOT and Set have the
%% same elements?
sets_equal(ORSWOT, ORSet) ->
    %% What matters is that both types have the exact same results.
    case lists:sort(riak_dt_orswot:value(ORSWOT)) ==
        lists:sort(riak_dt_orset:value(ORSet)) of
        true ->
            true;
        _ ->
            {ORSWOT, '/=', ORSet}
    end.

%% @doc `true' if `Element' is not in `ORSWOT', error tuple otherwise.
sets_not_member(Element, ORSWOT) ->
    case lists:member(Element, riak_dt_orswot:value(ORSWOT)) of
        false ->
            true;
        _ ->
            {Element, member, ORSWOT}
    end.

%% @doc Since OR-Set does not support deferred operations (yet!) After
%% merging the sets and the deferred list, see if any deferred removes
%% are now relevant.
model_apply_deferred(ORSet, Deferred) ->
    Elems = riak_dt_orset:value(removed, Deferred),
    lists:foldl(fun(E, {OIn, DIn}) ->
                        Frag = riak_dt_orset:value({fragment, E}, Deferred),
                        context_remove_model(E, Frag, OIn, DIn)
                end,
                %% Empty deferred list to build again for ops that are
                %% not enabled by merge
                {ORSet, riak_dt_orset:new()},
                Elems).

%% @TODO Knows about the internal working of riak_dt_orset,
%% riak_dt_osret should provide deferred operations. Also, this is
%% heinously ugly and hard to read.
context_remove_model(Elem, RemoveContext, ORSet, Deferred) ->
    Present = riak_dt_orset:value({tokens, Elem}, ORSet),
    Removing = riak_dt_orset:value({tokens, Elem}, RemoveContext),
    %% Any token in the orset that is present in the RemoveContext can
    %% be removed, any that is not must be deferred.
    orddict:fold(fun(Token, _Bool, {ORSetAcc, Defer}) ->
                         case orddict:is_key(Token, Present) of
                             true ->
                                 %% The target has this Token, so
                                 %% merge with it to ensure it is
                                 %% removed
                                 {riak_dt_orset:merge(ORSetAcc,
                                                      orddict:store(Elem,
                                                                    orddict:store(Token, true, orddict:new()),
                                                                    orddict:new())),
                                  Defer};
                             false ->
                                 %% This token does not yet exist as
                                 %% an add or remove in the orset, so
                                 %% defer it's removal until it has
                                 %% been added by a merge
                                 {ORSetAcc,
                                  orddict:update(Elem, fun(Tokens) ->
                                                               orddict:store(Token, true, Tokens)
                                                       end,
                                                 orddict:store(Token, true, orddict:new()),
                                                 Defer)}
                         end

                 end,
                 {ORSet, Deferred},
                 Removing).

%% @doc merge both orset and deferred operations
model_merge({S1, D1}, {S2, D2}) ->
    S = riak_dt_orset:merge(S1, S2),
    D = riak_dt_orset:merge(D1, D2),
    model_apply_deferred(S, D).
-endif.
