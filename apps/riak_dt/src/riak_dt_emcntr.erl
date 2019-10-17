%% -*- coding: utf-8 -*-
%% -------------------------------------------------------------------
%%
%% riak_dt_emcntr: A convergent, replicated, state based PN counter,
%% for embedding in riak_dt_map.
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc A PN-Counter CRDT. A PN-Counter is essentially two G-Counters:
%% one for increments and one for decrements. The value of the counter
%% is the difference between the value of the Positive G-Counter and
%% the value of the Negative G-Counter. However, this PN-Counter is
%% for using embedded in a riak_dt_map. The problem with an embedded
%% pn-counter is when the field is removed and added again. PN-Counter
%% merge takes the max of P and N as the merged value. In the case
%% that a field was removed and re-added P and N maybe be _lower_ than
%% their removed values, and when merged with a replica that has not
%% seen the remove, the remove is lost. This counter adds some
%% causality by storing a `dot' with P and N. Merge takes the max
%% event for each actor, so newer values win over old ones. The rest
%% of the mechanics are the same.
%%
%% @see riak_kv_gcounter
%%
%% @reference Marc Shapiro, Nuno Preguiça, Carlos Baquero, Marek Zawirski (2011) A comprehensive study of
%% Convergent and Commutative Replicated Data Types. [http://hal.upmc.fr/inria-00555588/]
%%
%% @end

-module(riak_dt_emcntr).
-behaviour(riak_dt).

-export([new/0, value/1, value/2]).
-export([update/3, merge/2, equal/2]).
-export([to_binary/1, from_binary/1]).
-export([to_binary/2]).
-export([stats/1, stat/2]).
-export([parent_clock/2, update/4]).
-export([to_version/2]).

%% EQC API
-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-export([gen_op/0, gen_op/1, update_expected/3, eqc_state_value/1, init_state/0, generate/0]).
-endif.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export_type([emcntr/0, emcntr_op/0]).

-type emcntr() :: {riak_dt_vclock:vclock(), [entry()]}.
-type entry() :: {Actor :: riak_dt:actor(), {Event :: pos_integer(),
                                             Inc :: pos_integer(),
                                             Dec :: pos_integer()}
                 }.


-type emcntr_op() :: increment_op() | decrement_op().
-type increment_op() :: increment | {increment, integer()}.
-type decrement_op() :: decrement | {decrement, integer()}.

-spec new() -> emcntr().
new() ->
    {riak_dt_vclock:fresh(), orddict:new()}.

%% @doc embedded CRDTs most share a causal context with their parent
%% Map, setting the internal clock to the parent clock ensures this
-spec parent_clock(riak_dt_vclock:vclock(), emcntr()) -> emcntr().
parent_clock(Clock, {_, Cntr}) ->
    {Clock, Cntr}.

%% @doc the current integer value of the counter
-spec value(emcntr()) -> integer().
value({_Clock, PNCnt}) ->
    lists:sum([Inc - Dec || {_Act, {_Event, Inc, Dec}} <- PNCnt]).

%% @doc query value, not implemented. Just returns result of `value/1'
-spec value(term(), emcntr()) -> integer().
value(_, Cntr) ->
    value(Cntr).

%% @doc increment/decrement the counter. Op is either a two tuple of
%% `{increment, By}', `{decrement, By}' where `By' is a positive
%% integer. Or simply the atoms `increment' or `decrement', which are
%% equivalent to `{increment | decrement, 1}' Returns the updated
%% counter.
%%
%% Note: the second argument must be a `riak_dt:dot()', that is a
%% 2-tuple of `{Actor :: term(), Event :: pos_integer()}' as this is
%% for embedding in a `riak_dt_map'
-spec update(emcntr_op(), riak_dt:dot(), emcntr()) -> {ok, emcntr()}.
update(Op, {Actor, Evt}=Dot, {Clock, PNCnt}) when is_tuple(Dot) ->
    Clock2 = riak_dt_vclock:merge([[Dot], Clock]),
    Entry = orddict:find(Actor, PNCnt),
    {Inc, Dec} = op(Op, Entry),
    {ok, {Clock2, orddict:store(Actor, {Evt, Inc, Dec}, PNCnt)}}.

%% @doc update with a context. Contexts have no effect. Same as
%% `update/3'
-spec update(emcntr_op(), riak_dt:dot(), emcntr(), riak_dt_vclock:vclock()) ->
                    {ok, emcntr()}.
update(Op, Dot, Cntr, _Ctx) ->
    update(Op, Dot, Cntr).

%% @private perform the operation `Op' on the {positive, negative}
%% pair for an actor.
-spec op(emcntr_op(), error | {ok, entry()} | {P::non_neg_integer(), N::non_neg_integer()}) ->
                {P::non_neg_integer(), N::non_neg_integer()}.
op(Op, error) ->
    op(Op, {0, 0});
op(Op, {ok, {_Evt, P, N}}) ->
    op(Op, {P, N});
op(increment, {P, N}) ->
    op({increment, 1}, {P, N});
op(decrement, {P, N}) ->
    op({decrement, 1}, {P, N});
op({_Op, 0}, {P, N}) ->
    {P, N};
op({increment, By}, {P, N}) when is_integer(By), By > 0 ->
    {P+By, N};
op({increment, By}, {P, N}) when is_integer(By), By < 0 ->
    op({decrement, -By}, {P, N});
op({decrement, By}, {P, N}) when is_integer(By), By > 0 ->
    {P, N+By};
op({decrement, By}, {P, N}) when is_integer(By), By < 0 ->
    op({increment, -By}, {P, N}).

%% @doc takes two `emcntr()'s and merges them into a single
%% `emcntr()'. This is the Least Upper Bound of the Semi-Lattice/CRDT
%% literature. The semantics of the `emnctr()' merge are explained in
%% the module docs. In a nutshell, merges version vectors, and keeps
%% only dots that are present on both sides, or concurrent.
-spec merge(emcntr(), emcntr()) -> emcntr().
merge(Cnt, Cnt) ->
    Cnt;
merge({ClockA, CntA}, {ClockB, CntB}) ->
    Clock = riak_dt_vclock:merge([ClockA, ClockB]),
    {Cnt0, BUnique} = merge_left(ClockB, CntA, CntB),
    Cnt = merge_right(ClockA, BUnique, Cnt0),
    {Clock, Cnt}.

%% @private merge the left handside counter (A) by filtering out the
%% dots that are unique to it, and dominated. Returns `[entry()]' as
%% an accumulator, and the dots that are unique to the right hand side
%% (B).
-spec merge_left(riak_dt_vclock:vclock(), [entry()], [entry()]) -> {[entry()], [entry()]}.
merge_left(RHSClock, LHS, RHS) ->
    orddict:fold(fun(Actor, {Evt, _Inc, _Dec}=Cnt, {Keep, RHSUnique}) ->
                         case orddict:find(Actor, RHS) of
                             error ->
                                 case riak_dt_vclock:descends(RHSClock, [{Actor, Evt}]) of
                                     true ->
                                         {Keep, RHSUnique};
                                     false ->
                                         {orddict:store(Actor, Cnt, Keep), RHSUnique}
                                 end;
                             %% RHS has this actor, with a greater dot
                             {ok, {E2, I, D}} when E2 > Evt ->
                                 {orddict:store(Actor, {E2, I, D}, Keep), orddict:erase(Actor, RHSUnique)};
                             %% RHS has this actor, but a lesser or equal dot
                             {ok, _} ->
                                 {orddict:store(Actor, Cnt, Keep), orddict:erase(Actor, RHSUnique)}
                         end
                 end,
                 {orddict:new(), RHS},
                 LHS).

%% @private merge the unique actor entries from the right hand side,
%% keeping the concurrent ones, and dropping the dominated.
-spec merge_right(riak_dt_vclock:vclock(), [entry()], [entry()]) -> [entry()].
merge_right(LHSClock, RHSUnique, Acc) ->
    orddict:fold(fun(Actor, {Evt, _Inc, _Dec}=Cnt, Keep) ->
                         case riak_dt_vclock:descends(LHSClock, [{Actor, Evt}]) of
                             true ->
                                 Keep;
                             false ->
                                 orddict:store(Actor, Cnt, Keep)
                         end
                 end,
                 Acc,
                 RHSUnique).

%% @doc equality of two counters internal structure, not the `value/1'
%% they produce.
-spec equal(emcntr(), emcntr()) -> boolean().
equal({ClockA, PNCntA}, {ClockB, PNCntB}) ->
    riak_dt_vclock:equal(ClockA, ClockB) andalso
        PNCntA =:= PNCntB.

%% @doc generate stats for this counter. Only `actor_count' is
%% produced at present.
-spec stats(emcntr()) -> [{actor_count, pos_integer()}].
stats(Emcntr) ->
    [{actor_count, stat(actor_count, Emcntr)}].

%% @doc generate stat for requested stat type at first argument. Only
%% `actor_count' is supported at present.  Return a `pos_integer()' for
%% the stat requested, or `undefined' if stat type is unsupported.
-spec stat(atom(), emcntr()) -> pos_integer() | undefined.
stat(actor_count, {Clock, _Emcntr}) ->
    length(Clock);
stat(_, _) -> undefined.

-include("riak_dt_tags.hrl").
-define(TAG, ?DT_EMCNTR_TAG).
-define(V1_VERS, 1).

%% @doc produce a compact binary representation of the counter.
%%
%% @see from_binary/1
-spec to_binary(emcntr()) -> binary().
to_binary(Cntr) ->
    Bin = term_to_binary(Cntr),
    <<?TAG:8/integer, ?V1_VERS:8/integer, Bin/binary>>.

-spec to_binary(Vers :: pos_integer(), emcntr()) -> {ok, binary()} | ?UNSUPPORTED_VERSION.
to_binary(1, Cntr) ->
    B = to_binary(Cntr),
    {ok, B};
to_binary(Vers, _Cntr) ->
    ?UNSUPPORTED_VERSION(Vers).

%% @doc Decode a binary encoded riak_dt_emcntr.
%%
%% @see to_binary/1
-spec from_binary(binary()) -> {ok, emcntr()} | ?INVALID_BINARY | ?UNSUPPORTED_VERSION.
from_binary(<<?TAG:8/integer, ?V1_VERS:8/integer, Bin/binary>>) ->
    {ok, binary_to_term(Bin)};
from_binary(<<?TAG:8/integer, Vers:8/integer, _Bin/binary>>) ->
    ?UNSUPPORTED_VERSION(Vers);
from_binary(_Bin) ->
    ?INVALID_BINARY.

-spec to_version(pos_integer(), emcntr()) -> emcntr().
to_version(_Version, Cntr) ->
    Cntr.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

-ifdef(EQC).
%% EQC generator
generate() ->
    ?LET({Ops, Actors}, {non_empty(list(gen_op())), non_empty(list(bitstring(16*8)))},
         begin
             {Generated, _Evts} = lists:foldl(fun(Op, {Cntr, Evt}) ->
                                                      Actor = case length(Actors) of
                                                                  1 -> hd(Actors);
                                                                  _ -> lists:nth(rand:uniform( length(Actors)+1), Actors)
                                                              end,
                                                      {ok, Cntr2} = riak_dt_emcntr:update(Op, {Actor, Evt}, Cntr),
                                                      {Cntr2, Evt+1}
                                              end,
                                              {riak_dt_emcntr:new(), 1},
                                              Ops),
             Generated
         end).

init_state() ->
    0.

gen_op(_Size) ->
    gen_op().

gen_op() ->
    oneof([increment,
           {increment, nat()},
           decrement,
           {decrement, nat()}
          ]).

update_expected(_ID, increment, Prev) ->
    Prev+1;
update_expected(_ID, decrement, Prev) ->
    Prev-1;
update_expected(_ID, {increment, By}, Prev) ->
    Prev+By;
update_expected(_ID, {decrement, By}, Prev) ->
    Prev-By;
update_expected(_ID, _Op, Prev) ->
    Prev.

eqc_state_value(S) ->
    S.
-endif.

new_test() ->
    ?assertEqual(0, value(new())).

make_counter(Ops, Evt) ->
    lists:foldl(fun({Actor, Op}, {Counter, Event}) ->
                        E2 = Event+1,
                        {ok, C2} = update(Op, {Actor, E2}, Counter),
                        {C2, E2} end,
                {new(), Evt},
                Ops).

make_counter(Ops) ->
    {Cnt, _Evt} = make_counter(Ops, 0),
    Cnt.

value_test() ->
    PNCnt1 = make_counter([{a, increment},
                           {b, {increment, 13}}, {b, {decrement, 10}},
                           {c, increment},
                           {d, decrement}]),
    PNCnt2 = make_counter([]),
    PNCnt3 = make_counter([{a, {increment,3}}, {a, {decrement, 3}},
                           {b, decrement}, {b, increment},
                           {c, increment}, {c, decrement}]),
    ?assertEqual(4, value(PNCnt1)),
    ?assertEqual(0, value(PNCnt2)),
    ?assertEqual(0, value(PNCnt3)).

update_increment_test() ->
    PNCnt0 = new(),
    {ok, PNCnt1} = update(increment, {a, 1}, PNCnt0),
    {ok, PNCnt2} = update(increment, {b, 1}, PNCnt1),
    {ok, PNCnt3} = update(increment, {a, 2}, PNCnt2),
    ?assertEqual(3, value(PNCnt3)).

update_increment_by_test() ->
    PNCnt0 = new(),
    {ok, PNCnt1} = update({increment, 7}, {a, 1}, PNCnt0),
    ?assertEqual(7, value(PNCnt1)).

update_decrement_test() ->
    PNCnt0 = new(),
    {ok, PNCnt1} = update(increment, {a, 1}, PNCnt0),
    {ok, PNCnt2} = update(increment, {b, 1}, PNCnt1),
    {ok, PNCnt3} = update(increment, {a, 2}, PNCnt2),
    {ok, PNCnt4} = update(decrement, {a, 3}, PNCnt3),
    ?assertEqual(2, value(PNCnt4)).

update_decrement_by_test() ->
    PNCnt0 = new(),
    {ok, PNCnt1} = update({increment, 7}, {a, 1}, PNCnt0),
    {ok, PNCnt2} = update({decrement, 5}, {a, 2}, PNCnt1),
    ?assertEqual(2, value(PNCnt2)).

update_neg_increment_by_test() ->
    PNCnt0 = new(),
    {ok, PNCnt1} = update({increment, -8}, {a, 1}, PNCnt0),
    {ok, PNCnt2} = update({decrement, -7}, {a, 2}, PNCnt1),
    ?assertEqual(-1, value(PNCnt2)).

merge_test() ->
    {PNCnt1, Evt} = make_counter([{<<"1">>, increment},
                           {<<"2">>, {increment, 2}},
                           {<<"4">>, {increment, 1}}], 0),
    {PNCnt2, _Evt2} = make_counter([{<<"3">>, {increment, 3}},
                           {<<"4">>, {increment, 3}}], Evt),
    ?assertEqual(new(), merge(new(), new())),
    ?assertEqual(9, value(merge(PNCnt1, PNCnt2))).

equal_test() ->
    PNCnt1 = make_counter([{1, {increment, 2}}, {1, decrement},
                           {2, increment},
                           {3, decrement},
                           {4, increment}]),
    PNCnt2 = make_counter([{1, increment},
                           {2, {increment, 4}},
                           {3, increment}]),
    PNCnt3 = make_counter([{1, {increment, 2}}, {1, decrement},
                           {2, increment},
                           {3, decrement},
                           {4, increment}]),
    ?assertNot(equal(PNCnt1, PNCnt2)),
    ?assert(equal(PNCnt1, PNCnt3)).

usage_test() ->
    PNCnt1 = new(),
    PNCnt2 = new(),
    ?assert(equal(PNCnt1, PNCnt2)),
    {ok, PNCnt1_1} = update({increment, 2}, {a1, 1}, PNCnt1),
    {ok, PNCnt2_1} = update(increment, {a2, 1}, PNCnt2),
    PNCnt3 = merge(PNCnt1_1, PNCnt2_1),
    {ok, PNCnt2_2} = update({increment, 3}, {a3, 1}, PNCnt2_1),
    {ok, PNCnt3_1} = update(increment, {a4, 1}, PNCnt3),
    {ok, PNCnt3_2} = update(increment, {a1, 2}, PNCnt3_1),
    {ok, PNCnt3_3} = update({decrement, 2}, {a5, 1}, PNCnt3_2),
    {ok, PNCnt2_3} = update(decrement, {a2, 2}, PNCnt2_2),
    ?assertEqual({[{a1, 2}, {a2, 2}, {a3, 1}, {a4, 1}, {a5, 1}],
                  [{a1, {2, 3,0}},
                   {a2, {2, 1, 1}},
                   {a3, {1, 3,0}},
                   {a4, {1, 1, 0}},
                   {a5, {1, 0,2}}]}, merge(PNCnt3_3, PNCnt2_3)).

roundtrip_bin_test() ->
    PN = new(),
    {ok, PN1} = update({increment, 2}, {<<"a1">>, 1}, PN),
    {ok, PN2} = update({decrement, 1000000000000000000000000}, {douglas_Actor, 1}, PN1),
    {ok, PN3} = update(increment, {[{very, ["Complex"], <<"actor">>}, honest], 900987}, PN2),
    {ok, PN4} = update(decrement, {"another_acotr", 28}, PN3),
    Bin = to_binary(PN4),
    {ok, Decoded} = from_binary(Bin),
    ?assert(equal(PN4, Decoded)).

stat_test() ->
    PN = new(),
    {ok, PN1} = update({increment, 50}, {a1, 1}, PN),
    {ok, PN2} = update({increment, 50}, {a2, 1}, PN1),
    {ok, PN3} = update({decrement, 15}, {a3, 1}, PN2),
    {ok, PN4} = update({decrement, 10}, {a4, 1}, PN3),
    ?assertEqual([{actor_count, 0}], stats(PN)),
    ?assertEqual(4, stat(actor_count, PN4)),
    ?assertEqual(undefined, stat(max_dot_length, PN4)).
-endif.
