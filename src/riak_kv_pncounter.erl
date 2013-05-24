%% -------------------------------------------------------------------
%%
%% riak_kv_pncounter: A convergent, replicated, state based PN counter
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

%% @doc
%% A PN-Counter CRDT. A PN-Counter is essentially two G-Counters: one for increments and
%% one for decrements. The value of the counter is the difference between the value of the
%% Positive G-Counter and the value of the Negative G-Counter.
%%
%% @see riak_kv_gcounter.erl
%%
%% @reference Marc Shapiro, Nuno Preguiça, Carlos Baquero, Marek Zawirski (2011) A comprehensive study of
%% Convergent and Commutative Replicated Data Types. http://hal.upmc.fr/inria-00555588/
%%
%% @end

-module(riak_kv_pncounter).

-export([new/0, new/2, value/1, update/3, merge/2, equal/2, to_binary/1, from_binary/1]).

%% EQC API
-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-export([gen_op/0, update_expected/3, eqc_state_value/1]).
-endif.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export_type([pncounter/0, pncounter_op/0]).

-opaque pncounter() :: {riak_kv_gcounter:gcounter(), riak_kv_gcounter:gcounter()}.
-type pncounter_op() :: riak_kv_gcounter:gcounter_op() | decrement_op().
-type decrement_op() :: decrement | {decrement, pos_integer()}.

%% @doc Create a new, empty `pncounter()'
-spec new() -> pncounter().
new() ->
    {riak_kv_gcounter:new(), riak_kv_gcounter:new()}.

%% @doc Create a `pncounter()' with an initial `Value' for `Actor'.
-spec new(term(), integer()) -> pncounter().
new(Actor, Value) when Value > 0 ->
    update({increment, Value}, Actor, new());
new(Actor, Value) when Value < 0 ->
    update({decrement, Value * -1}, Actor, new());
new(_Actor, _Zero) ->
    new().

%% @doc The single, total value of a `pncounter()'
-spec value(pncounter()) -> integer().
value({Incr, Decr}) ->
    riak_kv_gcounter:value(Incr) - riak_kv_gcounter:value(Decr).

%% @doc Update a `pncounter()'. The first argument is either the atom
%% `increment' or `decrement' or the two tuples `{increment, pos_integer()}' or
%% `{decrement, pos_integer()}'. In the case of the former, the operation's amount
%% is `1'. Otherwise it is the value provided in the tuple's second element.
%% `Actor' is any term, and the 3rd argument is the `pncounter()' to update.
%%
%% returns the updated `pncounter()'
-spec update(pncounter_op(), term(), pncounter()) -> pncounter().
update(increment, Actor, {Incr, Decr}) ->
    {riak_kv_gcounter:update(increment, Actor, Incr), Decr};
update({increment, By}, Actor, {Incr, Decr}) when is_integer(By), By > 0 ->
    {riak_kv_gcounter:update({increment, By}, Actor, Incr), Decr};
update(decrement, Actor, {Incr, Decr}) ->
    {Incr, riak_kv_gcounter:update(increment, Actor, Decr)};
update({decrement, By}, Actor, {Incr, Decr}) when is_integer(By), By > 0 ->
    {Incr, riak_kv_gcounter:update({increment, By}, Actor, Decr)}.

%% @doc Merge two `pncounter()'s to a single `pncounter()'. This is the Least Upper Bound
%% function described in the literature.
-spec merge(pncounter(), pncounter()) -> pncounter().
merge({Incr1, Decr1}, {Incr2, Decr2}) ->
    MergedIncr =  riak_kv_gcounter:merge(Incr1, Incr2),
    MergedDecr =  riak_kv_gcounter:merge(Decr1, Decr2),
    {MergedIncr, MergedDecr}.

%% @doc Are two `pncounter()'s structurally equal? This is not `value/1' equality.
%% Two counters might represent the total `-42', and not be `equal/2'. Equality here is
%% that both counters represent exactly the same information.
-spec equal(pncounter(), pncounter()) -> boolean().
equal({Incr1, Decr1}, {Incr2, Decr2}) ->
    riak_kv_gcounter:equal(Incr1, Incr2) andalso riak_kv_gcounter:equal(Decr1, Decr2).

-define(TAG, 71).
-define(V1_VERS, 1).

%% @doc Encode an effecient binary representation of `pncounter()'
-spec to_binary(pncounter()) -> binary().
to_binary({P, N}) ->
    PBin = riak_kv_gcounter:to_binary(P),
    NBin = riak_kv_gcounter:to_binary(N),
    PBinLen = byte_size(PBin),
    NBinLen = byte_size(NBin),
    <<?TAG:8/integer, ?V1_VERS:8/integer,
      PBinLen:32/integer, PBin:PBinLen/binary,
      NBinLen:32/integer, NBin:NBinLen/binary>>.

%% @doc Decode a binary encoded PN-Counter
-spec from_binary(binary()) -> pncounter().
from_binary(<<?TAG:8/integer, ?V1_VERS:8/integer,
              PBinLen:32/integer, PBin:PBinLen/binary,
              NBinLen:32/integer, NBin:NBinLen/binary>>) ->
    {riak_kv_gcounter:from_binary(PBin), riak_kv_gcounter:from_binary(NBin)}.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

-ifdef(EQC).
%% EQC generator
gen_op() ->
    oneof([increment, {increment, gen_pos()}, decrement, {decrement, gen_pos()} ]).

gen_pos()->
    ?LET(X, int(), 1+abs(X)).

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

eqc_value_test_() ->
    {timeout, 120, [?_assert(crdt_statem_eqc:prop_converge(0, 1000, ?MODULE))]}.
-endif.

new_test() ->
    ?assertEqual({[], []}, new()).

value_test() ->
    PNCnt1 = {[{1, 1}, {2, 13}, {3, 1}], [{2, 10}, {4, 1}]},
    PNCnt2 = {[], []},
    PNCnt3 = {[{1, 3}, {2, 1}, {3, 1}], [{1, 3}, {2, 1}, {3, 1}]},
    ?assertEqual(4, value(PNCnt1)),
    ?assertEqual(0, value(PNCnt2)),
    ?assertEqual(0, value(PNCnt3)).

update_increment_test() ->
    PNCnt0 = new(),
    PNCnt1 = update(increment, 1, PNCnt0),
    PNCnt2 = update(increment, 2, PNCnt1),
    PNCnt3 = update(increment, 1, PNCnt2),
    ?assertEqual({[{1, 2}, {2, 1}], []}, PNCnt3).

update_increment_by_test() ->
    PNCnt0 = new(),
    PNCnt1 = update({increment, 7}, 1, PNCnt0),
    ?assertEqual({[{1, 7}], []}, PNCnt1).

update_decrement_test() ->
    PNCnt0 = new(),
    PNCnt1 = update(increment, 1, PNCnt0),
    PNCnt2 = update(increment, 2, PNCnt1),
    PNCnt3 = update(increment, 1, PNCnt2),
    PNCnt4 = update(decrement, 1, PNCnt3),
    ?assertEqual({[{1, 2}, {2, 1}], [{1, 1}]}, PNCnt4).

update_decrement_by_test() ->
    PNCnt0 = new(),
    PNCnt1 = update({increment, 7}, 1, PNCnt0),
    PNCnt2 = update({decrement, 5}, 1, PNCnt1),
    ?assertEqual({[{1, 7}], [{1, 5}]}, PNCnt2).

merge_test() ->
    PNCnt1 = {[{<<"1">>, 1},
               {<<"2">>, 2},
               {<<"4">>, 4}], []},
    PNCnt2 = {[{<<"3">>, 3},
               {<<"4">>, 3}], []},
    ?assertEqual({[], []}, merge(new(), new())),
    ?assertEqual({[{<<"1">>,1},{<<"2">>,2},{<<"4">>,4},{<<"3">>,3}], []},
                merge(PNCnt1, PNCnt2)).

merge_too_test() ->
    PNCnt1 = {[{<<"5">>, 5}], [{<<"7">>, 4}]},
    PNCnt2 = {[{<<"6">>, 6}, {<<"7">>, 7}], [{<<"5">>, 2}]},
    ?assertEqual({[{<<"5">>, 5},{<<"6">>,6}, {<<"7">>, 7}], [{<<"7">>, 4}, {<<"5">>, 2}]},
                 merge(PNCnt1, PNCnt2)).

equal_test() ->
    PNCnt1 = {[{1, 2}, {2, 1}, {4, 1}], [{1, 1}, {3, 1}]},
    PNCnt2 = {[{1, 1}, {2, 4}, {3, 1}], []},
    PNCnt3 = {[{1, 2}, {2, 1}, {4, 1}], [{3, 1}, {1, 1}]},
    PNCnt4 = {[{4, 1}, {1, 2}, {2, 1}], [{1, 1}, {3, 1}]},
    ?assertNot(equal(PNCnt1, PNCnt2)),
    ?assert(equal(PNCnt3, PNCnt4)),
    ?assert(equal(PNCnt1, PNCnt3)).

usage_test() ->
    PNCnt1 = new(),
    PNCnt2 = new(),
    ?assert(equal(PNCnt1, PNCnt2)),
    PNCnt1_1 = update({increment, 2}, a1, PNCnt1),
    PNCnt2_1 = update(increment, a2, PNCnt2),
    PNCnt3 = merge(PNCnt1_1, PNCnt2_1),
    PNCnt2_2 = update({increment, 3}, a3, PNCnt2_1),
    PNCnt3_1 = update(increment, a4, PNCnt3),
    PNCnt3_2 = update(increment, a1, PNCnt3_1),
    PNCnt3_3 = update({decrement, 2}, a5, PNCnt3_2),
    PNCnt2_3 = update(decrement, a2, PNCnt2_2),
    ?assertEqual({[{a1, 3}, {a4, 1}, {a2, 1}, {a3, 3}], [{a5, 2}, {a2, 1}]},
                 merge(PNCnt3_3, PNCnt2_3)).

roundtrip_bin_test() ->
    PN = new(),
    PN1 = update({increment, 2}, <<"a1">>, PN),
    PN2 = update({decrement, 1000000000000000000000000}, douglas_Actor, PN1),
    PN3 = update(increment, [{very, ["Complex"], <<"actor">>}, honest], PN2),
    PN4 = update(decrement, "another_acotr", PN3),
    Bin = to_binary(PN4),
    Decoded = from_binary(Bin),
    ?assert(equal(PN4, Decoded)).

-endif.
