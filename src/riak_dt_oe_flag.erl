%% -------------------------------------------------------------------
%%
%% riak_dt_oe_flag: a flag that can be enabled and disabled as many
%%   times as you want, disabling wins, starts enabled.
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

-module(riak_dt_oe_flag).

-behaviour(riak_dt).

-export([new/0, value/1, value/2, update/3, merge/2, equal/2, from_binary/1, to_binary/1, stats/1, stat/2]).
-export([update/4, parent_clock/2]).
-export([to_binary/2]).
-export([to_version/2]).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-export([gen_op/0, init_state/0, update_expected/3, eqc_state_value/1, generate/0, size/1]).
-export([prop_crdt_converge/0, prop_crdt_bin_roundtrip/0]).
-endif.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export_type([oe_flag/0, oe_flag_op/0]).

-opaque oe_flag() :: {riak_dt_vclock:vclock(), boolean()}.
-type oe_flag_op() :: enable | disable.

-spec new() -> oe_flag().
new() ->
    {riak_dt_vclock:fresh(), true}.

-spec value(oe_flag()) -> boolean().
value({_,F}) -> F.

-spec value(term(), oe_flag()) -> boolean().
value(_, Flag) ->
    value(Flag).

-spec update(oe_flag_op(), riak_dt:actor(), oe_flag()) -> {ok, oe_flag()}.
update(disable, Actor, {Clock,_}) ->
    NewClock = riak_dt_vclock:increment(Actor, Clock),
    {ok, {NewClock, false}};
update(enable, _Actor, {Clock,_}) ->
    {ok, {Clock, true}}.

%% @todo this needs to use the context
-spec update(oe_flag_op(), riak_dt:actor(), oe_flag(), _Ctx) ->
                    {ok, oe_flag()}.
update(Op, Actor, Flag, _Ctx) ->
    update(Op, Actor, Flag).

-spec parent_clock(riak_dt_vclock:vclock(), oe_flag()) -> oe_flag().
parent_clock(_Clock, Flag) ->
    Flag.

-spec merge(oe_flag(), oe_flag()) -> oe_flag().
merge({C1, F}, {C2,F}) ->
    %% When they are the same result (true or false), just merge the
    %% vclock.
    {riak_dt_vclock:merge([C1, C2]), F};
merge({C1, _}=ODF1, {C2, _}=ODF2) ->
    %% When the flag disagrees:
    case {riak_dt_vclock:equal(C1, C2),
          riak_dt_vclock:descends(C1, C2),
          riak_dt_vclock:descends(C2, C1)} of
    %% 1) If the clocks are equal, the result is 'true' (observed
    %% enable).
        {true, _, _} ->
            {riak_dt_vclock:merge([C1, C2]), true};
    %% 2) If they are sibling/divergent clocks, the result is 'false'.
        {_, false, false} ->
            {riak_dt_vclock:merge([C1, C2]), false};
    %% 3) If one clock dominates the other, its value should be
    %% chosen.
        {_, true, false} ->
            ODF1;
        {_, false, true} ->
            ODF2
    end.

-spec equal(oe_flag(), oe_flag()) -> boolean().
equal({C1,F},{C2,F}) ->
    riak_dt_vclock:equal(C1,C2);
equal(_,_) -> false.

-include("riak_dt_tags.hrl").
-define(TAG, ?DT_OE_FLAG_TAG).
-define(VSN1, 1).

-spec from_binary(binary()) -> {ok, oe_flag()} | ?UNSUPPORTED_VERSION | ?INVALID_BINARY.
from_binary(<<?TAG:8, ?VSN1:8, BFlag:8, VClock/binary>>) ->
    Flag = case BFlag of
               1 -> true;
               0 -> false
           end,
    {ok, {riak_dt_vclock:from_binary(VClock), Flag}};
from_binary(<<?TAG:8, Vers:8, _BFlag/binary>>) ->
    ?UNSUPPORTED_VERSION(Vers);
from_binary(_B) ->
    ?INVALID_BINARY.

-spec to_binary(oe_flag()) -> binary().
to_binary({Clock, Flag}) ->
    BFlag = case Flag of
                true -> 1;
                false -> 0
            end,
    VCBin = riak_dt_vclock:to_binary(Clock),
    <<?TAG:8, ?VSN1:8, BFlag:8, VCBin/binary>>.

-spec to_binary(Vers :: pos_integer(), oe_flag()) -> {ok, binary()} | ?UNSUPPORTED_VERSION.
to_binary(1, Flag) ->
    {ok, to_binary(Flag)};
to_binary(Vers, _F) ->
    ?UNSUPPORTED_VERSION(Vers).

-spec stats(oe_flag()) -> [{atom(), integer()}].
stats(OEF) ->
    [{actor_count, stat(actor_count, OEF)}].

-spec stat(atom(), oe_flag()) -> number() | undefined.
stat(actor_count, {C, _}) ->
    length(C);
stat(_, _) -> undefined.

-spec to_version(pos_integer(), oe_flag()) -> oe_flag().
to_version(_Version, Flag) ->
    Flag.


%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

-ifdef(EQC).
prop_crdt_converge() ->
     crdt_statem_eqc:prop_converge(?MODULE).

prop_crdt_bin_roundtrip() ->
    crdt_statem_eqc:prop_bin_roundtrip(?MODULE).

% EQC generator
gen_op() ->
    oneof([disable,enable]).

size({Clock,_}) ->
    length(Clock).

init_state() ->
    orddict:new().

generate() ->
    ?LET(Ops, non_empty(list({gen_op(), binary(16)})),
         lists:foldl(fun({Op, Actor}, Flag) ->
                             {ok, F} = ?MODULE:update(Op, Actor, Flag),
                             F
                     end,
                     ?MODULE:new(),
                     Ops)).

update_expected(ID, create, Dict) ->
    orddict:store(ID, true, Dict);
update_expected(ID, enable, Dict) ->
    orddict:store(ID, true, Dict);
update_expected(ID, disable, Dict) ->
    orddict:store(ID, false, Dict);
update_expected(ID, {merge, SourceID}, Dict) ->
    Mine = orddict:fetch(ID, Dict),
    Theirs = orddict:fetch(SourceID, Dict),
    Merged = Mine and Theirs,
    orddict:store(ID, Merged, Dict).

eqc_state_value(Dict) ->
    orddict:fold(fun(_K,V,Acc) ->
            V and Acc
        end, true, Dict).
-endif.

new_test() ->
    ?assertEqual(true, value(new())).

update_enable_test() ->
    F0 = new(),
    {ok, F1} = update(disable, 1, F0),
    ?assertEqual(false, value(F1)).

update_enable_multi_test() ->
    F0 = new(),
    {ok, F1} = update(disable, 1, F0),
    {ok, F2} = update(enable, 1, F1),
    {ok, F3} = update(disable, 1, F2),
    ?assertEqual(false, value(F3)).

merge_offs_test() ->
    F0 = new(),
    ?assertEqual(true, value(merge(F0, F0))).

merge_simple_test() ->
    F0 = new(),
    {ok, F1} = update(disable, 1, F0),
    ?assertEqual(false, value(merge(F1, F0))),
    ?assertEqual(false, value(merge(F0, F1))),
    ?assertEqual(false, value(merge(F1, F1))).

merge_concurrent_test() ->
    F0 = new(),
    {ok, F1} = update(disable, 1, F0),
    {ok, F2} = update(enable, 1, F1),
    {ok, F3} = update(disable, 1, F1),
    ?assertEqual(false, value(merge(F1,F3))),
    ?assertEqual(true, value(merge(F1,F2))),
    ?assertEqual(false, value(merge(F2,F3))).

stat_test() ->
    F0 = new(),
    {ok, F1} = update(disable, 1, F0),
    {ok, F2} = update(disable, 2, F1),
    {ok, F3} = update(disable, 3, F2),
    {ok, F4} = update(enable, 4, F3), %% Observed-enable doesn't add an actor
    ?assertEqual([{actor_count, 3}], stats(F4)),
    ?assertEqual(3, stat(actor_count, F4)),
    ?assertEqual(undefined, stat(element_count, F4)).
-endif.
