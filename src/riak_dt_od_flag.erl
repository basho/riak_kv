%% -------------------------------------------------------------------
%%
%% riak_dt_od_flag: a flag that can be enabled and disabled as many
%%     times as you want, enabling wins, starts disabled.
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

-module(riak_dt_od_flag).

-behaviour(riak_dt).

-export([new/0, value/1, value/2, update/3, update/4]).
-export([ merge/2, equal/2, from_binary/1]).
-export([to_binary/1, stats/1, stat/2]).
-export([to_binary/2]).
-export([precondition_context/1]).
-export([parent_clock/2]).
-export([to_version/2]).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-export([gen_op/0, gen_op/1, init_state/0, update_expected/3, eqc_state_value/1, generate/0, size/1]).
-export([prop_crdt_converge/0, prop_crdt_bin_roundtrip/0]).
-endif.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export_type([od_flag/0, od_flag_op/0]).

-opaque od_flag() :: {riak_dt_vclock:vclock(), [riak_dt:dot()], deferred()}.
-type od_flag_op() :: enable | disable.
-type deferred() :: ordsets:ordset(riak_dt:context()).

-spec new() -> od_flag().
new() ->
    {riak_dt_vclock:fresh(), [], []}.

%% @doc sets the clock in the flag to that `Clock'. Used by a
%% containing Map for sub-CRDTs
-spec parent_clock(riak_dt_vclock:vclock(), od_flag()) -> od_flag().
parent_clock(Clock, {_SetClock, Flag , Deferred}) ->
    {Clock, Flag, Deferred}.

-spec value(od_flag()) -> boolean().
value({_, [], _}) -> false;
value({_, _, _}) -> true.

-spec value(term(), od_flag()) -> boolean().
value(_, Flag) ->
    value(Flag).

-spec update(od_flag_op(), riak_dt:actor() | riak_dt:dot(), od_flag()) -> {ok, od_flag()}.
update(enable, Dot, {Clock, Dots, Deferred}) when is_tuple(Dot) ->
    NewClock = riak_dt_vclock:merge([[Dot], Clock]),
    {ok, {NewClock, riak_dt_vclock:merge([[Dot], Dots]), Deferred}};
update(enable, Actor, {Clock, Dots, Deferred}) ->
    NewClock = riak_dt_vclock:increment(Actor, Clock),
    Dot = [{Actor, riak_dt_vclock:get_counter(Actor, NewClock)}],
    {ok, {NewClock, riak_dt_vclock:merge([Dot, Dots]), Deferred}};
update(disable, _Actor, Flag) ->
    disable(Flag, undefined).

-spec disable(od_flag(), riak_dt:context()) ->
    {ok, od_flag()}.
disable({Clock, _, Deferred}, undefined) ->
    {ok, {Clock, [], Deferred}};
disable({Clock, Dots, Deferred}, Ctx) ->
    NewDots = riak_dt_vclock:subtract_dots(Dots, Ctx),
    NewDeferred = defer_disable(Clock, Ctx, Deferred),
    {ok, {Clock, NewDots, NewDeferred}}.

%% @doc `update/4' is similar to `update/3' except that it takes a
%% `context' (obtained from calling `precondition_context/1'). This
%% context ensure that a `disable' operation does not `disable' a flag
%% that is has not been `seen` enabled by the caller. For example, the
%% flag has been enabled by `a', `b', and `c'. The user has a copy of
%% the flag obtained by reading `a'. Subsequently all replicas merge,
%% and the user sends a `disable' operation. The have only seen the
%% enable of `a', yet they disable the concurrecnt `b' and `c'
%% operations, unless they send a context obtained from `a'.
-spec update(od_flag_op(), riak_dt:actor() | riak_dt:dot(),
             od_flag(), riak_dt:context()) ->
                    {ok, od_flag()}.
update(Op, Actor, Flag, undefined) ->
    update(Op, Actor, Flag);
update(enable, Actor, Flag, _Ctx) ->
    update(enable, Actor, Flag);
update(disable, _Actor, Flag, Ctx) ->
    disable(Flag, Ctx).

%% @private Determine if a `disable' operation needs to be deferred,
%% or if it is complete.
-spec defer_disable(riak_dt_vclock:vclock(), riak_dt_vclock:vclock(), deferred()) ->
                           deferred().
defer_disable(Clock, Ctx, Deferred) ->
    case riak_dt_vclock:descends(Clock, Ctx) of
        true ->
            Deferred;
        false ->
            ordsets:add_element(Ctx, Deferred)
    end.

-spec merge(od_flag(), od_flag()) -> od_flag().
merge({Clock, Entries, Deferred}, {Clock, Entries, Deferred}) ->
    %% When they are the same result why merge?
    {Clock, Entries, Deferred};
merge({LHSClock, LHSDots, LHSDeferred}, {RHSClock, RHSDots, RHSDeferred}) ->
    NewClock = riak_dt_vclock:merge([LHSClock, RHSClock]),
    %% drop all the LHS dots that are dominated by the rhs clock
    %% drop all the RHS dots that dominated by the LHS clock
    %% keep all the dots that are in both
    %% save value as value of flag
    CommonDots = ordsets:intersection(ordsets:from_list(LHSDots), ordsets:from_list(RHSDots)),
    LHSUnique = ordsets:to_list(ordsets:subtract(ordsets:from_list(LHSDots), CommonDots)),
    RHSUnique = ordsets:to_list(ordsets:subtract(ordsets:from_list(RHSDots), CommonDots)),
    LHSKeep = riak_dt_vclock:subtract_dots(LHSUnique, RHSClock),
    RHSKeep = riak_dt_vclock:subtract_dots(RHSUnique, LHSClock),
    Flag = riak_dt_vclock:merge([ordsets:to_list(CommonDots), LHSKeep, RHSKeep]),
    Deferred = ordsets:union(LHSDeferred, RHSDeferred),

    apply_deferred(NewClock, Flag, Deferred).

apply_deferred(Clock, Flag, Deferred) ->
    lists:foldl(fun(Ctx, ODFlag) ->
                        {ok, ODFlag2} = disable(ODFlag, Ctx),
                        ODFlag2
                end,
                %% start with an empty deferred list, those
                %% non-descended ctx operations will be re-added to
                %% the deferred if they still cannot be executed.
                {Clock, Flag, []},
                Deferred).

-spec equal(od_flag(), od_flag()) -> boolean().
equal({C1,D1, Def1},{C2,D2, Def2}) ->
    riak_dt_vclock:equal(C1,C2) andalso
        riak_dt_vclock:equal(D1, D2) andalso
        Def1 == Def2.

-spec stats(od_flag()) -> [{atom(), integer()}].
stats(ODF) ->
    [{actor_count, stat(actor_count, ODF)},
     {dot_length, stat(dot_length, ODF)},
     {deferred_length, stat(deferred_length, ODF)}].

-spec stat(atom(), od_flag()) -> number() | undefined.
stat(actor_count, {C, _, _}) ->
    length(C);
stat(dot_length, {_, D, _}) ->
    length(D);
stat(deferred_length, {_Clock, _Dots, Deferred}) ->
    length(Deferred);
stat(_, _) -> undefined.

-include("riak_dt_tags.hrl").
-define(TAG, ?DT_OD_FLAG_TAG).
-define(VSN1, 1).

-spec from_binary(binary()) -> {ok, od_flag()} | ?UNSUPPORTED_VERSION | ?INVALID_BINARY.
from_binary(<<?TAG:8, ?VSN1:8, Bin/binary>>) ->
    {ok, riak_dt:from_binary(Bin)};
from_binary(<<?TAG:8, Vers:8, _Bin/binary>>) ->
    ?UNSUPPORTED_VERSION(Vers);
from_binary(_B) ->
    ?INVALID_BINARY.

-spec to_binary(od_flag()) -> binary().
to_binary(Flag) ->
    <<?TAG:8, ?VSN1:8, (riak_dt:to_binary(Flag))/binary>>.

-spec to_binary(Vers :: pos_integer(), od_flag()) -> {ok, binary()} | ?UNSUPPORTED_VERSION.
to_binary(1, Flag) ->
    B = to_binary(Flag),
    {ok, B};
to_binary(Vers, _F) ->
    ?UNSUPPORTED_VERSION(Vers).

%% @doc the `precondition_context' is an opaque piece of state that
%% can be used for context operations to ensure only observed state is
%% affected.
%%
%% @see update/4
-spec precondition_context(od_flag()) -> riak_dt:context().
precondition_context({Clock, _Flag, _Deferred}) ->
    Clock.

-spec to_version(pos_integer(), od_flag()) -> od_flag().
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
gen_op(_Size) ->
    gen_op().

gen_op() ->
    elements([disable,enable]).

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
    orddict:store(ID, false, Dict);
update_expected(ID, enable, Dict) ->
    orddict:store(ID, true, Dict);
update_expected(ID, disable, Dict) ->
    orddict:store(ID, false, Dict);
update_expected(ID, {merge, SourceID}, Dict) ->
    Mine = orddict:fetch(ID, Dict),
    Theirs = orddict:fetch(SourceID, Dict),
    Merged = Mine or Theirs,
    orddict:store(ID, Merged, Dict).

eqc_state_value(Dict) ->
    orddict:fold(fun(_K, V, Acc) -> V or Acc end, false, Dict).
-endif.

disable_test() ->
    {ok, A} = update(enable, a, new()),
    {ok, B} = update(enable, b, new()),
    C = A,
    {ok, A2} = update(disable, a, A),
    A3 = merge(A2, B),
    {ok, B2} = update(disable, b, B),
    Merged = merge(merge(C, A3), B2),
    ?assertEqual(false, value(Merged)).

new_test() ->
    ?assertEqual(false, value(new())).

update_enable_test() ->
    F0 = new(),
    {ok, F1} = update(enable, 1, F0),
    ?assertEqual(true, value(F1)).

update_enable_multi_test() ->
    F0 = new(),
    {ok, F1} = update(enable, 1, F0),
    {ok, F2} = update(disable, 1, F1),
    {ok, F3} = update(enable, 1, F2),
    ?assertEqual(true, value(F3)).

merge_offs_test() ->
    F0 = new(),
    ?assertEqual(false, value(merge(F0, F0))).

merge_simple_test() ->
    F0 = new(),
    {ok, F1} = update(enable, 1, F0),
    ?assertEqual(true, value(merge(F1, F0))),
    ?assertEqual(true, value(merge(F0, F1))),
    ?assertEqual(true, value(merge(F1, F1))).

merge_concurrent_test() ->
    F0 = new(),
    {ok, F1} = update(enable, 1, F0),
    {ok, F2} = update(disable, 1, F1),
    {ok, F3} = update(enable, 1, F1),
    ?assertEqual(true, value(merge(F1,F3))),
    ?assertEqual(false, value(merge(F1,F2))),
    ?assertEqual(true, value(merge(F2,F3))).

binary_roundtrip_test() ->
    F0 = new(),
    {ok, F1} = update(enable, 1, F0),
    {ok, F2} = update(disable, 1, F1),
    {ok, F3} = update(enable, 2, F2),
    Bin = to_binary(F3),
    {ok, F4} = from_binary(Bin),
    ?assert(equal(F4, F3)).

stat_test() ->
    {ok, F0} = update(enable, 1, new()),
    {ok, F1} = update(enable, 1, F0),
    {ok, F2} = update(enable, 2, F1),
    {ok, F3} = update(enable, 3, F2),
    ?assertEqual([{actor_count, 3}, {dot_length, 3}, {deferred_length, 0}], stats(F3)),
    {ok, F4} = update(disable, 4, F3), %% Observed-disable doesn't add an actor
    ?assertEqual([{actor_count, 3}, {dot_length, 0}, {deferred_length, 0}], stats(F4)),
    ?assertEqual(3, stat(actor_count, F4)),
    ?assertEqual(undefined, stat(max_dot_length, F4)).
-endif.
