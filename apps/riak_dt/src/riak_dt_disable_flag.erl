%% -------------------------------------------------------------------
%%
%% riak_dt_disable_flag: A state based, disable-only flag.
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

%%% @doc
%%% an ON/OFF (boolean) CRDT, which converges towards OFF
%%% it has a single operation: disable.
%%% @end

-module(riak_dt_disable_flag).

-behaviour(riak_dt).

-export([new/0, value/1, value/2, update/3, merge/2, equal/2, from_binary/1, to_binary/1, stats/1, stat/2]).
-export([update/4, parent_clock/2]).
-export([to_binary/2]).
-export([to_version/2]).
-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-export([gen_op/0, init_state/0, update_expected/3, eqc_state_value/1]).
-export([prop_crdt_converge/0]).
-endif.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include("riak_dt_tags.hrl").
-define(TAG, ?DT_DISABLE_FLAG_TAG).

-export_type([disable_flag/0]).
-opaque disable_flag() :: on | off.
-type disable_flag_op() :: disable.

-spec new() -> disable_flag().
new() ->
    on.

-spec value(disable_flag()) -> on | off.
value(Flag) ->
    Flag.

-spec value(term(), disable_flag()) -> on | off.
value(_, Flag) ->
    Flag.

-spec update(disable_flag_op(), riak_dt:actor(), disable_flag()) -> {ok, disable_flag()}.
update(disable, _Actor, _Flag) ->
    {ok, off}.

-spec update(disable_flag_op(), riak_dt:actor(), disable_flag(), riak_dt:context()) ->
                    {ok, disable_flag()}.
update(Op, Actor, Flag, _Ctx) ->
    update(Op, Actor, Flag).

-spec merge(disable_flag(), disable_flag()) -> disable_flag().
merge(FA, FB) ->
    flag_and(FA, FB).

-spec equal(disable_flag(), disable_flag()) -> boolean().
equal(FA,FB) ->
    FA =:= FB.

-spec parent_clock(riak_dt_vclock:vclock(), disable_flag()) ->
                          disable_flag().
parent_clock(_Clock, Flag) ->
    Flag.

-spec from_binary(binary()) -> {ok, disable_flag()} | ?INVALID_BINARY | ?UNSUPPORTED_VERSION.
from_binary(<<?TAG:7, 0:1>>) -> {ok, off};
from_binary(<<?TAG:7, 1:1>>) -> {ok, on};
from_binary(_Bin) -> ?INVALID_BINARY.

-spec to_binary(disable_flag()) -> binary().
to_binary(off) -> <<?TAG:7, 0:1>>;
to_binary(on) -> <<?TAG:7, 1:1>>.

-spec to_binary(Vers :: pos_integer(), disable_flag()) ->
                       {ok, binary()} | ?UNSUPPORTED_VERSION.
to_binary(1, Flag) ->
    B = to_binary(Flag),
    {ok, B};
to_binary(Vers, _Flag) ->
    ?UNSUPPORTED_VERSION(Vers).

-spec stats(disable_flag()) -> [{atom(), number()}].
stats(_) -> [].

-spec stat(atom(), disable_flag()) -> number() | undefined.
stat(_, _) -> undefined.

-spec to_version(pos_integer(), disable_flag()) -> disable_flag().
to_version(_Version, Flag) ->
    Flag.


%% priv
flag_and(on, on) ->
    on;
flag_and(off, _) ->
    off;
flag_and(_, off) ->
    off.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

new_test() ->
    ?assertEqual(on, new()).

update_enable_test() ->
    F0 = new(),
    {ok, F1} = update(disable, 1, F0),
    ?assertEqual(off, F1).

update_enable_multi_test() ->
    F0 = new(),
    {ok, F1} = update(disable, 1, F0),
    {ok, F2} = update(disable, 1, F1),
    ?assertEqual(off, F2).

merge_offs_test() ->
    F0 = new(),
    ?assertEqual(on, merge(F0, F0)).

merge_on_left_test() ->
    F0 = new(),
    {ok, F1} = update(disable, 1, F0),
    ?assertEqual(off, merge(F1, F0)).

merge_on_right_test() ->
    F0 = new(),
    {ok, F1} = update(disable, 1, F0),
    ?assertEqual(off, merge(F0, F1)).

merge_on_both_test() ->
    F0 = new(),
    {ok, F1} = update(disable, 1, F0),
    ?assertEqual(off, merge(F1, F1)).

stat_test() ->
    F0 = new(),
    {ok, F1} = update(disable, 1, F0),
    ?assertEqual([], stats(F1)),
    ?assertEqual(undefined, stat(actor_count, F1)),
    ?assertEqual(undefined, stat(max_dot_length, F1)).
-endif.


%% EQC generator
-ifdef(EQC).

prop_crdt_converge() ->
    crdt_statem_eqc:prop_converge(?MODULE).

init_state() ->
    on.

gen_op() ->
    disable.

update_expected(_ID, disable, _Prev) ->
    off;
update_expected(_ID, _Op, Prev) ->
    Prev.

eqc_state_value(S) ->
    S.
-endif.
