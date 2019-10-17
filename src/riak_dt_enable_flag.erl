%% -------------------------------------------------------------------
%%
%% riak_dt_enable_flag: A state based, enable-only flag.
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
%%% an ON/OFF (boolean) CRDT, which converges towards ON
%%% it has a single operation: enable.
%%% @end

-module(riak_dt_enable_flag).

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
-define(TAG, ?DT_ENABLE_FLAG_TAG).

-export_type([enable_flag/0]).
-opaque enable_flag() :: on | off.
-type enable_flag_op() :: enable.

-spec new() -> enable_flag().
new() ->
    off.

-spec value(enable_flag()) -> on | off.
value(Flag) ->
    Flag.

-spec value(term(), enable_flag()) -> on | off.
value(_, Flag) ->
    Flag.

-spec update(enable_flag_op(), riak_dt:actor(), enable_flag()) -> {ok, enable_flag()}.
update(enable, _Actor, _Flag) ->
    {ok, on}.

-spec update(enable_flag_op(), riak_dt:actor(), enable_flag(), riak_dt:context()) ->
                    {ok, enable_flag()}.
update(enable, _Actor, _Flag, _Ctx) ->
    {ok, on}.

-spec parent_clock(riak_dt_vclock:vclock(), enable_flag()) -> enable_flag().
parent_clock(_Clock, Flag) ->
    Flag.

-spec merge(enable_flag(), enable_flag()) -> enable_flag().
merge(FA, FB) ->
    flag_or(FA, FB).

-spec equal(enable_flag(), enable_flag()) -> boolean().
equal(FA,FB) ->
    FA =:= FB.

-spec from_binary(binary()) -> {ok, enable_flag()} | ?INVALID_BINARY.
from_binary(<<?TAG:7, 0:1>>) -> {ok, off};
from_binary(<<?TAG:7, 1:1>>) -> {ok, on};
from_binary(_B) -> ?INVALID_BINARY.

-spec to_binary(enable_flag()) -> binary().
to_binary(off) -> <<?TAG:7, 0:1>>;
to_binary(on) -> <<?TAG:7, 1:1>>.

-spec to_binary(Vers :: pos_integer(), enable_flag()) ->
                       {ok, binary()} | ?UNSUPPORTED_VERSION.
to_binary(1, Flag) ->
    {ok, to_binary(Flag)};
to_binary(Vers, _Flag) ->
    ?UNSUPPORTED_VERSION(Vers).

-spec stats(enable_flag()) -> [{atom(), number()}].
stats(_) -> [].

-spec stat(atom(), enable_flag()) -> number() | undefined.
stat(_, _) ->
    undefined.

-spec to_version(pos_integer(), enable_flag()) -> enable_flag().
to_version(_Version, Flag) ->
    Flag.

%% priv
flag_or(on, _) ->
    on;
flag_or(_, on) ->
    on;
flag_or(off, off) ->
    off.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

-ifdef(EQC).
prop_crdt_converge() ->
    crdt_statem_eqc:prop_converge(?MODULE).

%% EQC generator
init_state() ->
    off.

gen_op() ->
    enable.

update_expected(_ID, enable, _Prev) ->
    on;
update_expected(_ID, _Op, Prev) ->
    Prev.

eqc_state_value(S) ->
    S.
-endif.

new_test() ->
    ?assertEqual(off, new()).

update_enable_test() ->
    F0 = new(),
    {ok, F1} = update(enable, 1, F0),
    ?assertEqual(on, F1).

update_enable_multi_test() ->
    F0 = new(),
    {ok, F1} = update(enable, 1, F0),
    {ok, F2} = update(enable, 1, F1),
    ?assertEqual(on, F2).

merge_offs_test() ->
    F0 = new(),
    ?assertEqual(off, merge(F0, F0)).

merge_on_left_test() ->
    F0 = new(),
    {ok, F1} = update(enable, 1, F0),
    ?assertEqual(on, merge(F1, F0)).

merge_on_right_test() ->
    F0 = new(),
    {ok, F1} = update(enable, 1, F0),
    ?assertEqual(on, merge(F0, F1)).

merge_on_both_test() ->
    F0 = new(),
    {ok, F1} = update(enable, 1, F0),
    ?assertEqual(on, merge(F1, F1)).

stat_test() ->
    F0 = new(),
    {ok, F1} = update(enable, 1, F0),
    ?assertEqual([], stats(F1)),
    ?assertEqual(undefined, stat(actor_count, F1)),
    ?assertEqual(undefined, stat(max_dot_length, F1)).
-endif.
