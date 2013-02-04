%% -------------------------------------------------------------------
%%
%% riak_kv_counter: Counter logic to bridge riak_object and riak_kv_pncounter
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
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
-module(riak_kv_counter).

-export([update/4, merge/2, value/1]).

-include("riak_kv_wm_raw.hrl").

%% A counter is a two tuple of a `riak_kv_pncounter' stored in a `riak_object'
%% with the tag `riak_kv_pncounter' as the first element.
%% Since counters can be stored with any name, in any bucket, there is a
%% chance that some sibing value for a counter is
%% not a `riak_kv_pncounter' in that case, we keep the sibling
%% for later resolution by the user.
-spec update(riak_object:riak_object(), term(), binary(), integer()) ->
                    riak_object:riak_object().
update(RObj, IndexSpecs, Actor, Amt) ->
    Values = riak_object:get_contents(RObj),
    {Counter0, NonCounterSiblings} = merge_values(Values, riak_kv_pncounter:new(), []),
    Meta = merged_meta(IndexSpecs),
    Counter = update_counter(Counter0, Actor, Amt),
    update_object(RObj, Meta, Counter, NonCounterSiblings).

merge(RObj, IndexSpecs) ->
    Values = riak_object:get_contents(RObj),
    {Counter, NonCounterSiblings} = merge_values(Values, riak_kv_pncounter:new(), []),
    Meta = merged_meta(IndexSpecs),
    update_object(RObj, Meta, Counter, NonCounterSiblings).

merge_values([], Mergedest, NonCounterSiblings) ->
    {Mergedest, NonCounterSiblings};
merge_values([{_MD, {riak_kv_pncounter, Value}} | Rest], Mergedest, NonCounterSiblings) ->
    merge_values(Rest, do_merge(Value, Mergedest), NonCounterSiblings);
merge_values([NotACounter|Rest], Mergedest, NonCounterSiblings) ->
    merge_values(Rest, Mergedest, [NotACounter | NonCounterSiblings]).

do_merge(C1, C2) ->
    riak_kv_pncounter:merge(C1, C2).

%% Only indexes are allowed in counter
%% meta data.
%% The job of merging index meta data has
%% already been done to get the indexspecs
%% therefore create a meta that is
%% only the index meta data we already know about
merged_meta(IndexSpecs) ->
    Indexes = [{Index, Value} || {Op, Index, Value} <- IndexSpecs,
                                 Op =:= add],
    dict:store(?MD_INDEX, Indexes, dict:new()).

update_counter(Counter, Actor, Amt) ->
    Op = counter_op(Amt),
    riak_kv_pncounter:update(Op, Actor, Counter).

counter_op(Amt) when Amt < 0 ->
    {decrement, Amt * -1};
counter_op(Amt) ->
    {increment, Amt}.

update_object(RObj, Meta, Counter, []) ->
    RObj2 = riak_object:update_value(RObj, {riak_kv_pncounter, Counter}),
    RObj3 = riak_object:update_metadata(RObj2, Meta),
    riak_object:apply_updates(RObj3);
update_object(RObj, Meta, Counter, SiblingValues) ->
    %% keep non-counter siblings, too
    riak_object:set_contents(RObj, [{Meta, {riak_kv_pncounter, Counter}} | SiblingValues]).

value(RObj) ->
    Contents = riak_object:get_contents(RObj),
    {Counter, _NonCounterSiblings} = merge_values(Contents, riak_kv_pncounter:new(), []),
    riak_kv_pncounter:value(Counter).
