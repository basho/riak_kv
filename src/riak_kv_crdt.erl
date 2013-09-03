%% -------------------------------------------------------------------
%%
%% riak_kv_crdt: A general purpose bridge between a CRDT and riak_object
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

-module(riak_kv_crdt).

-export([update/3, merge/1, value/2, new/3,
         to_binary/1, from_binary/1, supported/1, parse_operation/2,
         to_mod/1, from_mod/1]).

-export([split_ops/1]).

-include("riak_kv_wm_raw.hrl").
-include_lib("riak_kv_types.hrl").

-ifdef(TEST).
-ifdef(EQC).
-export([test_split/0, test_split/1]).

-include_lib("eqc/include/eqc.hrl").
-endif.
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(TAG, 69).
-define(V1_VERS, 1).

%% NOTE operation needs to be a ?CRDT_OP
%% in case of siblings of different types
update(RObj, Actor, Operation) ->
    {CRDTs0, Siblings} = merge_object(RObj),
    CRDTs = update_crdt(CRDTs0, Actor, Operation),
    update_object(RObj, CRDTs, Siblings).

merge(RObj) ->
    {CRDTs, Siblings} = merge_object(RObj),
    update_object(RObj, CRDTs, Siblings).

value(RObj, Type) ->
    {CRDTs, _NonCRDTSiblings} = merge_object(RObj),
    crdt_value(Type, orddict:find(Type, CRDTs)).

%% @TODO in riak_dt change value to query allow query to take an
%% argument, (so as to query subfields of map, or set membership etc)
crdt_value(Type, error) ->
    {<<>>, Type:new()};
crdt_value(Type, {ok, {_Meta, ?CRDT{mod=Type, value=Value}}}) ->
    {get_context(Type, Value), Type:value(Value)}.

%% Merge contents _AND_ meta
merge_object(RObj) ->
    Contents = riak_object:get_contents(RObj),
    merge_contents(Contents).

%% Only merge the values of CRDTs
%% If there are siblings that are CRDTs
%% BUT NOT THE SAME TYPE (don't do that!!)
%% Merge type-to-type and store a single sibling per-type
%% If a non-CRDT datum is present, keep it as a sibling value
%% The accumulator is a dict of type -> {meta, value}
%% where meta and value are the most merged
merge_contents(Contents) ->
    lists:foldl(fun merge_value/2,
                {orddict:new(), []},
               Contents).

%% worker for `merge_contents/1'
merge_value({MD, <<?TAG:8/integer, ?V1_VERS:8/integer, CRDTBin/binary>>},
            {Dict, NonCounterSiblings}) ->
    CRDT=?CRDT{mod=Mod, value=Val, ctype=CType} = crdt_from_binary(CRDTBin),
    D2 = orddict:update(Mod, fun({Meta, Mergedest=?CRDT{value=Value}}) ->
                                     NewMeta = merge_meta(CType, Meta, MD),
                                     NewVal = Mod:merge(Value, Val),
                                     {NewMeta, Mergedest?CRDT{value = NewVal}}
                             end,
                        {MD, CRDT}, Dict),
    {D2, NonCounterSiblings};
merge_value(NonCRDT, {Dict, NonCRDTSiblings}) ->
    {Dict, [NonCRDT | NonCRDTSiblings]}.

%% @doc Apply the updates to the CRDT.
update_crdt(Dict, Actor, ?CRDT_OP{mod=Mod, op=Op, ctx=undefined}) ->
    {Meta, Record, Value} = fetch_with_default(Mod, Dict),
    {ok, NewVal} = Mod:update(Op, Actor, Value),
    orddict:store(Mod, {Meta, Record?CRDT{value=NewVal}}, Dict);
update_crdt(Dict, Actor, ?CRDT_OP{mod=?MAP_TYPE=Mod, op=Ops, ctx=OpCtx}) ->
    Ctx = context_to_crdt(Mod, OpCtx),
    {PreOps, PostOps} = split_ops(Ops),
    {ok, InitialVal} = Mod:update(PreOps, Actor, Ctx),
    orddict:update(Mod, fun({Meta, CRDT=?CRDT{value=Value}}) ->
                                Merged = Mod:merge(InitialVal, Value),
                                NewValue = Mod:update(PostOps, Actor, Merged),
                                {Meta, CRDT?CRDT{value=NewValue}} end,
                   {undefined, to_record(Mod, InitialVal)},
                   Dict);
update_crdt(Dict, Actor, ?CRDT_OP{mod=?SET_TYPE=Mod, op={RemOp, _}=Op, ctx=OpCtx}) when RemOp == remove; RemOp == remove_all ->
    Ctx = context_to_crdt(Mod, OpCtx),
    {ok, InitialVal} = Mod:update(Op, Actor, Ctx),
    orddict:update(Mod, fun({Meta, CRDT=?CRDT{value=Value}}) ->
                                {Meta, CRDT?CRDT{value=Mod:merge(InitialVal, Value)}} end,
                   {undefined, to_record(Mod, InitialVal)},
                   Dict).

context_to_crdt(Mod, undefined) ->
    Mod:new();
context_to_crdt(Mod, Ctx) ->
    Mod:from_binary(Ctx).

%% @doc get the merged CRDT for type `Mod' from the dictionary. If it
%% is not present generate a default entry
fetch_with_default(Mod, Dict) ->
    case orddict:find(Mod, Dict) of
        error ->
            Value = Mod:new(),
            {undefined, to_record(Mod, Value), Value};
        {ok, {Meta, Record=?CRDT{value=Value}}} ->
            {Meta, Record, Value}
    end.

%% @private Takes an update operation and splits it into two update
%% operations.  returns {pre, post} where pre is an update operation
%% that must be a applied to a context _before_ it is merged with
%% local replica state and post is an update operation that must be
%% applied after the context has been merged with the local replica
%% state.
%%
%% Why?  When fields are removed from Maps or elements from Sets the
%% remove must be applied to the state observed by the client.  If a
%% client as seen a field or element in a set, but the replica
%% handling the remove has not then a confusing preconditione failed
%% error will be generated for the user.
%%
%% The reason for applying remove type operations to the context
%% before the merge is to ensure that only the adds seen by the client
%% are removed, and not adds that happen to be present at this
%% replica.
%%
%% @TODO optimise the case where either Pre or Post comes back
%% essentially as a No Op (for example, and update to a nested Map
%% that has no operations. While this does not affect correctness, it
%% is not ideal.
split_ops(Ops) when is_list(Ops) ->
    split_ops(Ops, [], []);
split_ops({update, Ops}) ->
    {Pre, Post} = split_ops(Ops),
    {{update, Pre}, {update, Post}}.

split_ops([], Pre, Post) ->
    {lists:reverse(Pre), lists:reverse(Post)};
split_ops([{remove, _Key}=Op | Rest], Pre, Post) ->
    split_ops(Rest, [Op | Pre], Post);
split_ops([{update, _Key, {remove, _}}=Op | Rest], Pre, Post) -> %% BAD! Knows about the set remove operation
    split_ops(Rest, [Op | Pre], Post);
split_ops([{update, {_Name, ?MAP_TYPE}=Key, Op} | Rest], Pre0, Post0) ->
    {Pre, Post} = split_ops(Op),
    split_ops(Rest, [{update, Key, Pre} | Pre0], [{update, Key, Post} | Post0]);
split_ops([Op | Rest], Pre, Post) ->
    split_ops(Rest, Pre, [Op | Post]).

%% This uses an exported but marked INTERNAL
%% function of `riak_object:set_contents' to preserve
%% non-crdt sibling values and Metadata
%% NOTE: if `Meta' is `undefined' then this
%% is a new crdt.
update_object(RObj, CRDTs, SiblingValues) ->
    %% keep non-counter siblings, too
    CRDTSiblings = [{meta(Meta, CRDT), to_binary(CRDT)} || {_Mod, {Meta, CRDT}} <- orddict:to_list(CRDTs)],
    riak_object:set_contents(RObj, CRDTSiblings ++ SiblingValues).

meta(undefined, ?CRDT{ctype=CType}) ->
    Now = os:timestamp(),
    M = dict:new(),
    M2 = dict:store(?MD_LASTMOD, Now, M),
    M3 = dict:store(?MD_VTAG, riak_kv_util:make_vtag(Now), M2),
    dict:store(?MD_CTYPE, CType, M3);
meta(Meta, _CRDT) ->
    Meta.

%% Just a simple take the largest for meta values based on last mod
merge_meta(CType, Meta1, Meta2) ->
    Meta = case later(lastmod(Meta1), lastmod(Meta2)) of
        true ->
            Meta1;
        false ->
            Meta2
           end,
    %% Make sure the content type is
    %% up-to-date
    dict:store(?MD_CTYPE, CType, Meta).

lastmod(Meta) ->
    dict:fetch(?MD_LASTMOD, Meta).

later(TS1, TS2) ->
    case timer:now_diff(TS1, TS2) of
        Before when Before < 0 ->
            false;
        _ ->
            true
    end.

new(B, K, Mod) ->
    CRDT=#crdt{ctype=CType} = to_record(Mod, Mod:new()),
    Bin = to_binary(CRDT),
    Doc0 = riak_object:new(B, K, Bin, CType),
    riak_object:set_vclock(Doc0, vclock:fresh()).

to_binary(?CRDT{mod=Mod, value=Value}) ->
    CRDTBin = Mod:to_binary(Value),
    Type = atom_to_binary(Mod, latin1),
    TypeLen = byte_size(Type),
    <<?TAG:8/integer, ?V1_VERS:8/integer, TypeLen:32/integer, Type:TypeLen/binary, CRDTBin/binary>>.

from_binary(<<?TAG:8/integer,?V1_VERS:8/integer, CRDTBin/binary>>) ->
    crdt_from_binary(CRDTBin).

crdt_from_binary(<<TypeLen:32/integer, Type:TypeLen/binary, CRDTBin/binary>>) ->
    Mod = binary_to_existing_atom(Type, latin1),
    Val = Mod:from_binary(CRDTBin),
    to_record(Mod, Val).

to_record(?COUNTER_TYPE, Val) ->
    ?COUNTER_TYPE(Val);
to_record(?MAP_TYPE, Val) ->
    ?MAP_TYPE(Val);
to_record(?SET_TYPE, Val) ->
    ?SET_TYPE(Val).

%% @doc Check cluster capability for crdt support
%% @TODO what does this mean for Maps?
supported(Mod) ->
    lists:member(Mod, riak_core_capability:get({riak_kv, crdt}, [])).

%% @doc parse the operation from a binary
%% Assume type is validated and supported
%% This is where all the magic neds to be @TODO'd
parse_operation(CRDTOp, OpBin) ->
    try
        {ok, Tokens, _} = erl_scan:string(binary_to_list(OpBin)),
        {ok, Ops} = erl_parse:parse_term(Tokens),
        {ok, CRDTOp?CRDT_OP{op=Ops}}
    catch Class:Reason ->
            {error, {Class, Reason}}
    end.

%% @doc turn a string token / atom into a
%% CRDT type
to_mod("sets") ->
    ?SET_TYPE;
to_mod("counters") ->
    ?COUNTER_TYPE;
to_mod("maps") ->
    ?MAP_TYPE;
to_mod(Type) ->
    proplists:get_value(Type, ?MOD_MAP).

from_mod(Mod) ->
    case lists:keyfind(Mod, 2, ?MOD_MAP) of
        {Type, Mod} ->
            Type;
        false ->
            undefined
    end.

%% @Doc the update context can be empty for some types.
%% Those that support an precondition_context should supply
%% a smaller than Type:to_binary(Value) binary context.
get_context(Type, Value) ->
    case lists:member({precondition_context, 1}, Type:module_info(exports)) of
        true -> Type:precondition_context(Value);
        false -> <<>>
    end.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

-ifdef(EQC).

-define(NUM_TESTS, 1000).

eqc_test_() ->
    [?_assert(test_split() =:= true)].

test_split() ->
    test_split(?NUM_TESTS).

test_split(NumTests) ->
    eqc:quickcheck(eqc:numtests(NumTests, prop_split())).

prop_split() ->
    ?FORALL(Ops, riak_dt_multi:gen_op(),
            begin
                {Pre, Post} = split_ops(Ops),
                {update, AgOps} = Ops,
                ?WHENFAIL(
                   begin
                       io:format("Generated Ops ~p~n", [Ops]),
                       io:format("Split Pre ~p~n", [Pre]),
                       io:format("Split Post ~p~n", [Post])
                   end,
                   collect(length(AgOps),
                           collect(depth(AgOps, 0),
                                   conjunction([
                                                {pre, only_rem_ops(Pre)},
                                                {pre_depth, equals(depth(Pre, 0), depth(Ops, 0))},
                                                {post, only_add_ops(Post)},
                                                {post_depth, equals(depth(Post, 0), depth(Ops, 0))}
                                               ])))
                  )
            end).



depth({update, Ops}, Depth) ->
    depth(Ops, Depth);
depth([], Depth) ->
    Depth;
depth([{update, {_Name, ?MAP_TYPE}, {update, Ops}}| Rest], Depth) ->
    max(depth(Ops, Depth +1), depth(Rest, Depth));
depth([_Op | Rest], Depth) ->
    depth(Rest, Depth).

only_type_ops(_TestFun, []) ->
    true;
only_type_ops(TestFun, [Op | Rest]) ->
    case TestFun(Op) of
        false ->
            false;
        true ->
            only_type_ops(TestFun, Rest)
    end.

only_add_ops({update, Ops}) ->
    only_add_ops(Ops);
only_add_ops(Ops) ->
    only_type_ops(fun is_add_op/1, Ops).

only_rem_ops({update, Ops}) ->
    only_rem_ops(Ops);
only_rem_ops(Ops) ->
    only_type_ops(fun is_rem_op/1, Ops).

is_add_op({update, {_Name, ?MAP_TYPE}, {update, Ops}}) ->
    only_add_ops(Ops);
is_add_op(Op) ->
    not is_rem_op(Op).

is_rem_op({update, {_Name, ?MAP_TYPE}, {update, Ops}}) ->
    only_rem_ops(Ops);
is_rem_op({remove, _Key}) ->
    true;
is_rem_op({update, _Key, {remove, _Elem}}) ->
    true;
is_rem_op(_Op) ->
    false.

-endif.
-endif.
