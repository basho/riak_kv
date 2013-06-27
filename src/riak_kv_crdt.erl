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
         to_type/1, from_type/1]).

-include("riak_kv_wm_raw.hrl").
-include_lib("riak_kv_types.hrl").

-ifdef(TEST).
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
    {CRDTs, _NonCounterSiblings} = merge_object(RObj),
    crdt_value(Type, orddict:find(Type, CRDTs)).

%% @TODO in riak_dt change value to query allow query to take an
%% argument, (so as to query subfields of map, or set membership etc)
crdt_value(Type, error) ->
    Type:new();
crdt_value(Type, {ok, {_Meta, ?CRDT{mod=Type, value=Value}}}) ->
    Type:value(Value).

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

update_crdt(Dict, Actor, ?CRDT_OP{mod=Mod, op=Op}) ->
    InitialVal = Mod:update(Op, Actor, Mod:new()),
    orddict:update(Mod, fun({Meta, CRDT=?CRDT{value=Value}}) ->
                               {Meta, CRDT?CRDT{value=Mod:update(Op, Actor, Value)}} end,
                  {undefined, to_record(Mod, InitialVal)},
                  Dict).

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
to_record(?LWW_TYPE, Val) ->
    ?LWW_TYPE(Val);
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

%% @doc turn the string token into a
%% CRDT type
to_type("sets") ->
    ?SET_TYPE;
to_type("counters") ->
    ?COUNTER_TYPE;
to_type("lww") ->
    ?LWW_TYPE;
to_type("maps") ->
    ?MAP_TYPE;
to_type(_) ->
    undefined.

from_type(?SET_TYPE) ->
    "sets";
from_type(?COUNTER_TYPE) ->
    "counters";
from_type(?LWW_TYPE) ->
    "lww";
from_type(?MAP_TYPE) ->
    "maps".

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

%% roundtrip_bin_test() ->
%%     PN = riak_kv_pncounter:new(),
%%     PN1 = riak_kv_pncounter:update({increment, 2}, <<"a1">>, PN),
%%     PN2 = riak_kv_pncounter:update({decrement, 1000000000000000000000000}, douglas_Actor, PN1),
%%     PN3 = riak_kv_pncounter:update(increment, [{very, ["Complex"], <<"actor">>}, honest], PN2),
%%     PN4 = riak_kv_pncounter:update(decrement, "another_acotr", PN3),
%%     Bin = to_binary(PN4),
    %% ?assert(byte_size(Bin) < term_to_binary({riak_kv_pncounter, PN4})).

-endif.
