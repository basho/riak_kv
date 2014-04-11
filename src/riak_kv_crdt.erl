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
         supported/1, to_mod/1, from_mod/1]).
-export([to_binary/2, to_binary/1, from_binary/1]).
-export([log_merge_errors/4, meta/2, merge_value/2]).
%% MR helper funs
-export([value/1, counter_value/1, set_value/1, map_value/1]).

-include("riak_kv_wm_raw.hrl").
-include("riak_object.hrl").
-include_lib("riak_kv_types.hrl").

-ifdef(TEST).
-ifdef(EQC).
-compile(export_all).
-include_lib("eqc/include/eqc.hrl").
-endif.
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(TAG, 69).
-define(V1_VERS, 1).
-define(V2_VERS, 2).

-type crdts() :: [{DT_MOD::module(), crdt()}].
-type ro_content() :: {Meta::dict(), Value::binary()}.
-type ro_contents() :: [ro_content()].
-type precondition_error() :: {error, {precondition, {not_present, term()}}}.

%% @doc applies the given `Operation' to the merged value.  first
%% performs a merge (@see merge/1), and then applies the update.
%% NOTE: operation needs to be a ?CRDT_OP in case of siblings of different
%% types
-spec update(riak_object:riak_object(), riak_dt:actor(), riak_dt:operation()) ->
                    riak_object:riak_object() | precondition_error().
update(RObj, Actor, Operation) ->
    {CRDTs0, Siblings} = merge_object(RObj),
    case update_crdt(CRDTs0, Actor, Operation) of
        {error, _}=E ->
            E;
        CRDTs ->
            update_object(RObj, CRDTs, Siblings)
    end.

%% @doc Merge all sibling values that are CRDTs into a single value
%% for that CRDT type.  NOTE: handles sibling types. For example if
%% there are 5 siblings, 2 or which are riak_dt_pncounter, and 2 are
%% riak_dt_vvorset, and 1 user supplied opaque value, then the results
%% is a converge counter, a converged set, and the opaque sibling, a
%% total of 3 sibings. Hopefully with bucket types, sibling types will
%% NEVER occur.
-spec merge(riak_object:riak_object()) -> riak_object:riak_object().
merge(RObj) ->
    {CRDTs, Siblings} = merge_object(RObj),
    update_object(RObj, CRDTs, Siblings).

%% @doc for the given riak_object `RObj' and the provided `Type',
%% which must be a support riak_dt crdt module, returns an update
%% context, and user value. Performs a merge, then gets the CRDT end
%% user value.  @see merge/1
-spec value(riak_object:riak_object(), module()) -> {{binary(), riak_dt:value()}, [{atom(), atom(), number()}]}.
value(RObj, Type) ->
    {CRDTs, _NonCRDTSiblings} = merge_object(RObj),
    DType = orddict:find(Type, CRDTs),
    {crdt_value(Type, DType), crdt_stats(Type, DType)}.

%% @doc convenience function for (e.g.) MapReduce. Attempt to get a
%% CRDT value for a given object. Checks the bucket props for the
%% object, if it has a datatype entry, uses that to get value. Returns
%% either a tuple of `{Type, Value}' or `undefined' if not a 2.0 CRDT.
-spec value(riak_object:riak_object()) -> {atom(), term()} | undefined.
value(RObj) ->
    Bucket = riak_object:bucket(RObj),
    case riak_core_bucket:get_bucket(Bucket) of
        BProps when is_list(BProps) ->
            Type = proplists:get_value(datatype, BProps),
            Mod = riak_kv_crdt:to_mod(Type),
            case supported(Mod) of
                true ->
                    {{_Ctx, V}, _Stats} = value(RObj, Mod),
                    {Type, V};
                false ->
                    undefined
            end
    end.

%% @doc convenience for (e.g.) MapReduce functions. Pass an object,
%% get a 2.0+ counter type value, or zero if no counter is present.
-spec counter_value(riak_object:riak_object()) -> integer().
counter_value(RObj) ->
    {{_Ctx, Count}, _Stats}  = value(RObj, ?COUNTER_TYPE),
    Count.

%% @doc convenience for (e.g.) MapReduce functions. Pass an object,
%% get a 2.0+ Set type value, or `[]' if no Set is present.
-spec set_value(riak_object:riak_object()) -> list().
set_value(RObj) ->
    {{_Ctx, Set}, _Stats}  = value(RObj, ?SET_TYPE),
    Set.

%% @doc convenience for (e.g.) MapReduce functions. Pass an object,
%% get a 2.0+ Map type value, or `[]' if no Map is present.
-spec map_value(riak_object:riak_object()) -> proplist:proplist().
map_value(RObj) ->
    {{_Ctx, Map}, _Stats}  = value(RObj, ?MAP_TYPE),
    Map.

%% @TODO in riak_dt change value to query allow query to take an
%% argument, (so as to query subfields of map, or set membership etc)
-spec crdt_value(module(), error | {ok, {dict(), crdt()}}) ->
                        {binary(), riak_dt:value()}.
crdt_value(Type, error) ->
    {<<>>, Type:value(Type:new())};
crdt_value(Type, {ok, {_Meta, ?CRDT{mod=Type, value=Value}}}) ->
    {get_context(Type, Value), Type:value(Value)}.

crdt_stats(_, error) -> [];
crdt_stats(Type, {ok, {_Meta, ?CRDT{mod=Type, value=Value}}}) ->
    case lists:member({stat,2}, Type:module_info(exports)) of
        true ->
            EnabledStats = app_helper:get_env(riak_kv, datatype_stats, ?DATATYPE_STATS_DEFAULTS),
            lists:foldr(fun(S, Acc) ->
                                case Type:stat(S, Value) of
                                    undefined -> Acc;
                                    Stat -> [{from_mod(Type), S, Stat}|Acc]
                                end
                        end, [], EnabledStats);
        false -> []
    end.


%% @private Merge contents _AND_ meta
-spec merge_object(riak_object:riak_object()) ->
                          {crdts(), list()}.
merge_object(RObj) ->
    Contents = riak_object:get_contents(RObj),
    {CRDTs, NonCRDTSiblings, Errors} = merge_contents(Contents),
    Bucket = riak_object:bucket(RObj),
    Key = riak_object:key(RObj),
    log_errors(Bucket, Key, Errors),
    maybe_log_sibling_crdts(Bucket, Key, CRDTs),
    {CRDTs, NonCRDTSiblings}.

%% @doc log any accumulated merge errors
-spec log_merge_errors(riak_object:bucket(), riak_object:key(), crdts(), list()) -> ok.
log_merge_errors(Bucket, Key, CRDTs, Errors) ->
    log_errors(Bucket, Key, Errors),
    maybe_log_sibling_crdts(Bucket, Key, CRDTs).

log_errors(_, _, []) ->
    ok;
log_errors(Bucket, Key, Errors) ->
    lager:error("Error(s) deserializing CRDT at ~p ~p: ~p~n", [Bucket, Key, Errors]).

maybe_log_sibling_crdts(Bucket, Key, CRDTs) when length(CRDTs) > 1 ->
    lager:error("Sibling CRDTs at ~p ~p: ~p~n", [Bucket, Key, orddict:fetch_keys(CRDTs)]);
maybe_log_sibling_crdts(_, _, _) ->
    ok.

%% @private Only merge the values of CRDTs If there are siblings that
%% are CRDTs BUT NOT THE SAME TYPE (don't do that!!)  Merge
%% type-to-type and store a single sibling per-type If a non-CRDT data
%% are present, keep them as sibling values
-spec merge_contents(ro_contents()) ->
    {crdts(), ro_contents(), Errors::list()}.
merge_contents(Contents) ->
    lists:foldl(fun merge_value/2,
                {orddict:new(), [], []},
               Contents).

%% @doc if the content is a CRDT, de-binary it, merge it and store the
%% most merged value in the accumulator dictionary.
-spec merge_value(ro_content(), {crdts(), ro_contents(), Errors::list()}) ->
    {crdts(), ro_contents(), Errors::list()}.
merge_value({MD, <<?TAG:8/integer, Version:8/integer, CRDTBin/binary>>=Content},
            {Dict, NonCRDTSiblings, Errors}) ->
    case deserialize_crdt(Version, CRDTBin) of
        {ok, CRDT=?CRDT{mod=Mod, value=Val, ctype=CType}} ->
            D2 = orddict:update(Mod, fun({Meta, Mergedest=?CRDT{value=Value}}) ->
                                             NewMeta = merge_meta(CType, Meta, MD),
                                             NewVal = Mod:merge(Value, Val),
                                             {NewMeta, Mergedest?CRDT{value = NewVal}}
                                     end,
                                {MD, CRDT}, Dict),
            {D2, NonCRDTSiblings, Errors};
        {error, Error} ->
            {Dict, [{MD, Content} | NonCRDTSiblings], [Error | Errors]}
    end;
merge_value(NonCRDT, {Dict, NonCRDTSiblings, Errors}) ->
    {Dict, [NonCRDT | NonCRDTSiblings], Errors}.

deserialize_crdt(?V1_VERS, CounterBin) ->
    v1_counter_from_binary(CounterBin);
deserialize_crdt(?V2_VERS, CRDTBin) ->
    crdt_from_binary(CRDTBin);
deserialize_crdt(V, _Bin) ->
    {error, {invalid_version, V}}.

counter_op(N) when N < 0 ->
    {decrement, -N};
counter_op(N) ->
    {increment, N}.

%% @private Apply the updates to the CRDT. If there is no context for
%% the operation then apply the operation to the local merged replica,
%% and risk precondition errors and unexpected behaviour.
%%
%% @see split_ops/1 for more explanation
-spec update_crdt(orddict:orddict(), riak_dt:actor(), crdt_op() | non_neg_integer()) ->
                         orddict:orddict() | precondition_error().
update_crdt(Dict, Actor, Amt) when is_integer(Amt) ->
    %% Handle legacy 1.4 counter operation, upgrade to current OP
    CounterOp = counter_op(Amt),
    Op = ?CRDT_OP{mod=?V1_COUNTER_TYPE, op=CounterOp},
    update_crdt(Dict, Actor, Op);
update_crdt(Dict, Actor, ?CRDT_OP{mod=Mod, op=Op, ctx=undefined}) ->
    {Meta, Record, Value} = fetch_with_default(Mod, Dict),
    case Mod:update(Op, Actor, Value) of
        {ok, NewVal} ->
            orddict:store(Mod, {Meta, Record?CRDT{value=NewVal}}, Dict);
        {error, _}=E -> E
    end;
update_crdt(Dict, Actor, ?CRDT_OP{mod=Mod, op=Ops, ctx=OpCtx}) when Mod==?MAP_TYPE;
                                                                    Mod==?SET_TYPE->
    case orddict:find(Mod, Dict) of
        error ->
            %% No local replica of this CRDT, apply the ops to a new
            %% instance
            case update_crdt(Mod, Ops, Actor, Mod:new(), OpCtx) of
                {ok, InitialVal} ->
                    orddict:store(Mod, {undefined, to_record(Mod, InitialVal)}, Dict);
                E ->
                    E
            end;
        {ok, {Meta, LocalCRDT=?CRDT{value=LocalReplica}}} ->
            case update_crdt(Mod, Ops, Actor, LocalReplica, OpCtx) of
                {error, _}=E -> E;
                {ok, NewVal} ->
                    orddict:store(Mod, {Meta, LocalCRDT?CRDT{value=NewVal}}, Dict)
            end
    end.

%% @private call update/3 or update/4 depending on context value
-spec update_crdt(module(), term(), riak_dt:actor(), term(),
                  undefined | riak_dt_vclock:vclock()) ->
                         term().
update_crdt(Mod, Ops, Actor, CRDT, undefined) ->
    Mod:update(Ops, Actor, CRDT);
update_crdt(Mod, Ops, Actor, CRDT, Ctx0) ->
    Ctx = get_context(Ctx0),
    Mod:update(Ops, Actor, CRDT, Ctx).

-spec get_context(undefined | binary()) -> riak_dt_vclock:vclock().
get_context(undefined) ->
    undefined;
get_context(Bin) ->
    riak_dt_vclock:from_binary(Bin).

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
    drop_the_dot(dict:store(?MD_CTYPE, CType, Meta)).

%% @private Never keep a dot for CRDTs, we want all values to survive
%% a riak_obect:merge/2
drop_the_dot(Dict) ->
    dict:erase(?DOT, Dict).

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

%% @doc turn a `crdt()' record into a binary for storage on disk /
%% passing on the network
-spec to_binary(crdt()) -> binary().
to_binary(CRDT=?CRDT{mod=?V1_COUNTER_TYPE}) ->
    to_binary(CRDT, ?V1_VERS);
to_binary(?CRDT{mod=Mod, value=Value}) ->
    CRDTBin = Mod:to_binary(Value),
    Type = atom_to_binary(Mod, latin1),
    TypeLen = byte_size(Type),
    <<?TAG:8/integer, ?V2_VERS:8/integer, TypeLen:32/integer, Type:TypeLen/binary, CRDTBin/binary>>.

%% @doc turn a `crdt()' record into a `Version' binary for storage on
%% disk / passing on the network
-spec to_binary(crdt(), Version::pos_integer()) -> binary().
to_binary(CRDT, ?V2_VERS) ->
    to_binary(CRDT);
to_binary(?CRDT{mod=?V1_COUNTER_TYPE, value=Value}, ?V1_VERS) ->
    CounterBin = ?V1_COUNTER_TYPE:to_binary(Value),
    <<?TAG:8/integer, ?V1_VERS:8/integer, CounterBin/binary>>.

%% @doc deserialize a crdt from it's binary format.  The binary must
%% start with the riak_kv_crdt tag and a version If the binary can be
%% deserailised into a `crdt()' returns `{ok, crdt()}', otherwise
%% `{error, term()}'
-spec from_binary(binary()) -> {ok, crdt()} | {error, term()}.
from_binary(<<?TAG:8/integer, ?V1_VERS:8/integer, CounterBin/binary>>) ->
    v1_counter_from_binary(CounterBin);
from_binary(<<?TAG:8/integer, ?V2_VERS:8/integer, CRDTBin/binary>>) ->
    crdt_from_binary(CRDTBin);
from_binary(Bin) ->
    {error, {invalid_binary, Bin}}.

%% @private attempt to deserialize a v1 counter (riak 1.4.x counter)
v1_counter_from_binary(CounterBin) ->
    try
        to_record(?V1_COUNTER_TYPE, ?V1_COUNTER_TYPE:from_binary(CounterBin)) of
        ?CRDT{}=Counter ->
            {ok, Counter}
    catch
        Class:Err ->
            {error, {Class, Err}}
        end.

%% @private attempt to deserialize a v2 CRDT.
crdt_from_binary(<<TypeLen:32/integer, Type:TypeLen/binary, CRDTBin/binary>>) ->
    try
        Mod = binary_to_existing_atom(Type, latin1),
        Val = Mod:from_binary(CRDTBin),
        to_record(Mod, Val) of
        ?CRDT{}=CRDT ->
            {ok, CRDT}
    catch
        Class:Err ->
            {error, {Class, Err}}
    end;
crdt_from_binary(_) ->
    {error, {invalid_crdt_binary}}.

to_record(?V1_COUNTER_TYPE, Val) ->
    ?V1_COUNTER_TYPE(Val);
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

%% @doc turn a string token / atom into a
%% CRDT type
to_mod("sets") ->
    ?SET_TYPE;
to_mod("counters") ->
    ?COUNTER_TYPE;
to_mod("maps") ->
    ?MAP_TYPE;
to_mod(?CRDT{mod=Mod}) ->
    Mod;
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
        true -> riak_dt_vclock:to_binary(Type:precondition_context(Value));
        false -> <<>>
    end.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).


-ifdef(EQC).
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) ->
                              io:format(user, Str, Args) end, P)).

-define(TEST_TIME_SECONDS, 10).


-endif.
-endif.
