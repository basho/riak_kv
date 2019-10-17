%% -------------------------------------------------------------------
%%
%% riak_dt_map: OR-Set schema based multi CRDT container
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

%% @doc a multi CRDT holder. A Struct/Document-ish thing. Uses the
%% same tombstone-less, Observed Remove semantics as `riak_dt_orswot'.
%% A Map is set of `Field's a `Field' is a two-tuple of:
%% `{Name::binary(), CRDTModule::module()}' where the second element
%% is the name of a crdt module that may be embedded. CRDTs stored
%% inside the Map will have their `update/3/4' function called, but the
%% second argument will be a `riak_dt:dot()', so that they share the
%% causal context of the map, even when fields are removed, and
%% subsequently re-added.
%%
%% The contents of the Map are modeled as a dictionary of
%% `field_name()' to `field_value()' mappings. Where `field_ name()'
%% is a two tuple of an opaque `binary()' name, and one of the
%% embeddable crdt types (currently `riak_dt_orswot',
%% `riak_dt_emcntr', `riak_dt_lwwreg', `riak_dt_od_flag', and
%% `riak_dt_map'). The reason for this limitation is that embedded
%% types must support embedding: that is a shared, `dot'-based, causal
%% context, and a reset-remove semantic (more on these below.)  The
%% `field_value()' is a two-tuple of `entries()' and a
%% `tombstone()'. The presence of a `tombstone()' in a "tombstoneless"
%% Map is confusing. The `tombstone()' is only stored for fields that
%% are currently in the map, removing a field also removes its
%% tombstone.
%%
%% To use the Map create a `new()' Map. When you call `update/3' or
%% `update/4' you pass a list of operations and an optional causal
%% context. @See `update/3' or `update/4' for more details. The list
%% of operations is applied atomically in full, and new state
%% returned, or not at all, and an error is returned.
%%
%% <h2>Semantics</h2>
%%
%% The semantics of this Map are Observed-Remove-Reset-Remove. What
%% this means is practice is, if a field is removed, and concurrently
%% that same field is updated, the field is _in_ the Map (only
%% observed updates are removed) but those removes propagate, so only
%% the concurrent update survives. A concrete example helps: If a Map
%% contains a field that is a set, and the set has 5 elements in it,
%% and concurrently the replica at A removes the field that contains
%% the set, while the replica at B adds an item to the set, on merge
%% there is a field for the set, but it contains only the one item B
%% added. The removal of the field is semantically equivalent to
%% removing all elements in the set, and removing the field. The same
%% goes for an embedded Map. If concurrently a Map field is removed,
%% while a new sub-field is updated, only the updated field(s) survive
%% the reset-remove.
%%
%% There is an anomaly for embedded counters that does not fully
%% support reset remove. Embedded counters (@see riak_dt_emcntr) are
%% different to a normal `pn-counter'. Embedded counters map `dot's to
%% {P, N} pairs. When a counter is incremented a new dot is created,
%% that replaces the old dot with the new value. `pn-counter' usually
%% merges by taking the `max' of any `P' or `N' entry for an
%% actor. This does not work in an embedded context. When a counter
%% field is removed, and then _re_-added, the new `P' and `N' entries
%% may be lower than the old, and merging loses the remove
%% information. However, if a `dot' is stored with the value, and the
%% max of the `dot' is used in merge, new updates win over removed
%% updates. So far so good. Here is the problem. If Replica B removes
%% a counter field, and does not re-add it, and replica A concurrently
%% updates it's entry for that field, then the reset-remove does not
%% occur. All new dots are not `observed' by Replica B, so not
%% removed. The new `dots' contain the updates from the previous
%% `dots', and the old `dot' is discarded. To achieve reset-remove all
%% increments would need a dot, and need to be retained, which would
%% be very costly in terms of space. One way to accept this anomaly is
%% to think of a Map like a file system: removing a directory and
%% concurrently adding a file means that the directory is present and
%% only the file remains in it. Updating a counter and concurrently
%% removing it, means the counter remains, with the updated value,
%% much like appending to a file in the file system analogy: you don't
%% expect only the diff to survive, but the whole updated file.
%%
%% <h2>Merging/Size</h2>
%%
%% When any pair of Maps are merged, the embedded CRDTs are _not_
%% merged, instead each concurrent `dot'->`field()' entry is
%% kept. This leads to a greater size for Maps that are highly
%% divergent. Updating a field in the map, however, leads to all
%% entries for that field being merged to a single CRDT that is stored
%% against the new `dot'. As mentioned above, there is also a
%% `tombstone' entry per present field. This is bottom CRDT for the
%% field type with a clock that contains all seen and removed
%% `dots'. There tombstones are merged at merge time, so only one is
%% present per field. Clearly the repetition of actor information (the
%% clock, each embedded CRDT, the field `dots', the tombstones) is a
%% serious issue with regard to size/bloat of this data type. We use
%% erlang's `to_binary/2' function, which compresses the data, to get
%% around this at present.
%%
%% <h2>Context and Deferred operations</h2>
%%
%% For CRDTs that use version vectors and dots (this `Map' and all
%% CRDTs that may be embedded in it), the size of the CRDT is
%% influenced by the number of actors updating it. In some systems
%% (like Riak!) we attempt to minimize the number of actors by only
%% having the database update CRDTs. This leads to a kind of "action
%% at a distance", where a client sends operations to the database,
%% and an actor in the database system performs the operations. The
%% purpose is to ship minimal state between database and client, and
%% to limit the number of actors in the system. There is a problem
%% with action at a distance and the OR semantic. The client _must_ be
%% able to tell the database what has been observed when it sends a
%% remove operation. There is a further problem. A replica that
%% handles an operation may not have all the state the client
%% observed. We solve these two problems by asking the client to
%% provide a causal context for operations (@see `update/4'.) Context
%% operations solve the OR problem, but they don't solve the problem
%% of lagging replicas handling operations.
%%
%% <h3>Lagging replicas, deferred operations</h3>
%%
%% In a system like Riak, a replica that is not up-to-date (including,
%% never seen any state for a CRDT) maybe asked to perform an
%% operation. If no context is given, and the operation is a field
%% remove, or a "remove" like operation on an embedded CRDT, the
%% operation may fail with a precondition error (for example, remove a
%% field that is not present) or succeed and remove more state than
%% intended (a field remove with no context may remove updates unseen
%% by the client.) When a context is provided, and the Field to be
%% removed is absent, the Map state stores the context, and Field
%% name, in a list of deferred operations. When, eventually, through
%% propagation and merging, the Map's clock descends the context for
%% the operation, the operation is executed. It is important to note
%% that _only_ actorless (field remove) operations can occur this way.
%%
%% <h4>Embedded CRDTs Deferred Operations</h4>
%%
%% There is a bug with embedded types and deferred operations. Imagine
%% a client has seen a Map with a Set field, and the set contains {a,
%% b, c}. The client sends an operation to remove {a} from the set. A
%% replica that is new takes the operation. It will create a new Map,
%% a Field for the Set, and store the `remove` operation as part of
%% the Set's state. A client reads this new state, and sends a field
%% remove operation, that is executed by same replica. Now the
%% deferred operation is lost, since the field is removed. We're
%% working on ways to fix this. One idea is to not remove a field with
%% "undelivered" operations, but instead to "hide" it.
%%
%% See {@link riak_dt_orswot} for more on the OR semantic
%%
%% See {@link riak_dt_emcntr} for the embedded counter.
%% @end

-module(riak_dt_map).

-behaviour(riak_dt).

-include("riak_dt.hrl").

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-endif.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([new/0, value/1, value/2, update/3, update/4]).
-export([merge/2, equal/2, to_binary/1, from_binary/1]).
-export([to_binary/2]).
-export([precondition_context/1, stats/1, stat/2]).
-export([parent_clock/2]).
-export([to_version/2]).

%% EQC API
-ifdef(EQC).
-export([gen_op/0, gen_op/1, gen_field/0, gen_field/1,  generate/0, size/1]).
-export([prop_v1_downgrade_roundtrip/0]).
-endif.

-export_type([riak_dt_map/0, binary_map/0, map_op/0]).

-type binary_map() :: binary(). %% A binary that from_binary/1 will accept
-type riak_dt_map() :: {riak_dt_vclock:vclock(), entries(), deferred()}.
-type ord_map() :: {riak_dt_vclock:vclock(), orddict:orddict(), orddict:orddict()}.
-type any_map() :: riak_dt_map() | ord_map().
-type entries() :: dict(field_name(), field_value()).
-type field_name() :: {Name :: binary(), CRDTModule :: crdt_mod()}.
-type field_value() :: {crdts(), tombstone()}.

-type crdts() :: [entry()].
-type entry() :: {riak_dt:dot(), crdt()}.

%% Only for present fields, ensures removes propogate
-type tombstone() :: crdt().

%% Only field removals can be deferred. CRDTs stored in the map may
%% have contexts and deferred operations, but as these are part of the
%% state, they are stored under the field as an update like any other.
-type deferred() :: dict(context(), [field_name()]).

-ifdef(namespaced_types).
-type dict(A, B) :: dict:dict(A, B).
-else.
-type dict(_A, _B) :: dict().
-endif.

%% limited to only those mods that support both a shared causal
%% context, and by extension, the reset-remove semantic.
-type crdt_mod() :: riak_dt_emcntr | riak_dt_lwwreg |
                    riak_dt_od_flag |
                    riak_dt_map | riak_dt_orswot.

-type crdt()  ::  riak_dt_emcntr:emcntr() | riak_dt_od_flag:od_flag() |
                  riak_dt_lwwreg:lwwreg() |
                  riak_dt_orswot:orswot() |
                  riak_dt_map:riak_dt_map().

-type map_op() :: {update, [map_field_update() | map_field_op()]}.

-type map_field_op() ::  {remove, field_name()}.
-type map_field_update() :: {update, field_name(), crdt_op()}.

-type crdt_op() :: riak_dt_emcntr:emcntr_op() |
                   riak_dt_lwwreg:lwwreg_op() |
                   riak_dt_orswot:orswot_op() | riak_dt_od_flag:od_flag_op() |
                   riak_dt_map:map_op().

-type context() :: riak_dt_vclock:vclock() | undefined.

-type values() :: [value()].
-type value() :: {field_name(), riak_dt_map:values() | integer() | [term()] | boolean() | term()}.
-type precondition_error() :: {error, {precondition, {not_present, field_name()}}}.

-define(DICT, dict).
-define(SET, sets).

%% @doc Create a new, empty Map.
-spec new() -> riak_dt_map().
new() ->
    {riak_dt_vclock:fresh(), ?DICT:new(), ?DICT:new()}.

%% @doc sets the clock in the map to that `Clock'. Used by a
%% containing Map for sub-CRDTs
-spec parent_clock(riak_dt_vclock:vclock(), riak_dt_map()) ->
    riak_dt_map().
parent_clock(Clock, Map) ->
    {_MapClock, Values, Deferred} = to_v2(Map),
    {Clock, Values, Deferred}.

%% @doc get the current set of values for this Map
-spec value(riak_dt_map()) -> values().
value({_C, V, _D}=Map) when is_list(V) ->
    value(to_v2(Map));
value({_Clock, Values, _Deferred}) ->
    lists:sort(?DICT:fold(fun({Name, Type}, CRDTs, Acc) ->
                                  Merged = merge_crdts(Type, CRDTs),
                                  [{{Name, Type}, Type:value(Merged)} | Acc] end,
                          [],
                          Values)).

%% @private merge entry for field, if present, or return new if not
merge_field({_Name, Type}, error) ->
    Type:new();
merge_field({_Name, Type}, {ok, CRDTs}) ->
    merge_crdts(Type, CRDTs);
merge_field(Field, Values) ->
    merge_field(Field, ?DICT:find(Field, Values)).

%% @private merge the CRDTs of a type
merge_crdts(Type, {CRDTs, TS}) ->
    V = ?DICT:fold(fun(_Dot, CRDT, CRDT0) ->
                             Type:merge(CRDT0, CRDT) end,
                     Type:new(),
                     CRDTs),
    %% Merge with the tombstone to drop any removed dots
    Type:merge(TS, V).

%% @doc query map (not implemented yet)
-spec value(term(), riak_dt_map()) -> values().
value(_, Map) ->
    value(Map).

%% @doc update the `riak_dt_map()' or a field in the `riak_dt_map()' by
%% executing the `map_op()'. `Ops' is a list of one or more of the
%% following ops:
%%
%% `{update, field_name(), Op} where `Op' is a valid update operation for a
%% CRDT of type `Mod' from the `Key' pair `{Name, Mod}' If there is no
%% local value for `Key' a new CRDT is created, the operation applied
%% and the result inserted otherwise, the operation is applied to the
%% local value.
%%
%%  `{remove, `field_name()'}' where field is `{name, type}', results in
%%  the crdt at `field' and the key and value being removed. A
%%  concurrent `update' will "win" over a remove so that the field is
%%  still present, and it's value will contain the concurrent update.
%%
%% Atomic, all of `Ops' are performed successfully, or none are.
-spec update(map_op(), riak_dt:actor() | riak_dt:dot(), riak_dt_map()) ->
                    {ok, riak_dt_map()} | precondition_error().
update(Op, ActorOrDot, {_C, V, _D}=Map) when is_list(V) ->
    update(Op, ActorOrDot, to_v2(Map));
update(Op, ActorOrDot, Map) ->
    update(Op, ActorOrDot, Map, undefined).

%% @doc the same as `update/3' except that the context ensures no
%% unseen field updates are removed, and removal of unseen updates is
%% deferred. The Context is passed down as the context for any nested
%% types. hence the common clock.
%%
%% @see parent_clock/2
-spec update(map_op(), riak_dt:actor() | riak_dt:dot(), riak_dt_map(),
             riak_dt:context()) -> {ok, riak_dt_map()}.
update(Op, ActorOrDot, {_C, V, _D}=Map, Ctx) when is_list(V) ->
    update(Op, ActorOrDot, to_v2(Map), Ctx);
update({update, Ops}, ActorOrDot, {Clock0, Values, Deferred}, Ctx) ->
    {Dot, Clock} = update_clock(ActorOrDot, Clock0),
    apply_ops(Ops, Dot, {Clock, Values, Deferred}, Ctx).

%% @private update the clock, and get a dot for the operations. This
%% means that field removals increment the clock too.
-spec update_clock(riak_dt:actor() | riak_dt:dot(),
                   riak_dt_vclock:vclock()) ->
                          {riak_dt:dot(), riak_dt_vclock:vclock()}.
update_clock(Dot, Clock) when is_tuple(Dot) ->
    NewClock = riak_dt_vclock:merge([[Dot], Clock]),
    {Dot, NewClock};
update_clock(Actor, Clock) ->
    NewClock = riak_dt_vclock:increment(Actor, Clock),
    Dot = {Actor, riak_dt_vclock:get_counter(Actor, NewClock)},
    {Dot, NewClock}.

%% @private
-spec apply_ops([map_field_update() | map_field_op()], riak_dt:dot(),
                {riak_dt_vclock:vclock(), entries() , deferred()}, context()) ->
                       {ok, riak_dt_map()} | precondition_error().
apply_ops([], _Dot, Map, _Ctx) ->
    {ok, Map};
apply_ops([{update, {_Name, Type}=Field, Op} | Rest], Dot, {Clock, Values, Deferred}, Ctx) ->
    CRDT = merge_field(Field, Values),
    CRDT1 = Type:parent_clock(Clock, CRDT),
    case Type:update(Op, Dot, CRDT1, Ctx) of
        {ok, Updated} ->
            NewValues = ?DICT:store(Field, {?DICT:store(Dot, Updated, ?DICT:new()),
                                              %% old tombstone was
                                              %% merged into current
                                              %% value so create a new
                                              %% empty one
                                              Type:new()}
                                     , Values),
            apply_ops(Rest, Dot, {Clock, NewValues, Deferred}, Ctx);
        Error ->
            Error
    end;
apply_ops([{remove, Field} | Rest], Dot, Map, Ctx) ->
    case remove_field(Field, Map, Ctx)  of
        {ok, NewMap} ->
            apply_ops(Rest, Dot, NewMap, Ctx);
        E ->
            E
    end.

%% @private when context is undefined, we simply remove all instances
%% of Field, regardless of their dot. If the field is not present then
%% we warn the user with a precondition error. However, in the case
%% that a context is provided we can be more fine grained, and only
%% remove those field entries whose dots are seen by the context. This
%% preserves the "observed" part of "observed-remove". There is no
%% precondition error if we're asked to remove smoething that isn't
%% present, either we defer it, or it has been done already, depending
%% on if the Map clock descends the context clock or not.
%%
%% {@link defer_remove/4} for handling of removes of fields that are
%% _not_ present
-spec remove_field(field_name(), riak_dt_map(), context()) ->
                          {ok, riak_dt_map()} | precondition_error().
remove_field(Field, {Clock, Values, Deferred}, undefined) ->
    case ?DICT:find(Field, Values) of
        error ->
            {error, {precondition, {not_present, Field}}};
        {ok, _Removed} ->
            {ok, {Clock, ?DICT:erase(Field, Values), Deferred}}
    end;
%% Context removes
remove_field(Field, {Clock, Values, Deferred0}, Ctx) ->
    Deferred = defer_remove(Clock, Ctx, Field, Deferred0),
    NewValues = case ctx_rem_field(Field, Values, Ctx, Clock) of
                    empty ->
                        ?DICT:erase(Field, Values);
                    CRDTs ->
                        ?DICT:store(Field, CRDTs, Values)
                end,
    {ok, {Clock, NewValues, Deferred}}.

%% @private drop dominated fields
ctx_rem_field(_Field, error, _Ctx_, _Clock) ->
    empty;
ctx_rem_field({_, Type}, {ok, {CRDTs, TS0}}, Ctx, MapClock) ->
    %% Drop dominated fields, and update the tombstone.
    %%
    %% If the context is removing a field at dot {a, 1} and the
    %% current field is {a, 2}, the tombstone ensures that all events
    %% from {a, 1} are removed from the crdt value. If the ctx remove
    %% is at {a, 3} and the current field is at {a, 2} then we need to
    %% remove only events upto {a, 2}. The glb clock enables that.
    %%
    TombstoneClock = riak_dt_vclock:glb(Ctx, MapClock), %% GLB is events seen by both clocks only
    TS = Type:parent_clock(TombstoneClock, Type:new()),
    Remaining = ?DICT:filter(fun(Dot, _CRDT) ->
                                       is_dot_unseen(Dot, Ctx)
                               end,
                           CRDTs),
    case ?DICT:size(Remaining) of
        0 -> %% Ctx remove removed all dots for field
            empty;
        _ ->
            %% Update the tombstone with the GLB clock
            {Remaining, Type:merge(TS, TS0)}
    end;
ctx_rem_field(Field, Values, Ctx, MapClock) ->
    ctx_rem_field(Field, ?DICT:find(Field, Values), Ctx, MapClock).

%% @private If we're asked to remove something we don't have (or have,
%% but maybe not all 'updates' for it), is it because we've not seen
%% the some update that we've been asked to remove, or is it because
%% we already removed it? In the former case, we can "defer" this
%% operation by storing it, with its context, for later execution. If
%% the clock for the Map descends the operation clock, then we don't
%% need to defer the op, its already been done. It is _very_ important
%% to note, that only _actorless_ operations can be saved. That is
%% operations that DO NOT need to increment the clock. In a Map this
%% means field removals only. Contexts for update operations do not
%% result in deferred operations on the parent Map. This simulates
%% causal delivery, in that an `update' must be seen before it can be
%% `removed'.
-spec defer_remove(riak_dt_vclock:vclock(), riak_dt_vclock:vclock(), field_name(), deferred()) ->
                          deferred().
defer_remove(Clock, Ctx, Field, Deferred) ->
    case riak_dt_vclock:descends(Clock, Ctx) of
        %% no need to save this remove, we're done
        true -> Deferred;
        false -> ?DICT:update(Ctx,
                                fun(Fields) ->
                                        ordsets:add_element(Field, Fields) end,
                                ordsets:add_element(Field, ordsets:new()),
                                Deferred)
    end.

%% @doc merge two `riak_dt_map()'s.
-spec merge(riak_dt_map(), riak_dt_map()) -> riak_dt_map().
merge({_LHSC, LHSE, _LHSD}=LHS, {_RHSC, RHSE, _RHSD}=RHS) when is_list(LHSE);
                                                               is_list(RHSE) ->
    merge(to_v2(LHS), to_v2(RHS));
merge(Map, Map) ->
    Map;
%% @TODO is there a way to optimise this, based on clocks maybe?
merge({LHSClock, LHSEntries, LHSDeferred}, {RHSClock, RHSEntries, RHSDeferred}) ->
    Clock = riak_dt_vclock:merge([LHSClock, RHSClock]),
    {CommonKeys, LHSUnique, RHSUnique} = key_sets(LHSEntries, RHSEntries),
    Acc0 = filter_unique(LHSUnique, LHSEntries, RHSClock, ?DICT:new()),
    Acc1 = filter_unique(RHSUnique, RHSEntries, LHSClock, Acc0),
    Entries = merge_common(CommonKeys, LHSEntries, RHSEntries, LHSClock, RHSClock, Acc1),
    Deferred = merge_deferred(RHSDeferred, LHSDeferred),
    apply_deferred(Clock, Entries, Deferred).

%% @private filter the set of fields that are on one side of a merge
%% only.
-spec filter_unique(riak_dt_set(), entries(), riak_dt_vclock:vclock(), entries()) -> entries().
filter_unique(FieldSet, Entries, Clock, Acc) ->
    ordsets:fold(fun({_Name, Type}=Field, Keep) ->
                      {Dots, TS} = ?DICT:fetch(Field, Entries),
                      KeepDots = ?DICT:filter(fun(Dot, _CRDT) ->
                                                      is_dot_unseen(Dot, Clock)
                                              end,
                                              Dots),

                      case ?DICT:size(KeepDots) of
                          0 ->
                              Keep;
                          _ ->
                              %% create a tombstone since the
                              %% otherside does not have this field,
                              %% it either removed it, or never had
                              %% it. If it never had it, the removing
                              %% dots in the tombstone will have no
                              %% impact on the value, if the otherside
                              %% removed it, then the removed dots
                              %% will be propogated by the tombstone.
                              Tombstone = Type:merge(TS, Type:parent_clock(Clock, Type:new())),
                              ?DICT:store(Field, {KeepDots, Tombstone}, Keep)
                      end
              end,
              Acc,
              FieldSet).

%% @private predicate function, `true' if the provided `dot()' is
%% concurrent with the clock, `false' if the clock has seen the dot.
-spec is_dot_unseen(riak_dt:dot(), riak_dt_vclock:vclock()) -> boolean().
is_dot_unseen(Dot, Clock) ->
    not riak_dt_vclock:descends(Clock, [Dot]).

%% @doc Get the keys from an ?DICT as a ?SET
-spec key_set(riak_dt_dict()) -> riak_dt_set().
key_set(Dict) ->
    ordsets:from_list(?DICT:fetch_keys(Dict)).

%% @doc break the keys from an two ?DICTs out into three ?SETs, the
%% common keys, those unique to one, and those unique to the other.
-spec key_sets(riak_dt_dict(), riak_dt_dict()) -> {riak_dt_set(), riak_dt_set(), riak_dt_set()}.
key_sets(LHS, RHS) ->
    LHSet = key_set(LHS),
    RHSet = key_set(RHS),
    {ordsets:intersection(LHSet, RHSet),
     ordsets:subtract(LHSet, RHSet),
     ordsets:subtract(RHSet, LHSet)}.


%% @private for a set of dots (that are unique to one side) decide
%% whether to keep, or drop each.
-spec filter_dots(riak_dt_set(), riak_dt_dict(), riak_dt_vclock:vclock()) -> entries().
filter_dots(Dots, CRDTs, Clock) ->
    DotsToKeep = ordsets:filter(fun(Dot) ->
                                     is_dot_unseen(Dot, Clock)
                             end,
                             Dots),

    ?DICT:filter(fun(Dot, _CRDT) ->
                           ordsets:is_element(Dot, DotsToKeep)
                   end,
                   CRDTs).

%% @private merge the common fields into a set of surviving dots and a
%% tombstone per field.  If a dot is on both sides, keep it. If it is
%% only on one side, drop it if dominated by the otherside's clock.
merge_common(FieldSet, LHS, RHS, LHSClock, RHSClock, Acc) ->
    ordsets:fold(fun({_, Type}=Field, Keep) ->
                      {LHSDots, LHTS} = ?DICT:fetch(Field, LHS),
                      {RHSDots, RHTS} = ?DICT:fetch(Field, RHS),
                      {CommonDots, LHSUniqe, RHSUnique} = key_sets(LHSDots, RHSDots),
                      TS = Type:merge(RHTS, LHTS),

                      CommonSurviving = ordsets:fold(fun(Dot, Common) ->
                                                          L = ?DICT:fetch(Dot, LHSDots),
                                                          ?DICT:store(Dot, L, Common)
                                                  end,
                                                  ?DICT:new(),
                                                  CommonDots),

                      LHSSurviving = filter_dots(LHSUniqe, LHSDots, RHSClock),
                      RHSSurviving = filter_dots(RHSUnique, RHSDots, LHSClock),

                      Dots = ?DICT:from_list(lists:merge([?DICT:to_list(CommonSurviving),
                                                          ?DICT:to_list(LHSSurviving),
                                                          ?DICT:to_list(RHSSurviving)])),

                      case ?DICT:size(Dots) of
                          0 ->
                              Keep;
                          _ ->
                              ?DICT:store(Field, {Dots, TS}, Keep)
                      end

              end,
              Acc,
              FieldSet).

%% @private
-spec merge_deferred(deferred(), deferred()) -> deferred().
merge_deferred(LHS, RHS) ->
    ?DICT:merge(fun(_K, LH, RH) ->
                          ordsets:union(LH, RH) end,
                  LHS, RHS).

%% @private apply those deferred field removals, if they're
%% preconditions have been met, that is.
-spec apply_deferred(riak_dt_vclock:vclock(), entries(), deferred()) ->
                            {riak_dt_vclock:vclock(), entries(), deferred()}.
apply_deferred(Clock, Entries, Deferred) ->
    ?DICT:fold(fun(Ctx, Fields, Map) ->
                       remove_all(Fields, Map, Ctx)
               end,
               {Clock, Entries, ?DICT:new()},
               Deferred).

%% @private
-spec remove_all([field_name()], riak_dt_map(), context()) -> riak_dt_map().
remove_all(Fields, Map, Ctx) ->
    lists:foldl(fun(Field, MapAcc) ->
                        {ok, MapAcc2}= remove_field(Field, MapAcc, Ctx),
                        MapAcc2
                end,
                Map,
                Fields).

%% @doc compare two `riak_dt_map()'s for equality of structure Both
%% schemas and value list must be equal. Performs a pariwise equals for
%% all values in the value lists
-spec equal(riak_dt_map(), riak_dt_map()) -> boolean().
equal({_LHSC, LHSE, _LHSD}=LHS, {_RHSC, RHSE, _RHSD}=RHS) when is_list(LHSE);
                                                               is_list(RHSE) ->
    equal(to_v2(LHS), to_v2(RHS));
equal({Clock1, Values1, Deferred1}, {Clock2, Values2, Deferred2}) ->
    riak_dt_vclock:equal(Clock1, Clock2) andalso
        lists:sort(?DICT:to_list(Deferred1)) ==
        lists:sort(?DICT:to_list(Deferred2)) andalso
        pairwise_equals(lists:sort(?DICT:to_list(Values1)),
                        lists:sort(?DICT:to_list(Values2))).

-spec pairwise_equals([{field_name(), field_value()}],
                        [{field_name(), field_value()}]) -> boolean().
pairwise_equals([], []) ->
    true;
pairwise_equals([{{Name, Type}, {Dots1, TS1}}| Rest1], [{{Name, Type}, {Dots2, TS2}}|Rest2]) ->
    %% Tombstones don't need to be equal. When we merge with a map
    %% where one side is absent, we take the absent sides clock, when
    %% we merge where both sides have a field, we merge the
    %% tombstones, and apply deferred. The deferred remove uses a glb
    %% of the context and the clock, meaning we get a smaller
    %% tombstone. Both are correct when it comes to determining the
    %% final value. As long as tombstones are not conflicting (that is
    %% A == B | A > B | B > A)
    case {?DICT:fetch_keys(Dots1) == ?DICT:fetch_keys(Dots2), Type:equal(TS1, TS2)} of
        {true, true} ->
            pairwise_equals(Rest1, Rest2);
        _ ->
            false
    end;
pairwise_equals(_, _) ->
    false.

%% @doc an opaque context that can be passed to `update/4' to ensure
%% that only seen fields are removed. If a field removal operation has
%% a context that the Map has not seen, it will be deferred until
%% causally relevant.
-spec precondition_context(riak_dt_map()) -> riak_dt:context().
precondition_context({Clock, _Field, _Deferred}) ->
    Clock.

%% @doc stats on internal state of Map.
%% A proplist of `{StatName :: atom(), Value :: integer()}'. Stats exposed are:
%% `actor_count': The number of actors in the clock for the Map.
%% `field_count': The total number of fields in the Map (including divergent field entries).
%% `duplication': The number of duplicate entries in the Map across all fields.
%%                basically `field_count' - ( unique fields)
%% `deferred_length': How many operations on the deferred list, a reasonable expression
%%                   of lag/staleness.
-spec stats(riak_dt_map()) -> [{atom(), integer()}].
stats(Map) ->
    [ {S, stat(S, to_v2(Map))} || S <- [actor_count, field_count, duplication, deferred_length]].

-spec stat(atom(), riak_dt_map()) -> number() | undefined.
stat(Stat, {_, E, _D}=Map) when is_list(E) ->
    stat(Stat, to_v2(Map));
stat(actor_count, {Clock, _, _}) ->
    length(Clock);
stat(field_count, {_, Fields, _}) ->
    ?DICT:size(Fields);
stat(duplication, {_, Fields, _}) ->
    %% Number of duplicated fields
    {FieldCnt, Duplicates} = ?DICT:fold(fun(_Field, {Dots ,_}, {FCnt, DCnt}) ->
                                                  {FCnt+1, DCnt + ?DICT:size(Dots)}
                                          end,
                                          {0, 0},
                                          Fields),
    Duplicates - FieldCnt;
stat(deferred_length, {_, _, Deferred}) ->
    ?DICT:size(Deferred);
stat(_,_) -> undefined.

-include("riak_dt_tags.hrl").
-define(TAG, ?DT_MAP_TAG).
-define(V1_VERS, 1).
-define(V2_VERS, 2).

%% @doc returns a binary representation of the provided `riak_dt_map()'.
%% The resulting binary is tagged and versioned for ease of future
%% upgrade. Calling `from_binary/1' with the result of this function
%% will return the original map.  Use the application env var
%% `binary_compression' to turn t2b compression on (`true') and off
%% (`false')
%%
%% @see from_binary/1
-spec to_binary(riak_dt_map()) -> binary_map().
to_binary(Map) ->
    {ok, B} = to_binary(?V2_VERS, Map),
    B.

%% @private encode v1 maps as v2, and vice versa. The first argument
%% is the target binary type.
-spec to_binary(Vers :: pos_integer(), riak_dt_map()) -> {ok, binary_map()} | ?UNSUPPORTED_VERSION.
to_binary(?V1_VERS, Map0) ->
    Map = to_v1(Map0),
    {ok, <<?TAG:8/integer, ?V1_VERS:8/integer, (riak_dt:to_binary(Map))/binary>>};
to_binary(?V2_VERS, Map0) ->
    Map = to_v2(Map0),
    {ok, <<?TAG:8/integer, ?V2_VERS:8/integer, (riak_dt:to_binary(Map))/binary>>};
to_binary(Vers, _Map) ->
    ?UNSUPPORTED_VERSION(Vers).

-spec to_version(pos_integer(), any_map()) -> any_map().
to_version(2, Map) -> to_v2(Map);
to_version(1, Map) -> to_v1(Map);
to_version(_, Map) -> Map.


%% @private transpose a v1 map (orddicts) to a v2 (dicts)
-spec to_v2(any_map()) -> riak_dt_map().
to_v2({Clock, Fields0, Deferred0}) when is_list(Fields0),
                                         is_list(Deferred0) ->
    Fields = ?DICT:from_list([ field_to_v2(Key, Value) || {Key, Value} <- Fields0]),
    Deferred = ?DICT:from_list(Deferred0),
    {Clock, Fields, Deferred};
to_v2(S) ->
    S.

%% @private transpose a v2 map (dicts) to a v1 (orddicts)
-spec to_v1(any_map()) -> ord_map().
to_v1({_Clock, Fields0, Deferred0}=S) when is_list(Fields0),
                                           is_list(Deferred0) ->
    S;
to_v1({Clock, Fields0, Deferred0}) ->
    %% Must be dicts, there is no is_dict test though
    %% should we use error handling as logic here??
    Fields = orddict:map(fun field_to_v1/2, riak_dt:dict_to_orddict(Fields0)),
    Deferred = riak_dt:dict_to_orddict(Deferred0),
    {Clock, Fields, Deferred}.

%% @doc When the argument is a `binary_map()' produced by
%% `to_binary/1' will return the original `riak_dt_map()'.
%%

%% @see to_binary/1
-spec from_binary(binary_map()) -> {ok, riak_dt_map()} | ?UNSUPPORTED_VERSION | ?INVALID_BINARY.
from_binary(<<?TAG:8/integer, ?V1_VERS:8/integer, B/binary>>) ->
    Map = riak_dt:from_binary(B),
    %% upgrade ondisk v1 structure to v2 term. This will also handle
    %% the horrid riak-2.0.4 map that has lists for entries/deferred,
    %% but dict elsewhere, and v2 types nested!
    {ok, to_v2(Map)};
from_binary(<<?TAG:8/integer, ?V2_VERS:8/integer, B/binary>>) ->
    %% Only fully v2 maps are written as v2, calling to_v2 a paranoid
    %% waste?
    Map = riak_dt:from_binary(B),
    {ok, to_v2(Map)};
from_binary(<<?TAG:8/integer, Vers:8/integer, _B/binary>>) ->
    ?UNSUPPORTED_VERSION(Vers);
from_binary(_B) ->
    ?INVALID_BINARY.

field_to_v2({Name, Type}, {CRDTs0, Tombstone0}) when is_list(CRDTs0) ->
    Tombstone = Type:to_version(2, Tombstone0),
    CRDTs = dict:from_list([ {Dot, Type:to_version(2, CRDT)} || {Dot, CRDT} <- CRDTs0 ]),
    {{Name, Type}, {CRDTs, Tombstone}};
field_to_v2(FieldName, FieldValue) ->
    %% this is a messed up half v1 half v2 map from the ill fated
    %% riak2.0.4 release.  The top level `fields' and `deferred' were
    %% written to disk/wire as lists to be backwards compatible with
    %% v1, but internally it is all v2 still, it doesn't need
    %% recursing over internally.
    {FieldName, FieldValue}.


field_to_v1({_Name, Type}, {CRDTs0, Tombstone0}) ->
    Tombstone = Type:to_version(1, Tombstone0),
    CRDTs = orddict:map(fun(_Dot, CRDT) ->
                                Type:to_version(1, CRDT)
                        end, riak_dt:dict_to_orddict(CRDTs0)),
    {CRDTs, Tombstone}.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

%% This fails on previous version of riak_dt_map
assoc_test() ->
    Field = {'X', riak_dt_orswot},
    {ok, A} = update({update, [{update, Field, {add, 0}}]}, a, new()),
    {ok, B} = update({update, [{update, Field, {add, 0}}]}, b, new()),
    {ok, B2} = update({update, [{update, Field, {remove, 0}}]}, b, B),
    C = A,
    {ok, C3} = update({update, [{remove, Field}]}, c, C),
    ?assertEqual(merge(A, merge(B2, C3)), merge(merge(A, B2), C3)),
    ?assertEqual(value(merge(merge(A, C3), B2)), value(merge(merge(A, B2), C3))),
    ?assertEqual(merge(merge(A, C3), B2),  merge(merge(A, B2), C3)).

clock_test() ->
    Field = {'X', riak_dt_orswot},
    {ok, A} = update({update, [{update, Field, {add, 0}}]}, a, new()),
    B = A,
    {ok, B2} = update({update, [{update, Field, {add, 1}}]}, b, B),
    {ok, A2} = update({update, [{update, Field, {remove, 0}}]}, a, A),
    {ok, A3} = update({update, [{remove, Field}]}, a, A2),
    {ok, A4} = update({update, [{update, Field, {add, 2}}]}, a, A3),
    AB = merge(A4, B2),
    ?assertEqual([{Field, [1, 2]}], value(AB)).

remfield_test() ->
    Field = {'X', riak_dt_orswot},
    {ok, A} = update({update, [{update, Field, {add, 0}}]}, a, new()),
    B = A,
    {ok, A2} = update({update, [{update, Field, {remove, 0}}]}, a, A),
    {ok, A3} = update({update, [{remove, Field}]}, a, A2),
    {ok, A4} = update({update, [{update, Field, {add, 2}}]}, a, A3),
    AB = merge(A4, B),
    ?assertEqual([{Field, [2]}], value(AB)).

%% Bug found by EQC, not dropping dots in merge when an element is
%% present in both Maos leads to removed items remaining after merge.
present_but_removed_test() ->
    F = {'X', riak_dt_lwwreg},
    %% Add Z to A
    {ok, A} = update({update, [{update, F, {assign, <<"A">>}}]}, a, new()),
    %% Replicate it to C so A has 'Z'->{a, 1}
    C = A,
    %% Remove Z from A
    {ok, A2} = update({update, [{remove, F}]}, a, A),
    %% Add Z to B, a new replica
    {ok, B} = update({update, [{update, F, {assign, <<"B">>}}]}, b, new()),
    %%  Replicate B to A, so now A has a Z, the one with a Dot of
    %%  {b,1} and clock of [{a, 1}, {b, 1}]
    A3 = merge(B, A2),
    %% Remove the 'Z' from B replica
    {ok, B2} = update({update, [{remove, F}]}, b, B),
    %% Both C and A have a 'Z', but when they merge, there should be
    %% no 'Z' as C's has been removed by A and A's has been removed by
    %% C.
    Merged = lists:foldl(fun(Set, Acc) ->
                                 merge(Set, Acc) end,
                         %% the order matters, the two replicas that
                         %% have 'Z' need to merge first to provoke
                         %% the bug. You end up with 'Z' with two
                         %% dots, when really it should be removed.
                         A3,
                         [C, B2]),
    ?assertEqual([], value(Merged)).


%% A bug EQC found where dropping the dots in merge was not enough if
%% you then store the value with an empty clock (derp).
no_dots_left_test() ->
    F = {'Z', riak_dt_lwwreg},
    {ok, A} =  update({update, [{update, F, {assign, <<"A">>}}]}, a, new()),
    {ok, B} =  update({update, [{update, F, {assign, <<"B">>}}]}, b, new()),
    C = A, %% replicate A to empty C
    {ok, A2} = update({update, [{remove, F}]}, a, A),
    %% replicate B to A, now A has B's 'Z'
    A3 = merge(A2, B),
    %% Remove B's 'Z'
    {ok, B2} = update({update, [{remove, F}]}, b, B),
    %% Replicate C to B, now B has A's old 'Z'
    B3 = merge(B2, C),
    %% Merge everytyhing, without the fix You end up with 'Z' present,
    %% with no dots
    Merged = lists:foldl(fun(Set, Acc) ->
                                 merge(Set, Acc) end,
                         A3,
                         [B3, C]),
    ?assertEqual([], value(Merged)).

%% A reset-remove bug eqc found where dropping a superseded dot lost
%% field remove merge information the dropped dot contained, adding
%% the tombstone fixed this.
tombstone_remove_test() ->
    F = {'X', riak_dt_orswot},
    A=B=new(),
    {ok, A1} = update({update, [{update, F, {add, 0}}]}, a, A),
    %% Replicate!
    B1 = merge(A1, B),
    {ok, A2} = update({update, [{remove, F}]}, a, A1),
    {ok, B2} = update({update, [{update, F, {add, 1}}]}, b, B1),
    %% Replicate
    A3 = merge(A2, B2),
    %% that remove of F from A means remove the 0 A added to F
    ?assertEqual([{F, [1]}], value(A3)),
    {ok, B3} = update({update, [{update, F, {add, 2}}]}, b, B2),
    %% replicate to A
    A4 = merge(A3, B3),
    %% final values
    Final = merge(A4, B3),
    %% before adding the tombstone, the dropped dots were simply
    %% merged with the surviving field. When the second update to B
    %% was merged with A, that information contained in the superseded
    %% field in A at {b,1} was lost (since it was merged into the
    %% _VALUE_). This casued the [0] from A's first dot to
    %% resurface. By adding the tombstone, the superseded field merges
    %% it's tombstone with the surviving {b, 2} field so the remove
    %% information is preserved, even though the {b, 1} value is
    %% dropped. Pro-tip, don't alter the CRDTs' values in the merge!
    ?assertEqual([{F, [1,2]}], value(Final)).

%% This test is a regression test for a counter example found by eqc.
%% The previous version of riak_dt_map used the `dot' from the field
%% update/creation event as key in `merge_left/3'. Of course multiple
%% fields can be added/updated at the same time. This means they get
%% the same `dot'. When merging two replicas, it is possible that one
%% has removed one or more of the fields added at a particular `dot',
%% which meant a function clause error in `merge_left/3'. The
%% structure was wrong, it didn't take into account the possibility
%% that multiple fields could have the same `dot', when clearly, they
%% can. This test fails with `dot' as the key for a field in
%% `merge_left/3', but passes with the current structure, of
%% `{field_name(), dot()}' as key.
dot_key_test() ->
    {ok, A} = update({update, [{update, {'X', riak_dt_orswot}, {add, <<"a">>}}, {update, {'X', riak_dt_od_flag}, enable}]}, a, new()),
    B = A,
    {ok, A2} = update({update, [{remove, {'X', riak_dt_od_flag}}]}, a, A),
    ?assertEqual([{{'X', riak_dt_orswot}, [<<"a">>]}], value(merge(B, A2))).

stat_test() ->
    Map = new(),
    {ok, Map1} = update({update, [{update, {c, riak_dt_emcntr}, increment},
                                  {update, {s, riak_dt_orswot}, {add, <<"A">>}},
                                  {update, {m, riak_dt_map}, {update, [{update, {ss, riak_dt_orswot}, {add, 0}}]}},
                                  {update, {l, riak_dt_lwwreg}, {assign, <<"a">>, 1}},
                                  {update, {l2, riak_dt_lwwreg}, {assign, <<"b">>, 2}}]}, a1, Map),
    {ok, Map2} = update({update, [{update, {l, riak_dt_lwwreg}, {assign, <<"foo">>, 3}}]}, a2, Map1),
    {ok, Map3} = update({update, [{update, {l, riak_dt_lwwreg}, {assign, <<"bar">>, 4}}]}, a3, Map1),
    Map4 = merge(Map2, Map3),
    ?assertEqual([{actor_count, 0}, {field_count, 0}, {duplication, 0}, {deferred_length, 0}], stats(Map)),
    ?assertEqual(3, stat(actor_count, Map4)),
    ?assertEqual(5, stat(field_count, Map4)),
    ?assertEqual(undefined, stat(waste_pct, Map4)),
    ?assertEqual(1, stat(duplication, Map4)),
    {ok, Map5} = update({update, [{update, {l3, riak_dt_lwwreg}, {assign, <<"baz">>, 5}}]}, a3, Map4),
    ?assertEqual(6, stat(field_count, Map5)),
    ?assertEqual(1, stat(duplication, Map5)),
    %% Updating field {l, riak_dt_lwwreg} merges the duplicates to a single field
    %% {@link apply_ops}
    {ok, Map6} = update({update, [{update, {l, riak_dt_lwwreg}, {assign, <<"bim">>, 6}}]}, a2, Map5),
    ?assertEqual(0, stat(duplication, Map6)),
    {ok, Map7} = update({update, [{remove, {l, riak_dt_lwwreg}}]}, a1, Map6),
    ?assertEqual(5, stat(field_count, Map7)).

equals_test() ->
    {ok, A} = update({update, [{update, {'X', riak_dt_orswot}, {add, <<"a">>}}, {update, {'X', riak_dt_od_flag}, enable}]}, a, new()),
    {ok, B} = update({update, [{update, {'Y', riak_dt_orswot}, {add, <<"a">>}}, {update, {'Z', riak_dt_od_flag}, enable}]}, b, new()),
    ?assert(not equal(A, B)),
    C = merge(A, B),
    D = merge(B, A),
    ?assert(equal(C, D)),
    ?assert(equal(A, A)).

unsupported_version_test() ->
    ?assertMatch(?UNSUPPORTED_VERSION(12), to_binary(12, new())),
    ?assertMatch(?UNSUPPORTED_VERSION(8) , from_binary(<<?TAG:8/integer, 8:8/integer, (crypto:strong_rand_bytes(22))/binary>>)).

invalid_binary_test() ->
    ?assertMatch(?INVALID_BINARY, from_binary(<<(crypto:strong_rand_bytes(187))/binary>>)).

-endif.

-ifdef(EQC).
-define(NUMTESTS, 1000).
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) ->
                              io:format(user, Str, Args) end, P)).
%% ===================================
%% crdt_statem_eqc callbacks
%% ===================================
size(Map) ->
    %% How big is a Map? Maybe number of fields and depth matter? But
    %% then the number of fields in sub maps too?
    byte_size(term_to_binary(Map)) div 10.

generate() ->
        ?LET({Ops, Actors}, {non_empty(list(gen_op())), non_empty(list(bitstring(16*8)))},
         lists:foldl(fun(Op, Map) ->
                             Actor = case length(Actors) of
                                         1 -> hd(Actors);
                                         _ -> lists:nth(rand:uniform( length(Actors)), Actors)
                                     end,
                             case update(Op, Actor, Map) of
                                 {ok, M} -> M;
                                 _ -> Map
                             end
                     end,
                     new(),
                     Ops)).

%% Add depth parameter
gen_op() ->
    ?SIZED(Size, gen_op(Size)).

gen_op(Size) ->
    ?LET(Ops, non_empty(list(gen_update(Size))), {update, Ops}).

gen_update(Size) ->
    ?LET(Field, gen_field(Size),
         oneof([{remove, Field},
                {update, Field, gen_field_op(Field, Size div 2)}])).

gen_field() ->
    ?SIZED(Size, gen_field(Size)).

gen_field(Size) ->
    {growingelements(['A', 'B', 'C', 'X', 'Y', 'Z']) %% Macro? Bigger?
    , elements([
                riak_dt_emcntr,
                riak_dt_orswot,
                riak_dt_lwwreg,
                riak_dt_od_flag
               ] ++ [riak_dt_map || Size > 0])}.

gen_field_op({_Name, Type}, Size) ->
    Type:gen_op(Size).

prop_v1_downgrade_roundtrip() ->
    ?FORALL(Map, generate(),
            begin
                {ok, ConvertedMap} = from_binary(to_binary(to_version(1, Map))),
                conjunction([{equal, equal(Map, ConvertedMap)},
                             {not_v1, equals(to_v2(ConvertedMap), ConvertedMap)}])
            end).

-endif.

