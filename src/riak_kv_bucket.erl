%% -------------------------------------------------------------------
%%
%% riak_kv_bucket: bucket validation functions
%%
%% Copyright (c) 2007-2016 Basho Technologies, Inc.  All Rights Reserved.
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
%% @doc KV Bucket validation functions

-module(riak_kv_bucket).

-export([validate/4]).
-export([is_valid_property/1]).

-include("riak_kv_types.hrl").

-ifdef(TEST).
-ifdef(EQC).
-compile([export_all]).
-include_lib("eqc/include/eqc.hrl").
-endif.
-include_lib("eunit/include/eunit.hrl").
-endif.

-type prop() :: {PropName::atom(), PropValue::any()}.
-type error_reason() :: atom() | string().
-type error() :: {PropName::atom(), ErrorReason::error_reason()}.
-type props() :: [prop()].
-type errors() :: [error()].

-export_type([props/0]).

-define(VALID_PROPERTIES,
        ["allow_mult",
         "basic_quorum",
         "big_vclock",
         "bucket_type",
         "chash_keyfun",
         "dvv_enabled",
         "dw",
         "last_write_wins",
         "linkfun",
         "n_val",
         "notfound_ok",
         "old_vclock",
         "postcommit",
         "pr",
         "precommit",
         "pw",
         "r",
         "rw",
         "small_vclock",
         "w",
         "write_once",
         "young_vclock",
         %% TS-specific ones:
         "ddl",
         "table_def"
        ]).

-spec is_valid_property(string() | binary()) -> boolean().
%% @doc Checks whether a given binary or string is a valid bucket type
%%      property.
is_valid_property(P) when is_list(P) ->
    lists:member(P, ?VALID_PROPERTIES);
is_valid_property(P) when is_binary(P) ->
    is_valid_property(binary_to_list(P)).


%% @doc called by riak_core in a few places to ensure bucket
%%  properties are sane. The arguments combinations have the following
%%  meanings:-
%%
%% The first argument is the `Phase' of the bucket/bucket type
%% mutation and can be either `create' or `update'.
%%
%% `create' always means that we are creating a new bucket type or
%% updating an inactive bucket type. In the first case `Existing' is
%% the atom `undefined', in the second it is a list of the valid
%% properties returned from the first invocation of `validate/4'. The
%% value of `Bucket' will only ever be a two-tuple of `{binary(),
%% undefined}' for create, as it is only used on bucket types. The
%% final argument `BucketProps' is a list of the properties the user
%% provided for type creation merged with the default properties
%% defined in `riak_core_bucket_type:defaults/0' The job of the
%% function is to validate the given `BucketProps' and return a two
%% tuple `{Good, Bad}' where the first element is the list of valid
%% properties and the second a list of `error()' tuples. Riak_Core
%% will store the `Good' list in metadata iif the `Bad' list is the
%% empty list. It is worth noting that on `create' we must ignore the
%% `Existing' argument altogether.
%%
%% `update' means that we are either updating a bucket type or a
%% bucket. If `Bucket' is a `binary()' or a tuple `{binary(),
%% binary()}' then, a bucket is being updated. If `bucket' is a two
%% tuple of `{binary(), undefined}' then a bucket type is being
%% updated. When `validate/4' is called with `update' as the phase
%% then `Existing' will be the set of properties stored in metadata
%% for this bucket (the set returned as `Good' from the `create'
%% phase) and `BucketProps' will be ONLY the properties that user has
%% supplied as those to update (note: update may mean adding new
%% properties.) The job of `validate/4' in this case is to validate
%% the new properties and return a complete set of bucket properties
%% (ie the new properties merged with the existing propeties) in
%% `Good', riak will then persist these `Good' properties, providing
%% `Bad' is empty.
%%
%% `validate/4' can be used to enforce immutable or co-invariant bucket
%% properties, like "only non-default bucket types can have a
%% `datatype' property", and that "`datatype' buckets must be
%% allow_mult" and "once set, `datatype' cannot be changed".
%%
%% There is no way to _remove_ a property
%%
%% @see validate_dt_props/3
%% @see assert_no_datatype/1
-spec validate(create | update,
               {riak_core_bucket_type:bucket_type(), undefined | binary()} | binary(),
               undefined | props(),
               props()) -> {props(), errors()}.
validate(create, _Bucket, _Existing, BucketProps) when is_list(BucketProps) ->
    validate_create_bucket_type(BucketProps);
validate(update, {_TypeName, undefined}, Existing, New) when is_list(Existing),
                                                            is_list(New) ->
    validate_update_bucket_type(Existing, New);
validate(update, {Type, Name}, Existing, New) when is_list(Existing),
                                                   is_list(New),
                                                   is_binary(Name),
                                                   Type /= <<"default">> ->
    validate_update_typed_bucket(Existing, New);
validate(update, _Bucket, Existing, New) when is_list(Existing),
                                             is_list(New) ->
    validate_default_bucket(Existing, New).

%% @private bucket creation time validation
-spec validate_create_bucket_type(props()) -> {props(), errors()}.
validate_create_bucket_type(BucketProps) ->
    case proplists:get_value(consistent, BucketProps) of
        %% type is explicitly or implicitly not intended to be consistent
        Consistent when Consistent =:= false orelse
                        Consistent =:= undefined ->
            {Unvalidated, Valid, Errors} = case get_boolean(write_once, BucketProps) of
                true ->
                    validate_create_w1c_props(BucketProps);
                _ ->
                    validate_create_dt_props(BucketProps)
            end;
        %% type may be consistent (the value may not be valid)
        Consistent ->
            {Unvalidated, Valid, Errors} = validate_create_consistent_props(Consistent, BucketProps)
    end,
    {Good, Bad} = validate(Unvalidated, Valid, Errors),
    validate_post_merge(Good, Bad).

%% @private update phase of bucket type. Merges properties from
%% existing with valid new properties. Existing can be assumed valid,
%% since they were validated by the `create' phase.
-spec validate_update_bucket_type(props(), props()) -> {props(), errors()}.
validate_update_bucket_type(Existing, New) ->
    Type = type(Existing),
    {Unvalidated, Valid, Errors} = validate_update_type(Type, Existing, New),
    {Good, Bad} = validate(Unvalidated, Valid, Errors),
    validate_post_merge(merge(Good, Existing), Bad).

%% @private pick the validation function depending on existing type.
-spec validate_update_type(Type :: consistent | datatype | write_once | default,
                           Existing :: props(),
                           New :: props()) ->
                                  {Unvalidated :: props(),
                                   Valid  :: props(),
                                   Errors :: props()}.
validate_update_type(consistent, Existing, New) ->
    validate_update_consistent_props(Existing, New);
validate_update_type(write_once, _Existing, New) ->
    NewWriteOnce = proplists:get_value(write_once, New),
    validate_update_w1c_props(NewWriteOnce, New);
validate_update_type(datatype, Existing, New) ->
    validate_update_dt_props(Existing, New);
validate_update_type(default, _Existing, New) ->
    validate_update_default_props(New).

%% @private figure out what `type' the existing bucket is.  NOTE: only
%% call with validated props from existing buckets!!
-spec type(props()) -> consistent | default | datatype | write_once.
type(Props) ->
    type(proplists:get_value(consistent, Props, false),
         proplists:get_value(write_once, Props, false),
         proplists:get_value(datatype, Props, false)).

-spec type(boolean(), boolean(), atom()) ->
                  consistent | default | datatype | write_once.
type(_Consistent=true, _WriteOnce, _DataType) ->
    consistent;
type(_Consistent, _WriteOnce=true, _DataType) ->
    write_once;
type(_Consistent=false, _WriteOnce=false, _DataType=false) ->
    default;
type(_, _, _) ->
    datatype.

%% @private just delegates, but I added it to illustrate the many
%% possible type of validation.
-spec validate_update_typed_bucket(props(), props()) -> {props(), errors()}.
validate_update_typed_bucket(Existing, New) ->
    {Good, Bad} = validate_update_bucket_type(Existing, New),
    validate_post_merge(Good, Bad).

%% @private as far as datatypes go, default buckets are free to do as
%% they please, the datatypes API only works on typed buckets. Go
%% wild!
-spec validate_default_bucket(props(), props()) -> {props(), errors()}.
validate_default_bucket(Existing, New) ->
    {Good, Bad} = validate(New, [], []),
    validate_post_merge(merge(Good, Existing), Bad).

%% @private properties in new overwrite those in old
-spec merge(props(), props()) -> props().
merge(New, Old) ->
    riak_core_bucket_props:merge(New, Old).

%% @private general property validation
-spec validate(InProps::props(), ValidProps::props(), Errors::errors()) ->
                      {props(), errors()}.
validate([], ValidProps, Errors) ->
    {ValidProps, Errors};
validate([{BoolProp, MaybeBool}|T], ValidProps, Errors) when is_atom(BoolProp), BoolProp =:= allow_mult
                                                             orelse BoolProp =:= basic_quorum
                                                             orelse BoolProp =:= last_write_wins
                                                             orelse BoolProp =:= notfound_ok
                                                             orelse BoolProp =:= stat_tracked ->
    case coerce_bool(MaybeBool) of
        error ->
            validate(T, ValidProps, [{BoolProp, not_boolean}|Errors]);
        Bool ->
            validate(T, [{BoolProp, Bool}|ValidProps], Errors)
    end;
validate([{write_once, Value}|T], ValidProps, Errors) ->
    case Value of
        false -> validate(T, [{write_once, false} | ValidProps], Errors);
        _ -> validate(T, ValidProps, [{write_once, "cannot update write_once property"}|Errors])
    end;
validate([{consistent, Value}|T], ValidProps, Errors) ->
    case Value of
        false -> validate(T, [{consistent, false} | ValidProps], Errors);
        _ -> validate(T, ValidProps, [{consistent, "cannot update consistent property"}|Errors])
    end;
validate([{IntProp, MaybeInt}=Prop | T], ValidProps, Errors) when IntProp =:= big_vclock
                                                                  orelse IntProp =:= n_val
                                                                  orelse IntProp =:= old_vclock
                                                                  orelse IntProp =:= small_vclock ->
    case is_integer(MaybeInt) of
        true when MaybeInt > 0 ->
            validate(T, [Prop | ValidProps], Errors);
        _ ->
            validate(T, ValidProps, [{IntProp, not_integer} | Errors])
    end;
validate([{QProp, MaybeQ}=Prop | T], ValidProps, Errors) when  QProp =:= r
                                                              orelse QProp =:= rw
                                                              orelse QProp =:= w ->
    case is_quorum(MaybeQ) of
        true ->
            validate(T, [Prop | ValidProps], Errors);
        false ->
            validate(T, ValidProps, [{QProp, not_valid_quorum} | Errors])
    end;
validate([{QProp, MaybeQ}=Prop | T], ValidProps, Errors) when QProp =:= dw
                                                              orelse QProp =:= pw
                                                              orelse QProp =:= pr ->
    case is_opt_quorum(MaybeQ) of
        true ->
            validate(T, [Prop | ValidProps], Errors);
        false ->
            validate(T, ValidProps, [{QProp, not_valid_quorum} | Errors])
    end;
validate([Prop|T], ValidProps, Errors) ->
    validate(T, [Prop|ValidProps], Errors).


-spec is_quorum(term()) -> boolean().
is_quorum(Q) when is_integer(Q), Q > 0 ->
    true;
is_quorum(Q)  when Q =:= quorum
                   orelse Q =:= one
                   orelse Q =:= all
                   orelse Q =:= <<"quorum">>
                   orelse Q =:= <<"one">>
                   orelse Q =:= <<"all">> ->
    true;
is_quorum(_) ->
    false.

%% @private some quorum options can be zero
-spec is_opt_quorum(term()) -> boolean().
is_opt_quorum(Q) when is_integer(Q), Q >= 0 ->
    true;
is_opt_quorum(Q) ->
    is_quorum(Q).

-spec coerce_bool(any()) -> boolean() | error.
coerce_bool(true) ->
    true;
coerce_bool(false) ->
    false;
coerce_bool(MaybeBool) when is_atom(MaybeBool) ->
     coerce_bool(atom_to_list(MaybeBool));
coerce_bool(MaybeBool) when is_binary(MaybeBool) ->
    coerce_bool(binary_to_list(MaybeBool));
coerce_bool(Int) when is_integer(Int), Int =< 0 ->
    false;
coerce_bool(Int) when is_integer(Int) , Int > 0 ->
    true;
coerce_bool(MaybeBool) when is_list(MaybeBool) ->
    Lower = string:to_lower(MaybeBool),
    Atom = (catch list_to_existing_atom(Lower)),
    case Atom of
        true -> true;
        false -> false;
        _ -> error
    end;
coerce_bool(_) ->
    error.

%% @private riak consistent object support requires a bucket type
%% where `consistent' is defined and not `false' to have `consistent'
%% set to true. this function validates that property.
%%
%% We take the indication of a value other than `false' to mean the user
%% intended to create a consistent type. We validate that the value is actually
%% something Riak can understand -- `true'. Why don't we just convert any other
%% value to true? Well, the user maybe type "fals" so lets be careful.
-spec validate_create_consistent_props(any(), props()) -> {props(), props(), errors()}.
validate_create_consistent_props(true, New) ->
    % write_once and consistent can't both be true
    case get_boolean(write_once, New) of
        true ->
            {lists:keydelete(consistent, 1, New), [], [{consistent, "Write once buckets must be not be consistent=true"}]};
        _ ->
            {lists:keydelete(consistent, 1, New), [{consistent, true}], []}
    end;
validate_create_consistent_props(false, New) ->
    {lists:keydelete(consistent, 1, New), [{consistent, false}], []};
validate_create_consistent_props(undefined, New) ->
    {New, [], []};
validate_create_consistent_props(Invalid, New) ->
    Err = lists:flatten(io_lib:format("~p is not a valid value for consistent. Use \"true\" or \"false\"", [Invalid])),
    {lists:keydelete(consistent, 1, New), [], [{consistent, Err}]}.


%% @private riak datatype support requires a bucket type of `datatype'
%% and `allow_mult' set to `true'. These function enforces those
%% properties.
%%
%% We take the presence of a `datatype' property as indication that
%% this bucket type is a special type, somewhere to store CRDTs. I
%% realise this slightly undermines the reason for bucket types (no
%% magic names) but there has to be some way to indicate intent, and
%% that way is the "special" property name `datatype'.
%%
%% Since we don't ever want sibling CRDT types (though we can handle
%% them: see riak_kv_crdt), `datatype' is an immutable property. Once
%% you create a bucket with a certain datatype you can't change
%% it. The `update' bucket type path enforces this. It doesn't
%% validate the correctness of the type, since it assumes that was
%% done at creation, only that it is either the same as existing or
%% not present.

-spec validate_create_dt_props(props()) -> {props(), props(), errors()}.
validate_create_dt_props(New) ->
    validate_create_dt_props(proplists:get_value(datatype, New), New).

%% @private validate the datatype, if present
-spec validate_create_dt_props(undefined | atom(), props()) -> {props(), props(), errors()}.
validate_create_dt_props(undefined, New) ->
    {New, [], []};
validate_create_dt_props(DataType, New) ->
    Unvalidated = lists:keydelete(datatype, 1, New),
    Mod = riak_kv_crdt:to_mod(DataType),
    case lists:member(Mod, ?V2_TOP_LEVEL_TYPES) of
        true ->
            validate_create_dt_props(Unvalidated, [{datatype, DataType}], []);
        false ->
            Err = lists:flatten(io_lib:format("~p not supported for bucket datatype property", [DataType])),
            validate_create_dt_props(Unvalidated, [], [{datatype, Err}])
    end.

%% @private validate the boolean property, if `datatype' was present,
%% require `allow_mult=true' even if `datatype' was invalid, as we
%% assume the user meant to create `datatype' bucket
-spec validate_create_dt_props(props(), props(), errors()) -> {props(), props(), errors()}.
validate_create_dt_props(Unvalidated0, Valid, Invalid) ->
    Unvalidated = lists:keydelete(allow_mult, 1, Unvalidated0),
    case allow_mult(Unvalidated0) of
        true ->
            {Unvalidated, [{allow_mult, true} | Valid], Invalid};
        _ ->
            Err = io_lib:format("Data Type buckets must be allow_mult=true", []),
            {Unvalidated, Valid, [{allow_mult, Err} | Invalid]}
    end.


%% @private Riak write_once support requires a bucket type where
%% write_once is set to true.  This function validates that when
%% write_once is set to true, other properties are consistent.
%% See validate_create_w1c_props/3 for an enumeration of these rules.
-spec validate_create_w1c_props(props()) -> {props(), props(), errors()}.
validate_create_w1c_props(New) ->
    validate_create_w1c_props(proplists:get_value(write_once, New), New).

%% @private validate the write_once, if present
-spec validate_create_w1c_props(true, props()) -> {props(), props(), errors()}.
validate_create_w1c_props(true, New) ->
    Unvalidated = lists:keydelete(write_once, 1, New),
    validate_w1c_props(Unvalidated, [{write_once, true}], []).

%% @private checks that a bucket that is not a special immutable type
%% is not attempting to become one.
-spec validate_update_default_props(New :: props()) ->
                                           {Unvalidated :: props(),
                                            Valid :: props(),
                                            Error :: props()}.
validate_update_default_props(New) ->
    %% Only called if not already a consistent, datatype, write_once
    %% bucket. Check that none of those are being set to `true'/valid
    %% datatypes.
    ensure_not_present(New, [], [], [{datatype, "`datatype` must not be defined."},
                                     {consistent, true, "Write once buckets must not be consistent=true"},
                                     {write_once, true, "Cannot set existing bucket type to `write_once`"}]).

%% @private validate that strongly-consistent types and buckets do not
%% have their n_val changed, nor become eventually consistent
-spec validate_update_consistent_props(props(), props()) -> {props(), props(), errors()}.
validate_update_consistent_props(Existing, New) ->
    Unvalidated = lists:keydelete(n_val, 1, lists:keydelete(consistent, 1, New)),
    OldNVal = proplists:get_value(n_val, Existing),
    NewNVal = proplists:get_value(n_val, New, OldNVal),
    NewConsistent = proplists:get_value(consistent, New),
    CErr = "cannot update consistent property",
    NErr = "n_val cannot be modified for existing consistent type",
    case {NewConsistent, OldNVal, NewNVal} of
        {undefined, _, undefined} ->
            {Unvalidated, [], []};
        {undefined, _N, _N} ->
            {Unvalidated, [{n_val, NewNVal}], []};
        {true, _N, _N} ->
            {Unvalidated, [{n_val, NewNVal}, {consistent, true}], []};
        {C, _N, _N} when C =/= undefined orelse
                         C =/= true ->
            {Unvalidated, [{n_val, NewNVal}], [{consistent, CErr}]};
        {undefined, _OldN, _NewN} ->
            {Unvalidated, [], [{n_val, NErr}]};
        {true, _OldN, _NewN} ->
            {Unvalidated, [{consistent, true}], [{n_val, NErr}]};
        {_, _, _} ->
            {Unvalidated, [], [{n_val, NErr}, {consistent, CErr}]}
    end.

%% @private somewhat duplicates the create path, but easier to read
%% this way, and chars are free
-spec validate_update_dt_props(props(), props()) -> {props(), props(), errors()}.
validate_update_dt_props(Existing, New0) ->
    New = lists:keydelete(datatype, 1, New0),
    case {proplists:get_value(datatype, Existing), proplists:get_value(datatype, New0)} of
        {_Datatype, undefined} ->
            validate_update_dt_props(New, [] , []);
        {Datatype, Datatype} ->
            validate_update_dt_props(New, [{datatype, Datatype}] , []);
        {_Datatype, _Datatype2} ->
            validate_update_dt_props(New, [] , [{datatype, "Cannot update datatype on existing bucket"}])
    end.

%% @private check that allow_mult is correct
-spec validate_update_dt_props(props(), props(), errors()) -> {props(), props(), errors()}.
validate_update_dt_props(New, Valid, Invalid) ->
    Unvalidated = lists:keydelete(allow_mult, 1, New),
    case allow_mult(New) of
        undefined ->
            {Unvalidated, Valid, Invalid};
        true ->
            {Unvalidated, [{allow_mult, true} | Valid], Invalid};
        _ ->
            {Unvalidated, Valid, [{allow_mult, "Cannot change datatype bucket from allow_mult=true"} | Invalid]}
end.

%% @private
%% precondition: Existing contains {write_once, true}
-spec validate_update_w1c_props(boolean() | undefined, props()) ->
                                      {props(), props(), errors()}.
validate_update_w1c_props(NewFP, New) ->
    Unvalidated = lists:keydelete(write_once, 1, New),
    case NewFP of
        Unchanged when Unchanged == true orelse Unchanged == undefined ->
            validate_w1c_props(Unvalidated, [{write_once, true}], []);
        _ ->
            validate_w1c_props(Unvalidated, [],
                              [{write_once, "Cannot modify write_once property once set to true"}])
    end.

%% @private validate the boolean property, if `write_once' was present.
%% precondition: write_once is not an entry in Unvalidated
%%               write_once is an entry in Valid
%% The following rules apply when write_once is true:
%%   - datatype may not be defined
%%   - consistent may not be true
-spec validate_w1c_props(props(), props(), errors()) -> {props(), props(), errors()}.
validate_w1c_props(Unvalidated, Valid, Errors) ->
    ensure_not_present(Unvalidated, Valid, Errors,
                       [{consistent, true, "Write once buckets must not be consistent=true"},
                        {datatype, "Write once buckets must not have datatype defined"}
                       ]).

%% @private any property in `InvalidPropsSpec' present in
%% `Unvalidated' will be added to `Errors'. Returned is the as yet
%% unvalidated remainder properties from `Unvalidated', the properties
%% from `InvalidPropsSpec' that were present and not invalid added to
%% `Valid' and the accumulated errors added to `Errors'.
-spec ensure_not_present(props(), props(), props(), [{atom(), term(), string()} |
                                                     {atom(), term()}]) ->
                                {props(), props(), props()}.
ensure_not_present(Unvalidated, Valid, Errors, InvalidPropsSpec) ->
    lists:foldl(fun({Key, NotAllowed, ErrorMessage}, {U, V, E}) ->
                        case lists:keytake(Key, 1, U) of
                            false ->
                                {U, V, E};
                            {value, {Key, Val}, U2} ->
                                Val2 = coerce_bool(Val),
                                if Val2 == NotAllowed ->
                                        {U2, V, [{Key, ErrorMessage} | E]};
                                   true ->
                                        {U, V, E}
                                end
                        end;
                   ({Key, ErrorMessage}, {U, V, E}) ->
                        case lists:keytake(Key, 1, U) of
                            false -> {U, V, E};
                            {value, {Key, _Val}, U2} ->
                                {U2, V, [{Key, ErrorMessage} | E]}
                        end
                end,
                {Unvalidated, Valid, Errors},
                InvalidPropsSpec).

%% Validate properties after they have all been individually validated, merged,
%% and resolved to their final values. This allows for identifying invalid
%% combinations of properties, such as `last_write_wins=true' and
%% `dvv_enabled=true'.
-spec validate_post_merge(props(), errors()) -> {props(), errors()}.
validate_post_merge(Props, Errors) ->
    %% Currently, we only have one validation rule to apply at this stage, so
    %% just call the validation function directly. If more are added in the
    %% future, it would be good to use function composition to compose the
    %% individual validation functions into a single function.
    validate_last_write_wins_implies_not_dvv_enabled({Props, Errors}).

%% If `last_write_wins' is true, `dvv_enabled' must not also be true.
-spec validate_last_write_wins_implies_not_dvv_enabled({props(), errors()}) -> {props(), errors()}.
validate_last_write_wins_implies_not_dvv_enabled({Props, Errors}) ->
    case {last_write_wins(Props), dvv_enabled(Props)} of
        {true, true} ->
            {lists:keydelete(dvv_enabled, 1, Props),
             [{dvv_enabled,
               "If last_write_wins is true, dvv_enabled must be false"}
              |Errors]};
        {_, _} ->
            {Props, Errors}
    end.

%% @private just grab the allow_mult value if it exists
-spec allow_mult(props()) -> boolean() | 'undefined' | 'error'.
allow_mult(Props) ->
    case proplists:get_value(allow_mult, Props) of
        undefined ->
            undefined;
        MaybeBool ->
            coerce_bool(MaybeBool)
    end.

%% Boolean value of the `last_write_wins' property, or `undefined' if not present.
-spec last_write_wins(props()) -> boolean() | 'undefined' | 'error'.
last_write_wins(Props) ->
    get_boolean(last_write_wins, Props).

%% Boolean value of the `dvv_enabled' property, or `undefined' if not present.
-spec dvv_enabled(props()) -> boolean() | 'undefined' | 'error'.
dvv_enabled(Props) ->
    get_boolean(dvv_enabled, Props).

%% @private coerce the value under key to be a boolean, if defined; undefined, otherwise.
-spec get_boolean(PropName::atom(), props()) -> boolean() | 'undefined' | 'error'.
get_boolean(Key, Props) ->
    case proplists:get_value(Key, Props) of
        undefined ->
            undefined;
        MaybeBool ->
            coerce_bool(MaybeBool)
    end.


%%
%% EUNIT tests...
%%

-ifdef (TEST).

coerce_bool_test_ () ->
    [?_assertEqual(false, coerce_bool(false)),
     ?_assertEqual(true, coerce_bool(true)),
     ?_assertEqual(true, coerce_bool("True")),
     ?_assertEqual(false, coerce_bool("fAlSE")),
     ?_assertEqual(false, coerce_bool(<<"FAlse">>)),
     ?_assertEqual(true, coerce_bool(<<"trUe">>)),
     ?_assertEqual(true, coerce_bool(1)),
     ?_assertEqual(true, coerce_bool(234567)),
     ?_assertEqual(false, coerce_bool(0)),
     ?_assertEqual(false, coerce_bool(-1234)),
     ?_assertEqual(false, coerce_bool('FALSE')),
     ?_assertEqual(true, coerce_bool('TrUe')),
     ?_assertEqual(error, coerce_bool("Purple")),
     ?_assertEqual(error, coerce_bool(<<"frangipan">>)),
     ?_assertEqual(error, coerce_bool(erlang:make_ref()))
    ].

-ifdef(EQC).
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) ->
                              io:format(user, Str, Args) end, P)).

-define(TEST_TIME_SECS, 10).

immutable_test_() ->
   {timeout, ?TEST_TIME_SECS+5, [?_assert(test_immutable() =:= true)]}.

valid_test_() ->
   {timeout, ?TEST_TIME_SECS+5, [?_assert(test_create() =:= true)]}.

merges_props_test_() ->
    {timeout, ?TEST_TIME_SECS+5, [?_assert(test_merges() =:= true)]}.

-define(LAST_WRITE_WINS, {last_write_wins, true}).
-define(DVV_ENABLED, {dvv_enabled, true}).
-define(LWW_DVV, [?LAST_WRITE_WINS, ?DVV_ENABLED]).
validate_create_bucket_type_test() ->
    {Validated, Errors} = validate_create_bucket_type(?LWW_DVV),
    ?assertEqual([{last_write_wins, true}], Validated),
    ?assertMatch([{dvv_enabled, _Message}], Errors).

validate_update_bucket_type_test() ->
    {Validated, Errors} = validate_update_bucket_type([], ?LWW_DVV),
    ?assertEqual([{last_write_wins, true}], Validated),
    ?assertMatch([{dvv_enabled, _Message}], Errors).

validate_update_typed_bucket_test() ->
    {Validated, Errors} = validate_update_typed_bucket([], ?LWW_DVV),
    ?assertEqual([{last_write_wins, true}], Validated),
    ?assertMatch([{dvv_enabled, _Message}], Errors).

validate_default_bucket_test() ->
    {Validated, Errors} = validate_default_bucket([], ?LWW_DVV),
    ?assertEqual([{last_write_wins, true}], Validated),
    ?assertMatch([{dvv_enabled, _Message}], Errors).

validate_last_write_wins_implies_not_dvv_enabled_test() ->
    {Validated, Errors} = validate_last_write_wins_implies_not_dvv_enabled({?LWW_DVV, []}),
    ?assertEqual([{last_write_wins, true}], Validated),
    ?assertMatch([{dvv_enabled, _Message}], Errors).

test_immutable() ->
   test_immutable(?TEST_TIME_SECS).

test_immutable(TestTimeSecs) ->
       eqc:quickcheck(eqc:testing_time(TestTimeSecs, ?QC_OUT(prop_immutable()))).

test_create() ->
    test_create(?TEST_TIME_SECS).

test_create(TestTimeSecs) ->
        eqc:quickcheck(eqc:testing_time(TestTimeSecs, ?QC_OUT(prop_create_valid()))).

test_merges() ->
     test_merges(?TEST_TIME_SECS).

test_merges(TestTimeSecs) ->
         eqc:quickcheck(eqc:testing_time(TestTimeSecs, ?QC_OUT(prop_merges()))).

%% Props

%% When validating:
%%   * Once the datatype has been set, it cannot be unset or changed and
%%     allow_mult must remain true
%%   * the consistent property cannot change and neither can the n_val if
%%     the type is consistent
%%   * the write_once property cannot change
prop_immutable() ->
    ?FORALL(Args, gen_args(no_default_buckets),
            begin
                Result = erlang:apply(?MODULE, validate, Args),
                Phase = lists:nth(1, Args),
                Existing = lists:nth(3, Args),
                New = lists:nth(4, Args),
                ?WHENFAIL(
                   begin
                       io:format("Phase: ~p~n", [Phase]),
                       io:format("Bucket ~p~n", [lists:nth(2, Args)]),
                       io:format("Existing ~p~n", [Existing]),
                       io:format("New ~p~n", [New]),
                       io:format("Result ~p~n", [Result]),
                       io:format("{allow_mult, valid_dt, valid_consistent, n_val_changed}~n"),
                       io:format("{~p,~p,~p,~p}~n~n",
                                 [allow_mult(New), valid_datatype(New), valid_consistent(New), n_val_changed(Existing, New)])
                   end,
                   collect(with_title("{allow_mult, valid_dt, valid_consistent, n_val_changed}"),
                           {allow_mult(New), valid_datatype(New), valid_consistent(New), n_val_changed(Existing, New)},
                           immutable(Phase, New, Existing, Result)))
            end).

%% When creating a bucket type:
%%  * for datatypes, the datatype must be
%%    valid, and allow mult must be true
%%  * for consistent data, the consistent property must be valid
prop_create_valid() ->
    ?FORALL({Bucket, Existing, New}, {gen_bucket(create, bucket_types),
                                      gen_existing(), gen_new(create)},
            begin
                Result = validate(create, Bucket, Existing, New),
                ?WHENFAIL(
                   begin
                       io:format("Bucket ~p~n", [Bucket]),
                       io:format("Existing ~p~n", [Existing]),
                       io:format("New ~p~n", [New]),
                       io:format("Result ~p~n", [Result]),
                       io:format("{has_datatype, valid_datatype, allow_mult, has_w1c, valid_w1c, has_consistent, valid_consistent}~n"),
                       io:format("{~p,~p,~p,~p,~p,~p,~p}~n~n",
                                 [has_datatype(New), valid_datatype(New), allow_mult(New),
                                     has_w1c(New), valid_w1c(New),
                                     has_consistent(New), valid_consistent(New)])
                   end,
                   collect(with_title("{has_datatype, valid_datatype, allow_mult, has_w1c, valid_w1c, has_consistent, valid_consistent, lww, dvv_enabled}"),
                           {has_datatype(New), valid_datatype(New), allow_mult(New), has_w1c(New), valid_w1c(New), has_consistent(New), valid_consistent(New), last_write_wins(New), dvv_enabled(New)},
                           only_create_if_valid(Result, New)))
            end).

%% As of 2.* validate/4 must merge the new and existing props, verify
%% that. Not sure if this test isn't just a tautology. Reviewer?
prop_merges() ->
    ?FORALL({Bucket, Existing0, New}, {gen_bucket(update, any),
                                       gen_existing(),
                                       gen_new(update)},
            begin
                %% ensure default buckets are not marked consistent or write_once since that is invalid
                Existing = case default_bucket(Bucket) of
                               true -> lists:keydelete(write_once, 1, lists:keydelete(consistent, 1, Existing0));
                               false -> Existing0
                           end,
                Result={Good, Bad} = validate(update, Bucket, Existing, New),

                %% All we really want to check is that every key in
                %% Good replaces the same key in Existing, right?
                %% Remove `Bad' from the inputs to validate.
                F = fun({Name, _Err}, {Old, Neu}) ->
                     case lists:keytake(Name, 1, Neu) of
                         false ->
                             {Old, Neu};
                         {value, V, Neu2} ->
                             %% only want to remove the exact bad value from existing,
                             %% not the bad key!
                             {lists:delete(V, Old), Neu2}
                     end
                end,
                {NoBadExisting, OnlyGoodNew} = lists:foldl(F, {Existing, New}, Bad),

                %% What's left are the good ones from `New'. Replace
                %% their keys in `Existing'. `Expected' is the input
                %% set, minus the `Bad' properties, and plus the
                %% `Good' ones. Compare that to output props `from
                %% validate/4' to verify the merge happens as
                %% expected.
                Expected  = lists:ukeymerge(1, lists:ukeysort(1, OnlyGoodNew),
                                            lists:ukeysort(1, NoBadExisting)),
                ?WHENFAIL(
                   begin
                       io:format("Bucket ~p~n", [Bucket]),
                       io:format("Existing ~p~n", [lists:sort(Existing)]),
                       io:format("New ~p~n", [New]),
                       io:format("Result ~p~n", [Result]),
                       io:format("Expected ~p~n", [lists:sort(Expected)]),
                       io:format("Expected - Good ~p~n", [sets:to_list(sets:subtract(sets:from_list(Expected), sets:from_list(Good)))]),
                       io:format("Good - Expected ~p~n", [sets:to_list(sets:subtract(sets:from_list(Good), sets:from_list(Expected)))])
                   end,
                   case valid_dvv_lww({Good, Bad}) of
                       true ->
                           lists:sort(maybe_remove_dvv_enabled(Expected)) == lists:sort(maybe_remove_dvv_enabled(Good));
                       _ ->
                           false
                   end
                  )
            end).

valid_dvv_lww({Good, Bad}) ->
    case last_write_wins(Good) of
        true ->
            DvvEnabled = dvv_enabled(Good),
            (DvvEnabled =:= undefined) orelse (not DvvEnabled) orelse has_dvv_enabled(Bad);
        _ ->
            true
    end.

maybe_remove_dvv_enabled(Props) ->
    lists:keydelete(dvv_enabled, 1, lists:keydelete(last_write_wins, 1, Props)).

%% Generators
gen_args(GenDefBucket) ->
    ?LET(Phase, gen_phase(), [Phase, gen_bucket(Phase, GenDefBucket),
                              gen_existing(), gen_new(update)]).

gen_phase() ->
    oneof([create, update]).

gen_bucket(create, _) ->
    gen_bucket_type();
gen_bucket(update, no_default_buckets) ->
    oneof([gen_bucket_type(), gen_typed_bucket()]);
gen_bucket(update, _) ->
    oneof([gen_bucket_type(), gen_typed_bucket(), gen_bucket()]).

gen_bucket_type() ->
    {binary(20), undefined}.

gen_typed_bucket() ->
    {binary(20), binary(20)}.

gen_bucket() ->
    oneof([{<<"default">>, binary(20)}, binary(20)]).

gen_existing() ->
    Defaults = lists:ukeysort(1, riak_core_bucket_type:defaults()),
    ?LET(Special, oneof([gen_valid_mult_dt(), gen_valid_w1c(), gen_valid_consistent(), gen_valid_dvv_lww(), []]),
         lists:ukeymerge(1, lists:ukeysort(1, Special), Defaults)).

gen_maybe_consistent() ->
    oneof([[], gen_valid_consistent()]).

gen_maybe_bad_consistent() ->
    oneof([gen_valid_consistent(), [{consistent, notvalid}]]).

gen_valid_consistent() ->
    ?LET(Consistent, bool(), [{consistent, Consistent}]).

gen_valid_mult_dt() ->
    ?LET(Mult, bool(), gen_valid_mult_dt(Mult)).

gen_valid_mult_dt(false) ->
    ?LET(AllowMult, bool(), [{allow_mult, AllowMult}]);
gen_valid_mult_dt(true) ->
    ?LET(Datatype, gen_datatype(), [{allow_mult, true}, {datatype, Datatype}]).

gen_valid_dvv_lww() ->
    ?LET(LastWriteWins, bool(), gen_valid_dvv_lww(LastWriteWins)).

gen_valid_dvv_lww(true) ->
    [{last_write_wins, true}, {dvv_enabled, false}];
gen_valid_dvv_lww(false) ->
    ?LET(DvvEnabled, bool(), [{last_write_wins, false}, {dvv_enabled, DvvEnabled}]).

gen_new(update) ->
    ?LET(
        {Mult, Datatype, WriteOnce, Consistent, NVal, LastWriteWins, DvvEnabled},
        {
            gen_allow_mult(),
            oneof([[], gen_datatype_property()]),
            oneof([[], gen_valid_w1c()]),
            oneof([[], gen_maybe_bad_consistent()]),
            oneof([[], [{n_val, choose(1, 10)}]]),
            oneof([[], gen_lww()]),
            oneof([[], gen_dvv_enabled()])
        },
        Mult ++ Datatype ++ WriteOnce ++ Consistent ++ NVal ++ LastWriteWins ++ DvvEnabled
    );
gen_new(create) ->
    Defaults0 = riak_core_bucket_type:defaults(),
    Defaults1 = lists:keydelete(allow_mult, 1, Defaults0),
    Defaults2 = lists:keydelete(last_write_wins, 1, Defaults1),
    Defaults = lists:keydelete(dvv_enabled, 1, Defaults2),
    ?LET(
        {Mult, DatatypeOrConsistent, WriteOnce, LastWriteWins, DvvEnabled},
        {
            gen_allow_mult(),
            frequency([{5, gen_datatype_property()},
                       {5, gen_maybe_bad_consistent()},
                       {5, []}]),
            gen_w1c(), gen_lww(), gen_dvv_enabled()},
         Defaults ++ Mult ++ DatatypeOrConsistent ++ WriteOnce ++ LastWriteWins ++ DvvEnabled).

gen_allow_mult() ->
    ?LET(Mult, frequency([{9, bool()}, {1, binary()}]), [{allow_mult, Mult}]).

gen_datatype_property() ->
    ?LET(Datattype, oneof([gen_datatype(), notadatatype]), [{datatype, Datattype}]).

gen_lww() ->
    ?LET(LwwWins, bool(), [{last_write_wins, LwwWins}]).

gen_dvv_enabled() ->
    ?LET(DvvEnabled, bool(), [{dvv_enabled, DvvEnabled}]).

gen_datatype() ->
    ?LET(Datamod, oneof(?V2_TOP_LEVEL_TYPES), riak_kv_crdt:from_mod(Datamod)).

%gen_maybe_bad_w1c() ->
%    oneof([gen_valid_w1c(), {write_once, rubbish}]).

gen_w1c() ->
    ?LET(WriteOnce, frequency([{9, bool()}, {1, binary()}]), [{write_once, WriteOnce}]).

gen_valid_w1c() ->
    ?LET(WriteOnce, bool(), [{write_once, WriteOnce}]).

%% helpers

gen_string_bool() ->
    oneof(["true", "false"]).

-spec immutable(Phase :: create | update,
                New :: props(),
                Existing :: props(),
                Result :: {props(), errors()}) -> boolean().
immutable(create, _,  _, _) ->
    true;
immutable(_, _New, undefined, _) ->
    true;
immutable(update, New, Existing, {_Good, Bad}) ->
    case type(Existing)  of
        datatype ->
            NewDT = proplists:get_value(datatype, New),
            NewAM = proplists:get_value(allow_mult, New),
            ExistingDT = proplists:get_value(datatype, Existing),
            immutable_dt(NewDT, NewAM, ExistingDT, Bad);
        write_once ->
            OldFP = proplists:get_value(write_once, Existing),
            NewFP = proplists:get_value(write_once, New),
            immutable_write_once(OldFP, NewFP, New, Bad);
        default ->
            %% doesn't mean valid props, just that there is no
            %% immutability constraint.
            true;
        consistent ->
            %% existing type (or bucket) is consistent
            immutable_consistent(New, Existing, Bad)
    end.

immutable_consistent(New, Existing, Bad) ->
    NewCS = proplists:get_value(consistent, New),
    OldN = proplists:get_value(n_val, Existing),
    NewN = proplists:get_value(n_val, New),
    immutable_consistent(NewCS, OldN, NewN, Bad).

%% Consistent properties must remain consistent and
%% the n_val must not change. This function assumes the
%% existing value for consistent is true.
immutable_consistent(undefined, _N, undefined, _Bad) ->
    %% consistent and n_val not modified
    true;
immutable_consistent(true, _N, undefined, _Bad) ->
    %% consistent still set to true and n_val not modified
    true;
immutable_consistent(Consistent, _N, _N, _Bad) when Consistent =:= undefined orelse
                                                    Consistent =:= true ->
    %% consistent not modified or still set to true and n_val
    %% modified but set to same value
    true;
immutable_consistent(Consistent, _OldN, _NewN, Bad) when Consistent =:= undefined orelse
                                                         Consistent =:= true ->
    %% consistent not modified or still set to true but n_val modified
    has_n_val(Bad);
immutable_consistent(_Consistent, OldN, NewN, Bad) when OldN =:= NewN orelse
                                                        NewN =:= undefined ->
    %% consistent modified but set to invalid value or false, n_val not modified
    %% or set to existing value
    has_consistent(Bad);
immutable_consistent(_Consistent, _OldN, _NewN, Bad) ->
    has_consistent(Bad) andalso has_n_val(Bad).

%% @private only called when the existing bucket type is immutable All
%% that has to be true is that the bucket type is still write_once
immutable_write_once(true, New, NewProps, Bad) when New == true orelse New == undefined ->
    not has_write_once(Bad) andalso undefined_props([datatype, {consistent, true}], NewProps, Bad);
immutable_write_once(true, _New, NewProps, Bad) ->
    has_write_once(Bad) andalso undefined_props([datatype, {consistent, true}], NewProps, Bad);
immutable_write_once(_Existing, true, _NewProps, Bad) ->
    has_write_once(Bad).

%% @private every prop in Names that is present in Props, must be in
%% Errors.
undefined_props(Names, Props, Errors) ->
    lists:all(fun({Name, Value}) ->
                      (Value /= proplists:get_value(Name, Props)) orelse
                          lists:keymember(Name, 1, Errors);
                 (Name) ->
                      (not lists:keymember(Name, 1, Props)) orelse
                          lists:keymember(Name, 1, Errors)
              end,
              Names).

%% If data type and allow mult and are in New they must match what is in existing
%% or be in Bad
immutable_dt(_NewDT=undefined, _NewAllowMult=undefined, _ExistingDT, _Bad) ->
    %% datatype and allow_mult are not being modified, so its valid
    true;
immutable_dt(_Datatype, undefined, _Datatype, _Bad) ->
    %% data types from new and existing match and allow mult not modified, valid
    true;
immutable_dt(_Datatype, true, _Datatype, _Bad) ->
  %% data type from new and existing match and allow mult still set to true, valid
    true;
immutable_dt(undefined, true, _Datatype, _Bad) ->
    %% data type not modified and allow_mult still set to true, vald
    true;
immutable_dt(_Datatype, undefined, _Datatype2, Bad) ->
    %% data types do not match, allow_mult not modified
    has_datatype(Bad);
immutable_dt(_Datatype, true, _Datatype2, Bad) ->
    %% data types do not match, allow_mult still set to true
    has_datatype(Bad);
immutable_dt(_Datatype, false, undefined, Bad) ->
    %% datatype defined when it wasn't before
    has_datatype(Bad);
immutable_dt(_Datatype, false, _Datatype, Bad) ->
    %% attempt to set allow_mult to false when data type set is invalid, datatype not modified
    has_allow_mult(Bad);
immutable_dt(undefined, false, _Datatype, Bad) ->
    %% data type not modified but exists and allow_mult set to false is invalid
    has_allow_mult(Bad);
immutable_dt(_Datatype, false, _Datatype2, Bad) ->
    %% data type changed and allow mult modified to be false, both are invalid
    has_allow_mult(Bad) andalso has_datatype(Bad);
immutable_dt(undefined, _, _Datatype, Bad) ->
    %% datatype not modified but allow_mult is invalid
    has_allow_mult(Bad);
immutable_dt(_Datatype, _, _Datatype, Bad) ->
    %% allow mult is invalid but data types still match
    has_allow_mult(Bad);
immutable_dt(_, _, _, Bad) ->
    %% allow_mult and data type are invalid
    has_allow_mult(Bad) andalso has_datatype(Bad).

only_create_if_valid({Good, Bad}, New) ->
    case {last_write_wins(New), dvv_enabled(New)} of
        {true, true} ->
            case has_dvv_enabled(Bad) and has_last_write_wins(Good) of
                true ->
                    only_create_if_valid2({Good, Bad}, New);
                _ ->
                    false
            end;
        _ ->
            only_create_if_valid2({Good, Bad}, New)
    end.

has_dvv_enabled(Props) ->
    lists:keyfind(dvv_enabled, 1, Props) /= false.

has_last_write_wins(Props) ->
    lists:keyfind(last_write_wins, 1, Props) /= false.


only_create_if_valid2({Good, Bad}, New) ->
    DT = proplists:get_value(datatype, New),
    AM = proplists:get_value(allow_mult, New),
    FP = get_boolean(write_once, New),
    CS = proplists:get_value(consistent, New),
    case {DT, AM, FP, CS} of
        %% write_once true entails data type undefined and consistent false or undefined
        {_DataType, _AllowMult, true, _Consistent} ->
            not has_datatype(Good)
                andalso not is_consistent(Good)
                % NB. (!P v Q) iff P => Q
                andalso (not has_datatype(New) or has_datatype(Bad))
                andalso (not is_consistent(New) or has_consistent(Bad))
            ;
        %% if consistent or datatype properties are not defined then properties should be
        %% valid since no other properties generated can be in valid
        {undefined, _AllowMult, _WriteOnce, Consistent} when Consistent =:= false orelse
                                                 Consistent =:= undefined ->
            true;
        %% if datatype is defined, its not a consistent type and allow_mult=true
        %% then the datatype must be valid
        {Datatype, true, _WriteOnce,  Consistent} when Consistent =:= false orelse
                                          Consistent =:= undefined ->
            case lists:member(riak_kv_crdt:to_mod(Datatype), ?V2_TOP_LEVEL_TYPES) of
                true ->
                    has_datatype(Good) andalso has_allow_mult(Good);
                false ->
                    has_datatype(Bad) andalso has_allow_mult(Good)
            end;
        %% if the datatype is defined, the type is not consistent and allow_mult is false
        %% then allow_mult should be in the Bad list and the datatype may be depending on if it
        %% is valid
        {Datatype, _, _WriteOnce,  Consistent} when Consistent =:= false orelse
                                       Consistent =:= undefined->
            case lists:member(riak_kv_crdt:to_mod(Datatype), ?V2_TOP_LEVEL_TYPES) of
                true ->
                    has_allow_mult(Bad) andalso has_datatype(Good);
                false ->
                    has_datatype(Bad) andalso has_allow_mult(Bad)
            end;
        %% the type is consistent, whether it has a datatype or allow_mult set is irrelevant (for now
        %% at least)
        {_, _, _, true} ->
            has_consistent(Good);
        %% the type was not inconsistent (explicitly or implicitly) but the value is invalid
        {_, _, _, _Consistent} ->
            has_consistent(Bad)
    end.

has_datatype(Props) ->
    proplists:get_value(datatype, Props) /= undefined.

has_allow_mult(Props) ->
    proplists:get_value(allow_mult, Props) /= undefined.

valid_datatype(Props) ->
    Datatype = proplists:get_value(datatype, Props),
    lists:member(riak_kv_crdt:to_mod(Datatype), ?V2_TOP_LEVEL_TYPES).

has_w1c(Props) ->
    proplists:get_value(write_once, Props) /= undefined.

valid_w1c(Props) ->
    case proplists:get_value(write_once, Props) of
        true ->
            true;
        false ->
            true;
        _ ->
            false
    end.

is_w1c(Props) ->
    proplists:get_value(write_once, Props) =:= true.

has_consistent(Props) ->
    proplists:get_value(consistent, Props) /= undefined.

valid_consistent(Props) ->
    case proplists:get_value(consistent, Props) of
        true ->
            true;
        false ->
            true;
        _ ->
            false
    end.

is_consistent(Props) ->
    proplists:get_value(consistent, Props) =:= true.


has_n_val(Props) ->
    proplists:get_value(n_val, Props) /= undefined.

n_val_changed(Existing, New) ->
    NewN = proplists:get_value(n_val, New),
    proplists:get_value(n_val, Existing) =/= NewN andalso
        NewN =/= undefined.

has_write_once(Bad) ->
    proplists:get_value(write_once, Bad) /= undefined.

default_bucket({<<"default">>, _}) ->
    true;
default_bucket(B) when is_binary(B) ->
    true;
default_bucket(_) ->
    false.

maybe_bad_mult(error, Props) ->
    lists:keydelete(allow_mult, 1, Props);
maybe_bad_mult(_, Props) ->
    Props.

-endif.

-endif.
