%% -------------------------------------------------------------------
%%
%% riak_kv_bucket: bucket validation functions
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
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
%% @see validate_dt_props/3, assert_no_datatype/1
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
            {Unvalidated, Valid, Errors} = validate_create_dt_props(BucketProps);
        %% type may be consistent (the value may not be valid)
        Consistent ->
            {Unvalidated, Valid, Errors} = validate_create_consistent_props(Consistent, BucketProps)
    end,
    validate(Unvalidated, Valid, Errors).

%% @private update phase of bucket type. Merges properties from
%% existing with valid new properties
-spec validate_update_bucket_type(props(), props()) -> {props(), errors()}.
validate_update_bucket_type(Existing, New) ->
    case proplists:get_value(consistent, Existing) of
        %% type is explicitly or implicitly not already consistent
        Consistent when Consistent =:= false orelse
                        Consistent =:= undefined ->
            {Unvalidated, Valid, Errors} = validate_update_dt_props(Existing, New);
        _Consistent ->
            {Unvalidated, Valid, Errors} = validate_update_consistent_props(Existing, New)
    end,
    {Good, Bad} = validate(Unvalidated, Valid, Errors),
    {merge(Good, Existing), Bad}.

%% @private just delegates, but I added it to illustrate the many
%% possible type of validation.
-spec validate_update_typed_bucket(props(), props()) -> {props(), errors()}.
validate_update_typed_bucket(Existing, New) ->
    validate_update_bucket_type(Existing, New).

%% @private as far as datatypes go, default buckets are free to do as
%% they please, the datatypes API only works on typed buckets. Go
%% wild!
-spec validate_default_bucket(props(), props()) -> {props(), errors()}.
validate_default_bucket(Existing, New) ->
    Unvalidated = merge(New, Existing),
    validate(Unvalidated, [], []).

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
    {lists:keydelete(consistent, 1, New), [{consistent, true}], []};
validate_create_consistent_props(false, New) ->
    {lists:keydelete(consistent, 1, New), [{consistent, false}], []};
validate_create_consistent_props(undefined, New) ->
    {New, [], []};
validate_create_consistent_props(Invalid, New) ->
    Err = lists:flatten(io_lib:format("~p is not a valid value for consistent. use \"true\" or \"false\"", [Invalid])),
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
%% them @see riak_kv_crdt), `datatype' is an immutable property. Once
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
        {undefined, undefined} ->
            {New, [], []};
        {undefined, _Datatype} ->
            {New, [], [{datatype, "Cannot add datatype to existing bucket"}]};
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

%% @private just grab the allow_mult value if it exists
-spec allow_mult(props()) -> boolean() | 'undefined' | 'error'.
allow_mult(Props) ->
    case proplists:get_value(allow_mult, Props) of
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
                       io:format("{has_datatype, valid_datatype, allow_mult, has_consistent, valid_consistent}~n"),
                       io:format("{~p,~p,~p,~p,~p}~n~n",
                                 [has_datatype(New), valid_datatype(New), allow_mult(New), has_consistent(New), valid_consistent(New)])
                   end,
                   collect(with_title("{has_datatype, valid_datatype, allow_mult, has_consistent, valid_consistent}"),
                           {has_datatype(New), valid_datatype(New), allow_mult(New), has_consistent(New), valid_consistent(New)},
                           only_create_if_valid(Result, New)))
            end).

%% As of 2.0pre? validate/4 must merge the new and existing props,
%% verify that.
prop_merges() ->
    ?FORALL({Bucket, Existing0, New}, {gen_bucket(update, any),
                                      gen_existing(), gen_new(update)},
            begin
                %% ensure default buckets are not marked consistent since that is invalid
                Existing = case default_bucket(Bucket) of
                               true -> lists:keydelete(consistent, 1, Existing0);
                               false -> Existing0
                           end,
                Result={Good, _Bad} = validate(update, Bucket, Existing, New),

                DefaultBucket = default_bucket(Bucket),
                HasAllowMult = has_allow_mult(New),
                AllowMult = allow_mult(New),
                HasDatatype = has_datatype(Existing),
                NValChanged = n_val_changed(Existing, New),
                IsConsistent = is_consistent(Existing),
                NewConsistent = proplists:get_value(consistent, New),
                Expected = case {DefaultBucket, HasAllowMult, AllowMult, HasDatatype,
                                 NValChanged, IsConsistent, NewConsistent} of
                               %% default bucket, attempted to change consistent to invalid value
                               %% allow_mult may be invalid too
                               {true,_, Mult, _, _, false, notvalid} ->
                                   maybe_bad_mult(Mult, merge(lists:keydelete(consistent, 1, New), Existing));
                               %% default bucket, attempted to change consistent to true
                               %% allow_mult may be invalid too
                               {true, _, Mult, _, _, false, true} ->
                                   maybe_bad_mult(Mult, merge(lists:keydelete(consistent, 1, New), Existing));
                               %% all valid for default type buckets
                               {true, _, Mult,  _, _, _, _} when Mult /= error ->
                                   merge(New, Existing);
                               %% default bucket: allow mult is invalid
                               {true, true, _, _, _, _, _} ->
                                   maybe_bad_mult(error, merge(New, Existing));

                               %% typed bucket, allow mult changed but not consistent or data type
                               {false, true, Mult,  false, _, false, _} when Mult /= error ->
                                   %% the n_val is the only valid change we generate in this case. can't change
                                   %% data type or consistent peroperty
                                   NVal = proplists:get_value(n_val, New, proplists:get_value(n_val, Existing)),
                                   merge([proplists:lookup(allow_mult, New), {n_val, NVal}], Existing);
                               %% typed bucket, allow_mult change is invalid. n_val has changed. not a datatype or
                               %% consistent
                               {false, true, error, false, true, false, _} ->
                                   merge([proplists:lookup(n_val, New)], Existing);
                               %% typed bucket, allow_mult change is invalid and n_val hasn't changed
                               {false, true, error, false, false, false, _} ->
                                   Existing;
                               %% typed bucket, strongly-consistent, both n_val and consistent value are invalid changes
                               {false, _, Mult,_,true,true,false} ->
                                   maybe_bad_mult(Mult,
                                                  merge(lists:keydelete(consistent, 1, lists:keydelete(n_val, 1, New)), Existing));
                               %% typed bucket, strongly-consistent, both n_val and consistent value are invalid changes
                               {false, _, Mult,_,true,true,notvalid} ->
                                   maybe_bad_mult(Mult,
                                                  merge(lists:keydelete(consistent, 1, lists:keydelete(n_val, 1, New)), Existing));
                               %% typed bucket, strongly-consistent, n_val change is invalid
                               {false, _, Mult,_,true,true,_} ->
                                   maybe_bad_mult(Mult, merge(lists:keydelete(n_val, 1, New), Existing));
                               %% typed bucket, strongly-consistent, consistent value change is invalid
                               {false, _, Mult, _, _, true, false} ->
                                   maybe_bad_mult(Mult, merge(lists:keydelete(consistent, 1, New), Existing));
                               %% typed bucket, strongly-consistent, consistent value change is invalid
                               {false, _, Mult, _, _, true, notvalid} ->
                                   maybe_bad_mult(Mult, merge(lists:keydelete(consistent, 1, New), Existing));
                               %% typed bucket, strongly-consistent, all good (except maybe allow_mult)
                               {false, _, Mult, _, _, true, _} ->
                                   maybe_bad_mult(Mult, merge(New, Existing));
                               %% typed bucket, strongly-consistent, all good (except maybe allow_mult)
                               {false, true, _Mult, true,_, _, _} ->
                                   NVal = proplists:get_value(n_val, New, proplists:get_value(n_val, Existing)),
                                   merge([{n_val, NVal}], Existing);
                               %% typed bucket, bucket not strongly consistent or a data type, all valid
                               {false,_,_,_,_,false,_} ->
                                   merge(New, Existing)
                           end,

                ?WHENFAIL(
                   begin
                       io:format("Bucket ~p~n", [Bucket]),
                       io:format("Existing ~p~n", [lists:sort(Existing)]),
                       io:format("New ~p~n", [New]),
                       io:format("Result ~p~n", [Result]),
                       io:format("Diff ~p~n", [sets:to_list(sets:subtract(sets:from_list(Expected), sets:from_list(Good)))])
                   end,
                   sets:is_subset(sets:from_list(Expected), sets:from_list(Good)))
            end).

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
    Defaults0 = riak_core_bucket_type:defaults(),
    Defaults = lists:keydelete(allow_mult, 1, Defaults0),
    ?LET({MultDT, Consistent}, {gen_valid_mult_dt(), gen_maybe_consistent()},
         Defaults ++ MultDT ++ Consistent).

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

gen_new(update) ->
    ?LET({Mult, Datatype, Consistent, NVal},
         {gen_allow_mult(), oneof([[], gen_datatype_property()]),
          oneof([[], gen_maybe_bad_consistent()]), oneof([[], [{n_val, choose(1, 10)}]])},
         Mult ++ Datatype ++ Consistent ++ NVal);
gen_new(create) ->
    Defaults0 = riak_core_bucket_type:defaults(),
    Defaults = lists:keydelete(allow_mult, 1, Defaults0),
    ?LET({Mult, DatatypeOrConsistent}, {gen_allow_mult(), frequency([{5, gen_datatype_property()},
                                                                     {5, gen_maybe_bad_consistent()},
                                                                     {5, []}])},
         Defaults ++ Mult ++ DatatypeOrConsistent).

gen_allow_mult() ->
    ?LET(Mult, frequency([{9, bool()}, {1, binary()}]), [{allow_mult, Mult}]).

gen_datatype_property() ->
    ?LET(Datattype, oneof([gen_datatype(), notadatatype]), [{datatype, Datattype}]).

gen_datatype() ->
    ?LET(Datamod, oneof(?V2_TOP_LEVEL_TYPES), riak_kv_crdt:from_mod(Datamod)).
%% helpers

immutable(create, _,  _, _) ->
    true;
immutable(_, _New, undefined, _) ->
    true;
immutable(update, New, Existing, {_Good, Bad}) ->
    case proplists:get_value(consistent, Existing) of
        Consistent when Consistent =:= false orelse
                        Consistent =:= undefined ->
            %% not an existing consistent type (or bucket) so validate it
            %% as a datatype (immutable_dt also covers the case where it was
            %% not a datatype, yes i know this is kind of wierd when you read the
            %% test, sorry...)
            NewDT = proplists:get_value(datatype, New),
            NewAM = proplists:get_value(allow_mult, New),
            ExistingDT = proplists:get_value(datatype, Existing),
            immutable_dt(NewDT, NewAM, ExistingDT, Bad);
        true ->
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


%% If data type and allow mult and are in New they must match what is in existing
%% or be in Bad
immutable_dt(undefined, undefined, _Meh1, _Bad) ->
    %% datatype and allow_mult are not being modified, so its valid
    true;
immutable_dt(undefined, _, undefined, _Bad) ->
    %% data type not in new or existing, so its valid
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
immutable_dt(undefined, false, _Meh, Bad) ->
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
    DT = proplists:get_value(datatype, New),
    AM = proplists:get_value(allow_mult, New),
    CS = proplists:get_value(consistent, New),
    case {DT, AM, CS} of
        %% if consistent or datatype properties are not defined then properties should be
        %% valid since no other properties generated can be in valid
        {undefined, _AllowMult, Consistent} when Consistent =:= false orelse
                                                 Consistent =:= undefined ->
            true;
        %% if datatype is defined, its not a consistent type and allow_mult=true
        %% then the datatype must be valid
        {Datatype, true, Consistent} when Consistent =:= false orelse
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
        {Datatype, _, Consistent} when Consistent =:= false orelse
                                       Consistent =:= undefined->
            case lists:member(riak_kv_crdt:to_mod(Datatype), ?V2_TOP_LEVEL_TYPES) of
                true ->
                    has_allow_mult(Bad) andalso has_datatype(Good);
                false ->
                    has_datatype(Bad) andalso has_allow_mult(Bad)
            end;
        %% the type is consistent, whether it has a datatype or allow_mult set is irrelevant (for now
        %% at least)
        {_, _, true} ->
            has_consistent(Good);
        %% the type was not inconsistent (explicitly or implicitly) but the value is invalid
        {_, _, _Consistent} ->
            has_consistent(Bad)
    end.

has_datatype(Props) ->
    proplists:get_value(datatype, Props) /= undefined.

has_allow_mult(Props) ->
    proplists:get_value(allow_mult, Props) /= undefined.

valid_datatype(Props) ->
    Datatype = proplists:get_value(datatype, Props),
    lists:member(riak_kv_crdt:to_mod(Datatype), ?V2_TOP_LEVEL_TYPES).

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
