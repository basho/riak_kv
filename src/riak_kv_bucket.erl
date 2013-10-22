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

-include_lib("eunit/include/eunit.hrl").
-include("riak_kv_types.hrl").

-type prop() :: {PropName::atom(), PropValue::any()}.
-type error() :: {PropName::atom(), ErrorReason::atom()}.
-type props() :: [prop()].
-type errors() :: [error()].

-spec validate(create | update,
               {riak_core_bucket_type:bucket_type(), undefined | binary()} | binary(),
               undefined | props(),
               props()) -> {props(), errors()}.
validate(CreateOrUpdate, {_T, _N}, Existing0, BucketProps) when is_list(BucketProps) ->
    Existing = case Existing0 of
                   undefined -> [];
                   _ -> Existing0
               end,
    validate_dt_props(CreateOrUpdate, Existing, BucketProps);
validate(_CreateOrUpdate, _Bucket, _Existing, BucketProps) when is_list(BucketProps) ->
    validate(BucketProps, [], []).

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
validate([Prop|T], ValidProps, Errors) ->
    validate(T, [Prop|ValidProps], Errors).

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

%% @private riak datatype support requires a bucket type of `datatype'
%% and `allow_mult' set to `true'. This function enforces those
%% _before_ calling the usual validate code, since the two properties
%% are interdependant. We take the presence of a `datatype' property
%% as indication that this bucket type is a special type, somewhere to
%% store CRDTs. I realise this somewhat undermines the reason for
%% bucket types (no magic names) but there has to be some way to
%% indicate intent, and that way is the "special" property name
%% `datatype'.
%%
%% Since we don't ever want sibling CRDT types (though we can handle
%% them @see riak_kv_crdt), `datatype' is an immutable property. Once
%% you create a bucket with a certain datatype you can't change
%% it. The `update' bucket type path enforces this. It doesn't
%% validate the type, since it assumes that was done at creation.
-spec validate_dt_props(create | update, props(), props()) -> {props(), errors()}.
validate_dt_props(create, Existing, Props) ->
    validate_data_type(proplists:get_value(datatype, Props), Existing, Props);
validate_dt_props(update, Existing, Props) ->
    case proplists:get_value(datatype, Existing) of
        undefined ->
            validate_dt_props(create, Existing, Props);
        DType ->
            case proplists:get_value(datatype, Props) of
                undefined ->
                    validate_allow_mult(Existing, Props, [], []);
                DType ->
                    validate_allow_mult(Existing, Props, [{datatype, DType}], []);
                Other ->
                    Err = lists:flatten(io_lib:format("Cannot change datatype from ~p to ~p", [DType, Other])),
                    validate_allow_mult(Existing, Props, [], [{datatype, Err}])
            end
    end.

validate_allow_mult(Existing, Props, Valid, Errors) ->
    case allow_mult(allow_mult(Existing), allow_mult(Props)) of
        true ->
            validate(lists:keydelete(allow_mult, 1, Props), [{allow_mult, true} | Valid], Errors);
        false ->
            Err = io_lib:format("Data Type buckets must be allow_mult=true", []),
            validate(lists:keydelete(allow_mult, 1,  Props), Valid, [{allow_mult, Err} | Errors])
    end.

validate_data_type(undefined, _Existing, Props) ->
    validate(Props, [], []);
validate_data_type(DType, Existing, Props0) ->
    Props = lists:keydelete(datatype, 1, Props0),
    Mod = riak_kv_crdt:to_mod(DType),
    case lists:member(Mod, ?V2_TOP_LEVEL_TYPES) of
        true ->
            validate_allow_mult(Existing, Props, [{datatype, DType}], []);
        false ->
            Err = lists:flatten(io_lib:format("~p not supported for bucket datatype property", [DType])),
            validate_allow_mult(Existing, Props, [], [{datatype, Err}])
    end.

allow_mult(true, undefined) ->
    true;
allow_mult(_, true) ->
    true;
allow_mult(_, _) ->
    false.

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

-endif.
