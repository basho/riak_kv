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

-type prop() :: {PropName::atom(), PropValue::any()}.
-type error() :: {PropName::atom(), ErrorReason::atom()}.
-type props() :: [prop()].
-type errors() :: [error()].

-spec validate(create | update,
               {riak_core_bucket_type:bucket_type(), undefined | binary()} | binary(),
               undefined | props(),
               props()) -> {props(), errors()}.
validate(_CreateOrUpdate, _Bucket, _Existing, BucketProps) when is_list(BucketProps) ->
    lager:debug("Was called ~p~n", [BucketProps]),
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
validate([{IntProp, MaybeInt}=Prop | T], ValidProps, Errors) when is_atom(IntProp), IntProp =:= big_vclock
                                                                  orelse IntProp =:= n_val
                                                                  orelse IntProp =:= old_vclock
                                                                  orelse IntProp =:= small_vclock ->
case is_integer(MaybeInt) of
    true when MaybeInt > 0 ->
        validate(T, [Prop | ValidProps], Errors);
    _ ->
        validate(T, ValidProps, [{IntProp, not_integer} | Errors])
end;
validate([{QProp, MaybeQ}=Prop | T], ValidProps, Errors) when is_atom(QProp), QProp =:= dw
                                                              orelse QProp =:= pw
                                                              orelse QProp =:= pr
                                                              orelse QProp =:= r
                                                              orelse QProp =:= rw
                                                              orelse QProp =:= w ->
    case is_quorum(MaybeQ) of
        true ->
            validate(T, [Prop | ValidProps], Errors);
        false ->
            validate(T, ValidProps, [{QProp, not_valid_quorum} | Errors])
    end;
validate([Prop|T], ValidProps, Errors) ->
    validate(T, [Prop|ValidProps], Errors).


-spec is_quorum(term()) -> true | false.
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
