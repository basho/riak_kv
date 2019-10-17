%% Riak EnterpriseDS
%% Copyright (c) 2007-2014 Basho Technologies, Inc.  All Rights Reserved.
-module(riak_repl_bucket_type_util).

%% @doc Utility functions for interacting with bucket types

-export([bucket_props_match/1,
         bucket_props_match/2,
         is_bucket_typed/1,
         prop_get/3,
         property_hash/1]).

-include("riak_repl.hrl").

-define(DEFAULT_BUCKET_TYPE, <<"default">>).

-spec bucket_props_match(proplists:proplist()) -> boolean().
bucket_props_match(Props) ->
    case is_bucket_typed(Props) of
        true ->
            Type = prop_get(?BT_META_TYPE, ?DEFAULT_BUCKET_TYPE, Props),
            property_hash(Type)  =:= prop_get(?BT_META_PROPS_HASH, undefined, Props);
        false ->
            %% This is not a typed bucket. Check if the remote
            %% side is also untyped.
            undefined =:= prop_get(?BT_META_PROPS_HASH, undefined, Props)
    end.

-spec bucket_props_match(binary(), integer()) -> boolean().
bucket_props_match(Type, RemoteBucketTypeHash) ->
   property_hash(Type) =:= RemoteBucketTypeHash.

-spec is_bucket_typed({error, no_type} | proplists:proplist()) -> boolean().
is_bucket_typed({error, no_type}) ->
    false;
is_bucket_typed(Props) ->
    prop_get(?BT_META_TYPED_BUCKET, false, Props).

-spec prop_get(atom() | binary(), term(), {error, no_type} | proplists:proplist()) -> term().
prop_get(_Key, Default, {error, no_type}) ->
    Default;
prop_get(Key, Default, Props) ->
    case lists:keyfind(Key, 1, Props) of
        {Key, Value} ->
            Value;
        false ->
            Default
    end.

-spec property_hash(binary()) -> undefined | integer().
property_hash(undefined) ->
    undefined;
property_hash(Type) ->
    Defaults = riak_core_capability:get(
            {riak_repl, default_bucket_props_hash},
            [consistent, datatype, n_val, allow_mult, last_write_wins]),
    riak_core_bucket_type:property_hash(Type, Defaults).
