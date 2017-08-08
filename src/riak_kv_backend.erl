%% -------------------------------------------------------------------
%%
%% riak_kv_backend: Riak backend behaviour
%%
%% Copyright (c) 2007-2015 Basho Technologies, Inc.  All Rights Reserved.
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

-module(riak_kv_backend).

-export([callback_after/3]).

-type fold_buckets_fun() :: fun((binary(), any()) -> any() | no_return()).
-type fold_keys_fun() :: fun((binary(), binary(), any()) -> any() |
                                                            no_return()).
-type fold_objects_fun() :: fun((binary(), binary(), term(), any()) ->
                                       any() |
                                       no_return()).

-type index_spec() :: {add | remove, binary(), riak_object:index_value()}.

-export_type([fold_buckets_fun/0,
              fold_keys_fun/0,
              fold_objects_fun/0,
              fold_opts/0,
              index_spec/0]).

%% These are just here to make the callback specs more succinct and readable
-type state() :: term().
-type fold_acc() :: term().
-type fold_opts() :: [term()]. %% TODO maybe more specific? [{atom(), term()}]?
-type fold_result() :: {ok, fold_acc()} | {async, fun()} | {error, term()}.

-callback api_version() -> {ok, number()}.

-callback capabilities(state()) -> {ok, [atom()]}.
-callback capabilities(riak_object:bucket(), state()) -> {ok, [atom()]}.

-callback start(PartitionIndex :: non_neg_integer(), Config :: [{atom(), term()}]) ->
    {ok, state()} | {error, term()}.
-callback stop(state()) -> ok.

-callback get(riak_object:bucket(), riak_object:key(), state()) ->
    {ok, Value :: term(), state()} |
    {ok, not_found, state()} |
    {error, term(), state()}.
-callback put(riak_object:bucket(), riak_object:key(), [index_spec()], Value :: binary(),
              state()) ->
    {ok, state()} |
    {error, term(), state()}.
-callback delete(riak_object:bucket(), riak_object:key(), [index_spec()], state()) ->
    {ok, state()} |
    {error, term(), state()}.

-callback drop(state()) -> {ok, state()} | {error, term(), state()}.

-callback fold_buckets(fold_buckets_fun(), fold_acc(), fold_opts(), state()) -> fold_result().
-callback fold_keys(fold_keys_fun(), fold_acc(), fold_opts(), state()) -> fold_result().
-callback fold_objects(fold_objects_fun(), fold_acc(), fold_opts(), state()) -> fold_result().

-callback is_empty(state()) -> boolean() | {error, term()}.

-callback status(state()) -> [{atom(), term()}].

-callback callback(reference(), Msg :: term(), state()) -> {ok, state()}.

%% Queue a callback for the backend after Time ms.
-spec callback_after(integer(), reference(), term()) -> reference().
callback_after(Time, Ref, Msg) when is_integer(Time), is_reference(Ref) ->
    riak_core_vnode:send_command_after(Time, {backend_callback, Ref, Msg}).
