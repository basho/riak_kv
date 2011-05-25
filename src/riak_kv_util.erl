%% -------------------------------------------------------------------
%%
%% riak_util: functions that are useful throughout Riak
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
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


%% @doc Various functions that are useful throughout riak_kv.
-module(riak_kv_util).


-export([is_x_deleted/1,
         obj_not_deleted/1,
         expand_value/3,
         expand_rw_value/4,
         normalize_rw_value/2,
         make_request/2]).

-include_lib("riak_kv_vnode.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% ===================================================================
%% Public API
%% ===================================================================

%% @spec is_x_deleted(riak_object:riak_object()) -> boolean()
%% @doc 'true' if all contents of the input object are marked
%%      as deleted; 'false' otherwise
%% @equiv obj_not_deleted(Obj) == undefined
is_x_deleted(Obj) ->
    case obj_not_deleted(Obj) of
        undefined -> true;
        _ -> false
    end.

%% @spec obj_not_deleted(riak_object:riak_object()) ->
%%          undefined|riak_object:riak_object()
%% @doc Determine whether all contents of an object are marked as
%%      deleted.  Return is the atom 'undefined' if all contents
%%      are marked deleted, or the input Obj if any of them are not.
obj_not_deleted(Obj) ->
    case [{M, V} || {M, V} <- riak_object:get_contents(Obj),
                    dict:is_key(<<"X-Riak-Deleted">>, M) =:= false] of
        [] -> undefined;
        _ -> Obj
    end.

-spec make_request(vnode_req(), partition()) -> #riak_vnode_req_v1{}.
make_request(Request, Index) ->
    riak_core_vnode_master:make_request(Request,
                                        {fsm, undefined, self()},
                                        Index).

get_bucket_option(Type, BucketProps) ->
    case proplists:get_value(Type, BucketProps, default) of
        default ->
            {ok, DefaultProps} = application:get_env(riak_core, default_bucket_props),
            proplists:get_value(Type, DefaultProps, error);
        Val -> Val
    end.

expand_value(Type, default, BucketProps) ->
    get_bucket_option(Type, BucketProps);
expand_value(_Type, Value, _BucketProps) ->
    Value.

expand_rw_value(Type, default, BucketProps, N) ->
    normalize_rw_value(get_bucket_option(Type, BucketProps), N);
expand_rw_value(_Type, Val, _BucketProps, N) ->
    normalize_rw_value(Val, N).

normalize_rw_value(RW, _N) when is_integer(RW) -> RW;
normalize_rw_value(RW, N) when is_binary(RW) ->
    try
        ExistingAtom = binary_to_existing_atom(RW, utf8),
        normalize_rw_value(ExistingAtom, N)
    catch _:badarg ->
        error
    end;
normalize_rw_value(one, _N) -> 1;
normalize_rw_value(quorum, N) -> erlang:trunc((N/2)+1);
normalize_rw_value(all, N) -> N;
normalize_rw_value(_, _) -> error.


%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

normalize_test() ->
    3 = normalize_rw_value(3, 3),
    1 = normalize_rw_value(one, 3),
    2 = normalize_rw_value(quorum, 3),
    3 = normalize_rw_value(all, 3),
    1 = normalize_rw_value(<<"one">>, 3),
    2 = normalize_rw_value(<<"quorum">>, 3),
    3 = normalize_rw_value(<<"all">>, 3),
    error = normalize_rw_value(garbage, 3),
    error = normalize_rw_value(<<"garbage">>, 3).


deleted_test() ->
    O = riak_object:new(<<"test">>, <<"k">>, "v"),
    false = is_x_deleted(O),
    MD = dict:new(),
    O1 = riak_object:apply_updates(
           riak_object:update_metadata(
             O, dict:store(<<"X-Riak-Deleted">>, true, MD))),
    true = is_x_deleted(O1).

-endif.
