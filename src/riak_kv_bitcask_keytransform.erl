%% -------------------------------------------------------------------
%%
%% riak_kv_bitcask_keytranform: Bitcask Driver for Riak
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

-module(riak_kv_bitcask_keytransform).

-export([get_key_transform_fun/1, make_bk/3, bk_to_tuple/1]).

%% must not be 131, otherwise will match t2b in error
%% yes, I know that this is horrible.
-define(VERSION_1, 1).
-define(VERSION_BYTE, ?VERSION_1).
-define(CURRENT_KEY_TRANS, fun key_transform_to_1/1).


%% @doc Transformation functions for the keys coming off the disk.
key_transform_to_1(<<?VERSION_1:7, _:1, _Rest/binary>> = Key) ->
    Key;
key_transform_to_1(<<131:8,_Rest/bits>> = Key0) ->
    {Bucket, Key} = binary_to_term(Key0),
    make_bk(?VERSION_BYTE, Bucket, Key).

key_transform_to_0(<<?VERSION_1:7,_Rest/bits>> = Key0) ->
    term_to_binary(bk_to_tuple(Key0));
key_transform_to_0(<<131:8,_Rest/binary>> = Key) ->
    Key.

bk_to_tuple(<<?VERSION_1:7, HasType:1, Sz:16/integer,
             TypeOrBucket:Sz/bytes, Rest/binary>>) ->
    case HasType of
        0 ->
            %% no type, first field is bucket
            {TypeOrBucket, Rest};
        1 ->
            %% has a tyoe, extract bucket as well
            <<BucketSz:16/integer, Bucket:BucketSz/bytes, Key/binary>> = Rest,
            {{TypeOrBucket, Bucket}, Key}
    end;
bk_to_tuple(<<131:8,_Rest/binary>> = BK) ->
    binary_to_term(BK).

make_bk(0, Bucket, Key) ->
    term_to_binary({Bucket, Key});
make_bk(1, {Type, Bucket}, Key) ->
    TypeSz = size(Type),
    BucketSz = size(Bucket),
    <<?VERSION_BYTE:7, 1:1, TypeSz:16/integer, Type/binary,
      BucketSz:16/integer, Bucket/binary, Key/binary>>;
make_bk(1, Bucket, Key) ->
    BucketSz = size(Bucket),
    <<?VERSION_BYTE:7, 0:1, BucketSz:16/integer,
     Bucket/binary, Key/binary>>.

get_key_transform_fun(Config0) ->
    case app_helper:get_prop_or_env(small_keys, Config0, bitcask) of
        false ->
            C0 = proplists:delete(small_keys, Config0),
            C1 = C0 ++ [{key_transform, fun key_transform_to_0/1}],
            {C1, 0};
        _ ->
            C0 = proplists:delete(small_keys, Config0),
            C1 = C0 ++ [{key_transform, ?CURRENT_KEY_TRANS}],
            {C1, ?VERSION_BYTE}
    end.
