%% -------------------------------------------------------------------
%%
%% Copyright (c) 2017 Basho Technologies, Inc.  All Rights Reserved.
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

-module(riak_kv_group_list).
-export([fold_objects/6,
         to_group_params/1,
         get_prefix/1,
         get_delimiter/1,
         get_start_after/1,
         get_max_keys/1,
         get_continuation_token/1,
         set_prefix/2,
         set_delimiter/2,
         set_start_after/2,
         set_max_keys/2,
         set_continuation_token/2]).
-export_type([group_params/0]).

-record(group_params, {
          backend_mod :: module(),
          prefix :: binary(),
          delimiter :: binary(),
          start_after :: binary(),
          max_keys :: pos_integer(),
          continuation_token :: binary()
         }).

-opaque group_params() :: #group_params{}.

-spec fold_objects(module(), riak_kv_backend:fold_objects_fun(), riak_kv_fold_buffer:buffer(), proplists:proplist(), riak_kv_backend:fold_opts(), any()) ->
    {ok, any()} | {async, fun()}.
fold_objects(BackendMod, FoldFun, Acc, Opts, FoldOpts, DbRef) ->
    Bucket = proplists:get_value(bucket, Opts),
    GroupParams = proplists:get_value(group_params, Opts),
    GroupParams0 = GroupParams#group_params{backend_mod = BackendMod},
    Async = proplists:get_bool(async_fold, Opts),
    case Async of
        true ->
            {async, content_folder_fun(Bucket, GroupParams0, DbRef, FoldOpts, FoldFun, Acc)};
        false ->
            {ok, content_folder(Bucket, GroupParams0, DbRef, FoldOpts, FoldFun, Acc)}
    end.

to_group_params(PropList) ->
    #group_params{
       prefix=to_binary(value_or_default(prefix, PropList, undefined)),
       delimiter=to_binary(value_or_default(delimiter, PropList, undefined)),
       start_after=to_binary(value_or_default(start_after, PropList, undefined)),
       max_keys=to_integer(value_or_default(max_keys, PropList, 1000)),
       continuation_token=to_binary(value_or_default(continuation_token, PropList, undefined))
      }.

value_or_default(Key, PropList, Default)->
    case proplists:get_value(Key, PropList) of
        <<"">> ->
            Default;
        "" ->
            Default;
        undefined ->
            Default;
        Value ->
            Value
    end.

to_binary(undefined) ->
    undefined;
to_binary(Param) when is_list(Param) ->
    list_to_binary(Param);
to_binary(Param) when is_binary(Param) ->
    Param.

to_integer(undefined) ->
    undefined;
to_integer(Param) when is_list(Param) ->
    list_to_integer(Param);
to_integer(Param) when is_binary(Param) ->
    binary_to_integer(Param);
to_integer(Param) when is_integer(Param) ->
    Param.

get_backend(#group_params{backend_mod = BackendMod}) ->
    BackendMod.

get_prefix(#group_params{prefix = Prefix}) ->
    Prefix.

get_delimiter(#group_params{delimiter = Delimiter}) ->
    Delimiter.

get_start_after(#group_params{start_after = StartAfter}) ->
    StartAfter.

get_max_keys(#group_params{max_keys = MaxKeys}) ->
    MaxKeys.

get_continuation_token(#group_params{continuation_token = ContinuationToken}) ->
    ContinuationToken.

-spec set_prefix(group_params(), Prefix::binary()) -> group_params().
set_prefix(GroupParams, Prefix) ->
    GroupParams#group_params{prefix = Prefix}.

-spec set_delimiter(group_params(), Delimiter::binary()) -> group_params().
set_delimiter(GroupParams, Delimiter) ->
    GroupParams#group_params{delimiter = Delimiter}.

-spec set_start_after(group_params(), StartAfter::binary()) -> group_params().
set_start_after(GroupParams, StartAfter) ->
    GroupParams#group_params{start_after = StartAfter}.

-spec set_max_keys(group_params(), MaxKeys::pos_integer()) -> group_params().
set_max_keys(GroupParams, MaxKeys) ->
    GroupParams#group_params{max_keys = MaxKeys}.

-spec set_continuation_token(group_params(), ContinuationToken::binary()) ->
    group_params().
set_continuation_token(GroupParams, ContinuationToken) ->
    GroupParams#group_params{continuation_token = ContinuationToken}.

-spec content_folder_fun(riak_core_bucket:bucket(), group_params(), any(), list(), riak_kv_backend:fold_objects_fun(), riak_kv_fold_buffer:buffer()) ->
    fun(() -> any()).
content_folder_fun(Bucket, GroupParams, DbRef, FoldOpts, FoldFun, Acc) ->
    fun() ->
        content_folder(Bucket, GroupParams, DbRef, FoldOpts, FoldFun, Acc)
    end.

-spec content_folder(riak_core_bucket:bucket(), group_params(), eleveldb:db_ref(), list(), riak_kv_backend:fold_objects_fun(), riak_kv_fold_buffer:buffer()) ->
    riak_kv_fold_buffer:buffer().
content_folder(Bucket, GroupParams, DbRef, FoldOpts, FoldFun, Acc) when is_function(FoldFun, 4) ->
    BackendMod = get_backend(GroupParams),
    FoldOpts1 = [{first_key, BackendMod:to_first_key({bucket, Bucket})} | FoldOpts],
    try iterator_open(BackendMod, DbRef, FoldOpts1) of
        {ok, Itr} ->
            iterate(Bucket, GroupParams, Itr, FoldFun, Acc)
    catch Error ->
        lager:debug("Could not open iterator: ~p", [Error]),
        throw(Error)
    end.

iterator_open(BackendMod, DbRef, FoldOpts) ->
    BackendMod:iterator_open(DbRef, FoldOpts).

iterator_close(BackendMod, Itr) ->
    BackendMod:iterator_close(Itr).

iterator_move(BackendMod, Itr, Pos) ->
    BackendMod:iterator_move(Itr, Pos).

iterate(Bucket, GroupParams, Itr, FoldFun, Acc) ->
    Outcome = enumerate(undefined, Bucket, GroupParams, Itr, FoldFun, Acc),
    iterator_close(get_backend(GroupParams), Itr),
    case Outcome of
        {error, _Err} = Error ->
            throw(Error);
        {break, _Res} = Result ->
            throw(Result);
        Result -> Result
    end.

enumerate(PrevEntry, Bucket, GroupParams, Itr, FoldFun, Acc) ->
    Pos = next_pos(PrevEntry, Bucket, GroupParams),
    case Pos of
        iteration_complete ->
            Acc;
        _ ->
            BackendMod = get_backend(GroupParams),
            try iterator_move(BackendMod, Itr, Pos) of
                {error, invalid_iterator} ->
                    lager:debug( "invalid_iterator.  reached end.", []),
                    Acc;
                {error, iterator_closed} ->
                    lager:debug( "iterator_closed.", []),
                    Acc;
                {ok, BinaryBKey, BinaryValue} ->
                    BackendMod = BackendMod,
                    BKey = BackendMod:from_object_key(BinaryBKey),
                    maybe_accumulate({BKey, BinaryValue}, PrevEntry, Bucket, GroupParams, FoldFun, Acc, Itr)
            catch Error ->
                    {error, {eleveldb_error, Error}}
            end
    end.

start_pos(Bucket,
          GroupParams = #group_params{prefix=Prefix,
                                      start_after=StartAfter,
                                      continuation_token=ContinuationToken}) ->
    StartKey = start_key(Prefix, StartAfter, ContinuationToken),
    to_object_key(GroupParams, Bucket, StartKey).

-spec start_key(Prefix :: binary() | undefined,
                StartAfter :: binary() | undefined,
                ContinuationToken :: riak_kv_continuation:token() | undefined) ->
    binary().
start_key(Prefix, StartAfter, ContinuationToken) ->
    case riak_kv_continuation:decode_token(ContinuationToken) of
        undefined ->
            start_key(Prefix, StartAfter);
        Token ->
            start_key(Prefix, Token)
    end.

-spec start_key(Prefix :: binary() | undefined, StartAfter :: binary() | undefined) ->
    binary().
start_key(undefined, undefined) ->
    <<"">>;
start_key(Prefix, undefined) ->
    Prefix;
start_key(undefined, StartAfter) ->
    append_max_byte(StartAfter);
start_key(Prefix, StartAfter) when Prefix > StartAfter ->
    Prefix;
start_key(Prefix, StartAfter) when Prefix =< StartAfter ->
    append_max_byte(StartAfter).

next_pos(undefined, Bucket, GroupParams) ->
    start_pos(Bucket, GroupParams);
next_pos({Bucket, PrevKey}, _Bucket, GroupParams =  #group_params{prefix=undefined}) ->
    next_pos_after_key(GroupParams, Bucket, PrevKey);
next_pos({Bucket, PrevKey}, _Bucket, GroupParams =  #group_params{prefix=Prefix}) ->
    case is_prefix(Prefix, PrevKey) of
        true ->
            next_pos_after_key(GroupParams, Bucket, PrevKey);
        _ ->
            iteration_complete
    end.

next_pos_after_key(#group_params{prefix = undefined, delimiter = undefined}, _Bucket, _Key) ->
    next;
next_pos_after_key(GroupParams = #group_params{prefix = Prefix, delimiter = Delimiter}, Bucket, Key) ->
    case common_prefix(Key, Prefix, Delimiter) of
        undefined ->
            next;
        CommonPrefix ->
            to_object_key(GroupParams, Bucket, append_max_byte(CommonPrefix))
    end.

is_prefix(B1, B2) when is_binary(B1), is_binary(B2) ->
    binary:longest_common_prefix([B1, B2]) == size(B1);
is_prefix(_B1, _B2) ->
    false.

to_object_key(GroupParams, Bucket, Key) ->
    BackendMod = get_backend(GroupParams),
    BackendMod:to_object_key(Bucket, Key).

maybe_accumulate({undefined, _BinaryValue}, _PrevEntry, _Bucket, _GroupParams, _FoldFun, Acc, _Itr) ->
    Acc;
maybe_accumulate({ignore, _BinaryValue}, _PrevEntry, _Bucket, _GroupParams, _FoldFun, Acc, _Itr) ->
    lager:error("Encountered corrupt key while iterating entries. Grouped key list may be incomplete."),
    Acc;
maybe_accumulate(PrevEntry, PrevEntry, _Bucket, _GroupParams, _FoldKeysFun, _Acc, _Itr) ->
    {error, did_not_skip_to_next_entry};
maybe_accumulate({{Bucket, _Key}, _BinaryValue} = Entry, _PrevEntry, Bucket, #group_params{prefix = undefined} = GroupParams, FoldFun, Acc, Itr) ->
    maybe_limit_accumulate(Entry, Bucket, GroupParams, FoldFun, Acc, Itr);
maybe_accumulate(_Entry, _PrevEntry, _Bucket, #group_params{prefix=undefined}=_GroupParams, _FoldFun, Acc, _Itr) ->
    Acc;
maybe_accumulate({{Bucket, Key}, _BinaryValue} = Entry, _PrevEntry, Bucket, #group_params{prefix = Prefix}=GroupParams, FoldFun, Acc, Itr) ->
    case Prefix =/= undefined andalso is_prefix(Prefix, Key) of
        true ->
            maybe_limit_accumulate(Entry, Bucket, GroupParams, FoldFun, Acc, Itr);
        false ->
            Acc
    end;
maybe_accumulate({{_DifferentBucket, _Key}, _BinaryValue}, _PrevEntry, _Bucket, _GroupParams, _FoldFun, Acc, _Itr) ->
    Acc.

%% this is a maybe_accumulate but we can't pattern match into Accumulator size
maybe_limit_accumulate(Entry, Bucket, #group_params{max_keys = MaxKeys}=GroupParams, FoldFun, Acc, Itr) ->
    case riak_kv_fold_buffer:size(Acc) =< MaxKeys of
        true ->
            accumulate(Entry, Bucket, GroupParams, FoldFun, Acc, Itr);
        false -> Acc
    end.

%% Note that the two instances of `TargetBucket' below are intentional so that we accumulate only when
%% we match the bucket that we're looking for.
accumulate({{TargetBucket, Key}=BKey, BinaryValue},
           TargetBucket,
           GroupParams,
           FoldFun,
           Acc,
           Itr) ->
    NewAcc = try
                 PrefixOrMeta = common_prefix_or_metadata(BKey, BinaryValue, GroupParams),
                 FoldFun(TargetBucket, Key, PrefixOrMeta, Acc)
             catch Error ->
                 FoldFun(TargetBucket, Key, {error, Error}, Acc)
             end,
    enumerate(BKey, TargetBucket, GroupParams, Itr, FoldFun, NewAcc).

common_prefix_or_metadata({_Bucket, Key} = BKey,
                          BinaryValue,
                          #group_params{prefix = Prefix, delimiter = Delimiter}) ->

    case common_prefix(Key, Prefix, Delimiter) of
        undefined -> {metadata, extract_metadata(BKey, BinaryValue)};
        CommonPrefix -> {common_prefix, CommonPrefix}
    end.

common_prefix(_Key, _Prefix, undefined) ->
    undefined;
common_prefix(_Key, _Prefix, <<>>) ->
    undefined;
common_prefix(Key, undefined, Delimiter) ->
    common_prefix(Key, <<>>, Delimiter);
common_prefix(Key, Prefix, Delimiter) ->
    Index = binary:longest_common_prefix([Key, Prefix]),
    KeySansPrefix = binary_part(Key, Index, byte_size(Key) - Index),
    Parts = binary:split(KeySansPrefix, Delimiter, [global]),
    case length(Parts) > 1 andalso lists:all(fun(X) -> X =/= <<>> end, Parts) of
        true ->
            CommonPrefix = hd(Parts),
            <<Prefix/binary, CommonPrefix/binary, Delimiter/binary>>;
        false ->
            undefined
    end.

extract_metadata({Bucket, Key} = _BKey,
                 BinaryValue) ->
    RObj = riak_object:from_binary(Bucket, Key, BinaryValue),
    Contents = riak_object:get_contents(RObj),
    case Contents of
        [_Content] ->
            riak_object:get_metadata(RObj);
        _ ->
            %% TODO
            {error, riak_object_has_siblings}
    end.

%% 0xFF is the biggest byte for the purposes of sorting in LevelDB,
%% used for seeking past all keys starting with the prefix.
-spec append_max_byte(binary()) -> binary().
append_max_byte(Binary) when is_binary(Binary) ->
    <<Binary/binary, 16#FF>>.

%% ====================
%% TESTS
%% ====================

-ifdef(TEST).

-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

test_group_params(Prefix, Delimiter) ->
    Result = to_group_params([{prefix, Prefix}, {delimiter, Delimiter}]),
    Result#group_params{backend_mod = riak_kv_eleveldb_backend}.

expected_next_pos(Bucket, Key) ->
    riak_kv_eleveldb_backend:to_object_key(Bucket, append_max_byte(Key)).

common_prefix_test() ->
    Key = <<"foo/bar/baz">>,
    Prefix = <<"foo/">>,
    Delimiter = <<"/">>,
    Expected = <<"foo/bar/">>,
    Result = common_prefix(Key, Prefix, Delimiter),
    ?assertEqual(Expected, Result).

common_prefix_with_no_delimiter_test() ->
    Key = <<"foo/bar">>,
    Prefix = <<"foo/">>,
    Delimiter = <<"/">>,
    Expected = undefined,
    Result = common_prefix(Key, Prefix, Delimiter),
    ?assertEqual(Expected, Result).

common_prefix_with_key_equal_prefix_test() ->
    Key = <<"foo/">>,
    Prefix = <<"foo/">>,
    Delimiter = <<"/">>,
    Expected = undefined,
    Result = common_prefix(Key, Prefix, Delimiter),
    ?assertEqual(Expected, Result).

common_prefix_with_invalid_terminating_delimiter_test() ->
    Key = <<"foo/bar/">>,
    Prefix = <<"foo/">>,
    Delimiter = <<"/">>,
    Expected = undefined,
    Result = common_prefix(Key, Prefix, Delimiter),
    ?assertEqual(Expected, Result).

common_prefix_with_empty_segment_test() ->
    Key = <<"foo//bar/baz">>,
    Prefix = <<"foo/">>,
    Delimiter = <<"/">>,
    Expected = undefined,
    Result = common_prefix(Key, Prefix, Delimiter),
    ?assertEqual(Expected, Result).

common_prefix_with_empty_prefix_test() ->
    Key = <<"foo/bar/baz">>,
    Prefix = <<"">>,
    Delimiter = <<"/">>,
    Expected = <<"foo/">>,
    Result = common_prefix(Key, Prefix, Delimiter),
    ?assertEqual(Expected, Result).

common_prefix_with_multi_char_delimiter_test() ->
    Key = <<"foo::bar::baz">>,
    Prefix = <<"foo::">>,
    Delimiter = <<"::">>,
    Expected = <<"foo::bar::">>,
    Result = common_prefix(Key, Prefix, Delimiter),
    ?assertEqual(Expected, Result).

common_prefix_with_emoji_delimiter_test() ->
    Key = <<"foo🤔bar🤔baz">>,
    Prefix = <<"foo🤔">>,
    Delimiter = <<"🤔">>,
    Expected = <<"foo🤔bar🤔">>,
    Result = common_prefix(Key, Prefix, Delimiter),
    ?assertEqual(Expected, Result).

common_prefix_or_metadata_returns_metadata_test() ->
    Bucket = {<<"bucket_type">>, <<"bucket">>},
    Key  = <<"foo/actual_object">>,
    BKey = {Bucket, Key},
    RObj = riak_object:new(Bucket, Key,<<"value">>),
    GroupParams = #group_params{prefix = <<"foo/">>, delimiter = <<"/">>},
    ?assertMatch({metadata, _Anything}, common_prefix_or_metadata(BKey, RObj, GroupParams)).

common_prefix_or_metadata_returns_common_prefix_test() ->
    Bucket = {<<"bucket_type">>, <<"bucket">>},
    Key  = <<"foo/prefix/actual_object">>,
    BKey = {Bucket, Key},
    RObj = riak_object:new(Bucket, Key,<<"value">>),
    GroupParams = #group_params{prefix = <<"foo/">>, delimiter = <<"/">>},
    ?assertMatch({common_prefix, <<"foo/prefix/">>}, common_prefix_or_metadata(BKey, RObj, GroupParams)).

common_prefix_or_metadata_with_undefined_delimiter_test() ->
    Bucket = {<<"bucket_type">>, <<"bucket">>},
    Key  = <<"foo/prefix/actual_object">>,
    BKey = {Bucket, Key},
    RObj = riak_object:new(Bucket, Key,<<"value">>),
    GroupParams = #group_params{prefix = <<"foo/">>},
    ?assertMatch({metadata, _Anything}, common_prefix_or_metadata(BKey, RObj, GroupParams)).

start_key_test() ->
    Prefix = undefined,
    StartAfter = undefined,
    ContinuationToken = undefined,
    ?assertEqual(<<"">>, start_key(Prefix, StartAfter, ContinuationToken)).

start_key_with_prefix_test() ->
    Prefix = <<"foo/bar/baz">>,
    StartAfter = undefined,
    ContinuationToken = undefined,
    ?assertEqual(<<"foo/bar/baz">>, start_key(Prefix, StartAfter, ContinuationToken)).

start_key_with_start_after_test() ->
    Prefix = undefined,
    StartAfter = <<"foo/bar/baz">>,
    ContinuationToken = undefined,
    ?assertEqual(append_max_byte(<<"foo/bar/baz">>), start_key(Prefix, StartAfter, ContinuationToken)).

start_key_with_prefix_and_start_after_test() ->
    Prefix = <<"foo/bar/">>,
    StartAfter = <<"foo/bar/baz">>,
    ContinuationToken = undefined,
    ?assertEqual(append_max_byte(<<"foo/bar/baz">>), start_key(Prefix, StartAfter, ContinuationToken)).

start_key_with_continuation_test() ->
    Prefix = undefined,
    StartAfter = undefined,
    ContinuationToken = riak_kv_continuation:make_token(<<"foo/bar/baz">>),
    ?assertEqual(append_max_byte(<<"foo/bar/baz">>), start_key(Prefix, StartAfter, ContinuationToken)).

start_key_with_start_after_and_continuation_test() ->
    Prefix = undefined,
    StartAfter = <<"should/be/ignored">>,
    ContinuationToken = riak_kv_continuation:make_token(<<"should/be/used">>),
    ?assertEqual(append_max_byte(<<"should/be/used">>), start_key(Prefix, StartAfter, ContinuationToken)).

start_key_with_prefix_less_than_continuation_test() ->
    Prefix = <<"foo/">>,
    StartAfter = undefined,
    ContinuationToken = riak_kv_continuation:make_token(<<"foo/bar/baz">>),
    ?assertEqual(append_max_byte(<<"foo/bar/baz">>), start_key(Prefix, StartAfter, ContinuationToken)).

start_key_with_prefix_equal_to_continuation_test() ->
    Prefix = <<"foo/bar/">>,
    StartAfter = undefined,
    ContinuationToken = riak_kv_continuation:make_token(<<"foo/bar/">>),
    ?assertEqual(append_max_byte(<<"foo/bar/">>), start_key(Prefix, StartAfter, ContinuationToken)).

start_key_with_prefix_greater_than_continuation_test() ->
    Prefix = <<"foo/">>,
    StartAfter = undefined,
    ContinuationToken = riak_kv_continuation:make_token(<<"bar/baz">>),
    ?assertEqual(Prefix, start_key(Prefix, StartAfter, ContinuationToken)).

next_pos_after_key_with_no_prefix_or_delimiter_test() ->
    GroupParams = test_group_params(undefined, undefined),
    Bucket = <<"bucket">>,
    ?assertEqual(next, next_pos_after_key(GroupParams, Bucket, <<"foo">>)),
    ?assertEqual(next, next_pos_after_key(GroupParams, Bucket, <<"foo/bar">>)),
    ?assertEqual(next, next_pos_after_key(GroupParams, Bucket, <<"foo/bar/baz">>)).

next_pos_after_key_with_delimiter_test() ->
    GroupParams = test_group_params(undefined, <<"/">>),
    Bucket = <<"bucket">>,
    ?assertEqual(next, next_pos_after_key(GroupParams, Bucket, <<"foo">>)),
    ?assertEqual(expected_next_pos(Bucket, <<"foo/">>),
                 next_pos_after_key(GroupParams, Bucket, <<"foo/bar">>)),
    ?assertEqual(expected_next_pos(Bucket, <<"foo/">>),
                 next_pos_after_key(GroupParams, Bucket, <<"foo/bar/baz">>)).

next_pos_after_key_with_prefix_test() ->
    GroupParams = test_group_params(<<"foo/">>, undefined),
    Bucket = <<"bucket">>,
    ?assertEqual(next, next_pos_after_key(GroupParams, Bucket, <<"foo">>)),
    ?assertEqual(next,
                 next_pos_after_key(GroupParams, Bucket, <<"foo/bar">>)),
    ?assertEqual(next,
                 next_pos_after_key(GroupParams, Bucket, <<"foo/bar/baz">>)).

next_pos_after_key_with_prefix_and_delimiter_test() ->
    GroupParams = test_group_params(<<"foo/">>, <<"/">>),
    Bucket = <<"bucket">>,
    ?assertEqual(next, next_pos_after_key(GroupParams, Bucket, <<"foo">>)),
    ?assertEqual(next,
                 next_pos_after_key(GroupParams, Bucket, <<"foo/bar">>)),
    ?assertEqual(expected_next_pos(Bucket, <<"foo/bar/">>),
                 next_pos_after_key(GroupParams, Bucket, <<"foo/bar/baz">>)).

-endif.
