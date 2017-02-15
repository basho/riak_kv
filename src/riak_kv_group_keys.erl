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

-module(riak_kv_group_keys).
-export([fold_keys/6]).

-record(group_params, {
          backend_mod :: module(),
          prefix :: string(),
          delimiter :: string(),
          start_after :: string(),
          max_keys :: pos_integer()
         }).


fold_keys(BackendMod, FoldFun, Acc, Opts, FoldOpts, DbRef) ->
    Bucket = proplists:get_value(bucket, Opts),
    GroupParams = to_group_params(BackendMod, proplists:get_value(group_params, Opts, [])),
    ContentFolderFun = content_folder_fun(Bucket, GroupParams, DbRef, FoldOpts, FoldFun, Acc),
    Async = proplists:get_bool(async_fold, Opts),
    case Async of
        true ->
            {async, ContentFolderFun};
        false ->
            {ok, ContentFolderFun()}
    end.

to_group_params(BackendMod, PropList) ->
    #group_params{
       backend_mod=BackendMod,
       prefix=proplists:get_value(prefix, PropList),
       delimiter=proplists:get_value(delimiter, PropList),
       start_after=proplists:get_value(start_after, PropList),
       max_keys=proplists:get_value(max_keys, PropList)
      }.

backend_from(#group_params{backend_mod = BackendMod}) ->
    BackendMod.

content_folder_fun(Bucket, GroupParams, DbRef, FoldOpts, FoldFun, Acc) ->
    BackendMod = backend_from(GroupParams),
    fun() ->
            FoldOpts1 = [{first_key, BackendMod:to_first_key({bucket, Bucket})} | FoldOpts],
            try iterator_open(DbRef, FoldOpts1) of
                {ok, Itr} ->
                    iterate(Bucket, GroupParams, Itr, FoldFun, Acc)
            catch Error ->
                    lager:debug("Could not open iterator: ~p", [Error]),
                    throw(Error)
            end
    end.

iterator_open(DbRef, FoldOpts) ->
    eleveldb:iterator(DbRef, FoldOpts).

iterator_close(Itr) ->
    eleveldb:iterator_close(Itr).

iterator_move(Itr, Pos) ->
    eleveldb:iterator_move(Itr, Pos).

iterate(Bucket, GroupParams, Itr, FoldFun, Acc) ->
    Outcome = enumerate(undefined, Bucket, GroupParams, Itr, FoldFun, Acc),
    iterator_close(Itr),
    case Outcome of
        {error, _Err} = Error ->
            throw(Error);
        {break, _Res} = Result ->
            throw(Result);
        Result -> Result
    end.

enumerate(PrevEntry, Bucket, GroupParams, Itr, FoldFun, Acc) ->
    Pos = next_pos(PrevEntry, Bucket, GroupParams),
    BackendMod = backend_from(GroupParams),
    case Pos of
        npos ->
            Acc;
        _ ->
            try iterator_move(Itr, Pos) of
                {error, invalid_iterator} ->
                    lager:debug( "invalid_iterator.  reached end.", []),
                    Acc;
                {error, iterator_closed} ->
                    lager:debug( "iterator_closed.", []),
                    Acc;
                {ok, BinaryBKey, BinaryValue} ->
                    BKey = BackendMod:from_object_key(BinaryBKey),
                    maybe_accumulate({BKey, BinaryValue}, PrevEntry, Bucket, GroupParams, FoldFun, Acc, Itr)
            catch Error ->
                    {error, {eleveldb_error, Error}}
            end
    end.

start_pos(Bucket, GroupParams = #group_params{prefix=undefined, start_after=undefined}) ->
    to_object_key(GroupParams, Bucket, <<"">>);
start_pos(Bucket, GroupParams = #group_params{prefix=Prefix, start_after=undefined}) ->
    to_object_key(GroupParams, Bucket, Prefix);
start_pos(Bucket, GroupParams = #group_params{prefix=undefined, start_after=StartAfter}) ->
    to_object_key(GroupParams, Bucket, <<StartAfter/binary, 0>>);
start_pos(Bucket, GroupParams = #group_params{prefix=Prefix, start_after=StartAfter}) ->
    case StartAfter =< Prefix of
        true ->
            to_object_key(GroupParams, Bucket, Prefix);
        _ ->
            npos
    end.

next_pos(undefined, Bucket, GroupParams) ->
    start_pos(Bucket, GroupParams);
next_pos(_PrevBKey, _Bucket, #group_params{prefix=undefined}) ->
    next;
next_pos({_Bucket, PrevKey} = _PrevBKey, _Bucket, #group_params{prefix=Prefix}) ->
    case is_prefix(Prefix, PrevKey) of
        true ->
            next;
        _ ->
            npos
    end.

is_prefix(B1, B2) ->
    binary:longest_common_prefix([B1, B2]) == size(B1).

to_object_key(GroupParams, Bucket, Key) ->
    BackendMod = backend_from(GroupParams),
    BackendMod:to_object_key(Bucket, Key).

maybe_accumulate({undefined, _BinaryValue}, _PrevEntry, _Bucket, _GroupParams, _FoldFun, Acc, _Itr) ->
    Acc;
maybe_accumulate({ignore, _BinaryValue}, _PrevEntry, _Bucket, _GroupParams, _FoldFun, Acc, _Itr) ->
    lager:error("Encountered corrupt key while iterating entries. Grouped key list may be incomplete."),
    Acc;
maybe_accumulate(PrevEntry, PrevEntry, _Bucket, _GroupParams, _FoldKeysFun, _Acc, _Itr) ->
    {error, did_not_skip_to_next_entry};
maybe_accumulate({{Bucket, _Key}, _BinaryValue} = Entry, _PrevEntry, Bucket, #group_params{prefix=undefined}=GroupParams, FoldFun, Acc, Itr) ->
    accumulate(Entry, Bucket, GroupParams, FoldFun, Acc, Itr);
maybe_accumulate(_Entry, _PrevEntry, _Bucket, #group_params{prefix=undefined}=_GroupParams, _FoldFun, Acc, _Itr) ->
    Acc;
maybe_accumulate({{Bucket, Key}, _BinaryValue} = Entry, _PrevEntry, Bucket, #group_params{prefix=Prefix}=GroupParams, FoldFun, Acc, Itr) ->
    case Prefix =/= undefined andalso is_prefix(Prefix, Key) of
        true ->
            accumulate(Entry, Bucket, GroupParams, FoldFun, Acc, Itr);
        false ->
                                                % done
            Acc
    end;
maybe_accumulate({{_DifferentBucket, _Key}, _BinaryValue}, _PrevEntry, _Bucket, _GroupParams, _FoldFun, Acc, _Itr) ->
    Acc.

%% Note that the two instances of `TargetBucket' below are intentional so that we accumulate only when
%% we match the bucket that we're looking for.
accumulate({{TargetBucket, Key}=BKey, BinaryValue},
           TargetBucket,
           GroupParams = #group_params{delimiter = Delimiter},
           FoldFun,
           Acc,
           Itr) ->
    NewAcc = try
                 Contents = get_contents(BKey, BinaryValue),
                 FoldFun(TargetBucket, Key, Contents, Acc)
             catch Error ->
                     FoldFun(TargetBucket, Key, {error, Error}, Acc)
             end,
    enumerate(BKey, TargetBucket, GroupParams, Itr, FoldFun, NewAcc).

%% TODO: rename all contents to metadata once we have it wired through
get_contents({Bucket, Key} = _BKey, BinaryValue) ->
    RObj = riak_object:from_binary(Bucket, Key, BinaryValue),
    Contents = riak_object:get_contents(RObj),
    case Contents of
        [_Content] ->
            Metadata = riak_object:get_metadata(RObj),
            {metadata, Metadata};
        _ ->
            %% TODO
            {error, riak_object_has_siblings}
    end.
