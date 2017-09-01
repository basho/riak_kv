%% ----------------------------------------------------------------------------
%% This file is provided to you under the Apache License, Version 2.0 (the
%% "License"); you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
%% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
%% License for the specific language governing permissions and limitations
%% under the License.
%%
%% ----------------------------------------------------------------------------

-module(riak_kv_leveled_backend).
-behavior(riak_kv_backend).

%% KV Backend API
-export([api_version/0,
         capabilities/1,
         capabilities/2,
         start/2,
         stop/1,
         get/3,
         put/5,
         delete/4,
         drop/1,
         fold_buckets/4,
         fold_keys/4,
         fold_objects/4,
         is_empty/1,
         status/1,
         data_size/1,
         callback/3]).

%% Extended KV Backend API
-export([head/3,
            fold_heads/4]).


-include("riak_kv_index.hrl").
-include("riak_kv_leveled.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(RIAK_TAG, o_rkv).
-define(CAPABILITIES, [async_fold,
                        indexes,
                        head,
                        fold_heads,
                        direct_fetch,
                        putfsm_pause,
                        snap_prefold]).
-define(API_VERSION, 1).

-record(state, {bookie :: pid(),
                reference :: reference(),
                partition :: integer(),
                config }).

-type state() :: #state{}.

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Return the major version of the
%% current API.
-spec api_version() -> {ok, integer()}.
api_version() ->
    {ok, ?API_VERSION}.

%% @doc Return the capabilities of the backend.
-spec capabilities(state()) -> {ok, [atom()]}.
capabilities(_) ->
    {ok, ?CAPABILITIES}.

%% @doc Return the capabilities of the backend.
-spec capabilities(riak_object:bucket(), state()) -> {ok, [atom()]}.
capabilities(_, _) ->
    {ok, ?CAPABILITIES}.

%% @doc Start the hanoidb backend
-spec start(integer(), list()) -> {ok, state()} | {error, term()}.
start(Partition, Config) ->
    %% Get the data root directory - cuttlefish not working
    DataRoot = ?LEVELED_DATAROOT,
    case get_data_dir(DataRoot, integer_to_list(Partition)) of
        {ok, DataDir} ->
            case leveled_bookie:book_start(DataDir,
                                            ?LEVELED_LEDGERCACHE,
                                            ?LEVELED_JOURNALSIZE,
                                            ?LEVELED_SYNCSTRATEGY) of
                {ok, Bookie} ->
                    Ref = make_ref(),
                    schedule_journalcompaction(Ref, Partition),
                    {ok, #state{bookie=Bookie,
                                reference=Ref,
                                partition=Partition,
                                config=Config }};
                {error, OpenReason}=OpenError ->
                    lager:error("Failed to open leveled: ~p\n",
                                    [OpenReason]),
                    OpenError
            end;
        {error, Reason} ->
            lager:error("Failed to start leveled backend: ~p\n",
                            [Reason]),
            {error, Reason}
    end.

%% @doc Stop the leveled backend
-spec stop(state()) -> ok.
stop(#state{bookie=Bookie}) ->
    ok = leveled_bookie:book_close(Bookie).

%% @doc Retrieve an object from the leveled backend as a binary
-spec get(riak_object:bucket(), riak_object:key(), state()) ->
                 {ok, any(), state()} |
                 {ok, not_found, state()} |
                 {error, term(), state()}.
get(Bucket, Key, #state{bookie=Bookie}=State) ->
    case leveled_bookie:book_get(Bookie, Bucket, Key, ?RIAK_TAG) of
        {ok, Value} ->
            {ok, Value, State};
        not_found  ->
            {error, not_found, State};
        {error, Reason} ->
            {error, Reason, State}
    end.

%% @doc Retrieve an object from the leveled backend as a binary
-spec head(riak_object:bucket(), riak_object:key(), state()) ->
                 {ok, any(), state()} |
                 {ok, not_found, state()} |
                 {error, term(), state()}.
head(Bucket, Key, #state{bookie=Bookie}=State) ->
    case leveled_bookie:book_head(Bookie, Bucket, Key, ?RIAK_TAG) of
        {ok, Value} ->
            {ok, Value, State};
        not_found  ->
            {error, not_found, State};
        {error, Reason} ->
            {error, Reason, State}
    end.

%% @doc Insert an object into the leveled backend.
-type index_spec() :: {add, Index, SecondaryKey} |
                        {remove, Index, SecondaryKey}.

-spec put(riak_object:bucket(),
                    riak_object:key(),
                    [index_spec()],
                    binary(),
                    state()) ->
                         {ok, state()} |
                         {error, term(), state()}.
put(Bucket, Key, IndexSpecs, Val, #state{bookie=Bookie}=State) ->
    case leveled_bookie:book_put(Bookie,
                                    Bucket, Key, Val, IndexSpecs,
                                    ?RIAK_TAG) of
        ok ->
            {ok, State};
        pause ->
            % To be changed if back-pressure added to Riak put_fsm
            {ok, State}
    end.

%% @doc Delete an object from the leveled backend
-spec delete(riak_object:bucket(),
                riak_object:key(),
                [index_spec()],
                state()) ->
                    {ok, state()} |
                    {error, term(), state()}.
delete(Bucket, Key, IndexSpecs, #state{bookie=Bookie}=State) ->
    case leveled_bookie:book_put(Bookie,
                                    Bucket, Key, delete, IndexSpecs,
                                    ?RIAK_TAG) of
        ok ->
            {ok, State};
        pause ->
            % To be changed if back-pressure added to Riak put_fsm
            {ok, State}
    end.

%% @doc Fold over all the buckets
-spec fold_buckets(riak_kv_backend:fold_buckets_fun(),
                   any(),
                   [],
                   state()) -> {ok, any()} | {async, fun()}.
fold_buckets(FoldBucketsFun, Acc, Opts, #state{bookie=Bookie}) ->
    ListBucketQ = {binary_bucketlist, ?RIAK_TAG, {FoldBucketsFun, Acc}},
    {async, Folder} = leveled_bookie:book_returnfolder(Bookie, ListBucketQ),
    case lists:member(async_fold, Opts) of
        true ->
            {async, Folder};
        false ->
            {ok, Folder()}
    end.

%% @doc Fold over all the keys for one or all buckets.
-spec fold_keys(riak_kv_backend:fold_keys_fun(),
                any(),
                [{atom(), term()}],
                state()) -> {ok, term()} | {async, fun()}.
fold_keys(FoldKeysFun, Acc, Opts, #state{bookie=Bookie}) ->
    %% Figure out how we should limit the fold: by bucket, by
    %% secondary index, or neither (fold across everything.)
    Bucket = lists:keyfind(bucket, 1, Opts),
    Index = lists:keyfind(index, 1, Opts),

    SnapPreFold = lists:member(snap_prefold, Opts),

    %% Multiple limiters may exist. Take the most specific limiter.
    {async, Folder} =
        if
            Index /= false  ->
                {index, QBucket, ?KV_INDEX_Q{filter_field=Field,
                                                start_key=StartKey,
                                                start_term=StartTerm,
                                                end_term=EndTerm,
                                                return_terms=ReturnTerms,
                                                term_regex=TermRegex}} = Index,
                IndexQuery =
                    {index_query,
                                {QBucket, StartKey},
                                {FoldKeysFun, Acc},
                                {Field, StartTerm, EndTerm},
                                {ReturnTerms, TermRegex}},
                leveled_bookie:book_returnfolder(Bookie, IndexQuery);
            Bucket /= false ->
                {bucket, B} = Bucket,
                BucketQuery = {keylist, ?RIAK_TAG, B, {FoldKeysFun, Acc}},
                leveled_bookie:book_returnfolder(Bookie, BucketQuery);
            true ->
                AllKeyQuery = {keylist, ?RIAK_TAG, {FoldKeysFun, Acc}},
                leveled_bookie:book_returnfolder(Bookie, AllKeyQuery)
        end,

    case {lists:member(async_fold, Opts), SnapPreFold} of
        {true, true} ->
            {queue, Folder};
        {true, false} ->
            {async, Folder};
        _ ->
            {ok, Folder()}
    end.


%% @doc Fold over all the objects for one or all buckets.
-spec fold_objects(riak_kv_backend:fold_objects_fun(),
                   any(),
                   [{atom(), term()}],
                   state()) -> {ok, any()} | {async, fun()}.
fold_objects(FoldObjectsFun, Acc, Opts, #state{bookie=Bookie}) ->
    lager:info("Fold objects request with Opts ~w", [Opts]),
    Query =
        case proplists:get_value(bucket, Opts) of
            undefined ->
                {foldobjects_allkeys, ?RIAK_TAG, {FoldObjectsFun, Acc}};
            B ->
                {foldobjects_bybucket, ?RIAK_TAG, B, {FoldObjectsFun, Acc}}
        end,
    {async, ObjectFolder} = leveled_bookie:book_returnfolder(Bookie, Query),
    case lists:member(async_fold, Opts) of
        true ->
            {async, ObjectFolder};
        false ->
            {ok, ObjectFolder()}
    end.

%% @doc Fold over all the heads for one or all buckets.
%% Works as with fold_objects only the fun() will return
%% FoldHeadsFun(B, K, ProxyValue, Acc) not FoldObjectsFun(B, K, V, Acc)
%% ProxyValue may be an actual object, but will actually be a tuple of the
%% form {proxy_object, HeadBinary, Size, {FetchFun, Clone, FetchKey}} with the
%% expectation that the only the HeadBinary and Size is required, but if the
%% #r_content.value is required the whole original object can be fetched using
%% FetchFun(Clone, FetchKey), as long as the fold function has not finished
-spec fold_heads(riak_kv_backend:fold_objects_fun(),
                   any(),
                   [{atom(), term()}],
                   state()) -> {ok, any()} | {async, fun()}.
fold_heads(FoldHeadsFun, Acc, Opts, #state{bookie=Bookie}) ->
    CheckPresence =
        case proplists:get_value(check_presence, Opts) of
            undefined ->
                true;
            CP ->
                CP
        end,
    SnapPreFold = lists:member(snap_prefold, Opts),
    Query =
        case proplists:get_value(bucket, Opts) of
            undefined ->
                {foldheads_allkeys,
                  ?RIAK_TAG,
                  {FoldHeadsFun, Acc},
                  CheckPresence,
                  SnapPreFold};
            B ->
                {foldheads_bybucket,
                  ?RIAK_TAG,
                  B,
                  {FoldHeadsFun, Acc, CheckPresence},
                  CheckPresence,
                  SnapPreFold}
        end,

    {async, HeadFolder} = leveled_bookie:book_returnfolder(Bookie, Query),
    case {lists:member(async_fold, Opts), SnapPreFold} of
        {true, true} ->
            {queue, HeadFolder};
        {true, false} ->
            {async, HeadFolder};
        _ ->
            {ok, HeadFolder()}
    end.


%% @doc Delete all objects from this leveled backend
-spec drop(state()) -> {ok, state()} | {error, term(), state()}.
drop(#state{bookie=Bookie, partition=Partition, config=Config}=_State) ->
    ok = leveled_bookie:book_destroy(Bookie),
    start(Partition, Config).

%% @doc Returns true if this leveled backend contains any
%% non-tombstone values; otherwise returns false.
-spec is_empty(state()) -> boolean().
is_empty(#state{bookie=Bookie}) ->
    FoldBucketsFun = fun(B, Acc) -> sets:add_element(B, Acc) end,
    ListBucketQ = {binary_bucketlist,
                    ?RIAK_TAG,
                    {FoldBucketsFun, sets:new()}},
    {async, Folder} = leveled_bookie:book_returnfolder(Bookie, ListBucketQ),
    BSet = Folder(),
    case sets:size(BSet) of
        0 ->
            true;
        _ ->
            false
    end.

%% @doc Get the status information for this leveled backend
-spec status(state()) -> [{atom(), term()}].
status(_State) ->
    % TODO: not yet implemented
    % We can run the bucket stats query getting all stats, but this would not
    % mean an immediate response (and how frequently is this called?)
    [].

%% @doc Get the data_size for this leveled backend
-spec data_size(state()) -> undefined | {non_neg_integer(), objects}.
data_size(_State) ->
    % TODO: not yet implemented
    % We can run the bucket stats query getting all stats, but this would not
    % mean an immediate response (and how frequently is this called?)
    undefined.


%% @doc Register an asynchronous callback
-spec callback(reference(), any(), state()) -> {ok, state()}.
callback(Ref, compact_journal, State) ->
    case is_reference(Ref) of
        true ->
             prompt_journalcompaction(State#state.bookie,
                                        Ref,
                                        State#state.partition),
             {ok, State}
    end.

%% ===================================================================
%% Internal functions
%% ===================================================================

%% @private
%% Create the directory for this partition's files
get_data_dir(DataRoot, Partition) ->
    PartitionDir = filename:join([DataRoot, Partition]),
    case filelib:ensure_dir(PartitionDir) of
        ok ->
            {ok, PartitionDir};
        {error, Reason} ->
            lager:error("Failed to create leveled dir ~s: ~p",
                            [PartitionDir, Reason]),
            {error, Reason}
    end.

%% @private
%% Request a callback in the future to check for journal compaction
schedule_journalcompaction(Ref, PartitionID) when is_reference(Ref) ->
    ValidHours = app_helper:get_env(riak_kv,
                                    leveled_jc_valid_hours,
                                    ?LEVELED_JC_VALID_HOURS),
    PerDay = app_helper:get_env(riak_kv,
                                    leveled_jc_compactions_perday,
                                    ?LEVELED_JC_COMPACTIONS_PERDAY),
    random:seed(element(3, os:timestamp()),
                    erlang:phash2(self()),
                    PartitionID),
    Interval = leveled_iclerk:schedule_compaction(ValidHours,
                                                    PerDay,
                                                    os:timestamp()),
    lager:info("Schedule compaction for interval ~w on partition ~w",
                    [Interval, PartitionID]),
    riak_kv_backend:callback_after(Interval * 1000, % callback interval in ms
                                    Ref,
                                    compact_journal).

%% @private
%% Do journal compaction if the callback is in a valid time period
prompt_journalcompaction(Bookie, Ref, PartitionID) when is_reference(Ref) ->
    ValidHours = app_helper:get_env(riak_kv,
                                    leveled_jc_valid_hours,
                                    ?LEVELED_JC_VALID_HOURS),
    {{_Yr, _Mth, _Day}, {Hr, _Min, _Sec}} = calendar:local_time(),
    case lists:member(Hr, ValidHours) of
        true ->
            case leveled_bookie:book_islastcompactionpending(Bookie) of
                true ->
                    ok;
                false ->
                    leveled_bookie:book_compactjournal(Bookie, 30000)
            end;
        false ->
            ok
    end,
    schedule_journalcompaction(Ref, PartitionID).



%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).



-endif.
