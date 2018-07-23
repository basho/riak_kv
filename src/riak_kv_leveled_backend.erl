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
            fold_heads/4,
            return_self/1,
            shutdown_guid/2]).


-include("riak_kv_index.hrl").

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
                        snap_prefold,
                        segment_accelerate,
                        leveled,
                        shutdown_guid]).
-define(API_VERSION, 1).
-define(BUCKET_SDG, <<"MD">>).
-define(KEY_SDG, <<"SHUDOWN_GUID">>).
-define(TAG_SDG, o).

-record(state, {bookie :: pid(),
                reference :: reference(),
                partition :: integer(),
                config,
                compactions_perday :: integer(),
                valid_hours = [] :: list(integer())}).

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
    DataRoot = app_helper:get_prop_or_env(data_root, Config, leveled),
    MJS = app_helper:get_prop_or_env(journal_size, Config, leveled),
    BCS = app_helper:get_prop_or_env(cache_size, Config, leveled),
    PCS = app_helper:get_prop_or_env(penciller_cache_size, Config, leveled),
    SYS = app_helper:get_prop_or_env(sync_strategy, Config, leveled),
    CMM = app_helper:get_prop_or_env(compression_method, Config, leveled),
    CMP = app_helper:get_prop_or_env(compression_point, Config, leveled),
    CRD = app_helper:get_prop_or_env(compaction_runs_perday, Config, leveled),
    CLH = app_helper:get_prop_or_env(compaction_low_hour, Config, leveled),
    CTH = app_helper:get_prop_or_env(compaction_top_hour, Config, leveled),
    MRL = app_helper:get_prop_or_env(max_run_length, Config, leveled),
    MCP = app_helper:get_prop_or_env(maxrunlength_compactionpercentage, Config, leveled),
    SCP = app_helper:get_prop_or_env(singlefile_compactionpercentage, Config, leveled),

    case get_data_dir(DataRoot, integer_to_list(Partition)) of
        {ok, DataDir} ->
            StartOpts = [{root_path, DataDir},
                            {max_journalsize, MJS},
                            {cache_size, BCS},
                            {max_pencillercachesize, PCS},
                            {sync_strategy, SYS},
                            {compression_method, CMM},
                            {compression_point, CMP},
                            {max_run_length, MRL},
                            {maxrunlength_compactionpercentage, MCP},
                            {singlefile_compactionpercentage, SCP}],
            {ok, Bookie} = leveled_bookie:book_start(StartOpts),
            Ref = make_ref(),
            ValidHours = valid_hours(CLH, CTH),
            schedule_journalcompaction(Ref, Partition, CRD, ValidHours),
            {ok, #state{bookie=Bookie,
                        reference=Ref,
                        partition=Partition,
                        config=Config,
                        compactions_perday = CRD,
                        valid_hours = ValidHours}};
        {error, Reason} ->
            lager:error("Failed to start leveled backend: ~p\n",
                            [Reason]),
            {error, Reason}
    end.

-spec return_self(state()) -> pid().
%% @doc
%% Return the Bookie PID from the ModState
return_self(State) -> State#state.bookie.

%% @doc Stop the leveled backend
-spec stop(state()) -> ok.
stop(#state{bookie=Bookie}) ->
    ok = leveled_bookie:book_close(Bookie).

-spec shutdown_guid(state(), on_close|on_start) -> string()|none.
%% @doc
%% Check the shutdown GUID on startup, or generate and ave it on shutdown
shutdown_guid(#state{bookie=Bookie}, on_close) ->
    ShutdownGUID = leveled_util:generate_uuid(),
    ok = leveled_bookie:book_put(Bookie, 
                                    ?BUCKET_SDG, ?KEY_SDG, 
                                    ShutdownGUID, [], 
                                    ?TAG_SDG),
    ShutdownGUID;
shutdown_guid(#state{bookie=Bookie}, on_start) ->
    % Check for a shutdown GUID and delete if present
    case leveled_bookie:book_get(Bookie, ?BUCKET_SDG, ?KEY_SDG, ?TAG_SDG) of
        {ok, Value} ->
            leveled_bookie:book_put(Bookie,
                                    ?BUCKET_SDG, ?KEY_SDG, delete, [],
                                    ?TAG_SDG),
            Value;
        not_found  ->
            none
    end.

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
            {error, not_found, State}
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
            {error, not_found, State}
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

    %% All fold_keys queries are currently snapped prior to the fold, the 
    %% delta with setting this option is just whether the response is 
    %% {queue, Folder} or {async, Folder} - allowing for the riak vnode to
    %% distributed to the constrained core node_worker_pool rather than being
    %% directly run in the vnode_worker_pool (where it will almost certainly 
    %% be executed immediately).
    SnapPreFold = lists:member(snap_prefold, Opts),

    %% Multiple limiters may exist. Take the most specific limiter.
    {async, Folder} =
        if
            Index /= false  ->
                {index, QBucket, Q} = Index,
                ?KV_INDEX_Q{filter_field=Field,
                            start_key=StartKey,
                            start_term=StartTerm,
                            end_term=EndTerm,
                            return_terms=ReturnTerms,
                            term_regex=TermRegex} = riak_index:upgrade_query(Q),

                IndexQuery = case Field of
                                 <<"$bucket">> ->
                                     {keylist, ?RIAK_TAG, QBucket, {FoldKeysFun, Acc}};
                                 <<"$key">> ->
                                     {keylist, ?RIAK_TAG, QBucket, {StartKey, EndTerm}, {FoldKeysFun, Acc}};
                                 _ ->
                                     {index_query,
                                      {QBucket, StartKey},
                                      {FoldKeysFun, Acc},
                                      {Field, StartTerm, EndTerm},
                                      {ReturnTerms, TermRegex}}
                             end,
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
    Query =
        case proplists:get_value(bucket, Opts) of
            undefined ->
                {foldobjects_allkeys, ?RIAK_TAG, {FoldObjectsFun, Acc}, false};
            B ->
                {foldobjects_bybucket, ?RIAK_TAG, B, {FoldObjectsFun, Acc}, false}
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
    SegmentList = 
        case proplists:get_value(segment_accelerate, Opts) of 
            undefined ->
                false;
            SL ->
                SL 
        end,
    Query = 
        case lists:keyfind(index, 1, Opts) of 
            {index, Bucket, IdxQuery} ->
                case IdxQuery#riak_kv_index_v3.filter_field of 
                    <<"$key">> ->
                        {foldheads_bybucket,
                            ?RIAK_TAG,
                            Bucket,
                            {IdxQuery#riak_kv_index_v3.start_term,
                                IdxQuery#riak_kv_index_v3.end_term},
                            {FoldHeadsFun, Acc},
                            CheckPresence,
                            SnapPreFold,
                            SegmentList};
                    Field ->
                        {foldheads_byindex,
                            ?RIAK_TAG,
                            Bucket,
                            Field,
                            {IdxQuery#riak_kv_index_v3.start_term,
                                IdxQuery#riak_kv_index_v3.end_term},
                            {FoldHeadsFun, Acc},
                            SnapPreFold}
                end;
            false ->
                case proplists:get_value(bucket, Opts) of
                    undefined ->
                        {foldheads_allkeys,
                            ?RIAK_TAG,
                            {FoldHeadsFun, Acc},
                            CheckPresence,
                            SnapPreFold,
                            SegmentList};
                    B ->
                        {foldheads_bybucket,
                            ?RIAK_TAG,
                            B,
                            all,
                            {FoldHeadsFun, Acc},
                            CheckPresence,
                            SnapPreFold,
                            SegmentList}
                end
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
                                        State#state.partition,
                                        State#state.compactions_perday,
                                        State#state.valid_hours),
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

-spec schedule_journalcompaction(reference(), integer(), integer(), list(integer())) -> reference().
%% @private
%% Request a callback in the future to check for journal compaction
schedule_journalcompaction(Ref, PartitionID, PerDay, ValidHours) when is_reference(Ref) ->
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
prompt_journalcompaction(Bookie, Ref, PartitionID, PerDay, ValidHours) when is_reference(Ref) ->
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
    schedule_journalcompaction(Ref, PartitionID, PerDay, ValidHours).

%% @private
%% Change the low hour and high hour into a list of valid hours
valid_hours(LowHour, HighHour) when LowHour > HighHour ->
    lists:seq(0, HighHour) ++ lists:seq(LowHour, 23);
valid_hours(LowHour, HighHour) ->
    lists:seq(LowHour, HighHour).

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

-ifdef(EQC).

eqc_test_() ->
    {spawn,
     [{inorder,
       [{setup,
         fun setup/0,
         fun cleanup/1,
         [
          {timeout, 180,
            [?_assertEqual(true,
                          backend_eqc:test(?MODULE, false,
                                           [{data_root,
                                             "test/eleveldb-backend"}]))]}
         ]}]}]}.

setup() ->
    application:load(sasl),
    application:set_env(sasl, sasl_error_logger, {file, "riak_kv_eleveldb_backend_eqc_sasl.log"}),
    error_logger:tty(false),
    error_logger:logfile({open, "riak_kv_eleveldb_backend_eqc.log"}),

    ok.

cleanup(_) ->
    ?_assertCmd("rm -rf test/eleveldb-backend").

-endif. % EQC

-endif.
