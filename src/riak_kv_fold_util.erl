%% -------------------------------------------------------------------
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

%% @doc This module provides utility functions related to riak_kv
%% backend folds.

-module(riak_kv_fold_util).
-author('Kelly McLaughlin <kelly@basho.com>').

-export([fold_buffer/2,
         fold_buffer/3,
         finish_fold/2,
         finish_fold/3,
         flush_buffer/2,
         flush_buffer/3]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type from() :: {atom(), req_id(), pid()}.
-type req_id() :: non_neg_integer().
-type fold_result() :: {ok, term()} |
                       {async, fun()} |
                       mb_fold_result() |
                       {error, term()}.
-type mb_fold_result() :: {{sync, [term()]}, {async, [fun()]}}.

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Create a new fold buffer of the specified size that will
%% return any intermediate results to the specified sender using
%% riak_core_vnode:reply. The finish_fold function must be called
%% after the fold operation is completed to ensure all results have
%% been flushed from the buffer and returned to the sender.
-spec fold_buffer(pos_integer(), from()) -> riak_kv_fold_buffer:buffer().
fold_buffer(Size, Sender) ->
    riak_kv_fold_buffer:new(Size,
                            get_buffer_fun(false, Sender)).

%% @doc Create a new fold buffer of the specified size that will
%% return any intermediate results to the specified sender using
%% riak_core_vnode:reply. Each set of buffered results will be
%% returned to the sender as a two tuple with the Bucket parameter
%% being the first element and the result set as the second. The
%% finish_fold function must be called after the fold operation is
%% completed to ensure all results have been flushed from the buffer
%% and returned to the sender.
-spec fold_buffer(pos_integer(), binary(), from()) ->
                         riak_kv_fold_buffer:buffer().
fold_buffer(Size, Bucket, Sender) ->
    riak_kv_fold_buffer:new(Size,
                            get_buffer_fun({false, Bucket}, Sender)).

%% @doc Take the appropriate action to finalize a fold operation based
%% on the result from a call to a riak_kv backend fold function such
%% as fold_buckets.
-spec finish_fold(fold_result(), from()) -> term().
finish_fold(FoldResult, Sender) ->
    case FoldResult of
        {{sync, [FirstSyncResult | RestSyncResults]}, {async, []}} ->
            [riak_kv_fold_buffer:flush(SyncBuf, get_buffer_fun(false, Sender))
             || SyncBuf <- RestSyncResults],
            riak_kv_fold_buffer:flush(FirstSyncResult,
                                      get_buffer_fun(true, Sender));
        {{sync, SyncResults}, {async, AsyncWork}} ->
            [riak_kv_fold_buffer:flush(SyncBuf, get_buffer_fun(false, Sender))
             || SyncBuf <- SyncResults],
            {async, AsyncWork};
        {ok, Buffer} ->
            riak_kv_fold_buffer:flush(Buffer, get_buffer_fun(true, Sender));
        {async, FoldFun} ->
            {async, FoldFun};
        {error, Reason} ->
            riak_core_vnode:reply(Sender, {error, Reason})
    end.

%% @doc Take the appropriate action to finalize a fold operation based
%% on the result from a call to a riak_kv backend fold function such
%% as fold_keys.
-spec finish_fold(fold_result(), binary(), from()) -> term().
finish_fold(FoldResult, Bucket, Sender) ->
    case FoldResult of
        {{sync, [FirstSyncResult | RestSyncResults]}, {async, []}} ->
            [riak_kv_fold_buffer:flush(SyncBuf,
                                       get_buffer_fun({false, Bucket}, Sender))
             || SyncBuf <- RestSyncResults],
            riak_kv_fold_buffer:flush(FirstSyncResult,
                                      get_buffer_fun({true, Bucket}, Sender));
        {{sync, SyncResults}, {async, AsyncWork}} ->
            [riak_kv_fold_buffer:flush(SyncBuf,
                                       get_buffer_fun({false, Bucket}, Sender))
             || SyncBuf <- SyncResults],
            {async, {Bucket, AsyncWork}};
        {ok, Buffer} ->
            riak_kv_fold_buffer:flush(Buffer,
                                      get_buffer_fun({true, Bucket}, Sender));
        {async, FoldFun} ->
            {async, {Bucket, FoldFun}};
        {error, Reason} ->
            riak_core_vnode:reply(Sender, {error, Reason})
    end.

%% @doc This function flushes a buffer, but does not send information
%% to Sender that the fold operation has completed. This is useful for
%% handling asynchronous folds on backends that use multiple other
%% backends such as riak_kv_multi_backend.
-spec flush_buffer(riak_kv_fold_buffer:buffer(), from()) ->
                          riak_kv_fold_buffer:buffer().
flush_buffer(Buffer, Sender) ->
    riak_kv_fold_buffer:flush(Buffer,
                              get_buffer_fun(false, Sender)).

%% @doc This function flushes a buffer, but does not send information
%% to Sender that the fold operation has completed. This is useful for
%% handling asynchronous folds on backends that use multiple other
%% backends such as riak_kv_multi_backend.
-spec flush_buffer(riak_kv_fold_buffer:buffer(), binary(), from()) ->
                          riak_kv_fold_buffer:buffer().
flush_buffer(Buffer, Bucket, Sender) ->
    riak_kv_fold_buffer:flush(Buffer,
                              get_buffer_fun({false, Bucket}, Sender)).

%% ===================================================================
%% Internal functions
%% ===================================================================

%% @private
%% @doc Return a function to be called on a buffer of results
%% from a backend fold call. Depending on the input the function may
%% include information to indicate that the fold operation is complete
%% and all results have been returned or the bucket that produced the
%% results may be included.
get_buffer_fun(true, Sender) ->
    fun(Results) ->
            riak_core_vnode:reply(Sender,
                                  {final_results, Results})
    end;
get_buffer_fun(false, Sender) ->
    fun(Results) ->
            riak_core_vnode:reply(Sender,
                                  {results, Results})
    end;
get_buffer_fun({true, Bucket}, Sender) ->
    fun(Results) ->
            riak_core_vnode:reply(Sender,
                                  {final_results, {Bucket, Results}})
    end;
get_buffer_fun({false, Bucket}, Sender) ->
    fun(Results) ->
            riak_core_vnode:reply(Sender,
                                  {results, {Bucket, Results}})
    end.
