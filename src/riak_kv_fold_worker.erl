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

%% @doc This module uses the riak_core_vnode_worker behavior to perform
%% different riak_kv fold tasks asynchronously.

-module(riak_kv_fold_worker).
-author('Kelly McLaughlin <kelly@basho.com>').

-behaviour(riak_core_vnode_worker).

-export([init_worker/3,
         handle_work/3]).

-include_lib("riak_kv_vnode.hrl").

-record(state, {index :: partition()}).

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Initialize the worker. Currently only the VNode index
%% parameter is used.
init_worker(VNodeIndex, _Args, _Props) ->
    {ok, #state{index=VNodeIndex}}.

%% @doc Perform the asynchronous fold operation.
handle_work({fold, FoldFun}, _From, State) ->
    Acc = try
              FoldFun()
          catch
              {break, AccFinal} ->
                  AccFinal
          end,
    {reply, Acc, State};
handle_work({fold, SyncResults, []}, _From, State) ->
    SyncFoldFun = 
        fun(SyncResult, _) ->
                SyncResult
        end,
    Acc = lists:foldl(SyncFoldFun, [], SyncResults),
    {reply, Acc, State};
handle_work({fold, [], AsyncWork}, _From, State) ->
    AsyncFoldFun =
        fun(FoldFun, _) ->
                try
                    FoldFun()
                catch
                    {break, AccFinal} ->
                        AccFinal
                end
        end,
    Acc = lists:foldl(AsyncFoldFun, [], AsyncWork),
    {reply, Acc, State};
handle_work({fold, SyncResults, AsyncWork}, _From, State) ->
    SyncFoldFun = 
        fun(SyncResult,_) ->
                SyncResult
        end,
    AsyncFoldFun =
        fun(FoldFun, _) ->
                try
                    FoldFun()
                catch
                    {break, AccFinal} ->
                        AccFinal
                end
        end,
    Acc1 = lists:foldl(SyncFoldFun, [], SyncResults),
    lists:foldl(AsyncFoldFun, [], AsyncWork),
    {reply, Acc1, State};
handle_work({Bucket, [FoldFun | RestFoldFuns]}, From, State) ->
    FoldResults = try
                      FoldFun()
                  catch
                      {break, AccFinal} ->
                          AccFinal
                  end,
    case FoldResults of
        {Acc, _} ->
            ok;
        Acc ->
            ok
    end,
    case RestFoldFuns of
        [] ->
            riak_kv_fold_util:finish_fold({ok, Acc}, Bucket, From),
            {noreply, State};
        _ ->
            riak_kv_fold_util:flush_buffer(Acc, Bucket, From),
            handle_work({Bucket, RestFoldFuns}, From, State)
    end;
handle_work({Bucket, FoldFun}, From, State) ->
    Acc = try
              FoldFun()
          catch
              {break, AccFinal} ->
                  AccFinal
          end,
    riak_kv_fold_util:finish_fold({ok, Acc}, Bucket, From),
    {noreply, State};
handle_work([FoldFun | RestFoldFuns], From, State) ->
    FoldResults = try
                      FoldFun()
                  catch
                      {break, AccFinal} ->
                          AccFinal
                  end,
    case FoldResults of
        {Acc, _} ->
            ok;
        Acc ->
            ok
    end,
    case RestFoldFuns of
        [] ->
            riak_kv_fold_util:finish_fold({ok, Acc}, From),
            {noreply, State};
        _ ->
            riak_kv_fold_util:flush_buffer(Acc, From),
            handle_work(RestFoldFuns, From, State)
    end;
handle_work(FoldFun, From, State) ->
    FoldResults = try
                      FoldFun()
                  catch
                      {break, AccFinal} ->
                          AccFinal
                  end,
    case FoldResults of
        {Acc, _} ->
            ok;
        Acc ->
            ok
    end,
    riak_kv_fold_util:finish_fold({ok, Acc}, From),
    {noreply, State}.
