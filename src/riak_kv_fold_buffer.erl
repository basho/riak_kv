%% -------------------------------------------------------------------
%%
%% riak_kv_fold_buffer: Provide operations for creating and using 
%% size-limited buffers for use in folds.n
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

%% @doc Provide operations for creating and using 
%% size-limited buffers for use in folds.

-module(riak_kv_fold_buffer).

-author('Kelly McLaughlin <kelly@basho.com>').

%% Public API
-export([new/2,
         add/2,
         flush/1,
         flush/2,
         size/1]).

-record(buffer, {acc=[] :: [any()],
                 buffer_fun :: function(),
                 max_size :: pos_integer(),
                 size=0 :: non_neg_integer()}).
-opaque buffer() :: #buffer{}.

%% ===================================================================
%% Public API
%% ===================================================================


%% @doc Returns a new buffer with the specified
%% maximum size and buffer function.
-spec new(pos_integer(), function()) -> buffer().
new(MaxSize, Fun) ->
    #buffer{buffer_fun=Fun,
            max_size=MaxSize-1}.

%% @doc Add an item to the buffer. If the
%% size of the buffer is equal to the 
%% maximum size of this buffer then
%% the buffer function is called on
%% the accumlated buffer items and
%% then the buffer is emptied.
-spec add(any(), buffer()) -> buffer().
add(Item, #buffer{acc=Acc,
                  buffer_fun=Fun,
                  max_size=MaxSize,
                  size=MaxSize}=Buffer) ->
    Fun([Item | Acc]),
    Buffer#buffer{acc=[],
                  size=0};
add(Item, #buffer{acc=Acc,
                  size=Size}=Buffer) ->
    Buffer#buffer{acc=[Item | Acc],
                  size=Size+1}.

%% @doc Call the buffer function on the 
%% remaining items and then reset the buffer.
-spec flush(buffer()) -> buffer().
flush(#buffer{acc=Acc,
              buffer_fun=Fun}=Buffer) ->
    Fun(Acc),
    Buffer#buffer{acc=[],
                  size=0}.

%% @doc Call the specified buffer function on the 
%% remaining items and then reset the buffer.
-spec flush(buffer(), function()) -> buffer().
flush(#buffer{acc=Acc}=Buffer, Fun) ->
    Fun(Acc),
    Buffer#buffer{acc=[],
                  size=0}.

%% @doc Returns the size of the buffer.
-spec size(buffer()) -> non_neg_integer().
size(#buffer{size=Size}) ->
    Size.
