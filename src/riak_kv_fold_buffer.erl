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
         size/1]).

-export_type([buffer/0]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(buffer, {acc=[] :: [any()],
                 buffer_fun :: function(),
                 max_size :: pos_integer(),
                 size=0 :: non_neg_integer()}).
-type buffer() :: #buffer{}.

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Returns a new buffer with the specified
%% maximum size and buffer function.
-spec new(pos_integer(), fun(([any()]) -> any())) -> buffer().
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

%% @doc Returns the size of the buffer.
-spec size(buffer()) -> non_neg_integer().
size(#buffer{size=Size}) ->
    Size.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

fold_buffer_test_() ->
    Fun = fun(_) -> true end,
    Buffer = new(5, Fun),
    Buffer1 = add(1, Buffer),
    Buffer2 = add(2, Buffer1),
    Buffer3 = add(3, Buffer2),
    Buffer4 = add(4, Buffer3),
    Buffer5 = add(5, Buffer4),
    {spawn,
     [
      ?_assertEqual(#buffer{acc=[1],
                            buffer_fun=Fun,
                            max_size=4,
                            size=1},
                    Buffer1),
      ?_assertEqual(#buffer{acc=[2,1],
                            buffer_fun=Fun,
                            max_size=4,
                            size=2},
                    Buffer2),
      ?_assertEqual(#buffer{acc=[3,2,1],
                            buffer_fun=Fun,
                            max_size=4,
                            size=3},
                    Buffer3),
      ?_assertEqual(#buffer{acc=[4,3,2,1],
                            buffer_fun=Fun,
                            max_size=4,
                            size=4},
                    Buffer4),
      ?_assertEqual(#buffer{acc=[],
                            buffer_fun=Fun,
                            max_size=4,
                            size=0},
                    Buffer5),
      ?_assertEqual(Buffer5#buffer{acc=[],
                                   size=0},
                    flush(Buffer5)),
      ?_assertEqual(0, ?MODULE:size(Buffer)),
      ?_assertEqual(1, ?MODULE:size(Buffer1)),
      ?_assertEqual(2, ?MODULE:size(Buffer2)),
      ?_assertEqual(3, ?MODULE:size(Buffer3)),
      ?_assertEqual(4, ?MODULE:size(Buffer4)),
      ?_assertEqual(0, ?MODULE:size(Buffer5))
     ]}.

-endif.
