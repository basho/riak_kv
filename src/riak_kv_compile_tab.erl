%% -------------------------------------------------------------------
%%
%% Store the state about what bucket type DDLs have been compiled.
%%
%% Copyright (c) 2015 Basho Technologies, Inc.  All Rights Reserved.
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

%% TODO use dets

-module(riak_kv_compile_tab).

-export([is_compiling/1]).
-export([get_state/1]).
-export([new/1]).
-export([insert/4]).
-export([update_state/2]).

-define(TABLE, ?MODULE).

-type compiling_state() :: compiling | compiled | failed.
-export_type([compiling_state/0]).

-define(is_compiling_state(S), 
        (S == compiling orelse
         S == compiled orelse
         S == failed)).

%% 
-spec new(file:name()) ->
        {ok, dets:tab_name()} | {error, any()}.
new(File_dir) ->
    File_path = filename:join(File_dir, [?TABLE, ".dets"]),
    dets:open_file(?TABLE, [{type, set}, {repair, force}, {file, File_path}]).

%%
-spec insert(Bucket_type :: binary(),
             DDL :: term(),
             Compiler_pid :: pid(),
             State :: compiling_state()) -> ok.
insert(Bucket_type, DDL, Compiler_pid, State) ->
    dets:insert(?TABLE, {Bucket_type, DDL, Compiler_pid, State}),
    ok.

%% Check if the bucket type is in the compiling state.
-spec is_compiling(Bucket_type :: binary()) ->
    {true, pid()} | false.
is_compiling(Bucket_type) ->
    case dets:lookup(?TABLE, Bucket_type) of
        [{_,_,Pid,compiling}] ->
            {true, Pid};
        _ ->
            false
    end.

%%
-spec get_state(Bucket_type :: binary()) ->
        compiling_state() | notfound.
get_state(Bucket_type) when is_binary(Bucket_type) ->
    case dets:lookup(?TABLE, Bucket_type) of
        [{_,_,_,State}] ->
            State;
        [] ->
            notfound
    end.

%% Update the compilation state using the compiler pid as a key.
-spec update_state(Compiler_pid :: pid(), State :: compiling_state()) ->
        ok | notfound.
update_state(Compiler_pid, State) when is_pid(Compiler_pid), 
                                       ?is_compiling_state(State) ->
    case dets:match(?TABLE, {'$1','$2',Compiler_pid,'_'}) of
        [[Bucket_type, DDL]] ->
            insert(Bucket_type, DDL, Compiler_pid, State);
        [] ->
            notfound
    end.

%% ===================================================================
%% EUnit tests
%% ===================================================================

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-define(in_process(TestCode),
    Self = self(),
    spawn_link(
        fun() ->
            _ = riak_kv_compile_tab:new("."),
            TestCode,
            Self ! test_ok
        end),
    receive
        test_ok -> ok
    end
).

insert_test() ->
    ?in_process(
        begin
            Pid = spawn(fun() -> ok end),
            ok = insert(<<"my_type">>, {ddl_v1}, Pid, compiling),
            ?assertEqual(
                compiling,
                get_state(<<"my_type">>)
            )
        end).

update_state_test() ->
    ?in_process(
        begin
            Pid = spawn(fun() -> ok end),
            ok = insert(<<"my_type">>, {ddl_v1}, Pid, compiling),
            ok = update_state(Pid, compiled),
            ?assertEqual(
                compiled,
                get_state(<<"my_type">>)
            )
        end).

is_compiling_test() ->
    ?in_process(
        begin
            Pid = spawn(fun() -> ok end),
            ok = insert(<<"my_type">>, {ddl_v1}, Pid, compiling),
            ?assertEqual(
                {true, Pid},
                is_compiling(<<"my_type">>)
            )
        end).

-endif.