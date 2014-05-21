%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 Basho Technologies, Inc.  All Rights Reserved.
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

-module(tracer_large4).
-compile(export_all).

-record(r_object, {bucket         = '_',
                   key            = '_',
                   contents       = '_',
                   vclock         = '_',
                   updatemetadata = '_',
                   updatevalue    = '_'
                  }).

go(Time, Count, Size) ->
    ss(),
    %% gets
    GetMS = [{['_',
               #r_object{bucket='$1',
                         key='$2'}],
              [],
              [{message,{{'$1','$2'}}}, {return_trace}]}],
    erlang:trace_pattern({riak_kv_get_fsm, calculate_objsize, 2}, GetMS, [local]),

    %% puts
    PutMS = [{['$1','$2','_','$3','_'],
              [{'>',{size,'$3'},Size}],
              [{message,{{'$1','$2',{size,'$3'}}}}]}],
    erlang:trace_pattern({riak_kv_eleveldb_backend, put, 5}, PutMS, [local]),
    erlang:trace_pattern({riak_kv_bitcask_backend, put, 5}, PutMS, [local]),
    erlang:trace_pattern({riak_kv_memory_backend, put, 5}, PutMS, [local]),

    {Tracer, _} = spawn_monitor(?MODULE, tracer, [0, Count, Size, dict:new()]),
    erlang:trace(all, true, [call, arity, {tracer, Tracer}]),
    receive
        {'DOWN', _, process, Tracer, _} ->
            ok
    after Time ->
            exit(Tracer, kill),
            receive
                {'DOWN', _, process, Tracer, _} ->
                    ok
            end
    end,
    ss(),
    io:format("object trace stopped~n").

tracer(Limit, Limit, _, _) ->
    ok;
tracer(Count, Limit, Threshold, Objs) ->
    receive
        {trace,Pid,call,{riak_kv_get_fsm,calculate_objsize,2},{Bucket,Key}} ->
            Objs2 = dict:store(Pid, {Bucket,Key}, Objs),
            tracer(Count+1, Limit, Threshold, Objs2);
        {trace,Pid,return_from,{riak_kv_get_fsm,calculate_objsize,2},Size} ->
            case Size >= Threshold of
                true ->
                    case dict:find(Pid, Objs) of
                        {ok, {Bucket, Key}} ->
                            io:format("~p: get: ~p~n", [ts(), {Bucket, Key, Size}]);
                        _ ->
                            ok
                    end;
                false ->
                    ok
            end,
            Objs2 = dict:erase(Pid, Objs),
            tracer(Count+1, Limit, Threshold, Objs2);
        {trace,_Pid,call,{riak_kv_eleveldb_backend,put,5},{Bucket,Key,Size}} ->
            io:format("~p: put(l): ~p~n", [ts(), {Bucket, Key, Size}]),
            tracer(Count+1, Limit, Threshold, Objs);
        {trace,_Pid,call,{riak_kv_bitcask_backend,put,5},{Bucket,Key,Size}} ->
            io:format("~p: put(b): ~p~n", [ts(), {Bucket, Key, Size}]),
            tracer(Count+1, Limit, Threshold, Objs);
        {trace,_Pid,call,{riak_kv_memory_backend,put,5},{Bucket,Key,Size}} ->
            io:format("~p: put(m): ~p~n", [ts(), {Bucket, Key, Size}]),
            tracer(Count+1, Limit, Threshold, Objs);
        Msg ->
            io:format("tracer: ~p~n", [Msg]),
            tracer(Count+1, Limit, Threshold, Objs)
    end.

ts() ->
    calendar:now_to_datetime(os:timestamp()).

ss() ->
    erlang:trace_pattern({'_','_','_'}, false, [local]),
    erlang:trace(all, false, [call, arity]).
