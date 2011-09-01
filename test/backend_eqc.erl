%% -------------------------------------------------------------------
%%
%% backend_eqc: Quickcheck testing for the backend api.
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

-module(backend_eqc).

-ifdef(EQC).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_fsm.hrl").
-include_lib("eunit/include/eunit.hrl").

%% Public API
-export([test/1,
         test/2,
         test/3,
         test/4,
         test/5]).

%% eqc_fsm callbacks
-export([initial_state/0,
         initial_state_data/0,
         next_state_data/5,
         precondition/4,
         postcondition/5]).

%% eqc property
-export([prop_backend/4]).

%% States
-export([stopped/1,
         running/1]).

%% Helpers
-export([drop/2,
         init_backend/3]).

-define(TEST_ITERATIONS, 50).

-record(qcst, {backend, % Backend module under test
               volatile, % Indicates if backend is volatile
               c,  % Backend config
               s,  % Module state returned by Backend:start
               olds=sets:new(), % Old states after a stop
               worker_pool, % PID of worker pool for async folds
               d=[]}).% Orddict of values stored

%% ====================================================================
%% Public API
%% ====================================================================

test(Backend) ->
    test(Backend, false).

test(Backend, Volatile) ->
    test(Backend, Volatile, []).

test(Backend, Volatile, Config) ->
    test(Backend, Volatile, Config, fun(_BeState,_Olds) -> ok end).

test(Backend, Volatile, Config, Cleanup) ->
    test(Backend, Volatile, Config, Cleanup, ?TEST_ITERATIONS).

test(Backend, Volatile, Config, Cleanup, NumTests) ->
    eqc:quickcheck(eqc:numtests(NumTests,
                                prop_backend(Backend, Volatile, Config, Cleanup))).

%% ====================================================================
%% eqc property
%% ====================================================================

prop_backend(Backend, Volatile, Config, Cleanup) ->
    ?FORALL(Cmds, commands(?MODULE,
                           {stopped,
                            initial_state_data(Backend, Volatile, Config)}),
            begin
                {H,{_F,S},Res} = run_commands(?MODULE, Cmds),
                Cleanup(S#qcst.s, sets:to_list(S#qcst.olds)),
                aggregate(zip(state_names(H), command_names(Cmds)),
                          ?WHENFAIL(
                             begin
                                 ?debugFmt("Cmds: ~p~n",
                                           [zip(state_names(H),
                                                command_names(Cmds))]),
                                 ?debugFmt("Result: ~p~n", [Res]),
                                 ?debugFmt("History: ~p~n", [H]),
                                 ?debugFmt("BE Config: ~p~nBE State: ~p~nD: ~p~n",
                                           [S#qcst.c,
                                            S#qcst.s,
                                            orddict:to_list(S#qcst.d)])
                             end,
                             equals(ok, Res)))
            end
           ).

%%====================================================================
%% Generators
%%====================================================================

bucket() ->
    elements([<<"b1">>,<<"b2">>,<<"b3">>,<<"b4">>]).

key() ->
    elements([<<"k1">>,<<"k2">>,<<"k3">>,<<"k4">>]).

val() ->
    %% The creation of the riak object and the call
    %% to term_to_binary are to facilitate testing
    %% of riak_kv_index_backend. It does not matter
    %% that the bucket and key in the object may
    %% differ since at this point in the processing
    %% pipeline the information has already been
    %% extracted.
    term_to_binary(riak_object:new(<<"b1">>, <<"k1">>, binary())).

%%====================================================================
%% Helpers
%%====================================================================

fold_buckets_fun() ->
    fun(Bucket, Acc) ->
            riak_kv_fold_buffer:add(Bucket, Acc)
    end.

fold_keys_fun() ->
    fun(Bucket, Key, Acc) ->
            riak_kv_fold_buffer:add({Bucket, Key}, Acc)
    end.

fold_objects_fun() ->
    fun(Bucket, Key, Value, Acc) ->
            [{{Bucket, Key}, Value} | Acc]
    end.

init_backend(Backend, Volatile, Config) ->
    {ok, S} = Backend:start(42, Config),
    case Volatile of
        true ->
            S;
        false ->
            %% Drop the backend
            {ok, S1} = Backend:drop(S),
            S1
    end.

drop(Backend, State) ->
    case Backend:drop(State) of
        {ok, NewState} ->
            NewState;
        {error, _, NewState} ->
            NewState
    end.

get_fold_buffer() ->    
    riak_kv_fold_buffer:new(100, 
                            get_fold_buffer_fun({raw, foldid, self()})).

get_fold_buffer_fun(From) ->
    fun(Results) ->
            riak_core_vnode:reply(From,
                                  {results, Results})
    end.

get_flush_buffer_fun(From) ->
    fun(Results) ->
            riak_core_vnode:reply(From,
                                  {final_results, Results})
    end.

receive_fold_results(Acc) ->
    receive
        %% {results, {_, Results}} ->
        %%     receive_fold_results(Acc++Results);
        {_, {results, Results}} ->
            receive_fold_results(Acc++Results);
        %% {final_results, {_, Results}} ->
        %%     Acc++Results;
        {_, {final_results, Results}} ->
            Acc++Results;
        {_, Error} ->
            ?debugFmt("Error occurred: ~p~n", [Error]),
            []
    end.

%%====================================================================
%% eqc_fsm callbacks
%%====================================================================

initial_state() ->
    {stopped, true}.

initial_state_data() ->
    #qcst{d = orddict:new()}.

initial_state_data(Backend, Volatile, Config) ->
    {ok, PoolPid} =
        riak_core_vnode_worker_pool:start_link(riak_kv_fold_worker,
                                               2,
                                               42,
                                               [],
                                               worker_props),
    #qcst{backend=Backend,
          c=Config,
          d=orddict:new(),
          volatile=Volatile,
          worker_pool=PoolPid}.

next_state_data(running, stopped, S, _R,
                {call, _M, stop, _}) ->
    S#qcst{d=orddict:new(),
           olds = sets:add_element(S#qcst.s, S#qcst.olds)};
next_state_data(_From, _To, S, R, {call, _M, init_backend, _}) ->
    S#qcst{s=R};
next_state_data(_From, _To, S, _R, {call, _M, put, [Bucket, Key, [], Val, _]}) ->
    S#qcst{d = orddict:store({Bucket, Key}, Val, S#qcst.d)};
next_state_data(_From, _To, S, _R, {call, _M, delete, [Bucket, Key, _]}) ->
    S#qcst{d = orddict:erase({Bucket, Key}, S#qcst.d)};
next_state_data(_From, _To, S, R, {call, ?MODULE, drop, _}) ->
    S#qcst{d=orddict:new(), s=R};
next_state_data(_From, _To, S, _R, _C) ->
    S.

stopped(#qcst{backend=Backend,
              c=Config,
              volatile=Volatile}) ->
    [{running,
      {call, ?MODULE, init_backend, [Backend, Volatile, Config]}}].

running(#qcst{backend=Backend,
              s=State}) ->
    [
     {history, {call, Backend, put, [bucket(), key(), [], val(), State]}},
     {history, {call, Backend, get, [bucket(), key(), State]}},
     {history, {call, Backend, delete, [bucket(), key(), State]}},
     {history, {call, Backend, fold_buckets, [fold_buckets_fun(), get_fold_buffer(), [], State]}},
     {history, {call, Backend, fold_keys, [fold_keys_fun(), get_fold_buffer(), [], State]}},
     {history, {call, Backend, fold_objects, [fold_objects_fun(), [], [], State]}},
     {history, {call, Backend, is_empty, [State]}},
     {history, {call, ?MODULE, drop, [Backend, State]}},
     {stopped, {call, Backend, stop, [State]}}
    ].

precondition(_From,_To,_S,_C) ->
    true.

postcondition(_From, _To, S, _C={call, _M, get, [Bucket, Key, _BeState]}, R) ->
    case R of
        {error, not_found, _} ->
            not orddict:is_key({Bucket, Key}, S#qcst.d);
        {ok, Val, _} ->
            Res = orddict:find({Bucket, Key}, S#qcst.d),
            {ok, Val} =:= Res
    end;
postcondition(_From, _To, _S,
              {call, _M, put, [_Bucket, _Key, _Val, _BeState]}, {R, _RState}) ->
    R =:= ok orelse R =:= already_exists;
postcondition(_From, _To, _S,
              {call, _M, delete,[_Bucket, _Key, _BeState]}, {R, _RState}) ->
    R =:= ok;
postcondition(_From, _To, S,
              {call, _M, fold_buckets, [_FoldFun, _Acc, _Opts, _BeState]}, {ok, Buf}) ->
    ExpectedEntries = orddict:to_list(S#qcst.d),
    Buckets = [Bucket || {{Bucket, _}, _} <- ExpectedEntries],
    From = {raw, foldid, self()}, 
    riak_kv_fold_buffer:flush(Buf, get_flush_buffer_fun(From)),
    R = receive_fold_results([]),
    lists:usort(Buckets) =:= lists:sort(R);
postcondition(_From, _To, S=#qcst{worker_pool=Pool},
              {call, _M, fold_buckets, [_FoldFun, _Acc, _Opts, _BeState]}, {async, Work}) ->
    ExpectedEntries = orddict:to_list(S#qcst.d),
    Buckets = [Bucket || {{Bucket, _}, _} <- ExpectedEntries],
    From = {raw, foldid, self()}, 
    riak_core_vnode_worker_pool:handle_work(Pool, Work, From),
    R = receive_fold_results([]),
    lists:usort(Buckets) =:= lists:sort(R);
postcondition(_From, _To, S,
              {call, _M, fold_keys, [_FoldFun, _Acc, _Opts, _BeState]}, {ok, Buf}) ->
    ExpectedEntries = orddict:to_list(S#qcst.d),
    Keys = [{Bucket, Key} || {{Bucket, Key}, _} <- ExpectedEntries],
    From = {raw, foldid, self()}, 
    riak_kv_fold_buffer:flush(Buf, get_flush_buffer_fun(From)),
    R = receive_fold_results([]),
    lists:sort(Keys) =:= lists:sort(R);
postcondition(_From, _To, S=#qcst{worker_pool=Pool},
              {call, _M, fold_keys, [_FoldFun, _Acc, _Opts, _BeState]}, {async, Work}) ->
    ExpectedEntries = orddict:to_list(S#qcst.d),
    Keys = [{Bucket, Key} || {{Bucket, Key}, _} <- ExpectedEntries],
    From = {raw, foldid, self()}, 
    riak_core_vnode_worker_pool:handle_work(Pool, Work, From),
    R = receive_fold_results([]),
    lists:sort(Keys) =:= lists:sort(R);
postcondition(_From, _To, S,
              {call, _M, fold_objects, [_FoldFun, _Acc, _Opts, _BeState]}, {ok, R}) ->
    ExpectedEntries = orddict:to_list(S#qcst.d),
    Objects = [Object || Object <- ExpectedEntries],
    lists:sort(Objects) =:= lists:sort(R);
postcondition(_From, _To, S,{call, _M, is_empty, [_BeState]}, R) ->
    R =:= (orddict:size(S#qcst.d) =:= 0);
postcondition(_From, _To, _S, _C, _R) ->
    true.

-endif.

