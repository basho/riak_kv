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
-compile(export_all).
-export([test/1,
         test/2,
         test/3,
         test/4,
         test/5]).

%% eqc_fsm callbacks
-export([initial_state/0,
         initial_state_data/0,
         next_state_data/5,
         dynamic_precondition/4,
         precondition/4,
         postcondition/5]).

%% eqc property
-export([prop_backend/4]).

%% States
-export([stopped/1,
         running/1]).

%% Helpers
-export([drop/2,
         delete/5,
         init_backend/3]).

-define(TEST_ITERATIONS, 250).

-record(qcst, {backend, % Backend module under test
               volatile, % Indicates if backend is volatile
               c,  % Backend config
               s,  % Module state returned by Backend:start
               olds=sets:new(), % Old states after a stop
               d=[], % Orddict of values stored
               i=ordsets:new()}). % List of indexes

%% ====================================================================
%% Public API
%% ====================================================================

test(Backend) ->
    test(Backend, false).

test(Backend, Volatile) ->
    test(Backend, Volatile, []).

test(Backend, Volatile, Config) ->
    test(Backend, Volatile, Config, fun(BeState,_Olds) ->
                catch(Backend:stop(BeState)) end).

test(Backend, Volatile, Config, Cleanup) ->
    test(Backend, Volatile, Config, Cleanup, ?TEST_ITERATIONS).

test(Backend, Volatile, Config, Cleanup, NumTests) ->
    eqc:quickcheck(eqc:numtests(NumTests,
                                prop_backend(Backend, Volatile, Config, Cleanup))).

%% ====================================================================
%% eqc property
%% ====================================================================

prop_backend(Backend, Volatile, Config, Cleanup) ->
    ?FORALL(Cmds,
            commands(?MODULE,
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
                                 ?debugFmt("BE Config: ~p~nBE State: ~p~nD: ~p~nI: ~p~n",
                                           [S#qcst.c,
                                            S#qcst.s,
                                            orddict:to_list(S#qcst.d),
                                            ordsets:to_list(S#qcst.i)])
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
    term_to_binary(riak_object:new(<<"b1">>, <<"k1">>, <<"v1">>)).

g_opts() ->
    frequency([{5, [async_fold]}, {2, []}]).

fold_keys_opts() ->
    frequency([{5, [async_fold]}, {2, []}, {2, [{index, bucket(), index_query()}]}, {2, [{bucket, bucket()}]}]).

index_specs() ->
    ?LET(L, list(index_spec()), lists:usort(L)).

index_spec() ->
    oneof([
           {add, bin_index(), bin_posting()},
           {remove, bin_index(), bin_posting()},
           {add, int_index(), int_posting()},
           {remove, int_index(), int_posting()}
          ]).

index_query() ->
    oneof([
           {eq, <<"$bucket">>, bucket()}, %% the bucket() in this query is ignored/transformed
           range_query(<<"$key">>, key(), key()),
           eq_query(),
           range_query()
          ]).

eq_query() ->
    oneof([
           {eq, bin_index(), bin_posting()},
           {eq, int_index(), int_posting()}
          ]).

range_query() ->
    oneof([
           range_query(bin_index(), bin_posting(), bin_posting()),
           range_query(int_index(), int_posting(), int_posting())
          ]).

range_query(Idx, Min0, Max0) ->
    ?LET({Min, Max}, {Min0, Max0},
         if Min > Max ->
                 {range, Idx, Max, Min};
            true ->
                 {range, Idx, Min, Max}
         end).

bin_index() -> elements([<<"x_bin">>, <<"y_bin">>, <<"z_bin">>]).
bin_posting() ->
    elements([ <<C>> || C <- lists:seq($a, $z) ]).

int_index() -> elements([<<"i_int">>, <<"j_int">>, <<"k_int">>]).
int_posting() ->
    int().



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
            riak_kv_fold_buffer:add({{Bucket, Key}, Value}, Acc)
    end.

get_partition() ->
    {MegaSecs, Secs, MicroSecs} = erlang:now(),
    Partition = integer_to_list(MegaSecs) ++
        integer_to_list(Secs) ++
        integer_to_list(MicroSecs),
    case erlang:get(Partition) of
        undefined ->
            erlang:put(Partition, ok),
            list_to_integer(Partition);
        _ ->
            get_partition()
    end.

%% @TODO Volatile is unused now so remove it. Will require
%% updating each backend module as well.
init_backend(Backend, _Volatile, Config) ->
    Partition = get_partition(),
    %% Start an async worker pool
    {ok, PoolPid} =
        riak_core_vnode_worker_pool:start_link(riak_kv_worker,
                                               2,
                                               Partition,
                                               [],
                                               worker_props),
    %% Shutdown any previous running worker pool
    case erlang:get(worker_pool) of
        undefined ->
            ok;
        OldPoolPid ->
            riak_core_vnode_worker_pool:stop(OldPoolPid, normal)
    end,
    %% Store the info about the worker pool
    erlang:put(worker_pool, PoolPid),
    %% Start the backend
    {ok, S} = Backend:start(Partition, Config),
    S.

drop(Backend, State) ->
    State1 = case Backend:drop(State) of
                 {ok, NewState} ->
                     NewState;
                 {error, _, NewState} ->
                     NewState
             end,
    Backend:stop(State1).

delete(Bucket, Key, Backend, BackendState, Indexes) ->
    IndexSpecs = [{remove, Idx, SKey} || {B,Idx,SKey,K} <- Indexes,
                                         B == Bucket,
                                         K == Key],
    case Backend:delete(Bucket, Key, IndexSpecs, BackendState) of
        {ok, NewState} ->
            {ok, NewState};
        {error, Reason, _NewState} ->
            {error, Reason}
    end.

get_fold_buffer() ->
    riak_kv_fold_buffer:new(100,
                            get_fold_buffer_fun({raw, foldid, self()})).

get_fold_buffer_fun(From) ->
    fun(Results) ->
            riak_core_vnode:reply(From,
                                  Results)
    end.

%% @private
finish_fun(Sender) ->
    fun(Buffer) ->
            finish_fold(Buffer, Sender)
    end.

%% @private
finish_fold(Buffer, Sender) ->
    riak_kv_fold_buffer:flush(Buffer),
    riak_core_vnode:reply(Sender, done).

receive_fold_results(Acc) ->
    receive
        {_, done} ->
            Acc;
        {error, Error} ->
            ?debugFmt("Error occurred: ~p~n", [Error]),
            [];
        {_, Results} ->
            receive_fold_results(Acc++Results)
    end.

-type index_list() :: ordsets:ordset({riak_object:bucket(), binary(), binary() | integer(), riak_object:key()}).
-spec update_indexes(Bucket, Key, IndexSpecs, Indexes) -> Indexes1 when
      Bucket :: riak_object:bucket(),
      Key :: riak_object:key(),
      IndexSpecs :: [ {add, binary(), binary() | integer()} | {remove, binary(), binary() | integer()} ],
      Indexes :: index_list(),
      Indexes1 :: index_list().
update_indexes(_Bucket, _Key, [], Indexes) ->
    Indexes;
update_indexes(Bucket, Key, [{add, Index, SKey}|T], Indexes) ->
    PostingKey = {Bucket, Index, SKey, Key},
    update_indexes(Bucket, Key, T, ordsets:add_element(PostingKey, Indexes));
update_indexes(Bucket, Key, [{remove, Index, SKey}|T], Indexes) ->
    PostingKey = {Bucket, Index, SKey, Key},
    update_indexes(Bucket, Key, T, ordsets:del_element(PostingKey,Indexes)).

remove_indexes(Bucket, Key, Indexes) ->
    ordsets:filter(fun({B,_,_,K}) ->
                           B /= Bucket orelse K /= Key
                   end, Indexes).

%%====================================================================
%% eqc_fsm callbacks
%%====================================================================

initial_state() ->
    {stopped, true}.

initial_state_data() ->
    #qcst{d = orddict:new()}.

initial_state_data(Backend, Volatile, Config) ->
    #qcst{backend=Backend,
          c=Config,
          d=orddict:new(),
          volatile=Volatile}.

next_state_data(running, stopped, S, _R,
                {call, _M, stop, _}) ->
    S#qcst{d=orddict:new(),
           olds = sets:add_element(S#qcst.s, S#qcst.olds),
           i= ordsets:new()};
next_state_data(stopped, running, S, R, {call, _M, init_backend, _}) ->
    S#qcst{s=R};
next_state_data(_From, _To, S, _R, {call, _M, put, [Bucket, Key, IndexSpecs, Val, _]}) ->
    S#qcst{d = orddict:store({Bucket, Key}, Val, S#qcst.d),
           i = update_indexes(Bucket, Key, IndexSpecs, S#qcst.i)};
next_state_data(_From, _To, S, _R, {call, _M, delete, [Bucket, Key|_]}) ->
    S#qcst{d = orddict:erase({Bucket, Key}, S#qcst.d),
           i = remove_indexes(Bucket, Key, S#qcst.i)};
next_state_data(_From, _To, S, _R, {call, ?MODULE, drop, _}) ->
    
    S#qcst{d=orddict:new(),
           s=undefined,
           i=ordsets:new()};
next_state_data(_From, _To, S, _R, _C) ->
    S.

stopped(#qcst{backend=Backend,
              c=Config,
              volatile=Volatile}) ->
    [{running,
      {call, ?MODULE, init_backend, [Backend, Volatile, Config]}}].

running(#qcst{backend=Backend,
              s=State,
              i=Indexes}) ->
    [
     {history, {call, Backend, put, [bucket(), key(), index_specs(), val(), State]}},
     {history, {call, Backend, get, [bucket(), key(), State]}},
     {history, {call, ?MODULE, delete, [bucket(), key(), Backend, State, Indexes]}},
     {history, {call, Backend, fold_buckets, [fold_buckets_fun(), get_fold_buffer(), g_opts(), State]}},
     {history, {call, Backend, fold_keys, [fold_keys_fun(), get_fold_buffer(), fold_keys_opts(), State]}},
     {history, {call, Backend, fold_objects, [fold_objects_fun(), get_fold_buffer(), g_opts(), State]}},
     {history, {call, Backend, is_empty, [State]}},
     {stopped, {call, ?MODULE, drop, [Backend, State]}},
     {stopped, {call, Backend, stop, [State]}}
    ].

dynamic_precondition(_From,_To,#qcst{backend=Backend},{call, _M, fold_keys, [_FoldFun, _Acc, [{index, Bucket, _}], BeState]}) ->
    {ok, Capabilities} = Backend:capabilities(Bucket, BeState),
    lists:member(indexes, Capabilities);
dynamic_precondition(_From,_To,_S,_C) ->
    true.

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
              {call, _M, put, [_Bucket, _Key, _IndexEntries, _Val, _BeState]}, {R, _RState}) ->
    R =:= ok orelse R =:= already_exists;
postcondition(_From, _To, _S,
              {call, _M, delete, _}, {R, _RState}) ->
    R =:= ok;
postcondition(_From, _To, S,
              {call, _M, fold_buckets, [_FoldFun, _Acc, _Opts, _BeState]}, FoldRes) ->
    ExpectedEntries = orddict:to_list(S#qcst.d),
    Buckets = [Bucket || {{Bucket, _}, _} <- ExpectedEntries],
    From = {raw, foldid, self()},
    case FoldRes of
        {async, Work} ->
            Pool = erlang:get(worker_pool),
            FinishFun = finish_fun(From),
            riak_core_vnode_worker_pool:handle_work(Pool, {fold, Work, FinishFun}, From);
        {ok, Buffer} ->
            finish_fold(Buffer, From)
    end,
    R = receive_fold_results([]),
    lists:usort(Buckets) =:= lists:sort(R);
postcondition(_From, _To, S,
              {call, _M, fold_keys, [_FoldFun, _Acc, [{index, Bucket,{eq, <<"$bucket">>, _}}], _BeState]}, FoldRes) ->
    ExpectedEntries = orddict:to_list(S#qcst.d),
    Keys = [{B, Key} || {{B, Key}, _} <- ExpectedEntries, B == Bucket],
    From = {raw, foldid, self()},
    case FoldRes of
        {async, Work} ->
            Pool = erlang:get(worker_pool),
            FinishFun = finish_fun(From),
            riak_core_vnode_worker_pool:handle_work(Pool, {fold, Work, FinishFun}, From);
        {ok, Buffer} ->
            finish_fold(Buffer, From)
    end,
    R = receive_fold_results([]),
    lists:sort(Keys) =:= lists:sort(R);
postcondition(_From, _To, S,
              {call, _M, fold_keys, [_FoldFun, _Acc, [{index, Bucket,{range, <<"$key">>, Min, Max}}], _BeState]}, FoldRes) ->
    ExpectedEntries = orddict:to_list(S#qcst.d),
    Keys = [{B, Key} || {{B, Key}, _} <- ExpectedEntries,
                        B == Bucket, Key =< Max, Key >= Min],
    From = {raw, foldid, self()},
    case FoldRes of
        {async, Work} ->
            Pool = erlang:get(worker_pool),
            FinishFun = finish_fun(From),
            riak_core_vnode_worker_pool:handle_work(Pool, {fold, Work, FinishFun}, From);
        {ok, Buffer} ->
            finish_fold(Buffer, From)
    end,
    R = receive_fold_results([]),
    case lists:sort(Keys) =:= lists:sort(R) of
        true -> true;
        _ ->
            [{expected, Keys},{received, R}]
    end;
postcondition(_From, _To, S,
              {call, _M, fold_keys, [_FoldFun, _Acc, [{index, Bucket, {eq, Idx, SKey}}], _BeState]}, FoldRes) ->
    Keys = [ {B,K} || {B, I, V, K} <- ordsets:to_list(S#qcst.i),
                      B == Bucket, I == Idx, V == SKey],
    From = {raw, foldid, self()},
    case FoldRes of
        {async, Work} ->
            Pool = erlang:get(worker_pool),
            FinishFun = finish_fun(From),
            riak_core_vnode_worker_pool:handle_work(Pool, {fold, Work, FinishFun}, From);
        {ok, Buffer} ->
            finish_fold(Buffer, From)
    end,
    R = receive_fold_results([]),
    case lists:sort(Keys) =:= lists:sort(R) of
        true -> true;
        _ ->
            [{expected, Keys},{received, R}]
    end;
postcondition(_From, _To, S,
              {call, _M, fold_keys, [_FoldFun, _Acc, [{index, Bucket, {range, Idx, Min, Max}}], _BeState]}, FoldRes) ->
    Keys = [ {B, K} || {B, I, V, K} <- ordsets:to_list(S#qcst.i),
                       B == Bucket, I == Idx, V =< Max, V >= Min ],
    From = {raw, foldid, self()},
    case FoldRes of
        {async, Work} ->
            Pool = erlang:get(worker_pool),
            FinishFun = finish_fun(From),
            riak_core_vnode_worker_pool:handle_work(Pool, {fold, Work, FinishFun}, From);
        {ok, Buffer} ->
            finish_fold(Buffer, From)
    end,
    R = receive_fold_results([]),
    case lists:sort(Keys) =:= lists:sort(R) of
        true -> true;
        _ ->
            [{expected, Keys},{received, R}]
    end;
postcondition(_From, _To, S,
              {call, _M, fold_keys, [_FoldFun, _Acc, [{bucket, B}], _BeState]}, FoldRes) ->
    ExpectedEntries = orddict:to_list(S#qcst.d),
    Keys = [{Bucket, Key} || {{Bucket, Key}, _} <- ExpectedEntries, Bucket == B],
    From = {raw, foldid, self()},
    case FoldRes of
        {async, Work} ->
            Pool = erlang:get(worker_pool),
            FinishFun = finish_fun(From),
            riak_core_vnode_worker_pool:handle_work(Pool, {fold, Work, FinishFun}, From);
        {ok, Buffer} ->
            finish_fold(Buffer, From)
    end,
    R = receive_fold_results([]),
    case lists:sort(Keys) =:= lists:sort(R) of
        true -> true;
        _ ->
            [{expected, Keys},{received, R}]
    end;
postcondition(_From, _To, S,
              {call, _M, fold_keys, [_FoldFun, _Acc, _Opts, _BeState]}, FoldRes) ->
    ExpectedEntries = orddict:to_list(S#qcst.d),
    Keys = [{Bucket, Key} || {{Bucket, Key}, _} <- ExpectedEntries],
    From = {raw, foldid, self()},
    case FoldRes of
        {async, Work} ->
            Pool = erlang:get(worker_pool),
            FinishFun = finish_fun(From),
            riak_core_vnode_worker_pool:handle_work(Pool, {fold, Work, FinishFun}, From);
        {ok, Buffer} ->
            finish_fold(Buffer, From)
    end,
    R = receive_fold_results([]),
    case lists:sort(Keys) =:= lists:sort(R) of
        true -> true;
        _ ->
            [{expected, Keys},{received, R}]
    end;
postcondition(_From, _To, S,
              {call, _M, fold_objects, [_FoldFun, _Acc, _Opts, _BeState]}, FoldRes) ->
    ExpectedEntries = orddict:to_list(S#qcst.d),
    Objects = [Object || Object <- ExpectedEntries],
    From = {raw, foldid, self()},
    case FoldRes of
        {async, Work} ->
            Pool = erlang:get(worker_pool),
            FinishFun = finish_fun(From),
            riak_core_vnode_worker_pool:handle_work(Pool, {fold, Work, FinishFun}, From);
        {ok, Buffer} ->
            finish_fold(Buffer, From)
    end,
    R = receive_fold_results([]),
    lists:sort(Objects) =:= lists:sort(R);
postcondition(_From, _To, S,{call, _M, is_empty, [_BeState]}, R) ->
    R =:= (orddict:size(S#qcst.d) =:= 0);
postcondition(_From, _To, _S, _C, _R) ->
    true.

-endif.

