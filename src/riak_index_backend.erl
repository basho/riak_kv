%% -------------------------------------------------------------------
%%
%% riak_index_backend: Riak Index backend behaviour
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
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

-module(riak_index_backend).
-export([behaviour_info/1]).
-export([callback_after/3]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-export([standard_test/2]).
-endif.

-spec behaviour_info(atom()) -> 'undefined' | [{atom(), arity()}].
behaviour_info(callbacks) ->
    [{start,2},        % (Partition, Config)
     {stop,1},         % (State)
     {index, 2},       % (State, Postings)
     {delete, 2},      % (State, Postings)
     {lookup_sync, 4}, % (State, Index, Field, Term)
     {fold_index, 6},  % (State, Index, Query, SKFun, Acc, FinalFun)
     {drop,1},         % (State)
     {callback,3}];    % (State, Ref, Msg) ->
behaviour_info(_Other) ->
    undefined.

%% Queue a callback for the backend after Time ms.
-spec callback_after(integer(), reference(), term()) -> reference().
callback_after(Time, Ref, Msg) when is_integer(Time), is_reference(Ref) ->
    riak_core_vnode:send_command_after(Time, {backend_callback, Ref, Msg}).

-ifdef(TEST).

standard_test(BackendMod, Config) ->
    %% Start the backend...
    {ok, State} = BackendMod:start(42, Config),

    %% Index some postings...
    Postings1 = [
                 {<<"bucket">>, "field", "term1", "value_a", [], 1},
                 {<<"bucket">>, "field", "term1", "value_b", [], 1},
                 {<<"bucket">>, "field", "term1", "value_c", [], 1}
                ],
    BackendMod:index(State, Postings1),

    %% Retrieve what we just indexed...
    Results1 = [
                {"value_a", [{"field", "term1"}]},
                {"value_b", [{"field", "term1"}]},
                {"value_c", [{"field", "term1"}]}
               ],
    ?assertEqual(Results1, BackendMod:lookup_sync(State, <<"bucket">>, "field", "term1")),

    %% Index another posting...
    Postings2 = [
                 {<<"bucket">>, "field", "term2", "value_d", [], 1},
                 {<<"bucket">>, "field", "term2", "value_e", [], 1},
                 {<<"bucket">>, "field", "term2", "value_f", [], 1}
                ],
    BackendMod:index(State, Postings2),

    %% Retrieve what we just indexed...
    Results2 = [
                {"value_d", [{"field", "term2"}]},
                {"value_e", [{"field", "term2"}]},
                {"value_f", [{"field", "term2"}]}
               ],
    ?assertEqual(Results2, BackendMod:lookup_sync(State, <<"bucket">>, "field", "term2")),

    %% Delete some postings...
    Postings3 = [
                 {<<"bucket">>, "field", "term1", "value_a", [], 2},
                 {<<"bucket">>, "field", "term1", "value_c", [], 2}
                ],
    BackendMod:delete(State, Postings3),

    %% Retrieve some postings again...
    Results3 = [
                {"value_b", [{"field", "term1"}]}
               ],
    ?assertEqual(Results3, BackendMod:lookup_sync(State, <<"bucket">>, "field", "term1")),

    %% Drop the data...
    BackendMod:drop(State),

    %% Retrieve some postings again...
    Results4 = [],
    ?assertEqual(Results4, BackendMod:lookup_sync(State, <<"bucket">>, "field", "term1")),
    ?assertEqual(Results4, BackendMod:lookup_sync(State, <<"bucket">>, "field", "term2")),

    %% Stop the backend...
    ok = BackendMod:stop(State).

-endif. % TEST
