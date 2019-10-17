%% -------------------------------------------------------------------
%%
%% od_flag_eqc: Drive out the merge bugs the other statem couldn't
%%
%% Copyright (c) 2007-2014 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

-module(od_flag_eqc).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("eunit/include/eunit.hrl").
-export([initial_state/0, weight/2, prop_merge/0]).

-export([enable_pre/1, enable_args/1, enable/2, enable_next/3,
         enable_post/3]).

-export([create_replica_pre/2, create_replica/1, create_replica_next/3]).

-export([disable_pre/1, disable_args/1, disable_pre/2, disable/1, disable_post/3]).

-export([ctx_disable_dynamicpre/2, ctx_disable/2, ctx_disable_post/3]).

-export([replicate_pre/1, replicate_args/1, replicate_pre/2, replicate/2,
         replicate_post/3]).

-record(state,{replicas=[] :: [binary()], %% Sort of like the ring, upto N*2 ids
               counter=1 :: pos_integer() %% a unique tag per add
              }).

-define(NUMTESTS, 1000).
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) ->
                              io:format(user, Str, Args) end, P)).

%% Initialize the state
-spec initial_state() -> eqc_statem:symbolic_state().
initial_state() ->
    #state{}.

%% ------ Grouped operator: create_replica
create_replica_pre(#state{replicas=Replicas}) ->
    length(Replicas) < 10.

%% @doc create_replica_command - Command generator
create_replica_args(_S) ->
    %% Don't waste time shrinking the replicas ID binaries, they 8
    %% byte binaris as that is riak-esque.
    [noshrink(binary(8))].

%% @doc create_replica_pre - don't create a replica that already
%% exists
-spec create_replica_pre(S :: eqc_statem:symbolic_state(),
                         Args :: [term()]) -> boolean().
create_replica_pre(#state{replicas=Replicas}, [Id]) ->
    not lists:member(Id, Replicas).

%% @doc create a new replica, and store a bottom orswot/orset+deferred
%% in ets
create_replica(Id) ->
    ets:insert(od_flag_eqc, {Id, riak_dt_od_flag:new(), model_new()}).

%% @doc create_replica_next - Add the new replica ID to state
-spec create_replica_next(S :: eqc_statem:symbolic_state(),
                          V :: eqc_statem:var(),
                          Args :: [term()]) -> eqc_statem:symbolic_state().
create_replica_next(S=#state{replicas=R0}, _Value, [Id]) ->
    S#state{replicas=R0++[Id]}.

%% ------ Grouped operator: enable
%% enable the flag
enable_pre(#state{replicas=Replicas}) ->
    Replicas /= [].

enable_args(#state{replicas=Replicas, counter=Cnt}) ->
    [
     elements(Replicas), % The replica
     Cnt
    ].

enable_pre(#state{replicas=Replicas}, [Replica, _Cnt]) ->
    lists:member(Replica, Replicas).

enable(Actor, Cnt) ->
    [{Actor, Flag, Model}] = ets:lookup(od_flag_eqc, Actor),
    {ok, Flag2} = riak_dt_od_flag:update(enable, Actor, Flag),
    {ok, Model2} = model_enable(Cnt, Model),
    ets:insert(od_flag_eqc, {Actor, Flag2, Model2}),
    {Flag2, Model2}.

enable_next(S=#state{counter=Cnt}, _Res, [_, _]) ->
    S#state{counter=Cnt+1}.

enable_post(_S, _Args, Res) ->
    post_all(Res, enable).

%% ------ Grouped operator: disable

disable_pre(#state{replicas=Replicas}) ->
    Replicas /= [].

disable_args(#state{replicas=Replicas}) ->
    [
     elements(Replicas)
    ].

disable_pre(#state{replicas=Replicas}, [Replica]) ->
    lists:member(Replica, Replicas).

disable(Replica) ->
    [{Replica, Flag, Model}] = ets:lookup(od_flag_eqc, Replica),
    {ok, Flag2} = riak_dt_od_flag:update(disable, Replica, Flag),
    Model2 = model_disable(Model),
    ets:insert(od_flag_eqc, {Replica, Flag2, Model2}),
    {Flag2, Model2}.

disable_post(_S, _Args, Res) ->
    post_all(Res, disable).

%% ------ Grouped operator: ctx_disable
%% disable, but with a context
ctx_disable_pre(#state{replicas=Replicas}) ->
    Replicas /= [].

ctx_disable_args(#state{replicas=Replicas}) ->
    [
     elements(Replicas), %% read from
     elements(Replicas) %% send op too
    ].

%% @doc ctx_disable_pre - Ensure correct shrinking
-spec ctx_disable_pre(S :: eqc_statem:symbolic_state(),
                         Args :: [term()]) -> boolean().
ctx_disable_pre(#state{replicas=Replicas}, [From, To]) ->
    lists:member(From, Replicas) andalso lists:member(To, Replicas).

%% @doc a dynamic precondition uses concrete state, check that the
%% `From' flag is enabled
ctx_disable_dynamicpre(_S, [From, _To]) ->
    [{From, Flag, _FromORSet}] = ets:lookup(od_flag_eqc, From),
    riak_dt_od_flag:value(Flag).

ctx_disable(From, To) ->
    [{From, FromFlag, FromModel}] = ets:lookup(od_flag_eqc, From),
    [{To, ToFlag, ToModel}] = ets:lookup(od_flag_eqc, To),
    Ctx = riak_dt_od_flag:precondition_context(FromFlag),
    {ok, Flag} = riak_dt_od_flag:update(disable, To, ToFlag, Ctx),
    Model = model_ctx_disable(FromModel, ToModel),
    ets:insert(od_flag_eqc, {To, Flag, Model}),
    {Flag, Model}.

ctx_disable_post(_S, _Args, Res) ->
    post_all(Res, ctx_disable).

%% ------ Grouped operator: replicate
%% Merge two replicas' values
replicate_pre(#state{replicas=Replicas}) ->
    Replicas /= [].

replicate_args(#state{replicas=Replicas}) ->
    [
     elements(Replicas), %% Replicate from
     elements(Replicas) %% Replicate to
    ].

%% @doc replicate_pre - Ensure correct shrinking
-spec replicate_pre(S :: eqc_statem:symbolic_state(),
                    Args :: [term()]) -> boolean().
replicate_pre(#state{replicas=Replicas}, [From, To]) ->
    lists:member(From, Replicas) andalso lists:member(To, Replicas).

%% Replicate a CRDT from `From' to `To'
replicate(From, To) ->
    [{From, FromFlag, FromModel}] = ets:lookup(od_flag_eqc, From),
    [{To, ToFlag, ToModel}] = ets:lookup(od_flag_eqc, To),
    Flag = riak_dt_od_flag:merge(FromFlag, ToFlag),
    Model = model_merge(FromModel, ToModel),
    ets:insert(od_flag_eqc, {To, Flag, Model}),
    {Flag, Model}.

replicate_post(_S, _Args, Res) ->
    post_all(Res, rep).

%% @doc weights for commands. Don't create too many replicas, but
%% prejudice in favour of creating more than 1. Try and balance
%% disable with enables. But favour enables so we have something to
%% disable. See the aggregation output.
weight(S, create_replica) when length(S#state.replicas) > 2 ->
    1;
weight(S, create_replica) when length(S#state.replicas) < 5 ->
    4;
weight(_S, disable) ->
    3;
weight(_S, ctx_disable) ->
    3;
weight(_S, enable) ->
    6;
weight(_S, _) ->
    2.

%% Tests the property that an od_flag is equivalent to the flag model
prop_merge() ->
    ?FORALL(Cmds, commands(?MODULE),
            begin
                %% Store the state external to the statem for correct
                %% shrinking. This is best practice.
                ets:new(od_flag_eqc, [named_table, set]),
                {H, S, Res} = run_commands(?MODULE,Cmds),
                ReplicaData =               ets:tab2list(od_flag_eqc),
                %% Check that collapsing all values leads to the same results for flag and the Model
                {Flag, Model} = lists:foldl(fun({_Actor, F1, Mo1}, {F, Mo}) ->
                                                    {riak_dt_od_flag:merge(F, F1),
                                                     model_merge(Mo, Mo1)} end,
                                            {riak_dt_od_flag:new(), model_new()},
                                            ReplicaData),

                {FlagValue, ModelValue} = {riak_dt_od_flag:value(Flag), model_value(Model)},

                ets:delete(od_flag_eqc),

                aggregate(command_names(Cmds),
                          pretty_commands(?MODULE,Cmds, {H,S,Res},
                                          collect(with_title(value), FlagValue,
                                                    measure(replicas, length(ReplicaData),
                                                            conjunction([{result,  equals(Res, ok)},
                                                                         {values, equals(FlagValue, ModelValue)}])
                                                           ))))
            end).

%% -----------
%% Helpers
%% ----------
post_all({Flag, Model}, Cmd) ->
    %% What matters is that both types have the exact same value.
    case riak_dt_od_flag:value(Flag) == model_value(Model) of
        true ->
            true;
        _ ->
            {postcondition_failed, "Flag and Model don't match", Cmd, Flag, Model}
    end.

%% -----------
%% Model
%% ----------

-type model() :: {Cntr :: pos_integer(), %% Unique ID per operation
                      Enables :: list(), %% enables
                      Disable :: list(), %% Tombstones
                      %% Disable that are waiting for enables before they
                      %% can be run (like context ops in the
                      %% riak_dt_od_flag)
                      Deferred :: list()
                     }.

-spec model_new() -> model().
model_new() ->
    {[], [], []}.

model_enable(Cnt, {Enables, Disables, Deferred}) ->
    {ok, {lists:umerge([Cnt], Enables), Disables, Deferred}}.

model_disable({Enables, Disables, Deferred}) ->
    {Enables, lists:merge(Disables, Enables), Deferred}.

model_merge({Enables1, Disables1, Deferred1}, {Enables2, Disables2, Deferred2}) ->
    Enables = lists:umerge(Enables1, Enables2),
    Disables = lists:umerge(Disables1, Disables2),
    Deferred = lists:umerge(Deferred1, Deferred2),
    model_apply_deferred(Enables, Disables, Deferred).

model_apply_deferred(Enables, Disables, Deferred) ->
    D2 = lists:subtract(Deferred, Enables),
    Dis = lists:subtract(Deferred, D2),
    {Enables, lists:umerge(Dis, Disables), D2}.

model_ctx_disable({FromEnables, _FromDisables, _FromDeferred}, {ToEnables, ToDisables, ToDeferred}) ->
    %% Disable enables that are in both
    Disable = sets:to_list(sets:intersection(sets:from_list(FromEnables), sets:from_list(ToEnables))),
    %% Defer those enables not seen in target
    Defer = lists:subtract(FromEnables, ToEnables),
    {ToEnables, lists:umerge(ToDisables, Disable), lists:umerge(Defer, ToDeferred)}.

model_value({Enables, Disables, _Deferred}) ->
    Remaining = lists:subtract(Enables, Disables),
    length(Remaining) > 0.

-endif. % EQC
